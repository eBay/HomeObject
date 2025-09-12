#include <homestore/homestore.hpp>
#include <homestore/blkdata_service.hpp>
#include <homestore/meta_service.hpp>
#include <homestore/replication_service.hpp>

#include "hs_homeobject.hpp"
#include "replication_message.hpp"
#include "replication_state_machine.hpp"
#include "lib/homeobject_impl.hpp"

namespace homeobject {

SISL_LOGGING_DECL(shardmgr)

#define SLOG(level, trace_id, shard_id, msg, ...)                                                                      \
    LOG##level##MOD(shardmgr, "[trace_id={},shardID=0x{:x},pg={},shard=0x{:x}] " msg, trace_id, shard_id,              \
                    (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask), ##__VA_ARGS__)

#define SLOGT(trace_id, shard_id, msg, ...) SLOG(TRACE, trace_id, shard_id, msg, ##__VA_ARGS__)
#define SLOGD(trace_id, shard_id, msg, ...) SLOG(DEBUG, trace_id, shard_id, msg, ##__VA_ARGS__)
#define SLOGI(trace_id, shard_id, msg, ...) SLOG(INFO, trace_id, shard_id, msg, ##__VA_ARGS__)
#define SLOGW(trace_id, shard_id, msg, ...) SLOG(WARN, trace_id, shard_id, msg, ##__VA_ARGS__)
#define SLOGE(trace_id, shard_id, msg, ...) SLOG(ERROR, trace_id, shard_id, msg, ##__VA_ARGS__)
#define SLOGC(trace_id, shard_id, msg, ...) SLOG(CRITICAL, trace_id, shard_id, msg, ##__VA_ARGS__)

ShardError toShardError(ReplServiceError const& e) {
    switch (e) {
    case ReplServiceError::BAD_REQUEST:
        [[fallthrough]];
    case ReplServiceError::CANCELLED:
        [[fallthrough]];
    case ReplServiceError::CONFIG_CHANGING:
        [[fallthrough]];
    case ReplServiceError::SERVER_ALREADY_EXISTS:
        [[fallthrough]];
    case ReplServiceError::SERVER_IS_JOINING:
        [[fallthrough]];
    case ReplServiceError::SERVER_IS_LEAVING:
        [[fallthrough]];
    case ReplServiceError::RESULT_NOT_EXIST_YET:
        [[fallthrough]];
    case ReplServiceError::TERM_MISMATCH:
        return ShardError(ShardErrorCode::PG_NOT_READY);
    case ReplServiceError::NO_SPACE_LEFT:
        return ShardError(ShardErrorCode::NO_SPACE_LEFT);
    case ReplServiceError::NOT_LEADER:
        return ShardError(ShardErrorCode::NOT_LEADER);
    case ReplServiceError::TIMEOUT:
        return ShardError(ShardErrorCode::TIMEOUT);
    case ReplServiceError::NOT_IMPLEMENTED:
        return ShardError(ShardErrorCode::UNSUPPORTED_OP);
    case ReplServiceError::OK:
        DEBUG_ASSERT(false, "Should not process OK!");
        [[fallthrough]];
    case ReplServiceError::FAILED:
        return ShardError(ShardErrorCode::UNKNOWN);
    default:
        return ShardError(ShardErrorCode::UNKNOWN);
    }
}

uint64_t ShardManager::max_shard_size() { return Gi; }

uint64_t ShardManager::max_shard_num_in_pg() { return ((uint64_t)0x01) << shard_width; }

shard_id_t HSHomeObject::generate_new_shard_id(pg_id_t pgid) {
    std::scoped_lock lock_guard(_pg_lock);
    auto hs_pg = const_cast< HS_PG* >(_get_hs_pg_unlocked(pgid));
    RELEASE_ASSERT(hs_pg, "Missing pg info");

    auto new_sequence_num = ++hs_pg->shard_sequence_num_;
    RELEASE_ASSERT(new_sequence_num < ShardManager::max_shard_num_in_pg(),
                   "new shard id must be less than ShardManager::max_shard_num_in_pg()");
    return make_new_shard_id(pgid, new_sequence_num);
}

uint64_t HSHomeObject::get_sequence_num_from_shard_id(uint64_t shard_id) {
    return shard_id & (max_shard_num_in_pg() - 1);
}

std::string HSHomeObject::serialize_shard_info(const ShardInfo& info) {
    nlohmann::json j;
    j["shard_info"]["shard_id_t"] = info.id;
    j["shard_info"]["pg_id_t"] = info.placement_group;
    j["shard_info"]["state"] = info.state;
    j["shard_info"]["lsn"] = info.lsn;
    j["shard_info"]["sealed_lsn"] = info.sealed_lsn;
    j["shard_info"]["created_time"] = info.created_time;
    j["shard_info"]["modified_time"] = info.last_modified_time;
    j["shard_info"]["total_capacity"] = info.total_capacity_bytes;
    j["shard_info"]["available_capacity"] = info.available_capacity_bytes;
    return j.dump();
}

ShardInfo HSHomeObject::deserialize_shard_info(const char* json_str, size_t str_size) {
    ShardInfo shard_info;
    auto shard_json = nlohmann::json::parse(json_str, json_str + str_size);
    shard_info.id = shard_json["shard_info"]["shard_id_t"].get< shard_id_t >();
    shard_info.placement_group = shard_json["shard_info"]["pg_id_t"].get< pg_id_t >();
    shard_info.state = static_cast< ShardInfo::State >(shard_json["shard_info"]["state"].get< int >());
    shard_info.lsn = shard_json["shard_info"]["lsn"].get< uint64_t >();
    shard_info.sealed_lsn = shard_json["shard_info"]["sealed_lsn"].get< uint64_t >();
    shard_info.created_time = shard_json["shard_info"]["created_time"].get< uint64_t >();
    shard_info.last_modified_time = shard_json["shard_info"]["modified_time"].get< uint64_t >();
    shard_info.available_capacity_bytes = shard_json["shard_info"]["available_capacity"].get< uint64_t >();
    shard_info.total_capacity_bytes = shard_json["shard_info"]["total_capacity"].get< uint64_t >();
    return shard_info;
}

ShardManager::AsyncResult< ShardInfo > HSHomeObject::_create_shard(pg_id_t pg_owner, uint64_t size_bytes,
                                                                   trace_id_t tid) {
    if (is_shutting_down()) {
        LOGI("service is being shut down");
        return folly::makeUnexpected(ShardError(ShardErrorCode::SHUTTING_DOWN));
    }
    incr_pending_request_num();

    auto hs_pg = get_hs_pg(pg_owner);
    if (!hs_pg) {
        LOGW("failed to create shard with non-exist pg={}", pg_owner);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::UNKNOWN_PG));
    }
    if (hs_pg->pg_state_.is_state_set(PGStateMask::DISK_DOWN)) {
        LOGW("failed to create shard for pg={}, pg is disk down and not leader", pg_owner);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::NOT_LEADER));
    }
    auto repl_dev = hs_pg->repl_dev_;

    if (!repl_dev) {
        LOGW("failed to get repl dev instance for pg={}", pg_owner);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::PG_NOT_READY));
    }

    if (!repl_dev->is_leader()) {
        LOGW("failed to create shard for pg={}, not leader", pg_owner);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::NOT_LEADER, repl_dev->get_leader_id()));
    }

    if (!repl_dev->is_ready_for_traffic()) {
        LOGW("failed to create shard for pg={}, not ready for traffic", pg_owner);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::RETRY_REQUEST));
    }
    auto new_shard_id = generate_new_shard_id(pg_owner);
    SLOGD(tid, new_shard_id, "Create shard request: pg={}, size={}", pg_owner, size_bytes);

    // select chunk for shard.
    const auto v_chunkID = chunk_selector()->pick_most_available_blk_chunk(new_shard_id, pg_owner);

    if (!v_chunkID.has_value()) {
        SLOGW(tid, new_shard_id, "no availble chunk left to create shard for pg={}", pg_owner);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::NO_SPACE_LEFT));
    }

    // now, we put allocate blk for shard head/footer in on_commit of create/seal shard, so we have to make sure that
    // the blk can be successfully allocated immediately(or after emergent gc). otherwise, on_commit can not go ahead
    // and the whole raft group will be blocked forever.
    const auto v_chunk_id = v_chunkID.value();
    const auto exVchunk = chunk_selector()->get_pg_vchunk(pg_owner, v_chunk_id);

    // only seal_shard(footer) can used reserved space, so +2 here means we can at least write a shard header and a blob
    // except shard footer.
    if (exVchunk->available_blks() < get_reserved_blks() + 2) {
        const auto pchunk_id = exVchunk->get_chunk_id();
        LOGW("failed to create shard for pg={}, pchunk_id= {} is selected for vchunk_id={} is selected, not enough "
             "left space",
             pg_owner, pchunk_id, v_chunk_id);

        bool res = chunk_selector()->release_chunk(pg_owner, v_chunk_id);
        RELEASE_ASSERT(res, "Failed to release v_chunk_id={}, pg={}", v_chunk_id, pg_owner);

        auto gc_mgr = gc_manager();
        if (gc_mgr->is_started()) { gc_manager()->submit_gc_task(task_priority::normal, pchunk_id); }

        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::NO_SPACE_LEFT));
    }

    SLOGD(tid, new_shard_id, "vchunk_id={}", v_chunk_id);

    auto req = repl_result_ctx< ShardManager::Result< ShardInfo > >::make(0u /* header_extn_size */, 0u /* key_size */);

    // prepare msg header, log only
    req->header()->msg_type = ReplicationMessageType::CREATE_SHARD_MSG;
    req->header()->pg_id = pg_owner;
    req->header()->shard_id = new_shard_id;
    req->header()->vchunk_id = v_chunk_id;
    req->header()->payload_size = 0;
    req->header()->payload_crc = 0;
    req->header()->seal();

    // replicate this create shard message to PG members;
    repl_dev->async_alloc_write(req->cheader_buf(), sisl::blob{}, sisl::sg_list{}, req, false /* part_of_batch */, tid);
    return req->result().deferValue([this, req, repl_dev, tid, pg_owner, new_shard_id,
                                     v_chunk_id](const auto& result) -> ShardManager::AsyncResult< ShardInfo > {
        if (result.hasError()) {
            auto err = result.error();
            if (err.getCode() == ShardErrorCode::NOT_LEADER) { err.current_leader = repl_dev->get_leader_id(); }

            // we will never get no_space_left error here.
            bool res = chunk_selector()->release_chunk(pg_owner, v_chunk_id);
            RELEASE_ASSERT(res, "Failed to release v_chunk_id={}, pg={}", v_chunk_id, pg_owner);

            SLOGE(tid, new_shard_id, "got {} when creating shard at leader, failed to create shard {}!", err.getCode(),
                  new_shard_id);

            decr_pending_request_num();
            return folly::makeUnexpected(err);
        }
        auto shard_info = result.value();
        SLOGD(tid, shard_info.id, "Shard created success.");
        decr_pending_request_num();
        return shard_info;
    });
}

ShardManager::AsyncResult< ShardInfo > HSHomeObject::_seal_shard(ShardInfo const& info, trace_id_t tid) {
    if (is_shutting_down()) {
        LOGI("service is being shut down");
        return folly::makeUnexpected(ShardError(ShardErrorCode::SHUTTING_DOWN));
    }
    incr_pending_request_num();

    const auto pg_id = info.placement_group;
    const auto shard_id = info.id;
    SLOGD(tid, shard_id, "Seal shard request: is_open={}", info.is_open());
    auto hs_pg = get_hs_pg(pg_id);
    if (!hs_pg) {
        SLOGW(tid, shard_id, "pg={} not found", pg_id);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::UNKNOWN_PG));
    }

    if (hs_pg->pg_state_.is_state_set(PGStateMask::DISK_DOWN)) {
        LOGW("failed to seal shard for pg={}, pg is disk down and not leader", pg_id);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::NOT_LEADER));
    }

    auto repl_dev = hs_pg->repl_dev_;
    if (!repl_dev) {
        SLOGW(tid, shard_id, "failed to get repl dev instance for pg={}", pg_id);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::PG_NOT_READY));
    }

    if (!repl_dev->is_leader()) {
        SLOGW(tid, shard_id, "failed to seal shard, not leader");
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::NOT_LEADER, repl_dev->get_leader_id()));
    }

    if (!repl_dev->is_ready_for_traffic()) {
        SLOGW(tid, shard_id, "failed to seal shard, not ready for traffic");
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::RETRY_REQUEST));
    }

    auto v_chunkID = get_shard_v_chunk_id(shard_id);
    if (!v_chunkID.has_value()) {
        SLOGW(tid, shard_id, "failed to seal shard, vchunk id not found");
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::UNKNOWN_SHARD));
    }

    auto req = repl_result_ctx< ShardManager::Result< ShardInfo > >::make(0u /* header_extn_size */, 0u /* key_size */);
    req->header()->msg_type = ReplicationMessageType::SEAL_SHARD_MSG;
    req->header()->pg_id = pg_id;
    req->header()->shard_id = shard_id;
    req->header()->vchunk_id = v_chunkID.value();
    req->header()->payload_size = 0;
    req->header()->payload_crc = 0;
    req->header()->seal();

    // replicate this seal shard message to PG members;
    repl_dev->async_alloc_write(req->cheader_buf(), sisl::blob{}, sisl::sg_list{}, req, false /* part_of_batch */, tid);
    return req->result().deferValue(
        [this, req, repl_dev, tid](const auto& result) -> ShardManager::AsyncResult< ShardInfo > {
            if (result.hasError()) {
                auto err = result.error();
                if (err.getCode() == ShardErrorCode::NOT_LEADER) { err.current_leader = repl_dev->get_leader_id(); }
                decr_pending_request_num();
                return folly::makeUnexpected(err);
            }
            auto shard_info = result.value();
            SLOGD(tid, shard_info.id, "Seal shard request: Shard sealed success, is_open={}", shard_info.is_open());
            decr_pending_request_num();
            return shard_info;
        });
}

// move seal_shard to pre_commit can not fundamentally solve the conflict between seal_shard and put_blob, since
// put_blob handler will only check the shard state at the very beginning and will not check again before proposing to
// raft, so we need a callback to check whether we can handle this request before appending log, which is previous to
// pre_commit.

// FIXME after we have the callback, which is coming in homestore.

bool HSHomeObject::on_shard_message_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                               cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    auto tid = hs_ctx ? hs_ctx->traceID() : 0;
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());

    if (msg_header->corrupted()) {
        RELEASE_ASSERT(
            false, "replication message header is corrupted with crc error when pre_committing shard message, lsn={}",
            lsn);
        return false;
    }

    const auto msg_type = msg_header->msg_type;

    RELEASE_ASSERT(msg_type == ReplicationMessageType::CREATE_SHARD_MSG ||
                       msg_type == ReplicationMessageType::SEAL_SHARD_MSG,
                   "unsupport message tyep {} when pre committing shard message, fatal error!", msg_type);

    const auto& shard_id = msg_header->shard_id;

    if (msg_type == ReplicationMessageType::CREATE_SHARD_MSG) {
        SLOGD(tid, shard_id, "pre_commit create_shard message, type={}, lsn= {}", msg_header->msg_type, lsn);
    } else {
        SLOGD(tid, shard_id, "pre_commit seal_shard message, type={}, lsn= {}", msg_header->msg_type, lsn);

        {
            std::scoped_lock lock_guard(_shard_lock);
            auto iter = _shard_map.find(shard_id);
            if (iter == _shard_map.end()) {
                // if the create_shard message of this shard is not committed yet at this moment, we can not find it in
                // pre_commit sealing shard.
                SLOGW(tid, shard_id, "try to seal a shard, but not exist ATM! lsn={}", lsn);
                return false;
            }

            auto& state = (*iter->second)->info.state;
            // we just change the state to SEALED, so that it will fail the later coming put_blob on this shard and will
            // be easy for rollback. the update of superblk will be done in on_shard_message_commit;

            // note that , this is a best effort and we can not 100% avoid put_blob to a sealed shard, because the shard
            // state is open when checking the shard state, but changed immediately to sealed by pre_commiting sealing
            // shard, and as a result, this put blob might scheduled to an seald shard.
            if (state == ShardInfo::State::OPEN) {
                state = ShardInfo::State::SEALED;
            } else {
                SLOGW(tid, shard_id, "try to seal an unopened shard, lsn={}", lsn);
            }
        }
    }
    return true;
}

void HSHomeObject::on_shard_message_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                             cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< ShardManager::Result< ShardInfo > >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer()) {
        ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(hs_ctx).get();
    }
    auto tid = hs_ctx ? hs_ctx->traceID() : 0;
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());

    if (msg_header->corrupted()) {
        RELEASE_ASSERT(false,
                       "replication message header is corrupted with crc error in on_rollback, lsn={}, traceID={}", lsn,
                       tid);
        return;
    }

    const auto msg_type = msg_header->msg_type;

    RELEASE_ASSERT(msg_type == ReplicationMessageType::CREATE_SHARD_MSG ||
                       msg_type == ReplicationMessageType::SEAL_SHARD_MSG,
                   "unsupport message tyep {} when pre committing shard message, fatal error!", msg_type);

    const auto shard_id = msg_header->shard_id;

    // since we do nothing in pre_commit, so we do nothing in rollback
    if (msg_type == ReplicationMessageType::CREATE_SHARD_MSG) {
        SLOGD(tid, shard_id, "rollback create shard message, type={}, lsn= {}", msg_header->msg_type, lsn);
    } else {
        SLOGD(tid, shard_id, "rollback seal shard message, type={}, lsn= {}", msg_header->msg_type, lsn);
    }

    if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError(ShardErrorCode::RETRY_REQUEST))); }
}

void HSHomeObject::local_create_shard(ShardInfo shard_info, homestore::chunk_num_t v_chunk_id,
                                      homestore::chunk_num_t p_chunk_id, trace_id_t tid) {
    bool shard_exist = false;
    {
        scoped_lock lock_guard(_shard_lock);
        shard_exist = (_shard_map.find(shard_info.id) != _shard_map.end());
    }

    if (!shard_exist) {
        add_new_shard_to_map(std::make_unique< HS_Shard >(shard_info, p_chunk_id, v_chunk_id));
    } else {
        SLOGD(tid, shard_info.id, "shard already exist, this should happen in log replay case,  skip creating shard");
    }
}

void HSHomeObject::on_shard_message_commit(int64_t lsn, sisl::blob const& h, shared< homestore::ReplDev > repl_dev,
                                           cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< ShardManager::Result< ShardInfo > >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer()) {
        ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(hs_ctx).get();
    }
    auto tid = hs_ctx ? hs_ctx->traceID() : 0;
    auto header = r_cast< const ReplicationMessageHeader* >(h.cbytes());
    if (header->corrupted()) {
        RELEASE_ASSERT(false, "replication message header is corrupted with crc error in on_commit, lsn={}, traceID={}",
                       lsn, tid);
        return;
    }
    const auto shard_id = header->shard_id;

    RELEASE_ASSERT(header->msg_type == ReplicationMessageType::CREATE_SHARD_MSG ||
                       header->msg_type == ReplicationMessageType::SEAL_SHARD_MSG,
                   "unsupport message tyep {} when committing shard message, fatal error!", header->msg_type);

#ifdef VADLIDATE_ON_REPLAY
    sisl::io_blob_safe value_blob(blkids.blk_count() * repl_dev->get_blk_size(), io_align);
    sisl::sg_list value_sgs;
    value_sgs.iovs.emplace_back(iovec{.iov_base = value_blob.bytes(), .iov_len = value_blob.size()});
    value_sgs.size += value_blob.size();

    // Do a read sync read and validate the crc
    std::error_code err = repl_dev->async_read(blkids, value_sgs, value_blob.size()).get();
    if (err) {
        LOGW("failed to read data from homestore blks, lsn={}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError(ShardErrorCode::UNKNOWN));
        }
        return;
    }

    if (crc32_ieee(init_crc32, value.cbytes(), value.size()) != header->payload_crc) {
        // header & value is inconsistent;
        LOGW("replication message header is inconsistent with value, lsn={}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError(ShardErrorCode::CRC_MISMATCH));
        }
        return;
    }
#endif

    // allocate blk for shard header/footer
    const auto pg_id = header->pg_id;
    const auto vchunk_id = header->vchunk_id;

    homestore::blk_alloc_hints hints;
    hints.application_hint = static_cast< uint64_t >(pg_id) << 16 | vchunk_id;
    hints.reserved_blks = header->msg_type == ReplicationMessageType::CREATE_SHARD_MSG ? get_reserved_blks() : 0;

    homestore::MultiBlkId blkids;
    homestore::BlkAllocStatus alloc_status;
    auto gc_mgr = gc_manager();

    for (uint8_t i = 0; i < 5; ++i) {
        // we need select the chunk before starting allocate blk for shard header, so that we can make sure gc will not
        // hit this chunk after we allocate blk for shard header.Here, we specify the application_hint, so it will try
        // to select the vchunk for chunk selector(call select_specific_chunk).
        alloc_status = homestore::data_service().alloc_blks(
            sisl::round_up(sizeof(shard_info_superblk), repl_dev->get_blk_size()), hints, blkids);

        if (alloc_status == homestore::BlkAllocStatus::SUCCESS) {
            SLOGD(tid, shard_id, "successfully alloc blk for committing shard message, lsn={}", lsn);
            break;
        } else if (alloc_status == homestore::BlkAllocStatus::SPACE_FULL) {
            SLOGD(tid, shard_id, "no_space_left happens when allocating blk for committing shard message, lsn={}", lsn);
            if (!gc_mgr->is_started()) {
                RELEASE_ASSERT(false,
                               "no_space_left when allocating blk for committing shard message, but gc is disabled, "
                               "can not proceed, fatal error!");
            }

            auto vchunk = chunk_selector_->get_pg_vchunk(pg_id, vchunk_id);
            RELEASE_ASSERT(vchunk != nullptr, "chunk selection failed with vchunk_id={} in pg={}", vchunk_id, pg_id);
            const auto pchunk = vchunk->get_chunk_id();

            // although we have checked the available_blks of the selected chunk before accept the creating shard
            // request, there is also a case that we have to trigger emergent gc here, for example, a staled push_data
            // of a put_blob will consume the capacity of this chunk and lead to no_space_left.
            auto ret = gc_mgr->submit_gc_task(task_priority::emergent, pchunk).get();
            SLOGD(tid, shard_id, "emergent gc is completed for chunk_id={}, lsn={}, ok={}", pchunk, lsn, ret);
        } else {
            RELEASE_ASSERT(false,
                           "fatal error={} happens when allocating blk for committing shard message, vchunk={}, lsn={}",
                           alloc_status, vchunk_id, lsn);
        }
    }

    RELEASE_ASSERT(
        alloc_status == homestore::BlkAllocStatus::SUCCESS,
        "can not allocate blk for shard meta blk after trying for 5 times, fatal error! vchunk={}, pg={}, shard={}",
        vchunk_id, pg_id, shard_id);

    ShardInfo shard_info;

    switch (header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        SLOGD(tid, shard_id, "pchunk {} is selected for vchunk {} in pg {} for creating shard", blkids.chunk_num(),
              vchunk_id, pg_id);
        // fill shard info
        shard_info.id = shard_id;
        shard_info.placement_group = pg_id;
        shard_info.created_time = get_current_timestamp();
        shard_info.last_modified_time = get_current_timestamp();
        shard_info.total_capacity_bytes = blkids.blk_count() * homestore::data_service().get_blk_size();
        shard_info.lsn = lsn;
        shard_info.state = ShardInfo::State::OPEN;
        shard_info.available_capacity_bytes = shard_info.total_capacity_bytes;

        local_create_shard(shard_info, vchunk_id, blkids.chunk_num(), tid);
        if (ctx) { ctx->promise_.setValue(ShardManager::Result< ShardInfo >(shard_info)); }
        SLOGD(tid, shard_id, "Commit done for creating shard at lsn={}", lsn);
        break;
    }

    case ReplicationMessageType::SEAL_SHARD_MSG: {
        {
            std::scoped_lock lock_guard(_shard_lock);
            auto iter = _shard_map.find(shard_id);
            RELEASE_ASSERT(iter != _shard_map.end(), "shardID=0x{:x}, pg={}, shard=0x{:x}, shard does not exist",
                           shard_id, (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask));
            shard_info = (*iter->second)->info;
        }

        // if we fail the pre_commit for sealing shard(when pre_commit sealing shard, the create_shard is not committed
        // yet), when reaching here, the shard state will be open. we just seal it here if the above case happens.
        if (shard_info.state != ShardInfo::State::SEALED) {
            SLOGW(tid, shard_id,
                  "the shard state is not sealed when committing seal_shard message at lsn={}, change it to sealed!",
                  lsn);
            shard_info.state = ShardInfo::State::SEALED;
        }

        shard_info.sealed_lsn = lsn;
        bool res = chunk_selector()->release_chunk(pg_id, vchunk_id);
        RELEASE_ASSERT(res, "Failed to release v_chunk_id={}, pg={}", vchunk_id, pg_id);
        update_shard_in_map(shard_info);

        if (ctx) { ctx->promise_.setValue(ShardManager::Result< ShardInfo >(shard_info)); }
        SLOGD(tid, shard_id, "Commit done for sealing shard at lsn={}", lsn);
    }

    default:
        break;
    }

    // write shard header/footer blk. in log replay case, one more shard header/footer will be written again. it does
    // not matter.
    sisl::sg_list sgs = generate_shard_super_blk_sg_list(shard_id);
    RELEASE_ASSERT(sgs.iovs.size() == 1, "sgs.iovs.size() for shard header/footer should be 1, but not!");
    homestore::data_service().async_write(sgs, blkids).thenValue([sgs, lsn, msgtype = header->msg_type](auto&& err) {
        // it does not matter if fail to write shard header/footer, we never read them
        if (err) { LOGW("failed to write shard super blk, err={}, lsn={}, msgType={}", err.message(), lsn, msgtype); }
        iomanager.iobuf_free(reinterpret_cast< uint8_t* >(sgs.iovs[0].iov_base));
    });

    auto hs_pg = get_hs_pg(pg_id);
    RELEASE_ASSERT(hs_pg != nullptr, "shardID=0x{:x}, pg={}, shard=0x{:x}, PG not found", shard_id,
                   (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask));
    const_cast< HS_PG* >(hs_pg)->durable_entities_update(
        // shard footer/header will also take one blk.
        [](auto& de) { de.total_occupied_blk_count.fetch_add(1, std::memory_order_relaxed); });
}

void HSHomeObject::on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf) {
    homestore::superblk< shard_info_superblk > sb(_shard_meta_name);
    sb.load(buf, mblk);
    add_new_shard_to_map(std::make_unique< HS_Shard >(std::move(sb)));
}

void HSHomeObject::on_shard_meta_blk_recover_completed(bool success) {
    std::unordered_set< homestore::chunk_num_t > excluding_chunks;
    std::scoped_lock lock_guard(_pg_lock);
    for (auto& pair : _pg_map) {
        if (dynamic_cast< HS_PG* >(pair.second.get())->pg_state_.is_state_set(PGStateMask::DISK_DOWN)) {
            LOGW("pg={} is disk down, skip recover chunk state from shards", pair.first);
            continue;
        }
        excluding_chunks.clear();
        excluding_chunks.reserve(pair.second->shards_.size());
        for (auto& shard : pair.second->shards_) {
            if (shard->info.state == ShardInfo::State::OPEN) {
                excluding_chunks.emplace(d_cast< HS_Shard* >(shard.get())->sb_->v_chunk_id);
            }
        }
        bool res = chunk_selector_->recover_pg_chunks_states(pair.first, excluding_chunks);
        RELEASE_ASSERT(res, "Failed to recover pg chunk heap, pg={}", pair.first);
    }
}

void HSHomeObject::add_new_shard_to_map(std::unique_ptr< HS_Shard > shard) {
    // TODO: We are taking a global lock for all pgs to create shard. Is it really needed??
    // We need to have fine grained per PG lock and take only that.
    std::scoped_lock lock_guard(_pg_lock, _shard_lock);
    auto hs_pg = const_cast< HS_PG* >(_get_hs_pg_unlocked(shard->info.placement_group));
    RELEASE_ASSERT(hs_pg, "Missing pg info, pg={}", shard->info.placement_group);
    if (hs_pg->pg_state_.is_state_set(PGStateMask::DISK_DOWN)) {
        LOGW("pg={} is disk down, skip add shard to map, shardID=0x{:x}", shard->info.placement_group, shard->info.id);
        return;
    }
    auto p_chunk_id = shard->p_chunk_id();
    auto& shards = hs_pg->shards_;
    auto shard_id = shard->info.id;
    auto iter = shards.emplace(shards.end(), std::move(shard));
    auto [_, happened] = _shard_map.emplace(shard_id, iter);
    RELEASE_ASSERT(happened, "shardID=0x{:x}, pg={}, shard=0x{:x}, duplicated shard info", shard_id,
                   (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask));

    const auto [it, h] = chunk_to_shards_map_.try_emplace(p_chunk_id, std::set< shard_id_t >());
    if (h) { LOGDEBUG("chunk_id={} is not in chunk_to_shards_map, add it", p_chunk_id); }
    auto& per_chunk_shard_list = it->second;
    const auto inserted = (per_chunk_shard_list.emplace(shard_id)).second;

    RELEASE_ASSERT(inserted,
                   "shardID=0x{:x}, pg={}, shard=0x{:x}, duplicated shard info found when inserting into "
                   "per_chunk_shard_list for chunk_id={}",
                   shard_id, (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask), p_chunk_id);

    // following part gives follower members a chance to catch up shard sequence num;
    auto sequence_num = get_sequence_num_from_shard_id(shard_id);
    if (sequence_num > hs_pg->shard_sequence_num_) { hs_pg->shard_sequence_num_ = sequence_num; }
}

void HSHomeObject::update_shard_in_map(const ShardInfo& shard_info) {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(shard_info.id);
    RELEASE_ASSERT(shard_iter != _shard_map.end(), "shardID=0x{:x}, pg={}, shard=0x{:x}, shard does not exist",
                   shard_info.id, (shard_info.id >> homeobject::shard_width), (shard_info.id & homeobject::shard_mask));
    auto hs_shard = d_cast< HS_Shard* >((*shard_iter->second).get());
    hs_shard->update_info(shard_info);
}

const Shard* HSHomeObject::_get_hs_shard(const shard_id_t shard_id) const {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(shard_id);
    if (shard_iter == _shard_map.end()) { return nullptr; }
    return (*shard_iter->second).get();
}

std::optional< homestore::chunk_num_t > HSHomeObject::get_shard_p_chunk_id(shard_id_t id) const {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(id);
    if (shard_iter == _shard_map.end()) { return std::nullopt; }
    auto hs_shard = d_cast< HS_Shard* >((*shard_iter->second).get());
    return std::make_optional< homestore::chunk_num_t >(hs_shard->sb_->p_chunk_id);
}

const std::set< shard_id_t > HSHomeObject::get_shards_in_chunk(homestore::chunk_num_t chunk_id) const {
    std::scoped_lock lock_guard(_shard_lock);
    const auto it = chunk_to_shards_map_.find(chunk_id);
    if (it == chunk_to_shards_map_.cend()) {
        LOGW("chunk_id={} not found in chunk_to_shards_map", chunk_id);
        return {};
    }
    return it->second;
}

void HSHomeObject::update_shard_meta_after_gc(const homestore::chunk_num_t move_from_chunk,
                                              const homestore::chunk_num_t move_to_chunk, const uint64_t task_id) {
    auto shards = get_shards_in_chunk(move_from_chunk);

    // TODO::optimize this lock
    std::scoped_lock lock_guard(_shard_lock);

    // if crash recovery happens, some shards(which existed in the move_from_chunk) had been updated to the
    // move_to_chunk, but others might not

    auto iter = chunk_to_shards_map_.find(move_from_chunk);
    if (iter == chunk_to_shards_map_.end()) {
        LOGW("gc task_id={}, find no shard in move_from_chunk={}, skip update shard meta blk after gc!", task_id,
             move_from_chunk);
        return;
    }

    auto& shards_in_move_to_chunk = chunk_to_shards_map_[move_to_chunk];

    for (const auto& shard_id : iter->second) {
        auto shard_iter = _shard_map.find(shard_id);

        RELEASE_ASSERT(
            shard_iter != _shard_map.end(),
            "try to update shard meta blk after gc, but shard {} does not exist!! move_from_chunk={}, move_to_chunk={}",
            shard_id, move_from_chunk, move_to_chunk);

        auto hs_shard = d_cast< HS_Shard* >((*shard_iter->second).get());

        RELEASE_ASSERT(
            hs_shard->p_chunk_id() == move_from_chunk,
            "shardID=0x{:x}, pg={}, shard=0x{:x}, "
            "p_chunk_id={} is expected to exist in move_from_chunk={}, but exist in chunk={}, move_to_chunk={}",
            shard_id, (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask),
            hs_shard->p_chunk_id(), move_from_chunk, hs_shard->p_chunk_id(), move_to_chunk);

        auto shard_info = hs_shard->info;
        shard_info.last_modified_time = get_current_timestamp();
        if (shard_info.state == ShardInfo::State::SEALED) { shard_info.available_capacity_bytes = 0; }

        // total_capacity_bytes and available_capacity_bytes are not used since we never set a limitation for shard
        // capacity

        hs_shard->update_info(shard_info, move_to_chunk);
        LOGD("gc task_id={}, update shard={} pchunk from {} to {}", task_id, shard_id, move_from_chunk, move_to_chunk);
        shards_in_move_to_chunk.insert(shard_id);
    }

    chunk_to_shards_map_.erase(iter);
}

std::optional< homestore::chunk_num_t > HSHomeObject::get_shard_v_chunk_id(const shard_id_t id) const {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(id);
    if (shard_iter == _shard_map.end()) { return std::nullopt; }
    auto hs_shard = d_cast< HS_Shard* >((*shard_iter->second).get());
    return std::make_optional< homestore::chunk_num_t >(hs_shard->sb_->v_chunk_id);
}

bool HSHomeObject::release_chunk_based_on_create_shard_message(sisl::blob const& header) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGW("replication message header is corrupted with crc error");
        return false;
    }

    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        const pg_id_t pg_id = msg_header->pg_id;
        const auto shard_id = msg_header->shard_id;
        if (!pg_exists(pg_id)) {
            LOGW("shardID=0x{:x}, pg={}, shard=0x{:x}, Requesting a chunk for an unknown pg={}", shard_id,
                 (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask), pg_id);
            return false;
        }
        auto sb = r_cast< shard_info_superblk const* >(header.cbytes() + sizeof(ReplicationMessageHeader));
        bool res = chunk_selector_->release_chunk(sb->info.placement_group, sb->v_chunk_id);
        if (!res) { LOGW("Failed to release chunk {} to pg={}", sb->v_chunk_id, sb->info.placement_group); }
        return res;
    }
    default: {
        LOGW("Unexpected message type encountered={}. This function should only be called with 'CREATE_SHARD_MSG'.",
             msg_header->msg_type);
        return false;
    }
    }
}

void HSHomeObject::destroy_shards(pg_id_t pg_id) {
    auto lg = std::scoped_lock(_pg_lock, _shard_lock);
    auto hs_pg = _get_hs_pg_unlocked(pg_id);
    if (hs_pg == nullptr) {
        LOGW("on shards destroy with unknown pg={}", pg_id);
        return;
    }

    for (auto& shard : hs_pg->shards_) {
        auto hs_shard = s_cast< HS_Shard* >(shard.get());
        chunk_to_shards_map_.erase(hs_shard->p_chunk_id());
        // destroy shard super blk
        hs_shard->sb_.destroy();
        // erase shard in shard map
        _shard_map.erase(shard->info.id);
    }
    LOGD("Shards in pg={} have all been destroyed", pg_id);
}

HSHomeObject::HS_Shard::HS_Shard(ShardInfo shard_info, homestore::chunk_num_t p_chunk_id,
                                 homestore::chunk_num_t v_chunk_id) :
        Shard(std::move(shard_info)), sb_(_shard_meta_name) {
    sb_.create(sizeof(shard_info_superblk));
    sb_->info = info;
    sb_->p_chunk_id = p_chunk_id;
    sb_->v_chunk_id = v_chunk_id;
    sb_.write();
}

HSHomeObject::HS_Shard::HS_Shard(homestore::superblk< shard_info_superblk >&& sb) :
        Shard(sb->info), sb_(std::move(sb)) {}

void HSHomeObject::HS_Shard::update_info(const ShardInfo& shard_info,
                                         std::optional< homestore::chunk_num_t > p_chunk_id) {
    if (p_chunk_id != std::nullopt) { sb_->p_chunk_id = p_chunk_id.value(); }
    info = shard_info;
    sb_->info = info;
    sb_.write();
}

sisl::sg_list HSHomeObject::generate_shard_super_blk_sg_list(shard_id_t shard_id) {
    // TODO: do the buffer check before using it.
    auto raw_shard_sb = _get_hs_shard(shard_id);
    RELEASE_ASSERT(raw_shard_sb, "can not find shard super blk for shard_id={} !!!", shard_id);

    const auto shard_sb = const_cast< HS_Shard* >(d_cast< const HS_Shard* >(raw_shard_sb))->sb_.get();

    auto blk_size = homestore::data_service().get_blk_size();
    auto shard_sb_size = sizeof(shard_info_superblk);
    auto total_size = sisl::round_up(shard_sb_size, blk_size);
    auto shard_sb_buf = iomanager.iobuf_alloc(blk_size, total_size);

    std::memcpy(shard_sb_buf, shard_sb, shard_sb_size);

    sisl::sg_list shard_sb_sgs;
    shard_sb_sgs.size = total_size;
    shard_sb_sgs.iovs.emplace_back(iovec{.iov_base = shard_sb_buf, .iov_len = total_size});
    return shard_sb_sgs;
}

} // namespace homeobject
