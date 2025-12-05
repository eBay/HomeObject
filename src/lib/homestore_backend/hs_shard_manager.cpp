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

pg_id_t HSHomeObject::get_pg_id_from_shard_id(uint64_t shard_id) {
    // get highest 16bit
    return shard_id >> shard_width;
}

std::string HSHomeObject::serialize_shard_info(const ShardInfo& info) {
    nlohmann::json j;
    j["shard_info"]["shard_id_t"] = info.id;
    j["shard_info"]["pg_id_t"] = info.placement_group;
    j["shard_info"]["state"] = info.state;
    j["shard_info"]["lsn"] = info.lsn;
    j["shard_info"]["created_time"] = info.created_time;
    j["shard_info"]["modified_time"] = info.last_modified_time;
    j["shard_info"]["total_capacity"] = info.total_capacity_bytes;
    j["shard_info"]["available_capacity"] = info.available_capacity_bytes;
    j["shard_info"]["meta"] = std::string(reinterpret_cast< const char* >(info.meta));
    return j.dump();
}

ShardInfo HSHomeObject::deserialize_shard_info(const char* json_str, size_t str_size) {
    ShardInfo shard_info;
    auto shard_json = nlohmann::json::parse(json_str, json_str + str_size);
    shard_info.id = shard_json["shard_info"]["shard_id_t"].get< shard_id_t >();
    shard_info.placement_group = shard_json["shard_info"]["pg_id_t"].get< pg_id_t >();
    shard_info.state = static_cast< ShardInfo::State >(shard_json["shard_info"]["state"].get< int >());
    shard_info.lsn = shard_json["shard_info"]["lsn"].get< uint64_t >();
    shard_info.created_time = shard_json["shard_info"]["created_time"].get< uint64_t >();
    shard_info.last_modified_time = shard_json["shard_info"]["modified_time"].get< uint64_t >();
    shard_info.available_capacity_bytes = shard_json["shard_info"]["available_capacity"].get< uint64_t >();
    shard_info.total_capacity_bytes = shard_json["shard_info"]["total_capacity"].get< uint64_t >();
    auto meta_str = shard_json["shard_info"]["meta"].get< std::string >();
    std::memcpy(shard_info.meta, meta_str.data(), meta_str.length());
    shard_info.meta[meta_str.length()] = '\0';
    return shard_info;
}

ShardManager::AsyncResult< ShardInfo > HSHomeObject::_create_shard(pg_id_t pg_owner, uint64_t size_bytes, std::string meta,
                                                                   trace_id_t tid) {

    if (is_shutting_down()) {
        LOGI("service is being shut down");
        return folly::makeUnexpected(ShardError(ShardErrorCode::SHUTTING_DOWN));
    }
    incr_pending_request_num();
    if (!meta.empty() && meta.length() > ShardInfo::meta_length - 1) {
        LOGW("meta length {} exceeds max meta length {}, trace_id={}", meta.length(), ShardInfo::meta_length - 1, tid);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::INVALID_ARG));
    }
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
    auto create_time = get_current_timestamp();

    // select chunk for shard.
    const auto v_chunkID = chunk_selector()->get_most_available_blk_chunk(new_shard_id, pg_owner);
    if (!v_chunkID.has_value()) {
        SLOGW(tid, new_shard_id, "no availble chunk left to create shard for pg={}", pg_owner);
        decr_pending_request_num();
        return folly::makeUnexpected(ShardError(ShardErrorCode::NO_SPACE_LEFT));
    }
    const auto v_chunk_id = v_chunkID.value();
    SLOGD(tid, new_shard_id, "vchunk_id={}", v_chunk_id);

    // Prepare the shard info block
    sisl::io_blob_safe sb_blob(sisl::round_up(sizeof(shard_info_superblk), repl_dev->get_blk_size()), io_align);
    shard_info_superblk* sb = new (sb_blob.bytes()) shard_info_superblk();
    sb->type = DataHeader::data_type_t::SHARD_INFO;
    sb->info = ShardInfo{.id = new_shard_id,
                         .placement_group = pg_owner,
                         .state = ShardInfo::State::OPEN,
                         .lsn = 0,
                         .created_time = create_time,
                         .last_modified_time = create_time,
                         .available_capacity_bytes = size_bytes,
                         .total_capacity_bytes = size_bytes};
    if (!meta.empty()) {
        std::memcpy(sb->info.meta, meta.data(), meta.length());
        sb->info.meta[meta.length()] = '\0';
    }
    sb->p_chunk_id = 0;
    sb->v_chunk_id = v_chunk_id;

    auto req = repl_result_ctx< ShardManager::Result< ShardInfo > >::make(
        sizeof(shard_info_superblk) /* header_extn_size */, 0u /* key_size */);

    // for create shard, we disable push_data, so that all the selecting chunk for creating shard will go through raft
    // log channel, and thus, the the selecting chunk of later creating shard will go after that of the former one.
    req->disable_push_data();

    // prepare msg header;
    req->header()->msg_type = ReplicationMessageType::CREATE_SHARD_MSG;
    req->header()->pg_id = pg_owner;
    req->header()->shard_id = new_shard_id;
    req->header()->payload_size = sizeof(shard_info_superblk);
    req->header()->payload_crc = crc32_ieee(init_crc32, sb_blob.cbytes(), sizeof(shard_info_superblk));
    req->header()->seal();

    // ShardInfo block is persisted on both on header and in data portion.
    // It is persisted in header portion, so that it is written in journal and hence replay of journal on most cases
    // doesn't need additional read from data blks.
    // We also persist in data blocks for following reasons:
    //   * To recover the shard information in case both journal and metablk are lost
    //   * For garbage collection, we directly read from the data chunk and get shard information.
    std::memcpy(req->header_extn(), sb_blob.cbytes(), sizeof(shard_info_superblk));
    req->add_data_sg(std::move(sb_blob));

    // replicate this create shard message to PG members;
    repl_dev->async_alloc_write(req->cheader_buf(), sisl::blob{}, req->data_sgs(), req, false /* part_of_batch */, tid);
    return req->result().deferValue([this, req, repl_dev, tid, pg_owner, new_shard_id,
                                     v_chunk_id](const auto& result) -> ShardManager::AsyncResult< ShardInfo > {
        if (result.hasError()) {
            auto err = result.error();
            if (err.getCode() == ShardErrorCode::NOT_LEADER) { err.current_leader = repl_dev->get_leader_id(); }

            bool res = chunk_selector()->release_chunk(pg_owner, v_chunk_id);
            RELEASE_ASSERT(res, "Failed to release v_chunk_id={}, pg={}", v_chunk_id, pg_owner);

            SLOGE(tid, new_shard_id, "got {} when creating shard at leader, failed to create shard {}!", err.getCode(),
                  new_shard_id);

            if (err.getCode() == ShardErrorCode::NO_SPACE_LEFT) {
                gc_manager()->submit_gc_task(task_priority::normal,
                                             chunk_selector()->get_pg_vchunk(pg_owner, v_chunk_id)->get_chunk_id());
                SLOGD(tid, new_shard_id, "got no space left error when creating shard {} at leader", new_shard_id);
            }

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

    auto pg_id = info.placement_group;
    auto shard_id = info.id;
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

    ShardInfo tmp_info = info;
    tmp_info.state = ShardInfo::State::SEALED;

    // Prepare the shard info block
    sisl::io_blob_safe sb_blob(sisl::round_up(sizeof(shard_info_superblk), repl_dev->get_blk_size()), io_align);
    shard_info_superblk* sb = new (sb_blob.bytes()) shard_info_superblk();
    sb->type = DataHeader::data_type_t::SHARD_INFO;
    sb->info = tmp_info;
    // p_chunk_id and v_chunk_id will never be used in seal shard workflow.
    sb->p_chunk_id = 0;
    sb->v_chunk_id = 0;

    auto req = repl_result_ctx< ShardManager::Result< ShardInfo > >::make(
        sizeof(shard_info_superblk) /* header_extn_size */, 0u /* key_size */);
    req->header()->msg_type = ReplicationMessageType::SEAL_SHARD_MSG;
    req->header()->pg_id = pg_id;
    req->header()->shard_id = shard_id;
    req->header()->payload_size = sizeof(shard_info_superblk);
    req->header()->payload_crc = crc32_ieee(init_crc32, sb_blob.cbytes(), sizeof(shard_info_superblk));
    req->header()->seal();

    // Similar to create shard - ShardInfo block is persisted on both on header and in data portion.
    std::memcpy(req->header_extn(), sb_blob.cbytes(), sizeof(shard_info_superblk));
    req->add_data_sg(std::move(sb_blob));

    // replicate this seal shard message to PG members;
    repl_dev->async_alloc_write(req->cheader_buf(), sisl::blob{}, req->data_sgs(), req, false /* part_of_batch */, tid);
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

// TODO: introduce shard sealed_lsn to solve the conflict between seal_shard and put_blob,
bool HSHomeObject::on_shard_message_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                               cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< ShardManager::Result< ShardInfo > >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer()) {
        ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(hs_ctx).get();
    }
    auto tid = hs_ctx ? hs_ctx->traceID() : 0;
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGW("replication message header is corrupted with crc error, lsn={}, traceID={}", lsn, tid);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError(ShardErrorCode::CRC_MISMATCH))); }
        // TODO::if fail to pre_commit, shuold we crash here?

        return false;
    }
    switch (msg_header->msg_type) {
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        auto sb = r_cast< shard_info_superblk const* >(header.cbytes() + sizeof(ReplicationMessageHeader));
        auto const shard_info = sb->info;

        {
            std::scoped_lock lock_guard(_shard_lock);
            auto iter = _shard_map.find(shard_info.id);
            RELEASE_ASSERT(iter != _shard_map.end(), "shardID=0x{:x}, pg={}, shard=0x{:x}, shard does not exist",
                           shard_info.id, (shard_info.id >> homeobject::shard_width),
                           (shard_info.id & homeobject::shard_mask));
            auto& state = (*iter->second)->info.state;
            // we just change the state to SEALED, so that it will fail the later coming put_blob on this shard and will
            // be easy for rollback.
            // the update of superblk will be done in on_shard_message_commit;
            if (state == ShardInfo::State::OPEN) {
                state = ShardInfo::State::SEALED;
            } else {
                SLOGW(tid, shard_info.id, "try to seal an unopened shard");
            }
        }
    }
    default: {
        break;
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
        LOGW("replication message header is corrupted with crc error, lsn={}, traceID={}", lsn, tid);
        RELEASE_ASSERT(false, "shardID=0x{:x}, pg={}, shard=0x{:x}, failed to rollback create_shard msg",
                       msg_header->shard_id, (msg_header->shard_id >> homeobject::shard_width),
                       (msg_header->shard_id & homeobject::shard_mask));
        return;
    }

    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        if (ctx) {
            ctx->promise_.setValue(folly::makeUnexpected(ShardError(ShardErrorCode::RETRY_REQUEST)));
        } else {
            // we have already added release_chunk logic to thenValue of hoemobject#create_shard in originator, so here
            // we just need to release_chunk for non-originater case since it will bring a bug if a chunk is released
            // for two times. for exampele, as a originator:

            // t1 : chunk1 is released in the rollback of create_shard, the chunk state is marked as available
            // t2 : chunk1 is select by a new create shard (shard1), the chunk state is marked as inuse
            // t3 : chunk1 is released in thenValue of create_shard, the chunk state is marked as available
            // t4 : chunk1 is select by a new create shard (shard2), the chunk state is marked as inuse
            // now, shard1 and shard2 hold the same chunk.
            bool res = release_chunk_based_on_create_shard_message(header);
            if (!res) {
                RELEASE_ASSERT(false,
                               "shardID=0x{:x}, pg={}, shard=0x{:x}, failed to release chunk based on create shard msg",
                               msg_header->shard_id, (msg_header->shard_id >> homeobject::shard_width),
                               (msg_header->shard_id & homeobject::shard_mask));
            }
        }
        break;
    }
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        auto sb = r_cast< shard_info_superblk const* >(header.cbytes() + sizeof(ReplicationMessageHeader));
        auto const shard_info = sb->info;
        {
            std::scoped_lock lock_guard(_shard_lock);
            auto iter = _shard_map.find(shard_info.id);
            RELEASE_ASSERT(iter != _shard_map.end(), "shardID=0x{:x}, pg={}, shard=0x{:x}, shard does not exist",
                           shard_info.id, (shard_info.id >> homeobject::shard_width),
                           (shard_info.id & homeobject::shard_mask));
            auto& state = (*iter->second)->info.state;
            // we just change the state to SEALED, since it will be easy for rollback
            // the update of superblk will be done in on_shard_message_commit;
            if (state == ShardInfo::State::SEALED) {
                state = ShardInfo::State::OPEN;
            } else {
                SLOGW(tid, shard_info.id, "try to rollback seal_shard message , but the shard state is not sealed");
            }
        }
        // TODO:set a proper error code
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError(ShardErrorCode::RETRY_REQUEST))); }

        break;
    }
    default: {
        break;
    }
    }
}

void HSHomeObject::local_create_shard(ShardInfo shard_info, homestore::chunk_num_t v_chunk_id,
                                      homestore::chunk_num_t p_chunk_id, homestore::blk_count_t blk_count,
                                      trace_id_t tid) {
    bool shard_exist = false;
    {
        scoped_lock lock_guard(_shard_lock);
        shard_exist = (_shard_map.find(shard_info.id) != _shard_map.end());
    }

    if (!shard_exist) {
        add_new_shard_to_map(std::make_unique< HS_Shard >(shard_info, p_chunk_id, v_chunk_id));
        // select_specific_chunk() will do something only when we are relaying journal after restart, during the
        // runtime flow chunk is already been be mark busy when we write the shard info to the repldev.
        auto pg_id = shard_info.placement_group;
        auto chunk = chunk_selector_->select_specific_chunk(pg_id, v_chunk_id);
        RELEASE_ASSERT(chunk != nullptr, "chunk selection failed with v_chunk_id={} in pg={}", v_chunk_id, pg_id);
    } else {
        SLOGD(tid, shard_info.id, "shard already exist, skip creating shard");
    }

    // update pg's total_occupied_blk_count
    auto hs_pg = get_hs_pg(shard_info.placement_group);
    RELEASE_ASSERT(hs_pg != nullptr, "shardID=0x{:x}, pg={}, shard=0x{:x}, PG not found", shard_info.id,
                   (shard_info.id >> homeobject::shard_width), (shard_info.id & homeobject::shard_mask));

    SLOGD(tid, shard_info.id, "local_create_shard {}, vchunk_id={}, p_chunk_id={}, pg_id={}", shard_info.id, v_chunk_id,
          p_chunk_id, shard_info.placement_group);

    const_cast< HS_PG* >(hs_pg)->durable_entities_update(
        [blk_count](auto& de) { de.total_occupied_blk_count.fetch_add(blk_count, std::memory_order_relaxed); });
}

void HSHomeObject::on_shard_message_commit(int64_t lsn, sisl::blob const& h, homestore::MultiBlkId const& blkids,
                                           shared< homestore::ReplDev > repl_dev,
                                           cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< ShardManager::Result< ShardInfo > >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer()) {
        ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(hs_ctx).get();
    }
    auto tid = hs_ctx ? hs_ctx->traceID() : 0;
    auto header = r_cast< const ReplicationMessageHeader* >(h.cbytes());
    if (header->corrupted()) {
        LOGW("replication message header is corrupted with crc error, lsn={}, traceID={}", lsn, tid);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError(ShardErrorCode::CRC_MISMATCH))); }
        // TODO::if fail to commit, shuold we crash here?
        return;
    }

#ifdef VADLIDATE_ON_REPLAY
    sisl::io_blob_safe value_blob(blkids.blk_count() * repl_dev->get_blk_size(), io_align);
    sisl::sg_list value_sgs;
    value_sgs.iovs.emplace_back(iovec{.iov_base = value_blob.bytes(), .iov_len = value_blob.size()});
    value_sgs.size += value_blob.size();

    // Do a read sync read and validate the crc
    std::error_code err = repl_dev->async_read(blkids, value_sgs, value_blob.size()).get();
    if (err) {
        LOGW("failed to read data from homestore blks, lsn={}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError(ShardErrorCode::UNKNOWN))); }
        return;
    }

    if (crc32_ieee(init_crc32, value.cbytes(), value.size()) != header->payload_crc) {
        // header & value is inconsistent;
        LOGW("replication message header is inconsistent with value, lsn={}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError(ShardErrorCode::CRC_MISMATCH))); }
        return;
    }
#endif

    switch (header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        auto sb = r_cast< shard_info_superblk const* >(h.cbytes() + sizeof(ReplicationMessageHeader));
        auto shard_info = sb->info;
        auto v_chunk_id = sb->v_chunk_id;
        shard_info.lsn = lsn;

        local_create_shard(shard_info, v_chunk_id, blkids.chunk_num(), blkids.blk_count(), tid);
        if (ctx) { ctx->promise_.setValue(ShardManager::Result< ShardInfo >(shard_info)); }

        SLOGD(tid, shard_info.id, "Commit done for creating shard");

        break;
    }

    case ReplicationMessageType::SEAL_SHARD_MSG: {
        auto sb = r_cast< shard_info_superblk const* >(h.cbytes() + sizeof(ReplicationMessageHeader));
        auto const shard_info = sb->info;

        ShardInfo::State state;
        {
            std::scoped_lock lock_guard(_shard_lock);
            auto iter = _shard_map.find(shard_info.id);
            RELEASE_ASSERT(iter != _shard_map.end(), "shardID=0x{:x}, pg={}, shard=0x{:x}, shard does not exist",
                           shard_info.id, (shard_info.id >> homeobject::shard_width),
                           (shard_info.id & homeobject::shard_mask));
            state = (*iter->second)->info.state;
        }

        if (state == ShardInfo::State::SEALED) {
            auto pg_id = shard_info.placement_group;
            auto v_chunkID = get_shard_v_chunk_id(shard_info.id);
            RELEASE_ASSERT(v_chunkID.has_value(), "v_chunk id not found");
            bool res = chunk_selector()->release_chunk(pg_id, v_chunkID.value());
            RELEASE_ASSERT(res, "Failed to release v_chunk_id={}, pg={}", v_chunkID.value(), pg_id);
            update_shard_in_map(shard_info);
        } else
            SLOGW(tid, shard_info.id, "try to commit SEAL_SHARD_MSG but shard state is not sealed.");
        if (ctx) { ctx->promise_.setValue(ShardManager::Result< ShardInfo >(shard_info)); }
        SLOGD(tid, shard_info.id, "Commit done for sealing shard");

        auto hs_pg = get_hs_pg(shard_info.placement_group);
        RELEASE_ASSERT(hs_pg != nullptr, "shardID=0x{:x}, pg={}, shard=0x{:x}, PG not found", shard_info.id,
                       (shard_info.id >> homeobject::shard_width), (shard_info.id & homeobject::shard_mask));
        const_cast< HS_PG* >(hs_pg)->durable_entities_update(
            // shard_footer will also occupy one blk.
            [](auto& de) { de.total_occupied_blk_count.fetch_add(1, std::memory_order_relaxed); });

        break;
    }
    default:
        break;
    }
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

std::optional< homestore::chunk_num_t > HSHomeObject::resolve_v_chunk_id_from_msg(sisl::blob const& header) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGW("replication message header is corrupted with crc error");
        return std::nullopt;
    }

    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        const pg_id_t pg_id = msg_header->pg_id;
        if (!pg_exists(pg_id)) {
            LOGW("shardID=0x{:x}, pg={}, shard=0x{:x}, Requesting a chunk for an unknown pg={}", msg_header->shard_id,
                 (msg_header->shard_id >> homeobject::shard_width), (msg_header->shard_id & homeobject::shard_mask),
                 pg_id);
            return std::nullopt;
        }
        auto sb = r_cast< shard_info_superblk const* >(header.cbytes() + sizeof(ReplicationMessageHeader));
        return sb->v_chunk_id;
    }
    default: {
        LOGW("Unexpected message type encountered={}. This function should only be called with 'CREATE_SHARD_MSG'.",
             msg_header->msg_type);
        return std::nullopt;
    }
    }
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
        if (!pg_exists(pg_id)) {
            LOGW("shardID=0x{:x}, pg={}, shard=0x{:x}, Requesting a chunk for an unknown pg={}", msg_header->shard_id,
                 (msg_header->shard_id >> homeobject::shard_width), (msg_header->shard_id & homeobject::shard_mask),
                 pg_id);
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

} // namespace homeobject
