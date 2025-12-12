#include "hs_backend_config.hpp"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <homestore/replication_service.hpp>
#include <utility>
#include "hs_homeobject.hpp"
#include "replication_state_machine.hpp"

using namespace homestore;
namespace homeobject {
PGError toPgError(ReplServiceError const& e) {
    switch (e) {
    case ReplServiceError::BAD_REQUEST:
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
    case ReplServiceError::NOT_IMPLEMENTED:
        return PGError::INVALID_ARG;
    case ReplServiceError::CANCELLED:
        return PGError::CANCELLED;
    case ReplServiceError::NOT_LEADER:
        return PGError::NOT_LEADER;
    case ReplServiceError::CANNOT_REMOVE_LEADER:
    case ReplServiceError::SERVER_NOT_FOUND:
        return PGError::UNKNOWN_PEER;
    case ReplServiceError::TIMEOUT:
        return PGError::TIMEOUT;
    case ReplServiceError::NO_SPACE_LEFT:
        return PGError::NO_SPACE_LEFT;
    case ReplServiceError::DRIVE_WRITE_ERROR:
        return PGError::DRIVE_WRITE_ERROR;
    case ReplServiceError::QUORUM_NOT_MET:
        return PGError::QUORUM_NOT_MET;
    case ReplServiceError::RETRY_REQUEST:
        return PGError::RETRY_REQUEST;
    /* TODO: enable this after add error type to homestore
            case ReplServiceError::CRC_MISMATCH:
                return PGError::CRC_MISMATCH;
             */
    case ReplServiceError::OK:
        DEBUG_ASSERT(false, "Should not process OK!");
        [[fallthrough]];
    case ReplServiceError::DATA_DUPLICATED:
        [[fallthrough]];
    case ReplServiceError::FAILED:
        return PGError::UNKNOWN;
    default:
        return PGError::UNKNOWN;
    }
}

PGReplaceMemberTaskStatus toPGReplaceMemberTaskStatus(ReplaceMemberStatus const& status) {
    switch (status) {
    case ReplaceMemberStatus::COMPLETED:
        return PGReplaceMemberTaskStatus::COMPLETED;
    case ReplaceMemberStatus::IN_PROGRESS:
        return PGReplaceMemberTaskStatus::IN_PROGRESS;
    case ReplaceMemberStatus::NOT_LEADER:
        return PGReplaceMemberTaskStatus::NOT_LEADER;
    case ReplaceMemberStatus::TASK_ID_MISMATCH:
        return PGReplaceMemberTaskStatus::TASK_ID_MISMATCH;
    case ReplaceMemberStatus::TASK_NOT_FOUND:
        return PGReplaceMemberTaskStatus::TASK_NOT_FOUND;
    default:
        return PGReplaceMemberTaskStatus::UNKNOWN;
    }
}

[[maybe_unused]] static homestore::ReplDev& pg_repl_dev(PG const& pg) {
    return *(static_cast< HSHomeObject::HS_PG const& >(pg).repl_dev_);
}

PGManager::NullAsyncResult HSHomeObject::_create_pg(PGInfo&& pg_info, std::set< peer_id_t > const& peers,
                                                    trace_id_t tid) {
    if (is_shutting_down()) {
        LOGI("service is being shut down");
        return folly::makeUnexpected(PGError::SHUTTING_DOWN);
    }
    incr_pending_request_num();

    auto pg_id = pg_info.id;
    auto hs_pg = get_hs_pg(pg_id);
    if (hs_pg) {
        if (!pg_info.is_equivalent_to(hs_pg->pg_info_)) {
            LOGW("PG already exists with different info! pg={}, pg_info={}, hs_pg_info={}", pg_id, pg_info.to_string(),
                 hs_pg->pg_info_.to_string());
            decr_pending_request_num();
            return folly::makeUnexpected(PGError::INVALID_ARG);
        }
        LOGW("PG already exists! pg={}", pg_id);
        decr_pending_request_num();
        return folly::Unit();
    }

    const auto chunk_size = chunk_selector()->get_chunk_size();
    if (pg_info.size < chunk_size) {
        LOGW("Not support to create PG which pg_size={} < chunk_size={}", pg_info.size, chunk_size);
        decr_pending_request_num();
        return folly::makeUnexpected(PGError::INVALID_ARG);
    }

    auto const num_chunk = chunk_selector()->select_chunks_for_pg(pg_id, pg_info.size);
    if (!num_chunk.has_value()) {
        LOGW("Failed to select chunks for pg={}", pg_id);
        decr_pending_request_num();
        return folly::makeUnexpected(PGError::NO_SPACE_LEFT);
    }
    if (pg_info.expected_member_num == 0) { pg_info.expected_member_num = pg_info.members.size(); }
    pg_info.chunk_size = chunk_size;
    pg_info.replica_set_uuid = boost::uuids::random_generator()();
    const auto repl_dev_group_id = pg_info.replica_set_uuid;
    return hs_repl_service()
        .create_repl_dev(pg_info.replica_set_uuid, peers)
        .via(executor_)
        .thenValue([this, pg_info = std::move(pg_info), tid](auto&& v) mutable -> PGManager::NullAsyncResult {
#ifdef _PRERELEASE
            if (iomgr_flip::instance()->test_flip("create_pg_create_repl_dev_error")) {
                LOGW("Simulating create repl dev error in creating pg");
                v = folly::makeUnexpected(ReplServiceError::FAILED);
            }
#endif

            if (v.hasError()) { return folly::makeUnexpected(toPgError(v.error())); }
            // we will write a PGHeader across the raft group and when it is committed
            // all raft members will create PGinfo and index table for this PG.

            // FIXME:https://github.com/eBay/HomeObject/pull/136#discussion_r1470504271
            return do_create_pg(v.value(), std::move(pg_info), tid);
        })
        .thenValue([this, pg_id, repl_dev_group_id, tid](auto&& r) -> PGManager::NullAsyncResult {
            // reclaim resources if failed to create pg
            if (r.hasError()) {
                bool res = chunk_selector_->return_pg_chunks_to_dev_heap(pg_id);
                RELEASE_ASSERT(res, "Failed to return pg={} chunks to dev_heap", pg_id);
                // no matter if create repl dev successfully, remove it.
                // if don't have repl dev, it will return ReplServiceError::SERVER_NOT_FOUND
                return hs_repl_service()
                    .remove_repl_dev(repl_dev_group_id)
                    .deferValue([r, repl_dev_group_id, this](auto&& e) -> PGManager::NullAsyncResult {
                        if (e != ReplServiceError::OK) {
                            LOGW("Failed to remove repl device which group_id={}, error={}", repl_dev_group_id, e);
                        }
                        decr_pending_request_num();
                        // still return the original error
                        return folly::makeUnexpected(r.error());
                    });
            }
            decr_pending_request_num();
            return folly::Unit();
        });
}

PGManager::NullAsyncResult HSHomeObject::do_create_pg(cshared< homestore::ReplDev > repl_dev, PGInfo&& pg_info,
                                                      trace_id_t tid) {
    auto serailized_pg_info = serialize_pg_info(pg_info);
    auto info_size = serailized_pg_info.size();

    auto req = repl_result_ctx< PGManager::NullResult >::make(info_size, 0);
    req->header()->msg_type = ReplicationMessageType::CREATE_PG_MSG;
    req->header()->payload_size = info_size;
    req->header()->payload_crc = crc32_ieee(init_crc32, r_cast< const uint8_t* >(serailized_pg_info.data()), info_size);
    req->header()->seal();
    std::memcpy(req->header_extn(), serailized_pg_info.data(), info_size);

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("create_pg_raft_message_error")) {
        LOGW("Simulating raft message error in creating pg");
        return folly::makeUnexpected(PGError::UNKNOWN);
    }
#endif

    // replicate this create pg message to all raft members of this group
    repl_dev->async_alloc_write(req->header_buf(), sisl::blob{}, sisl::sg_list{}, req, false /* part_of_batch */, tid);
    return req->result().deferValue([req](auto const& e) -> PGManager::NullAsyncResult {
        if (!e) { return folly::makeUnexpected(e.error()); }
        return folly::Unit();
    });
}

folly::Expected< HSHomeObject::HS_PG*, PGError > HSHomeObject::local_create_pg(shared< ReplDev > repl_dev,
                                                                               PGInfo pg_info, trace_id_t tid) {
    auto pg_id = pg_info.id;
    if (auto hs_pg = get_hs_pg(pg_id); hs_pg) {
        // pg info may have changed due to replace_member, so we just log pg info here
        LOGW("PG already exists, pg={}, trace_id={}, pg_info={}, hs_pg_info={}", pg_id, tid, pg_info.to_string(),
             hs_pg->pg_info_.to_string());
        return const_cast< HS_PG* >(hs_pg);
    }

    auto local_chunk_size = chunk_selector()->get_chunk_size();
    if (pg_info.chunk_size != local_chunk_size) {
        LOGE("Chunk sizes are inconsistent, leader_chunk_size={}, local_chunk_size={}, trace_id={}", pg_info.chunk_size,
             local_chunk_size, tid);
        return folly::makeUnexpected< PGError >(PGError::UNKNOWN);
    }

    // select chunks for pg
    auto const num_chunk = chunk_selector()->select_chunks_for_pg(pg_id, pg_info.size);
    if (!num_chunk.has_value()) {
        LOGW("Failed to select chunks for pg={}, trace_id={}", pg_id, tid);
        return folly::makeUnexpected(PGError::NO_SPACE_LEFT);
    }
    auto chunk_ids = chunk_selector()->get_pg_chunks(pg_id);
    if (chunk_ids == nullptr) {
        LOGW("Failed to get pg chunks, pg={}, trace_id={}", pg_id, tid);
        return folly::makeUnexpected(PGError::NO_SPACE_LEFT);
    }

    // create index table and pg
    auto index_table = create_pg_index_table();
    auto uuid_str = boost::uuids::to_string(index_table->uuid());

    repl_dev->set_custom_rdev_name(fmt::format("rdev{}", pg_info.id));
    auto hs_pg = std::make_unique< HS_PG >(std::move(pg_info), std::move(repl_dev), index_table, chunk_ids);
    auto ret = hs_pg.get();
    {
        scoped_lock lck(index_lock_);
        RELEASE_ASSERT(index_table_pg_map_.count(uuid_str) == 0, "duplicate index table found");
        index_table_pg_map_[uuid_str] = PgIndexTable{pg_id, index_table};

        LOGI("create pg={} successfully, index table uuid={} pg_size={} num_chunk={}, trace_id={}", pg_id, uuid_str,
             pg_info.size, num_chunk.value(), tid);
        hs_pg->index_table_ = index_table;
        // Add to index service, so that it gets cleaned up when index service is shutdown.
        hs()->index_service().add_index_table(index_table);
        add_pg_to_map(std::move(hs_pg));
    }
    return ret;
}

void HSHomeObject::on_create_pg_message_commit(int64_t lsn, sisl::blob const& header,
                                               shared< homestore::ReplDev > repl_dev,
                                               cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< PGManager::NullResult >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer()) {
        ctx = boost::static_pointer_cast< repl_result_ctx< PGManager::NullResult > >(hs_ctx).get();
    }
    auto tid = hs_ctx ? hs_ctx->traceID() : 0;

    auto const* msg_header = r_cast< ReplicationMessageHeader const* >(header.cbytes());

    if (msg_header->corrupted()) {
        LOGE("create PG message header is corrupted , lsn={}; header={}, trace_id={}", lsn, msg_header->to_string(),
             tid);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(PGError::CRC_MISMATCH)); }
        return;
    }

    auto serailized_pg_info_buf = header.cbytes() + sizeof(ReplicationMessageHeader);
    const auto serailized_pg_info_size = header.size() - sizeof(ReplicationMessageHeader);

    if (crc32_ieee(init_crc32, serailized_pg_info_buf, serailized_pg_info_size) != msg_header->payload_crc) {
        // header & value is inconsistent;
        LOGE("create PG message header is inconsistent with value, lsn={}, trace_id={}", lsn, tid);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(PGError::CRC_MISMATCH)); }
        return;
    }

    auto pg_info = deserialize_pg_info(serailized_pg_info_buf, serailized_pg_info_size);
    auto ret = local_create_pg(std::move(repl_dev), pg_info);
    if (ctx) {
        if (ret.hasError()) {
            ctx->promise_.setValue(folly::makeUnexpected(ret.error()));
        } else {
            ctx->promise_.setValue(folly::Unit());
        }
    }
}

// HS's replace_member will have two phases:
// 1. Set the old member to learner and add the new member. This step will call `on_pg_start_replace_member`.
// 2. HS takes the responsiblity to track the replication progress, and complete the replace member(remove the old
// member) when the new member is fully synced.  This step will call `on_pg_complete_replace_member`.
PGManager::NullAsyncResult HSHomeObject::_replace_member(pg_id_t pg_id, std::string& task_id,
                                                         peer_id_t const& old_member_id, PGMember const& new_member,
                                                         uint32_t commit_quorum, trace_id_t tid) {
    if (is_shutting_down()) {
        LOGI("service is being shut down, trace_id={}", tid);
        return folly::makeUnexpected(PGError::SHUTTING_DOWN);
    }
    incr_pending_request_num();

    auto hs_pg = get_hs_pg(pg_id);
    if (hs_pg == nullptr) {
        decr_pending_request_num();
        return folly::makeUnexpected(PGError::UNKNOWN_PG);
    }

    auto& repl_dev = pg_repl_dev(*hs_pg);
    if (!repl_dev.is_leader() && commit_quorum == 0) {
        // Only leader can replace a member
        decr_pending_request_num();
        return folly::makeUnexpected(PGError::NOT_LEADER);
    }
    auto group_id = repl_dev.group_id();

    LOGI("PG replace member initated member_out={} member_in={} trace_id={}", boost::uuids::to_string(old_member_id),
         boost::uuids::to_string(new_member.id), tid);

    replica_member_info out_replica;
    out_replica.id = old_member_id;
    replica_member_info in_replica = to_replica_member_info(new_member);

    return hs_repl_service()
        .replace_member(group_id, task_id, out_replica, in_replica, commit_quorum, tid)
        .via(executor_)
        .thenValue([this](auto&& v) mutable -> PGManager::NullAsyncResult {
            decr_pending_request_num();
            if (v.hasError()) { return folly::makeUnexpected(toPgError(v.error())); }
            return folly::Unit();
        });
}

PGMember HSHomeObject::to_pg_member(const replica_member_info& replica_info) const {
    PGMember pg_member(replica_info.id);
    pg_member.name = std::string(replica_info.name);
    pg_member.priority = replica_info.priority;
    return pg_member;
}

replica_member_info HSHomeObject::to_replica_member_info(const PGMember& pg_member) const {
    replica_member_info replica_info;
    replica_info.id = pg_member.id;
    std::strncpy(replica_info.name, pg_member.name.data(), pg_member.name.size());
    replica_info.name[pg_member.name.size()] = '\0';
    replica_info.priority = pg_member.priority;
    return replica_info;
}

void HSHomeObject::on_pg_start_replace_member(group_id_t group_id, const std::string& task_id,
                                              const replica_member_info& member_out,
                                              const replica_member_info& member_in, trace_id_t tid) {
    auto lg = std::shared_lock(_pg_lock);
    for (const auto& iter : _pg_map) {
        auto& pg = iter.second;
        if (pg_repl_dev(*pg).group_id() == group_id) {
            // Remove the old member and add the new member
            auto hs_pg = static_cast< HSHomeObject::HS_PG* >(pg.get());
            pg->pg_info_.members.emplace(std::move(to_pg_member(member_in)));
            pg->pg_info_.members.emplace(std::move(to_pg_member(member_out)));

            uint32_t i{0};
            pg_members* sb_members = hs_pg->pg_sb_->get_pg_members_mutable();
            for (auto const& m : pg->pg_info_.members) {
                sb_members[i].id = m.id;
                DEBUG_ASSERT(m.name.size() <= PGMember::max_name_len, "member name exceeds max len, name={}", m.name);
                auto name_len = std::min(m.name.size(), PGMember::max_name_len);
                std::strncpy(sb_members[i].name, m.name.c_str(), name_len);
                sb_members[i].name[name_len] = '\0';
                sb_members[i].priority = m.priority;
                ++i;
            }
            hs_pg->pg_sb_->num_dynamic_members = pg->pg_info_.members.size();
            // Update the latest membership info to pg superblk.
            hs_pg->pg_sb_.write();
            LOGI("PG start replace member done, task_id={} member_out={} member_in={}, member_nums={}, trace_id={}",
                 task_id, boost::uuids::to_string(member_out.id), boost::uuids::to_string(member_in.id),
                 pg->pg_info_.members.size(), tid);
            return;
        }
    }

    LOGE("PG replace member failed task_id={}, member_out={} member_in={}, trace_id={}", task_id,
         boost::uuids::to_string(member_out.id), boost::uuids::to_string(member_in.id), tid);
}

void HSHomeObject::on_pg_complete_replace_member(group_id_t group_id, const std::string& task_id,
                                                 const replica_member_info& member_out,
                                                 const replica_member_info& member_in, trace_id_t tid) {
    auto lg = std::shared_lock(_pg_lock);
    for (const auto& iter : _pg_map) {
        auto& pg = iter.second;
        if (pg_repl_dev(*pg).group_id() == group_id) {
            // Remove the old member and add the new member
            auto hs_pg = static_cast< HSHomeObject::HS_PG* >(pg.get());
            pg->pg_info_.members.erase(PGMember(member_out.id));
            pg->pg_info_.members.emplace(std::move(to_pg_member(member_in)));

            uint32_t i{0};
            pg_members* sb_members = hs_pg->pg_sb_->get_pg_members_mutable();
            for (auto const& m : pg->pg_info_.members) {
                sb_members[i].id = m.id;
                DEBUG_ASSERT(m.name.size() <= PGMember::max_name_len, "member name exceeds max len, name={}", m.name);
                auto name_len = std::min(m.name.size(), PGMember::max_name_len);
                std::strncpy(sb_members[i].name, m.name.c_str(), name_len);
                sb_members[i].name[name_len] = '\0';
                sb_members[i].priority = m.priority;
                ++i;
            }
            hs_pg->pg_sb_->num_dynamic_members = pg->pg_info_.members.size();
            // Update the latest membership info to pg superblk.
            hs_pg->pg_sb_.write();
            LOGI("PG complete replace member done member_out={} member_in={}, member_nums={}, trace_id={}",
                 boost::uuids::to_string(member_out.id), boost::uuids::to_string(member_in.id),
                 pg->pg_info_.members.size(), tid);
            return;
        }
    }
    LOGE("PG complete replace member failed member_out={} member_in={}, trace_id={}",
         boost::uuids::to_string(member_out.id), boost::uuids::to_string(member_in.id), tid);
}

PGReplaceMemberStatus HSHomeObject::_get_replace_member_status(pg_id_t id, std::string& task_id,
                                                               const PGMember& old_member, const PGMember& new_member,
                                                               const std::vector< PGMember >& others,
                                                               uint64_t trace_id) const {
    PGReplaceMemberStatus ret;
    ret.task_id = task_id;
    auto hs_pg = get_hs_pg(id);
    if (hs_pg == nullptr) {
        ret.status = PGReplaceMemberTaskStatus::UNKNOWN;
        return ret;
    }
    std::vector< replica_member_info > other_replicas;
    for (const auto& member : others) {
        other_replicas.push_back(to_replica_member_info(member));
    }
    ret.status = toPGReplaceMemberTaskStatus(hs_repl_service().get_replace_member_status(
        hs_pg->repl_dev_->group_id(), task_id, to_replica_member_info(old_member), to_replica_member_info(new_member),
        other_replicas, trace_id));
    hs_pg->get_peer_info(ret.members);
    return ret;
}

std::optional< pg_id_t > HSHomeObject::get_pg_id_with_group_id(group_id_t group_id) const {
    auto lg = std::shared_lock(_pg_lock);
    auto iter = std::find_if(_pg_map.begin(), _pg_map.end(), [group_id](const auto& entry) {
        return pg_repl_dev(*entry.second).group_id() == group_id;
    });
    if (iter != _pg_map.end()) {
        return iter->first;
    } else {
        return std::nullopt;
    }
}

void HSHomeObject::_destroy_pg(pg_id_t pg_id) { pg_destroy(pg_id); }

bool HSHomeObject::pg_destroy(pg_id_t pg_id, bool need_to_pause_pg_state_machine) {
    if (need_to_pause_pg_state_machine && !pause_pg_state_machine(pg_id)) {
        LOGI("Failed to pause pg state machine, pg_id={}", pg_id);
        return false;
    }
    LOGI("Destroying pg={}", pg_id);
    mark_pg_destroyed(pg_id);

    // we have the assumption that after pg is marked as destroyed, it will not be marked as alive again.
    // TODO:: if this assumption is broken, we need to handle it.
    gc_mgr_->drain_pg_pending_gc_task(pg_id);

    destroy_shards(pg_id);
    destroy_hs_resources(pg_id);
    destroy_pg_index_table(pg_id);
    destroy_pg_superblk(pg_id);

    // return pg chunks to dev heap
    // which must be done after destroying pg super blk to avoid multiple pg use same chunks
    bool res = chunk_selector_->return_pg_chunks_to_dev_heap(pg_id);
    RELEASE_ASSERT(res, "Failed to return pg={} chunks to dev_heap", pg_id);

    LOGI("pg={} is destroyed", pg_id);
    return true;
}

bool HSHomeObject::pause_pg_state_machine(pg_id_t pg_id) {
    LOGI("Pause pg state machine, pg={}", pg_id);
    auto hs_pg = const_cast< HS_PG* >(_get_hs_pg_unlocked(pg_id));
    auto repl_dev = hs_pg ? hs_pg->repl_dev_ : nullptr;
    auto timeout = HS_BACKEND_DYNAMIC_CONFIG(state_machine_pause_timeout_ms);
    auto retry = HS_BACKEND_DYNAMIC_CONFIG(state_machine_pause_retry_count);
    for (auto i = 0; i < static_cast< int >(retry); i++) {
        hs_pg->repl_dev_->pause_state_machine(timeout /* ms */);
        if (repl_dev->is_state_machine_paused()) {
            LOGI("pg={} state machine is paused", pg_id);
            return true;
        }
    }
    LOGE("Failed to pause pg={} state machine after {} ms", pg_id, timeout * retry);
    return false;
}

bool HSHomeObject::resume_pg_state_machine(pg_id_t pg_id) {
    LOGI("Resuming pg state machine, pg={}", pg_id);
    auto hs_pg = const_cast< HS_PG* >(_get_hs_pg_unlocked(pg_id));
    auto repl_dev = hs_pg ? hs_pg->repl_dev_ : nullptr;
    while (repl_dev->is_state_machine_paused()) {
        hs_pg->repl_dev_->resume_state_machine();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    LOGE("Resumed pg state machine, pg=", pg_id);
    return true;
}

void HSHomeObject::mark_pg_destroyed(pg_id_t pg_id) {
    auto lg = std::scoped_lock(_pg_lock);
    auto hs_pg = const_cast< HS_PG* >(_get_hs_pg_unlocked(pg_id));
    if (hs_pg == nullptr) {
        LOGW("mark pg destroyed with unknown pg={}", pg_id);
        return;
    }
    hs_pg->pg_sb_->state = PGState::DESTROYED;
    hs_pg->pg_sb_.write();
    LOGD("pg={} is marked as destroyed", pg_id);
}

bool HSHomeObject::can_chunks_in_pg_be_gc(pg_id_t pg_id) const {
    auto lg = std::scoped_lock(_pg_lock);
    auto hs_pg = const_cast< HS_PG* >(_get_hs_pg_unlocked(pg_id));
    if (hs_pg == nullptr) {
        LOGW("unknown pg={}", pg_id);
        return false;
    }

    return hs_pg->pg_sb_->state == PGState::ALIVE;
}

void HSHomeObject::destroy_hs_resources(pg_id_t pg_id) { chunk_selector_->reset_pg_chunks(pg_id); }

void HSHomeObject::destroy_pg_index_table(pg_id_t pg_id) {
    std::shared_ptr< BlobIndexTable > index_table;

    {
        // index_table->destroy() will trigger a cp_flush, which will call homeobject#cp_flush and try to acquire
        // `_pg_lock`, so we need to release the lock here to avoid a dead lock
        auto lg = std::scoped_lock(_pg_lock);
        auto hs_pg = _get_hs_pg_unlocked(pg_id);
        if (hs_pg == nullptr) {
            LOGW("destroy pg index table with unknown pg={}", pg_id);
            return;
        }
        index_table = hs_pg->index_table_;
    }

    if (nullptr != index_table) {
        auto uuid_str = boost::uuids::to_string(index_table->uuid());
        index_table_pg_map_.erase(uuid_str);
        hs()->index_service().remove_index_table(index_table);
        index_table->destroy();
        LOGD("pg={} index table is destroyed", pg_id);
    } else {
        LOGD("pg={} index table is not found, skip destroy", pg_id);
    }
}

void HSHomeObject::destroy_pg_superblk(pg_id_t pg_id) {
    // pay attention: cp_flush will try get '_pg_lock' to flush all pg ops.
    // before destroy pg superblk, we must ensure all ops on this pg are persisted
    // set force=true to ensure cp flush is triggered
    auto fut = homestore::hs()->cp_mgr().trigger_cp_flush(true /* force */);
    auto on_complete = [&](auto success) {
        RELEASE_ASSERT(success, "Failed to trigger CP flush");
        LOGI("CP Flush triggered by pg_destroy completed, pg={}", pg_id);
    };
    on_complete(std::move(fut).get());

    {
        auto lg = std::scoped_lock(_pg_lock);
        auto hs_pg = const_cast< HS_PG* >(_get_hs_pg_unlocked(pg_id));
        if (hs_pg == nullptr) {
            LOGW("destroy pg superblk with unknown pg={}", pg_id);
            return;
        }

        hs_pg->pg_sb_.destroy();
        destroy_snapshot_sb(hs_pg->repl_dev_->group_id());
        hs_pg->snp_rcvr_info_sb_.destroy();
        hs_pg->snp_rcvr_shard_list_sb_.destroy();

        // erase pg in pg map
        auto iter = _pg_map.find(pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "Failed to find pg in pg map");
        _pg_map.erase(iter);
    }
}

void HSHomeObject::add_pg_to_map(unique< HS_PG > hs_pg) {
    RELEASE_ASSERT(hs_pg->pg_info_.replica_set_uuid == hs_pg->repl_dev_->group_id(),
                   "PGInfo replica set uuid mismatch with ReplDev instance for {}",
                   boost::uuids::to_string(hs_pg->pg_info_.replica_set_uuid));
    auto lg = std::scoped_lock(_pg_lock);
    auto id = hs_pg->pg_info_.id;
    auto [it1, _] = _pg_map.try_emplace(id, std::move(hs_pg));
    RELEASE_ASSERT(_pg_map.end() != it1, "Unknown map insert error!");
}

std::string HSHomeObject::serialize_pg_info(const PGInfo& pginfo) {
    nlohmann::json j;
    j["pg_info"]["pg_id_t"] = pginfo.id;
    j["pg_info"]["pg_size"] = pginfo.size;
    j["pg_info"]["expected_member_num"] = pginfo.expected_member_num;
    j["pg_info"]["chunk_size"] = pginfo.chunk_size;
    j["pg_info"]["repl_uuid"] = boost::uuids::to_string(pginfo.replica_set_uuid);

    nlohmann::json members_j{};
    for (auto const& member : pginfo.members) {
        nlohmann::json member_j;
        member_j["member_id"] = boost::uuids::to_string(member.id);
        member_j["name"] = member.name;
        member_j["priority"] = member.priority;
        members_j.push_back(member_j);
    }
    j["pg_info"]["members"] = members_j;
    return j.dump();
}

PGInfo HSHomeObject::deserialize_pg_info(const unsigned char* json_str, size_t size) {
    auto pg_json = nlohmann::json::parse(json_str, json_str + size);

    PGInfo pg_info(pg_json["pg_info"]["pg_id_t"].get< pg_id_t >());
    pg_info.size = pg_json["pg_info"]["pg_size"].get< uint64_t >();
    pg_info.expected_member_num = pg_json["pg_info"]["expected_member_num"].get< uint32_t >();
    pg_info.chunk_size = pg_json["pg_info"]["chunk_size"].get< uint64_t >();
    pg_info.replica_set_uuid = boost::uuids::string_generator()(pg_json["pg_info"]["repl_uuid"].get< std::string >());

    for (auto const& m : pg_json["pg_info"]["members"]) {
        auto uuid_str = m["member_id"].get< std::string >();
        PGMember member(boost::uuids::string_generator()(uuid_str));
        member.name = m["name"].get< std::string >();
        member.priority = m["priority"].get< int32_t >();
        pg_info.members.emplace(std::move(member));
    }
    return pg_info;
}

void HSHomeObject::on_pg_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie) {
    LOGI("on_pg_meta_blk_found is called")
    homestore::superblk< pg_info_superblk > pg_sb(_pg_meta_name);
    pg_sb.load(buf, meta_cookie);

    auto v = hs_repl_service().get_repl_dev(pg_sb->replica_set_uuid);
    if (v.hasError()) {
        // TODO: We need to raise an alert here, since without pg repl_dev all operations on that pg will fail
        LOGE("open_repl_dev for group_id={} has failed, pg={}", boost::uuids::to_string(pg_sb->replica_set_uuid),
             pg_sb->id);
        return;
    }
    auto pg_id = pg_sb->id;
    std::vector< chunk_num_t > p_chunk_ids(pg_sb->get_chunk_ids(), pg_sb->get_chunk_ids() + pg_sb->num_chunks);
    bool set_pg_chunks_res = chunk_selector_->recover_pg_chunks(pg_id, std::move(p_chunk_ids));
    auto uuid_str = boost::uuids::to_string(pg_sb->index_table_uuid);
    auto hs_pg = std::make_unique< HS_PG >(std::move(pg_sb), std::move(v.value()));
    if (!set_pg_chunks_res) {
        hs_pg->pg_state_.set_state(PGStateMask::DISK_DOWN);
        hs_pg->repl_dev_->set_stage(homestore::repl_dev_stage_t::UNREADY);
        LOGW("Failed to recover pg={} chunks,set pg_state to DISK_DOWN", pg_id);
    }
    // During PG recovery check if index is already recoverd else
    // add entry in map, so that index recovery can update the PG.
    std::scoped_lock lg(index_lock_);
    auto it = index_table_pg_map_.find(uuid_str);
    if (it != index_table_pg_map_.end()) {
        hs_pg->index_table_ = it->second.index_table;
        it->second.pg_id = pg_id;
    } else {
        RELEASE_ASSERT(hs_pg->pg_sb_->state == PGState::DESTROYED, "IndexTable should be recovered before PG");
        hs_pg->index_table_ = nullptr;
        LOGI("Index table not found for destroyed pg={}, index_table_uuid={}", pg_id, uuid_str);
    }

    add_pg_to_map(std::move(hs_pg));
}

PGInfo HSHomeObject::HS_PG::pg_info_from_sb(homestore::superblk< pg_info_superblk > const& sb) {
    PGInfo pginfo{sb->id};
    const pg_members* sb_members = sb->get_pg_members();
    for (uint32_t i{0}; i < sb->num_dynamic_members; ++i) {
        pginfo.members.emplace(sb_members[i].id, std::string(sb_members[i].name), sb_members[i].priority);
    }
    pginfo.size = sb->pg_size;
    pginfo.replica_set_uuid = sb->replica_set_uuid;
    pginfo.chunk_size = sb->chunk_size;
    pginfo.expected_member_num = sb->num_expected_members;
    return pginfo;
}

HSHomeObject::HS_PG::HS_PG(PGInfo info, shared< homestore::ReplDev > rdev, shared< BlobIndexTable > index_table,
                           std::shared_ptr< const std::vector< chunk_num_t > > pg_chunk_ids) :
        PG{std::move(info)},
        pg_sb_{_pg_meta_name},
        repl_dev_{std::move(rdev)},
        index_table_{std::move(index_table)},
        metrics_{*this},
        snp_rcvr_info_sb_{_snp_rcvr_meta_name},
        snp_rcvr_shard_list_sb_{_snp_rcvr_shard_list_meta_name} {
    RELEASE_ASSERT(pg_chunk_ids != nullptr, "PG chunks null, pg={}", pg_info_.id);
    const uint32_t num_chunks = pg_chunk_ids->size();
    pg_sb_.create(sizeof(pg_info_superblk) - sizeof(char) + 2 * pg_info_.expected_member_num * sizeof(pg_members) +
                  num_chunks * sizeof(homestore::chunk_num_t));
    pg_sb_->id = pg_info_.id;
    pg_sb_->state = PGState::ALIVE;
    pg_sb_->num_expected_members = pg_info_.expected_member_num;
    pg_sb_->num_dynamic_members = pg_info_.members.size();
    pg_sb_->num_chunks = num_chunks;
    pg_sb_->chunk_size = pg_info_.chunk_size;
    pg_sb_->pg_size = pg_info_.size;
    pg_sb_->replica_set_uuid = repl_dev_->group_id();
    pg_sb_->index_table_uuid = index_table_->uuid();
    pg_sb_->active_blob_count = 0;
    pg_sb_->tombstone_blob_count = 0;
    pg_sb_->total_occupied_blk_count = 0;
    uint32_t i{0};
    pg_members* pg_sb_members = pg_sb_->get_pg_members_mutable();
    for (auto const& m : pg_info_.members) {
        pg_sb_members[i].id = m.id;
        DEBUG_ASSERT(m.name.size() <= PGMember::max_name_len, "member name exceeds max len, name={}", m.name);
        auto name_len = std::min(m.name.size(), PGMember::max_name_len);
        std::strncpy(pg_sb_members[i].name, m.name.c_str(), name_len);
        pg_sb_members[i].name[name_len] = '\0';
        pg_sb_members[i].priority = m.priority;
        ++i;
    }
    chunk_num_t* pg_sb_chunk_ids = pg_sb_->get_chunk_ids_mutable();
    for (i = 0; i < num_chunks; ++i) {
        pg_sb_chunk_ids[i] = pg_chunk_ids->at(i);
    }
    pg_sb_.write();
}

HSHomeObject::HS_PG::HS_PG(superblk< pg_info_superblk >&& sb, shared< ReplDev > rdev) :
        PG{pg_info_from_sb(sb)}, pg_sb_{std::move(sb)}, repl_dev_{std::move(rdev)}, metrics_{*this} {
    durable_entities_.blob_sequence_num = pg_sb_->blob_sequence_num;
    durable_entities_.active_blob_count = pg_sb_->active_blob_count;
    durable_entities_.tombstone_blob_count = pg_sb_->tombstone_blob_count;
    durable_entities_.total_occupied_blk_count = pg_sb_->total_occupied_blk_count;
    durable_entities_.total_reclaimed_blk_count = pg_sb_->total_reclaimed_blk_count;
}

uint32_t HSHomeObject::HS_PG::total_shards() const { return shards_.size(); }

uint32_t HSHomeObject::HS_PG::open_shards() const {
    return std::count_if(shards_.begin(), shards_.end(), [](auto const& s) { return s->is_open(); });
}

// Return the percentage of snapshot progress
uint32_t HSHomeObject::HS_PG::get_snp_progress() const {
    return snp_rcvr_info_sb_->progress.total_bytes > 0
        ? 100 * snp_rcvr_info_sb_->progress.complete_bytes / snp_rcvr_info_sb_->progress.total_bytes
        : 0;
}

void HSHomeObject::HS_PG::get_peer_info(std::vector< peer_info >& members) const {
    auto const replication_status = repl_dev_->get_replication_status();
    for (auto const& m : pg_info_.members) {
        // replication_status can be empty in follower
        auto peer = peer_info{.id = m.id, .name = m.name};
        for (auto const& r : replication_status) {
            if (r.id_ == m.id) {
                peer.can_vote = r.can_vote;
                peer.last_commit_lsn = r.replication_idx_;
                peer.last_succ_resp_us = r.last_succ_resp_us_;
                break;
            }
        }
        members.emplace_back(peer);
    }
}

void HSHomeObject::HS_PG::reconcile_leader() const { repl_dev_->reconcile_leader(); }

void HSHomeObject::HS_PG::yield_leadership_to_follower() const {
    if (!repl_dev_->is_leader()) {
        LOGDEBUG("Not a leader, no need to yield leadership");
        return;
    }

    auto leader_id = repl_dev_->get_leader_id();
    auto candidate_leader_id = leader_id;
    int32_t highest_prority = 0;
    auto const replication_status = repl_dev_->get_replication_status();
    for (auto const& r : replication_status) {
        if (r.id_ == leader_id) continue;
        auto priority = static_cast< int32_t >(r.priority_);
        if (priority > highest_prority) {
            highest_prority = priority;
            candidate_leader_id = r.id_;
        }
    }

    if (candidate_leader_id == leader_id) {
        LOGDEBUG("cannot find a candidate leader except current leader {}, candidate_priority={}",
                 boost::uuids::to_string(leader_id), highest_prority);
        return;
    }

    LOGI("Trying to yield leadership from {} to {}, candidate_priority={}", boost::uuids::to_string(leader_id),
         boost::uuids::to_string(candidate_leader_id), highest_prority);
    repl_dev_->yield_leadership(false /*immediate_yield*/, candidate_leader_id);
}

void HSHomeObject::HS_PG::trigger_snapshot_creation(int64_t compact_lsn, bool is_async) const {
    repl_dev_->trigger_snapshot_creation(compact_lsn, is_async);
}

std::vector< Shard > HSHomeObject::HS_PG::get_chunk_shards(chunk_num_t v_chunk_id) const {
    std::vector< Shard > ret;
    for (auto const& s : shards_) {
        auto hs_shard = dynamic_cast< HS_Shard* >(s.get());
        if (hs_shard->v_chunk_id() == v_chunk_id) { ret.push_back(*s); }
    }
    return ret;
}

// NOTE: caller should hold the _pg_lock
const HSHomeObject::HS_PG* HSHomeObject::_get_hs_pg_unlocked(pg_id_t pg_id) const {
    auto iter = _pg_map.find(pg_id);
    return iter == _pg_map.end() ? nullptr : dynamic_cast< HS_PG* >(iter->second.get());
}

const HSHomeObject::HS_PG* HSHomeObject::get_hs_pg(pg_id_t pg_id) const {
    std::shared_lock lck(_pg_lock);
    return _get_hs_pg_unlocked(pg_id);
}

bool HSHomeObject::_get_stats(pg_id_t id, PGStats& stats) const {
    auto hs_pg = get_hs_pg(id);
    if (hs_pg == nullptr) return false;
    if (hs_pg->pg_state_.is_state_set(PGStateMask::DISK_DOWN)) {
        stats.id = hs_pg->pg_info_.id;
        stats.pg_state = hs_pg->pg_state_.get();
        stats.replica_set_uuid = hs_pg->pg_info_.replica_set_uuid;
        return true;
    }
    auto const blk_size = hs_pg->repl_dev_->get_blk_size();

    stats.id = hs_pg->pg_info_.id;
    stats.replica_set_uuid = hs_pg->pg_info_.replica_set_uuid;
    stats.num_members = hs_pg->pg_info_.members.size();
    stats.pg_state = hs_pg->pg_state_.get();
    stats.total_shards = hs_pg->total_shards();
    stats.open_shards = hs_pg->open_shards();
    stats.leader_id = hs_pg->repl_dev_->get_leader_id();
    stats.num_active_objects = hs_pg->durable_entities().active_blob_count.load(std::memory_order_relaxed);
    stats.num_tombstone_objects = hs_pg->durable_entities().tombstone_blob_count.load(std::memory_order_relaxed);

    hs_pg->get_peer_info(stats.members);

    stats.avail_open_shards = chunk_selector()->avail_num_chunks(hs_pg->pg_info_.id);
    stats.avail_bytes = chunk_selector()->avail_blks(hs_pg->pg_info_.id) * blk_size;
    stats.used_bytes = hs_pg->durable_entities().total_occupied_blk_count.load(std::memory_order_relaxed) * blk_size;
    if (!hs_pg->snp_rcvr_info_sb_.is_empty()) { stats.snp_progress = hs_pg->get_snp_progress(); }
    return true;
}

void HSHomeObject::_get_pg_ids(std::vector< pg_id_t >& pg_ids) const {
    {
        auto lg = std::shared_lock(_pg_lock);
        for (auto& [id, _] : _pg_map) {
            pg_ids.push_back(id);
        }
    }
}

void HSHomeObject::on_create_pg_message_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                                 cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< PGManager::NullResult >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer()) {
        ctx = boost::static_pointer_cast< repl_result_ctx< PGManager::NullResult > >(hs_ctx).get();
    }

    auto const* msg_header = r_cast< ReplicationMessageHeader const* >(header.cbytes());

    if (msg_header->corrupted()) {
        LOGE("create PG message header is corrupted , lsn={}, header={}", lsn, msg_header->to_string());
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(PGError::CRC_MISMATCH)); }
        return;
    }

    auto serailized_pg_info_buf = header.cbytes() + sizeof(ReplicationMessageHeader);
    const auto serailized_pg_info_size = header.size() - sizeof(ReplicationMessageHeader);

    if (crc32_ieee(init_crc32, serailized_pg_info_buf, serailized_pg_info_size) != msg_header->payload_crc) {
        // header & value is inconsistent;
        LOGE("create PG message header is inconsistent with value, lsn={}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(PGError::CRC_MISMATCH)); }
        return;
    }

    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_PG_MSG: {
        // TODO:: add rollback logic for create pg rollback if necessary
        LOGI("lsn={}, msg_type={}, pg={}, create pg is rollbacked", lsn, msg_header->msg_type, msg_header->pg_id);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(PGError::ROLL_BACK)); }
        break;
    }
    default: {
        LOGW("lsn={}, mes_type={}, pg={}, should not happen in pg message rollback", lsn, msg_header->msg_type,
             msg_header->pg_id);
        break;
    }
    }
}

uint32_t HSHomeObject::get_pg_tombstone_blob_count(pg_id_t pg_id) const {
    // caller should hold the _pg_lock
    auto hs_pg = _get_hs_pg_unlocked(pg_id);
    if (hs_pg == nullptr) {
        LOGW("get pg tombstone blob count with unknown pg={}", pg_id);
        return 0;
    }

    uint32_t tombstone_blob_count{0};

    auto start_key =
        BlobRouteKey{BlobRoute{uint64_t(pg_id) << homeobject::shard_width, std::numeric_limits< uint64_t >::min()}};
    auto end_key =
        BlobRouteKey{BlobRoute{uint64_t(pg_id + 1) << homeobject::shard_width, std::numeric_limits< uint64_t >::min()}};

    homestore::BtreeQueryRequest< BlobRouteKey > query_req{
        homestore::BtreeKeyRange< BlobRouteKey >{std::move(start_key), true /* inclusive */, std::move(end_key),
                                                 false /* inclusive */},
        homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY,
        std::numeric_limits< uint32_t >::max() /* blob count in a shard will not exceed uint32_t_max*/,
        [&tombstone_blob_count](homestore::BtreeKey const& key, homestore::BtreeValue const& value) mutable -> bool {
            BlobRouteValue existing_value{value};
            if (existing_value.pbas() == HSHomeObject::tombstone_pbas) { tombstone_blob_count++; }
            return false;
        }};

    std::vector< std::pair< BlobRouteKey, BlobRouteValue > > valid_blob_indexes;

    auto const status = hs_pg->index_table_->query(query_req, valid_blob_indexes);
    if (status != homestore::btree_status_t::success && status != homestore::btree_status_t::has_more) {
        LOGERROR("Failed to query blobs in index table for pg={}", pg_id);
        return 0;
    }
    // we should not have any valid blob indexes in tombstone query
    RELEASE_ASSERT(valid_blob_indexes.empty(), "we only query tombstone in pg={}, but got vaild blob in return value",
                   pg_id);

    return tombstone_blob_count;
}

void HSHomeObject::update_pg_meta_after_gc(const pg_id_t pg_id, const homestore::chunk_num_t move_from_chunk,
                                           const homestore::chunk_num_t move_to_chunk, const uint64_t task_id) {
    // 1 update pg metrics
    std::unique_lock lck(_pg_lock);
    auto iter = _pg_map.find(pg_id);
    // TODO:: revisit here with the considering of destroying pg
    RELEASE_ASSERT(iter != _pg_map.end(), "can not find pg_id={} in pg_map", pg_id);
    auto hs_pg = dynamic_cast< HS_PG* >(iter->second.get());
    auto move_from_v_chunk = chunk_selector()->get_extend_vchunk(move_from_chunk);

    // TODO:: for now, when updating pchunk for a vchunk, we have to update the whole pg super blk. we can optimize this
    // by persist a single superblk for each vchunk in the pg, so that we only need to update the vchunk superblk
    // itself.

    auto pg_chunks = hs_pg->pg_sb_->get_chunk_ids_mutable();

    RELEASE_ASSERT(move_from_v_chunk != nullptr, "can not find EXVchunk for chunk={}", move_from_chunk);
    RELEASE_ASSERT(move_from_v_chunk->m_v_chunk_id.has_value(), "can not find vchunk for chunk={}, pg_id={}",
                   move_from_chunk, pg_id);
    auto v_chunk_id = move_from_v_chunk->m_v_chunk_id.value();

    if (sisl_unlikely(pg_chunks[v_chunk_id] == move_to_chunk)) {
        // this might happens when crash recovery. the crash happens after pg metablk is updated but before gc task
        // metablk is destroyed.
        LOGD("gc task_id={}, the pchunk_id for vchunk={} for pg_id={} is already {},  update pg metablk again!",
             task_id, v_chunk_id, pg_id, move_to_chunk);
    } else {
        RELEASE_ASSERT(pg_chunks[v_chunk_id] == move_from_chunk,
                       "vchunk_id={} chunk_id={} is not equal to move_from_chunk={} for pg={}", v_chunk_id,
                       pg_chunks[v_chunk_id], move_from_chunk, pg_id);
        // update the vchunk to new pchunk(move_to_chunk)
        pg_chunks[v_chunk_id] = move_to_chunk;
        LOGD("gc task_id={}, pchunk for vchunk={} of pg_id={} is updated from {} to {}", task_id, v_chunk_id, pg_id,
             move_from_chunk, move_to_chunk);

        // TODO:hs_pg->shards_.size() will be decreased by 1 in delete_shard if gc finds a empty shard, which will be
        // implemented later
        hs_pg->durable_entities_update([this, move_from_v_chunk, &move_to_chunk, &move_from_chunk, &pg_id,
                                        &task_id](auto& de) {
            // active_blob_count is updated by put/delete blob, not change it here.

            // considering the complexity of gc crash recovery for tombstone_blob_count, we get it directly from index
            // table , which is the most accurate.

            // TODO::do we need this as durable entity? remove it and get all the from pg index in real time.
            de.tombstone_blob_count = get_pg_tombstone_blob_count(pg_id);

            auto move_to_v_chunk = chunk_selector()->get_extend_vchunk(move_to_chunk);

            auto total_occupied_blk_count_by_move_from_chunk = move_from_v_chunk->get_used_blks();
            auto total_occupied_blk_count_by_move_to_chunk = move_to_v_chunk->get_used_blks();

            // TODO::in recovery case , this might be updated again , fix me later.
            const auto reclaimed_blk_count =
                total_occupied_blk_count_by_move_from_chunk - total_occupied_blk_count_by_move_to_chunk;

            de.total_occupied_blk_count -= reclaimed_blk_count;
            de.total_reclaimed_blk_count += reclaimed_blk_count;

            LOGD("gc task_id={}, move_from_chunk={}, total_occupied_blk_count_by_move_from_chunk={}, move_to_chunk={}, "
                 "total_occupied_blk_count_by_move_to_chunk={}, total_occupied_blk_count={}",
                 task_id, move_from_chunk, total_occupied_blk_count_by_move_from_chunk, move_to_chunk,
                 total_occupied_blk_count_by_move_to_chunk, de.total_occupied_blk_count.load());
        });

        hs_pg->pg_sb_->total_occupied_blk_count =
            hs_pg->durable_entities().total_occupied_blk_count.load(std::memory_order_relaxed);

        hs_pg->pg_sb_->tombstone_blob_count =
            hs_pg->durable_entities().tombstone_blob_count.load(std::memory_order_relaxed);

        // we need to persist the updated pg superblk since we have updated the pg_chunks
        hs_pg->pg_sb_.write();

        // 2 change the pg_chunkcollection in chunk selector.
        chunk_selector()->switch_chunks_for_pg(pg_id, move_from_chunk, move_to_chunk, task_id);
    }
}

} // namespace homeobject
