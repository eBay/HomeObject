#include "replication_message.hpp"
#include "replication_state_machine.hpp"
#include "hs_backend_config.hpp"

#include "generated/resync_pg_data_generated.h"
#include "generated/resync_shard_data_generated.h"
#include "generated/resync_blob_data_generated.h"
#include <homestore/replication/repl_dev.h>
#include <homestore/replication/repl_decls.h>
#include "hs_homeobject.hpp"

namespace homeobject {

void ReplicationStateMachine::on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                                        const std::vector< homestore::MultiBlkId >& pbas,
                                        cintrusive< homestore::repl_req_ctx >& ctx) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    RELEASE_ASSERT_EQ(pbas.size(), 1, "Invalid blklist size");

    LOGT("applying raft log commit with lsn={}, msg type={}", lsn, msg_header->msg_type);
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_PG_MSG: {
        home_object_->on_create_pg_message_commit(lsn, header, repl_dev(), ctx);
        break;
    }
    case ReplicationMessageType::CREATE_SHARD_MSG:
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        home_object_->on_shard_message_commit(lsn, header, pbas[0], repl_dev(), ctx);
        break;
    }

    case ReplicationMessageType::PUT_BLOB_MSG: {
        home_object_->on_blob_put_commit(lsn, header, key, pbas[0], ctx);
        break;
    }
    case ReplicationMessageType::DEL_BLOB_MSG:
        home_object_->on_blob_del_commit(lsn, header, key, ctx);
        break;
    default: {
        break;
    }
    }
}

void ReplicationStateMachine::notify_committed_lsn(int64_t lsn) {
    // handle no_space_left error if we have any
    const auto [target_lsn, chunk_id] = get_no_space_left_error_info();
    if (std::numeric_limits< homestore::repl_lsn_t >::max() == target_lsn) {
        // no pending no_space_left error to handle
        return;
    }

    RELEASE_ASSERT(target_lsn >= lsn,
                   "the lsn of no_space_left_error_info should be greater than or equal to the lsn of the "
                   "committed rreq. "
                   "current no_space_left_error_info lsn={}, committed rreq lsn={}",
                   target_lsn, lsn);

    if (repl_dev()->is_leader()) {
        // no need to handle stale no_space_left as a leader, since that log(casuing no_space_left) is not in the
        // log store, and will not be committed.
        LOGT("I am leader, reset no_space_left_error_info(lsn={}, chunk_id={}) after lsn={} is committed", target_lsn,
             chunk_id, lsn);
        reset_no_space_left_error_info();
        return;
    }

    if (target_lsn == lsn) {
        LOGD("match no_space_left_error_info, lsn={}, chunk_id={}", lsn, chunk_id);
        handle_no_space_left(lsn, chunk_id);
        reset_no_space_left_error_info();
    }
}

bool ReplicationStateMachine::on_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                            cintrusive< homestore::repl_req_ctx >& ctx) {
    // For shard creation, since homestore repldev inside will write shard header to data service first before this
    // function is called. So there is nothing is needed to do and we can get the binding chunk_id with the newly shard
    // from the blkid in on_commit()
    homestore::journal_type_t op_code = ctx->op_code();
    bool is_data_op =
        (op_code == homestore::journal_type_t::HS_DATA_LINKED || op_code == homestore::journal_type_t::HS_DATA_INLINED);
    if (!is_data_op) {
        LOGI("pre_commit {} log entry, lsn={}", enum_name(op_code), lsn);
        return true;
    }

    RELEASE_ASSERT(is_data_op, "pre_commit should only be called for linked or inlined data, current op_code={}",
                   enum_name(op_code));
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGE("corrupted message in pre_commit, lsn={}", lsn);
        return false;
    }
    LOGT("on_pre_commit with lsn={}, msg type={}", lsn, msg_header->msg_type);
    switch (msg_header->msg_type) {
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        return home_object_->on_shard_message_pre_commit(lsn, header, key, ctx);
    }
    default: {
        break;
    }
    }
    return true;
}

void ReplicationStateMachine::on_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                          cintrusive< homestore::repl_req_ctx >& ctx) {
    LOGI("on_rollback  with lsn={}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGE("corrupted message in rollback, lsn={}", lsn);
        return;
    }
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG:
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        home_object_->on_shard_message_rollback(lsn, header, key, ctx);
        break;
    }

    case ReplicationMessageType::PUT_BLOB_MSG:
    case ReplicationMessageType::DEL_BLOB_MSG: {
        home_object_->on_blob_message_rollback(lsn, header, key, ctx);
        break;
    }

    case ReplicationMessageType::CREATE_PG_MSG: {
        home_object_->on_create_pg_message_rollback(lsn, header, key, ctx);
        break;
    }

    default: {
        break;
    }
    }

    const auto [target_lsn, chunk_id] = get_no_space_left_error_info();

    RELEASE_ASSERT(
        target_lsn >= lsn,
        "wait_commit_lsn should be bigger than rollbacked lsn wait_commit_lsn={}, chunk_id={}, current lsn={}",
        target_lsn, chunk_id, lsn);

    // if target_lsn is int64_max, it`s is also ok to reset_no_space_left_error_info
    reset_no_space_left_error_info();
}

void ReplicationStateMachine::on_config_rollback(int64_t lsn) {
    LOGD("rollback config at lsn={}", lsn);

    const auto [target_lsn, chunk_id] = get_no_space_left_error_info();

    RELEASE_ASSERT(
        target_lsn >= lsn,
        "wait_commit_lsn should be bigger than rollbacked lsn wait_commit_lsn={}, chunk_id={}, current lsn={}",
        target_lsn, chunk_id, lsn);

    // if target_lsn is int64_max, it`s is also ok to reset_no_space_left_error_info
    reset_no_space_left_error_info();
}

void ReplicationStateMachine::on_restart() { LOGD("ReplicationStateMachine::on_restart"); }

void ReplicationStateMachine::on_error(ReplServiceError error, const sisl::blob& header, const sisl::blob& key,
                                       cintrusive< repl_req_ctx >& ctx) {
    RELEASE_ASSERT(ctx, "ctx should not be nullptr in on_error");
    RELEASE_ASSERT(ctx->is_proposer(), "on_error should only be called from proposer");
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    LOGE("on_error, message type={} with lsn={}, error={}", msg_header->msg_type, ctx->lsn(), error);
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_PG_MSG: {
        auto result_ctx = boost::static_pointer_cast< repl_result_ctx< PGManager::NullResult > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(homeobject::toPgError(error)));
        break;
    }
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        bool res = home_object_->release_chunk_based_on_create_shard_message(header);
        if (!res) { LOGW("failed to release chunk based on create shard msg"); }
        auto result_ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(toShardError(error)));
        break;
    }
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        auto result_ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(toShardError(error)));
        break;
    }
    case ReplicationMessageType::PUT_BLOB_MSG:
    case ReplicationMessageType::DEL_BLOB_MSG: {
        auto result_ctx =
            boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< HSHomeObject::BlobInfo > > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(toBlobError(error)));
        break;
    }
    default: {
        LOGE("Unknown message type, error unhandled , error={}, lsn={}", error, ctx->lsn());
        break;
    }
    }
}

homestore::ReplResult< homestore::blk_alloc_hints >
ReplicationStateMachine::get_blk_alloc_hints(sisl::blob const& header, uint32_t data_size,
                                             cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        pg_id_t pg_id = msg_header->pg_id;
        // check whether the pg exists
        if (!home_object_->pg_exists(pg_id)) {
            LOGI("shardID=0x{:x}, pg={}, shard=0x{:x}, can not find pg={} when getting blk_alloc_hint",
                 msg_header->shard_id, (msg_header->shard_id >> homeobject::shard_width),
                 (msg_header->shard_id & homeobject::shard_mask), pg_id);
            // TODO:: add error code to indicate the pg not found in homestore side
            return folly::makeUnexpected(homestore::ReplServiceError::RESULT_NOT_EXIST_YET);
        }

        auto v_chunkID = home_object_->resolve_v_chunk_id_from_msg(header);
        if (!v_chunkID.has_value()) {
            LOGW("shardID=0x{:x}, pg={}, shard=0x{:x}, can not resolve v_chunk_id from msg", msg_header->shard_id,
                 (msg_header->shard_id >> homeobject::shard_width), (msg_header->shard_id & homeobject::shard_mask));
            return folly::makeUnexpected(homestore::ReplServiceError::FAILED);
        }
        homestore::blk_alloc_hints hints;
        // Both chunk_num_t and pg_id_t are of type uint16_t.
        static_assert(std::is_same< pg_id_t, uint16_t >::value, "pg_id_t is not uint16_t");
        static_assert(std::is_same< homestore::chunk_num_t, uint16_t >::value, "chunk_num_t is not uint16_t");
        homestore::chunk_num_t v_chunk_id = v_chunkID.value();
        hints.application_hint = ((uint64_t)pg_id << 16) | v_chunk_id;
        if (hs_ctx->is_proposer()) { hints.reserved_blks = home_object_->get_reserved_blks(); }

        auto tid = hs_ctx ? hs_ctx->traceID() : 0;
        LOGD("tid={}, get_blk_alloc_hint for creating shard, select vchunk_id={} for pg={}, shardID={}", tid,
             v_chunk_id, pg_id, msg_header->shard_id);

        return hints;
    }

    case ReplicationMessageType::SEAL_SHARD_MSG: {
        auto p_chunkID = home_object_->get_shard_p_chunk_id(msg_header->shard_id);
        if (!p_chunkID.has_value()) {
            LOGW("shardID=0x{:x}, pg={}, shard=0x{:x}, shard does not exist, underlying engine will retry this later",
                 msg_header->shard_id, (msg_header->shard_id >> homeobject::shard_width),
                 (msg_header->shard_id & homeobject::shard_mask));
            return folly::makeUnexpected(homestore::ReplServiceError::RESULT_NOT_EXIST_YET);
        }
        homestore::blk_alloc_hints hints;
        hints.chunk_id_hint = p_chunkID.value();
        return hints;
    }

    case ReplicationMessageType::PUT_BLOB_MSG:
        return home_object_->blob_put_get_blk_alloc_hints(header, hs_ctx);

    default: {
        LOGW("not support msg type for {} in get_blk_alloc_hints", msg_header->msg_type);
        break;
    }
    }
    return homestore::blk_alloc_hints();
}

void ReplicationStateMachine::on_start_replace_member(const std::string& task_id,
                                                      const homestore::replica_member_info& member_out,
                                                      const homestore::replica_member_info& member_in, trace_id_t tid) {
    home_object_->on_pg_start_replace_member(repl_dev()->group_id(), task_id, member_out, member_in, tid);
}

void ReplicationStateMachine::on_complete_replace_member(const std::string& task_id,
                                                         const homestore::replica_member_info& member_out,
                                                         const homestore::replica_member_info& member_in,
                                                         trace_id_t tid) {
    home_object_->on_pg_complete_replace_member(repl_dev()->group_id(), task_id, member_out, member_in, tid);
}

void ReplicationStateMachine::on_destroy(const homestore::group_id_t& group_id) {
    auto PG_ID = home_object_->get_pg_id_with_group_id(group_id);
    if (!PG_ID.has_value()) {
        LOGW("do not have pg mapped by group_id={}", boost::uuids::to_string(group_id));
        return;
    }
    home_object_->pg_destroy(PG_ID.value());
    LOGI("replica destroyed, cleared pg={} resources with group_id={}", PG_ID.value(),
         boost::uuids::to_string(group_id));
}

homestore::AsyncReplResult<>
ReplicationStateMachine::create_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    std::lock_guard lk(m_snapshot_lock);
    if (get_snapshot_context() != nullptr && context->get_lsn() < m_snapshot_context->get_lsn()) {
        LOGI("Skipping create snapshot, new snapshot lsn={} is less than current snapshot lsn={}", context->get_lsn(),
             m_snapshot_context->get_lsn());
        return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
    }

    LOGI("create snapshot with lsn={}", context->get_lsn());
    set_snapshot_context(context);
    return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
}

bool ReplicationStateMachine::apply_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
#ifdef _PRERELEASE
    auto delay = iomgr_flip::instance()->get_test_flip< long >("simulate_apply_snapshot_delay");
    LOGD("simulate_apply_snapshot_delay flip, triggered={}", delay.has_value());
    if (delay) {
        LOGI("Simulating apply snapshot with delay, delay={}", delay.get());
        std::this_thread::sleep_for(std::chrono::milliseconds(delay.get()));
    }
    // Currently, nuraft will pause state machine and resume it after the last snp obj is saved. So we don't need to
    // resume it explicitly. home_object_->resume_pg_state_machine(m_snp_rcv_handler->get_context_pg_id());
#endif
    m_snp_rcv_handler->destroy_context_and_metrics();

    std::lock_guard lk(m_snapshot_lock);
    set_snapshot_context(context);
    return true;
}

std::shared_ptr< homestore::snapshot_context > ReplicationStateMachine::last_snapshot() {
    std::lock_guard lk(m_snapshot_lock);
    return get_snapshot_context();
}

int ReplicationStateMachine::read_snapshot_obj(std::shared_ptr< homestore::snapshot_context > context,
                                               std::shared_ptr< homestore::snapshot_obj > snp_obj) {
    std::shared_ptr< HSHomeObject::PGBlobIterator > pg_iter;
    {
        std::lock_guard lk(m_snp_sync_ctx_lock);
        if (snp_obj->user_ctx == nullptr) {
            // Create the pg blob iterator for the first time.
            pg_iter = std::make_shared< HSHomeObject::PGBlobIterator >(*home_object_, repl_dev()->group_id(),
                                                                       context->get_lsn());
            auto pg_iter_ptr = new std::shared_ptr< HSHomeObject::PGBlobIterator >(pg_iter);
            snp_obj->user_ctx = static_cast< void* >(pg_iter_ptr);
            LOGD("Allocated new pg blob iterator={}, group={}, lsn={}", snp_obj->user_ctx,
                 boost::uuids::to_string(repl_dev()->group_id()), context->get_lsn());
        } else {
            auto pg_iter_ptr = static_cast< std::shared_ptr< HSHomeObject::PGBlobIterator >* >(snp_obj->user_ctx);
            pg_iter = *pg_iter_ptr;
        }
    }

    // Nuraft uses obj_id as a way to track the state of the snapshot read and write.
    // Nuraft starts with obj_id == 0 as first message always, leader send all the shards and
    // PG metadata as response. Follower responds with next obj_id it expects. obj_id's are
    // encoded in the form ofobj_id (64 bits) = type_bit (1 bit) | shard_seq_num (48 bits) | batch_id (15 bits)
    // Leader starts with shard sequence number 1 and read upto maximum size of data
    // and send to follower in a batch. Once all blob's are send in a shard,
    // leader notifies the follower by setting is_last_batch in the payload. Follower
    // moves to the next shard by incrementing shard_seq_num and reset batch number to 0.
    // Batch number is used to identify which batch in the current shard sequence number.
    // We use pg blob iterator to go over all the blobs in all the shards in that PG.
    // Once all the shards are done, follower will return next obj Id = LAST_OBJ_ID(ULLONG_MAX) as a end marker,
    // leader will stop sending the snapshot data.
    auto log_str = fmt::format("group={}, lsn={}", uuids::to_string(repl_dev()->group_id()), context->get_lsn());
    if (snp_obj->offset == LAST_OBJ_ID) {
        // No more shards to read, baseline resync is finished after this.
        snp_obj->is_last_obj = true;
        LOGD("Read snapshot end, {}", log_str);
        return 0;
    }

    auto obj_id = objId(snp_obj->offset);
    log_str = fmt::format("{} shard_seq_num=0x{:x} batch_num={}", log_str, obj_id.shard_seq_num, obj_id.batch_id);

    LOGI("Read current snp obj {}", log_str)
    if (!pg_iter->update_cursor(obj_id)) {
        // There is a known corner case (not sure if it is the only case): If free_user_snp_ctx and read_snapshot_obj
        // (we enable NuRaft bg snapshot) occur at the same time, and free_user_snp_ctx is called first, pg_iter is
        // released, and then in read_snapshot_obj, pg_iter will be created with cur_obj_id_ = 0|0 while the next_obj_id
        // will be x|y which may hit into invalid objId condition. If inconsistency happens, reset the cursor to the
        // beginning(0|0), send an empty message, and let follower validate (lsn may change) and reset its cursor to the
        // checkpoint to proceed with snapshot resync.
        LOGW("Invalid objId in snapshot read, reset cursor to the beginning, {}", log_str);
        pg_iter->reset_cursor();
        return 0;
    }

    // pg metadata message
    // shardId starts from 1
    if (obj_id.shard_seq_num == 0) {
        if (!pg_iter->create_pg_snapshot_data(snp_obj->blob)) {
            LOGE("Failed to create pg snapshot data for snapshot read, {}", log_str);
            return -1;
        }
        return 0;
    }

    // shard metadata message
    if (obj_id.batch_id == 0) {
        if (!pg_iter->generate_shard_blob_list()) {
            LOGE("Failed to generate shard blob list for snapshot read, {}", log_str);
            return -1;
        }
        if (!pg_iter->create_shard_snapshot_data(snp_obj->blob)) {
            LOGE("Failed to create shard meta data for snapshot read, {}", log_str);
            return -1;
        }
        return 0;
    }

    // general blob message
    if (!pg_iter->create_blobs_snapshot_data(snp_obj->blob)) {
        LOGE("Failed to create blob batch data for snapshot read, {}", log_str);
        return -1;
    }
    return 0;
}

void ReplicationStateMachine::write_snapshot_obj(std::shared_ptr< homestore::snapshot_context > context,
                                                 std::shared_ptr< homestore::snapshot_obj > snp_obj) {
    RELEASE_ASSERT(context != nullptr, "Context null");
    RELEASE_ASSERT(snp_obj != nullptr, "Snapshot data null");
    auto r_dev = repl_dev();
    if (!m_snp_rcv_handler) {
        m_snp_rcv_handler = std::make_unique< HSHomeObject::SnapshotReceiveHandler >(*home_object_, r_dev);
        if (m_snp_rcv_handler->load_prev_context_and_metrics()) {
            LOGI("Reloaded previous snapshot context, lsn={} pg={} next_shard:0x{:x}", context->get_lsn(),
                 m_snp_rcv_handler->get_context_pg_id(), m_snp_rcv_handler->get_next_shard());
        }
    }

    auto obj_id = objId(snp_obj->offset);
    auto log_suffix =
        fmt::format("group={} lsn={} shard=0x{:x} batch_num={} size={}", uuids::to_string(r_dev->group_id()),
                    context->get_lsn(), obj_id.shard_seq_num, obj_id.batch_id, snp_obj->blob.size());
    LOGI("Received snapshot obj, {}", log_suffix);

    if (snp_obj->is_last_obj) {
        LOGD("Write snapshot reached is_last_obj true {}", log_suffix);
        set_snapshot_context(context); // Update the snapshot context in case apply_snapshot is not called
        auto hs_pg = home_object_->get_hs_pg(m_snp_rcv_handler->get_context_pg_id());
        hs_pg->pg_state_.clear_state(PGStateMask::BASELINE_RESYNC);
        return;
    }

    // Check message integrity
#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("state_machine_write_corrupted_data")) {
        LOGW("Simulating writing corrupted snapshot data, lsn={}, obj_id={} shard 0x{:x} batch={}", context->get_lsn(),
             obj_id.value, obj_id.shard_seq_num, obj_id.batch_id);
        return;
    }
#endif
    if (snp_obj->blob.size() < sizeof(SyncMessageHeader)) {
        LOGE("invalid snapshot message size {} in write_snapshot_data, lsn={}, obj_id={} shard 0x{:x} batch={}",
             snp_obj->blob.size(), context->get_lsn(), obj_id.value, obj_id.shard_seq_num, obj_id.batch_id);
        return;
    }
    auto header = r_cast< const SyncMessageHeader* >(snp_obj->blob.cbytes());
    if (header->corrupted()) {
        LOGE("corrupted message in write_snapshot_data, lsn={}, obj_id={} shard 0x{:x} batch={}", context->get_lsn(),
             obj_id.value, obj_id.shard_seq_num, obj_id.batch_id);
        return;
    }
    if (auto payload_size = snp_obj->blob.size() - sizeof(SyncMessageHeader); payload_size != header->payload_size) {
        LOGE("payload size mismatch in write_snapshot_data {} != {}, lsn={}, obj_id={} shard 0x{:x} batch={}",
             payload_size, header->payload_size, context->get_lsn(), obj_id.value, obj_id.shard_seq_num,
             obj_id.batch_id);
        return;
    }
    auto data_buf = snp_obj->blob.cbytes() + sizeof(SyncMessageHeader);

    // Case 1: PG metadata & shard list message
    if (obj_id.shard_seq_num == 0) {
        RELEASE_ASSERT(obj_id.batch_id == 0, "Invalid obj_id");

        auto pg_data = GetSizePrefixedResyncPGMetaData(data_buf);

        if (m_snp_rcv_handler->get_context_lsn() == context->get_lsn() && m_snp_rcv_handler->get_shard_cursor() != 0) {
            // Request to resume from the beginning of shard
            snp_obj->offset =
                m_snp_rcv_handler->get_shard_cursor() == HSHomeObject::SnapshotReceiveHandler::shard_list_end_marker
                ? LAST_OBJ_ID
                : objId(HSHomeObject::get_sequence_num_from_shard_id(m_snp_rcv_handler->get_shard_cursor()), 0).value;
            LOGI("Resume from previous context breakpoint, lsn={} pg={} next_shard:0x{:x}, shard_cursor:0x{:x}",
                 context->get_lsn(), pg_data->pg_id(), m_snp_rcv_handler->get_next_shard(),
                 m_snp_rcv_handler->get_shard_cursor());
            return;
        }

        // Init a new transmission
        // If PG already exists, clean the stale pg resources. Let's resync on a pristine base
        if (home_object_->pg_exists(pg_data->pg_id())) {
            LOGI("pg already exists, clean pg resources before snapshot, pg={} {}", pg_data->pg_id(), log_suffix);
            // Need to pause state machine before destroying the PG, if fail, let raft retry.
            if (!home_object_->pg_destroy(pg_data->pg_id(), true /* pause state machine */)) {
                LOGE("failed to destroy existing pg, let raft retry, pg={} {}", pg_data->pg_id(), log_suffix);
                return;
            }
        }
        LOGI("reset context from lsn={} to lsn={}", m_snp_rcv_handler->get_context_lsn(), context->get_lsn());
        m_snp_rcv_handler->reset_context_and_metrics(context->get_lsn(), pg_data->pg_id());

        auto ret = m_snp_rcv_handler->process_pg_snapshot_data(*pg_data);
        if (ret) {
            // Do not proceed, will request for resending the PG data
            LOGE("Failed to process PG snapshot data lsn={} obj_id={} shard 0x{:x} batch={}, err={}",
                 context->get_lsn(), obj_id.value, obj_id.shard_seq_num, obj_id.batch_id, ret);
            return;
        }
        snp_obj->offset =
            objId(HSHomeObject::get_sequence_num_from_shard_id(m_snp_rcv_handler->get_next_shard()), 0).value;
        LOGI("Write snapshot, processed PG data pg={} {}", pg_data->pg_id(), log_suffix);
        return;
    }

    // There can be obj id mismatch if the follower crashes and restarts immediately within the sync_ctx_timeout.
    // The leader will continue with the previous request, which could be the same message the follower received
    // before the crash or the next message. But anyway, all the follower needs is to simply resume from the
    // beginning of its shard cursor if it's not valid.
    if (!m_snp_rcv_handler->is_valid_obj_id(obj_id)) {
        if (m_snp_rcv_handler->get_shard_cursor() == HSHomeObject::SnapshotReceiveHandler::shard_list_end_marker) {
            snp_obj->offset = LAST_OBJ_ID;
            LOGW("Leader resending last batch , we already done. Setting offset to LAST_OBJ_ID", context->get_lsn(),
                 m_snp_rcv_handler->get_next_shard(), m_snp_rcv_handler->get_shard_cursor());
        } else if (m_snp_rcv_handler->get_shard_cursor() == HSHomeObject::SnapshotReceiveHandler::invalid_shard_id) {
            // Could happen if interrupted before the first shard is done
            snp_obj->offset = objId(0, 0).value;
            LOGW("No shard cursor found, resume from the beginning pg meta. lsn={}", context->get_lsn());
        } else {
            snp_obj->offset =
                objId(HSHomeObject::get_sequence_num_from_shard_id(m_snp_rcv_handler->get_shard_cursor()), 0).value;
            LOGW("Obj id not matching with the current shard/blob cursor, resume from previous context breakpoint, "
                 "lsn={} next_shard:0x{:x}, shard_cursor:0x{:x}",
                 context->get_lsn(), m_snp_rcv_handler->get_next_shard(), m_snp_rcv_handler->get_shard_cursor());
        }
        return;
    }

    RELEASE_ASSERT(m_snp_rcv_handler->get_context_lsn() == context->get_lsn(), "Snapshot context lsn not matching");

    // Case 2: Shard metadata message
    if (obj_id.batch_id == 0) {
        RELEASE_ASSERT(obj_id.shard_seq_num != 0, "Invalid obj_id");

        auto shard_data = GetSizePrefixedResyncShardMetaData(data_buf);
        auto ret = m_snp_rcv_handler->process_shard_snapshot_data(*shard_data);
        if (ret) {
            // Do not proceed, will request for resending the shard data
            LOGE("Failed to process shard snapshot data lsn={} obj_id={} shard 0x{:x} batch={}, err={}",
                 context->get_lsn(), obj_id.value, obj_id.shard_seq_num, obj_id.batch_id, ret);
            return;
        }
        // Request for the next batch
        snp_obj->offset = objId(obj_id.shard_seq_num, 1).value;
        LOGD("Write snapshot, processed shard data shard_seq_num:0x{:x} {}", obj_id.shard_seq_num, log_suffix);
        return;
    }

    // Case 3: Blob data message
    auto blob_batch = GetSizePrefixedResyncBlobDataBatch(data_buf);
    auto ret =
        m_snp_rcv_handler->process_blobs_snapshot_data(*blob_batch, obj_id.batch_id, blob_batch->is_last_batch());
    if (ret) {
        // Do not proceed, will request for resending the current blob batch
        LOGE("Failed to process blob snapshot data lsn={} obj_id={} shard 0x{:x} batch={}, err={}", context->get_lsn(),
             obj_id.value, obj_id.shard_seq_num, obj_id.batch_id, ret);
        return;
    }
    // Set next obj_id to fetch
    if (blob_batch->is_last_batch()) {
        auto next_shard = m_snp_rcv_handler->get_next_shard();
        if (next_shard == HSHomeObject::SnapshotReceiveHandler::shard_list_end_marker) {
            snp_obj->offset = LAST_OBJ_ID;
        } else {
            snp_obj->offset = objId(HSHomeObject::get_sequence_num_from_shard_id(next_shard), 0).value;
        }
    } else {
        snp_obj->offset = objId(obj_id.shard_seq_num, obj_id.batch_id + 1).value;
    }

    LOGD("Write snapshot, processed blob data shard_seq_num:0x{:x} batch_num={} {}", obj_id.shard_seq_num,
         obj_id.batch_id, log_suffix);
}

void ReplicationStateMachine::free_user_snp_ctx(void*& user_snp_ctx) {
    if (user_snp_ctx == nullptr) {
        LOGE("User snapshot context null group={}", boost::uuids::to_string(repl_dev()->group_id()));
        return;
    }
    std::lock_guard lk(m_snp_sync_ctx_lock);
    auto pg_iter_ptr = static_cast< std::shared_ptr< HSHomeObject::PGBlobIterator >* >(user_snp_ctx);
    LOGD("Freeing snapshot iterator={}, pg={} group={}", user_snp_ctx, (*pg_iter_ptr)->pg_id,
         boost::uuids::to_string((*pg_iter_ptr)->group_id));
    pg_iter_ptr->get()->stop();
    delete pg_iter_ptr;
    user_snp_ctx = nullptr;
}

std::shared_ptr< homestore::snapshot_context > ReplicationStateMachine::get_snapshot_context() {
    if (m_snapshot_context == nullptr) {
        // Try to load from snapshot superblk first
        auto sb_data = home_object_->get_snapshot_sb_data(repl_dev()->group_id());
        if (sb_data.size() > 0) {
            m_snapshot_context = repl_dev()->deserialize_snapshot_context(sb_data);
            LOGI("Loaded previous snapshot from superblk, lsn={}", m_snapshot_context->get_lsn());
        }
    }
    return m_snapshot_context;
}

void ReplicationStateMachine::set_snapshot_context(std::shared_ptr< homestore::snapshot_context > context) {
    home_object_->update_snapshot_sb(repl_dev()->group_id(), context);
    m_snapshot_context = context;
}

folly::Future< std::error_code > ReplicationStateMachine::on_fetch_data(const int64_t lsn, const sisl::blob& header,
                                                                        const homestore::MultiBlkId& local_blk_id,
                                                                        sisl::sg_list& sgs) {
    if (0 == header.size()) {
        LOGW("Header is empty in on_fetch_data for lsn {}", lsn);
        return folly::makeFuture< std::error_code >(std::make_error_code(std::errc::invalid_argument));
    }

    // the lsn here will mostly be -1 ,since this lsn has not been appeneded and thus get no lsn
    // however, there is a corner case that fetch_data happens after push_data is received and log is appended. in
    // this case, lsn will be the corresponding lsn.

    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());

    if (msg_header->corrupted()) {
        LOGW("replication message header is corrupted with crc error, lsn={}, header={}", lsn, msg_header->to_string());
        return folly::makeFuture< std::error_code >(std::make_error_code(std::errc::bad_message));
    }

    LOGD("fetch data with lsn={}, msg type={}", lsn, msg_header->msg_type);

    // for nuobject case, we can make this assumption, since we use append_blk_allocator.
    RELEASE_ASSERT(sgs.iovs.size() == 1, "sgs iovs size should be 1, lsn={}, msg_type={}", lsn, msg_header->msg_type);

    auto const total_size = local_blk_id.blk_count() * repl_dev()->get_blk_size();
    RELEASE_ASSERT(total_size == sgs.size,
                   "total_blk_size does not match, lsn={}, msg_type={}, expected size={}, given buffer size={}", lsn,
                   msg_header->msg_type, total_size, sgs.size);

    auto given_buffer = (uint8_t*)(sgs.iovs[0].iov_base);
    std::memset(given_buffer, 0, total_size);

    // in homeobject, we have three kinds of requests that will write data(thus fetch_data might happen) to a
    // chunk:
    // 1 create_shard : will write a shard header to a chunk
    // 2 seal_shard : will write a shard footer to a chunk
    // 3 put_blob: will write user data to a chunk

    // for any type that writes data to a chunk, we need to handle the fetch_data request for it.

    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG:
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        // this function only returns data, not care about raft related logic, so no need to check the existence of
        // shard, just return the shard header/footer directly. Also, no need to read the data from disk, generate
        // it from Header.
        auto sb =
            r_cast< HSHomeObject::shard_info_superblk const* >(header.cbytes() + sizeof(ReplicationMessageHeader));
        auto const raw_size = sizeof(HSHomeObject::shard_info_superblk);
        auto const expected_size = sisl::round_up(raw_size, repl_dev()->get_blk_size());

        RELEASE_ASSERT(
            sgs.size == expected_size,
            "shard metadata size does not match, lsn={}, msg_type={}, expected size={}, given buffer size={}", lsn,
            msg_header->msg_type, expected_size, sgs.size);

        // TODO：：return error_code if assert fails, so it will not crash here because of the assert failure.
        std::memcpy(given_buffer, sb, raw_size);
        return folly::makeFuture< std::error_code >(std::error_code{});
    }

        // TODO: for shard header and footer, follower can generate it itself according to header, no need to fetch
        // it from leader. this can been done by adding another callback, which will be called before follower tries
        // to fetch data.

    case ReplicationMessageType::PUT_BLOB_MSG: {

        const auto blob_id = msg_header->blob_id;
        const auto shard_id = msg_header->shard_id;

        LOGD("fetch data with blob_id={}, shard=0x{:x}", blob_id, shard_id);
        // we first try to read data according to the local_blk_id to see if it matches the blob_id
        return std::move(homestore::data_service().async_read(local_blk_id, given_buffer, total_size))
            .thenValue([this, lsn, blob_id, shard_id, given_buffer, total_size](auto&& err) {
                // io error
                if (err) {
                    LOGE("FetchData fails to read blob, lsn={}, blob_id={}, shard_id={}, err_value={}, error={}", lsn,
                         blob_id, shard_id, err.value(), err.message());
                    throw std::system_error(err);
                }

                // folly future has no machenism to bypass the later thenValue in the then value chain. so for all
                // the case that no need to schedule the later async_read, we throw a system_error with no error
                // code to bypass the next thenValue.

                // if data matches
                if (home_object_->verify_blob(given_buffer, shard_id, blob_id)) {
                    LOGD("local_blk_id matches blob data, lsn={}, blob_id={}, shard_id={}", lsn, blob_id, shard_id);
                    throw std::system_error(std::error_code{});
                }

                // if data does not match, try to read data according to the index table. this might happen if the
                // chunk has once been gc.
                pg_id_t pg_id = shard_id >> homeobject::shard_width;
                auto hs_pg = home_object_->get_hs_pg(pg_id);
                if (!hs_pg) {
                    LOGE("pg not found for pg={}, shardID=0x{:x}", pg_id, shard_id);
                    // TODO: use a proper error code.
                    throw std::system_error(std::make_error_code(std::errc::bad_address));
                }
                const auto& index_table = hs_pg->index_table_;

                BlobRouteKey index_key{BlobRoute{shard_id, blob_id}};
                BlobRouteValue index_value;
                homestore::BtreeSingleGetRequest get_req{&index_key, &index_value};

                LOGD("fetch data with blob_id={}, shardID=0x{:x}, pg={} from index table", blob_id, shard_id, pg_id);

                auto rc = index_table->get(get_req);
                if (sisl_unlikely(homestore::btree_status_t::success != rc)) {
                    // blob never exists or has been gc
                    LOGD("on_fetch_data failed to get from index table, blob never exists or has been gc, blob_id={}, "
                         "shardID=0x{:x}, pg={}",
                         blob_id, shard_id, pg_id);

                    // we return a specific blob as a delete marker if not found in index table
                    std::memset(given_buffer, 0, total_size);
                    RELEASE_ASSERT(HSHomeObject::delete_marker_blob_data.size() <= total_size,
                                   "delete marker blob size is larger than total_size");
                    std::memcpy(given_buffer, HSHomeObject::delete_marker_blob_data.data(),
                                HSHomeObject::delete_marker_blob_data.size());

                    throw std::system_error(std::error_code{});
                }

                const auto& pbas = index_value.pbas();
                if (sisl_unlikely(pbas == HSHomeObject::tombstone_pbas)) {
                    LOGD("on_fetch_data: blob has been deleted, blob_id={}, shardID=0x{:x}, pg={}", blob_id, shard_id,
                         pg_id);

                    // we return a specific blob as a delete marker for tombstone pbas
                    std::memset(given_buffer, 0, total_size);
                    RELEASE_ASSERT(HSHomeObject::delete_marker_blob_data.size() <= total_size,
                                   "delete marker blob size is larger than total_size");
                    std::memcpy(given_buffer, HSHomeObject::delete_marker_blob_data.data(),
                                HSHomeObject::delete_marker_blob_data.size());

                    throw std::system_error(std::error_code{});
                }

                RELEASE_ASSERT(pbas.blk_count() * repl_dev()->get_blk_size() == total_size,
                               "pbas blk size does not match total_size");

                LOGD("on_fetch_data: read data with blob_id={}, shardID=0x{:x}, pg={} from pbas={}", blob_id, shard_id,
                     pg_id, pbas.to_string());

                return homestore::data_service().async_read(pbas, given_buffer, total_size);
            })
            .thenValue([this, lsn, blob_id, shard_id, given_buffer, total_size](auto&& err) {
                // io error
                if (err) throw std::system_error(err);
                // if data matches
                if (home_object_->verify_blob(given_buffer, shard_id, blob_id)) {
                    LOGD("pba matches blob data, lsn={}, blob_id={}, shardID=0x{:x}, pg={}", lsn, blob_id, shard_id,
                         (shard_id >> homeobject::shard_width));
                    return std::error_code{};
                } else {
                    // there is a scenario that the chunk is gced after we get the pba, but before we schecdule
                    // the read. we can try to read the index table and read data again, but for the simlicity
                    // here, we just return error, and let follower to retry fetch data.
                    return std::make_error_code(std::errc::resource_unavailable_try_again);
                }
            })
            .thenError< std::system_error >([blob_id, shard_id](const std::system_error& e) {
                auto ec = e.code();

                if (!ec) {
                    // if no error code, we come to here, which means the data is valid or no need to read data
                    // again.
                    LOGD("blob valid, blob_id={}, shardID=0x{:x}, pg={}", blob_id, shard_id,
                         (shard_id >> homeobject::shard_width));
                } else {
                    // if any error happens, we come to here
                    LOGE("IO error happens when reading data for blob_id={}, shardID=0x{:x}, pg={}, error={}", blob_id,
                         shard_id, (shard_id >> homeobject::shard_width), e.what());
                }

                return ec;
            });
    }
    default: {
        LOGW("msg type={}, should not happen in fetch_data rpc", msg_header->msg_type);
        return folly::makeFuture< std::error_code >(std::make_error_code(std::errc::operation_not_supported));
    }
    }
}

sisl::io_blob_safe HSHomeObject::get_snapshot_sb_data(homestore::group_id_t group_id) {
    std::shared_lock lk(snp_sbs_lock_);
    auto it = snp_ctx_sbs_.find(group_id);
    if (it == snp_ctx_sbs_.end()) {
        LOGD("Snapshot context superblk not found for group_id={}", group_id);
        return {};
    }

    auto sb_data = sisl::io_blob_safe(it->second->data_size);
    std::copy_n(it->second->data, it->second->data_size, reinterpret_cast< char* >(sb_data.bytes()));
    return sb_data;
}

void HSHomeObject::update_snapshot_sb(homestore::group_id_t group_id,
                                      std::shared_ptr< homestore::snapshot_context > ctx) {
    std::unique_lock lk(snp_sbs_lock_);
    auto it = snp_ctx_sbs_.find(group_id);
    if (it != snp_ctx_sbs_.end()) {
        LOGD("Existing snapshot context superblk destroyed for group_id={}, lsn={}", group_id, it->second->lsn);
        it->second.destroy();
    } else {
        it = snp_ctx_sbs_.insert({group_id, homestore::superblk< snapshot_ctx_superblk >(_snp_ctx_meta_name)}).first;
    }
    auto data = ctx->serialize();
    it->second.create(sizeof(snapshot_ctx_superblk) - sizeof(char) + data.size());
    it->second->group_id = group_id;
    it->second->lsn = ctx->get_lsn();
    it->second->data_size = data.size();
    std::copy_n(data.cbytes(), data.size(), it->second->data);
    it->second.write();
    LOGI("Snapshot context superblk updated for group_id={}, lsn={}", group_id, ctx->get_lsn());
}

void HSHomeObject::destroy_snapshot_sb(homestore::group_id_t group_id) {
    std::unique_lock lk(snp_sbs_lock_);
    auto it = snp_ctx_sbs_.find(group_id);
    if (it == snp_ctx_sbs_.end()) {
        LOGI("Snapshot context superblk not found for group_id={}", group_id);
        return;
    }

    it->second.destroy();
    snp_ctx_sbs_.erase(it);
    LOGI("Snapshot context superblk destroyed for group_id={}", group_id);
}

void HSHomeObject::on_snp_ctx_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf) {
    LOGI("Found snapshot context meta blk");
    homestore::superblk< snapshot_ctx_superblk > sb(_snp_ctx_meta_name);
    sb.load(buf, mblk);

    std::unique_lock lk(snp_sbs_lock_);
    if (auto it = snp_ctx_sbs_.find(sb->group_id); it != snp_ctx_sbs_.end()) {
        LOGWARN("Found duplicate snapshot context superblk for group_id={}, current lsn={}, existing lsn={}",
                sb->group_id, sb->lsn, it->second->lsn);
        if (it->second->lsn <= sb->lsn) {
            LOGI("Replacing existing snapshot context superblk with new one");
            it->second.destroy();
        } else {
            LOGI("Keeping existing snapshot context superblk");
            return;
        }
    }
    snp_ctx_sbs_[sb->group_id] = std::move(sb);
}

void HSHomeObject::on_snp_ctx_meta_blk_recover_completed(bool success) {
    LOGI("Snapshot context meta blk recover completed");
}

void ReplicationStateMachine::on_no_space_left(homestore::repl_lsn_t lsn, sisl::blob const& header) {
    homestore::chunk_num_t chunk_id{0};
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());

    if (sisl_unlikely(msg_header->corrupted())) {
        LOGE("shardID=0x{:x}, pg={}, shard=0x{:x}, replication message header is corrupted with crc error when "
             "handling on_no_space_left",
             msg_header->shard_id, (msg_header->shard_id >> homeobject::shard_width),
             (msg_header->shard_id & homeobject::shard_mask));
    } else {
        const pg_id_t pg_id = msg_header->pg_id;

        switch (msg_header->msg_type) {
        // this case is only that no_space_left happens when writing shard header block on follower side.
        case ReplicationMessageType::CREATE_SHARD_MSG: {
            if (!home_object_->pg_exists(pg_id)) {
                LOGW("shardID=0x{:x}, shard=0x{:x}, can not find pg={} when handling on_no_space_left",
                     msg_header->shard_id, (msg_header->shard_id & homeobject::shard_mask), pg_id);
            }
            auto v_chunkID = home_object_->resolve_v_chunk_id_from_msg(header);
            if (!v_chunkID.has_value()) {
                LOGW("shardID=0x{:x}, pg={}, shard=0x{:x}, can not resolve v_chunk_id from msg", msg_header->shard_id,
                     pg_id, (msg_header->shard_id & homeobject::shard_mask));
            } else {
                chunk_id = home_object_->chunk_selector()->get_pg_vchunk(pg_id, v_chunkID.value())->get_chunk_id();
            }

            break;
        }

        case ReplicationMessageType::SEAL_SHARD_MSG:
        case ReplicationMessageType::PUT_BLOB_MSG: {
            auto p_chunkID = home_object_->get_shard_p_chunk_id(msg_header->shard_id);
            if (!p_chunkID.has_value()) {
                LOGW("shardID=0x{:x}, pg={}, shard=0x{:x}, shard does not exist when handling on_no_space_left, "
                     "underlying "
                     "engine will retry this later",
                     msg_header->shard_id, pg_id, (msg_header->shard_id & homeobject::shard_mask));
            } else {
                chunk_id = p_chunkID.value();
            }

            break;
        }

        default: {
            LOGW("not support msg type for {} in handling on_no_space_left", msg_header->msg_type);
        }
        }
    }

    if (0 == chunk_id) {
        LOGW("can not get a valid chunk_id, skip handling on_no_space_left for lsn={}", lsn);
        return;
    }

    LOGD("got no_space_left error at lsn={}, chunk_id={}", lsn, chunk_id);

    const auto [target_lsn, error_chunk_id] = get_no_space_left_error_info();

    RELEASE_ASSERT(lsn - 1 <= target_lsn,
                   "new target lsn should be less than or equal to the existing target "
                   "lsn, new_target_lsn={}, existing_target_lsn={}",
                   lsn - 1, target_lsn);

    // set a new error info or overwrite an existing error info, postpone handling this error until lsn - 1 is
    // committed.
    LOGD("set no_space_left error info with lsn={}, chunk_id={}, existing error info: lsn={}, chunk_id={}", lsn - 1,
         chunk_id, target_lsn, error_chunk_id);

    // setting the same error info is ok.
    set_no_space_left_error_info(lsn - 1, chunk_id);
}

void ReplicationStateMachine::set_no_space_left_error_info(homestore::repl_lsn_t lsn, homestore::chunk_num_t chunk_id) {
    std::unique_lock lk(m_no_space_left_error_info.mutex);
    m_no_space_left_error_info.wait_commit_lsn = lsn;
    m_no_space_left_error_info.chunk_id = chunk_id;
}

void ReplicationStateMachine::reset_no_space_left_error_info() {
    std::unique_lock lk(m_no_space_left_error_info.mutex);
    m_no_space_left_error_info.wait_commit_lsn = std::numeric_limits< homestore::repl_lsn_t >::max();
    m_no_space_left_error_info.chunk_id = 0;
}

std::pair< homestore::repl_lsn_t, homestore::chunk_num_t >
ReplicationStateMachine::get_no_space_left_error_info() const {
    std::shared_lock lk(m_no_space_left_error_info.mutex);
    return {m_no_space_left_error_info.wait_commit_lsn, m_no_space_left_error_info.chunk_id};
}

void ReplicationStateMachine::handle_no_space_left(homestore::repl_lsn_t lsn, homestore::chunk_num_t chunk_id) {
    LOGW("start handling no_space_left error for chunk_id={} , lsn={}", chunk_id, lsn);
    // 1 drain all the pending requests and refuse later coming new requests for repl_dev, so that no new block can
    // be allocated from now on.
    repl_dev()->quiesce_reqs();

    // 2 clear all the in-memory rreqs that already allocated blocks on the chunk.
    repl_dev()->clear_chunk_req(chunk_id);

    // 3 handling this error. for homeobject, we will submit an emergent gc task and wait for the completion.
    auto gc_mgr = home_object_->gc_manager();
    gc_mgr->submit_gc_task(task_priority::emergent, chunk_id)
        .via(&folly::InlineExecutor::instance())
        .thenValue([this, lsn, chunk_id](auto&& res) {
            if (!res) {
                LOGERROR("failed to submit emergent gc task for chunk_id={} , lsn={}, will retry again if new "
                         "no_space_left happens",
                         chunk_id, lsn);
            } else {
                LOGD("successfully handle no_space_left error for chunk_id={} , lsn={}", chunk_id, lsn);
            }

            // start accepting new requests again.
            repl_dev()->resume_accepting_reqs();
        });
}

void ReplicationStateMachine::on_log_replay_done(const homestore::group_id_t& group_id) {
    // when we reaching here, it means all the logs of this group has been replayed, but we don`t join the raft group
    // ATM and thus no new request can be handled. Now, we can safely mark all the chunks with open shard in this group
    // to inuse state, so that gc can not gc this chunk.

    // we must do this job here, because:

    // 1 we should change the chunk state after all the logs are replayed. since if we mark the chunk state to inuse
    // before all the logs are replayed, there is a case that the chunk will be released by the last seal_shard, which
    // is prior to the lastest open_shard of this chunk. for example: lsn 10 is create_shard_1 ,lsn 20 is seal_shard_1
    // and lsn 30 is create_shard_2. shard1 and shard 2 are in the same chunk. now if we mark the chunk to inuse before
    // replay lsn 20, then when replaying lsn 20, the chunk will be released by lsn 20(commit_seal_shard). and when we
    // completed replay, the final state of this chunk is available. but what we want is inuse, since shard_2 is opened
    // in lsn 30 and not sealed.

    // 2 if we mark the chunk state to in use after the repl_dev join the raft group, then it can accept new request or
    // can be gc before we mark it to inuse.

    // so this is the only timepoint we should make all the chunks, which contains open shards, to inuse state.

    auto pg_id_opt = home_object_->get_pg_id_with_group_id(group_id);
    if (!pg_id_opt.has_value()) {
        // there are two cases we might reach here.

        //  1 when baseline resync, crash happens after pg is destroyed but before new pg is created in follower. when
        //  recovery, we will find we have a group , but the corresponding pg does not exist.

        //  2 when replacememeber, we will destroy the repl_dev after destroying the pg. if crash happens after pg is
        //  destroyed but before the repl_dev superblk is destroyed, then when recovery, we will find we have a
        //  group(repl_dev), but have not corresponding pg.

        //  3 when fail to create repl_dev, there might be some stale repl_dev left on a node. for example, success to
        //  add the first member but fail to add the second member, the there will be a stale repl_dev on leader and
        //  the first member. see https://github.com/eBay/HomeObject/pull/136#discussion_r1470504271
        LOGW("can not find any pg for group={}!", group_id);
        return;
    }

    const auto pg_id = pg_id_opt.value();
    RELEASE_ASSERT(home_object_->pg_exists(pg_id), "pg={} should exist, but not! fatal error!", pg_id);

    const auto& shards_in_pg = (const_cast< HSHomeObject::HS_PG* >(home_object_->_get_hs_pg_unlocked(pg_id)))->shards_;
    auto chunk_selector = home_object_->chunk_selector();

    for (const auto& shard_iter : shards_in_pg) {
        const auto& shard_sb = ((d_cast< HSHomeObject::HS_Shard* >(shard_iter.get()))->sb_).get();
        if (shard_sb->info.is_open()) {
            const auto pg_id = shard_sb->info.placement_group;
            const auto vchunk_id = shard_sb->v_chunk_id;
            auto chunk = chunk_selector->select_specific_chunk(pg_id, vchunk_id);
            RELEASE_ASSERT(chunk != nullptr, "chunk selection failed with v_chunk_id={} in pg={}", vchunk_id, pg_id);
            LOGD("vchunk={} is selected for shard={} in pg={} when recovery", vchunk_id, shard_sb->info.id, pg_id);
        }
    }
}

} // namespace homeobject
