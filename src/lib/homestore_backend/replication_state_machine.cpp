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
                                        const homestore::MultiBlkId& pbas, cintrusive< homestore::repl_req_ctx >& ctx) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    LOGD("applying raft log commit with lsn:{}, msg type: {}", lsn, msg_header->msg_type);
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_PG_MSG: {
        home_object_->on_create_pg_message_commit(lsn, header, repl_dev(), ctx);
        break;
    }
    case ReplicationMessageType::CREATE_SHARD_MSG:
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        home_object_->on_shard_message_commit(lsn, header, pbas, repl_dev(), ctx);
        break;
    }

    case ReplicationMessageType::PUT_BLOB_MSG: {
        home_object_->on_blob_put_commit(lsn, header, key, pbas, ctx);
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

bool ReplicationStateMachine::on_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                            cintrusive< homestore::repl_req_ctx >& ctx) {
    // For shard creation, since homestore repldev inside will write shard header to data service first before this
    // function is called. So there is nothing is needed to do and we can get the binding chunk_id with the newly shard
    // from the blkid in on_commit()
    if (ctx->op_code() == homestore::journal_type_t::HS_CTRL_REPLACE) {
        LOGI("pre_commit replace member log entry, lsn:{}", lsn);
        return true;
    }

    if (ctx->op_code() == homestore::journal_type_t::HS_CTRL_DESTROY) {
        LOGI("pre_commit destroy member log entry, lsn:{}", lsn);
        return true;
    }

    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGE("corrupted message in pre_commit, lsn:{}", lsn);
        return false;
    }
    LOGD("on_pre_commit with lsn:{}, msg type: {}", lsn, msg_header->msg_type);
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
    LOGI("on_rollback  with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGE("corrupted message in rollback, lsn:{}", lsn);
        return;
    }
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        home_object_->on_shard_message_rollback(lsn, header, key, ctx);
        break;
    }
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        home_object_->on_shard_message_rollback(lsn, header, key, ctx);
        break;
    }
    default: {
        break;
    }
    }
}

void ReplicationStateMachine::on_restart() { LOGD("ReplicationStateMachine::on_restart"); }

void ReplicationStateMachine::on_error(ReplServiceError error, const sisl::blob& header, const sisl::blob& key,
                                       cintrusive< repl_req_ctx >& ctx) {
    RELEASE_ASSERT(ctx, "ctx should not be nullptr in on_error");
    RELEASE_ASSERT(ctx->is_proposer(), "on_error should only be called from proposer");
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    LOGE("on_error, message type {} with lsn {}, error {}", msg_header->msg_type, ctx->lsn(), error);
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

    case ReplicationMessageType::PUT_BLOB_MSG: {
        auto result_ctx =
            boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< HSHomeObject::BlobInfo > > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(toBlobError(error)));
        break;
    }
    case ReplicationMessageType::DEL_BLOB_MSG: {
        auto result_ctx =
            boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< HSHomeObject::BlobInfo > > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(toBlobError(error)));
        break;
    }
    default: {
        LOGE("Unknown message type, error unhandled , error :{}, lsn {}", error, ctx->lsn());
        break;
    }
    }
}

homestore::ReplResult< homestore::blk_alloc_hints >
ReplicationStateMachine::get_blk_alloc_hints(sisl::blob const& header, uint32_t data_size) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        pg_id_t pg_id = msg_header->pg_id;
        // check whether the pg exists
        if (!home_object_->pg_exists(pg_id)) {
            LOGI("can not find pg {} when getting blk_alloc_hint", pg_id);
            // TODO:: add error code to indicate the pg not found in homestore side
            return folly::makeUnexpected(homestore::ReplServiceError::NO_SPACE_LEFT);
        }

        auto v_chunkID = home_object_->resolve_v_chunk_id_from_msg(header);
        if (!v_chunkID.has_value()) {
            LOGW("can not resolve v_chunk_id from msg");
            return folly::makeUnexpected(homestore::ReplServiceError::FAILED);
        }
        homestore::blk_alloc_hints hints;
        // Both chunk_num_t and pg_id_t are of type uint16_t.
        static_assert(std::is_same< pg_id_t, uint16_t >::value, "pg_id_t is not uint16_t");
        static_assert(std::is_same< homestore::chunk_num_t, uint16_t >::value, "chunk_num_t is not uint16_t");
        homestore::chunk_num_t v_chunk_id = v_chunkID.value();
        hints.application_hint = ((uint64_t)pg_id << 16) | v_chunk_id;
        return hints;
    }

    case ReplicationMessageType::SEAL_SHARD_MSG: {
        auto p_chunkID = home_object_->get_shard_p_chunk_id(msg_header->shard_id);
        if (!p_chunkID.has_value()) {
            LOGW("shard does not exist, underlying engine will retry this later", msg_header->shard_id);
            return folly::makeUnexpected(homestore::ReplServiceError::RESULT_NOT_EXIST_YET);
        }
        homestore::blk_alloc_hints hints;
        hints.chunk_id_hint = p_chunkID.value();
        return hints;
    }

    case ReplicationMessageType::PUT_BLOB_MSG:
        return home_object_->blob_put_get_blk_alloc_hints(header, nullptr);

    case ReplicationMessageType::DEL_BLOB_MSG:
    default:
        break;
    }

    return homestore::blk_alloc_hints();
}

void ReplicationStateMachine::on_replace_member(const homestore::replica_member_info& member_out,
                                                const homestore::replica_member_info& member_in) {
    home_object_->on_pg_replace_member(repl_dev()->group_id(), member_out, member_in);
}

void ReplicationStateMachine::on_destroy(const homestore::group_id_t& group_id) {
    auto PG_ID = home_object_->get_pg_id_with_group_id(group_id);
    if (!PG_ID.has_value()) {
        LOGW("do not have pg mapped by group_id {}", boost::uuids::to_string(group_id));
        return;
    }
    home_object_->pg_destroy(PG_ID.value());
    LOGI("replica destroyed, cleared PG {} resources with group_id {}", PG_ID.value(),
         boost::uuids::to_string(group_id));
}

homestore::AsyncReplResult<>
ReplicationStateMachine::create_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    std::lock_guard lk(m_snapshot_lock);
    if (get_snapshot_context() != nullptr && context->get_lsn() < m_snapshot_context->get_lsn()) {
        LOGI("Skipping create snapshot, new snapshot lsn: {} is less than current snapshot lsn: {}", context->get_lsn(),
             m_snapshot_context->get_lsn());
        return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
    }

    LOGI("create snapshot with lsn: {}", context->get_lsn());
    set_snapshot_context(context);
    return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
}

bool ReplicationStateMachine::apply_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
#ifdef _PRERELEASE
    auto delay = iomgr_flip::instance()->get_test_flip< long >("simulate_apply_snapshot_delay");
    LOGD("simulate_apply_snapshot_delay flip, triggered: {}", delay.has_value());
    if (delay) {
        LOGI("Simulating apply snapshot with delay, delay:{}", delay.get());
        std::this_thread::sleep_for(std::chrono::milliseconds(delay.get()));
    }
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
    HSHomeObject::PGBlobIterator* pg_iter = nullptr;

    if (snp_obj->user_ctx == nullptr) {
        // Create the pg blob iterator for the first time.
        pg_iter = new HSHomeObject::PGBlobIterator(*home_object_, repl_dev()->group_id(), context->get_lsn());
        snp_obj->user_ctx = (void*)pg_iter;
        LOGD("Allocated new pg blob iterator {}, group={}, lsn={}", static_cast< void* >(pg_iter),
             boost::uuids::to_string(repl_dev()->group_id()), context->get_lsn());
    } else {
        pg_iter = r_cast< HSHomeObject::PGBlobIterator* >(snp_obj->user_ctx);
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
    auto log_str = fmt::format("group={}, lsn={},", uuids::to_string(repl_dev()->group_id()), context->get_lsn());
    if (snp_obj->offset == LAST_OBJ_ID) {
        // No more shards to read, baseline resync is finished after this.
        snp_obj->is_last_obj = true;
        LOGD("Read snapshot end, {}", log_str);
        return 0;
    }

    auto obj_id = objId(snp_obj->offset);
    log_str = fmt::format("{} shard_seq_num={} batch_num={}", log_str, obj_id.shard_seq_num, obj_id.batch_id);

    LOGD("read current snp obj {}", log_str)
    // invalid Id
    if (!pg_iter->update_cursor(obj_id)) {
        LOGW("Invalid objId in snapshot read, {}, current shard_seq_num={}, current batch_num={}", log_str,
             pg_iter->cur_obj_id_.shard_seq_num, pg_iter->cur_obj_id_.batch_id);
        return -1;
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
            LOGI("Reloaded previous snapshot context, lsn:{} pg_id:{} next_shard:{}", context->get_lsn(),
                 m_snp_rcv_handler->get_context_pg_id(), m_snp_rcv_handler->get_next_shard());
        }
    }

    auto obj_id = objId(snp_obj->offset);
    auto log_suffix = fmt::format("group={} lsn={} shard={} batch_num={} size={}", uuids::to_string(r_dev->group_id()),
                                  context->get_lsn(), obj_id.shard_seq_num, obj_id.batch_id, snp_obj->blob.size());
    LOGI("received snapshot obj, {}", log_suffix);

    if (snp_obj->is_last_obj) {
        LOGD("Write snapshot reached is_last_obj true {}", log_suffix);
        set_snapshot_context(context); // Update the snapshot context in case apply_snapshot is not called
        return;
    }

    // Check message integrity
#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("state_machine_write_corrupted_data")) {
        LOGW("Simulating writing corrupted snapshot data, lsn:{}, obj_id {} shard {} batch {}", context->get_lsn(),
             obj_id.value, obj_id.shard_seq_num, obj_id.batch_id);
        return;
    }
#endif
    auto header = r_cast< const SyncMessageHeader* >(snp_obj->blob.cbytes());
    if (header->corrupted()) {
        LOGE("corrupted message in write_snapshot_data, lsn:{}, obj_id {} shard {} batch {}", context->get_lsn(),
             obj_id.value, obj_id.shard_seq_num, obj_id.batch_id);
        return;
    }
    if (auto payload_size = snp_obj->blob.size() - sizeof(SyncMessageHeader); payload_size != header->payload_size) {
        LOGE("payload size mismatch in write_snapshot_data {} != {}, lsn:{}, obj_id {} shard {} batch {}", payload_size,
             header->payload_size, context->get_lsn(), obj_id.value, obj_id.shard_seq_num, obj_id.batch_id);
        return;
    }
    auto data_buf = snp_obj->blob.cbytes() + sizeof(SyncMessageHeader);

    if (obj_id.shard_seq_num == 0) {
        // PG metadata & shard list message
        RELEASE_ASSERT(obj_id.batch_id == 0, "Invalid obj_id");

        auto pg_data = GetSizePrefixedResyncPGMetaData(data_buf);

        if (m_snp_rcv_handler->get_context_lsn() == context->get_lsn() && m_snp_rcv_handler->get_shard_cursor() != 0) {
            // Request to resume from the beginning of shard
            snp_obj->offset =
                m_snp_rcv_handler->get_shard_cursor() == HSHomeObject::SnapshotReceiveHandler::shard_list_end_marker
                ? LAST_OBJ_ID
                : objId(HSHomeObject::get_sequence_num_from_shard_id(m_snp_rcv_handler->get_shard_cursor()), 0).value;
            LOGI("Resume from previous context breakpoint, lsn:{} pg_id:{} next_shard:{}, shard_cursor:{}",
                 context->get_lsn(), pg_data->pg_id(), m_snp_rcv_handler->get_next_shard(),
                 m_snp_rcv_handler->get_shard_cursor());
            return;
        }

        // Init a new transmission
        // If PG already exists, clean the stale pg resources. Let's resync on a pristine base
        if (home_object_->pg_exists(pg_data->pg_id())) {
            LOGI("pg already exists, clean pg resources before snapshot, pg_id:{} {}", pg_data->pg_id(), log_suffix);
            home_object_->pg_destroy(pg_data->pg_id());
        }
        LOGI("reset context from lsn:{} to lsn:{}", m_snp_rcv_handler->get_context_lsn(), context->get_lsn());
        m_snp_rcv_handler->reset_context_and_metrics(context->get_lsn(), pg_data->pg_id());

        auto ret = m_snp_rcv_handler->process_pg_snapshot_data(*pg_data);
        if (ret) {
            // Do not proceed, will request for resending the PG data
            LOGE("Failed to process PG snapshot data lsn:{} obj_id {} shard {} batch {}, err {}", context->get_lsn(),
                 obj_id.value, obj_id.shard_seq_num, obj_id.batch_id, ret);
            return;
        }
        snp_obj->offset =
            objId(HSHomeObject::get_sequence_num_from_shard_id(m_snp_rcv_handler->get_next_shard()), 0).value;
        LOGD("Write snapshot, processed PG data pg_id:{} {}", pg_data->pg_id(), log_suffix);
        return;
    }

    RELEASE_ASSERT(m_snp_rcv_handler->get_context_lsn() == context->get_lsn(), "Snapshot context lsn not matching");

    if (obj_id.batch_id == 0) {
        // Shard metadata message
        RELEASE_ASSERT(obj_id.shard_seq_num != 0, "Invalid obj_id");

        auto shard_data = GetSizePrefixedResyncShardMetaData(data_buf);
        auto ret = m_snp_rcv_handler->process_shard_snapshot_data(*shard_data);
        if (ret) {
            // Do not proceed, will request for resending the shard data
            LOGE("Failed to process shard snapshot data lsn:{} obj_id {} shard {} batch {}, err {}", context->get_lsn(),
                 obj_id.value, obj_id.shard_seq_num, obj_id.batch_id, ret);
            return;
        }
        // Request for the next batch
        snp_obj->offset = objId(obj_id.shard_seq_num, 1).value;
        LOGD("Write snapshot, processed shard data shard_seq_num:{} {}", obj_id.shard_seq_num, log_suffix);
        return;
    }

    // Blob data message
    // TODO: enhance error handling for wrong shard id - what may cause this?
    RELEASE_ASSERT(obj_id.shard_seq_num ==
                       HSHomeObject::get_sequence_num_from_shard_id(m_snp_rcv_handler->get_shard_cursor()),
                   "Shard id not matching with the current shard cursor");
    auto blob_batch = GetSizePrefixedResyncBlobDataBatch(data_buf);
    auto ret =
        m_snp_rcv_handler->process_blobs_snapshot_data(*blob_batch, obj_id.batch_id, blob_batch->is_last_batch());
    if (ret) {
        // Do not proceed, will request for resending the current blob batch
        LOGE("Failed to process blob snapshot data lsn:{} obj_id {} shard {} batch {}, err {}", context->get_lsn(),
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

    LOGD("Write snapshot, processed blob data shard_seq_num:{} batch_num:{} {}", obj_id.shard_seq_num, obj_id.batch_id,
         log_suffix);
}

void ReplicationStateMachine::free_user_snp_ctx(void*& user_snp_ctx) {
    if (user_snp_ctx == nullptr) {
        LOGE("User snapshot context null group={}", boost::uuids::to_string(repl_dev()->group_id()));
        return;
    }

    auto pg_iter = r_cast< HSHomeObject::PGBlobIterator* >(user_snp_ctx);
    LOGD("Freeing snapshot iterator {}, pg_id={} group={}", static_cast< void* >(pg_iter), pg_iter->pg_id_,
         boost::uuids::to_string(pg_iter->group_id_));
    delete pg_iter;
    user_snp_ctx = nullptr;
}

std::shared_ptr< homestore::snapshot_context > ReplicationStateMachine::get_snapshot_context() {
    if (m_snapshot_context == nullptr) {
        // Try to load from snapshot superblk first
        auto sb_data = home_object_->get_snapshot_sb_data(repl_dev()->group_id());
        if (sb_data.size() > 0) {
            m_snapshot_context = repl_dev()->deserialize_snapshot_context(sb_data);
            LOGI("Loaded previous snapshot from superblk, lsn:{}", m_snapshot_context->get_lsn());
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
    // the lsn here will always be -1 ,since this lsn has not been appeneded and thus get no lsn

    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());

    if (msg_header->corrupted()) {
        LOGW("replication message header is corrupted with crc error, lsn:{}", lsn);
        return folly::makeFuture< std::error_code >(std::make_error_code(std::errc::bad_message));
    }

    LOGD("fetch data with lsn: {}, msg type: {}", lsn, msg_header->msg_type);

    // for nuobject case, we can make this assumption, since we use append_blk_allocator.
    RELEASE_ASSERT(sgs.iovs.size() == 1, "sgs iovs size should be 1, lsn: {}, msg_type: {}", lsn, msg_header->msg_type);

    auto const total_size = local_blk_id.blk_count() * repl_dev()->get_blk_size();
    RELEASE_ASSERT(total_size == sgs.size,
                   "total_blk_size does not match, lsn: {}, msg_type: {}, expected size {}, given buffer size {}", lsn,
                   msg_header->msg_type, total_size, sgs.size);

    auto given_buffer = (uint8_t*)(sgs.iovs[0].iov_base);

    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG:
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        // this function only returns data, not care about raft related logic, so no need to check the existence of
        // shard, just return the shard header/footer directly. Also, no need to read the data from disk, generate it
        // from Header.
        auto sb =
            r_cast< HSHomeObject::shard_info_superblk const* >(header.cbytes() + sizeof(ReplicationMessageHeader));

        auto const expected_size =
            sisl::round_up(sizeof(HSHomeObject::shard_info_superblk), repl_dev()->get_blk_size());

        RELEASE_ASSERT(
            sgs.size == expected_size,
            "shard metadata size does not match, lsn: {}, msg_type: {}, expected size {}, given buffer size {}", lsn,
            msg_header->msg_type, expected_size, sgs.size);

        // TODO：：return error_code if assert fails, so it will not crash here because of the assert failure.

        std::memcpy(given_buffer, sb, total_size);
        return folly::makeFuture< std::error_code >(std::error_code{});
    }

        // TODO: for shard header and footer, follower can generate it itself according to header, no need to fetch it
        // from leader. this can been done by adding another callback, which will be called before follower tries to
        // fetch data.

    case ReplicationMessageType::PUT_BLOB_MSG: {

        const auto blob_id = msg_header->blob_id;
        const auto shard_id = msg_header->shard_id;

        LOGD("fetch data with blob_id {}, shard_id {}", blob_id, shard_id);
        // we first try to read data according to the local_blk_id to see if it matches the blob_id
        return std::move(homestore::data_service().async_read(local_blk_id, given_buffer, total_size))
            .via(folly::getGlobalIOExecutor())
            .thenValue([this, lsn, blob_id, shard_id, given_buffer, total_size](auto&& err) {
                // io error
                if (err) throw std::system_error(err);
#ifdef _PRERELEASE
                if (iomgr_flip::instance()->test_flip("force_to_read_by_index_table")) {
                    LOGI("Simulating forcing to read by indextable");
                } else if (validate_blob(shard_id, blob_id, given_buffer, total_size)) {
                    LOGD("local_blk_id matches blob data, lsn: {}, blob_id: {}, shard_id: {}", lsn, blob_id, shard_id);
                    return std::error_code{};
                }
#else
                // if data matches
                if (validate_blob(shard_id, blob_id, given_buffer, total_size)) {
                    LOGD("local_blk_id matches blob data, lsn: {}, blob_id: {}, shard_id: {}", lsn, blob_id, shard_id);
                    return std::error_code{};
                }
#endif

                // if data does not match, try to read data according to the index table. this might happen if the chunk
                // has once been gc.
                pg_id_t pg_id = shard_id >> homeobject::shard_width;
                auto hs_pg = home_object_->get_hs_pg(pg_id);
                if (!hs_pg) {
                    LOGE("pg not found for pg_id: {}, shard_id: {}", pg_id, shard_id);
                    // TODO: use a proper error code.
                    throw std::system_error(std::make_error_code(std::errc::bad_address));
                }
                const auto& index_table = hs_pg->index_table_;

                BlobRouteKey index_key{BlobRoute{shard_id, blob_id}};
                BlobRouteValue index_value;
                homestore::BtreeSingleGetRequest get_req{&index_key, &index_value};

                LOGD("fetch data with blob_id {}, shard_id {} from index table", blob_id, shard_id);
                if (homestore::btree_status_t::success == index_table->get(get_req)) {
                    if (const auto& pbas = index_value.pbas(); pbas != HSHomeObject::tombstone_pbas) {
                        RELEASE_ASSERT(pbas.blk_count() * repl_dev()->get_blk_size() == total_size,
                                       "pbas blk size does not match total_size");
                        // blocking in an async call is good, since it will not block the caller thread
                        auto error = homestore::data_service().async_read(pbas, given_buffer, total_size).get();
                        // io error
                        if (error) throw std::system_error(err);
                        // if data matches
                        if (validate_blob(shard_id, blob_id, given_buffer, total_size)) {
                            LOGD("pba matches blob data, lsn: {}, blob_id: {}, shard_id: {}", lsn, blob_id, shard_id);
                            return std::error_code{};
                        } else {
                            // there is a scenario that the chunk is gced after we get the pba, but before we schecdule
                            // the read. we can try to read the index table and read data again, but for the simlicity
                            // here, we just return error, and let follower to retry fetch data.
                            throw std::system_error(std::make_error_code(std::errc::resource_unavailable_try_again));
                        }
                    } else {
                        // blob is deleted, so returning whatever data is good , since client will never read it.
                        LOGD("on_fetch_data: blob has been deleted, blob_id: {}, shard_id: {}", blob_id, shard_id);
                        return std::error_code{};
                    }
                } else {
                    // blob never exists or has been gc
                    LOGD("on_fetch_data failed to get from index table, blob never exists or has been gc, blob_id: {}, "
                         "shard_id: {}",
                         blob_id, shard_id);
                    // returning whatever data is good , since client will never read it.
                    return std::error_code{};
                }
            })
            .thenError< std::system_error >([blob_id, shard_id](const std::system_error& e) {
                // if any error happens, we come to here
                LOGE("IO error happens when reading data for blob_id: {}, shard_id: {}, error: {}", blob_id, shard_id,
                     e.what());
                return e.code();
            });
    }
    default: {
        LOGW("msg type: {}, should not happen in fetch_data rpc", msg_header->msg_type);
        return folly::makeFuture< std::error_code >(std::make_error_code(std::errc::operation_not_supported));
    }
    }
}

bool ReplicationStateMachine::validate_blob(shard_id_t shard_id, blob_id_t blob_id, void* data, size_t size) {
    auto const* header = r_cast< HSHomeObject::BlobHeader const* >(data);
    if (!header->valid()) {
        LOGD("blob header is invalid, blob_id: {}, shard_id: {}, size: {}", blob_id, shard_id, size);
        return false;
    }
    if (header->shard_id != shard_id) {
        LOGD("shard_id does not match , expected shard_id: {} , shard_id in header: {}", shard_id, header->shard_id);
        return false;
    }
    if (header->blob_id != blob_id) {
        LOGD("blob_id does not match , expected blob_id: {} , blob_id in header: {}", blob_id, header->blob_id);
        return false;
    }
    return true;
}

sisl::io_blob_safe HSHomeObject::get_snapshot_sb_data(homestore::group_id_t group_id) {
    std::shared_lock lk(snp_sbs_lock_);
    auto it = snp_ctx_sbs_.find(group_id);
    if (it == snp_ctx_sbs_.end()) {
        LOGD("Snapshot context superblk not found for group_id {}", group_id);
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
        LOGD("Existing snapshot context superblk destroyed for group_id {}, lsn {}", group_id, it->second->lsn);
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
    LOGI("Snapshot context superblk updated for group_id {}, lsn {}", group_id, ctx->get_lsn());
}

void HSHomeObject::destroy_snapshot_sb(homestore::group_id_t group_id) {
    std::unique_lock lk(snp_sbs_lock_);
    auto it = snp_ctx_sbs_.find(group_id);
    if (it == snp_ctx_sbs_.end()) {
        LOGI("Snapshot context superblk not found for group_id {}", group_id);
        return;
    }

    it->second.destroy();
    snp_ctx_sbs_.erase(it);
    LOGI("Snapshot context superblk destroyed for group_id {}", group_id);
}

void HSHomeObject::on_snp_ctx_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf) {
    LOGI("Found snapshot context meta blk");
    homestore::superblk< snapshot_ctx_superblk > sb(_snp_ctx_meta_name);
    sb.load(buf, mblk);

    std::unique_lock lk(snp_sbs_lock_);
    if (auto it = snp_ctx_sbs_.find(sb->group_id); it != snp_ctx_sbs_.end()) {
        LOGWARN("Found duplicate snapshot context superblk for group_id {}, current lsn {}, existing lsn {}",
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
} // namespace homeobject
