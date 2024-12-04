#include "replication_message.hpp"
#include "replication_state_machine.hpp"
#include "hs_backend_config.hpp"

#include "generated/resync_pg_data_generated.h"
#include "generated/resync_shard_data_generated.h"
#include "generated/resync_blob_data_generated.h"
#include <homestore/replication/repl_dev.h>
#include <homestore/replication/repl_decls.h>

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

void ReplicationStateMachine::on_restart() { LOGD("ReplicationStateMachine::on_restart");}

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
    // TODO:: add the logic to handle destroy
    LOGI("replica destroyed");
}

homestore::AsyncReplResult<>
ReplicationStateMachine::create_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    // TODO::add create snapshot logic
    auto ctx = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(context);
    auto s = ctx->nuraft_snapshot();

    std::lock_guard lk(m_snapshot_lock);
    if (m_snapshot_context != nullptr) {
        auto current = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(m_snapshot_context)->nuraft_snapshot();
        if (s->get_last_log_idx() < current->get_last_log_idx()) {
            LOGI("Skipping create snapshot new idx/term: {}/{}  current idx/term: {}/{}", s->get_last_log_idx(),
                 s->get_last_log_term(), current->get_last_log_idx(), current->get_last_log_term());
            return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
        }
    }

    LOGI("create snapshot last_log_idx: {} last_log_term: {}", s->get_last_log_idx(), s->get_last_log_term());
    m_snapshot_context = context;
    return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
}

bool ReplicationStateMachine::apply_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    // TODO persist snapshot
    std::lock_guard lk(m_snapshot_lock);
    m_snapshot_context = context;
    return true;
}

std::shared_ptr< homestore::snapshot_context > ReplicationStateMachine::last_snapshot() {
    std::lock_guard lk(m_snapshot_lock);
    auto snp = m_snapshot_context;
    return snp;
}

int ReplicationStateMachine::read_snapshot_obj(std::shared_ptr< homestore::snapshot_context > context,
                                                std::shared_ptr< homestore::snapshot_obj > snp_data) {
    HSHomeObject::PGBlobIterator* pg_iter = nullptr;
    auto s = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(context)->nuraft_snapshot();

    if (snp_data->user_ctx == nullptr) {
        // Create the pg blob iterator for the first time.
        pg_iter = new HSHomeObject::PGBlobIterator(*home_object_, repl_dev()->group_id(), context->get_lsn());
        snp_data->user_ctx = (void*)pg_iter;
    } else { pg_iter = r_cast< HSHomeObject::PGBlobIterator* >(snp_data->user_ctx); }

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
    auto log_str = fmt::format("group={}, term={}, lsn={},",
                               boost::uuids::to_string(repl_dev()->group_id()), s->get_last_log_term(),
                               s->get_last_log_idx());
    //TODO snp_data->offset is int64, need to change to uint64 in homestore
    if (snp_data->offset == int64_t(LAST_OBJ_ID)) {
        // No more shards to read, baseline resync is finished after this.
        snp_data->is_last_obj = true;
        LOGD("Read snapshot end, {}", log_str);
        return 0;
    }

    auto obj_id = objId(snp_data->offset);
    log_str = fmt::format("{} shard_seq_num={} batch_num={}", log_str, obj_id.shard_seq_num, obj_id.batch_id);

    //invalid Id
    if (!pg_iter->update_cursor(obj_id)) {
        LOGW("Invalid objId in snapshot read, {}, current shard_seq_num={}, current batch_num={}",
             log_str, pg_iter->cur_obj_id_.shard_seq_num, pg_iter->cur_obj_id_.batch_id);
        return -1;
    }

    //pg metadata message
    //shardId starts from 1
    if (obj_id.shard_seq_num == 0) {
        if (!pg_iter->create_pg_snapshot_data(snp_data->blob)) {
            LOGE("Failed to create pg snapshot data for snapshot read, {}", log_str);
            return -1;
        }
        return 0;
    }
    //shard metadata message
    if (obj_id.shard_seq_num != 0 && obj_id.batch_id == 0) {
        if (!pg_iter->generate_shard_blob_list()) {
            LOGE("Failed to generate shard blob list for snapshot read, {}", log_str);
            return -1;
        }
        if (!pg_iter->create_shard_snapshot_data(snp_data->blob)) {
            LOGE("Failed to create shard meta data for snapshot read, {}", log_str);
            return -1;
        }
        return 0;
    }
    //general blob message
    if (!pg_iter->create_blobs_snapshot_data(snp_data->blob)) {
        LOGE("Failed to create blob batch data for snapshot read, {}", log_str);
        return -1;
    }
    return 0;
}

void ReplicationStateMachine::write_snapshot_obj(std::shared_ptr< homestore::snapshot_context > context,
                                                  std::shared_ptr< homestore::snapshot_obj > snp_data) {
    RELEASE_ASSERT(context != nullptr, "Context null");
    RELEASE_ASSERT(snp_data != nullptr, "Snapshot data null");
    auto r_dev = repl_dev();
    if (!m_snp_rcv_handler) {
        m_snp_rcv_handler = std::make_unique< HSHomeObject::SnapshotReceiveHandler >(*home_object_, r_dev);
    }

    auto s = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(context)->nuraft_snapshot();
    auto obj_id = objId(static_cast< snp_obj_id_t >(snp_data->offset));
    auto log_suffix = fmt::format("group={} term={} lsn={} shard={} batch_num={} size={}",
                                  uuids::to_string(r_dev->group_id()), s->get_last_log_term(), s->get_last_log_idx(),
                                  obj_id.shard_seq_num, obj_id.batch_id, snp_data->blob.size());

    if (snp_data->is_last_obj) {
        LOGD("Write snapshot reached is_last_obj true {}", log_suffix);
        return;
    }

    // Check message integrity
    // TODO: add a flip here to simulate corrupted message
    auto header = r_cast< const SyncMessageHeader* >(snp_data->blob.cbytes());
    if (header->corrupted()) {
        LOGE("corrupted message in write_snapshot_data, lsn:{}, obj_id {} shard {} batch {}", s->get_last_log_idx(),
             obj_id.value, obj_id.shard_seq_num, obj_id.batch_id);
        return;
    }
    if (auto payload_size = snp_data->blob.size() - sizeof(SyncMessageHeader); payload_size != header->payload_size) {
        LOGE("payload size mismatch in write_snapshot_data {} != {}, lsn:{}, obj_id {} shard {} batch {}", payload_size,
             header->payload_size, s->get_last_log_idx(), obj_id.value, obj_id.shard_seq_num, obj_id.batch_id);
        return;
    }
    auto data_buf = snp_data->blob.cbytes() + sizeof(SyncMessageHeader);

    if (obj_id.shard_seq_num == 0) {
        // PG metadata & shard list message
        RELEASE_ASSERT(obj_id.batch_id == 0, "Invalid obj_id");

        auto pg_data = GetSizePrefixedResyncPGMetaData(data_buf);

        // Check if the snapshot context is same as the current snapshot context.
        // If not, drop the previous context and re-init a new one
        if (m_snp_rcv_handler->get_context_lsn() != context->get_lsn()) {
            m_snp_rcv_handler->reset_context(pg_data->pg_id(), context->get_lsn());
            // TODO: Reset all data of current PG - let's resync on a pristine base
        }

        auto ret = m_snp_rcv_handler->process_pg_snapshot_data(*pg_data);
        if (ret) {
            // Do not proceed, will request for resending the PG data
            LOGE("Failed to process PG snapshot data lsn:{} obj_id {} shard {} batch {}, err {}", s->get_last_log_idx(),
                 obj_id.value, obj_id.shard_seq_num, obj_id.batch_id, ret);
            return;
        }
        snp_data->offset =
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
            LOGE("Failed to process shard snapshot data lsn:{} obj_id {} shard {} batch {}, err {}",
                 s->get_last_log_idx(), obj_id.value, obj_id.shard_seq_num, obj_id.batch_id, ret);
            return;
        }
        // Request for the next batch
        snp_data->offset = objId(obj_id.shard_seq_num, 1).value;
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
        LOGE("Failed to process blob snapshot data lsn:{} obj_id {} shard {} batch {}, err {}", s->get_last_log_idx(),
             obj_id.value, obj_id.shard_seq_num, obj_id.batch_id, ret);
        return;
    }
    // Set next obj_id to fetch
    if (blob_batch->is_last_batch()) {
        auto next_shard = m_snp_rcv_handler->get_next_shard();
        if (next_shard == HSHomeObject::SnapshotReceiveHandler::shard_list_end_marker) {
            snp_data->offset = LAST_OBJ_ID;
        } else {
            snp_data->offset = objId(HSHomeObject::get_sequence_num_from_shard_id(next_shard), 0).value;
        }
    } else {
        snp_data->offset = objId(obj_id.shard_seq_num, obj_id.batch_id + 1).value;
    }

    LOGD("Write snapshot, processed blob data shard_seq_num:{} batch_num:{} {}", obj_id.shard_seq_num, obj_id.batch_id,
         log_suffix);
}

void ReplicationStateMachine::free_user_snp_ctx(void*& user_snp_ctx) {
    if (user_snp_ctx) {
        LOGE("User snapshot context null group={}", boost::uuids::to_string(repl_dev()->group_id()));
        return;
    }

    auto pg_iter = r_cast< HSHomeObject::PGBlobIterator* >(user_snp_ctx);
    LOGD("Freeing snapshot iterator pg_id={} group={}", pg_iter->pg_id_, boost::uuids::to_string(pg_iter->group_id_));
    delete pg_iter;
}

} // namespace homeobject
