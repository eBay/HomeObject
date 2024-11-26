#include "replication_message.hpp"
#include "replication_state_machine.hpp"
#include "hs_backend_config.hpp"
#include "lib/blob_route.hpp"

#include "generated/resync_pg_shard_generated.h"
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

void ReplicationStateMachine::on_restart() { home_object_->on_replica_restart(); }

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
        RELEASE_ASSERT(p_chunkID.has_value(), "unknown shard id to get binded chunk");
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
    auto current = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(m_snapshot_context)->nuraft_snapshot();
    if (s->get_last_log_idx() < current->get_last_log_idx()) {
        LOGI("Skipping create snapshot new idx/term: {}/{}  current idx/term: {}/{}", s->get_last_log_idx(),
             s->get_last_log_term(), current->get_last_log_idx(), current->get_last_log_term());
        return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
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

int ReplicationStateMachine::read_snapshot_data(std::shared_ptr< homestore::snapshot_context > context,
                                                std::shared_ptr< homestore::snapshot_data > snp_data) {
    HSHomeObject::PGBlobIterator* pg_iter = nullptr;
    auto s = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(context)->nuraft_snapshot();

    if (snp_data->user_ctx == nullptr) {
        // Create the pg blob iterator for the first time.
        pg_iter = new HSHomeObject::PGBlobIterator(*home_object_, repl_dev()->group_id());
        snp_data->user_ctx = (void*)pg_iter;
    } else {
        pg_iter = r_cast< HSHomeObject::PGBlobIterator* >(snp_data->user_ctx);
    }

    // Nuraft uses obj_id as a way to track the state of the snapshot read and write.
    // Nuraft starts with obj_id == 0 as first message always, leader send all the shards and
    // PG metadata as response. Follower responds with next obj_id it expects. obj_id's are
    // encoded in the form of obj_id = shard_seq_num(6 bytes) | batch_number(2 bytes)
    // Leader starts with shard sequence number 1 and read upto maximum size of data
    // and send to follower in a batch. Once all blob's are send in a shard,
    // leader notifies the follower by setting end_of_batch in the payload. Follower
    // moves to the next shard by incrementing shard_seq_num and reset batch number to 0.
    // Batch number is used to identify which batch in the current shard sequence number.
    // We use pg blob iterator to go over all the blobs in all the shards in that PG.
    // Once all the blobs are finished sending, end_of_scan will be true.
    int64_t obj_id = snp_data->offset;
    uint64_t shard_seq_num = obj_id >> 16;
    uint64_t batch_number = obj_id & 0xFFFF;
    auto log_str = fmt::format("group={} term={} lsn={} shard_seq={} batch_num={} size={}",
                               boost::uuids::to_string(repl_dev()->group_id()), s->get_last_log_term(),
                               s->get_last_log_idx(), shard_seq_num, batch_number, snp_data->blob.size());
    if (obj_id == 0) {
        // obj_id = 0 means its the first message and we send the pg and its shards metadata.
        pg_iter->cur_snapshot_batch_num = 0;
        pg_iter->create_pg_shard_snapshot_data(snp_data->blob);
        RELEASE_ASSERT(snp_data->blob.size() > 0, "Empty metadata snapshot data");
        LOGD("Read snapshot data first message {}", log_str);
        return 0;
    }

    if (shard_seq_num != pg_iter->cur_shard_seq_num_ || batch_number != pg_iter->cur_snapshot_batch_num) {
        // Follower can request for the old shard again. This may be due to error in writing or
        // it crashed and want to continue from where it left.
        LOGW("Shard or batch number not same as in iterator shard={}/{} batch_num={}/{}", shard_seq_num,
             pg_iter->cur_shard_seq_num_, batch_number, pg_iter->cur_snapshot_batch_num);
        if (shard_seq_num > pg_iter->cur_shard_seq_num_ || batch_number > pg_iter->cur_snapshot_batch_num) {
            // If we retrieve some invalid values, return error.
            return -1;
        }

        // Use the shard sequence number provided by the follower and we restart the batch.
        pg_iter->cur_shard_seq_num_ = shard_seq_num;
        pg_iter->cur_blob_id_ = -1;
        pg_iter->cur_snapshot_batch_num = 0;
    }

    if (pg_iter->end_of_scan()) {
        // No more shards to read, baseline resync is finished after this.
        snp_data->is_last_obj = true;
        LOGD("Read snapshot reached is_last_obj true {}", log_str);
        return 0;
    }

    // Get next set of blobs in the batch.
    std::vector< HSHomeObject::BlobInfoData > blob_data_vec;
    bool end_of_shard;
    auto result = pg_iter->get_next_blobs(HS_BACKEND_DYNAMIC_CONFIG(max_num_blobs_in_snapshot_batch),
                                          HS_BACKEND_DYNAMIC_CONFIG(max_snapshot_batch_size_mb) * 1024 * 1024,
                                          blob_data_vec, end_of_shard);
    if (result != 0) {
        LOGE("Failed to get next blobs in snapshot read result={} {}", result, log_str);
        return -1;
    }

    // Create snapshot flatbuffer data.
    pg_iter->create_blobs_snapshot_data(blob_data_vec, snp_data->blob, end_of_shard);
    if (end_of_shard) {
        pg_iter->cur_snapshot_batch_num = 0;
    } else {
        pg_iter->cur_snapshot_batch_num++;
    }

    LOGT("Read snapshot num_blobs={} end_of_shard={} {}", blob_data_vec.size(), end_of_shard, log_str);
    return 0;
}

void ReplicationStateMachine::write_snapshot_data(std::shared_ptr< homestore::snapshot_context > context,
                                                  std::shared_ptr< homestore::snapshot_data > snp_data) {
    RELEASE_ASSERT(context != nullptr, "Context null");
    RELEASE_ASSERT(snp_data != nullptr, "Snapshot data null");
    auto s = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(context)->nuraft_snapshot();
    int64_t obj_id = snp_data->offset;
    uint64_t shard_seq_num = obj_id >> 16;
    uint64_t batch_number = obj_id & 0xFFFF;

    auto log_str = fmt::format("group={} term={} lsn={} shard_seq={} batch_num={} size={}",
                               boost::uuids::to_string(repl_dev()->group_id()), s->get_last_log_term(),
                               s->get_last_log_idx(), shard_seq_num, batch_number, snp_data->blob.size());

    if (snp_data->is_last_obj) {
        LOGD("Write snapshot reached is_last_obj true {}", log_str);
        return;
    }

    if (obj_id == 0) {
        snp_data->offset = 1 << 16;
        // TODO add metadata.
        return;
    }

    auto snp = GetSizePrefixedResyncBlobDataBatch(snp_data->blob.bytes());
    // TODO Add blob puts

    if (snp->end_of_batch()) {
        snp_data->offset = (shard_seq_num + 1) << 16;
    } else {
        snp_data->offset = (shard_seq_num << 16) | (batch_number + 1);
    }

    LOGT("Read snapshot num_blobs={} end_of_batch={} {}", snp->data_array()->size(), snp->end_of_batch(), log_str);
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
