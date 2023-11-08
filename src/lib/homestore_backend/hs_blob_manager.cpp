#include "hs_homeobject.hpp"
#include "replication_message.hpp"
#include "replication_state_machine.hpp"
#include "lib/homeobject_impl.hpp"
#include "lib/blob_route.hpp"

SISL_LOGGING_DECL(blobmgr)

namespace homeobject {
static constexpr uint64_t io_align{512};

BlobManager::AsyncResult< blob_id_t > HSHomeObject::_put_blob(ShardInfo const& shard, Blob&& blob) {
    auto& pg_id = shard.placement_group;
    shared< homestore::ReplDev > repl_dev;
    blob_id_t new_blob_id;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        repl_dev = static_cast< HS_PG* >(iter->second.get())->repl_dev_;
        new_blob_id = iter->second->blob_sequence_num_.fetch_add(1, std::memory_order_relaxed);
        RELEASE_ASSERT(new_blob_id < std::numeric_limits< decltype(new_blob_id) >::max(),
                       "exhausted all available blob ids");
    }

    RELEASE_ASSERT(repl_dev != nullptr, "Repl dev instance null");

    const uint32_t needed_size = sizeof(ReplicationMessageHeader);
    auto req = put_result_ctx< BlobManager::Result< BlobInfo > >::make(needed_size, io_align, new_blob_id);

    uint8_t* raw_ptr = req->hdr_buf_.bytes;
    ReplicationMessageHeader* header = new (raw_ptr) ReplicationMessageHeader();
    header->msg_type = ReplicationMessageType::PUT_BLOB_MSG;
    header->payload_size = 0;
    header->payload_crc = 0;
    header->shard_id = shard.id;
    header->pg_id = pg_id;
    header->header_crc = header->calculate_crc();

    auto dev_block_size = repl_dev->get_blk_size();
    bool blob_copied = false, user_key_copied = false;
    sisl::sg_list sgs;
    sgs.size = 0;

    // Create blob header.
    auto blob_header_size = sisl::round_up(sizeof(BlobHeader), io_align);
    auto blob_header = r_cast< BlobHeader* >(iomanager.iobuf_alloc(io_align, blob_header_size));
    blob_header->magic = BlobHeader::blob_header_magic;
    blob_header->version = BlobHeader::blob_header_version;
    blob_header->shard_id = shard.id;
    blob_header->blob_id = new_blob_id;
    blob_header->hash_algorithm = BlobHeader::HashAlgorithm::CRC32;
    blob_header->blob_size = blob.body.size;
    blob_header->user_key_size = blob.user_key.size();
    blob_header->object_offset = blob.object_off;
    sgs.iovs.emplace_back(iovec{.iov_base = blob_header, .iov_len = blob_header_size});
    sgs.size += blob_header_size;

    // Append blob bytes.
    auto blob_bytes = blob.body.bytes;
    auto blob_size = blob.body.size;
    if ((reinterpret_cast< uintptr_t >(blob.body.bytes) % io_align != 0) || (blob_size % io_align != 0)) {
        // If address or size is not aligned, align it and create a separate buffer
        // and do expensive memcpy.
        blob_size = sisl::round_up(blob_size, io_align);
        blob_bytes = iomanager.iobuf_alloc(io_align, blob_size);
        std::memcpy(blob_bytes, blob.body.bytes, blob.body.size);
        blob_copied = true;
    }

    sgs.iovs.emplace_back(iovec{.iov_base = blob_bytes, .iov_len = blob_size});
    sgs.size += blob_size;

    // Append metadata if present and update the offsets and total size.
    size_t user_key_size = 0;
    uint8_t* user_key_bytes = nullptr;
    if (!blob.user_key.empty()) {
        user_key_size = blob.user_key.size();
        user_key_bytes = r_cast< uint8_t* >(blob.user_key.data());
        if ((reinterpret_cast< uintptr_t >(user_key_bytes) % io_align != 0) || (user_key_size % io_align != 0)) {
            // If address or size is not aligned, create a separate buffer and do expensive memcpy.
            user_key_size = sisl::round_up(user_key_size, io_align);
            user_key_bytes = r_cast< uint8_t* >(iomanager.iobuf_alloc(io_align, user_key_size));
            std::memcpy(user_key_bytes, blob.user_key.data(), blob.user_key.size());
            user_key_copied = true;
        }

        sgs.iovs.emplace_back(iovec{.iov_base = user_key_bytes, .iov_len = user_key_size});
        sgs.size += user_key_size;
        // Set offset of user meta data is after blob bytes.
        blob_header->user_key_offset = blob_size;
    }

    // Check if any padding of zeroes needs to be added to be aligned to device block size.
    auto pad_len = sisl::round_up(sgs.size, dev_block_size) - sgs.size;
    uint8_t* pad_zeroes = nullptr;
    if (pad_len != 0) {
        // TODO reuse a single pad zero buffer of len dev_block_size
        pad_zeroes = r_cast< uint8_t* >(iomanager.iobuf_alloc(io_align, pad_len));
        std::memset(pad_zeroes, 0, pad_len);
        sgs.iovs.emplace_back(iovec{.iov_base = pad_zeroes, .iov_len = pad_len});
        sgs.size += pad_len;
    }

    // Compute the checksum of blob and metadata.
    compute_blob_payload_hash(blob_header->hash_algorithm, blob.body.bytes, blob.body.size,
                              (uint8_t*)blob.user_key.data(), blob.user_key.size(), blob_header->hash,
                              BlobHeader::blob_max_hash_len);

    repl_dev->async_alloc_write(req->hdr_buf_, sisl::blob{}, sgs, req);
    return req->result().deferValue([this, header, blob_header, blob = std::move(blob), blob_bytes, blob_copied,
                                     user_key_bytes, user_key_copied,
                                     pad_zeroes](const auto& result) -> BlobManager::AsyncResult< blob_id_t > {
        header->~ReplicationMessageHeader();
        iomanager.iobuf_free(r_cast< uint8_t* >(blob_header));
        if (blob_copied) { iomanager.iobuf_free(blob_bytes); }
        if (user_key_copied) { iomanager.iobuf_free(r_cast< uint8_t* >(user_key_bytes)); }
        if (pad_zeroes) { iomanager.iobuf_free(r_cast< uint8_t* >(pad_zeroes)); }

        if (result.hasError()) { return folly::makeUnexpected(result.error()); }
        auto blob_info = result.value();
        LOGTRACEMOD(blobmgr, "Put blob success shard {} blob {} pbas {}", blob_info.shard_id, blob_info.blob_id,
                    blob_info.pbas.to_string());

        return blob_info.blob_id;
    });
}

void HSHomeObject::on_blob_put_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                      const homestore::MultiBlkId& pbas,
                                      cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    put_result_ctx< BlobManager::Result< BlobInfo > >* ctx{nullptr};
    if (hs_ctx != nullptr) {
        ctx = boost::static_pointer_cast< put_result_ctx< BlobManager::Result< BlobInfo > > >(hs_ctx).get();
    }

    auto msg_header = r_cast< ReplicationMessageHeader* >(header.bytes);
    if (msg_header->corrupted()) {
        LOGE("replication message header is corrupted with crc error, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH)); }
        return;
    }

    shared< BlobIndexTable > index_table;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(msg_header->pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        index_table = static_cast< HS_PG* >(iter->second.get())->index_table_;
        RELEASE_ASSERT(index_table != nullptr, "Index table not intialized");
        if (iter->second->blob_sequence_num_.load() <= ctx->blob_id_) {
            iter->second->blob_sequence_num_.store(ctx->blob_id_ + 1);
        }
    }

    BlobInfo blob_info;
    blob_info.shard_id = msg_header->shard_id;
    blob_info.blob_id = ctx->blob_id_;
    blob_info.pbas = pbas;

    // Write to index table with key {shard id, blob id } and value {pba}.
    auto r = add_to_index_table(index_table, blob_info);
    if (r.hasError()) {
        LOGE("Failed to insert into index table for blob {} err {}", lsn, r.error());
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(r.error())); }
        return;
    }

    if (ctx) { ctx->promise_.setValue(BlobManager::Result< BlobInfo >(blob_info)); }
}

BlobManager::AsyncResult< Blob > HSHomeObject::_get_blob(ShardInfo const& shard, blob_id_t blob_id, uint64_t req_offset,
                                                         uint64_t req_len) const {
    auto& pg_id = shard.placement_group;
    shared< BlobIndexTable > index_table;
    shared< homestore::ReplDev > repl_dev;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        repl_dev = static_cast< HS_PG* >(iter->second.get())->repl_dev_;
        index_table = static_cast< HS_PG* >(iter->second.get())->index_table_;
    }

    RELEASE_ASSERT(repl_dev != nullptr, "Repl dev instance null");
    RELEASE_ASSERT(index_table != nullptr, "Index table instance null");

    auto r = get_blob_from_index_table(index_table, shard.id, blob_id);
    if (!r) {
        LOGW("Blob not found in index [route={}]", BlobRoute{blob_id, shard.id});
        return folly::makeUnexpected(r.error());
    }

    auto multi_blkids = r.value();
    auto block_size = repl_dev->get_blk_size();
    sisl::sg_list sgs;
    auto total_size = multi_blkids.blk_count() * block_size;
    shared< uint8_t > iov_base(iomanager.iobuf_alloc(block_size, total_size),
                               [](uint8_t* buf) { iomanager.iobuf_free(buf); });
    sgs.size = total_size;
    sgs.iovs.emplace_back(iovec{.iov_base = iov_base.get(), .iov_len = total_size});

    return repl_dev->async_read(multi_blkids, sgs, total_size)
        .thenValue([this, blob_id, req_len, req_offset, shard, multi_blkids,
                    iov_base](auto&& result) mutable -> BlobManager::AsyncResult< Blob > {
            if (result) {
                LOGE("Failed to read blob {} shard {} err {}", blob_id, shard.id, result.value());
                return folly::makeUnexpected(BlobError::READ_FAILED);
            }

            auto const b_route = BlobRoute{blob_id, shard.id};
            BlobHeader* header = (BlobHeader*)iov_base.get();
            if (!header->valid()) {
                LOGE("Invalid header found for [route={}] [header={}]", b_route, header->to_string());
                return folly::makeUnexpected(BlobError::READ_FAILED);
            }

            if (header->shard_id != shard.id) {
                LOGE("Invalid shard id found in header for [route={}] [header={}]", b_route, header->to_string());
                return folly::makeUnexpected(BlobError::READ_FAILED);
            }

            // Metadata start offset is just after blob.
            size_t blob_size = header->blob_size;
            auto blob_header_size = sisl::round_up(sizeof(BlobHeader), io_align);
            uint8_t* blob_bytes = (uint8_t*)iov_base.get() + blob_header_size;
            uint8_t* user_key_bytes = nullptr;
            size_t user_key_size = 0;
            if (header->user_key_offset != 0) {
                user_key_bytes = blob_bytes + header->user_key_offset;
                user_key_size = header->user_key_size;
            }

            uint8_t computed_hash[BlobHeader::blob_max_hash_len]{};
            compute_blob_payload_hash(header->hash_algorithm, blob_bytes, blob_size, user_key_bytes, user_key_size,
                                      computed_hash, BlobHeader::blob_max_hash_len);
            if (std::memcmp(computed_hash, header->hash, BlobHeader::blob_max_hash_len) != 0) {
                LOGE("Hash mismatch for [route={}] [header={}] [computed={}]", b_route, header->to_string(),
                     spdlog::to_hex(computed_hash, computed_hash + BlobHeader::blob_max_hash_len));
                return folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH);
            }

            if (req_offset + req_len > blob_size) {
                LOGE("Invalid offset length request in get blob {} offset {} len {} size {}", blob_id, req_offset,
                     req_len, blob_size);
                return folly::makeUnexpected(BlobError::INVALID_ARG);
            }

            // Copy the blob bytes from the offset. If request len is 0, take the
            // whole blob size else copy only the request length.
            auto res_len = req_len == 0 ? blob_size - req_offset : req_len;
            auto body = sisl::io_blob_safe(res_len);
            std::memcpy(body.bytes, blob_bytes + req_offset, res_len);

            // Copy the metadata if its present.
            std::string user_key{};
            if (header->user_key_offset != 0) {
                user_key.resize(user_key_size);
                std::memcpy(user_key.data(), user_key_bytes, user_key_size);
            }

            LOGTRACEMOD(blobmgr, "Blob get success for blob {} shard {} blkid {}", blob_id, shard.id,
                        multi_blkids.to_string());
            return Blob(std::move(body), std::move(user_key), header->object_offset);
        });
}

homestore::blk_alloc_hints HSHomeObject::blob_put_get_blk_alloc_hints(sisl::blob const& header,
                                                                      cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< BlobManager::Result< BlobInfo > >* ctx{nullptr};
    if (hs_ctx != nullptr) {
        ctx = boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< BlobInfo > > >(hs_ctx).get();
    }

    auto msg_header = r_cast< ReplicationMessageHeader* >(header.bytes);
    if (msg_header->corrupted()) {
        LOGE("replication message header is corrupted with crc error shard:{}", msg_header->shard_id);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH)); }
        return {};
    }

    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(msg_header->shard_id);
    RELEASE_ASSERT(shard_iter != _shard_map.end(), "Couldnt find shard id");
    auto hs_shard = d_cast< HS_Shard* >((*shard_iter->second).get());
    auto chunk_id = hs_shard->sb_->chunk_id;
    LOGI("Got shard id {} chunk id {}", msg_header->shard_id, chunk_id);
    homestore::blk_alloc_hints hints;
    hints.chunk_id_hint = chunk_id;
    return hints;
}

BlobManager::NullAsyncResult HSHomeObject::_del_blob(ShardInfo const& shard, blob_id_t blob_id) {
    auto& pg_id = shard.placement_group;
    shared< homestore::ReplDev > repl_dev;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        repl_dev = static_cast< HS_PG* >(iter->second.get())->repl_dev_;
    }

    RELEASE_ASSERT(repl_dev != nullptr, "Repl dev instance null");

    auto req = repl_result_ctx< BlobManager::Result< BlobInfo > >::make(sizeof(blob_id_t), io_align);

    req->header_.msg_type = ReplicationMessageType::DEL_BLOB_MSG;
    req->header_.payload_size = 0;
    req->header_.payload_crc = 0;
    req->header_.shard_id = shard.id;
    req->header_.pg_id = pg_id;
    req->header_.seal();
    sisl::blob header;
    header.bytes = r_cast< uint8_t* >(&req->header_);
    header.size = sizeof(req->header_);

    memcpy(req->hdr_buf_.bytes, &blob_id, sizeof(blob_id_t));

    repl_dev->async_alloc_write(header, req->hdr_buf_, sisl::sg_list{}, req);
    return req->result().deferValue([](const auto& result) -> folly::Expected< folly::Unit, BlobError > {
        if (result.hasError()) { return folly::makeUnexpected(result.error()); }
        auto blob_info = result.value();
        LOGTRACEMOD(blobmgr, "Delete blob success,  shard_id {} , blob_id {}", blob_info.shard_id, blob_info.blob_id);

        return folly::Unit();
    });
}

void HSHomeObject::on_blob_del_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                      cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< BlobManager::Result< BlobInfo > >* ctx{nullptr};
    if (hs_ctx != nullptr) {
        ctx = boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< BlobInfo > > >(hs_ctx).get();
    }

    auto msg_header = r_cast< ReplicationMessageHeader* >(header.bytes);
    if (msg_header->corrupted()) {
        LOGERROR("replication message header is corrupted with crc error, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH)); }
        return;
    }

    shared< BlobIndexTable > index_table;
    shared< homestore::ReplDev > repl_dev;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(msg_header->pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        index_table = static_cast< HS_PG* >(iter->second.get())->index_table_;
        repl_dev = static_cast< HS_PG* >(iter->second.get())->repl_dev_;
        RELEASE_ASSERT(index_table != nullptr, "Index table not intialized");
        RELEASE_ASSERT(repl_dev != nullptr, "Repl dev instance null");
    }

    BlobInfo blob_info;
    blob_info.shard_id = msg_header->shard_id;
    blob_info.blob_id = *r_cast< blob_id_t* >(key.bytes);

    auto r = move_to_tombstone(index_table, blob_info);
    if (!r) {
        LOGW("fail to move blob to tombstone,  blob_id {}, shard_id {}, {}", blob_info.blob_id, blob_info.shard_id,
             r.error());
        if (ctx) ctx->promise_.setValue(folly::makeUnexpected(r.error()));
        return;
    }

    auto& multiBlks = r.value();
    repl_dev->async_free_blks(lsn, multiBlks);
    if (ctx) { ctx->promise_.setValue(BlobManager::Result< BlobInfo >(blob_info)); }
}

void HSHomeObject::compute_blob_payload_hash(BlobHeader::HashAlgorithm algorithm, const uint8_t* blob_bytes,
                                             size_t blob_size, const uint8_t* user_key_bytes, size_t user_key_size,
                                             uint8_t* hash_bytes, size_t hash_len) const {
    std::memset(hash_bytes, 0, hash_len);
    switch (algorithm) {
    case HSHomeObject::BlobHeader::HashAlgorithm::NONE: {
        break;
    }
    case HSHomeObject::BlobHeader::HashAlgorithm::CRC32: {
        auto hash32 = crc32_ieee(init_crc32, blob_bytes, blob_size);
        if (user_key_size != 0) { hash32 = crc32_ieee(hash32, user_key_bytes, user_key_size); }
        RELEASE_ASSERT(sizeof(uint32_t) <= hash_len, "Hash length invalid");
        std::memcpy(hash_bytes, r_cast< uint8_t* >(&hash32), sizeof(uint32_t));
        break;
    }
    default:
        RELEASE_ASSERT(false, "Hash not implemented");
    }
}

} // namespace homeobject
