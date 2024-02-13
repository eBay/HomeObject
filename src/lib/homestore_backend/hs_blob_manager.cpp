#include "hs_homeobject.hpp"
#include "replication_message.hpp"
#include "replication_state_machine.hpp"
#include "lib/homeobject_impl.hpp"
#include "lib/blob_route.hpp"
#include "hs_hmobj_cp.hpp"
#include <homestore/homestore.hpp>

SISL_LOGGING_DECL(blobmgr)

#define BLOG(level, shard_id, blob_id, msg, ...)                                                                       \
    LOG##level##MOD(blobmgr, "[pg={},shard={},blob={}] " msg, (shard_id >> homeobject::shard_width),                   \
                    (shard_id & homeobject::shard_mask), blob_id, ##__VA_ARGS__)

#define BLOGT(shard_id, blob_id, msg, ...) BLOG(TRACE, shard_id, blob_id, msg, ##__VA_ARGS__)
#define BLOGD(shard_id, blob_id, msg, ...) BLOG(DEBUG, shard_id, blob_id, msg, ##__VA_ARGS__)
#define BLOGI(shard_id, blob_id, msg, ...) BLOG(INFO, shard_id, blob_id, msg, ##__VA_ARGS__)
#define BLOGW(shard_id, blob_id, msg, ...) BLOG(WARN, shard_id, blob_id, msg, ##__VA_ARGS__)
#define BLOGE(shard_id, blob_id, msg, ...) BLOG(ERROR, shard_id, blob_id, msg, ##__VA_ARGS__)
#define BLOGC(shard_id, blob_id, msg, ...) BLOG(CRITICAL, shard_id, blob_id, msg, ##__VA_ARGS__)

namespace homeobject {
struct put_blob_req_ctx : public repl_result_ctx< BlobManager::Result< HSHomeObject::BlobInfo > > {
    uint32_t blob_header_idx_{0};

    // Unaligned buffer is good enough for header and key, since they will be explicity copied
    static intrusive< put_blob_req_ctx > make(uint32_t data_hdr_size) {
        return intrusive< put_blob_req_ctx >{new put_blob_req_ctx(data_hdr_size)};
    }

    put_blob_req_ctx(uint32_t data_hdr_size) : repl_result_ctx(0u /* header_extn_size */, sizeof(blob_id_t)) {
        uint32_t aligned_size = uint32_cast(sisl::round_up(data_hdr_size, io_align));
        sisl::io_blob_safe buf{aligned_size, io_align};
        new (buf.bytes()) HSHomeObject::BlobHeader();
        add_data_sg(std::move(buf));
        blob_header_idx_ = data_bufs_.size() - 1;
    }

    void copy_user_key(std::string const& user_key) {
        std::memcpy((blob_header_buf().bytes() + sizeof(HSHomeObject::BlobHeader)), user_key.data(), user_key.size());
    }

    HSHomeObject::BlobHeader* blob_header() { return r_cast< HSHomeObject::BlobHeader* >(blob_header_buf().bytes()); }
    sisl::io_blob_safe& blob_header_buf() { return data_bufs_[blob_header_idx_]; }
};

BlobManager::AsyncResult< blob_id_t > HSHomeObject::_put_blob(ShardInfo const& shard, Blob&& blob) {
    auto& pg_id = shard.placement_group;
    shared< homestore::ReplDev > repl_dev;
    blob_id_t new_blob_id;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        auto hs_pg = static_cast< HS_PG* >(iter->second.get());
        repl_dev = hs_pg->repl_dev_;
        new_blob_id = hs_pg->blob_sequence_num_.fetch_add(1, std::memory_order_relaxed);

        hs_pg->cache_pg_sb_->blob_sequence_num = hs_pg->blob_sequence_num_.load();
        auto cur_cp = homestore::HomeStore::instance()->cp_mgr().cp_guard();
        auto cp_ctx = s_cast< HomeObjCPContext* >(cur_cp->context(homestore::cp_consumer_t::HS_CLIENT));
        cp_ctx->add_pg_to_dirty_list(hs_pg->cache_pg_sb_);

        RELEASE_ASSERT(new_blob_id < std::numeric_limits< decltype(new_blob_id) >::max(),
                       "exhausted all available blob ids");
    }

    RELEASE_ASSERT(repl_dev != nullptr, "Repl dev instance null");

    // Create a put_blob request which allocates for header, key and blob_header. Data sgs are added later
    auto req = put_blob_req_ctx::make(sizeof(BlobHeader) + blob.user_key.size());
    req->header()->msg_type = ReplicationMessageType::PUT_BLOB_MSG;
    req->header()->payload_size = 0;
    req->header()->payload_crc = 0;
    req->header()->shard_id = shard.id;
    req->header()->pg_id = pg_id;
    req->header()->seal();

    // Serialize blob_id as Replication key
    *(reinterpret_cast< blob_id_t* >(req->key_buf().bytes())) = new_blob_id;

    // Now we are in data section. Data section has 4 areas
    // 1. Blob header (information about the type of blob, size, hash etc)
    // 2. User Key (if present)
    // 3. Actual data blobs
    // 4. Any padding of zeros (to round off to nearest block size)

    // Blob Header section.
    auto const blob_size = blob.body.size();
    req->blob_header()->type = DataHeader::data_type_t::BLOB_INFO;
    req->blob_header()->shard_id = shard.id;
    req->blob_header()->blob_id = new_blob_id;
    req->blob_header()->hash_algorithm = BlobHeader::HashAlgorithm::CRC32;
    req->blob_header()->blob_size = blob_size;
    req->blob_header()->user_key_size = blob.user_key.size();
    req->blob_header()->object_offset = blob.object_off;

    // Append the user key information if present.
    if (!blob.user_key.empty()) { req->copy_user_key(blob.user_key); }

    // Set offset of actual data after the blob header and user key (rounded off)
    req->blob_header()->data_offset = req->blob_header_buf().size();

    // Append blob bytes.
    if (((r_cast< uintptr_t >(blob.body.cbytes()) % io_align) != 0) || ((blob_size % io_align) != 0)) {
        // If address or size is not aligned, create a separate aligned buffer and do expensive memcpy.
        sisl::io_blob_safe new_body = sisl::io_blob_safe(sisl::round_up(blob_size, io_align), io_align);
        std::memcpy(new_body.bytes(), blob.body.cbytes(), blob_size);
        blob.body = std::move(new_body);
    }

    // Compute the checksum of blob and metadata.
    compute_blob_payload_hash(req->blob_header()->hash_algorithm, blob.body.cbytes(), blob_size,
                              (uint8_t*)blob.user_key.data(), blob.user_key.size(), req->blob_header()->hash,
                              BlobHeader::blob_max_hash_len);
    req->add_data_sg(std::move(blob.body));

    // Check if any padding of zeroes needs to be added to be aligned to device block size.
    auto pad_len = sisl::round_up(req->data_sgs().size, repl_dev->get_blk_size()) - req->data_sgs().size;
    if (pad_len != 0) {
        sisl::io_blob_safe& zbuf = get_pad_buf(pad_len);
        req->add_data_sg(zbuf.bytes(), pad_len);
    }

    BLOGT(req->blob_header()->shard_id, req->blob_header()->blob_id, "Put blob: header=[{}] sgs=[{}]",
          req->blob_header()->to_string(), req->data_sgs_string());

    repl_dev->async_alloc_write(req->cheader_buf(), req->ckey_buf(), req->data_sgs(), req);
    return req->result().deferValue([this, req](const auto& result) -> BlobManager::AsyncResult< blob_id_t > {
        if (result.hasError()) { return folly::makeUnexpected(result.error()); }
        auto blob_info = result.value();
        BLOGT(blob_info.shard_id, blob_info.blob_id, "Put blob success blkid=[{}]", blob_info.pbas.to_string());
        return blob_info.blob_id;
    });
}

void HSHomeObject::on_blob_put_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                      homestore::MultiBlkId const& pbas,
                                      cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< BlobManager::Result< BlobInfo > >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer) {
        ctx = boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< BlobInfo > > >(hs_ctx).get();
    }

    auto msg_header = r_cast< ReplicationMessageHeader const* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGE("replication message header is corrupted with crc error, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH)); }
        return;
    }

    auto const blob_id = *(reinterpret_cast< blob_id_t* >(const_cast< uint8_t* >(key.cbytes())));
    shared< BlobIndexTable > index_table;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(msg_header->pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        auto hs_pg = static_cast< HS_PG* >(iter->second.get());
        index_table = hs_pg->index_table_;
        RELEASE_ASSERT(index_table != nullptr, "Index table not intialized");
        if (hs_pg->blob_sequence_num_.load() <= blob_id) {
            hs_pg->blob_sequence_num_.store(blob_id + 1);
            hs_pg->cache_pg_sb_->blob_sequence_num = hs_pg->blob_sequence_num_.load();

            auto cur_cp = homestore::HomeStore::instance()->cp_mgr().cp_guard();
            auto cp_ctx = s_cast< HomeObjCPContext* >(cur_cp->context(homestore::cp_consumer_t::HS_CLIENT));
            cp_ctx->add_pg_to_dirty_list(hs_pg->cache_pg_sb_);
        }
    }

    BlobInfo blob_info;
    blob_info.shard_id = msg_header->shard_id;
    blob_info.blob_id = blob_id;
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
        BLOGW(shard.id, blob_id, "Blob not found in index");
        return folly::makeUnexpected(r.error());
    }

    auto const blkid = r.value();
    auto const total_size = blkid.blk_count() * repl_dev->get_blk_size();
    sisl::io_blob_safe read_buf{total_size, io_align};

    sisl::sg_list sgs;
    sgs.size = total_size;
    sgs.iovs.emplace_back(iovec{.iov_base = read_buf.bytes(), .iov_len = read_buf.size()});

    BLOGT(shard.id, blob_id, "Blob get request: blkid={}, buf={}", blkid.to_string(), (void*)read_buf.bytes());
    return repl_dev->async_read(blkid, sgs, total_size)
        .thenValue([this, blob_id, shard_id = shard.id, req_len, req_offset, blkid,
                    read_buf = std::move(read_buf)](auto&& result) mutable -> BlobManager::AsyncResult< Blob > {
            if (result) {
                BLOGE(shard_id, blob_id, "Failed to get blob: err={}", blob_id, shard_id, result.value());
                return folly::makeUnexpected(BlobError::READ_FAILED);
            }

            BlobHeader const* header = r_cast< BlobHeader const* >(read_buf.cbytes());
            if (!header->valid()) {
                BLOGE(shard_id, blob_id, "Invalid header found: [header={}]", header->to_string());
                return folly::makeUnexpected(BlobError::READ_FAILED);
            }

            if (header->shard_id != shard_id) {
                BLOGE(shard_id, blob_id, "Invalid shard_id in header: [header={}]", header->to_string());
                return folly::makeUnexpected(BlobError::READ_FAILED);
            }

            // Metadata start offset is just after blob header
            std::string user_key = header->user_key_size
                ? std::string((const char*)(read_buf.bytes() + sizeof(BlobHeader)), (size_t)header->user_key_size)
                : std::string{};

            uint8_t const* blob_bytes = read_buf.bytes() + header->data_offset;
            uint8_t computed_hash[BlobHeader::blob_max_hash_len]{};
            compute_blob_payload_hash(header->hash_algorithm, blob_bytes, header->blob_size,
                                      uintptr_cast(user_key.data()), header->user_key_size, computed_hash,
                                      BlobHeader::blob_max_hash_len);
            if (std::memcmp(computed_hash, header->hash, BlobHeader::blob_max_hash_len) != 0) {
                BLOGE(shard_id, blob_id, "Hash mismatch header [{}] [computed={:np}]", header->to_string(),
                      spdlog::to_hex(computed_hash, computed_hash + BlobHeader::blob_max_hash_len));
                return folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH);
            }

            if (req_offset + req_len > header->blob_size) {
                BLOGE(shard_id, blob_id, "Invalid offset length requested in get blob offset={} len={} size={}",
                      req_offset, req_len, header->blob_size);
                return folly::makeUnexpected(BlobError::INVALID_ARG);
            }

            // Copy the blob bytes from the offset. If request len is 0, take the
            // whole blob size else copy only the request length.
            auto res_len = req_len == 0 ? header->blob_size - req_offset : req_len;
            auto body = sisl::io_blob_safe(res_len);
            std::memcpy(body.bytes(), blob_bytes + req_offset, res_len);

            BLOGT(blob_id, shard_id, "Blob get success: blkid={}", blkid.to_string());
            return Blob(std::move(body), std::move(user_key), header->object_offset);
        });
}

homestore::blk_alloc_hints HSHomeObject::blob_put_get_blk_alloc_hints(sisl::blob const& header,
                                                                      cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< BlobManager::Result< BlobInfo > >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer) {
        ctx = boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< BlobInfo > > >(hs_ctx).get();
    }

    auto msg_header = r_cast< ReplicationMessageHeader* >(const_cast< uint8_t* >(header.cbytes()));
    if (msg_header->corrupted()) {
        LOGE("replication message header is corrupted with crc error shard:{}", msg_header->shard_id);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(BlobError::CHECKSUM_MISMATCH)); }
        return {};
    }

    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(msg_header->shard_id);
    RELEASE_ASSERT(shard_iter != _shard_map.end(), "Couldnt find shard id");
    auto hs_shard = d_cast< HS_Shard* >((*shard_iter->second).get());
    BLOGD(msg_header->shard_id, "n/a", "Picked chunk_id={}", hs_shard->sb_->chunk_id);

    homestore::blk_alloc_hints hints;
    hints.chunk_id_hint = hs_shard->sb_->chunk_id;
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

    // Create an unaligned header request unaligned
    auto req = repl_result_ctx< BlobManager::Result< BlobInfo > >::make(0u /* header_extn */,
                                                                        sizeof(blob_id_t) /* key_size */);
    req->header()->msg_type = ReplicationMessageType::DEL_BLOB_MSG;
    req->header()->payload_size = 0;
    req->header()->payload_crc = 0;
    req->header()->shard_id = shard.id;
    req->header()->pg_id = pg_id;
    req->header()->seal();

    // Populate the key
    std::memcpy(req->key_buf().bytes(), &blob_id, sizeof(blob_id_t));

    repl_dev->async_alloc_write(req->cheader_buf(), req->ckey_buf(), sisl::sg_list{}, req);
    return req->result().deferValue([](const auto& result) -> folly::Expected< folly::Unit, BlobError > {
        if (result.hasError()) { return folly::makeUnexpected(result.error()); }
        auto blob_info = result.value();
        BLOGT(blob_info.shard_id, blob_info.blob_id, "Delete blob successful");
        return folly::Unit();
    });
}

void HSHomeObject::on_blob_del_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                      cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< BlobManager::Result< BlobInfo > >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer) {
        ctx = boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< BlobInfo > > >(hs_ctx).get();
    }

    auto msg_header = r_cast< ReplicationMessageHeader* >(const_cast< uint8_t* >(header.cbytes()));
    if (msg_header->corrupted()) {
        BLOGE(msg_header->shard_id, *r_cast< blob_id_t const* >(key.cbytes()),
              "replication message header is corrupted with crc error, lsn={} header=[{}]", lsn,
              msg_header->to_string());
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
    blob_info.blob_id = *r_cast< blob_id_t const* >(key.cbytes());

    auto r = move_to_tombstone(index_table, blob_info);
    if (!r) {
        if (recovery_done_) {
            BLOGE(blob_info.shard_id, blob_info.blob_id, "Failed to move blob to tombstone, error={}", r.error());
            if (ctx) ctx->promise_.setValue(folly::makeUnexpected(r.error()));
            return;
        } else {
            if (ctx) { ctx->promise_.setValue(BlobManager::Result< BlobInfo >(blob_info)); }
            return;
        }
    }

    auto& multiBlks = r.value();
    if (multiBlks != tombstone_pbas) { repl_dev->async_free_blks(lsn, multiBlks); }

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
