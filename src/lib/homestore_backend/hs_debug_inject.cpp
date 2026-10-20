#include "hs_debug_inject.hpp"

#include <cstring>
#include <random>

#include <homestore/blkdata_service.hpp>
#include <homestore/btree/btree_kv.hpp>
#include <homestore/btree/btree_req.hpp>
#include <homestore/homestore.hpp>

#include "hs_homeobject.hpp"

namespace homeobject {
namespace hs_debug_inject {

InjectResult delete_blob_from_index(HSHomeObject& ho, pg_id_t pg_id, shard_id_t shard_id, blob_id_t blob_id) {
    auto hs_pg = ho.get_hs_pg(pg_id);
    if (!hs_pg) { return folly::makeUnexpected(fmt::format("pg {} not found", pg_id)); }

    BlobRouteKey blob_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue out_value;
    homestore::BtreeSingleRemoveRequest remove_req{&blob_key, &out_value};
    auto status = hs_pg->index_table_->remove(remove_req);
    if (status != homestore::btree_status_t::success) {
        return folly::makeUnexpected(
            fmt::format("failed to remove blob {} from index, status={}", blob_id, static_cast< int >(status)));
    }
    return folly::Unit();
}

InjectResult delete_shard_from_index(HSHomeObject& ho, pg_id_t pg_id, shard_id_t shard_id) {
    auto hs_pg = ho.get_hs_pg(pg_id);
    if (!hs_pg) { return folly::makeUnexpected(fmt::format("pg {} not found", pg_id)); }

    auto start_key = BlobRouteKey{BlobRoute{shard_id, 0}};
    auto end_key = BlobRouteKey{BlobRoute{shard_id, std::numeric_limits< uint64_t >::max()}};
    homestore::BtreeRangeRemoveRequest< BlobRouteKey > range_remove_req{
        homestore::BtreeKeyRange< BlobRouteKey >{std::move(start_key), true /* inclusive */, std::move(end_key),
                                                 true /* inclusive */},
        nullptr, std::numeric_limits< uint32_t >::max(),
        [](homestore::BtreeKey const&, homestore::BtreeValue const&) -> bool { return true; }};

    auto status = hs_pg->index_table_->remove(range_remove_req);
    if (status != homestore::btree_status_t::success && status != homestore::btree_status_t::not_found) {
        return folly::makeUnexpected(
            fmt::format("failed to remove shard {} from index, status={}", shard_id, static_cast< int >(status)));
    }
    return folly::Unit();
}

InjectResult corrupt_blob_data(HSHomeObject& ho, pg_id_t pg_id, shard_id_t shard_id, blob_id_t blob_id) {
    auto hs_pg = ho.get_hs_pg(pg_id);
    if (!hs_pg) { return folly::makeUnexpected(fmt::format("pg {} not found", pg_id)); }

    auto& data_service = homestore::data_service();
    const auto blk_size = data_service.get_blk_size();

    BlobRouteKey blob_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue out_value;
    homestore::BtreeSingleGetRequest blob_get_req{&blob_key, &out_value};
    auto status = hs_pg->index_table_->get(blob_get_req);
    if (status != homestore::btree_status_t::success) {
        return folly::makeUnexpected(
            fmt::format("blob {} not found in index, status={}", blob_id, static_cast< int >(status)));
    }

    auto pbas = out_value.pbas();
    auto total_size = pbas.blk_count() * blk_size;
    sisl::sg_list data_sgs;
    data_sgs.size = total_size;
    data_sgs.iovs.emplace_back(iovec{.iov_base = iomanager.iobuf_alloc(blk_size, total_size), .iov_len = total_size});

    std::string err_msg;
    try {
        data_service.async_read(pbas, data_sgs, total_size)
            .thenValue([&](auto&& err) {
                if (err) { throw std::runtime_error(fmt::format("read failed: {}", err.message())); }

                auto* data_ptr = reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base);
                for (size_t i = 0; i < data_sgs.iovs[0].iov_len / 2; i++) {
                    data_ptr[i] ^= 0xFF;
                }

                return data_service.async_write(data_sgs, pbas).thenValue([](auto&& werr) {
                    if (werr) { throw std::runtime_error(fmt::format("write failed: {}", werr.message())); }
                });
            })
            .get();
    } catch (const std::exception& e) { err_msg = e.what(); }

    iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
    if (!err_msg.empty()) { return folly::makeUnexpected(err_msg); }
    return folly::Unit();
}

InjectResult make_blob_inconsistent(HSHomeObject& ho, pg_id_t pg_id, shard_id_t shard_id, blob_id_t blob_id) {
    auto hs_pg = ho.get_hs_pg(pg_id);
    if (!hs_pg) { return folly::makeUnexpected(fmt::format("pg {} not found", pg_id)); }

    auto& data_service = homestore::data_service();
    const auto blk_size = data_service.get_blk_size();

    BlobRouteKey blob_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue out_value;
    homestore::BtreeSingleGetRequest blob_get_req{&blob_key, &out_value};
    auto status = hs_pg->index_table_->get(blob_get_req);
    if (status != homestore::btree_status_t::success) {
        return folly::makeUnexpected(
            fmt::format("blob {} not found in index, status={}", blob_id, static_cast< int >(status)));
    }

    auto pbas = out_value.pbas();
    auto total_size = pbas.blk_count() * blk_size;
    sisl::sg_list data_sgs;
    data_sgs.size = total_size;
    data_sgs.iovs.emplace_back(iovec{.iov_base = iomanager.iobuf_alloc(blk_size, total_size), .iov_len = total_size});

    std::string err_msg;
    try {
        data_service.async_read(pbas, data_sgs, total_size)
            .thenValue([&](auto&& err) {
                if (err) { throw std::runtime_error(fmt::format("read failed: {}", err.message())); }

                uint8_t* read_buf = reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base);
                auto* header = reinterpret_cast< HSHomeObject::BlobHeader* >(read_buf);
                uint8_t* blob_bytes = read_buf + header->data_offset;

                std::mt19937 rng{std::random_device{}()};
                std::uniform_int_distribution< int > dist(0, 255);
                for (size_t i = 0; i < header->blob_size / 2; i++) {
                    blob_bytes[i] ^= static_cast< uint8_t >(dist(rng));
                }

                uint8_t computed_hash[HSHomeObject::BlobHeader::blob_max_hash_len]{};
                ho.compute_blob_payload_hash(header->hash_algorithm, blob_bytes, header->blob_size, computed_hash,
                                             HSHomeObject::BlobHeader::blob_max_hash_len);
                std::memcpy(header->hash, computed_hash, HSHomeObject::BlobHeader::blob_max_hash_len);
                std::memset(header->header_hash, 0, HSHomeObject::BlobHeader::blob_max_hash_len);
                uint32_t computed_header_hash = crc32_ieee(0, (uint8_t*)header, sizeof(HSHomeObject::BlobHeader));
                std::memcpy(header->header_hash, &computed_header_hash, sizeof(uint32_t));

                if (!ho.verify_blob(data_sgs.iovs[0].iov_base, header->shard_id, header->blob_id)) {
                    throw std::runtime_error(
                        fmt::format("blob verification failed after modification, blob_id={}", blob_id));
                }

                return data_service.async_write(data_sgs, pbas).thenValue([](auto&& werr) {
                    if (werr) { throw std::runtime_error(fmt::format("write failed: {}", werr.message())); }
                });
            })
            .get();
    } catch (const std::exception& e) { err_msg = e.what(); }

    iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
    if (!err_msg.empty()) { return folly::makeUnexpected(err_msg); }
    return folly::Unit();
}

} // namespace hs_debug_inject
} // namespace homeobject
