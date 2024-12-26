#include <boost/uuid/uuid.hpp>
#include <boost/uuid/random_generator.hpp>
#include "hs_homeobject.hpp"
#include "index_kv.hpp"

SISL_LOGGING_DECL(blobmgr)

namespace homeobject {

std::shared_ptr< BlobIndexTable > HSHomeObject::create_index_table() {
    homestore::uuid_t uuid = boost::uuids::random_generator()();
    homestore::uuid_t parent_uuid = boost::uuids::random_generator()();
    std::string uuid_str = boost::uuids::to_string(uuid);
    homestore::BtreeConfig bt_cfg(homestore::hs()->index_service().node_size());
    bt_cfg.m_leaf_node_type = homestore::btree_node_type::FIXED;
    bt_cfg.m_int_node_type = homestore::btree_node_type::FIXED;

    auto index_table =
        std::make_shared< homestore::IndexTable< BlobRouteKey, BlobRouteValue > >(uuid, parent_uuid, 0, bt_cfg);

    return index_table;
}

std::shared_ptr< BlobIndexTable >
HSHomeObject::recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb) {
    homestore::BtreeConfig bt_cfg(homestore::hs()->index_service().node_size());
    bt_cfg.m_leaf_node_type = homestore::btree_node_type::FIXED;
    bt_cfg.m_int_node_type = homestore::btree_node_type::FIXED;

    auto uuid_str = boost::uuids::to_string(sb->uuid);
    auto index_table = std::make_shared< homestore::IndexTable< BlobRouteKey, BlobRouteValue > >(std::move(sb), bt_cfg);

    // Check if PG is already recovered.
    std::scoped_lock lock_guard(index_lock_);
    auto it = index_table_pg_map_.find(uuid_str);
    RELEASE_ASSERT(it == index_table_pg_map_.end(), "PG should be recovered after IndexTable");
    index_table_pg_map_.emplace(uuid_str, PgIndexTable{0, index_table});

    LOGTRACEMOD(blobmgr, "Recovered index table uuid {}", uuid_str);
    return index_table;
}

//The bool result indicates if the blob already exists, but if the existing pbas is the same as the new pbas, it will return homestore::btree_status_t::success.
std::pair< bool, homestore::btree_status_t > HSHomeObject::add_to_index_table(shared< BlobIndexTable > index_table,
                                                                              const BlobInfo& blob_info) {
    BlobRouteKey index_key{BlobRoute{blob_info.shard_id, blob_info.blob_id}};
    BlobRouteValue index_value{blob_info.pbas}, existing_value;
    homestore::BtreeSinglePutRequest put_req{&index_key, &index_value, homestore::btree_put_type::INSERT,
                                             &existing_value};
    auto status = index_table->put(put_req);
    if (status != homestore::btree_status_t::success) {
        if ((existing_value.pbas().is_valid() && existing_value.pbas() == blob_info.pbas)
            || existing_value.pbas() == tombstone_pbas) {
            LOGT(
                "blob already exists, but existing pbas is the same as the new pbas or has been deleted, ignore it, blob_id={}, pbas={}, status={}",
                blob_info.blob_id, blob_info.pbas.to_string(), status);
            return {true, homestore::btree_status_t::success};
        }
        if (existing_value.pbas().is_valid()) {
            LOGE(
                "blob already exists, and conflict occurs, blob_id={}, existing pbas={}, new pbas={}, status={}",
                blob_info.blob_id, existing_value.pbas().to_string(), blob_info.pbas.to_string(), status);
            return {true, status};
        }
        LOGE("Failed to put to index table error {}", status);
    }

    return {false, status};
}

BlobManager::Result< homestore::MultiBlkId >
HSHomeObject::get_blob_from_index_table(shared< BlobIndexTable > index_table, shard_id_t shard_id,
                                        blob_id_t blob_id) const {
    BlobRouteKey index_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue index_value;
    homestore::BtreeSingleGetRequest get_req{&index_key, &index_value};

    if (homestore::btree_status_t::success != index_table->get(get_req)) {
        LOGDEBUG("Failed to get from index table [route={}]", index_key);
        return folly::makeUnexpected(BlobError(BlobErrorCode::UNKNOWN_BLOB));
    }

    // blob get API
    if (const auto& pbas = index_value.pbas(); pbas != tombstone_pbas) return pbas;
    return folly::makeUnexpected(BlobError(BlobErrorCode::UNKNOWN_BLOB));
}

BlobManager::Result< homestore::MultiBlkId > HSHomeObject::move_to_tombstone(shared< BlobIndexTable > index_table,
                                                                             const BlobInfo& blob_info) {
    BlobRouteKey index_key{BlobRoute{blob_info.shard_id, blob_info.blob_id}};
    BlobRouteValue index_value_get;
    homestore::BtreeSingleGetRequest get_req{&index_key, &index_value_get};
    auto status = index_table->get(get_req);
    if (status != homestore::btree_status_t::success) {
        LOGE("Failed to get from index table [route={}]", index_key);
        return folly::makeUnexpected(BlobError(BlobErrorCode::UNKNOWN_BLOB));
    }

    BlobRouteValue index_value_put{tombstone_pbas};
    homestore::BtreeSinglePutRequest put_req{&index_key, &index_value_put, homestore::btree_put_type::UPDATE};
    status = index_table->put(put_req);
    if (status != homestore::btree_status_t::success) {
        LOGDEBUG("Failed to move blob to tombstone in index table [route={}]", index_key);
        return folly::makeUnexpected(BlobError(BlobErrorCode::INDEX_ERROR));
    }

    return index_value_get.pbas();
}

void HSHomeObject::print_btree_index(pg_id_t pg_id) {
    shared< BlobIndexTable > index_table;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "Unknown PG");
        index_table = static_cast< HS_PG* >(iter->second.get())->index_table_;
        RELEASE_ASSERT(index_table != nullptr, "Index table not intialized");
    }

    LOGI("Index UUID {}", boost::uuids::to_string(index_table->uuid()));
    index_table->dump_tree_to_file();
}

shared< BlobIndexTable > HSHomeObject::get_index_table(pg_id_t pg_id) {
    std::shared_lock lock_guard(_pg_lock);
    auto iter = _pg_map.find(pg_id);
    if (iter == _pg_map.end()) {
        LOGW("PG not found for pg_id={} when getting inde table", pg_id);
        return nullptr;
    }
    auto hs_pg = static_cast< HSHomeObject::HS_PG* >(iter->second.get());
    RELEASE_ASSERT(hs_pg->index_table_ != nullptr, "Index table not found for PG");
    return hs_pg->index_table_;
}

BlobManager::Result< std::vector< HSHomeObject::BlobInfo > >
HSHomeObject::query_blobs_in_shard(pg_id_t pg_id, uint64_t cur_shard_seq_num, blob_id_t start_blob_id,
                                   uint64_t max_num_in_batch) {
    // Query all blobs from start_blob_id to the maximum blob_id value.
    std::vector< std::pair< BlobRouteKey, BlobRouteValue > > out_vector;
    auto shard_id = make_new_shard_id(pg_id, cur_shard_seq_num);
    auto start_key = BlobRouteKey{BlobRoute{shard_id, start_blob_id}};
    auto end_key = BlobRouteKey{BlobRoute{shard_id, std::numeric_limits< uint64_t >::max()}};
    homestore::BtreeQueryRequest< BlobRouteKey > query_req{
        homestore::BtreeKeyRange< BlobRouteKey >{std::move(start_key), true /* inclusive */, std::move(end_key),
                                                 true /* inclusive */},
        homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY, static_cast< uint32_t >(max_num_in_batch)};
    auto index_table = get_index_table(pg_id);
    auto const ret = index_table->query(query_req, out_vector);
    if (ret != homestore::btree_status_t::success && ret != homestore::btree_status_t::has_more) {
        LOGE("Failed to query blobs in index table for ret={} shard={} start_blob_id={}", ret, shard_id, start_blob_id);
        return folly::makeUnexpected(BlobErrorCode::INDEX_ERROR);
    }

    std::vector< BlobInfo > blob_info_vec;
    blob_info_vec.reserve(out_vector.size());
    for (auto& [r, v] : out_vector) {
        blob_info_vec.push_back(BlobInfo{r.key().shard, r.key().blob, v.pbas()});
    }

    return blob_info_vec;
}

} // namespace homeobject
