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

BlobManager::NullResult HSHomeObject::add_to_index_table(shared< BlobIndexTable > index_table,
                                                         const BlobInfo& blob_info) {
    BlobRouteKey index_key{BlobRoute{blob_info.shard_id, blob_info.blob_id}};
    BlobRouteValue index_value{blob_info.pbas}, existing_value;
    homestore::BtreeSinglePutRequest put_req{&index_key, &index_value, homestore::btree_put_type::INSERT,
                                             &existing_value};
    auto status = index_table->put(put_req);
    if (status != homestore::btree_status_t::success) {
        if (existing_value.pbas().is_valid() || existing_value.pbas() == tombstone_pbas) {
            // Check if the blob id already exists in the index or its tombstone.
            return folly::Unit();
        }
        LOGE("Failed to put to index table error {}", status);
        return folly::makeUnexpected(BlobError::INDEX_ERROR);
    }

    return folly::Unit();
}

BlobManager::Result< homestore::MultiBlkId >
HSHomeObject::get_blob_from_index_table(shared< BlobIndexTable > index_table, shard_id_t shard_id,
                                        blob_id_t blob_id) const {
    BlobRouteKey index_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue index_value;
    homestore::BtreeSingleGetRequest get_req{&index_key, &index_value};

    if (homestore::btree_status_t::success != index_table->get(get_req)) {
        LOGDEBUG("Failed to get from index table [route={}]", index_key);
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
    }

    // blob get API
    if (const auto& pbas = index_value.pbas(); pbas != tombstone_pbas) return pbas;
    return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
}

BlobManager::Result< homestore::MultiBlkId > HSHomeObject::move_to_tombstone(shared< BlobIndexTable > index_table,
                                                                             const BlobInfo& blob_info) {
    BlobRouteKey index_key{BlobRoute{blob_info.shard_id, blob_info.blob_id}};
    BlobRouteValue index_value_get;
    homestore::BtreeSingleGetRequest get_req{&index_key, &index_value_get};
    auto status = index_table->get(get_req);
    if (status != homestore::btree_status_t::success) {
        LOGE("Failed to get from index table [route={}]", index_key);
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
    }

    BlobRouteValue index_value_put{tombstone_pbas};
    homestore::BtreeSinglePutRequest put_req{&index_key, &index_value_put, homestore::btree_put_type::UPDATE};
    status = index_table->put(put_req);
    if (status != homestore::btree_status_t::success) {
        LOGDEBUG("Failed to move blob to tombstone in index table [route={}]", index_key);
        return folly::makeUnexpected(BlobError::INDEX_ERROR);
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
    index_table->print_tree();
}

} // namespace homeobject
