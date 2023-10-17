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
HSHomeObject::recover_index_table(const homestore::superblk< homestore::index_table_sb >& sb) {
    homestore::BtreeConfig bt_cfg(homestore::hs()->index_service().node_size());
    bt_cfg.m_leaf_node_type = homestore::btree_node_type::FIXED;
    bt_cfg.m_int_node_type = homestore::btree_node_type::FIXED;

    auto uuid_str = boost::uuids::to_string(sb->uuid);
    auto index_table = std::make_shared< homestore::IndexTable< BlobRouteKey, BlobRouteValue > >(sb, bt_cfg);

    // Check if PG is already recovered.
    std::scoped_lock lock_guard(index_lock_);
    auto it = index_table_pg_map_.find(uuid_str);
    if (it != index_table_pg_map_.end()) {
        std::shared_lock lg(_pg_lock);
        auto iter = _pg_map.find(it->second.pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "Unknown PG id");
        // Found a PG, update its index table.
        static_cast< HS_PG* >(iter->second.get())->index_table_ = index_table;
    } else {
        index_table_pg_map_.emplace(uuid_str, PgIndexTable{0, index_table});
    }

    LOGTRACEMOD(blobmgr, "Recovered index table uuid {}", uuid_str);
    return index_table;
}

BlobManager::NullResult HSHomeObject::add_to_index_table(shared< BlobIndexTable > index_table,
                                                         const BlobInfo& blob_info) {
    BlobRouteKey index_key{BlobRoute{blob_info.shard_id, blob_info.blob_id}};
    BlobRouteValue index_value{blob_info.pbas};
    homestore::BtreeSinglePutRequest put_req{&index_key, &index_value,
                                             homestore::btree_put_type::INSERT_ONLY_IF_NOT_EXISTS};
    auto status = index_table->put(put_req);
    if (status != homestore::btree_status_t::success) { return folly::makeUnexpected(BlobError::INDEX_ERROR); }

    return folly::Unit();
}

BlobManager::Result< homestore::MultiBlkId >
HSHomeObject::get_from_index_table(shared< BlobIndexTable > index_table, shard_id_t shard_id, blob_id_t blob_id) const {
    BlobRouteKey index_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue index_value;
    homestore::BtreeSingleGetRequest get_req{&index_key, &index_value};
    auto status = index_table->get(get_req);
    if (status != homestore::btree_status_t::success) {
        LOGERROR("Failed to get from index table {}", index_key.to_string());
        return folly::makeUnexpected(BlobError::INDEX_ERROR);
    }

    return index_value.pbas();
}

void HSHomeObject::print_btree_index(pg_id_t pg_id) {
    shared< BlobIndexTable > index_table;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_id);
        RELEASE_ASSERT (iter != _pg_map.end(), "Unknown PG");
        index_table = static_cast< HS_PG* >(iter->second.get())->index_table_;
        RELEASE_ASSERT(index_table != nullptr, "Index table not intialized");
    }

    LOGINFO("Index UUID {}", boost::uuids::to_string(index_table->uuid()));
    index_table->print_tree();
}

} // namespace homeobject
