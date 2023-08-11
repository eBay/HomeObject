#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <utility>

#include <homeobject/homeobject.hpp>
#include <homeobject/blob_manager.hpp>
#include <homeobject/pg_manager.hpp>
#include <homeobject/shard_manager.hpp>

namespace homeobject {
struct BlobRoute {
    shard_id shard;
    blob_id blob;
};

inline std::string toString(BlobRoute const& r) { return fmt::format("{}:{}", r.shard, r.blob); }

inline bool operator<(BlobRoute const& lhs, BlobRoute const& rhs) { return toString(lhs) < toString(rhs); }

inline bool operator==(BlobRoute const& lhs, BlobRoute const& rhs) { return toString(lhs) == toString(rhs); }
} // namespace homeobject

template <>
struct std::hash< homeobject::BlobRoute > {
    std::size_t operator()(homeobject::BlobRoute const& r) const noexcept {
        return std::hash< std::string >()(homeobject::toString(r));
    }
};

namespace homeobject {

class MockHomeObject : public HomeObject,
                       public BlobManager,
                       public PGManager,
                       public ShardManager,
                       public std::enable_shared_from_this< MockHomeObject > {
    /// This simulates the MetaBlkSvc thats used within real HomeObject
    mutable std::mutex _pg_lock;
    std::map< pg_id, std::pair< PGInfo, std::unordered_set< shard_id > > > _pg_map;
    ///

    /// This simulates the IndexSvc thats used within real HomeObject
    mutable std::mutex _shard_lock;
    std::map< shard_id, ShardInfo > _shards;
    shard_id _cur_shard_id{0};
    ///

    /// Simulates the Shard=>Chunk mapping in IndexSvc *and* DataSvc BlkId=>Data
    mutable std::mutex _data_lock;
    std::unordered_map< BlobRoute, Blob > _in_memory_disk;
    blob_id _cur_blob_id{0};
    ///

public:
    explicit MockHomeObject([[maybe_unused]] HomeObject::lookup_cb const& cb = nullptr) {}
    ~MockHomeObject() override = default;

    std::shared_ptr< BlobManager > blob_manager() override { return shared_from_this(); }
    std::shared_ptr< PGManager > pg_manager() override { return shared_from_this(); }
    std::shared_ptr< ShardManager > shard_manager() override { return shared_from_this(); }

    // BlobManager
    folly::Future< std::variant< blob_id, BlobError > > put(shard_id shard, Blob&&) override;
    folly::Future< std::variant< Blob, BlobError > > get(shard_id shard, blob_id const& blob, uint64_t off,
                                                         uint64_t len) const override;
    folly::Future< BlobError > del(shard_id shard, blob_id const& blob) override;

    // PGManager
    folly::Future< PGError > create_pg(PGInfo const& pg_info) override;
    folly::Future< PGError > replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member) override;

    // ShardManager
    folly::Future< ShardManager::info_var > create_shard(pg_id pg_owner, uint64_t size_bytes) override;
    ShardManager::info_var get_shard(shard_id id) const override;
    folly::Future< ShardManager::list_var > list_shards(pg_id id) const override;
    folly::Future< ShardManager::info_var > seal_shard(shard_id id) override;
};

} // namespace homeobject
