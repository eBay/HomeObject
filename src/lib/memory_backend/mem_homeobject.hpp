#pragma once

#include <atomic>
#include <utility>

#include <folly/concurrency/ConcurrentHashMap.h>
#include "lib/homeobject_impl.hpp"
#include "lib/blob_route.hpp"

namespace homeobject {

///
// Used to TombStone Blob's in the Index to defer for GC.
ENUM(BlobState, uint8_t, ALIVE = 0, DELETED);

struct BlobExt {
    BlobState state_{BlobState::DELETED};
    Blob* blob_;

    explicit operator bool() const { return state_ == BlobState::ALIVE; }
    bool operator==(const BlobExt& rhs) const { return blob_ == rhs.blob_; }
};

struct ShardIndex {
    folly::ConcurrentHashMap< BlobRoute, BlobExt > btree_;
    std::atomic< blob_id_t > shard_seq_num_{0ull};
    ~ShardIndex();
};

class MemoryHomeObject : public HomeObjectImpl {
    /// Simulates the Shard=>Chunk mapping in IndexSvc
    using index_svc = folly::ConcurrentHashMap< shard_id_t, std::unique_ptr< ShardIndex > >;
    index_svc index_;
    ///

    /// Helpers
    // ShardManager
    ShardManager::AsyncResult< ShardInfo > _create_shard(pg_id_t, uint64_t size_bytes) override;
    ShardManager::Result< ShardInfo > _seal_shard(shard_id_t) override;

    // BlobManager
    BlobManager::AsyncResult< blob_id_t > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::AsyncResult< Blob > _get_blob(ShardInfo const&, blob_id_t, uint64_t off = 0,
                                          uint64_t len = 0) const override;
    BlobManager::NullAsyncResult _del_blob(ShardInfo const&, blob_id_t) override;
    ///

    // PGManager
    PGManager::NullAsyncResult _create_pg(PGInfo&& pg_info, std::set< std::string, std::less<> > peers) override;
    PGManager::NullAsyncResult _replace_member(pg_id_t id, peer_id_t const& old_member,
                                               PGMember const& new_member) override;

    ShardIndex& _find_index(shard_id_t) const;

public:
    MemoryHomeObject(std::weak_ptr< HomeObjectApplication >&& application);
    ~MemoryHomeObject() override = default;
};

} // namespace homeobject
