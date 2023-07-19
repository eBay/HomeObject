#pragma once

#include <memory>
#include <mutex>
#include <set>

#include "mock_blob_manager.hpp"
#include "mock_pg_manager.hpp"
#include "mock_shard_manager.hpp"

namespace homeobject {

class MockHomeObject : public BlobManager,
                   public PGManager,
                   public ShardManager,
		   public std::enable_shared_from_this< MockHomeObject > {
			   std::mutex _pg_lock;
			   std::set< PGInfo > _pgs;

public:
    MockHomeObject() = default;
    ~MockHomeObject() override = default;

    std::shared_ptr< BlobManager > blob_manager() { return shared_from_this(); }
    std::shared_ptr< PGManager > pg_manager() { return shared_from_this(); }
    std::shared_ptr< ShardManager > shard_manager() { return shared_from_this(); }

    // BlobManager
    void put(shard_id shard, Blob const&, id_cb cb) override;
    void get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len, get_cb) const override;
    void del(shard_id shard, blob_id const& blob, BlobManager::ok_cb cb) override;

    // PGManager
    void create_pg(PGInfo const& pg_info, PGManager::ok_cb cb) override;
    void replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member, PGManager::ok_cb cb) override;

    // ShardManager
    void create_shard(pg_id pg_owner, uint64_t size_mb, info_cb cb) override;
    void get_shard(shard_id id, info_cb cb) const override;
    void list_shards(pg_id id, info_cb cb) const override;
    void seal_shard(shard_id id, ShardManager::ok_cb cb) override;
};

} // namespace homeobject
