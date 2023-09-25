#pragma once

#include <memory>
#include <mutex>

#include "mocks/repl_service.h"
#include "lib/homeobject_impl.hpp"
#include "heap_chunk_selector.h"

#include <homestore/homestore.hpp>
#include <homestore/replication/repl_dev.h>

namespace homestore {
struct meta_blk;
}

namespace homeobject {

class HSHomeObject : public HomeObjectImpl {
    std::shared_ptr< HeapChunkSelector > _chunk_selector;

private:
    /// Overridable Helpers
    PGManager::NullAsyncResult _create_pg(PGInfo&& pg_info, std::set< std::string, std::less<> >&& peers) override;

    ShardManager::Result< ShardInfo > _create_shard(pg_id, uint64_t size_bytes) override;
    ShardManager::Result< ShardInfo > _seal_shard(shard_id) override;

    BlobManager::Result< blob_id > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::Result< Blob > _get_blob(ShardInfo const&, blob_id) const override;
    BlobManager::NullResult _del_blob(ShardInfo const&, blob_id) override;
    ///

private:
    shard_id generate_new_shard_id(pg_id pg);
    uint64_t get_sequence_num_from_shard_id(uint64_t shard_id);
    std::string serialize_shard(const Shard& shard) const;
    Shard deserialize_shard(const char* json_str, size_t str_size) const;
    void do_commit_new_shard(const Shard& shard);
    void do_commit_seal_shard(const Shard& shard);
    void register_homestore_metablk_callback();

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~HSHomeObject();

    void init_homestore();

    void on_shard_message_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                 homestore::MultiBlkId const& blkids, void* user_ctx,
                                 homestore::ReplDev& repl_dev);

    void* get_shard_metablk(shard_id id) const;

    ShardManager::Result< uint16_t > get_shard_chunk(shard_id id) const;

    // Recovery part
    static const std::string s_shard_info_sub_type;
    void on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf, size_t size);
    void on_shard_meta_blk_recover_completed(bool success);
};

} // namespace homeobject
