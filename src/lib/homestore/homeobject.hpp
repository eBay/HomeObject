#pragma once

#include <memory>
#include <mutex>

#include "mocks/repl_service.h"

#include "lib/homeobject_impl.hpp"

namespace homestore {
struct meta_blk;
}

namespace homeobject {

class HSHomeObject : public HomeObjectImpl {
    /// Overridable Helpers
    ShardManager::Result< ShardInfo > _create_shard(pg_id, uint64_t size_bytes) override;
    ShardManager::Result< ShardInfo > _seal_shard(shard_id) override;

    BlobManager::Result< blob_id > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::Result< Blob > _get_blob(ShardInfo const&, blob_id) const override;
    BlobManager::NullResult _del_blob(ShardInfo const&, blob_id) override;
    ///
private:
    bool check_if_pg_exist(pg_id pg) const;
    shard_id generate_new_shard_id(pg_id pg);
    shard_id make_new_shard_id(pg_id pg, uint64_t sequence_num) const;
    uint64_t get_sequence_num_from_shard_id(uint64_t shard_id);
    std::string serialize_shard_info(ShardPtr shard) const;
    ShardPtr deserialize_shard_info(const std::string& shard_info_str) const;
    void do_commit_new_shard(ShardPtr shard_info);
    void do_commit_seal_shard(ShardPtr shard_info);
public:
    using HomeObjectImpl::HomeObjectImpl;
    ~HSHomeObject();

    void init_homestore();

    static const std::string s_shard_info_sub_type;  
    void on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf, size_t size);

    void on_pre_commit_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                 void* user_ctx);
    void on_rollback_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                               void* user_ctx);
    void on_shard_message_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                 void* user_ctx);  
};

} // namespace homeobject
