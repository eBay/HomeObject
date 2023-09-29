#pragma once

#include <memory>
#include <mutex>

#include <homestore/homestore.hpp>
#include <homestore/superblk_handler.hpp>
#include <homestore/replication/repl_dev.h>

#include "heap_chunk_selector.h"
#include "lib/homeobject_impl.hpp"
#include "replication_message.hpp"

namespace homestore {
struct meta_blk;
}

namespace homeobject {

class HSHomeObject : public HomeObjectImpl {
    /// Overridable Helpers
    ShardManager::Result< ShardInfo > _create_shard(pg_id_t, uint64_t size_bytes) override;
    ShardManager::Result< ShardInfo > _seal_shard(shard_id_t) override;

    BlobManager::Result< blob_id_t > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::Result< Blob > _get_blob(ShardInfo const&, blob_id_t) const override;
    BlobManager::NullResult _del_blob(ShardInfo const&, blob_id_t) override;

    PGManager::NullAsyncResult _create_pg(PGInfo&& pg_info, std::set< std::string, std::less<> > peers) override;
    PGManager::NullAsyncResult _replace_member(pg_id_t id, peer_id_t const& old_member,
                                               PGMember const& new_member) override;

public:
#pragma pack(1)
    struct pg_members {
        static constexpr uint64_t max_name_len = 32;
        peer_id_t id;
        char name[max_name_len];
        int32_t priority{0};
    };

    struct pg_info_superblk {
        pg_id_t id;
        uint32_t num_members;
        peer_id_t replica_set_uuid;
        pg_members members[1]; // ISO C++ forbids zero-size array
    };

    struct shard_info_superblk {
        shard_id_t id;
        pg_id_t placement_group;
        ShardInfo::State state;
        uint64_t created_time;
        uint64_t last_modified_time;
        uint64_t available_capacity_bytes;
        uint64_t total_capacity_bytes;
        uint64_t deleted_capacity_bytes;
        homestore::chunk_num_t chunk_id;
    };
#pragma pack()

    struct HS_PG : public PG {
        homestore::superblk< pg_info_superblk > pg_sb_;
        shared< homestore::ReplDev > repl_dev_;

        HS_PG(PGInfo info, shared< homestore::ReplDev > rdev);
        HS_PG(homestore::superblk< pg_info_superblk > const& sb, shared< homestore::ReplDev > rdev);
        virtual ~HS_PG() = default;

        static PGInfo pg_info_from_sb(homestore::superblk< pg_info_superblk > const& sb);
    };

    struct HS_Shard : public Shard {
        homestore::superblk< shard_info_superblk > sb_;
        HS_Shard(ShardInfo info, homestore::chunk_num_t chunk_id);
        HS_Shard(homestore::superblk< shard_info_superblk > const& sb);
        virtual ~HS_Shard() = default;

        void update_info(const ShardInfo& info);
        void write_sb();
        static ShardInfo shard_info_from_sb(homestore::superblk< shard_info_superblk > const& sb);
    };

private:
    shared< HeapChunkSelector > chunk_selector_;
    std::shared_mutex recovery_mutex_;
    std::map< pg_id_t, std::list< homestore::superblk< shard_info_superblk > > > pending_recovery_shards_;

private:
    static homestore::ReplicationService& hs_repl_service() { return homestore::hs()->repl_service(); }

    void add_pg_to_map(shared< HS_PG > hs_pg);
    shard_id_t generate_new_shard_id(pg_id_t pg);
    uint64_t get_sequence_num_from_shard_id(uint64_t shard_id_t);

    static ShardInfo deserialize_shard_info(const char* shard_info_str, size_t size);
    static std::string serialize_shard_info(const ShardInfo& info);
    void add_new_shard_to_map(ShardPtr shard);
    void update_shard_in_map(const ShardInfo& shard_info);
    void do_shard_message_commit(int64_t lsn, ReplicationMessageHeader& header, homestore::MultiBlkId const& blkids,
                                 sisl::blob value, cintrusive< homestore::repl_req_ctx >& hs_ctx);
    // recover part
    void register_homestore_metablk_callback();
    void on_pg_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);
    void on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf);
    void on_shard_meta_blk_recover_completed(bool success);

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~HSHomeObject();

    void init_homestore();

    void on_shard_message_commit(int64_t lsn, sisl::blob const& header, homestore::MultiBlkId const& blkids,
                                 homestore::ReplDev* repl_dev, cintrusive< homestore::repl_req_ctx >& hs_ctx);

    ShardManager::Result< homestore::chunk_num_t > get_shard_chunk(shard_id_t id) const;

    shared< HeapChunkSelector > chunk_selector() { return chunk_selector_; }
};

} // namespace homeobject
