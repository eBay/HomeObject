#pragma once

#include <memory>
#include <mutex>

#include <homestore/homestore.hpp>
#include <homestore/superblk_handler.hpp>
#include <homestore/replication/repl_dev.h>
#include "lib/homeobject_impl.hpp"

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

    mutable std::shared_mutex _flying_shard_lock;
    std::map< int64_t, Shard > _flying_shards;

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
#pragma pack()

    struct HS_PG : public PG {
        homestore::superblk< pg_info_superblk > pg_sb_;
        shared< homestore::ReplDev > repl_dev_;

        HS_PG(PGInfo info, shared< homestore::ReplDev > rdev);
        HS_PG(homestore::superblk< pg_info_superblk > const& sb, shared< homestore::ReplDev > rdev);
        virtual ~HS_PG() = default;

        static PGInfo pg_info_from_sb(homestore::superblk< pg_info_superblk > const& sb);
    };

private:
    static homestore::ReplicationService& hs_repl_service() { return homestore::hs()->repl_service(); }

    shard_id_t generate_new_shard_id(pg_id_t pg);
    uint64_t get_sequence_num_from_shard_id(uint64_t shard_id_t);
    std::string serialize_shard(const Shard& shard) const;
    Shard deserialize_shard(const std::string& shard_info_str) const;

    void do_commit_new_shard(const Shard& shard);
    void do_commit_seal_shard(const Shard& shard);
    void register_homestore_metablk_callback();
    void* get_shard_metablk(shard_id_t id);
    void on_pg_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);
    void add_pg_to_map(unique< HS_PG > hs_pg);

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~HSHomeObject();

    void init_homestore();

    static const std::string s_shard_info_sub_type;
    void on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf, size_t size);

    bool precheck_and_decode_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key, std::string* msg);

    bool on_pre_commit_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                 cintrusive< homestore::repl_req_ctx >&);
    void on_rollback_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                               cintrusive< homestore::repl_req_ctx >&);
    void on_shard_message_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                 cintrusive< homestore::repl_req_ctx >& hs_ctx);
};

} // namespace homeobject
