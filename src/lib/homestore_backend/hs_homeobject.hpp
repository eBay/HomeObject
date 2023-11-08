#pragma once

#include <memory>
#include <mutex>

#include <homestore/homestore.hpp>
#include <homestore/index/index_table.hpp>
#include <homestore/superblk_handler.hpp>
#include <homestore/replication/repl_dev.h>

#include "heap_chunk_selector.h"
#include "lib/homeobject_impl.hpp"
#include "replication_message.hpp"

namespace homestore {
struct meta_blk;
class IndexTableBase;
} // namespace homestore

namespace homeobject {

class BlobRouteKey;
class BlobRouteValue;
using BlobIndexTable = homestore::IndexTable< BlobRouteKey, BlobRouteValue >;

class HSHomeObject : public HomeObjectImpl {
    /// NOTE: Be wary to change these as they effect on-disk format!
    inline static auto const _svc_meta_name = std::string("HomeObject");
    inline static auto const _pg_meta_name = std::string("PGManager");
    inline static auto const _shard_meta_name = std::string("ShardManager");
    ///

    /// Overridable Helpers
    ShardManager::AsyncResult< ShardInfo > _create_shard(pg_id_t, uint64_t size_bytes) override;
    ShardManager::AsyncResult< ShardInfo > _seal_shard(ShardInfo const&) override;

    BlobManager::AsyncResult< blob_id_t > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::AsyncResult< Blob > _get_blob(ShardInfo const&, blob_id_t, uint64_t off = 0,
                                               uint64_t len = 0) const override;
    BlobManager::NullAsyncResult _del_blob(ShardInfo const&, blob_id_t) override;

    PGManager::NullAsyncResult _create_pg(PGInfo&& pg_info, std::set< std::string, std::less<> > peers) override;
    PGManager::NullAsyncResult _replace_member(pg_id_t id, peer_id_t const& old_member,
                                               PGMember const& new_member) override;

    // Mapping from index table uuid to pg id.
    std::shared_mutex index_lock_;
    struct PgIndexTable {
        pg_id_t pg_id;
        std::shared_ptr< BlobIndexTable > index_table;
    };
    std::unordered_map< std::string, PgIndexTable > index_table_pg_map_;

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
        homestore::uuid_t index_table_uuid;
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

        std::optional< homestore::chunk_num_t > any_allocated_chunk_id_{};
        std::shared_ptr< BlobIndexTable > index_table_;

        HS_PG(PGInfo info, shared< homestore::ReplDev > rdev, shared< BlobIndexTable > index_table);
        HS_PG(homestore::superblk< pg_info_superblk > const& sb, shared< homestore::ReplDev > rdev);
        virtual ~HS_PG() = default;

        static PGInfo pg_info_from_sb(homestore::superblk< pg_info_superblk > const& sb);
    };

    struct HS_Shard : public Shard {
        homestore::superblk< shard_info_superblk > sb_;
        HS_Shard(ShardInfo info, homestore::chunk_num_t chunk_id);
        HS_Shard(homestore::superblk< shard_info_superblk > const& sb);
        ~HS_Shard() override = default;

        void update_info(const ShardInfo& info);
        void write_sb();
        static ShardInfo shard_info_from_sb(homestore::superblk< shard_info_superblk > const& sb);
    };

#pragma pack(1)
    // Every blob payload stored in disk as blob header | blob data | blob metadata(optional) | padding.
    // Padding of zeroes is added to make sure the whole payload be aligned to device block size.
    struct BlobHeader {
        static constexpr uint64_t blob_max_hash_len = 32;
        static constexpr uint8_t blob_header_version = 0x01;
        static constexpr uint64_t blob_header_magic = 0x21fdffdba8d68fc6; // echo "BlobHeader" | md5sum

        enum class HashAlgorithm : uint8_t {
            NONE = 0,
            CRC32 = 1,
            MD5 = 2,
            SHA1 = 3,
        };

        uint64_t magic{blob_header_magic};
        uint8_t version{blob_header_version};
        HashAlgorithm hash_algorithm;
        uint8_t hash[blob_max_hash_len]{};
        shard_id_t shard_id;
        uint32_t blob_size{};
        uint64_t object_offset{};   // Offset of this blob in the object. Provided by GW.
        uint32_t user_key_offset{}; // Offset of metadata stored after the blob data.
        uint32_t user_key_size{};

        bool valid() const { return magic == blob_header_magic || version <= blob_header_version; }
        std::string to_string() {
            return fmt::format("magic={:#x} version={} shard={} blob_size={} user_size={} algo={} hash={}\n", magic,
                               version, shard_id, blob_size, user_key_size, (uint8_t)hash_algorithm,
                               spdlog::to_hex(hash, hash + blob_max_hash_len));
        }
    };
#pragma pack()

    struct BlobInfo {
        shard_id_t shard_id;
        blob_id_t blob_id;
        homestore::MultiBlkId pbas;
    };

    enum class BlobState : uint8_t {
        ALIVE = 0,
        TOMBSTONE = 1,
        ALL = 2,
    };

    inline const static homestore::MultiBlkId tombstone_pbas{0, 0, 0};

private:
    shared< HeapChunkSelector > chunk_selector_;

private:
    static homestore::ReplicationService& hs_repl_service() { return homestore::hs()->repl_service(); }

    void add_pg_to_map(unique< HS_PG > hs_pg);
    shard_id_t generate_new_shard_id(pg_id_t pg);
    uint64_t get_sequence_num_from_shard_id(uint64_t shard_id_t);

    static ShardInfo deserialize_shard_info(const char* shard_info_str, size_t size);
    static std::string serialize_shard_info(const ShardInfo& info);
    void add_new_shard_to_map(ShardPtr&& shard);
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
    ~HSHomeObject() override;

    void init_homestore();

    void on_shard_message_commit(int64_t lsn, sisl::blob const& header, homestore::MultiBlkId const& blkids,
                                 homestore::ReplDev* repl_dev, cintrusive< homestore::repl_req_ctx >& hs_ctx);

    std::optional< homestore::chunk_num_t > get_shard_chunk(shard_id_t id) const;

    std::optional< homestore::chunk_num_t > get_any_chunk_id(pg_id_t const pg);

    shared< HeapChunkSelector > chunk_selector() { return chunk_selector_; }

    bool on_pre_commit_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                 cintrusive< homestore::repl_req_ctx >&);
    void on_rollback_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                               cintrusive< homestore::repl_req_ctx >&);
    void on_shard_message_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                 cintrusive< homestore::repl_req_ctx >& hs_ctx);

    // Blob manager related.
    void on_blob_put_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                            const homestore::MultiBlkId& pbas, cintrusive< homestore::repl_req_ctx >& hs_ctx);
    void on_blob_del_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                            cintrusive< homestore::repl_req_ctx >& hs_ctx);
    homestore::blk_alloc_hints blob_put_get_blk_alloc_hints(sisl::blob const& header,
                                                            cintrusive< homestore::repl_req_ctx >& ctx);
    void compute_blob_payload_hash(BlobHeader::HashAlgorithm algorithm, const uint8_t* blob_bytes, size_t blob_size,
                                   const uint8_t* user_key_bytes, size_t user_key_size, uint8_t* hash_bytes,
                                   size_t hash_len) const;

    std::shared_ptr< BlobIndexTable > create_index_table();

    std::shared_ptr< BlobIndexTable > recover_index_table(const homestore::superblk< homestore::index_table_sb >& sb);

    BlobManager::NullResult add_to_index_table(shared< BlobIndexTable > index_table, const BlobInfo& blob_info);

    BlobManager::Result< homestore::MultiBlkId >
    get_blob_from_index_table(shared< BlobIndexTable > index_table, shard_id_t shard_id, blob_id_t blob_id) const;

    BlobManager::Result< homestore::MultiBlkId > move_to_tombstone(shared< BlobIndexTable > index_table,
                                                                   const BlobInfo& blob_info);
    void print_btree_index(pg_id_t pg_id);
};

class BlobIndexServiceCallbacks : public homestore::IndexServiceCallbacks {
public:
    BlobIndexServiceCallbacks(HSHomeObject* home_object) : home_object_(home_object) {}
    std::shared_ptr< homestore::IndexTableBase >
    on_index_table_found(const homestore::superblk< homestore::index_table_sb >& sb) override {
        LOGI("Recovered index table to index service");
        return home_object_->recover_index_table(sb);
    }

private:
    HSHomeObject* home_object_;
};

} // namespace homeobject
