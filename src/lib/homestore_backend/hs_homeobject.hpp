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
class HttpManager;

static constexpr uint64_t io_align{512};
PGError toPgError(homestore::ReplServiceError const&);
BlobError toBlobError(homestore::ReplServiceError const&);
ShardError toShardError(homestore::ReplServiceError const&);

class HSHomeObject : public HomeObjectImpl {
    /// NOTE: Be wary to change these as they effect on-disk format!
    inline static auto const _svc_meta_name = std::string("HomeObject");
    inline static auto const _pg_meta_name = std::string("PGManager");
    inline static auto const _shard_meta_name = std::string("ShardManager");
    static constexpr uint32_t _data_block_size = 1024;
    ///

    /// Overridable Helpers
    ShardManager::AsyncResult< ShardInfo > _create_shard(pg_id_t, uint64_t size_bytes) override;
    ShardManager::AsyncResult< ShardInfo > _seal_shard(ShardInfo const&) override;

    BlobManager::AsyncResult< blob_id_t > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::AsyncResult< Blob > _get_blob(ShardInfo const&, blob_id_t, uint64_t off = 0,
                                               uint64_t len = 0) const override;
    BlobManager::NullAsyncResult _del_blob(ShardInfo const&, blob_id_t) override;

    PGManager::NullAsyncResult _create_pg(PGInfo&& pg_info, std::set< peer_id_t > const& peers) override;
    PGManager::NullAsyncResult _replace_member(pg_id_t id, peer_id_t const& old_member,
                                               PGMember const& new_member) override;

    bool _get_stats(pg_id_t id, PGStats& stats) const override;
    void _get_pg_ids(std::vector< pg_id_t >& pg_ids) const override;

    HomeObjectStats _get_stats() const override;

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
        blob_id_t blob_sequence_num;
        uint64_t active_blob_count;        // Total number of active blobs
        uint64_t tombstone_blob_count;     // Total number of tombstones
        uint64_t total_occupied_blk_count; // Total number of occupied blocks
        pg_members members[1];             // ISO C++ forbids zero-size array

        uint32_t size() const { return sizeof(pg_info_superblk) + ((num_members - 1) * sizeof(pg_members)); }
        static std::string name() { return _pg_meta_name; }

        pg_info_superblk() = default;
        pg_info_superblk(pg_info_superblk const& rhs) { *this = rhs; }

        pg_info_superblk& operator=(pg_info_superblk const& rhs) {
            id = rhs.id;
            num_members = rhs.num_members;
            replica_set_uuid = rhs.replica_set_uuid;
            index_table_uuid = rhs.index_table_uuid;
            blob_sequence_num = rhs.blob_sequence_num;
            memcpy(members, rhs.members, sizeof(pg_members) * num_members);
            return *this;
        }

        void copy(pg_info_superblk const& rhs) { *this = rhs; }
    };

    struct DataHeader {
        static constexpr uint8_t data_header_version = 0x01;
        static constexpr uint64_t data_header_magic = 0x21fdffdba8d68fc6; // echo "BlobHeader" | md5sum

        enum class data_type_t : uint32_t { SHARD_INFO = 1, BLOB_INFO = 2 };

    public:
        bool valid() const { return ((magic == data_header_magic) && (version <= data_header_version)); }

    public:
        uint64_t magic{data_header_magic};
        uint8_t version{data_header_version};
        data_type_t type{data_type_t::BLOB_INFO};
    };

    struct shard_info_superblk : public DataHeader {
        ShardInfo info;
        homestore::chunk_num_t chunk_id;
    };
#pragma pack()

public:
    class MyCPCallbacks : public homestore::CPCallbacks {
    public:
        MyCPCallbacks(HSHomeObject& ho) : home_obj_{ho} {};
        virtual ~MyCPCallbacks() = default;

    public:
        std::unique_ptr< homestore::CPContext > on_switchover_cp(homestore::CP* cur_cp, homestore::CP* new_cp) override;
        folly::Future< bool > cp_flush(homestore::CP* cp) override;
        void cp_cleanup(homestore::CP* cp) override;
        int cp_progress_percent() override;

    private:
        HSHomeObject& home_obj_;
    };

    struct HS_PG : public PG {
        struct PGMetrics : public sisl::MetricsGroup {
        public:
            PGMetrics(HS_PG const& pg) : sisl::MetricsGroup{"PG", std::to_string(pg.pg_info_.id)}, pg_(pg) {
                // We use replica_set_uuid instead of pg_id for metrics to make it globally unique to allow aggregating
                // across multiple nodes
                REGISTER_GAUGE(shard_count, "Number of shards");
                REGISTER_GAUGE(open_shard_count, "Number of open shards");
                REGISTER_GAUGE(active_blob_count, "Number of valid blobs present");
                REGISTER_GAUGE(tombstone_blob_count, "Number of tombstone blobs which can be garbage collected");
                REGISTER_GAUGE(total_occupied_space,
                               "Total Size occupied (including padding, user_key, blob) rounded to block size");
                REGISTER_COUNTER(total_user_key_size, "Total user key size provided",
                                 sisl::_publish_as::publish_as_gauge);

                REGISTER_HISTOGRAM(blobs_per_shard,
                                   "Distribution of blobs per shard"); // TODO: Add a bucket for blob sizes
                REGISTER_HISTOGRAM(actual_blob_size, "Distribution of actual blob sizes");

                register_me_to_farm();
                attach_gather_cb(std::bind(&PGMetrics::on_gather, this));
                blk_size = pg_.repl_dev_->get_blk_size();
            }
            ~PGMetrics() { deregister_me_from_farm(); }
            PGMetrics(const PGMetrics&) = delete;
            PGMetrics(PGMetrics&&) noexcept = delete;
            PGMetrics& operator=(const PGMetrics&) = delete;
            PGMetrics& operator=(PGMetrics&&) noexcept = delete;

            void on_gather() {
                GAUGE_UPDATE(*this, shard_count, pg_.total_shards());
                GAUGE_UPDATE(*this, open_shard_count, pg_.open_shards());
                GAUGE_UPDATE(*this, active_blob_count,
                             pg_.durable_entities().active_blob_count.load(std::memory_order_relaxed));
                GAUGE_UPDATE(*this, tombstone_blob_count,
                             pg_.durable_entities().tombstone_blob_count.load(std::memory_order_relaxed));
                GAUGE_UPDATE(*this, total_occupied_space,
                             pg_.durable_entities().total_occupied_blk_count.load(std::memory_order_relaxed) *
                                 blk_size);
            }

        private:
            HS_PG const& pg_;
            uint32_t blk_size;
        };

    public:
        homestore::superblk< pg_info_superblk > pg_sb_;
        shared< homestore::ReplDev > repl_dev_;
        std::optional< homestore::chunk_num_t > any_allocated_chunk_id_{};
        std::shared_ptr< BlobIndexTable > index_table_;
        PGMetrics metrics_;

        HS_PG(PGInfo info, shared< homestore::ReplDev > rdev, shared< BlobIndexTable > index_table);
        HS_PG(homestore::superblk< pg_info_superblk >&& sb, shared< homestore::ReplDev > rdev);
        ~HS_PG() override = default;

        static PGInfo pg_info_from_sb(homestore::superblk< pg_info_superblk > const& sb);

        ///////////////// PG stats APIs /////////////////
        /// Note: Caller needs to hold the _pg_lock before calling these apis
        /**
         * Returns the total number of created shards on this PG.
         * It is caller's responsibility to hold the _pg_lock.
         */
        uint32_t total_shards() const;

        /**
         * Returns the number of open shards on this PG.
         */
        uint32_t open_shards() const;

        /**
         * Retrieves the device hint associated with this PG(if any shard is created).
         *
         * @param selector The HeapChunkSelector object.
         * @return An optional uint32_t value representing the device hint, or std::nullopt if no hint is available.
         */
        std::optional< uint32_t > dev_hint(cshared< HeapChunkSelector >) const;
    };

    struct HS_Shard : public Shard {
        homestore::superblk< shard_info_superblk > sb_;
        HS_Shard(ShardInfo info, homestore::chunk_num_t chunk_id);
        HS_Shard(homestore::superblk< shard_info_superblk >&& sb);
        ~HS_Shard() override = default;

        void update_info(const ShardInfo& info);
        auto chunk_id() const { return sb_->chunk_id; }
    };

#pragma pack(1)
    // Every blob payload stored in disk as blob header | blob data | blob metadata(optional) | padding.
    // Padding of zeroes is added to make sure the whole payload be aligned to device block size.
    struct BlobHeader : public DataHeader {
        static constexpr uint64_t blob_max_hash_len = 32;

        enum class HashAlgorithm : uint8_t {
            NONE = 0,
            CRC32 = 1,
            MD5 = 2,
            SHA1 = 3,
        };

        HashAlgorithm hash_algorithm;
        uint8_t hash[blob_max_hash_len]{};
        shard_id_t shard_id;
        blob_id_t blob_id;
        uint32_t blob_size;
        uint64_t object_offset; // Offset of this blob in the object. Provided by GW.
        uint32_t data_offset;   // Offset of actual data blob stored after the metadata.
        uint32_t user_key_size; // Actual size of the user key.

        std::string to_string() const {
            return fmt::format("magic={:#x} version={} shard={:#x} blob_size={} user_size={} algo={} hash={:np}\n",
                               magic, version, shard_id, blob_size, user_key_size, (uint8_t)hash_algorithm,
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
    unique< HttpManager > http_mgr_;
    bool recovery_done_{false};

    static constexpr size_t max_zpad_bufs = _data_block_size / io_align;
    std::array< sisl::io_blob_safe, max_zpad_bufs > zpad_bufs_; // Zero padded buffers for blob payload.

private:
    static homestore::ReplicationService& hs_repl_service() { return homestore::hs()->repl_service(); }

    // create pg related
    PGManager::NullAsyncResult do_create_pg(cshared< homestore::ReplDev > repl_dev, PGInfo&& pg_info);
    static std::string serialize_pg_info(const PGInfo& info);
    static PGInfo deserialize_pg_info(const unsigned char* pg_info_str, size_t size);
    void add_pg_to_map(unique< HS_PG > hs_pg);

    // create shard related
    shard_id_t generate_new_shard_id(pg_id_t pg);
    uint64_t get_sequence_num_from_shard_id(uint64_t shard_id_t) const;

    static ShardInfo deserialize_shard_info(const char* shard_info_str, size_t size);
    static std::string serialize_shard_info(const ShardInfo& info);
    void add_new_shard_to_map(ShardPtr&& shard);
    void update_shard_in_map(const ShardInfo& shard_info);

    // recover part
    void register_homestore_metablk_callback();
    void on_pg_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);
    void on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf);
    void on_shard_meta_blk_recover_completed(bool success);

    void persist_pg_sb();

    // helpers
    DevType get_device_type(string const& devname);

public:
    using HomeObjectImpl::HomeObjectImpl;
    HSHomeObject();
    ~HSHomeObject() override;

    /**
     * Initializes the homestore.
     */
    void init_homestore();

#if 0
    /**
     * @brief Initializes a timer thread.
     *
     */
    void init_timer_thread();
#endif

    /**
     * @brief Initializes the checkpinting for the home object.
     *
     */
    void init_cp();

    /**
     * @brief Callback function invoked when createPG message is committed on a shard.
     *
     * @param lsn The logical sequence number of the message.
     * @param header The header of the message.
     * @param repl_dev The replication device.
     * @param hs_ctx The replication request context.
     */
    void on_create_pg_message_commit(int64_t lsn, sisl::blob const& header, shared< homestore::ReplDev > repl_dev,
                                     cintrusive< homestore::repl_req_ctx >& hs_ctx);

    /**
     * @brief Callback function invoked when a message is committed on a shard.
     *
     * @param lsn The logical sequence number of the message.
     * @param header The header of the message.
     * @param blkids The IDs of the blocks associated with the message.
     * @param repl_dev The replication device.
     * @param hs_ctx The replication request context.
     */
    void on_shard_message_commit(int64_t lsn, sisl::blob const& header, homestore::MultiBlkId const& blkids,
                                 shared< homestore::ReplDev > repl_dev, cintrusive< homestore::repl_req_ctx >& hs_ctx);

    bool on_shard_message_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                     cintrusive< homestore::repl_req_ctx >&);
    void on_shard_message_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                   cintrusive< homestore::repl_req_ctx >&);

    /**
     * @brief Retrieves the chunk number associated with the given shard ID.
     *
     * @param id The ID of the shard to retrieve the chunk number for.
     * @return An optional chunk number if the shard ID is valid, otherwise an empty optional.
     */
    std::optional< homestore::chunk_num_t > get_shard_chunk(shard_id_t id) const;

    /**
     * @brief recover PG and shard from the superblock.
     *
     */
    void on_replica_restart();

    /**
     * @brief Returns any chunk number for the given pg ID.
     *
     * @param pg The pg ID to get the chunk number for.
     * @return A tuple of <if pg exist, if shard exist, chunk number if both exist>.
     */
    std::tuple< bool, bool, homestore::chunk_num_t > get_any_chunk_id(pg_id_t pg);

    cshared< HeapChunkSelector > chunk_selector() const { return chunk_selector_; }

    // Blob manager related.
    void on_blob_put_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                            const homestore::MultiBlkId& pbas, cintrusive< homestore::repl_req_ctx >& hs_ctx);
    void on_blob_del_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                            cintrusive< homestore::repl_req_ctx >& hs_ctx);
    homestore::ReplResult< homestore::blk_alloc_hints >
    blob_put_get_blk_alloc_hints(sisl::blob const& header, cintrusive< homestore::repl_req_ctx >& ctx);
    void compute_blob_payload_hash(BlobHeader::HashAlgorithm algorithm, const uint8_t* blob_bytes, size_t blob_size,
                                   const uint8_t* user_key_bytes, size_t user_key_size, uint8_t* hash_bytes,
                                   size_t hash_len) const;

    std::shared_ptr< BlobIndexTable > recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb);

private:
    std::shared_ptr< BlobIndexTable > create_index_table();

    std::pair< bool, homestore::btree_status_t > add_to_index_table(shared< BlobIndexTable > index_table,
                                                                    const BlobInfo& blob_info);

    BlobManager::Result< homestore::MultiBlkId >
    get_blob_from_index_table(shared< BlobIndexTable > index_table, shard_id_t shard_id, blob_id_t blob_id) const;

    BlobManager::Result< homestore::MultiBlkId > move_to_tombstone(shared< BlobIndexTable > index_table,
                                                                   const BlobInfo& blob_info);
    void print_btree_index(pg_id_t pg_id);

    // Zero padding buffer related.
    size_t max_pad_size() const;
    sisl::io_blob_safe& get_pad_buf(uint32_t pad_len);

    // void trigger_timed_events();
};

class BlobIndexServiceCallbacks : public homestore::IndexServiceCallbacks {
public:
    BlobIndexServiceCallbacks(HSHomeObject* home_object) : home_object_(home_object) {}
    std::shared_ptr< homestore::IndexTableBase >
    on_index_table_found(homestore::superblk< homestore::index_table_sb >&& sb) override {
        LOGI("Recovered index table to index service");
        return home_object_->recover_index_table(std::move(sb));
    }

private:
    HSHomeObject* home_object_;
};

} // namespace homeobject
