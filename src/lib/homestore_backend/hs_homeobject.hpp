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
#include "homeobject/common.hpp"
#include "index_kv.hpp"
#include "gc_manager.hpp"
#include "hs_backend_config.hpp"
#include "generated/resync_pg_data_generated.h"
#include "generated/resync_shard_data_generated.h"
#include "generated/resync_blob_data_generated.h"

namespace homestore {
struct meta_blk;
class IndexTableBase;
} // namespace homestore

namespace homeobject {

class BlobRouteKey;
class BlobRouteByChunkKey;
class BlobRouteValue;
using BlobIndexTable = homestore::IndexTable< BlobRouteKey, BlobRouteValue >;

class HttpManager;

static constexpr uint64_t io_align{512};
PGError toPgError(homestore::ReplServiceError const&);
BlobError toBlobError(homestore::ReplServiceError const&);
ShardError toShardError(homestore::ReplServiceError const&);
ENUM(PGState, uint8_t, ALIVE = 0, DESTROYED);

class HSHomeObject : public HomeObjectImpl {
private:
    /// NOTE: Be wary to change these as they effect on-disk format!
    inline static auto const _svc_meta_name = std::string("HomeObject");
    inline static auto const _pg_meta_name = std::string("PGManager");
    inline static auto const _shard_meta_name = std::string("ShardManager");
    inline static auto const _snp_ctx_meta_name = std::string("SnapshotContext");
    inline static auto const _snp_rcvr_meta_name = std::string("SnapshotReceiver");
    inline static auto const _snp_rcvr_shard_list_meta_name = std::string("SnapshotReceiverShardList");
    static constexpr uint64_t HS_CHUNK_SIZE = 2 * Gi;
    static constexpr uint32_t _data_block_size = 4 * Ki;
    static uint64_t _hs_chunk_size;
    uint32_t _hs_reserved_blks = 0;

    /// Overridable Helpers
    ShardManager::AsyncResult< ShardInfo > _create_shard(pg_id_t, uint64_t size_bytes, trace_id_t tid) override;
    ShardManager::AsyncResult< ShardInfo > _seal_shard(ShardInfo const&, trace_id_t tid) override;

    BlobManager::AsyncResult< blob_id_t > _put_blob(ShardInfo const&, Blob&&, trace_id_t tid) override;
    BlobManager::AsyncResult< Blob > _get_blob(ShardInfo const&, blob_id_t, uint64_t off, uint64_t len,
                                               trace_id_t tid) const override;
    BlobManager::NullAsyncResult _del_blob(ShardInfo const&, blob_id_t, trace_id_t tid) override;

    PGManager::NullAsyncResult _create_pg(PGInfo&& pg_info, std::set< peer_id_t > const& peers,
                                          trace_id_t tid) override;
    PGManager::NullAsyncResult _replace_member(pg_id_t id, std::string& task_id, peer_id_t const& old_member,
                                               PGMember const& new_member, uint32_t commit_quorum,
                                               trace_id_t tid) override;
    PGReplaceMemberStatus _get_replace_member_status(pg_id_t id, std::string& task_id, const PGMember& old_member,
                                                     const PGMember& new_member, const std::vector< PGMember >& others,
                                                     uint64_t trace_id) const override;

    bool _get_stats(pg_id_t id, PGStats& stats) const override;
    void _get_pg_ids(std::vector< pg_id_t >& pg_ids) const override;

    HomeObjectStats _get_stats() const override;
    void _destroy_pg(pg_id_t pg_id) override;

    // Mapping from index table uuid to pg id.
    std::shared_mutex index_lock_;
    struct PgIndexTable {
        pg_id_t pg_id;
        std::shared_ptr< BlobIndexTable > index_table;
    };
    std::unordered_map< std::string, PgIndexTable > index_table_pg_map_;
    std::unordered_map< std::string, std::shared_ptr< GCBlobIndexTable > > gc_index_table_map;
    std::once_flag replica_restart_flag_;
    bool is_first_time_startup_{false};

    // mapping from chunk to shard list.
    std::unordered_map< homestore::chunk_num_t, std::set< shard_id_t > > chunk_to_shards_map_;

public:
#pragma pack(1)
    struct pg_members {
        peer_id_t id;
        // PGMember::max_name_len is the actual maximum string length, adding 1 for the null terminator.
        char name[PGMember::max_name_len + 1];
        int32_t priority{0};
    };

    struct pg_info_superblk {
        pg_id_t id;
        PGState state;
        // num_expected_members means how many members a  pg should have, while num_dynamic_members is the actual
        // members count, as members might float during member replacement.
        uint32_t num_expected_members;
        uint32_t num_dynamic_members;
        uint32_t num_chunks;
        uint64_t chunk_size;
        peer_id_t replica_set_uuid;
        uint64_t pg_size;
        homestore::uuid_t index_table_uuid;
        blob_id_t blob_sequence_num;
        uint64_t active_blob_count;         // Total number of active blobs
        uint64_t tombstone_blob_count;      // Total number of tombstones
        uint64_t total_occupied_blk_count;  // Total number of occupied blocks
        uint64_t total_reclaimed_blk_count; // Total number of reclaimed blocks
        char data[1];                       // ISO C++ forbids zero-size array
        // Data layout inside 'data':
        // First, an array of 'pg_members' structures:
        // | pg_members[0] | pg_members[1] | ... | pg_members[num_dynamic_members-1] | reserved
        // pg_members[num_dynamic_members]...| reserved pg_members[2*num_expected_members-1] Immediately followed by an
        // array of 'chunk_num_t' values (representing physical chunkID): | chunk_num_t[0] | chunk_num_t[1] | ... |
        // chunk_num_t[num_chunks-1] | Here, 'chunk_num_t[i]' represents the p_chunk_id for the v_chunk_id 'i', where
        // v_chunk_id starts from 0 and increases sequentially.
        uint32_t pg_members_space_size() const { return 2 * num_expected_members * sizeof(pg_members); }
        uint32_t size() const {
            return sizeof(pg_info_superblk) - sizeof(char) + this->pg_members_space_size() +
                num_chunks * sizeof(homestore::chunk_num_t);
        }
        static std::string name() { return _pg_meta_name; }

        pg_info_superblk() = default;
        pg_info_superblk(pg_info_superblk const& rhs) { *this = rhs; }

        pg_info_superblk& operator=(pg_info_superblk const& rhs) {
            id = rhs.id;
            state = rhs.state;
            num_expected_members = rhs.num_expected_members;
            num_dynamic_members = rhs.num_dynamic_members;
            num_chunks = rhs.num_chunks;
            pg_size = rhs.pg_size;
            replica_set_uuid = rhs.replica_set_uuid;
            index_table_uuid = rhs.index_table_uuid;
            blob_sequence_num = rhs.blob_sequence_num;

            memcpy(get_pg_members_mutable(), rhs.get_pg_members(), this->pg_members_space_size());
            memcpy(get_chunk_ids_mutable(), rhs.get_chunk_ids(), sizeof(homestore::chunk_num_t) * num_chunks);
            return *this;
        }

        void copy(pg_info_superblk const& rhs) { *this = rhs; }

        pg_members* get_pg_members_mutable() { return reinterpret_cast< pg_members* >(data); }
        const pg_members* get_pg_members() const { return reinterpret_cast< const pg_members* >(data); }

        homestore::chunk_num_t* get_chunk_ids_mutable() {
            return reinterpret_cast< homestore::chunk_num_t* >(data + this->pg_members_space_size());
        }
        const homestore::chunk_num_t* get_chunk_ids() const {
            return reinterpret_cast< const homestore::chunk_num_t* >(data + this->pg_members_space_size());
        }
    };

    struct DataHeader {
        static constexpr uint8_t data_header_version = 0x01;
        static constexpr uint64_t data_header_magic = 0x21fdffdba8d68fc6; // echo "BlobHeader" | md5sum

        enum class data_type_t : uint32_t { SHARD_INFO = 1, BLOB_INFO = 2 };

        bool valid() const { return ((magic == data_header_magic) && (version <= data_header_version)); }

        uint64_t magic{data_header_magic};
        uint8_t version{data_header_version};
        data_type_t type{data_type_t::BLOB_INFO};
    };

    struct shard_info_superblk : DataHeader {
        ShardInfo info;
        homestore::chunk_num_t p_chunk_id;
        homestore::chunk_num_t v_chunk_id;
    };

    struct snapshot_ctx_superblk {
        homestore::group_id_t group_id;
        int64_t lsn;
        uint64_t data_size;
        char data[1];
    };

    struct durable_snapshot_progress {
        uint64_t start_time{0};
        uint64_t total_blobs{0};
        uint64_t total_bytes{0};
        uint64_t total_shards{0};
        uint64_t complete_blobs{0};
        uint64_t complete_bytes{0};
        uint64_t complete_shards{0};
        uint64_t corrupted_blobs{0};
    };

    struct snapshot_progress {
        uint64_t start_time{0};
        uint64_t total_blobs{0};
        uint64_t total_bytes{0};
        uint64_t total_shards{0};
        uint64_t complete_blobs{0};
        uint64_t complete_bytes{0};
        uint64_t complete_shards{0};
        // The count of the blobs which have been corrupted on the leader side.
        uint64_t corrupted_blobs{0};
        // Used to handle the retried batch message.
        uint64_t cur_batch_blobs{0};
        uint64_t cur_batch_bytes{0};
        uint64_t error_count{0};

        snapshot_progress() = default;
        explicit snapshot_progress(durable_snapshot_progress p) {
            start_time = p.start_time;
            total_blobs = p.total_blobs;
            total_bytes = p.total_bytes;
            total_shards = p.total_shards;
            complete_blobs = p.complete_blobs;
            complete_bytes = p.complete_bytes;
            complete_shards = p.complete_shards;
            corrupted_blobs = p.corrupted_blobs;
        }
    };

    // Since shard list can be quite large and only need to be persisted once, we store it in a separate superblk
    struct snapshot_rcvr_info_superblk {
        shard_id_t shard_cursor;
        int64_t snp_lsn;
        pg_id_t pg_id;
        durable_snapshot_progress progress;

        uint32_t size() const { return sizeof(snapshot_rcvr_info_superblk); }
        static auto name() -> string { return _snp_rcvr_meta_name; }
    };

    struct snapshot_rcvr_shard_list_superblk {
        pg_id_t pg_id;
        int64_t snp_lsn;
        uint64_t shard_cnt;       // count of shards
        shard_id_t shard_list[1]; // array of shard ids

        uint32_t size() const {
            return sizeof(snapshot_rcvr_shard_list_superblk) - sizeof(shard_id_t) + shard_cnt * sizeof(shard_id_t);
        }

        std::vector< shard_id_t > get_shard_list() const { return std::vector(shard_list, shard_list + shard_cnt); }
        static auto name() -> string { return _snp_rcvr_shard_list_meta_name; }
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
                REGISTER_GAUGE(total_reclaimed_space, "Total Size occupied that has been reclaimed by GC");

                REGISTER_COUNTER(total_user_key_size, "Total user key size provided",
                                 sisl::_publish_as::publish_as_gauge);

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
                GAUGE_UPDATE(*this, total_reclaimed_space,
                             pg_.durable_entities().total_reclaimed_blk_count.load(std::memory_order_relaxed) *
                                 blk_size);
            }

        private:
            HS_PG const& pg_;
            uint32_t blk_size;
        };

        homestore::superblk< pg_info_superblk > pg_sb_;
        shared< homestore::ReplDev > repl_dev_;
        std::shared_ptr< BlobIndexTable > index_table_;
        PGMetrics metrics_;
        mutable pg_state pg_state_{0};

        // Snapshot receiver progress info, used as a checkpoint for recovery
        // Placed within HS_PG since HomeObject is unable to locate the ReplicationStateMachine
        mutable homestore::superblk< snapshot_rcvr_info_superblk > snp_rcvr_info_sb_;
        mutable homestore::superblk< snapshot_rcvr_shard_list_superblk > snp_rcvr_shard_list_sb_;

        HS_PG(PGInfo info, shared< homestore::ReplDev > rdev, shared< BlobIndexTable > index_table,
              std::shared_ptr< const std::vector< homestore::chunk_num_t > > pg_chunk_ids);
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
         * Returns the progress of the baseline resync.
         */
        uint32_t get_snp_progress() const;

        /**
         * Returns all replication info of all peers.
         */
        void get_peer_info(std::vector< peer_info >& members) const;

        void reconcile_leader() const;

        void yield_leadership_to_follower() const;

        void trigger_snapshot_creation(int64_t compact_lsn, bool is_async) const;

        /**
         * Returns all shards
         */
        std::vector< Shard > get_chunk_shards(homestore::chunk_num_t v_chunk_id) const;
    };

    struct HS_Shard : public Shard {
        homestore::superblk< shard_info_superblk > sb_;
        HS_Shard(ShardInfo info, homestore::chunk_num_t p_chunk_id, homestore::chunk_num_t v_chunk_id);
        HS_Shard(homestore::superblk< shard_info_superblk >&& sb);
        ~HS_Shard() override = default;

        void update_info(const ShardInfo& info, std::optional< homestore::chunk_num_t > p_chunk_id = std::nullopt);
        auto p_chunk_id() const { return sb_->p_chunk_id; }
        auto v_chunk_id() const { return sb_->v_chunk_id; }
    };

#pragma pack(1)
    // Every blob payload stored in disk as blob header | blob data | blob metadata(optional) | padding.
    // Padding of zeroes is added to make sure the whole payload be aligned to device block size.
    struct BlobHeader : DataHeader {
        static constexpr uint64_t blob_max_hash_len = 32;

        enum class HashAlgorithm : uint8_t {
            NONE = 0,
            CRC32 = 1,
            MD5 = 2,
            SHA1 = 3,
        };

        HashAlgorithm hash_algorithm;
        mutable uint8_t header_hash[blob_max_hash_len]{};
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

        bool valid() const {
            if (!DataHeader::valid()) { return false; }

            uint8_t hash_arr[blob_max_hash_len];
            std::memcpy(hash_arr, header_hash, blob_max_hash_len);
            if (!_do_seal()) {
                std::memcpy(header_hash, hash_arr, blob_max_hash_len);
                return false;
            }

            bool hash_valid = std::memcmp(header_hash, hash_arr, blob_max_hash_len) == 0;
            std::memcpy(header_hash, hash_arr, blob_max_hash_len);
            return hash_valid;
        }

        void seal() const {
            if (!_do_seal()) {
                DEBUG_ASSERT(false, "Invalid hash algorithm"); // Not implemented
            }
        }

    private:
        bool _do_seal() const {
            switch (hash_algorithm) {
            case HashAlgorithm::NONE:
                return true;
            case HashAlgorithm::CRC32: {
                std::memset(header_hash, 0, blob_max_hash_len);
                uint32_t computed_hash = crc32_ieee(0, (uint8_t*)this, sizeof(BlobHeader));

                static_assert(sizeof(header_hash) == blob_max_hash_len && blob_max_hash_len >= sizeof(uint32_t),
                              "buffer too small!!");
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-overflow"
                std::memcpy(header_hash, &computed_hash, sizeof(uint32_t));
#pragma GCC diagnostic pop

                return true;
            }
            case HashAlgorithm::MD5:
            case HashAlgorithm::SHA1:
            default:
                break; // Not implemented
            }
            return false;
        }
    };
#pragma pack()

    struct BlobInfo {
        shard_id_t shard_id;
        blob_id_t blob_id;
        homestore::MultiBlkId pbas;
    };

    struct BlobInfoData : public BlobInfo {
        Blob blob;
    };

    enum class BlobState : uint8_t {
        ALIVE = 0,
        TOMBSTONE = 1,
        ALL = 2,
    };

    inline const static homestore::MultiBlkId tombstone_pbas{0, 0, 0};
    inline const static std::string delete_marker_blob_data{"HOMEOBJECT_BLOB_DELETE_MARKER"};

    class PGBlobIterator {
    public:
        struct blob_read_result {
            blob_id_t blob_id_;
            sisl::io_blob_safe blob_;
            ResyncBlobState state_;
            blob_read_result(blob_id_t blob_id, sisl::io_blob_safe&& blob, ResyncBlobState state) :
                    blob_id_(blob_id), blob_(std::move(blob)), state_(state) {}
        };

        PGBlobIterator(HSHomeObject& home_obj, homestore::group_id_t group_id, uint64_t upto_lsn = 0);
        bool update_cursor(const objId& id);
        void reset_cursor();
        bool generate_shard_blob_list();
        bool create_pg_snapshot_data(sisl::io_blob_safe& meta_blob);
        bool create_shard_snapshot_data(sisl::io_blob_safe& meta_blob);
        bool create_blobs_snapshot_data(sisl::io_blob_safe& data_blob);
        void stop();

        objId cur_obj_id{0, 0};
        homestore::group_id_t group_id;
        pg_id_t pg_id;

    private:
        PG* get_pg_metadata() const;
        objId expected_next_obj_id() const;
        BlobManager::AsyncResult< blob_read_result > load_blob_data(const BlobInfo& blob_info);
        bool prefetch_blobs_snapshot_data();
        void pack_resync_message(sisl::io_blob_safe& dest_blob, SyncMessageType type);

        // All the leader's metrics are in-memory
        struct DonerSnapshotMetrics : sisl::MetricsGroup {
            explicit DonerSnapshotMetrics(pg_id_t pg_id) : sisl::MetricsGroup("snapshot_doner", std::to_string(pg_id)) {
                REGISTER_COUNTER(snp_dnr_load_blob, "Loaded blobs in baseline resync");
                REGISTER_COUNTER(snp_dnr_load_bytes, "Loaded bytes in baseline resync");
                REGISTER_COUNTER(snp_dnr_resend_count, "Mesg resend times in baseline resync");
                REGISTER_COUNTER(snp_dnr_error_count, "Error times when reading blobs in baseline resync");
                REGISTER_HISTOGRAM(snp_dnr_blob_process_latency,
                                   "Time cost(us) of successfully process a blob in baseline resync",
                                   HistogramBucketsType(LowResolutionLatecyBuckets));
                REGISTER_HISTOGRAM(snp_dnr_batch_process_latency,
                                   "Time cost(ms) of successfully process a batch in baseline resync",
                                   HistogramBucketsType(LowResolutionLatecyBuckets));
                REGISTER_HISTOGRAM(snp_dnr_batch_e2e_latency,
                                   "Time cost(ms) of a batch end-to-end round trip in baseline resync",
                                   HistogramBucketsType(LowResolutionLatecyBuckets));
                register_me_to_farm();
            }

            ~DonerSnapshotMetrics() { deregister_me_from_farm(); }
            DonerSnapshotMetrics(const DonerSnapshotMetrics&) = delete;
            DonerSnapshotMetrics(DonerSnapshotMetrics&&) noexcept = delete;
            DonerSnapshotMetrics& operator=(const DonerSnapshotMetrics&) = delete;
            DonerSnapshotMetrics& operator=(DonerSnapshotMetrics&&) noexcept = delete;
        };

        struct ShardEntry {
            ShardInfo info;
            homestore::chunk_num_t v_chunk_num;
        };

        std::vector< ShardEntry > shard_list_{0};

        int64_t cur_shard_idx_{-1};
        std::vector< BlobInfo > cur_blob_list_{0};
        uint64_t inflight_prefetch_bytes_{0};
        std::map< blob_id_t, BlobManager::AsyncResult< blob_read_result > > prefetched_blobs_;
        uint64_t cur_start_blob_idx_{0};
        uint64_t cur_batch_blob_count_{0};
        Clock::time_point cur_batch_start_time_;
        flatbuffers::FlatBufferBuilder builder_;

        HSHomeObject& home_obj_;
        uint64_t snp_start_lsn_;
        shared< homestore::ReplDev > repl_dev_;
        uint64_t max_batch_size_;
        std::unique_ptr< DonerSnapshotMetrics > metrics_;
        bool stopped_{false};
        std::mutex op_mut_; // Protects all operations and coordinates with stop()
    };

    class SnapshotReceiveHandler {
    public:
        enum ErrorCode {
            ALLOC_BLK_ERR = 1,
            WRITE_DATA_ERR,
            COMMIT_BLK_ERR,
            INVALID_BLOB_HEADER,
            BLOB_DATA_CORRUPTED,
            ADD_BLOB_INDEX_ERR,
            CREATE_PG_ERR,
        };

        constexpr static shard_id_t invalid_shard_id = 0;
        constexpr static shard_id_t shard_list_end_marker = ULLONG_MAX;

        SnapshotReceiveHandler(HSHomeObject& home_obj, shared< homestore::ReplDev > repl_dev);

        int process_pg_snapshot_data(ResyncPGMetaData const& pg_meta);
        int process_shard_snapshot_data(ResyncShardMetaData const& shard_meta);
        int process_blobs_snapshot_data(ResyncBlobDataBatch const& data_blobs, snp_batch_id_t batch_num,
                                        bool is_last_batch);

        int64_t get_context_lsn() const;
        pg_id_t get_context_pg_id() const;

        // Try to load existing snapshot context info
        bool load_prev_context_and_metrics();

        // Reset the context for a new snapshot, should be called before each new snapshot transmission
        void reset_context_and_metrics(int64_t lsn, pg_id_t pg_id);
        void destroy_context_and_metrics();
        bool is_valid_obj_id(const objId& obj_id) const;

        shard_id_t get_shard_cursor() const;
        shard_id_t get_next_shard() const;

    private:
        // SnapshotContext is the context data of current snapshot transmission
        struct SnapshotContext {
            shard_id_t shard_cursor{invalid_shard_id};
            snp_batch_id_t cur_batch_num{0};
            std::vector< shard_id_t > shard_list;
            const int64_t snp_lsn;
            const pg_id_t pg_id;
            shared< BlobIndexTable > index_table;
            std::shared_mutex progress_lock;
            snapshot_progress progress;
            SnapshotContext(int64_t lsn, pg_id_t pg_id) : snp_lsn{lsn}, pg_id{pg_id} {}
        };

        struct ReceiverSnapshotMetrics : sisl::MetricsGroup {
            ReceiverSnapshotMetrics(std::shared_ptr< SnapshotContext > ctx) :
                    sisl::MetricsGroup("snapshot_receiver", std::to_string(ctx->pg_id)), ctx_{ctx} {
                REGISTER_GAUGE(snp_rcvr_total_blob, "Total blobs in baseline resync");
                REGISTER_GAUGE(snp_rcvr_total_bytes, "Total bytes in baseline resync")
                REGISTER_GAUGE(snp_rcvr_total_shards, "Total shards in baseline resync")
                REGISTER_GAUGE(snp_rcvr_complete_blob, "Complete blob in baseline resync")
                REGISTER_GAUGE(snp_rcvr_complete_bytes, "Complete bytes in baseline resync")
                REGISTER_GAUGE(snp_rcvr_complete_shards, "Complete shards in baseline resync")
                REGISTER_GAUGE(snp_rcvr_corrupted_blobs, "Corrupted blobs in baseline resync");
                REGISTER_GAUGE(snp_rcvr_elapsed_time_sec, "Time cost(seconds) of baseline resync");
                REGISTER_GAUGE(snp_rcvr_error_count, "Error count in baseline resync");
                REGISTER_HISTOGRAM(snp_rcvr_blob_process_time,
                                   "Time cost(us) of successfully process a blob in baseline resync",
                                   HistogramBucketsType(LowResolutionLatecyBuckets));
                REGISTER_HISTOGRAM(snp_rcvr_batch_process_time,
                                   "Time cost(ms) of successfully process a batch in baseline resync",
                                   HistogramBucketsType(LowResolutionLatecyBuckets));

                attach_gather_cb(std::bind(&ReceiverSnapshotMetrics::on_gather, this));
                register_me_to_farm();
            }
            ~ReceiverSnapshotMetrics() { deregister_me_from_farm(); }
            ReceiverSnapshotMetrics(const ReceiverSnapshotMetrics&) = delete;
            ReceiverSnapshotMetrics(ReceiverSnapshotMetrics&&) noexcept = delete;
            ReceiverSnapshotMetrics& operator=(const ReceiverSnapshotMetrics&) = delete;
            ReceiverSnapshotMetrics& operator=(ReceiverSnapshotMetrics&&) noexcept = delete;

            void on_gather() {
                if (ctx_) {
                    std::shared_lock< std::shared_mutex > lock(ctx_->progress_lock);
                    GAUGE_UPDATE(*this, snp_rcvr_total_blob, ctx_->progress.total_blobs);
                    GAUGE_UPDATE(*this, snp_rcvr_total_bytes, ctx_->progress.total_bytes);
                    GAUGE_UPDATE(*this, snp_rcvr_total_shards, ctx_->progress.total_shards);
                    GAUGE_UPDATE(*this, snp_rcvr_complete_blob, ctx_->progress.complete_blobs);
                    GAUGE_UPDATE(*this, snp_rcvr_complete_bytes, ctx_->progress.complete_bytes);
                    GAUGE_UPDATE(*this, snp_rcvr_complete_shards, ctx_->progress.complete_shards);
                    GAUGE_UPDATE(*this, snp_rcvr_corrupted_blobs, ctx_->progress.corrupted_blobs);
                    GAUGE_UPDATE(*this, snp_rcvr_error_count, ctx_->progress.error_count);
                    auto duration = get_elapsed_time_ms(ctx_->progress.start_time * 1000) / 1000;
                    GAUGE_UPDATE(*this, snp_rcvr_elapsed_time_sec, duration);
                }
            }

        private:
            std::shared_ptr< SnapshotContext > ctx_;
        };

        HSHomeObject& home_obj_;
        const shared< homestore::ReplDev > repl_dev_;

        std::shared_ptr< SnapshotContext > ctx_;
        std::unique_ptr< ReceiverSnapshotMetrics > metrics_;
        folly::Future< bool > cp_fut;

        // Update the snp_info superblock
        void update_snp_info_sb(bool init = false);
    };

private:
    std::unordered_map< homestore::group_id_t, homestore::superblk< snapshot_ctx_superblk > > snp_ctx_sbs_;
    mutable std::shared_mutex snp_sbs_lock_;
    shared< HeapChunkSelector > chunk_selector_;
    shared< GCManager > gc_mgr_;
    unique< HttpManager > http_mgr_;

    static constexpr size_t max_zpad_bufs = _data_block_size / io_align;
    std::array< sisl::io_blob_safe, max_zpad_bufs > zpad_bufs_; // Zero padded buffers for blob payload.

    static homestore::ReplicationService& hs_repl_service() { return homestore::hs()->repl_service(); }

    // blob related
    BlobManager::AsyncResult< Blob > _get_blob_data(const shared< homestore::ReplDev >& repl_dev, shard_id_t shard_id,
                                                    blob_id_t blob_id, uint64_t req_offset, uint64_t req_len,
                                                    const homestore::MultiBlkId& blkid, trace_id_t tid) const;

    // create pg related
    static PGManager::NullAsyncResult do_create_pg(cshared< homestore::ReplDev > repl_dev, PGInfo&& pg_info,
                                                   trace_id_t tid = 0);
    folly::Expected< HSHomeObject::HS_PG*, PGError > local_create_pg(shared< homestore::ReplDev > repl_dev,
                                                                     PGInfo pg_info, trace_id_t tid = 0);
    static std::string serialize_pg_info(const PGInfo& info);
    static PGInfo deserialize_pg_info(const unsigned char* pg_info_str, size_t size);
    void add_pg_to_map(unique< HS_PG > hs_pg);

    // create shard related
    shard_id_t generate_new_shard_id(pg_id_t pg);

    static ShardInfo deserialize_shard_info(const char* shard_info_str, size_t size);
    static std::string serialize_shard_info(const ShardInfo& info);
    void local_create_shard(ShardInfo shard_info, homestore::chunk_num_t v_chunk_id, homestore::chunk_num_t p_chunk_id,
                            homestore::blk_count_t blk_count, trace_id_t tid = 0);
    void add_new_shard_to_map(std::unique_ptr< HS_Shard > shard);
    void update_shard_in_map(const ShardInfo& shard_info);

    // recover part
    void register_homestore_metablk_callback();
    void on_pg_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);
    void on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf);
    void on_shard_meta_blk_recover_completed(bool success);
    void on_snp_ctx_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf);
    void on_snp_ctx_meta_blk_recover_completed(bool success);
    void on_snp_rcvr_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf);
    void on_snp_rcvr_meta_blk_recover_completed(bool success);
    void on_snp_rcvr_shard_list_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf);
    void on_snp_rcvr_shard_list_meta_blk_recover_completed(bool success);

    void persist_pg_sb();

    // helpers
    DevType get_device_type(string const& devname);

public:
    using HomeObjectImpl::HomeObjectImpl;
    HSHomeObject();
    ~HSHomeObject() override;
    void shutdown() override final;

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
     * @brief Function invoked when start a member replacement
     *
     * @param group_id The group id of replication device.
     * @param member_out Member which is removed from group
     * @param member_in Member which is added to group
     * */
    void on_pg_start_replace_member(homestore::group_id_t group_id, const std::string& task_id,
                                    const homestore::replica_member_info& member_out,
                                    const homestore::replica_member_info& member_in, trace_id_t tid);

    /**
     * @brief Function invoked when complete a member replacement
     *
     * @param group_id The group id of replication device.
     * @param member_out Member which is removed from group
     * @param member_in Member which is added to group
     * */
    void on_pg_complete_replace_member(homestore::group_id_t group_id, const std::string& task_id,
                                       const homestore::replica_member_info& member_out,
                                       const homestore::replica_member_info& member_in, trace_id_t tid);

    /**
     * @brief Cleans up and recycles resources for the PG identified by the given pg_id on the current node.
     *
     * This function is called when the replication device leaves or when a specific PG is destroyed on the current
     * node. Note that this function does not perform Raft synchronization with other nodes.
     *
     * Possible scenarios for calling this function include:
     * - A member-out node cleaning up resources for a specified PG.
     * - During baseline rsync to clean up PG resources on the current node.
     *
     * @param pg_id The ID of the PG to be destroyed.
     */
    bool pg_destroy(pg_id_t pg_id, bool need_to_pause_pg_state_machine = false);

    bool pause_pg_state_machine(pg_id_t pg_id);

    bool resume_pg_state_machine(pg_id_t pg_id);

    /**
     * @brief Get HS_PG object from given pg_id.
     * @param pg_id The ID of the PG.
     * @return The HS_PG object matching the given pg_id, otherwise nullptr.
     */
    const HS_PG* get_hs_pg(pg_id_t pg_id) const;

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
                                     cintrusive< homestore::repl_req_ctx >& hs_ctx);
    void on_shard_message_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                   cintrusive< homestore::repl_req_ctx >& hs_ctx);

    /**
     * @brief Retrieves the chunk number associated with the given shard ID.
     *
     * @param id The ID of the shard to retrieve the chunk number for.
     * @return An optional chunk number values shard p_chunk_id if the shard ID is valid, otherwise an empty optional.
     */
    std::optional< homestore::chunk_num_t > get_shard_p_chunk_id(shard_id_t id) const;

    void update_shard_meta_after_gc(const homestore::chunk_num_t move_from_chunk,
                                    const homestore::chunk_num_t move_to_chunk, const uint64_t task_id);

    /**
     * @brief Retrieves the chunk number associated with the given shard ID.
     *
     * @param id The ID of the shard to retrieve the chunk number for.
     * @return An optional chunk number values shard v_chunk_id if the shard ID is valid, otherwise an empty optional.
     */
    std::optional< homestore::chunk_num_t > get_shard_v_chunk_id(const shard_id_t id) const;

    /**
     * @brief Get the sequence number of the shard from the shard id.
     *
     * @param shard_id The ID of the shard.
     * @return The sequence number of the shard.
     */
    static uint64_t get_sequence_num_from_shard_id(uint64_t shard_id);

    /**
     * @brief Get the sequence number of the shard from the shard id.
     *
     * @param shard_id The ID of the shard.
     * @return The PG ID of the shard.
     */
    pg_id_t get_pg_id_from_shard_id(uint64_t shard_id);
    /**
     * @brief recover PG and shard from the superblock.
     *
     */
    void on_replica_restart();

    /**
     * @brief Extracts the physical chunk ID for create shard from the message.
     *
     * @param header The message header that includes the shard_info_superblk, which contains the data necessary for
     * extracting and mapping the chunk ID.
     * @return An optional virtual chunk id if the extraction and mapping process is successful, otherwise an empty
     * optional.
     */
    std::optional< homestore::chunk_num_t > resolve_v_chunk_id_from_msg(sisl::blob const& header);

    /**
     * @brief Releases a chunk based on the information provided in a CREATE_SHARD message.
     *
     * This function is invoked during log rollback or when the proposer encounters an error.
     * Its primary purpose is to ensure that the state of pg_chunks is reverted to the correct state.
     *
     * @param header The message header that includes the shard_info_superblk, which contains the data necessary for
     * extracting and mapping the chunk ID.
     * @return Returns true if the chunk was successfully released, false otherwise.
     */
    bool release_chunk_based_on_create_shard_message(sisl::blob const& header);

    /**
     * @brief check whether the chunks in a given pg can be gc.
     *
     * @param pg_id The ID of the PG whose shards are to be destroyed.
     * @return True if the chunks in the PG can be garbage collected, false otherwise.
     */
    bool can_chunks_in_pg_be_gc(pg_id_t pg_id) const;

    bool pg_exists(pg_id_t pg_id) const;

    uint32_t get_reserved_blks() const { return _hs_reserved_blks; }

    void on_create_pg_message_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                       cintrusive< homestore::repl_req_ctx >& hs_ctx);

    cshared< HeapChunkSelector > chunk_selector() const { return chunk_selector_; }
    cshared< GCManager > gc_manager() const { return gc_mgr_; }

    /**
     * @brief Reconciles the leaders for all PGs or a specific PG identified by pg_id.
     *
     * This function ensures that the leadership status of PGs is consistent across the system.
     * If a specific pg_id is provided, it reconciles the leader for that particular PG.
     * If pg_id is -1 (default), it reconciles leaders for all PGs.
     *
     * @param pg_id The ID of the PG to reconcile. Default is -1, which indicates all PGs.
     */
    void reconcile_pg_leader(int32_t pg_id = -1);

    /**
     * @brief yield leadership to follower with newest progress, only used for test
     */
    void yield_pg_leadership_to_follower(int32_t pg_id = 1);

    /**
     * @brief Manually trigger a snapshot creation.
     * @param compact_lsn Expected compact up to LSN. Default is -1, meaning it depends directly on the current HS
     * status.
     * @param is_async Trigger mode. Default is true, meaning an async snapshot is triggered on the next earliest
     * committed log. If false, a snapshot is triggered immediately based on the latest committed log.
     *
     * Recommendation:
     * - If there is continuous traffic, it is recommended to set is_async=true. This only sets a flag, relying on
     * subsequent commits to trigger the snapshot asynchronously, making it more lightweight.
     * - If there is no continuous traffic, set is_async=false to trigger a snapshot immediately.
     */
    void trigger_snapshot_creation(int32_t pg_id, int64_t compact_lsn, bool is_async);

    // Blob manager related.
    void on_blob_message_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                  cintrusive< homestore::repl_req_ctx >& hs_ctx);
    void on_blob_put_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                            const homestore::MultiBlkId& pbas, cintrusive< homestore::repl_req_ctx >& hs_ctx);
    void on_blob_del_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                            cintrusive< homestore::repl_req_ctx >& hs_ctx);
    bool local_add_blob_info(pg_id_t pg_id, BlobInfo const& blob_info, trace_id_t tid = 0);
    homestore::ReplResult< homestore::blk_alloc_hints >
    blob_put_get_blk_alloc_hints(sisl::blob const& header, cintrusive< homestore::repl_req_ctx >& ctx);
    void compute_blob_payload_hash(BlobHeader::HashAlgorithm algorithm, const uint8_t* blob_bytes, size_t blob_size,
                                   const uint8_t* user_key_bytes, size_t user_key_size, uint8_t* hash_bytes,
                                   size_t hash_len) const;

    std::shared_ptr< homestore::IndexTableBase >
    recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb);
    std::optional< pg_id_t > get_pg_id_with_group_id(homestore::group_id_t group_id) const;

    const std::set< shard_id_t > get_shards_in_chunk(homestore::chunk_num_t chunk_id) const;

    void update_pg_meta_after_gc(const pg_id_t pg_id, const homestore::chunk_num_t move_from_chunk,
                                 const homestore::chunk_num_t move_to_chunk, const uint64_t task_id);
    uint32_t get_pg_tombstone_blob_count(pg_id_t pg_id) const;

    // Snapshot persistence related
    sisl::io_blob_safe get_snapshot_sb_data(homestore::group_id_t group_id);
    void update_snapshot_sb(homestore::group_id_t group_id, std::shared_ptr< homestore::snapshot_context > ctx);
    void destroy_snapshot_sb(homestore::group_id_t group_id);
    const Shard* _get_hs_shard(const shard_id_t shard_id) const;
    std::shared_ptr< GCBlobIndexTable > get_gc_index_table(std::string uuid) const;
    void trigger_immediate_gc();
    const HS_PG* _get_hs_pg_unlocked(pg_id_t pg_id) const;
    bool verify_blob(const void* blob, const shard_id_t shard_id, const blob_id_t blob_id,
                     bool allow_delete_marker = false) const;

    BlobManager::Result< std::vector< BlobInfo > > get_shard_blobs(shard_id_t shard_id);

private:
    std::shared_ptr< BlobIndexTable > create_pg_index_table();
    std::shared_ptr< GCBlobIndexTable > create_gc_index_table();

    std::pair< bool, homestore::btree_status_t > add_to_index_table(shared< BlobIndexTable > index_table,
                                                                    const BlobInfo& blob_info);

    BlobManager::Result< homestore::MultiBlkId >
    get_blob_from_index_table(shared< BlobIndexTable > index_table, shard_id_t shard_id, blob_id_t blob_id) const;

    void print_btree_index(pg_id_t pg_id) const;

    shared< BlobIndexTable > get_index_table(pg_id_t pg_id);

    BlobManager::Result< std::vector< BlobInfo > >
    query_blobs_in_shard(pg_id_t pg_id, uint64_t cur_shard_seq_num, blob_id_t start_blob_id, uint64_t max_num_in_batch);

    // Zero padding buffer related.
    size_t max_pad_size() const;
    sisl::io_blob_safe& get_pad_buf(uint32_t pad_len);

    // void trigger_timed_events();

    /**
     * @brief Marks the PG as destroyed.
     *
     * Updates the internal state to indicate that the specified PG is destroyed and ensures its state is persisted.
     *
     * @param pg_id The ID of the PG to be marked as destroyed.
     */
    void mark_pg_destroyed(pg_id_t pg_id);

    /**
     * @brief Cleans up and recycles resources for shards in the PG located using a PG ID.
     *
     * @param pg_id The ID of the PG whose shards are to be destroyed.
     */
    void destroy_shards(pg_id_t pg_id);

    /**
     *  @brief Cleans up resources associated with the PG identified by pg_id on HomeStore side.
     *
     *  @param pg_id The ID of the PG to be
     */
    void destroy_hs_resources(pg_id_t pg_id);

    /**
     * @brief destroy index table for the PG located using a pg_id.
     *
     * @param pg_id The ID of the PG to be destroyed.
     */
    void destroy_pg_index_table(pg_id_t pg_id);

    /**
     * @brief Destroy the superblock for the PG identified by pg_id.
     * Ensures all operations are persisted by triggering a cp flush before destruction.
     *
     * @param pg_id The ID of the PG to be destroyed.
     */

    void destroy_pg_superblk(pg_id_t pg_id);

    // graceful shutdown related
private:
    std::atomic_bool shutting_down{false};
    mutable std::atomic_uint64_t pending_request_num{0};

    bool is_shutting_down() const { return shutting_down.load(); }
    void start_shutting_down() { shutting_down = true; }

    uint64_t get_pending_request_num() const { return pending_request_num.load(); }

    // only leader will call incr and decr pending request num
    void incr_pending_request_num() const {
        uint64_t now = pending_request_num.fetch_add(1);
        LOGT("inc pending req, was {}", now);
    }
    void decr_pending_request_num() const {
        uint64_t now = pending_request_num.fetch_sub(1);
        LOGT("desc pending req, was {}", now);
        DEBUG_ASSERT(now > 0, "pending == 0 ");
    }
    homestore::replica_member_info to_replica_member_info(const PGMember& pg_member) const;
    PGMember to_pg_member(const homestore::replica_member_info& replica_info) const;
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
