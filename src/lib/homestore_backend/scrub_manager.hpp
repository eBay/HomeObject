#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <folly/futures/Future.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#pragma GCC diagnostic pop

#include <iomgr/iomgr.hpp>
#include "homeobject/common.hpp"
#include <homestore/blk.h>
#include <homestore/superblk_handler.hpp>
#include "lib/blob_route.hpp"
#include "MPMCPriorityQueue.hpp"
#include "generated/scrub_common_generated.h"

namespace homeobject {

class HSHomeObject;

ENUM(SCRUB_TRIGGER_TYPE, uint8_t, PERIODICALLY = 0, MANUALLY);
ENUM(SCRUB_TYPE, uint8_t, PG_META = 0, DEEP_SHARD, SHALLOW_SHARD, DEEP_BLOB, SHALLOW_BLOB);

class ScrubManager {
public:
    ScrubManager(HSHomeObject* homeobject);
    ~ScrubManager();

    // Disallow copy and move
    ScrubManager(const ScrubManager&) = delete;
    ScrubManager(ScrubManager&&) = delete;
    ScrubManager& operator=(const ScrubManager&) = delete;
    ScrubManager& operator=(ScrubManager&&) = delete;

public:
    inline static auto const pg_scrub_meta_name = std::string("PG_SCRUB");
    static constexpr uint64_t blob_max_hash_len = 32;
    using BlobHashArray = std::array< uint8_t, blob_max_hash_len >;
    using chunk_id_t = homestore::chunk_num_t;
    // TODO: persist this into metablk.
    inline static atomic_uint64_t scrub_task_id{1};

    // pg scrub superblk
#pragma pack(1)
    struct pg_scrub_superblk {
        uint64_t last_deep_scrub_timestamp;
        uint64_t last_shallow_scrub_timestamp;
        pg_id_t pg_id;
        static std::string name() { return pg_scrub_meta_name; }
    };
#pragma pack()

    // scrub req
public:
    class base_scrub_req {
    public:
        base_scrub_req() = default;
        base_scrub_req(uint64_t task_id, uint64_t req_id, int64_t scrub_lsn, peer_id_t issuer_peer_id, pg_id_t pg_id,
                       bool is_deep_scrub) :
                task_id(task_id),
                req_id(req_id),
                scrub_lsn(scrub_lsn),
                issuer_peer_id(issuer_peer_id),
                pg_id(pg_id),
                is_deep_scrub_(is_deep_scrub) {}

        bool is_deep_scrub() const { return is_deep_scrub_; }

        virtual ~base_scrub_req() = default;
        virtual SCRUB_TYPE get_scrub_type() const { return SCRUB_TYPE::PG_META; }
        virtual flatbuffers::DetachedBuffer build_flat_buffer() const;
        virtual bool load(uint8_t const* buf_ptr, const uint32_t buf_size);

    public:
        uint64_t task_id;
        uint64_t req_id;
        int64_t scrub_lsn;
        peer_id_t issuer_peer_id;
        pg_id_t pg_id;
        bool is_deep_scrub_;
    };

    class blob_scrub_req : public base_scrub_req {
    public:
        blob_scrub_req() = default;
        blob_scrub_req(uint64_t task_id, uint64_t req_id, int64_t scrub_lsn, peer_id_t issuer_peer_id, pg_id_t pg_id,
                       blob_id_t start, blob_id_t end, bool is_deep_scrub) :
                base_scrub_req(task_id, req_id, scrub_lsn, issuer_peer_id, pg_id, is_deep_scrub),
                start(start),
                end(end) {}
        ~blob_scrub_req() = default;

        SCRUB_TYPE get_scrub_type() const override {
            return is_deep_scrub() ? SCRUB_TYPE::DEEP_BLOB : SCRUB_TYPE::SHALLOW_BLOB;
        }
        flatbuffers::DetachedBuffer build_flat_buffer() const override;
        bool load(uint8_t const* buf_ptr, const uint32_t buf_size) override;

    public:
        blob_id_t start;
        blob_id_t end;
    };

    class shard_scrub_req : public base_scrub_req {
    public:
        shard_scrub_req() = default;
        shard_scrub_req(uint64_t task_id, uint64_t req_id, int64_t scrub_lsn, peer_id_t issuer_peer_id, pg_id_t pg_id,
                        uint64_t start, uint64_t end, bool is_deep_scrub) :
                base_scrub_req(task_id, req_id, scrub_lsn, issuer_peer_id, pg_id, is_deep_scrub),
                start(start),
                end(end) {}
        ~shard_scrub_req() = default;

        SCRUB_TYPE get_scrub_type() const override {
            return is_deep_scrub() ? SCRUB_TYPE::DEEP_SHARD : SCRUB_TYPE::SHALLOW_SHARD;
        }

        flatbuffers::DetachedBuffer build_flat_buffer() const override;
        bool load(uint8_t const* buf_ptr, const uint32_t buf_size) override;

    public:
        uint64_t start;
        uint64_t end;
    };

    // scrub map, the scrub result of a specific scrub.
public:
    class BaseScrubMap {
    public:
        BaseScrubMap() = default;
        BaseScrubMap(pg_id_t pg_id, uint64_t task_id, uint64_t req_id, int64_t scrub_lsn, peer_id_t peer_id) :
                pg_id(pg_id), task_id(task_id), req_id(req_id), scrub_lsn(scrub_lsn), peer_id(peer_id) {}
        virtual ~BaseScrubMap() = default;

    public:
        // convert the scrub map to io_blob_list for sending through data rpc
        virtual flatbuffers::DetachedBuffer build_flat_buffer() const = 0;
        virtual bool load(uint8_t const* buf_ptr, const uint32_t buf_size) = 0;
        virtual SCRUB_TYPE get_scrub_type() const = 0;

        bool match(std::shared_ptr< base_scrub_req > req) const {
            if (!req) return false;

            // TODO:: add more logic to check. for example, adding a random sha256 for each req in a scrub task.
            return pg_id == req->pg_id && task_id == req->task_id && req_id == req->req_id &&
                scrub_lsn == req->scrub_lsn && get_scrub_type() == req->get_scrub_type();
        }

    public:
        pg_id_t pg_id;
        uint64_t task_id;
        uint64_t req_id;
        int64_t scrub_lsn;
        peer_id_t peer_id;
    };

    class DeepBlobScrubMap : public BaseScrubMap {
    public:
        DeepBlobScrubMap() = default;
        DeepBlobScrubMap(pg_id_t pg_id, uint64_t task_id, uint64_t req_id, int64_t scrub_lsn, peer_id_t peer_id,
                         blob_id_t start, blob_id_t end) :
                BaseScrubMap(pg_id, task_id, req_id, scrub_lsn, peer_id), start(start), end(end) {}

        flatbuffers::DetachedBuffer build_flat_buffer() const override;
        bool load(uint8_t const* buf_ptr, const uint32_t buf_size) override;
        SCRUB_TYPE get_scrub_type() const override { return SCRUB_TYPE::DEEP_BLOB; }

        void add_blob_result(const BlobRoute& blob_route, std::variant< ScrubResult, BlobHashArray > scrub_result) {
            blobs[blob_route] = scrub_result;
        }

    public:
        blob_id_t start; // inclusive
        blob_id_t end;   // exclusive
        std::map< BlobRoute, std::variant< ScrubResult, BlobHashArray > > blobs;
    };

    class ShallowBlobScrubMap : public BaseScrubMap {
    public:
        ShallowBlobScrubMap() = default;
        ShallowBlobScrubMap(pg_id_t pg_id, uint64_t task_id, uint64_t req_id, int64_t scrub_lsn, peer_id_t peer_id,
                            blob_id_t start, blob_id_t end) :
                BaseScrubMap(pg_id, task_id, req_id, scrub_lsn, peer_id), start(start), end(end) {}

        flatbuffers::DetachedBuffer build_flat_buffer() const override;
        bool load(uint8_t const* buf_ptr, const uint32_t buf_size) override;
        SCRUB_TYPE get_scrub_type() const override { return SCRUB_TYPE::SHALLOW_BLOB; }

        void add_blob(const BlobRoute& blob_route) { blobs.insert(blob_route); }

    public:
        blob_id_t start; // inclusive
        blob_id_t end;   // exclusive
        std::set< BlobRoute > blobs;
    };

    class ShallowShardScrubMap : public BaseScrubMap {
    public:
        ShallowShardScrubMap() = default;
        ShallowShardScrubMap(pg_id_t pg_id, uint64_t task_id, uint64_t req_id, int64_t scrub_lsn, peer_id_t peer_id) :
                BaseScrubMap(pg_id, task_id, req_id, scrub_lsn, peer_id) {}

        flatbuffers::DetachedBuffer build_flat_buffer() const override;
        bool load(uint8_t const* buf_ptr, const uint32_t buf_size) override;
        SCRUB_TYPE get_scrub_type() const override { return SCRUB_TYPE::SHALLOW_SHARD; }

        void add_shard(const shard_id_t& shard_id) { shards.insert(shard_id); }

    public:
        std::set< shard_id_t > shards;
    };

    class DeepShardScrubMap : public ShallowShardScrubMap {
    public:
        DeepShardScrubMap() = default;
        DeepShardScrubMap(pg_id_t pg_id, uint64_t task_id, uint64_t req_id, int64_t scrub_lsn, peer_id_t peer_id) :
                ShallowShardScrubMap(pg_id, task_id, req_id, scrub_lsn, peer_id) {}

        flatbuffers::DetachedBuffer build_flat_buffer() const override;
        bool load(uint8_t const* buf_ptr, const uint32_t buf_size) override;
        SCRUB_TYPE get_scrub_type() const override { return SCRUB_TYPE::DEEP_SHARD; }

        void add_problematic_shard(const shard_id_t& shard_id, ScrubResult scrub_result) {
            problematic_shards[shard_id] = scrub_result;
        }

    public:
        std::map< shard_id_t, ScrubResult > problematic_shards;
    };

    class PGMetaScrubMap : public BaseScrubMap {
    public:
        PGMetaScrubMap() = default;
        PGMetaScrubMap(pg_id_t pg_id, uint64_t task_id, uint64_t req_id, int64_t scrub_lsn, peer_id_t peer_id) :
                BaseScrubMap(pg_id, task_id, req_id, scrub_lsn, peer_id) {}

        flatbuffers::DetachedBuffer build_flat_buffer() const override;
        bool load(uint8_t const* buf_ptr, const uint32_t buf_size) override;
        SCRUB_TYPE get_scrub_type() const override { return SCRUB_TYPE::PG_META; }

    public:
        ScrubResult pg_meta_scrub_result{ScrubResult::NONE};
    };

    // scrub report
public:
    // shallow scrub report for a pg
    class ShallowScrubReport {
    public:
        ShallowScrubReport(pg_id_t pg_id) : pg_id_(pg_id) {}
        virtual ~ShallowScrubReport() = default;

    public:
        pg_id_t get_pg_id() const { return pg_id_; }
        void add_missing_shard(shard_id_t shard_id, peer_id_t peer_id) { missing_shard_ids[peer_id].insert(shard_id); }
        void add_missing_blob(BlobRoute blob_route, peer_id_t peer_id) { missing_blobs[peer_id].insert(blob_route); }
        const auto& get_missing_shard_ids() const { return missing_shard_ids; }
        const auto& get_missing_blobs() const { return missing_blobs; }
        virtual void merge(const std::map< peer_id_t, std::shared_ptr< BaseScrubMap > >& peer_sm_map);
        virtual void print() const;
        virtual void
        filter_out_deleted_blobs(const folly::ConcurrentHashMap< BlobRoute, int64_t >& deleted_blobs_when_scrubbing);

    public:
        std::map< peer_id_t, std::set< shard_id_t > > missing_shard_ids;
        std::map< peer_id_t, std::set< BlobRoute > > missing_blobs;
        pg_id_t pg_id_;
    };

    // deep scrub report for a pg
    class DeepScrubReport : public ShallowScrubReport {
    public:
        DeepScrubReport(pg_id_t pg_id) : ShallowScrubReport(pg_id) {}
        ~DeepScrubReport() = default;
        void add_corrupted_blob(peer_id_t peer_id, BlobRoute blob_route, ScrubResult scrub_result) {
            corrupted_blobs[peer_id][blob_route] = scrub_result;
        }
        void add_corrupted_shard(peer_id_t peer_id, shard_id_t shard_id, ScrubResult scrub_result) {
            corrupted_shards[peer_id][shard_id] = scrub_result;
        }
        void add_inconsistent_blob(BlobRoute blob_route, peer_id_t peer_id, BlobHashArray hash) {
            inconsistent_blobs[blob_route][peer_id] = hash;
        }
        void add_corrupted_pg_meta(peer_id_t peer_id, ScrubResult scrub_result) {
            corrupted_pg_metas[peer_id] = scrub_result;
        }

        const auto& get_corrupted_blobs() const { return corrupted_blobs; }
        const auto& get_corrupted_shards() const { return corrupted_shards; }
        const auto& get_inconsistent_blobs() const { return inconsistent_blobs; }
        const auto& get_corrupted_pg_metas() const { return corrupted_pg_metas; }
        void merge(const std::map< peer_id_t, std::shared_ptr< BaseScrubMap > >& peer_sm_map) override;
        void print() const override;
        virtual void filter_out_deleted_blobs(
            const folly::ConcurrentHashMap< BlobRoute, int64_t >& deleted_blobs_when_scrubbing) override;

    private:
        std::map< peer_id_t, std::map< BlobRoute, ScrubResult > > corrupted_blobs;
        std::map< peer_id_t, std::map< shard_id_t, ScrubResult > > corrupted_shards;
        std::map< BlobRoute, std::map< peer_id_t, BlobHashArray > > inconsistent_blobs;
        std::map< peer_id_t, ScrubResult > corrupted_pg_metas;
    };

    // scrub task that will be put into scrub task queue, and executed by scrub worker
public:
    struct scrub_task {
        // Default constructor (required for std::regular)
        scrub_task() :
                task_id{0},
                last_scrub_time{0},
                pg_id{0},
                is_deep_scrub{false},
                triggered{SCRUB_TRIGGER_TYPE::PERIODICALLY} {}

        // Main constructor
        scrub_task(uint64_t last_scrub_time, pg_id_t pg_id, bool is_deep_scrub, SCRUB_TRIGGER_TYPE trigger_type,
                   folly::Promise< std::shared_ptr< ShallowScrubReport > > promise) :
                task_id{scrub_task_id.fetch_add(1)},
                last_scrub_time{last_scrub_time},
                pg_id{pg_id},
                is_deep_scrub{is_deep_scrub},
                triggered{trigger_type},
                scrub_report_promise{
                    std::make_shared< folly::Promise< std::shared_ptr< ShallowScrubReport > > >(std::move(promise))} {}

        scrub_task(const scrub_task& other) = default;
        scrub_task& operator=(const scrub_task& other) = default;
        scrub_task(scrub_task&& other) noexcept = default;
        scrub_task& operator=(scrub_task&& other) noexcept = default;

        ~scrub_task() {
            // make sure there is not any unfulfilled promise
            if (scrub_report_promise && scrub_report_promise->isFulfilled() == false) {
                scrub_report_promise->setValue(nullptr);
            }
        }

        uint64_t task_id;
        uint64_t last_scrub_time;
        pg_id_t pg_id;
        bool is_deep_scrub;
        SCRUB_TRIGGER_TYPE triggered;
        std::shared_ptr< folly::Promise< std::shared_ptr< ShallowScrubReport > > > scrub_report_promise;

        // Equality operator (required for std::regular)
        bool operator==(const scrub_task& other) const noexcept { return task_id == other.task_id; }

        // the priority of `manually` is higher than `periodically`
        bool operator<(const scrub_task& other) const noexcept {
            using U = std::underlying_type_t< SCRUB_TRIGGER_TYPE >;
            // First compare by trigger type (manually > periodically)
            if (static_cast< U >(triggered) != static_cast< U >(other.triggered)) {
                return static_cast< U >(triggered) < static_cast< U >(other.triggered);
            }
            // If same trigger type, compare by task_id (earlier tasks have higher priority)
            return task_id > other.task_id;
            // TODO:: add more logic to decide the priority between two tasks after we introduce more logic for
            // automatic schedule, the following are some criteria we can consider:
            /*
            2. Time Since Last Scrub
                - PGs that haven't been scrubbed in the longest time get higher priority
                - Uses last_scrub_stamp timestamp to track
                - Prevents starvation of individual PGs
            3. Deep vs Shallow Scrub Deadline
                - Deep scrub deadline (deep_scrub_interval, default 7 days)
                - Shallow scrub deadline (scrub_interval_randomize_ratio, default 24 hours)
                - PGs approaching their deadline get boosted priority
            4. Load Balancing
                - scrub_load_threshold prevents scrubbing during high I/O load
                - scrub_min_interval and scrub_max_interval control frequency
                - Time window restrictions (scrub_begin_hour, scrub_end_hour)
            5. Concurrency Limits
                - max_scrubs (default 1) limits concurrent scrubs per sm
                - Prevents multiple PGs from overwhelming single sm
            */
        }
    };

    // PG Scrub Context, every pg being scrubbed has a scrub context to track its progress
private:
    class PGScrubContext {
    public:
        PGScrubContext(uint64_t task_id, std::unordered_set< peer_id_t > member_peer_ids) :
                task_id(task_id), member_peer_ids_(member_peer_ids) {}
        ~PGScrubContext() = default;

    public:
        bool add_scrub_map(std::shared_ptr< BaseScrubMap > bsm);
        void reset_for_new_req();
        bool wait_for_all_req_sms(std::chrono::milliseconds timeout);
        std::vector< peer_id_t > get_peers_to_retry() const;
        void cancel();
        bool is_cancelled() const { return cancelled.load(); }
        void add_deleted_blob(const BlobRoute& blob_route, int64_t delete_lsn) {
            deleted_blobs_when_scrubbing_.try_emplace(blob_route, delete_lsn);
        }

    public:
        uint64_t task_id{0};
        std::unordered_set< peer_id_t > member_peer_ids_;
        std::shared_ptr< base_scrub_req > current_req{nullptr};
        atomic_uint64_t req_id{0};
        mutable std::mutex mtx_;
        std::map< peer_id_t, std::shared_ptr< BaseScrubMap > > peer_sm_map_;
        folly::ConcurrentHashMap< BlobRoute, int64_t > deleted_blobs_when_scrubbing_;

    private:
        std::atomic_bool cancelled{false};
        std::condition_variable cv_;
    };

    /*scrub scheduler*/
public:
    void start();
    void stop();

    folly::SemiFuture< std::shared_ptr< ShallowScrubReport > >
    submit_scrub_task(const pg_id_t& pg_id, const bool is_deep, const bool force = false,
                      SCRUB_TRIGGER_TYPE trigger_type = SCRUB_TRIGGER_TYPE::PERIODICALLY);

    // cancel will only cancel a running scrub task. for those submitted but not running tasks in the queue, cancel will
    // not remove them from the queue.
    void cancel_scrub_task(const pg_id_t& pg_id);

    bool add_scrub_map(const pg_id_t pg_id, std::shared_ptr< BaseScrubMap > bsm);
    // new pg is created
    void add_pg(const pg_id_t pg_id);
    // new pg permanently removed
    void remove_pg(const pg_id_t pg_id);
    std::optional< pg_scrub_superblk > get_scrub_superblk(const pg_id_t pg_id) const;
    void save_scrub_superblk(const pg_id_t pg_id, const bool is_deep_scrub, bool force_update = true);
    void add_scrub_req(std::shared_ptr< base_scrub_req > req);
    void add_pg_deleted_blob(const pg_id_t pg_id, const BlobRoute& blob_route, int64_t delete_lsn);

    /*local scrub*/
public:
    std::shared_ptr< BaseScrubMap > local_scrub_blob(std::shared_ptr< blob_scrub_req > req);
    std::shared_ptr< ShallowShardScrubMap > local_scrub_shard(std::shared_ptr< shard_scrub_req > req);
    std::shared_ptr< PGMetaScrubMap > scrub_pg_meta(std::shared_ptr< base_scrub_req > req);

    // handlers
private:
    void scan_pg_for_scrub();
    void handle_pg_scrub_task(scrub_task task);

    bool send_scrub_req_and_wait(pg_id_t pg_id, uint64_t task_id, shared< homestore::ReplDev > pg_repl_dev,
                                 const std::unordered_set< peer_id_t >& all_member_peer_ids, const peer_id_t& my_uuid,
                                 std::shared_ptr< flatbuffers::DetachedBuffer > flat_buffer, SCRUB_TYPE scrub_type,
                                 std::shared_ptr< PGScrubContext > scrub_ctx, uint32_t max_retries,
                                 std::chrono::seconds timeout);

    bool is_eligible_for_deep_scrub(const pg_id_t& pg_id);
    bool is_eligible_for_shallow_scrub(const pg_id_t& pg_id);
    void on_pg_scrub_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie,
                                    std::vector< homestore::superblk< pg_scrub_superblk > >& stale_pg_scrub_sbs);
    void handle_deep_pg_scrub_report(std::shared_ptr< DeepScrubReport > report);
    void handle_shallow_pg_scrub_report(std::shared_ptr< ShallowScrubReport > report);
    void handle_scrub_req(std::shared_ptr< base_scrub_req > req);
    bool wait_for_scrub_lsn_commit(shared< homestore::ReplDev > repl_dev, int64_t scrub_lsn);

private:
    iomgr::timer_handle_t m_scrub_timer_hdl{iomgr::null_timer_handle};
    iomgr::io_fiber_t m_scrub_timer_fiber{nullptr};
    HSHomeObject* m_hs_home_object{nullptr};
    MPMCPriorityQueue< scrub_task > m_scrub_task_queue;
    std::shared_ptr< folly::IOThreadPoolExecutor > m_scrub_executor;
    folly::ConcurrentHashMap< pg_id_t, std::shared_ptr< PGScrubContext > > m_pg_scrub_ctx_map;
    folly::ConcurrentHashMap< pg_id_t, std::shared_ptr< homestore::superblk< pg_scrub_superblk > > > m_pg_scrub_sb_map;

    std::shared_ptr< folly::IOThreadPoolExecutor > m_scrub_req_executor;
};
} // namespace homeobject

// TODO:: consider the following scenarios and decide how we want to handle them in scrub manager
// 1 baseline resync
// 2 replace memeber
// 3 permeantly destroy pg
// 4 GC