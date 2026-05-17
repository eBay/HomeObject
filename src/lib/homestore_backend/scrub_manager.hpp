#pragma once

#include <atomic>
#include <cstdint>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <folly/futures/Future.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/MPMCQueue.h>
#pragma GCC diagnostic pop

#include <iomgr/iomgr.hpp>
#include "homeobject/common.hpp"
#include <homestore/blk.h>
#include <homestore/superblk_handler.hpp>
#include "lib/blob_route.hpp"
#include "MPMCPriorityQueue.hpp"
#include "generated/scrub_common_generated.h"
#include "generated/scrub_req_generated.h"
#include "generated/scrub_result_generated.h"

namespace homeobject {

class HSHomeObject;

ENUM(SCRUB_TRIGGER_TYPE, uint8_t, PERIODICALLY = 0, MANUALLY);
ENUM(SCRUB_TYPE, uint8_t, META = 0, DEEP_BLOB, SHALLOW_BLOB, CHECK_BLOB_EXISTENCE, CHECK_SHARD_EXISTENCE);

inline const char* scrub_result_to_string(ScrubStatus type) {
    switch (type) {
    case ScrubStatus::NONE:
        return "NONE";
    case ScrubStatus::IO_ERROR:
        return "IO_ERROR";
    case ScrubStatus::MISMATCH:
        return "MISMATCH";
    case ScrubStatus::NOT_FOUND:
        return "NOT_FOUND";
    default:
        return "UNKNOWN";
    }
}

class ScrubManager {
public:
    explicit ScrubManager(HSHomeObject* homeobject);
    ~ScrubManager();

    // Disallow copy and move
    ScrubManager(const ScrubManager&) = delete;
    ScrubManager(ScrubManager&&) = delete;
    ScrubManager& operator=(const ScrubManager&) = delete;
    ScrubManager& operator=(ScrubManager&&) = delete;

    // pg scrub superblk
    // TODO:: move this into pg_super_blk with a separate PR, since this is not a backward incompatible change.
    // other backward incompatible meta change will be:
    // 1 shard sealed lsn.

#pragma pack(1)
    struct pg_scrub_superblk {
        uint64_t last_deep_scrub_timestamp{0};
        uint64_t last_shallow_scrub_timestamp{0};
        pg_id_t pg_id{0};
        static std::string name() { return pg_scrub_meta_name; }
    };
#pragma pack()

    // scrub req
    struct scrub_req {
        scrub_req() = default;
        ~scrub_req() = default;

        scrub_req(pg_id_t pg_id, uint64_t req_id, int64_t scrub_lsn, uint64_t start_shard_id, uint64_t start_blob_id,
                  uint64_t end_shard_id, uint64_t end_blob_id, SCRUB_TYPE scrub_type, peer_id_t issuer_peer_id) :
                pg_id{pg_id},
                req_id{req_id},
                scrub_lsn{scrub_lsn},
                start_shard_id{start_shard_id},
                start_blob_id{start_blob_id},
                end_shard_id{end_shard_id},
                end_blob_id{end_blob_id},
                scrub_type{scrub_type},
                issuer_peer_id{issuer_peer_id} {}

        flatbuffers::DetachedBuffer build_flat_buffer() const;
        bool load(uint8_t const* buf_ptr, uint32_t buf_size);

        pg_id_t pg_id{0};
        uint64_t req_id{0};
        int64_t scrub_lsn{0};
        uint64_t start_shard_id{0};
        uint64_t start_blob_id{0};
        uint64_t end_shard_id{0};
        uint64_t end_blob_id{0};
        SCRUB_TYPE scrub_type{SCRUB_TYPE::META};
        peer_id_t issuer_peer_id{};
    };

    class range_scrub_result;

    // Maps to the ScrubResult / ScrubResultEntry tables in scrub_result.fbs.
    // One entry inside a ScrubResult — maps to the ScrubResultEntry FlatBuffers table.
    struct scrub_result_entry {
        shard_id_t shard_id{0};
        blob_id_t blob_id{0};

        // only when ScrubStatus is NONE, we store the CRC64 hash
        std::variant< ScrubStatus, uint64_t > status_or_hash{ScrubStatus::NONE};
    };

    // Full result for one scrub request — maps to the ScrubResult FlatBuffers table.
    class scrub_result {
    public:
        scrub_result() = default;
        scrub_result(uint64_t req_id, peer_id_t issuer_peer_id) : req_id{req_id}, issuer_peer_id{issuer_peer_id} {}
        ~scrub_result() = default;

        void add_entry(const scrub_result_entry& entry);
        flatbuffers::DetachedBuffer build_flat_buffer() const;
        bool load(uint8_t const* buf_ptr, uint32_t buf_size);
        std::string to_string() const;

        uint64_t req_id{0};
        peer_id_t issuer_peer_id{};

    private:
        friend class ScrubManager;
        friend class PGScrubContext;
        friend class range_scrub_result;
        std::map< BlobRoute, std::variant< ScrubStatus, uint64_t > > entries;
        mutable std::mutex mutex_;
    };

    // Aggregated results across a contiguous range of shards/blobs, which collects multiple scrub_result objects (one
    // per scrub req) that together cover [start_shard_id, start_shard_id] to [end_shard_id, end_blob_id].
    class range_scrub_result {
    public:
        range_scrub_result(uint64_t start_shard_id, uint64_t start_blob_id, uint64_t end_shard_id, uint64_t end_blob_id,
                           SCRUB_TYPE scrub_type, peer_id_t peer_id) :
                start_shard_id{start_shard_id},
                start_blob_id{start_blob_id},
                end_shard_id{end_shard_id},
                end_blob_id{end_blob_id},
                scrub_type{scrub_type},
                peer_id{peer_id} {}
        ~range_scrub_result() = default;

        bool match(const std::shared_ptr< range_scrub_result >& other) const {
            return other && scrub_type == other->scrub_type && start_shard_id == other->start_shard_id &&
                start_blob_id == other->start_blob_id && end_shard_id == other->end_shard_id &&
                end_blob_id == other->end_blob_id;
        }

        std::string to_string() const;

        // this will only be called in a single thread, add a lock if we want to make it thread safe.
        // TODO:: add a lock if we want to make it thread safe, but currently the design is one range_scrub_result per
        // scrub req, so it should be only accessed by one thread.
        void handle_scrub_req_resp(scrub_result& result) {
            results.merge(result.entries);

            // the newly added result should not overlap with existing results in the range_scrub_result, otherwise it
            // means there are duplicate scrub reqs covering the same shard/blob range, which should not happen.
            RELEASE_ASSERT(result.entries.empty(),
                           "should not have duplicate blob route in range_scrub_result, scrub_type={}, "
                           "start_shard_id={}, start_blob_id={}, end_shard_id={}, end_blob_id={}, req_id={}",
                           (int)scrub_type, start_shard_id, start_blob_id, end_shard_id, end_blob_id, result.req_id);
        }

        uint64_t start_shard_id{0};
        uint64_t start_blob_id{0};
        uint64_t end_shard_id{0};
        uint64_t end_blob_id{0};
        SCRUB_TYPE scrub_type{SCRUB_TYPE::META};
        peer_id_t peer_id{};

    private:
        friend class ScrubManager;
        friend class PGScrubContext;
        friend class MetaScrubReport;
        friend class ShallowScrubReport;
        friend class DeepScrubReport;
        std::map< BlobRoute, std::variant< ScrubStatus, uint64_t > > results;
    };

    // scrub report
    // base class for all scrub reports — handles PG and shard scrub results (SCRUB_TYPE::META)
    class MetaScrubReport {
    public:
        MetaScrubReport(pg_id_t pg_id) : pg_id_(pg_id) {}
        virtual ~MetaScrubReport() = default;

        pg_id_t get_pg_id() const { return pg_id_; }
        // shard_id starts from 1, so we use shard_id=0 for pg meta
        const auto& get_corrupted_shards() const { return corrupted_shard_metas; }
        const auto& get_corrupted_pg_metas() const { return corrupted_pg_metas; }
        const auto& get_inconsistent_shard_metas() const { return inconsistent_shard_metas; }
        const auto& get_missing_shard_ids() const { return missing_shard_ids; }

        virtual bool
        merge(const std::map< peer_id_t, std::shared_ptr< range_scrub_result > >& peer_scrub_result_map) = 0;
        virtual void print() const = 0;

    protected:
        void remove_shard_existence_from_peer(shard_id_t shard_id, peer_id_t peer);

    private:
        friend class ScrubManager;
        std::map< peer_id_t, std::map< shard_id_t, ScrubStatus > > corrupted_shard_metas;
        std::map< peer_id_t, ScrubStatus > corrupted_pg_metas;
        std::map< shard_id_t, std::map< peer_id_t, uint64_t > > inconsistent_shard_metas;
        // peer set means shard only exists on these peers.
        std::map< shard_id_t, std::set< peer_id_t > > missing_shard_ids;
        pg_id_t pg_id_;
        std::mutex mutex_;
    };

    // shallow scrub report for a pg — extends MetaScrubReport with missing blob tracking (SHALLOW_BLOB)
    class ShallowScrubReport : public MetaScrubReport {
    public:
        ShallowScrubReport(pg_id_t pg_id) : MetaScrubReport(pg_id) {}
        ~ShallowScrubReport() override = default;

        const auto& get_missing_blobs() const { return missing_blobs; }
        bool merge(const std::map< peer_id_t, std::shared_ptr< range_scrub_result > >& peer_scrub_result_map) override;
        void print() const override;

    protected:
        void remove_blob_existence_from_peer(BlobRoute blob_route, peer_id_t peer);

    private:
        friend class ScrubManager;
        // peer set means blob only exists on these peers.
        std::map< BlobRoute, std::set< peer_id_t > > missing_blobs;
    };

    // deep scrub report for a pg — extends ShallowScrubReport with blob corruption and inconsistency (DEEP_BLOB)
    class DeepScrubReport : public ShallowScrubReport {
    public:
        DeepScrubReport(pg_id_t pg_id) : ShallowScrubReport(pg_id) {}
        ~DeepScrubReport() override = default;

        const auto& get_corrupted_blobs() const { return corrupted_blobs; }
        const auto& get_inconsistent_blobs() const { return inconsistent_blobs; }
        bool merge(const std::map< peer_id_t, std::shared_ptr< range_scrub_result > >& peer_scrub_result_map) override;
        void print() const override;

    private:
        std::map< peer_id_t, std::map< BlobRoute, ScrubStatus > > corrupted_blobs;
        std::map< BlobRoute, std::map< peer_id_t, uint64_t > > inconsistent_blobs;
    };

    // PG Scrub Context — full definition lives in scrub_manager.cpp to avoid referencing
    // HSHomeObject::HS_PG while HSHomeObject is still an incomplete type in this header.
private:
    class PGScrubContext;

    // scrub scheduler
public:
    void start();
    void stop();

    folly::SemiFuture< std::shared_ptr< ShallowScrubReport > >
    submit_scrub_task(const pg_id_t& pg_id, const bool is_deep,
                      SCRUB_TRIGGER_TYPE trigger_type = SCRUB_TRIGGER_TYPE::PERIODICALLY);

    // cancel will only cancel a running scrub task. for those submitted but not running tasks in the queue, cancel will
    // not remove them from the queue.
    void cancel_scrub_task(const pg_id_t& pg_id);

    void handle_scrub_req_resp(const pg_id_t pg_id, std::shared_ptr< scrub_result > scrub_result);
    // new pg is created
    void add_pg(const pg_id_t pg_id);
    // new pg permanently removed
    void remove_pg(const pg_id_t pg_id);
    std::optional< pg_scrub_superblk > get_scrub_superblk(const pg_id_t pg_id) const;
    void save_scrub_superblk(const pg_id_t pg_id, const bool is_deep_scrub, bool force_update = true);
    void add_scrub_req(std::shared_ptr< scrub_req > req);

    // local scrub
    std::shared_ptr< scrub_result > local_scrub_blob(std::shared_ptr< scrub_req > req);
    std::shared_ptr< scrub_result > local_scrub_meta(std::shared_ptr< scrub_req > req);

private:
    inline static auto const pg_scrub_meta_name = std::string("PG_SCRUB");
    // TODO: persist this into metablk.
    inline static atomic_uint64_t scrub_task_id{1};

    // refer to docs/adr/scrub-blob-range-coverage.md
    static constexpr uint64_t max_scrub_batch_size = 100'000;
    static constexpr uint64_t deep_blob_scrub_batch_size = 10;

    struct scrub_task {
        scrub_task(uint64_t last_scrub_time, pg_id_t pg_id, bool is_deep_scrub, SCRUB_TRIGGER_TYPE trigger_type,
                   folly::Promise< std::shared_ptr< ShallowScrubReport > > promise) :
                task_id{scrub_task_id.fetch_add(1)},
                last_scrub_time{last_scrub_time},
                pg_id{pg_id},
                is_deep_scrub{is_deep_scrub},
                triggered{trigger_type},
                scrub_report_promise{
                    std::make_shared< folly::Promise< std::shared_ptr< ShallowScrubReport > > >(std::move(promise))} {}

        ~scrub_task() {
            if (scrub_report_promise && !scrub_report_promise->isFulfilled()) {
                scrub_report_promise->setValue(nullptr);
            }
        }

        scrub_task(scrub_task&&) = default;
        scrub_task& operator=(scrub_task&&) = default;
        scrub_task(const scrub_task&) = delete;
        scrub_task& operator=(const scrub_task&) = delete;

        uint64_t task_id;
        uint64_t last_scrub_time;
        pg_id_t pg_id;
        bool is_deep_scrub;
        SCRUB_TRIGGER_TYPE triggered;
        std::shared_ptr< folly::Promise< std::shared_ptr< ShallowScrubReport > > > scrub_report_promise;

        bool operator==(const scrub_task& other) const noexcept { return task_id == other.task_id; }

        // manually > periodically; among equal triggers, earlier task_id wins.
        bool operator<(const scrub_task& other) const noexcept {
            using U = std::underlying_type_t< SCRUB_TRIGGER_TYPE >;
            if (static_cast< U >(triggered) != static_cast< U >(other.triggered)) {
                return static_cast< U >(triggered) < static_cast< U >(other.triggered);
            }
            return task_id > other.task_id;

            /*
            TODO:: add more logic to decide the priority between two tasks after we introduce more logic for
            automatic schedule, the following are some criteria we can consider:

            1. Time Since Last Scrub
                - PGs that haven't been scrubbed in the longest time get higher priority
                - Uses last_scrub_stamp timestamp to track
                - Prevents starvation of individual PGs
            2. Deep vs Shallow Scrub Deadline
                - Deep scrub deadline (deep_scrub_interval, default 7 days)
                - Shallow scrub deadline (scrub_interval_randomize_ratio, default 24 hours)
                - PGs approaching their deadline get boosted priority
            3. other considerations.
            */
        }
    };

    void scan_pg_for_scrub();
    void handle_pg_scrub_task(scrub_task task);
    bool is_eligible_for_deep_scrub(const pg_id_t& pg_id);
    bool is_eligible_for_shallow_scrub(const pg_id_t& pg_id);
    void on_pg_scrub_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie,
                                    std::vector< homestore::superblk< pg_scrub_superblk > >& stale_pg_scrub_sbs);
    void handle_deep_pg_scrub_report(std::shared_ptr< DeepScrubReport > report);
    void handle_shallow_pg_scrub_report(std::shared_ptr< ShallowScrubReport > report);
    void handle_scrub_req(std::shared_ptr< scrub_req > req);
    bool wait_for_scrub_lsn_commit(shared< homestore::ReplDev > repl_dev, int64_t scrub_lsn);
    uint64_t compute_crc64(const void* data, size_t len, uint64_t crc = 0) const;

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
// 2 replace member
// 3 permanently destroy pg
// 4 GC