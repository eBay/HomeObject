#include "hs_homeobject.hpp"
#include <homestore/btree/btree_req.hpp>
#include <homestore/btree/btree_kv.hpp>
#include <array>
#include <sstream>
#include <algorithm>

namespace homeobject {

SISL_LOGGING_DECL(scrubmgr)

#define SCRUBLOG(level, pg_id, task_id, msg, ...)                                                                      \
    LOG##level##MOD(scrubmgr, "[pg_id={}, task_id={}] " msg, pg_id, task_id, ##__VA_ARGS__)

#define SCRUBLOGD(pg_id, task_id, msg, ...) SCRUBLOG(DEBUG, pg_id, task_id, msg, ##__VA_ARGS__)
#define SCRUBLOGI(pg_id, task_id, msg, ...) SCRUBLOG(INFO, pg_id, task_id, msg, ##__VA_ARGS__)
#define SCRUBLOGW(pg_id, task_id, msg, ...) SCRUBLOG(WARN, pg_id, task_id, msg, ##__VA_ARGS__)
#define SCRUBLOGE(pg_id, task_id, msg, ...) SCRUBLOG(ERROR, pg_id, task_id, msg, ##__VA_ARGS__)
#define SCRUBLOGC(pg_id, task_id, msg, ...) SCRUBLOG(CRITICAL, pg_id, task_id, msg, ##__VA_ARGS__)

class ScrubManager::PGScrubContext {
public:
    PGScrubContext(uint64_t task_id, const HSHomeObject::HS_PG* hs_pg);
    ~PGScrubContext();

    bool scrub_meta_batch(std::shared_ptr< ScrubManager::MetaScrubReport > scrub_report, shard_id_t start_shard_id,
                          shard_id_t end_shard_id, blob_id_t last_blob_id, int64_t scrub_lsn,
                          std::map< shard_id_t, uint32_t >& shard_blob_count_in_batch);

    bool scrub_blob_batch(std::shared_ptr< ScrubManager::ShallowScrubReport > scrub_report, shard_id_t start_shard_id,
                          shard_id_t end_shard_id, blob_id_t last_blob_id, int64_t scrub_lsn, bool is_deep_scrub);

    void reconcile_scrub_report(std::shared_ptr< ScrubManager::ShallowScrubReport > scrub_report);

    folly::Future< bool > check_existence_in_peer(peer_id_t peer_id, BlobRoute blob, bool check_blob);

    void handle_scrub_req_resp(std::shared_ptr< ScrubManager::scrub_result > result);
    uint64_t random_req_id() const;
    void send_req_to_peer(const ScrubManager::scrub_req& req, const peer_id_t& peer_id);
    void cancel() {
        cancelled.store(true);
        const auto pg_id = hs_pg->pg_id();
        SCRUBLOGI(pg_id, task_id, "scrub task is cancelled");
    }

    uint64_t task_id{0};
    std::atomic_bool cancelled{false};

private:
    std::shared_ptr< folly::IOThreadPoolExecutor > m_scrub_executor;
    const HSHomeObject::HS_PG* hs_pg;
    folly::ConcurrentHashMap< peer_id_t,
                              std::shared_ptr< folly::MPMCQueue< std::shared_ptr< ScrubManager::scrub_result > > > >
        peer_scrub_result_queue_map_;
};

ScrubManager::ScrubManager(HSHomeObject* homeobject) : m_hs_home_object{homeobject} {
    // Register meta_service handlers to recover pg scrub superblocks
    std::vector< homestore::superblk< pg_scrub_superblk > > stale_pg_scrub_sbs;
    homestore::meta_service().register_handler(
        pg_scrub_meta_name,
        [this, &stale_pg_scrub_sbs](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_pg_scrub_meta_blk_found(std::move(buf), voidptr_cast(mblk), stale_pg_scrub_sbs);
        },
        nullptr, true);
    homestore::meta_service().read_sub_sb(pg_scrub_meta_name);

    // remove stale pg scrub superblocks
    for (auto& sb : stale_pg_scrub_sbs)
        sb.destroy();
}

ScrubManager::~ScrubManager() { stop(); }

void ScrubManager::scan_pg_for_scrub() {
    for (auto const& [pg_id, _] : m_pg_scrub_sb_map) {
        if (is_eligible_for_deep_scrub(pg_id)) {
            LOGINFOMOD(scrubmgr, "pg={} is eligible for deep scrub, submit scrub task", pg_id);
            submit_scrub_task(pg_id, true)
                .via(&folly::InlineExecutor::instance())
                .thenValue([this, pg_id](std::shared_ptr< ShallowScrubReport > report) {
                    if (!report) {
                        LOGERRORMOD(scrubmgr, "deep scrub failed for pg={}", pg_id);
                        return;
                    }
                    LOGINFOMOD(scrubmgr, "deep scrub is completed for pg={}", pg_id);
                    auto deep_report = std::dynamic_pointer_cast< DeepScrubReport >(report);
                    if (!deep_report) {
                        LOGERRORMOD(scrubmgr, "report for deep scrub cannot be casted to DeepScrubReport for pg={}",
                                    pg_id);
                        return;
                    }
                    handle_deep_pg_scrub_report(std::move(deep_report));
                });
            return;
        }

        if (is_eligible_for_shallow_scrub(pg_id)) {
            LOGINFOMOD(scrubmgr, "pg={} is eligible for shallow scrub, submit scrub task", pg_id);
            submit_scrub_task(pg_id, false)
                .via(&folly::InlineExecutor::instance())
                .thenValue([this, pg_id](std::shared_ptr< ShallowScrubReport > report) {
                    if (!report) {
                        LOGERRORMOD(scrubmgr, "shallow scrub failed for pg={}", pg_id);
                        return;
                    }
                    LOGINFOMOD(scrubmgr, "shallow scrub is completed for pg={}", pg_id);
                    handle_shallow_pg_scrub_report(std::move(report));
                });
            return;
        }

        LOGDEBUGMOD(scrubmgr, "pg={} is not eligible for any scrubbing", pg_id);
    }
}

void ScrubManager::handle_shallow_pg_scrub_report(std::shared_ptr< ShallowScrubReport > report) {
    if (!report) {
        LOGERRORMOD(scrubmgr, "Shallow scrub report is null!");
        return;
    }

    report->print();
    // TODO:: add more logic, log event for notification, report to metrics?.
}

void ScrubManager::handle_deep_pg_scrub_report(std::shared_ptr< DeepScrubReport > report) {
    if (!report) {
        LOGERRORMOD(scrubmgr, "Deep scrub report is null!");
        return;
    }

    report->print();
    // TODO:: add more logic, log event for notification, report to metrics?.
}

bool ScrubManager::is_eligible_for_deep_scrub(const pg_id_t& pg_id) {
    // TODO:: add the real eligibility check logic
    return false;
}

bool ScrubManager::is_eligible_for_shallow_scrub(const pg_id_t& pg_id) {
    // TODO:: add the real eligibility check logic
    return false;
}

void ScrubManager::start() {
    // 1 set scrub task handling threads.
    // TODO :: make thread count configurable, thread number is the most concurrent scrub tasks that can be handled
    // concurrently. Too many concurrent scrub tasks may bring too much pressure to the node
    const auto most_concurrent_scrub_task_num = 2;
    m_scrub_executor = std::make_shared< folly::IOThreadPoolExecutor >(most_concurrent_scrub_task_num);
    for (int i = 0; i < most_concurrent_scrub_task_num; ++i) {
        m_scrub_executor->add([this]() {
            while (true) {
                // if no available scrub task, it will be blocked here.
                auto pop_result = m_scrub_task_queue.pop();
                if (pop_result.is_closed()) {
                    LOGINFOMOD(scrubmgr, "scrub task queue is stopped, no need to handle scrub task anymore!");
                    break;
                }
                RELEASE_ASSERT(pop_result.value.has_value() && pop_result.is_ok(),
                               "pop from scrub task queue should not fail when it is not closed!");
                auto task = std::move(pop_result.value.value());
                // we handle pg scrub task in a single thread , so that we can control the concurrent scrub tasks by
                // controlling the thread number of m_scrub_executor.
                handle_pg_scrub_task(std::move(task));
            }
        });
    }

    // 2 set scrub req handling threads.
    const auto most_concurrent_scrub_req_num = 2;
    // we don't set priority for req as that of task, only control the concurrency to not bring too much io/cpu pressure
    // to this node.
    m_scrub_req_executor = std::make_shared< folly::IOThreadPoolExecutor >(most_concurrent_scrub_req_num);

    iomanager.run_on_wait(iomgr::reactor_regex::random_worker, [&]() {
        m_scrub_timer_fiber = iomanager.iofiber_self();
        // TODO: make the interval configurable, for now set it to 60 seconds
        m_scrub_timer_hdl = iomanager.schedule_thread_timer(60ull * 1000 * 1000 * 1000, true, nullptr /*cookie*/,
                                                            [this](void*) { scan_pg_for_scrub(); });
    });
    LOGINFOMOD(scrubmgr, "scrub manager started!");
}

void ScrubManager::stop() {
    // shutdown timer — only if it was ever started
    if (m_scrub_timer_hdl != iomgr::null_timer_handle) {
        RELEASE_ASSERT(m_scrub_timer_fiber,
                       "m_scrub_timer_hdl is not null_timer_handle, but m_scrub_timer_fiber is null, fatal error!");
        LOGINFOMOD(scrubmgr, "stop scrub scheduler timer");
        iomanager.run_on_wait(m_scrub_timer_fiber, [&]() {
            iomanager.cancel_timer(m_scrub_timer_hdl, true);
            m_scrub_timer_hdl = iomgr::null_timer_handle;
        });
        m_scrub_timer_fiber = nullptr;
    } else {
        LOGINFOMOD(scrubmgr, "scrub scheduler timer is not running, no need to stop it");
    }

    // cancel all the running scrub tasks and clear the scrub task queue.
    // TODO:: add a stopped flag to avoid adding new scrub task if stopped.
    if (!m_scrub_task_queue.is_closed()) { m_scrub_task_queue.close(); }
    for (auto& [_, pg_scrub_ctx] : m_pg_scrub_ctx_map) {
        pg_scrub_ctx->cancel();
    }

    if (m_scrub_executor) {
        m_scrub_executor->stop();
        m_scrub_executor.reset();
    }
    if (m_scrub_req_executor) {
        m_scrub_req_executor->stop();
        m_scrub_req_executor.reset();
    }
    LOGINFOMOD(scrubmgr, "scrub manager stopped!");
}

void ScrubManager::add_scrub_req(std::shared_ptr< scrub_req > req) {
    m_scrub_req_executor->add([this, req = std::move(req)]() { handle_scrub_req(req); });
}

void ScrubManager::handle_scrub_req_resp(const pg_id_t pg_id, std::shared_ptr< scrub_result > result) {
    auto pg_scrub_ctx_it = m_pg_scrub_ctx_map.find(pg_id);
    if (pg_scrub_ctx_it == m_pg_scrub_ctx_map.end()) {
        LOGERRORMOD(scrubmgr, "cannot find scrub context for pg_id={}, fail to add scrub result!", pg_id);
        return;
    }

    pg_scrub_ctx_it->second->handle_scrub_req_resp(std::move(result));
}

void ScrubManager::handle_scrub_req(std::shared_ptr< scrub_req > req) {
    if (!req) {
        LOGERRORMOD(scrubmgr, "scrub req is null, cannot handle it!");
        return;
    }

    const auto& pg_id = req->pg_id;
    const auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        LOGERRORMOD(scrubmgr, "cannot find hs_pg for pg {}, fail to handle scrub req!", pg_id);
        return;
    }

    const auto& pg_repl_dev = hs_pg->repl_dev_;
    if (!pg_repl_dev) {
        LOGERRORMOD(scrubmgr, "repl_dev is null for pg {}, fail to handle scrub req!", pg_id);
        return;
    }

    // leader still need to handle the scrub req, as leader also needs to do scrub and send scrub result to itself to
    // trigger the logic after receiving scrub result.

    std::shared_ptr< scrub_result > range_result;
    auto& remote_peer_id = req->issuer_peer_id;

    // sleep for a while to avoid handling scrub req immediately, which may cause high IOPS to the node.
    // for example, handling a deep blob scrub req will take some io resource. we sleep 1s here so that there is a
    // interval in the middle of handing two deep blob scrub reqs.

    // TODO:: for different scrub req, we sleep different duration.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 1 do scrub
    const auto& scrub_type = req->scrub_type;
    switch (scrub_type) {
    case SCRUB_TYPE::META: {
        LOGDEBUGMOD(scrubmgr, "handling meta scrub req for pg {}", pg_id);
        range_result = local_scrub_meta(req);
        break;
    }
    case SCRUB_TYPE::DEEP_BLOB:
    case SCRUB_TYPE::SHALLOW_BLOB: {
        LOGDEBUGMOD(scrubmgr, "handling blob scrub req for pg {}, scrub_type={}", pg_id, scrub_type);
        range_result = local_scrub_blob(req);
        break;
    }
    default:
        RELEASE_ASSERT(false, "unknown scrub req type: {}!", scrub_type);
    }

    if (!range_result) {
        LOGERRORMOD(scrubmgr, "fail to handle scrub req for pg {}, scrub_type={}, drop it!", pg_id, scrub_type);
        return;
    }

    // 2 send scrub result back to leader
    auto flatbuffer = range_result->build_flat_buffer();
    sisl::io_blob_list_t blob_list;
    blob_list.emplace_back(flatbuffer.data(), flatbuffer.size(), false);
    // no need to retry, leader will handle retries
    pg_repl_dev->data_request_unidirectional(remote_peer_id, HSHomeObject::PUSH_SCRUB_RESULT, blob_list)
        .via(&folly::InlineExecutor::instance())
        .thenValue([pg_id, remote_peer_id, flatbuffer = std::move(flatbuffer), scrub_type](auto&& response) {
            if (response.hasError()) {
                LOGERRORMOD(scrubmgr, "failed to send scrub result to peer {} in pg {}, scrub_type:{}, error={}",
                            remote_peer_id, pg_id, scrub_type, response.error());
                return;
            }

            LOGDEBUGMOD(scrubmgr, "successfully sent scrub result to peer {} in pg {}, scrub_type:{}", remote_peer_id,
                        pg_id, scrub_type);
        });
}

bool ScrubManager::wait_for_scrub_lsn_commit(shared< homestore::ReplDev > repl_dev, int64_t scrub_lsn) {
    if (!repl_dev) {
        LOGERRORMOD(scrubmgr, "repl_dev is null, cannot wait for scrub lsn commit!");
        return false;
    }

    // TODO:: make this configurable
    const auto wait_retry_times = 5;
    for (auto i = 0; i < wait_retry_times; ++i) {
        auto commit_lsn = repl_dev->get_last_commit_lsn();
        if (commit_lsn >= scrub_lsn) {
            LOGINFOMOD(scrubmgr, "commit lsn {} is greater than or equal to scrub lsn {}, wait successfully",
                       commit_lsn, scrub_lsn);
            return true;
        }
        LOGDEBUGMOD(scrubmgr,
                    "commit lsn {} is less than scrub lsn {}, wait for 1 second before retrying, retry times {}/{}",
                    commit_lsn, scrub_lsn, i + 1, wait_retry_times);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return false;
}

uint64_t ScrubManager::compute_crc64(const void* data, size_t len, uint64_t crc) const {
    static constexpr uint64_t kCrc64Poly = 0x42F0E1EBA9EA3693ULL;
    static constexpr auto kCrc64Table = []() {
        std::array< uint64_t, 256 > t{};
        for (int i = 0; i < 256; ++i) {
            uint64_t c = static_cast< uint64_t >(i) << 56;
            for (int b = 0; b < 8; ++b) {
                c = (c & 0x8000000000000000ULL) ? ((c << 1) ^ kCrc64Poly) : (c << 1);
            }
            t[i] = c;
        }
        return t;
    }();

    const uint8_t* p = static_cast< const uint8_t* >(data);
    while (len--) {
        uint8_t idx = static_cast< uint8_t >((crc >> 56) ^ *p++);
        crc = kCrc64Table[idx] ^ (crc << 8);
    }
    return crc;
}

std::shared_ptr< ScrubManager::scrub_result > ScrubManager::local_scrub_blob(std::shared_ptr< scrub_req > req) {
    if (!req) {
        LOGERRORMOD(scrubmgr, "blob scrub req is null, cannot handle it!");
        return nullptr;
    }

    const auto& req_id = req->req_id;
    const auto& scrub_lsn = req->scrub_lsn;
    const auto& pg_id = req->pg_id;
    const auto& scrub_type = req->scrub_type;

    if (scrub_type != SCRUB_TYPE::DEEP_BLOB && scrub_type != SCRUB_TYPE::SHALLOW_BLOB) {
        LOGERRORMOD(scrubmgr,
                    "invalid scrub req type for local_scrub_blob, pg_id={}, req_id={}, scrub_type={}, scrub_lsn={}",
                    pg_id, req_id, scrub_type, scrub_lsn);
        return nullptr;
    }

    LOGDEBUGMOD(scrubmgr, "handling blob scrub req for pg {}, req_id={}, scrub_lsn={}, scrub_type={}", pg_id, req_id,
                scrub_lsn, scrub_type);

    auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        LOGERRORMOD(scrubmgr, "req_id={} cannot find hs_pg for pg={}, fail to do deep blob scrub!", req_id, pg_id);
        return nullptr;
    }

    if (!wait_for_scrub_lsn_commit(hs_pg->repl_dev_, scrub_lsn)) {
        LOGERRORMOD(scrubmgr,
                    "pg_id={}, req_id={}, commit lsn is not advanced to scrub lsn {} after waiting for a while, fail "
                    "to do {} blob scrub",
                    pg_id, req_id, scrub_lsn, scrub_type == SCRUB_TYPE::DEEP_BLOB ? "deep" : "shallow");
        return nullptr;
    }

    if (req->start_shard_id > req->end_shard_id) {
        LOGERRORMOD(
            scrubmgr,
            "received incorrect blob scrub req, start_shard_id={}, end_shard_id={}, start_blob_id={}, end_blob_id={}",
            req->start_shard_id, req->end_shard_id, req->start_blob_id, req->end_blob_id);
        return nullptr;
    }

    // refer to docs/adr/scrub-blob-range-coverage.md
    // TODO:: make this configurable.
    uint32_t batch_capacity = static_cast< uint32_t >(
        scrub_type == SCRUB_TYPE::SHALLOW_BLOB ? max_scrub_batch_size : deep_blob_scrub_batch_size);

    const auto start = BlobRouteKey{BlobRoute{req->start_shard_id, req->start_blob_id}};
    const auto end = BlobRouteKey{BlobRoute{req->end_shard_id, req->end_blob_id}};
    homestore::BtreeQueryRequest< BlobRouteKey > query_req{
        homestore::BtreeKeyRange< BlobRouteKey >{start, true /* inclusive */, end, true /* inclusive */},
        homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY, batch_capacity,
        [last_blob_id = req->end_blob_id](homestore::BtreeKey const& key, homestore::BtreeValue const& value) -> bool {
            BlobRouteValue blob_value{value};
            BlobRouteKey blob_key{key};

            return blob_value.pbas() != HSHomeObject::tombstone_pbas && blob_key.key().blob <= last_blob_id;
        }};

    std::vector< std::pair< BlobRouteKey, BlobRouteValue > > out;
    auto const status = hs_pg->index_table_->query(query_req, out);

    // if there are more blobs to be scrubbed, we will handle them in the next scrub req, so we don't consider has_more
    // as an error here.
    if (status != homestore::btree_status_t::success && status != homestore::btree_status_t::has_more) {
        LOGERRORMOD(
            scrubmgr,
            "pg_id={}, req_id={}, scrub_type={}, scrub_lsn={}, Failed to query blobs in index table for status={}",
            pg_id, req_id, scrub_type, scrub_lsn, status);
        return nullptr;
    }

    auto blob_scrub_result = std::make_shared< ScrubManager::scrub_result >(req_id, m_hs_home_object->our_uuid());

    if (scrub_type == SCRUB_TYPE::SHALLOW_BLOB) {
        // for shallow blob scrubbing, we only check the existence of blobs, no io will be issued to hard drive.
        for (const auto& [k, _] : out) {
            blob_scrub_result->add_entry({k.key().shard, k.key().blob, ScrubStatus::NONE});
        }
        LOGDEBUGMOD(scrubmgr,
                    "pg_id={}, req_id={}, scrub_lsn={}, shallow blob scrub completed, return {} blobs in range [{},{})",
                    pg_id, req_id, scrub_lsn, blob_scrub_result->entries.size(), start, end);
        return blob_scrub_result;
    }

    // Sort blobs by PBA (physical block address) for sequential disk access, this is a best effort, not guaranteed,
    // since client io will move the disk pointer and break the sequence of io.
    std::sort(out.begin(), out.end(), [](const auto& a, const auto& b) {
        // Compare by PBA single blkid for ordering
        const auto pba_a = a.second.pbas().to_single_blkid();
        const auto pba_b = b.second.pbas().to_single_blkid();
        return pba_a.blk_num() < pba_b.blk_num();
    });

    // deep scrub: read and check blobs.
    auto& data_service = homestore::data_service();
    const auto blk_size = data_service.get_blk_size();
    std::vector< folly::Future< folly::Unit > > futs;

    for (const auto& [k, v] : out) {
        auto pba = v.pbas();
        auto total_size = pba.blk_count() * blk_size;
        sisl::sg_list data_sgs;
        data_sgs.size = total_size;
        data_sgs.iovs.emplace_back(
            iovec{.iov_base = iomanager.iobuf_alloc(blk_size, total_size), .iov_len = total_size});

        const auto& shard_id = k.key().shard;
        const auto& blob_id = k.key().blob;

        futs.emplace_back(std::move(
            data_service.async_read(pba, data_sgs, total_size)
                .thenValue([this, shard_id, blob_id, data_sgs = std::move(data_sgs), blob_scrub_result](auto&& err) {
                    auto blob = data_sgs.iovs[0].iov_base;
                    struct buffer_free_guard {
                        uint8_t* buf;
                        ~buffer_free_guard() { iomanager.iobuf_free(buf); }
                    } guard{reinterpret_cast< uint8_t* >(blob)};

                    ScrubManager::scrub_result_entry entry{shard_id, blob_id, ScrubStatus::NONE};

                    if (err) {
                        LOGERRORMOD(scrubmgr, "Failed to read blob for deep scrub, shard_id={}, blob_id={}, error={}",
                                    shard_id, blob_id, err.message());
                        entry.status_or_hash = ScrubStatus::IO_ERROR;
                    } else {
                        const auto blob_verify_succeed = m_hs_home_object->verify_blob(blob, shard_id, blob_id, true);
                        if (!blob_verify_succeed) {
                            // note that, if gc kicks in, the pba might be overwritten and lead to verification
                            // failure.

                            // FIXME:: handle this case by query and read the blob again.
                            LOGERRORMOD(scrubmgr, "Blob verification failed for deep scrub, shard_id={}, blob_id={}",
                                        shard_id, blob_id);
                            entry.status_or_hash = ScrubStatus::MISMATCH;
                        } else {
                            // we only calculate crc64 for data part.
                            const auto* header = reinterpret_cast< const HSHomeObject::BlobHeader* >(blob);
                            const auto* blob_data = reinterpret_cast< const uint8_t* >(blob) + header->data_offset;
                            entry.status_or_hash = compute_crc64(blob_data, header->blob_size);
                        }
                    }

                    LOGDEBUGMOD(scrubmgr, "add entry to blob scrub result: shard_id={}, blob_id={}", entry.shard_id,
                                entry.blob_id);
                    blob_scrub_result->add_entry(entry);
                })));
    }

    folly::collectAllUnsafe(futs).wait();

    LOGDEBUGMOD(scrubmgr, "pg_id={}, req_id={}, deep blob scrub completed, found {} blobs in range [{},{}] to [{},{})",
                pg_id, req_id, out.size(), req->start_shard_id, req->start_blob_id, req->end_shard_id,
                req->end_blob_id);

    return blob_scrub_result;
}

std::shared_ptr< ScrubManager::scrub_result > ScrubManager::local_scrub_meta(std::shared_ptr< scrub_req > req) {
    if (!req) {
        LOGERRORMOD(scrubmgr, "meta scrub req is null, cannot handle it!");
        return nullptr;
    }

    const auto& req_id = req->req_id;
    const auto& scrub_lsn = req->scrub_lsn;
    const auto& pg_id = req->pg_id;
    const auto& end_shard_id = req->end_shard_id;
    const auto& start_shard_id = req->start_shard_id;

    if (req->scrub_type != SCRUB_TYPE::META) {
        LOGERRORMOD(scrubmgr,
                    "invalid scrub req type for local_scrub_meta, pg_id={}, req_id={}, scrub_type={}, scrub_lsn={}",
                    pg_id, req_id, req->scrub_type, scrub_lsn);
        return nullptr;
    }

    LOGDEBUGMOD(scrubmgr, "handling meta scrub req for pg {}, req_id={}, scrub_lsn={}", pg_id, req_id, scrub_lsn);

    auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        LOGERRORMOD(scrubmgr, "cannot find hs_pg for pg={}, fail to scrub meta!", pg_id);
        return nullptr;
    }

    if (start_shard_id > end_shard_id) {
        LOGERRORMOD(scrubmgr, "received incorrect meta scrub req, start_shard_id={} > end_shard_id={}", start_shard_id,
                    end_shard_id);
        return nullptr;
    }

    if (!wait_for_scrub_lsn_commit(hs_pg->repl_dev_, scrub_lsn)) {
        LOGERRORMOD(
            scrubmgr,
            "commit lsn is not advanced to scrub lsn {} after waiting for a while, fail to do local shard scrub, pg={}",
            scrub_lsn, pg_id);
        return nullptr;
    }

    RELEASE_ASSERT(0 == req->start_blob_id, "for meta scrub, start_blob_id should be 0, pg_id={}, req_id={}", pg_id,
                   req_id);
    const auto& end_blob_id = req->end_blob_id;

    LOGDEBUGMOD(scrubmgr,
                "received meta scrub req for pg {}, req_id={}, scrub_lsn={}, start_shard_id={}, end_shard_id={}, "
                "end_blob_id={}",
                pg_id, req_id, scrub_lsn, start_shard_id, end_shard_id, end_blob_id);

    auto meta_scrub_result = std::make_shared< ScrubManager::scrub_result >(req_id, m_hs_home_object->our_uuid());

    // we don't have a specific method to directly read a specific pg/shard meta_blk. the only way we can read metablk
    // for now is registering a handler and then call meta_service read_sub_sb. we just skip this step for now
    // TODO::
    // 1 to add a new method to directly read a specific meta_blk in meta_service. or we do this by registering handler
    // and scan!!
    // 2 add the ScrubResult for metablk to scrub_result_entry
    // 3 calculate the hash of metablk.

    // for empty shard, without deleting shard(to be done), we can find the shard meta blk and shard meta data in
    // memory, but cannot find any blob of this shard in pg_index_table. we consider the pg_index_table, not the
    // shard_meta_blk, as the source of truth for meta scrub, so we will scan the index table to do meta scrub and don't
    // care about empty shard, since no valid data in it.

    // scrub pg meta if start_shard_id is 0. since shard_id starts from 1, we use shard_id 0 to represent pg meta for
    // convenience.
    if (0 == (start_shard_id & homeobject::shard_mask)) {
        LOGDEBUGMOD(scrubmgr, "scrubbing pg meta of pg={}", pg_id);
        // blob_id here means the shard count of this pg. Since this is useless ATM, we just do it like this.

        // TODO:: do real pg meta blk scrub and calculate the hash for pg meta blk, for now we just skip this step and
        // set its hash to 0.
        meta_scrub_result->add_entry({0, hs_pg->total_shards() /*pg shard count*/, uint64_t(0) /*pg metablk hash*/});
    }

    auto start_key = BlobRouteKey{BlobRoute{std::max(uint64_t{1}, start_shard_id), 0}};
    auto end_key = BlobRouteKey{BlobRoute{end_shard_id, end_blob_id}};

    ScrubManager::scrub_result_entry entry{0, 0, ScrubStatus::NONE};
    // max_scrub_batch_size here means how many shards we want to scrub in one batch, referring to
    // docs/adr/scrub-blob-range-coverage.md

    // TODO: make this configurable
    uint32_t batch_capacity = max_scrub_batch_size;

    homestore::BtreeQueryRequest< BlobRouteKey > qr{
        homestore::BtreeKeyRange< BlobRouteKey >{start_key, true, end_key, true},
        homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY, std::numeric_limits< uint32_t >::max(),
        [&entry, &batch_capacity, end_blob_id, meta_scrub_result](homestore::BtreeKey const& key,
                                                                  homestore::BtreeValue const& value) mutable -> bool {
            if (batch_capacity) {
                BlobRouteKey blob_key{key};
                BlobRouteValue blob_value{value};

                const auto shard_id = blob_key.key().shard;
                if (shard_id != entry.shard_id) {
                    // coming to a new shard
                    if (entry.shard_id) {
                        // TODO:: do real shard meta blk scrub and calculate the hash for shard meta blk, for now we
                        // just set its hash to 0.
                        entry.status_or_hash = uint64_t(0);
                        meta_scrub_result->add_entry(entry);
                        if (--batch_capacity == 0) {
                            // so that it will not be added to meta_scrub_result again outside of the query loop.
                            entry.shard_id = 0;
                            return false; // Continue scanning
                        }
                    }
                    // reset entry for the new shard
                    entry.shard_id = shard_id;
                    entry.blob_id = 0;
                }

                // there might be some deletion happens when we do meta scrub and lead to the inconsistency of the blob
                // count of a specific shard among different replicas. this does not matter, since we will use the max
                // blobs count of this shard in different replicas as the actual blob count of this shard, referring to
                // docs/adr/scrub-blob-range-coverage.md
                const auto blob_id = blob_key.key().blob;
                if (blob_value.pbas() != HSHomeObject::tombstone_pbas && blob_id <= end_blob_id) { entry.blob_id++; }
            }

            return false; // Continue scanning
        }};

    std::vector< std::pair< BlobRouteKey, BlobRouteValue > > out;
    auto const ret = hs_pg->index_table_->query(qr, out);
    if (ret != homestore::btree_status_t::success && ret != homestore::btree_status_t::has_more) {
        LOGERRORMOD(scrubmgr, "[pg={}] failed to query index table, error={}", pg_id, ret);
        return nullptr;
    }

    // if we the last scrubbed shard in this batch is not 0, we add the scrub_result_entry for it here since it cannot
    // be added in the query loop.
    if (entry.shard_id) {
        LOGDEBUGMOD(scrubmgr, "add last entry, shard_id={}, blob_id={}", entry.shard_id, entry.blob_id);
        meta_scrub_result->add_entry(entry);
    }

    LOGDEBUGMOD(scrubmgr, "meta scrub completed, checked {} shards in range [{},{}) to [{}, {}] in pg={}",
                meta_scrub_result->entries.size(), start_shard_id, 0, end_shard_id, end_blob_id, pg_id);

    return meta_scrub_result;
}

folly::SemiFuture< std::shared_ptr< ScrubManager::ShallowScrubReport > >
ScrubManager::submit_scrub_task(const pg_id_t& pg_id, const bool is_deep, SCRUB_TRIGGER_TYPE trigger_type) {
    LOGINFOMOD(scrubmgr, "submit a scrub task for pg={}, deep_scrub={}, trigger_type={}", pg_id, is_deep, trigger_type);

    // Check if a scrub task is already running for this PG.
    // Note: There's still a small race window between this check and task execution in handle_pg_scrub_task,
    // but the in_scrubbing CAS below provides the final guard. This check prevents unnecessary work.
    auto it = m_pg_scrub_ctx_map.find(pg_id);
    if (it != m_pg_scrub_ctx_map.end()) {
        LOGWARNMOD(scrubmgr, "a scrub task is already running for pg={}, no need to submit another one!", pg_id);
        return folly::makeSemiFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
    }

    const auto ps_scrub_super_blk_it = m_pg_scrub_sb_map.find(pg_id);
    if (ps_scrub_super_blk_it == m_pg_scrub_sb_map.end()) {
        LOGERRORMOD(scrubmgr, "cannot find scrub superblk for pg={}, fail to submit scrub task!", pg_id);
        return folly::makeSemiFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
    }

    // Get the PG and check its state
    const auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        LOGERRORMOD(scrubmgr, "cannot find hs_pg for pg={}, fail to submit scrub task!", pg_id);
        return folly::makeSemiFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
    }

    // Check if pg_state is HEALTHY (state must be 0)
    const auto current_state = hs_pg->pg_state_.get();
    if (current_state != 0) {
        LOGWARNMOD(scrubmgr, "pg={} is not in HEALTHY state (current_state={}), cannot submit scrub task!", pg_id,
                   current_state);
        return folly::makeSemiFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
    }

    // TODO:: use PGStateMask::SCRUBBING state to replace the in_scrubbing flag after cm supports
    // PGStateMask::SCRUBBING. in_scrubbing here is used to indicate whether there is a scrub task  pending/running for
    // this pg.
    bool expected = false;
    if (!hs_pg->in_scrubbing.compare_exchange_strong(expected, true)) {
        LOGWARNMOD(scrubmgr, "pg={} scrub submission already in-flight, skip!", pg_id);
        return folly::makeSemiFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
    }

    const auto& pg_scrub_sb = *(ps_scrub_super_blk_it->second);
    const auto last_scrub_time =
        is_deep ? pg_scrub_sb->last_deep_scrub_timestamp : pg_scrub_sb->last_shallow_scrub_timestamp;

    auto [promise, future] = folly::makePromiseContract< std::shared_ptr< ShallowScrubReport > >();
    ScrubManager::scrub_task task(last_scrub_time, pg_id, is_deep, trigger_type, std::move(promise));
    if (!m_scrub_task_queue.push(std::move(task))) {
        // Queue is closed (scrub manager is stopped); roll back in_scrubbing so future submissions are not blocked.
        hs_pg->in_scrubbing.store(false);
        LOGWARNMOD(scrubmgr, "pg={} scrub task queue is closed/stopped, skip!", pg_id);
        return folly::makeSemiFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
    }
    return std::move(future);
}

void ScrubManager::cancel_scrub_task(const pg_id_t& pg_id) {
    auto it = m_pg_scrub_ctx_map.find(pg_id);
    if (it == m_pg_scrub_ctx_map.end()) {
        LOGWARNMOD(scrubmgr, "no running scrub task for pg={}, no need to cancel!", pg_id);
        return;
    }
    it->second->cancel();
    LOGINFOMOD(scrubmgr, "cancel scrub task for pg={}", pg_id);
}

void ScrubManager::handle_pg_scrub_task(scrub_task task) {
    const auto& pg_id = task.pg_id;
    const auto& task_id = task.task_id;
    const auto& is_deep_scrub = task.is_deep_scrub;

    SCRUBLOGD(pg_id, task_id,
              "Starting handling {} scrub task, last_scrub_time={} =====", is_deep_scrub ? "deep" : "shallow",
              task.last_scrub_time);

    std::shared_ptr< ShallowScrubReport > pg_scrub_report =
        is_deep_scrub ? std::make_shared< DeepScrubReport >(pg_id) : std::make_shared< ShallowScrubReport >(pg_id);

    struct scrub_task_guard {
        HSHomeObject* home_obj;
        folly::ConcurrentHashMap< pg_id_t, std::shared_ptr< PGScrubContext > >& pg_scrub_ctx_map;
        scrub_task& task;
        std::shared_ptr< ShallowScrubReport >& scrub_report;
        const pg_id_t& pg_id;

        ~scrub_task_guard() {
            pg_scrub_ctx_map.erase(pg_id);
            task.scrub_report_promise->setValue(scrub_report);
            auto hs_pg = home_obj->get_hs_pg(pg_id);
            if (hs_pg) {
                hs_pg->in_scrubbing.store(false);
                LOGINFOMOD(scrubmgr, "cleared SCRUBBING state for pg={}", pg_id);
            } else {
                // pg destroyed during scrubbing
                LOGWARNMOD(scrubmgr, "cannot find hs_pg to clear SCRUBBING state for pg={}!", pg_id);
            }
        }
    } guard{m_hs_home_object, m_pg_scrub_ctx_map, task, pg_scrub_report, pg_id};

    const auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        SCRUBLOGE(pg_id, task_id, "cannot find hs_pg for this pg, fail this scrub task!");
        return;
    }

    auto [ctx_it, happened] = m_pg_scrub_ctx_map.try_emplace(pg_id, std::make_shared< PGScrubContext >(task_id, hs_pg));
    RELEASE_ASSERT(happened,
                   "pg={} should not have a running scrub task since we set in_scrubbing in submit_scrub_task", pg_id);
    auto& scrub_ctx = ctx_it->second;

    // this is the last committed shard_id. we cannot get shard_sequence_num here since some of the shard might be
    // not committed yet. note that, this depends on the fact that the last committed shard is always at the end of
    // the shard list.
    const auto last_committed_shard_id = m_hs_home_object->get_last_shard_id_in_pg(pg_id);
    const auto last_committed_blob_id = hs_pg->get_last_committed_blob_id();

    // we get scrub_lsn after we get last_committed_shard_id and last_committed_blob_id, so we can guarantee for any
    // replica, if it has committed to scrub_lsn , it can at least see last_committed_shard_id and
    // last_committed_blob_id.
    // Now, the scrub range is finalized to [{0,0}, {last_committed_shard_id, last_committed_blob_id}]
    const int64_t scrub_lsn = hs_pg->repl_dev_->get_last_commit_lsn();

    // Step 1: Scrub META
    SCRUBLOGD(pg_id, task_id, "Starting META scrubbing");
    std::map< shard_id_t, uint32_t > shard_blob_count;
    for (shard_id_t start_shard_id = 0; start_shard_id <= last_committed_shard_id;) {
        if (scrub_ctx->cancelled.load()) {
            SCRUBLOGD(pg_id, task_id, "scrub task cancelled after meta scrub, skip blob scrub");
            return;
        }

        std::map< shard_id_t, uint32_t > shard_blob_count_in_batch;
        if (!scrub_ctx->scrub_meta_batch(pg_scrub_report, start_shard_id, last_committed_shard_id,
                                         last_committed_blob_id, scrub_lsn, shard_blob_count_in_batch)) {
            SCRUBLOGE(pg_id, task_id, "meta scrub failed for batch in range: {} to {}, scrub_lsn={}", start_shard_id,
                      last_committed_shard_id, scrub_lsn);
            return;
        }

        SCRUBLOGD(pg_id, task_id, "meta scrub batch completed in range: {} to {}, scrub_lsn={}", start_shard_id,
                  last_committed_shard_id, scrub_lsn);

        if (shard_blob_count_in_batch.empty()) {
            SCRUBLOGD(pg_id, task_id, "no more shard to scrub, end meta scrub");
            break;
        }

        // next shard_id after the last shard_id in this batch
        start_shard_id = shard_blob_count_in_batch.rbegin()->first + 1;
        shard_blob_count.merge(shard_blob_count_in_batch);
    }

    // Step 2: Scrub BLOB
    if (!shard_blob_count.empty()) {
        SCRUBLOGD(pg_id, task_id, "Starting {} blob scrubbing", is_deep_scrub ? "deep" : "shallow");
        auto it = shard_blob_count.begin();
        shard_id_t start_shard_id = it->first;
        shard_id_t end_shard_id = start_shard_id;
        uint64_t total_blob_count_in_batch = it->second;

        // start from the second shard
        for (++it; it != shard_blob_count.end(); ++it) {
            if (scrub_ctx->cancelled.load()) {
                SCRUBLOGD(pg_id, task_id, "scrub task cancelled during blob batch accumulation, stop");
                return;
            }
            auto blob_count = it->second;
            if (total_blob_count_in_batch + blob_count >= max_scrub_batch_size) {
                // scrub current batch
                if (!scrub_ctx->scrub_blob_batch(pg_scrub_report, start_shard_id, end_shard_id, last_committed_blob_id,
                                                 scrub_lsn, is_deep_scrub)) {
                    SCRUBLOGE(pg_id, task_id, "{} blob scrub failed for shard range: {} to {}, scrub_lsn={}",
                              is_deep_scrub ? "deep" : "shallow", start_shard_id, end_shard_id, scrub_lsn);
                    return;
                }

                // start a new batch
                start_shard_id = it->first;
                total_blob_count_in_batch = 0;
            }

            total_blob_count_in_batch += blob_count;
            end_shard_id = it->first;
        }

        // scrub last batch
        if (!scrub_ctx->scrub_blob_batch(pg_scrub_report, start_shard_id, end_shard_id, last_committed_blob_id,
                                         scrub_lsn, is_deep_scrub)) {
            SCRUBLOGE(pg_id, task_id, "{} blob scrub batch failed for shard range: {} to {}, scrub_lsn={}",
                      is_deep_scrub ? "deep" : "shallow", start_shard_id, end_shard_id, scrub_lsn);
            return;
        }
    }

#ifdef _PRERELEASE
    // Trigger the callback flip to delete missing blob during scrub if enabled
    iomgr_flip::instance()->callback_flip("delete_missing_blob_through_raft");
#endif

    // when scrubbing is on going, blob or shard deletion probably happens and lead to false-positive missing blobs(and
    // shards after we have delete shard). we reconcile the missing blobs in scrub report after all the scrubbing is
    // completed to reduce the false-positive item.

    // if we reach here, we can make sure the other replicas have committed to scrub_lsn.
    scrub_ctx->reconcile_scrub_report(pg_scrub_report);

    // only if pg is successfully scrubbed, we persist scrub metablk.

    // FIXME:: spread this to all followers, so that if leader changes, the new leader konws the last scrub time and can
    // trigger the next scrub in time.
    save_scrub_superblk(pg_id, is_deep_scrub, true);
    SCRUBLOGD(pg_id, task_id, "successfully complete {} scrub task!", is_deep_scrub ? "deep" : "shallow");
}

void ScrubManager::add_pg(const pg_id_t pg_id) {
    // TODO:: make this thread safe.
    LOGINFOMOD(scrubmgr, "added new scrub superblock for pg={}", pg_id);
    if (nullptr == m_hs_home_object->get_hs_pg(pg_id)) {
        LOGINFOMOD(scrubmgr, "cannot find pg={}!", pg_id);
        return;
    }

    // to avoid create-pg log replay overriding existing scrub superblock, we only create new superblock when there is
    // no existing one
    save_scrub_superblk(pg_id, false, false);
}

void ScrubManager::remove_pg(const pg_id_t pg_id) {
    cancel_scrub_task(pg_id);
    m_pg_scrub_ctx_map.erase(pg_id);

    auto it = m_pg_scrub_sb_map.find(pg_id);
    if (it == m_pg_scrub_sb_map.end()) {
        LOGINFOMOD(scrubmgr, "no scrub superblock found for pg={}, no need to remove", pg_id);
        return;
    }

    LOGINFOMOD(scrubmgr, "removed pg={} in scrub manager!", pg_id);
    it->second->destroy();
    m_pg_scrub_sb_map.erase(it);
}

// this function is called in meta_service thread context
void ScrubManager::on_pg_scrub_meta_blk_found(
    sisl::byte_view const& buf, void* meta_cookie,
    std::vector< homestore::superblk< pg_scrub_superblk > >& stale_pg_scrub_sbs) {
    auto sb = std::make_shared< homestore::superblk< pg_scrub_superblk > >();
    (*sb).load(buf, meta_cookie);
    const auto pg_id = (*sb)->pg_id;

    auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        // this is a stale pg scrub superblock, we just log and destroy it.
        LOGINFOMOD(scrubmgr, "cannot find pg={}, destroy stale scrub superblock", pg_id);
        stale_pg_scrub_sbs.emplace_back(std::move(*sb));
        return;
    }
    const auto last_deep_scrub_time = (*sb)->last_deep_scrub_timestamp;
    const auto last_shallow_scrub_time = (*sb)->last_shallow_scrub_timestamp;

    m_pg_scrub_sb_map.emplace(pg_id, std::move(sb));
    LOGINFOMOD(scrubmgr, "loaded scrub superblock for pg={}, last_deep_scrub_time={}, last_shallow_scrub_time={}",
               pg_id, last_deep_scrub_time, last_shallow_scrub_time);
}

void ScrubManager::save_scrub_superblk(const pg_id_t pg_id, const bool is_deep_scrub, bool force_update) {
    const auto current_time =
        std::chrono::duration_cast< std::chrono::seconds >(std::chrono::system_clock::now().time_since_epoch()).count();

    auto it = m_pg_scrub_sb_map.find(pg_id);
    if (it == m_pg_scrub_sb_map.end()) {
        // Create new superblock for this PG
        auto sb = std::make_shared< homestore::superblk< pg_scrub_superblk > >(pg_scrub_meta_name);
        (*sb).create(sizeof(pg_scrub_superblk));
        (*sb)->pg_id = pg_id;
        (*sb)->last_deep_scrub_timestamp = current_time;
        (*sb)->last_shallow_scrub_timestamp = current_time;
        (*sb).write();
        m_pg_scrub_sb_map.emplace(pg_id, std::move(sb));
        return;
    }

    if (force_update) {
        // Update existing superblock
        if (is_deep_scrub) {
            (*(it->second))->last_deep_scrub_timestamp = current_time;
        } else {
            (*(it->second))->last_shallow_scrub_timestamp = current_time;
        }
        (*(it->second)).write();
    } else {
        LOGINFOMOD(scrubmgr, "skip updating scrub superblock for pg={} since there is no scrub progress update", pg_id);
    }
}

std::optional< ScrubManager::pg_scrub_superblk > ScrubManager::get_scrub_superblk(const pg_id_t pg_id) const {
    auto it = m_pg_scrub_sb_map.find(pg_id);
    if (it == m_pg_scrub_sb_map.end()) {
        LOGWARNMOD(scrubmgr, "scrub superblk not found for pg {}", pg_id);
        return std::nullopt;
    }

    return *(*(it->second));
}

ScrubManager::PGScrubContext::PGScrubContext(uint64_t task_id, const HSHomeObject::HS_PG* hs_pg) :
        task_id(task_id), hs_pg(hs_pg) {

    const auto& members = (hs_pg->pg_info_).members;
    for (const auto& member : members) {
        // TODO::make the queue size configurable
        peer_scrub_result_queue_map_.emplace(
            member.id, std::make_shared< folly::MPMCQueue< std::shared_ptr< scrub_result > > >(10));
    }

    m_scrub_executor = std::make_shared< folly::IOThreadPoolExecutor >(peer_scrub_result_queue_map_.size());
    // TODO: handle the following cases:
    // 1 the node is removed from the raft group? handle this case later
    // 2 pg is destroyed during scrubbing?
}

ScrubManager::PGScrubContext::~PGScrubContext() {
    m_scrub_executor->stop();
    m_scrub_executor.reset();
}

void ScrubManager::PGScrubContext::handle_scrub_req_resp(std::shared_ptr< ScrubManager::scrub_result > result) {
    auto it = peer_scrub_result_queue_map_.find(result->issuer_peer_id);
    if (it != peer_scrub_result_queue_map_.end()) {
        SCRUBLOGD(hs_pg->pg_id(), task_id, "add scrub result from peer {}, req_id={}, result entries count={}",
                  result->issuer_peer_id, result->req_id, result->entries.size());
        it->second->blockingWrite(result);
    } else {
        SCRUBLOGW(hs_pg->pg_id(), task_id, "received scrub result from unknown peer {}, req_id={}, dropping",
                  result->issuer_peer_id, result->req_id);
    }
}

void ScrubManager::PGScrubContext::send_req_to_peer(const ScrubManager::scrub_req& req, const peer_id_t& peer_id) {
    const auto pg_id = hs_pg->pg_id();
    auto& repl_dev = hs_pg->repl_dev_;
    if (!repl_dev) {
        cancel();
        SCRUBLOGE(pg_id, task_id,
                  "replication device is not available, cannot send scrub req to peer {}, req_id={}, "
                  "scrub_type={}",
                  peer_id, req.req_id, req.scrub_type);
        return;
    }

    auto flatbuffer = req.build_flat_buffer();
    sisl::io_blob_list_t blob_list;
    blob_list.emplace_back(flatbuffer.data(), flatbuffer.size(), false);

    repl_dev->data_request_unidirectional(peer_id, HSHomeObject::PUSH_SCRUB_REQ, blob_list)
        .via(&folly::InlineExecutor::instance())
        .thenValue([pg_id, peer_id, task_id = this->task_id, flatbuffer = std::move(flatbuffer), req_id = req.req_id,
                    scrub_type = req.scrub_type](auto&& response) {
            if (response.hasError()) {
                SCRUBLOGE(pg_id, task_id, "failed to send scrub req to peer {}, req_id={}, error={}, scrub_type={}",
                          peer_id, req_id, response.error(), scrub_type);
            } else {
                SCRUBLOGD(pg_id, task_id, "successfully sent scrub req to peer {}, req_id={}, scrub_type={}", peer_id,
                          req_id, scrub_type);
            }
        });
}

bool ScrubManager::PGScrubContext::scrub_meta_batch(std::shared_ptr< ScrubManager::MetaScrubReport > scrub_report,
                                                    shard_id_t start_shard_id, shard_id_t end_shard_id,
                                                    blob_id_t last_blob_id, int64_t scrub_lsn,
                                                    std::map< shard_id_t, uint32_t >& shard_blob_count_in_batch) {
    const auto pg_id = hs_pg->pg_id();
    SCRUBLOGD(pg_id, task_id, "start scrubbing meta for shard range: {} to {}, last_blob_id={}, scrub_lsn={}",
              start_shard_id, end_shard_id, last_blob_id, scrub_lsn);

    std::vector< folly::Future< std::shared_ptr< ScrubManager::range_scrub_result > > > futs;
    for (const auto& [peer_id, scrub_result_queue] : peer_scrub_result_queue_map_) {
        auto [promise, future] = folly::makePromiseContract< std::shared_ptr< ScrubManager::range_scrub_result > >();
        futs.emplace_back(std::move(future).via(&folly::InlineExecutor::instance()));
        m_scrub_executor->add([this, pg_id, peer_id, start_shard_id, end_shard_id, last_blob_id, scrub_lsn,
                               promise = std::move(promise), scrub_result_queue]() mutable {
            std::shared_ptr< ScrubManager::scrub_result > scrub_result;
            ScrubManager::scrub_req current_req(pg_id, random_req_id(), scrub_lsn, start_shard_id, 0, end_shard_id,
                                                last_blob_id, SCRUB_TYPE::META, hs_pg->home_obj_.our_uuid());
            auto range_scrub_result = std::make_shared< ScrubManager::range_scrub_result >(
                start_shard_id, 0, end_shard_id, last_blob_id, SCRUB_TYPE::META, peer_id);

            for (uint8_t retry_count = 0;;) {
                if (cancelled.load()) {
                    SCRUBLOGD(pg_id, task_id,
                              "scrub task is cancelled, stop waiting for scrub result from peer {}, "
                              "shard range: {} to {}, last_blob_id={}, scrub_lsn={}",
                              peer_id, start_shard_id, end_shard_id, last_blob_id, scrub_lsn);
                    break;
                }

                send_req_to_peer(current_req, peer_id);

                // Drain stale results until we get the expected req_id or time out.
                // Do NOT re-send on stale results — the peer already has the outstanding request.
                bool got_expected = false;
                while (!got_expected) {
                    // TODO::make the timeout here configurable
                    if (!scrub_result_queue->tryReadUntil(std::chrono::steady_clock::now() + std::chrono::seconds{10},
                                                          scrub_result)) {
                        // timeout
                        SCRUBLOGD(pg_id, task_id,
                                  "did not receive scrub result from peer {} for shard range: {} to {}, "
                                  "last_blob_id={}, scrub_lsn={}, try again",
                                  peer_id, start_shard_id, end_shard_id, last_blob_id, scrub_lsn);
                        retry_count++;

                        // TODO::make the max retry count configurable
                        if (retry_count > 5) {
                            SCRUBLOGE(pg_id, task_id,
                                      "did not receive scrub result from peer {} after {} retries, "
                                      "shard range: {} to {}, last_blob_id={}, scrub_lsn={}, "
                                      "cancel this scrub task",
                                      peer_id, retry_count, start_shard_id, end_shard_id, last_blob_id, scrub_lsn);
                            // this will cancel the entire task
                            cancel();
                        }
                        break; // timed out — outer loop will re-send or detect cancellation
                    }

                    // TODO:: add more logic to check if the scrub result is the expected one, for example, we can add
                    // the shard range and scrub_lsn in the scrub req and check if they are consistent with the received
                    // scrub result
                    if (scrub_result->req_id != current_req.req_id) {
                        SCRUBLOGD(pg_id, task_id,
                                  "received scrub result with unexpected req_id from peer {}, expected req_id={}, "
                                  "actual req_id={}, drain and wait again",
                                  peer_id, current_req.req_id, scrub_result->req_id);
                        continue; // drain stale result and wait again — do NOT re-send
                    }

                    got_expected = true;
                }

                if (!got_expected) {
                    continue; // timed out — outer loop re-sends or cancels
                }

                // Got the expected result. Reset retry count since the peer is responsive.
                retry_count = 0;
                SCRUBLOGD(pg_id, task_id, "meta scrub: received scrub result from peer {}, result entries count={}",
                          peer_id, scrub_result->entries.size());

                if (scrub_result->entries.empty()) {
                    SCRUBLOGD(pg_id, task_id,
                              "received empty scrub result from peer {}, shard range: {} to {}, "
                              "last_blob_id={}, scrub_lsn={}, we consider this range scrub is completed and break "
                              "the loop!",
                              peer_id, start_shard_id, end_shard_id, last_blob_id, scrub_lsn);
                    break;
                }

                const auto received_batch_size = scrub_result->entries.size();
                SCRUBLOGD(pg_id, task_id,
                          "received expected meta scrub result from peer {}, req_id={}, start_shard_id={}, "
                          "start_blob_id={}, end_shard_id={}, end_blob_id={}, scrub_lsn={}, details={}",
                          peer_id, current_req.req_id, current_req.start_shard_id, current_req.start_blob_id,
                          current_req.end_shard_id, current_req.end_blob_id, scrub_lsn, scrub_result->to_string());

                const auto last_shard_id_in_result = (scrub_result->entries).rbegin()->first.shard;
                range_scrub_result->handle_scrub_req_resp(*scrub_result);
                SCRUBLOGD(pg_id, task_id, "after adding new scrub_result for meta scrub, {}",
                          range_scrub_result->to_string());

                if (received_batch_size < max_scrub_batch_size) {
                    // if the received batch size is smaller than the max batch size, it means the peer has no more data
                    // to scrub in this range, we can consider the range scrub is completed for this peer and break the
                    // loop to avoid unnecessary scrub req sending and waiting.
                    SCRUBLOGD(pg_id, task_id,
                              "received meta scrub result with batch size {} smaller than max batch size {} from peer "
                              "{}, shard range: {} to {}, last_blob_id={}, scrub_lsn={}, we consider this range scrub "
                              "is completed and break the loop!",
                              received_batch_size, max_scrub_batch_size, peer_id, start_shard_id, end_shard_id,
                              last_blob_id, scrub_lsn);
                    break;
                }

                RELEASE_ASSERT(received_batch_size == max_scrub_batch_size,
                               "the received batch size {} should be equal to max scrub batch size {} for meta scrub, "
                               "but it is not, peer {}, shard range: {} to {}, last_blob_id={}, scrub_lsn={}",
                               received_batch_size, max_scrub_batch_size, peer_id, start_shard_id, end_shard_id,
                               last_blob_id, scrub_lsn);

                current_req.start_shard_id = last_shard_id_in_result + 1;
                if (current_req.start_shard_id > end_shard_id) {
                    // the range scrub is completed for this batch, we can break the loop and return the result.
                    SCRUBLOGD(pg_id, task_id,
                              "completed scrubbing meta for shard range: {} to {}, last_blob_id={}, scrub_lsn={}, "
                              "peer {}",
                              start_shard_id, end_shard_id, last_blob_id, scrub_lsn, peer_id);
                    break;
                }

                // new req_id for the next scrub req
                current_req.req_id = random_req_id();
            }

            if (cancelled.load()) {
                promise.setException(folly::make_exception_wrapper< std::runtime_error >("cancelled"));
            } else {
                promise.setValue(range_scrub_result);
            }
        });
    }

    return folly::collectAllUnsafe(futs)
        .thenValue([this, pg_id, &scrub_report, &shard_blob_count_in_batch](auto&& results) {
            std::map< peer_id_t, std::shared_ptr< range_scrub_result > > peer_scrub_result_map;
            for (auto& r : results) {
                if (r.hasException()) {
                    SCRUBLOGE(pg_id, task_id, "scrub meta batch is failed, error={}", r.exception().what());
                    return false;
                }
                auto range_scrub_result = r.value();
                if (!range_scrub_result) {
                    SCRUBLOGE(pg_id, task_id, "scrub meta batch is failed, receive nullptr scrub result");
                    return false;
                }
                peer_scrub_result_map[range_scrub_result->peer_id] = range_scrub_result;

                SCRUBLOGD(pg_id, task_id, "complete meta range scrub: {}", range_scrub_result->to_string());
            }

            // 1 consolidate peer_scrub_result_map into shard_blob_count_in_batch
            std::map< shard_id_t, std::pair< size_t /*occurrence count*/, uint32_t /*max blob count*/ > >
                shard_count_map;

            for (const auto& [_, range_scrub_result] : peer_scrub_result_map) {
                for (const auto& [route, _] : range_scrub_result->results) {
                    // shard occurrence count
                    shard_count_map[route.shard].first++;

                    // blob count. in meta scrub result, route.blob stands for the blob count in this shard, we need to
                    // get the max blob count among all peers since some peer might lose some blobs
                    const auto blob_count = shard_count_map[route.shard].second;
                    shard_count_map[route.shard].second = std::max(blob_count, static_cast< uint32_t >(route.blob));
                }
            }

            for (const auto& [shard_id, count_pair] : shard_count_map) {
                // if shard_id is 0, this is a pg meta scrub result, which does not represent a real shard, we can skip
                // it in blob scrub phase.
                if (!shard_id) continue;

                if (count_pair.first == peer_scrub_result_map.size()) {
                    // all peers have this shard, we can consider this shard is successfully scrubbed, and update the
                    // blob count. for the empty shard, we set the blob count to 1. Actually, empty shard should not
                    // appear since it does not in pg_index_table.
                    shard_blob_count_in_batch[shard_id] = std::max(count_pair.second, uint32_t{1});
                } else {
                    // missing shard
                    RELEASE_ASSERT(
                        count_pair.first < peer_scrub_result_map.size(),
                        "the occurrence count of shard_id {} should not be larger than peer count, but it is {}",
                        shard_id, count_pair.first);
                    // not all peers have this shard, we consider this shard is missing, and set blob count to max value
                    // to make sure it will be scrubbed in a single batch in blob scrub phase
                    shard_blob_count_in_batch[shard_id] = UINT32_MAX;
                }
            }

            // 2 consolidate peer_scrub_result_map into scrub_report
            scrub_report->merge(peer_scrub_result_map);

            return true;
        })
        .get();
}

bool ScrubManager::PGScrubContext::scrub_blob_batch(std::shared_ptr< ScrubManager::ShallowScrubReport > scrub_report,
                                                    shard_id_t start_shard_id, shard_id_t end_shard_id,
                                                    blob_id_t last_blob_id, int64_t scrub_lsn, bool is_deep_scrub) {
    const auto pg_id = hs_pg->pg_id();
    const auto scrub_type = is_deep_scrub ? SCRUB_TYPE::DEEP_BLOB : SCRUB_TYPE::SHALLOW_BLOB;

    SCRUBLOGD(pg_id, task_id,
              "start scrubbing blob for shard range: {} to {}, last_blob_id={}, scrub_lsn={}, scrub_type={}",
              start_shard_id, end_shard_id, last_blob_id, scrub_lsn, scrub_type);

    std::vector< folly::Future< std::shared_ptr< ScrubManager::range_scrub_result > > > futs;
    for (const auto& [peer_id, scrub_result_queue] : peer_scrub_result_queue_map_) {
        auto [promise, future] = folly::makePromiseContract< std::shared_ptr< ScrubManager::range_scrub_result > >();
        futs.emplace_back(std::move(future).via(&folly::InlineExecutor::instance()));
        m_scrub_executor->add([this, pg_id, peer_id, start_shard_id, end_shard_id, last_blob_id, scrub_lsn, scrub_type,
                               promise = std::move(promise), scrub_result_queue]() mutable {
            std::shared_ptr< ScrubManager::scrub_result > scrub_result;
            ScrubManager::scrub_req current_req(pg_id, random_req_id(), scrub_lsn, start_shard_id, 0, end_shard_id,
                                                last_blob_id, scrub_type, hs_pg->home_obj_.our_uuid());
            auto range_scrub_result = std::make_shared< ScrubManager::range_scrub_result >(
                start_shard_id, 0, end_shard_id, last_blob_id, scrub_type, peer_id);

            for (uint8_t retry_count = 0;;) {
                if (cancelled.load()) {
                    SCRUBLOGD(pg_id, task_id,
                              "scrub task is cancelled, stop waiting for scrub result from peer {}, "
                              "shard range: {} to {}, last_blob_id={}, scrub_lsn={}",
                              peer_id, start_shard_id, end_shard_id, last_blob_id, scrub_lsn);
                    break;
                }

                send_req_to_peer(current_req, peer_id);

                // Drain stale results until we get the expected req_id or time out.
                // Do NOT re-send on stale results — the peer already has the outstanding request.
                // TODO::make the timeout here configurable
                bool got_expected = false;
                while (!got_expected) {
                    if (!scrub_result_queue->tryReadUntil(std::chrono::steady_clock::now() + std::chrono::seconds{10},
                                                          scrub_result)) {
                        // timeout
                        SCRUBLOGD(pg_id, task_id,
                                  "did not receive scrub result from peer {} for shard range: {} to {}, "
                                  "last_blob_id={}, scrub_lsn={}, try again",
                                  peer_id, start_shard_id, end_shard_id, last_blob_id, scrub_lsn);
                        retry_count++;

                        // TODO::make the max retry count configurable
                        if (retry_count > 5) {
                            SCRUBLOGE(pg_id, task_id,
                                      "did not receive scrub result from peer {} after {} retries, "
                                      "shard range: {} to {}, last_blob_id={}, scrub_lsn={}, "
                                      "cancel this scrub task",
                                      peer_id, retry_count, start_shard_id, end_shard_id, last_blob_id, scrub_lsn);
                            // this will cancel the entire task
                            cancel();
                        }
                        break; // timed out — outer loop will re-send or detect cancellation
                    }

                    // TODO:: add more logic to check if the scrub result is the expected one, for example, we can
                    // add the shard range and scrub_lsn in the scrub req and check if they are consistent with the
                    // received scrub result
                    if (scrub_result->req_id != current_req.req_id) {
                        SCRUBLOGD(pg_id, task_id,
                                  "received scrub result with unexpected req_id from peer {}, expected req_id={}, "
                                  "actual req_id={}, scrub_type={}, drain and wait again",
                                  peer_id, current_req.req_id, scrub_result->req_id, scrub_type);
                        continue; // drain stale result and wait again — do NOT re-send
                    }

                    got_expected = true;
                }

                if (!got_expected) {
                    continue; // timed out — outer loop re-sends or cancels
                }

                // Got the expected result. Reset retry count since the peer is responsive.
                retry_count = 0;
                SCRUBLOGD(pg_id, task_id, "blob scrub: received scrub result from peer {}, result entries count={}",
                          peer_id, scrub_result->entries.size());

                if (scrub_result->entries.empty()) {
                    SCRUBLOGD(pg_id, task_id,
                              "received empty scrub result from peer {}, shard range: {} to {}, "
                              "last_blob_id={}, scrub_lsn={}, we consider this range scrub is completed and break "
                              "the loop!",
                              peer_id, start_shard_id, end_shard_id, last_blob_id, scrub_lsn);
                    break;
                }

                const auto received_batch_size = scrub_result->entries.size();
                SCRUBLOGD(pg_id, task_id,
                          "received expected blob scrub result from peer {}, req_id={}, start_shard_id={}, "
                          "start_blob_id={}, end_shard_id={}, end_blob_id={}, scrub_lsn={}, details={}",
                          peer_id, current_req.req_id, current_req.start_shard_id, current_req.start_blob_id,
                          current_req.end_shard_id, current_req.end_blob_id, scrub_lsn, scrub_result->to_string());

                const auto last_shard_id_in_result = (scrub_result->entries).rbegin()->first.shard;
                const auto last_blob_id_in_result = (scrub_result->entries).rbegin()->first.blob;

                range_scrub_result->handle_scrub_req_resp(*scrub_result);
                SCRUBLOGD(pg_id, task_id, "after adding new scrub_result for blob scrub, {}",
                          range_scrub_result->to_string());

                uint32_t batch_capacity = static_cast< uint32_t >(
                    scrub_type == SCRUB_TYPE::SHALLOW_BLOB ? max_scrub_batch_size : deep_blob_scrub_batch_size);
                if (received_batch_size < batch_capacity) {
                    // if the received batch size is smaller than the batch capacity, it means the peer has no more data
                    // to scrub in this range, we can consider the range scrub is completed for this peer and break the
                    // loop to avoid unnecessary scrub req sending and waiting.
                    SCRUBLOGD(pg_id, task_id,
                              "received blob scrub result with batch size {} smaller than batch capacity {} from peer "
                              "{}, shard range: {} to {}, last_blob_id={}, scrub_lsn={}, we consider this range scrub "
                              "is completed and break the loop!",
                              received_batch_size, batch_capacity, peer_id, start_shard_id, end_shard_id, last_blob_id,
                              scrub_lsn);
                    break;
                }

                RELEASE_ASSERT(
                    received_batch_size == batch_capacity,
                    "the received batch size {} should be equal to batch capacity {} for blob scrub, but it is not, "
                    "peer {}, shard range: {} to {}, last_blob_id={}, scrub_lsn={}, scrub_type={}",
                    received_batch_size, batch_capacity, peer_id, start_shard_id, end_shard_id, last_blob_id, scrub_lsn,
                    scrub_type);

                RELEASE_ASSERT(last_blob_id_in_result <= last_blob_id,
                               "the last_blob_id_in_result {} should not be larger than last_blob_id {}, but it is, "
                               "peer {}, shard range: {} to {}, scrub_lsn={}, scrub_type={}",
                               last_blob_id_in_result, last_blob_id, peer_id, start_shard_id, end_shard_id, scrub_lsn,
                               scrub_type);

                if (last_blob_id_in_result == last_blob_id) {
                    current_req.start_shard_id = last_shard_id_in_result + 1;
                    current_req.start_blob_id = 0;
                } else {
                    current_req.start_shard_id = last_shard_id_in_result;
                    current_req.start_blob_id = last_blob_id_in_result + 1;
                }

                // Completed when we've passed the last blob of the last shard in this batch.
                if (current_req.start_shard_id > end_shard_id) {
                    SCRUBLOGD(pg_id, task_id,
                              "completed scrubbing blob for shard range: {} to {}, last_blob_id={}, scrub_lsn={}, "
                              "peer {}",
                              start_shard_id, end_shard_id, last_blob_id, scrub_lsn, peer_id);
                    break;
                }

                // new req_id for the next scrub req
                current_req.req_id = random_req_id();
            }

            if (cancelled.load()) {
                promise.setException(folly::make_exception_wrapper< std::runtime_error >("cancelled"));
            } else {
                promise.setValue(range_scrub_result);
            }
        });
    }

    return folly::collectAllUnsafe(futs)
        .thenValue([this, pg_id, &scrub_report](auto&& results) {
            std::map< peer_id_t, std::shared_ptr< range_scrub_result > > peer_scrub_result_map;
            for (auto& r : results) {
                if (r.hasException()) {
                    SCRUBLOGE(pg_id, task_id, "scrub blob batch is failed, error={}", r.exception().what());
                    return false;
                }
                auto range_scrub_result = r.value();
                if (!range_scrub_result) {
                    SCRUBLOGE(pg_id, task_id, "scrub blob batch is failed, receive nullptr scrub result");
                    return false;
                }
                peer_scrub_result_map.emplace(range_scrub_result->peer_id, range_scrub_result);

                SCRUBLOGD(pg_id, task_id, "complete blob range scrub: {}", range_scrub_result->to_string());
            }

            // consolidate peer_scrub_result_map into scrub_report
            scrub_report->merge(peer_scrub_result_map);

            return true;
        })
        .get();
}

void ScrubManager::PGScrubContext::reconcile_scrub_report(std::shared_ptr< ShallowScrubReport > scrub_report) {
    // A shard/blob reported missing may be a false positive from a concurrent deletion during scrubbing.
    // For each peer that is tracked as HAVING the shard/blob (in the existence-tracking set), we re-check
    // whether it still holds the item. If it no longer does (concurrent deletion), we remove that peer from
    // the existence-tracking set; when the set empties the entry is dropped, eliminating the false positive.
    const auto pg_id = hs_pg->pg_id();

    // TODO::make this configurable
    const uint8_t max_reconcile_retry_count = 3;
    for (uint8_t retry_count = 0; retry_count < max_reconcile_retry_count; ++retry_count) {
        if (cancelled.load()) {
            SCRUBLOGD(pg_id, task_id, "scrub task cancelled, skip reconciliation");
            return;
        }

        const auto missing_shards = scrub_report->get_missing_shard_ids();
        const auto missing_blobs = scrub_report->get_missing_blobs();

        if (missing_shards.empty() && missing_blobs.empty()) {
            SCRUBLOGD(pg_id, task_id, "no missing shard/blob in scrub report, no need to reconcile");
            return;
        }

        std::vector< folly::Future< folly::Unit > > reconcile_futs;

        for (const auto& [shard_id, peer_set] : missing_shards) {
            for (const auto& peer_id : peer_set) {
                reconcile_futs.emplace_back(std::move(
                    check_existence_in_peer(peer_id, {shard_id, 0}, false /* check blob */)
                        .thenTry([this, pg_id, peer_id, shard_id, &scrub_report](folly::Try< bool > result) {
                            if (result.hasException()) {
                                SCRUBLOGE(pg_id, task_id,
                                          "failed to check shard existence in peer {}, shard {}, error: {}", peer_id,
                                          shard_id, result.exception().what());
                                return;
                            }

                            const auto& exists = result.value();
                            if (!exists) {
                                SCRUBLOGD(pg_id, task_id,
                                          "reconcile check: shard {} confirmed absent on peer {}, removing from "
                                          "existence-tracking set",
                                          shard_id, peer_id);
                                scrub_report->remove_shard_existence_from_peer(shard_id, peer_id);
                            } else {
                                SCRUBLOGD(pg_id, task_id,
                                          "reconcile check: shard {} still present on peer {}, no change", shard_id,
                                          peer_id);
                            }
                        })));
            }
        }

        for (const auto& [blob_route, peer_set] : missing_blobs) {
            for (const auto& peer_id : peer_set) {
                reconcile_futs.emplace_back(std::move(
                    check_existence_in_peer(peer_id, blob_route, true /* check blob */)
                        .thenTry([this, pg_id, peer_id, blob_route, &scrub_report](folly::Try< bool > result) {
                            if (result.hasException()) {
                                SCRUBLOGE(
                                    pg_id, task_id,
                                    "failed to check blob existence in peer {}, shard_id={}, blob_id={}, error: {}",
                                    peer_id, blob_route.shard, blob_route.blob, result.exception().what());
                                return;
                            }

                            const auto& exists = result.value();
                            if (!exists) {
                                SCRUBLOGD(pg_id, task_id,
                                          "reconcile check: shard_id={}, blob_id={} confirmed absent on peer {}, "
                                          "removing from existence-tracking set",
                                          blob_route.shard, blob_route.blob, peer_id);
                                scrub_report->remove_blob_existence_from_peer(blob_route, peer_id);
                            } else {
                                SCRUBLOGD(
                                    pg_id, task_id,
                                    "reconcile check: shard_id={}, blob_id={} still present on peer {}, no change",
                                    blob_route.shard, blob_route.blob, peer_id);
                            }
                        })));
            }
        }

        folly::collectAllUnsafe(reconcile_futs).wait();
    }

    const auto remaining_missing_shards = scrub_report->get_missing_shard_ids().size();
    const auto remaining_missing_blobs = scrub_report->get_missing_blobs().size();
    if (remaining_missing_shards || remaining_missing_blobs) {
        SCRUBLOGW(pg_id, task_id,
                  "reconciliation finished after {} retries but {} missing shards and {} missing blobs remain",
                  max_reconcile_retry_count, remaining_missing_shards, remaining_missing_blobs);
    } else {
        SCRUBLOGD(pg_id, task_id, "reconciliation cleared all missing items after {} retries",
                  max_reconcile_retry_count);
    }
}

folly::Future< bool > ScrubManager::PGScrubContext::check_existence_in_peer(peer_id_t peer_id, BlobRoute blob,
                                                                            bool check_blob) {
    auto [promise, future] = folly::makePromiseContract< bool >();
    const auto pg_id = hs_pg->pg_id();

    auto repl_dev = hs_pg->repl_dev_;
    if (!repl_dev) {
        promise.setException(folly::make_exception_wrapper< std::runtime_error >("repl dev is not available"));
        return std::move(future).via(&folly::InlineExecutor::instance());
    }

    ScrubManager::scrub_req check_blob_req;
    check_blob_req.start_shard_id = blob.shard;
    check_blob_req.start_blob_id = blob.blob;
    check_blob_req.scrub_type = check_blob ? SCRUB_TYPE::CHECK_BLOB_EXISTENCE : SCRUB_TYPE::CHECK_SHARD_EXISTENCE;

    auto flatbuffer = check_blob_req.build_flat_buffer();
    sisl::io_blob_list_t blob_list;
    blob_list.emplace_back(flatbuffer.data(), flatbuffer.size(), false);

    const auto check_type_str = check_blob ? "blob" : "shard";

    // this is a bidirectional request, no need to add a req_id.
    repl_dev->data_request_bidirectional(peer_id, HSHomeObject::PUSH_SCRUB_REQ, blob_list)
        .via(&folly::InlineExecutor::instance())
        .thenValue([pg_id, peer_id, task_id = this->task_id, blob, check_type_str, flatbuffer = std::move(flatbuffer),
                    promise = std::move(promise)](auto&& response) mutable {
            if (response.hasError()) {
                SCRUBLOGE(pg_id, task_id, "failed to check {} existence in peer {}, blob {}, error code: {}",
                          check_type_str, peer_id, blob, static_cast< int >(response.error()));
                promise.setException(
                    folly::make_exception_wrapper< std::runtime_error >("rpc bidirectional request failed"));
            } else {
                const auto& resp_blob = response.value().response_blob();
                if (resp_blob.size() != sizeof(bool)) {
                    SCRUBLOGE(pg_id, task_id,
                              "invalid response for {} existence check from peer {}, blob {}, response size={}",
                              check_type_str, peer_id, blob, resp_blob.size());
                    promise.setException(
                        folly::make_exception_wrapper< std::runtime_error >("invalid response for existence check"));
                } else {
                    const bool exists = *reinterpret_cast< const bool* >(resp_blob.cbytes());
                    SCRUBLOGD(pg_id, task_id,
                              "successfully checked {} existence in peer {}, shard_id={}, blob_id={}, exists={}",
                              check_type_str, peer_id, blob.shard, blob.blob, exists);
                    promise.setValue(exists);
                }
            }
        });

    return std::move(future).via(&folly::InlineExecutor::instance());
}

uint64_t ScrubManager::PGScrubContext::random_req_id() const {
    static std::atomic< uint64_t > ctr{0};

    static const uint64_t seed = []() -> uint64_t {
        std::random_device rd;
        uint64_t s = (uint64_t(rd()) << 32) ^ uint64_t(rd());
        return s ? s : 0x123456789abcdef0ULL;
    }();

    auto splitmix64 = [](uint64_t x) -> uint64_t {
        x += 0x9e3779b97f4a7c15ULL;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    };

    uint64_t x = ctr.fetch_add(1, std::memory_order_relaxed) ^ seed;
    return splitmix64(x);
}

flatbuffers::DetachedBuffer ScrubManager::scrub_req::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder builder;

    // Convert peer_id_t (boost::uuids::uuid, 16 bytes) to a byte vector for the issuer_uuid field.
    // issuer_uuid is [ubyte:16] in the schema; if issuer_peer_id is nil (all-zero), it is still
    // serialized so the receiver can always read a consistent value.
    std::vector< uint8_t > uuid_bytes(issuer_peer_id.begin(), issuer_peer_id.end());
    auto issuer_uuid_offset = builder.CreateVector(uuid_bytes);

    // Build the ScrubReq table.  All scalar fields are written unconditionally; absent values on
    // the reader side will fall back to FlatBuffers defaults (0 / ScrubType::META).
    auto req_offset =
        CreateScrubReq(builder, static_cast< uint16_t >(pg_id), req_id, scrub_lsn, start_shard_id, start_blob_id,
                       end_shard_id, end_blob_id, issuer_uuid_offset, static_cast< ScrubType >(scrub_type));

    builder.FinishSizePrefixed(req_offset);
    return builder.Release();
}

bool ScrubManager::scrub_req::load(uint8_t const* buf_ptr, const uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERRORMOD(scrubmgr, "scrub_req::load called with null or empty buffer");
        return false;
    }

    auto scrub_req_fb = GetSizePrefixedScrubReq(buf_ptr);
    if (!scrub_req_fb) {
        LOGERRORMOD(scrubmgr, "scrub_req::load: GetSizePrefixedScrubReq returned null");
        return false;
    }

    // Scalar fields always carry a value (FlatBuffers default = 0 when absent in the wire format).
    pg_id = static_cast< pg_id_t >(scrub_req_fb->pg_id());
    req_id = scrub_req_fb->req_id();
    scrub_lsn = scrub_req_fb->scrub_lsn();
    start_shard_id = scrub_req_fb->start_shard_id();
    start_blob_id = scrub_req_fb->start_blob_id();
    end_shard_id = scrub_req_fb->end_shard_id();
    end_blob_id = scrub_req_fb->end_blob_id();
    scrub_type = static_cast< SCRUB_TYPE >(scrub_req_fb->scrub_type());

    // issuer_uuid is a vector field and may be absent (nullptr) when the sender did not set it.
    // In that case issuer_peer_id keeps its default-initialized nil UUID.
    if (auto uuid_vec = scrub_req_fb->issuer_uuid(); uuid_vec != nullptr) {
        const auto copy_len = std::min(static_cast< size_t >(uuid_vec->size()), issuer_peer_id.size());
        std::copy_n(uuid_vec->data(), copy_len, issuer_peer_id.begin());
    }

    return true;
}

flatbuffers::DetachedBuffer ScrubManager::scrub_result::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder builder;

    std::vector< flatbuffers::Offset< ScrubResultEntry > > entry_offsets;
    entry_offsets.reserve(entries.size());

    {
        std::lock_guard lock(mutex_);
        for (const auto& [route, result_variant] : entries) {
            ScrubStatus status;
            uint64_t hash = 0;
            if (std::holds_alternative< uint64_t >(result_variant)) {
                status = ScrubStatus::NONE;
                hash = std::get< uint64_t >(result_variant);
            } else {
                status = std::get< ScrubStatus >(result_variant);
            }
            entry_offsets.push_back(CreateScrubResultEntry(builder, route.shard, route.blob, status, hash));
        }
    }

    auto entries_offset = builder.CreateVector(entry_offsets);

    std::vector< uint8_t > uuid_bytes(issuer_peer_id.begin(), issuer_peer_id.end());
    auto uuid_offset = builder.CreateVector(uuid_bytes);

    auto result_offset = CreateScrubResult(builder, req_id, uuid_offset, entries_offset);
    builder.FinishSizePrefixed(result_offset);
    return builder.Release();
}

void ScrubManager::scrub_result::add_entry(const scrub_result_entry& entry) {
    BlobRoute route{entry.shard_id, entry.blob_id};
    std::lock_guard lock(mutex_);
    RELEASE_ASSERT(entries.find(route) == entries.end(), "duplicate scrub result entry for {}", route);
    entries[route] = entry.status_or_hash;
}

bool ScrubManager::scrub_result::load(uint8_t const* buf_ptr, uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERRORMOD(scrubmgr, "scrub_result::load called with null or empty buffer");
        return false;
    }

    auto result_fb = GetSizePrefixedScrubResult(buf_ptr);
    if (!result_fb) {
        LOGERRORMOD(scrubmgr, "scrub_result::load: GetSizePrefixedScrubResult returned null");
        return false;
    }

    req_id = result_fb->req_id();

    // issuer_uuid is a vector field — absent (nullptr) when the sender omitted it;
    // in that case issuer_peer_id keeps its default-initialized nil UUID.
    if (auto uuid_vec = result_fb->issuer_uuid(); uuid_vec != nullptr) {
        const auto copy_len = std::min(static_cast< size_t >(uuid_vec->size()), issuer_peer_id.size());
        std::copy_n(uuid_vec->data(), copy_len, issuer_peer_id.begin());
    }

    {
        std::lock_guard lock(mutex_);
        entries.clear();
        if (auto results_vec = result_fb->scrub_results(); results_vec != nullptr) {
            for (const auto* entry_fb : *results_vec) {
                if (!entry_fb) { continue; }
                BlobRoute route{entry_fb->shard_id(), entry_fb->blob_id()};
                std::variant< ScrubStatus, uint64_t > result_variant;
                if (entry_fb->scrub_result() == ScrubStatus::NONE) {
                    result_variant = entry_fb->hash();
                } else {
                    result_variant = static_cast< ScrubStatus >(entry_fb->scrub_result());
                }
                entries.emplace(route, std::move(result_variant));
            }
        }
    }

    return true;
}

std::string ScrubManager::scrub_result::to_string() const {
    std::lock_guard lock(mutex_);
    std::stringstream ss;
    ss << "scrub_result[req_id=" << req_id << ",issuer=" << issuer_peer_id << ",entries_count=" << entries.size()
       << ",entries={";
    bool first = true;
    for (const auto& [route, result_variant] : entries) {
        if (!first) ss << ",";
        ss << "shard=" << static_cast< uint64_t >(route.shard) << ",blob=" << static_cast< uint64_t >(route.blob)
           << ":";
        if (std::holds_alternative< uint64_t >(result_variant)) {
            ss << fmt::format("{:016x}", std::get< uint64_t >(result_variant));
        } else {
            ss << scrub_result_to_string(std::get< ScrubStatus >(result_variant));
        }
        first = false;
    }
    ss << "}]";
    return ss.str();
}

std::string ScrubManager::range_scrub_result::to_string() const {
    std::stringstream ss;
    ss << "range_scrub_result[peer=" << peer_id << ",scrub_type=" << scrub_type << ",range=[shard=" << start_shard_id
       << ",blob=" << start_blob_id << "]->[shard=" << end_shard_id << ",blob=" << end_blob_id
       << "],results_count=" << results.size() << ",results={";
    bool first = true;
    for (const auto& [route, result_variant] : results) {
        if (!first) ss << ",";
        ss << "shard=" << static_cast< uint64_t >(route.shard) << ",blob=" << static_cast< uint64_t >(route.blob)
           << ":";
        if (std::holds_alternative< uint64_t >(result_variant)) {
            ss << fmt::format("{:016x}", std::get< uint64_t >(result_variant));
        } else {
            ss << scrub_result_to_string(std::get< ScrubStatus >(result_variant));
        }
        first = false;
    }
    ss << "}]";
    return ss.str();
}

//=========================== Scrub Report Merge Functions ===========================//

void ScrubManager::MetaScrubReport::print() const {
    std::stringstream ss;
    ss << "MetaScrubReport for pg=" << pg_id_ << " | ";

    ss << "CorruptedPgMeta={";
    bool first = true;
    for (const auto& [peer_id, scrub_status] : corrupted_pg_metas) {
        if (!first) ss << ",";
        ss << "peer=" << peer_id << "(" << scrub_result_to_string(scrub_status) << ")";
        first = false;
    }
    ss << "} | ";

    ss << "CorruptedShardMeta={";
    first = true;
    for (const auto& [peer_id, shard_map] : corrupted_shard_metas) {
        if (!first) ss << ",";
        ss << "peer=" << peer_id << ":[";
        bool inner_first = true;
        for (const auto& [shard_id, scrub_status] : shard_map) {
            if (!inner_first) ss << ",";
            ss << shard_id << "(" << scrub_result_to_string(scrub_status) << ")";
            inner_first = false;
        }
        ss << "]";
        first = false;
    }
    ss << "} | ";

    ss << "InconsistentShardMeta={";
    first = true;
    for (const auto& [shard_id, peer_hash_map] : inconsistent_shard_metas) {
        if (!first) ss << ",";
        ss << "shard=" << shard_id << ":[";
        bool inner_first = true;
        for (const auto& [peer_id, hash] : peer_hash_map) {
            if (!inner_first) ss << ",";
            ss << "peer=" << peer_id << fmt::format("(hash={:016x})", hash);
            inner_first = false;
        }
        ss << "]";
        first = false;
    }
    ss << "} | ";

    ss << "MissingShards={";
    first = true;
    for (const auto& [shard_id, peer_set] : missing_shard_ids) {
        if (!first) ss << ",";
        ss << "shard=" << shard_id << ":[";
        bool inner_first = true;
        for (const auto& peer_id : peer_set) {
            if (!inner_first) ss << ",";
            ss << peer_id;
            inner_first = false;
        }
        ss << "]";
        first = false;
    }
    ss << "}";

    LOGINFOMOD(scrubmgr, "{}", ss.str());
}

bool ScrubManager::MetaScrubReport::merge(
    const std::map< peer_id_t, std::shared_ptr< range_scrub_result > >& peer_scrub_result_map) {
    if (peer_scrub_result_map.empty()) {
        LOGWARNMOD(scrubmgr, "[pg={}] MetaScrubReport::merge: empty map, skip", pg_id_);
        return false;
    }

    // All entries must have scrub_type == META and identical range fields.
    const auto& ref = peer_scrub_result_map.begin()->second;
    if (!ref || ref->scrub_type != SCRUB_TYPE::META) {
        LOGWARNMOD(scrubmgr, "[pg={}] MetaScrubReport::merge: first entry is null or not META, skip", pg_id_);
        return false;
    }
    for (auto it = std::next(peer_scrub_result_map.cbegin()); it != peer_scrub_result_map.cend(); ++it) {
        const auto& [peer_id, rsr] = *it;
        if (!ref->match(rsr)) {
            LOGWARNMOD(scrubmgr,
                       "[pg={}] MetaScrubReport::merge: null, wrong type, or mismatched range from peer {}, skip",
                       pg_id_, peer_id);
            return false;
        }
    }

    std::map< shard_id_t, std::map< peer_id_t, uint64_t > > shard_hash_map;
    std::set< peer_id_t > shard_reporting_peers;
    std::map< shard_id_t, std::set< peer_id_t > > shard_present_map;

    for (const auto& [peer_id, range_result] : peer_scrub_result_map) {
        shard_reporting_peers.insert(peer_id);
        for (const auto& [route, result_variant] : range_result->results) {
            const auto shard_id = route.shard;
            shard_present_map[shard_id].insert(peer_id);

            // For META results, healthy shards always carry uint64_t (the active-blob count used
            // as a placeholder hash); only a non-NONE ScrubStatus indicates corruption.
            // ScrubStatus::NONE does not appear in META results: local_scrub_meta stores uint64_t(0)
            // directly, and scrub_result::load converts any on-wire NONE to uint64_t via the hash field.
            if (std::holds_alternative< ScrubStatus >(result_variant)) {
                auto status = std::get< ScrubStatus >(result_variant);
                if (status != ScrubStatus::NONE) {
                    if (shard_id == 0) {
                        corrupted_pg_metas[peer_id] = status;
                    } else {
                        corrupted_shard_metas[peer_id][shard_id] = status;
                    }
                    LOGWARNMOD(scrubmgr, "[pg={}] find corruption for META shard={} peer={}", pg_id_, shard_id,
                               peer_id);
                }
                continue;
            }

            // uint64_t: shard is healthy; use its hash value for cross-peer consistency comparison.
            shard_hash_map[shard_id][peer_id] = std::get< uint64_t >(result_variant);
        }
    }

    // Detect shard meta inconsistency across peers.
    // Note: peers that reported corruption (IO_ERROR/MISMATCH) are excluded from shard_hash_map and therefore
    // from this check. Corruption and inconsistency are tracked separately; callers must correlate
    // corrupted_shard_metas with inconsistent_shard_metas to get the full picture.
    for (const auto& [shard_id, peer_hash_map] : shard_hash_map) {
        if (peer_hash_map.size() > 1) {
            const uint64_t ref_hash = peer_hash_map.begin()->second;
            bool consistent = std::all_of(peer_hash_map.begin(), peer_hash_map.end(),
                                          [ref_hash](const auto& kv) { return kv.second == ref_hash; });
            if (!consistent) {
                for (const auto& [peer_id, hash] : peer_hash_map) {
                    inconsistent_shard_metas[shard_id][peer_id] = hash;
                }
            }
        }
    }

    // Detect missing shards: shards seen by some peers but absent on others.
    // Record which peers HAVE the shard (existence-tracking set; see missing_shard_ids semantics in hpp).
    // shard_id=0 (pg_meta) is intentionally excluded: its corruption is captured in
    // corrupted_pg_metas, and reconcile_scrub_report uses {shard_id, 0} as the existence-
    // check route, which would be wrong for pg_meta whose route is {0, total_shards}.
    for (const auto& [shard_id, peer_set] : shard_present_map) {
        if (shard_id) {
            if (peer_set.size() < shard_reporting_peers.size()) {
                RELEASE_ASSERT(missing_shard_ids.find(shard_id) == missing_shard_ids.end(),
                               "shard_id {} should not already exist in missing_shard_ids", shard_id);

                missing_shard_ids[shard_id] = peer_set;
            }
        }

        // shard_id == 0 represents pg_meta, which is not a real shard and should not be treated as missing even if some
        // peers don't report it.
    }

    LOGINFOMOD(scrubmgr,
               "[pg={}] Meta scrub merge completed: {} corrupted shard metas, {} inconsistent shard metas, "
               "{} peers with missing shards",
               pg_id_, corrupted_shard_metas.size(), inconsistent_shard_metas.size(), missing_shard_ids.size());
    return true;
}

void ScrubManager::MetaScrubReport::remove_shard_existence_from_peer(shard_id_t shard_id, peer_id_t peer) {
    std::lock_guard lock(mutex_);
    auto it = missing_shard_ids.find(shard_id);
    if (it != missing_shard_ids.end()) {
        it->second.erase(peer);
        if (it->second.empty()) { missing_shard_ids.erase(it); }
    }
}

void ScrubManager::ShallowScrubReport::print() const {
    MetaScrubReport::print();
    std::stringstream ss;
    ss << "ShallowScrubReport for pg=" << pg_id_ << " | MissingBlobs={";
    bool first = true;
    for (const auto& [blob_route, peer_set] : missing_blobs) {
        if (!first) ss << ",";
        ss << fmt::format("{}", blob_route) << ":[";
        bool inner = true;
        for (const auto& peer_id : peer_set) {
            if (!inner) ss << ",";
            ss << peer_id;
            inner = false;
        }
        ss << "]";
        first = false;
    }
    ss << "}";
    LOGINFOMOD(scrubmgr, "{}", ss.str());
}

bool ScrubManager::ShallowScrubReport::merge(
    const std::map< peer_id_t, std::shared_ptr< range_scrub_result > >& peer_scrub_result_map) {
    if (peer_scrub_result_map.empty()) {
        LOGWARNMOD(scrubmgr, "[pg={}] ShallowScrubReport::merge: empty map, skip", pg_id_);
        return false;
    }

    const auto& ref = peer_scrub_result_map.begin()->second;
    if (!ref) {
        LOGWARNMOD(scrubmgr, "[pg={}] ShallowScrubReport::merge: first entry is null, skip", pg_id_);
        return false;
    }

    // META results are fully handled by the base class.
    if (ref->scrub_type == SCRUB_TYPE::META) { return MetaScrubReport::merge(peer_scrub_result_map); }

    RELEASE_ASSERT(ref->scrub_type == SCRUB_TYPE::SHALLOW_BLOB || ref->scrub_type == SCRUB_TYPE::DEEP_BLOB,
                   "unexpected scrub_type {} in ShallowScrubReport::merge", (int)ref->scrub_type);

    for (auto it = std::next(peer_scrub_result_map.cbegin()); it != peer_scrub_result_map.cend(); ++it) {
        const auto& [peer_id, rsr] = *it;
        if (!ref->match(rsr)) {
            LOGWARNMOD(scrubmgr,
                       "[pg={}] ShallowScrubReport::merge: null, wrong type, or mismatched range from peer {}, skip",
                       pg_id_, peer_id);
            return false;
        }
    }

    // Detect missing blobs: track which peers reported each blob, find absent ones
    std::map< BlobRoute, std::set< peer_id_t > > blob_peers_map;
    for (const auto& [peer_id, range_result] : peer_scrub_result_map) {
        if (!range_result) continue;
        for (const auto& [route, result_variant] : range_result->results) {
            blob_peers_map[route].insert(peer_id);
        }
    }

    for (const auto& [blob_route, peer_set] : blob_peers_map) {
        if (peer_set.size() < peer_scrub_result_map.size()) {
            RELEASE_ASSERT(missing_blobs.find(blob_route) == missing_blobs.end(),
                           "blob_route {} should not already exist in missing_blobs", blob_route);
            missing_blobs[blob_route] = peer_set;
        }
    }

    LOGDEBUGMOD(scrubmgr, "[pg={}] Shallow scrub merge completed!", pg_id_);
    return true;
}

void ScrubManager::ShallowScrubReport::remove_blob_existence_from_peer(BlobRoute blob_route, peer_id_t peer) {
    std::lock_guard lock(mutex_);
    auto it = missing_blobs.find(blob_route);
    if (it != missing_blobs.end()) {
        it->second.erase(peer);
        if (it->second.empty()) { missing_blobs.erase(it); }
    }
}

void ScrubManager::DeepScrubReport::print() const {
    ShallowScrubReport::print();

    std::stringstream ss;
    ss << "DeepScrubReport for pg=" << pg_id_ << " | CorruptedBlobs={";
    bool first = true;
    for (const auto& [peer_id, blob_map] : corrupted_blobs) {
        if (!first) ss << ",";
        ss << "peer=" << peer_id << ":[";
        bool inner = true;
        for (const auto& [blob_route, scrub_result] : blob_map) {
            if (!inner) ss << ",";
            ss << fmt::format("{}", blob_route) << "(" << scrub_result_to_string(scrub_result) << ")";
            inner = false;
        }
        ss << "]";
        first = false;
    }
    ss << "} | InconsistentBlobs={";
    first = true;
    for (const auto& [blob_route, peer_hash_map] : inconsistent_blobs) {
        if (!first) ss << ",";
        ss << fmt::format("{}", blob_route) << ":[";
        bool inner = true;
        for (const auto& [peer_id, hash] : peer_hash_map) {
            if (!inner) ss << ",";
            ss << "peer=" << peer_id << fmt::format("(hash={:016x})", hash);
            inner = false;
        }
        ss << "]";
        first = false;
    }
    ss << "}";
    LOGINFOMOD(scrubmgr, "{}", ss.str());
}

bool ScrubManager::DeepScrubReport::merge(
    const std::map< peer_id_t, std::shared_ptr< range_scrub_result > >& peer_scrub_result_map) {
    if (peer_scrub_result_map.empty()) {
        LOGWARNMOD(scrubmgr, "[pg={}] DeepScrubReport::merge: empty map, skip", pg_id_);
        return false;
    }

    const auto& ref = peer_scrub_result_map.begin()->second;
    if (!ref) {
        LOGWARNMOD(scrubmgr, "[pg={}] DeepScrubReport::merge: first entry is null, skip", pg_id_);
        return false;
    }

    const auto scrub_type = ref->scrub_type;

    // META and SHALLOW_BLOB results are fully handled by parent classes; no deep-specific work needed.
    if (scrub_type != SCRUB_TYPE::DEEP_BLOB) { return ShallowScrubReport::merge(peer_scrub_result_map); }

    // DEEP_BLOB: first detect missing blobs via ShallowScrubReport (which also validates the range),
    // then add corrupted-blob and hash-inconsistency detection.
    if (!ShallowScrubReport::merge(peer_scrub_result_map)) { return false; }

    // Detect corrupted blobs (non-NONE scrub_result) reported by any peer.
    // Detect hash inconsistencies among healthy blobs in the same pass.
    std::map< BlobRoute, std::map< peer_id_t, uint64_t > > hash_map_per_blob;
    for (const auto& [peer_id, range_result] : peer_scrub_result_map) {
        if (!range_result) continue;
        for (const auto& [blob_route, result_variant] : range_result->results) {
            if (std::holds_alternative< ScrubStatus >(result_variant)) {
                auto status = std::get< ScrubStatus >(result_variant);
                if (status != ScrubStatus::NONE) {
                    corrupted_blobs[peer_id][blob_route] = status;
                    LOGWARNMOD(scrubmgr, "[pg={}] find corruption for blob shard_id={}, blob_id={}, peer={}", pg_id_,
                               blob_route.shard, blob_route.blob, peer_id);
                }
            } else {
                hash_map_per_blob[blob_route][peer_id] = std::get< uint64_t >(result_variant);
            }
        }
    }

    // Note: peers that reported corruption (IO_ERROR/MISMATCH) have no hash entry in hash_map_per_blob.
    // Hash inconsistency therefore requires ≥2 healthy peers for detection. Corruption is captured
    // separately in corrupted_blobs.
    for (const auto& [blob_route, hash_map] : hash_map_per_blob) {
        if (hash_map.size() > 1) {
            uint64_t ref_hash = hash_map.begin()->second;
            bool consistent = std::all_of(hash_map.begin(), hash_map.end(),
                                          [ref_hash](const auto& kv) { return kv.second == ref_hash; });
            if (!consistent) {
                for (const auto& [peer_id, hash_val] : hash_map) {
                    inconsistent_blobs[blob_route][peer_id] = hash_val;
                }
            }
        }
    }

    LOGINFOMOD(scrubmgr,
               "[pg={}] Deep blob scrub merge completed: {} missing blobs, {} corrupted blobs, {} inconsistent blobs",
               pg_id_, missing_blobs.size(), corrupted_blobs.size(), inconsistent_blobs.size());
    return true;
}

} // namespace homeobject