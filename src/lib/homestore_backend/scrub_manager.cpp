#include "hs_homeobject.hpp"
#include <homestore/btree/btree_req.hpp>
#include <homestore/btree/btree_kv.hpp>
#include <sstream>
#include <algorithm>

namespace homeobject {

SISL_LOGGING_DECL(scrubmgr)
#define NO_TASK_ID 0
#define HDD_IOPS 200

#define SCRUBLOG(level, pg_id, task_id, msg, ...)                                                                      \
    LOG##level##MOD(scrubmgr, "[pg_id={}, task_id={}] " msg, pg_id, task_id, ##__VA_ARGS__)

#define SCRUBLOGD(pg_id, task_id, msg, ...) SCRUBLOG(DEBUG, pg_id, task_id, msg, ##__VA_ARGS__)
#define SCRUBLOGI(pg_id, task_id, msg, ...) SCRUBLOG(INFO, pg_id, task_id, msg, ##__VA_ARGS__)
#define SCRUBLOGW(pg_id, task_id, msg, ...) SCRUBLOG(WARN, pg_id, task_id, msg, ##__VA_ARGS__)
#define SCRUBLOGE(pg_id, task_id, msg, ...) SCRUBLOG(ERROR, pg_id, task_id, msg, ##__VA_ARGS__)
#define SCRUBLOGC(pg_id, task_id, msg, ...) SCRUBLOG(CRITICAL, pg_id, task_id, msg, ##__VA_ARGS__)

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
            LOGINFOMOD(scrubmgr, "pg={} is eligible for deep scrub", pg_id);
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
                        LOGERRORMOD(scrubmgr, "report for deep scrub can not be casted to DeepScrubReport for pg={}",
                                    pg_id);
                        return;
                    }
                    handle_deep_pg_scrub_report(std::move(deep_report));
                });
        } else if (is_eligible_for_shallow_scrub(pg_id)) {
            LOGINFOMOD(scrubmgr, "pg={} is eligible for shallow scrub", pg_id);
            submit_scrub_task(pg_id, false)
                .via(&folly::InlineExecutor::instance())
                .thenValue([this, pg_id](std::shared_ptr< ShallowScrubReport > report) {
                    if (!report) {
                        LOGERRORMOD(scrubmgr, "deep scrub failed for pg={}", pg_id);
                        return;
                    }
                    LOGINFOMOD(scrubmgr, "shallow scrub is completed for pg={}", pg_id);
                    handle_shallow_pg_scrub_report(std::move(report));
                });
        } else {
            LOGINFOMOD(scrubmgr, "pg={} is not eligible for any scrubbing!", pg_id);
        }
    }
}

void ScrubManager::handle_shallow_pg_scrub_report(std::shared_ptr< ShallowScrubReport > report) {
    if (!report) {
        LOGERRORMOD(scrubmgr, "Shallow scrub report is null!");
        return;
    }

    report->print();
    // TODO:: add more logic, log event for notifcation.
}

void ScrubManager::handle_deep_pg_scrub_report(std::shared_ptr< DeepScrubReport > report) {
    if (!report) {
        LOGERRORMOD(scrubmgr, "Deep scrub report is null!");
        return;
    }

    report->print();
    // TODO:: add more logic, log event for notifcation.
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
    // TODO :: make thread count configurable, thread number is the most concurrent scrub tasks that can be handled
    // concurrently. too many concurrent scrub tasks may bring too much pressure to the node.
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
                auto task = pop_result.value.value();
                // we handle pg scrub task in a single thread , so that we can control the concurrent scrub tasks by
                // controlling the thread number of m_scrub_executor.
                handle_pg_scrub_task(std::move(task));
            }
        });
    }

    const auto most_concurrent_scrub_req_num = 2;
    // we don`t set priority for req as that of task, only control the concurrency to not bring too much pressuer to
    // this node.
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
    // shutdown timer
    if (m_scrub_timer_hdl == iomgr::null_timer_handle) {
        LOGINFOMOD(scrubmgr, "scrub scheduler timer is not running, no need to stop it");
        return;
    }
    RELEASE_ASSERT(m_scrub_timer_fiber,
                   "m_scrub_timer_hdl is not null_timer_handle, but m_scrub_timer_fiber is null, fatal error!");
    LOGINFOMOD(scrubmgr, "stop scrub scheduler timer");
    iomanager.run_on_wait(m_scrub_timer_fiber, [&]() {
        iomanager.cancel_timer(m_scrub_timer_hdl, true);
        m_scrub_timer_hdl = iomgr::null_timer_handle;
    });
    m_scrub_timer_fiber = nullptr;

    // cancel all the running scrub tasks and clear the scrub task queue.
    // TODO:: add a stoopeed flag to avoid adding new scrub task if stopped.
    m_scrub_task_queue.close();
    for (auto& [_, pg_scrub_ctx] : m_pg_scrub_ctx_map) {
        pg_scrub_ctx->cancel();
    }

    m_scrub_executor->stop();
    m_scrub_executor.reset();
    m_scrub_req_executor->stop();
    m_scrub_req_executor.reset();
    LOGINFOMOD(scrubmgr, "scrub manager stopped!");
}

void ScrubManager::add_scrub_req(std::shared_ptr< base_scrub_req > req) {
    m_scrub_req_executor->add([this, req = std::move(req)]() { handle_scrub_req(req); });
}

bool ScrubManager::add_scrub_map(const pg_id_t pg_id, std::shared_ptr< BaseScrubMap > bsm) {
    auto pg_scrub_ctx_it = m_pg_scrub_ctx_map.find(pg_id);
    if (pg_scrub_ctx_it == m_pg_scrub_ctx_map.end()) {
        LOGERRORMOD(scrubmgr, "can not find scrub context for pg_id={}, fail to add scrub map!", pg_id);
        return false;
    }

    auto& pg_scrub_ctx = pg_scrub_ctx_it->second;
    return pg_scrub_ctx->add_scrub_map(std::move(bsm));
}

void ScrubManager::handle_scrub_req(std::shared_ptr< base_scrub_req > req) {
    if (!req) {
        LOGERRORMOD(scrubmgr, "scrub req is null, can not handle it!");
        return;
    }

    const auto& pg_id = req->pg_id;
    const auto& task_id = req->task_id;
    const auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        SCRUBLOGD(pg_id, task_id, "can not find hs_pg, fail to handle scrub req!");
        return;
    }

    const auto& pg_repl_dev = hs_pg->repl_dev_;
    if (!pg_repl_dev) {
        SCRUBLOGD(pg_id, task_id, "repl_dev is null, fail to handle scrub req!");
        return;
    }

    if (pg_repl_dev->is_leader()) {
        SCRUBLOGD(pg_id, task_id, "leader of pg, no need to handle stale scrub req!");
        return;
    }

    std::shared_ptr< BaseScrubMap > scrub_map;
    auto& remote_peer_id = req->issuer_peer_id;

    // 1 do scrub
    const auto scrub_type = req->get_scrub_type();
    switch (scrub_type) {
    case SCRUB_TYPE::PG_META: {
        SCRUBLOGD(pg_id, task_id, "handling pg meta scrub req, ");
        scrub_map = scrub_pg_meta(req);
        break;
    }
    case SCRUB_TYPE::DEEP_BLOB:
    case SCRUB_TYPE::SHALLOW_BLOB: {
        auto blob_req = std::dynamic_pointer_cast< blob_scrub_req >(req);
        RELEASE_ASSERT(blob_req, "Failed to cast to blob_scrub_req");
        SCRUBLOGD(pg_id, task_id, "handling blob scrub req, is_deep_scrub:{}", req->is_deep_scrub());
        scrub_map = local_scrub_blob(blob_req);
        break;
    }
    case SCRUB_TYPE::DEEP_SHARD:
    case SCRUB_TYPE::SHALLOW_SHARD: {
        auto shard_req = std::dynamic_pointer_cast< shard_scrub_req >(req);
        RELEASE_ASSERT(shard_req, "Failed to cast to shard_scrub_req");
        SCRUBLOGD(pg_id, task_id, "handling shard scrub req, is_deep_scrub:{}", req->is_deep_scrub());
        scrub_map = local_scrub_shard(shard_req);
        break;
    }
    default:
        RELEASE_ASSERT(false, "unknown scrub req type: {}!", scrub_type);
    }

    if (!scrub_map) {
        SCRUBLOGD(pg_id, task_id, "fail to handle scrub req, drop it!");
        return;
    }

    // 2 send scrub map back to leader
    auto flatbuffer = scrub_map->build_flat_buffer();
    sisl::io_blob_list_t blob_list;
    blob_list.emplace_back(reinterpret_cast< const uint8_t* >(&scrub_type), sizeof(scrub_type), false);
    blob_list.emplace_back(flatbuffer.data(), flatbuffer.size(), false);

    // no need to retry, leader will handle retries
    pg_repl_dev->data_request_unidirectional(remote_peer_id, HSHomeObject::PUSH_SCRUB_MAP, blob_list)
        .via(&folly::InlineExecutor::instance())
        .thenValue([pg_id, remote_peer_id, scrub_type, task_id](auto&& response) {
            if (response.hasError()) {
                SCRUBLOGD(pg_id, task_id, "failed to send scrub map to peer {}, scrub_type:{}, error={}",
                          remote_peer_id, scrub_type, response.error());
                return;
            }

            SCRUBLOGD(pg_id, task_id, "successfully sent scrub map to peer {}, scrub_type:{}", remote_peer_id,
                      scrub_type);
        });
}

bool ScrubManager::wait_for_scrub_lsn_commit(shared< homestore::ReplDev > repl_dev, int64_t scrub_lsn) {
    if (!repl_dev) {
        LOGERRORMOD(scrubmgr, "repl_dev is null, can not wait for scrub lsn commit!");
        return false;
    }

    // TODO:: make this configurable
    const auto wait_retry_times = 2;
    for (auto i = 0; i < wait_retry_times; ++i) {
        auto commit_lsn = repl_dev->get_last_commit_lsn();
        if (commit_lsn >= scrub_lsn) {
            LOGINFOMOD(scrubmgr, "commit lsn {} is greater than or equal to scrub lsn {}, wait successfully",
                       commit_lsn, scrub_lsn);
            return true;
        }
        LOGINFOMOD(scrubmgr,
                   "commit lsn {} is less than scrub lsn {}, wait for 1 second before retrying, retry times {}/{}",
                   commit_lsn, scrub_lsn, i + 1, wait_retry_times);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return false;
}

std::shared_ptr< ScrubManager::PGMetaScrubMap > ScrubManager::scrub_pg_meta(std::shared_ptr< base_scrub_req > req) {
    const auto my_uuid = m_hs_home_object->our_uuid();
    const auto pg_id = req->pg_id;
    const auto task_id = req->task_id;
    const auto req_id = req->req_id;
    const auto scrub_lsn = req->scrub_lsn;
    auto pg_meta_scrub_map =
        std::make_shared< ScrubManager::PGMetaScrubMap >(pg_id, task_id, req_id, scrub_lsn, my_uuid);

    SCRUBLOGD(pg_id, task_id, "req_id={}, do pg meta scrub", req_id);

    // TODO:: add support to read the pg meta blk of a specific pg.
    // read pg metablk and compare with in-memory state, return the real pg meta scrub map after comparison.

    return pg_meta_scrub_map;
}

std::shared_ptr< ScrubManager::BaseScrubMap > ScrubManager::local_scrub_blob(std::shared_ptr< blob_scrub_req > req) {
    const auto my_uuid = m_hs_home_object->our_uuid();
    const auto task_id = req->task_id;
    const auto req_id = req->req_id;
    const auto scrub_lsn = req->scrub_lsn;
    const auto& pg_id = req->pg_id;
    const auto& start_blob_id = req->start;
    const auto& end_blob_id = req->end;

    SCRUBLOGD(pg_id, task_id, "req_id={}, scrub_blob: range [{}, {}], scrub_lsn={}", req_id, start_blob_id, end_blob_id,
              scrub_lsn);

    auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        SCRUBLOGD(pg_id, task_id, "req_id={},can not find hs_pg, fail to do deep blob scrub!", req_id);
        return nullptr;
    }

    if (!wait_for_scrub_lsn_commit(hs_pg->repl_dev_, scrub_lsn)) {
        SCRUBLOGD(pg_id, task_id,
                  "req_id={}, commit lsn is not advanced to scrub lsn {} after waiting for a while, fail to do deep "
                  "blob scrub",
                  req_id, scrub_lsn);
        return nullptr;
    }

    // get all the scrub candidate blobs. we only get those blobs in this range and the sealed_lsn of the shard is after
    // the scrub_lsn.
    const auto start = BlobRouteKey{BlobRoute{0, start_blob_id}};
    const auto end = BlobRouteKey{BlobRoute{std::numeric_limits< uint64_t >::max(), end_blob_id}};

    std::vector< std::pair< BlobRouteKey, BlobRouteValue > > scrub_candidate_blobs;
    auto& pg_index_table = hs_pg->index_table_;
    homestore::BtreeQueryRequest< BlobRouteKey > query_req{
        homestore::BtreeKeyRange< BlobRouteKey >{start, true /* inclusive */, end, true /* inclusive */},
        homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY, std::numeric_limits< uint32_t >::max(),
        [scrub_lsn, start_blob_id, end_blob_id](homestore::BtreeKey const& key,
                                                homestore::BtreeValue const& value) -> bool {
            BlobRouteValue existing_value{value};
            if (existing_value.pbas() == HSHomeObject::tombstone_pbas) { return false; }
            const auto blob_route_key = BlobRouteKey{key};
            if (blob_route_key.key().blob < start_blob_id || blob_route_key.key().blob > end_blob_id) { return false; }

            // TODO:: after we have shard seal_lsn, check whether the shard of the blob is sealed after scrub_lsn. If
            // yes, filter it out as well.

            return true;
        }};

    auto const status = pg_index_table->query(query_req, scrub_candidate_blobs);
    if (status != homestore::btree_status_t::success) {
        SCRUBLOGD(pg_id, task_id, "req_id={}, Failed to query blobs in index table for status={}", req_id, status);
        return nullptr;
    }

    const bool is_deep_scrub = req->is_deep_scrub();

    if (!is_deep_scrub) {
        auto shallow_srcub_map = std::make_shared< ScrubManager::ShallowBlobScrubMap >(
            pg_id, task_id, req_id, scrub_lsn, my_uuid, start_blob_id, end_blob_id);

        for (const auto& [k, _] : scrub_candidate_blobs) {
            shallow_srcub_map->add_blob(k.key());
        }

        SCRUBLOGD(pg_id, task_id, "req_id={}, shallow blob scrub completed, found {} blobs in range [{},{})", req_id,
                  shallow_srcub_map->blobs.size(), start, end);

        return shallow_srcub_map;
    }

    // deep scrub: read and check blobs.
    auto deep_scrub_map = std::make_shared< ScrubManager::DeepBlobScrubMap >(pg_id, task_id, req_id, scrub_lsn, my_uuid,
                                                                             start_blob_id, end_blob_id);
    auto& data_service = homestore::data_service();
    const auto blk_size = data_service.get_blk_size();

    // Sort scrub_candidate_blobs by PBA (physical block address) for sequential disk access
    std::sort(scrub_candidate_blobs.begin(), scrub_candidate_blobs.end(), [](const auto& a, const auto& b) {
        // Compare by PBA to_string() for ordering
        const auto pba_a = a.second.pbas().to_single_blkid();
        const auto pba_b = b.second.pbas().to_single_blkid();
        return pba_a.blk_num() < pba_b.blk_num();
    });

    // to not bring to much io pressure, we deep scrub blob one by one.
    // TODO: scrubbing blobs concurrently if neccessary.
    for (const auto& [k, v] : scrub_candidate_blobs) {
        auto pba = v.pbas();
        auto total_size = pba.blk_count() * blk_size;
        sisl::sg_list data_sgs;
        data_sgs.size = total_size;
        data_sgs.iovs.emplace_back(
            iovec{.iov_base = iomanager.iobuf_alloc(blk_size, total_size), .iov_len = total_size});

        data_service.async_read(pba, data_sgs, total_size)
            .thenValue([this, &k, data_sgs = std::move(data_sgs), deep_scrub_map](auto&& err) {
                auto blob = data_sgs.iovs[0].iov_base;

                struct buffer_free_guard {
                    uint8_t* buf;
                    ~buffer_free_guard() { iomanager.iobuf_free(buf); }
                } guard{reinterpret_cast< uint8_t* >(blob)};

                if (err) {
                    LOGERRORMOD(scrubmgr, "Failed to read blob for deep scrub, blob_route={}, error={}", k.key(),
                                err.message());
                    deep_scrub_map->add_blob_result(k.key(), ScrubResult::IO_ERROR);
                    return;
                }

                const auto& shard_id = k.key().shard;
                const auto& blob_id = k.key().blob;
                const auto blob_verify_succeed = m_hs_home_object->verify_blob(blob, shard_id, blob_id, true);
                if (!blob_verify_succeed) {
                    LOGERRORMOD(scrubmgr, "Blob verification failed for deep scrub, blob_route={}", k.key());
                    deep_scrub_map->add_blob_result(k.key(), ScrubResult::MISMATCH);
                    return;
                }

                BlobHashArray blob_hash{};
                HSHomeObject::BlobHeader const* header = r_cast< HSHomeObject::BlobHeader const* >(blob);
                std::memcpy(blob_hash.data(), header->hash, blob_hash.size());
                deep_scrub_map->add_blob_result(k.key(), blob_hash);
            })
            // we do deep blob sequentially, so that we can control the io pressure brought by deep scrub.
            .get();
    }

    SCRUBLOGD(pg_id, task_id, "req_id={}, deep blob scrub completed, found {} blobs in range [{},{})", req_id,
              deep_scrub_map->blobs.size(), start, end);

    return deep_scrub_map;
}

std::shared_ptr< ScrubManager::ShallowShardScrubMap >
ScrubManager::local_scrub_shard(std::shared_ptr< shard_scrub_req > req) {
    const auto my_uuid = m_hs_home_object->our_uuid();
    const auto task_id = req->task_id;
    const auto req_id = req->req_id;
    const auto scrub_lsn = req->scrub_lsn;
    const auto& pg_id = req->pg_id;
    const auto start = req->start;
    const auto end = req->end;

    auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        SCRUBLOGD(pg_id, task_id, "can not find hs_pg, fail to do deep shard scrub!");
        return nullptr;
    }

    if (!wait_for_scrub_lsn_commit(hs_pg->repl_dev_, scrub_lsn)) {
        SCRUBLOGD(pg_id, task_id,
                  "commit lsn is not advanced to scrub lsn {} after waiting for a while, fail to do local shard scrub ",
                  scrub_lsn);
        return nullptr;
    }

    std::shared_ptr< ScrubManager::ShallowShardScrubMap > shard_srcub_map;
    const bool is_deep_scrub = req->is_deep_scrub();
    if (is_deep_scrub) {
        shard_srcub_map =
            std::make_shared< ScrubManager::DeepShardScrubMap >(pg_id, task_id, req_id, scrub_lsn, my_uuid);
    } else {
        shard_srcub_map =
            std::make_shared< ScrubManager::ShallowShardScrubMap >(pg_id, task_id, req_id, scrub_lsn, my_uuid);
    }

    // Iterate through all shards in the PG
    for (const auto& shard_it : hs_pg->shards_) {
        auto shard_id = (shard_it->info).id;
        // remove pg_id, get the pure shard id.
        auto pure_shard_id = (shard_id << pg_width) >> pg_width;
        if (pure_shard_id < start || pure_shard_id > end) { continue; }

        // TODO:: filter out those shards whose seal_lsn is after scrub_lsn, as they are not in the candidate shard
        // list for scrub.
        shard_srcub_map->add_shard(shard_id);

        // TODO:: optimize the folloing logic, dynamic cast once.
        if (is_deep_scrub) {
            auto deep_shard_scrub_map = std::dynamic_pointer_cast< DeepShardScrubMap >(shard_srcub_map);
            RELEASE_ASSERT(deep_shard_scrub_map,
                           "shard_srcub_map should be DeepShardScrubMap when is_deep_scrub is true!");

            // TODO: Read and verify shard metablk
            // For now, we just mark it as NONE (no error found) since we can not read a specific shard metablk for now.
            // it needs the support of homestore#metaservice. we should:
            // 1. Read shard metablk from homestore
            // 2. Compare with in-memory shard info
            // 3. If mismatch or not found or error_io, add to problematic_shards with appropriate ScrubResult

            // TODO:: if find any problematic shard meta blk.
            //  deep_shard_scrub_map->add_problematic_shard(shard_id, ScrubResult::NONE);
        }
    }

    SCRUBLOGD(pg_id, task_id, "shard scrub completed, checked {} shards in range [{},{})",
              shard_srcub_map->shards.size(), start, end);

    return shard_srcub_map;
}

folly::SemiFuture< std::shared_ptr< ScrubManager::ShallowScrubReport > >
ScrubManager::submit_scrub_task(const pg_id_t& pg_id, const bool is_deep, const bool force,
                                SCRUB_TRIGGER_TYPE trigger_type) {
    LOGINFOMOD(scrubmgr, "submit a scrub task for pg={}, deep_scrub:{}", pg_id, is_deep);
    auto it = m_pg_scrub_ctx_map.find(pg_id);
    if (it != m_pg_scrub_ctx_map.end()) {
        // TODO:: there is case that two thread try to submit scrub task for the same pg at the same time, we can
        // optimize it by adding a lock for each pg or using atomic operation to make sure only one scrub task can be
        // submitted for each pg, and other threads can get the existing scrub task if they want to submit another scrub
        // task for the same pg.
        LOGWARNMOD(scrubmgr, "a scrub task is already running for pg={}, no need to submit another one!", pg_id);
        return folly::makeFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
    }

    const auto ps_scrub_super_blk_it = m_pg_scrub_sb_map.find(pg_id);
    if (ps_scrub_super_blk_it == m_pg_scrub_sb_map.end()) {
        LOGERRORMOD(scrubmgr, "can not find scrub superblk for pg={}, fail to submit scrub task!", pg_id);
        return folly::makeFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
    }

    // Get the PG and check its state
    const auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        LOGERRORMOD(scrubmgr, "can not find hs_pg for pg={}, fail to submit scrub task!", pg_id);
        return folly::makeFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
    }

    // Check if pg_state is HEALTHY (state must be 0)
    if (!force) {
        const auto current_state = hs_pg->pg_state_.get();
        if (current_state != 0) {
            LOGWARNMOD(scrubmgr, "pg={} is not in HEALTHY state (current_state={}), cannot submit scrub task!", pg_id,
                       current_state);
            return folly::makeFuture(std::shared_ptr< ScrubManager::ShallowScrubReport >(nullptr));
        }

        // Set SCRUBBING state
        hs_pg->pg_state_.set_state(PGStateMask::SCRUBBING);
        LOGINFOMOD(scrubmgr, "set SCRUBBING state for pg={}", pg_id);
    }

    // TODO::check the stopped flag to avoid submit new scrub task when scrub manager is stopped.

    const auto& pg_scrub_sb = *(ps_scrub_super_blk_it->second);
    const auto last_scrub_time =
        is_deep ? pg_scrub_sb->last_deep_scrub_timestamp : pg_scrub_sb->last_shallow_scrub_timestamp;

    auto [promise, future] = folly::makePromiseContract< std::shared_ptr< ShallowScrubReport > >();
    ScrubManager::scrub_task task(last_scrub_time, pg_id, is_deep, trigger_type, std::move(promise));
    m_scrub_task_queue.push(std::move(task));
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

// Helper function to send scrub requests to all peers and handle retries
bool ScrubManager::send_scrub_req_and_wait(pg_id_t pg_id, uint64_t task_id,
                                           const std::unordered_set< peer_id_t >& all_member_peer_ids,
                                           const peer_id_t& my_uuid, shared< homestore::ReplDev > pg_repl_dev,
                                           const sisl::io_blob_list_t& req_blob_list,
                                           std::shared_ptr< PGScrubContext > scrub_ctx, uint32_t max_retries,
                                           std::chrono::seconds timeout, const std::string& scrub_type_name) {
    // Lambda to send requests to a list of peers
    auto send_requests_to_remote_peers = [&](const auto& peer_list, bool is_retry) {
        for (const auto& peer_id : peer_list) {
            if (peer_id == my_uuid) continue;
            pg_repl_dev->data_request_unidirectional(peer_id, HSHomeObject::PUSH_SCRUB_REQ, req_blob_list)
                .via(&folly::InlineExecutor::instance())
                .thenValue([pg_id, peer_id, task_id, scrub_type_name, is_retry](auto&& response) {
                    if (response.hasError()) {
                        SCRUBLOGE(pg_id, task_id, "{} to send {} scrub request to peer {}",
                                  is_retry ? "retry failed" : "failed", scrub_type_name, peer_id);
                    }
                });
        }
    };

    // Send initial requests to all peers
    send_requests_to_remote_peers(all_member_peer_ids, false);

    // Wait for all responses and retry if needed
    if (!scrub_ctx->wait_for_all_req_sms(timeout)) {
        for (uint32_t retry = 0; retry < max_retries; ++retry) {
            auto peers_to_retry = scrub_ctx->get_peers_to_retry();
            if (peers_to_retry.empty()) break;

            SCRUBLOGD(pg_id, task_id, "Retrying {} scrub for {} peers", scrub_type_name, peers_to_retry.size());
            send_requests_to_remote_peers(peers_to_retry, true);

            if (scrub_ctx->wait_for_all_req_sms(timeout)) break;
        }
    }

    // Check if cancelled or incomplete
    if (scrub_ctx->is_cancelled() || scrub_ctx->peer_sm_map_.size() != scrub_ctx->member_peer_ids_.size()) {
        SCRUBLOGD(pg_id, task_id, "scrub task is cancelled or incomplete when scrubbing {}!", scrub_type_name);
        return false;
    }
    return true;
}

void ScrubManager::handle_pg_scrub_task(scrub_task task) {
    // we handle deep and shallow scrub task in the same fuction to reduce code duplication.
    // TODO:: separate them if the logic is very different in the future.

    const auto& pg_id = task.pg_id;
    const auto& task_id = task.task_id;
    const auto is_deep_scrub = task.is_deep_scrub;
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

            // Clear SCRUBBING state from pg_state
            auto hs_pg = home_obj->get_hs_pg(pg_id);
            if (hs_pg) {
                hs_pg->pg_state_.clear_state(PGStateMask::SCRUBBING);
                LOGINFOMOD(scrubmgr, "cleared SCRUBBING state for pg={}", pg_id);
            }
        }
    } guard{m_hs_home_object, m_pg_scrub_ctx_map, task, pg_scrub_report, pg_id};

    const auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        SCRUBLOGE(pg_id, task_id, "can not find hs_pg for this pg, fail this scrub task!");
        return;
    }

    const auto& members = (hs_pg->pg_info_).members;
    std::unordered_set< peer_id_t > all_member_peer_ids;
    for (const auto& member : members) {
        all_member_peer_ids.insert(member.id);
    }

    const auto& my_uuid = m_hs_home_object->our_uuid();
    // TODO: the node is removed from the raft group? handle this case later
    RELEASE_ASSERT(all_member_peer_ids.find(my_uuid) != all_member_peer_ids.end(),
                   "my uuid={} is not in the member list of this pg, something is wrong!", my_uuid);

    auto [ctx_it, happened] =
        m_pg_scrub_ctx_map.try_emplace(pg_id, std::make_shared< PGScrubContext >(task_id, all_member_peer_ids));
    if (!happened) {
        SCRUBLOGE(pg_id, task_id, "a scrub task is already running for this pg, fail this {} scrub task!",
                  is_deep_scrub ? "deep" : "shallow");
        return;
    }

    auto& scrub_ctx = ctx_it->second;
    const auto& pg_repl_dev = hs_pg->repl_dev_;
    auto scrub_lsn = pg_repl_dev->get_last_commit_lsn();

    // Scrub timeout configuration (based on HDD random read performance， iops)
    // Worst case scenario: 2GB data in a chunk, 4K (min blob size) random read, 7200 RPM HDD ~200 IOPS
    // Total read operations: 2GB / 4KB = 524,288 reads
    // Estimated time: 524,288 / 200 = 5,243 seconds ≈ 44minutes
    // so scrub the whole chunk at a time is not acceptable. we need to scrub blobs range by range , every range should
    // have a acceptable timeout.

    // TODO::make the following parameters configurable and find the optimal value based on real world scrub performance
    // test. for hdd, iops matters more than throughput in scrubbing case.
    constexpr uint32_t MAX_RETRIES = 5; // Maximum retry attempts
    constexpr auto SM_REQUEST_TIMEOUT = std::chrono::seconds(10);

    // Step 1: Scrub PG Meta (only for deep scrub)
    if (is_deep_scrub) {
        SCRUBLOGD(pg_id, task_id, "Starting PG meta scrub");
        auto pg_meta_req = std::make_shared< base_scrub_req >(task_id, scrub_ctx->req_id.fetch_add(1), scrub_lsn,
                                                              my_uuid, pg_id, true);
        // TODO:: add a lock here to protect add_scrub_map when changing current_req.
        scrub_ctx->current_req = pg_meta_req;
        // Send requests to all peers
        auto flatbuffer = pg_meta_req->build_flat_buffer();
        sisl::io_blob_list_t req_blob_list;
        const auto scrub_type = SCRUB_TYPE::PG_META;
        req_blob_list.emplace_back(reinterpret_cast< const uint8_t* >(&scrub_type), sizeof(scrub_type), false);
        req_blob_list.emplace_back(flatbuffer.data(), flatbuffer.size(), false);

        // Scrub locally async (runs in parallel with remote requests)
        m_scrub_req_executor->add([this, pg_meta_req, scrub_ctx, pg_id, task_id]() {
            auto pg_meta_map = scrub_pg_meta(pg_meta_req);
            if (!scrub_ctx->add_scrub_map(pg_meta_map)) {
                SCRUBLOGE(pg_id, task_id, "failed to add local PG meta scrub map to context!");
            } else {
                SCRUBLOGD(pg_id, task_id, "Local PG meta scrub added");
            }
        });

        // Send requests to all peers and wait for responses
        if (!send_scrub_req_and_wait(pg_id, task_id, all_member_peer_ids, my_uuid, pg_repl_dev, req_blob_list,
                                     scrub_ctx, MAX_RETRIES, SM_REQUEST_TIMEOUT, "PG meta")) {
            return;
        }

        // Merge PG meta scrub results
        pg_scrub_report->merge(scrub_ctx->peer_sm_map_);
        SCRUBLOGD(pg_id, task_id, "PG meta scrub completed");
    }

    // Step 2: Scrub Shard Range
    SCRUBLOGD(pg_id, task_id, "Starting shard range {} scrub", is_deep_scrub ? "deep" : "shallow");
    {
        // we can not scrub all shards based on the shard sealed_lsn, especially after we have shard seal_lsn. since
        // leader might lose some shard, so if we select the shard range seen by leader itself, we might miss those lost
        // shards which exist on the follower , but not on leader. so for now, we select shard range based on the
        // current shard_sequence_num_ at leader, which is the last shard_id in of the pg.

        // we assume shard_id will not overflow uint64_t;
        const auto last_shard_id = hs_pg->shard_sequence_num_;

        // the key point here is that until we commit to scrub_lsn , we should at least see last_shard_id.
        scrub_lsn = pg_repl_dev->get_last_commit_lsn();
        SCRUBLOGD(pg_id, task_id, "Shard range: 0 to {}, scrub_lsn={}", last_shard_id, scrub_lsn);

        // TODO:: make it configurable。
        // a shard_id_t is uint64(8B). if we want the max size of a shard scrub map is 16MB, then the num of
        // shard_it in a shard scrub map should be 16MB/8B=2M(2097152)
        const auto shard_scrub_range_size = 2097152;

        // Scrub shard range
        uint64_t shard_start = 0;
        uint64_t shard_end = shard_scrub_range_size;
        uint64_t shard_range_count = 0;
        for (; shard_start <= last_shard_id;
             shard_start = shard_end + 1, shard_end = std::min(shard_end + shard_scrub_range_size, last_shard_id)) {
            ++shard_range_count;
            SCRUBLOGD(pg_id, task_id, "Scrubbing shard range {}: [{}, {}]", shard_range_count, shard_start, shard_end);

            auto shard_req = std::make_shared< shard_scrub_req >(task_id, scrub_ctx->req_id.fetch_add(1), scrub_lsn,
                                                                 my_uuid, pg_id, shard_start, shard_end, is_deep_scrub);
            scrub_ctx->reset_for_new_req();
            scrub_ctx->current_req = shard_req;

            // scrub locally async (runs in parallel with remote requests)
            m_scrub_req_executor->add([this, shard_req, scrub_ctx, pg_id, task_id, is_deep_scrub]() {
                auto scrub_map = local_scrub_shard(shard_req);
                if (!scrub_ctx->add_scrub_map(scrub_map)) {
                    SCRUBLOGE(pg_id, task_id, "failed to add local {} shard scrub map to context!",
                              is_deep_scrub ? "deep" : "shallow");
                } else {
                    SCRUBLOGD(pg_id, task_id, "local {} shard scrub map added!", is_deep_scrub ? "deep" : "shallow");
                }
            });

            // request remote peers to scrub this shard range and wait for responses
            auto flatbuffer = shard_req->build_flat_buffer();
            sisl::io_blob_list_t req_blob_list;
            const auto scrub_type = is_deep_scrub ? SCRUB_TYPE::DEEP_SHARD : SCRUB_TYPE::SHALLOW_SHARD;
            req_blob_list.emplace_back(reinterpret_cast< const uint8_t* >(&scrub_type), sizeof(scrub_type), false);
            req_blob_list.emplace_back(flatbuffer.data(), flatbuffer.size(), false);

            if (!send_scrub_req_and_wait(pg_id, task_id, all_member_peer_ids, my_uuid, pg_repl_dev, req_blob_list,
                                         scrub_ctx, MAX_RETRIES, SM_REQUEST_TIMEOUT, "shard")) {
                SCRUBLOGE(pg_id, task_id, "shard scrub failed or was cancelled");
                return;
            }

            SCRUBLOGD(pg_id, task_id, "Merging shard scrub results for range [{}, {}]", shard_start, shard_end);
            pg_scrub_report->merge(scrub_ctx->peer_sm_map_);
        }
        SCRUBLOGD(pg_id, task_id, "shard scrub completed, total ranges scrubbed: {}", shard_range_count);
    }

    // Step 3: Scrub Blob Range
    SCRUBLOGD(pg_id, task_id, "Starting blob range {} scrub", is_deep_scrub ? "deep" : "shallow");
    {
        // we assume shard_id will not overflow uint64_t;
        const auto last_blob_id = hs_pg->get_last_blob_id();

        // just like shard, the key point here is that until we commit to scrub_lsn , we can see last_blob_id.
        scrub_lsn = pg_repl_dev->get_last_commit_lsn();
        SCRUBLOGD(pg_id, task_id, "Blob range: 0 to {}, scrub_lsn={}", last_blob_id, scrub_lsn);

        // For deep scrub: since we have a SM_REQUEST_TIMEOUT as scrub map request timeout. assuming the iops of a hdd
        // is 200, and we want at most half of the time to be spent on io, so we have this blob range.
        // For shallow scrub: we will not schedule io to disk, so we set blob scrub range the same as that of shard.
        const auto blob_scrub_range_size = is_deep_scrub ? (HDD_IOPS * (SM_REQUEST_TIMEOUT.count() / 2)) : 2097152;

        // Scrub blob range
        uint64_t blob_start = 0;
        uint64_t blob_end = blob_scrub_range_size;
        uint64_t blob_range_count = 0;
        for (; blob_start <= last_blob_id;
             blob_start = blob_end + 1, blob_end = std::min(blob_end + blob_scrub_range_size, last_blob_id)) {
            ++blob_range_count;
            SCRUBLOGD(pg_id, task_id, "Scrubbing blob range {}: [{}, {}]", blob_range_count, blob_start, blob_end);

            auto blob_req = std::make_shared< blob_scrub_req >(task_id, scrub_ctx->req_id.fetch_add(1), scrub_lsn,
                                                               my_uuid, pg_id, blob_start, blob_end, is_deep_scrub);
            scrub_ctx->reset_for_new_req();
            scrub_ctx->current_req = blob_req;

            // locally scrub this blob range async (runs in parallel with remote requests)
            m_scrub_req_executor->add([this, blob_req, scrub_ctx, pg_id, task_id, is_deep_scrub]() {
                auto scrub_map = local_scrub_blob(blob_req);
                if (!scrub_ctx->add_scrub_map(scrub_map)) {
                    SCRUBLOGE(pg_id, task_id, "failed to add local {} blob scrub map to context!",
                              is_deep_scrub ? "deep" : "shallow");
                } else {
                    SCRUBLOGD(pg_id, task_id, "local {} blob scrub map added!", is_deep_scrub ? "deep" : "shallow");
                }
            });

            // request remote peers to scrub this blob range and wait for responses
            auto flatbuffer = blob_req->build_flat_buffer();
            sisl::io_blob_list_t req_blob_list;
            const auto scrub_type = is_deep_scrub ? SCRUB_TYPE::DEEP_BLOB : SCRUB_TYPE::SHALLOW_BLOB;
            req_blob_list.emplace_back(reinterpret_cast< const uint8_t* >(&scrub_type), sizeof(scrub_type), false);
            req_blob_list.emplace_back(flatbuffer.data(), flatbuffer.size(), false);

            if (!send_scrub_req_and_wait(pg_id, task_id, all_member_peer_ids, my_uuid, pg_repl_dev, req_blob_list,
                                         scrub_ctx, MAX_RETRIES, SM_REQUEST_TIMEOUT, "blob")) {
                SCRUBLOGE(pg_id, task_id, "blob scrub failed or was cancelled");
                return;
            }

            SCRUBLOGD(pg_id, task_id, "Merging blob scrub results for range [{}, {}]", blob_start, blob_end);
            pg_scrub_report->merge(scrub_ctx->peer_sm_map_);
        }
        SCRUBLOGD(pg_id, task_id, "blob scrub completed, total ranges scrubbed: {}", blob_range_count);
    }

    // only if pg is successfully scrubbed, we persist scrub metablk.
    save_scrub_superblk(pg_id, is_deep_scrub, true);
    SCRUBLOGD(pg_id, task_id, "successfully complete {} scrub task!", is_deep_scrub ? "deep" : "shallow");
}

void ScrubManager::add_pg(const pg_id_t pg_id) {
    LOGINFOMOD(scrubmgr, "added new scrub superblock for pg={}", pg_id);
    if (nullptr == m_hs_home_object->get_hs_pg(pg_id)) {
        LOGINFOMOD(scrubmgr, "can not find pg={}!", pg_id);
        return;
    }

    // to avoid create-pg log replay overriding existing scrub superblock, we only create new superblock when there is
    // no existing one
    save_scrub_superblk(pg_id, false, false);
}

void ScrubManager::remove_pg(const pg_id_t pg_id) {
    auto it = m_pg_scrub_sb_map.find(pg_id);
    if (it == m_pg_scrub_sb_map.end()) {
        LOGINFOMOD(scrubmgr, "no scrub superblock found for pg={}, no need to remove", pg_id);
        return;
    }

    LOGINFOMOD(scrubmgr, "removed pg={} in scrub manager!", pg_id);
    cancel_scrub_task(pg_id);
    it->second->destroy();
    m_pg_scrub_ctx_map.erase(pg_id);
    m_pg_scrub_sb_map.erase(it);
}

// this function is called in meta_service thread context and m_pg_scrub_sb_map_mtx
void ScrubManager::on_pg_scrub_meta_blk_found(
    sisl::byte_view const& buf, void* meta_cookie,
    std::vector< homestore::superblk< pg_scrub_superblk > >& stale_pg_scrub_sbs) {
    auto sb = std::make_shared< homestore::superblk< pg_scrub_superblk > >();
    (*sb).load(buf, meta_cookie);
    const auto pg_id = (*sb)->pg_id;

    auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);
    if (!hs_pg) {
        // this is a stale pg scrub superblock, we just log and destroy it.
        LOGINFOMOD(scrubmgr, "can not find pg={}, destroy stale scrub superblock", pg_id);
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
        (*sb)->last_deep_scrub_timestamp = current_time;
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

/* ScrubContext */
bool ScrubManager::PGScrubContext::add_scrub_map(std::shared_ptr< ScrubManager::BaseScrubMap > bsm) {
    if (!bsm) {
        LOGWARNMOD(scrubmgr, "received null scrub map, ignore it!");
        return false;
    }

    const auto& peer_id = bsm->peer_id;
    const auto pg_id = bsm->pg_id;
    if (member_peer_ids_.find(peer_id) == member_peer_ids_.end()) {
        SCRUBLOGD(pg_id, task_id, "received scrub map from peer {} which is not in the pg member list, ignore it!",
                  peer_id);
        return false;
    }

    {
        std::lock_guard lg(mtx_);
        if (!bsm->match(current_req)) {
            SCRUBLOGD(pg_id, task_id, "scrub map does not match up with current req, skip adding");
            return false;
        }
        auto [_, happened] = peer_sm_map_.try_emplace(peer_id, bsm);
        if (!happened) {
            SCRUBLOGD(pg_id, task_id, "already received scrub map from peer {}, ignore the duplicated one!", peer_id);
            return false;
        }
        const auto received_sm_count = peer_sm_map_.size();
        RELEASE_ASSERT(received_sm_count <= member_peer_ids_.size(),
                       "received scrub map count {} should not exceed member peer count {}, something is wrong!",
                       received_sm_count, member_peer_ids_.size());
    }

    // this is a best effort notification, wait might miss this notification and wait for timeout, but it won't cause
    // correctness issue.
    cv_.notify_all();
    SCRUBLOGD(pg_id, task_id, "added scrub map from peer {}, current received scrub map count {}/{}", peer_id,
              peer_sm_map_.size(), member_peer_ids_.size());
    return true;
}

std::vector< peer_id_t > ScrubManager::PGScrubContext::get_peers_to_retry() const {
    std::vector< peer_id_t > peers_to_retry;
    std::lock_guard lg(mtx_);
    for (const auto& peer_id : member_peer_ids_) {
        if (peer_sm_map_.find(peer_id) == peer_sm_map_.end()) { peers_to_retry.push_back(peer_id); }
    }

    return peers_to_retry;
}

// wait until sms from all peers are received, or the task is cancelled, or timeout happens. if timeout happens, caller
// can decide to retry or not.
bool ScrubManager::PGScrubContext::wait_for_all_req_sms(std::chrono::milliseconds timeout) {
    // return true means no need to wait and can proceed, false means timeout and need to retry.
    if (cancelled) {
        LOGINFOMOD(scrubmgr, "scrub task is cancelled, no need to wait for req sms!");
        return true;
    }

    std::unique_lock lock(mtx_);
    if (peer_sm_map_.size() == member_peer_ids_.size()) return true;

    // receiving sm or task cancellation will notify this condition variable.
    cv_.wait_for(lock, timeout, [this] { return cancelled || peer_sm_map_.size() == member_peer_ids_.size(); });

    // if task is cancelled or all the req sms are received, we can proceed, otherwise it means timeout happens and we
    // can retry for pending peers.
    return cancelled || (peer_sm_map_.size() == member_peer_ids_.size());
}

void ScrubManager::PGScrubContext::cancel() {
    cancelled.store(true);
    cv_.notify_all();
}

void ScrubManager::PGScrubContext::reset_for_new_req() {
    std::lock_guard lg(mtx_);
    peer_sm_map_.clear();
    current_req.reset();
}

//=========================== Scrub Request Serialization/Deserialization ===========================//

// base_scrub_req implementations
flatbuffers::DetachedBuffer ScrubManager::base_scrub_req::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder fb_builder;
    // Prepare peer_id as UUID bytes
    std::vector< uint8_t > peer_uuid_bytes(issuer_peer_id.data, issuer_peer_id.data + 16);
    auto scrub_info_off =
        CreateScrubInfo(fb_builder, pg_id, task_id, req_id, scrub_lsn, fb_builder.CreateVector(peer_uuid_bytes));
    auto pg_meta_req_off = CreatePgMetaScrubReq(fb_builder, scrub_info_off);
    FinishSizePrefixedPgMetaScrubReqBuffer(fb_builder, pg_meta_req_off);
    return fb_builder.Release();
}

bool ScrubManager::base_scrub_req::load(uint8_t const* buf_ptr, const uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERROR("Invalid buffer for base_scrub_req deserialization");
        return false;
    }

    auto fb_req = GetSizePrefixedPgMetaScrubReq(buf_ptr);
    if (!fb_req) {
        LOGERROR("Failed to parse base_scrub_req from buffer");
        return false;
    }

    auto scrub_info = fb_req->scrub_info();
    if (!scrub_info) {
        LOGERROR("Missing scrub_info in base_scrub_req");
        return false;
    }

    pg_id = scrub_info->pg_id();
    task_id = scrub_info->task_id();
    req_id = scrub_info->req_id();
    scrub_lsn = scrub_info->scrub_lsn();

    // Load peer_id from issuer_uuid
    auto issuer_uuid_bytes = scrub_info->issuer_uuid();
    if (issuer_uuid_bytes && issuer_uuid_bytes->size() == 16) {
        std::memcpy(issuer_peer_id.data, issuer_uuid_bytes->data(), 16);
    }

    return true;
}

// blob_scrub_req implementations
flatbuffers::DetachedBuffer ScrubManager::blob_scrub_req::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder fb_builder;
    // Prepare peer_id as UUID bytes
    std::vector< uint8_t > peer_uuid_bytes(issuer_peer_id.data, issuer_peer_id.data + 16);
    auto scrub_info_off =
        CreateScrubInfo(fb_builder, pg_id, task_id, req_id, scrub_lsn, fb_builder.CreateVector(peer_uuid_bytes));
    auto blob_req_off = CreateBlobScrubReq(fb_builder, scrub_info_off, start, end, is_deep_scrub_);
    FinishSizePrefixedBlobScrubReqBuffer(fb_builder, blob_req_off);
    return fb_builder.Release();
}

bool ScrubManager::blob_scrub_req::load(uint8_t const* buf_ptr, const uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERROR("Invalid buffer for blob_scrub_req deserialization");
        return false;
    }

    auto fb_req = GetSizePrefixedBlobScrubReq(buf_ptr);
    if (!fb_req) {
        LOGERROR("Failed to parse blob_scrub_req from buffer");
        return false;
    }

    auto scrub_info = fb_req->scrub_info();
    if (!scrub_info) {
        LOGERROR("Missing scrub_info in blob_scrub_req");
        return false;
    }

    pg_id = scrub_info->pg_id();
    task_id = scrub_info->task_id();
    req_id = scrub_info->req_id();
    scrub_lsn = scrub_info->scrub_lsn();

    // Load peer_id from issuer_uuid
    auto issuer_uuid_bytes = scrub_info->issuer_uuid();
    if (issuer_uuid_bytes && issuer_uuid_bytes->size() == 16) {
        std::memcpy(issuer_peer_id.data, issuer_uuid_bytes->data(), 16);
    }

    // Load start and end blob_id
    start = fb_req->start();
    end = fb_req->end();

    is_deep_scrub_ = fb_req->isdeepscrub();

    return true;
}

// shard_scrub_req implementations
flatbuffers::DetachedBuffer ScrubManager::shard_scrub_req::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder fb_builder;
    // Prepare peer_id as UUID bytes
    std::vector< uint8_t > peer_uuid_bytes(issuer_peer_id.data, issuer_peer_id.data + 16);
    auto scrub_info_off =
        CreateScrubInfo(fb_builder, pg_id, task_id, req_id, scrub_lsn, fb_builder.CreateVector(peer_uuid_bytes));
    auto shard_req_off = CreateShardScrubReq(fb_builder, scrub_info_off, start, end, is_deep_scrub_);
    FinishSizePrefixedShardScrubReqBuffer(fb_builder, shard_req_off);
    return fb_builder.Release();
}

bool ScrubManager::shard_scrub_req::load(uint8_t const* buf_ptr, const uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERROR("Invalid buffer for shard_scrub_req deserialization");
        return false;
    }

    auto fb_req = GetSizePrefixedShardScrubReq(buf_ptr);
    if (!fb_req) {
        LOGERROR("Failed to parse shard_scrub_req from buffer");
        return false;
    }

    auto scrub_info = fb_req->scrub_info();
    if (!scrub_info) {
        LOGERROR("Missing scrub_info in shard_scrub_req");
        return false;
    }

    pg_id = scrub_info->pg_id();
    task_id = scrub_info->task_id();
    req_id = scrub_info->req_id();
    scrub_lsn = scrub_info->scrub_lsn();

    // Load peer_id from issuer_uuid
    auto issuer_uuid_bytes = scrub_info->issuer_uuid();
    if (issuer_uuid_bytes && issuer_uuid_bytes->size() == 16) {
        std::memcpy(issuer_peer_id.data, issuer_uuid_bytes->data(), 16);
    }

    start = fb_req->start();
    end = fb_req->end();
    is_deep_scrub_ = fb_req->isdeepscrub();

    return true;
}

//=========================== Scrub Map Serialization/Deserialization ===========================//

// DeepBlobScrubMap implementations
flatbuffers::DetachedBuffer ScrubManager::DeepBlobScrubMap::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder fb_builder;
    // Prepare peer_id as UUID bytes
    std::vector< uint8_t > peer_uuid_bytes(16);
    peer_uuid_bytes.assign(peer_id.begin(), peer_id.end());
    // Create scrub_info
    auto scrub_info_off =
        CreateScrubInfo(fb_builder, pg_id, task_id, req_id, scrub_lsn, fb_builder.CreateVector(peer_uuid_bytes));
    // Create deep blob scrub result entries
    std::vector< flatbuffers::Offset< DeepBlobScrubResultEntry > > result_entries;
    for (const auto& [blob_route, scrub_result_variant] : blobs) {
        auto blob_key_off = CreateBlobKey(fb_builder, blob_route.shard, blob_route.blob);

        homeobject::ScrubValue scrub_value_type;
        flatbuffers::Offset< void > scrub_value_off;
        if (std::holds_alternative< ScrubResult >(scrub_result_variant)) {
            // It's a ScrubResult
            auto result = std::get< ScrubResult >(scrub_result_variant);
            scrub_value_type = homeobject::ScrubValue::ScrubResultValue;
            scrub_value_off = CreateScrubResultValue(fb_builder, result).Union();
        } else {
            // It's a BlobHashArray
            const auto& hash_array = std::get< BlobHashArray >(scrub_result_variant);
            std::vector< uint8_t > hash_vec(hash_array.begin(), hash_array.end());
            scrub_value_type = homeobject::ScrubValue::HashValue;
            scrub_value_off = CreateHashValueDirect(fb_builder, &hash_vec).Union();
        }

        result_entries.push_back(
            CreateDeepBlobScrubResultEntry(fb_builder, blob_key_off, scrub_value_type, scrub_value_off));
    }
    auto results_vec_off = fb_builder.CreateVector(result_entries);
    auto deep_blob_map_off = CreateDeepBlobScrubMap(fb_builder, scrub_info_off, start, end, results_vec_off);
    FinishSizePrefixedDeepBlobScrubMapBuffer(fb_builder, deep_blob_map_off);

    return fb_builder.Release();
}

bool ScrubManager::DeepBlobScrubMap::load(uint8_t const* buf_ptr, const uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERROR("Invalid buffer for DeepBlobScrubMap deserialization");
        return false;
    }

    auto fb_map = GetSizePrefixedDeepBlobScrubMap(buf_ptr);
    if (!fb_map) {
        LOGERROR("Failed to parse DeepBlobScrubMap from buffer");
        return false;
    }

    // Load scrub_info
    auto scrub_info = fb_map->scrub_info();
    if (!scrub_info) {
        LOGERROR("Missing scrub_info in DeepBlobScrubMap");
        return false;
    }

    pg_id = scrub_info->pg_id();
    task_id = scrub_info->task_id();
    req_id = scrub_info->req_id();
    scrub_lsn = scrub_info->scrub_lsn();

    // Load peer_id from issuer_uuid
    auto issuer_uuid_bytes = scrub_info->issuer_uuid();
    if (issuer_uuid_bytes && issuer_uuid_bytes->size() == 16) {
        std::memcpy(peer_id.data, issuer_uuid_bytes->data(), 16);
    }

    // Load start and end blob_id
    start = fb_map->start();
    end = fb_map->end();

    // Load blob results
    blobs.clear();
    auto results = fb_map->deep_blob_scrub_results();
    if (results) {
        for (const auto* entry : *results) {
            if (!entry || !entry->blob_key()) continue;

            BlobRoute blob_route(entry->blob_key()->shard_id(), entry->blob_key()->blob_id());

            auto scrub_value_type = entry->scrub_result_type();
            if (scrub_value_type == ScrubValue::ScrubResultValue) {
                auto result_value = static_cast< const ScrubResultValue* >(entry->scrub_result());
                blobs[blob_route] = result_value->result();
            } else if (scrub_value_type == ScrubValue::HashValue) {
                auto hash_value = static_cast< const HashValue* >(entry->scrub_result());
                BlobHashArray hash_array;
                if (hash_value->hash() && hash_value->hash()->size() <= blob_max_hash_len) {
                    std::memcpy(hash_array.data(), hash_value->hash()->data(), hash_value->hash()->size());
                }
                blobs[blob_route] = hash_array;
            }
        }
    }

    return true;
}

// ShallowBlobScrubMap implementations
flatbuffers::DetachedBuffer ScrubManager::ShallowBlobScrubMap::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder fb_builder;
    // Prepare peer_id as UUID bytes
    std::vector< uint8_t > peer_uuid_bytes(16);
    peer_uuid_bytes.assign(peer_id.begin(), peer_id.end());
    // Create scrub_info
    auto scrub_info_off =
        CreateScrubInfo(fb_builder, pg_id, task_id, req_id, scrub_lsn, fb_builder.CreateVector(peer_uuid_bytes));
    // Create blob keys vector
    std::vector< flatbuffers::Offset< BlobKey > > blob_keys;
    for (const auto& blob_route : blobs) {
        blob_keys.push_back(CreateBlobKey(fb_builder, blob_route.shard, blob_route.blob));
    }
    auto blobs_vec_off = fb_builder.CreateVector(blob_keys);
    auto shallow_blob_map_off = CreateShallowBlobScrubMap(fb_builder, scrub_info_off, start, end, blobs_vec_off);
    FinishSizePrefixedShallowBlobScrubMapBuffer(fb_builder, shallow_blob_map_off);
    return fb_builder.Release();
}

bool ScrubManager::ShallowBlobScrubMap::load(uint8_t const* buf_ptr, const uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERROR("Invalid buffer for ShallowBlobScrubMap deserialization");
        return false;
    }

    auto fb_map = GetSizePrefixedShallowBlobScrubMap(buf_ptr);
    if (!fb_map) {
        LOGERROR("Failed to parse ShallowBlobScrubMap from buffer");
        return false;
    }

    // Load scrub_info
    auto scrub_info = fb_map->scrub_info();
    if (!scrub_info) {
        LOGERROR("Missing scrub_info in ShallowBlobScrubMap");
        return false;
    }

    pg_id = scrub_info->pg_id();
    task_id = scrub_info->task_id();
    req_id = scrub_info->req_id();
    scrub_lsn = scrub_info->scrub_lsn();

    // Load peer_id from issuer_uuid
    auto issuer_uuid_bytes = scrub_info->issuer_uuid();
    if (issuer_uuid_bytes && issuer_uuid_bytes->size() == 16) {
        std::memcpy(peer_id.data, issuer_uuid_bytes->data(), 16);
    }

    // Load start and end blob_id
    start = fb_map->start();
    end = fb_map->end();

    // Load blob routes
    blobs.clear();
    auto blob_keys = fb_map->blobs();
    if (blob_keys) {
        for (const auto* blob_key : *blob_keys) {
            if (!blob_key) continue;
            blobs.insert(BlobRoute(blob_key->shard_id(), blob_key->blob_id()));
        }
    }

    return true;
}

// ShallowShardScrubMap implementations
flatbuffers::DetachedBuffer ScrubManager::ShallowShardScrubMap::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder fb_builder;
    // Prepare peer_id as UUID bytes
    std::vector< uint8_t > peer_uuid_bytes(16);
    peer_uuid_bytes.assign(peer_id.begin(), peer_id.end());
    // Create scrub_info
    auto scrub_info_off =
        CreateScrubInfo(fb_builder, pg_id, task_id, req_id, scrub_lsn, fb_builder.CreateVector(peer_uuid_bytes));
    // Note: ShallowShardScrubMap doesn't have start/end in flatbuffer schema
    // Create shard ids vector
    std::vector< shard_id_t > shard_ids_vec;
    for (const auto& shard_id : shards) {
        shard_ids_vec.push_back(shard_id); // Assuming BlobRoute.shard is the shard_id
    }
    auto shards_vec_off = fb_builder.CreateVector(shard_ids_vec);
    auto shallow_shard_map_off = CreateShallowShardScrubMap(fb_builder, scrub_info_off, 0, 0, shards_vec_off);
    FinishSizePrefixedShallowShardScrubMapBuffer(fb_builder, shallow_shard_map_off);
    return fb_builder.Release();
}

bool ScrubManager::ShallowShardScrubMap::load(uint8_t const* buf_ptr, const uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERROR("Invalid buffer for ShallowShardScrubMap deserialization");
        return false;
    }

    auto fb_map = GetSizePrefixedShallowShardScrubMap(buf_ptr);
    if (!fb_map) {
        LOGERROR("Failed to parse ShallowShardScrubMap from buffer");
        return false;
    }

    // Load scrub_info
    auto scrub_info = fb_map->scrub_info();
    if (!scrub_info) {
        LOGERROR("Missing scrub_info in ShallowShardScrubMap");
        return false;
    }

    pg_id = scrub_info->pg_id();
    task_id = scrub_info->task_id();
    req_id = scrub_info->req_id();
    scrub_lsn = scrub_info->scrub_lsn();

    // Load peer_id from issuer_uuid
    auto issuer_uuid_bytes = scrub_info->issuer_uuid();
    if (issuer_uuid_bytes && issuer_uuid_bytes->size() == 16) {
        std::memcpy(peer_id.data, issuer_uuid_bytes->data(), 16);
    }

    // Load shard ids
    shards.clear();
    auto shard_ids = fb_map->shards();
    if (shard_ids) {
        for (auto shard_id : *shard_ids) {
            shards.insert(shard_id);
        }
    }

    return true;
}

// DeepShardScrubMap implementations
flatbuffers::DetachedBuffer ScrubManager::DeepShardScrubMap::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder fb_builder;
    // Prepare peer_id as UUID bytes
    std::vector< uint8_t > peer_uuid_bytes(16);
    peer_uuid_bytes.assign(peer_id.begin(), peer_id.end());
    // Create scrub_info
    auto scrub_info_off =
        CreateScrubInfo(fb_builder, pg_id, task_id, req_id, scrub_lsn, fb_builder.CreateVector(peer_uuid_bytes));
    // Create shallow shard scrub map (base class data)
    std::vector< uint64_t > shard_ids_vec;
    for (const auto& shard_id : shards) {
        shard_ids_vec.push_back(shard_id);
    }
    auto shards_vec_off = fb_builder.CreateVector(shard_ids_vec);
    auto shallow_shard_map_off = CreateShallowShardScrubMap(fb_builder, scrub_info_off, 0, 0, shards_vec_off);
    // Create problematic shards entries
    std::vector< flatbuffers::Offset< DeepShardScrubResultEntry > > result_entries;
    for (const auto& [shard_id, scrub_result] : problematic_shards) {
        result_entries.push_back(CreateDeepShardScrubResultEntry(fb_builder, shard_id, scrub_result));
    }
    auto results_vec_off = fb_builder.CreateVector(result_entries);
    auto deep_shard_map_off = CreateDeepShardScrubMap(fb_builder, shallow_shard_map_off, results_vec_off);
    FinishSizePrefixedDeepShardScrubMapBuffer(fb_builder, deep_shard_map_off);

    return fb_builder.Release();
}

bool ScrubManager::DeepShardScrubMap::load(uint8_t const* buf_ptr, const uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERROR("Invalid buffer for DeepShardScrubMap deserialization");
        return false;
    }

    auto fb_map = GetSizePrefixedDeepShardScrubMap(buf_ptr);
    if (!fb_map) {
        LOGERROR("Failed to parse DeepShardScrubMap from buffer");
        return false;
    }

    // Load shallow shard scrub map (base class data)
    auto shallow_map = fb_map->shallow_map();
    if (!shallow_map) {
        LOGERROR("Missing shallow_map in DeepShardScrubMap");
        return false;
    }

    // Load scrub_info
    auto scrub_info = shallow_map->scrub_info();
    if (!scrub_info) {
        LOGERROR("Missing scrub_info in DeepShardScrubMap");
        return false;
    }

    pg_id = scrub_info->pg_id();
    task_id = scrub_info->task_id();
    req_id = scrub_info->req_id();
    scrub_lsn = scrub_info->scrub_lsn();

    // Load peer_id from issuer_uuid
    auto issuer_uuid_bytes = scrub_info->issuer_uuid();
    if (issuer_uuid_bytes && issuer_uuid_bytes->size() == 16) {
        std::memcpy(peer_id.data, issuer_uuid_bytes->data(), 16);
    }

    // Load shard ids from shallow map
    shards.clear();
    auto shard_ids = shallow_map->shards();
    if (shard_ids) {
        for (auto shard_id : *shard_ids) {
            shards.insert(shard_id);
        }
    }

    // Load problematic shards
    problematic_shards.clear();
    auto results = fb_map->problematic_shards();
    if (results) {
        for (const auto* entry : *results) {
            if (!entry) continue;
            problematic_shards[entry->shard_id()] = entry->result();
        }
    }

    return true;
}

flatbuffers::DetachedBuffer ScrubManager::PGMetaScrubMap::build_flat_buffer() const {
    flatbuffers::FlatBufferBuilder fb_builder;
    // Prepare peer_id as UUID bytes
    std::vector< uint8_t > peer_uuid_bytes(16);
    peer_uuid_bytes.assign(peer_id.begin(), peer_id.end());
    // Create scrub_info
    auto scrub_info_off =
        CreateScrubInfo(fb_builder, pg_id, task_id, req_id, scrub_lsn, fb_builder.CreateVector(peer_uuid_bytes));
    auto pg_meta_map_off = CreatePGMetaScrubMap(fb_builder, scrub_info_off, pg_meta_scrub_result);
    FinishSizePrefixedPGMetaScrubMapBuffer(fb_builder, pg_meta_map_off);
    return fb_builder.Release();
}

bool ScrubManager::PGMetaScrubMap::load(uint8_t const* buf_ptr, const uint32_t buf_size) {
    if (!buf_ptr || buf_size == 0) {
        LOGERROR("Invalid buffer for PGMetaScrubMap deserialization");
        return false;
    }

    auto fb_map = GetSizePrefixedPGMetaScrubMap(buf_ptr);
    if (!fb_map) {
        LOGERROR("Failed to parse PGMetaScrubMap from buffer");
        return false;
    }

    // Load scrub_info
    auto scrub_info = fb_map->scrub_info();
    if (!scrub_info) {
        LOGERROR("Missing scrub_info in PGMetaScrubMap");
        return false;
    }

    pg_id = scrub_info->pg_id();
    task_id = scrub_info->task_id();
    req_id = scrub_info->req_id();
    scrub_lsn = scrub_info->scrub_lsn();

    // Load peer_id from issuer_uuid
    auto issuer_uuid_bytes = scrub_info->issuer_uuid();
    if (issuer_uuid_bytes && issuer_uuid_bytes->size() == 16) {
        std::memcpy(peer_id.data, issuer_uuid_bytes->data(), 16);
    }

    // Load PG meta scrub result
    pg_meta_scrub_result = fb_map->pg_meta_scrub_result();

    return true;
}

//=========================== Scrub Report Merge Functions ===========================//

void ScrubManager::ShallowScrubReport::print() const {
    std::stringstream ss;
    ss << "ShallowScrubReport for pg=" << pg_id_ << " | ";

    // Report missing shards
    ss << "MissingShards={";
    for (const auto& [peer_id, shard_set] : missing_shard_ids) {
        ss << "peer=" << peer_id << ":[";
        bool first = true;
        for (const auto& shard_id : shard_set) {
            if (!first) ss << ",";
            ss << shard_id;
            first = false;
        }
        ss << "] ";
    }
    ss << "} | ";

    // Report missing blobs
    ss << "MissingBlobs={";
    for (const auto& [peer_id, blob_set] : missing_blobs) {
        ss << "peer=" << peer_id << ":[";
        bool first = true;
        for (const auto& blob_route : blob_set) {
            if (!first) ss << ",";
            ss << fmt::format("{}", blob_route);
            first = false;
        }
        ss << "] ";
    }
    ss << "}";

    LOGINFOMOD(scrubmgr, "{}", ss.str());
}

void ScrubManager::ShallowScrubReport::merge(
    const std::map< peer_id_t, std::shared_ptr< BaseScrubMap > >& peer_sm_map) {
    if (peer_sm_map.empty()) {
        LOGWARNMOD(scrubmgr, "[pg={}] No scrub maps to merge", pg_id_);
        return;
    }

    // Collect all blobs and shards from all peers
    std::map< BlobRoute, std::set< peer_id_t > > blob_peers_map;   // blob -> set of peers that have it
    std::map< shard_id_t, std::set< peer_id_t > > shard_peers_map; // shard -> set of peers that have it

    for (const auto& [peer_id, scrub_map] : peer_sm_map) {
        if (!scrub_map) {
            LOGWARNMOD(scrubmgr, "[pg={}] Null scrub map from peer {}", pg_id_, peer_id);
            continue;
        }

        // Handle ShallowBlobScrubMap
        auto shallow_blob_map = std::dynamic_pointer_cast< ShallowBlobScrubMap >(scrub_map);
        if (shallow_blob_map) {
            for (const auto& blob_route : shallow_blob_map->blobs) {
                blob_peers_map[blob_route].insert(peer_id);
            }
            continue;
        }

        // Handle DeepBlobScrubMap (also contains blob list)
        auto deep_blob_map = std::dynamic_pointer_cast< DeepBlobScrubMap >(scrub_map);
        if (deep_blob_map) {
            for (const auto& [blob_route, _] : deep_blob_map->blobs) {
                blob_peers_map[blob_route].insert(peer_id);
            }
            continue;
        }

        // Handle ShallowShardScrubMap
        auto shallow_shard_map = std::dynamic_pointer_cast< ShallowShardScrubMap >(scrub_map);
        if (shallow_shard_map) {
            for (const auto& shard_id : shallow_shard_map->shards) {
                shard_peers_map[shard_id].insert(peer_id);
            }
            continue;
        }

        // Handle DeepShardScrubMap (inherits from ShallowShardScrubMap)
        auto deep_shard_map = std::dynamic_pointer_cast< DeepShardScrubMap >(scrub_map);
        if (deep_shard_map) {
            for (const auto& shard_id : deep_shard_map->shards) {
                shard_peers_map[shard_id].insert(peer_id);
            }
            continue;
        }
    }

    // Determine which blobs are missing on which peers
    // A blob is considered missing on a peer if it appears on other peers but not this one
    for (const auto& [blob_route, peer_set] : blob_peers_map) {
        // If not all peers have this blob, some are missing it
        if (peer_set.size() < peer_sm_map.size()) {
            for (const auto& [peer_id, _] : peer_sm_map) {
                if (peer_set.find(peer_id) == peer_set.end()) {
                    // This peer is missing the blob
                    add_missing_blob(blob_route, peer_id);
                }
            }
        }
    }

    // Determine which shards are missing on which peers
    for (const auto& [shard_id, peer_set] : shard_peers_map) {
        if (peer_set.size() < peer_sm_map.size()) {
            for (const auto& [peer_id, _] : peer_sm_map) {
                if (peer_set.find(peer_id) == peer_set.end()) {
                    // This peer is missing the shard
                    add_missing_shard(shard_id, peer_id);
                }
            }
        }
    }

    // Count total missing blobs and shards across all peers
    size_t total_missing_blobs = 0;
    for (const auto& [peer_id, blobs] : missing_blobs) {
        total_missing_blobs += blobs.size();
    }
    size_t total_missing_shards = 0;
    for (const auto& [peer_id, shards] : missing_shard_ids) {
        total_missing_shards += shards.size();
    }
    LOGINFOMOD(scrubmgr,
               "[pg={}] Shallow scrub merge completed: {} peers with missing blobs (total {} blobs), {} peers with "
               "missing shards (total {} shards)",
               pg_id_, missing_blobs.size(), total_missing_blobs, missing_shard_ids.size(), total_missing_shards);
}

void ScrubManager::DeepScrubReport::print() const {
    std::stringstream ss;
    ss << "DeepScrubReport for pg=" << pg_id_ << " | ";

    // Report missing shards (from ShallowScrubReport)
    ss << "MissingShards={";
    for (const auto& [peer_id, shard_set] : missing_shard_ids) {
        ss << "peer=" << peer_id << ":[";
        bool first = true;
        for (const auto& shard_id : shard_set) {
            if (!first) ss << ",";
            ss << shard_id;
            first = false;
        }
        ss << "] ";
    }
    ss << "} | ";

    // Report missing blobs (from ShallowScrubReport)
    ss << "MissingBlobs={";
    for (const auto& [peer_id, blob_set] : missing_blobs) {
        ss << "peer=" << peer_id << ":[";
        bool first = true;
        for (const auto& blob_route : blob_set) {
            if (!first) ss << ",";
            ss << fmt::format("{}", blob_route);
            first = false;
        }
        ss << "] ";
    }
    ss << "} | ";

    // Report corrupted blobs
    ss << "CorruptedBlobs={";
    for (const auto& [peer_id, blob_map] : corrupted_blobs) {
        ss << "peer=" << peer_id << ":[";
        bool first = true;
        for (const auto& [blob_route, scrub_result] : blob_map) {
            if (!first) ss << ",";
            ss << fmt::format("{}", blob_route) << "(" << SCRUB_RESULT_STRING(scrub_result) << ")";
            first = false;
        }
        ss << "] ";
    }
    ss << "} | ";

    // Report corrupted shards
    ss << "CorruptedShards={";
    for (const auto& [peer_id, shard_map] : corrupted_shards) {
        ss << "peer=" << peer_id << ":[";
        bool first = true;
        for (const auto& [shard_id, scrub_result] : shard_map) {
            if (!first) ss << ",";
            ss << shard_id << "(" << SCRUB_RESULT_STRING(scrub_result) << ")";
            first = false;
        }
        ss << "] ";
    }
    ss << "} | ";

    // Report inconsistent blobs (different hashes across replicas)
    ss << "InconsistentBlobs={";
    for (const auto& [blob_route, peer_hash_map] : inconsistent_blobs) {
        ss << fmt::format("{}", blob_route);
        bool first = true;
        for (const auto& [peer_id, hash] : peer_hash_map) {
            if (!first) ss << ",";
            ss << "peer=" << peer_id << "(hash=";
            // Print first 8 bytes of hash for brevity
            for (size_t i = 0; i < std::min(size_t(8), hash.size()); ++i) {
                ss << fmt::format("{:02x}", hash[i]);
            }
            ss << ")";
            first = false;
        }
        ss << "] ";
    }
    ss << "} | ";

    // Report corrupted PG metadata
    ss << "CorruptedPGMeta={";
    bool first = true;
    for (const auto& [peer_id, scrub_result] : corrupted_pg_metas) {
        if (!first) ss << ",";
        ss << "peer=" << peer_id << "(" << SCRUB_RESULT_STRING(scrub_result) << ")";
        first = false;
    }
    ss << "}";

    LOGINFOMOD(scrubmgr, "{}", ss.str());
}

void ScrubManager::DeepScrubReport::merge(const std::map< peer_id_t, std::shared_ptr< BaseScrubMap > >& peer_sm_map) {
    // First do shallow merge to find missing blobs/shards
    ShallowScrubReport::merge(peer_sm_map);

    if (peer_sm_map.empty()) { return; }

    // Now do deep scrub specific comparisons
    std::map< BlobRoute, std::map< peer_id_t, std::variant< ScrubResult, BlobHashArray > > > blob_results_map;
    std::map< shard_id_t, std::map< peer_id_t, ScrubResult > > shard_results_map;
    std::map< peer_id_t, ScrubResult > pg_meta_results_map;

    // Collect all deep scrub results
    for (const auto& [peer_id, scrub_map] : peer_sm_map) {
        if (!scrub_map) continue;

        // Handle DeepBlobScrubMap
        auto deep_blob_map = std::dynamic_pointer_cast< DeepBlobScrubMap >(scrub_map);
        if (deep_blob_map) {
            for (const auto& [blob_route, result_variant] : deep_blob_map->blobs) {
                blob_results_map[blob_route][peer_id] = result_variant;
            }
            continue;
        }

        // Handle DeepShardScrubMap
        auto deep_shard_map = std::dynamic_pointer_cast< DeepShardScrubMap >(scrub_map);
        if (deep_shard_map) {
            for (const auto& [shard_id, scrub_result] : deep_shard_map->problematic_shards) {
                shard_results_map[shard_id][peer_id] = scrub_result;
            }
            continue;
        }

        // Handle PGMetaScrubMap
        auto pg_meta_map = std::dynamic_pointer_cast< PGMetaScrubMap >(scrub_map);
        if (pg_meta_map) {
            if (pg_meta_map->pg_meta_scrub_result != ScrubResult::NONE) {
                pg_meta_results_map[peer_id] = pg_meta_map->pg_meta_scrub_result;
            }
            continue;
        }
    }

    // Analyze blob results
    for (const auto& [blob_route, peer_results] : blob_results_map) {
        std::map< peer_id_t, BlobHashArray > hash_map;
        bool has_error = false;

        for (const auto& [peer_id, result_variant] : peer_results) {
            if (std::holds_alternative< ScrubResult >(result_variant)) {
                // This peer has an error (IO_ERROR, MISMATCH, NOT_FOUND)
                auto scrub_result = std::get< ScrubResult >(result_variant);
                add_corrupted_blob(peer_id, blob_route, scrub_result);
                has_error = true;
            } else {
                // This peer has a valid hash
                hash_map[peer_id] = std::get< BlobHashArray >(result_variant);
            }
        }

        // Check for hash inconsistencies among peers with valid hashes
        if (!has_error && hash_map.size() > 1) {
            // Compare all hashes
            BlobHashArray reference_hash;
            peer_id_t reference_peer;
            bool first = true;
            bool hashes_consistent = true;

            for (const auto& [peer_id, hash] : hash_map) {
                if (first) {
                    reference_hash = hash;
                    reference_peer = peer_id;
                    first = false;
                } else {
                    if (std::memcmp(reference_hash.data(), hash.data(), blob_max_hash_len) != 0) {
                        hashes_consistent = false;
                        break;
                    }
                }
            }

            // If hashes are inconsistent, record all of them
            if (!hashes_consistent) {
                for (const auto& [peer_id, hash] : hash_map) {
                    add_inconsistent_blob(blob_route, peer_id, hash);
                }
            }
        }
    }

    // Analyze shard results
    for (const auto& [shard_id, peer_results] : shard_results_map) {
        for (const auto& [peer_id, scrub_result] : peer_results) {
            if (scrub_result != ScrubResult::NONE) { add_corrupted_shard(peer_id, shard_id, scrub_result); }
        }
    }

    // Record PG meta errors
    for (const auto& [peer_id, scrub_result] : pg_meta_results_map) {
        add_corrupted_pg_meta(peer_id, scrub_result);
    }

    LOGINFOMOD(scrubmgr,
               "[pg={}] Deep scrub merge completed: {} corrupted blobs, {} inconsistent blobs, "
               "{} corrupted shards, {} corrupted pg metas",
               pg_id_, corrupted_blobs.size(), inconsistent_blobs.size(), corrupted_shards.size(),
               corrupted_pg_metas.size());
}

} // namespace homeobject