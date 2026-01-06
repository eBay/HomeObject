#include <homestore/btree/btree_req.hpp>
#include <homestore/btree/btree_kv.hpp>

#include "hs_homeobject.hpp"
namespace homeobject {

SISL_LOGGING_DECL(gcmgr)

#define NO_SHARD_ID UINT64_MAX
#define RECOVERD_GC_TASK_ID 0

#define GCLOG(level, gc_task_id, pg_id, shard_id, msg, ...)                                                            \
    LOG##level##MOD(gcmgr, "[gc_task_id={}, pg_id={}, shard_id=0x{:x}] " msg, gc_task_id, pg_id, shard_id,             \
                    ##__VA_ARGS__)

#define GCLOGT(gc_task_id, pg_id, shard_id, msg, ...) GCLOG(TRACE, gc_task_id, pg_id, shard_id, msg, ##__VA_ARGS__)
#define GCLOGD(gc_task_id, pg_id, shard_id, msg, ...) GCLOG(DEBUG, gc_task_id, pg_id, shard_id, msg, ##__VA_ARGS__)
#define GCLOGI(gc_task_id, pg_id, shard_id, msg, ...) GCLOG(INFO, gc_task_id, pg_id, shard_id, msg, ##__VA_ARGS__)
#define GCLOGW(gc_task_id, pg_id, shard_id, msg, ...) GCLOG(WARN, gc_task_id, pg_id, shard_id, msg, ##__VA_ARGS__)
#define GCLOGE(gc_task_id, pg_id, shard_id, msg, ...) GCLOG(ERROR, gc_task_id, pg_id, shard_id, msg, ##__VA_ARGS__)
#define GCLOGC(gc_task_id, pg_id, shard_id, msg, ...) GCLOG(CRITICAL, gc_task_id, pg_id, shard_id, msg, ##__VA_ARGS__)

/* GCManager */

GCManager::GCManager(HSHomeObject* homeobject) :
        m_chunk_selector{homeobject->chunk_selector()}, m_hs_home_object{homeobject} {
    homestore::meta_service().register_handler(
        GCManager::_gc_actor_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_gc_actor_meta_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        nullptr, true);

    homestore::meta_service().register_handler(
        GCManager::_gc_reserved_chunk_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_reserved_chunk_meta_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        [this](bool success) {
            RELEASE_ASSERT(success, "Failed to recover all reserved chunk!!!");
            // we need to guarantee that pg meta blk is recovered before we start recover reserved chunk
            m_chunk_selector->build_pdev_available_chunk_heap();
        },
        true);

    homestore::meta_service().register_handler(
        GCManager::_gc_task_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_gc_task_meta_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        nullptr, true);

    auto gcmgr_log_level = sisl::logging::GetModuleLogLevel("gcmgr");
    if (gcmgr_log_level == spdlog::level::level_enum::err) {
        // if we do not set gcmgr log level by start command line(for exampel, --log_mods gcmgr:info), then set it to
        // debug level by default
        sisl::logging::SetModuleLogLevel("gcmgr", spdlog::level::debug);
    }
}

void GCManager::on_gc_task_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie) {
    // this will only be called in the metablk#readsub, so it is guaranteed to be called sequentially

    // here, we are under the protection of the lock of metaservice. however, we will also try to update pg and shard
    // metablk and then destroy the gc_task_sb, which will also try to acquire the lock of metaservice, as a result, a
    // dead lock will happen. so here we will handle all the gc tasks after read all the their metablks
    m_recovered_gc_tasks.emplace_back(GCManager::_gc_task_meta_name);
    m_recovered_gc_tasks.back().load(buf, meta_cookie);
}

void GCManager::handle_all_recovered_gc_tasks() {
    for (auto& recovered_gc_task : m_recovered_gc_tasks) {
        // if a gc_task_super blk is found, we can make sure that all the valid data in move_from_chunk has been copied
        // to move_to_chunk, and all the blob -> (new pba) have been written to the gc index table. Now, what we need to
        // do is just updating blob indexes in pg index table according to the blob indexes in gc index table.

        // pg_index_table: [pg_id, shard_id, blob_id] -> old pba
        // gc_index_table: [move_to_chunk_id, pg_id, shard_id, blob_id] -> new pba

        // we need to find all keys with the prefix of move_to_chunk_id in gc index table, and update the corrsponding
        // keys(same pg_id + shard_id + blob_id) in pg index table with the new pba.

        auto pdev_id = m_chunk_selector->get_extend_vchunk(recovered_gc_task->move_from_chunk)->get_pdev_id();
        auto gc_actor = get_pdev_gc_actor(pdev_id);
        RELEASE_ASSERT(gc_actor, "can not get gc actor for pdev {}!", pdev_id);
        gc_actor->handle_recovered_gc_task(recovered_gc_task);
    }
    m_recovered_gc_tasks.clear();
}

void GCManager::on_gc_actor_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie) {
    m_gc_actor_sbs.emplace_back(GCManager::_gc_actor_meta_name);
    auto& gc_actor_sb = m_gc_actor_sbs.back();
    gc_actor_sb.load(buf, meta_cookie);
    auto pdev_id = gc_actor_sb->pdev_id;

    // create a gc actor for this pdev if not exists
    auto gc_actor = try_create_pdev_gc_actor(pdev_id, gc_actor_sb);
    RELEASE_ASSERT(gc_actor, "can not get gc actor for pdev {}!", pdev_id);
}

void GCManager::on_reserved_chunk_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie) {
    homestore::superblk< GCManager::gc_reserved_chunk_superblk > reserved_chunk_sb(
        GCManager::_gc_reserved_chunk_meta_name);
    auto chunk_id = reserved_chunk_sb.load(buf, meta_cookie)->chunk_id;
    auto EXVchunk = m_chunk_selector->get_extend_vchunk(chunk_id);
    if (EXVchunk == nullptr) {
        LOGWARNMOD(gcmgr,
                   "the disk for chunk {} is not found, probably lost, skip recovering gc mateblk for this chunk!",
                   chunk_id);
    } else {
        auto pdev_id = EXVchunk->get_pdev_id();
        auto gc_actor = get_pdev_gc_actor(pdev_id);
        RELEASE_ASSERT(gc_actor, "can not get gc actor for pdev {}!", pdev_id);
        gc_actor->add_reserved_chunk(std::move(reserved_chunk_sb));
    }
}

GCManager::~GCManager() { stop(); }

void GCManager::start() {
    for (const auto& [pdev_id, gc_actor] : m_pdev_gc_actors) {
        gc_actor->start();
        LOGINFOMOD(gcmgr, "start gc actor for pdev={}", pdev_id);
    }
    start_gc_scan_timer();
}

void GCManager::start_gc_scan_timer() {
    const auto gc_scan_interval_sec = HS_BACKEND_DYNAMIC_CONFIG(gc_scan_interval_sec);

    // the initial idea here is that we want gc timer to run in a reactor that not shared with other fibers that
    // probably hold a lock before fiber switching(for example , cp io reactor) to prevent some potential dead lock
    // issue. here, we make gc timer run in a random worker reactor, which is created by iomanager itself(not created
    // by user). worker reactor is mainly used for handling disk io(async_write/async_read, sync_write/sync_read), and
    // they are all handled in the main_fiber. moreover, the timer assigned to the worker reactor is also handled in the
    // main_fiber. as a result, only main fiber works for all the io and timer and no fiber swithing happens, unless
    // user explicitly selectes a sync_io_fiber of a worker reactor to do some io operation, which does not happens in
    // homestore backend and homeobject code base.

    // theoretically， there is no way to make a user created reactor totally pure, because even if we create a separate
    // user reactor for gc timer, if some one user all_user to do io operation or set timer, then this reactor will be
    // polluted by other fibers or timer
    iomanager.run_on_wait(iomgr::reactor_regex::random_worker, [&]() {
        m_gc_timer_fiber = iomanager.iofiber_self();
        m_gc_timer_hdl = iomanager.schedule_thread_timer(gc_scan_interval_sec * 1000 * 1000 * 1000, true,
                                                         nullptr /*cookie*/, [this](void*) { scan_chunks_for_gc(); });
    });

    LOGINFOMOD(gcmgr, "gc scheduler timer has started, interval is set to {} seconds", gc_scan_interval_sec);
}

void GCManager::stop_gc_scan_timer() {
    if (m_gc_timer_hdl == iomgr::null_timer_handle) {
        LOGWARNMOD(gcmgr, "gc scheduler timer is not running, no need to stop it");
        return;
    }
    RELEASE_ASSERT(m_gc_timer_fiber,
                   "m_gc_timer_hdl is not null_timer_handle, but m_gc_timer_fiber is null, fatal error!");

    LOGINFOMOD(gcmgr, "stop gc scheduler timer");
    iomanager.run_on_wait(m_gc_timer_fiber, [&]() {
        iomanager.cancel_timer(m_gc_timer_hdl, true);
        m_gc_timer_hdl = iomgr::null_timer_handle;
    });
    m_gc_timer_fiber = nullptr;
}

void GCManager::stop() {
    stop_gc_scan_timer();

    for (const auto& [pdev_id, gc_actor] : m_pdev_gc_actors) {
        gc_actor->stop();
        LOGINFOMOD(gcmgr, "stop gc actor for pdev={}", pdev_id);
    }
}

folly::SemiFuture< bool > GCManager::submit_gc_task(task_priority priority, chunk_id_t chunk_id) {
    auto ex_vchunk = m_chunk_selector->get_extend_vchunk(chunk_id);
    if (ex_vchunk == nullptr) {
        LOGERRORMOD(gcmgr, "chunk {} not found when submit gc task!", chunk_id);
        return folly::makeFuture< bool >(false);
    }

    // if the chunk has no garbage to be reclaimed, we don`t need to gc it , return true directly
    const auto defrag_blk_num = ex_vchunk->get_defrag_nblks();
    if (!defrag_blk_num) {
        LOGERRORMOD(gcmgr, "chunk {} has no garbage to be reclaimed, skip gc for this chunk!", chunk_id);
        return folly::makeFuture< bool >(true);
    }

    auto pdev_id = ex_vchunk->get_pdev_id();
    auto it = m_pdev_gc_actors.find(pdev_id);
    if (it == m_pdev_gc_actors.end()) {
        LOGINFOMOD(gcmgr, "pdev gc actor not found for pdev_id={}, chunk={}", pdev_id, chunk_id);
        return folly::makeFuture< bool >(false);
    }
    auto& actor = it->second;
    return actor->add_gc_task(static_cast< uint8_t >(priority), chunk_id);
}

std::shared_ptr< GCManager::pdev_gc_actor >
GCManager::try_create_pdev_gc_actor(uint32_t pdev_id,
                                    const homestore::superblk< GCManager::gc_actor_superblk >& gc_actor_sb) {
    auto const [it, happened] = m_pdev_gc_actors.try_emplace(
        pdev_id, std::make_shared< pdev_gc_actor >(gc_actor_sb, m_chunk_selector, m_hs_home_object));
    RELEASE_ASSERT((it != m_pdev_gc_actors.end()), "Unexpected error in m_pdev_gc_actors!!!");
    if (happened) {
        LOGINFOMOD(gcmgr, "create new gc actor for pdev_id: {}", pdev_id);
    } else {
        LOGINFOMOD(gcmgr, "pdev gc actor already exists for pdev_id: {}", pdev_id);
    }
    return it->second;
}

std::shared_ptr< GCManager::pdev_gc_actor > GCManager::get_pdev_gc_actor(uint32_t pdev_id) {
    auto it = m_pdev_gc_actors.find(pdev_id);
    if (it == m_pdev_gc_actors.end()) {
        LOGERRORMOD(gcmgr, "pdev gc actor not found for pdev_id: {}", pdev_id);
        return nullptr;
    }
    return it->second;
}

bool GCManager::is_eligible_for_gc(chunk_id_t chunk_id) {
    auto chunk = m_chunk_selector->get_extend_vchunk(chunk_id);
    const auto defrag_blk_num = chunk->get_defrag_nblks();
    if (!defrag_blk_num) { return false; }

    // 1 if the chunk state is inuse, it is occupied by a open shard, so it can not be selected and we don't need gc it.
    // 2 if the chunk state is gc, it means this chunk is being gc, or this is a reserved chunk, so we don't need gc it.
    if (chunk->m_state != ChunkState::AVAILABLE) {
        LOGDEBUGMOD(gcmgr, "chunk_id={} state is {}, not eligible for gc", chunk_id, chunk->m_state)
        return false;
    }

    const auto total_blk_num = chunk->get_total_blks();
    const auto gc_garbage_rate_threshold = HS_BACKEND_DYNAMIC_CONFIG(gc_garbage_rate_threshold);
    bool should_gc = 100 * defrag_blk_num > total_blk_num * gc_garbage_rate_threshold;

    LOGDEBUGMOD(gcmgr,
                "gc scan chunk_id={}, use_blks={}, available_blks={}, total_blks={}, defrag_blks={}, should_gc={}",
                chunk_id, chunk->get_used_blks(), chunk->available_blks(), total_blk_num, defrag_blk_num, should_gc);

    return should_gc;
}

void GCManager::scan_chunks_for_gc() {
    const auto reserved_chunk_num_per_pdev = HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev);
    const auto reserved_chunk_num_per_pdev_for_egc = HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev_for_egc);

    for (const auto& [pdev_id, chunks] : m_chunk_selector->get_pdev_chunks()) {
        auto max_task_num = 2 * (reserved_chunk_num_per_pdev - reserved_chunk_num_per_pdev_for_egc);
        auto it = m_pdev_gc_actors.find(pdev_id);
        RELEASE_ASSERT(it != m_pdev_gc_actors.end(), "can not find gc actor for pdev_id {} when scanning chunks for gc",
                       pdev_id);
        auto& actor = it->second;

        for (const auto& chunk_id : chunks) {
            if (is_eligible_for_gc(chunk_id)) {
                auto future = actor->add_gc_task(static_cast< uint8_t >(task_priority::normal), chunk_id);
                if (future.isReady()) {
                    if (future.value()) {
                        LOGINFOMOD(
                            gcmgr,
                            "gc task for chunk_id={} on pdev_id={} has been submitted and successfully completed "
                            "shortly",
                            chunk_id, pdev_id);
                    } else {
                        LOGWARNMOD(gcmgr,
                                   "got false after add_gc_task for chunk_id={} on pdev_id={}, it means we cannot mark "
                                   "this chunk to gc state(there is an open shard on this chunk ATM) or this task is "
                                   "executed shortly but fails(fail to copy data or update gc index table) ",
                                   chunk_id, pdev_id);
                    }
                } else if (0 == --max_task_num) {
                    LOGINFOMOD(gcmgr, "reached max gc task limit for pdev_id={}, stopping further gc task submissions",
                               pdev_id);
                    break;
                }
            }
        }
    }
}

/* pdev_gc_actor */

GCManager::pdev_gc_actor::pdev_gc_actor(const homestore::superblk< GCManager::gc_actor_superblk >& gc_actor_sb,
                                        std::shared_ptr< HeapChunkSelector > chunk_selector, HSHomeObject* homeobject) :
        m_pdev_id{gc_actor_sb->pdev_id},
        m_chunk_selector{chunk_selector},
        m_reserved_chunk_queue{HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev)},
        m_index_table{homeobject->get_gc_index_table(boost::uuids::to_string(gc_actor_sb->index_table_uuid))},
        m_hs_home_object{homeobject},
        m_enable_read_verify{HS_BACKEND_DYNAMIC_CONFIG(gc_enable_read_verify)},
        metrics_{*this} {

    RELEASE_ASSERT(m_index_table, "index_table for a gc_actor should not be nullptr!!!");

    durable_entities_.success_gc_task_count = gc_actor_sb->success_gc_task_count;
    durable_entities_.success_egc_task_count = gc_actor_sb->success_egc_task_count;
    durable_entities_.failed_gc_task_count = gc_actor_sb->failed_gc_task_count;
    durable_entities_.failed_egc_task_count = gc_actor_sb->failed_egc_task_count;
    durable_entities_.total_reclaimed_blk_count_by_gc = gc_actor_sb->total_reclaimed_blk_count_by_gc;
    durable_entities_.total_reclaimed_blk_count_by_egc = gc_actor_sb->total_reclaimed_blk_count_by_egc;
}

void GCManager::pdev_gc_actor::start() {
    bool stopped = true;
    if (!m_is_stopped.compare_exchange_strong(stopped, false, std::memory_order_release, std::memory_order_relaxed)) {
        LOGERRORMOD(gcmgr, "pdev gc actor for pdev_id={} is already started, no need to start again!", m_pdev_id);
        return;
    }

    const auto reserved_chunk_num_per_pdev = HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev);
    const auto reserved_chunk_num_per_pdev_for_egc = HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev_for_egc);

    RELEASE_ASSERT(reserved_chunk_num_per_pdev > reserved_chunk_num_per_pdev_for_egc,
                   "reserved chunk number {} per pdev should be greater than {}", reserved_chunk_num_per_pdev,
                   reserved_chunk_num_per_pdev_for_egc);
    // thread number is the same as reserved chunk, which can make sure every gc thread can take a reserved chunk
    // for gc
    m_gc_executor = std::make_shared< folly::IOThreadPoolExecutor >(reserved_chunk_num_per_pdev -
                                                                    reserved_chunk_num_per_pdev_for_egc);

    m_egc_executor = std::make_shared< folly::IOThreadPoolExecutor >(reserved_chunk_num_per_pdev_for_egc);

    LOGINFOMOD(gcmgr, "pdev gc actor for pdev_id={} has started", m_pdev_id);
}

void GCManager::pdev_gc_actor::stop() {
    bool stopped = false;
    if (!m_is_stopped.compare_exchange_strong(stopped, true, std::memory_order_release, std::memory_order_relaxed)) {
        LOGWARNMOD(gcmgr, "pdev gc actor for pdev_id={} is already stopped, no need to stop again!", m_pdev_id);
        return;
    }
    m_gc_executor->stop();
    m_gc_executor.reset();

    m_egc_executor->stop();
    m_egc_executor.reset();
    LOGINFOMOD(gcmgr, "pdev gc actor for pdev_id={} has stopped", m_pdev_id);
}

void GCManager::pdev_gc_actor::add_reserved_chunk(
    homestore::superblk< GCManager::gc_reserved_chunk_superblk > reserved_chunk_sb) {
    auto chunk_id = reserved_chunk_sb->chunk_id;
    // mark a reserved chunk as gc state, so that it will not be selected as a gc candidate
    m_chunk_selector->try_mark_chunk_to_gc_state(chunk_id, true /* force */);
    m_reserved_chunks.emplace_back(std::move(reserved_chunk_sb));
    m_reserved_chunk_queue.blockingWrite(chunk_id);
    LOGDEBUGMOD(gcmgr, "chunk_id={} is added to reserved chunk queue", chunk_id);
}

folly::SemiFuture< bool > GCManager::pdev_gc_actor::add_gc_task(uint8_t priority, chunk_id_t move_from_chunk) {
    if (m_is_stopped.load()) {
        LOGWARNMOD(gcmgr, "pdev gc actor for pdev_id={} is not started yet or already stopped, cannot add gc task!",
                   m_pdev_id);
        return folly::makeSemiFuture< bool >(false);
    }
    auto EXvchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);
    // it does not belong to any pg, so we don't need to gc it.
    if (!EXvchunk->m_pg_id.has_value()) {
        LOGDEBUGMOD(gcmgr, "chunk_id={} belongs to no pg, not eligible for gc", move_from_chunk)
        return folly::makeSemiFuture< bool >(false);
    }

    const auto pg_id = EXvchunk->m_pg_id.value();

    m_hs_home_object->gc_manager()->incr_pg_pending_gc_task(pg_id);

    if (!m_hs_home_object->can_chunks_in_pg_be_gc(pg_id)) {
        LOGDEBUGMOD(gcmgr, "chunk_id={} belongs to pg {}, which is not eligible for gc at this moment!",
                    move_from_chunk, pg_id)
        m_hs_home_object->gc_manager()->decr_pg_pending_gc_task(pg_id);
        return folly::makeSemiFuture< bool >(false);
    }

    if (m_chunk_selector->try_mark_chunk_to_gc_state(move_from_chunk,
                                                     priority == static_cast< uint8_t >(task_priority::emergent))) {
        auto [promise, future] = folly::makePromiseContract< bool >();
        const auto gc_task_id = GCManager::_gc_task_id.fetch_add(1);

        if (sisl_unlikely(priority == static_cast< uint8_t >(task_priority::emergent))) {
            m_egc_executor->add([this, gc_task_id, priority, move_from_chunk, promise = std::move(promise)]() mutable {
                LOGDEBUGMOD(gcmgr, "start emergent gc task : move_from_chunk_id={}, priority={}", move_from_chunk,
                            priority);
                process_gc_task(move_from_chunk, priority, std::move(promise), gc_task_id);
            });
        } else {
            m_gc_executor->add([this, gc_task_id, priority, move_from_chunk, promise = std::move(promise)]() mutable {
                LOGDEBUGMOD(gcmgr, "start gc task : move_from_chunk_id={}, priority={}", move_from_chunk, priority);
                process_gc_task(move_from_chunk, priority, std::move(promise), gc_task_id);
            });
        }

        return std::move(future);
    }

    LOGWARNMOD(gcmgr, "fail to submit gc task for chunk_id={}, priority={}", move_from_chunk, priority);
    m_hs_home_object->gc_manager()->decr_pg_pending_gc_task(pg_id);
    return folly::makeSemiFuture< bool >(false);
}

void GCManager::drain_pg_pending_gc_task(const pg_id_t pg_id) {
    while (true) {
        uint64_t pending_gc_task_num{0};
        {
            std::unique_lock lock(m_pending_gc_task_mtx);
            pending_gc_task_num = m_pending_gc_task_num_per_pg.try_emplace(pg_id, 0).first->second.load();
        }

        if (pending_gc_task_num) {
            LOGDEBUGMOD(gcmgr, "{} pending gc tasks to be completed for pg={}, wait for 2 seconds!",
                        pending_gc_task_num, pg_id);
        } else {
            break;
        }
        // wait until all the pending gc tasks for this pg are completed
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    LOGDEBUGMOD(gcmgr, "all pending gc tasks for pg_id={} are completed", pg_id);
}

void GCManager::decr_pg_pending_gc_task(const pg_id_t pg_id) {
    std::unique_lock lock(m_pending_gc_task_mtx);
    auto& pending_gc_task_num = m_pending_gc_task_num_per_pg.try_emplace(pg_id, 0).first->second;
    if (pending_gc_task_num.load()) {
        // TODO::avoid overflow here.
        --pending_gc_task_num;
        LOGDEBUGMOD(gcmgr, "decrease pending gc task num for pg_id={}, now it is {}", pg_id,
                    pending_gc_task_num.load());
        return;
    }
    LOGDEBUGMOD(gcmgr, "pending gc task num for pg_id={} is already 0, no need to decrease it", pg_id);
}

void GCManager::incr_pg_pending_gc_task(const pg_id_t pg_id) {
    std::unique_lock lock(m_pending_gc_task_mtx);
    auto& pending_gc_task_num = m_pending_gc_task_num_per_pg.try_emplace(pg_id, 0).first->second;
    ++pending_gc_task_num;
    LOGDEBUGMOD(gcmgr, "increase pending gc task num for pg_id={}, now it is {}", pg_id, pending_gc_task_num.load());
}

// this method is expected to be called sequentially when replaying metablk, so we don't need to worry about the
// concurrency issue.
void GCManager::pdev_gc_actor::handle_recovered_gc_task(
    homestore::superblk< GCManager::gc_task_superblk >& gc_task_sb) {

    const chunk_id_t move_from_chunk = gc_task_sb->move_from_chunk;
    const chunk_id_t move_to_chunk = gc_task_sb->move_to_chunk;
    const uint8_t priority = gc_task_sb->priority;

    LOGDEBUGMOD(gcmgr, "start handling recovered gc task: move_from_chunk_id={}, move_to_chunk_id={}, priority={}",
                move_from_chunk, move_to_chunk, priority);

    // 1 we need to move the move_to_chunk out of the reserved chunk queue
    std::list< chunk_id_t > reserved_chunks;
    chunk_id_t chunk_id{0};
    for (; m_reserved_chunk_queue.read(chunk_id);) {
        if (chunk_id == move_to_chunk || chunk_id == move_from_chunk) {
            // we found the chunk to be moved to, so we can stop reading
            break;
        }
        reserved_chunks.emplace_back(chunk_id);
    }

    // crash might happen before and after reserved chunk metablk is updated, so at least we need find one of the two
    // chunks.
    RELEASE_ASSERT(chunk_id == move_to_chunk || chunk_id == move_from_chunk,
                   "can not find neither move_to_chunk={} nor move_from_chunk={} in reserved chunk queue, priority={}",
                   move_to_chunk, move_from_chunk, priority);

    // now we need to put the reserved chunks back to the reserved chunk queue
    for (const auto& reserved_chunk : reserved_chunks) {
        m_reserved_chunk_queue.blockingWrite(reserved_chunk);
    }

    // 2 make the two chunks to the state as that before gc task starts
    m_chunk_selector->try_mark_chunk_to_gc_state(move_from_chunk, true /* force */);
    m_chunk_selector->try_mark_chunk_to_gc_state(move_to_chunk, true /* force */);

    auto move_from_vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);
    auto move_to_vchunk = m_chunk_selector->get_extend_vchunk(move_to_chunk);

    const auto pg_id = gc_task_sb->pg_id;
    move_from_vchunk->m_pg_id = pg_id;
    move_from_vchunk->m_v_chunk_id = gc_task_sb->vchunk_id;

    move_to_vchunk->m_pg_id = std::nullopt;
    move_to_vchunk->m_v_chunk_id = std::nullopt;

    std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > > valid_blob_indexes;

    if (!get_blobs_to_replace(move_to_chunk, valid_blob_indexes, 0, pg_id)) {
        RELEASE_ASSERT(
            false, "failed to get valid blob indexes from gc index table for move_to_chunk={} when recovery, pg_id={}",
            move_to_chunk, pg_id);
    }

    if (!process_after_gc_metablk_persisted(gc_task_sb, valid_blob_indexes, 0)) {
        RELEASE_ASSERT(false,
                       "failed to process after gc metablk persisted when recovery, "
                       "move_from_chunk={}, move_to_chunk={}, priority={}, pg_id={}",
                       move_from_chunk, move_to_chunk, priority, pg_id);
    }

    GCLOGD(RECOVERD_GC_TASK_ID, pg_id, NO_SHARD_ID,
           "finish handling recovered gc task: move_from_chunk_id={}, move_to_chunk_id={}, priority={}",
           move_from_chunk, move_to_chunk, priority);
}

bool GCManager::pdev_gc_actor::get_blobs_to_replace(
    chunk_id_t move_to_chunk, std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes,
    const uint64_t task_id, const pg_id_t pg_id) {

    auto start_key = BlobRouteByChunkKey{BlobRouteByChunk(move_to_chunk, 0, 0)};
    auto end_key = BlobRouteByChunkKey{BlobRouteByChunk{move_to_chunk, std::numeric_limits< uint64_t >::max(),
                                                        std::numeric_limits< uint64_t >::max()}};
    homestore::BtreeQueryRequest< BlobRouteByChunkKey > query_req{homestore::BtreeKeyRange< BlobRouteByChunkKey >{
        std::move(start_key), true /* inclusive */, std::move(end_key), true /* inclusive */
    }};

    auto ret = m_index_table->query(query_req, valid_blob_indexes);
    if (ret != homestore::btree_status_t::success) {
        // "ret != homestore::btree_status_t::has_more" is not expetced here, since we are querying all the pbas in one
        // time.
        GCLOGE(task_id, pg_id, NO_SHARD_ID,
               "Failed to query blobs in gc index table for move_to_chunk={}, index ret={}", move_to_chunk, ret);
        return false;
    }

    return true;
}

bool GCManager::pdev_gc_actor::replace_blob_index(
    chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
    const std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes, const uint64_t task_id) {

    // 1 get pg index table
    auto move_from_vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);
    RELEASE_ASSERT(move_from_vchunk->m_pg_id.has_value(), "chunk_id={} is expected to belong to a pg, but not!",
                   move_from_chunk);
    auto pg_id = move_from_vchunk->m_pg_id.value();
    auto hs_pg = m_hs_home_object->get_hs_pg(pg_id);

    // TODO:: add logic to handle pg_index_table is nullptr if destroying pg happens when GC
    RELEASE_ASSERT(hs_pg, "Unknown PG for pg_id={}", pg_id);
    auto pg_index_table = hs_pg->index_table_;
    RELEASE_ASSERT(pg_index_table, "Index table not found for PG pg_id={}", pg_id);

    // 2 update pg index table according to the query result of gc index table.
    // BtreeRangePutRequest only support update a range of keys to the same value, so we need to update the pg
    // indextable here one by one. since the update of index table is very fast , and gc is not time sensitive, so
    // we do this sequentially ATM.

    // TODO:: optimization, concurrently update pg index table.
    for (const auto& [k, v] : valid_blob_indexes) {
        const auto& shard = k.key().shard;
        const auto& blob = k.key().blob;
        BlobRouteKey index_key{BlobRoute{shard, blob}};

        homestore::BtreeSinglePutRequest update_req{
            &index_key, &v, homestore::btree_put_type::UPDATE, nullptr,
            [&pg_id, &shard, &blob, &move_from_chunk, &move_to_chunk,
             &task_id](homestore::BtreeKey const& key, homestore::BtreeValue const& value_in_btree,
                       homestore::BtreeValue const& new_value) -> homestore::put_filter_decision {
                BlobRouteValue existing_value{value_in_btree};
                BlobRouteValue new_pba_value{new_value};
                const auto& existing_pbas = existing_value.pbas();
                const auto& new_pbas = new_pba_value.pbas();

                if (existing_pbas == HSHomeObject::tombstone_pbas) {
                    GCLOGD(task_id, pg_id, shard,
                           "remove tombstone when updating pg index after data copy blob_id={}, move_from_chunk={}, "
                           "move_to_chunk={}",
                           blob, move_from_chunk, move_to_chunk);
                    homestore::data_service().async_free_blk(new_pba_value.pbas());
                    return homestore::put_filter_decision::remove;
                }

                if (existing_pbas.chunk_num() != move_from_chunk) {
                    GCLOGW(
                        task_id, pg_id, shard,
                        "existing pbas chunk={} should be equal to move_from_chunk={}, blob_id={}, move_to_chunk={}, "
                        "existing_pbas={}, new_pbas={}, this case might happen when crash recovery.",
                        existing_pbas.chunk_num(), move_from_chunk, blob, move_to_chunk, existing_pbas.to_string(),
                        new_pbas.to_string());
                    return homestore::put_filter_decision::keep;
                }

                GCLOGD(task_id, pg_id, shard,
                       "will replace blob_id={}, move_from_chunk={}, move_to_chunk={} from blk_id={} to blk_id={}",
                       blob, move_from_chunk, move_to_chunk, existing_pbas.to_string(), new_pbas.to_string());

                return homestore::put_filter_decision::replace;
            }};

        const auto ret = pg_index_table->put(update_req);

        // 1 if the key exist, and the filter returns homestore::put_filter_decision::replace, the ret will be
        // homestore::btree_status_t::success

        // 2 if the key exist , and the filter returns homestore::put_filter_decision::remove,  the ret will be
        // homestore::btree_status_t::filtered_out.(this might happen if a key is deleted after data copy but before
        // replace index)

        // 3 if the key does not exist, the ret will be homestore::btree_status_t::not_found(this might
        // happen when crash recovery)

        if (ret != homestore::btree_status_t::success && ret != homestore::btree_status_t::filtered_out &&
            ret != homestore::btree_status_t::not_found) {
            GCLOGE(task_id, pg_id, shard,
                   "Failed to update blob in pg index table, move_from_chunk={}, error_status={}, move_to_chunk={}",
                   move_from_chunk, ret, move_to_chunk);
            // pg index table might be partial updated, we can not put move_to_chunk back to the queue
            // m_reserved_chunk_queue.blockingWrite(move_to_chunk);
            return false;
        }

        GCLOGD(task_id, pg_id, shard,
               "successfully update index table, ret={}, move_from_chunk={}, move_to_chunk={}, blob_id={}", ret,
               move_from_chunk, move_to_chunk, blob);
    }

    // TODO:: revisit the following part with the consideration of persisting order for recovery.

    // 3 update pg metablk and related in-memory data structures
    m_hs_home_object->update_pg_meta_after_gc(pg_id, move_from_chunk, move_to_chunk, task_id);

    // 4 update shard metablk and related in-memory data structures
    m_hs_home_object->update_shard_meta_after_gc(move_from_chunk, move_to_chunk, task_id);

    return true;
}

sisl::sg_list GCManager::pdev_gc_actor::generate_shard_super_blk_sg_list(shard_id_t shard_id) {
    // TODO: do the buffer check before using it.
    auto raw_shard_sb = m_hs_home_object->_get_hs_shard(shard_id);
    RELEASE_ASSERT(raw_shard_sb, "can not find shard super blk for shard_id=0x{:x} !!!", shard_id);

    const auto shard_sb =
        const_cast< HSHomeObject::HS_Shard* >(d_cast< const HSHomeObject::HS_Shard* >(raw_shard_sb))->sb_.get();

    auto blk_size = homestore::data_service().get_blk_size();
    auto shard_sb_size = sizeof(HSHomeObject::shard_info_superblk);
    auto total_size = sisl::round_up(shard_sb_size, blk_size);
    auto shard_sb_buf = iomanager.iobuf_alloc(blk_size, total_size);

    std::memcpy(shard_sb_buf, shard_sb, shard_sb_size);

    sisl::sg_list shard_sb_sgs;
    shard_sb_sgs.size = total_size;
    shard_sb_sgs.iovs.emplace_back(iovec{.iov_base = shard_sb_buf, .iov_len = total_size});
    return shard_sb_sgs;
}

// note that, when we copy data, there is not create shard or put blob in this chunk, only delete blob might happen.
bool GCManager::pdev_gc_actor::copy_valid_data(
    chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
    folly::ConcurrentHashMap< BlobRouteByChunk, BlobRouteValue >& copied_blobs, const uint64_t task_id) {
    auto move_to_vchunk = m_chunk_selector->get_extend_vchunk(move_to_chunk);
    auto move_from_vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);

    RELEASE_ASSERT(move_from_vchunk->m_pg_id.has_value(), "move_from_chunk={} is expected to belong to a pg, but not!",
                   move_from_chunk);
    const auto pg_id = move_from_vchunk->m_pg_id.value();

    RELEASE_ASSERT(move_to_vchunk->m_state == ChunkState::GC, "move_to_chunk={} should be in GC state, but in state {}",
                   move_to_chunk, move_to_vchunk->m_state);
    RELEASE_ASSERT(move_from_vchunk->m_state == ChunkState::GC,
                   "move_from_chunk={} should be in GC state, but in state {}", move_from_chunk,
                   move_from_vchunk->m_state);

    auto move_to_chunk_total_blks = move_to_vchunk->get_total_blks();
    auto move_to_chunk_available_blks = move_to_vchunk->available_blks();

    RELEASE_ASSERT(move_to_chunk_total_blks == move_to_chunk_available_blks,
                   "move_to_chunk should be empty, total_blks={}, available_blks={}, move_to_chunk_id={}",
                   move_to_chunk_total_blks, move_to_chunk_available_blks, move_to_chunk);

    auto shards = m_hs_home_object->get_shards_in_chunk(move_from_chunk);
    if (shards.empty()) {
        GCLOGW(task_id, pg_id, NO_SHARD_ID, "no shard found in move_from_chunk, chunk_id={}", move_from_chunk);
        return true;
    }

    auto& data_service = homestore::data_service();

    const auto last_shard_id = *(shards.rbegin());
    const auto& shard_info = m_hs_home_object->_get_hs_shard(last_shard_id)->info;
    const auto& last_shard_state = shard_info.state;

    // in most cases(put_blob and seal_shard), the last shard in the chunk, which triggers emergent gc, should be in
    // open state. but if the emergent gc is triggered by a creat_shard request, then the last shard is not in open
    // state, it is in sealed state.
    if (last_shard_state == ShardInfo::State::OPEN) {
        GCLOGW(task_id, pg_id, last_shard_id, "last shard in move_from_chunk={} has a state of OPEN!", move_from_chunk);
    }

    homestore::blk_alloc_hints hints;
    hints.chunk_id_hint = move_to_chunk;
    homestore::MultiBlkId out_blkids;

    auto pg_index_table = m_hs_home_object->get_hs_pg(pg_id)->index_table_;
    const auto blk_size = data_service.get_blk_size();

    for (const auto& shard_id : shards) {
        bool is_last_shard = (shard_id == last_shard_id);
        std::vector< std::pair< BlobRouteKey, BlobRouteValue > > valid_blob_indexes;
        auto start_key = BlobRouteKey{BlobRoute{shard_id, std::numeric_limits< uint64_t >::min()}};
        auto end_key = BlobRouteKey{BlobRoute{shard_id, std::numeric_limits< uint64_t >::max()}};

#if 0
        // range_remove will hit "Node lock and refresh failed" in some case and reture a not_found even if some key has
        // been remove, then
        // 1 we can not know whether should we try , it will not return retry.
        // 2 if not_found happens, whether it means the shard is empty, or just a failure when searching.
        // 3 valid_blob_indexes will lost some keys since "Node lock and refresh failed" happen and the call will return
        // in advance

        // so not use this until index svc has fixed this. delete all the tombstone keys in pg index table
        // and get the valid blob keys
        homestore::BtreeRangeRemoveRequest< BlobRouteKey > range_remove_req{
            homestore::BtreeKeyRange< BlobRouteKey >{
                std::move(start_key), true /* inclusive */, std::move(end_key), true /* inclusive */
            },
            nullptr, std::numeric_limits< uint32_t >::max(),
            [&valid_blob_indexes](homestore::BtreeKey const& key, homestore::BtreeValue const& value) mutable -> bool {
                BlobRouteValue existing_value{value};
                if (existing_value.pbas() == HSHomeObject::tombstone_pbas) {
                    // delete tombstone key value
                    return true;
                }
                valid_blob_indexes.emplace_back(key, value);
                return false;
            }};

        auto status = pg_index_table->remove(range_remove_req);
        if (status != homestore::btree_status_t::success &&
            status != homestore::btree_status_t::not_found /*empty shard*/) {
            GCLOGW(task_id, pg_id, shard_id, "can not range remove blobs with tombstone in pg index table, status={}", status);
            return false;
        }
#endif

        // query will never hit "Node lock and refresh failed" and never need to retry
        homestore::BtreeQueryRequest< BlobRouteKey > query_req{
            homestore::BtreeKeyRange< BlobRouteKey >{std::move(start_key), true /* inclusive */, std::move(end_key),
                                                     true /* inclusive */},
            homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY,
            std::numeric_limits< uint32_t >::max() /* blob count in a shard will not exceed uint32_t_max*/,
            [](homestore::BtreeKey const& key, homestore::BtreeValue const& value) -> bool {
                BlobRouteValue existing_value{value};
                if (existing_value.pbas() == HSHomeObject::tombstone_pbas) { return false; }
                return true;
            }};

        auto const status = pg_index_table->query(query_req, valid_blob_indexes);
        if (status != homestore::btree_status_t::success) {
            GCLOGE(task_id, pg_id, shard_id, "Failed to query blobs in index table for status={}", status);
            return false;
        }

        if (valid_blob_indexes.empty()) {
            GCLOGD(task_id, pg_id, shard_id, "empty shard found in move_from_chunk={}", move_from_chunk);
            // TODO::send a delete shard request to raft channel. there is a case that when we are doing gc, the
            // shard becomes empty, need to handle this case

            // we should always write a shard header for the last shard if the state of it is open.
            if (last_shard_state != ShardInfo::State::OPEN || !is_last_shard) continue;
        } else {
            GCLOGD(task_id, pg_id, shard_id, "{} valid blobs found in move_from_chunk={}", valid_blob_indexes.size(),
                   move_from_chunk);
        }

        // prepare a shard header for this shard in move_to_chunk
        sisl::sg_list header_sgs = generate_shard_super_blk_sg_list(shard_id);

        // we ignore the state in shard header blk. we never read a shard header since we don`t know where it is(nor
        // record the pba in indextable)
#if 0
        // we now generate shard header from metablk. the shard state in shard header blk should be open, but for sealed
        // shard, the state in the generated in-memory header_sgs is sealed.
        if (!is_last_shard_in_emergent_chunk) {
            // for the sealed shard, the shard state in header should also be open.now, the written header is the same
            // as footer except the shard state, so we lost the original header.
            r_cast< HSHomeObject::shard_info_superblk* >(header_sgs.iovs[0].iov_base)->info.state =
                ShardInfo::State::OPEN;

            // TODO:: get the original header from the move_from_chunk and change the following part if needed.
            /*
            uint64_t created_time;
            uint64_t last_modified_time;
            uint64_t available_capacity_bytes;
            uint64_t total_capacity_bytes;
            */
        }
#endif

        // for emergent gc, we directly use the current shard header as the new header

        // TODO::involve ratelimiter in the following code, where read/write are scheduled. or do we need a central
        // ratelimter shared by all components except client io?
        auto succeed_copying_shard =
            // 1 write the shard header to move_to_chunk
            data_service.async_alloc_write(header_sgs, hints, out_blkids)
                .thenValue([this, &hints, &move_to_chunk, &move_from_chunk, &is_last_shard, &shard_id, &blk_size,
                            &valid_blob_indexes, &data_service, task_id, &last_shard_state, &copied_blobs, pg_id,
                            header_sgs = std::move(header_sgs)](auto&& err) {
                    RELEASE_ASSERT(header_sgs.iovs.size() == 1, "header_sgs.iovs.size() should be 1, but not!");
                    iomanager.iobuf_free(reinterpret_cast< uint8_t* >(header_sgs.iovs[0].iov_base));
                    if (err) {
                        GCLOGE(task_id, pg_id, shard_id,
                               "Failed to write shard header for move_to_chunk={}, err={}, err_category={}, "
                               "err_message={}",
                               move_to_chunk, err.value(), err.category().name(), err.message());
                        return folly::makeFuture< bool >(false);
                    }

                    if (valid_blob_indexes.empty()) {
                        RELEASE_ASSERT(is_last_shard && last_shard_state == ShardInfo::State::OPEN,
                                       "find empty shard in move_from_chunk={} "
                                       "but is_emergent is false, shard_id=0x{:x}, pg_id={}",
                                       move_from_chunk, shard_id, pg_id);
                        return folly::makeFuture< bool >(true);
                    }

                    std::vector< folly::Future< bool > > futs;

                    // 2 copy all the valid blobs in the shard from move_from_chunk to move_to_chunk
                    for (const auto& [k, v] : valid_blob_indexes) {
                        // k is shard_id + blob_id, v is multiblk id
                        auto pba = v.pbas();
                        auto total_size = pba.blk_count() * blk_size;

                        // buffer for read and write data
                        sisl::sg_list data_sgs;
                        data_sgs.size = total_size;
                        data_sgs.iovs.emplace_back(
                            iovec{.iov_base = iomanager.iobuf_alloc(blk_size, total_size), .iov_len = total_size});

                        futs.emplace_back(std::move(
                            // read blob from move_from_chunk
                            data_service.async_read(pba, data_sgs, total_size)
                                .thenValue([this, k, &hints, &move_from_chunk, &move_to_chunk, &data_service, task_id,
                                            pg_id, data_sgs = std::move(data_sgs), pba, &copied_blobs](auto&& err) {
                                    RELEASE_ASSERT(data_sgs.iovs.size() == 1,
                                                   "data_sgs.iovs.size() should be 1, but not!");

                                    const auto shard_id = k.key().shard;
                                    const auto blob_id = k.key().blob;

                                    if (err) {
                                        GCLOGE(task_id, pg_id, shard_id,
                                               "Failed to read blob from move_from_chunk={}, blob_id={}, err={}, "
                                               "err_category={}, err_message={}",
                                               move_from_chunk, blob_id, err.value(), err.category().name(),
                                               err.message());
                                        iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                                        return folly::makeFuture< bool >(false);
                                    }

                                    GCLOGD(task_id, pg_id, shard_id,
                                           "successfully read blob from move_from_chunk={}, blob_id={}, pba={}",
                                           move_from_chunk, blob_id, pba.to_string());

                                    if (m_enable_read_verify) {
                                        // after a blob is deleted at originator, if it receives a fetch_data request of
                                        // this blob, a fake delete_marker blob will be returned to the requester. This
                                        // case happens in incremental resync scenario. when verifying blob, if it is a
                                        // delete_marker, we let it pass the verification in gc scenario so that it will
                                        // not block any gc task.
                                        if (!m_hs_home_object->verify_blob(data_sgs.iovs[0].iov_base, shard_id, blob_id,
                                                                           true)) {
                                            GCLOGE(task_id, pg_id, shard_id,
                                                   "blob verification fails for move_from_chunk={}, blob_id={}, pba={}",
                                                   move_from_chunk, blob_id, pba.to_string());
                                            iomanager.iobuf_free(
                                                reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                                            return folly::makeFuture< bool >(false);
                                        }
                                    }

                                    // write the blob to the move_to_chunk. we do not care about the blob order in a
                                    // shard since we can not guarantee a certain order
                                    homestore::MultiBlkId new_pba;
                                    return data_service.async_alloc_write(data_sgs, hints, new_pba)
                                        .thenValue([this, shard_id, blob_id, new_pba, &move_to_chunk, task_id, pg_id,
                                                    &copied_blobs, data_sgs = std::move(data_sgs)](auto&& err) {
                                            RELEASE_ASSERT(data_sgs.iovs.size() == 1,
                                                           "data_sgs.iovs.size() should be 1, but not!");
                                            iomanager.iobuf_free(
                                                reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                                            if (err) {
                                                GCLOGE(task_id, pg_id, shard_id,
                                                       "Failed to write blob to move_to_chunk={}, blob_id={}, err={}, "
                                                       "err_category={}, err_message={}",
                                                       move_to_chunk, blob_id, err.value(), err.category().name(),
                                                       err.message());
                                                return false;
                                            }

                                            // insert a new entry to gc index table for this blob. [move_to_chunk_id,
                                            // shard_id, blob_id] -> [new pba]
                                            BlobRouteByChunkKey key{BlobRouteByChunk{move_to_chunk, shard_id, blob_id}};
                                            BlobRouteValue value{new_pba}, existing_value;

                                            homestore::BtreeSinglePutRequest put_req{
                                                &key, &value, homestore::btree_put_type::INSERT, &existing_value};
                                            auto status = m_index_table->put(put_req);
                                            if (status != homestore::btree_status_t::success) {
                                                GCLOGE(task_id, pg_id, shard_id,
                                                       "Failed to insert new key to gc index table for "
                                                       "move_to_chunk={}, blob_id={}, err={}",
                                                       move_to_chunk, blob_id, status);
                                                return false;
                                            }

                                            GCLOGD(task_id, pg_id, shard_id,
                                                   "successfully insert new key to gc index table for "
                                                   "move_to_chunk={}, blob_id={}, new_pba={}",
                                                   move_to_chunk, blob_id, new_pba.to_string());

                                            BlobRouteByChunk route_key{move_to_chunk, shard_id, blob_id};
                                            auto ret = copied_blobs.insert(route_key, value);
                                            RELEASE_ASSERT(ret.second,
                                                           "we should not copy the same blob twice in gc task, "
                                                           "move_to_chunk={}, shard_id=0x{:x}, pg_id={}, blob_id={}",
                                                           move_to_chunk, shard_id, pg_id, blob_id);

                                            return true;
                                        });
                                })));
                    }

                    // 3 write a shard footer for this shard
                    sisl::sg_list footer_sgs = generate_shard_super_blk_sg_list(shard_id);
                    return folly::collectAllUnsafe(futs)
                        .thenValue([this, &is_last_shard, &shard_id, &blk_size, &hints, &move_to_chunk, pg_id,
                                    &last_shard_state, task_id, &data_service, footer_sgs](auto&& results) {
                            // if any blob copy fails, we will not write footer, and drop this gc task
                            for (auto const& ok : results) {
                                RELEASE_ASSERT(ok.hasValue(), "we never throw any exception when copying data");
                                if (!ok.value()) {
                                    GCLOGE(task_id, pg_id, shard_id,
                                           "Failed to copy blob for move_to_chunk={}, will cancel this task",
                                           move_to_chunk);
                                    // if any op fails, we drop this gc task.
                                    return folly::makeFuture< std::error_code >(
                                        std::make_error_code(std::errc::operation_canceled));
                                }
                            }

                            // we skip writing footer only if the last shard of this chunk is in open state.
                            if (is_last_shard && last_shard_state == ShardInfo::State::OPEN) {
                                GCLOGD(task_id, pg_id, shard_id,
                                       "skip writing the footer for move_to_chunk={} for emergent gc task",
                                       move_to_chunk);
                                return folly::makeFuture< std::error_code >(std::error_code{});
                            }

                            // write shard footer
                            homestore::MultiBlkId out_blkids;
                            return data_service.async_alloc_write(footer_sgs, hints, out_blkids);
                        })
                        .thenValue([this, &move_to_chunk, &shard_id, footer_sgs, task_id, pg_id](auto&& err) {
                            RELEASE_ASSERT(footer_sgs.iovs.size() == 1, "footer_sgs.iovs.size() should be 1, but not!");
                            iomanager.iobuf_free(reinterpret_cast< uint8_t* >(footer_sgs.iovs[0].iov_base));

                            if (err) {
                                // we will come here if:
                                //  1 any blob copy fails, then err is operation_canceled
                                //  2 write footer fails， then err is the error code of write footer
                                GCLOGE(task_id, pg_id, shard_id,
                                       "Failed to copy some blos or failed to write shard footer for move_to_chunk={}, "
                                       "err={}, error_category={}, error_message={}, pls check the log for more "
                                       "detailed info",
                                       move_to_chunk, err.value(), err.category().name(), err.message());
                                return false;
                            }
                            return true;
                        });
                })
                .get();

        if (!succeed_copying_shard) {
            GCLOGE(task_id, pg_id, shard_id, "Failed to copy all blobs from move_from_chunk={} to move_to_chunk={}",
                   move_from_chunk, move_to_chunk);
            return false;
        }

        GCLOGD(task_id, pg_id, shard_id, "successfully copy blobs from move_from_chunk={} to move_to_chunk={}",
               move_from_chunk, move_to_chunk);
    }

    GCLOGD(task_id, pg_id, NO_SHARD_ID, "all valid blobs are copied from move_from_chunk={} to move_to_chunk={}",
           move_from_chunk, move_to_chunk);

    // we need to commit_blk for the move_to_chunk to make sure the last offset of append_blk_allocator is updated.
    // However, we don`t know the exact last blk in move_to_chunk. for normal, we can use the footer blk of the last
    // shard as the last blk. But, for emergent gc, all the blks in the last shard are written concurrently and there is
    // no footer for the last shard. so we use a fake multiblk here to make sure the append_blk_allocator is committed
    // to the exact last offset.
    const auto used_blks = move_to_vchunk->get_used_blks();
    if (used_blks) {
        homestore::MultiBlkId commit_blk_id(used_blks - 1, 1, move_to_chunk);

        if (data_service.commit_blk(commit_blk_id) != homestore::BlkAllocStatus::SUCCESS) {
            GCLOGE(task_id, pg_id, NO_SHARD_ID, "fail to commit_blk for move_to_chunk={}, commit_blk_id={}",
                   move_to_chunk, commit_blk_id.to_string());
            return false;
        }

        GCLOGD(task_id, pg_id, NO_SHARD_ID, "successfully commit_blk in move_to_chunk={}, commit_blk_id={}",
               move_to_chunk, commit_blk_id.to_string());
    } else {
        GCLOGD(task_id, pg_id, NO_SHARD_ID, "no used blks in move_to_chunk={}, so no need to commit_blk",
               move_to_chunk);
    }

    // remove all the tombstone keys in pg index table for this chunk
    // TODO:: we can enable the range_remove above and delete this part after the indexsvc issue is fixed
    for (const auto& shard_id : shards) {
        auto start_key = BlobRouteKey{BlobRoute{shard_id, std::numeric_limits< uint64_t >::min()}};
        auto end_key = BlobRouteKey{BlobRoute{shard_id, std::numeric_limits< uint64_t >::max()}};
        homestore::BtreeRangeRemoveRequest< BlobRouteKey > range_remove_req{
            homestore::BtreeKeyRange< BlobRouteKey >{
                std::move(start_key), true /* inclusive */, std::move(end_key), true /* inclusive */
            },
            nullptr, std::numeric_limits< uint32_t >::max(),
            [](homestore::BtreeKey const& key, homestore::BtreeValue const& value) -> bool {
                BlobRouteValue existing_value{value};
                if (existing_value.pbas() == HSHomeObject::tombstone_pbas) {
                    // delete tombstone key value
                    return true;
                }
                return false;
            }};

        auto status = pg_index_table->remove(range_remove_req);
        if (status != homestore::btree_status_t::success &&
            status != homestore::btree_status_t::not_found /*empty shard*/) {
            // if fail to remove tombstone, it does not matter and they will be removed in the next gc task.
            GCLOGW(task_id, pg_id, shard_id, "fail to remove tombstone, ret={}", status);
        }
        // TODO:: after the completion of indexsvc bug fix, we need to retry according to the returned status.

        GCLOGD(task_id, pg_id, shard_id, "remove tombstone successfully, ret={}, move_from_chunk={}, move_to_chunk={}",
               status, move_from_chunk, move_to_chunk);
    }

    GCLOGD(task_id, pg_id, NO_SHARD_ID, "data copied successfully for move_from_chunk={} to move_to_chunk={}",
           move_from_chunk, move_to_chunk);

    return true;
}

bool GCManager::pdev_gc_actor::purge_reserved_chunk(chunk_id_t chunk, const uint64_t task_id, const pg_id_t pg_id) {
    auto vchunk = m_chunk_selector->get_extend_vchunk(chunk);
    RELEASE_ASSERT(!vchunk->m_pg_id.has_value(),
                   "chunk_id={} is expected to be a reserved chunk, and not belong to a pg", chunk);
    RELEASE_ASSERT(vchunk->m_state == ChunkState::GC,
                   "chunk_id={} is a reserved chunk, expected to have a GC state, but actuall state is {} ", chunk,
                   vchunk->m_state);

    GCLOGD(task_id, pg_id, NO_SHARD_ID, "reset chunk={} before using it for gc", vchunk->get_chunk_id());
    vchunk->reset(); // reset the chunk to make sure it is empty

    // clear all the entries of this chunk in the gc index table
    auto start_key = BlobRouteByChunkKey{BlobRouteByChunk(chunk, 0, 0)};
    auto end_key = BlobRouteByChunkKey{
        BlobRouteByChunk{chunk, std::numeric_limits< uint64_t >::max(), std::numeric_limits< uint64_t >::max()}};

    homestore::BtreeRangeRemoveRequest< BlobRouteByChunkKey > range_remove_req{
        homestore::BtreeKeyRange< BlobRouteByChunkKey >{
            start_key, true /* inclusive */, end_key, true /* inclusive */
        }};

    auto status = m_index_table->remove(range_remove_req);
    if (status != homestore::btree_status_t::success &&
        status != homestore::btree_status_t::not_found /*already empty*/) {
        GCLOGW(task_id, pg_id, NO_SHARD_ID, "fail to purge gc index for chunk={}", chunk);
        return false;
    }

    // after range_remove, we check again to make sure there is not any entry in the gc index table for this chunk
    homestore::BtreeQueryRequest< BlobRouteByChunkKey > query_req{homestore::BtreeKeyRange< BlobRouteByChunkKey >{
        std::move(start_key), true /* inclusive */, std::move(end_key), true /* inclusive */
    }};

    std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > > valid_blob_indexes;

    status = m_index_table->query(query_req, valid_blob_indexes);
    if (status != homestore::btree_status_t::success) {
        GCLOGE(task_id, pg_id, NO_SHARD_ID,
               "Failed to query blobs after purging reserved chunk={} in gc index table, index ret={}", chunk, status);
        return false;
    }

    if (!valid_blob_indexes.empty()) {
        GCLOGE(task_id, pg_id, NO_SHARD_ID,
               "gc index table is not empty for chunk={} after purging, valid_blob_indexes.size={}", chunk,
               valid_blob_indexes.size());
        return false;
    }

    return true;
}

bool GCManager::pdev_gc_actor::compare_blob_indexes(
    folly::ConcurrentHashMap< BlobRouteByChunk, BlobRouteValue > const& copied_blobs,
    std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > > const& valid_blob_indexes, const uint64_t task_id,
    const pg_id_t pg_id) {

    // there probably be some indexservice bug which will cause some blobs missing in gc index table after put, so we
    // make an aggressive check here to make sure the blob got from gc_index_table the same as those copied by gc data
    // copy, and print out the detailed info if the check fails.
    bool ret{true};
    if (copied_blobs.size() != valid_blob_indexes.size()) {
        GCLOGW(
            task_id, pg_id, NO_SHARD_ID,
            "the number of copied blobs number {} is not the same as the number of valid blobs number {} from gc index "
            "table",
            copied_blobs.size(), valid_blob_indexes.size());
        ret = false;
    }

    if (ret) {
        for (const auto& [k, v] : valid_blob_indexes) {
            const auto shard_id = k.key().shard;
            BlobRouteByChunk route_key{k.key().chunk, shard_id, k.key().blob};
            const auto it = copied_blobs.find(route_key);
            if (it == copied_blobs.end()) {
                GCLOGW(task_id, pg_id, shard_id,
                       "can not find copied blob in copied_blobs for move_to_chunk={}, blob_id={}", k.key().chunk,
                       k.key().blob);
                ret = false;
                break;
            }

            if (v.pbas() != it->second.pbas()) {
                GCLOGW(task_id, pg_id, shard_id,
                       "pba of copied blob is not the same as that in gc index table for move_to_chunk={}, blob_id={}, "
                       "copied_pba={}, gc_index_table_pba={}",
                       k.key().chunk, k.key().blob, it->second.pbas().to_string(), v.pbas().to_string());
                ret = false;
                break;
            }
        }
    }

    if (!ret) {
        GCLOGW(task_id, pg_id, NO_SHARD_ID,
               "copied blobs do not match those in gc index table, start printing copied blobs:");
        for (const auto& [k, v] : copied_blobs) {
            const auto shard_id = k.shard;
            GCLOGW(task_id, pg_id, shard_id, "copied blob: move_to_chunk={}, blob_id={}, pba={}", k.chunk, k.blob,
                   v.pbas().to_string());
        }

        GCLOGW(task_id, pg_id, NO_SHARD_ID, "start printing valid blobs from gc index table:");
        for (const auto& [k, v] : valid_blob_indexes) {
            const auto shard_id = k.key().shard;
            GCLOGW(task_id, pg_id, shard_id, "valid blob: move_to_chunk={}, blob_id={}, pba={}", k.key().chunk,
                   k.key().blob, v.pbas().to_string());
        }

        RELEASE_ASSERT(false, "copied blobs are not the same as the valid blobs got from gc index table");
    }

    return ret;
}

void GCManager::pdev_gc_actor::handle_error_before_persisting_gc_metablk(chunk_id_t move_from_chunk,
                                                                         chunk_id_t move_to_chunk,
                                                                         folly::Promise< bool > task,
                                                                         const uint64_t task_id, uint8_t priority,
                                                                         const pg_id_t& pg_id) {
    GCLOGE(task_id, pg_id, NO_SHARD_ID,
           "move_from_chunk={} to move_to_chunk={} with priority={} failed before persisting gc metablk",
           move_from_chunk, move_to_chunk, priority);

    const auto final_state =
        priority == static_cast< uint8_t >(task_priority::normal) ? ChunkState::AVAILABLE : ChunkState::INUSE;
    m_chunk_selector->mark_chunk_out_of_gc_state(move_from_chunk, final_state, task_id);

    task.setValue(false);
    m_reserved_chunk_queue.blockingWrite(move_to_chunk);

    durable_entities_update([this, priority](auto& de) {
        priority == static_cast< uint8_t >(task_priority::normal) ? de.failed_gc_task_count.fetch_add(1)
                                                                  : de.failed_egc_task_count.fetch_add(1);
    });
    m_hs_home_object->gc_manager()->decr_pg_pending_gc_task(pg_id);
}

void GCManager::pdev_gc_actor::process_gc_task(chunk_id_t move_from_chunk, uint8_t priority,
                                               folly::Promise< bool > task, const uint64_t task_id) {
    auto start_time = std::chrono::steady_clock::now();
    auto vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);
    RELEASE_ASSERT(vchunk->m_pg_id.has_value(), "chunk_id={} is expected to belong to a pg, but not!", move_from_chunk);
    const auto pg_id = vchunk->m_pg_id.value();

    GCLOGD(task_id, pg_id, NO_SHARD_ID, "start process gc task for move_from_chunk={} with priority={} ",
           move_from_chunk, priority);

    if (vchunk->m_state != ChunkState::GC) {
        GCLOGW(task_id, pg_id, NO_SHARD_ID, "move_from_chunk={} is expected to in GC state but not!", move_from_chunk);
        task.setValue(false);
        m_hs_home_object->gc_manager()->decr_pg_pending_gc_task(pg_id);
        return;
    }

    RELEASE_ASSERT(vchunk->m_v_chunk_id.has_value(), "pg={}, chunk_id={} is expected to have a vchunk id, but not!",
                   pg_id, move_from_chunk);
    const auto vchunk_id = vchunk->m_v_chunk_id.value();

    chunk_id_t move_to_chunk;

    // wait for a reserved chunk to be available. now, the amount of threads in the folly executor(thread pool) is equal
    // to the amount of reserved number, so we can make sure that a gc task handle thread can always get a reserved
    // chunk, so actually the blockingRead here will not block in any case and return immediately.
    m_reserved_chunk_queue.blockingRead(move_to_chunk);
    GCLOGD(task_id, pg_id, NO_SHARD_ID,
           "task for move_from_chunk={} to move_to_chunk={} with priority={} start copying data", move_from_chunk,
           move_to_chunk, priority);

    if (!purge_reserved_chunk(move_to_chunk, task_id, pg_id)) {
        GCLOGW(task_id, pg_id, NO_SHARD_ID, "can not purge move_to_chunk={}", move_to_chunk);
        handle_error_before_persisting_gc_metablk(move_from_chunk, move_to_chunk, std::move(task), task_id, priority,
                                                  pg_id);
        return;
    }

    folly::ConcurrentHashMap< BlobRouteByChunk, BlobRouteValue > copied_blobs;
    if (!copy_valid_data(move_from_chunk, move_to_chunk, copied_blobs, task_id)) {
        GCLOGW(task_id, pg_id, NO_SHARD_ID,
               "failed to copy data from move_from_chunk={} to move_to_chunk={} with priority={}", move_from_chunk,
               move_to_chunk, priority);
        handle_error_before_persisting_gc_metablk(move_from_chunk, move_to_chunk, std::move(task), task_id, priority,
                                                  pg_id);
        return;
    }

    std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > > valid_blob_indexes;
    if (!get_blobs_to_replace(move_to_chunk, valid_blob_indexes, task_id, pg_id)) {
        GCLOGW(task_id, pg_id, NO_SHARD_ID, "failed to get valid blob indexes from gc index table for move_to_chunk={}",
               move_to_chunk);
        handle_error_before_persisting_gc_metablk(move_from_chunk, move_to_chunk, std::move(task), task_id, priority,
                                                  pg_id);
        return;
    }

    if (!compare_blob_indexes(copied_blobs, valid_blob_indexes, task_id, pg_id)) {
        GCLOGW(task_id, pg_id, NO_SHARD_ID,
               "copied blobs are not the same as the valid blobs got from gc index table for move_to_chunk={}",
               move_to_chunk);
        handle_error_before_persisting_gc_metablk(move_from_chunk, move_to_chunk, std::move(task), task_id, priority,
                                                  pg_id);
        return;
    }

    // trigger cp to make sure the offset the the append blk allocator and the wbcache of gc index table are both
    // flushed.
    auto fut = homestore::hs()->cp_mgr().trigger_cp_flush(true /* force */);
    RELEASE_ASSERT(std::move(fut).get(), "expect gc index table and blk allocator to be flushed but failed!");

    // after data copy, we persist the gc task meta blk. now, we can make sure all the valid blobs are successfully
    // copyed and new blob indexes have be written to gc index table before gc task superblk is persisted.
    homestore::superblk< GCManager::gc_task_superblk > gc_task_sb{GCManager::_gc_task_meta_name};
    gc_task_sb.create(sizeof(GCManager::gc_task_superblk));
    gc_task_sb->move_from_chunk = move_from_chunk;
    gc_task_sb->move_to_chunk = move_to_chunk;
    gc_task_sb->vchunk_id = vchunk_id;
    gc_task_sb->priority = priority;
    gc_task_sb->pg_id = pg_id;
    // write the gc task meta blk to the meta service, so that it can be recovered when restarting
    gc_task_sb.write();

    GCLOGD(task_id, pg_id, NO_SHARD_ID,
           "gc task for move_from_chunk={} to move_to_chunk={} with priority={} start replacing blob index",
           move_from_chunk, move_to_chunk, priority);

    if (!process_after_gc_metablk_persisted(gc_task_sb, valid_blob_indexes, task_id)) {
        // TODO::add a method to restore the old index if any error happen when replacing blob index
        RELEASE_ASSERT(false,
                       "Fail to process after gc metablk persisted, move_from_chunk={}, move_to_chunk={}, priority={}",
                       move_from_chunk, move_to_chunk, priority);
    }

    durable_entities_update([this, priority](auto& de) {
        priority == static_cast< uint8_t >(task_priority::normal) ? de.success_gc_task_count.fetch_add(1)
                                                                  : de.success_egc_task_count.fetch_add(1);
    });

    if (priority == static_cast< uint8_t >(task_priority::normal)) {
        HISTOGRAM_OBSERVE(metrics(), gc_time_duration_s_gc, get_elapsed_time_sec(start_time));
    } else {
        HISTOGRAM_OBSERVE(metrics(), gc_time_duration_s_egc, get_elapsed_time_sec(start_time));
    }

    task.setValue(true);
    m_reserved_chunk_queue.blockingWrite(move_from_chunk);
    m_hs_home_object->gc_manager()->decr_pg_pending_gc_task(pg_id);
    GCLOGI(task_id, pg_id, NO_SHARD_ID,
           "task for move_from_chunk={} to move_to_chunk={} with priority={} is completed!", move_from_chunk,
           move_to_chunk, priority);
}

bool GCManager::pdev_gc_actor::process_after_gc_metablk_persisted(
    homestore::superblk< GCManager::gc_task_superblk >& gc_task_sb,
    const std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes, const uint64_t task_id) {

    const chunk_id_t move_from_chunk = gc_task_sb->move_from_chunk;
    const chunk_id_t move_to_chunk = gc_task_sb->move_to_chunk;
    const uint8_t priority = gc_task_sb->priority;
    const auto pg_id = gc_task_sb->pg_id;
    const auto vchunk_id = gc_task_sb->vchunk_id;

    if (!replace_blob_index(move_from_chunk, move_to_chunk, valid_blob_indexes, task_id)) {
        // if we fail to replace blob index, the worst case is some of the valid blobs index is update, but others not.
        // At this moment, we can not drop any one of move_from_chunk and move_to_chunk, since they both contains valid
        // blob data. we can not go ahead
        GCLOGE(task_id, pg_id, NO_SHARD_ID,
               "failed to replace blob index, move_from_chunk={} to move_to_chunk={} with priority={}", move_from_chunk,
               move_to_chunk, priority);
        return false;
    }

#ifdef _PRERELEASE
    // now, the move_from_chunk is not put back to the reserved chunk queue, we simulate this
    if (iomgr_flip::instance()->test_flip("simulate_gc_crash_recovery")) {
        // don`t not delete gc task sb to simlulate_gc_crash_recovery
        GCLOGI(task_id, pg_id, NO_SHARD_ID, "gc task superblk is not deleted to simulate recovery");
        return true;
    }
#endif

    // trigger again to make sure the pg index wbcache is persisted.
    auto fut = homestore::hs()->cp_mgr().trigger_cp_flush(true /* force */);
    RELEASE_ASSERT(std::move(fut).get(), "expect pg index table to be flushed but failed!");

    // update the reserved chunk meta blk. we need to do this before gc_task_sb is destroyed because if we do this after
    // gc_task_sb is destroyed and crash happens before we update reserved_chunk metablk, then we have no chance to
    // update this reserved chunk metablk. the move_to_chunk now contains user data, and it is no longer a reserved
    // chunk. but the reserved_chunk metablk tells us this is a reserved chunk and will be involved in new gc task. this
    // case, data loss will happen
    for (auto& reserved_chunk : m_reserved_chunks) {
        if (reserved_chunk->chunk_id == move_to_chunk) {
            reserved_chunk->chunk_id = move_from_chunk;
            reserved_chunk.write();
            break;
        }
    }

    gc_task_sb.destroy();

    const auto reclaimed_blk_count = m_chunk_selector->get_extend_vchunk(move_from_chunk)->get_used_blks() -
        m_chunk_selector->get_extend_vchunk(move_to_chunk)->get_used_blks();

    durable_entities_update([this, priority, reclaimed_blk_count](auto& de) {
        priority == static_cast< uint8_t >(task_priority::normal)
            ? de.total_reclaimed_blk_count_by_gc.fetch_add(reclaimed_blk_count)
            : de.total_reclaimed_blk_count_by_egc.fetch_add(reclaimed_blk_count);
    });

    const auto final_state =
        priority == static_cast< uint8_t >(task_priority::normal) ? ChunkState::AVAILABLE : ChunkState::INUSE;

    m_chunk_selector->update_vchunk_info_after_gc(move_from_chunk, move_to_chunk, final_state, pg_id, vchunk_id,
                                                  task_id);

    GCLOGD(
        task_id, pg_id, NO_SHARD_ID,
        "vchunk_id={} has been update from move_from_chunk={} to move_to_chunk={}, {} blks are reclaimed, final state "
        "is updated to {}",
        vchunk_id, move_from_chunk, move_to_chunk, reclaimed_blk_count, final_state);

    const auto total_blks_in_chunk = m_chunk_selector->get_extend_vchunk(move_from_chunk)->get_total_blks();

    if (priority == static_cast< uint8_t >(task_priority::normal)) {
        HISTOGRAM_OBSERVE(metrics(), reclaim_ratio_gc,
                          100 * static_cast< double >(reclaimed_blk_count) / total_blks_in_chunk);
    } else {
        HISTOGRAM_OBSERVE(metrics(), reclaim_ratio_egc,
                          100 * static_cast< double >(reclaimed_blk_count) / total_blks_in_chunk);
    }

    return true;
}

GCManager::pdev_gc_actor::~pdev_gc_actor() {
    stop();
    LOGINFOMOD(gcmgr, "gc actor for pdev_id={} is destroyed", m_pdev_id);
}

/* RateLimiter */
GCManager::RateLimiter::RateLimiter(uint64_t refill_count_per_second) :
        tokens_(refill_count_per_second), refillRate_(refill_count_per_second) {
    lastRefillTime_ = std::chrono::steady_clock::now();
}

bool GCManager::RateLimiter::allowRequest(uint64_t count) {
    std::lock_guard lock(mutex_);
    refillTokens();
    if (tokens_ >= count) {
        tokens_ -= count;
        return true;
    }
    return false;
}

void GCManager::RateLimiter::refillTokens() {
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::seconds >(now - lastRefillTime_).count();
    if (duration) {
        tokens_ = refillRate_;
        lastRefillTime_ = now;
    }
}

} // namespace homeobject
