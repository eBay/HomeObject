#include <homestore/btree/btree_req.hpp>
#include <homestore/btree/btree_kv.hpp>

#include "hs_homeobject.hpp"
namespace homeobject {

/* GCManager */

GCManager::GCManager(std::shared_ptr< HeapChunkSelector > chunk_selector, HSHomeObject* homeobject) :
        m_chunk_selector{chunk_selector}, m_hs_home_object{homeobject} {
    homestore::HomeStore::instance()->meta_service().register_handler(
        GCManager::_gc_actor_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_gc_actor_meta_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        nullptr, true);

    homestore::HomeStore::instance()->meta_service().register_handler(
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

    homestore::HomeStore::instance()->meta_service().register_handler(
        GCManager::_gc_task_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_gc_task_meta_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        nullptr, true);
}

void GCManager::on_gc_task_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie) {
    homestore::superblk< GCManager::gc_task_superblk > gc_task_sb(GCManager::_gc_task_meta_name);
    const auto pending_gc_task = gc_task_sb.load(buf, meta_cookie);

    // if a gc_task_super blk is found, we can make sure that all the valid data in move_from_chunk has been copied to
    // move_to_chunk, and all the blob -> (new pba) have been written to the gc index table. Now, what we need to do is
    // just updating blob indexes in pg index table according to the blob indexes in gc index table.

    // pg_index_table: [pg_id, shard_id, blob_id] -> old pba
    // gc_index_table: [move_to_chunk_id, pg_id, shard_id, blob_id] -> new pba

    // we need to find all keys with the prefix of move_to_chunk_id in gc index table, and update the corrsponding
    // keys(same pg_id + shard_id + blob_id) in pg index table with the new pba.
    auto pdev_id = m_chunk_selector->get_extend_vchunk(pending_gc_task->move_from_chunk)->get_pdev_id();
    auto gc_actor = get_pdev_gc_actor(pdev_id);
    RELEASE_ASSERT(gc_actor, "can not get gc actor for pdev {}!", pdev_id);
    gc_actor->handle_recovered_gc_task(pending_gc_task);

    // delete gc task meta blk, so that it will not be recovered again if restart
    gc_task_sb.destroy();
}

void GCManager::on_gc_actor_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie) {
    homestore::superblk< GCManager::gc_actor_superblk > gc_actor_sb(GCManager::_gc_actor_meta_name);
    gc_actor_sb.load(buf, meta_cookie);
    auto pdev_id = gc_actor_sb->pdev_id;
    auto index_table_uuid = gc_actor_sb->index_table_uuid;

    auto gc_index_table = m_hs_home_object->get_gc_index_table(boost::uuids::to_string(index_table_uuid));

    RELEASE_ASSERT(gc_index_table, "can not get gc index table for pdev {} with uuid {}!", pdev_id,
                   boost::uuids::to_string(index_table_uuid));

    // create a gc actor for this pdev if not exists
    auto gc_actor = try_create_pdev_gc_actor(pdev_id, gc_index_table);
    RELEASE_ASSERT(gc_actor, "can not get gc actor for pdev {}!", pdev_id);
}

void GCManager::on_reserved_chunk_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie) {
    homestore::superblk< GCManager::gc_reserved_chunk_superblk > reserved_chunk_sb(
        GCManager::_gc_reserved_chunk_meta_name);
    reserved_chunk_sb.load(buf, meta_cookie);
    auto chunk_id = reserved_chunk_sb->chunk_id;
    auto pdev_id = m_chunk_selector->get_extend_vchunk(chunk_id)->get_pdev_id();
    auto gc_actor = get_pdev_gc_actor(pdev_id);
    RELEASE_ASSERT(gc_actor, "can not get gc actor for pdev {}!", pdev_id);
    gc_actor->add_reserved_chunk(chunk_id);
    // mark a reserved chunk as gc state, so that it will not be selected as a gc candidate
    m_chunk_selector->try_mark_chunk_to_gc_state(chunk_id, true /* force */);
}

GCManager::~GCManager() { stop(); }

void GCManager::start() {
    for (const auto& [pdev_id, gc_actor] : m_pdev_gc_actors) {
        gc_actor->start();
        LOGINFO("start gc actor for pdev={}", pdev_id);
    }

    m_gc_timer_hdl = iomanager.schedule_global_timer(
        GC_SCAN_INTERVAL_SEC * 1000 * 1000 * 1000, true, nullptr /*cookie*/, iomgr::reactor_regex::all_user,
        [this](void*) { scan_chunks_for_gc(); }, true /* wait_to_schedule */);
    LOGINFO("gc scheduler timer has started, interval is set to {} seconds", GC_SCAN_INTERVAL_SEC);
}

void GCManager::stop() {
    if (m_gc_timer_hdl == iomgr::null_timer_handle) {
        LOGWARN("gc scheduler timer is not running, no need to stop it");
        return;
    }

    LOGINFO("stop gc scheduler timer");
    iomanager.cancel_timer(m_gc_timer_hdl, true);
    m_gc_timer_hdl = iomgr::null_timer_handle;

    for (const auto& [pdev_id, gc_actor] : m_pdev_gc_actors) {
        gc_actor->stop();
        LOGINFO("stop gc actor for pdev={}", pdev_id);
    }
}

folly::SemiFuture< bool > GCManager::submit_gc_task(task_priority priority, chunk_id_t chunk_id) {
    auto pdev_id = m_chunk_selector->get_extend_vchunk(chunk_id)->get_pdev_id();
    auto it = m_pdev_gc_actors.find(pdev_id);
    if (it == m_pdev_gc_actors.end()) {
        LOGINFO("pdev gc actor not found for pdev_id: {}", pdev_id);
        return folly::makeFuture< bool >(false);
    }
    auto& actor = it->second;
    return actor->add_gc_task(static_cast< uint8_t >(priority), chunk_id);
}

std::shared_ptr< GCManager::pdev_gc_actor >
GCManager::try_create_pdev_gc_actor(uint32_t pdev_id, std::shared_ptr< GCBlobIndexTable > index_table) {
    auto const [it, happened] = m_pdev_gc_actors.try_emplace(
        pdev_id, std::make_shared< pdev_gc_actor >(pdev_id, m_chunk_selector, index_table, m_hs_home_object));
    RELEASE_ASSERT((it != m_pdev_gc_actors.end()), "Unexpected error in m_pdev_gc_actors!!!");
    if (happened) {
        LOGINFO("create new gc actor for pdev_id: {}", pdev_id);
    } else {
        LOGINFO("pdev gc actor already exists for pdev_id: {}", pdev_id);
    }
    return it->second;
}

std::shared_ptr< GCManager::pdev_gc_actor > GCManager::get_pdev_gc_actor(uint32_t pdev_id) {
    auto it = m_pdev_gc_actors.find(pdev_id);
    if (it == m_pdev_gc_actors.end()) {
        LOGERROR("pdev gc actor not found for pdev_id: {}", pdev_id);
        return nullptr;
    }
    return it->second;
}

bool GCManager::is_eligible_for_gc(chunk_id_t chunk_id) {
    auto chunk = m_chunk_selector->get_extend_vchunk(chunk_id);

    // 1 if the chunk state is inuse, it is occupied by a open shard, so it can not be selected and we don't need gc it.
    // 2 if the chunk state is gc, it means this chunk is being gc, or this is a reserved chunk, so we don't need gc it.
    if (chunk->m_state != ChunkState::AVAILABLE) {
        LOGINFO("chunk_id={} state is {}, not eligible for gc", chunk_id, chunk->m_state)
        return false;
    }
    // it does not belong to any pg, so we don't need to gc it.
    if (!chunk->m_pg_id.has_value()) {
        LOGINFO("chunk_id={} belongs to no pg, not eligible for gc", chunk_id)
        return false;
    }

    LOGINFO("chunk_id={} is eligible for gc, belongs to pg {}", chunk_id, chunk->m_pg_id.value());

    auto defrag_blk_num = chunk->get_defrag_nblks();
    auto total_blk_num = chunk->get_total_blks();

    // defrag_blk_num > (GC_THRESHOLD_PERCENT/100) * total_blk_num, to avoid floating point number calculation
    // TODO: avoid overflow here.
    return 100 * defrag_blk_num > total_blk_num * GC_GARBAGE_RATE_THRESHOLD;
}

void GCManager::scan_chunks_for_gc() {
    for (const auto& [pdev_id, chunks] : m_chunk_selector->get_pdev_chunks()) {
        // in every iteration, we will select at most 2 * RESERVED_CHUNK_NUM_PER_PDEV gc tasks
        auto max_task_num = 2 * (RESERVED_CHUNK_NUM_PER_PDEV - RESERVED_CHUNK_NUM_DEDICATED_FOR_EGC);
        auto it = m_pdev_gc_actors.find(pdev_id);
        RELEASE_ASSERT(it != m_pdev_gc_actors.end(), "can not find gc actor for pdev_id {} when scanning chunks for gc",
                       pdev_id);
        auto& actor = it->second;

        for (const auto& chunk_id : chunks) {
            if (is_eligible_for_gc(chunk_id)) {
                auto future = actor->add_gc_task(static_cast< uint8_t >(task_priority::normal), chunk_id);
                if (future.isReady()) {
                    if (future.value()) {
                        LOGINFO("gc task for chunk_id={} on pdev_id={} has been submitted and successfully completed "
                                "shortly",
                                chunk_id, pdev_id);
                    } else {
                        LOGWARN("got false after add_gc_task for chunk_id={} on pdev_id={}, it means we cannot mark "
                                "this chunk to gc state(there is an open shard on this chunk ATM) or this task is "
                                "executed shortly but fails(fail to copy data or update gc index table) ",
                                chunk_id, pdev_id);
                    }
                } else if (0 == --max_task_num) {
                    LOGINFO("reached max gc task limit for pdev_id={}, stopping further gc task submissions", pdev_id);
                    break;
                }
            }
        }
    }
}

/* pdev_gc_actor */

GCManager::pdev_gc_actor::pdev_gc_actor(uint32_t pdev_id, std::shared_ptr< HeapChunkSelector > chunk_selector,
                                        std::shared_ptr< GCBlobIndexTable > index_table, HSHomeObject* homeobject) :
        m_pdev_id{pdev_id},
        m_chunk_selector{chunk_selector},
        m_reserved_chunk_queue{RESERVED_CHUNK_NUM_PER_PDEV},
        m_index_table{index_table},
        m_hs_home_object{homeobject} {
    RELEASE_ASSERT(index_table, "index_table for a gc_actor should not be nullptr!!!");
}

void GCManager::pdev_gc_actor::start() {
    bool stopped = true;
    if (!m_is_stopped.compare_exchange_strong(stopped, false, std::memory_order_release, std::memory_order_relaxed)) {
        LOGERROR("pdev gc actor for pdev_id={} is already started, no need to start again!", m_pdev_id);
        return;
    }
    RELEASE_ASSERT(RESERVED_CHUNK_NUM_PER_PDEV > RESERVED_CHUNK_NUM_DEDICATED_FOR_EGC,
                   "reserved chunk number {} per pdev should be greater than {}", RESERVED_CHUNK_NUM_PER_PDEV,
                   RESERVED_CHUNK_NUM_DEDICATED_FOR_EGC);
    // thread number is the same as reserved chunk, which can make sure every gc thread can take a reserved chunk
    // for gc
    m_gc_executor = std::make_shared< folly::IOThreadPoolExecutor >(RESERVED_CHUNK_NUM_PER_PDEV -
                                                                    RESERVED_CHUNK_NUM_DEDICATED_FOR_EGC);
    m_egc_executor = std::make_shared< folly::IOThreadPoolExecutor >(RESERVED_CHUNK_NUM_DEDICATED_FOR_EGC);

    LOGINFO("pdev gc actor for pdev_id={} has started", m_pdev_id);
}

void GCManager::pdev_gc_actor::stop() {
    bool stopped = false;
    if (!m_is_stopped.compare_exchange_strong(stopped, true, std::memory_order_release, std::memory_order_relaxed)) {
        LOGWARN("pdev gc actor for pdev_id={} is already stopped, no need to stop again!", m_pdev_id);
        return;
    }
    m_gc_executor->stop();
    m_gc_executor.reset();

    m_egc_executor->stop();
    m_egc_executor.reset();
    LOGINFO("pdev gc actor for pdev_id={} has stopped", m_pdev_id);
}

void GCManager::pdev_gc_actor::add_reserved_chunk(chunk_id_t chunk_id) {
    m_reserved_chunk_queue.blockingWrite(chunk_id);
}

folly::SemiFuture< bool > GCManager::pdev_gc_actor::add_gc_task(uint8_t priority, chunk_id_t move_from_chunk) {
    if (m_chunk_selector->try_mark_chunk_to_gc_state(move_from_chunk,
                                                     priority == static_cast< uint8_t >(task_priority::emergent))) {
        auto [promise, future] = folly::makePromiseContract< bool >();

        if (sisl_unlikely(priority == static_cast< uint8_t >(task_priority::emergent))) {
            m_egc_executor->add([this, priority, move_from_chunk, promise = std::move(promise)]() mutable {
                LOGINFO("start emergent gc task : move_from_chunk_id={}, priority={}", move_from_chunk, priority);
                process_gc_task(move_from_chunk, priority, std::move(promise));
            });
        } else {
            m_gc_executor->add([this, priority, move_from_chunk, promise = std::move(promise)]() mutable {
                LOGINFO("start gc task : move_from_chunk_id={}, priority={}", move_from_chunk, priority);
                process_gc_task(move_from_chunk, priority, std::move(promise));
            });
        }
        return std::move(future);
    }

    LOGWARN("fail to submit gc task for chunk_id={}, priority={}", move_from_chunk, priority);
    return folly::makeSemiFuture< bool >(false);
}

// this method is expected to be called sequentially when replaying metablk, so we don't need to worry about the
// concurrency issue.
void GCManager::pdev_gc_actor::handle_recovered_gc_task(const GCManager::gc_task_superblk* gc_task) {
    chunk_id_t move_from_chunk = gc_task->move_from_chunk;
    chunk_id_t move_to_chunk = gc_task->move_to_chunk;
    uint8_t priority = gc_task->priority;

    // 1 we need to move the move_to_chunk out of the reserved chunk queue
    std::list< chunk_id_t > reserved_chunks;
    chunk_id_t chunk_id;
    for (; m_reserved_chunk_queue.read(chunk_id);) {
        if (chunk_id == move_to_chunk) {
            // we found the chunk to be moved, so we can stop reading
            break;
        }
        reserved_chunks.emplace_back(chunk_id);
    }
    // now we need to put the reserved chunks back to the reserved chunk queue
    for (const auto& reserved_chunk : reserved_chunks) {
        m_reserved_chunk_queue.blockingWrite(reserved_chunk);
    }

    // 2 we need to select the move_from_chunk out of per pg chunk heap in chunk selector if it is a gc task with
    // normal priority. for the task with emergent priority, it is already selected since it is now used for an open
    // shard.

    auto vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);
    RELEASE_ASSERT(vchunk, "can not find vchunk for pchunk {} in chunk selector!", move_from_chunk);
    RELEASE_ASSERT(vchunk->m_pg_id.has_value(), "chunk_id={} is expected to belong to a pg, but not !",
                   move_from_chunk);

    m_chunk_selector->try_mark_chunk_to_gc_state(move_from_chunk, true /* force */);
    // 3 now we can switch the two chunks.
    if (!replace_blob_index(move_from_chunk, move_to_chunk, priority)) {
        RELEASE_ASSERT(false,
                       "failed to handle recovered gc task for move_from_chunk={} to move_to_chunk={} with priority={}",
                       move_from_chunk, move_to_chunk, priority);
    }
}

bool GCManager::pdev_gc_actor::replace_blob_index(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
                                                  uint8_t priority) {
    return true;
}

sisl::sg_list GCManager::pdev_gc_actor::generate_shard_super_blk_sg_list(shard_id_t shard_id) {
    // TODO: do the buffer check before using it.
    auto raw_shard_sb = m_hs_home_object->_get_hs_shard(shard_id);
    RELEASE_ASSERT(raw_shard_sb, "can not find shard super blk for shard_id={} !!!", shard_id);

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

bool GCManager::pdev_gc_actor::copy_valid_data(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk, bool is_emergent) {
    return true;
}

void GCManager::pdev_gc_actor::purge_reserved_chunk(chunk_id_t chunk) {
    auto vchunk = m_chunk_selector->get_extend_vchunk(chunk);
    RELEASE_ASSERT(!vchunk->m_pg_id.has_value(),
                   "chunk_id={} is expected to be a reserved chunk, and not belong to a pg", chunk);
    vchunk->reset(); // reset the chunk to make sure it is empty

    // clear all the entries of this chunk in the gc index table
    auto start_key = BlobRouteByChunkKey{BlobRouteByChunk(chunk, 0, 0)};
    auto end_key = BlobRouteByChunkKey{
        BlobRouteByChunk{chunk, std::numeric_limits< uint64_t >::max(), std::numeric_limits< uint64_t >::max()}};
    homestore::BtreeRangeRemoveRequest< BlobRouteByChunkKey > range_remove_req{
        homestore::BtreeKeyRange< BlobRouteByChunkKey >{
            std::move(start_key), true /* inclusive */, std::move(end_key), true /* inclusive */
        }};
    auto status = m_index_table->remove(range_remove_req);
    if (status != homestore::btree_status_t::success) {
        // TODO:: handle the error case here!
        RELEASE_ASSERT(false, "can not clear gc index table, chunk_id={}", chunk);
    }
}

void GCManager::pdev_gc_actor::process_gc_task(chunk_id_t move_from_chunk, uint8_t priority,
                                               folly::Promise< bool > task) {
    LOGINFO("start process gc task for move_from_chunk={} with priority={} ", move_from_chunk, priority);

    // make chunk to gc state, so that it can be select for creating shard
    auto succeed = m_chunk_selector->try_mark_chunk_to_gc_state(
        move_from_chunk, priority == static_cast< uint8_t >(task_priority::emergent) /* force */);

    // the move_from_chunk probably now is used by an open shard, so we need to check if it can be marked as gc
    // state.
    if (!succeed) {
        LOGWARN("move_from_chunk={} is expected to be mark as gc state, but not!", move_from_chunk);
        task.setValue(false);
        return;
    }

    chunk_id_t move_to_chunk;
    // wait for a reserved chunk to be available
    m_reserved_chunk_queue.blockingRead(move_to_chunk);
    LOGINFO("gc task for move_from_chunk={} to move_to_chunk={} with priority={} start copying data", move_from_chunk,
            move_to_chunk, priority);

    purge_reserved_chunk(move_to_chunk);

    if (!copy_valid_data(move_from_chunk, move_to_chunk)) {
        LOGWARN("failed to copy data from move_from_chunk={} to move_to_chunk={} with priority={}", move_from_chunk,
                move_to_chunk, priority);
        task.setValue(false);
        return;
    }

    // trigger cp to make sure the offset the the append blk allocator and the wbcache of gc index table are both
    // flushed.
    auto fut = homestore::hs()->cp_mgr().trigger_cp_flush(true /* force */);
    LOGINFO("CP Flush {} after copy data from move_from_chunk {} to move_to_chunk {} with priority={}",
            std::move(fut).get() ? "success" : "failed", move_from_chunk, move_to_chunk, priority);

    // after data copy, we need persist the gc task meta blk, so that if crash happens, we can recover it after
    // restart.
    homestore::superblk< GCManager::gc_task_superblk > gc_task_sb{GCManager::_gc_task_meta_name};
    gc_task_sb.create(sizeof(GCManager::gc_task_superblk));
    gc_task_sb->move_from_chunk = move_from_chunk;
    gc_task_sb->move_to_chunk = move_to_chunk;
    gc_task_sb->priority = priority;
    // write the gc task meta blk to the meta service, so that it can be recovered after restart
    gc_task_sb.write();

    // after the data copy is done, we can switch the two chunks.
    LOGINFO("gc task for move_from_chunk={} to move_to_chunk={} with priority={} start switching chunk",
            move_from_chunk, move_to_chunk, priority);
    if (!replace_blob_index(move_from_chunk, move_to_chunk, priority)) {
        RELEASE_ASSERT(false, "failed to replace blob index, move_from_chunk={} to move_to_chunk={} with priority={}",
                       move_from_chunk, move_to_chunk, priority);
    }
    // TODO: change the chunk state of move_to_chunk to AVAILABLE so that it can be used for new shard.

    // now we can complete the task. for emergent gc, we need wait for the gc task to be completed
    gc_task_sb.destroy();
    task.setValue(true);
    LOGINFO("gc task for move_from_chunk={} to move_to_chunk={} with priority={} is completed", move_from_chunk,
            move_to_chunk, priority);
}

GCManager::pdev_gc_actor::~pdev_gc_actor() {
    stop();
    LOGINFO("gc actor for pdev_id={} is destroyed", m_pdev_id);
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
