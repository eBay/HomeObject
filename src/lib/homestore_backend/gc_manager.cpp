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
    // this will only be called in the metablk#readsub, so it is guaranteed to be called sequentially

    // here, we are under the protection of the lock of metaservice. however, we will also try to update pg and shard
    // metablk and then destory the gc_task_sb, which will also try to acquire the lock of metaservice, as a result, a
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
    auto chunk_id = reserved_chunk_sb.load(buf, meta_cookie)->chunk_id;
    auto EXVchunk = m_chunk_selector->get_extend_vchunk(chunk_id);
    if (EXVchunk == nullptr) {
        LOGW("the disk for chunk {} is not found, probably lost, skip recovering gc mateblk for this chunk!", chunk_id);
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
        LOGINFO("start gc actor for pdev={}", pdev_id);
    }

    const auto gc_scan_interval_sec = HS_BACKEND_DYNAMIC_CONFIG(gc_scan_interval_sec);

    m_gc_timer_hdl = iomanager.schedule_global_timer(
        gc_scan_interval_sec * 1000 * 1000 * 1000, true, nullptr /*cookie*/, iomgr::reactor_regex::all_user,
        [this](void*) { scan_chunks_for_gc(); }, true /* wait_to_schedule */);
    LOGINFO("gc scheduler timer has started, interval is set to {} seconds", gc_scan_interval_sec);
}

bool GCManager::is_started() { return m_gc_timer_hdl != iomgr::null_timer_handle; }

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
    if (!is_started()) return folly::makeFuture< bool >(false);
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

    const auto defrag_blk_num = chunk->get_defrag_nblks();

    if (!defrag_blk_num) return false;

    // 1 if the chunk state is inuse, it is occupied by a open shard, so it can not be selected and we don't need gc it.
    // 2 if the chunk state is gc, it means this chunk is being gc, or this is a reserved chunk, so we don't need gc it.
    if (chunk->m_state != ChunkState::AVAILABLE) {
        LOGDEBUG("chunk_id={} state is {}, not eligible for gc", chunk_id, chunk->m_state)
        return false;
    }
    // it does not belong to any pg, so we don't need to gc it.
    if (!chunk->m_pg_id.has_value()) {
        LOGDEBUG("chunk_id={} belongs to no pg, not eligible for gc", chunk_id)
        return false;
    }

    LOGDEBUG("chunk_id={} is eligible for gc, belongs to pg {}", chunk_id, chunk->m_pg_id.value());
    const auto total_blk_num = chunk->get_total_blks();

    const auto gc_garbage_rate_threshold = HS_BACKEND_DYNAMIC_CONFIG(gc_garbage_rate_threshold);

    // defrag_blk_num > (GC_THRESHOLD_PERCENT/100) * total_blk_num, to avoid floating point number calculation
    // TODO: avoid overflow here.
    return 100 * defrag_blk_num >= total_blk_num * gc_garbage_rate_threshold;
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
        m_reserved_chunk_queue{HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev)},
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

void GCManager::pdev_gc_actor::add_reserved_chunk(
    homestore::superblk< GCManager::gc_reserved_chunk_superblk > reserved_chunk_sb) {
    auto chunk_id = reserved_chunk_sb->chunk_id;
    // mark a reserved chunk as gc state, so that it will not be selected as a gc candidate
    m_chunk_selector->try_mark_chunk_to_gc_state(chunk_id, true /* force */);
    m_reserved_chunks.emplace_back(std::move(reserved_chunk_sb));
    m_reserved_chunk_queue.blockingWrite(chunk_id);
    LOGDEBUG("chunk_id={} is added to reserved chunk queue", chunk_id);
}

folly::SemiFuture< bool > GCManager::pdev_gc_actor::add_gc_task(uint8_t priority, chunk_id_t move_from_chunk) {
    if (m_chunk_selector->try_mark_chunk_to_gc_state(move_from_chunk,
                                                     priority == static_cast< uint8_t >(task_priority::emergent))) {
        auto [promise, future] = folly::makePromiseContract< bool >();

        if (sisl_unlikely(priority == static_cast< uint8_t >(task_priority::emergent))) {
            m_egc_executor->add([this, priority, move_from_chunk, promise = std::move(promise)]() mutable {
                LOGDEBUG("start emergent gc task : move_from_chunk_id={}, priority={}", move_from_chunk, priority);
                process_gc_task(move_from_chunk, priority, std::move(promise));
            });
        } else {
            m_gc_executor->add([this, priority, move_from_chunk, promise = std::move(promise)]() mutable {
                LOGDEBUG("start gc task : move_from_chunk_id={}, priority={}", move_from_chunk, priority);
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
void GCManager::pdev_gc_actor::handle_recovered_gc_task(
    homestore::superblk< GCManager::gc_task_superblk >& gc_task_sb) {

    const chunk_id_t move_from_chunk = gc_task_sb->move_from_chunk;
    const chunk_id_t move_to_chunk = gc_task_sb->move_to_chunk;
    const uint8_t priority = gc_task_sb->priority;

    LOGDEBUG("start handling recovered gc task: move_from_chunk_id={}, move_to_chunk_id={}, priority={}",
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

    move_from_vchunk->m_pg_id = gc_task_sb->pg_id;
    move_from_vchunk->m_v_chunk_id = gc_task_sb->vchunk_id;

    move_to_vchunk->m_pg_id = std::nullopt;
    move_to_vchunk->m_v_chunk_id = std::nullopt;

    std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > > valid_blob_indexes;

    if (!get_blobs_to_replace(move_to_chunk, valid_blob_indexes)) {
        RELEASE_ASSERT(false, "failed to get valid blob indexes from gc index table for move_to_chunk={} when recovery",
                       move_to_chunk);
    }

    if (!process_after_gc_metablk_persisted(gc_task_sb, valid_blob_indexes)) {
        RELEASE_ASSERT(false,
                       "failed to process after gc metablk persisted when recovery, "
                       "move_from_chunk={}, move_to_chunk={}, priority={}",
                       move_from_chunk, move_to_chunk, priority);
    }

    LOGDEBUG("finish handling recovered gc task: move_from_chunk_id={}, move_to_chunk_id={}, priority={}",
             move_from_chunk, move_to_chunk, priority);
}

bool GCManager::pdev_gc_actor::get_blobs_to_replace(
    chunk_id_t move_to_chunk, std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes) {

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
        // TODO:: handle the error case here.
        LOGERROR("Failed to query blobs in gc index table for move_to_chunk={}, index ret={}", move_to_chunk, ret);
        return false;
    }

    return true;
}

bool GCManager::pdev_gc_actor::replace_blob_index(
    chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
    const std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes) {

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
            [&pg_id, &shard, &blob](homestore::BtreeKey const& key, homestore::BtreeValue const& value_in_btree,
                                    homestore::BtreeValue const& new_value) -> homestore::put_filter_decision {
                BlobRouteValue existing_value{value_in_btree};
                if (existing_value.pbas() == HSHomeObject::tombstone_pbas) {
                    LOGDEBUG(
                        "remove tombstone when updating pg index after data copy , pg_id={}, shard_id={}, blob_id={}",
                        pg_id, shard, blob);
                    BlobRouteValue new_pba_value{new_value};
                    homestore::data_service().async_free_blk(new_pba_value.pbas());
                    return homestore::put_filter_decision::remove;
                }
                return homestore::put_filter_decision::replace;
            }};

        const auto ret = pg_index_table->put(update_req);

        // 1 if the key exist, and the filter returns homestore::put_filter_decision::replace, then ret will be
        // homestore::btree_status_t::success

        // 2 if the key exist , and the filter returns homestore::put_filter_decision::remove,  the ret will be
        // homestore::btree_status_t::filtered_out.(this might happen is a key is deleted after data copy but before
        // replace index)

        // 3 if the key doest not exist, the ret will be homestore::btree_status_t::not_found(this might
        // happen when crash recovery)

        if (ret != homestore::btree_status_t::success && ret != homestore::btree_status_t::filtered_out &&
            ret != homestore::btree_status_t::not_found) {
            LOGERROR(
                "Failed to update blob in pg index table for move_from_chunk={}, error status = {}, move_to_chunk={}",
                move_from_chunk, ret, move_to_chunk);
            // pg index table might be partial updated, we can not put move_to_chunk back to the queue
            // m_reserved_chunk_queue.blockingWrite(move_to_chunk);
            return false;
        }
        LOGDEBUG("update index table for pg={}, ret={}, move_from_chunk={}, move_to_chunk={}, shard={}, blob={}", pg_id,
                 ret, move_from_chunk, move_to_chunk, shard, blob);
    }

    // TODO:: revisit the following part with the consideration of persisting order for recovery.

    // 3 update pg metablk and related in-memory data structures
    m_hs_home_object->update_pg_meta_after_gc(pg_id, move_from_chunk, move_to_chunk);

    // 4 update shard metablk and related in-memory data structures
    m_hs_home_object->update_shard_meta_after_gc(move_from_chunk, move_to_chunk);

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

// note that, when we copy data, there is not create shard or put blob in this chunk, only delete blob might happen.
bool GCManager::pdev_gc_actor::copy_valid_data(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk, bool is_emergent) {
    auto move_to_vchunk = m_chunk_selector->get_extend_vchunk(move_to_chunk);
    auto move_from_vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);

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
        LOGWARN("no shard found in move_from_chunk, chunk_id={}, ", move_from_chunk);
        return true;
    }

    auto& data_service = homestore::data_service();

    const auto last_shard_id = *(shards.rbegin());
    const auto& shard_info = m_hs_home_object->_get_hs_shard(last_shard_id)->info;
    const auto& shard_state = shard_info.state;

    // the last shard that triggers emergent gc should be in open state
    if (shard_state != ShardInfo::State::OPEN && is_emergent) {
        LOGERROR("shard state is not open for emergent gc, shard_id={} !!!", last_shard_id);
        return false;
    }

    homestore::blk_alloc_hints hints;
    hints.chunk_id_hint = move_to_chunk;
    homestore::MultiBlkId out_blkids;

    const auto pg_id = shard_info.placement_group;
    auto pg_index_table = m_hs_home_object->get_hs_pg(pg_id)->index_table_;
    auto blk_size = data_service.get_blk_size();

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
            LOGWARN("can not range remove blobs with tombstone in pg index table , pg_id={}, status={}", pg_id, status);
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
            LOGERROR("Failed to query blobs in index table for status={} shard={}", status, shard_id);
            return false;
        }

        const auto is_last_shard_in_emergent_chunk = is_emergent && is_last_shard;

        if (valid_blob_indexes.empty()) {
            LOGDEBUG("empty shard found in move_from_chunk, chunk_id={}, shard_id={}", move_from_chunk, shard_id);
            // TODO::send a delete shard request to raft channel. there is a case that when we are doing gc, the
            // shard becomes empty, need to handle this case

            // we should always write a shard header for the last shard of emergent gc.
            if (!is_last_shard_in_emergent_chunk) continue;
        } else {
            LOGDEBUG("{} valid blobs found in move_from_chunk, chunk_id={}, shard_id={}", valid_blob_indexes.size(),
                     move_from_chunk, shard_id);
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
                .thenValue([this, &hints, &move_to_chunk, &move_from_chunk, &is_emergent, &is_last_shard, &shard_id,
                            &blk_size, &valid_blob_indexes, &data_service,
                            header_sgs = std::move(header_sgs)](auto&& err) {
                    RELEASE_ASSERT(header_sgs.iovs.size() == 1, "header_sgs.iovs.size() should be 1, but not!");
                    iomanager.iobuf_free(reinterpret_cast< uint8_t* >(header_sgs.iovs[0].iov_base));
                    if (err) {
                        LOGERROR("Failed to write shard header for move_to_chunk={} shard_id={}, err={}", move_to_chunk,
                                 shard_id, err.value());
                        return folly::makeFuture< bool >(false);
                    }

                    if (valid_blob_indexes.empty()) {
                        RELEASE_ASSERT(is_emergent,
                                       "find empty shard in move_from_chunk={} "
                                       "but is_emergent is false, shard_id={}",
                                       move_from_chunk, shard_id);
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
                                .thenValue([this, k, &hints, &move_from_chunk, &move_to_chunk, &data_service,
                                            data_sgs = std::move(data_sgs)](auto&& err) {
                                    RELEASE_ASSERT(data_sgs.iovs.size() == 1,
                                                   "data_sgs.iovs.size() should be 1, but not!");
                                    if (err) {
                                        LOGERROR("Failed to read blob from move_from_chunk={}, shard_id={}, "
                                                 "blob_id={}: err={}",
                                                 move_from_chunk, k.key().shard, k.key().blob, err.value());
                                        iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                                        return folly::makeFuture< bool >(false);
                                    }

                                    // write the blob to the move_to_chunk. we do not care about the blob order in a
                                    // shard since we can not guarantee a certain order
                                    homestore::MultiBlkId new_pba;
                                    return data_service.async_alloc_write(data_sgs, hints, new_pba)
                                        .thenValue([this, k, new_pba, &move_to_chunk,
                                                    data_sgs = std::move(data_sgs)](auto&& err) {
                                            RELEASE_ASSERT(data_sgs.iovs.size() == 1,
                                                           "data_sgs.iovs.size() should be 1, but not!");
                                            iomanager.iobuf_free(
                                                reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                                            if (err) {
                                                LOGERROR("Failed to write blob to move_to_chunk={}, shard_id={}, "
                                                         "blob_id={}, err={}",
                                                         move_to_chunk, k.key().shard, k.key().blob, err.value());
                                                return false;
                                            }

                                            // insert a new entry to gc index table for this blob. [move_to_chunk_id,
                                            // shard_id, blob_id] -> [new pba]
                                            BlobRouteByChunkKey key{
                                                BlobRouteByChunk{move_to_chunk, k.key().shard, k.key().blob}};
                                            BlobRouteValue value{new_pba}, existing_value;

                                            homestore::BtreeSinglePutRequest put_req{
                                                &key, &value, homestore::btree_put_type::INSERT, &existing_value};
                                            auto status = m_index_table->put(put_req);
                                            if (status != homestore::btree_status_t::success) {
                                                LOGERROR("Failed to insert new key to gc index table for "
                                                         "move_to_chunk={}, shard_id={}, blob_id={}, err={}",
                                                         move_to_chunk, k.key().shard, k.key().blob, status);
                                                return false;
                                            }
                                            LOGDEBUG("successfully insert new key to gc index table for "
                                                     "move_to_chunk={}, shard_id={}, blob_id={}",
                                                     move_to_chunk, k.key().shard, k.key().blob);
                                            return true;
                                        });
                                })));
                    }

                    // 3 write a shard footer for this shard
                    sisl::sg_list footer_sgs = generate_shard_super_blk_sg_list(shard_id);
                    return folly::collectAllUnsafe(futs)
                        .thenValue([this, &is_emergent, &is_last_shard, &shard_id, &blk_size, &hints, &move_to_chunk,
                                    &data_service, footer_sgs](auto&& results) {
                            for (auto const& ok : results) {
                                RELEASE_ASSERT(ok.hasValue(), "we never throw any exception when copying data");
                                if (!ok.value()) {
                                    // if any op fails, we drop this gc task.
                                    return folly::makeFuture< std::error_code >(
                                        std::make_error_code(std::errc::operation_canceled));
                                }
                            }

                            // the shard that triggers emergent gc should be the last shard in the chunk, so it should
                            // be open and we skip writing the footer for this case.
                            if (is_emergent && is_last_shard) {
                                LOGDEBUG(
                                    "skip writing the footer for move_to_chunk={} shard_id={} for emergent gc task",
                                    move_to_chunk, shard_id);
                                return folly::makeFuture< std::error_code >(std::error_code{});
                            }
                            homestore::MultiBlkId out_blkids;
                            return data_service.async_alloc_write(footer_sgs, hints, out_blkids);
                        })
                        .thenValue([this, &move_to_chunk, &shard_id, footer_sgs](auto&& err) {
                            RELEASE_ASSERT(footer_sgs.iovs.size() == 1, "footer_sgs.iovs.size() should be 1, but not!");
                            iomanager.iobuf_free(reinterpret_cast< uint8_t* >(footer_sgs.iovs[0].iov_base));

                            if (err) {
                                LOGERROR("Failed to write shard footer for move_to_chunk={} shard_id={}, "
                                         "err={}",
                                         move_to_chunk, shard_id, err.value());
                                return false;
                            }
                            return true;
                        });
                })
                .get();

        if (!succeed_copying_shard) {
            LOGERROR("Failed to copy all blobs from move_from_chunk={} to move_to_chunk={} for shard_id={}",
                     move_from_chunk, move_to_chunk, shard_id);
            return false;
        }

        LOGDEBUG("successfully copy blobs from move_from_chunk={} to move_to_chunk={} for shard_id={}", move_from_chunk,
                 move_to_chunk, shard_id);
    }

    LOGDEBUG("all valid blobs are copied from move_from_chunk={} to move_to_chunk={}", move_from_chunk, move_to_chunk);

    // we need to commit_blk for the move_to_chunk to make sure the last offset of append_blk_allocator is updated.
    // However, we don`t know the exact last blk in move_to_chunk. for normal, we can use the footer blk of the last
    // shard as the last blk. But, for emergent gc, all the blks in the last shard are written concurrently and there is
    // no footer for the last shard. so we use a fake multiblk here to make sure the append_blk_allocator is committed
    // to the exact last offset.
    homestore::MultiBlkId commit_blk_id(0, move_to_vchunk->get_used_blks(), move_to_chunk);
    if (data_service.commit_blk(commit_blk_id) != homestore::BlkAllocStatus::SUCCESS) {
        LOGERROR("fail to commit_blk for move_to_chunk={}, move_from_chunk={}", move_to_chunk, move_from_chunk);
        return false;
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
            LOGWARN("fail to remove tombstone for  pg_id={}, shard={}, ret={}", pg_id, shard_id, status);
        }
        // TODO:: after the completion of indexsvc bug fix, we need to retry according to the returned status.

        LOGDEBUG("remove tombstone for pg={}, shard={}, ret={}, move_from_chunk={}, move_to_chunk={},", pg_id, shard_id,
                 status, move_from_chunk, move_to_chunk);
    }

    return true;
}

bool GCManager::pdev_gc_actor::purge_reserved_chunk(chunk_id_t chunk) {
    auto vchunk = m_chunk_selector->get_extend_vchunk(chunk);
    RELEASE_ASSERT(!vchunk->m_pg_id.has_value(),
                   "chunk_id={} is expected to be a reserved chunk, and not belong to a pg", chunk);
    RELEASE_ASSERT(vchunk->m_state == ChunkState::GC,
                   "chunk_id={} is a reserved chunk, expected to have a GC state, but actuall state is {} ", chunk,
                   vchunk->m_state);
    vchunk->reset(); // reset the chunk to make sure it is empty

    // clear all the entries of this chunk in the gc index table
    auto start_key = BlobRouteByChunkKey{BlobRouteByChunk(chunk, 0, 0)};
    auto end_key = BlobRouteByChunkKey{
        BlobRouteByChunk{chunk, std::numeric_limits< uint64_t >::max(), std::numeric_limits< uint64_t >::max()}};

    homestore::BtreeRangeRemoveRequest< BlobRouteByChunkKey > range_remove_req{
        homestore::BtreeKeyRange< BlobRouteByChunkKey >{
            std::move(start_key), true /* inclusive */
            ,
            std::move(end_key), true /* inclusive */
        }};

    auto status = m_index_table->remove(range_remove_req);
    if (status != homestore::btree_status_t::success &&
        status != homestore::btree_status_t::not_found /*already empty*/) {
        LOGWARN("fail to purge gc index for chunk={}", chunk);
        return false;
    }

    return true;
}

void GCManager::pdev_gc_actor::process_gc_task(chunk_id_t move_from_chunk, uint8_t priority,
                                               folly::Promise< bool > task) {
    LOGDEBUG("start process gc task for move_from_chunk={} with priority={} ", move_from_chunk, priority);
    auto vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);

    if (vchunk->m_state != ChunkState::GC) {
        LOGWARN("move_from_chunk={} is expected to in GC state, but not!", move_from_chunk);
        task.setValue(false);
        return;
    }

    RELEASE_ASSERT(vchunk->m_pg_id.has_value(), "chunk_id={} is expected to belong to a pg, but not!", move_from_chunk);
    const auto pg_id = vchunk->m_pg_id.value();

    RELEASE_ASSERT(vchunk->m_v_chunk_id.has_value(), "pg={}, chunk_id={} is expected to have a vchunk id, but not!",
                   pg_id, move_from_chunk);
    const auto vchunk_id = vchunk->m_v_chunk_id.value();

    chunk_id_t move_to_chunk;

    // wait for a reserved chunk to be available. now, the amount of threads in the folly executor(thread pool) is equal
    // to the amount of reserved number, so we can make sure that a gc task handle thread can always get a reserved
    // chunk, so acutally the blockingRead here will not block in any case and return immediately.
    m_reserved_chunk_queue.blockingRead(move_to_chunk);
    LOGDEBUG("gc task for move_from_chunk={} to move_to_chunk={} with priority={} start copying data", move_from_chunk,
             move_to_chunk, priority);

    if (!purge_reserved_chunk(move_to_chunk)) {
        LOGWARN("can not purge move_to_chunk={}", move_to_chunk);
        task.setValue(false);
        m_reserved_chunk_queue.blockingWrite(move_to_chunk);
        return;
    }

    if (!copy_valid_data(move_from_chunk, move_to_chunk, priority == static_cast< uint8_t >(task_priority::emergent))) {
        LOGWARN("failed to copy data from move_from_chunk={} to move_to_chunk={} with priority={}", move_from_chunk,
                move_to_chunk, priority);
        task.setValue(false);
        m_reserved_chunk_queue.blockingWrite(move_to_chunk);
        return;
    }

    std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > > valid_blob_indexes;
    if (!get_blobs_to_replace(move_to_chunk, valid_blob_indexes)) {
        LOGWARN("failed to get valid blob indexes from gc index table for move_to_chunk={}", move_to_chunk);
        task.setValue(false);
        m_reserved_chunk_queue.blockingWrite(move_to_chunk);
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

    LOGDEBUG("gc task for move_from_chunk={} to move_to_chunk={} with priority={} start replacing blob index",
             move_from_chunk, move_to_chunk, priority);

    if (!process_after_gc_metablk_persisted(gc_task_sb, valid_blob_indexes)) {
        // TODO::add a method to restore the old index if any error happen when replacing blob index
        RELEASE_ASSERT(false,
                       "Fail to process after gc metablk persisted, "
                       "move_from_chunk={}, move_to_chunk={}, priority={}",
                       move_from_chunk, move_to_chunk, priority);
    }

    task.setValue(true);
    m_reserved_chunk_queue.blockingWrite(move_from_chunk);
    LOGINFO("gc task for move_from_chunk={} to move_to_chunk={} with priority={} is completed", move_from_chunk,
            move_to_chunk, priority);
}

bool GCManager::pdev_gc_actor::process_after_gc_metablk_persisted(
    homestore::superblk< GCManager::gc_task_superblk >& gc_task_sb,
    const std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes) {
    const chunk_id_t move_from_chunk = gc_task_sb->move_from_chunk;
    const chunk_id_t move_to_chunk = gc_task_sb->move_to_chunk;
    const uint8_t priority = gc_task_sb->priority;
    const auto pg_id = gc_task_sb->pg_id;
    const auto vchunk_id = gc_task_sb->vchunk_id;

    if (!replace_blob_index(move_from_chunk, move_to_chunk, valid_blob_indexes)) {
        // if we fail to replace blob index, the worst case is some of the valid blobs index is update, but others not.
        // At this moment, we can not drop any one of move_from_chunk and move_to_chunk, since they both contains valid
        // blob data. we can not go ahead
        LOGERROR("failed to replace blob index, move_from_chunk={} to move_to_chunk={} with priority={}",
                 move_from_chunk, move_to_chunk, priority);
        return false;
    }

#ifdef _PRERELEASE
    // now, the move_from_chunk is not put back to the reserved chunk queue, we simulate this
    if (iomgr_flip::instance()->test_flip("simulate_gc_crash_recovery")) {
        // don`t not delete gc task sb to simlulate_gc_crash_recovery
        LOGINFO("gc task superblk is not deleted to simulate recovery");
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

    // if the state of move_to_chunk is updated to inuse or available, then it might be selected for gc immediately. if
    // we change the state of move_to_chunk before gc_task_sb is destroyed, when crash recovery and redo the gc task,
    // the move_to_chunk will probably has new data written into it, which is different from the data we copied from
    // move_from_chunk. so, we need to switch the chunk state after gc_task_sb is destroyed.

    auto move_to_vchunk = m_chunk_selector->get_extend_vchunk(move_to_chunk);
    auto move_from_vchunk = m_chunk_selector->get_extend_vchunk(move_from_chunk);

    // 1 change the pg_id and vchunk_id of the move_to_chunk according to metablk
    move_to_vchunk->m_pg_id = pg_id;
    move_to_vchunk->m_v_chunk_id = vchunk_id;

    // 2 update the chunk state of move_from_chunk, now it is a reserved chunk
    move_from_vchunk->m_pg_id = std::nullopt;
    move_from_vchunk->m_v_chunk_id = std::nullopt;
    move_from_vchunk->m_state = ChunkState::GC;

    // 3 update the state of move_to_chunk, so that it can be used for creating shard or putting blob. we need to do
    // this after reserved_chunk meta blk is updated, so that if crash happens, we recovery the move_to_chunk is the
    // same as that before crash. here, the same means not new put_blob or create_shard happens to it, the data on the
    // chunk is the same as before.
    move_to_vchunk->m_state =
        priority == static_cast< uint8_t >(task_priority::normal) ? ChunkState::AVAILABLE : ChunkState::INUSE;

    LOGDEBUG("move_to_chunk={} state is updated to {}", move_to_chunk, move_to_vchunk->m_state);

    return true;
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
