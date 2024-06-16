#include "gc_manager.hpp"

#include <thread>

namespace homeobject {

void GCManager::start(uint32_t gc_defrag_refresh_interval_second) {
    if (!m_heap_chunk_selector) {
        LOGERRORMOD(
            homeobject,
            "GCManager have not been initialized!, please use initialzie_with_chunk_selector() to initialize it.");
        return;
    }

    // gc thread function
    auto gc_task_handler = [this](uint32_t pdev_id) {
        RELEASE_ASSERT(m_per_dev_gc_controller.contains(pdev_id), "controller of pdev {} is not initialized!", pdev_id);
        auto& controller = m_per_dev_gc_controller[pdev_id];
        // the reservced chunk is a brand new chunk. if the move_to chunk of a gc task is not specified, the reserved
        // chunk will be used as the move_to chunk.
        // here, select chunk is called before homeobject service start, so this will make sure we can select a brand
        // new chunk.
        homestore::blk_alloc_hints hint;
        hint.pdev_id_hint = pdev_id;
        VChunk reserved_chunk(m_heap_chunk_selector->select_chunk(0, hint));

        m_total_threads_num++;
        while (!m_stop_gc) {
            VChunk move_to_chunk(nullptr);
            VChunk move_from_chunk(nullptr);

            std::unique_lock< std::mutex > lock(controller.mtx);
            if (!controller.high_priority_gc_task_queue.empty()) {
                // TODO: add seperate new thread for high priority gc task if necessary, so that is can be processed
                // ASAP.
                std::tie(move_from_chunk, move_to_chunk) = controller.high_priority_gc_task_queue.front();
                controller.high_priority_gc_task_queue.pop();
            } else if (!controller.defrag_heap.empty()) {
                move_from_chunk = controller.defrag_heap.top();
                controller.defrag_heap.pop();
            } else {
                lock.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            lock.unlock();

            RELEASE_ASSERT(move_from_chunk.get_pdev_id() == pdev_id,
                           "currrent pdev_id is {}, but get a move_from_chunk with chunk_id {} and pdev_id {}, which "
                           "is not in the same pdev!",
                           pdev_id, move_from_chunk.get_chunk_id(), move_from_chunk.get_pdev_id());

            if (!m_heap_chunk_selector->select_specific_chunk(move_from_chunk.get_chunk_id())) {
                LOGWARNMOD(homeobject, "failed to select specific chunk with chunk_id {}",
                           move_from_chunk.get_chunk_id());
                continue;
            }

            if (!move_to_chunk.get_internal_chunk()) move_to_chunk = reserved_chunk;
            // handle gc task
            gc(move_from_chunk, move_to_chunk);
            m_total_gc_chunk_count++;

            // after gc, the move_from_chunk is empty, so we can use is as the new reserved chunk.
            // the move_to_chunk is not empty, so we need to release it.
            reserved_chunk = move_from_chunk;
            m_heap_chunk_selector->release_chunk(move_to_chunk.get_chunk_id());
        }
        m_total_threads_num--;
    };

    uint32_t gc_total_thread_num = 0;
    for (const auto& [pdev_id, controller] : m_per_dev_gc_controller) {
        // TODO: add gc thread pool for each pdev if necessary
        std::thread(gc_task_handler, pdev_id).detach();
        gc_total_thread_num++;
    }

    // wait until all the gc threads have selected a brand new chunk as the reserved move_to_chunk.
    while (m_total_threads_num < gc_total_thread_num) {
        // we can also use a condition variable to replace all the while wait and notify in this file.
        // we can change this if necessary.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // TODO:make the interval of refresh defrag heap and iolimiter configurable

    // start thread to refresh defrag_heap for each pdev periodically
    std::thread([this, gc_defrag_refresh_interval_second]() {
        m_total_threads_num++;
        while (!m_stop_gc) {
            for (const auto& [pdev_id, _] : m_per_dev_gc_controller)
                refresh_defrag_heap(pdev_id);

            // Sleep for a period of time before refreshing again
            std::this_thread::sleep_for(std::chrono::seconds(gc_defrag_refresh_interval_second));
        }
        m_total_threads_num--;
    }).detach();

    // start thread to refresh io_limiter for each pdev periodically
    std::thread([this]() {
        m_total_threads_num++;
        while (!m_stop_gc) {
            for (auto& [_, controller] : m_per_dev_gc_controller)
                controller.io_limiter.refresh();
            // Sleep for a period of time before refreshing again
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        m_total_threads_num--;
    }).detach();

    LOGINFOMOD(homeobject, "gc manager has been started with {} gc thread.", gc_total_thread_num);
}

void GCManager::stop() {
    m_stop_gc = true;
    // theoretically，some gc thread might not started before stop() is called.
    // TODO:make sure waiting for all gc thread exit gracefully
    while (m_total_threads_num > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    LOGINFOMOD(homeobject, "all GC threads have stopped.");
}

void GCManager::add_gc_task(VChunk& move_from_chunk, VChunk& move_to_chunk) {
    auto pdev_id = move_from_chunk.get_pdev_id();
    if (pdev_id != move_to_chunk.get_pdev_id()) {
        LOGWARNMOD(homeobject, "move_from_chunk(pdev_id {}) and move_to_chunk(pdev_id {}) should be in the same pdev!",
                   pdev_id, move_to_chunk.get_pdev_id());
        return;
    }
    LOGINFOMOD(homeobject, "high priority gc task is added, move_from_chunk: {}, move_to_chunk: {}, pdev_id: {}",
               move_from_chunk.get_chunk_id(), move_to_chunk.get_chunk_id(), pdev_id);
    std::unique_lock< std::mutex > lock(m_per_dev_gc_controller[pdev_id].mtx);
    m_per_dev_gc_controller[pdev_id].high_priority_gc_task_queue.emplace(move_from_chunk, move_to_chunk);
}

GCManager::GCManager(std::shared_ptr< HeapChunkSelector > chunk_selector) :
        m_heap_chunk_selector{chunk_selector}, metrics_{*this} {
    RELEASE_ASSERT(m_heap_chunk_selector, "chunk_selector is nullptr!");
    auto pdev_ids = m_heap_chunk_selector->get_all_pdev_ids();
    RELEASE_ASSERT(pdev_ids.size(), "chunk_selector is not initialized!");
    for (const auto& pdev_id : pdev_ids)
        m_per_dev_gc_controller[pdev_id];
}

void GCManager::refresh_defrag_heap(uint32_t pdev_id) {
    if (!m_heap_chunk_selector) {
        LOGERRORMOD(homeobject,
                    "GCManager has not been initialized, fail to refresh defrag heap for pdev {}. skip the operation",
                    pdev_id);
        return;
    }
    const auto& pdev_chunks = m_heap_chunk_selector->get_all_chunks(pdev_id);
    VChunkDefragHeap new_defrag_heap;
    for (const auto& chunk : pdev_chunks) {
        if (chunk.get_defrag_nblks()) new_defrag_heap.emplace(chunk);
    }
    new_defrag_heap.swap(m_per_dev_gc_controller[pdev_id].defrag_heap);
}

void GCManager::gc(VChunk move_from_chunk, VChunk move_to_chunk) {
    LOGINFOMOD(homeobject, "start gc chunk {} , move to chunk {}, pdev_id {}.", move_from_chunk.get_chunk_id(),
               move_to_chunk.get_chunk_id(), move_from_chunk.get_pdev_id());
    // TODO: implement gc logic
}

} // namespace homeobject