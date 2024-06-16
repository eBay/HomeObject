#pragma once

#include "heap_chunk_selector.h"

#include <sisl/utility/enum.hpp>

namespace homeobject {

class GCManager {
public:
    GCManager(std::shared_ptr< HeapChunkSelector >);

    void start(uint32_t);
    void stop();

    /**
     * @brief this is used for emergency or recovered GC task when restarting
     */
    void add_gc_task(VChunk&, VChunk&);

    ~GCManager() = default;

private:
    class VChunkDefragComparator {
    public:
        bool operator()(VChunk& lhs, VChunk& rhs) { return lhs.get_defrag_nblks() < rhs.get_defrag_nblks(); }
    };

    using VChunkDefragHeap = std::priority_queue< VChunk, std::vector< VChunk >, VChunkDefragComparator >;
    struct PerDevGCController {
        class IOLimiter {
        public:
            IOLimiter() : m_tokenBucket(1000), m_lastRefreshTime(std::chrono::steady_clock::now()) {}

            void refresh() {
                std::lock_guard< std::mutex > lock(m_mutex);
                auto currentTime = std::chrono::steady_clock::now();
                auto elapsedTime =
                    std::chrono::duration_cast< std::chrono::milliseconds >(currentTime - m_lastRefreshTime);
                m_tokenBucket += m_tokensPerSecond * elapsedTime.count();
                m_tokenBucket = std::min(m_tokenBucket, m_maxTokens);
                m_lastRefreshTime = currentTime;
            }

            // gc thread will consume tokens before doing IO to limit the IO resource occupied by gc for each pdev
            bool tryConsumeTokens(uint32_t tokens) {
                std::lock_guard< std::mutex > lock(m_mutex);
                if (m_tokenBucket >= tokens) {
                    m_tokenBucket -= tokens;
                    return true;
                }
                return false;
            }

        private:
            std::mutex m_mutex;
            uint32_t m_tokenBucket;
            // TODO: make m_maxTokens and m_tokensPerSecond configurable, and set them as optimized values after testing
            const uint32_t m_maxTokens = 1000;      // Maximum number of tokens in the bucket
            const uint32_t m_tokensPerSecond = 100; // Number of tokens added per millisecond
            std::chrono::steady_clock::time_point m_lastRefreshTime;
        };

        std::mutex mtx;
        IOLimiter io_limiter;
        VChunkDefragHeap defrag_heap;
        // emergency and recovered GC tasks will be put in this queue
        std::queue< std::pair< VChunk, VChunk > > high_priority_gc_task_queue;
    };

    void refresh_defrag_heap(uint32_t pdev_id);

    void gc(VChunk move_from_chunk, VChunk move_to_chunk);

    // metrics
    struct GCMetrics : public sisl::MetricsGroup {
    public:
        GCMetrics(GCManager const& gc_manager) : sisl::MetricsGroup{"GC", "Singelton"}, gc_manager{gc_manager} {
            // TODO:: add more metrics
            REGISTER_GAUGE(total_gc_chunk_count, "Number of chunks which has been GC");

            register_me_to_farm();
            attach_gather_cb(std::bind(&GCMetrics::on_gather, this));
        }
        ~GCMetrics() { deregister_me_from_farm(); }
        GCMetrics(const GCMetrics&) = delete;
        GCMetrics(GCMetrics&&) noexcept = delete;
        GCMetrics& operator=(const GCMetrics&) = delete;
        GCMetrics& operator=(GCMetrics&&) noexcept = delete;

        void on_gather() { GAUGE_UPDATE(*this, total_gc_chunk_count, gc_manager.total_gc_chunk_count()); }

    private:
        GCManager const& gc_manager;
    };

    uint64_t total_gc_chunk_count() const { return m_total_gc_chunk_count; }

private:
    GCManager(const GCManager&) = delete;
    GCManager& operator=(const GCManager&) = delete;

    std::shared_ptr< HeapChunkSelector > m_heap_chunk_selector;
    std::unordered_map< uint32_t, PerDevGCController > m_per_dev_gc_controller;
    std::atomic_ushort m_total_threads_num{0};
    std::atomic_bool m_stop_gc{false};

    // metrics
    GCMetrics metrics_;
    std::atomic_uint16_t m_total_gc_chunk_count{0};
};

} // namespace homeobject