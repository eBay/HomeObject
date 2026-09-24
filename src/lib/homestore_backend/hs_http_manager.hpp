/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#pragma once

#include <iomgr/io_environment.hpp>
#include <sisl/http/http_server.hpp>
#include <httplib/httplib.h>

#include <sisl/async/task.hpp>
#include <sisl/async/coro.hpp>
#include <sisl/async/when_all.hpp>
#include <sisl/fds/lru_map.hpp>
#include <chrono>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <string>
#include <nlohmann/json.hpp>

namespace homeobject {
class HSHomeObject;

class HttpManager {
public:
    HttpManager(HSHomeObject& ho);

private:
    void get_obj_life(httplib::Request const& request, httplib::Response& response);
    void get_malloc_stats(httplib::Request const& request, httplib::Response& response);
    void reconcile_leader(httplib::Request const& request, httplib::Response& response);
    void yield_leadership_to_follower(httplib::Request const& request, httplib::Response& response);
    void trigger_snapshot_creation(httplib::Request const& request, httplib::Response& response);
    void get_pg(httplib::Request const& request, httplib::Response& response);
    void get_pg_chunks(httplib::Request const& request, httplib::Response& response);
    void dump_chunk(httplib::Request const& request, httplib::Response& response);
    void dump_shard(httplib::Request const& request, httplib::Response& response);
    void get_shard(httplib::Request const& request, httplib::Response& response);
    void flip_learner_flag(httplib::Request const& request, httplib::Response& response);
    void remove_member(httplib::Request const& request, httplib::Response& response);
    void clean_replace_member_task(httplib::Request const& request, httplib::Response& response);
    void list_pg_replace_member_task(httplib::Request const& request, httplib::Response& response);
    void reconcile_membership(httplib::Request const& request, httplib::Response& response);
    void get_pg_quorum(httplib::Request const& request, httplib::Response& response);
    void exit_pg(httplib::Request const& request, httplib::Response& response);
    void trigger_gc(httplib::Request const& request, httplib::Response& response);
    void get_gc_job_status(httplib::Request const& request, httplib::Response& response);
    sisl::async::task< std::monostate > trigger_gc_for_pg(uint16_t pg_id, const std::string& job_id);
    void get_job_status(const std::string& job_id, nlohmann::json& result);
    void trigger_pg_scrub(httplib::Request const& request, httplib::Response& response);
    void get_scrub_job_status(httplib::Request const& request, httplib::Response& response);
    void cancel_scrub_job(httplib::Request const& request, httplib::Response& response);

#ifdef _PRERELEASE
    void crash_system(httplib::Request const& request, httplib::Response& response);
#endif

private:
    enum class GCJobStatus { RUNNING, COMPLETED, FAILED };

    struct GCJobInfo {
        std::string job_id;
        GCJobStatus status;
        std::optional< uint16_t > pg_id;
        std::optional< uint32_t > chunk_id;

        // Statistics for batch GC jobs (all chunks)
        uint32_t total_chunks{0};
        uint32_t success_count{0};
        uint32_t failed_count{0};

        GCJobInfo(const std::string& id, std::optional< uint16_t > pgid = std::nullopt,
                  std::optional< uint32_t > cid = std::nullopt) :
                job_id(id), status(GCJobStatus::RUNNING), pg_id(pgid), chunk_id(cid) {}
    };

    enum class ScrubJobStatus { RUNNING, COMPLETED, FAILED, CANCELLED };

    struct ScrubJobInfo {
        std::string job_id;
        uint16_t pg_id;
        bool is_deep;

        // Mutable fields protected by mutex
        mutable std::mutex mtx_;
        ScrubJobStatus status;
        std::chrono::system_clock::time_point start_time;
        std::chrono::system_clock::time_point end_time;
        std::string error_message;
        nlohmann::json report_summary;

        // Flag to prevent status update after cancellation
        std::atomic< bool > is_cancelled{false};

        ScrubJobInfo(const std::string& id, uint16_t pgid, bool deep) :
                job_id(id),
                pg_id(pgid),
                is_deep(deep),
                status(ScrubJobStatus::RUNNING),
                start_time(std::chrono::system_clock::now()) {}

        // Thread-safe status update - returns false if already cancelled
        bool try_complete(ScrubJobStatus new_status, const std::string& error_msg = "",
                          const nlohmann::json& summary = nlohmann::json()) {
            std::lock_guard< std::mutex > lock(mtx_);
            if (is_cancelled.load(std::memory_order_acquire)) { return false; } // Already cancelled, reject update

            status = new_status;
            end_time = std::chrono::system_clock::now();
            error_message = error_msg;
            if (!summary.empty()) { report_summary = summary; }
            return true;
        }

        // Thread-safe cancel
        void cancel() {
            std::lock_guard< std::mutex > lock(mtx_);
            is_cancelled.store(true, std::memory_order_release);
            status = ScrubJobStatus::CANCELLED;
            end_time = std::chrono::system_clock::now();
            error_message = "Cancelled by user";
        }
    };

    std::string generate_job_id();
    nlohmann::json build_scrub_job_json(const std::shared_ptr< ScrubJobInfo >& job_info);

private:
    HSHomeObject& ho_;
    std::atomic< uint64_t > job_counter_{0};
    std::shared_mutex gc_job_mutex_;
    std::shared_mutex scrub_job_mutex_;

    // Keep status of the latest 100 jobs for query (formerly folly EvictingCacheMap).
    // get() does NOT refresh LRU — only set() promotes; matches write-ordered eviction.
    sisl::LruMap< std::string, std::shared_ptr< GCJobInfo > > gc_jobs_map_{100};
    sisl::LruMap< std::string, std::shared_ptr< ScrubJobInfo > > scrub_jobs_map_{100};
};
} // namespace homeobject
