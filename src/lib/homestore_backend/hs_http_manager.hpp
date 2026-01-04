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
#include <iomgr/io_environment.hpp>
#include <iomgr/http_server.hpp>

#include <folly/futures/Future.h>
#include <folly/container/EvictingCacheMap.h>
#include <chrono>
#include <atomic>

namespace homeobject {
class HSHomeObject;

class HttpManager {
public:
    HttpManager(HSHomeObject& ho);

private:
    void get_obj_life(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_malloc_stats(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void reconcile_leader(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void yield_leadership_to_follower(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_pg(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_pg_chunks(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void dump_chunk(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void dump_shard(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_shard(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void trigger_gc(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_gc_job_status(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);

#ifdef _PRERELEASE
    void crash_system(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
#endif

private:
    enum class GCJobStatus { RUNNING, COMPLETED, FAILED };

    struct GCJobInfo {
        std::string job_id;
        GCJobStatus status;
        std::optional< uint32_t > chunk_id;
        std::optional< uint32_t > pdev_id;
        std::optional< bool > result;
        std::optional< uint16_t > pg_id;

        // Statistics for batch GC jobs (all chunks)
        uint32_t total_chunks{0};
        uint32_t success_count{0};
        uint32_t failed_count{0};

        GCJobInfo(const std::string& id, std::optional< uint32_t > cid = std::nullopt,
                  std::optional< uint32_t > pid = std::nullopt) :
                job_id(id), status(GCJobStatus::RUNNING), chunk_id(cid), pdev_id(pid) {}
    };

    std::string generate_job_id();

private:
    HSHomeObject& ho_;
    std::atomic< uint64_t > job_counter_{0};
    std::mutex gc_job_mutex_;
    folly::EvictingCacheMap< std::string, std::shared_ptr< GCJobInfo > > gc_jobs_map_{100};
};
} // namespace homeobject