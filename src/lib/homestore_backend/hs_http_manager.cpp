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
#include <boost/algorithm/string.hpp>
#include <sisl/version.hpp>
#include <sisl/settings/settings.hpp>

#include "hs_http_manager.hpp"
#include "hs_homeobject.hpp"

namespace homeobject {

HttpManager::HttpManager(HSHomeObject& ho) : ho_(ho) {
    using namespace Pistache;
    using namespace Pistache::Rest;

    LOGINFO("Setting up HomeObject HTTP routes");

    std::vector< iomgr::http_route > routes = {
        {Pistache::Http::Method::Get, "/api/v1/getObjLife",
         Pistache::Rest::Routes::bind(&HttpManager::get_obj_life, this)},
        {Pistache::Http::Method::Get, "/api/v1/mallocStats",
         Pistache::Rest::Routes::bind(&HttpManager::get_malloc_stats, this)},
        {Pistache::Http::Method::Post, "/api/v1/reconcile_leader",
         Pistache::Rest::Routes::bind(&HttpManager::reconcile_leader, this)},
        {Pistache::Http::Method::Post, "/api/v1/yield_leadership_to_follower",
         Pistache::Rest::Routes::bind(&HttpManager::yield_leadership_to_follower, this)},
#ifdef _PRERELEASE
        {Pistache::Http::Method::Post, "/api/v1/crashSystem",
         Pistache::Rest::Routes::bind(&HttpManager::crash_system, this)},
#endif
        {Pistache::Http::Method::Get, "/api/v1/pg", Pistache::Rest::Routes::bind(&HttpManager::get_pg, this)},
        {Pistache::Http::Method::Get, "/api/v1/chunks",
         Pistache::Rest::Routes::bind(&HttpManager::get_pg_chunks, this)},
        {Pistache::Http::Method::Get, "/api/v1/shard", Pistache::Rest::Routes::bind(&HttpManager::get_shard, this)},
        {Pistache::Http::Method::Get, "/api/v1/chunk/dump",
         Pistache::Rest::Routes::bind(&HttpManager::dump_chunk, this)},
        {Pistache::Http::Method::Get, "/api/v1/shard/dump",
         Pistache::Rest::Routes::bind(&HttpManager::dump_shard, this)},
        {Pistache::Http::Method::Get, "/api/v1/trigger_gc",
         Pistache::Rest::Routes::bind(&HttpManager::trigger_gc, this)},
        {Pistache::Http::Method::Get, "/api/v1/gc_job_status",
         Pistache::Rest::Routes::bind(&HttpManager::get_gc_job_status, this)}};

    auto http_server = ioenvironment.get_http_server();
    if (!http_server) {
        LOGERROR("http server not available");
        return;
    }
    try {
        http_server->setup_routes(routes);
    } catch (std::runtime_error const& e) { LOGERROR("setup routes failed, {}", e.what()); }
}

void HttpManager::get_obj_life(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    nlohmann::json j;
    sisl::ObjCounterRegistry::foreach ([&j](const std::string& name, int64_t created, int64_t alive) {
        std::stringstream ss;
        ss << "created=" << created << " alive=" << alive;
        j[name] = ss.str();
    });
    response.send(Pistache::Http::Code::Ok, j.dump());
}

void HttpManager::get_malloc_stats(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    response.send(Pistache::Http::Code::Ok, sisl::get_malloc_stats_detailed().dump(2));
}

void HttpManager::reconcile_leader(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    const auto pg_id_param = request.query().get("pg_id");
    int32_t pg_id = std::stoi(pg_id_param.value_or("-1"));
    LOGINFO("Received reconcile leader request for pg_id {}", pg_id);
    ho_.reconcile_pg_leader(pg_id);
    response.send(Pistache::Http::Code::Ok, "Reconcile leader request submitted");
}

void HttpManager::yield_leadership_to_follower(const Pistache::Rest::Request& request,
                                               Pistache::Http::ResponseWriter response) {
    const auto pg_id_param = request.query().get("pg_id");
    int32_t pg_id = std::stoi(pg_id_param.value_or("-1"));
    LOGINFO("Received yield leadership request for pg_id {} to follower", pg_id);
    ho_.yield_pg_leadership_to_follower(pg_id);
    response.send(Pistache::Http::Code::Ok, "Yield leadership request submitted");
}

void HttpManager::get_pg(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto pg_str = request.query().get("pg_id");
    if (!pg_str) {
        response.send(Pistache::Http::Code::Bad_Request, "pg_id is required");
        return;
    }
    uint16_t pg_id = std::stoul(pg_str.value());
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        response.send(Pistache::Http::Code::Not_Found, "pg not found");
        return;
    }
    auto peers = hs_pg->repl_dev_->get_replication_status();
    nlohmann::json json;
    json["pg"]["id"] = pg_id;
    json["pg"]["raft_group_id"] = boost::uuids::to_string(hs_pg->pg_info_.replica_set_uuid);
    json["pg"]["leader"] = boost::uuids::to_string(hs_pg->repl_dev_->get_leader_id());
    for (const auto& member : hs_pg->pg_info_.members) {
        nlohmann::json member_json;
        for (const auto p : peers) {
            if (p.id_ == member.id) {
                member_json["last_commit_lsn"] = p.replication_idx_;
                break;
            }
        }
        member_json["id"] = boost::uuids::to_string(member.id);
        member_json["name"] = member.name;
        json["pg"]["members"].push_back(member_json);
    }
    response.send(Pistache::Http::Code::Ok, json.dump());
}

void HttpManager::get_pg_chunks(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto pg_str = request.query().get("pg_id");
    if (!pg_str) {
        response.send(Pistache::Http::Code::Bad_Request, "pg_id is required");
        return;
    }
    uint16_t pg_id = std::stoul(pg_str.value());
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        response.send(Pistache::Http::Code::Not_Found, "pg not found");
        return;
    }
    auto json = ho_.chunk_selector()->dump_chunks_info(pg_id);
    json["pg"]["blk_size"] = hs_pg->repl_dev_->get_blk_size();
    response.send(Pistache::Http::Code::Ok, json.dump());
}

void HttpManager::get_shard(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto shard_str = request.query().get("shard_id");
    if (!shard_str) {
        response.send(Pistache::Http::Code::Bad_Request, "shard_id is required");
        return;
    }
    uint64_t shard_id = std::stoul(shard_str.value());
    nlohmann::json j;
    j["shard_id"] = shard_id;
    auto chk = ho_.get_shard_v_chunk_id(shard_id);
    if (!chk) {
        response.send(Pistache::Http::Code::Not_Found, "shard not found");
        return;
    }
    auto pchk = ho_.get_shard_p_chunk_id(shard_id);
    j["v_chunk_id"] = chk.value();
    j["p_chunk_id"] = pchk.value();
    pg_id_t pg_id = ho_.get_pg_id_from_shard_id(shard_id);
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        response.send(Pistache::Http::Code::Internal_Server_Error, "pg not found");
        return;
    }
    auto r = ho_.shard_manager()->get_shard(shard_id).get();
    if (!r) {
        response.send(Pistache::Http::Code::Internal_Server_Error, "failed to get shard");
        return;
    }
    j["created_time"] = r.value().created_time;
    j["state"] = r.value().state;
    j["lsn"] = r.value().lsn;
    auto blobs = ho_.get_shard_blobs(shard_id);
    if (!blobs) {
        response.send(Pistache::Http::Code::Internal_Server_Error, "failed to get shard blobs");
        return;
    }
    j["total_blob_count"] = blobs.value().size();

    response.send(Pistache::Http::Code::Ok, j.dump());
}

void HttpManager::dump_chunk(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto pg_str = request.query().get("pg_id");
    auto chunk_str = request.query().get("v_chunk_id");
    if (!pg_str || !chunk_str) {
        response.send(Pistache::Http::Code::Bad_Request, "pg_id and v_chunk_id are required");
        return;
    }
    uint16_t pg_id = std::stoul(pg_str.value());
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        response.send(Pistache::Http::Code::Not_Found, "pg not found");
        return;
    }
    uint16_t v_chunk_id = std::stoul(chunk_str.value());
    nlohmann::json j;
    j["v_chunk_id"] = v_chunk_id;
    auto shards = hs_pg->get_chunk_shards(v_chunk_id);
    for (auto const& s : shards) {
        nlohmann::json shard_json;
        shard_json["shard_id"] = s.info.id;
        shard_json["created_time"] = s.info.created_time;
        shard_json["state"] = s.info.state;
        shard_json["lsn"] = s.info.lsn;
        j["shards"].push_back(shard_json);
    }
    j["total_shard_count"] = shards.size();
    response.send(Pistache::Http::Code::Ok, j.dump());
}

void HttpManager::dump_shard(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto shard_str = request.query().get("shard_id");
    if (!shard_str) {
        response.send(Pistache::Http::Code::Bad_Request, "shard_id is required");
        return;
    }
    uint64_t shard_id = std::stoul(shard_str.value());
    nlohmann::json j;
    j["shard_id"] = shard_id;
    auto chk = ho_.get_shard_v_chunk_id(shard_id);
    if (!chk) {
        response.send(Pistache::Http::Code::Not_Found, "shard not found");
        return;
    }
    j["v_chunk_id"] = chk.value();
    auto r = ho_.get_shard_blobs(shard_id);
    if (!r) {
        response.send(Pistache::Http::Code::Internal_Server_Error, "failed to get shard blobs");
        return;
    }
    for (auto const& blob : r.value()) {
        nlohmann::json blob_json;
        blob_json["blob_id"] = blob.blob_id;
        blob_json["blk_count"] = blob.pbas.blk_count();
        j["blobs"].push_back(blob_json);
    }
    response.send(Pistache::Http::Code::Ok, j.dump());
}

void HttpManager::trigger_gc(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    const auto chunk_id_param = request.query().get("chunk_id");

    auto gc_mgr = ho_.gc_manager();
    if (!gc_mgr) {
        response.send(Pistache::Http::Code::Internal_Server_Error, "GC manager not available");
        return;
    }

    auto chunk_selector = ho_.chunk_selector();
    if (!chunk_selector) {
        response.send(Pistache::Http::Code::Internal_Server_Error, "Chunk selector not available");
        return;
    }

    std::string job_id = generate_job_id();
    nlohmann::json result;

    // trigger gc for all chunks
    if (!chunk_id_param || chunk_id_param.value().empty()) {
        LOGINFO("Received trigger_gc request for all chunks, job_id={}", job_id);

        auto job_info = std::make_shared< GCJobInfo >(job_id);
        {
            std::lock_guard< std::mutex > lock(gc_job_mutex_);
            gc_jobs_map_.set(job_id, job_info);
        }

        result["job_id"] = job_id;
        result["message"] = "GC triggered for all eligible chunks, pls query job status using gc_job_status API";
        response.send(Pistache::Http::Code::Accepted, result.dump());

        LOGINFO("GC job {} stopping GC scan timer", job_id);
        gc_mgr->stop_gc_scan_timer();

        std::vector< pg_id_t > pg_ids;
        ho_.get_pg_ids(pg_ids);
        LOGINFO("GC job {} will process {} PGs", job_id, pg_ids.size());

        std::vector< folly::SemiFuture< bool > > gc_task_futures;

        for (const auto& pg_id : pg_ids) {
            auto hs_pg = const_cast< HSHomeObject::HS_PG* >(ho_.get_hs_pg(pg_id));
            RELEASE_ASSERT(hs_pg, "HS PG {} not found during GC job {}", pg_id, job_id);

            LOGINFO("GC job {} draining pending GC tasks for PG {}", job_id, pg_id);
            gc_mgr->drain_pg_pending_gc_task(pg_id);

            auto pg_sb = hs_pg->pg_sb_.get();
            std::vector< homestore::chunk_num_t > pg_chunks(pg_sb->get_chunk_ids(),
                                                            pg_sb->get_chunk_ids() + pg_sb->num_chunks);

            LOGINFO("GC job {} processing PG {} with {} chunks", job_id, pg_id, pg_chunks.size());

            // Resume accepting new requests for this pg
            hs_pg->repl_dev_->quiesce_reqs();

            for (const auto& chunk_id : pg_chunks) {
                job_info->total_chunks++;
                // Determine priority based on chunk state (INUSE means has open shard)
                auto chunk = chunk_selector->get_extend_vchunk(chunk_id);
                RELEASE_ASSERT(chunk, "Chunk {} not found during GC job {}", chunk_id, job_id);
                auto priority = chunk->m_state == ChunkState::INUSE ? task_priority::emergent : task_priority::normal;

                // Clear in-memory requests only for emergent priority chunks (chunks with open shards)
                if (priority == task_priority::emergent) { hs_pg->repl_dev_->clear_chunk_req(chunk_id); }

                // Submit GC task for this chunk
                auto future = gc_mgr->submit_gc_task(priority, chunk_id);
                gc_task_futures.push_back(std::move(future));
                LOGDEBUG("GC job {} in PG {} with priority={}", job_id, chunk_id, pg_id,
                         (priority == task_priority::emergent) ? "emergent" : "normal");
            }
        }

        folly::collectAllUnsafe(gc_task_futures)
            .thenValue([job_info](auto&& results) {
                for (auto const& ok : results) {
                    RELEASE_ASSERT(ok.hasValue(), "we never throw any exception when copying data");
                    if (ok.value()) {
                        job_info->success_count++;
                    } else {
                        job_info->failed_count++;
                    }
                }
            })
            .thenValue([this, pg_ids, job_info, gc_mgr](auto&& rets) {
                LOGINFO("All GC tasks have been processed");
                const auto& job_id = job_info->job_id;
                for (const auto& pg_id : pg_ids) {
                    auto hs_pg = const_cast< HSHomeObject::HS_PG* >(ho_.get_hs_pg(pg_id));
                    RELEASE_ASSERT(hs_pg, "HS PG {} not found during GC job {}", pg_id, job_id);
                    // Resume accepting new requests for this pg
                    hs_pg->repl_dev_->resume_accepting_reqs();
                    LOGINFO("GC job {} resumed accepting requests for PG {}", job_id, pg_id);
                }

                job_info->result = (job_info->failed_count == 0);
                job_info->status = job_info->result.value() ? GCJobStatus::COMPLETED : GCJobStatus::FAILED;
                LOGINFO("GC job {} completed: total={}, success={}, failed={}", job_id, job_info->total_chunks,
                        job_info->success_count, job_info->failed_count);

                // Restart the GC scan timer
                LOGINFO("GC job {} restarting GC scan timer", job_id);
                gc_mgr->start_gc_scan_timer();
            });
    } else {
        // trigger gc for specific chunk
        uint32_t chunk_id = std::stoul(chunk_id_param.value());
        LOGINFO("Received trigger_gc request for chunk_id={}, job_id={}", chunk_id, job_id);

        auto chunk = chunk_selector->get_extend_vchunk(chunk_id);
        if (!chunk) {
            nlohmann::json error;
            error["chunk_id"] = chunk_id;
            error["error"] = "Chunk not found";
            response.send(Pistache::Http::Code::Not_Found, error.dump());
            return;
        }

        if (!chunk->m_pg_id.has_value()) {
            nlohmann::json error;
            error["chunk_id"] = chunk_id;
            error["error"] = "Chunk belongs to no pg";
            response.send(Pistache::Http::Code::Not_Found, error.dump());
            return;
        }

        const auto pg_id = chunk->m_pg_id.value();
        auto pdev_id = chunk->get_pdev_id();

        result["chunk_id"] = chunk_id;
        result["pdev_id"] = pdev_id;
        result["pg_id"] = pg_id;
        result["job_id"] = job_id;

        if (chunk->m_state == ChunkState::GC) {
            result["message"] = "chunk is already under GC now";
            response.send(Pistache::Http::Code::Accepted, result.dump());
            return;
        }

        // Check for active job and create new job atomically under the same lock
        auto job_info = std::make_shared< GCJobInfo >(job_id, chunk_id, pdev_id);
        {
            std::lock_guard< std::mutex > lock(gc_job_mutex_);
            gc_jobs_map_.set(job_id, job_info);
        }

        result["message"] = "GC triggered for chunk, pls query job status using gc_job_status API";
        response.send(Pistache::Http::Code::Accepted, result.dump());

        // Clear in-memory requests only for emergent priority chunks (chunks with open shards)
        auto hs_pg = const_cast< HSHomeObject::HS_PG* >(ho_.get_hs_pg(pg_id));
        RELEASE_ASSERT(hs_pg, "HS PG {} not found during GC job {}", pg_id, job_id);
        auto repl_dev = hs_pg->repl_dev_;
        repl_dev->quiesce_reqs();
        repl_dev->clear_chunk_req(chunk_id);
        const auto priority = chunk->m_state == ChunkState::INUSE ? task_priority::emergent : task_priority::normal;

        gc_mgr->submit_gc_task(priority, chunk_id)
            .via(&folly::InlineExecutor::instance())
            .thenValue([this, job_info, repl_dev](bool res) {
                job_info->result = res;
                job_info->status = res ? GCJobStatus::COMPLETED : GCJobStatus::FAILED;
                // Resume accepting new requests for this pg
                repl_dev->resume_accepting_reqs();
            });
    }
}

std::string HttpManager::generate_job_id() {
    auto counter = job_counter_.fetch_add(1, std::memory_order_relaxed);
    return fmt::format("trigger-gc-task-{}", counter);
}

void HttpManager::get_gc_job_status(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto job_id_param = request.query().get("job_id");
    if (!job_id_param) {
        response.send(Pistache::Http::Code::Bad_Request, "job_id is required");
        return;
    }

    std::string job_id = job_id_param.value();
    std::shared_ptr< GCJobInfo > job_info;
    {
        std::lock_guard< std::mutex > lock(gc_job_mutex_);
        job_info = gc_jobs_map_.get(job_id);
    }

    if (!job_info) {
        response.send(Pistache::Http::Code::Not_Found, "job_id not found, or job has been evicted");
        return;
    }

    // Access job_info outside the lock
    nlohmann::json result;
    result["job_id"] = job_info->job_id;

    switch (job_info->status) {
    case GCJobStatus::RUNNING:
        result["status"] = "running";
        break;
    case GCJobStatus::COMPLETED:
        result["status"] = "completed";
        break;
    case GCJobStatus::FAILED:
        result["status"] = "failed";
        break;
    }

    if (job_info->chunk_id.has_value()) {
        result["chunk_id"] = job_info->chunk_id.value();
        if (job_info->pdev_id.has_value()) { result["pdev_id"] = job_info->pdev_id.value(); }
    }

    if (job_info->total_chunks > 0) {
        nlohmann::json stats;
        stats["total_chunks"] = job_info->total_chunks;
        stats["success_count"] = job_info->success_count;
        stats["failed_count"] = job_info->failed_count;
        result["statistics"] = stats;
    }

    if (job_info->result.has_value()) { result["result"] = job_info->result.value(); }

    response.send(Pistache::Http::Code::Ok, result.dump());
}

#ifdef _PRERELEASE
void HttpManager::crash_system(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    std::string crash_type;
    const auto _crash_type{request.query().get("type")};
    if (_crash_type) { crash_type = _crash_type.value(); }

    std::string resp = "";
    if (crash_type.empty() || boost::iequals(crash_type, "assert")) {
        RELEASE_ASSERT(0, "Fake Assert in response to an http request");
    } else if (boost::iequals(crash_type, "segv")) {
        int* x{nullptr};
        LOGINFO("Simulating a segv with dereferencing nullptr={}", *x);
    } else {
        resp = "crash type " + crash_type + " not supported yet";
    }
    response.send(Pistache::Http::Code::Ok, resp);
}
#endif

} // namespace homeobject
