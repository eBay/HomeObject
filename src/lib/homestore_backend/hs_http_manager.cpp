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
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <ctime>
#include <limits>
#include <string>

#include "hs_http_manager.hpp"
#include "hs_homeobject.hpp"

namespace homeobject {

namespace {
// Helper function to format time as ISO 8601
std::string format_iso8601_time(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm;
    gmtime_r(&time_t, &tm); // Thread-safe version
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return std::string(buf);
}

// Helper to count total items across peer map
template < typename PeerMap >
size_t count_peer_map_items(const PeerMap& peer_map) {
    size_t count = 0;
    for (const auto& [peer_id, items] : peer_map) {
        count += items.size();
    }
    return count;
}
} // anonymous namespace

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
        {Pistache::Http::Method::Get, "/api/v1/pg_quorum",
         Pistache::Rest::Routes::bind(&HttpManager::get_pg_quorum, this)},
        {Pistache::Http::Method::Post, "/api/v1/flip_learner",
         Pistache::Rest::Routes::bind(&HttpManager::flip_learner_flag, this)},
        {Pistache::Http::Method::Delete, "/api/v1/member",
         Pistache::Rest::Routes::bind(&HttpManager::remove_member, this)},
        {Pistache::Http::Method::Delete, "/api/v1/pg_replacemember_task",
         Pistache::Rest::Routes::bind(&HttpManager::clean_replace_member_task, this)},
        {Pistache::Http::Method::Get, "/api/v1/pg_replacemember_tasks",
         Pistache::Rest::Routes::bind(&HttpManager::list_pg_replace_member_task, this)},
        {Pistache::Http::Method::Post, "/api/v1/reconcile_membership",
         Pistache::Rest::Routes::bind(&HttpManager::reconcile_membership, this)},
        {Pistache::Http::Method::Delete, "/api/v1/pg", Pistache::Rest::Routes::bind(&HttpManager::exit_pg, this)},
        {Pistache::Http::Method::Post, "/api/v1/trigger_snapshot_creation",
         Pistache::Rest::Routes::bind(&HttpManager::trigger_snapshot_creation, this)},
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

        // we support triggering gc for:
        // 1 all the chunks in all the pg: no input param
        // 2 all the chunks in a specific pg: input param is pg_id
        // 3 a specific chunk: input param is pchunk_id

        {Pistache::Http::Method::Post, "/api/v1/trigger_gc",
         Pistache::Rest::Routes::bind(&HttpManager::trigger_gc, this)},
        {Pistache::Http::Method::Get, "/api/v1/gc_job_status",
         Pistache::Rest::Routes::bind(&HttpManager::get_gc_job_status, this)},
        {Pistache::Http::Method::Post, "/api/v1/trigger_pg_scrub",
         Pistache::Rest::Routes::bind(&HttpManager::trigger_pg_scrub, this)},
        {Pistache::Http::Method::Get, "/api/v1/scrub_job_status",
         Pistache::Rest::Routes::bind(&HttpManager::get_scrub_job_status, this)},
        {Pistache::Http::Method::Post, "/api/v1/cancel_scrub_job",
         Pistache::Rest::Routes::bind(&HttpManager::cancel_scrub_job, this)}};

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

void HttpManager::trigger_snapshot_creation(const Pistache::Rest::Request& request,
                                            Pistache::Http::ResponseWriter response) {
    // Extract and validate pg_id parameter (required)
    const auto pg_id_param = request.query().get("pg_id");
    if (!pg_id_param) {
        response.send(Pistache::Http::Code::Bad_Request, "pg_id is required");
        return;
    }
    const int32_t pg_id = std::stoi(pg_id_param.value());

    // Extract compact_lsn parameter (optional, default: -1 means use current HS status)
    const auto compact_lsn_param = request.query().get("compact_lsn");
    const int64_t compact_lsn = std::stoll(compact_lsn_param.value_or("-1"));

    // Extract wait_for_commit parameter (optional, default: true)
    const auto wait_for_commit_param = request.query().get("wait_for_commit");
    std::string wait_for_commit_mode = wait_for_commit_param.value_or("true");
    if (wait_for_commit_mode != "true" && wait_for_commit_mode != "false") {
        response.send(Pistache::Http::Code::Bad_Request, "wait_for_commit must be 'true' or 'false'");
        return;
    }
    bool wait_for_commit = (wait_for_commit_mode == "true");

    LOGINFO("Received snapshot creation request for pg_id={}, compact_lsn={}, wait_for_commit={}", pg_id, compact_lsn,
            wait_for_commit);

    ho_.trigger_snapshot_creation(pg_id, compact_lsn, wait_for_commit);
    response.send(Pistache::Http::Code::Ok, "Snapshot creation request submitted");
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

void HttpManager::flip_learner_flag(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    try {
        auto body = request.body();
        auto j = nlohmann::json::parse(body);

        std::string pg_id_str = j.at("pg_id").get< std::string >();
        pg_id_t pg_id = std::stoull(pg_id_str);
        std::string member_id_str = j.at("member_id").get< std::string >();
        peer_id_t member_id = boost::uuids::string_generator()(member_id_str);
        std::string learner = j.at("learner").get< std::string >();
        std::string commit_quorum_str = j.at("commit_quorum").get< std::string >();
        uint32_t commit_quorum = std::stoul(commit_quorum_str);
        auto tid = generateRandomTraceId();
        LOGINFO("Flipping learner flag, pg_id={}, member_id={}, learner={}, commit_quorum={}, tid={}", pg_id,
                boost::uuids::to_string(member_id), learner, commit_quorum, tid);
        auto result = ho_.flip_learner_flag(pg_id, member_id, learner == "true", commit_quorum, tid).get();
        if (!result) {
            LOGI("PG flip learner flag failed, err={}", result.error());
            response.send(Pistache::Http::Code::Internal_Server_Error,
                          fmt::format("Failed to flip learner flag, err={}", result.error()));
            return;
        }
        response.send(Pistache::Http::Code::Ok);
    } catch (const std::exception& e) {
        response.send(Pistache::Http::Code::Bad_Request, std::string("Invalid JSON: ") + e.what());
    }
}

void HttpManager::remove_member(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    try {
        auto body = request.body();
        auto j = nlohmann::json::parse(body);

        std::string pg_id_str = j.at("pg_id").get< std::string >();
        pg_id_t pg_id = std::stoull(pg_id_str);
        std::string member_id_str = j.at("member_id").get< std::string >();
        peer_id_t member_id = boost::uuids::string_generator()(member_id_str);
        std::string commit_quorum_str = j.at("commit_quorum").get< std::string >();
        uint32_t commit_quorum = std::stoul(commit_quorum_str);
        auto tid = generateRandomTraceId();
        LOGINFO("Remove member, pg_id={}, member_id={}, commit_quorum={}, tid={}", pg_id,
                boost::uuids::to_string(member_id), commit_quorum, tid);
        auto result = ho_.remove_member(pg_id, member_id, commit_quorum, tid).get();
        if (!result) {
            // Some times remove member may fail with RETRY_REQUEST if the target member is not responding,
            // in this case return 503 so that the caller can retry later.
            auto code = result.error() == PGError::RETRY_REQUEST ? Pistache::Http::Code::Service_Unavailable
                                                                 : Pistache::Http::Code::Internal_Server_Error;
            response.send(code, fmt::format("Failed to remove member, err={}", result.error()));
            return;
        }
        response.send(Pistache::Http::Code::Ok);
    } catch (const std::exception& e) {
        response.send(Pistache::Http::Code::Bad_Request, std::string("Invalid JSON: ") + e.what());
    }
}

void HttpManager::clean_replace_member_task(const Pistache::Rest::Request& request,
                                            Pistache::Http::ResponseWriter response) {
    try {
        auto body = request.body();
        auto j = nlohmann::json::parse(body);

        std::string pg_id_str = j.at("pg_id").get< std::string >();
        pg_id_t pg_id = std::stoull(pg_id_str);
        std::string task_id = j.at("task_id").get< std::string >();
        std::string commit_quorum_str = j.at("commit_quorum").get< std::string >();
        uint32_t commit_quorum = std::stoul(commit_quorum_str);
        auto tid = generateRandomTraceId();
        LOGINFO("Clean replace member task, pg_id={}, task_id={}, commit_quorum={}, tid={}", pg_id, task_id,
                commit_quorum, tid);
        auto result = ho_.clean_replace_member_task(pg_id, task_id, commit_quorum, tid).get();
        if (!result) {
            response.send(Pistache::Http::Code::Internal_Server_Error,
                          fmt::format("Failed to clean replace member task, err={}", result.error()));
            return;
        }
        response.send(Pistache::Http::Code::Ok);
    } catch (const std::exception& e) {
        response.send(Pistache::Http::Code::Bad_Request, std::string("Invalid JSON: ") + e.what());
    }
}
void HttpManager::reconcile_membership(const Pistache::Rest::Request& request,
                                       Pistache::Http::ResponseWriter response) {
    try {
        auto body = request.body();
        auto j = nlohmann::json::parse(body);

        std::string pg_id_str = j.at("pg_id").get< std::string >();
        pg_id_t pg_id = std::stoull(pg_id_str);

        LOGINFO("Reconcile membership for pg_id={}", pg_id);

        bool success = ho_.reconcile_membership(pg_id);
        if (!success) {
            response.send(Pistache::Http::Code::Internal_Server_Error,
                          fmt::format("Failed to reconcile membership for pg_id={}", pg_id));
            return;
        }

        nlohmann::json result;
        result["status"] = "success";
        result["pg_id"] = pg_id;
        result["message"] = "Membership reconciled successfully";

        response.send(Pistache::Http::Code::Ok, result.dump());
    } catch (const std::exception& e) {
        response.send(Pistache::Http::Code::Bad_Request, std::string("Invalid JSON: ") + e.what());
    }
}

void HttpManager::list_pg_replace_member_task(const Pistache::Rest::Request& request,
                                              Pistache::Http::ResponseWriter response) {
    auto tid = generateRandomTraceId();
    auto ret = ho_.list_all_replace_member_tasks(tid);
    if (ret.hasError()) {
        response.send(Pistache::Http::Code::Internal_Server_Error,
                      fmt::format("Failed to list replace member task, err={}", ret.error()));
        return;
    }
    LOGINFO("list pg replace member tasks, count={}, tid={}", ret.value().size(), tid);
    nlohmann::json j = nlohmann::json::array();
    for (const auto& task : ret.value()) {
        nlohmann::json task_j;
        task_j["task_id"] = task.task_id;
        task_j["replica_out"] = to_string(task.replica_out);
        task_j["replica_in"] = to_string(task.replica_in);
        j.push_back(task_j);
    }
    response.send(Pistache::Http::Code::Ok, j.dump(2));
}

// This API is used to get the PG quorum status, typically used by CM to fix its view of the PG status after a pg move
// failure.
void HttpManager::get_pg_quorum(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto pg_id_str{request.query().get("pg_id")};
    if (pg_id_str == std::nullopt) {
        response.send(Pistache::Http::Code::Bad_Request, "Missing pg_id query parameter");
        return;
    }
    pg_id_t pg_id = std::stoull(pg_id_str.value());
    PGStats stats;
    if (ho_.get_stats(pg_id, stats)) {
        nlohmann::json j;
        j["pg_id"] = pg_id;
        j["replica_set_uuid"] = boost::uuids::to_string(stats.replica_set_uuid);
        j["leader"] = boost::uuids::to_string(stats.leader_id);
        j["members"] = nlohmann::json::array();
        for (auto peer : stats.members) {
            nlohmann::json member_j;
            member_j["id"] = boost::uuids::to_string(peer.id);
            member_j["name"] = peer.name;
            member_j["can_vote"] = peer.can_vote;
            member_j["last_commit_lsn"] = peer.last_commit_lsn;
            member_j["last_succ_resp_us"] = peer.last_succ_resp_us;
            j["members"].push_back(member_j);
        }
        response.send(Pistache::Http::Code::Ok, j.dump(2));
    } else {
        response.send(Pistache::Http::Code::Internal_Server_Error, fmt::format("Failed to get pg quorum"));
    }
}

void HttpManager::exit_pg(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto group_id_str{request.query().get("group_id")};
    auto peer_id_str{request.query().get("replica_id")};
    auto tid = generateRandomTraceId();
    if (group_id_str == std::nullopt || peer_id_str == std::nullopt) {
        response.send(Pistache::Http::Code::Bad_Request, "Missing group_id or replica_id query parameter");
        return;
    }
    uuid_t group_id;
    uuid_t peer_id;
    try {
        group_id = boost::uuids::string_generator()(group_id_str.value());
        peer_id = boost::uuids::string_generator()(peer_id_str.value());
    } catch (const std::runtime_error& e) {
        response.send(Pistache::Http::Code::Bad_Request, "Invalid group_id or replica_id query parameter");
        return;
    }
    LOGINFO("Exit pg request received for group_id={}, peer_id={}, tid={}", group_id_str.value(), peer_id_str.value(),
            tid);
    auto ret = ho_.exit_pg(group_id, peer_id, tid);
    if (ret.hasError()) {
        response.send(Pistache::Http::Code::Internal_Server_Error,
                      fmt::format("Failed to list replace member task, err={}", ret.error()));
        return;
    }
    response.send(Pistache::Http::Code::Ok, "Exit pg request submitted");
}

void HttpManager::trigger_pg_scrub(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto scrub_mgr = ho_.scrub_manager();
    if (!scrub_mgr) {
        response.send(Pistache::Http::Code::Internal_Server_Error, "Scrub manager not available");
        return;
    }

    // Get query parameters
    const auto pg_id_param = request.query().get("pg_id");
    const auto is_deep_param = request.query().get("deep");
    const auto force_param = request.query().get("force");

    // Validate pg_id parameter (required)
    if (!pg_id_param || pg_id_param.value().empty()) {
        nlohmann::json error;
        error["error"] = "Missing required parameter: pg_id";
        error["usage"] = "POST /api/v1/trigger_pg_scrub?pg_id=<id>&deep=<true|false>&force=<true|false>";
        response.send(Pistache::Http::Code::Bad_Request, error.dump());
        return;
    }

    uint16_t pg_id;
    try {
        auto val = std::stoul(pg_id_param.value());
        if (val > std::numeric_limits< uint16_t >::max()) {
            nlohmann::json error;
            error["error"] = "pg_id out of range";
            error["pg_id"] = pg_id_param.value();
            response.send(Pistache::Http::Code::Bad_Request, error.dump());
            return;
        }
        pg_id = static_cast< uint16_t >(val);
    } catch (const std::invalid_argument& e) {
        nlohmann::json error;
        error["error"] = "Invalid pg_id format: not a number";
        error["pg_id"] = pg_id_param.value();
        response.send(Pistache::Http::Code::Bad_Request, error.dump());
        return;
    } catch (const std::out_of_range& e) {
        nlohmann::json error;
        error["error"] = "pg_id out of range";
        error["pg_id"] = pg_id_param.value();
        response.send(Pistache::Http::Code::Bad_Request, error.dump());
        return;
    }

    // Parse optional parameters
    bool is_deep = false;
    if (is_deep_param && !is_deep_param.value().empty()) {
        const auto& value = is_deep_param.value();
        is_deep = (value == "true" || value == "1" || value == "yes");
    }

    bool force = false;
    if (force_param && !force_param.value().empty()) {
        const auto& value = force_param.value();
        force = (value == "true" || value == "1" || value == "yes");
    }

    LOGINFO("Received trigger_pg_scrub request for pg_id={}, deep={}, force={}", pg_id, is_deep, force);

    // Verify PG exists
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        nlohmann::json error;
        error["error"] = "PG not found";
        error["pg_id"] = pg_id;
        response.send(Pistache::Http::Code::Not_Found, error.dump());
        return;
    }

    // Generate job ID and create job info
    const auto job_id = generate_job_id();
    auto job_info = std::make_shared< ScrubJobInfo >(job_id, pg_id, is_deep);

    {
        std::lock_guard< std::shared_mutex > lock(scrub_job_mutex_);
        scrub_jobs_map_.set(job_id, job_info);
    }

    // Prepare immediate response
    nlohmann::json result;
    result["job_id"] = job_id;
    result["pg_id"] = pg_id;
    result["scrub_type"] = is_deep ? "deep" : "shallow";
    result["force"] = force;
    result["message"] = "Scrub task submitted, query status using /api/v1/scrub_job_status?job_id=" + job_id;

    // Return immediately with HTTP 202 Accepted
    response.send(Pistache::Http::Code::Accepted, result.dump());

    // Submit scrub task (MANUALLY trigger type) - runs asynchronously
    scrub_mgr->submit_scrub_task(pg_id, is_deep, force, SCRUB_TRIGGER_TYPE::MANUALLY)
        .via(&folly::InlineExecutor::instance())
        .thenValue([job_info, is_deep](std::shared_ptr< ScrubManager::ShallowScrubReport > report) {
            if (!report) {
                job_info->try_complete(ScrubJobStatus::FAILED, "Scrub task failed or was cancelled");
                return;
            }

            // Build report summary
            nlohmann::json report_summary;
            report_summary["pg_id"] = report->get_pg_id();

            // Add missing shards info
            const auto& missing_shards = report->get_missing_shard_ids();
            if (!missing_shards.empty()) {
                nlohmann::json missing_shards_json;
                for (const auto& [peer_id, shard_ids] : missing_shards) {
                    missing_shards_json[boost::uuids::to_string(peer_id)] = shard_ids;
                }
                report_summary["missing_shards"] = missing_shards_json;
            }

            // Add missing blobs info
            const auto& missing_blobs = report->get_missing_blobs();
            if (!missing_blobs.empty()) { report_summary["missing_blobs_count"] = count_peer_map_items(missing_blobs); }

            // If it's a deep scrub report, add additional info
            if (is_deep) {
                auto deep_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(report);
                if (deep_report) {
                    // Add corrupted blobs count
                    const auto& corrupted_blobs = deep_report->get_corrupted_blobs();
                    if (!corrupted_blobs.empty()) {
                        report_summary["corrupted_blobs_count"] = count_peer_map_items(corrupted_blobs);
                    }

                    // Add inconsistent blobs count
                    const auto& inconsistent_blobs = deep_report->get_inconsistent_blobs();
                    if (!inconsistent_blobs.empty()) {
                        report_summary["inconsistent_blobs_count"] = inconsistent_blobs.size();
                    }

                    // Add corrupted shards count
                    const auto& corrupted_shards = deep_report->get_corrupted_shards();
                    if (!corrupted_shards.empty()) {
                        report_summary["corrupted_shards_count"] = count_peer_map_items(corrupted_shards);
                    }

                    // Add corrupted PG meta info
                    const auto& corrupted_pg_metas = deep_report->get_corrupted_pg_metas();
                    if (!corrupted_pg_metas.empty()) {
                        report_summary["corrupted_pg_metas_count"] = corrupted_pg_metas.size();
                    }
                }
            }

            // Complete the job with success status and report
            job_info->try_complete(ScrubJobStatus::COMPLETED, "", report_summary);
        })
        .thenError([job_info](const folly::exception_wrapper& ew) {
            job_info->try_complete(ScrubJobStatus::FAILED, ew.what().c_str());
        });
}

void HttpManager::trigger_gc(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
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

    const auto chunk_id_param = request.query().get("chunk_id");
    const auto pg_id_param = request.query().get("pg_id");

    if (chunk_id_param && !chunk_id_param.value().empty()) {
        // trigger gc for a specific chunk, the chunk_id is pchunk_id, not vchunk_id
        uint32_t chunk_id = std::stoul(chunk_id_param.value());
        LOGINFO("Received trigger_gc request for chunk_id {}", chunk_id);

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
        nlohmann::json result;
        const auto job_id = generate_job_id();

        result["chunk_id"] = chunk_id;
        result["pg_id"] = pg_id;
        result["job_id"] = job_id;

        if (chunk->m_state == ChunkState::GC) {
            result["message"] = "chunk is already under GC now, this task will not be executed!";
            response.send(Pistache::Http::Code::Ok, result.dump());
            return;
        }
        result["message"] = "GC triggered for chunk, pls query job status using gc_job_status API";

        // return response before starting the GC so that we don't block the client.
        response.send(Pistache::Http::Code::Accepted, result.dump());

        auto job_info = std::make_shared< GCJobInfo >(job_id, pg_id, chunk_id);
        {
            std::lock_guard lock(gc_job_mutex_);
            gc_jobs_map_.set(job_id, job_info);
        }

        // sumbit gc task for this chunk

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
                job_info->status = res ? GCJobStatus::COMPLETED : GCJobStatus::FAILED;
                // Resume accepting new requests for this pg
                repl_dev->resume_accepting_reqs();
            });
    } else if (pg_id_param && !pg_id_param.value().empty()) {
        // trigger gc for all chunks in a specific pg
        const auto pg_id = std::stoul(pg_id_param.value());
        LOGINFO("Received trigger_gc request for pg_id {}", pg_id);
        auto hs_pg = const_cast< HSHomeObject::HS_PG* >(ho_.get_hs_pg(pg_id));
        if (!hs_pg) {
            nlohmann::json error;
            error["pg_id"] = pg_id;
            error["error"] = "PG not found";
            response.send(Pistache::Http::Code::Not_Found, error.dump());
            return;
        }

        nlohmann::json result;
        const auto job_id = generate_job_id();
        result["pg_id"] = pg_id;
        result["job_id"] = job_id;
        result["message"] = "GC triggered for a single pg, pls query job status using gc_job_status API";
        // return response before starting the GC so that we don't block the client.
        response.send(Pistache::Http::Code::Accepted, result.dump());

        auto job_info = std::make_shared< GCJobInfo >(job_id, pg_id);
        {
            std::lock_guard lock(gc_job_mutex_);
            gc_jobs_map_.set(job_id, job_info);
        }

        LOGINFO("GC job {} stopping GC scan timer", job_id);
        gc_mgr->stop_gc_scan_timer();

        // we block here until all gc tasks for the pg are done
        trigger_gc_for_pg(pg_id, job_id);

        LOGINFO("GC job {} restarting GC scan timer", job_id);
        gc_mgr->start_gc_scan_timer();
    } else {
        LOGINFO("Received trigger_gc request for all chunks");
        nlohmann::json result;
        const auto job_id = generate_job_id();
        result["job_id"] = job_id;
        result["message"] = "GC triggered for all chunks, pls query job status using gc_job_status API";
        // return response before starting the GC so that we don't block the client.
        response.send(Pistache::Http::Code::Accepted, result.dump());

        auto job_info = std::make_shared< GCJobInfo >(job_id);
        {
            std::lock_guard lock(gc_job_mutex_);
            gc_jobs_map_.set(job_id, job_info);
        }

        std::vector< pg_id_t > pg_ids;
        ho_.get_pg_ids(pg_ids);
        LOGINFO("GC job {} will process {} PGs", job_id, pg_ids.size());
        LOGINFO("GC job {} stopping GC scan timer", job_id);
        gc_mgr->stop_gc_scan_timer();

        // we block here until all gc tasks for the pg are done
        for (const auto& pg_id : pg_ids) {
            trigger_gc_for_pg(pg_id, job_id);
        }

        LOGINFO("GC job {} restarting GC scan timer", job_id);
        gc_mgr->start_gc_scan_timer();
    }
}

std::string HttpManager::generate_job_id() {
    auto counter = job_counter_.fetch_add(1, std::memory_order_relaxed);
    return fmt::format("job-{}", counter);
}

void HttpManager::get_job_status(const std::string& job_id, nlohmann::json& result) {
    result["job_id"] = job_id;
    std::shared_ptr< GCJobInfo > job_info;
    {
        std::shared_lock lock(gc_job_mutex_);
        job_info = gc_jobs_map_.get(job_id);
    }

    if (!job_info) {
        result["error"] = "job_id not found, or job has been evicted";
        return;
    }

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

    if (job_info->chunk_id.has_value()) { result["chunk_id"] = job_info->chunk_id.value(); }
    if (job_info->pg_id.has_value()) { result["pg_id"] = job_info->pg_id.value(); }

    if (job_info->total_chunks > 0) {
        nlohmann::json stats;
        stats["total_chunks"] = job_info->total_chunks;
        stats["success_count"] = job_info->success_count;
        stats["failed_count"] = job_info->failed_count;
        result["statistics"] = stats;
    }
}

void HttpManager::get_gc_job_status(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto job_id_param = request.query().get("job_id");
    if (job_id_param && !job_id_param.value().empty()) {
        const auto job_id = job_id_param.value();
        LOGINFO("query job {} status!", job_id);
        nlohmann::json result;
        get_job_status(job_id, result);
        response.send(Pistache::Http::Code::Ok, result.dump());
        return;
    }

    LOGINFO("query all job status!");
    nlohmann::json result;
    std::vector< std::string > job_ids;
    {
        std::shared_lock lock(gc_job_mutex_);
        for (const auto& [k, v] : gc_jobs_map_) {
            job_ids.push_back(k);
        }
    }

    for (const auto& job_id : job_ids) {
        nlohmann::json job_json;
        get_job_status(job_id, job_json);
        result["jobs"].push_back(job_json);
    }

    response.send(Pistache::Http::Code::Ok, result.dump());
}

void HttpManager::trigger_gc_for_pg(uint16_t pg_id, const std::string& job_id) {
    auto hs_pg = const_cast< HSHomeObject::HS_PG* >(ho_.get_hs_pg(pg_id));
    RELEASE_ASSERT(hs_pg, "HS PG {} not found during GC job {}", pg_id, job_id);

    LOGINFO("GC job {} draining pending GC tasks for PG {}", job_id, pg_id);
    auto gc_mgr = ho_.gc_manager();
    gc_mgr->drain_pg_pending_gc_task(pg_id);
    auto pg_sb = hs_pg->pg_sb_.get();
    std::vector< homestore::chunk_num_t > pg_chunks(pg_sb->get_chunk_ids(), pg_sb->get_chunk_ids() + pg_sb->num_chunks);

    LOGINFO("GC job {} processing PG {} with {} chunks", job_id, pg_id, pg_chunks.size());
    hs_pg->repl_dev_->quiesce_reqs();
    std::vector< folly::SemiFuture< bool > > gc_task_futures;

    std::shared_ptr< GCJobInfo > job_info;
    {
        std::shared_lock lock(gc_job_mutex_);
        job_info = gc_jobs_map_.get(job_id);
    }

    auto chunk_selector = ho_.chunk_selector();

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
        LOGDEBUG("GC job {} for chunk {} in PG {} with priority={}", job_id, chunk_id, pg_id,
                 (priority == task_priority::emergent) ? "emergent" : "normal");
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
        .thenValue([this, pg_id, job_info, gc_mgr](auto&& rets) {
            LOGINFO("All GC tasks have been processed");
            const auto& job_id = job_info->job_id;

            auto hs_pg = const_cast< HSHomeObject::HS_PG* >(ho_.get_hs_pg(pg_id));
            RELEASE_ASSERT(hs_pg, "HS PG {} not found during GC job {}", pg_id, job_id);
            // Resume accepting new requests for this pg
            hs_pg->repl_dev_->resume_accepting_reqs();
            LOGINFO("GC job {} resumed accepting requests for PG {}", job_id, pg_id);

            job_info->status = job_info->failed_count ? GCJobStatus::FAILED : GCJobStatus::COMPLETED;
            LOGINFO("GC job {} completed: total={}, success={}, failed={}", job_id, job_info->total_chunks,
                    job_info->success_count, job_info->failed_count);
        })
        .get();
}

void HttpManager::get_scrub_job_status(const Pistache::Rest::Request& request,
                                       Pistache::Http::ResponseWriter response) {
    auto job_id_param = request.query().get("job_id");

    if (job_id_param && !job_id_param.value().empty()) {
        // Query specific job
        const auto job_id = job_id_param.value();
        LOGINFO("Query scrub job {} status", job_id);

        std::shared_ptr< ScrubJobInfo > job_info;
        {
            std::shared_lock lock(scrub_job_mutex_);
            job_info = scrub_jobs_map_.get(job_id);
        }

        if (!job_info) {
            nlohmann::json error;
            error["error"] = "Job not found";
            error["job_id"] = job_id;
            response.send(Pistache::Http::Code::Not_Found, error.dump());
            return;
        }

        nlohmann::json result = build_scrub_job_json(job_info);
        response.send(Pistache::Http::Code::Ok, result.dump());
        return;
    }

    // Query all jobs
    LOGINFO("Query all scrub job status");
    nlohmann::json result;
    std::vector< std::shared_ptr< ScrubJobInfo > > all_jobs;

    {
        std::shared_lock lock(scrub_job_mutex_);
        for (const auto& [k, v] : scrub_jobs_map_) {
            all_jobs.push_back(v);
        }
    }

    for (const auto& job_info : all_jobs) {
        result["jobs"].push_back(build_scrub_job_json(job_info));
    }

    response.send(Pistache::Http::Code::Ok, result.dump());
}

nlohmann::json HttpManager::build_scrub_job_json(const std::shared_ptr< ScrubJobInfo >& job_info) {
    nlohmann::json result;

    // Helper to convert status enum to string
    auto status_to_string = [](ScrubJobStatus status) -> std::string {
        switch (status) {
        case ScrubJobStatus::RUNNING:
            return "running";
        case ScrubJobStatus::COMPLETED:
            return "completed";
        case ScrubJobStatus::FAILED:
            return "failed";
        case ScrubJobStatus::CANCELLED:
            return "cancelled";
        default:
            return "unknown";
        }
    };

    // Thread-unsafe fields (read-only after construction)
    result["job_id"] = job_info->job_id;
    result["pg_id"] = job_info->pg_id;
    result["scrub_type"] = job_info->is_deep ? "deep" : "shallow";

    // Thread-safe fields (protected by mutex)
    {
        std::lock_guard< std::mutex > lock(job_info->mtx_);

        // Status
        result["status"] = status_to_string(job_info->status);

        // Timestamps - convert to ISO 8601 format (no newline)
        result["start_time"] = format_iso8601_time(job_info->start_time);

        if (job_info->status != ScrubJobStatus::RUNNING) {
            result["end_time"] = format_iso8601_time(job_info->end_time);

            auto duration =
                std::chrono::duration_cast< std::chrono::seconds >(job_info->end_time - job_info->start_time);
            result["duration_seconds"] = duration.count();
        }

        // Error message (if any)
        if (!job_info->error_message.empty()) { result["error_message"] = job_info->error_message; }

        // Report summary (if completed)
        if (job_info->status == ScrubJobStatus::COMPLETED && !job_info->report_summary.empty()) {
            result["report"] = job_info->report_summary;
        }
    }

    return result;
}

void HttpManager::cancel_scrub_job(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto job_id_param = request.query().get("job_id");

    if (!job_id_param || job_id_param.value().empty()) {
        nlohmann::json error;
        error["error"] = "Missing required parameter: job_id";
        error["usage"] = "POST /api/v1/cancel_scrub_job?job_id=<id>";
        response.send(Pistache::Http::Code::Bad_Request, error.dump());
        return;
    }

    const auto job_id = job_id_param.value();
    LOGINFO("Cancel scrub job {}", job_id);

    std::shared_ptr< ScrubJobInfo > job_info;
    {
        std::shared_lock lock(scrub_job_mutex_);
        job_info = scrub_jobs_map_.get(job_id);
    }

    if (!job_info) {
        nlohmann::json error;
        error["error"] = "Job not found";
        error["job_id"] = job_id;
        response.send(Pistache::Http::Code::Not_Found, error.dump());
        return;
    }

    // Check if job is still running (thread-safe)
    bool can_cancel = false;
    std::string current_status_str;
    {
        std::lock_guard< std::mutex > lock(job_info->mtx_);
        can_cancel = (job_info->status == ScrubJobStatus::RUNNING);
        if (!can_cancel) {
            // Get status string for error message
            switch (job_info->status) {
            case ScrubJobStatus::COMPLETED:
                current_status_str = "completed";
                break;
            case ScrubJobStatus::FAILED:
                current_status_str = "failed";
                break;
            case ScrubJobStatus::CANCELLED:
                current_status_str = "cancelled";
                break;
            default:
                current_status_str = "unknown";
            }
        }
    }

    if (!can_cancel) {
        nlohmann::json result;
        result["job_id"] = job_id;
        result["message"] = "Job is not running, cannot cancel";
        result["current_status"] = current_status_str;
        response.send(Pistache::Http::Code::Bad_Request, result.dump());
        return;
    }

    // Cancel the scrub task
    auto scrub_mgr = ho_.scrub_manager();
    if (!scrub_mgr) {
        nlohmann::json error;
        error["error"] = "Scrub manager not available";
        response.send(Pistache::Http::Code::Internal_Server_Error, error.dump());
        return;
    }

    // Cancel in scrub manager first (this will stop ongoing work)
    scrub_mgr->cancel_scrub_task(job_info->pg_id);

    // Update job status (thread-safe)
    job_info->cancel();

    nlohmann::json result;
    result["job_id"] = job_id;
    result["message"] = "Scrub job cancelled successfully";
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
