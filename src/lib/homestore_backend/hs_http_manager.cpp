#include <httplib/httplib.h>
#include <sisl/http/http_server.hpp>
#include <sisl/async/task.hpp>
#include <sisl/async/coro.hpp>
#include <sisl/async/when_all.hpp>
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

} // anonymous namespace

HttpManager::HttpManager(HSHomeObject& ho) : ho_(ho) {
    LOGINFO("Setting up HomeObject HTTP routes");

    std::vector< sisl::http_route > routes = {
        {sisl::http_method::Get, "/api/v1/getObjLife",
         [this](httplib::Request const& req, httplib::Response& res) { get_obj_life(req, res); }},
        {sisl::http_method::Get, "/api/v1/mallocStats",
         [this](httplib::Request const& req, httplib::Response& res) { get_malloc_stats(req, res); }},
        {sisl::http_method::Post, "/api/v1/reconcile_leader",
         [this](httplib::Request const& req, httplib::Response& res) { reconcile_leader(req, res); }},
        {sisl::http_method::Post, "/api/v1/yield_leadership_to_follower",
         [this](httplib::Request const& req, httplib::Response& res) { yield_leadership_to_follower(req, res); }},
        {sisl::http_method::Get, "/api/v1/pg_quorum",
         [this](httplib::Request const& req, httplib::Response& res) { get_pg_quorum(req, res); }},
        {sisl::http_method::Post, "/api/v1/flip_learner",
         [this](httplib::Request const& req, httplib::Response& res) { flip_learner_flag(req, res); }},
        {sisl::http_method::Delete, "/api/v1/member",
         [this](httplib::Request const& req, httplib::Response& res) { remove_member(req, res); }},
        {sisl::http_method::Delete, "/api/v1/pg_replacemember_task",
         [this](httplib::Request const& req, httplib::Response& res) { clean_replace_member_task(req, res); }},
        {sisl::http_method::Get, "/api/v1/pg_replacemember_tasks",
         [this](httplib::Request const& req, httplib::Response& res) { list_pg_replace_member_task(req, res); }},
        {sisl::http_method::Post, "/api/v1/reconcile_membership",
         [this](httplib::Request const& req, httplib::Response& res) { reconcile_membership(req, res); }},
        {sisl::http_method::Delete, "/api/v1/pg",
         [this](httplib::Request const& req, httplib::Response& res) { exit_pg(req, res); }},
        {sisl::http_method::Post, "/api/v1/trigger_snapshot_creation",
         [this](httplib::Request const& req, httplib::Response& res) { trigger_snapshot_creation(req, res); }},
#ifdef _PRERELEASE
        {sisl::http_method::Post, "/api/v1/crashSystem",
         [this](httplib::Request const& req, httplib::Response& res) { crash_system(req, res); }},
#endif
        {sisl::http_method::Get, "/api/v1/pg",
         [this](httplib::Request const& req, httplib::Response& res) { get_pg(req, res); }},
        {sisl::http_method::Get, "/api/v1/chunks",
         [this](httplib::Request const& req, httplib::Response& res) { get_pg_chunks(req, res); }},
        {sisl::http_method::Get, "/api/v1/shard",
         [this](httplib::Request const& req, httplib::Response& res) { get_shard(req, res); }},
        {sisl::http_method::Get, "/api/v1/chunk/dump",
         [this](httplib::Request const& req, httplib::Response& res) { dump_chunk(req, res); }},
        {sisl::http_method::Get, "/api/v1/shard/dump",
         [this](httplib::Request const& req, httplib::Response& res) { dump_shard(req, res); }},

        // we support triggering gc for:
        // 1 all the chunks in all the pg: no input param
        // 2 all the chunks in a specific pg: input param is pg_id
        // 3 a specific chunk: input param is pchunk_id

        {sisl::http_method::Post, "/api/v1/trigger_gc",
         [this](httplib::Request const& req, httplib::Response& res) { trigger_gc(req, res); }},
        {sisl::http_method::Get, "/api/v1/gc_job_status",
         [this](httplib::Request const& req, httplib::Response& res) { get_gc_job_status(req, res); }},
        {sisl::http_method::Post, "/api/v1/trigger_pg_scrub",
         [this](httplib::Request const& req, httplib::Response& res) { trigger_pg_scrub(req, res); }},
        {sisl::http_method::Get, "/api/v1/scrub_job_status",
         [this](httplib::Request const& req, httplib::Response& res) { get_scrub_job_status(req, res); }},
        {sisl::http_method::Post, "/api/v1/cancel_scrub_job",
         [this](httplib::Request const& req, httplib::Response& res) { cancel_scrub_job(req, res); }}};

    auto http_server = ioenvironment.get_http_server();
    if (!http_server) {
        LOGERROR("http server not available");
        return;
    }
    try {
        http_server->setup_routes(routes);
    } catch (std::runtime_error const& e) { LOGERROR("setup routes failed, {}", e.what()); }
}

void HttpManager::get_obj_life(httplib::Request const& request, httplib::Response& response) {
    nlohmann::json j;
    sisl::ObjCounterRegistry::foreach ([&j](const std::string& name, int64_t created, int64_t alive) {
        std::stringstream ss;
        ss << "created=" << created << " alive=" << alive;
        j[name] = ss.str();
    });
    response.status = 200;
    response.set_content(j.dump(), "application/json");
}

void HttpManager::get_malloc_stats(httplib::Request const& request, httplib::Response& response) {
    response.status = 200;
    response.set_content(sisl::get_malloc_stats_detailed().dump(2), "application/json");
}

void HttpManager::reconcile_leader(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > pg_id_param =
        request.has_param("pg_id") ? std::optional< std::string >{request.get_param_value("pg_id")} : std::nullopt;
    int32_t pg_id = std::stoi(pg_id_param.value_or("-1"));
    LOGINFO("Received reconcile leader request for pg_id {}", pg_id);
    ho_.reconcile_pg_leader(pg_id);
    response.status = 200;
    response.set_content("Reconcile leader request submitted", "text/plain");
}

void HttpManager::yield_leadership_to_follower(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > pg_id_param =
        request.has_param("pg_id") ? std::optional< std::string >{request.get_param_value("pg_id")} : std::nullopt;
    int32_t pg_id = std::stoi(pg_id_param.value_or("-1"));

    std::optional< std::string > candidate_param = request.has_param("candidate")
        ? std::optional< std::string >{request.get_param_value("candidate")}
        : std::nullopt;
    if (candidate_param && candidate_param->empty()) {
        response.status = 400;
        response.set_content("candidate must not be empty", "text/plain");
        return;
    }
    if (candidate_param && pg_id < 0) {
        response.status = 400;
        response.set_content("candidate requires pg_id to be specified", "text/plain");
        return;
    }

    std::optional< peer_id_t > candidate;
    auto candidate_str = candidate_param.value_or("auto");
    if (candidate_param) {
        LOGINFO("Checking candidate {} for pg_id {}", candidate_str, pg_id);
        try {
            candidate = boost::uuids::string_generator()(candidate_str);
        } catch (const std::exception&) {
            response.status = 400;
            response.set_content("Invalid candidate UUID format", "text/plain");
            return;
        }
        auto hs_pg = ho_.get_hs_pg(static_cast< uint16_t >(pg_id));
        if (!hs_pg) {
            response.status = 404;
            response.set_content("pg not found", "text/plain");
            return;
        }
        auto const& members = hs_pg->pg_info_.members;
        if (!std::any_of(members.begin(), members.end(), [&](const auto& m) { return m.id == *candidate; })) {
            response.status = 400;
            response.set_content(fmt::format("candidate {} is not a member of pg {}", candidate_str, pg_id),
                                 "text/plain");
            return;
        }
    }

    LOGINFO("Received yield leadership request for pg_id {} to follower, candidate={}", pg_id, candidate_str);
    ho_.yield_pg_leadership_to_follower(pg_id, candidate);
    response.status = 200;
    response.set_content("Yield leadership request submitted", "text/plain");
}

void HttpManager::trigger_snapshot_creation(httplib::Request const& request, httplib::Response& response) {
    // Extract and validate pg_id parameter (required)
    std::optional< std::string > pg_id_param =
        request.has_param("pg_id") ? std::optional< std::string >{request.get_param_value("pg_id")} : std::nullopt;
    if (!pg_id_param) {
        response.status = 400;
        response.set_content("pg_id is required", "text/plain");
        return;
    }
    const int32_t pg_id = std::stoi(pg_id_param.value());

    // Extract compact_lsn parameter (optional, default: -1 means use current HS status)
    std::optional< std::string > compact_lsn_param = request.has_param("compact_lsn")
        ? std::optional< std::string >{request.get_param_value("compact_lsn")}
        : std::nullopt;
    const int64_t compact_lsn = std::stoll(compact_lsn_param.value_or("-1"));

    // Extract wait_for_commit parameter (optional, default: true)
    std::optional< std::string > wait_for_commit_param = request.has_param("wait_for_commit")
        ? std::optional< std::string >{request.get_param_value("wait_for_commit")}
        : std::nullopt;
    std::string wait_for_commit_mode = wait_for_commit_param.value_or("true");
    if (wait_for_commit_mode != "true" && wait_for_commit_mode != "false") {
        response.status = 400;
        response.set_content("wait_for_commit must be 'true' or 'false'", "text/plain");
        return;
    }
    bool wait_for_commit = (wait_for_commit_mode == "true");

    LOGINFO("Received snapshot creation request for pg_id={}, compact_lsn={}, wait_for_commit={}", pg_id, compact_lsn,
            wait_for_commit);

    ho_.trigger_snapshot_creation(pg_id, compact_lsn, wait_for_commit);
    response.status = 200;
    response.set_content("Snapshot creation request submitted", "text/plain");
}

void HttpManager::get_pg(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > pg_str =
        request.has_param("pg_id") ? std::optional< std::string >{request.get_param_value("pg_id")} : std::nullopt;
    if (!pg_str) {
        response.status = 400;
        response.set_content("pg_id is required", "text/plain");
        return;
    }
    uint16_t pg_id = std::stoul(pg_str.value());
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        response.status = 404;
        response.set_content("pg not found", "text/plain");
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
    response.status = 200;
    response.set_content(json.dump(), "application/json");
}

void HttpManager::get_pg_chunks(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > pg_str =
        request.has_param("pg_id") ? std::optional< std::string >{request.get_param_value("pg_id")} : std::nullopt;
    if (!pg_str) {
        response.status = 400;
        response.set_content("pg_id is required", "text/plain");
        return;
    }
    uint16_t pg_id = std::stoul(pg_str.value());
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        response.status = 404;
        response.set_content("pg not found", "text/plain");
        return;
    }
    auto json = ho_.chunk_selector()->dump_chunks_info(pg_id);
    json["pg"]["blk_size"] = hs_pg->repl_dev_->get_blk_size();
    response.status = 200;
    response.set_content(json.dump(), "application/json");
}

void HttpManager::get_shard(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > shard_str = request.has_param("shard_id")
        ? std::optional< std::string >{request.get_param_value("shard_id")}
        : std::nullopt;
    if (!shard_str) {
        response.status = 400;
        response.set_content("shard_id is required", "text/plain");
        return;
    }
    uint64_t shard_id = std::stoull(shard_str.value(), nullptr, 0);
    nlohmann::json j;
    j["shard_id"] = shard_id;
    auto chk = ho_.get_shard_v_chunk_id(shard_id);
    if (!chk) {
        response.status = 404;
        response.set_content("shard not found", "text/plain");
        return;
    }
    auto pchk = ho_.get_shard_p_chunk_id(shard_id);
    j["v_chunk_id"] = chk.value();
    j["p_chunk_id"] = pchk.value();
    pg_id_t pg_id = ho_.get_pg_id_from_shard_id(shard_id);
    if (auto vchunk = ho_.chunk_selector()->get_pg_vchunk(pg_id, chk.value()); vchunk) {
        j["v_chunk_state"] = enum_name(vchunk->m_state);
    }
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        response.status = 500;
        response.set_content("pg not found", "text/plain");
        return;
    }
    // Sync in-memory lookup — avoid sync_get(shard_manager()->get_shard) on HTTP thread.
    auto const* shard = ho_._get_hs_shard(shard_id);
    if (!shard) {
        response.status = 500;
        response.set_content("failed to get shard", "text/plain");
        return;
    }
    const auto& shard_info = shard->info;
    j["created_time"] = shard_info.created_time;
    j["last_modified_time"] = shard_info.last_modified_time;
    j["state"] = shard_info.state;
    j["created_lsn"] = shard_info.create_lsn;
    j["sealed_lsn"] = shard_info.sealed_lsn;
    j["meta"] = std::string(reinterpret_cast< const char* >(shard_info.meta));
    auto blobs = ho_.get_shard_blobs(shard_id);
    if (!blobs) {
        response.status = 500;
        response.set_content("failed to get shard blobs", "text/plain");
        return;
    }
    j["total_blob_count"] = blobs.value().size();

    response.status = 200;
    response.set_content(j.dump(), "application/json");
}

void HttpManager::dump_chunk(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > pg_str =
        request.has_param("pg_id") ? std::optional< std::string >{request.get_param_value("pg_id")} : std::nullopt;
    std::optional< std::string > chunk_str = request.has_param("v_chunk_id")
        ? std::optional< std::string >{request.get_param_value("v_chunk_id")}
        : std::nullopt;
    if (!pg_str || !chunk_str) {
        response.status = 400;
        response.set_content("pg_id and v_chunk_id are required", "text/plain");
        return;
    }
    uint16_t pg_id = std::stoul(pg_str.value());
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        response.status = 404;
        response.set_content("pg not found", "text/plain");
        return;
    }
    uint16_t v_chunk_id = std::stoul(chunk_str.value());
    nlohmann::json j;
    j["v_chunk_id"] = v_chunk_id;

    if (auto vchunk = ho_.chunk_selector()->get_pg_vchunk(pg_id, v_chunk_id); vchunk) {
        j["v_chunk_state"] = enum_name(vchunk->m_state);
    }

    auto shards = hs_pg->get_chunk_shards(v_chunk_id);
    for (auto const& s : shards) {
        nlohmann::json shard_json;
        shard_json["shard_id"] = s.info.id;
        shard_json["created_time"] = s.info.created_time;
        shard_json["last_modified_time"] = s.info.last_modified_time;
        shard_json["state"] = s.info.state;
        shard_json["created_lsn"] = s.info.create_lsn;
        shard_json["sealed_lsn"] = s.info.sealed_lsn;
        shard_json["meta"] = std::string(reinterpret_cast< const char* >(s.info.meta));
        j["shards"].push_back(shard_json);
    }
    j["total_shard_count"] = shards.size();
    response.status = 200;
    response.set_content(j.dump(), "application/json");
}

void HttpManager::dump_shard(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > shard_str = request.has_param("shard_id")
        ? std::optional< std::string >{request.get_param_value("shard_id")}
        : std::nullopt;
    if (!shard_str) {
        response.status = 400;
        response.set_content("shard_id is required", "text/plain");
        return;
    }
    uint64_t shard_id = std::stoull(shard_str.value(), nullptr, 0);
    nlohmann::json j;
    j["shard_id"] = shard_id;
    auto chk = ho_.get_shard_v_chunk_id(shard_id);
    if (!chk) {
        response.status = 404;
        response.set_content("shard not found", "text/plain");
        return;
    }
    j["v_chunk_id"] = chk.value();

    pg_id_t pg_id = ho_.get_pg_id_from_shard_id(shard_id);
    if (auto vchunk = ho_.chunk_selector()->get_pg_vchunk(pg_id, chk.value()); vchunk) {
        j["v_chunk_state"] = enum_name(vchunk->m_state);
    }

    // Sync in-memory lookup — avoid sync_get(shard_manager()->get_shard) on HTTP thread.
    auto const* shard = ho_._get_hs_shard(shard_id);
    if (!shard) {
        response.status = 500;
        response.set_content("failed to get shard", "text/plain");
        return;
    }
    const auto& shard_info = shard->info;
    j["created_time"] = shard_info.created_time;
    j["last_modified_time"] = shard_info.last_modified_time;
    j["state"] = shard_info.state;
    j["created_lsn"] = shard_info.create_lsn;
    j["sealed_lsn"] = shard_info.sealed_lsn;
    j["meta"] = std::string(reinterpret_cast< const char* >(shard_info.meta));

    auto r = ho_.get_shard_blobs(shard_id);
    if (!r) {
        response.status = 500;
        response.set_content("failed to get shard blobs", "text/plain");
        return;
    }
    for (auto const& blob : r.value()) {
        nlohmann::json blob_json;
        blob_json["blob_id"] = blob.blob_id;
        blob_json["blk_num"] = blob.pbas.blk_num();
        blob_json["blk_count"] = blob.pbas.blk_count();
        blob_json["chunk_num"] = blob.pbas.chunk_num();
        j["blobs"].push_back(blob_json);
    }
    response.status = 200;
    response.set_content(j.dump(), "application/json");
}

void HttpManager::flip_learner_flag(httplib::Request const& request, httplib::Response& response) {
    try {
        auto body = request.body;
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
        // ADR: no sync_get on HTTP thread — submit async and return 202 Accepted.
        bool is_learner = (learner == "true");
        sisl::async::detach_then(ho_.flip_learner_flag(pg_id, member_id, is_learner, commit_quorum, tid),
                                 [pg_id, member_id, tid](auto result) {
                                     if (!result) {
                                         LOGERROR("PG flip learner flag failed, pg_id={}, member_id={}, err={}, tid={}",
                                                  pg_id, boost::uuids::to_string(member_id), result.error(), tid);
                                     }
                                 });
        nlohmann::json accepted;
        accepted["status"] = "accepted";
        accepted["message"] = "flip_learner request submitted";
        response.status = 202;
        response.set_content(accepted.dump(), "application/json");
    } catch (const std::exception& e) {
        response.status = 400;
        response.set_content(std::string("Invalid JSON: ") + e.what(), "text/plain");
    }
}

void HttpManager::remove_member(httplib::Request const& request, httplib::Response& response) {
    try {
        auto body = request.body;
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
        // ADR: no sync_get on HTTP thread — submit async and return 202 Accepted.
        // Errors (including RETRY_REQUEST) are logged; clients should poll membership / retry via a new request.
        sisl::async::detach_then(ho_.remove_member(pg_id, member_id, commit_quorum, tid),
                                 [pg_id, member_id, tid](auto result) {
                                     if (!result) {
                                         LOGERROR("Remove member failed, pg_id={}, member_id={}, err={}, tid={}", pg_id,
                                                  boost::uuids::to_string(member_id), result.error(), tid);
                                     }
                                 });
        nlohmann::json accepted;
        accepted["status"] = "accepted";
        accepted["message"] = "remove_member request submitted";
        response.status = 202;
        response.set_content(accepted.dump(), "application/json");
    } catch (const std::exception& e) {
        response.status = 400;
        response.set_content(std::string("Invalid JSON: ") + e.what(), "text/plain");
    }
}

void HttpManager::clean_replace_member_task(httplib::Request const& request, httplib::Response& response) {
    try {
        auto body = request.body;
        auto j = nlohmann::json::parse(body);

        std::string pg_id_str = j.at("pg_id").get< std::string >();
        pg_id_t pg_id = std::stoull(pg_id_str);
        // Keep task_id alive for the async op (API takes std::string&).
        auto task_id = std::make_shared< std::string >(j.at("task_id").get< std::string >());
        std::string commit_quorum_str = j.at("commit_quorum").get< std::string >();
        uint32_t commit_quorum = std::stoul(commit_quorum_str);
        auto tid = generateRandomTraceId();
        LOGINFO("Clean replace member task, pg_id={}, task_id={}, commit_quorum={}, tid={}", pg_id, *task_id,
                commit_quorum, tid);
        // ADR: no sync_get on HTTP thread — submit async and return 202 Accepted.
        sisl::async::detach_then(
            ho_.clean_replace_member_task(pg_id, *task_id, commit_quorum, tid), [pg_id, task_id, tid](auto result) {
                if (!result) {
                    LOGERROR("Clean replace member task failed, pg_id={}, task_id={}, err={}, tid={}", pg_id, *task_id,
                             result.error(), tid);
                }
            });
        nlohmann::json accepted;
        accepted["status"] = "accepted";
        accepted["message"] = "clean_replace_member_task request submitted";
        response.status = 202;
        response.set_content(accepted.dump(), "application/json");
    } catch (const std::exception& e) {
        response.status = 400;
        response.set_content(std::string("Invalid JSON: ") + e.what(), "text/plain");
    }
}
void HttpManager::reconcile_membership(httplib::Request const& request, httplib::Response& response) {
    try {
        auto body = request.body;
        auto j = nlohmann::json::parse(body);

        std::string pg_id_str = j.at("pg_id").get< std::string >();
        pg_id_t pg_id = std::stoull(pg_id_str);

        LOGINFO("Reconcile membership for pg_id={}", pg_id);

        bool success = ho_.reconcile_membership(pg_id);
        if (!success) {
            response.status = 500;
            response.set_content(fmt::format("Failed to reconcile membership for pg_id={}", pg_id), "text/plain");
            return;
        }

        nlohmann::json result;
        result["status"] = "success";
        result["pg_id"] = pg_id;
        result["message"] = "Membership reconciled successfully";

        response.status = 200;
        response.set_content(result.dump(), "application/json");
    } catch (const std::exception& e) {
        response.status = 400;
        response.set_content(std::string("Invalid JSON: ") + e.what(), "text/plain");
    }
}

void HttpManager::list_pg_replace_member_task(httplib::Request const& request, httplib::Response& response) {
    auto tid = generateRandomTraceId();
    auto ret = ho_.list_all_replace_member_tasks(tid);
    if (!ret) {
        response.status = 500;
        response.set_content(fmt::format("Failed to list replace member task, err={}", ret.error()), "text/plain");
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
    response.status = 200;
    response.set_content(j.dump(2), "application/json");
}

// This API is used to get the PG quorum status, typically used by CM to fix its view of the PG status after a pg move
// failure.
void HttpManager::get_pg_quorum(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > pg_id_str =
        request.has_param("pg_id") ? std::optional< std::string >{request.get_param_value("pg_id")} : std::nullopt;
    if (pg_id_str == std::nullopt) {
        response.status = 400;
        response.set_content("Missing pg_id query parameter", "text/plain");
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
        response.status = 200;
        response.set_content(j.dump(2), "application/json");
    } else {
        response.status = 500;
        response.set_content(fmt::format("Failed to get pg quorum"), "text/plain");
    }
}

void HttpManager::exit_pg(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > group_id_str = request.has_param("group_id")
        ? std::optional< std::string >{request.get_param_value("group_id")}
        : std::nullopt;
    std::optional< std::string > peer_id_str = request.has_param("replica_id")
        ? std::optional< std::string >{request.get_param_value("replica_id")}
        : std::nullopt;
    auto tid = generateRandomTraceId();
    if (group_id_str == std::nullopt || peer_id_str == std::nullopt) {
        response.status = 400;
        response.set_content("Missing group_id or replica_id query parameter", "text/plain");
        return;
    }
    uuid_t group_id;
    uuid_t peer_id;
    try {
        group_id = boost::uuids::string_generator()(group_id_str.value());
        peer_id = boost::uuids::string_generator()(peer_id_str.value());
    } catch (const std::runtime_error& e) {
        response.status = 400;
        response.set_content("Invalid group_id or replica_id query parameter", "text/plain");
        return;
    }
    LOGINFO("Exit pg request received for group_id={}, peer_id={}, tid={}", group_id_str.value(), peer_id_str.value(),
            tid);
    auto ret = ho_.exit_pg(group_id, peer_id, tid);
    if (!ret) {
        response.status = 500;
        response.set_content(fmt::format("Failed to list replace member task, err={}", ret.error()), "text/plain");
        return;
    }
    response.status = 200;
    response.set_content("Exit pg request submitted", "text/plain");
}

void HttpManager::trigger_pg_scrub(httplib::Request const& request, httplib::Response& response) {
    auto scrub_mgr = ho_.scrub_manager();
    if (!scrub_mgr) {
        response.status = 500;
        response.set_content("Scrub manager not available", "text/plain");
        return;
    }

    // Get query parameters
    std::optional< std::string > pg_id_param =
        request.has_param("pg_id") ? std::optional< std::string >{request.get_param_value("pg_id")} : std::nullopt;
    std::optional< std::string > is_deep_param =
        request.has_param("deep") ? std::optional< std::string >{request.get_param_value("deep")} : std::nullopt;

    // Validate pg_id parameter (required)
    if (!pg_id_param || pg_id_param.value().empty()) {
        nlohmann::json error;
        error["error"] = "Missing required parameter: pg_id";
        error["usage"] = "POST /api/v1/trigger_pg_scrub?pg_id=<id>&deep=<true|false>";
        response.status = 400;
        response.set_content(error.dump(), "application/json");
        return;
    }

    uint16_t pg_id;
    try {
        auto val = std::stoul(pg_id_param.value());
        if (val > std::numeric_limits< uint16_t >::max()) {
            nlohmann::json error;
            error["error"] = "pg_id out of range";
            error["pg_id"] = pg_id_param.value();
            response.status = 400;
            response.set_content(error.dump(), "application/json");
            return;
        }
        pg_id = static_cast< uint16_t >(val);
    } catch (const std::invalid_argument& e) {
        nlohmann::json error;
        error["error"] = "Invalid pg_id format: not a number";
        error["pg_id"] = pg_id_param.value();
        response.status = 400;
        response.set_content(error.dump(), "application/json");
        return;
    } catch (const std::out_of_range& e) {
        nlohmann::json error;
        error["error"] = "pg_id out of range";
        error["pg_id"] = pg_id_param.value();
        response.status = 400;
        response.set_content(error.dump(), "application/json");
        return;
    }

    // Parse optional parameters
    bool is_deep = false;
    if (is_deep_param && !is_deep_param.value().empty()) {
        const auto& value = is_deep_param.value();
        is_deep = (value == "true" || value == "1" || value == "yes");
    }

    LOGINFO("Received trigger_pg_scrub request for pg_id={}, deep={}", pg_id, is_deep);

    // Verify PG exists
    auto hs_pg = ho_.get_hs_pg(pg_id);
    if (!hs_pg) {
        nlohmann::json error;
        error["error"] = "PG not found";
        error["pg_id"] = pg_id;
        response.status = 404;
        response.set_content(error.dump(), "application/json");
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
    result["message"] = "Scrub task submitted, query status using /api/v1/scrub_job_status?job_id=" + job_id;

    // Return immediately with HTTP 202 Accepted
    response.status = 202;
    response.set_content(result.dump(), "application/json");

    // Submit scrub task (MANUALLY trigger type) - runs asynchronously
    sisl::async::detach_then(
        scrub_mgr->submit_scrub_task(pg_id, is_deep, SCRUB_TRIGGER_TYPE::MANUALLY),
        [job_info, is_deep](std::shared_ptr< ScrubManager::ShallowScrubReport > report) {
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
                for (const auto& [shard_id, peer_ids] : missing_shards) {
                    nlohmann::json peer_list = nlohmann::json::array();
                    for (const auto& peer_id : peer_ids) {
                        peer_list.push_back(boost::uuids::to_string(peer_id));
                    }
                    missing_shards_json[std::to_string(shard_id)] = peer_list;
                }
                report_summary["missing_shards"] = missing_shards_json;
            }

            // Add missing blobs info
            const auto& missing_blobs = report->get_missing_blobs();
            if (!missing_blobs.empty()) {
                nlohmann::json missing_blobs_json;
                for (const auto& [blob_route, peer_ids] : missing_blobs) {
                    nlohmann::json peer_list = nlohmann::json::array();
                    for (const auto& peer_id : peer_ids) {
                        peer_list.push_back(boost::uuids::to_string(peer_id));
                    }
                    missing_blobs_json[fmt::format("{}", blob_route)] = peer_list;
                }
                report_summary["missing_blobs"] = missing_blobs_json;
            }

            // If it's a deep scrub report, add additional info
            if (is_deep) {
                auto deep_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(report);
                if (deep_report) {
                    // Add corrupted blobs info
                    const auto& corrupted_blobs = deep_report->get_corrupted_blobs();
                    if (!corrupted_blobs.empty()) {
                        nlohmann::json corrupted_blobs_json;
                        for (const auto& [peer_id, blob_map] : corrupted_blobs) {
                            nlohmann::json blob_status_json;
                            for (const auto& [blob_route, status] : blob_map) {
                                blob_status_json[fmt::format("{}", blob_route)] = scrub_result_to_string(status);
                            }
                            corrupted_blobs_json[boost::uuids::to_string(peer_id)] = blob_status_json;
                        }
                        report_summary["corrupted_blobs"] = corrupted_blobs_json;
                    }

                    // Add inconsistent blobs info
                    const auto& inconsistent_blobs = deep_report->get_inconsistent_blobs();
                    if (!inconsistent_blobs.empty()) {
                        nlohmann::json inconsistent_blobs_json;
                        for (const auto& [blob_route, peer_hash_map] : inconsistent_blobs) {
                            nlohmann::json peer_hash_json;
                            for (const auto& [peer_id, hash] : peer_hash_map) {
                                peer_hash_json[boost::uuids::to_string(peer_id)] = fmt::format("{:016x}", hash);
                            }
                            inconsistent_blobs_json[fmt::format("{}", blob_route)] = peer_hash_json;
                        }
                        report_summary["inconsistent_blobs"] = inconsistent_blobs_json;
                    }

                    // Add corrupted shards info
                    const auto& corrupted_shards = deep_report->get_corrupted_shards();
                    if (!corrupted_shards.empty()) {
                        nlohmann::json corrupted_shards_json;
                        for (const auto& [peer_id, shard_map] : corrupted_shards) {
                            nlohmann::json shard_status_json;
                            for (const auto& [shard_id, status] : shard_map) {
                                shard_status_json[std::to_string(shard_id)] = scrub_result_to_string(status);
                            }
                            corrupted_shards_json[boost::uuids::to_string(peer_id)] = shard_status_json;
                        }
                        report_summary["corrupted_shards"] = corrupted_shards_json;
                    }

                    // Add corrupted PG meta info
                    const auto& corrupted_pg_metas = deep_report->get_corrupted_pg_metas();
                    if (!corrupted_pg_metas.empty()) {
                        nlohmann::json corrupted_pg_metas_json;
                        for (const auto& [peer_id, status] : corrupted_pg_metas) {
                            corrupted_pg_metas_json[boost::uuids::to_string(peer_id)] = scrub_result_to_string(status);
                        }
                        report_summary["corrupted_pg_metas"] = corrupted_pg_metas_json;
                    }
                }
            }

            // Complete the job with success status and report
            job_info->try_complete(ScrubJobStatus::COMPLETED, "", report_summary);
        });
}

void HttpManager::trigger_gc(httplib::Request const& request, httplib::Response& response) {
    auto gc_mgr = ho_.gc_manager();
    if (!gc_mgr) {
        response.status = 500;
        response.set_content("GC manager not available", "text/plain");
        return;
    }

    auto chunk_selector = ho_.chunk_selector();
    if (!chunk_selector) {
        response.status = 500;
        response.set_content("Chunk selector not available", "text/plain");
        return;
    }

    std::optional< std::string > chunk_id_param = request.has_param("chunk_id")
        ? std::optional< std::string >{request.get_param_value("chunk_id")}
        : std::nullopt;
    std::optional< std::string > pg_id_param =
        request.has_param("pg_id") ? std::optional< std::string >{request.get_param_value("pg_id")} : std::nullopt;

    if (chunk_id_param && !chunk_id_param.value().empty()) {
        // trigger gc for a specific chunk, the chunk_id is pchunk_id, not vchunk_id
        uint32_t chunk_id = std::stoul(chunk_id_param.value());
        LOGINFO("Received trigger_gc request for chunk_id {}", chunk_id);

        auto chunk = chunk_selector->get_extend_vchunk(chunk_id);
        if (!chunk) {
            nlohmann::json error;
            error["chunk_id"] = chunk_id;
            error["error"] = "Chunk not found";
            response.status = 404;
            response.set_content(error.dump(), "application/json");
            return;
        }

        if (!chunk->m_pg_id.has_value()) {
            nlohmann::json error;
            error["chunk_id"] = chunk_id;
            error["error"] = "Chunk belongs to no pg";
            response.status = 404;
            response.set_content(error.dump(), "application/json");
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
            response.status = 200;
            response.set_content(result.dump(), "application/json");
            return;
        }
        result["message"] = "GC triggered for chunk, pls query job status using gc_job_status API";

        // return response before starting the GC so that we don't block the client.
        response.status = 202;
        response.set_content(result.dump(), "application/json");

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

        sisl::async::detach_then(gc_mgr->submit_gc_task(priority, chunk_id), [this, job_info, repl_dev](bool res) {
            job_info->status = res ? GCJobStatus::COMPLETED : GCJobStatus::FAILED;
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
            response.status = 404;
            response.set_content(error.dump(), "application/json");
            return;
        }

        nlohmann::json result;
        const auto job_id = generate_job_id();
        result["pg_id"] = pg_id;
        result["job_id"] = job_id;
        result["message"] = "GC triggered for a single pg, pls query job status using gc_job_status API";
        // return response before starting the GC so that we don't block the client.
        response.status = 202;
        response.set_content(result.dump(), "application/json");

        auto job_info = std::make_shared< GCJobInfo >(job_id, pg_id);
        {
            std::lock_guard lock(gc_job_mutex_);
            gc_jobs_map_.set(job_id, job_info);
        }

        LOGINFO("GC job {} stopping GC scan timer", job_id);
        gc_mgr->stop_gc_scan_timer();

        sisl::async::detach_then(trigger_gc_for_pg(pg_id, job_id), [job_info, gc_mgr, job_id](auto&&) {
            job_info->status = job_info->failed_count ? GCJobStatus::FAILED : GCJobStatus::COMPLETED;
            LOGINFO("GC job {} completed: total={}, success={}, failed={}", job_info->job_id, job_info->total_chunks,
                    job_info->success_count, job_info->failed_count);
            LOGINFO("GC job {} restarting GC scan timer", job_id);
            gc_mgr->start_gc_scan_timer();
        });
    } else {
        LOGINFO("Received trigger_gc request for all chunks");
        nlohmann::json result;
        std::vector< pg_id_t > pg_ids;
        ho_.get_pg_ids(pg_ids);

        const auto job_id = generate_job_id();
        result["job_id"] = job_id;

        auto job_info = std::make_shared< GCJobInfo >(job_id);
        {
            std::lock_guard lock(gc_job_mutex_);
            gc_jobs_map_.set(job_id, job_info);
        }

        if (pg_ids.empty()) {
            LOGINFO("GC job {} no PGs found, marking as completed", job_id);
            job_info->status = GCJobStatus::COMPLETED;
            result["message"] = "No PGs found, GC completed";
            response.status = 200;
            response.set_content(result.dump(), "application/json");
            return;
        }

        result["message"] = "GC triggered for all chunks, pls query job status using gc_job_status API";
        // return response before starting the GC so that we don't block the client.
        response.status = 202;
        response.set_content(result.dump(), "application/json");

        LOGINFO("GC job {} will process {} PGs", job_id, pg_ids.size());
        LOGINFO("GC job {} stopping GC scan timer", job_id);
        gc_mgr->stop_gc_scan_timer();

        std::vector< sisl::async::task< std::monostate > > pg_futures;
        for (const auto& pg_id : pg_ids) {
            pg_futures.push_back(trigger_gc_for_pg(pg_id, job_id));
        }

        // Set job status after all PGs are processed
        sisl::async::detach_then(sisl::async::when_all(std::move(pg_futures)), [job_info, gc_mgr, job_id](auto&&) {
            job_info->status = job_info->failed_count ? GCJobStatus::FAILED : GCJobStatus::COMPLETED;
            LOGINFO("GC job {} completed: total={}, success={}, failed={}", job_info->job_id, job_info->total_chunks,
                    job_info->success_count, job_info->failed_count);
            LOGINFO("GC job {} restarting GC scan timer", job_id);
            gc_mgr->start_gc_scan_timer();
        });
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

void HttpManager::get_gc_job_status(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > job_id_param =
        request.has_param("job_id") ? std::optional< std::string >{request.get_param_value("job_id")} : std::nullopt;
    if (job_id_param && !job_id_param.value().empty()) {
        const auto job_id = job_id_param.value();
        LOGINFO("query job {} status!", job_id);
        nlohmann::json result;
        get_job_status(job_id, result);
        response.status = 200;
        response.set_content(result.dump(), "application/json");
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

    response.status = 200;
    response.set_content(result.dump(), "application/json");
}

sisl::async::task< std::monostate > HttpManager::trigger_gc_for_pg(uint16_t pg_id, const std::string& job_id) {
    auto gc_mgr = ho_.gc_manager();
    std::shared_ptr< GCJobInfo > job_info;
    {
        std::shared_lock lock(gc_job_mutex_);
        job_info = gc_jobs_map_.get(job_id);
    }
    if (!job_info) { co_return std::monostate{}; }

    auto hs_pg = const_cast< HSHomeObject::HS_PG* >(ho_.get_hs_pg(pg_id));
    if (!hs_pg) { co_return std::monostate{}; }

    auto pg_sb = hs_pg->pg_sb_.get();
    std::vector< sisl::async::task< bool > > gc_task_futures;
    for (uint32_t i = 0; i < pg_sb->num_chunks; ++i) {
        auto chunk_id = pg_sb->get_chunk_ids()[i];
        auto chunk = ho_.chunk_selector()->get_extend_vchunk(chunk_id);
        if (!chunk || chunk->m_state == ChunkState::GC) { continue; }
        job_info->total_chunks++;
        const auto priority = chunk->m_state == ChunkState::INUSE ? task_priority::emergent : task_priority::normal;
        gc_task_futures.push_back(gc_mgr->submit_gc_task(priority, chunk_id));
    }

    if (gc_task_futures.empty()) { co_return std::monostate{}; }

    auto results = co_await sisl::async::when_all(std::move(gc_task_futures));
    for (auto const& res : results) {
        if (res) {
            job_info->success_count++;
        } else {
            job_info->failed_count++;
        }
    }
    co_return std::monostate{};
}

void HttpManager::get_scrub_job_status(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > job_id_param =
        request.has_param("job_id") ? std::optional< std::string >{request.get_param_value("job_id")} : std::nullopt;

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
            response.status = 404;
            response.set_content(error.dump(), "application/json");
            return;
        }

        nlohmann::json result = build_scrub_job_json(job_info);
        response.status = 200;
        response.set_content(result.dump(), "application/json");
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

    response.status = 200;
    response.set_content(result.dump(), "application/json");
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

void HttpManager::cancel_scrub_job(httplib::Request const& request, httplib::Response& response) {
    std::optional< std::string > job_id_param =
        request.has_param("job_id") ? std::optional< std::string >{request.get_param_value("job_id")} : std::nullopt;

    if (!job_id_param || job_id_param.value().empty()) {
        nlohmann::json error;
        error["error"] = "Missing required parameter: job_id";
        error["usage"] = "POST /api/v1/cancel_scrub_job?job_id=<id>";
        response.status = 400;
        response.set_content(error.dump(), "application/json");
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
        response.status = 404;
        response.set_content(error.dump(), "application/json");
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
        response.status = 400;
        response.set_content(result.dump(), "application/json");
        return;
    }

    // Cancel the scrub task
    auto scrub_mgr = ho_.scrub_manager();
    if (!scrub_mgr) {
        nlohmann::json error;
        error["error"] = "Scrub manager not available";
        response.status = 500;
        response.set_content(error.dump(), "application/json");
        return;
    }

    // Cancel in scrub manager first (this will stop ongoing work)
    scrub_mgr->cancel_scrub_task(job_info->pg_id);

    // Update job status (thread-safe)
    job_info->cancel();

    nlohmann::json result;
    result["job_id"] = job_id;
    result["message"] = "Scrub job cancelled successfully";
    response.status = 200;
    response.set_content(result.dump(), "application/json");
}

#ifdef _PRERELEASE
void HttpManager::crash_system(httplib::Request const& request, httplib::Response& response) {
    std::string crash_type;
    const std::optional< std::string > _crash_type =
        request.has_param("type") ? std::optional< std::string >{request.get_param_value("type")} : std::nullopt;
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
    response.status = 200;
    response.set_content(resp, "text/plain");
}
#endif

} // namespace homeobject
