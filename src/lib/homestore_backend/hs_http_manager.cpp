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
        {Pistache::Http::Method::Delete, "/api/v1/pg", Pistache::Rest::Routes::bind(&HttpManager::exit_pg, this)},
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
         Pistache::Rest::Routes::bind(&HttpManager::dump_shard, this)}};

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
