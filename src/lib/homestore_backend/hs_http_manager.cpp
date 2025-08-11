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
        {Pistache::Http::Method::Get, "/api/v1/PgReplaceMemberTasks",
         Pistache::Rest::Routes::bind(&HttpManager::list_pg_replace_member_task, this)},
        {Pistache::Http::Method::Get, "/api/v1/pgQuorum",
         Pistache::Rest::Routes::bind(&HttpManager::get_pg_quorum, this)},
        {Pistache::Http::Method::Post, "/api/v1/learnerFlag",
         Pistache::Rest::Routes::bind(&HttpManager::flip_learner_flag, this)},
        {Pistache::Http::Method::Delete, "/api/v1/member",
         Pistache::Rest::Routes::bind(&HttpManager::remove_member, this)},
        {Pistache::Http::Method::Delete, "/api/v1/replaceMemberTask",
         Pistache::Rest::Routes::bind(&HttpManager::clean_replace_member_task, this)},
#ifdef _PRERELEASE
        {Pistache::Http::Method::Post, "/api/v1/crashSystem",
         Pistache::Rest::Routes::bind(&HttpManager::crash_system, this)},
#endif
    };

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
        ho_.flip_learner_flag(pg_id, member_id, learner == "true", commit_quorum, tid)
            .deferValue([&response](auto const& e) {
                if (!e) {
                    response.send(Pistache::Http::Code::Internal_Server_Error,
                                  fmt::format("Failed to flip learner flag, err={}", e.error()));
                    return;
                }
                response.send(Pistache::Http::Code::Ok);
            });
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
        ho_.remove_member(pg_id, member_id, commit_quorum, tid).deferValue([&response](auto const& e) {
            if (!e) {
                response.send(Pistache::Http::Code::Internal_Server_Error,
                              fmt::format("Failed to remove member, err={}", e.error()));
                return;
            }
            response.send(Pistache::Http::Code::Ok);
        });
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
        ho_.clean_replace_member_task(pg_id, task_id, commit_quorum, tid).deferValue([&response](auto const& e) {
            if (!e) {
                response.send(Pistache::Http::Code::Internal_Server_Error,
                              fmt::format("Failed to clean replace member task, err={}", e.error()));
                return;
            }
            response.send(Pistache::Http::Code::Ok);
        });
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
