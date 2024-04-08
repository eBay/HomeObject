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

    LOGINFO("Starting HomeObject HTTP Manager");
    auto http_server = ioenvironment.get_http_server();
    try {
        http_server->setup_route(Http::Method::Get, "/api/v1/version", Routes::bind(&HttpManager::get_version, this));
        http_server->setup_route(Http::Method::Get, "/api/v1/getMetrics",
                                 Rest::Routes::bind(&HttpManager::get_metrics, this));
        http_server->setup_route(Http::Method::Get, "/metrics",
                                 Rest::Routes::bind(&HttpManager::get_prometheus_metrics, this), iomgr::url_t::safe);
        http_server->setup_route(Http::Method::Get, "/api/v1/getObjLife",
                                 Routes::bind(&HttpManager::get_obj_life, this));
        http_server->setup_route(Http::Method::Get, "/api/v1/getLogLevel",
                                 Routes::bind(&HttpManager::get_log_level, this));
        http_server->setup_route(Http::Method::Post, "/api/v1/setLogLevel",
                                 Routes::bind(&HttpManager::set_log_level, this));
        http_server->setup_route(Http::Method::Get, "/api/v1/mallocStats",
                                 Routes::bind(&HttpManager::get_malloc_stats, this));
        http_server->setup_route(Http::Method::Get, "/api/v1/getConfig", Routes::bind(&HttpManager::get_config, this));
        http_server->setup_route(Http::Method::Post, "/api/v1/reloadConfig",
                                 Routes::bind(&HttpManager::reload_dynamic_config, this));
#ifdef _PRERELEASE
        http_server->setup_route(Http::Method::Post, "/api/v1/crashSystem",
                                 Routes::bind(&HttpManager::crash_system, this));
#endif
    } catch (const std::runtime_error& e) { LOGWARN("{}", e.what()) }
}

void HttpManager::get_version(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    auto vers = sisl::VersionMgr::getVersions();
    std::stringstream ss;
    for (auto const& v : vers) {
        ss << v.first << ": " << v.second << "; ";
    }
    response.send(Pistache::Http::Code::Ok, ss.str());
}

void HttpManager::get_metrics(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    response.send(Pistache::Http::Code::Ok, sisl::MetricsFarm::getInstance().get_result_in_json_string());
}

void HttpManager::get_prometheus_metrics(const Pistache::Rest::Request& request,
                                         Pistache::Http::ResponseWriter response) {
    response.send(Pistache::Http::Code::Ok, sisl::MetricsFarm::getInstance().report(sisl::ReportFormat::kTextFormat));
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

void HttpManager::set_log_level(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    std::string logmodule;
    const auto _new_log_module{request.query().get("logmodule")};
    if (_new_log_module) { logmodule = _new_log_module.value(); }

    const auto _new_log_level{request.query().get("loglevel")};
    if (!_new_log_level) {
        response.send(Pistache::Http::Code::Bad_Request, "Invalid loglevel param!");
        return;
    }
    auto new_log_level = _new_log_level.value();

    std::string resp;
    if (logmodule.empty()) {
        sisl::logging::SetAllModuleLogLevel(spdlog::level::from_str(new_log_level));
        resp = sisl::logging::GetAllModuleLogLevel().dump(2);
    } else {
        sisl::logging::SetModuleLogLevel(logmodule, spdlog::level::from_str(new_log_level));
        resp = std::string("logmodule ") + logmodule + " level set to " +
            spdlog::level::to_string_view(sisl::logging::GetModuleLogLevel(logmodule)).data();
    }

    response.send(Pistache::Http::Code::Ok, resp);
}

void HttpManager::get_log_level(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    std::string logmodule;
    const auto _new_log_module{request.query().get("logmodule")};
    if (_new_log_module) { logmodule = _new_log_module.value(); }

    std::string resp;
    if (logmodule.empty()) {
        resp = sisl::logging::GetAllModuleLogLevel().dump(2);
    } else {
        resp = std::string("logmodule ") + logmodule +
            " level = " + spdlog::level::to_string_view(sisl::logging::GetModuleLogLevel(logmodule)).data();
    }
    response.send(Pistache::Http::Code::Ok, resp);
}

void HttpManager::get_malloc_stats(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    response.send(Pistache::Http::Code::Ok, sisl::get_malloc_stats_detailed().dump(2));
}

void HttpManager::get_config(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    nlohmann::json j = sisl::SettingsFactoryRegistry::instance().get_json();
    response.send(Pistache::Http::Code::Ok, j.dump(2));
}

void HttpManager::reload_dynamic_config(const Pistache::Rest::Request& request,
                                        Pistache::Http::ResponseWriter response) {
    bool restart_needed = sisl::SettingsFactoryRegistry::instance().reload_all();
    response.send(Pistache::Http::Code::Ok,
                  fmt::format("All config reloaded, is app restarted {}\n", (restart_needed ? "true" : "false")));
    if (restart_needed) {
        LOGINFO("Restarting HomeObject because of config change which needed a restart");
        std::this_thread::sleep_for(std::chrono::microseconds{1000});
        std::raise(SIGTERM);
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
