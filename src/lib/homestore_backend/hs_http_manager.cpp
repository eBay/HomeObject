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

void HttpManager::reconcile_leader(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
    const auto pg_id_param = request.query().get("pg_id");
    int32_t pg_id = std::stoi(pg_id_param.value_or("-1"));
    LOGINFO("Received reconcile leader request for pg_id {}", pg_id);
    ho_.reconcile_pg_leader(pg_id);
    response.send(Pistache::Http::Code::Ok, "Reconcile leader request submitted");
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
