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

namespace homeobject {
class HSHomeObject;

class HttpManager {
public:
    HttpManager(HSHomeObject& ho);

private:
    void get_version(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_metrics(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_prometheus_metrics(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_obj_life(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void set_log_level(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_log_level(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_malloc_stats(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void get_config(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
    void reload_dynamic_config(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);

#ifdef _PRERELEASE
    void crash_system(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response);
#endif

private:
    HSHomeObject& ho_;
};
} // namespace homeobject