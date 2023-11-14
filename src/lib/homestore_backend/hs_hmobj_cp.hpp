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
#include <homestore/checkpoint/cp_mgr.hpp>
#include <homestore/checkpoint/cp.hpp>
#include <homestore/homestore_decl.hpp>

using homestore::CPCallbacks;
using homestore::CPContext;
namespace homestore {
class CP;
}; // namespace homestore

using homestore::CP;

namespace homeobject {

class HomeObjCPCallbacks : public CPCallbacks {
public:
    HomeObjCPCallbacks();
    virtual ~HomeObjCPCallbacks() = default;

public:
    std::unique_ptr< CPContext > on_switchover_cp(CP* cur_cp, CP* new_cp) override;
    folly::Future< bool > cp_flush(CP* cp) override;
    void cp_cleanup(CP* cp) override;
    int cp_progress_percent() override;

private:
};

class HomeObjCPContext : public CPContext {
public:
    HomeObjCPContext(CP* cp);
    virtual ~HomeObjCPContext() = default;
};

} // namespace homeobject