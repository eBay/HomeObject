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
#include <folly/concurrency/ConcurrentHashMap.h>
#include <homeobject/common.hpp>

#include "hs_homeobject.hpp"

using homestore::CPCallbacks;
using homestore::CPContext;
namespace homestore {
class CP;
}; // namespace homestore

using homestore::CP;

namespace homeobject {

class HSHomeObject;

class HomeObjCPCallbacks : public CPCallbacks {
public:
    // HomeObjCPCallbacks(cshared< HomeObjectImpl > home_obj_ptr);
    HomeObjCPCallbacks() = default;
    virtual ~HomeObjCPCallbacks() = default;

public:
    std::unique_ptr< CPContext > on_switchover_cp(CP* cur_cp, CP* new_cp) override;
    folly::Future< bool > cp_flush(CP* cp) override;
    void cp_cleanup(CP* cp) override;
    int cp_progress_percent() override;

private:
    // cshared< HomeObjectImpl > home_obj_{nullptr};
};

//
// This is a per_cp context for home object.
// When a new CP is created, a new HomeObjCPContext is created by CP Manager;
// CP consumer doesn't need to free the dirty list inside this context as it will be automatically freed when this cp is
// completed (goes out of life cycle) which is controlled by cp manager;
//
class HomeObjCPContext : public CPContext {
public:
    HomeObjCPContext(CP* cp);
    virtual ~HomeObjCPContext() = default;

public:
    //
    // HSHomeObject will add the dirty PG sb into this list.
    // When CP is flushed, this list will be flushed to disk, when CP is completed, this list will be freed.
    // Consumer needs to make sure anything being dirtied into this list are log protected.
    //
    folly::ConcurrentHashMap< pg_id_t, homestore::superblk< HSHomeObject::pg_info_superblk > > pg_dirty_list_;
};

} // namespace homeobject