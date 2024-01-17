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
    HomeObjCPCallbacks(HSHomeObject* home_obj_ptr) : home_obj_(home_obj_ptr){};
    virtual ~HomeObjCPCallbacks() = default;

public:
    std::unique_ptr< CPContext > on_switchover_cp(CP* cur_cp, CP* new_cp) override;
    folly::Future< bool > cp_flush(CP* cp) override;
    void cp_cleanup(CP* cp) override;
    int cp_progress_percent() override;

private:
    HSHomeObject* home_obj_{nullptr}; // it is a raw pointer because HSHomeObject triggers shutdown in its destructor,
                                      // holding a shared_ptr will cause a shutdown deadlock.
};

//
// This is a per_cp context for home object.
// When a new CP is created, a new HomeObjCPContext is created by CP Manager;
// CP consumer doesn't need to free the dirty list inside this context as it will be automatically freed when this cp is
// completed (goes out of life cycle);
//
class HomeObjCPContext : public CPContext {
public:
    HomeObjCPContext(CP* cp);
    virtual ~HomeObjCPContext() {
        for (auto x : pg_dirty_list_) {
            free(x.second);
        }
    };

    /**
     * @brief Adds the PG sb to the dirty list.
     *
     * This function adds the given pg superblock to the dirty list, indicating that it has been modified and needs to
     * be written back to storage. If the same pg (identified by its id) has been added before in the same CP, it will
     * update the same dirty buffer in the dirty list. Caller doesn't need to worry about whether same pg sb has been
     * added or not.
     *
     * Memory:
     * This function will allocate memory for the pg sb if it is the first time the pg sb is added;
     * otherwise, it will reuse the memory allocated before.
     *
     * @param pg_sb A pointer to the page superblock to be added to the dirty list.
     */
    void add_pg_to_dirty_list(HSHomeObject::pg_info_superblk* pg_sb);

    static void init_pg_sb(homestore::superblk< HSHomeObject::pg_info_superblk >&& sb) {
        std::scoped_lock lock_guard(s_mtx_);
        pg_sb_[sb->id] = std::move(sb); // move the sb to the map;
    };

    //////////////// Per-CP instance members ////////////////
    std::mutex dl_mtx_; // mutex to protect dirty list
    std::unordered_map< pg_id_t, HSHomeObject::pg_info_superblk* > pg_dirty_list_;

    //////////////// Shared by all CPs ////////////////
    static std::mutex s_mtx_; // mutex to protect pg_sb_
    // static so that only one superblk instance can write to metablk;
    static std::unordered_map< pg_id_t, homestore::superblk< HSHomeObject::pg_info_superblk > > pg_sb_;
};

} // namespace homeobject
