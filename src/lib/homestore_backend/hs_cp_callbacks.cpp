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
#include <vector>
#include <homestore/homestore.hpp>
#include "hs_homeobject.hpp"

using homestore::CP;
using homestore::CPCallbacks;
using homestore::CPContext;

namespace homeobject {

std::unique_ptr< CPContext > HSHomeObject::MyCPCallbacks::on_switchover_cp(CP* cur_cp, CP* new_cp) {
    return std::make_unique< CPContext >(new_cp);
}

// when cp_flush is called, it means that all the dirty candidates are already in the dirty list.
// new dirty candidates will arrive on next cp's context.
folly::Future< bool > HSHomeObject::MyCPCallbacks::cp_flush(CP* cp) {
    std::vector< HSHomeObject::HS_PG* > dirty_pg_list;
    dirty_pg_list.reserve(home_obj_._pg_map.size());

    // the metablk update in cp flush might have a confict with gc, which will also try to update metablk, so we need
    // hold the unique lock until all the updates are completed
    std::unique_lock lock_guard(home_obj_._pg_lock);
    for (auto const& [id, pg] : home_obj_._pg_map) {
        auto hs_pg = static_cast< HSHomeObject::HS_PG* >(pg.get());

        // All dirty durable entries are updated in the superblk. We persist outside the pg_lock
        if (!hs_pg->is_dirty_.exchange(false)) { continue; }

        hs_pg->pg_sb_->blob_sequence_num = hs_pg->durable_entities().blob_sequence_num.load();
        hs_pg->pg_sb_->active_blob_count = hs_pg->durable_entities().active_blob_count.load();
        hs_pg->pg_sb_->tombstone_blob_count = hs_pg->durable_entities().tombstone_blob_count.load();
        hs_pg->pg_sb_->total_occupied_blk_count = hs_pg->durable_entities().total_occupied_blk_count.load();
        hs_pg->pg_sb_->total_reclaimed_blk_count = hs_pg->durable_entities().total_reclaimed_blk_count.load();
        dirty_pg_list.push_back(hs_pg);
    }

    for (auto& hs_pg : dirty_pg_list) {
        hs_pg->pg_sb_.write();
    }

    // flush gc durable_entities
    auto gc_manager = home_obj_.gc_manager();
    auto& gc_actor_superblks = gc_manager->get_gc_actor_superblks();
    for (auto& gc_actor_sb : gc_actor_superblks) {
        const auto pdev_id = gc_actor_sb->pdev_id;
        const auto gc_actor = gc_manager->get_pdev_gc_actor(pdev_id);
        RELEASE_ASSERT(gc_actor, "can not get gc actor for pdev {}!", pdev_id);
        if (gc_actor->is_dirty_.exchange(false)) {
            gc_actor_sb->success_gc_task_count = gc_actor->durable_entities().success_gc_task_count.load();
            gc_actor_sb->failed_gc_task_count = gc_actor->durable_entities().failed_gc_task_count.load();
            gc_actor_sb->success_egc_task_count = gc_actor->durable_entities().success_egc_task_count.load();
            gc_actor_sb->failed_egc_task_count = gc_actor->durable_entities().failed_egc_task_count.load();
            gc_actor_sb->total_reclaimed_blk_count_by_gc =
                gc_actor->durable_entities().total_reclaimed_blk_count_by_gc.load();
            gc_actor_sb->total_reclaimed_blk_count_by_egc =
                gc_actor->durable_entities().total_reclaimed_blk_count_by_egc.load();

            gc_actor_sb.write();
        }
    }

    return folly::makeFuture< bool >(true);
}

void HSHomeObject::MyCPCallbacks::cp_cleanup(CP* cp) {}

int HSHomeObject::MyCPCallbacks::cp_progress_percent() { return 0; }

} // namespace homeobject
