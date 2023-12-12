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
#include <homestore/homestore.hpp>
#include "hs_hmobj_cp.hpp"

namespace homeobject {

std::unique_ptr< CPContext > HomeObjCPCallbacks::on_switchover_cp(CP* cur_cp, CP* new_cp) {
    return std::make_unique< HomeObjCPContext >(new_cp);
}

// when cp_flush is called, it means that all the dirty candidates are already in the dirty list.
// new dirty candidates will arrive on next cp's context.
folly::Future< bool > HomeObjCPCallbacks::cp_flush(CP* cp) {
    auto cp_ctx = s_cast< HomeObjCPContext* >(cp->context(homestore::cp_consumer_t::HS_CLIENT));

    // start to flush all dirty candidates.
    // no need to take the lock as no more dirty candidate is going to be added to this context when we are here;
    for (auto it = cp_ctx->pg_dirty_list_.begin(); it != cp_ctx->pg_dirty_list_.end(); ++it) {
        auto id = it->first;
        // auto pg_sb = it->second.get();
        auto pg_sb = it->second;
        auto const pit = cp_ctx->pg_sb_.find(id);
#if 0  
        // releax this assert if HS_PG won't write pg_sb first before cp_flush;
        RELEASE_ASSERT(pit != cp_ctx->pg_sb_.end(), "pg_sb_ should have this pg_id");
#endif
        if (pit == cp_ctx->pg_sb_.end()) {
            cp_ctx->pg_sb_[id] =
                homestore::superblk< HSHomeObject::pg_info_superblk >(HSHomeObject::pg_info_superblk::name());
            cp_ctx->pg_sb_[id].create(pg_sb->size());
        }

        // reuse the superblk in the cp context if the same pg has been dirtied in same cp;

        // copy the dirty buffer to the superblk;
        cp_ctx->pg_sb_[id].get()->copy(*pg_sb);

        // write to disk;
        cp_ctx->pg_sb_[id].write();
    }

    cp_ctx->complete(true);

    return folly::makeFuture< bool >(true);
}

void HomeObjCPCallbacks::cp_cleanup(CP* cp) {}

int HomeObjCPCallbacks::cp_progress_percent() { return 0; }

HomeObjCPContext::HomeObjCPContext(CP* cp) : CPContext(cp) { pg_dirty_list_.clear(); }

void HomeObjCPContext::add_pg_to_dirty_list(HSHomeObject::pg_info_superblk* pg_sb) {
    // this will be called in io path, so take the lock;
    std::scoped_lock lock_guard(dl_mtx_);
    HSHomeObject::pg_info_superblk* sb_copy{nullptr};

    if (pg_dirty_list_.find(pg_sb->id) == pg_dirty_list_.end()) {
        sb_copy = (HSHomeObject::pg_info_superblk*)malloc(pg_sb->size());
    } else {
        sb_copy = pg_dirty_list_[pg_sb->id];
    }

    sb_copy->copy(*pg_sb);
    pg_dirty_list_.emplace(pg_sb->id, sb_copy);
}

std::unordered_map< pg_id_t, homestore::superblk< HSHomeObject::pg_info_superblk > > HomeObjCPContext::pg_sb_;
} // namespace homeobject