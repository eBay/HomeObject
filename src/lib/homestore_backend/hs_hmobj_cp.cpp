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

folly::Future< bool > HomeObjCPCallbacks::cp_flush(CP* cp) {
    auto cp_ctx = s_cast< HomeObjCPContext* >(cp->context(homestore::cp_consumer_t::HS_CLIENT));

    // start to flush all dirty candidates.
    for (auto it = cp_ctx->pg_dirty_list_.begin(); it != cp_ctx->pg_dirty_list_.end(); ++it) {
        auto pg_sb = it->second;
        pg_sb.write(); // this is a blocking io;
    }

    cp_ctx->complete(true);

    return folly::makeFuture< bool >(true);
}

void HomeObjCPCallbacks::cp_cleanup(CP* cp) {}

int HomeObjCPCallbacks::cp_progress_percent() { return 0; }

HomeObjCPContext::HomeObjCPContext(CP* cp) : CPContext(cp) { pg_dirty_list_.clear(); }

} // namespace homeobject