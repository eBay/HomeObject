#include <homestore/homestore.hpp>
#include <homestore/index_service.hpp>
#include <homestore/meta_service.hpp>
#include <homestore/replication_service.hpp>
#include <iomgr/io_environment.hpp>

#include <homeobject/homeobject.hpp>
#include "hs_homeobject.hpp"
#include "heap_chunk_selector.h"

namespace homeobject {

extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application) {
    LOGINFOMOD(homeobject, "Initializing HomeObject");
    auto instance = std::make_shared< HSHomeObject >(std::move(application));
    instance->init_homestore();
    return instance;
}

///
// Start HomeStore based on the options retrived from the Application
//
// This should assert if we can not initialize HomeStore.
//
void HSHomeObject::init_homestore() {
    auto app = _application.lock();
    RELEASE_ASSERT(app, "HomeObjectApplication lifetime unexpected!");

    LOGINFO("Starting iomgr with {} threads, spdk: {}", app->threads(), false);
    ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = app->threads(), .is_spdk = app->spdk_mode()});

    /// TODO Where should this come from?
    const uint64_t app_mem_size = 2 * Gi;
    LOGINFO("Initialize and start HomeStore with app_mem_size = {}", homestore::in_bytes(app_mem_size));

    std::vector< homestore::dev_info > device_info;
    for (auto const& path : app->devices()) {
        device_info.emplace_back(std::filesystem::canonical(path).string(), homestore::HSDevType::Data);
    }

    chunk_selector_ = std::make_shared< HeapChunkSelector >();
    using namespace homestore;
    bool need_format = HomeStore::instance()
                           ->with_index_service(nullptr)
                           .with_repl_data_service(repl_impl_type::solo, chunk_selector_)
                           .start(hs_input_params{.devices = device_info, .app_mem_size = app_mem_size},
                                  [this]() { register_homestore_metablk_callback(); });
    if (need_format) {
        LOGWARN("Seems like we are booting/starting first time, Formatting!!");
        HomeStore::instance()->format_and_start({
            {HS_SERVICE::META, hs_format_params{.size_pct = 5.0}},
            {HS_SERVICE::LOG_REPLICATED, hs_format_params{.size_pct = 10.0}},
            {HS_SERVICE::LOG_LOCAL, hs_format_params{.size_pct = 0.1}}, // TODO: Remove this after HS disables LOG_LOCAL
            {HS_SERVICE::REPLICATION,
             hs_format_params{.size_pct = 80.0,
                              .num_chunks = 65000,
                              .block_size = 1024,
                              .alloc_type = blk_allocator_type_t::append,
                              .chunk_sel_type = chunk_selector_type_t::CUSTOM}},
            {HS_SERVICE::INDEX, hs_format_params{.size_pct = 5.0}},
        });
    }

    LOGINFO("Initialize and start HomeStore is successfully");
}

void HSHomeObject::register_homestore_metablk_callback() {
    // register some callbacks for metadata recovery;
    using namespace homestore;
    HomeStore::instance()->meta_service().register_handler(
        "ShardManager",
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) { on_shard_meta_blk_found(mblk, buf); },
        [this](bool success) { on_shard_meta_blk_recover_completed(success); }, true);

    HomeStore::instance()->meta_service().register_handler(
        "PGManager",
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_pg_meta_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        nullptr, true);
}

HSHomeObject::~HSHomeObject() {
    homestore::HomeStore::instance()->shutdown();
    homestore::HomeStore::reset_instance();
    iomanager.stop();
}

void HSHomeObject::on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf) {
    homestore::superblk< shard_info_superblk > sb;
    sb.load(buf, mblk);

    bool pg_is_recovery = false;
    {
        std::scoped_lock lock_guard(_pg_lock);
        pg_is_recovery = _pg_map.find(sb->placement_group) != _pg_map.end();
    }

    if (pg_is_recovery) {
        auto hs_shard = std::make_shared< HS_Shard >(sb);
        add_new_shard_to_map(hs_shard);
        return;
    }

    // There is no guarantee that pg info will be recovery before shard recovery
    std::scoped_lock lock_guard(recovery_mutex_);
    pending_recovery_shards_[sb->placement_group].push_back(std::move(sb));
}

void HSHomeObject::on_shard_meta_blk_recover_completed(bool success) {
    // Find all shard with opening state and excluede their binding chunks from the HeapChunkSelector;
    RELEASE_ASSERT(pending_recovery_shards_.empty(), "some shards is still pending on recovery");
    std::unordered_set< homestore::chunk_num_t > excluding_chunks;
    std::scoped_lock lock_guard(_pg_lock);
    for (auto& pair : _pg_map) {
        for (auto& shard : pair.second->shards_) {
            if (shard->info.state == ShardInfo::State::OPEN) {
                excluding_chunks.emplace(dp_cast< HS_Shard >(shard)->sb_->chunk_id);
            }
        }
    }

    chunk_selector_->build_per_dev_chunk_heap(excluding_chunks);
}

} // namespace homeobject
