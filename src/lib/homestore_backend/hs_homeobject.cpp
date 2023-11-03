#include "hs_homeobject.hpp"

#include <spdlog/fmt/bin_to_hex.h>

#include <homestore/homestore.hpp>
#include <homestore/meta_service.hpp>
#include <homestore/replication_service.hpp>
#include <homestore/index_service.hpp>
#include <iomgr/io_environment.hpp>

#include <homeobject/homeobject.hpp>
#include "heap_chunk_selector.h"
#include "index_kv.hpp"
#include "hs_backend_config.hpp"

namespace homeobject {

// HSHomeObject's own SuperBlock. Currently this only contains the SvcId SM
// receives so we can set HomeObject::_our_id upon recovery
struct svc_info_superblk_t {
    peer_id_t svc_id_;
};

extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application) {
    LOGI("Initializing HomeObject");
    auto instance = std::make_shared< HSHomeObject >(std::move(application));
    instance->init_homestore();
    instance->init_timer_thread();
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

    LOGI("Starting iomgr with {} threads, spdk: {}", app->threads(), false);
    ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = app->threads(), .is_spdk = app->spdk_mode()});

    /// TODO Where should this come from?
    const uint64_t app_mem_size = 2 * Gi;
    LOGI("Initialize and start HomeStore with app_mem_size = {}", homestore::in_bytes(app_mem_size));

    std::vector< homestore::dev_info > device_info;
    for (auto const& path : app->devices()) {
        device_info.emplace_back(std::filesystem::canonical(path).string(), homestore::HSDevType::Data);
    }

    chunk_selector_ = std::make_shared< HeapChunkSelector >();
    using namespace homestore;
    bool need_format = HomeStore::instance()
                           ->with_index_service(std::make_unique< BlobIndexServiceCallbacks >(this))
                           .with_repl_data_service(repl_impl_type::solo, chunk_selector_)
                           .start(hs_input_params{.devices = device_info, .app_mem_size = app_mem_size},
                                  [this]() { register_homestore_metablk_callback(); });

    // We either recoverd a UUID and no FORMAT is needed, or we need one for a later superblock
    if (need_format) {
        _our_id = _application.lock()->discover_svcid(boost::uuids::uuid());
        RELEASE_ASSERT(!_our_id.is_nil(), "Received no SvcId and need FORMAT!");
        LOGW("We are starting for the first time on [{}], Formatting!!", to_string(_our_id));

        HomeStore::instance()->format_and_start({
            {HS_SERVICE::META, hs_format_params{.size_pct = 5.0}},
            {HS_SERVICE::LOG_REPLICATED, hs_format_params{.size_pct = 10.0}},
            {HS_SERVICE::LOG_LOCAL, hs_format_params{.size_pct = 0.1}}, // TODO: Remove this after HS disables LOG_LOCAL
            {HS_SERVICE::REPLICATION,
             hs_format_params{.size_pct = 79.0,
                              .num_chunks = 65000,
                              .block_size = 1024,
                              .alloc_type = blk_allocator_type_t::append,
                              .chunk_sel_type = chunk_selector_type_t::CUSTOM}},
            {HS_SERVICE::INDEX, hs_format_params{.size_pct = 5.0}},
        });

        // Create a superblock that contains our SvcId
        auto svc_sb = homestore::superblk< svc_info_superblk_t >(_svc_meta_name);
        svc_sb.create(sizeof(svc_info_superblk_t));
        svc_sb->svc_id_ = _our_id;
        svc_sb.write();
    } else {
        RELEASE_ASSERT(!_our_id.is_nil(), "No SvcId read after HomeStore recovery!");
        auto const new_id = _application.lock()->discover_svcid(_our_id);
        RELEASE_ASSERT(new_id == _our_id, "Received new SvcId [{}] AFTER recovery of [{}]?!", to_string(new_id),
                       to_string(_our_id));
    }
    LOGI("Initialize and start HomeStore is successfully");
}

void HSHomeObject::init_timer_thread() {
    struct Context {
        std::condition_variable cv;
        std::mutex mtx;
        bool created{false};
    };
    auto ctx = std::make_shared< Context >();

    iomanager.create_reactor("ho_timer_thread", iomgr::INTERRUPT_LOOP, 4u, [&ctx](bool is_started) {
        if (is_started) {
            {
                std::unique_lock< std::mutex > lk{ctx->mtx};
                ctx->created = true;
            }
            ctx->cv.notify_one();
        }
    });

    {
        std::unique_lock< std::mutex > lk{ctx->mtx};
        ctx->cv.wait(lk, [&ctx] { return ctx->created; });
    }

    ho_timer_thread_handle_ = iomanager.schedule_global_timer(
        HS_BACKEND_DYNAMIC_CONFIG(backend_timer_us) * 1000, true /*recurring*/, nullptr /*cookie*/,
        iomgr::reactor_regex::all_user, [this](void*) { trigger_timed_events(); }, true /* wait_to_schedule */);
    LOGI("homeobject timer thread started successfully with freq {} usec", HS_BACKEND_DYNAMIC_CONFIG(backend_timer_us));
}

void HSHomeObject::trigger_timed_events() { persist_pg_sb(); }

void HSHomeObject::register_homestore_metablk_callback() {
    // register some callbacks for metadata recovery;
    using namespace homestore;
    HomeStore::instance()->meta_service().register_handler(
        _svc_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            auto svc_sb = homestore::superblk< svc_info_superblk_t >(_svc_meta_name);
            svc_sb.load(buf, mblk);
            _our_id = svc_sb->svc_id_;
            LOGI("Found existing SvcId: [{}]", to_string(_our_id));
        },
        nullptr, true);

    HomeStore::instance()->meta_service().register_handler(
        _shard_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) { on_shard_meta_blk_found(mblk, buf); },
        [this](bool success) { on_shard_meta_blk_recover_completed(success); }, true,
        std::optional< meta_subtype_vec_t >(meta_subtype_vec_t{_pg_meta_name}));

    HomeStore::instance()->meta_service().register_handler(
        _pg_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_pg_meta_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        // TODO: move "repl_dev" to homestore::repl_dev and "index" to homestore::index
        nullptr, true, std::optional< meta_subtype_vec_t >(meta_subtype_vec_t{"repl_dev", "index"}));
}

HSHomeObject::~HSHomeObject() {
    if (ho_timer_thread_handle_.first) {
        iomanager.cancel_timer(ho_timer_thread_handle_, true);
        ho_timer_thread_handle_ = iomgr::null_timer_handle;
    }

    trigger_timed_events();
    homestore::HomeStore::instance()->shutdown();
    homestore::HomeStore::reset_instance();
    iomanager.stop();
}

void HSHomeObject::on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf) {
    homestore::superblk< shard_info_superblk > sb;
    sb.load(buf, mblk);
    add_new_shard_to_map(std::make_unique< HS_Shard >(sb));
}

void HSHomeObject::on_shard_meta_blk_recover_completed(bool success) {
    std::unordered_set< homestore::chunk_num_t > excluding_chunks;
    std::scoped_lock lock_guard(_pg_lock);
    for (auto& pair : _pg_map) {
        for (auto& shard : pair.second->shards_) {
            if (shard->info.state == ShardInfo::State::OPEN) {
                excluding_chunks.emplace(d_cast< HS_Shard* >(shard.get())->sb_->chunk_id);
            }
        }
    }

    chunk_selector_->build_per_dev_chunk_heap(excluding_chunks);
}

} // namespace homeobject
