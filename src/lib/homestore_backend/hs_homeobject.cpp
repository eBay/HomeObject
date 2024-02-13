#include <latch>
#include <optional>
#include <spdlog/fmt/bin_to_hex.h>
#include <folly/Uri.h>

#include <homestore/homestore.hpp>
#include <homestore/checkpoint/cp_mgr.hpp>
#include <homestore/meta_service.hpp>
#include <homestore/replication_service.hpp>
#include <homestore/index_service.hpp>
#include <iomgr/io_environment.hpp>

#include <homeobject/homeobject.hpp>
#include "hs_homeobject.hpp"
#include "heap_chunk_selector.h"
#include "index_kv.hpp"
#include "hs_backend_config.hpp"
#include "hs_hmobj_cp.hpp"
#include "replication_state_machine.hpp"

const string uri_prefix{"http://"};

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
    // instance->init_timer_thread();
    instance->init_cp();
    return instance;
}

// repl application to init homestore
class HSReplApplication : public homestore::ReplApplication {
public:
    HSReplApplication(homestore::repl_impl_type impl_type, bool need_timeline_consistency, HSHomeObject* home_object,
                      std::shared_ptr< HomeObjectApplication > ho_application) :
            _impl_type(impl_type),
            _need_timeline_consistency(need_timeline_consistency),
            _home_object(home_object),
            _ho_application(ho_application) {}

    // TODO: make this override after the base class in homestore adds a virtual destructor
    virtual ~HSReplApplication() = default;

    // overrides
    homestore::repl_impl_type get_impl_type() const override { return _impl_type; }

    bool need_timeline_consistency() const override { return _need_timeline_consistency; }

    std::shared_ptr< homestore::ReplDevListener > create_repl_dev_listener(homestore::group_id_t group_id) override {
        std::scoped_lock lock_guard(_repl_sm_map_lock);
        auto [it, inserted] = _repl_sm_map.emplace(group_id, nullptr);
        if (inserted) { it->second = std::make_shared< ReplicationStateMachine >(_home_object); }
        return it->second;
    }

    std::pair< std::string, uint16_t > lookup_peer(homestore::replica_id_t uuid) const override {
        auto endpoint = _ho_application->lookup_peer(uuid);
        // for folly::uri to parse correctly, we need to add "http://" prefix
        endpoint = uri_prefix + endpoint;
        std::pair< std::string, uint16_t > host_port;
        try {
            folly::Uri uri(endpoint);
            host_port.first = uri.host();
            host_port.second = uri.port();
        } catch (std::runtime_error const& e) {
            LOGE("can't extract host from uuid {}, endpoint: {}; error: {}", to_string(uuid), endpoint, e.what());
        }
        return host_port;
    }

    homestore::replica_id_t get_my_repl_id() const override { return _home_object->our_uuid(); }

private:
    homestore::repl_impl_type _impl_type;
    bool _need_timeline_consistency;
    HSHomeObject* _home_object;
    std::shared_ptr< HomeObjectApplication > _ho_application;
    std::map< homestore::group_id_t, std::shared_ptr< ReplicationStateMachine > > _repl_sm_map;
    std::mutex _repl_sm_map_lock;
};

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
    auto repl_app = std::make_shared< HSReplApplication >(repl_impl_type::server_side, false, this, app);
    bool need_format = HomeStore::instance()
                           ->with_index_service(std::make_unique< BlobIndexServiceCallbacks >(this))
                           .with_repl_data_service(repl_app, chunk_selector_)
                           .start(hs_input_params{.devices = device_info, .app_mem_size = app_mem_size},
                                  [this]() { register_homestore_metablk_callback(); });

    // We either recoverd a UUID and no FORMAT is needed, or we need one for a later superblock
    if (need_format) {
        _our_id = _application.lock()->discover_svcid(std::nullopt);
        RELEASE_ASSERT(!_our_id.is_nil(), "Received no SvcId and need FORMAT!");
        LOGW("We are starting for the first time on [{}], Formatting!!", to_string(_our_id));

        HomeStore::instance()->format_and_start({
            {HS_SERVICE::META, hs_format_params{.size_pct = 5.0}},
            {HS_SERVICE::LOG, hs_format_params{.size_pct = 10.0, .chunk_size = 32 * Mi}},
            {HS_SERVICE::REPLICATION,
             hs_format_params{.size_pct = 79.0,
                              .num_chunks = 65000,
                              .block_size = _data_block_size,
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
    recovery_done_ = true;
    LOGI("Initialize and start HomeStore is successfully");

    // Now cache the zero padding bufs to avoid allocating during IO time
    for (size_t i{0}; i < max_zpad_bufs; ++i) {
        size_t const size = io_align * (i + 1);
        zpad_bufs_[i] = std::move(sisl::io_blob_safe(uint32_cast(size), io_align));
        std::memset(zpad_bufs_[i].bytes(), 0, size);
    }
}

#if 0
void HSHomeObject::init_timer_thread() {
    auto ctx = std::make_shared< std::latch >(1);
    iomanager.create_reactor("ho_timer_thread", iomgr::INTERRUPT_LOOP, 4u,
                             [ctx = std::weak_ptr< std::latch >(ctx)](bool is_started) {
                                 if (auto s_ctx = ctx.lock(); is_started) {
                                     RELEASE_ASSERT(s_ctx, "latch is null!");
                                     s_ctx->count_down();
                                 }
                             });
    ctx->wait();

    ho_timer_thread_handle_ = iomanager.schedule_global_timer(
        HS_BACKEND_DYNAMIC_CONFIG(backend_timer_us) * 1000, true /*recurring*/, nullptr /*cookie*/,
        iomgr::reactor_regex::all_user, [this](void*) { trigger_timed_events(); }, true /* wait_to_schedule */);
    LOGI("homeobject timer thread started successfully with freq {} usec", HS_BACKEND_DYNAMIC_CONFIG(backend_timer_us));
}
#endif

void HSHomeObject::init_cp() {
    using namespace homestore;
    // Register to CP for flush dirty buffers;
    HomeStore::instance()->cp_mgr().register_consumer(cp_consumer_t::HS_CLIENT,
                                                      std::move(std::make_unique< HomeObjCPCallbacks >(this)));
}

// void HSHomeObject::trigger_timed_events() { persist_pg_sb(); }

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
#if 0
    if (ho_timer_thread_handle_.first) {
        iomanager.cancel_timer(ho_timer_thread_handle_, true);
        ho_timer_thread_handle_ = iomgr::null_timer_handle;
    }
    trigger_timed_events();
#endif
    homestore::HomeStore::instance()->shutdown();
    homestore::HomeStore::reset_instance();
    iomanager.stop();
}

void HSHomeObject::on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf) {
    homestore::superblk< shard_info_superblk > sb(_shard_meta_name);
    sb.load(buf, mblk);
    add_new_shard_to_map(std::make_unique< HS_Shard >(std::move(sb)));
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

HomeObjectStats HSHomeObject::_get_stats() const {
    HomeObjectStats stats;
    auto const& repl_svc = homestore::hs()->repl_service();
    stats.total_capacity_bytes = repl_svc.get_cap_stats().total_capacity;
    stats.used_capacity_bytes = repl_svc.get_cap_stats().used_capacity;

    uint32_t num_open_shards = 0ul;
    std::scoped_lock lock_guard(_pg_lock);
    for (auto const& [_, pg] : _pg_map) {
        auto hs_pg = static_cast< HS_PG* >(pg.get());
        num_open_shards += hs_pg->open_shards();
    }

    stats.num_open_shards = num_open_shards;
    stats.avail_open_shards = chunk_selector()->total_chunks() - num_open_shards;
    return stats;
}

size_t HSHomeObject::max_pad_size() const { return zpad_bufs_[max_zpad_bufs - 1].size(); }

sisl::io_blob_safe& HSHomeObject::get_pad_buf(uint32_t pad_len) {
    auto const idx = pad_len / io_align;
    if (idx >= max_zpad_bufs) {
        RELEASE_ASSERT(false, "Requested pad len {} is too large", pad_len);
        return zpad_bufs_[0];
    }
    return zpad_bufs_[idx];
}

} // namespace homeobject
