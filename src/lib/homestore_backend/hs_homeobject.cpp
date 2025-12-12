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
#include <sisl/version.hpp>

#include <homeobject/homeobject.hpp>
#include "hs_homeobject.hpp"
#include "heap_chunk_selector.h"
#include "hs_http_manager.hpp"
#include "index_kv.hpp"
#include "hs_backend_config.hpp"
#include "replication_state_machine.hpp"

namespace homeobject {

// HSHomeObject's own SuperBlock. Currently this only contains the SvcId SM
// receives so we can set HomeObject::_our_id upon recovery
struct svc_info_superblk_t {
    peer_id_t svc_id_;
};

extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application) {
    LOGI("Initializing HomeObject");
    auto instance = std::make_shared< HSHomeObject >(std::move(application));
    std::string version = PACKAGE_VERSION;
#ifndef NDEBUG
    LOGINFO("HomeObject DEBUG version: {}", version);
#else
    LOGINFO("HomeObject RELEASE version: {}", version);
#endif
    sisl::VersionMgr::addVersion(PACKAGE_NAME, version::Semver200_version(PACKAGE_VERSION));
    instance->init_homestore();
    instance->init_cp();
    return instance;
}

// repl application to init homestore
class HSReplApplication : public homestore::ReplApplication {
public:
    HSReplApplication(homestore::repl_impl_type impl_type, bool need_timeline_consistency, HSHomeObject* home_object,
                      std::weak_ptr< HomeObjectApplication > ho_application) :
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

    void destroy_repl_dev_listener(homestore::group_id_t group_id) override {
        std::scoped_lock lock_guard(_repl_sm_map_lock);
        auto it = _repl_sm_map.find(group_id);
        if (it == _repl_sm_map.end()) {
            LOGE("cannot find group id in repl_sm_map");
            DEBUG_ASSERT(it != _repl_sm_map.end(), "cannot find group id in repl_sm_map");
            return;
        }
        _repl_sm_map.erase(it);
    }

    void on_repl_devs_init_completed() override { _home_object->on_replica_restart(); }

    std::pair< std::string, uint16_t > lookup_peer(homestore::replica_id_t uuid) const override {
        std::string endpoint;
        // for folly::uri to parse correctly, we need to add "http://" prefix
        static std::string const uri_prefix{"http://"};
        if (auto app = _ho_application.lock(); app) {
            endpoint = fmt::format("{}{}", uri_prefix, app->lookup_peer(uuid));
        } else {
            LOGW("HomeObjectApplication lifetime unexpected! Shutdown in progress?");
            return {};
        }

        std::pair< std::string, uint16_t > host_port;
        try {
            folly::Uri uri(endpoint);
            host_port.first = uri.host();
            host_port.second = uri.port();
        } catch (std::runtime_error const& e) {
            LOGE("can't extract host from uuid {}, endpoint={}; error={}", to_string(uuid), endpoint, e.what());
        }
        return host_port;
    }

    homestore::replica_id_t get_my_repl_id() const override { return _home_object->our_uuid(); }

    uint32_t get_my_repl_svc_port() const override {
        uint32_t port = 0;
        if (auto app = _ho_application.lock(); app) {
            port = app->get_my_repl_svc_port();
        } else {
            LOGW("HomeObjectApplication lifetime unexpected! Shutdown in progress?");
        }
        return port;
    }

private:
    homestore::repl_impl_type _impl_type;
    bool _need_timeline_consistency;
    HSHomeObject* _home_object;
    std::weak_ptr< HomeObjectApplication > _ho_application;
    std::map< homestore::group_id_t, std::shared_ptr< ReplicationStateMachine > > _repl_sm_map;
    std::mutex _repl_sm_map_lock;
};

///
// Start HomeStore based on the options retrived from the Application
//
// This should assert if we can not initialize HomeStore.
//
uint64_t HSHomeObject::_hs_chunk_size = HS_CHUNK_SIZE;

DevType HSHomeObject::get_device_type(string const& devname) {
    const iomgr::drive_type dtype = iomgr::DriveInterface::get_drive_type(devname);
    if (dtype == iomgr::drive_type::block_hdd || dtype == iomgr::drive_type::file_on_hdd) { return DevType::HDD; }
    if (dtype == iomgr::drive_type::file_on_nvme || dtype == iomgr::drive_type::block_nvme) { return DevType::NVME; }
    return DevType::UNSUPPORTED;
}

void HSHomeObject::init_homestore() {
    auto app = _application.lock();
    RELEASE_ASSERT(app, "HomeObjectApplication lifetime unexpected!");

    LOGI("Starting iomgr with {} threads, spdk={}", app->threads(), false);
    ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = app->threads(), .is_spdk = app->spdk_mode()})
        .with_http_server();

    http_mgr_ = std::make_unique< HttpManager >(*this);

    const uint64_t app_mem_size = app->mem_size();
    RELEASE_ASSERT(app_mem_size > 0, "Invalid app_mem_size");
    LOGI("Initialize and start HomeStore with app_mem_size = {}", homestore::in_bytes(app_mem_size));

    if (HS_BACKEND_DYNAMIC_CONFIG(reserved_bytes_in_chunk) > 0) {
        _hs_reserved_blks = sisl::round_up(HS_BACKEND_DYNAMIC_CONFIG(reserved_bytes_in_chunk) / _data_block_size, 1);
        LOGI("will reserve {} blks in each chunk", _hs_reserved_blks);
    }
    std::vector< homestore::dev_info > device_info;
    bool has_data_dev = false;
    bool has_fast_dev = false;
    for (auto const& dev : app->devices()) {
        auto input_dev_type = dev.type;
        auto detected_type = get_device_type(dev.path.string());
        LOGD("Device {} detected as {}", dev.path.string(), detected_type);
        auto final_type = (dev.type == DevType::AUTO_DETECT) ? detected_type : input_dev_type;
        if (final_type == DevType::UNSUPPORTED) {
            LOGW("Device {} is not supported, skipping", dev.path.string());
            continue;
        }
        if (input_dev_type != DevType::AUTO_DETECT && detected_type != final_type) {
            LOGW("Device {} detected as {}, but input type is {}, using input type", dev.path.string(), detected_type,
                 input_dev_type);
        }
        auto hs_type = (final_type == DevType::HDD) ? homestore::HSDevType::Data : homestore::HSDevType::Fast;
        if (hs_type == homestore::HSDevType::Data) { has_data_dev = true; }
        if (hs_type == homestore::HSDevType::Fast) { has_fast_dev = true; }
        device_info.emplace_back(std::filesystem::canonical(dev.path).string(), hs_type);
    }
    RELEASE_ASSERT(device_info.size() != 0, "No supported devices found!");

    chunk_selector_ = std::make_shared< HeapChunkSelector >();
    using namespace homestore;
    auto repl_app = std::make_shared< HSReplApplication >(repl_impl_type::server_side, false, this, _application);
    uint64_t max_snapshot_batch_size_in_bytes = HS_BACKEND_DYNAMIC_CONFIG(max_snapshot_batch_size_mb) * Mi;
    RELEASE_ASSERT(max_snapshot_batch_size_in_bytes <= INT_MAX, "snapshot size is larger than the grpc limit");
    bool need_format =
        HomeStore::instance()
            ->with_index_service(std::make_unique< BlobIndexServiceCallbacks >(this))
            .with_repl_data_service(repl_app, chunk_selector_)
            .start(hs_input_params{.devices = device_info,
                                   .app_mem_size = app_mem_size,
                                   .max_data_size = app->max_data_size(),
                                   .max_snapshot_batch_size = s_cast< int >(max_snapshot_batch_size_in_bytes)},
                   [this]() { register_homestore_metablk_callback(); });

    // We either recoverd a UUID and no FORMAT is needed, or we need one for a later superblock
    if (need_format) {
        _our_id = app->discover_svcid(std::nullopt);
        RELEASE_ASSERT(!_our_id.is_nil(), "Received no SvcId and need FORMAT!");
        LOGW("We are starting for the first time on [{}], Formatting!!", to_string(_our_id));
        is_first_time_startup_ = true;

        if (has_data_dev && has_fast_dev) {
            // Hybrid mode
            LOGD("Has both Data and Fast, running with Hybrid mode");
            HomeStore::instance()->format_and_start({
                {HS_SERVICE::META, hs_format_params{.dev_type = HSDevType::Fast, .size_pct = 9.0, .num_chunks = 64}},
                {HS_SERVICE::LOG,
                 hs_format_params{.dev_type = HSDevType::Fast, .size_pct = 45.0, .chunk_size = 32 * Mi}},
                {HS_SERVICE::INDEX, hs_format_params{.dev_type = HSDevType::Fast, .size_pct = 45.0, .num_chunks = 128}},
                {HS_SERVICE::REPLICATION,
                 hs_format_params{.dev_type = HSDevType::Data,
                                  .size_pct = 99.0,
                                  .num_chunks = 0,
                                  .chunk_size = _hs_chunk_size,
                                  .block_size = _data_block_size,
                                  .alloc_type = blk_allocator_type_t::append,
                                  .chunk_sel_type = chunk_selector_type_t::CUSTOM}},
            });
        } else {
            auto run_on_type = has_fast_dev ? homestore::HSDevType::Fast : homestore::HSDevType::Data;
            LOGD("Running with Single mode, all service on {}", run_on_type);
            HomeStore::instance()->format_and_start({
                // FIXME:  this is to work around the issue in HS that varsize allocator doesn't work with small chunk
                // size.
                {HS_SERVICE::META, hs_format_params{.dev_type = run_on_type, .size_pct = 5.0, .num_chunks = 1}},
                {HS_SERVICE::LOG, hs_format_params{.dev_type = run_on_type, .size_pct = 10.0, .chunk_size = 32 * Mi}},
                {HS_SERVICE::INDEX, hs_format_params{.dev_type = run_on_type, .size_pct = 5.0, .num_chunks = 1}},
                {HS_SERVICE::REPLICATION,
                 hs_format_params{.dev_type = run_on_type,
                                  .size_pct = 79.0,
                                  .num_chunks = 0,
                                  .chunk_size = _hs_chunk_size,
                                  .block_size = _data_block_size,
                                  .alloc_type = blk_allocator_type_t::append,
                                  .chunk_sel_type = chunk_selector_type_t::CUSTOM}},
            });
        }
    } else {
        RELEASE_ASSERT(!_our_id.is_nil(), "No SvcId read after HomeStore recovery!");
        auto const new_id = app->discover_svcid(_our_id);
        RELEASE_ASSERT(new_id == _our_id, "Received new SvcId [{}] AFTER recovery of [{}]?!", to_string(new_id),
                       to_string(_our_id));
    }

    LOGI("Initialize and start HomeStore is successfully");

    // Now cache the zero padding bufs to avoid allocating during IO time
    for (size_t i{0}; i < max_zpad_bufs; ++i) {
        size_t const size = io_align * (i + 1);
        zpad_bufs_[i] = std::move(sisl::io_blob_safe(uint32_cast(size), io_align));
        std::memset(zpad_bufs_[i].bytes(), 0, size);
    }
}

void HSHomeObject::on_replica_restart() {
    std::call_once(replica_restart_flag_, [this]() {
        LOGI("Register PG, shard and gc related meta blk handlers");
        using namespace homestore;
        // recover PG
        homestore::meta_service().register_handler(
            _pg_meta_name,
            [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
                on_pg_meta_blk_found(std::move(buf), voidptr_cast(mblk));
            },
            nullptr, true);

        homestore::meta_service().read_sub_sb(_pg_meta_name);

        // recover shard
        homestore::meta_service().register_handler(
            _shard_meta_name,
            [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) { on_shard_meta_blk_found(mblk, buf); },
            [this](bool success) { on_shard_meta_blk_recover_completed(success); }, true);
        homestore::meta_service().read_sub_sb(_shard_meta_name);

        // recover snapshot context
        homestore::meta_service().register_handler(
            _snp_ctx_meta_name,
            [this](meta_blk* mblk, sisl::byte_view buf, size_t size) { on_snp_ctx_meta_blk_found(mblk, buf); },
            [this](bool success) { on_snp_ctx_meta_blk_recover_completed(success); }, true);
        homestore::meta_service().read_sub_sb(_snp_ctx_meta_name);

        // recover snapshot transmission progress info
        homestore::meta_service().register_handler(
            _snp_rcvr_meta_name,
            [this](meta_blk* mblk, sisl::byte_view buf, size_t size) { on_snp_rcvr_meta_blk_found(mblk, buf); },
            [this](bool success) { on_snp_rcvr_meta_blk_recover_completed(success); }, true);
        homestore::meta_service().read_sub_sb(_snp_rcvr_meta_name);

        homestore::meta_service().register_handler(
            _snp_rcvr_shard_list_meta_name,
            [this](meta_blk* mblk, sisl::byte_view buf, size_t size) {
                on_snp_rcvr_shard_list_meta_blk_found(mblk, buf);
            },
            [this](bool success) { on_snp_rcvr_shard_list_meta_blk_recover_completed(success); }, true);
        homestore::meta_service().read_sub_sb(_snp_rcvr_shard_list_meta_name);

        // gc_manager will be created only once here. we need make sure gc manager is created after all the pg meta blk
        // are replayed since we build pdev chunk heap in the constructor of gc manager , which depends on the pg meta.

        // gc metablk handlers are registered in the constructor of gc manager
        gc_mgr_ = std::make_shared< GCManager >(this);

        if (is_first_time_startup_) {
            // if this is the first time we are starting, we need to create gc metablk for each pdev， which record the
            // reserved chunks and indextable.
            auto pdev_chunks = chunk_selector_->get_pdev_chunks();
            const auto reserved_chunk_num_per_pdev = HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev);
            for (auto const& [pdev_id, chunks] : pdev_chunks) {
                // 1 create gc index table for each pdev
                auto gc_index_table = create_gc_index_table();
                auto uuid = gc_index_table->uuid();
                // no need lock here for gc_index_table_map
                gc_index_table_map.emplace(boost::uuids::to_string(uuid), gc_index_table);

                // 2 create gc actor superblk for each pdev, which contains the pdev_id and index table uuid.
                homestore::superblk< GCManager::gc_actor_superblk > gc_actor_sb{GCManager::_gc_actor_meta_name};
                gc_actor_sb.create(sizeof(GCManager::gc_actor_superblk));
                gc_actor_sb->pdev_id = pdev_id;
                gc_actor_sb->index_table_uuid = uuid;
                gc_actor_sb.write();

                RELEASE_ASSERT(chunks.size() > reserved_chunk_num_per_pdev,
                               "pdev {} has {} chunks, but we need at least {} chunks for reserved chunk", pdev_id,
                               chunks.size(), reserved_chunk_num_per_pdev);

                // 3 create reserved chunk meta blk for each pdev, which contains the reserved chunks.
                for (size_t i = 0; i < reserved_chunk_num_per_pdev; ++i) {
                    auto chunk = chunks[i];
                    homestore::superblk< GCManager::gc_reserved_chunk_superblk > reserved_chunk_sb{
                        GCManager::_gc_reserved_chunk_meta_name};
                    reserved_chunk_sb.create(sizeof(GCManager::gc_reserved_chunk_superblk));
                    reserved_chunk_sb->chunk_id = chunk;
                    reserved_chunk_sb.write();
                }
            }

            // Create a superblock that contains our SvcId
            auto svc_sb = homestore::superblk< svc_info_superblk_t >(_svc_meta_name);
            svc_sb.create(sizeof(svc_info_superblk_t));
            svc_sb->svc_id_ = _our_id;
            svc_sb.write();
        }

        // when initializing, there is not gc task. we need to recover reserved chunks here, so that the reserved chunks
        // will not be put into pdev heap when built
        homestore::meta_service().read_sub_sb(GCManager::_gc_actor_meta_name);
        homestore::meta_service().read_sub_sb(GCManager::_gc_reserved_chunk_meta_name);
        homestore::meta_service().read_sub_sb(GCManager::_gc_task_meta_name);

        gc_mgr_->handle_all_recovered_gc_tasks();

        if (HS_BACKEND_DYNAMIC_CONFIG(enable_gc)) {
            LOGI("Starting GC manager");
            gc_mgr_->start();
        } else {
            LOGI("GC is disabled");
        }
    });
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
                                                      std::move(std::make_unique< MyCPCallbacks >(*this)));
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
}

HSHomeObject::~HSHomeObject() { LOGI("HSHomeObject: Executing destruct procedure"); }

void HSHomeObject::shutdown() {
    if (is_shutting_down()) {
        LOGI("HomeObject is already shutting down");
        return;
    }

    LOGI("start shutting down HomeObject");
#if 0
    if (ho_timer_thread_handle_.first) {
        iomanager.cancel_timer(ho_timer_thread_handle_, true);
        ho_timer_thread_handle_ = iomgr::null_timer_handle;
    }
    trigger_timed_events();
#endif

    start_shutting_down();
    // Wait for all pending requests to complete
    while (true) {
        auto pending_reqs = get_pending_request_num();
        if (0 == pending_reqs) break;
        LOGI("waiting for {} pending requests to complete", pending_reqs);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    };

    LOGI("start shutting down HomeStore");
    homestore::HomeStore::instance()->shutdown();
    homestore::HomeStore::reset_instance();
    gc_mgr_.reset();
    iomanager.stop();

    LOGI("complete shutting down HomeStore");
}

HomeObjectStats HSHomeObject::_get_stats() const {
    HomeObjectStats stats;
    // total capacity
    auto const& repl_svc = homestore::hs()->repl_service();
    auto const num_pdevs = chunk_selector()->get_pdev_chunks().size();
    auto const reserved_chunk_num_per_pdev = HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev);
    uint64_t reserved_gc_bytes = num_pdevs * reserved_chunk_num_per_pdev * _hs_chunk_size;
    stats.total_capacity_bytes = repl_svc.get_cap_stats().total_capacity - reserved_gc_bytes;
    // used capacity
    stats.used_capacity_bytes = chunk_selector()->get_used_blks() * _data_block_size;

    uint32_t num_open_shards = 0ul;
    std::scoped_lock lock_guard(_pg_lock);
    for (auto const& [_, pg] : _pg_map) {
        auto hs_pg = static_cast< HS_PG* >(pg.get());
        num_open_shards += hs_pg->open_shards();
    }

    stats.num_open_shards = num_open_shards;
    stats.avail_open_shards = chunk_selector()->total_chunks() - num_open_shards;
    stats.num_disks = chunk_selector()->total_disks();
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

bool HSHomeObject::pg_exists(pg_id_t pg_id) const {
    std::shared_lock lock_guard(_pg_lock);
    return _pg_map.contains(pg_id);
}

std::shared_ptr< GCBlobIndexTable > HSHomeObject::get_gc_index_table(std::string uuid) const {
    const auto it = gc_index_table_map.find(uuid);
    if (it == gc_index_table_map.end()) {
        LOGE("gc index table {} not found", uuid);
        return nullptr;
    }
    return it->second;
}

void HSHomeObject::trigger_immediate_gc() {
    if (!gc_mgr_) {
        LOGI("scan chunks for gc immediately");
        gc_mgr_->scan_chunks_for_gc();
    } else {
        LOGE("GC is not enabled");
    }
}

void HSHomeObject::reconcile_pg_leader(int32_t pg_id) {
    if (pg_id == -1) {
        LOGI("PG id not set, start reconciling leaders for all PGs");
        std::shared_lock lock_guard(_pg_lock);
        std::vector< std::future< void > > futures;
        for (const auto& [id, pg] : _pg_map) {
            auto hs_pg = static_cast< HS_PG* >(pg.get());
            futures.emplace_back(std::async(std::launch::async, [hs_pg, id]() {
                hs_pg->reconcile_leader();
                LOGI("Triggered reconcile leader for PG {}", id);
            }));
        }
        for (auto& future : futures) {
            future.get();
        }
    } else {
        LOGI("Reconciling leader for PG {}", pg_id);
        auto hs_pg = get_hs_pg(pg_id);
        if (hs_pg) {
            hs_pg->reconcile_leader();
        } else {
            LOGE("PG {} not found", pg_id);
        }
    }
}

void HSHomeObject::yield_pg_leadership_to_follower(int32_t pg_id) {
    if (pg_id == -1) {
        LOGI("PG id not set, start yield leaders for all PGs");
        std::shared_lock lock_guard(_pg_lock);
        std::vector< std::future< void > > futures;
        for (const auto& [id, pg] : _pg_map) {
            auto hs_pg = static_cast< HS_PG* >(pg.get());
            futures.emplace_back(std::async(std::launch::async, [hs_pg, id]() {
                hs_pg->yield_leadership_to_follower();
                LOGI("Triggered reconcile leader for PG {}", id);
            }));
        }
        for (auto& future : futures) {
            future.get();
        }
    } else {
        LOGI("Yielding leader for PG {}", pg_id);
        auto hs_pg = get_hs_pg(pg_id);
        if (hs_pg) {
            hs_pg->yield_leadership_to_follower();
        } else {
            LOGE("PG {} not found", pg_id);
        }
    }
}

void HSHomeObject::trigger_snapshot_creation(int32_t pg_id, int64_t compact_lsn, bool is_async) {
    LOGI("Triggering snapshot creation for pg_id={}, compact_lsn={}, is_async={}", pg_id, compact_lsn, is_async);
    auto hs_pg = get_hs_pg(pg_id);
    if (!hs_pg) {
        LOGE("Failed to trigger snapshot: PG {} not found", pg_id);
        return;
    }
    hs_pg->trigger_snapshot_creation(compact_lsn, is_async);
}

} // namespace homeobject
