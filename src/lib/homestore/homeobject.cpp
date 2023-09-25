#include "homeobject.hpp"
#include "replication_state_machine.hpp"

#include <homestore/homestore.hpp>
#include <homestore/index_service.hpp>
#include <homestore/meta_service.hpp>
#include <iomgr/io_environment.hpp>

namespace homeobject {

const std::string HSHomeObject::s_shard_info_sub_type = "shard_info";

extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application) {
    LOGINFOMOD(homeobject, "Initializing HomeObject");
    auto instance = std::make_shared< HSHomeObject >(std::move(application));
    instance->init_homestore();
    instance->init_repl_svc();
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

    _chunk_selector = std::make_shared< HeapChunkSelector >();

    using namespace homestore;
    auto hsi = HomeStore::instance();
    // when enable replication service of homestore, log service will be enabled automatically.
    // and meta service is already enabled by default.
    hsi->with_repl_data_service(repl_impl_type::solo, std::make_unique< HOReplServiceCallbacks >(this),
                                _chunk_selector);

    bool need_format = hsi->start(hs_input_params{.devices = device_info, .app_mem_size = app_mem_size},
                                  [this]() { register_homestore_metablk_callback(); });

    /// TODO how should this work?
    LOGWARN("Persistence Looks Vacant, Formatting!!");
    if (need_format) {
        hsi->format_and_start(std::map< uint32_t, hs_format_params >{
            {HS_SERVICE::META, hs_format_params{.size_pct = 5.0}},
            {HS_SERVICE::LOG_REPLICATED, hs_format_params{.size_pct = 10.0}},
            {HS_SERVICE::LOG_LOCAL, hs_format_params{.size_pct = 5.0}},
            {HS_SERVICE::INDEX, hs_format_params{.size_pct = 30.0}},
            {HS_SERVICE::REPLICATION,
             hs_format_params{.size_pct = 50.0,
                              .alloc_type = homestore::blk_allocator_type_t::append,
                              .chunk_sel_type = homestore::chunk_selector_type_t::CUSTOM}},
        });
    }
}

void HSHomeObject::register_homestore_metablk_callback() {
    // register some callbacks for metadata recovery;
    using namespace homestore;
    HomeStore::instance()->meta_service().register_handler(
        HSHomeObject::s_shard_info_sub_type,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_shard_meta_blk_found(mblk, buf, size);
        },
        [this](bool success) { on_shard_meta_blk_recover_completed(success); }, true);
}

void HomeObjectImpl::init_repl_svc() {
    auto lg = std::scoped_lock(_repl_lock);
    if (!_repl_svc) {
        // TODO this should come from persistence.
        LOGINFOMOD(homeobject, "First time start-up...initiating request for SvcId.");
        _our_id = _application.lock()->discover_svcid(std::nullopt);
        LOGINFOMOD(homeobject, "SvcId received: {}", to_string(_our_id));
        _repl_svc = home_replication::create_repl_service([](auto) { return nullptr; });
    }
}

HSHomeObject::~HSHomeObject() {
    homestore::HomeStore::instance()->shutdown();
    homestore::HomeStore::reset_instance();
    iomanager.stop();
}

void HSHomeObject::on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
    auto shard = deserialize_shard(r_cast< const char* >(buf.bytes()), size);
    shard.metablk_cookie = mblk;
    // As shard info in the homestore metablk is always the latest state(OPEN or SEALED),
    // we can always create a shard from this shard info and once shard is deleted, the associated metablk will be
    // deleted too.
    do_commit_new_shard(shard);
}

void HSHomeObject::on_shard_meta_blk_recover_completed(bool success) {
    // Find all shard with opening state and excluede their binding chunks from the HeapChunkSelector;
    std::unordered_set< homestore::chunk_num_t > excluding_chunks;
    std::scoped_lock lock_guard(_pg_lock);
    for (auto& pair : _pg_map) {
        for (auto& shard : pair.second.shards) {
            if (shard.info.state == ShardInfo::State::OPEN) { excluding_chunks.emplace(shard.chunk_id); }
        }
    }

    _chunk_selector->build_per_dev_chunk_heap(excluding_chunks);
}

} // namespace homeobject
