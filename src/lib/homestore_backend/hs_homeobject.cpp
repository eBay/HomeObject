#include <homestore/homestore.hpp>
#include <homestore/index_service.hpp>
#include <homestore/meta_service.hpp>
#include <homestore/replication_service.hpp>
#include <iomgr/io_environment.hpp>

#include <homeobject/homeobject.hpp>
#include "hs_homeobject.hpp"
#include "heap_chunk_selector.h"

namespace homeobject {

const std::string HSHomeObject::s_shard_info_sub_type = "shard_info";

extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application) {
    LOGI("Initializing HomeObject");
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

    LOGI("Starting iomgr with {} threads, spdk: {}", app->threads(), false);
    ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = app->threads(), .is_spdk = app->spdk_mode()});

    /// TODO Where should this come from?
    const uint64_t app_mem_size = 2 * Gi;
    LOGI("Initialize and start HomeStore with app_mem_size = {}", homestore::in_bytes(app_mem_size));

    std::vector< homestore::dev_info > device_info;
    for (auto const& path : app->devices()) {
        device_info.emplace_back(std::filesystem::canonical(path).string(), homestore::HSDevType::Data);
    }

    using namespace homestore;
    bool need_format = HomeStore::instance()
                           ->with_index_service(nullptr)
                           .with_repl_data_service(repl_impl_type::solo, std::make_shared< HeapChunkSelector >())
                           .start(hs_input_params{.devices = device_info, .app_mem_size = app_mem_size},
                                  [this]() { register_homestore_metablk_callback(); });
    if (need_format) {
        LOGW("Seems like we are booting/starting first time, Formatting!!");
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
}

void HSHomeObject::register_homestore_metablk_callback() {
    // register some callbacks for metadata recovery;
    using namespace homestore;
    HomeStore::instance()->meta_service().register_handler(
        HSHomeObject::s_shard_info_sub_type,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_shard_meta_blk_found(mblk, buf, size);
        },
        nullptr, true);

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

void HSHomeObject::on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
    std::string shard_info_str;
    shard_info_str.append(r_cast< const char* >(buf.bytes()), size);

    auto shard = deserialize_shard(shard_info_str);
    shard.metablk_cookie = mblk;

    // As shard info in the homestore metablk is always the latest state(OPEN or SEALED),
    // we can always create a shard from this shard info and once shard is deleted, the associated metablk will be
    // deleted too.
    do_commit_new_shard(shard);
}
} // namespace homeobject
