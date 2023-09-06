#include "homeobject.hpp"

#include <homestore/homestore.hpp>
#include <homestore/index_service.hpp>
#include <iomgr/io_environment.hpp>

namespace homeobject {

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

    /// TODO need Repl service eventually yeah?
    using namespace homestore;
    uint32_t services = HS_SERVICE::META | HS_SERVICE::LOG_REPLICATED | HS_SERVICE::LOG_LOCAL | HS_SERVICE::DATA;

    bool need_format = HomeStore::instance()->start(
        hs_input_params{.devices = device_info, .app_mem_size = app_mem_size, .services = services});

    /// TODO how should this work?
    LOGWARN("Persistence Looks Vacant, Formatting!!");
    if (need_format) {
        HomeStore::instance()->format_and_start(std::map< uint32_t, hs_format_params >{
            {HS_SERVICE::META, hs_format_params{.size_pct = 5.0}},
            {HS_SERVICE::LOG_REPLICATED, hs_format_params{.size_pct = 10.0}},
            {HS_SERVICE::LOG_LOCAL, hs_format_params{.size_pct = 5.0}},
            {HS_SERVICE::DATA, hs_format_params{.size_pct = 50.0}},
            {HS_SERVICE::INDEX, hs_format_params{.size_pct = 30.0}},
        });
    }
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

} // namespace homeobject
