#include "homeobject.hpp"

#include <homestore/homestore.hpp>
#include <homestore/index_service.hpp>
#include <homestore/meta_service.hpp>
#include <iomgr/io_environment.hpp>

namespace homeobject {

const std::string HomeObjectImpl::s_shard_info_sub_type = "shard_info";

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

    //register some callbacks for metadata recovery;
    HomeStore::instance()->meta_service().register_handler(HomeObjectImpl::s_shard_info_sub_type,
                                                     [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
                                                         on_shard_meta_blk_found(mblk, buf, size);
                                                     },
                                                     nullptr,
                                                     true);

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

void HSHomeObject::on_shard_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
    const char* shard_info_json_str = r_cast<const char*>(buf.bytes());
    auto shard_info_json = nlohmann::json::parse(shard_info_json_str, shard_info_json_str + size);

    CShardInfo shard_info = std::make_shared<ShardInfo>();
    shard_info->id = shard_info_json["shard_id"].get<shard_id>();
    shard_info->placement_group = shard_info_json["pg_id"].get<pg_id>();
    shard_info->state = static_cast<ShardInfo::State>(shard_info_json["state"].get<int>());
    shard_info->total_capacity_bytes = shard_info_json["capacity"].get<uint64_t>();

    std::scoped_lock lock_guard(_pg_lock, _shard_lock);
    if (shard_info->state == ShardInfo::State::OPEN) {
        // create shard;
        auto pg_iter = _pg_map.find(shard_info->placement_group);
        RELEASE_ASSERT(pg_iter != _pg_map.end(), "Missing PG info");
        pg_iter->second.shards.push_back(shard_info);
        auto sequence_num = get_sequence_num_from_shard_id(shard_info->id);
        if (sequence_num > pg_iter->second.next_sequence_num_for_new_shard) {
            pg_iter->second.next_sequence_num_for_new_shard = sequence_num;
        }
        _shard_map[shard_info->id]  = shard_info;
    } else {
        auto shard_iter = _shard_map.find(shard_info->id);
        RELEASE_ASSERT(shard_iter != _shard_map.end(), "Missing shard info");
        shard_iter->second = shard_info;
    }
}

} // namespace homeobject
