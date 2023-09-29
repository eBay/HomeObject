#include "file_homeobject.hpp"

#include <boost/uuid/random_generator.hpp>

SISL_OPTION_GROUP(homeobject_file,
                  (max_filesize, "", "max_filesize", "Maximum File (Shard) size",
                   cxxopts::value< uint32_t >()->default_value("1024"), "mb"))

namespace homeobject {

/// NOTE: We give ourselves the option to provide a different HR instance here than libhomeobject.a
extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application) {
    auto devices = application.lock()->devices();
    auto instance = std::make_shared< FileHomeObject >(std::move(application), *devices.begin());
    return instance;
}

FileHomeObject::FileHomeObject(std::weak_ptr< HomeObjectApplication >&& application,
                               std::filesystem::path const& root) :
        HomeObjectImpl::HomeObjectImpl(std::move(application)), file_store_(root) {
    _our_id = _application.lock()->discover_svcid(_our_id);
}

} // namespace homeobject
