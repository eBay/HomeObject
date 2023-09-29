#include "file_homeobject.hpp"

#include <boost/uuid/random_generator.hpp>

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
