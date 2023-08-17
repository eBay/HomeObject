#include <boost/uuid/random_generator.hpp>

#include "mock_homeobject.hpp"

namespace homeobject {

/// NOTE: We give ourselves the option to provide a different HR instance here than libhomeobject.a
void HomeObjectImpl::init_repl_svc() {
    auto lg = std::scoped_lock(_repl_lock);
    if (!_repl_svc) {
        _our_id = boost::uuids::random_generator()();
        LOGINFOMOD(homeobject, "SvcId mocked: {}", to_string(_our_id));
        _repl_svc = home_replication::create_repl_service([](auto) { return nullptr; });
    }
}

extern std::shared_ptr< HomeObject > init_homeobject(HomeObject::init_params&& params) {
    return std::make_shared< MockHomeObject >(std::move(params));
}

} // namespace homeobject
