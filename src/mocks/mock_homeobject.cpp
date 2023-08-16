#include "mock_homeobject.hpp"

namespace homeobject {

extern std::shared_ptr< HomeObject > init_homeobject(HomeObject::init_params&& params) {
    return std::make_shared< MockHomeObject >(std::move(params));
}

void HomeObjectImpl::init_repl_svc() {
    auto lg = std::scoped_lock(_repl_lock);
    if (!_repl_svc) _repl_svc = home_replication::create_repl_service(*this);
}

} // namespace homeobject
