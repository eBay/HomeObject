
#include "homeobject_impl.hpp"

namespace homeobject {

extern std::shared_ptr< HomeObject > init_homeobject(HomeObject::init_params&& params) {
    LOGINFOMOD(homeobject, "Initializing HomeObject");
    return std::make_shared< HomeObjectImpl >(std::move(params));
}

void HomeObjectImpl::init_repl_svc() {
    auto lg = std::scoped_lock(_repl_lock);
    if (!_repl_svc) _repl_svc = home_replication::create_repl_service(*this);
}

} // namespace homeobject
