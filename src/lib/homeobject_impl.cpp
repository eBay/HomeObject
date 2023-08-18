#include "homeobject_impl.hpp"

#include <boost/uuid/uuid_io.hpp>
#include <folly/executors/QueuedImmediateExecutor.h>

namespace homeobject {

extern std::shared_ptr< HomeObject > init_homeobject(HomeObject::init_params&& params) {
    LOGINFOMOD(homeobject, "Initializing HomeObject");
    return std::make_shared< HomeObjectImpl >(std::move(params));
}

void HomeObjectImpl::init_repl_svc() {
    auto lg = std::scoped_lock(_repl_lock);
    if (!_repl_svc) {
        // TODO this should come from persistence.
        LOGINFOMOD(homeobject, "First time start-up...initiating request for SvcId.");
        _our_id = _svcid_routines.get_svc_id().via(&folly::QueuedImmediateExecutor::instance()).get();
        LOGINFOMOD(homeobject, "SvcId received: {}", to_string(_our_id));
        _repl_svc = home_replication::create_repl_service([](auto) { return nullptr; });
    }
}

} // namespace homeobject
