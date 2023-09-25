#include "mem_homeobject.hpp"

#include <boost/uuid/random_generator.hpp>

namespace homeobject {

/// NOTE: We give ourselves the option to provide a different HR instance here than libhomeobject.a
extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application) {
    return std::make_shared< MemoryHomeObject >(std::move(application));
}

#if 0
void HomeObjectImpl::init_repl_svc() {
    auto lg = std::scoped_lock(_repl_lock);
    if (!_repl_svc) {
        _our_id = boost::uuids::random_generator()();
        LOGINFOMOD(homeobject, "SvcId faked: {}", to_string(_our_id));
        _our_id = _application.lock()->discover_svcid(_our_id);
        _repl_svc = home_replication::create_repl_service([](auto) { return nullptr; });
    }
}
#endif

ShardIndex::~ShardIndex() {
    for (auto it = btree_.begin(); it != btree_.end(); ++it) {
        delete it->second.blob_;
    }
}

} // namespace homeobject
