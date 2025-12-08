#include "mem_homeobject.hpp"

namespace homeobject {

/// NOTE: We give ourselves the option to provide a different HR instance here than libhomeobject.a
extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application) {
    return std::make_shared< MemoryHomeObject >(std::move(application));
}

MemoryHomeObject::MemoryHomeObject(std::weak_ptr< HomeObjectApplication >&& application) :
        HomeObjectImpl::HomeObjectImpl(std::move(application)) {
    _our_id = _application.lock()->discover_svcid(std::nullopt);
}

void MemoryHomeObject::shutdown() { LOGI("MemoryHomeObject: Executing shutdown procedure"); }

ShardIndex::~ShardIndex() {
    for (auto it = btree_.begin(); it != btree_.end(); ++it) {
        delete it->second.blob_;
    }
}

} // namespace homeobject
