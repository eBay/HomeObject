#include "homeobject_impl.hpp"

SISL_LOGGING_DEF(HOMEOBJECT_LOG_MODS)

namespace homeobject {

HomeObjectImpl::HomeObjectImpl(std::weak_ptr< HomeObjectApplication >&& application) :
        _application(std::move(application)) {
    LOGI("HomeObjectImpl initialized");
}
} // namespace homeobject
