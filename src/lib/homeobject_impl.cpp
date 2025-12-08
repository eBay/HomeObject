#include "homeobject_impl.hpp"

#include <algorithm>

SISL_OPTION_GROUP(homeobject,
                  (executor_type, "", "executor", "Executor to use for Future deferal",
                   ::cxxopts::value< std::string >()->default_value("immediate"), "immediate|cpu|io"));

SISL_LOGGING_DEF(HOMEOBJECT_LOG_MODS)

namespace homeobject {

HomeObjectImpl::HomeObjectImpl(std::weak_ptr< HomeObjectApplication >&& application) :
        _application(std::move(application)) {
    auto exe_type = SISL_OPTIONS["executor"].as< std::string >();
    std::transform(exe_type.begin(), exe_type.end(), exe_type.begin(), ::tolower);

    if ("immediate" == exe_type) [[likely]]
        executor_ = &folly::QueuedImmediateExecutor::instance();
    else if ("io" == exe_type)
        executor_ = folly::getGlobalIOExecutor();
    else if ("cpu" == exe_type)
        executor_ = folly::getGlobalCPUExecutor();
    else
        RELEASE_ASSERT(false, "Unknown Folly Executor type: [{}]", exe_type);
    LOGI("initialized with [executor={}]", exe_type);
}
} // namespace homeobject
