#include <sisl/options/options.h>
#include <homeobject/homeobject.hpp>
#include <boost/uuid/random_generator.hpp>

SISL_LOGGING_INIT(HOMEOBJECT_LOG_MODS)

SISL_OPTIONS_ENABLE(logging)

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, logging)
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    sisl::logging::SetModuleLogLevel("home_replication", spdlog::level::level_enum::trace);

    auto homeobj = init_homeobject(homeobject::HomeObject::init_params{
        []() { return boost::uuids::random_generator()(); }, [](auto) { return homeobject::endpoint(); }});

    return 0;
}
