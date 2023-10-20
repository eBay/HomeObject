#include <folly/init/Init.h>
#include <sisl/options/options.h>
#include <homeobject/homeobject.hpp>
#include <boost/uuid/random_generator.hpp>

SISL_LOGGING_INIT(HOMEOBJECT_LOG_MODS)

SISL_OPTIONS_ENABLE(logging, homeobject_options)

class TestApp : public homeobject::HomeObjectApplication {
public:
    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return 1; }
    std::list< std::filesystem::path > devices() const override { return std::list< std::filesystem::path >(); }
    homeobject::peer_id_t discover_svcid(std::optional< homeobject::peer_id_t > const& p) const override {
        return boost::uuids::random_generator()();
    }
    std::string lookup_peer(homeobject::peer_id_t const&) const override { return "test_package.com"; }
};

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, logging, homeobject_options)
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");

    auto parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);

    auto a = std::make_shared< TestApp >();
    homeobject::init_homeobject(a);
    return 0;
}
