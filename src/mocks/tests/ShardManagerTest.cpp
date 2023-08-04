#include <chrono>
#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "mocks/mock_homeobject.hpp"

using namespace std::chrono_literals;
using homeobject::shard_id;
using homeobject::ShardError;
using homeobject::ShardInfo;

SISL_LOGGING_INIT(logging, homeobject)
SISL_OPTIONS_ENABLE(logging)

class ShardManagerFixture : public ::testing::Test {
public:
    homeobject::pg_id _pg_id{1u};
    homeobject::peer_id _peer1;
    homeobject::peer_id _peer2;

    void SetUp() override {
        auto params = homeobject::init_params{[](homeobject::peer_id const&) { return std::string(); }};
        m_mock_homeobj = homeobject::init_homeobject(params);

        _peer1 = boost::uuids::random_generator()();
        _peer2 = boost::uuids::random_generator()();

        auto info = homeobject::PGInfo(_pg_id);
        info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});
        m_mock_homeobj->pg_manager()->create_pg(info,
                                                [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::OK, e); });
    }

protected:
    std::shared_ptr< homeobject::HomeObject > m_mock_homeobj;
};

TEST_F(ShardManagerFixture, CreateShardNoPg) {
    m_mock_homeobj->shard_manager()->create_shard(
        0u, Mi, [](std::variant< ShardInfo, ShardError > const& v, std::optional< homeobject::peer_id >) {
            EXPECT_TRUE(std::holds_alternative< ShardError >(v));
            EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_PG);
        });
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    return RUN_ALL_TESTS();
}
