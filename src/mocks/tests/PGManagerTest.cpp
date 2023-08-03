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
using homeobject::PGInfo;
using homeobject::PGMember;

SISL_LOGGING_INIT(logging, homeobject)
SISL_OPTIONS_ENABLE(logging)

class PgManagerFixture : public ::testing::Test {
public:
    void SetUp() override {
        auto params = homeobject::init_params{[](homeobject::peer_id const&) { return std::string(); }};
        m_mock_homeobj = homeobject::init_homeobject(params);
    }

protected:
    std::shared_ptr< homeobject::HomeObject > m_mock_homeobj;
};

TEST_F(PgManagerFixture, CreatePgEmpty) {
    m_mock_homeobj->pg_manager()->create_pg(
        PGInfo(0u), [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::INVALID_ARG, e); });
}

TEST_F(PgManagerFixture, CreatePgNoLeader) {
    auto info = PGInfo(0u);
    info.members.insert(PGMember{boost::uuids::random_generator()()});
    m_mock_homeobj->pg_manager()->create_pg(
        info, [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::INVALID_ARG, e); });
}

TEST_F(PgManagerFixture, CreatePgNoCb) {
    auto info = PGInfo(0u);
    info.members.insert(PGMember{boost::uuids::random_generator()(), "peer1", 1});
    m_mock_homeobj->pg_manager()->create_pg(info, nullptr);
}

class PgManagerFixtureWPg : public PgManagerFixture {
public:
    homeobject::pg_id _pg_id{1u};
    homeobject::peer_id _peer1;
    homeobject::peer_id _peer2;

    void SetUp() override {
        auto params = homeobject::init_params{[](homeobject::peer_id const&) { return std::string(); }};
        m_mock_homeobj = homeobject::init_homeobject(params);
        _peer1 = boost::uuids::random_generator()();
        _peer2 = boost::uuids::random_generator()();

        auto info = PGInfo(_pg_id);
        info.members.insert(PGMember{_peer1, "peer1", 1});
        info.members.insert(PGMember{_peer2, "peer2", 0});
        m_mock_homeobj->pg_manager()->create_pg(info,
                                                [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::OK, e); });
    }
};

TEST_F(PgManagerFixtureWPg, CreateDuplicatePg) {
    auto info = PGInfo(_pg_id);
    info.members.insert(PGMember{boost::uuids::random_generator()(), "peer3", 6});
    info.members.insert(PGMember{boost::uuids::random_generator()(), "peer4", 2});
    m_mock_homeobj->pg_manager()->create_pg(
        info, [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::INVALID_ARG, e); });
}

TEST_F(PgManagerFixtureWPg, MigrateUnknownPg) {
    m_mock_homeobj->pg_manager()->replace_member(
        UINT16_MAX, boost::uuids::random_generator()(), PGMember{boost::uuids::random_generator()()},
        [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::UNKNOWN_PG, e); });
}

TEST_F(PgManagerFixtureWPg, MigrateUnknownPeer) {
    m_mock_homeobj->pg_manager()->replace_member(
        _pg_id, boost::uuids::random_generator()(), PGMember{boost::uuids::random_generator()()},
        [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::UNKNOWN_PEER, e); });
}

TEST_F(PgManagerFixtureWPg, MigrateSamePeer) {
    m_mock_homeobj->pg_manager()->replace_member(_pg_id, _peer1, PGMember{_peer1}, [](homeobject::PGError e) {
        EXPECT_EQ(homeobject::PGError::INVALID_ARG, e);
    });
}

TEST_F(PgManagerFixtureWPg, MigrateDuplicate) {
    m_mock_homeobj->pg_manager()->replace_member(_pg_id, _peer1, PGMember{_peer2}, [](homeobject::PGError e) {
        EXPECT_EQ(homeobject::PGError::INVALID_ARG, e);
    });
}

TEST_F(PgManagerFixtureWPg, MigrateOK) {
    m_mock_homeobj->pg_manager()->replace_member(_pg_id, _peer2, PGMember{boost::uuids::random_generator()()},
                                                 [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::OK, e); });
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    return RUN_ALL_TESTS();
}
