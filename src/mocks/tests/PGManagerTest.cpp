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
using homeobject::PGError;
using homeobject::PGInfo;
using homeobject::PGMember;

SISL_LOGGING_INIT(logging, HOMEOBJECT_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

class PgManagerFixture : public ::testing::Test {
public:
    void SetUp() override {
        m_mock_homeobj = homeobject::init_homeobject(homeobject::HomeObject::init_params{
            [](auto p) { return folly::makeSemiFuture(p.value()); }, [](auto) { return "test_fixture"; }});
    }

protected:
    std::shared_ptr< homeobject::HomeObject > m_mock_homeobj;
};

TEST_F(PgManagerFixture, CreatePgEmpty) {
    EXPECT_EQ(m_mock_homeobj->pg_manager()->create_pg(PGInfo(0u)).get(), PGError::INVALID_ARG);
}

TEST_F(PgManagerFixture, CreatePgNoLeader) {
    auto info = PGInfo(0u);
    info.members.insert(PGMember{boost::uuids::random_generator()()});
    EXPECT_EQ(m_mock_homeobj->pg_manager()->create_pg(std::move(info)).get(), PGError::INVALID_ARG);
}

TEST_F(PgManagerFixture, CreatePgNotMember) {
    auto info = PGInfo(0u);
    info.members.insert(PGMember{boost::uuids::random_generator()(), "unknown", 1});
    EXPECT_EQ(m_mock_homeobj->pg_manager()->create_pg(std::move(info)).get(), PGError::INVALID_ARG);
}

class PgManagerFixtureWPg : public PgManagerFixture {
public:
    homeobject::pg_id _pg_id{1u};
    homeobject::peer_id _peer1;
    homeobject::peer_id _peer2;

    void SetUp() override {
        PgManagerFixture::SetUp();
        _peer1 = m_mock_homeobj->our_uuid();
        _peer2 = boost::uuids::random_generator()();

        auto info = PGInfo(_pg_id);
        info.members.insert(PGMember{_peer1, "peer1", 1});
        info.members.insert(PGMember{_peer2, "peer2", 0});
        EXPECT_EQ(m_mock_homeobj->pg_manager()->create_pg(std::move(info)).get(), PGError::OK);
    }
};

TEST_F(PgManagerFixtureWPg, CreateDuplicatePg) {
    auto info = PGInfo(_pg_id);
    info.members.insert(PGMember{boost::uuids::random_generator()(), "peer3", 6});
    info.members.insert(PGMember{boost::uuids::random_generator()(), "peer4", 2});
    EXPECT_EQ(m_mock_homeobj->pg_manager()->create_pg(std::move(info)).get(), PGError::INVALID_ARG);
}

TEST_F(PgManagerFixtureWPg, Migrate) {
    EXPECT_EQ(m_mock_homeobj->pg_manager()
                  ->replace_member(UINT16_MAX, boost::uuids::random_generator()(),
                                   PGMember{boost::uuids::random_generator()()})
                  .get(),
              PGError::UNKNOWN_PG);
    EXPECT_EQ(
        m_mock_homeobj->pg_manager()
            ->replace_member(_pg_id, boost::uuids::random_generator()(), PGMember{boost::uuids::random_generator()()})
            .get(),
        PGError::UNKNOWN_PEER);
    EXPECT_EQ(m_mock_homeobj->pg_manager()->replace_member(_pg_id, _peer1, PGMember{_peer1}).get(),
              PGError::INVALID_ARG);
    EXPECT_EQ(m_mock_homeobj->pg_manager()->replace_member(_pg_id, _peer1, PGMember{_peer2}).get(),
              PGError::INVALID_ARG);
    EXPECT_EQ(m_mock_homeobj->pg_manager()
                  ->replace_member(_pg_id, _peer1, PGMember{boost::uuids::random_generator()()})
                  .get(),
              PGError::INVALID_ARG);
    EXPECT_EQ(m_mock_homeobj->pg_manager()
                  ->replace_member(_pg_id, _peer2, PGMember{boost::uuids::random_generator()()})
                  .get(),
              PGError::OK);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    return RUN_ALL_TESTS();
}
