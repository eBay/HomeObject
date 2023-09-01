#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "lib/memory/homeobject.hpp"

using homeobject::PGError;
using homeobject::PGInfo;
using homeobject::PGMember;

SISL_LOGGING_INIT(logging, HOMEOBJECT_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

class FixtureApp : public homeobject::HomeObjectApplication {
public:
    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return 2; }
    std::list< std::filesystem::path > devices() const override { return std::list< std::filesystem::path >(); }
    homeobject::peer_id discover_svcid(std::optional< homeobject::peer_id > const& p) const override {
        return p.value();
    }
    std::string lookup_peer(homeobject::peer_id const&) const override { return "test_fixture.com"; }
};

class PgManagerFixture : public ::testing::Test {
public:
    void SetUp() override {
        app = std::make_shared< FixtureApp >();
        m_memory_homeobj = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    }

protected:
    std::shared_ptr< FixtureApp > app;
    std::shared_ptr< homeobject::HomeObject > m_memory_homeobj;
};

TEST_F(PgManagerFixture, CreatePgEmpty) {
    EXPECT_EQ(m_memory_homeobj->pg_manager()->create_pg(PGInfo(0u)).get().error(), PGError::INVALID_ARG);
}

TEST_F(PgManagerFixture, CreatePgNoLeader) {
    auto info = PGInfo(0u);
    info.members.insert(PGMember{boost::uuids::random_generator()()});
    EXPECT_EQ(m_memory_homeobj->pg_manager()->create_pg(std::move(info)).get().error(), PGError::INVALID_ARG);
}

TEST_F(PgManagerFixture, CreatePgNotMember) {
    auto info = PGInfo(0u);
    info.members.insert(PGMember{boost::uuids::random_generator()(), "unknown", 1});
    EXPECT_EQ(m_memory_homeobj->pg_manager()->create_pg(std::move(info)).get().error(), PGError::INVALID_ARG);
}

class PgManagerFixtureWPg : public PgManagerFixture {
public:
    homeobject::pg_id _pg_id{1u};
    homeobject::peer_id _peer1;
    homeobject::peer_id _peer2;

    void SetUp() override {
        PgManagerFixture::SetUp();
        _peer1 = m_memory_homeobj->our_uuid();
        _peer2 = boost::uuids::random_generator()();

        auto info = PGInfo(_pg_id);
        info.members.insert(PGMember{_peer1, "peer1", 1});
        info.members.insert(PGMember{_peer2, "peer2", 0});
        EXPECT_TRUE(m_memory_homeobj->pg_manager()->create_pg(std::move(info)).get());
    }
};

TEST_F(PgManagerFixtureWPg, CreateDuplicatePg) {
    auto info = PGInfo(_pg_id);
    info.members.insert(PGMember{boost::uuids::random_generator()(), "peer3", 6});
    info.members.insert(PGMember{boost::uuids::random_generator()(), "peer4", 2});
    EXPECT_EQ(m_memory_homeobj->pg_manager()->create_pg(std::move(info)).get().error(), PGError::INVALID_ARG);
}

TEST_F(PgManagerFixtureWPg, Migrate) {
    EXPECT_EQ(m_memory_homeobj->pg_manager()
                  ->replace_member(UINT16_MAX, boost::uuids::random_generator()(),
                                   PGMember{boost::uuids::random_generator()()})
                  .get()
                  .error(),
              PGError::UNKNOWN_PG);
    EXPECT_EQ(
        m_memory_homeobj->pg_manager()
            ->replace_member(_pg_id, boost::uuids::random_generator()(), PGMember{boost::uuids::random_generator()()})
            .get()
            .error(),
        PGError::UNKNOWN_PEER);
    EXPECT_EQ(m_memory_homeobj->pg_manager()->replace_member(_pg_id, _peer1, PGMember{_peer1}).get().error(),
              PGError::INVALID_ARG);
    EXPECT_EQ(m_memory_homeobj->pg_manager()->replace_member(_pg_id, _peer1, PGMember{_peer2}).get().error(),
              PGError::INVALID_ARG);
    EXPECT_EQ(m_memory_homeobj->pg_manager()
                  ->replace_member(_pg_id, _peer1, PGMember{boost::uuids::random_generator()()})
                  .get()
                  .error(),
              PGError::INVALID_ARG);
    EXPECT_TRUE(m_memory_homeobj->pg_manager()
                    ->replace_member(_pg_id, _peer2, PGMember{boost::uuids::random_generator()()})
                    .get());
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);
    return RUN_ALL_TESTS();
}
