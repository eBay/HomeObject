#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/init/Init.h>
#include <folly/executors/GlobalExecutor.h>

#include <homeobject/pg_manager.hpp>
#include "lib/tests/fixture_app.hpp"

using homeobject::PGError;
using homeobject::PGInfo;
using homeobject::PGMember;

TEST_F(TestFixture, CreatePgEmpty) {
    EXPECT_EQ(homeobj_->pg_manager()->create_pg(PGInfo(0u)).get().error(), PGError::INVALID_ARG);
}

TEST_F(TestFixture, CreatePgNoLeader) {
    auto info = PGInfo(0u);
    info.members.insert(PGMember{boost::uuids::random_generator()()});
    EXPECT_EQ(homeobj_->pg_manager()->create_pg(std::move(info)).get().error(), PGError::INVALID_ARG);
}

TEST_F(TestFixture, CreatePgNotMember) {
    auto info = PGInfo(0u);
    info.members.insert(PGMember{boost::uuids::random_generator()(), "unknown", 1});
    EXPECT_EQ(homeobj_->pg_manager()->create_pg(std::move(info)).get().error(), PGError::INVALID_ARG);
}

TEST_F(TestFixture, CreateDuplicatePg) {
    auto info = PGInfo(_pg_id);
    info.members.insert(PGMember{boost::uuids::random_generator()(), "peer3", 6});
    info.members.insert(PGMember{boost::uuids::random_generator()(), "peer4", 2});
    EXPECT_EQ(homeobj_->pg_manager()->create_pg(std::move(info)).get().error(), PGError::INVALID_ARG);
}

TEST_F(TestFixture, Migrate) {
    EXPECT_EQ(homeobj_->pg_manager()
                  ->replace_member(UINT16_MAX, boost::uuids::random_generator()(),
                                   PGMember{boost::uuids::random_generator()()})
                  .get()
                  .error(),
              PGError::UNSUPPORTED_OP);
    EXPECT_EQ(
        homeobj_->pg_manager()
            ->replace_member(_pg_id, boost::uuids::random_generator()(), PGMember{boost::uuids::random_generator()()})
            .get()
            .error(),
        PGError::UNSUPPORTED_OP);
    EXPECT_EQ(homeobj_->pg_manager()->replace_member(_pg_id, _peer1, PGMember{_peer1}).get().error(),
              PGError::INVALID_ARG);
    EXPECT_EQ(homeobj_->pg_manager()->replace_member(_pg_id, _peer1, PGMember{_peer2}).get().error(),
              PGError::INVALID_ARG);
    EXPECT_EQ(homeobj_->pg_manager()
                  ->replace_member(_pg_id, _peer1, PGMember{boost::uuids::random_generator()()})
                  .get()
                  .error(),
              PGError::INVALID_ARG);
    EXPECT_FALSE(
        homeobj_->pg_manager()->replace_member(_pg_id, _peer2, PGMember{boost::uuids::random_generator()()}).get());
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
