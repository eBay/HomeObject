#include <boost/uuid/random_generator.hpp>

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
    // info.members.insert(PGMember{boost::uuids::random_generator()(), "peer4", 2});
    EXPECT_EQ(homeobj_->pg_manager()->create_pg(std::move(info)).get().error(), PGError::INVALID_ARG);
}

TEST_F(TestFixture, Migrate) {
    EXPECT_EQ(homeobj_->pg_manager()
                  ->replace_member(UINT16_MAX, boost::uuids::random_generator()(),
                                   PGMember{boost::uuids::random_generator()()}, 0)
                  .get()
                  .error(),
              PGError::UNKNOWN_PG);
    EXPECT_EQ(homeobj_->pg_manager()->replace_member(_pg_id, _peer1, PGMember{_peer1}).get().error(),
              PGError::INVALID_ARG);
    // TODO enable after HO test framework is enabled
#if 0
    EXPECT_EQ(homeobj_->pg_manager()
                  ->replace_member(_pg_id, _peer1, PGMember{boost::uuids::random_generator()()}, 0)
                  .get()
                  .error(),
              PGError::INVALID_ARG);
    EXPECT_FALSE(
        homeobj_->pg_manager()->replace_member(_pg_id, _peer2, PGMember{boost::uuids::random_generator()()}).get());
#endif
}
