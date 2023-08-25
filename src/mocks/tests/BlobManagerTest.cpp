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
using homeobject::Blob;
using homeobject::blob_id;
using homeobject::BlobError;
using homeobject::peer_id;

SISL_LOGGING_INIT(logging, HOMEOBJECT_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

class BlobManagerFixture : public ::testing::Test {
public:
    homeobject::ShardInfo _shard;
    homeobject::pg_id _pg_id{1u};
    peer_id _peer1;
    peer_id _peer2;
    blob_id _blob_id;

    void SetUp() override {
        m_mock_homeobj = homeobject::init_homeobject(homeobject::HomeObject::init_params{
            [](auto p) { return folly::makeSemiFuture(p.value()); }, [](auto) { return "test_fixture"; }});

        _peer1 = m_mock_homeobj->our_uuid();
        _peer2 = boost::uuids::random_generator()();

        auto info = homeobject::PGInfo(_pg_id);
        info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});

        LOGDEBUG("Setup Pg");
        EXPECT_TRUE(m_mock_homeobj->pg_manager()->create_pg(std::move(info)).get());

        LOGDEBUG("Setup Shard");
        auto s_e = m_mock_homeobj->shard_manager()->create_shard(_pg_id, Mi).get();
        ASSERT_TRUE(!!s_e);
        s_e.then([this](auto&& i) { _shard = std::move(i); });

        LOGDEBUG("Insert Blob to: {}", _shard.id);
        auto o_e =
            m_mock_homeobj->blob_manager()
                ->put(_shard.id, Blob{std::make_unique< sisl::byte_array_impl >(4 * Ki, 512u), "test_blob", 4 * Mi})
                .get();
        EXPECT_TRUE(!!o_e);
        o_e.then([this](auto&& b) mutable { _blob_id = std::move(b); });
    }

protected:
    std::shared_ptr< homeobject::HomeObject > m_mock_homeobj;
};

TEST_F(BlobManagerFixture, UnknownShardOrBlob) {
    EXPECT_EQ(BlobError::UNKNOWN_SHARD, m_mock_homeobj->blob_manager()->put(_shard.id + 1, Blob()).get().error());
    EXPECT_EQ(BlobError::UNKNOWN_SHARD,
              m_mock_homeobj->blob_manager()->get(_shard.id + 1, UINT64_MAX, 0, 0).get().error());
    EXPECT_EQ(BlobError::UNKNOWN_BLOB, m_mock_homeobj->blob_manager()->get(_shard.id, UINT64_MAX, 0, 0).get().error());
    EXPECT_EQ(BlobError::UNKNOWN_SHARD, m_mock_homeobj->blob_manager()->del(_shard.id + 1, _blob_id).get().error());
    EXPECT_EQ(BlobError::UNKNOWN_BLOB, m_mock_homeobj->blob_manager()->del(_shard.id, _blob_id + 1).get().error());
}

TEST_F(BlobManagerFixture, KnownBlob) {
    auto e = m_mock_homeobj->blob_manager()->get(_shard.id, _blob_id, 0, 0).get();
    EXPECT_TRUE(!!e);
    e.then([](auto const& blob) {
        EXPECT_STREQ(blob.user_key.c_str(), "test_blob");
        EXPECT_EQ(blob.object_off, 4 * Mi);
    });
    EXPECT_TRUE(m_mock_homeobj->blob_manager()->del(_shard.id, _blob_id).get());
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    return RUN_ALL_TESTS();
}
