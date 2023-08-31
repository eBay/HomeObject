#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/init/Init.h>
#include <folly/executors/GlobalExecutor.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "mocks/mock_homeobject.hpp"

using homeobject::Blob;
using homeobject::blob_id;
using homeobject::BlobError;
using homeobject::peer_id;

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

class BlobManagerFixture : public ::testing::Test {
    std::shared_ptr< FixtureApp > app;

public:
    homeobject::ShardInfo _shard;
    homeobject::pg_id _pg_id{1u};
    peer_id _peer1;
    peer_id _peer2;
    blob_id _blob_id;

    void SetUp() override {
        app = std::make_shared< FixtureApp >();
        m_mock_homeobj = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
        _peer1 = m_mock_homeobj->our_uuid();
        _peer2 = boost::uuids::random_generator()();

        auto info = homeobject::PGInfo(_pg_id);
        info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});

        LOGDEBUG("Setup Pg");
        EXPECT_TRUE(m_mock_homeobj->pg_manager()->create_pg(std::move(info)).via(folly::getGlobalCPUExecutor()).get());

        LOGDEBUG("Setup Shard");
        auto s_e = m_mock_homeobj->shard_manager()->create_shard(_pg_id, Mi).via(folly::getGlobalCPUExecutor()).get();
        ASSERT_TRUE(!!s_e);
        s_e.then([this](auto&& i) { _shard = std::move(i); });

        LOGDEBUG("Insert Blob to: {}", _shard.id);
        auto o_e =
            m_mock_homeobj->blob_manager()
                ->put(_shard.id, Blob{std::make_unique< sisl::byte_array_impl >(4 * Ki, 512u), "test_blob", 4 * Mi})
                .via(folly::getGlobalCPUExecutor())
                .get();
        EXPECT_TRUE(!!o_e);
        o_e.then([this](auto&& b) mutable { _blob_id = std::move(b); });
    }

protected:
    std::shared_ptr< homeobject::HomeObject > m_mock_homeobj;
};

TEST_F(BlobManagerFixture, BasicTests) {
    auto calls = std::vector< folly::SemiFuture< folly::Unit > >();
    for (auto i = _blob_id + _shard.id + 1; (_blob_id + _shard.id + 1) + (8 * Ki) > i; ++i) {
        calls.push_back(m_mock_homeobj->blob_manager()->get(_shard.id, _blob_id).deferValue([](auto const& e) {
            EXPECT_TRUE(!!e);
            e.then([](auto const& blob) {
                EXPECT_STREQ(blob.user_key.c_str(), "test_blob");
                EXPECT_EQ(blob.object_off, 4 * Mi);
            });
        }));
        calls.push_back(m_mock_homeobj->blob_manager()->get(i, _blob_id).deferValue([](auto const& e) {
            EXPECT_EQ(BlobError::UNKNOWN_SHARD, e.error());
        }));
        calls.push_back(m_mock_homeobj->blob_manager()->get(_shard.id, i).deferValue([](auto const&) {}));
        calls.push_back(m_mock_homeobj->blob_manager()->put(i, Blob()).deferValue(
            [](auto const& e) { EXPECT_EQ(BlobError::UNKNOWN_SHARD, e.error()); }));
        calls.push_back(
            m_mock_homeobj->blob_manager()
                ->put(_shard.id, Blob{std::make_unique< sisl::byte_array_impl >(4 * Ki, 512u), "test_blob", 4 * Mi})
                .deferValue([](auto const& e) { EXPECT_TRUE(!!e); }));
        calls.push_back(m_mock_homeobj->blob_manager()->del(i, _blob_id).deferValue([](auto const& e) {
            EXPECT_EQ(BlobError::UNKNOWN_SHARD, e.error());
        }));
        calls.push_back(m_mock_homeobj->blob_manager()->del(_shard.id, i).deferValue([](auto const& e) {
            EXPECT_EQ(BlobError::UNKNOWN_BLOB, e.error());
        }));
    }
    folly::collectAll(calls).via(folly::getGlobalCPUExecutor()).get();

    EXPECT_TRUE(m_mock_homeobj->blob_manager()->del(_shard.id, _blob_id).get());
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
