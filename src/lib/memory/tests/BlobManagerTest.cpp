#include <mutex>
#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/init/Init.h>
#include <folly/executors/GlobalExecutor.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "lib/memory/homeobject.hpp"

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
    homeobject::ShardInfo _shard_1;
    homeobject::ShardInfo _shard_2;
    homeobject::pg_id _pg_id{1u};
    peer_id _peer1;
    peer_id _peer2;
    blob_id _blob_id;

    void SetUp() override {
        app = std::make_shared< FixtureApp >();
        m_memory_homeobj = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
        _peer1 = m_memory_homeobj->our_uuid();
        _peer2 = boost::uuids::random_generator()();

        auto info = homeobject::PGInfo(_pg_id);
        info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});

        LOGDEBUG("Setup Pg");
        EXPECT_TRUE(m_memory_homeobj->pg_manager()->create_pg(std::move(info)).get());

        LOGDEBUG("Setup Shards");
        auto s_e = m_memory_homeobj->shard_manager()->create_shard(_pg_id, Mi).get();
        ASSERT_TRUE(!!s_e);
        s_e.then([this](auto&& i) { _shard_1 = std::move(i); });

        s_e = m_memory_homeobj->shard_manager()->create_shard(_pg_id, Mi).get();
        ASSERT_TRUE(!!s_e);
        s_e.then([this](auto&& i) { _shard_2 = std::move(i); });

        LOGDEBUG("Get on empty Shard: {}", _shard_1.id);
        auto g_e = m_memory_homeobj->blob_manager()->get(_shard_1.id, 0).get();
        ASSERT_FALSE(g_e);
        EXPECT_EQ(BlobError::UNKNOWN_BLOB, g_e.error());

        LOGDEBUG("Insert Blob to: {}", _shard_1.id);
        auto o_e =
            m_memory_homeobj->blob_manager()
                ->put(_shard_1.id, Blob{std::make_unique< sisl::byte_array_impl >(4 * Ki, 512u), "test_blob", 4 * Mi})
                .get();
        EXPECT_TRUE(!!o_e);
        o_e.then([this](auto&& b) mutable { _blob_id = std::move(b); });
    }

protected:
    std::shared_ptr< homeobject::HomeObject > m_memory_homeobj;
};

TEST_F(BlobManagerFixture, BasicTests) {
    auto const batch_sz = 4;
    std::mutex call_lock;
    auto calls = std::list< folly::SemiFuture< folly::Unit > >();

    auto t_v = std::vector< std::thread >();
    for (auto k = 0; batch_sz > k; ++k) {
        t_v.push_back(std::thread([this, &call_lock, &calls, batch_sz]() mutable {
            auto our_calls = std::list< folly::SemiFuture< folly::Unit > >();
            for (auto i = _blob_id + _shard_2.id + 1; (_blob_id + _shard_1.id + 1) + ((100 * Ki) / batch_sz) > i; ++i) {
                our_calls.push_back(
                    m_memory_homeobj->blob_manager()->get(_shard_1.id, _blob_id).deferValue([](auto const& e) {
                        EXPECT_TRUE(!!e);
                        e.then([](auto const& blob) {
                            EXPECT_STREQ(blob.user_key.c_str(), "test_blob");
                            EXPECT_EQ(blob.object_off, 4 * Mi);
                        });
                    }));
                our_calls.push_back(
                    m_memory_homeobj->blob_manager()->get(i, _blob_id).deferValue([](auto const& e) {}));
                our_calls.push_back(
                    m_memory_homeobj->blob_manager()->get(_shard_1.id, i).deferValue([](auto const&) {}));
                our_calls.push_back(
                    m_memory_homeobj->blob_manager()->get(_shard_2.id, i).deferValue([](auto const&) {}));
                our_calls.push_back(m_memory_homeobj->blob_manager()->put(i, Blob()).deferValue(
                    [](auto const& e) { EXPECT_EQ(BlobError::UNKNOWN_SHARD, e.error()); }));
                our_calls.push_back(folly::makeSemiFuture().deferValue([this](auto) {
                    m_memory_homeobj->blob_manager()
                        ->put(_shard_1.id,
                              Blob{std::make_unique< sisl::byte_array_impl >(4 * Ki, 512u), "test_blob", 4 * Mi})
                        .deferValue([](auto const& e) { EXPECT_TRUE(!!e); });
                }));
                our_calls.push_back(folly::makeSemiFuture().deferValue([this](auto) {
                    m_memory_homeobj->blob_manager()
                        ->put(_shard_2.id,
                              Blob{std::make_unique< sisl::byte_array_impl >(8 * Ki, 512u), "test_blob_2", 4 * Mi})
                        .deferValue([](auto const& e) { EXPECT_TRUE(!!e); });
                }));
                our_calls.push_back(
                    m_memory_homeobj->blob_manager()->del(i, _blob_id).deferValue([](auto const& e) {}));
                our_calls.push_back(m_memory_homeobj->blob_manager()->del(_shard_1.id, i).deferValue([](auto const& e) {
                    EXPECT_EQ(BlobError::UNKNOWN_BLOB, e.error());
                }));
            }

            auto lg = std::scoped_lock(call_lock);
            calls.splice(calls.end(), std::move(our_calls));
        }));
    }
    for (auto& t : t_v)
        t.join();
    folly::collectAll(calls).via(folly::getGlobalCPUExecutor()).get();
    EXPECT_TRUE(m_memory_homeobj->shard_manager()->seal_shard(_shard_1.id).get());
    auto p_e =
        m_memory_homeobj->blob_manager()
            ->put(_shard_1.id, Blob{std::make_unique< sisl::byte_array_impl >(4 * Ki, 512u), "test_blob", 4 * Mi})
            .get();
    ASSERT_TRUE(!p_e);
    EXPECT_EQ(BlobError::INVALID_ARG, p_e.error());

    EXPECT_TRUE(m_memory_homeobj->blob_manager()->del(_shard_1.id, _blob_id).get());
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
