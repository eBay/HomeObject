#include <chrono>
#include <future>
#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "mocks/mock_homeobject.hpp"

#define START_TEST auto _f = _p.get_future();
#define END_TEST EXPECT_TRUE(_f.get());

using namespace std::chrono_literals;
using homeobject::Blob;
using homeobject::blob_id;
using homeobject::BlobError;
using homeobject::peer_id;

SISL_LOGGING_INIT(logging, homeobject)
SISL_OPTIONS_ENABLE(logging)

class BlobManagerFixture : public ::testing::Test {
public:
    homeobject::ShardInfo _shard;
    homeobject::pg_id _pg_id{1u};
    peer_id _peer1;
    peer_id _peer2;
    std::promise< bool > _p;
    std::promise< bool > _p_init_blob;
    blob_id _blob_id;

    void SetUp() override {
        auto params = homeobject::init_params{[](peer_id const&) { return std::string(); }};
        m_mock_homeobj = homeobject::init_homeobject(params);

        _peer1 = boost::uuids::random_generator()();
        _peer2 = boost::uuids::random_generator()();

        auto info = homeobject::PGInfo(_pg_id);
        info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});
        m_mock_homeobj->pg_manager()->create_pg(info,
                                                [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::OK, e); });
        m_mock_homeobj->shard_manager()->create_shard(
            _pg_id, Mi,
            [this](std::variant< homeobject::ShardInfo, homeobject::ShardError > const& v,
                   std::optional< peer_id >) mutable {
                ASSERT_TRUE(std::holds_alternative< homeobject::ShardInfo >(v));
                _shard = std::get< homeobject::ShardInfo >(v);
            });

        auto temp_f = _p_init_blob.get_future();
        m_mock_homeobj->blob_manager()->put(
            _shard.id, Blob{std::make_unique< sisl::byte_array_impl >(4 * Ki, 512u), "test_blob", 4 * Mi},
            [this](std::variant< blob_id, BlobError > const& v, std::optional< peer_id > p) mutable {
                ASSERT_TRUE(std::holds_alternative< blob_id >(v));
                _blob_id = std::get< blob_id >(v);
                EXPECT_TRUE(!p);
                _p_init_blob.set_value(true);
            });
        ASSERT_TRUE(temp_f.get());
    }

protected:
    std::shared_ptr< homeobject::HomeObject > m_mock_homeobj;
};

TEST_F(BlobManagerFixture, PutUnknownShard) {
    START_TEST
    m_mock_homeobj->blob_manager()->put(
        _shard.id + 1, Blob(), [this](std::variant< blob_id, BlobError > const& v, std::optional< peer_id > p) mutable {
            ASSERT_TRUE(std::holds_alternative< BlobError >(v));
            EXPECT_EQ(std::get< BlobError >(v), BlobError::UNKNOWN_SHARD);
            EXPECT_TRUE(!p);
            _p.set_value(true);
        });
    END_TEST
}

TEST_F(BlobManagerFixture, GetUnknownShard) {
    START_TEST
    m_mock_homeobj->blob_manager()->get(
        _shard.id + 1, UINT64_MAX, 0, 0,
        [this](std::variant< Blob, BlobError > const& v, std::optional< peer_id > p) mutable {
            ASSERT_TRUE(std::holds_alternative< BlobError >(v));
            EXPECT_EQ(std::get< BlobError >(v), BlobError::UNKNOWN_SHARD);
            EXPECT_TRUE(!p);
            _p.set_value(true);
        });
    END_TEST
}

TEST_F(BlobManagerFixture, GetUnknownBlob) {
    START_TEST
    m_mock_homeobj->blob_manager()->get(
        _shard.id, UINT64_MAX, 0, 0,
        [this](std::variant< Blob, BlobError > const& v, std::optional< peer_id > p) mutable {
            ASSERT_TRUE(std::holds_alternative< BlobError >(v));
            EXPECT_EQ(std::get< BlobError >(v), BlobError::UNKNOWN_BLOB);
            EXPECT_TRUE(!p);
            _p.set_value(true);
        });
    END_TEST
}

TEST_F(BlobManagerFixture, GetBlob) {
    START_TEST
    m_mock_homeobj->blob_manager()->get(
        _shard.id, _blob_id, 0, 0,
        [this](std::variant< Blob, BlobError > const& v, std::optional< peer_id > p) mutable {
            ASSERT_TRUE(std::holds_alternative< Blob >(v));
            auto const& blob = std::get< Blob >(v);
            EXPECT_STREQ(blob.user_key.c_str(), "test_blob");
            EXPECT_EQ(blob.object_off, 4 * Mi);
            EXPECT_TRUE(!p);
            _p.set_value(true);
        });
    END_TEST
}

TEST_F(BlobManagerFixture, DeleteBlobUnknownShard) {
    START_TEST
    m_mock_homeobj->blob_manager()->del(_shard.id + 1, _blob_id,
                                        [this](BlobError const& e, std::optional< peer_id > p) mutable {
                                            EXPECT_EQ(BlobError::UNKNOWN_SHARD, e);
                                            EXPECT_TRUE(!p);
                                            _p.set_value(true);
                                        });
    END_TEST
}

TEST_F(BlobManagerFixture, DeleteBlobUnknownBlob) {
    START_TEST
    m_mock_homeobj->blob_manager()->del(_shard.id, _blob_id + 1,
                                        [this](BlobError const& e, std::optional< peer_id > p) mutable {
                                            EXPECT_EQ(BlobError::UNKNOWN_BLOB, e);
                                            EXPECT_TRUE(!p);
                                            _p.set_value(true);
                                        });
    END_TEST
}

TEST_F(BlobManagerFixture, DeleteBlob) {
    START_TEST
    m_mock_homeobj->blob_manager()->del(_shard.id, _blob_id,
                                        [this](BlobError const& e, std::optional< peer_id > p) mutable {
                                            EXPECT_EQ(BlobError::OK, e);
                                            EXPECT_TRUE(!p);
                                            _p.set_value(true);
                                        });
    END_TEST
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    return RUN_ALL_TESTS();
}
