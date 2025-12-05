#include <boost/uuid/random_generator.hpp>
#include <folly/init/Init.h>
#include <ostream>

#include "fixture_app.hpp"

SISL_OPTION_GROUP(
    test_home_object,
    (num_iters, "", "num_iters", "number of iterations per loop", ::cxxopts::value< uint64_t >()->default_value("1000"),
     "number"),
    (num_pgs, "", "num_pgs", "number of pgs", ::cxxopts::value< uint64_t >()->default_value("10"), "number"),
    (num_shards, "", "num_shards", "number of shards", ::cxxopts::value< uint64_t >()->default_value("20"), "number"),
    (num_blobs, "", "num_blobs", "number of blobs", ::cxxopts::value< uint64_t >()->default_value("50"), "number"));

SISL_LOGGING_INIT(HOMEOBJECT_LOG_MODS)

#define test_options logging, homeobject, config, test_home_object

SISL_OPTIONS_ENABLE(test_options)

FixtureApp::FixtureApp(bool is_hybrid) : is_hybrid_(is_hybrid) {
    clean();
    LOGWARN("creating HDD device {} file with size {} ", path_hdd_, 10 * Gi);
    std::ofstream ofs{path_hdd_, std::ios::binary | std::ios::out | std::ios::trunc};
    std::filesystem::resize_file(path_hdd_, 10 * Gi);

    if (is_hybrid_) {
        LOGWARN("creating SSD device {} file with size {} ", path_ssd_, 10 * Gi);
        std::ofstream ofs{path_ssd_, std::ios::binary | std::ios::out | std::ios::trunc};
        std::filesystem::resize_file(path_ssd_, 10 * Gi);
    }
}

homeobject::peer_id_t FixtureApp::discover_svcid(std::optional< homeobject::peer_id_t > const& p) const {
    return p.has_value() ? p.value() : boost::uuids::random_generator()();
}

void TestFixture::SetUp() {
    app = std::make_shared< FixtureApp >();
    homeobj_ = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    _peer1 = homeobj_->our_uuid();
    _peer2 = boost::uuids::random_generator()();

    auto info = homeobject::PGInfo(_pg_id);
    info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
    // info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});

    auto tid = homeobject::generateRandomTraceId();
    LOGDEBUG("Setup Pg, trace_id={}", tid);
    EXPECT_TRUE(homeobj_->pg_manager()->create_pg(std::move(info), tid).get());

    LOGDEBUG("Setup Shards, trace_id={}", tid);
    auto s_e = homeobj_->shard_manager()->create_shard(_pg_id, Mi, "shard meta", tid).get();
    ASSERT_TRUE(!!s_e);
    s_e.then([this](auto&& i) { _shard_1 = std::move(i); });

    s_e = homeobj_->shard_manager()->create_shard(_pg_id, Mi, "shard meta", tid).get();
    ASSERT_TRUE(!!s_e);
    s_e.then([this](auto&& i) { _shard_2 = std::move(i); });

    LOGDEBUG("Get on empty Shard={}, trace_id={}", _shard_1.id, tid);
    auto g_e = homeobj_->blob_manager()->get(_shard_1.id, 0, 0, 0, tid).get();
    ASSERT_FALSE(g_e);
    EXPECT_EQ(homeobject::BlobErrorCode::UNKNOWN_BLOB, g_e.error().getCode());

    LOGDEBUG("Insert Blob to={}, trace_id={}", _shard_1.id, tid);
    auto o_e = homeobj_->blob_manager()
                   ->put(_shard_1.id, homeobject::Blob{sisl::io_blob_safe(4 * Ki, 512u), "test_blob", 4 * Mi}, tid)
                   .get();
    EXPECT_TRUE(!!o_e);
    o_e.then([this](auto&& b) mutable { _blob_id = std::move(b); });

    g_e = homeobj_->blob_manager()->get(_shard_1.id, _blob_id, 0, 0, tid).get();
    EXPECT_TRUE(!!g_e);
    g_e.then([](auto&& blob) {
        EXPECT_STREQ(blob.user_key.c_str(), "test_blob");
        EXPECT_EQ(blob.object_off, 4 * Mi);
    });

    // cover the memory version of get_stats
    // homestore version has a dedicated test for this.
    homeobject::PGStats stats;
    std::vector< homeobject::pg_id_t > pg_ids;
    homeobj_->pg_manager()->get_stats(_pg_id, stats);
    homeobj_->get_stats();
    homeobj_->pg_manager()->get_pg_ids(pg_ids);
}

void TestFixture::TearDown() {
    if (homeobj_ && homeobj_.get()) {
        homeobj_->shutdown();
        homeobj_.reset();
    }
    std::dynamic_pointer_cast< FixtureApp >(app)->clean();
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, test_options);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);
    return RUN_ALL_TESTS();
}
