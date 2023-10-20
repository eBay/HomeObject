#include <mutex>

#include <folly/init/Init.h>
#include <folly/executors/GlobalExecutor.h>

#include <homeobject/blob_manager.hpp>
#include "lib/tests/fixture_app.hpp"

TEST_F(TestFixture, BasicTests) {
    auto const batch_sz = 4;
    std::mutex call_lock;
    auto calls = std::list< folly::SemiFuture< folly::Unit > >();

    auto t_v = std::vector< std::thread >();
    for (auto k = 0; batch_sz > k; ++k) {
        t_v.push_back(std::thread([this, &call_lock, &calls, batch_sz]() mutable {
            auto our_calls = std::list< folly::SemiFuture< folly::Unit > >();
            for (auto i = _blob_id + _shard_2.id + 1;
                 (_blob_id + _shard_1.id + 1) + (SISL_OPTIONS["num_iters"].as< uint64_t >() / batch_sz) > i; ++i) {
                our_calls.push_back(homeobj_->blob_manager()->get(_shard_1.id, _blob_id).deferValue([](auto const& e) {
                    EXPECT_TRUE(!!e);
                    e.then([](auto const& blob) {
                        EXPECT_STREQ(blob.user_key.c_str(), "test_blob");
                        EXPECT_EQ(blob.object_off, 4 * Mi);
                        EXPECT_STREQ((char*)blob.body.bytes, "HELLO, WORLD!");
                    });
                }));
                our_calls.push_back(homeobj_->blob_manager()->get(i, _blob_id).deferValue([](auto const& e) {}));
                our_calls.push_back(
                    homeobj_->blob_manager()->get(_shard_1.id, (i - _shard_2.id)).deferValue([](auto const&) {}));
                our_calls.push_back(
                    homeobj_->blob_manager()->get(_shard_2.id, (i - _shard_2.id)).deferValue([](auto const&) {}));
                our_calls.push_back(
                    homeobj_->blob_manager()
                        ->put(i, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul})
                        .deferValue([](auto const& e) { EXPECT_EQ(BlobError::UNKNOWN_SHARD, e.error()); }));
                our_calls.push_back(homeobj_->blob_manager()
                                        ->put(_shard_1.id, Blob{sisl::io_blob_safe(4 * Ki, 512u), "test_blob", 4 * Mi})
                                        .deferValue([](auto const& e) { EXPECT_TRUE(!!e); }));
                our_calls.push_back(
                    homeobj_->blob_manager()
                        ->put(_shard_2.id, Blob{sisl::io_blob_safe(8 * Ki, 512u), "test_blob_2", 4 * Mi})
                        .deferValue([](auto const& e) { EXPECT_TRUE(!!e); }));
                our_calls.push_back(homeobj_->blob_manager()->del(i, _blob_id).deferValue([](auto const& e) {}));
                our_calls.push_back(
                    homeobj_->blob_manager()->del(_shard_1.id, (i - _shard_2.id)).deferValue([](auto const&) {
                        // TODO enable this with del_blob impl
                        // EXPECT_EQ(BlobError::UNKNOWN_BLOB, e.error());
                    }));
            }

            auto lg = std::scoped_lock(call_lock);
            calls.splice(calls.end(), std::move(our_calls));
        }));
    }
    for (auto& t : t_v)
        t.join();
    folly::collectAll(calls).via(folly::getGlobalCPUExecutor()).get();
    EXPECT_TRUE(homeobj_->shard_manager()->seal_shard(_shard_1.id).get());
    auto p_e =
        homeobj_->blob_manager()->put(_shard_1.id, Blob{sisl::io_blob_safe(4 * Ki, 512u), "test_blob", 4 * Mi}).get();
    ASSERT_TRUE(!p_e);
    EXPECT_EQ(BlobError::SEALED_SHARD, p_e.error());

    // TODO EXPECT TRUE when delete implemented
    homeobj_->blob_manager()->del(_shard_1.id, _blob_id).get();
    homeobj_->blob_manager()->get(_shard_1.id, _blob_id).get();
    homeobj_->blob_manager()->del(_shard_1.id, _blob_id).get();
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_home_object);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);
    return RUN_ALL_TESTS();
}
