#include <mutex>
#include <folly/executors/GlobalExecutor.h>

#include <homeobject/blob_manager.hpp>
#include <homeobject/common.hpp>
#include "lib/tests/fixture_app.hpp"

using homeobject::Blob;
using homeobject::BlobError;
using homeobject::BlobErrorCode;

TEST_F(TestFixture, BasicBlobTests) {
    auto const batch_sz = 4;
    std::mutex call_lock;
    auto calls = std::list< folly::SemiFuture< folly::Unit > >();

    auto t_v = std::vector< std::thread >();
    for (auto k = 0; batch_sz > k; ++k) {
        t_v.push_back(std::thread([this, &call_lock, &calls, batch_sz]() mutable {
            auto our_calls = std::list< folly::SemiFuture< folly::Unit > >();
            for (auto i = _blob_id + _shard_2.id + 1;
                 (_blob_id + _shard_1.id + 1) + (SISL_OPTIONS["num_iters"].as< uint64_t >() / batch_sz) > i; ++i) {
                auto tid = homeobject::generateRandomTraceId();
                our_calls.push_back(homeobj_->blob_manager()->get(_shard_1.id, _blob_id).deferValue([](auto const& e) {
                    EXPECT_TRUE(!!e);
                    e.then([](auto const& blob) {
                        EXPECT_STREQ(blob.user_key.c_str(), "test_blob");
                        EXPECT_EQ(blob.object_off, 4 * Mi);
                    });
                }));
                our_calls.push_back(homeobj_->blob_manager()->get(i, _blob_id).deferValue([](auto const& e) {}));
                our_calls.push_back(
                    homeobj_->blob_manager()->get(_shard_1.id, (i - _shard_2.id)).deferValue([](auto const&) {}));
                our_calls.push_back(
                    homeobj_->blob_manager()->get(_shard_2.id, (i - _shard_2.id)).deferValue([](auto const&) {}));
                our_calls.push_back(
                    homeobj_->blob_manager()
                        ->put(i, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}, tid)
                        .deferValue([](auto const& e) { EXPECT_EQ(BlobErrorCode::UNKNOWN_SHARD, e.error().code); }));
                LOGINFO("Calling to put blob, shard {}", _shard_1.id);
                our_calls.push_back(homeobj_->blob_manager()
                                        ->put(_shard_1.id, Blob{sisl::io_blob_safe(4 * Ki, 512u), "test_blob", 4 * Mi}, tid)
                                        .deferValue([this](auto const& e) {
                                            EXPECT_TRUE(!!e);
                                            e.then([this](auto const& blob_id) {
                                                LOGINFO("Successfully put blob, shard {}, blobID {}", _shard_1.id,
                                                        blob_id);
                                            });
                                        }));
                our_calls.push_back(
                    homeobj_->blob_manager()
                        ->put(_shard_2.id, Blob{sisl::io_blob_safe(8 * Ki, 512u), "test_blob_2", 4 * Mi}, tid)
                        .deferValue([](auto const& e) { EXPECT_TRUE(!!e); }));
                our_calls.push_back(homeobj_->blob_manager()->del(i, _blob_id, tid).deferValue([](auto const& e) {
                    EXPECT_FALSE(!!e);
                    EXPECT_EQ(BlobErrorCode::UNKNOWN_SHARD, e.error().getCode());
                }));
                LOGINFO("Calling to Deleting blob, shard {}, blobID {}", _shard_1.id, (i - _shard_2.id));
                our_calls.push_back(
                    homeobj_->blob_manager()->del(_shard_1.id, (i - _shard_2.id), tid).deferValue([this, i](auto const& e) {
                        // It is a racing test with other threads as well as putBlob
                        // the result should be either success or failed with UNKNOWN_BLOB
                        LOGINFO("Deleted blob, shard {}, blobID {}, success {}", _shard_1.id, (i - _shard_2.id), !!e);
                        if (!!e) {
                            return;
                        } else {
                            EXPECT_FALSE(!!e);
                            EXPECT_EQ(BlobErrorCode::UNKNOWN_BLOB, e.error().getCode());
                        }
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
    auto tid = homeobject::generateRandomTraceId();
    auto p_e =
        homeobj_->blob_manager()->put(_shard_1.id, Blob{sisl::io_blob_safe(4 * Ki, 512u), "test_blob", 4 * Mi}, tid).get();
    ASSERT_FALSE(!!p_e);
    EXPECT_EQ(BlobErrorCode::SEALED_SHARD, p_e.error().getCode());

    // BLOB exists
    EXPECT_TRUE(homeobj_->blob_manager()->get(_shard_1.id, _blob_id).get());

    // BLOB is deleted
    EXPECT_TRUE(homeobj_->blob_manager()->del(_shard_1.id, _blob_id, tid).get());

    // BLOB is now unknown
    auto g_e = homeobj_->blob_manager()->get(_shard_1.id, _blob_id).get();
    ASSERT_FALSE(!!g_e);
    EXPECT_EQ(BlobErrorCode::UNKNOWN_BLOB, g_e.error().getCode());

    // Delete is Idempotent
    EXPECT_TRUE(homeobj_->blob_manager()->del(_shard_1.id, _blob_id, tid).get());
}
