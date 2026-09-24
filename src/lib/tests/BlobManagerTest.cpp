#include <mutex>
#include <thread>
#include <vector>

#include <sisl/async/coro.hpp>
#include <sisl/async/when_all.hpp>

#include <homeobject/blob_manager.hpp>
#include <homeobject/common.hpp>
#include "lib/tests/fixture_app.hpp"

using homeobject::Blob;
using homeobject::BlobError;
using homeobject::BlobErrorCode;

namespace {

// Coroutine parameters (not lambda captures) are copied into the frame. A temporary IIFE
// coroutine lambda is destroyed at the end of the full-expression, so captures would dangle.

sisl::async::task< std::monostate > get_known_blob(std::shared_ptr< homeobject::HomeObject > homeobj,
                                                   homeobject::shard_id_t shard_id, blob_id_t blob_id) {
    auto e = co_await homeobj->blob_manager()->get(shard_id, blob_id);
    EXPECT_TRUE(!!e);
    if (e) {
        EXPECT_STREQ(e->user_key.c_str(), "test_blob");
        EXPECT_EQ(e->object_off, 4 * Mi);
    }
    co_return std::monostate{};
}

sisl::async::task< std::monostate > get_ignore(std::shared_ptr< homeobject::HomeObject > homeobj,
                                               homeobject::shard_id_t shard_id, blob_id_t blob_id) {
    (void)co_await homeobj->blob_manager()->get(shard_id, blob_id);
    co_return std::monostate{};
}

sisl::async::task< std::monostate > put_unknown_shard(std::shared_ptr< homeobject::HomeObject > homeobj,
                                                      homeobject::shard_id_t shard_id, homeobject::trace_id_t tid) {
    auto e =
        co_await homeobj->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}, tid);
    EXPECT_FALSE(!!e);
    EXPECT_EQ(BlobErrorCode::UNKNOWN_SHARD, e.error().code);
    co_return std::monostate{};
}

sisl::async::task< std::monostate > put_ok(std::shared_ptr< homeobject::HomeObject > homeobj,
                                           homeobject::shard_id_t shard_id, uint32_t size, char const* user_key,
                                           homeobject::trace_id_t tid) {
    auto e =
        co_await homeobj->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(size, 512u), user_key, 4 * Mi}, tid);
    EXPECT_TRUE(!!e);
    if (e) { LOGINFO("Successfully put blob, shard {}, blobID {}", shard_id, *e); }
    co_return std::monostate{};
}

sisl::async::task< std::monostate > del_unknown_shard(std::shared_ptr< homeobject::HomeObject > homeobj,
                                                      homeobject::shard_id_t shard_id, blob_id_t blob_id,
                                                      homeobject::trace_id_t tid) {
    auto e = co_await homeobj->blob_manager()->del(shard_id, blob_id, tid);
    EXPECT_FALSE(!!e);
    EXPECT_EQ(BlobErrorCode::UNKNOWN_SHARD, e.error().getCode());
    co_return std::monostate{};
}

sisl::async::task< std::monostate > del_racing(std::shared_ptr< homeobject::HomeObject > homeobj,
                                               homeobject::shard_id_t shard_id, blob_id_t blob_id,
                                               homeobject::trace_id_t tid) {
    auto e = co_await homeobj->blob_manager()->del(shard_id, blob_id, tid);
    // Racing with other threads / putBlob: success or UNKNOWN_BLOB.
    LOGINFO("Deleted blob, shard {}, blobID {}, success {}", shard_id, blob_id, !!e);
    if (!e) { EXPECT_EQ(BlobErrorCode::UNKNOWN_BLOB, e.error().getCode()); }
    co_return std::monostate{};
}

} // namespace

TEST_F(TestFixture, BasicBlobTests) {
    auto const batch_sz = 4;
    std::mutex call_lock;
    auto calls = std::vector< sisl::async::task< std::monostate > >();

    auto t_v = std::vector< std::thread >();
    for (auto k = 0; batch_sz > k; ++k) {
        t_v.push_back(std::thread([this, &call_lock, &calls, batch_sz]() mutable {
            auto our_calls = std::vector< sisl::async::task< std::monostate > >();
            for (auto i = _blob_id + _shard_2.id + 1;
                 (_blob_id + _shard_1.id + 1) + (SISL_OPTIONS["num_iters"].as< uint64_t >() / batch_sz) > i; ++i) {
                auto tid = homeobject::generateRandomTraceId();
                our_calls.push_back(get_known_blob(homeobj_, _shard_1.id, _blob_id));
                our_calls.push_back(get_ignore(homeobj_, i, _blob_id));
                our_calls.push_back(get_ignore(homeobj_, _shard_1.id, (i - _shard_2.id)));
                our_calls.push_back(get_ignore(homeobj_, _shard_2.id, (i - _shard_2.id)));
                our_calls.push_back(put_unknown_shard(homeobj_, i, tid));
                LOGINFO("Calling to put blob, shard {}", _shard_1.id);
                our_calls.push_back(put_ok(homeobj_, _shard_1.id, 4 * Ki, "test_blob", tid));
                our_calls.push_back(put_ok(homeobj_, _shard_2.id, 8 * Ki, "test_blob_2", tid));
                our_calls.push_back(del_unknown_shard(homeobj_, i, _blob_id, tid));
                LOGINFO("Calling to Deleting blob, shard {}, blobID {}", _shard_1.id, (i - _shard_2.id));
                our_calls.push_back(del_racing(homeobj_, _shard_1.id, (i - _shard_2.id), tid));
            }

            auto lg = std::scoped_lock(call_lock);
            // exec::task is move-constructible but not assignable, so avoid vector::insert.
            calls.reserve(calls.size() + our_calls.size());
            for (auto& t : our_calls) {
                calls.push_back(std::move(t));
            }
        }));
    }
    for (auto& t : t_v)
        t.join();
    sisl::async::sync_get(sisl::async::when_all(std::move(calls)));
    EXPECT_TRUE(sisl::async::sync_get(homeobj_->shard_manager()->seal_shard(_shard_1.id)));
    auto tid = homeobject::generateRandomTraceId();
    auto p_e = sisl::async::sync_get(
        homeobj_->blob_manager()->put(_shard_1.id, Blob{sisl::io_blob_safe(4 * Ki, 512u), "test_blob", 4 * Mi}, tid));
    ASSERT_FALSE(!!p_e);
    EXPECT_EQ(BlobErrorCode::SEALED_SHARD, p_e.error().getCode());

    // BLOB exists
    EXPECT_TRUE(sisl::async::sync_get(homeobj_->blob_manager()->get(_shard_1.id, _blob_id)));

    // BLOB is deleted
    EXPECT_TRUE(sisl::async::sync_get(homeobj_->blob_manager()->del(_shard_1.id, _blob_id, tid)));

    // BLOB is now unknown
    auto g_e = sisl::async::sync_get(homeobj_->blob_manager()->get(_shard_1.id, _blob_id));
    ASSERT_FALSE(!!g_e);
    EXPECT_EQ(BlobErrorCode::UNKNOWN_BLOB, g_e.error().getCode());

    // Delete is Idempotent
    EXPECT_TRUE(sisl::async::sync_get(homeobj_->blob_manager()->del(_shard_1.id, _blob_id, tid)));
}
