#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <folly/ScopeGuard.h>
#include "homeobj_fixture.hpp"

class TombstoneRefreshTest : public HomeObjectFixture {
public:
    using Clock = GCManager::RefreshClock;

    void SetUp() override {
        HomeObjectFixture::SetUp();
        auto mgr = manager();
        mgr->stop_gc_scan_timer();
        if (!mgr->m_tombstone_refresh_executor) { mgr->start_tombstone_refresh(); }
        ASSERT_NO_FATAL_FAILURE(create_pg(1));
        ASSERT_NO_FATAL_FAILURE(create_pg(2));
    }

    void TearDown() override {
#ifdef _PRERELEASE
        for (auto& gate : gates_) { gate->release(); }
        EXPECT_TRUE(wait_for_idle());
        for (auto& name : flips_) { remove_flip(name); }
#endif
        HomeObjectFixture::TearDown();
    }

    std::shared_ptr< GCManager > manager() { return _obj_inst->gc_manager(); }

    std::chrono::seconds window() {
        return std::chrono::seconds(HS_BACKEND_DYNAMIC_CONFIG(gc_tombstone_refresh_interval_sec));
    }

    std::shared_ptr< GCManager::PgTombstoneRefresh > state(pg_id_t pg_id) {
        auto mgr = manager();
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        return mgr->m_tombstone_refresh.at(pg_id);
    }

    bool wait_for_idle() {
        auto mgr = manager();
        std::unique_lock lock(mgr->m_tombstone_refresh_mtx);
        return mgr->m_tombstone_refresh_cv.wait_for(lock, 30s, [&]() {
            return !mgr->m_tombstone_refresh_work.has_value();
        });
    }

    void populate(pg_id_t pg_id) {
        auto index = _obj_inst->get_hs_pg(pg_id)->index_table_;
        const homestore::MultiBlkId synthetic_pba{0, 1, 1};
        for (uint64_t i = 0; i < 4; ++i) {
            BlobRouteKey key{BlobRoute{make_new_shard_id(pg_id, 1), i}};
            BlobRouteValue value{i % 2 == 0 ? HSHomeObject::tombstone_pbas : synthetic_pba};
            BlobRouteValue old_value;
            homestore::BtreeSinglePutRequest req{&key, &value, homestore::btree_put_type::INSERT, &old_value};
            ASSERT_EQ(index->put(req), homestore::btree_status_t::success);
        }
    }

    void request_due(pg_id_t pg_id) {
        manager()->mark_pg_tombstone_refresh(pg_id, Clock::now() - window() - 1s);
    }

#ifdef _PRERELEASE
    struct Gate {
        std::mutex mtx;
        std::condition_variable cv;
        bool entered{false};
        bool released{false};

        void pause() {
            std::unique_lock lock(mtx);
            entered = true;
            cv.notify_all();
            cv.wait(lock, [&]() { return released; });
        }

        bool wait() {
            std::unique_lock lock(mtx);
            return cv.wait_for(lock, 30s, [&]() { return entered; });
        }

        void release() {
            std::lock_guard lock(mtx);
            released = true;
            cv.notify_all();
        }
    };

    std::shared_ptr< Gate > pause_at(const std::string& name) {
        auto gate = std::make_shared< Gate >();
        set_callback_flip(name, [gate]() { gate->pause(); });
        flips_.push_back(name);
        gates_.push_back(gate);
        return gate;
    }

    std::vector< std::string > flips_;
    std::vector< std::shared_ptr< Gate > > gates_;
#endif
};

TEST_F(TombstoneRefreshTest, CoalescesWithoutPostponingEligibility) {
    auto mgr = manager();
    const auto first = Clock::now();
    mgr->mark_pg_tombstone_refresh(1, first);
    auto token = state(1);
    for (uint32_t i = 1; i < 10; ++i) { mgr->mark_pg_tombstone_refresh(1, first + std::chrono::milliseconds(i)); }
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_EQ(mgr->m_tombstone_refresh.size(), 1);
        EXPECT_EQ(token->pending_since, first);
        EXPECT_EQ(token->eligible_at, first + window());
    }
    mgr->dispatch_tombstone_refresh(first + window() - 1ms);
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_FALSE(mgr->m_tombstone_refresh_work);
    }
    mgr->dispatch_tombstone_refresh(first + window());
    ASSERT_TRUE(wait_for_idle());
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_FALSE(token->pending_since);
        EXPECT_TRUE(token->last_success);
    }
}

TEST_F(TombstoneRefreshTest, PublishesOnlyTombstoneHintAndPersistsWithCP) {
    ASSERT_NO_FATAL_FAILURE(populate(1));
    auto pg = const_cast< HSHomeObject::HS_PG* >(_obj_inst->get_hs_pg(1));
    pg->durable_entities_update([](auto& de) {
        de.active_blob_count = 99;
        de.tombstone_blob_count = 777;
        de.total_occupied_blk_count = 123;
        de.total_reclaimed_blk_count = 456;
    });
    pg->last_committed_blob_id = 42;
    const auto blob_sequence = pg->durable_entities().blob_sequence_num.load();
    request_due(1);
    manager()->dispatch_tombstone_refresh();
    ASSERT_TRUE(wait_for_idle());
    EXPECT_EQ(pg->durable_entities().tombstone_blob_count, 2);
    EXPECT_EQ(pg->durable_entities().active_blob_count, 99);
    EXPECT_EQ(pg->durable_entities().total_occupied_blk_count, 123);
    EXPECT_EQ(pg->durable_entities().total_reclaimed_blk_count, 456);
    EXPECT_EQ(pg->last_committed_blob_id, 42);
    EXPECT_EQ(pg->durable_entities().blob_sequence_num, blob_sequence);
    trigger_cp(true);
    EXPECT_EQ(pg->pg_sb_->tombstone_blob_count, 2);
}

TEST_F(TombstoneRefreshTest, PendingDestroyDiscardsDemand) {
    request_due(1);
    auto token = state(1);
    _obj_inst->mark_pg_destroyed(1);
    manager()->stop_pg_tombstone_refresh(1);
    manager()->mark_pg_tombstone_refresh(1);
    manager()->dispatch_tombstone_refresh();
    auto mgr = manager();
    std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
    EXPECT_FALSE(token->valid);
    EXPECT_EQ(mgr->m_tombstone_refresh.count(1), 0);
    EXPECT_FALSE(mgr->m_tombstone_refresh_work);
}

TEST_F(TombstoneRefreshTest, ManualGCDrainDoesNotCancelRefresh) {
    request_due(1);
    auto token = state(1);
    manager()->drain_pg_pending_gc_task(1);
    auto mgr = manager();
    std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
    EXPECT_TRUE(token->valid);
    EXPECT_TRUE(token->pending_since);
}

TEST_F(TombstoneRefreshTest, SuccessfulGCMarksButDoesNotSynchronouslyRefresh) {
    auto mgr = manager();
    if (!HS_BACKEND_DYNAMIC_CONFIG(enable_gc)) { GTEST_SKIP() << "This test requires GC actors"; }
    auto pg = const_cast< HSHomeObject::HS_PG* >(_obj_inst->get_hs_pg(1));
    pg->durable_entities_update([](auto& de) { de.tombstone_blob_count = 777; });
    auto chunks = _obj_inst->chunk_selector()->get_pg_chunks(1);
    ASSERT_TRUE(chunks);
    ASSERT_FALSE(chunks->empty());
    ASSERT_TRUE(mgr->submit_gc_task(task_priority::emergent, chunks->front()).get());
    mgr->drain_pg_pending_gc_task(1);
    auto token = state(1);
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_TRUE(token->pending_since);
        EXPECT_FALSE(mgr->m_tombstone_refresh_work);
    }
    EXPECT_EQ(pg->durable_entities().tombstone_blob_count, 777);
}

TEST_F(TombstoneRefreshTest, ReplayRefreshDoesNotDependOnOldPendingState) {
    ASSERT_NO_FATAL_FAILURE(populate(1));
    auto pg = const_cast< HSHomeObject::HS_PG* >(_obj_inst->get_hs_pg(1));
    pg->durable_entities_update([](auto& de) { de.tombstone_blob_count = 777; });
    manager()->mark_pg_tombstone_refresh(1);
    restart();
    auto recovered = _obj_inst->get_hs_pg(1);
    ASSERT_NE(recovered, nullptr);
    EXPECT_EQ(recovered->durable_entities().tombstone_blob_count, 2);
}

TEST_F(TombstoneRefreshTest, StaleIndexIdentityIsSkippedWithoutRetry) {
    ASSERT_NO_FATAL_FAILURE(populate(1));
    auto pg = const_cast< HSHomeObject::HS_PG* >(_obj_inst->get_hs_pg(1));
    pg->durable_entities_update([](auto& de) { de.tombstone_blob_count = 777; });
    request_due(1);
    auto token = state(1);
    auto mgr = manager();
    {
        // The state stays valid but records another index, as if the PG ID had been reused by a new PG.
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        token->index_uuid = boost::uuids::random_generator()();
    }
    mgr->dispatch_tombstone_refresh();
    ASSERT_TRUE(wait_for_idle());
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_FALSE(token->active);
        EXPECT_FALSE(token->pending_since);
        EXPECT_FALSE(token->last_success);
    }
    // A scan of the live index would have stored 2.
    EXPECT_EQ(pg->durable_entities().tombstone_blob_count, 777);
}

#ifdef _PRERELEASE
TEST_F(TombstoneRefreshTest, ScanExceptionReleasesAccessAndWorkerCanRetry) {
    set_basic_flip("tombstone_refresh_scan_exception");
    flips_.push_back("tombstone_refresh_scan_exception");
    request_due(1);
    auto token = state(1);
    auto mgr = manager();
    mgr->dispatch_tombstone_refresh();
    ASSERT_TRUE(wait_for_idle());
    Clock::time_point retry_at;
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_FALSE(token->active);
        EXPECT_TRUE(token->pending_since);
        EXPECT_FALSE(token->last_success);
        retry_at = token->eligible_at;
    }
    mgr->dispatch_tombstone_refresh(retry_at);
    ASSERT_TRUE(wait_for_idle());
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_TRUE(token->last_success);
    }
}

TEST_F(TombstoneRefreshTest, SelectsEarliestPGAndAdvancesWithOneSlot) {
    auto gate = pause_at("pause_tombstone_refresh_before_admission");
    auto mgr = manager();
    mgr->mark_pg_tombstone_refresh(1, Clock::now() - window() - 1s);
    mgr->mark_pg_tombstone_refresh(2, Clock::now() - window() - 2s);
    mgr->dispatch_tombstone_refresh();
    ASSERT_TRUE(gate->wait());
    for (uint32_t i = 0; i < 10; ++i) { mgr->dispatch_tombstone_refresh(); }
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        ASSERT_TRUE(mgr->m_tombstone_refresh_work);
        EXPECT_EQ(mgr->m_tombstone_refresh_work->pg_id, 2);
        EXPECT_TRUE(mgr->m_tombstone_refresh.at(1)->pending_since);
    }
    gate->release();
    ASSERT_TRUE(wait_for_idle());
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_TRUE(mgr->m_tombstone_refresh.at(1)->last_success);
        EXPECT_TRUE(mgr->m_tombstone_refresh.at(2)->last_success);
    }
}

TEST_F(TombstoneRefreshTest, CompletionDuringScanRetainsNextBatch) {
    auto gate = pause_at("pause_tombstone_refresh_scan");
    request_due(1);
    auto token = state(1);
    auto mgr = manager();
    mgr->dispatch_tombstone_refresh();
    ASSERT_TRUE(gate->wait());
    const auto next = Clock::now();
    mgr->mark_pg_tombstone_refresh(1, next);
    mgr->mark_pg_tombstone_refresh(1, next + 1ms);
    gate->release();
    ASSERT_TRUE(wait_for_idle());
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_EQ(token->pending_since, next);
        EXPECT_EQ(token->eligible_at, next + window());
    }
    mgr->dispatch_tombstone_refresh(next + window());
    ASSERT_TRUE(wait_for_idle());
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_FALSE(token->pending_since);
    }
}

TEST_F(TombstoneRefreshTest, FailedScanRetainsHintAndAllowsOtherPG) {
    ASSERT_NO_FATAL_FAILURE(populate(1));
    auto pg = const_cast< HSHomeObject::HS_PG* >(_obj_inst->get_hs_pg(1));
    pg->durable_entities_update([](auto& de) { de.tombstone_blob_count = 777; });
    set_basic_flip("tombstone_refresh_scan_failure");
    flips_.push_back("tombstone_refresh_scan_failure");
    request_due(1);
    auto token = state(1);
    auto mgr = manager();
    mgr->dispatch_tombstone_refresh();
    ASSERT_TRUE(wait_for_idle());
    Clock::time_point retry_at;
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_TRUE(token->pending_since);
        EXPECT_FALSE(token->last_success);
        EXPECT_GT(token->eligible_at, Clock::now());
        retry_at = token->eligible_at;
    }
    EXPECT_EQ(pg->durable_entities().tombstone_blob_count, 777);
    request_due(2);
    mgr->dispatch_tombstone_refresh();
    ASSERT_TRUE(wait_for_idle());
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_TRUE(mgr->m_tombstone_refresh.at(2)->last_success);
        EXPECT_TRUE(token->pending_since);
    }
    mgr->dispatch_tombstone_refresh(retry_at);
    ASSERT_TRUE(wait_for_idle());
    EXPECT_EQ(pg->durable_entities().tombstone_blob_count, 2);
}

TEST_F(TombstoneRefreshTest, FailedGCTaskMarksRefresh) {
    auto mgr = manager();
    if (!HS_BACKEND_DYNAMIC_CONFIG(enable_gc)) { GTEST_SKIP() << "This test requires GC actors"; }
    set_basic_flip("fail_gc_copy_for_tombstone_refresh_test");
    flips_.push_back("fail_gc_copy_for_tombstone_refresh_test");
    auto chunks = _obj_inst->chunk_selector()->get_pg_chunks(1);
    ASSERT_TRUE(chunks);
    ASSERT_FALSE(chunks->empty());
    EXPECT_FALSE(mgr->submit_gc_task(task_priority::emergent, chunks->front()).get());
    mgr->drain_pg_pending_gc_task(1);
    auto token = state(1);
    std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
    EXPECT_TRUE(token->pending_since);
}

TEST_F(TombstoneRefreshTest, SelectedWorkCannotPublishToRecreatedPG) {
    auto gate = pause_at("pause_tombstone_refresh_before_admission");
    request_due(1);
    auto old_token = state(1);
    manager()->dispatch_tombstone_refresh();
    ASSERT_TRUE(gate->wait());
    ASSERT_TRUE(_obj_inst->pg_destroy(1));
    g_helper->sync();
    ASSERT_NO_FATAL_FAILURE(create_pg(1));
    auto pg = const_cast< HSHomeObject::HS_PG* >(_obj_inst->get_hs_pg(1));
    pg->durable_entities_update([](auto& de) { de.tombstone_blob_count = 909; });
    manager()->mark_pg_tombstone_refresh(1);
    auto new_token = state(1);
    EXPECT_NE(new_token, old_token);
    gate->release();
    ASSERT_TRUE(wait_for_idle());
    auto mgr = manager();
    std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
    EXPECT_FALSE(old_token->valid);
    EXPECT_TRUE(new_token->pending_since);
    EXPECT_EQ(pg->durable_entities().tombstone_blob_count, 909);
}

TEST_F(TombstoneRefreshTest, ActiveScanDoesNotHoldGlobalLockAndDestroyWaits) {
    auto gate = pause_at("pause_tombstone_refresh_scan");
    request_due(1);
    auto token = state(1);
    auto mgr = manager();
    mgr->dispatch_tombstone_refresh();
    ASSERT_TRUE(gate->wait());
    std::atomic< bool > destroyed{false};
    std::thread destroyer([&]() { destroyed = _obj_inst->pg_destroy(1); });
    SCOPE_EXIT {
        gate->release();
        if (destroyer.joinable()) { destroyer.join(); }
    };
    {
        std::unique_lock lock(mgr->m_tombstone_refresh_mtx);
        ASSERT_TRUE(mgr->m_tombstone_refresh_cv.wait_for(lock, 30s, [&]() { return !token->valid; }));
        EXPECT_TRUE(token->active);
        EXPECT_FALSE(destroyed.load());
    }
    EXPECT_NE(_obj_inst->get_hs_pg(2), nullptr);
    gate->release();
    destroyer.join();
    EXPECT_TRUE(destroyed.load());
    EXPECT_EQ(_obj_inst->get_hs_pg(1), nullptr);
    ASSERT_TRUE(wait_for_idle());
}

TEST_F(TombstoneRefreshTest, ShutdownDrainsActiveWorkAndRejectsNewDemand) {
    auto gate = pause_at("pause_tombstone_refresh_scan");
    request_due(1);
    auto mgr = manager();
    mgr->dispatch_tombstone_refresh();
    ASSERT_TRUE(gate->wait());
    std::atomic< bool > stopped{false};
    std::thread stopper([&]() {
        mgr->stop();
        stopped = true;
    });
    SCOPE_EXIT {
        gate->release();
        if (stopper.joinable()) { stopper.join(); }
    };
    {
        std::unique_lock lock(mgr->m_tombstone_refresh_mtx);
        ASSERT_TRUE(mgr->m_tombstone_refresh_cv.wait_for(lock, 30s, [&]() {
            return mgr->m_tombstone_refresh_stopped;
        }));
        EXPECT_FALSE(stopped.load());
    }
    gate->release();
    stopper.join();
    EXPECT_TRUE(stopped.load());
    mgr->mark_pg_tombstone_refresh(1);
    mgr->dispatch_tombstone_refresh();
    mgr->stop();
    std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
    EXPECT_TRUE(mgr->m_tombstone_refresh.empty());
    EXPECT_FALSE(mgr->m_tombstone_refresh_work);
}
#endif

class TombstoneRefreshDisabledTest : public HomeObjectFixture {
public:
    void SetUp() override {
        original_enable_gc_ = HS_BACKEND_DYNAMIC_CONFIG(enable_gc);
        HS_BACKEND_SETTINGS_FACTORY().modifiable_settings([](auto& settings) { settings.enable_gc = false; });
        HomeObjectFixture::SetUp();
        ASSERT_NO_FATAL_FAILURE(create_pg(1));
    }

    void TearDown() override {
        HomeObjectFixture::TearDown();
        HS_BACKEND_SETTINGS_FACTORY().modifiable_settings(
            [this](auto& settings) { settings.enable_gc = original_enable_gc_; });
    }

    bool original_enable_gc_{true};
};

TEST_F(TombstoneRefreshDisabledTest, CanRegisterBeforeStartupAndStopWithoutWorker) {
    auto mgr = _obj_inst->gc_manager();
    EXPECT_FALSE(mgr->m_tombstone_refresh_executor);
    mgr->mark_pg_tombstone_refresh(1);
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_TRUE(mgr->m_tombstone_refresh.at(1)->pending_since);
    }
    mgr->dispatch_tombstone_refresh(GCManager::RefreshClock::now() + 1h);
    {
        std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
        EXPECT_FALSE(mgr->m_tombstone_refresh_work);
    }
    mgr->stop();
    mgr->mark_pg_tombstone_refresh(1);
    mgr->stop();
    std::lock_guard lock(mgr->m_tombstone_refresh_mtx);
    EXPECT_TRUE(mgr->m_tombstone_refresh_stopped);
    EXPECT_TRUE(mgr->m_tombstone_refresh.empty());
    EXPECT_FALSE(mgr->m_tombstone_refresh_executor);
}
