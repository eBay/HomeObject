#include <homeobject/watchdog_registry.hpp>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>

#include <chrono>
#include <thread>

SISL_LOGGING_DEF(homeobject)
SISL_LOGGING_INIT(homeobject)

using namespace homeobject;
using namespace std::chrono_literals;

namespace {

bool wait_until_failed(WatchdogRegistry const& registry, std::chrono::milliseconds budget) {
    auto const deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (!registry.failures().empty()) { return true; }
        std::this_thread::sleep_for(5ms);
    }
    return !registry.failures().empty();
}

} // namespace

TEST(WatchdogRegistry, EmptyRegistryHasNoFailures) {
    WatchdogRegistry registry;
    EXPECT_TRUE(registry.failures().empty());
}

TEST(WatchdogRegistry, ScopedNotFailedBeforeLimit) {
    WatchdogRegistry registry;
    auto wd = registry.watch("gc.iteration", 5s);
    (void)wd;
    EXPECT_TRUE(registry.failures().empty());
}

TEST(WatchdogRegistry, ScopedFailsAfterLimitAndClearsOnDrop) {
    WatchdogRegistry registry;
    {
        auto wd = registry.watch("gc.iteration", 20ms);
        (void)wd;
        EXPECT_TRUE(wait_until_failed(registry, 500ms));
        auto const failures = registry.failures();
        ASSERT_EQ(failures.size(), 1u);
        EXPECT_EQ(failures[0].name, "gc.iteration");
        EXPECT_NE(failures[0].details.find("last seen"), std::string::npos);
        EXPECT_EQ(failures[0].severity, WatchdogSeverity::critical);
    }
    EXPECT_TRUE(registry.failures().empty());
}

TEST(WatchdogRegistry, LeaseKickExtendsDeadline) {
    WatchdogRegistry registry;
    auto lease = registry.create_lease("heartbeat.loop", 80ms);
    std::this_thread::sleep_for(20ms);
    lease.kick();
    EXPECT_TRUE(registry.failures().empty());
    EXPECT_TRUE(wait_until_failed(registry, 500ms));
    ASSERT_EQ(registry.failures().size(), 1u);
    EXPECT_EQ(registry.failures()[0].name, "heartbeat.loop");
}

TEST(WatchdogRegistry, LeaseFailsAndClearsOnDrop) {
    WatchdogRegistry registry;
    {
        auto lease = registry.create_lease("heartbeat.loop", 20ms);
        (void)lease;
        EXPECT_TRUE(wait_until_failed(registry, 500ms));
        EXPECT_EQ(registry.failures().size(), 1u);
    }
    EXPECT_TRUE(registry.failures().empty());
}

TEST(WatchdogRegistry, BarkFailsImmediatelyAndClearsOnDrop) {
    WatchdogRegistry registry;
    {
        auto bark = registry.bark("raft.system_exit", "exit_code=1");
        (void)bark;
        auto const failures = registry.failures();
        ASSERT_EQ(failures.size(), 1u);
        EXPECT_EQ(failures[0].name, "raft.system_exit");
        EXPECT_EQ(failures[0].details, "exit_code=1");
        EXPECT_EQ(failures[0].severity, WatchdogSeverity::critical);
    }
    EXPECT_TRUE(registry.failures().empty());
}

TEST(WatchdogRegistry, MoveTransfersRegistration) {
    WatchdogRegistry registry;
    WatchdogBark moved;
    {
        auto bark = registry.bark("raft.system_exit", "exit_code=2");
        moved = std::move(bark);
        EXPECT_EQ(registry.failures().size(), 1u);
    }
    EXPECT_EQ(registry.failures().size(), 1u);
    moved = WatchdogBark{};
    EXPECT_TRUE(registry.failures().empty());
}

TEST(WatchdogRegistry, MultipleCriticalFailures) {
    WatchdogRegistry registry;
    auto bark = registry.bark("raft.system_exit", "exit_code=1");
    auto scoped = registry.watch("stuck", 20ms);
    (void)bark;
    (void)scoped;
    auto const deadline = std::chrono::steady_clock::now() + 500ms;
    while (std::chrono::steady_clock::now() < deadline && registry.failures().size() < 2u) {
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_EQ(registry.failures().size(), 2u);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
