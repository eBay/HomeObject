#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace homeobject {

enum class WatchdogSeverity {
    critical, // contributes to liveness failures; non-critical reserved for future use
};

struct WatchdogFailure {
    std::string name;
    std::string details;
    WatchdogSeverity severity;
};

class WatchdogRegistry;

// Common RAII base for all watchdog handle types: move-only, destructor deregisters.
class WatchdogHandle {
public:
    WatchdogHandle() = default;
    ~WatchdogHandle();
    WatchdogHandle(WatchdogHandle&&) noexcept;
    WatchdogHandle& operator=(WatchdogHandle&&) noexcept;
    WatchdogHandle(const WatchdogHandle&) = delete;
    WatchdogHandle& operator=(const WatchdogHandle&) = delete;

protected:
    void kick_impl() noexcept;

private:
    friend class WatchdogRegistry;
    void release() noexcept;

    WatchdogRegistry* registry_{nullptr};
    uint64_t id_{0};
};

// For a single operation that must complete within a fixed deadline. Automatically deregisters on destruction.
// Example: registry.watch("gc.iteration", 10min) wraps one GC cycle.
class ScopedWatchdog : public WatchdogHandle {};

// For a recurring loop that must keep making progress. Call kick() in each iteration to extend the deadline.
// Example: a heartbeat loop that calls kick() on every iteration.
class WatchdogLease : public WatchdogHandle {
public:
    void kick() { kick_impl(); }
};

// For signaling that the system is already in a known-bad state. Fails immediately on construction.
// Example: nuraft fatal exit — hold until process restarts.
class WatchdogBark : public WatchdogHandle {};

class WatchdogRegistry {
public:
    ScopedWatchdog watch(std::string name, std::chrono::milliseconds limit,
                         WatchdogSeverity severity = WatchdogSeverity::critical);

    WatchdogLease create_lease(std::string name, std::chrono::milliseconds limit,
                               WatchdogSeverity severity = WatchdogSeverity::critical);

    WatchdogBark bark(std::string name, std::string details = {},
                      WatchdogSeverity severity = WatchdogSeverity::critical);

    // Registered entries that are failed and have severity == critical.
    std::vector< WatchdogFailure > failures() const;

private:
    friend class WatchdogHandle;

    enum class Kind { scoped, lease, bark };

    struct Entry {
        std::string name;
        std::string details;
        WatchdogSeverity severity{WatchdogSeverity::critical};
        Kind kind{Kind::scoped};
        std::chrono::steady_clock::time_point anchor{};
        std::chrono::milliseconds limit{0};
    };

    template < typename H >
    H make_handle(Entry entry);

    uint64_t add(Entry entry);
    void release(uint64_t id);
    void kick(uint64_t id);

    mutable std::mutex mu_;
    uint64_t next_id_{1};
    std::unordered_map< uint64_t, Entry > entries_;
};

} // namespace homeobject
