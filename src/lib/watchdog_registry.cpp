#include <homeobject/watchdog_registry.hpp>
#include <sisl/logging/logging.h>

namespace homeobject {

WatchdogHandle::~WatchdogHandle() { release(); }

WatchdogHandle::WatchdogHandle(WatchdogHandle&& other) noexcept : registry_(other.registry_), id_(other.id_) {
    other.registry_ = nullptr;
    other.id_ = 0;
}

WatchdogHandle& WatchdogHandle::operator=(WatchdogHandle&& other) noexcept {
    if (this != &other) {
        release();
        registry_ = other.registry_;
        id_ = other.id_;
        other.registry_ = nullptr;
        other.id_ = 0;
    }
    return *this;
}

void WatchdogHandle::release() noexcept {
    if (registry_ == nullptr) { return; }
    registry_->release(id_);
    registry_ = nullptr;
    id_ = 0;
}

void WatchdogHandle::kick_impl() noexcept {
    if (registry_ == nullptr) { return; }
    registry_->kick(id_);
}

template < typename H >
H WatchdogRegistry::make_handle(Entry entry) {
    H h;
    h.registry_ = this;
    h.id_ = add(std::move(entry));
    return h;
}

uint64_t WatchdogRegistry::add(Entry entry) {
    std::lock_guard< std::mutex > lock(mu_);
    auto const id = next_id_++;
    entries_.emplace(id, std::move(entry));
    return id;
}

void WatchdogRegistry::release(uint64_t id) {
    std::lock_guard< std::mutex > lock(mu_);
    entries_.erase(id);
}

void WatchdogRegistry::kick(uint64_t id) {
    std::lock_guard< std::mutex > lock(mu_);
    auto it = entries_.find(id);
    if (it == entries_.end()) { return; }
    it->second.anchor = std::chrono::steady_clock::now();
}

ScopedWatchdog WatchdogRegistry::watch(std::string name, std::chrono::milliseconds limit, WatchdogSeverity severity) {
    Entry entry;
    entry.name = std::move(name);
    entry.severity = severity;
    entry.kind = Kind::scoped;
    entry.anchor = std::chrono::steady_clock::now();
    entry.limit = limit;
    return make_handle< ScopedWatchdog >(std::move(entry));
}

WatchdogLease WatchdogRegistry::create_lease(std::string name, std::chrono::milliseconds limit,
                                             WatchdogSeverity severity) {
    Entry entry;
    entry.name = std::move(name);
    entry.severity = severity;
    entry.kind = Kind::lease;
    entry.anchor = std::chrono::steady_clock::now();
    entry.limit = limit;
    return make_handle< WatchdogLease >(std::move(entry));
}

WatchdogBark WatchdogRegistry::bark(std::string name, std::string details, WatchdogSeverity severity) {
    Entry entry;
    entry.name = std::move(name);
    entry.details = std::move(details);
    entry.severity = severity;
    entry.kind = Kind::bark;
    entry.anchor = std::chrono::steady_clock::now();
    return make_handle< WatchdogBark >(std::move(entry));
}

std::vector< WatchdogFailure > WatchdogRegistry::failures() const {
    auto const now = std::chrono::steady_clock::now();
    std::lock_guard< std::mutex > lock(mu_);
    std::vector< WatchdogFailure > out;
    out.reserve(entries_.size());
    for (auto const& item : entries_) {
        auto const& entry = item.second;
        if (entry.severity != WatchdogSeverity::critical) { continue; }
        std::string details;
        if (entry.kind == Kind::bark) {
            // bark is always failed while alive — no deadline
            details = entry.details;
        } else {
            if (now <= entry.anchor + entry.limit) { continue; }
            auto const last_seen_ms =
                std::chrono::duration_cast< std::chrono::milliseconds >(now - entry.anchor).count();
            auto const limit_ms = std::chrono::duration_cast< std::chrono::milliseconds >(entry.limit).count();
            details = "last seen " + std::to_string(last_seen_ms) + "ms ago (limit=" + std::to_string(limit_ms) + "ms)";
        }
        LOGWARN("watchdog failure: name={}, details={}", entry.name, details);
        out.push_back(WatchdogFailure{entry.name, std::move(details), entry.severity});
    }
    return out;
}

} // namespace homeobject
