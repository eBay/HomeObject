#include "mem_homeobject.hpp"

namespace homeobject {
PGManager::NullAsyncResult MemoryHomeObject::_create_pg(PGInfo&& pg_info, std::set< peer_id_t > const&,
                                                        trace_id_t tid) {
    (void)tid;
    auto lg = std::scoped_lock(_pg_lock);
    auto [it1, _] = _pg_map.try_emplace(pg_info.id, std::make_unique< PG >(pg_info));
    RELEASE_ASSERT(_pg_map.end() != it1, "Unknown map insert error!");
    return folly::makeSemiFuture< PGManager::NullResult >(PGManager::NullResult());
}

PGManager::NullAsyncResult MemoryHomeObject::_replace_member(pg_id_t id, std::string& task_id,
                                                             peer_id_t const& old_member, PGMember const& new_member,
                                                             uint32_t commit_quorum, trace_id_t tid) {
    (void)old_member;
    (void)new_member;
    (void)commit_quorum;
    (void)tid;
    auto lg = std::shared_lock(_pg_lock);
    auto it = _pg_map.find(id);
    if (_pg_map.end() == it) {
        return folly::makeSemiFuture< PGManager::NullResult >(std::unexpected(PGError::UNKNOWN_PG));
    }
    return folly::makeSemiFuture< PGManager::NullResult >(std::unexpected(PGError::UNSUPPORTED_OP));
}

PGReplaceMemberStatus MemoryHomeObject::_get_replace_member_status(pg_id_t id, std::string& task_id,
                                                                   const PGMember& old_member,
                                                                   const PGMember& new_member,
                                                                   const std::vector< PGMember >& others,
                                                                   uint64_t trace_id) const {
    (void)id;
    (void)task_id;
    (void)old_member;
    (void)new_member;
    (void)others;
    (void)trace_id;
    return PGReplaceMemberStatus{};
}

bool MemoryHomeObject::_get_stats(pg_id_t id, PGStats& stats) const {
    auto lg = std::shared_lock(_pg_lock);
    auto it = _pg_map.find(id);
    if (_pg_map.end() == it) { return false; }
    auto pg = it->second.get();
    stats.id = pg->pg_info_.id;
    stats.replica_set_uuid = pg->pg_info_.replica_set_uuid;
    stats.num_members = pg->pg_info_.members.size();
    stats.total_shards = pg->shards_.size();
    stats.open_shards =
        std::count_if(pg->shards_.begin(), pg->shards_.end(), [](auto const& s) { return s->is_open(); });
    for (auto const& m : pg->pg_info_.members) {
        stats.members.emplace_back(peer_info{.id = m.id, .name = m.name});
    }

    return true;
}

void MemoryHomeObject::_get_pg_ids(std::vector< pg_id_t >& pg_ids) const {
    auto lg = std::shared_lock(_pg_lock);
    for (auto& [id, _] : _pg_map) {
        pg_ids.push_back(id);
    }
}

HomeObjectStats MemoryHomeObject::_get_stats() const {
    HomeObjectStats stats;
    uint32_t num_open_shards = 0ul;
    std::scoped_lock shared_lock(_pg_lock);
    for (auto const& [_, pg] : _pg_map) {
        auto mem_pg = pg.get();
        num_open_shards +=
            std::count_if(mem_pg->shards_.begin(), mem_pg->shards_.end(), [](auto const& s) { return s->is_open(); });
    }

    stats.num_open_shards = num_open_shards;
    return stats;
}

void MemoryHomeObject::_destroy_pg(pg_id_t pg_id) {
    auto lg = std::unique_lock(_pg_lock);
    _pg_map.erase(pg_id);
}

} // namespace homeobject
