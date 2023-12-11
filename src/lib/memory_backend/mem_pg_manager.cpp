#include "mem_homeobject.hpp"

namespace homeobject {
PGManager::NullAsyncResult MemoryHomeObject::_create_pg(PGInfo&& pg_info, std::set< std::string, std::less<> >) {
    auto lg = std::scoped_lock(_pg_lock);
    auto [it1, _] = _pg_map.try_emplace(pg_info.id, std::make_unique< PG >(pg_info));
    RELEASE_ASSERT(_pg_map.end() != it1, "Unknown map insert error!");
    return folly::makeSemiFuture< PGManager::NullResult >(folly::Unit());
}

PGManager::NullAsyncResult MemoryHomeObject::_replace_member(pg_id_t id, peer_id_t const& old_member,
                                                             PGMember const& new_member) {
    return folly::makeSemiFuture< PGManager::NullResult >(folly::makeUnexpected(PGError::UNSUPPORTED_OP));
}

bool MemoryHomeObject::_get_stats(pg_id_t id, PGStats& stats) const { RELEASE_ASSERT(false, "Not implemented!"); }

void MemoryHomeObject::_get_pg_ids(std::vector< pg_id_t >& pg_ids) const { RELEASE_ASSERT(false, "Not implemented!"); }

HomeObjectStats MemoryHomeObject::_get_stats() const { RELEASE_ASSERT(false, "Not implemented!"); }
} // namespace homeobject
