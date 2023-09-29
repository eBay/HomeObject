#include "file_homeobject.hpp"

namespace homeobject {
PGManager::NullAsyncResult FileHomeObject::_create_pg(PGInfo&& pg_info, std::set< std::string, std::less<> >) {
    auto lg = std::scoped_lock(_pg_lock);

    auto const pg_path = file_store_ / std::filesystem::path(fmt::format("{:04x}", pg_info.id));
    std::filesystem::create_directories(pg_path);

    auto [it1, _] = _pg_map.try_emplace(pg_info.id, std::make_unique< PG >(pg_info));
    RELEASE_ASSERT(_pg_map.end() != it1, "Unknown map insert error!");
    return folly::makeSemiFuture< PGManager::NullResult >(folly::Unit());
}

PGManager::NullAsyncResult FileHomeObject::_replace_member(pg_id_t id, peer_id_t const& old_member,
                                                           PGMember const& new_member) {
    return folly::makeSemiFuture< PGManager::NullResult >(folly::makeUnexpected(PGError::UNSUPPORTED_OP));
}
} // namespace homeobject
