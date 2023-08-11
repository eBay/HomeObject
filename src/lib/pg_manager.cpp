#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< PGManager > HomeObjectImpl::pg_manager() { return shared_from_this(); }

folly::Future< PGError > HomeObjectImpl::create_pg(PGInfo const& pg_info) {
    return folly::makeFuture(PGError::TIMEOUT);
}

folly::Future< PGError > HomeObjectImpl::replace_member(pg_id id, peer_id const& old_member,
                                                        PGMember const& new_member) {
    return folly::makeFuture(PGError::UNKNOWN_PG);
}

} // namespace homeobject
