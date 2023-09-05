#pragma once
#include <functional>
#include <memory>
#include <string>
#include <variant>

#include <folly/futures/Future.h>

#include "repl_decls.h"
#include "repl_set.h"

namespace nuraft {
class state_machine;
}

namespace home_replication {

using rs_ptr_t = std::shared_ptr< ReplicaSet >;

using ReplServiceError = nuraft::cmd_result_code;

using on_replica_set_init_t = std::function< std::unique_ptr< ReplicaSetListener >(const rs_ptr_t& rs) >;

class ReplicationService {
public:
    virtual ~ReplicationService() = default;

    using set_var = std::variant< rs_ptr_t, ReplServiceError >;

    /// Sync APIs
    virtual set_var get_replica_set(std::string const& group_id) const = 0;
    virtual void iterate_replica_sets(std::function< void(const rs_ptr_t&) > cb) const = 0;

    /// Async APIs
    virtual folly::SemiFuture< set_var > create_replica_set(std::string const& group_id,
                                                            std::set< std::string, std::less<> >&& members) = 0;
    virtual folly::SemiFuture< ReplServiceError >
    replace_member(std::string const& group_id, std::string const& member_out, std::string const& member_in) const = 0;
};

extern std::shared_ptr< ReplicationService > create_repl_service(on_replica_set_init_t&& init_cb);

} // namespace home_replication
