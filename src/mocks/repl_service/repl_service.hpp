#pragma once
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <variant>

#include <folly/futures/Future.h>

#include "repl_common.hpp"
#include "repl_set.hpp"

namespace nuraft {
class state_machine;
}

namespace home_replication {

using rs_ptr_t = std::shared_ptr< ReplicaSet >;

using ReplServiceError = nuraft::cmd_result_code;

class ReplicatedServer {
public:
    virtual ~ReplicatedServer() = default;

    /// Map HomeObject instance to IP
    virtual folly::SemiFuture< std::string > member_address(uuid const&) const = 0;
};

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

extern std::shared_ptr< ReplicationService > create_repl_service(ReplicatedServer& server);

} // namespace home_replication
