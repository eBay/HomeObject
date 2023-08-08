#pragma once
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <variant>

#include "repl_common.hpp"

#include "repl_set.hpp"

namespace nuraft {
class state_machine;
}

namespace home_replication {

using rs_ptr_t = std::shared_ptr< ReplicaSet >;

using ReplServiceError = nuraft::cmd_result_code;

class ReplicationService {
public:
    using lookup_member_cb = std::function< std::string(uuid const&) >;
    using each_set_cb = std::function< void(const rs_ptr_t&) >;
    using on_replica_set_init_t = std::function< std::shared_ptr< nuraft::state_machine >(const rs_ptr_t& rs) >;

    virtual ~ReplicationService() = default;

    virtual std::variant< rs_ptr_t, ReplServiceError > create_replica_set(std::string_view group_id) = 0;
    virtual std::variant< rs_ptr_t, ReplServiceError > lookup_replica_set(std::string_view group_id) const = 0;
    virtual ReplServiceError replace_member(std::string_view group_id, std::string_view member_out,
                                            std::string_view member_in) const = 0;
    virtual void iterate_replica_sets(each_set_cb cb) const = 0;
};

extern std::shared_ptr< ReplicationService > create_repl_service(ReplicationService::on_replica_set_init_t cb,
                                                                 ReplicationService::lookup_member_cb);

} // namespace home_replication
