#include "repl_service.hpp"

namespace home_replication {

class MockReplicationService : public ReplicationService {
public:
    ~MockReplicationService() override = default;

    std::variant< rs_ptr_t, ReplServiceError > create_replica_set(std::string_view group_id) override;
    std::variant< rs_ptr_t, ReplServiceError > get_replica_set(std::string_view group_id) const override;
    folly::SemiFuture< ReplServiceError > replace_member(std::string_view group_id, std::string_view member_out,
                                                         std::string_view member_in) const override;
    void iterate_replica_sets(std::function< void(const rs_ptr_t&) > cb) const override;
};

std::variant< rs_ptr_t, ReplServiceError > MockReplicationService::create_replica_set(std::string_view group_id) {
    return ReplServiceError::CANCELLED;
}

std::variant< rs_ptr_t, ReplServiceError > MockReplicationService::get_replica_set(std::string_view group_id) const {
    return ReplServiceError::CANCELLED;
}
folly::SemiFuture< ReplServiceError > MockReplicationService::replace_member(std::string_view group_id,
                                                                             std::string_view member_out,
                                                                             std::string_view member_in) const {
    return folly::makeSemiFuture(ReplServiceError::CANCELLED);
}

void MockReplicationService::iterate_replica_sets(std::function< void(const rs_ptr_t&) > cb) const {}

} // namespace home_replication
