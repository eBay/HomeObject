#include "repl_service.hpp"

namespace home_replication {

class MockReplicationService : public ReplicationService {
public:
    ~MockReplicationService() override = default;

    std::variant< rs_ptr_t, ReplServiceError > create_replica_set(std::string_view group_id) override;
    std::variant< rs_ptr_t, ReplServiceError > lookup_replica_set(std::string_view group_id) const override;
    ReplServiceError replace_member(std::string_view group_id, std::string_view member_out,
                                    std::string_view member_in) const override;
    void iterate_replica_sets(each_set_cb cb) const override;
};

std::variant< rs_ptr_t, ReplServiceError > MockReplicationService::create_replica_set(std::string_view group_id) {
    return ReplServiceError::CANCELLED;
}

std::variant< rs_ptr_t, ReplServiceError > MockReplicationService::lookup_replica_set(std::string_view group_id) const {
    return ReplServiceError::CANCELLED;
}
ReplServiceError MockReplicationService::replace_member(std::string_view group_id, std::string_view member_out,
                                                        std::string_view member_in) const {
    return ReplServiceError::CANCELLED;
}

void MockReplicationService::iterate_replica_sets(each_set_cb) const {}

} // namespace home_replication
