#include "repl_service.hpp"

#include <mutex>

namespace home_replication {

class MockReplicationService : public ReplicationService {
    std::shared_mutex _map_lock;
    std::map< std::string, rs_ptr_t > _set_map;

public:
    ~MockReplicationService() override = default;

    std::variant< rs_ptr_t, ReplServiceError > create_replica_set(std::string_view group_id) override;
    std::variant< rs_ptr_t, ReplServiceError > get_replica_set(std::string_view group_id) const override;
    folly::SemiFuture< ReplServiceError > replace_member(std::string_view group_id, std::string_view member_out,
                                                         std::string_view member_in) const override;
    void iterate_replica_sets(std::function< void(const rs_ptr_t&) > cb) const override;
};

class MockReplicaSet : public ReplicaSet {
    std::string _g_id;

public:
    explicit MockReplicaSet(std::string_view group_id) : _g_id(group_id) {}
    ~MockReplicaSet() override = default;

    void attach_state_machine(std::unique_ptr< nuraft::state_machine >) override {}
    void write(const sisl::blob&, const sisl::blob&, const sisl::sg_list&, void*) override {}
    void transfer_pba_ownership(int64_t, const pba_list_t&) override {}
    void send_data_service_response(sisl::io_blob_list_t const&,
                                    boost::intrusive_ptr< sisl::GenericRpcData >&) override {}
    void append_entry(nuraft::buffer const&) override {}
    bool is_leader() const override { return true; }
    std::string_view group_id() const override { return _g_id; }

    /// nuraft_mesg::mesg_state_mgr overrides
    uint32_t get_logstore_id() const override { return 0u; }
    std::shared_ptr< nuraft::state_machine > get_state_machine() { return nullptr; }
    void permanent_destroy() override {}
    void leave() override {}
    ///

    /// nuraft::state_mgr overrides
    std::shared_ptr< nuraft::cluster_config > load_config() override { return nullptr; }
    void save_config(const nuraft::cluster_config&) override {}
    void save_state(const nuraft::srv_state&) override {}
    std::shared_ptr< nuraft::srv_state > read_state() override { return nullptr; }
    std::shared_ptr< nuraft::log_store > load_log_store() override { return nullptr; }
    int32_t server_id() override { return 0; }
    void system_exit(const int) override {}
    ///
};

std::variant< rs_ptr_t, ReplServiceError > MockReplicationService::create_replica_set(std::string_view group_id) {
    auto lk = std::scoped_lock(_map_lock);
    if (auto [it, happened] = _set_map.try_emplace(std::string{group_id}, nullptr); _set_map.end() != it) {
        if (happened) it->second = std::make_shared< MockReplicaSet >(group_id);
        return it->second;
    }
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
