#include "repl_service.h"

#include <mutex>

namespace home_replication {

class MockReplicationService : public ReplicationService {
    mutable std::shared_mutex _map_lock;
    std::map< std::string, rs_ptr_t > _set_map;

public:
    explicit MockReplicationService() {}
    ~MockReplicationService() override = default;

    folly::SemiFuture< set_var > create_replica_set(std::string const& group_id,
                                                    std::set< std::string, std::less<> >&& members) override;
    set_var get_replica_set(std::string const& group_id) const override;
    folly::SemiFuture< ReplServiceError > replace_member(std::string const& group_id, std::string const& member_out,
                                                         std::string const& member_in) const override;
    void iterate_replica_sets(std::function< void(const rs_ptr_t&) > cb) const override;
};

class MockReplicaSet : public ReplicaSet {
    std::string _g_id;

public:
    MockReplicaSet(std::string const& group_id, std::set< std::string, std::less<> >&& members) :
            _g_id(group_id), _members(std::move(members)) {}
    ~MockReplicaSet() override = default;

    std::set< std::string, std::less<> > _members;

    void write(const sisl::blob&, const sisl::blob&, const sisl::sg_list&, void*) override {}
    void transfer_pba_ownership(int64_t, const pba_list_t&) override {}
    void send_data_service_response(sisl::io_blob_list_t const&,
                                    boost::intrusive_ptr< sisl::GenericRpcData >&) override {}
    void append_entry(nuraft::buffer const&) override {}
    bool is_leader() const override { return true; }
    std::string group_id() const override { return _g_id; }

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

folly::SemiFuture< ReplicationService::set_var >
MockReplicationService::create_replica_set(std::string const& group_id,
                                           std::set< std::string, std::less<> >&& members) {
    if (1 > members.size()) return folly::makeSemiFuture< set_var >(ReplServiceError::BAD_REQUEST);

    auto lk = std::scoped_lock(_map_lock);
    if (auto [it, happened] = _set_map.try_emplace(group_id, nullptr); _set_map.end() != it) {
        if (!happened) return folly::makeSemiFuture< set_var >(ReplServiceError::SERVER_ALREADY_EXISTS);
        LOGDEBUG("Creating Pg [{}] of {} members", group_id, members.size());
        it->second = std::make_shared< MockReplicaSet >(group_id, std::move(members));
        return folly::makeSemiFuture< set_var >(it->second);
    }
    return folly::makeSemiFuture< set_var >(ReplServiceError::CANCELLED);
}

ReplicationService::set_var MockReplicationService::get_replica_set(std::string const& group_id) const {
    auto lk = std::scoped_lock(_map_lock);
    if (auto it = _set_map.find(group_id); _set_map.end() != it) { return it->second; }
    return ReplServiceError::SERVER_NOT_FOUND;
}

folly::SemiFuture< ReplServiceError > MockReplicationService::replace_member(std::string const& group_id,
                                                                             std::string const& old_member,
                                                                             std::string const& new_member) const {
    auto lk = std::scoped_lock(_map_lock);
    if (auto it = _set_map.find(group_id); _set_map.end() != it) {
        auto& repl_set = *std::dynamic_pointer_cast< MockReplicaSet >(it->second);
        if (0 == repl_set._members.erase(old_member))
            return folly::makeSemiFuture(ReplServiceError::CANNOT_REMOVE_LEADER);
        auto [_, happened] = repl_set._members.insert(new_member);
        if (!happened) return folly::makeSemiFuture(ReplServiceError::CANCELLED);
        return folly::makeSemiFuture(ReplServiceError::OK);
    }
    return folly::makeSemiFuture(ReplServiceError::SERVER_NOT_FOUND);
}

void MockReplicationService::iterate_replica_sets(std::function< void(const rs_ptr_t&) >) const {}

std::shared_ptr< ReplicationService > create_repl_service(on_replica_set_init_t&&) {
    return std::make_shared< MockReplicationService >();
}

} // namespace home_replication
