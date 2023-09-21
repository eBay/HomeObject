#pragma once

#include "repl_service.h"

namespace home_replication {

class MockReplicaSet : public ReplicaSet {
    std::string _g_id;
    bool _is_leader{true};
    uint32_t _current_lsn{0};
    std::shared_ptr< ReplicaSetListener > _listener;

public:
    MockReplicaSet(std::string const& group_id, std::set< std::string, std::less<> >&& members) :
            _g_id(group_id), _members(std::move(members)) {}
    ~MockReplicaSet() override = default;

    std::set< std::string, std::less<> > _members;

    void write(const sisl::blob& head, const sisl::blob& key, const sisl::sg_list&, void* ctx) override {
        // This is a mock replica set without really replication behaviors, so it just call
        // ReplicaSetListener::on_pre_commit() and ReplicaSetListener::on_commit();
        if (_listener) {
            _listener->on_pre_commit(_current_lsn, head, key, ctx);
            home_replication::pba_list_t pba;
            _listener->on_commit(_current_lsn, head, key, pba, ctx);
        }
        ++_current_lsn;
    }
    void transfer_pba_ownership(int64_t, const pba_list_t&) override {}
    void send_data_service_response(sisl::io_blob_list_t const&,
                                    boost::intrusive_ptr< sisl::GenericRpcData >&) override {}
    void append_entry(nuraft::buffer const&) override {}
    void set_leader() { _is_leader = true; }
    void set_follower() { _is_leader = false; }
    bool is_leader() const override { return _is_leader; }
    std::string group_id() const override { return _g_id; }
    void set_listener(std::shared_ptr< ReplicaSetListener > listener) { _listener = listener; }

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

} // namespace home_replication
