#include "homeobject.hpp"
#include "replication_message.hpp"

#include <homestore/homestore.hpp>
#include <homestore/meta_service.hpp>

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }
uint64_t ShardManager::max_shard_num_in_pg() {return ((uint64_t)0x01) << 48;}

bool HSHomeObject::check_if_pg_exist(pg_id pg) const {
    std::scoped_lock lock_guard(_pg_lock);
    auto iter = _pg_map.find(pg);
    return (iter == _pg_map.end()) ? false : true;
}

shard_id HSHomeObject::generate_new_shard_id(pg_id pg) {
    std::scoped_lock lock_guard(_pg_lock);
    auto iter = _pg_map.find(pg);
    if (iter == _pg_map.end()) {
        auto created_pg = PG(std::move(PGInfo(pg)));
        iter = _pg_map.insert(std::make_pair(pg, std::move(created_pg))).first;
    }
    auto new_sequence_num = ++iter->second.next_sequence_num_for_new_shard;
    RELEASE_ASSERT(new_sequence_num < ShardManager::max_shard_num_in_pg(), "new shard id must be less than ShardManager::max_shard_num_in_pg()");
    return make_new_shard_id(pg, new_sequence_num);
}

shard_id HSHomeObject::make_new_shard_id(pg_id pg, uint64_t sequence_num) const {
    return ((uint64_t)pg << 48) | sequence_num; 
}

uint64_t HSHomeObject::get_sequence_num_from_shard_id(uint64_t shard_id) {
    return shard_id & (0x0000FFFFFFFFFFFF);
}
std::string HSHomeObject::prepare_create_shard_message(pg_id pg, shard_id new_shard_id, uint64_t shard_size) const {
    nlohmann::json j;
    j["pg_id"] = pg;
    j["shard_id"] = new_shard_id;
    j["state"] = ShardInfo::State::OPEN;
    j["capacity"] = shard_size;
    return j.dump();
}

ShardManager::Result< ShardInfo > HSHomeObject::_create_shard(pg_id pg_owner, uint64_t size_bytes) {
    bool pg_exist = check_if_pg_exist(pg_owner);
    if (!pg_exist) {
        LOGWARN("failed to create shard with non-exist pg [{}]", pg_owner);
        return folly::makeUnexpected(ShardError::UNKNOWN_PG);
    }

    auto repl_dev = _repl_svc->get_replica_dev(fmt::format("{}", pg_owner));
    if (repl_dev == nullptr) {
        LOGWARN("failed to get replica set instance for pg [{}]", pg_owner);
	return folly::makeUnexpected(ShardError::UNKNOWN_PG);
    }

    if (!repl_dev->is_leader()) {
        LOGWARN("pg [{}] replica set is not a leader, please retry other pg members", pg_owner);
        return folly::makeUnexpected(ShardError::NOT_LEADER);
    }

    auto new_shard_id = generate_new_shard_id(pg_owner);
    std::string create_shard_message = prepare_create_shard_message(pg_owner, new_shard_id, size_bytes);
    ReplicationMessageHeader header;
    header.message_type = ReplicationMessageType::SHARD_MESSAGE;
    header.message_size = create_shard_message.size();
    header.message_crc = crc32_ieee(0, r_cast< const uint8_t* >(create_shard_message.c_str()), create_shard_message.size());
    header.header_crc = header.calculate_crc();
    sisl::sg_list value;
    value.size = create_shard_message.size();
    value.iovs.emplace_back(iovec{static_cast<void*>(const_cast<char*>(create_shard_message.c_str())),
                                  create_shard_message.size()});
    //replicate this create shard message to PG members;
    auto [p, sf] = folly::makePromiseContract< ShardManager::Result<ShardInfo> >();
    repl_dev->async_alloc_write(sisl::blob(uintptr_cast(&header), sizeof(header)),
                                sisl::blob(),
                                value,
                                static_cast<void*>(&p));
    auto info = std::move(sf).via(folly::getGlobalCPUExecutor()).get();
    if (!bool(info)) {
        LOGWARN("create new shard [{}] on pg [{}] is failed with error:{}", new_shard_id, pg_owner, info.error());
    }
    return info;
}

ShardManager::Result< ShardInfo > HSHomeObject::_seal_shard(shard_id id) {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

void HSHomeObject::on_shard_message_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                             sisl::sg_list const& value, void* user_ctx) {
    const ReplicationMessageHeader* msg_header = r_cast<const ReplicationMessageHeader*>(header.bytes);
    folly::Promise<ShardManager::Result<ShardInfo> > *promise = nullptr;
    // user_ctx will be nullptr when:
    // 1. on the follower side
    // 2. on the leader side but homeobject restarts and replay all commited log entries from the last commit id;
    if (user_ctx != nullptr) {
        promise = r_cast<folly::Promise<ShardManager::Result<ShardInfo> >*> (user_ctx);      
    }

    if (msg_header->header_crc != msg_header->calculate_crc()) {
        LOGWARN("replication message header is corrupted with crc error, lsn:{}", lsn);
        if (promise) {
            promise->setValue(folly::makeUnexpected(ShardError::INVALID_ARG));	  
        }
        return;
    }

    std::string string_value;
    for (auto & iovec: value.iovs) {
        string_value.append(static_cast<char*>(iovec.iov_base), iovec.iov_len);
    }

    auto crc = crc32_ieee(0, r_cast< const uint8_t* >(string_value.c_str()), string_value.size());
    if (msg_header->message_crc != crc) {
        LOGWARN("replication message body is corrupted with crc error, lsn:{}", lsn);
        if (promise) {
            promise->setValue(folly::makeUnexpected(ShardError::INVALID_ARG));	  
        }
        return;
    }

    //preapre ShardInfoExt msg and persisted this ShardInfoExt to homestore MetaBlkService;
    void* cookie = nullptr;
    homestore::hs()->meta_service().add_sub_sb(HomeObjectImpl::s_shard_info_sub_type, r_cast<const uint8_t*>(string_value.c_str()), string_value.size(), cookie);

    //update in-memory shard map;
    CShardInfo shard_info = std::make_shared<ShardInfo>();
    auto shard_info_json = nlohmann::json::parse(string_value);
    shard_info->id = shard_info_json["shard_id"].get<shard_id>();
    shard_info->placement_group = shard_info_json["pg_id"].get<pg_id>();
    shard_info->state = static_cast<ShardInfo::State>(shard_info_json["state"].get<int>());
    shard_info->total_capacity_bytes = shard_info_json["capacity"].get<uint64_t>();
    std::scoped_lock lock_guard(_pg_lock, _shard_lock);
    auto pg_iter = _pg_map.find(shard_info->placement_group);
    RELEASE_ASSERT(pg_iter != _pg_map.end(), "Missing PG info");
    pg_iter->second.shards.push_back(shard_info);
    _shard_map[shard_info->id]  = shard_info;

    if (promise) {
        promise->setValue(ShardManager::Result<ShardInfo>(*shard_info));
    }
}

} // namespace homeobject
