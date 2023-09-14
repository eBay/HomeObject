#include "homeobject.hpp"
#include "replication_message.hpp"

#include <homestore/homestore.hpp>
#include <homestore/blkdata_service.hpp>
#include <homestore/meta_service.hpp>

namespace homeobject {

static constexpr uint32_t SEQUENCE_BIT_NUM_IN_SHARD{48};

uint64_t ShardManager::max_shard_size() { return Gi; }

uint64_t ShardManager::max_shard_num_in_pg() {return ((uint64_t)0x01) << SEQUENCE_BIT_NUM_IN_SHARD;}

shard_id HSHomeObject::generate_new_shard_id(pg_id pg) {
    std::scoped_lock lock_guard(_pg_lock);
    auto iter = _pg_map.find(pg);
    RELEASE_ASSERT(iter != _pg_map.end(), "Missing pg info");
    auto new_sequence_num = ++(iter->second.shard_sequence_num);
    RELEASE_ASSERT(new_sequence_num < ShardManager::max_shard_num_in_pg(), "new shard id must be less than ShardManager::max_shard_num_in_pg()");
    return make_new_shard_id(pg, new_sequence_num);
}

shard_id HSHomeObject::make_new_shard_id(pg_id pg, uint64_t sequence_num) const {
    return ((uint64_t)pg << SEQUENCE_BIT_NUM_IN_SHARD) | sequence_num;
}

uint64_t HSHomeObject::get_sequence_num_from_shard_id(uint64_t shard_id) {
    return shard_id & (max_shard_num_in_pg() - 1);
}

std::string HSHomeObject::serialize_shard(const Shard& shard) const {
    nlohmann::json j;    
    j["shard_info"]["shard_id"] = shard.info.id;
    j["shard_info"]["pg_id"] = shard.info.placement_group;
    j["shard_info"]["state"] = shard.info.state;
    j["shard_info"]["created_time"] = shard.info.created_time;
    j["shard_info"]["modified_time"] = shard.info.last_modified_time;
    j["shard_info"]["total_capacity"] = shard.info.total_capacity_bytes;
    j["shard_info"]["available_capacity"] = shard.info.available_capacity_bytes;
    j["shard_info"]["deleted_capacity"] = shard.info.deleted_capacity_bytes;
    j["ext_info"]["chunk_id"] = shard.chunk_id;
    return j.dump();
}

Shard HSHomeObject::deserialize_shard(const std::string& shard_json_str) const {
    auto shard_json = nlohmann::json::parse(shard_json_str);  
    ShardInfo shard_info;
    shard_info.id = shard_json["shard_info"]["shard_id"].get<shard_id>();
    shard_info.placement_group = shard_json["shard_info"]["pg_id"].get<pg_id>();
    shard_info.state = static_cast<ShardInfo::State>(shard_json["shard_info"]["state"].get<int>());
    shard_info.created_time = shard_json["shard_info"]["created_time"].get<uint64_t>();
    shard_info.last_modified_time = shard_json["shard_info"]["modified_time"].get<uint64_t>();
    shard_info.available_capacity_bytes = shard_json["shard_info"]["available_capacity"].get<uint64_t>();
    shard_info.total_capacity_bytes = shard_json["shard_info"]["total_capacity"].get<uint64_t>();
    shard_info.deleted_capacity_bytes = shard_json["shard_info"]["deleted_capacity"].get<uint64_t>();
    auto shard = Shard(shard_info);
    shard.chunk_id = shard_json["ext_info"]["chunk_id"].get<uint16_t>();
    return shard;
}

ShardManager::Result< ShardInfo > HSHomeObject::_create_shard(pg_id pg_owner, uint64_t size_bytes) {
    auto pg = _get_pg(pg_owner);
    if (!bool(pg)) {
        LOGWARN("failed to create shard with non-exist pg [{}]", pg_owner);
        return folly::makeUnexpected(ShardError::UNKNOWN_PG);
    }
    //TODO: will update to ReplDev when ReplDev on HomeStore is ready;
    auto replica_set_var = _repl_svc->get_replica_set(fmt::format("{}", pg_owner));
    if (std::holds_alternative< home_replication:: ReplServiceError>(replica_set_var)) {
        LOGWARN("failed to get replica set instance for pg [{}]", pg_owner);
	return folly::makeUnexpected(ShardError::UNKNOWN_PG);	
    }

    auto replica_set = std::get< home_replication::rs_ptr_t >(replica_set_var);
    if (!replica_set->is_leader()) {
        LOGWARN("pg [{}] replica set is not a leader, please retry other pg members", pg_owner);
        return folly::makeUnexpected(ShardError::NOT_LEADER);
    }

    auto new_shard_id = generate_new_shard_id(pg_owner);
    auto create_time = get_current_timestamp();
    auto shard_info = ShardInfo(new_shard_id, pg_owner, ShardInfo::State::OPEN,
                                create_time, create_time, size_bytes, size_bytes, 0);
    std::string create_shard_message = serialize_shard(Shard(shard_info));
    //preapre msg header;
    const uint32_t needed_size = sizeof(ReplicationMessageHeader) + create_shard_message.size();
    auto buf = nuraft::buffer::alloc(needed_size);
    uint8_t* raw_ptr = static_cast<uint8_t*>(buf->data_begin());
    ReplicationMessageHeader *header = new(raw_ptr) ReplicationMessageHeader();
    header->message_type = ReplicationMessageType::SHARD_MESSAGE;
    header->payload_size = create_shard_message.size();
    header->payload_crc = crc32_ieee(init_crc32, r_cast< const uint8_t* >(create_shard_message.c_str()), create_shard_message.size());
    header->header_crc = header->calculate_crc();
    raw_ptr += sizeof(ReplicationMessageHeader);
    std::memcpy(raw_ptr, create_shard_message.c_str(), create_shard_message.size());
    
    sisl::sg_list value;
    //replicate this create shard message to PG members;
    auto [p, sf] = folly::makePromiseContract< ShardManager::Result<ShardInfo> >();
    replica_set->write(sisl::blob(buf->data_begin(), needed_size), sisl::blob(), value,
                       static_cast<void*>(&p));
    auto info = std::move(sf).get();
    if (!bool(info)) {
        LOGWARN("create new shard [{}] on pg [{}] is failed with error:{}", new_shard_id, pg_owner, info.error());
    }
    header->~ReplicationMessageHeader();
    return info;
}

ShardManager::Result< ShardInfo > HSHomeObject::_seal_shard(shard_id id) {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

bool HSHomeObject::precheck_and_decode_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                                       std::string* msg) {
    const ReplicationMessageHeader* msg_header = r_cast<const ReplicationMessageHeader*>(header.bytes);
    if (msg_header->header_crc != msg_header->calculate_crc()) {
        LOGWARN("replication message header is corrupted with crc error, lsn:{}", lsn);
        return false;
    }

    std::string shard_msg;
    shard_msg.append(r_cast<char*>(header.bytes + sizeof(ReplicationMessageHeader)), msg_header->payload_size);

    auto crc = crc32_ieee(init_crc32, r_cast< const uint8_t* >(shard_msg.c_str()), shard_msg.size());
    if (msg_header->payload_crc != crc) {
        LOGWARN("replication message body is corrupted with crc error, lsn:{}", lsn);
	return false;
    }
    *msg = std::move(shard_msg);
    return true;
}

void HSHomeObject::on_pre_commit_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                           void* user_ctx) {
    std::string shard_msg;
    if (!precheck_and_decode_shard_msg(lsn, header, key, &shard_msg)) {
        //header is broken, nothing to do;
        return;
    }

    auto shard = deserialize_shard(shard_msg);
    if (shard.info.state != ShardInfo::State::OPEN) {
        //it is not an create shard msg, nothing to do;
        return;
    }

    //write shard header to ReplDev which will bind the newly created shard to one underlay homestore physical chunk;
    using namespace homestore;
    auto datasvc_page_size = HomeStore::instance()->data_service().get_page_size();
    if (shard_msg.size() % datasvc_page_size != 0) {
        uint32_t need_size = (shard_msg.size() / datasvc_page_size + 1)  * datasvc_page_size;
        shard_msg.resize(need_size);
    }

    auto write_buf = iomanager.iobuf_alloc(512, shard_msg.size());
    std::memcpy(r_cast<uint8_t*>(write_buf), r_cast<const uint8_t*>(shard_msg.c_str()), shard_msg.size());

    sisl::sg_list sgs;
    sgs.size = shard_msg.size();
    sgs.iovs.emplace_back(iovec(static_cast<void*>(write_buf), shard_msg.size()));
    std::vector< homestore::BlkId > out_blkids;
    auto future = HomeStore::instance()->data_service().async_alloc_write(sgs, homestore::blk_alloc_hints(), out_blkids);
    bool write_result = std::move(future).get();
    iomanager.iobuf_free(write_buf);
    if (!write_result) {
        LOGWARN("write lsn {} msg to homestore data service is failed", lsn);
        return;
    }
    RELEASE_ASSERT(out_blkids.size() == 1, "output blkids num is not 1");
    shard.chunk_id = out_blkids[0].get_chunk_num();
    std::scoped_lock lock_guard(_flying_shard_lock);
    auto [_, happened] = _flying_shards.emplace(lsn, std::move(shard));
    RELEASE_ASSERT(happened, "duplicated flying create shard msg");
}

void HSHomeObject::on_rollback_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                         void* user_ctx) {
    std::string shard_msg;
    if (!precheck_and_decode_shard_msg(lsn, header, key, &shard_msg)) {
        //header is broken, nothing to do;
        return;
    }

    auto shard = deserialize_shard(shard_msg);
    if (shard.info.state != ShardInfo::State::OPEN) {
        //it is not an create shard msg, nothing to do;
        return;
    }
    
    std::scoped_lock lock_guard(_flying_shard_lock);
    auto iter = _flying_shards.find(lsn);
    if (iter != _flying_shards.end()) {
        _flying_shards.erase(iter);
    }
}

void HSHomeObject::on_shard_message_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                           void* user_ctx) {

    folly::Promise<ShardManager::Result<ShardInfo> > *promise = nullptr;
    // user_ctx will be nullptr when:
    // 1. on the follower side
    // 2. on the leader side but homeobject restarts and replay all commited log entries from the last commit id;
    if (user_ctx != nullptr) {
        promise = r_cast<folly::Promise<ShardManager::Result<ShardInfo> >*> (user_ctx);      
    }

    std::string shard_msg;
    if (!precheck_and_decode_shard_msg(lsn, header, key, &shard_msg)) {
        if (promise) {
            promise->setValue(folly::makeUnexpected(ShardError::INVALID_ARG));
        }
    }

    auto shard = deserialize_shard(shard_msg);
    if (shard.info.state == ShardInfo::State::OPEN) {
        std::scoped_lock lock_guard(_flying_shard_lock);
        auto iter = _flying_shards.find(lsn);
        if (iter == _flying_shards.end()) {
            LOGWARN("can not find flying shards on lsn {}", lsn);
            if (promise) {
                promise->setValue(folly::makeUnexpected(ShardError::UNKNOWN));
            }
            return;
        }
        shard.chunk_id = iter->second.chunk_id;
        _flying_shards.erase(iter);
        //deserialize the finalized shard msg;
        shard_msg = serialize_shard(shard);
    }

    // persist the serialize result to homestore MetaBlkService;
    void* cookie = nullptr;
    homestore::hs()->meta_service().add_sub_sb(HSHomeObject::s_shard_info_sub_type,
                                               r_cast<const uint8_t*>(shard_msg.c_str()), shard_msg.size(), cookie);

    //update in-memory shard map;
    if (shard.info.state == ShardInfo::State::OPEN) {
        //create shard;
        do_commit_new_shard(shard);
    } else {
        do_commit_seal_shard(shard);
    }

    if (promise) {
        promise->setValue(ShardManager::Result<ShardInfo>(shard.info));
    }
}

void HSHomeObject::do_commit_new_shard(const Shard& shard) {
    std::scoped_lock lock_guard(_pg_lock, _shard_lock);  
    auto pg_iter = _pg_map.find(shard.info.placement_group);
    RELEASE_ASSERT(pg_iter != _pg_map.end(), "Missing PG info");
    auto& shards = pg_iter->second.shards;
    auto iter = shards.emplace(shards.end(), shard);
    auto [_, happened] = _shard_map.emplace(shard.info.id, iter);
    RELEASE_ASSERT(happened, "duplicated shard info");

    //following part is must for follower members or when member is restarted;
    auto sequence_num = get_sequence_num_from_shard_id(shard.info.id);
    if (sequence_num > pg_iter->second.shard_sequence_num) {
        pg_iter->second.shard_sequence_num = sequence_num;
    }
}

void HSHomeObject::do_commit_seal_shard(const Shard& shard) {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(shard.info.id);
    RELEASE_ASSERT(shard_iter != _shard_map.end(), "Missing shard info");
    *(shard_iter->second) = shard;
}

} // namespace homeobject
