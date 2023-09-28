#include <homestore/homestore.hpp>
#include <homestore/blkdata_service.hpp>
#include <homestore/meta_service.hpp>

#include "hs_homeobject.hpp"
#include "replication_message.hpp"
#include "replication_state_machine.hpp"
#include "lib/homeobject_impl.hpp"

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

uint64_t ShardManager::max_shard_num_in_pg() { return ((uint64_t)0x01) << shard_width; }

shard_id_t HSHomeObject::generate_new_shard_id(pg_id_t pgid) {
    std::scoped_lock lock_guard(_pg_lock);
    auto iter = _pg_map.find(pgid);
    RELEASE_ASSERT(iter != _pg_map.end(), "Missing pg info");
    auto new_sequence_num = ++(iter->second->shard_sequence_num_);
    RELEASE_ASSERT(new_sequence_num < ShardManager::max_shard_num_in_pg(),
                   "new shard id must be less than ShardManager::max_shard_num_in_pg()");
    return make_new_shard_id(pgid, new_sequence_num);
}

uint64_t HSHomeObject::get_sequence_num_from_shard_id(uint64_t shard_id_t) {
    return shard_id_t & (max_shard_num_in_pg() - 1);
}

std::string HSHomeObject::serialize_shard(const Shard& shard) const {
    nlohmann::json j;
    j["shard_info"]["shard_id"] = shard.info.id;
    j["shard_info"]["pg_id_t"] = shard.info.placement_group;
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
    shard_info.id = shard_json["shard_info"]["shard_id"].get< shard_id_t >();
    shard_info.placement_group = shard_json["shard_info"]["pg_id_t"].get< pg_id_t >();
    shard_info.state = static_cast< ShardInfo::State >(shard_json["shard_info"]["state"].get< int >());
    shard_info.created_time = shard_json["shard_info"]["created_time"].get< uint64_t >();
    shard_info.last_modified_time = shard_json["shard_info"]["modified_time"].get< uint64_t >();
    shard_info.available_capacity_bytes = shard_json["shard_info"]["available_capacity"].get< uint64_t >();
    shard_info.total_capacity_bytes = shard_json["shard_info"]["total_capacity"].get< uint64_t >();
    shard_info.deleted_capacity_bytes = shard_json["shard_info"]["deleted_capacity"].get< uint64_t >();
    auto shard = Shard(shard_info);
    shard.chunk_id = shard_json["ext_info"]["chunk_id"].get< uint16_t >();
    return shard;
}

ShardManager::Result< ShardInfo > HSHomeObject::_create_shard(pg_id_t pg_owner, uint64_t size_bytes) {
    shared< homestore::ReplDev > repl_dev;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_owner);
        if (iter == _pg_map.end()) {
            LOGWARN("failed to create shard with non-exist pg [{}]", pg_owner);
            return folly::makeUnexpected(ShardError::UNKNOWN_PG);
        }
        repl_dev = std::static_pointer_cast< HS_PG >(iter->second)->repl_dev_;
    }
    if (!repl_dev) {
        LOGWARN("failed to get repl dev instance for pg [{}]", pg_owner);
        return folly::makeUnexpected(ShardError::PG_NOT_READY);
    }

    auto new_shard_id = generate_new_shard_id(pg_owner);
    auto create_time = get_current_timestamp();
    auto shard_info =
        ShardInfo(new_shard_id, pg_owner, ShardInfo::State::OPEN, create_time, create_time, size_bytes, size_bytes, 0);
    std::string const create_shard_message = serialize_shard(Shard(shard_info));

    // prepare msg header;
    const uint32_t needed_size = sizeof(ReplicationMessageHeader) + create_shard_message.size();
    auto req = repl_result_ctx< ShardManager::Result< ShardInfo > >::make(needed_size);

    uint8_t* raw_ptr = req->hdr_buf_.bytes;
    ReplicationMessageHeader* header = new (raw_ptr) ReplicationMessageHeader();
    header->message_type = ReplicationMessageType::SHARD_MESSAGE;
    header->payload_size = create_shard_message.size();
    header->payload_crc =
        crc32_ieee(init_crc32, r_cast< const uint8_t* >(create_shard_message.c_str()), create_shard_message.size());
    header->header_crc = header->calculate_crc();
    raw_ptr += sizeof(ReplicationMessageHeader);
    std::memcpy(raw_ptr, create_shard_message.c_str(), create_shard_message.size());

    // replicate this create shard message to PG members;
    repl_dev->async_alloc_write(req->hdr_buf_, sisl::blob{}, sisl::sg_list{}, req);
    auto info = req->result().get();

    header->~ReplicationMessageHeader();
    return info;
}

ShardManager::Result< ShardInfo > HSHomeObject::_seal_shard(shard_id_t id) {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

bool HSHomeObject::precheck_and_decode_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                                 std::string* msg) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.bytes);
    if (msg_header->header_crc != msg_header->calculate_crc()) {
        LOGWARN("replication message header is corrupted with crc error, lsn:{}", lsn);
        return false;
    }

    std::string shard_msg;
    shard_msg.append(r_cast< char* >(header.bytes + sizeof(ReplicationMessageHeader)), msg_header->payload_size);

    auto crc = crc32_ieee(init_crc32, r_cast< const uint8_t* >(shard_msg.c_str()), shard_msg.size());
    if (msg_header->payload_crc != crc) {
        LOGWARN("replication message body is corrupted with crc error, lsn:{}", lsn);
        return false;
    }
    *msg = std::move(shard_msg);
    return true;
}

bool HSHomeObject::on_pre_commit_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                           cintrusive< homestore::repl_req_ctx >&) {
    std::string shard_msg;
    if (!precheck_and_decode_shard_msg(lsn, header, key, &shard_msg)) {
        // header is broken, nothing to do;
        return false;
    }

    auto shard = deserialize_shard(shard_msg);
    if (shard.info.state != ShardInfo::State::OPEN) {
        // it is not an create shard msg, nothing to do;
        return false;
    }

    std::scoped_lock lock_guard(_flying_shard_lock);
    auto [_, happened] = _flying_shards.emplace(lsn, std::move(shard));
    RELEASE_ASSERT(happened, "duplicated flying create shard msg");

    return true;
}

void HSHomeObject::on_rollback_shard_msg(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                         cintrusive< homestore::repl_req_ctx >&) {
    std::string shard_msg;
    if (!precheck_and_decode_shard_msg(lsn, header, key, &shard_msg)) {
        // header is broken, nothing to do;
        return;
    }

    auto shard = deserialize_shard(shard_msg);
    if (shard.info.state != ShardInfo::State::OPEN) {
        // it is not an create shard msg, nothing to do;
        return;
    }

    std::scoped_lock lock_guard(_flying_shard_lock);
    auto iter = _flying_shards.find(lsn);
    if (iter != _flying_shards.end()) { _flying_shards.erase(iter); }
}

void HSHomeObject::on_shard_message_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                           cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    // ctx will be nullptr when:
    // 1. on the follower side
    // 2. on the leader side but homeobject restarts and replay all commited log entries from the last commit id;
    repl_result_ctx< ShardManager::Result< ShardInfo > >* ctx{nullptr};
    if (hs_ctx != nullptr) {
        ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(hs_ctx).get();
    }

    std::string shard_msg;
    if (!precheck_and_decode_shard_msg(lsn, header, key, &shard_msg)) {
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError::INVALID_ARG)); }
        return;
    }

    auto shard = deserialize_shard(shard_msg);
    switch (shard.info.state) {
    case ShardInfo::State::OPEN: {
        std::scoped_lock lock_guard(_flying_shard_lock);
        auto iter = _flying_shards.find(lsn);
        if (iter == _flying_shards.end()) {
            LOGWARN("can not find flying shards on lsn {}", lsn);
            if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError::UNKNOWN)); }
            return;
        }
        shard.chunk_id = iter->second.chunk_id;
        _flying_shards.erase(iter);
        // serialize the finalized shard msg;
        shard_msg = serialize_shard(shard);
        // persist the serialize result to homestore MetaBlkService;
        homestore::hs()->meta_service().add_sub_sb(HSHomeObject::s_shard_info_sub_type,
                                                   r_cast< const uint8_t* >(shard_msg.c_str()), shard_msg.size(),
                                                   shard.metablk_cookie);
        // update in-memory shard map;
        do_commit_new_shard(shard);
        break;
    }

    case ShardInfo::State::SEALED: {
        void* metablk_cookie = get_shard_metablk(shard.info.id);
        RELEASE_ASSERT(metablk_cookie != nullptr, "seal shard when metablk is nullptr");
        homestore::hs()->meta_service().update_sub_sb(r_cast< const uint8_t* >(shard_msg.c_str()), shard_msg.size(),
                                                      metablk_cookie);
        shard.metablk_cookie = metablk_cookie;
        do_commit_seal_shard(shard);
        break;
    }
    default: {
        break;
    }
    }

    if (ctx) { ctx->promise_.setValue(ShardManager::Result< ShardInfo >(shard.info)); }
}

void HSHomeObject::do_commit_new_shard(const Shard& shard) {
    // TODO: We are taking a global lock for all pgs to create shard. Is it really needed??
    // We need to have fine grained per PG lock and take only that.
    std::scoped_lock lock_guard(_pg_lock, _shard_lock);
    auto pg_iter = _pg_map.find(shard.info.placement_group);
    RELEASE_ASSERT(pg_iter != _pg_map.end(), "Missing PG info");
    auto& shards = pg_iter->second->shards_;
    auto iter = shards.emplace(shards.end(), shard);
    auto [_, happened] = _shard_map.emplace(shard.info.id, iter);
    RELEASE_ASSERT(happened, "duplicated shard info");

    // following part is must for follower members or when member is restarted;
    auto sequence_num = get_sequence_num_from_shard_id(shard.info.id);
    if (sequence_num > pg_iter->second->shard_sequence_num_) { pg_iter->second->shard_sequence_num_ = sequence_num; }
}

void HSHomeObject::do_commit_seal_shard(const Shard& shard) {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(shard.info.id);
    RELEASE_ASSERT(shard_iter != _shard_map.end(), "Missing shard info");
    *(shard_iter->second) = shard;
}

void* HSHomeObject::get_shard_metablk(shard_id_t id) {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(id);
    if (shard_iter == _shard_map.end()) { return nullptr; }
    return (*shard_iter->second).metablk_cookie;
}

} // namespace homeobject
