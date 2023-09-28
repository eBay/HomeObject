#include <homestore/homestore.hpp>
#include <homestore/blkdata_service.hpp>
#include <homestore/meta_service.hpp>
#include <homestore/replication_service.hpp>

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

std::string HSHomeObject::serialize_shard_info(const ShardInfo& info) {
    nlohmann::json j;
    j["shard_info"]["shard_id_t"] = info.id;
    j["shard_info"]["pg_id_t"] = info.placement_group;
    j["shard_info"]["state"] = info.state;
    j["shard_info"]["created_time"] = info.created_time;
    j["shard_info"]["modified_time"] = info.last_modified_time;
    j["shard_info"]["total_capacity"] = info.total_capacity_bytes;
    j["shard_info"]["available_capacity"] = info.available_capacity_bytes;
    j["shard_info"]["deleted_capacity"] = info.deleted_capacity_bytes;
    return j.dump();
}

ShardInfo HSHomeObject::deserialize_shard_info(const char* json_str, size_t str_size) {
    ShardInfo shard_info;
    auto shard_json = nlohmann::json::parse(json_str, json_str + str_size);
    shard_info.id = shard_json["shard_info"]["shard_id_t"].get< shard_id_t >();
    shard_info.placement_group = shard_json["shard_info"]["pg_id_t"].get< pg_id_t >();
    shard_info.state = static_cast< ShardInfo::State >(shard_json["shard_info"]["state"].get< int >());
    shard_info.created_time = shard_json["shard_info"]["created_time"].get< uint64_t >();
    shard_info.last_modified_time = shard_json["shard_info"]["modified_time"].get< uint64_t >();
    shard_info.available_capacity_bytes = shard_json["shard_info"]["available_capacity"].get< uint64_t >();
    shard_info.total_capacity_bytes = shard_json["shard_info"]["total_capacity"].get< uint64_t >();
    shard_info.deleted_capacity_bytes = shard_json["shard_info"]["deleted_capacity"].get< uint64_t >();
    return shard_info;
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
    std::string const create_shard_message = serialize_shard_info(
        ShardInfo(new_shard_id, pg_owner, ShardInfo::State::OPEN, create_time, create_time, size_bytes, size_bytes, 0));
    const auto msg_size = sisl::round_up(create_shard_message.size(), repl_dev->get_blk_size());
    auto req = repl_result_ctx< ShardManager::Result< ShardInfo > >::make(msg_size, 512 /*alignment*/);
    auto buf_ptr = req->hdr_buf_.bytes;
    std::memset(buf_ptr, 0, msg_size);
    std::memcpy(buf_ptr, create_shard_message.c_str(), create_shard_message.size());
    // preapre msg header;
    req->header_.msg_type = ReplicationMessageType::CREATE_SHARD_MSG;
    req->header_.pg_id = pg_owner;
    req->header_.shard_id = new_shard_id;
    req->header_.payload_size = msg_size;
    req->header_.payload_crc = crc32_ieee(init_crc32, buf_ptr, msg_size);
    req->header_.seal();
    sisl::blob header;
    header.bytes = r_cast< uint8_t* >(&req->header_);
    header.size = sizeof(req->header_);
    sisl::sg_list value;
    value.size = msg_size;
    value.iovs.push_back(iovec(buf_ptr, msg_size));
    // replicate this create shard message to PG members;
    repl_dev->async_alloc_write(header, sisl::blob{}, value, req);
    auto info = req->result().get();
    if (!bool(info)) {
        LOGWARN("create new shard [{}] on pg [{}] is failed with error:{}", new_shard_id & shard_mask, pg_owner,
                info.error());
    }
    return info;
}

ShardManager::Result< ShardInfo > HSHomeObject::_seal_shard(shard_id_t id) {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

void HSHomeObject::on_shard_message_commit(int64_t lsn, sisl::blob const& header, homestore::MultiBlkId const& blkids,
                                           homestore::ReplDev* repl_dev,
                                           cintrusive< homestore::repl_req_ctx >& hs_ctx) {

    if (hs_ctx != nullptr) {
        auto ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(hs_ctx);
        do_shard_message_commit(lsn, *r_cast< ReplicationMessageHeader* >(header.bytes), blkids, ctx->hdr_buf_, hs_ctx);
        return;
    }

    // hs_ctx will be nullptr when HS is restarting and replay all commited log entries from the last checkpoint;
    // but do we really need to handle this for create_shard or seal_shard?
    // because this raft log had been commit before HO restarts and commit result is already saved in HS metablks
    // when HS restarts, all PG/Shard infos will be recovered from HS metablks and if we commit again, it will cause
    // duplication;
#if 0     
    sisl::sg_list value;
    value.size = blkids.blk_count() * repl_dev->get_blk_size();
    auto value_buf = iomanager.iobuf_alloc(512, value.size);
    value.iovs.push_back(iovec{.iov_base = value_buf, .iov_len = value.size});
    // header will be released when this function returns, but we still need the header when async_read() finished.
    const ReplicationMessageHeader msg_header = *r_cast< const ReplicationMessageHeader* >(header.bytes);
    repl_dev->async_read(blkids, value, value.size).thenValue([this, lsn, msg_header, blkids, value](auto&& err) {
        if (err) {
            LOGWARNMOD(homeobject, "failed to read data from homestore pba, lsn:{}", lsn);
        } else {
            sisl::blob value_blob(r_cast< uint8_t* >(value.iovs[0].iov_base), value.size);
            do_shard_message_commit(lsn, msg_header, blkids, value_blob, nullptr);
        }
        iomanager.iobuf_free(r_cast< uint8_t* >(value.iovs[0].iov_base));
    });
#endif
}

void HSHomeObject::do_shard_message_commit(int64_t lsn, const ReplicationMessageHeader& header,
                                           homestore::MultiBlkId const& blkids, sisl::blob value,
                                           cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< ShardManager::Result< ShardInfo > >* ctx{nullptr};
    if (hs_ctx != nullptr) {
        ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(hs_ctx).get();
    }

    if (header.corrupted()) {
        LOGWARNMOD(homeobject, "replication message header is corrupted with crc error, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError::INVALID_ARG)); }
        return;
    }

    if (crc32_ieee(init_crc32, value.bytes, value.size) != header.payload_crc) {
        // header & value is inconsistent;
        LOGWARNMOD(homeobject, "replication message header is inconsistent with value, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError::INVALID_ARG)); }
        return;
    }

    auto shard_info = deserialize_shard_info(r_cast< const char* >(value.bytes), value.size);
    switch (header.msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        auto hs_shard = std::make_shared< HS_Shard >(shard_info, blkids.chunk_num());
        add_new_shard_to_map(hs_shard);
        break;
    }

    case ReplicationMessageType::SEAL_SHARD_MSG: {
        update_shard_in_map(shard_info);
        break;
    }
    default: {
        break;
    }
    }

    if (ctx) { ctx->promise_.setValue(ShardManager::Result< ShardInfo >(shard_info)); }
}

void HSHomeObject::add_new_shard_to_map(ShardPtr shard) {
    // TODO: We are taking a global lock for all pgs to create shard. Is it really needed??
    // We need to have fine grained per PG lock and take only that.
    std::scoped_lock lock_guard(_pg_lock, _shard_lock);
    auto pg_iter = _pg_map.find(shard->info.placement_group);
    RELEASE_ASSERT(pg_iter != _pg_map.end(), "Missing PG info");
    auto& shards = pg_iter->second->shards_;
    shards.emplace_back(shard);
    auto [_, happened] = _shard_map.emplace(shard->info.id, shard);
    RELEASE_ASSERT(happened, "duplicated shard info");

    // following part gives follower members a chance to catch up shard sequence num;
    auto sequence_num = get_sequence_num_from_shard_id(shard->info.id);
    if (sequence_num > pg_iter->second->shard_sequence_num_) { pg_iter->second->shard_sequence_num_ = sequence_num; }
}

void HSHomeObject::update_shard_in_map(const ShardInfo& shard_info) {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(shard_info.id);
    RELEASE_ASSERT(shard_iter != _shard_map.end(), "Missing shard info");
    auto hs_shard = dp_cast< HS_Shard >(shard_iter->second);
    hs_shard->update_info(shard_info);
}

ShardManager::Result< homestore::chunk_num_t > HSHomeObject::get_shard_chunk(shard_id_t id) const {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(id);
    if (shard_iter == _shard_map.end()) { return folly::makeUnexpected(ShardError::UNKNOWN_SHARD); }
    auto hs_shard = dp_cast< HS_Shard >(shard_iter->second);
    return hs_shard->sb_->chunk_id;
}

HSHomeObject::HS_Shard::HS_Shard(ShardInfo shard_info, homestore::chunk_num_t chunk_id) :
        Shard(std::move(shard_info)), sb_("ShardManager") {
    sb_.create(sizeof(shard_info_superblk));
    sb_->chunk_id = chunk_id;
    write_sb();
}

HSHomeObject::HS_Shard::HS_Shard(homestore::superblk< shard_info_superblk > const& sb) :
        Shard(shard_info_from_sb(sb)), sb_(sb) {}

void HSHomeObject::HS_Shard::update_info(const ShardInfo& shard_info) {
    info = shard_info;
    write_sb();
}

void HSHomeObject::HS_Shard::write_sb() {
    sb_->id = info.id;
    sb_->placement_group = info.placement_group;
    sb_->state = info.state;
    sb_->created_time = info.created_time;
    sb_->last_modified_time = info.last_modified_time;
    sb_->available_capacity_bytes = info.available_capacity_bytes;
    sb_->total_capacity_bytes = info.total_capacity_bytes;
    sb_->deleted_capacity_bytes = info.deleted_capacity_bytes;
    sb_.write();
}

ShardInfo HSHomeObject::HS_Shard::shard_info_from_sb(homestore::superblk< shard_info_superblk > const& sb) {
    ShardInfo info;
    info.id = sb->id;
    info.placement_group = sb->placement_group;
    info.state = sb->state;
    info.created_time = sb->created_time;
    info.last_modified_time = sb->last_modified_time;
    info.available_capacity_bytes = sb->available_capacity_bytes;
    info.total_capacity_bytes = sb->total_capacity_bytes;
    info.deleted_capacity_bytes = sb->deleted_capacity_bytes;
    return info;
}

} // namespace homeobject
