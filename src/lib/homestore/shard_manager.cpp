#include "homeobject.hpp"
#include "replication_message.hpp"

#include <homestore/homestore.hpp>
#include <homestore/blkdata_service.hpp>
#include <homestore/meta_service.hpp>
#include <homestore/replication_service.hpp>

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

uint64_t ShardManager::max_shard_num_in_pg() { return ((uint64_t)0x01) << shard_width; }

shard_id HSHomeObject::generate_new_shard_id(pg_id pg) {
    std::scoped_lock lock_guard(_pg_lock);
    auto iter = _pg_map.find(pg);
    RELEASE_ASSERT(iter != _pg_map.end(), "Missing pg info");
    auto new_sequence_num = ++(iter->second.shard_sequence_num);
    RELEASE_ASSERT(new_sequence_num < ShardManager::max_shard_num_in_pg(),
                   "new shard id must be less than ShardManager::max_shard_num_in_pg()");
    return make_new_shard_id(pg, new_sequence_num);
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

Shard HSHomeObject::deserialize_shard(const char* json_str, size_t str_size) const {
    auto shard_json = nlohmann::json::parse(json_str, json_str + str_size);
    ShardInfo shard_info;
    shard_info.id = shard_json["shard_info"]["shard_id"].get< shard_id >();
    shard_info.placement_group = shard_json["shard_info"]["pg_id"].get< pg_id >();
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

ShardManager::Result< ShardInfo > HSHomeObject::_create_shard(pg_id pg_owner, uint64_t size_bytes) {
    auto pg = _get_pg(pg_owner);
    if (!bool(pg)) {
        LOGWARN("failed to create shard with non-exist pg [{}]", pg_owner);
        return folly::makeUnexpected(ShardError::UNKNOWN_PG);
    }

    homestore::ReplicationService* replication_service =
        (homestore::ReplicationService*)(&homestore::HomeStore::instance()->repl_service());
    auto repl_dev = replication_service->get_replica_dev(pg.value().repl_dev_uuid);
    if (!bool(repl_dev)) {
        LOGWARN("failed to get replica set instance for pg [{}]", pg_owner);
        return folly::makeUnexpected(ShardError::UNKNOWN_PG);
    }

    auto new_shard_id = generate_new_shard_id(pg_owner);
    auto create_time = get_current_timestamp();
    auto shard_info =
        ShardInfo(new_shard_id, pg_owner, ShardInfo::State::OPEN, create_time, create_time, size_bytes, size_bytes, 0);
    std::string create_shard_message = serialize_shard(Shard(shard_info));

    // round up msg size to homestore datasvc blk size
    const auto datasvc_blk_size = homestore::HomeStore::instance()->data_service().get_blk_size();
    auto msg_size = create_shard_message.size();
    if (msg_size % datasvc_blk_size != 0) { msg_size = (msg_size / datasvc_blk_size + 1) * datasvc_blk_size; }
    auto msg_buf = iomanager.iobuf_alloc(512, msg_size);
    std::memset(r_cast< uint8_t* >(msg_buf), 0, msg_size);
    std::memcpy(r_cast< uint8_t* >(msg_buf), create_shard_message.c_str(), create_shard_message.size());

    // preapre msg header;
    ReplicationMessageHeader header;
    header.repl_group_id = pg.value().pg_info.id;
    header.msg_type = ReplicationMessageType::CREATE_SHARD_MSG;
    header.payload_size = msg_size;
    header.payload_crc = crc32_ieee(init_crc32, r_cast< const uint8_t* >(msg_buf), msg_size);
    header.header_crc = header.calculate_crc();
    sisl::sg_list value;
    value.size = msg_size;
    value.iovs.push_back(iovec(msg_buf, msg_size));
    // replicate this create shard message to PG members;
    auto [p, sf] = folly::makePromiseContract< ShardManager::Result< ShardInfo > >();
    repl_dev.value()->async_alloc_write(sisl::blob(r_cast< uint8_t* >(&header), sizeof(header)), sisl::blob(), value,
                                        static_cast< void* >(&p));
    auto info = std::move(sf).get();
    if (!bool(info)) {
        LOGWARN("create new shard [{}] on pg [{}] is failed with error:{}", new_shard_id & shard_mask, pg_owner,
                info.error());
    }
    iomanager.iobuf_free(msg_buf);
    return info;
}

ShardManager::Result< ShardInfo > HSHomeObject::_seal_shard(shard_id id) {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

void HSHomeObject::on_shard_message_commit(int64_t lsn, sisl::blob const& header, sisl::blob const&,
                                           homestore::MultiBlkId const& blkids, void* user_ctx,
                                           homestore::ReplDev& repl_dev) {
    folly::Promise< ShardManager::Result< ShardInfo > >* promise = nullptr;
    // user_ctx will be nullptr when:
    // 1. on the follower side
    // 2. on the leader side but homeobject restarts and replay all commited log entries from the last checkpoint;
    if (user_ctx != nullptr) { promise = r_cast< folly::Promise< ShardManager::Result< ShardInfo > >* >(user_ctx); }

    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.bytes);
    if (msg_header->header_crc != msg_header->calculate_crc()) {
        LOGWARN("replication message header is corrupted with crc error, lsn:{}", lsn);
        if (promise) { promise->setValue(folly::makeUnexpected(ShardError::INVALID_ARG)); }
        return;
    }

    // read value from PBA;
    auto value_size = homestore::HomeStore::instance()->data_service().get_blk_size() * blkids.blk_count();
    auto value_buf = iomanager.iobuf_alloc(512, value_size);
    sisl::sg_list value;
    value.size = value_size;
    value.iovs.emplace_back(iovec{.iov_base = value_buf, .iov_len = value_size});

    repl_dev.async_read(blkids, value, value_size)
        .thenValue([this, header, msg_header, blkids, value_buf, value_size, promise](auto&& err) {
            if (err ||
                crc32_ieee(init_crc32, r_cast< const uint8_t* >(value_buf), value_size) != msg_header->payload_crc) {
                // read failure or read successfully but data is corrupted;
                if (promise) { promise->setValue(folly::makeUnexpected(ShardError::INVALID_ARG)); }
                iomanager.iobuf_free(value_buf);
                return;
            }

            auto shard = deserialize_shard(r_cast< const char* >(value_buf), value_size);
            switch (msg_header->msg_type) {
            case ReplicationMessageType::CREATE_SHARD_MSG: {
                shard.chunk_id = blkids.chunk_num();
                // serialize the finalized shard msg and persist the serialize result to homestore MetaBlkService;
                auto shard_msg = serialize_shard(shard);
                homestore::hs()->meta_service().add_sub_sb(HSHomeObject::s_shard_info_sub_type,
                                                           r_cast< const uint8_t* >(shard_msg.c_str()),
                                                           shard_msg.size(), shard.metablk_cookie);
                do_commit_new_shard(shard);
                break;
            }

            case ReplicationMessageType::SEAL_SHARD_MSG: {
                void* metablk_cookie = get_shard_metablk(shard.info.id);
                RELEASE_ASSERT(metablk_cookie != nullptr, "seal shard when metablk is nullptr");
                homestore::hs()->meta_service().update_sub_sb(r_cast< const uint8_t* >(value_buf), value_size,
                                                              metablk_cookie);
                shard.metablk_cookie = metablk_cookie;
                do_commit_seal_shard(shard);
                break;
            }
            default: {
                break;
            }
            }

            iomanager.iobuf_free(value_buf);
            if (promise) { promise->setValue(ShardManager::Result< ShardInfo >(shard.info)); }
        });
}

void HSHomeObject::do_commit_new_shard(const Shard& shard) {
    std::scoped_lock lock_guard(_pg_lock, _shard_lock);
    auto pg_iter = _pg_map.find(shard.info.placement_group);
    RELEASE_ASSERT(pg_iter != _pg_map.end(), "Missing PG info");
    auto& shards = pg_iter->second.shards;
    auto iter = shards.emplace(shards.end(), shard);
    auto [_, happened] = _shard_map.emplace(shard.info.id, iter);
    RELEASE_ASSERT(happened, "duplicated shard info");

    // following part give follower members a chance to catch up shard sequence num;
    auto sequence_num = get_sequence_num_from_shard_id(shard.info.id);
    if (sequence_num > pg_iter->second.shard_sequence_num) { pg_iter->second.shard_sequence_num = sequence_num; }
}

void HSHomeObject::do_commit_seal_shard(const Shard& shard) {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(shard.info.id);
    RELEASE_ASSERT(shard_iter != _shard_map.end(), "Missing shard info");
    *(shard_iter->second) = shard;
}

void* HSHomeObject::get_shard_metablk(shard_id id) const {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(id);
    if (shard_iter == _shard_map.end()) { return nullptr; }
    return (*shard_iter->second).metablk_cookie;
}

ShardManager::Result< uint16_t > HSHomeObject::get_shard_chunk(shard_id id) const {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(id);
    if (shard_iter == _shard_map.end()) { return folly::makeUnexpected(ShardError::UNKNOWN_SHARD); }
    return (*shard_iter->second).chunk_id;
}

} // namespace homeobject
