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

ShardManager::AsyncResult< ShardInfo > HSHomeObject::_create_shard(pg_id_t pg_owner, uint64_t size_bytes) {
    shared< homestore::ReplDev > repl_dev;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_owner);
        if (iter == _pg_map.end()) {
            LOGW("failed to create shard with non-exist pg [{}]", pg_owner);
            return folly::makeUnexpected(ShardError::UNKNOWN_PG);
        }
        repl_dev = static_cast< HS_PG* >(iter->second.get())->repl_dev_;
    }

    if (!repl_dev) {
        LOGW("failed to get repl dev instance for pg [{}]", pg_owner);
        return folly::makeUnexpected(ShardError::PG_NOT_READY);
    }

    auto new_shard_id = generate_new_shard_id(pg_owner);
    auto create_time = get_current_timestamp();

    // Prepare the shard info block
    sisl::io_blob_safe sb_blob(sisl::round_up(sizeof(shard_info_superblk), repl_dev->get_blk_size()), io_align);
    shard_info_superblk* sb = new (sb_blob.bytes()) shard_info_superblk();
    sb->type = DataHeader::data_type_t::SHARD_INFO;
    sb->info = ShardInfo{.id = new_shard_id,
                         .placement_group = pg_owner,
                         .state = ShardInfo::State::OPEN,
                         .created_time = create_time,
                         .last_modified_time = create_time,
                         .available_capacity_bytes = size_bytes,
                         .total_capacity_bytes = size_bytes,
                         .deleted_capacity_bytes = 0};
    sb->chunk_id = 0;

    auto req = repl_result_ctx< ShardManager::Result< ShardInfo > >::make(
        sizeof(shard_info_superblk) /* header_extn_size */, 0u /* key_size */);

    // preapre msg header;
    req->header()->msg_type = ReplicationMessageType::CREATE_SHARD_MSG;
    req->header()->pg_id = pg_owner;
    req->header()->shard_id = new_shard_id;
    req->header()->payload_size = sizeof(shard_info_superblk);
    req->header()->payload_crc = crc32_ieee(init_crc32, sb_blob.cbytes(), sizeof(shard_info_superblk));
    req->header()->seal();

    // ShardInfo block is persisted on both on header and in data portion.
    // It is persisted in header portion, so that it is written in journal and hence replay of journal on most cases
    // doesn't need additional read from data blks.
    // We also persist in data blocks for following reasons:
    //   * To recover the shard information in case both journal and metablk are lost
    //   * For garbage collection, we directly read from the data chunk and get shard information.
    std::memcpy(req->header_extn(), sb_blob.cbytes(), sizeof(shard_info_superblk));
    req->add_data_sg(std::move(sb_blob));

    // replicate this create shard message to PG members;
    repl_dev->async_alloc_write(req->cheader_buf(), sisl::blob{}, req->data_sgs(), req);
    return req->result();
}

ShardManager::AsyncResult< ShardInfo > HSHomeObject::_seal_shard(ShardInfo const& info) {
    auto pg_id = info.placement_group;
    auto shard_id = info.id;

    shared< homestore::ReplDev > repl_dev;
    {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pg_id);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        repl_dev = static_cast< HS_PG* >(iter->second.get())->repl_dev_;
        RELEASE_ASSERT(repl_dev != nullptr, "Repl dev null");
    }

    ShardInfo tmp_info = info;
    tmp_info.state = ShardInfo::State::SEALED;

    // Prepare the shard info block
    sisl::io_blob_safe sb_blob(sisl::round_up(sizeof(shard_info_superblk), repl_dev->get_blk_size()), io_align);
    shard_info_superblk* sb = new (sb_blob.bytes()) shard_info_superblk();
    sb->type = DataHeader::data_type_t::SHARD_INFO;
    sb->info = tmp_info;
    sb->chunk_id = 0;

    auto req = repl_result_ctx< ShardManager::Result< ShardInfo > >::make(
        sizeof(shard_info_superblk) /* header_extn_size */, 0u /* key_size */);
    req->header()->msg_type = ReplicationMessageType::SEAL_SHARD_MSG;
    req->header()->pg_id = pg_id;
    req->header()->shard_id = shard_id;
    req->header()->payload_size = sizeof(shard_info_superblk);
    req->header()->payload_crc = crc32_ieee(init_crc32, sb_blob.cbytes(), sizeof(shard_info_superblk));
    req->header()->seal();

    // Similar to create shard - ShardInfo block is persisted on both on header and in data portion.
    std::memcpy(req->header_extn(), sb_blob.cbytes(), sizeof(shard_info_superblk));
    req->add_data_sg(std::move(sb_blob));

    // replicate this seal shard message to PG members;
    repl_dev->async_alloc_write(req->cheader_buf(), sisl::blob{}, req->data_sgs(), req);
    return req->result();
}

void HSHomeObject::on_shard_message_commit(int64_t lsn, sisl::blob const& h, homestore::MultiBlkId const& blkids,
                                           shared< homestore::ReplDev > repl_dev,
                                           cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< ShardManager::Result< ShardInfo > >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer) {
        ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(hs_ctx).get();
    }

    auto header = r_cast< const ReplicationMessageHeader* >(h.cbytes());
    if (header->corrupted()) {
        LOGW("replication message header is corrupted with crc error, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError::CRC_MISMATCH)); }
        return;
    }

#ifdef VADLIDATE_ON_REPLAY
    sisl::io_blob_safe value_blob(blkids.blk_count() * repl_dev->get_blk_size(), io_align);
    sisl::sg_list value_sgs;
    value_sgs.iovs.emplace_back(iovec{.iov_base = value_blob.bytes(), .iov_len = value_blob.size()});
    value_sgs.size += value_blob.size();

    // Do a read sync read and validate the crc
    std::error_code err = repl_dev->async_read(blkids, value_sgs, value_blob.size()).get();
    if (err) {
        LOGW("failed to read data from homestore blks, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError::UNKNOWN)); }
        return;
    }

    if (crc32_ieee(init_crc32, value.cbytes(), value.size()) != header->payload_crc) {
        // header & value is inconsistent;
        LOGW("replication message header is inconsistent with value, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError::CRC_MISMATCH)); }
        return;
    }
#endif

    switch (header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        auto sb = r_cast< shard_info_superblk const* >(h.cbytes() + sizeof(ReplicationMessageHeader));
        auto const shard_info = sb->info;

        bool shard_exist = false;
        {
            std::scoped_lock lock_guard(_shard_lock);
            shard_exist = (_shard_map.find(shard_info.id) != _shard_map.end());
        }

        if (!shard_exist) {
            add_new_shard_to_map(std::make_unique< HS_Shard >(std::move(shard_info), blkids.chunk_num()));
            // select_specific_chunk() will do something only when we are relaying journal after restart, during the
            // runtime flow chunk is already been be mark busy when we write the shard info to the repldev.
            chunk_selector_->select_specific_chunk(blkids.chunk_num());
        }
        if (ctx) { ctx->promise_.setValue(ShardManager::Result< ShardInfo >(shard_info)); }
        break;
    }

    case ReplicationMessageType::SEAL_SHARD_MSG: {
        auto sb = r_cast< shard_info_superblk const* >(h.cbytes() + sizeof(ReplicationMessageHeader));
        auto const shard_info = sb->info;

        ShardInfo::State state;
        {
            std::scoped_lock lock_guard(_shard_lock);
            auto iter = _shard_map.find(shard_info.id);
            RELEASE_ASSERT(iter != _shard_map.end(), "Missing shard info");
            state = (*iter->second)->info.state;
        }

        if (state == ShardInfo::State::OPEN) {
            auto chunk_id = get_shard_chunk(shard_info.id);
            RELEASE_ASSERT(chunk_id.has_value(), "Chunk id not found");
            chunk_selector()->release_chunk(chunk_id.value());
            update_shard_in_map(shard_info);
        }
        if (ctx) { ctx->promise_.setValue(ShardManager::Result< ShardInfo >(shard_info)); }
        break;
    }
    default:
        break;
    }
}

#if 0
void HSHomeObject::do_shard_message_commit(int64_t lsn, ReplicationMessageHeader const& header,
                                           homestore::MultiBlkId const& blkids, sisl::blob value,
                                           cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    repl_result_ctx< ShardManager::Result< ShardInfo > >* ctx{nullptr};
    if (hs_ctx && hs_ctx->is_proposer) {
        ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(hs_ctx).get();
    }

    if (header.corrupted()) {
        LOGW("replication message header is corrupted with crc error, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError::CRC_MISMATCH)); }
        return;
    }

    if (crc32_ieee(init_crc32, value.cbytes(), value.size()) != header.payload_crc) {
        // header & value is inconsistent;
        LOGW("replication message header is inconsistent with value, lsn:{}", lsn);
        if (ctx) { ctx->promise_.setValue(folly::makeUnexpected(ShardError::CRC_MISMATCH)); }
        return;
    }

    auto shard_info = deserialize_shard_info(r_cast< const char* >(value.cbytes()), value.size());
    switch (header.msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        bool shard_exist = false;
        {
            std::scoped_lock lock_guard(_shard_lock);
            shard_exist = (_shard_map.find(shard_info.id) != _shard_map.end());
        }

        if (!shard_exist) {
            add_new_shard_to_map(std::make_unique< HS_Shard >(shard_info, blkids.chunk_num()));
            // select_specific_chunk() will do something only when we are relaying journal after restart, during the
            // runtime flow chunk is already been be mark busy when we write the shard info to the repldev.
            chunk_selector_->select_specific_chunk(blkids.chunk_num());
        }

        break;
    }

    case ReplicationMessageType::SEAL_SHARD_MSG: {
        ShardInfo::State state;
        {
            std::scoped_lock lock_guard(_shard_lock);
            auto iter = _shard_map.find(shard_info.id);
            RELEASE_ASSERT(iter != _shard_map.end(), "Missing shard info");
            state = (*iter->second)->info.state;
        }

        if (state == ShardInfo::State::OPEN) {
            auto chunk_id = get_shard_chunk(shard_info.id);
            RELEASE_ASSERT(chunk_id.has_value(), "Chunk id not found");
            chunk_selector()->release_chunk(chunk_id.value());
            update_shard_in_map(shard_info);
        }

        break;
    }
    default: {
        break;
    }
    }

    if (ctx) { ctx->promise_.setValue(ShardManager::Result< ShardInfo >(shard_info)); }
}
#endif

void HSHomeObject::add_new_shard_to_map(ShardPtr&& shard) {
    // TODO: We are taking a global lock for all pgs to create shard. Is it really needed??
    // We need to have fine grained per PG lock and take only that.
    std::scoped_lock lock_guard(_pg_lock, _shard_lock);
    auto pg_iter = _pg_map.find(shard->info.placement_group);
    RELEASE_ASSERT(pg_iter != _pg_map.end(), "Missing PG info");
    auto& shards = pg_iter->second->shards_;
    auto shard_id = shard->info.id;
    auto iter = shards.emplace(shards.end(), std::move(shard));
    auto [_, happened] = _shard_map.emplace(shard_id, iter);
    RELEASE_ASSERT(happened, "duplicated shard info");

    // following part gives follower members a chance to catch up shard sequence num;
    auto sequence_num = get_sequence_num_from_shard_id(shard_id);
    if (sequence_num > pg_iter->second->shard_sequence_num_) { pg_iter->second->shard_sequence_num_ = sequence_num; }
}

void HSHomeObject::update_shard_in_map(const ShardInfo& shard_info) {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(shard_info.id);
    RELEASE_ASSERT(shard_iter != _shard_map.end(), "Missing shard info");
    auto hs_shard = d_cast< HS_Shard* >((*shard_iter->second).get());
    hs_shard->update_info(shard_info);
}

std::optional< homestore::chunk_num_t > HSHomeObject::get_shard_chunk(shard_id_t id) const {
    std::scoped_lock lock_guard(_shard_lock);
    auto shard_iter = _shard_map.find(id);
    if (shard_iter == _shard_map.end()) { return std::nullopt; }
    auto hs_shard = d_cast< HS_Shard* >((*shard_iter->second).get());
    return std::make_optional< homestore::chunk_num_t >(hs_shard->sb_->chunk_id);
}

std::optional< homestore::chunk_num_t > HSHomeObject::get_any_chunk_id(pg_id_t const pg_id) {
    std::scoped_lock lock_guard(_pg_lock);
    auto pg_iter = _pg_map.find(pg_id);
    RELEASE_ASSERT(pg_iter != _pg_map.end(), "Missing PG info");
    HS_PG* pg = static_cast< HS_PG* >(pg_iter->second.get());
    if (pg->any_allocated_chunk_id_.has_value()) {
        // it is already cached and use it;
        return pg->any_allocated_chunk_id_;
    }

    auto& shards = pg->shards_;
    if (shards.empty()) { return std::nullopt; }
    auto hs_shard = d_cast< HS_Shard* >(shards.front().get());
    // cache it;
    pg->any_allocated_chunk_id_ = hs_shard->sb_->chunk_id;
    return pg->any_allocated_chunk_id_;
}

HSHomeObject::HS_Shard::HS_Shard(ShardInfo shard_info, homestore::chunk_num_t chunk_id) :
        Shard(std::move(shard_info)), sb_(_shard_meta_name) {
    sb_.create(sizeof(shard_info_superblk));
    sb_->info = info;
    sb_->chunk_id = chunk_id;
    sb_.write();
}

HSHomeObject::HS_Shard::HS_Shard(homestore::superblk< shard_info_superblk >&& sb) :
        Shard(sb->info), sb_(std::move(sb)) {}

void HSHomeObject::HS_Shard::update_info(const ShardInfo& shard_info) {
    info = shard_info;
    sb_->info = info;
    sb_.write();
}

} // namespace homeobject
