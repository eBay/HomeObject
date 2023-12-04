#include <boost/uuid/random_generator.hpp>
#include <homestore/replication_service.hpp>
#include "hs_homeobject.hpp"
#include "replication_state_machine.hpp"

using namespace homestore;
namespace homeobject {

PGError toPgError(ReplServiceError const& e) {
    switch (e) {
    case ReplServiceError::BAD_REQUEST:
        [[fallthrough]];
    case ReplServiceError::CANCELLED:
        [[fallthrough]];
    case ReplServiceError::CONFIG_CHANGING:
        [[fallthrough]];
    case ReplServiceError::SERVER_ALREADY_EXISTS:
        [[fallthrough]];
    case ReplServiceError::SERVER_IS_JOINING:
        [[fallthrough]];
    case ReplServiceError::SERVER_IS_LEAVING:
        [[fallthrough]];
    case ReplServiceError::RESULT_NOT_EXIST_YET:
        [[fallthrough]];
    case ReplServiceError::NOT_LEADER:
        [[fallthrough]];
    case ReplServiceError::TERM_MISMATCH:
    case ReplServiceError::NOT_IMPLEMENTED:
        return PGError::INVALID_ARG;
    case ReplServiceError::CANNOT_REMOVE_LEADER:
        return PGError::UNKNOWN_PEER;
    case ReplServiceError::TIMEOUT:
        return PGError::TIMEOUT;
    case ReplServiceError::SERVER_NOT_FOUND:
        return PGError::UNKNOWN_PG;
    case ReplServiceError::OK:
        DEBUG_ASSERT(false, "Should not process OK!");
        [[fallthrough]];
    case ReplServiceError::FAILED:
        return PGError::UNKNOWN;
    }
    return PGError::UNKNOWN;
}

[[maybe_unused]] static homestore::ReplDev& pg_repl_dev(PG const& pg) {
    return *(static_cast< HSHomeObject::HS_PG const& >(pg).repl_dev_);
}

PGManager::NullAsyncResult HSHomeObject::_create_pg(PGInfo&& pg_info, std::set< std::string, std::less<> > peers) {
    pg_info.replica_set_uuid = boost::uuids::random_generator()();
    return hs_repl_service()
        .create_repl_dev(pg_info.replica_set_uuid, std::move(peers), std::make_unique< ReplicationStateMachine >(this))
        .thenValue([this, pg_info = std::move(pg_info)](auto&& v) mutable -> PGManager::NullResult {
            if (v.hasError()) { return folly::makeUnexpected(toPgError(v.error())); }

            // TODO create index table during create shard.
            auto index_table = create_index_table();
            auto uuid_str = boost::uuids::to_string(index_table->uuid());

            auto pg_id = pg_info.id;
            auto hs_pg = std::make_unique< HS_PG >(std::move(pg_info), std::move(v.value()), index_table);
            std::scoped_lock lock_guard(index_lock_);
            RELEASE_ASSERT(index_table_pg_map_.count(uuid_str) == 0, "duplicate index table found");
            index_table_pg_map_[uuid_str] = PgIndexTable{pg_id, index_table};

            LOGI("Index table created for pg {} uuid {}", pg_id, uuid_str);
            hs_pg->index_table_ = index_table;
            // Add to index service, so that it gets cleaned up when index service is shutdown.
            homestore::hs()->index_service().add_index_table(index_table);
            add_pg_to_map(std::move(hs_pg));
            return folly::Unit();
        });
}

PGManager::NullAsyncResult HSHomeObject::_replace_member(pg_id_t id, peer_id_t const& old_member,
                                                         PGMember const& new_member) {
    return folly::makeSemiFuture< PGManager::NullResult >(folly::makeUnexpected(PGError::UNSUPPORTED_OP));
}

void HSHomeObject::add_pg_to_map(unique< HS_PG > hs_pg) {
    RELEASE_ASSERT(hs_pg->pg_info_.replica_set_uuid == hs_pg->repl_dev_->group_id(),
                   "PGInfo replica set uuid mismatch with ReplDev instance for {}",
                   boost::uuids::to_string(hs_pg->pg_info_.replica_set_uuid));
    auto lg = std::scoped_lock(_pg_lock);
    auto id = hs_pg->pg_info_.id;
    auto [it1, _] = _pg_map.try_emplace(id, std::move(hs_pg));
    RELEASE_ASSERT(_pg_map.end() != it1, "Unknown map insert error!");
}

#if 0
std::string HSHomeObject::serialize_pg_info(PGInfo const& pginfo) {
    nlohmann::json j;
    j["pg_info"]["pg_id_t"] = pginfo.id;
    j["pg_info"]["repl_uuid"] = boost::uuids::to_string(pginfo.replica_set_uuid);

    nlohmann::json members_j = {};
    for (auto const& member : pginfo.members) {
        nlohmann::json member_j;
        member_j["member_id"] = member.id;
        member_j["name"] = member.name;
        member_j["priority"] = member.priority;
        members_j.push_back(member_j);
    }
    j["pg_info"]["members"] = members_j;
    return j.dump();
}

PGInfo HSHomeObject::deserialize_pg_info(std::string const& json_str) {
    auto pg_json = nlohmann::json::parse(json_str);

    PGInfo pg_info;
    pg_info.id = pg_json["pg_info"]["pg_id_t"].get< pg_id_t >();
    pg_info.replica_set_uuid = boost::uuids::string_generator()(pg_json["pg_info"]["repl_uuid"].get< std::string >());

    for (auto const& m : pg_info["pg_info"]["members"]) {
        PGMember member;
        member.id = m["member_id"].get< pg_id_t >();
        member.name = m["name"].get< std::string >();
        member.priority = m["priority"].get< int32_t >();
        pg_info.members.emplace(std::move(member));
    }
    return pg_info;
}
#endif

void HSHomeObject::on_pg_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie) {
    homestore::superblk< pg_info_superblk > pg_sb(_pg_meta_name);
    pg_sb.load(buf, meta_cookie);

    hs_repl_service()
        .open_repl_dev(pg_sb->replica_set_uuid, std::make_unique< ReplicationStateMachine >(this))
        .thenValue([this, pg_sb = std::move(pg_sb)](auto&& v) {
            if (v.hasError()) {
                // TODO: We need to raise an alert here, since without pg repl_dev all operations on that pg will fail
                LOGE("open_repl_dev for group_id={} has failed", boost::uuids::to_string(pg_sb->replica_set_uuid));
                return;
            }

            auto hs_pg = std::make_unique< HS_PG >(pg_sb, std::move(v.value()));
            // During PG recovery check if index is already recoverd else
            // add entry in map, so that index recovery can update the PG.
            std::scoped_lock lg(index_lock_);
            auto uuid_str = boost::uuids::to_string(pg_sb->index_table_uuid);
            auto it = index_table_pg_map_.find(uuid_str);
            RELEASE_ASSERT(it != index_table_pg_map_.end(), "IndexTable should be recovered before PG");
            hs_pg->index_table_ = it->second.index_table;
            it->second.pg_id = pg_sb->id;

            add_pg_to_map(std::move(hs_pg));
        });
}

PGInfo HSHomeObject::HS_PG::pg_info_from_sb(homestore::superblk< pg_info_superblk > const& sb) {
    PGInfo pginfo{sb->id};
    for (uint32_t i{0}; i < sb->num_members; ++i) {
        pginfo.members.emplace(sb->members[i].id, std::string(sb->members[i].name), sb->members[i].priority);
    }
    pginfo.replica_set_uuid = sb->replica_set_uuid;
    return pginfo;
}

HSHomeObject::HS_PG::HS_PG(PGInfo info, shared< homestore::ReplDev > rdev, shared< BlobIndexTable > index_table) :
        PG{std::move(info)}, pg_sb_{_pg_meta_name}, repl_dev_{std::move(rdev)}, index_table_(index_table) {
    pg_sb_.create(sizeof(pg_info_superblk) + ((pg_info_.members.size() - 1) * sizeof(pg_members)));
    pg_sb_->id = pg_info_.id;
    pg_sb_->num_members = pg_info_.members.size();
    pg_sb_->replica_set_uuid = repl_dev_->group_id();
    pg_sb_->index_table_uuid = index_table_->uuid();

    uint32_t i{0};
    for (auto const& m : pg_info_.members) {
        pg_sb_->members[i].id = m.id;
        std::strncpy(pg_sb_->members[i].name, m.name.c_str(), std::min(m.name.size(), pg_members::max_name_len));
        pg_sb_->members[i].priority = m.priority;
        ++i;
    }

    pg_sb_.write();
}

HSHomeObject::HS_PG::HS_PG(homestore::superblk< HSHomeObject::pg_info_superblk > const& sb,
                           shared< homestore::ReplDev > rdev) :
        PG{pg_info_from_sb(sb)}, pg_sb_{sb}, repl_dev_{std::move(rdev)} {
    blob_sequence_num_ = sb->blob_sequence_num;
}

uint32_t HSHomeObject::HS_PG::total_shards() const { return shards_.size(); }

uint32_t HSHomeObject::HS_PG::open_shards() const {
    return std::count_if(shards_.begin(), shards_.end(), [](auto const& s) { return s->is_open(); });
}

std::optional< uint32_t > HSHomeObject::HS_PG::dev_hint(cshared< HeapChunkSelector > chunk_sel) const {
    if (shards_.empty()) { return std::nullopt; }
    auto const hs_shard = d_cast< HS_Shard* >(shards_.front().get());
    auto const hint = chunk_sel->chunk_to_hints(hs_shard->chunk_id());
    return hint.pdev_id_hint;
}

void HSHomeObject::persist_pg_sb() {
    auto lg = std::shared_lock(_pg_lock);
    for (auto& [_, pg] : _pg_map) {
        auto hs_pg = static_cast< HS_PG* >(pg.get());
        hs_pg->pg_sb_->blob_sequence_num = hs_pg->blob_sequence_num_;
        hs_pg->pg_sb_.write();
    }
}

bool HSHomeObject::_get_stats(pg_id_t id, PGStats& stats) const {
    auto lg = std::shared_lock(_pg_lock);
    auto it = _pg_map.find(id);
    if (_pg_map.end() == it) { return false; }
    auto const& pg = it->second;
    auto hs_pg = static_cast< HS_PG* >(pg.get());
    auto const blk_size = hs_pg->repl_dev_->get_blk_size();

    stats.id = hs_pg->pg_info_.id;
    stats.replica_set_uuid = hs_pg->pg_info_.replica_set_uuid;
    stats.num_members = hs_pg->pg_info_.members.size();
    stats.total_shards = hs_pg->total_shards();
    stats.open_shards = hs_pg->open_shards();

    auto const pdev_id_hint = hs_pg->dev_hint(chunk_selector());
    if (pdev_id_hint.has_value()) {
        auto total_chunks_on_dev = 0ul;
        auto open_shards_on_dev = 0ul; // one open shard maps to one unique chunk;

        // get all the open shards from all PGs on the same drive;
        for (auto& [_, pg] : _pg_map) {
            // multiple PGs could be on same device;
            auto cur_hs_pg = static_cast< HS_PG* >(pg.get());
            auto const& cur_pdev_id_hint = cur_hs_pg->dev_hint(chunk_selector());
            if (cur_pdev_id_hint.has_value() && cur_pdev_id_hint.value() == pdev_id_hint.value()) {
                // open shards will never be on same chunk;
                open_shards_on_dev += cur_hs_pg->open_shards();
            }
        }

        // get number of total chunks on the this drive;
        chunk_selector()->foreach_chunks([this, &pdev_id_hint, &total_chunks_on_dev](auto const& chunk) {
            VChunk const& vchunk(chunk);
            if (vchunk.get_pdev_id() == pdev_id_hint) { ++total_chunks_on_dev; }
        });

        stats.avail_open_shards = total_chunks_on_dev - open_shards_on_dev;

        stats.avail_bytes = chunk_selector()->avail_blks(pdev_id_hint) * blk_size;

        stats.used_bytes = chunk_selector()->total_blks(pdev_id_hint.value()) * blk_size - stats.avail_bytes;
    } else {
        // if no shard has been created on this PG yet, it means this PG could arrive on any drive that has the most
        // available open shards;
        stats.avail_open_shards = chunk_selector()->most_available_num_chunks();

        // if no shards is created yet on this PG, set used bytes to zero;
        stats.used_bytes = 0ul;

        // if no shard has been created on this PG yet, it means this PG could arrive on any drive that has the most
        // available space;
        stats.avail_bytes = chunk_selector()->avail_blks(std::nullopt) * blk_size;
    }

    return true;
}

void HSHomeObject::_get_pg_ids(std::vector< pg_id_t >& pg_ids) const {
    {
        auto lg = std::shared_lock(_pg_lock);
        for (auto& [id, _] : _pg_map) {
            pg_ids.push_back(id);
        }
    }
}
} // namespace homeobject
