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

            LOGINFO("Index table created for pg {} uuid {}", pg_id, uuid_str);
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
    homestore::superblk< pg_info_superblk > pg_sb;
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

            // check if any shard recovery is pending by this pg;
            auto iter = pending_recovery_shards_.find(pg_sb->id);
            if (iter != pending_recovery_shards_.end()) {
                for (auto& sb : iter->second) {
                    add_new_shard_to_map(std::make_unique< HS_Shard >(sb));
                }
                pending_recovery_shards_.erase(iter);
            }

            // During PG recovery check if index is already recoverd else
            // add entry in map, so that index recovery can update the PG.
            std::scoped_lock lg(index_lock_);
            auto uuid_str = boost::uuids::to_string(pg_sb->index_table_uuid);
            auto it = index_table_pg_map_.find(uuid_str);
            if (it != index_table_pg_map_.end()) {
                hs_pg->index_table_ = it->second.index_table;
                it->second.pg_id = pg_sb->id;
            } else {
                index_table_pg_map_.emplace(uuid_str, PgIndexTable{pg_sb->id, nullptr});
            }

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
        PG{std::move(info)}, pg_sb_{"PGManager"}, repl_dev_{std::move(rdev)}, index_table_(index_table) {
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
        PG{pg_info_from_sb(sb)}, pg_sb_{sb}, repl_dev_{std::move(rdev)} {}

} // namespace homeobject
