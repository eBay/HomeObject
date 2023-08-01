#include "homeobject/homeobject.hpp"

#include <sisl/logging/logging.h>

#include <memory>

#include "homeobject/blob_manager.hpp"
#include "homeobject/pg_manager.hpp"
#include "homeobject/shard_manager.hpp"

using std::nullopt;

SISL_LOGGING_DECL(homeobject);

constexpr uint64_t Kb = 1024ul;
constexpr uint64_t Mb = Kb * Kb;
constexpr uint64_t Gb = Kb * Mb;

namespace homeobject {

class HomeObjectImpl : public HomeObject,
                       public BlobManager,
                       public PGManager,
                       public ShardManager,
                       public std::enable_shared_from_this< HomeObjectImpl > {
    HomeObject::lookup_cb _lookup_peer;

public:
    explicit HomeObjectImpl(HomeObject::lookup_cb const& lookup) : _lookup_peer(lookup) {}
    ~HomeObjectImpl() override = default;

    std::shared_ptr< BlobManager > blob_manager() override { return shared_from_this(); }
    std::shared_ptr< PGManager > pg_manager() override { return shared_from_this(); }
    std::shared_ptr< ShardManager > shard_manager() override { return shared_from_this(); }

    /// PgManager
    void create_pg(PGInfo const& pg_info, PGManager::ok_cb const& cb) override;
    void replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member,
                        PGManager::ok_cb const& cb) override;

    /// ShardManager
    void create_shard(pg_id pg_owner, uint64_t size_mb, ShardManager::info_cb const& cb) override;
    void list_shards(pg_id pg, ShardManager::list_cb const& cb) const override;
    void get_shard(shard_id id, ShardManager::info_cb const& cb) const override;
    void seal_shard(shard_id id, ShardManager::info_cb const& cb) override;

    /// BlobManager
    void put(shard_id shard, Blob&&, BlobManager::id_cb const& cb) override;
    void get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len,
             BlobManager::get_cb const& cb) const override;
    void del(shard_id shard, blob_id const& blob, BlobManager::ok_cb const& cb) override;
};

uint64_t ShardManager::max_shard_size_mb() { return Gb / Mb; }

std::shared_ptr< HomeObject > init_homeobject(init_params const& params) {
    LOGINFOMOD(homeobject, "Initializing HomeObject");
    return std::make_shared< HomeObjectImpl >(params.lookup);
}

void HomeObjectImpl::create_pg(PGInfo const& pg_info, PGManager::ok_cb const& cb) { cb(PGError::TIMEOUT); }

void HomeObjectImpl::replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member,
                                    PGManager::ok_cb const& cb) {
    cb(PGError::UNKNOWN_PG);
}

void HomeObjectImpl::create_shard(pg_id pg_owner, uint64_t size_mb, ShardManager::info_cb const& cb) {
    if (0 == size_mb || max_shard_size_mb() < size_mb)
        cb(ShardError::INVALID_ARG, nullopt);
    else
        cb(ShardError::UNKNOWN_PG, nullopt);
}

void HomeObjectImpl::list_shards(pg_id pg, ShardManager::list_cb const& cb) const {
    cb(ShardError::UNKNOWN_PG, nullopt);
}

void HomeObjectImpl::get_shard(shard_id id, ShardManager::info_cb const& cb) const {
    cb(ShardError::UNKNOWN_SHARD, nullopt);
}

void HomeObjectImpl::seal_shard(shard_id id, ShardManager::info_cb const& cb) {
    cb(ShardError::UNKNOWN_SHARD, nullopt);
}

void HomeObjectImpl::put(shard_id shard, Blob&&, BlobManager::id_cb const& cb) {
    cb(BlobError::UNKNOWN_SHARD, nullopt);
}

void HomeObjectImpl::get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len,
                         BlobManager::get_cb const& cb) const {
    cb(BlobError::UNKNOWN_SHARD, nullopt);
}

void HomeObjectImpl::del(shard_id shard, blob_id const& blob, BlobManager::ok_cb const& cb) {
    cb(BlobError::UNKNOWN_SHARD, nullopt);
}
} // namespace homeobject
