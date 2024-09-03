#pragma once
#include <memory>
#include <optional>
#include <string>

#include <sisl/fds/buffer.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(BlobError, uint16_t, UNKNOWN = 1, TIMEOUT, INVALID_ARG, UNSUPPORTED_OP, NOT_LEADER, REPLICATION_ERROR,
     UNKNOWN_SHARD, UNKNOWN_BLOB, CHECKSUM_MISMATCH, READ_FAILED, INDEX_ERROR, SEALED_SHARD);

struct BlobResponseBase {
    std::optional< peer_id_t > current_leader{std::nullopt};
};

struct Blob : BlobResponseBase {
    Blob() = default;
    Blob(sisl::io_blob_safe b, std::string const& u, uint64_t o) : body(std::move(b)), user_key(u), object_off(o) {}

    Blob clone() const;

    sisl::io_blob_safe body;
    std::string user_key{};
    uint64_t object_off{};
};

struct PutBlobRes : BlobResponseBase {
    blob_id_t blob_id;
};

struct DelBlobRes : BlobResponseBase {
};

class BlobManager : public Manager< BlobError > {
public:
    virtual AsyncResult< PutBlobRes > put(shard_id_t shard, Blob&&) = 0;
    virtual AsyncResult< Blob > get(shard_id_t shard, blob_id_t const& blob, uint64_t off = 0,
                                    uint64_t len = 0) const = 0;
    virtual AsyncResult< DelBlobRes > del(shard_id_t shard, blob_id_t const& blob) = 0;
};

} // namespace homeobject