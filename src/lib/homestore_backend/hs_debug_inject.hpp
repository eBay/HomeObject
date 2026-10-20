#pragma once

#include <folly/Expected.h>
#include <folly/Unit.h>
#include <string>

#include "homeobject/common.hpp"

namespace homeobject {

class HSHomeObject;

namespace hs_debug_inject {

using InjectResult = folly::Expected< folly::Unit, std::string >;

// Deletes a single blob's entry from its PG's local B-tree index, simulating a
// missing blob on this replica without going through the replication layer.
InjectResult delete_blob_from_index(HSHomeObject& ho, pg_id_t pg_id, shard_id_t shard_id, blob_id_t blob_id);

// Deletes all blob entries for a shard from its PG's local B-tree index,
// simulating a missing shard on this replica.
InjectResult delete_shard_from_index(HSHomeObject& ho, pg_id_t pg_id, shard_id_t shard_id);

// Flips bits in the first half of a blob's on-disk data, simulating silent
// bit-rot. The blob's stored hash is left untouched, so scrub should report MISMATCH.
InjectResult corrupt_blob_data(HSHomeObject& ho, pg_id_t pg_id, shard_id_t shard_id, blob_id_t blob_id);

// Flips bits in a blob's on-disk data and recomputes a self-consistent hash,
// simulating a replica whose copy differs from its peers but passes its own
// integrity check.
InjectResult make_blob_inconsistent(HSHomeObject& ho, pg_id_t pg_id, shard_id_t shard_id, blob_id_t blob_id);

} // namespace hs_debug_inject
} // namespace homeobject
