#pragma once
#include <boost/uuid/uuid.hpp>

namespace homeobject {

using blob_id = uint64_t;
using peer_id = boost::uuids::uuid;
using pg_id = uint16_t;
using shard_id = uint64_t;

} // namespace homeobject
