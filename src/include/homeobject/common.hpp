#pragma once
#include <boost/uuid/uuid.hpp>
#include <sisl/logging/logging.h>

/// Anything that links libhomestore requires these common symbols to be included in each
/// DSO; so just include it here for all.
#include <home_replication/repl_decls.h>

SISL_LOGGING_DECL(homeobject);

#define HOMEOBJECT_LOG_MODS HOMEREPL_LOG_MODS, homeobject

#ifndef Ki
constexpr uint64_t Ki = 1024ul;
#endif
#ifndef Mi
constexpr uint64_t Mi = Ki * Ki;
#endif
#ifndef Gi
constexpr uint64_t Gi = Ki * Mi;
#endif

namespace homeobject {

using blob_id = uint64_t;
using peer_id = boost::uuids::uuid;
using pg_id = uint16_t;
using shard_id = uint64_t;

} // namespace homeobject
