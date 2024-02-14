#pragma once

#include <boost/uuid/uuid.hpp>
#include <folly/Expected.h>
#include <folly/Unit.h>
#include <folly/futures/Future.h>

#include <sisl/logging/logging.h>

SISL_LOGGING_DECL(homeobject);

#define HOMEOBJECT_LOG_MODS homeobject, blobmgr

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

using blob_id_t = uint64_t;

using peer_id_t = boost::uuids::uuid;
using pg_id_t = uint16_t;
using shard_id_t = uint64_t;

template < class E >
class Manager {
public:
    template < typename T >
    using Result = folly::Expected< T, E >;
    template < typename T >
    using AsyncResult = folly::SemiFuture< Result< T > >;

    using NullResult = Result< folly::Unit >;
    using NullAsyncResult = AsyncResult< folly::Unit >;

    virtual ~Manager() = default;
};

} // namespace homeobject
