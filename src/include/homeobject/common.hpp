#pragma once

#include <expected>
#include <random>
#include <variant>

#include <boost/uuid/uuid.hpp>
#include <sisl/async/task.hpp>
#include <sisl/logging/logging.h>

SISL_LOGGING_DECL(homeobject);

#define HOMEOBJECT_LOG_MODS homeobject, blobmgr, shardmgr, gcmgr, scrubmgr

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
using snp_batch_id_t = uint16_t;
using snp_obj_id_t = uint64_t;
using trace_id_t = uint64_t;
using uuid_t = boost::uuids::uuid;

inline uint64_t generateRandomTraceId() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution< uint64_t > dis;
    return dis(gen);
}

template < class E >
class Manager {
public:
    template < typename T >
    using Result = std::expected< T, E >;
    template < typename T >
    using AsyncResult = sisl::async::task< Result< T > >;

    using NullResult = Result< std::monostate >;
    using NullAsyncResult = AsyncResult< std::monostate >;

    virtual ~Manager() = default;
};

} // namespace homeobject
