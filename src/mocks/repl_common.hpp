#pragma once

#include <string>

#include <boost/uuid/uuid.hpp>
#include <folly/small_vector.h>
#include <sisl/logging/logging.h>

SISL_LOGGING_DECL(home_replication)

namespace home_replication {

using boost::uuids::uuid;

using pba_t = uint64_t;
using pba_list_t = folly::small_vector< pba_t, 4 >;

} // namespace home_replication
