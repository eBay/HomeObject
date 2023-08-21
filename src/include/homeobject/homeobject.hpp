#pragma once
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <folly/futures/Future.h>

#include "common.hpp"

namespace homeobject {

class BlobManager;
class PGManager;
class ShardManager;

using endpoint = std::string;

class HomeObject {
public:
    using svc_id_cb = std::function< folly::SemiFuture< peer_id >(std::optional< peer_id > const& found) >;
    using lookup_cb = std::function< folly::SemiFuture< endpoint >(peer_id const&) >;
    struct init_params {
        svc_id_cb get_svc_id; // Callback made after determining if a SvcId exists or not (optional arg)
        lookup_cb lookup;     // When RAFT operations take place, we must map the SvcId to a gethostbyaddr() value (IP)
    };

    virtual ~HomeObject() = default;
    virtual peer_id our_uuid() const = 0;
    virtual std::shared_ptr< BlobManager > blob_manager() = 0;
    virtual std::shared_ptr< PGManager > pg_manager() = 0;
    virtual std::shared_ptr< ShardManager > shard_manager() = 0;
};

extern std::shared_ptr< HomeObject > init_homeobject(HomeObject::init_params&& params);

} // namespace homeobject
