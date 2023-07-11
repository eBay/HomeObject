#pragma once
#include <functional>
#include <memory>
#include <variant>
#include <vector>

#include "common.hpp"

namespace homeobject {

class BlobManager;
class PGManager;
class ShardManager;

using endpoint = std::string;

class HomeObject {
public:
    using lookup_cb = std::function< endpoint(peer_id const&) >;

    virtual ~HomeObject() = default;
    virtual std::shared_ptr< BlobManager > blob_manager() = 0;
    virtual std::shared_ptr< PGManager > pg_manager() = 0;
    virtual std::shared_ptr< ShardManager > shard_manager() = 0;
};

extern std::shared_ptr< HomeObject > init_homeobject(HomeObject::lookup_cb);

} // namespace homeobject
