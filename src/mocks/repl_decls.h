#pragma once
#include <iostream>
#include <string>

#include <folly/small_vector.h>
#include <sisl/logging/logging.h>
#include <iomgr/iomgr_types.hpp>
#include <homestore/homestore_decl.hpp>
#include <sisl/fds/buffer.hpp>

SISL_LOGGING_DECL(home_replication)

#define HOMEREPL_LOG_MODS grpc_server, HOMESTORE_LOG_MODS, nuraft_mesg, nuraft, home_replication

namespace home_replication {
using pba_t = uint64_t;
using pba_list_t = folly::small_vector< pba_t, 4 >;

// Fully qualified domain pba, unique pba id across replica set
struct fully_qualified_pba {
    fully_qualified_pba(uint32_t s, pba_t p, uint32_t sz) : server_id{s}, pba{p}, size{sz} {}
    uint32_t server_id;
    pba_t pba;
    uint32_t size; // corresponding size of this pba;
    std::string to_key_string() const { return fmt::format("{}_{}", std::to_string(server_id), std::to_string(pba)); }
};
using fq_pba_list_t = folly::small_vector< fully_qualified_pba, 4 >;

// data service api names
static std::string const SEND_DATA{"send_data"};
static std::string const FETCH_DATA{"fetch_data"};

} // namespace home_replication
