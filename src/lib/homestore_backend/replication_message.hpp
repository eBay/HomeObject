#pragma once

#include "homeobject/common.hpp"

#include <sisl/utility/enum.hpp>
#include <isa-l/crc.h>

namespace homeobject {

VENUM(ReplicationMessageType, uint16_t, CREATE_SHARD_MSG = 0, SEAL_SHARD_MSG = 1, PUT_BLOB_MSG = 2, DEL_BLOB_MSG = 3, UNKNOWN_MSG = 4);

// magic num comes from the first 8 bytes of 'echo homeobject_replication | md5sum'
static constexpr uint64_t HOMEOBJECT_REPLICATION_MAGIC = 0x11153ca24efc8d34;
static constexpr uint32_t HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1 = 0x01;
static constexpr uint32_t init_crc32 = 0;

using replication_group_id_t = pg_id;

#pragma pack(1)
struct ReplicationMessageHeader {
    uint64_t magic_num{HOMEOBJECT_REPLICATION_MAGIC};
    uint32_t protocol_version{HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1};
    replication_group_id_t repl_group_id; // replication group id upon which the msg is being replicating;
    ReplicationMessageType msg_type;    // message type
    uint32_t payload_size;
    uint32_t payload_crc;
    uint8_t reserved_pad[4]{};
    uint32_t header_crc;
    uint32_t calculate_crc() const {
        return crc32_ieee(init_crc32, r_cast< const unsigned char* >(this), sizeof(*this) - sizeof(header_crc));
    }
};

#pragma pack()

} // namespace homeobject
