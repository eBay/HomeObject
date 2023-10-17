#pragma once

#include "homeobject/common.hpp"

#include <sisl/utility/enum.hpp>
#include <isa-l/crc.h>

namespace homeobject {

VENUM(ReplicationMessageType, uint16_t, CREATE_SHARD_MSG = 0, SEAL_SHARD_MSG = 1, PUT_BLOB_MSG = 2, DEL_BLOB_MSG = 3,
      UNKNOWN_MSG = 4);

// magic num comes from the first 8 bytes of 'echo homeobject_replication | md5sum'
static constexpr uint64_t HOMEOBJECT_REPLICATION_MAGIC = 0x11153ca24efc8d34;
static constexpr uint32_t HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1 = 0x01;
static constexpr uint32_t init_crc32 = 0;

#pragma pack(1)
struct ReplicationMessageHeader {
    uint64_t magic_num{HOMEOBJECT_REPLICATION_MAGIC};
    uint32_t protocol_version{HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1};
    ReplicationMessageType msg_type; // message type
    pg_id_t pg_id;
    shard_id_t shard_id;
    uint32_t payload_size;
    uint32_t payload_crc;
    uint8_t reserved_pad[4]{};
    uint32_t header_crc;
    void seal() {
        header_crc = 0;
        header_crc = calculate_crc();
    }

    bool corrupted() {
        if (magic_num != HOMEOBJECT_REPLICATION_MAGIC ||
            protocol_version != HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1) {
            return true;
        }

        auto saved_crc = header_crc;
        header_crc = 0;
        bool corrupted = (saved_crc != calculate_crc());
        header_crc = saved_crc;
        return corrupted;
    }

    uint32_t calculate_crc() const {
        return crc32_ieee(init_crc32, r_cast< const unsigned char* >(this), sizeof(*this));
    }
};

#pragma pack()

} // namespace homeobject
