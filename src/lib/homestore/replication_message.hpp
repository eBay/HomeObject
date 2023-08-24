#pragma once

#include <sisl/utility/enum.hpp>
#include <isa-l/crc.h>

namespace homeobject {

ENUM(ReplicationMessageType, uint16_t, PG_MESSAGE = 0, SHARD_MESSAGE, BLOB_MESSAGE, UNKNOWN_MESSAGE);

// magic num comes from the first 8 bytes of 'echo homeobject_replication | md5sum'
static constexpr uint64_t HOMEOBJECT_REPLICATION_MAGIC = 0x11153ca24efc8d34;

struct ReplicationMessageHeader {
    uint64_t magic_num{HOMEOBJECT_REPLICATION_MAGIC};
    ReplicationMessageType message_type;
    uint32_t message_size;
    uint32_t message_crc;
    uint8_t reserved_pad[2]{};
    uint32_t header_crc;
    uint32_t calculate_crc() const {
        return crc32_ieee(0, r_cast<const unsigned char*>(this), sizeof(*this) - sizeof(header_crc));
    }
};


}
