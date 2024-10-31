#pragma once

#include "homeobject/common.hpp"

#include <sisl/utility/enum.hpp>
#include <sisl/fds/utils.hpp>

#include <homestore/crc.h>

namespace homeobject {

VENUM(ReplicationMessageType, uint16_t, CREATE_PG_MSG = 0, CREATE_SHARD_MSG = 1, SEAL_SHARD_MSG = 2, PUT_BLOB_MSG = 3,
      DEL_BLOB_MSG = 4, UNKNOWN_MSG = 5);
VENUM(SyncMessageType, uint16_t, PG_META = 0, SHARD_META = 1, SHARD_BATCH = 2,  LAST_MSG = 3);

// magic num comes from the first 8 bytes of 'echo homeobject_replication | md5sum'
static constexpr uint64_t HOMEOBJECT_REPLICATION_MAGIC = 0x11153ca24efc8d34;
static constexpr uint32_t HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1 = 0x01;
static constexpr uint32_t HOMEOBJECT_RESYNC_PROTOCOL_VERSION_V1 = 0x01;
static constexpr uint32_t init_crc32 = 0;
static constexpr uint64_t LAST_OBJ_ID =ULLONG_MAX;

#pragma pack(1)
struct BaseMessageHeader {
    uint64_t magic_num{HOMEOBJECT_REPLICATION_MAGIC};
    uint32_t protocol_version;
    uint32_t payload_size;
    uint32_t payload_crc;
    mutable uint32_t header_crc;
    uint8_t reserved_pad[4]{};

    virtual uint32_t expected_protocol_version() const = 0;

    void seal() {
        header_crc = 0;
        header_crc = calculate_crc();
    }

    uint32_t calculate_crc() const {
        return crc32_ieee(init_crc32, reinterpret_cast<const unsigned char*>(this), sizeof(*this));
    }

    bool corrupted() const {
        if (magic_num != HOMEOBJECT_REPLICATION_MAGIC || protocol_version != expected_protocol_version()) {
            return true;
        }

        auto saved_crc = header_crc;
        header_crc = 0;
        bool is_corrupted = (saved_crc != calculate_crc());
        header_crc = saved_crc;
        return is_corrupted;
    }

    std::string to_string() const {
        return fmt::format(
            "magic={:#x} version={} payload_size={} payload_crc={} header_crc={}\n",
            magic_num, protocol_version, payload_size, payload_crc, header_crc);
    }
};

struct ReplicationMessageHeader : public BaseMessageHeader{
    uint32_t protocol_version{HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1};
    ReplicationMessageType msg_type; // message type
    pg_id_t pg_id{0};
    shard_id_t shard_id{0};

    uint32_t expected_protocol_version() const override {
        return HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1;  // Specific version for sync messages
    }

    std::string to_string() const {
        return fmt::format(
            "magic={:#x} version={} msg_type={} pg_id={} shard_id={} payload_size={} payload_crc={} header_crc={}\n",
            magic_num, protocol_version, enum_name(msg_type), pg_id, shard_id, payload_size, payload_crc, header_crc);
    }
};

struct SyncMessageHeader : public BaseMessageHeader {
    uint32_t protocol_version{HOMEOBJECT_RESYNC_PROTOCOL_VERSION_V1};
    SyncMessageType msg_type; // message type

    uint32_t expected_protocol_version() const override {
        return HOMEOBJECT_RESYNC_PROTOCOL_VERSION_V1;  // Specific version for sync messages
    }

    std::string to_string() const {
        return fmt::format(
            "magic={:#x} version={} msg_type={} payload_size={} payload_crc={} header_crc={}\n",
            magic_num, protocol_version, enum_name(msg_type), payload_size, payload_crc, header_crc);
    }
};
#pragma pack()

// objId is the logical offset of the snapshot in baseline resync
// obj_id (64 bits) = type_bit (1 bit) | shard_id (48 bits) | batch_id (15 bits)
// type_bit = 0 for HomeStore, 1 for HomeObject
struct objId {
    snp_obj_id_t value;
    shard_id_t shardId;
    snp_batch_id_t batchId;

    objId(shard_id_t shard_id, snp_batch_id_t batch_id) : shardId(shard_id), batchId(batch_id) {
        //type_bit (1 bit) | shard_id (48 bits) | batch_id (15 bits)
        value= static_cast<uint64_t>(1) << 63 | (shard_id) << 15 | batch_id;
    }
    explicit objId(snp_obj_id_t value) : value(value) {
        shardId = (value >> 15) & 0xFFFFFFFFFFFF;
        batchId = value & 0x7FFF;
    }

    std::string to_string() const {
        return fmt::format("{}[shardId={}, batchId={} ]", value, shardId, batchId);
    }
};

} // namespace homeobject
