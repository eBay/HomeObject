#pragma once

#include "homeobject/common.hpp"

#include <sisl/utility/enum.hpp>
#include <sisl/fds/utils.hpp>

#include <homestore/crc.h>

namespace homeobject {

VENUM(ReplicationMessageType, uint16_t, CREATE_PG_MSG = 0, CREATE_SHARD_MSG = 1, SEAL_SHARD_MSG = 2, PUT_BLOB_MSG = 3,
      DEL_BLOB_MSG = 4, UNKNOWN_MSG = 5);
VENUM(SyncMessageType, uint16_t, PG_META = 0, SHARD_META = 1, SHARD_BATCH = 2,  LAST_MSG = 3);
VENUM(ResyncBlobState, uint8_t, NORMAL = 0, DELETED = 1, CORRUPTED = 2);

// magic num comes from the first 8 bytes of 'echo homeobject_replication | md5sum'
static constexpr uint64_t HOMEOBJECT_REPLICATION_MAGIC = 0x11153ca24efc8d34;
// magic num comes from the first 8 bytes of 'echo homeobject_resync | md5sum'
static constexpr uint64_t HOMEOBJECT_RESYNC_MAGIC = 0xbb6813cb4a339f30;
static constexpr uint32_t HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1 = 0x01;
static constexpr uint32_t HOMEOBJECT_RESYNC_PROTOCOL_VERSION_V1 = 0x01;
static constexpr uint32_t init_crc32 = 0;
static constexpr uint64_t LAST_OBJ_ID =ULLONG_MAX;
static constexpr uint64_t DEFAULT_MAX_BATCH_SIZE_MB =128;

#pragma pack(1)
template<typename Header>
struct BaseMessageHeader {
    uint64_t magic_num{HOMEOBJECT_REPLICATION_MAGIC};
    uint32_t protocol_version;
    uint32_t payload_size;
    uint32_t payload_crc;
    mutable uint32_t header_crc;

    void seal() {
        header_crc = 0;
        header_crc = calculate_crc();
    }

    uint32_t calculate_crc() const {
        const auto* hdr=static_cast<const Header*>(this);
        return crc32_ieee(init_crc32, reinterpret_cast<const unsigned char*>(hdr), sizeof(*hdr));
    }

    bool corrupted() const {
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

struct ReplicationMessageHeader : public BaseMessageHeader<ReplicationMessageHeader>{
    ReplicationMessageHeader(): BaseMessageHeader() {
        magic_num = HOMEOBJECT_REPLICATION_MAGIC;
        protocol_version = HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1;
    }
    ReplicationMessageType msg_type;
    pg_id_t pg_id{0};
    uint8_t reserved_pad[4]{};
    shard_id_t shard_id{0};
    blob_id_t blob_id{0};

    bool corrupted() const{
        if (magic_num != HOMEOBJECT_REPLICATION_MAGIC || protocol_version != HOMEOBJECT_REPLICATION_PROTOCOL_VERSION_V1) {
            return true;
        }
        return BaseMessageHeader::corrupted();
    }

    std::string to_string() const {
        return fmt::format(
            "magic={:#x} version={} msg_type={} pg={} shard_id={} payload_size={} payload_crc={} header_crc={}\n",
            magic_num, protocol_version, enum_name(msg_type), pg_id, shard_id, payload_size, payload_crc, header_crc);
    }
};

struct SyncMessageHeader : public BaseMessageHeader<SyncMessageHeader> {
    SyncMessageHeader() : BaseMessageHeader() {
        magic_num = HOMEOBJECT_RESYNC_MAGIC;
        protocol_version = HOMEOBJECT_RESYNC_PROTOCOL_VERSION_V1;
    }
    SyncMessageType msg_type;
    uint8_t reserved_pad[6]{};

    bool corrupted() const{
        if (magic_num != HOMEOBJECT_RESYNC_MAGIC || protocol_version != HOMEOBJECT_RESYNC_PROTOCOL_VERSION_V1) {
            return true;
        }
        return BaseMessageHeader::corrupted();
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
    shard_id_t shard_seq_num;
    snp_batch_id_t batch_id;

    objId(shard_id_t shard_seq_num, snp_batch_id_t batch_id) : shard_seq_num(shard_seq_num), batch_id(batch_id) {
        if (shard_seq_num != (shard_seq_num & 0xFFFFFFFFFFFF)) {
            throw std::invalid_argument("shard_id is too large");
        }
        if (batch_id != (batch_id & 0x7FFF)){
            throw std::invalid_argument("batch_id is too large");
        }
        //type_bit (1 bit) | shard_id (48 bits) | batch_id (15 bits)
        value= static_cast<uint64_t>(1) << 63 | (shard_seq_num) << 15 | batch_id;
    }
    explicit objId(snp_obj_id_t value) : value(value) {
        shard_seq_num = (value >> 15) & 0xFFFFFFFFFFFF;
        batch_id = value & 0x7FFF;
    }

    std::string to_string() const {
        return fmt::format("{}[shardSeqNum={}, batchId={} ]", value, shard_seq_num, batch_id);
    }
};

} // namespace homeobject
