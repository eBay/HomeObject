#include "hs_homeobject.hpp"
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/settings/settings.hpp>
#include "generated/resync_blob_data_generated.h"
#include "generated/resync_pg_data_generated.h"
#include "generated/resync_shard_data_generated.h"

namespace homeobject {
HSHomeObject::PGBlobIterator::PGBlobIterator(HSHomeObject& home_obj, homestore::group_id_t group_id,
                                             uint64_t upto_lsn) :
        home_obj_(home_obj), group_id_(group_id) {
    auto pg = get_pg_metadata();
    pg_id_ = pg->pg_info_.id;
    repl_dev_ = static_cast< HS_PG* >(pg)->repl_dev_;
    if (upto_lsn != 0) {
        // Iterate all shards and its blob which have lsn <= upto_lsn
        for (auto& shard : pg->shards_) {
            auto sequence_num = home_obj_.get_sequence_num_from_shard_id(shard->info.id);
            if (shard->info.lsn <= upto_lsn) { max_shard_seq_num_ = std::max(max_shard_seq_num_, sequence_num); }
        }
    } else {
        max_shard_seq_num_ = pg->shard_sequence_num_;
    }
}

PG* HSHomeObject::PGBlobIterator::get_pg_metadata() {
    std::scoped_lock lock_guard(home_obj_._pg_lock);
    auto iter = home_obj_._pg_map.begin();
    for (; iter != home_obj_._pg_map.end(); iter++) {
        if (iter->second->pg_info_.replica_set_uuid == group_id_) { break; }
    }

    RELEASE_ASSERT(iter != home_obj_._pg_map.end(), "PG not found replica_set_uuid={}",
                   boost::uuids::to_string(group_id_));
    return iter->second.get();
}

void HSHomeObject::PGBlobIterator::create_pg_shard_snapshot_data(sisl::io_blob_safe& meta_blob) {
    // auto pg = get_pg_metadata();
    // auto& pg_info = pg->pg_info_;
    // auto& pg_shards = pg->shards_;
    //
    // flatbuffers::FlatBufferBuilder builder;
    // std::vector< std::uint8_t > uuid(pg_info.replica_set_uuid.size());
    // std::copy(pg_info.replica_set_uuid.begin(), pg_info.replica_set_uuid.end(), uuid.begin());
    // auto pg_entry = CreatePGInfoEntry(builder, pg_info.id, 0 /* priority*/, builder.CreateVector(uuid));
    //
    // std::vector< ::flatbuffers::Offset< ShardInfoEntry > > shard_entries;
    // for (auto& shard : pg_shards) {
    //     auto& shard_info = shard->info;
    //     // TODO add lsn.
    //     shard_entries.push_back(CreateShardInfoEntry(
    //         builder, static_cast< uint8_t >(shard_info.state), shard_info.placement_group, shard_info.id,
    //         shard_info.total_capacity_bytes, shard_info.created_time, shard_info.last_modified_time));
    // }
    // builder.FinishSizePrefixed(CreateResyncPGShardInfo(builder, pg_entry, builder.CreateVector(shard_entries)));
    // meta_blob = sisl::io_blob_safe{builder.GetSize()};
    // std::memcpy(meta_blob.bytes(), builder.GetBufferPointer(), builder.GetSize());
}

void HSHomeObject::PGBlobIterator::create_pg_snapshot_data(sisl::io_blob_safe& meta_blob) {
    //TODO
}

void HSHomeObject::PGBlobIterator::create_shard_snapshot_data(sisl::io_blob_safe& meta_blob) {
    //TODO
}

int64_t HSHomeObject::PGBlobIterator::get_next_blobs(uint64_t max_num_blobs_in_batch, uint64_t max_batch_size_bytes,
                                                     std::vector< BlobInfoData >& blob_data_vec, bool& end_of_shard) {
    // end_of_shard = false;
    // uint64_t total_bytes = 0, num_blobs = 0;
    // while (true) {
    //     auto r = home_obj_.query_blobs_in_shard(pg_id_, cur_shard_seq_num_, cur_blob_id_ + 1, max_num_blobs_in_batch);
    //     if (!r) { return -1; }
    //     auto& index_results_vec = r.value();
    //     for (auto& info : index_results_vec) {
    //         if (info.pbas == HSHomeObject::tombstone_pbas) {
    //             // Skip deleted blobs
    //             continue;
    //         }
    //         auto result = home_obj_
    //                           ._get_blob_data(repl_dev_, info.shard_id, info.blob_id, 0 /*start_offset*/,
    //                                           0 /* req_len */, info.pbas)
    //                           .get();
    //         if (!result) {
    //             LOGE("Failed to retrieve blob for shard={} blob={} pbas={}", info.shard_id, info.blob_id,
    //                  info.pbas.to_string(), result.error());
    //             return -1;
    //         }
    //
    //         auto& blob = result.value();
    //         num_blobs++;
    //         total_bytes += blob.body.size() + blob.user_key.size();
    //         if (num_blobs > max_num_blobs_in_batch || total_bytes > max_batch_size_bytes) { return 0; }
    //
    //         BlobInfoData blob_data{{info.shard_id, info.blob_id, std::move(info.pbas)}, std::move(blob)};
    //         blob_data_vec.push_back(std::move(blob_data));
    //         cur_blob_id_ = info.blob_id;
    //     }
    //
    //     if (index_results_vec.empty()) {
    //         // We got empty results from index, which means we read
    //         // all the blobs in the current shard
    //         end_of_shard = true;
    //         cur_shard_seq_num_++;
    //         cur_blob_id_ = -1;
    //         break;
    //     }
    // }
    //
    return 0;
}

void HSHomeObject::PGBlobIterator::create_blobs_snapshot_data(std::vector< BlobInfoData >& blob_data_vec,
                                                              sisl::io_blob_safe& data_blob, bool end_of_shard) {
    // std::vector< ::flatbuffers::Offset< BlobData > > blob_entries;
    // flatbuffers::FlatBufferBuilder builder;
    // for (auto& b : blob_data_vec) {
    //     blob_entries.push_back(
    //         CreateBlobData(builder, b.shard_id, b.blob_id, b.blob.user_key.size(), b.blob.body.size(),
    //                        builder.CreateVector(r_cast< uint8_t* >(const_cast< char* >(b.blob.user_key.data())),
    //                                             b.blob.user_key.size()),
    //                        builder.CreateVector(b.blob.body.bytes(), b.blob.body.size())));
    // }
    // builder.FinishSizePrefixed(
    //     CreateResyncBlobDataBatch(builder, builder.CreateVector(blob_entries), end_of_shard /* end_of_batch */));
    // data_blob = sisl::io_blob_safe{builder.GetSize()};
    // std::memcpy(data_blob.bytes(), builder.GetBufferPointer(), builder.GetSize());
}

bool HSHomeObject::PGBlobIterator::end_of_scan() const {
    return max_shard_seq_num_ == 0 || cur_shard_seq_num_ > max_shard_seq_num_;
}

} // namespace homeobject