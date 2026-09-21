# HomeObject Log Catalog

Auto-generated catalog of all log statements in the HomeObject codebase, organized by log module and severity level.

## Log Modules

HomeObject defines 5 named log modules in `src/include/homeobject/common.hpp`:

```cpp
#define HOMEOBJECT_LOG_MODS homeobject, blobmgr, shardmgr, gcmgr, scrubmgr
```

A sixth implicit module **`base`** is used wherever bare `LOGINFO`/`LOGDEBUG`/etc. calls appear (no `MOD` suffix).
These resolve to `LOGINFOMOD(base, ...)` etc. via the sisl logging
library (`sisl/include/sisl/logging/logging.h:201-205`).

| Module       | Primary Files                                                                                 | Macro Family                                             | Structured Prefix                                            |
|--------------|-----------------------------------------------------------------------------------------------|----------------------------------------------------------|--------------------------------------------------------------|
| `homeobject` | `homeobject_impl.cpp/.hpp`, `blob_manager.cpp`...                                             | `LOGT/LOGD/LOGI/LOGW/LOGE/LOGC`                          | `bare message (no structured prefix)`                        |
| `blobmgr`    | `homestore_backend/hs_blob_manager.cpp`                                                       | `BLOGT/BLOGD/BLOGI/BLOGW/BLOGE/BLOGC`                    | `[traceID={}, shardID=0x{:x}, pg={}, shard=0x{:x}, blob={}]` |
| `shardmgr`   | `homestore_backend/hs_shard_manager.cpp`                                                      | `SLOGT/SLOGD/SLOGI/SLOGW/SLOGE/SLOGC`                    | `[trace_id={}, shardID=0x{:x}, pg={}, shard=0x{:x}]`         |
| `gcmgr`      | `homestore_backend/gc_manager.cpp`                                                            | `GCLOGT/GCLOGD/GCLOGI/GCLOGW/GCLOGE/GCLOGC,`             | `[gc_task_id={}, pg_id={}, shard_id=0x{:x}]`                 |
| `scrubmgr`   | `homestore_backend/scrub_manager.cpp`                                                         | `SCRUBLOGD/SCRUBLOGI/SCRUBLOGW/SCRUBLOGE/SCRUBLOGC`      | `[pg_id={}, task_id={}]`                                     |
| `base`       | `homestore_backend/hs_http_manager.cpp`, `homestore_backend/replication_state_machine.cpp`... | `LOGDEBUG/LOGINFO/LOGWARN/LOGERROR/LOGTRACE/LOGCRITICAL` | `bare message (no structured prefix)`                        |

## Severity Levels

All modules support the same 6 levels in ascending severity: TRACE < DEBUG < INFO < WARN < ERROR < CRITICAL.

## Log Count Summary

| Module       | TRACE | DEBUG | INFO | WARN | ERROR | CRITICAL | Total   |
|--------------|-------|-------|------|------|-------|----------|---------|
| `homeobject` | 20    | 73    | 122  | 109  | 75    | 0        | 399     |
| `blobmgr`    | 9     | 11    | 0    | 5    | 7     | 0        | 32      |
| `shardmgr`   | 0     | 10    | 0    | 7    | 1     | 0        | 18      |
| `gcmgr`      | 0     | 34    | 15   | 26   | 20    | 0        | 95      |
| `scrubmgr`   | 0     | 43    | 26   | 20   | 43    | 0        | 132     |
| `base`       | 0     | 6     | 42   | 1    | 4     | 0        | 53      |
| **Total**    | 29    | 177   | 205  | 168  | 150   | 0        | **729** |

---

## Module: `homeobject`

**Description:** General HomeObject lifecycle, PG management, replication state machine, initialization, and
cross-cutting concerns.

**Macros:
** `LOGT/LOGD/LOGI/LOGW/LOGE/LOGC (shorthands in homeobject_impl.hpp) or LOGTRACEMOD/LOGDEBUGMOD/LOGINFOMOD/LOGWARNMOD/LOGERRORMOD/LOGCRITICALMOD(homeobject, ...)`

**Log prefix:** `bare message (no structured prefix)`

**Source files:
** `homeobject_impl.cpp/.hpp`, `blob_manager.cpp`, `shard_manager.cpp`, `pg_manager.cpp`, `homestore_backend/hs_homeobject.cpp`, `homestore_backend/hs_pg_manager.cpp`, `homestore_backend/hs_shard_manager.cpp`, `homestore_backend/hs_blob_manager.cpp`, `homestore_backend/replication_state_machine.cpp`, `homestore_backend/heap_chunk_selector.cpp`, `homestore_backend/pg_blob_iterator.cpp`, `homestore_backend/snapshot_receive_handler.cpp`, `homestore_backend/index_kv.cpp`, `memory_backend/mem_*.cpp`

### TRACE (20)

| #  | Message                                                                                               | Source                                 |
|----|-------------------------------------------------------------------------------------------------------|----------------------------------------|
| 1  | `applying raft log commit with lsn={}, msg type={}`                                                   | `hs/replication_state_machine.cpp:26`  |
| 2  | `I am leader, reset no_space_left_error_info(lsn={}, chunk_id={}) after lsn={} is committed`          | `hs/replication_state_machine.cpp:68`  |
| 3  | `on_pre_commit with lsn={}, msg type={}`                                                              | `hs/replication_state_machine.cpp:101` |
| 4  | `blob already exists, but existing pbas is the same as the new pbas or has been deleted, ignore it, ` | `hs/index_kv.cpp:82`                   |
| 5  | `Reading resync blob: pg={}, shard=0x{:x}, blob={}, blkid={}, bytes={}`                               | `hs/pg_blob_iterator.cpp:287`          |
| 6  | `Read resync blob: pg={}, shard=0x{:x}, blob={}`                                                      | `hs/pg_blob_iterator.cpp:300`          |
| 7  | `Prefetching blobs: pg={}, shard_seq=0x{:x}, cursor_blob={}, frontier={}, inflight_bytes={}`          | `hs/pg_blob_iterator.cpp:348`          |
| 8  | `Skipping deleted resync blob: pg={}, shard=0x{:x}, blob={}`                                          | `hs/pg_blob_iterator.cpp:356`          |
| 9  | `Retaining prefetched resync blob: pg={}, blob={}`                                                    | `hs/pg_blob_iterator.cpp:363`          |
| 10 | `Submitting resync blob read: pg={}, shard_id=0x{:x}, blob={}`                                        | `hs/pg_blob_iterator.cpp:389`          |
| 11 | `Prefetched resync blob: pg={}, blob={}, blkid={}`                                                    | `hs/pg_blob_iterator.cpp:403`          |
| 12 | `Skipping deleted resync blob: pg={}, shard=0x{:x}, blob={}`                                          | `hs/pg_blob_iterator.cpp:443`          |
| 13 | `Packed resync message: pg={}, type={}, payload_bytes={}, payload_crc=0x{:x}`                         | `hs/pg_blob_iterator.cpp:513`          |
| 14 | `Draining prefetched resync blob: pg={}, blob={}`                                                     | `hs/pg_blob_iterator.cpp:536`          |
| 15 | `Skipping deleted resync blob: pg={}, shard_id=0x{:x}, blob={}`                                       | `hs/snapshot_receive_handler.cpp:165`  |
| 16 | `Re-committed persisted resync blob: pg={}, shard_id=0x{:x}, blob={}, blkid={}`                       | `hs/snapshot_receive_handler.cpp:203`  |
| 17 | `Writing resync blob: pg={}, shard_id=0x{:x}, blob={}, blkid={}`                                      | `hs/snapshot_receive_handler.cpp:270`  |
| 18 | `Wrote resync blob: pg={}, shard_id=0x{:x}, blob={}, blkid={}`                                        | `hs/snapshot_receive_handler.cpp:285`  |
| 19 | `Persisted resync blob: pg={}, shard_id=0x{:x}, blob={}, duration_us={}`                              | `hs/snapshot_receive_handler.cpp:306`  |
| 20 | `[route={}]`                                                                                          | `mem/mem_blob_manager.cpp:12`          |

### DEBUG (73)

| #  | Message                                                                                              | Source                                 |
|----|------------------------------------------------------------------------------------------------------|----------------------------------------|
| 1  | `found [shard={}], trace_id=[{}]`                                                                    | `shard_manager.cpp:26`                 |
| 2  | `Device {} detected as {}`                                                                           | `hs/hs_homeobject.cpp:172`             |
| -  | `shard_id={}, blob_id={} has been moved to tombstone, lsn={}`                                        | `hs/hs_blob_manager.cpp:686`           |
| 4  | `gc task_id={}, update shard={} pchunk from {} to {}`                                                | `hs/hs_shard_manager.cpp:788`          |
| 5  | `Shards in pg={} have all been destroyed`                                                            | `hs/hs_shard_manager.cpp:819`          |
| 6  | `Membership already in sync for pg={}, no update needed`                                             | `hs/hs_pg_manager.cpp:474`             |
| 7  | `pg={} is marked as destroyed`                                                                       | `hs/hs_pg_manager.cpp:801`             |
| 8  | `pg={} index table is destroyed`                                                                     | `hs/hs_pg_manager.cpp:836`             |
| 9  | `pg={} index table is not found, skip destroy`                                                       | `hs/hs_pg_manager.cpp:838`             |
| 10 | `Received scrub req for pg={}`                                                                       | `hs/hs_pg_manager.cpp:1189`            |
| 11 | `Scrub req loaded from flatbuffer for pg={}, scrub_type:{}, issuer_peer_id:{}`                       | `hs/hs_pg_manager.cpp:1216`            |
| 12 | `handle check existence req for pg={}, shard_id={}, blob_id={}, req_type={}`                         | `hs/hs_pg_manager.cpp:1226`            |
| 13 | `Received scrub result for pg={}`                                                                    | `hs/hs_pg_manager.cpp:1293`            |
| 14 | `Scrub result loaded from flatbuffer for pg={}, req_id:{}, issuer_peer_id:{}`                        | `hs/hs_pg_manager.cpp:1321`            |
| 15 | `gc task_id={}, the pchunk_id for vchunk={} for pg_id={} is already {}, skip updating pg metablk!`   | `hs/hs_pg_manager.cpp:1569`            |
| 16 | `gc task_id={}, pchunk for vchunk={} of pg_id={} is updated from {} to {}`                           | `hs/hs_pg_manager.cpp:1577`            |
| 17 | `gc task_id={}, move_from_chunk={}, total_occupied_blk_count_by_move_from_chunk={}, `                | `hs/hs_pg_manager.cpp:1604`            |
| 18 | `match no_space_left_error_info, lsn={}, chunk_id={}`                                                | `hs/replication_state_machine.cpp:75`  |
| 19 | `rollback config at lsn={}`                                                                          | `hs/replication_state_machine.cpp:156` |
| 20 | `ReplicationStateMachine::on_restart`                                                                | `hs/replication_state_machine.cpp:169` |
| 21 | `Skipping older snapshot context: group={}, requested_lsn={}, current_lsn={}`                        | `hs/replication_state_machine.cpp:276` |
| 22 | `Simulating resync snapshot apply delay: group={}, delay_ms={}`                                      | `hs/replication_state_machine.cpp:291` |
| 23 | `Allocated PGBlobIterator: iterator={}, group={}, snapshot_lsn={}`                                   | `hs/replication_state_machine.cpp:323` |
| 24 | `Simulating corrupted resync object: {}`                                                             | `hs/replication_state_machine.cpp:433` |
| 25 | `Resetting resync receiver context: previous_lsn={}, new_lsn={}`                                     | `hs/replication_state_machine.cpp:479` |
| 26 | `Dispatched resync PG metadata: pg={}, {}`                                                           | `hs/replication_state_machine.cpp:490` |
| 27 | `Dispatched resync shard metadata: {}`                                                               | `hs/replication_state_machine.cpp:532` |
| 28 | `Dispatched resync shard batch: {}`                                                                  | `hs/replication_state_machine.cpp:552` |
| 29 | `Freeing snapshot iterator={}, pg={} group={}`                                                       | `hs/replication_state_machine.cpp:562` |
| 30 | `fetch data with lsn={}, msg type={}`                                                                | `hs/replication_state_machine.cpp:605` |
| 31 | `fetch data with blob_id={}, shard=0x{:x}`                                                           | `hs/replication_state_machine.cpp:628` |
| 32 | `local_blk_id matches blob data, lsn={}, blob_id={}, shard_id={}`                                    | `hs/replication_state_machine.cpp:645` |
| 33 | `fetch data with blob_id={}, shardID=0x{:x}, pg={} from index table`                                 | `hs/replication_state_machine.cpp:664` |
| 34 | `on_fetch_data: failed to get from index table, blob never exists or has been gc, blob_id={}, `      | `hs/replication_state_machine.cpp:672` |
| 35 | `on_fetch_data: got tombstone pba for blob_id={}, shardID=0x{:x}, pg={}`                             | `hs/replication_state_machine.cpp:679` |
| 36 | `on_fetch_data: return delete marker for blob_id={}, shardID=0x{:x}, pg={}`                          | `hs/replication_state_machine.cpp:686` |
| 37 | `on_fetch_data: read data with blob_id={}, shardID=0x{:x}, pg={} from pbas={}`                       | `hs/replication_state_machine.cpp:700` |
| 38 | `pba matches blob data, lsn={}, blob_id={}, shardID=0x{:x}, pg={}`                                   | `hs/replication_state_machine.cpp:710` |
| 39 | `blob valid, blob_id={}, shardID=0x{:x}, pg={}`                                                      | `hs/replication_state_machine.cpp:726` |
| 40 | `Snapshot context superblk not found for group_id={}`                                                | `hs/replication_state_machine.cpp:748` |
| 41 | `Existing snapshot context superblk destroyed for group_id={}, lsn={}`                               | `hs/replication_state_machine.cpp:762` |
| 42 | `got no_space_left error at lsn={}, chunk_id={}`                                                     | `hs/replication_state_machine.cpp:851` |
| 43 | `set no_space_left error info with lsn={}, chunk_id={}, existing error info: lsn={}, chunk_id={}`    | `hs/replication_state_machine.cpp:862` |
| 44 | `successfully handle no_space_left error for chunk_id={} , lsn={}`                                   | `hs/replication_state_machine.cpp:906` |
| 45 | `vchunk={} is selected for shard={} in pg={} when recovery`                                          | `hs/replication_state_machine.cpp:977` |
| 46 | `gc task_id={}, chunk_id={} is marked out of gc state, final_state={}`                               | `hs/heap_chunk_selector.cpp:88`        |
| 47 | `v_chunk_id={} for pg={} is pchunk_id={}, in GC state, wait and retry!`                              | `hs/heap_chunk_selector.cpp:116`       |
| 48 | `chunk={} is selected for v_chunk_id={}, pg={}`                                                      | `hs/heap_chunk_selector.cpp:132`       |
| 49 | `reset chunk={} in pg={} for destruction`                                                            | `hs/heap_chunk_selector.cpp:178`       |
| 50 | `gc task_id={}, update vchunk info after gc, move_to_chunk={} now in pg={}, vchunk={}, state={}`     | `hs/heap_chunk_selector.cpp:337`       |
| 51 | `gc task_id={}, switch chunks for pg_id={}, old_chunk={}, new_chunk={}`                              | `hs/heap_chunk_selector.cpp:344`       |
| 52 | `gc task_id={}, the pchunk_id for vchunk={} in chunkselector for pg_id={} is already {},  skip `     | `hs/heap_chunk_selector.cpp:371`       |
| 53 | `gc task_id={}, vchunk={} in pg_chunk_collection for pg_id={} has been update from pchunk_id={} to ` | `hs/heap_chunk_selector.cpp:384`       |
| 54 | `Picked v_chunk_id={} : [p_chunk_id={}, avail={}], ctx=0x{:x}`                                       | `hs/heap_chunk_selector.cpp:516`       |
| 55 | `Rejecting cursor update on stopped PGBlobIterator: pg={}, requested_obj={}`                         | `hs/pg_blob_iterator.cpp:68`           |
| 56 | `Advanced resync cursor: pg={}, obj={}, shard_index={}, blob_index={}`                               | `hs/pg_blob_iterator.cpp:126`          |
| 57 | `Ignoring cursor reset on stopped PGBlobIterator: pg={}`                                             | `hs/pg_blob_iterator.cpp:134`          |
| 58 | `Rejecting PG metadata request on stopped PGBlobIterator: pg={}`                                     | `hs/pg_blob_iterator.cpp:179`          |
| 59 | `Simulating resync PG metadata creation failure: pg={}`                                              | `hs/pg_blob_iterator.cpp:185`          |
| 60 | `Rejecting shard blob-list request on stopped PGBlobIterator: pg={}`                                 | `hs/pg_blob_iterator.cpp:226`          |
| 61 | `Simulating resync shard blob-list query failure: pg={}, shard_seq=0x{:x}`                           | `hs/pg_blob_iterator.cpp:232`          |
| 62 | `Prepared resync shard blob list: pg={}, shard_seq=0x{:x}, blobs={}`                                 | `hs/pg_blob_iterator.cpp:244`          |
| 63 | `Rejecting shard metadata request on stopped PGBlobIterator: pg={}`                                  | `hs/pg_blob_iterator.cpp:252`          |
| 64 | `Simulating resync blob prefetch failure: pg={}, blob={}`                                            | `hs/pg_blob_iterator.cpp:376`          |
| 65 | `Simulating resync blob read delay: pg={}, blob={}, delay_ms={}`                                     | `hs/pg_blob_iterator.cpp:383`          |
| 66 | `Resync prefetch window: pg={}, shard_seq=0x{:x}, cursor_blob={}, frontier={}, submitted_blobs={}, ` | `hs/pg_blob_iterator.cpp:410`          |
| 67 | `Rejecting blob batch request on stopped PGBlobIterator: pg={}`                                      | `hs/pg_blob_iterator.cpp:420`          |
| 68 | `Resync PG membership: pg={}, expected_members={}, members={}`                                       | `hs/snapshot_receive_handler.cpp:38`   |
| 69 | `Simulating resync PG metadata failure: pg={}`                                                       | `hs/snapshot_receive_handler.cpp:43`   |
| 70 | `Simulating resync blob write delay: pg={}, shard_id=0x{:x}, blob={}, delay_ms={}`                   | `hs/snapshot_receive_handler.cpp:180`  |
| 71 | `Simulating resync blob allocation failure: pg={}, shard_id=0x{:x}, blob={}`                         | `hs/snapshot_receive_handler.cpp:240`  |
| 72 | `Simulating resync blob write failure: pg={}, shard_id=0x{:x}, blob={}`                              | `hs/snapshot_receive_handler.cpp:261`  |
| 73 | `[route={}] missing`                                                                                 | `mem/mem_blob_manager.cpp:16`          |

### INFO (122)

| #   | Message                                                                                                 | Source                                  |
|-----|---------------------------------------------------------------------------------------------------------|-----------------------------------------|
| 1   | `initialized with [executor={}]`                                                                        | `homeobject_impl.cpp:26`                |
| 2   | `HomeObjectImpl: Executing shutdown procedure`                                                          | `homeobject_impl.hpp:168`               |
| 3   | `[pg={}] has [{}] members, trace_id={}`                                                                 | `pg_manager.cpp:10`                     |
| 4   | `[pg={}] replace member [{}] with [{}]  task_id [{}] quorum [{}] trace_id [{}]`                         | `pg_manager.cpp:26`                     |
| 5   | `Initializing HomeObject`                                                                               | `hs/hs_homeobject.cpp:39`               |
| 6   | `Starting iomgr with {} threads, spdk={}`                                                               | `hs/hs_homeobject.cpp:152`              |
| 7   | `Initialize and start HomeStore with app_mem_size = {}`                                                 | `hs/hs_homeobject.cpp:160`              |
| 8   | `will reserve {} blks in each chunk`                                                                    | `hs/hs_homeobject.cpp:164`              |
| 9   | `  vdev [{}]: dev_type={} size_pct={:.1f}%`                                                             | `hs/hs_homeobject.cpp:288`              |
| 10  | `Initialize and start HomeStore is successfully`                                                        | `hs/hs_homeobject.cpp:299`              |
| 11  | `Starting GC manager`                                                                                   | `hs/hs_homeobject.cpp:310`              |
| 12  | `GC is disabled`                                                                                        | `hs/hs_homeobject.cpp:313`              |
| 13  | `Starting scrub manager`                                                                                | `hs/hs_homeobject.cpp:318`              |
| 14  | `scrub manager is disabled`                                                                             | `hs/hs_homeobject.cpp:321`              |
| 15  | `Register PG, shard and gc related meta blk handlers`                                                   | `hs/hs_homeobject.cpp:327`              |
| 16  | `Redo destroy pg for stale destroyed pg {}`                                                             | `hs/hs_homeobject.cpp:442`              |
| 17  | `homeobject timer thread started successfully with freq {} usec`                                        | `hs/hs_homeobject.cpp:467`              |
| 18  | `Found existing SvcId: [{}]`                                                                            | `hs/hs_homeobject.cpp:489`              |
| 19  | `HSHomeObject: Executing destruct procedure`                                                            | `hs/hs_homeobject.cpp:494`              |
| 20  | `HomeObject is already shutting down`                                                                   | `hs/hs_homeobject.cpp:498`              |
| 21  | `start shutting down HomeObject`                                                                        | `hs/hs_homeobject.cpp:502`              |
| 22  | `waiting for {} pending requests to complete`                                                           | `hs/hs_homeobject.cpp:516`              |
| 23  | `stopping GC`                                                                                           | `hs/hs_homeobject.cpp:519`              |
| 24  | `stopping scrubbing`                                                                                    | `hs/hs_homeobject.cpp:525`              |
| 25  | `start shutting down HomeStore`                                                                         | `hs/hs_homeobject.cpp:528`              |
| 26  | `complete shutting down HomeStore`                                                                      | `hs/hs_homeobject.cpp:534`              |
| 27  | `scan chunks for gc immediately`                                                                        | `hs/hs_homeobject.cpp:588`              |
| 28  | `PG id not set, start reconciling leaders for all PGs`                                                  | `hs/hs_homeobject.cpp:597`              |
| 29  | `Triggered reconcile leader for PG {}`                                                                  | `hs/hs_homeobject.cpp:604`              |
| 30  | `Reconciling leader for PG {}`                                                                          | `hs/hs_homeobject.cpp:611`              |
| 31  | `PG id not set, start yield leaders for all PGs`                                                        | `hs/hs_homeobject.cpp:624`              |
| 32  | `Triggered reconcile leader for PG {}`                                                                  | `hs/hs_homeobject.cpp:631`              |
| 33  | `Yielding leader for PG {}`                                                                             | `hs/hs_homeobject.cpp:638`              |
| 34  | `Triggering snapshot creation for pg_id={}, compact_lsn={}, wait_for_commit={}`                         | `hs/hs_homeobject.cpp:649`              |
| 35  | `service is being shut down`                                                                            | `hs/hs_blob_manager.cpp:90`             |
| 36  | `service is being shutdown`                                                                             | `hs/hs_blob_manager.cpp:348`            |
| 37  | `service is being shut down`                                                                            | `hs/hs_blob_manager.cpp:578`            |
| 38  | `traceID={}, lsn={}, mes_type={} is rollbacked`                                                         | `hs/hs_blob_manager.cpp:745`            |
| 39  | `service is being shut down`                                                                            | `hs/hs_shard_manager.cpp:159`           |
| 40  | `service is being shut down`                                                                            | `hs/hs_shard_manager.cpp:295`           |
| 41  | `Detected v2 shard superblk (shard={}, pg={}, p_chunk={}, v_chunk={}), migrating to v3`                 | `hs/hs_shard_manager.cpp:595`           |
| 42  | `Queued shard_id={} for v2->v3 migration write after recovery`                                          | `hs/hs_shard_manager.cpp:611`           |
| 43  | `Writing {} migrated shard v3 superblocks`                                                              | `hs/hs_shard_manager.cpp:645`           |
| 44  | `Successfully wrote migrated v3 shard superblk for shard_id={}`                                         | `hs/hs_shard_manager.cpp:659`           |
| 45  | `Completed migrating all shard superblocks from v2 to v3`                                               | `hs/hs_shard_manager.cpp:667`           |
| 46  | `service is being shut down`                                                                            | `hs/hs_pg_manager.cpp:88`               |
| 47  | `create pg={} successfully, index table uuid={} pg_size={} num_chunk={}, trace_id={}`                   | `hs/hs_pg_manager.cpp:233`              |
| 48  | `service is being shut down, trace_id={}`                                                               | `hs/hs_pg_manager.cpp:293`              |
| 49  | `PG replace member initated member_out={} member_in={} trace_id={}`                                     | `hs/hs_pg_manager.cpp:312`              |
| 50  | `PG start replace member done, task_id={} member_out={} member_in={}, member_nums={}, trace_id={}`      | `hs/hs_pg_manager.cpp:370`              |
| 51  | `PG complete replace member done member_out={} member_in={}, member_nums={}, trace_id={}`               | `hs/hs_pg_manager.cpp:399`              |
| 52  | `PG clean replace member task done (rollback), task_id={}, removed in_member={} (removed={}), `         | `hs/hs_pg_manager.cpp:430`              |
| 53  | `Reconciling PG {} membership: removing {} members, adding {} members, old membership count {}, new `   | `hs/hs_pg_manager.cpp:492`              |
| 54  | `  - Removing member: {}`                                                                               | `hs/hs_pg_manager.cpp:497`              |
| 55  | `  - Adding member: {}`                                                                                 | `hs/hs_pg_manager.cpp:500`              |
| 56  | `Successfully reconciled PG {} membership, new member_count={}`                                         | `hs/hs_pg_manager.cpp:507`              |
| 57  | `PG remove member done member={} member_nums={}, trace_id={}`                                           | `hs/hs_pg_manager.cpp:521`              |
| 58  | `PG remove member done, member doesn't exist, member={} member_nums={}, trace_id={}`                    | `hs/hs_pg_manager.cpp:524`              |
| 59  | `service is being shut down, trace_id={}`                                                               | `hs/hs_pg_manager.cpp:557`              |
| 60  | `PG flip learner flag to {}, pg_id={} member={} trace_id={}`                                            | `hs/hs_pg_manager.cpp:576`              |
| 61  | `PG flip learner flag done`                                                                             | `hs/hs_pg_manager.cpp:588`              |
| 62  | `service is being shut down, trace_id={}`                                                               | `hs/hs_pg_manager.cpp:596`              |
| 63  | `PG remove member, pg_id={}, member={} trace_id={}`                                                     | `hs/hs_pg_manager.cpp:615`              |
| 64  | `service is being shut down, trace_id={}`                                                               | `hs/hs_pg_manager.cpp:629`              |
| 65  | `PG clean replace member task, pg={}, task={} trace_id={}`                                              | `hs/hs_pg_manager.cpp:648`              |
| 66  | `service is being shut down, trace_id={}`                                                               | `hs/hs_pg_manager.cpp:661`              |
| 67  | `resource of pg={} is destroyed`                                                                        | `hs/hs_pg_manager.cpp:709`              |
| 68  | `Failed to pause pg state machine, pg_id={}`                                                            | `hs/hs_pg_manager.cpp:723`              |
| 69  | `Destroying pg={}`                                                                                      | `hs/hs_pg_manager.cpp:727`              |
| 70  | `group_id is nil, nothing to exit, trace_id={}`                                                         | `hs/hs_pg_manager.cpp:745`              |
| 71  | `Pause pg state machine, pg={}`                                                                         | `hs/hs_pg_manager.cpp:764`              |
| 72  | `pg={} state machine is paused`                                                                         | `hs/hs_pg_manager.cpp:772`              |
| 73  | `Resuming pg state machine, pg={}`                                                                      | `hs/hs_pg_manager.cpp:781`              |
| 74  | `CP Flush triggered by pg_destroy completed, pg={}`                                                     | `hs/hs_pg_manager.cpp:849`              |
| 75  | `on_pg_meta_blk_found is called`                                                                        | `hs/hs_pg_manager.cpp:930`              |
| 76  | `Index table not found for destroyed pg={}, index_table_uuid={}`                                        | `hs/hs_pg_manager.cpp:979`              |
| 77  | `Trying to yield leadership from {} to specified candidate {}`                                          | `hs/hs_pg_manager.cpp:1102`             |
| 78  | `Trying to yield leadership from {} to {}, candidate_priority={}`                                       | `hs/hs_pg_manager.cpp:1127`             |
| 79  | `PG membership updated, member_nums={}`                                                                 | `hs/hs_pg_manager.cpp:1160`             |
| 80  | `Successfully registered PUSH_SCRUB_REQ RPC handler for pg={}`                                          | `hs/hs_pg_manager.cpp:1174`             |
| 81  | `Successfully registered PUSH_SCRUB_RESULT RPC handler for pg={}`                                       | `hs/hs_pg_manager.cpp:1181`             |
| 82  | `lsn={}, msg_type={}, pg={}, create pg is rollbacked`                                                   | `hs/hs_pg_manager.cpp:1411`             |
| 83  | `Refreshed statistics for pg={}: active_blobs={} (original={}), tombstone_blobs={} (original={}), `     | `hs/hs_pg_manager.cpp:1539`             |
| 84  | `PG flip learner flag failed, err={}`                                                                   | `hs/hs_http_manager.cpp:408`            |
| 85  | `pre_commit {} log entry, lsn={}`                                                                       | `hs/replication_state_machine.cpp:90`   |
| 86  | `on_rollback  with lsn={}`                                                                              | `hs/replication_state_machine.cpp:115`  |
| 87  | `replica destroyed, cleared pg={} resources with group_id={}`                                           | `hs/replication_state_machine.cpp:265`  |
| 88  | `Created resync snapshot context: group={}, lsn={}`                                                     | `hs/replication_state_machine.cpp:281`  |
| 89  | `Applied resync snapshot: group={}, lsn={}`                                                             | `hs/replication_state_machine.cpp:302`  |
| 90  | `Completed resync snapshot read: pg={}, {}`                                                             | `hs/replication_state_machine.cpp:347`  |
| 91  | `Reading resync object: {}`                                                                             | `hs/replication_state_machine.cpp:354`  |
| 92  | `Reloaded resync receiver context: lsn={}, pg={}, next_shard=0x{:x}`                                    | `hs/replication_state_machine.cpp:406`  |
| 93  | `Writing resync object: {}`                                                                             | `hs/replication_state_machine.cpp:416`  |
| 94  | `Completed resync snapshot write: {}`                                                                   | `hs/replication_state_machine.cpp:426`  |
| 95  | `Resuming resync receiver context: lsn={}, pg={}, next_shard=0x{:x}, shard_cursor=0x{:x}`               | `hs/replication_state_machine.cpp:462`  |
| 96  | `Resetting existing PG before resync: pg={}, {}`                                                        | `hs/replication_state_machine.cpp:471`  |
| 97  | `Loaded previous snapshot from superblk, lsn={}`                                                        | `hs/replication_state_machine.cpp:575`  |
| 98  | `Snapshot context superblk updated for group_id={}, lsn={}`                                             | `hs/replication_state_machine.cpp:774`  |
| 99  | `Snapshot context superblk not found for group_id={}`                                                   | `hs/replication_state_machine.cpp:781`  |
| 100 | `Snapshot context superblk destroyed for group_id={}`                                                   | `hs/replication_state_machine.cpp:787`  |
| 101 | `Found snapshot context meta blk`                                                                       | `hs/replication_state_machine.cpp:791`  |
| 102 | `Replacing existing snapshot context superblk with new one`                                             | `hs/replication_state_machine.cpp:800`  |
| 103 | `Keeping existing snapshot context superblk`                                                            | `hs/replication_state_machine.cpp:803`  |
| 104 | `Snapshot context meta blk recover completed`                                                           | `hs/replication_state_machine.cpp:811`  |
| 105 | `Starting statistics refresh for pg={}`                                                                 | `hs/replication_state_machine.cpp:982`  |
| 106 | `become follower of group {}, cancel scrub task for pg={}`                                              | `hs/replication_state_machine.cpp:1006` |
| 107 | `select pdev[id={}, name={}] for pg_id={}, num_chunk={}`                                                | `hs/heap_chunk_selector.cpp:274`        |
| 108 | `Index UUID {}`                                                                                         | `hs/index_kv.cpp:121`                   |
| 109 | `Created PGBlobIterator: pg={}, snapshot_lsn={}, shards={}, batch_size={}`                              | `hs/pg_blob_iterator.cpp:60`            |
| 110 | `Resending resync object: pg={}, obj={}`                                                                | `hs/pg_blob_iterator.cpp:81`            |
| 111 | `Resuming resync at shard: pg={}, shard_seq=0x{:x}, shard_index={}`                                     | `hs/pg_blob_iterator.cpp:100`           |
| 112 | `Reset resync cursor: pg={}`                                                                            | `hs/pg_blob_iterator.cpp:144`           |
| 113 | `Created resync PG metadata: pg={}, shards={}, active_blobs={}, occupied_bytes={}`                      | `hs/pg_blob_iterator.cpp:218`           |
| 114 | `Created resync shard metadata: pg={}, shard_id=0x{:x}, shard_seq=0x{:x}, state={}, blobs={}`           | `hs/pg_blob_iterator.cpp:266`           |
| 115 | `Resync blob relocated by GC during read; retrying: pg={}, shard_id=0x{:x}, blob={}, old_blkid={}, `    | `hs/pg_blob_iterator.cpp:332`           |
| 116 | `Created resync shard batch: pg={}, shard_seq=0x{:x}, batch={}, blobs={}, skipped_blobs={}, bytes={}, ` | `hs/pg_blob_iterator.cpp:495`           |
| 117 | `Stopping PGBlobIterator: pg={}, prefetched_blobs={}, inflight_bytes={}`                                | `hs/pg_blob_iterator.cpp:529`           |
| 118 | `Stopped PGBlobIterator: pg={}`                                                                         | `hs/pg_blob_iterator.cpp:545`           |
| 119 | `Processed resync PG metadata: pg={}, shards={}, total_blobs={}, total_bytes={}`                        | `hs/snapshot_receive_handler.cpp:68`    |
| 120 | `Processed resync shard metadata: pg={}, shard_id=0x{:x}, state={}`                                     | `hs/snapshot_receive_handler.cpp:108`   |
| 121 | `Processed resync shard batch: pg={}, shard_id=0x{:x}, batch={}, blobs={}, bytes={}, end_of_shard={}`   | `hs/snapshot_receive_handler.cpp:363`   |
| 122 | `MemoryHomeObject: Executing shutdown procedure`                                                        | `mem/mem_homeobject.cpp:15`             |

### WARN (109)

| #   | Message                                                                                                 | Source                                 |
|-----|---------------------------------------------------------------------------------------------------------|----------------------------------------|
| 1   | `rejecting identical replacement SvcId [{}]! task_id [{}] trace_id [{}]`                                | `pg_manager.cpp:29`                    |
| 2   | `HomeObjectApplication lifetime unexpected! Shutdown in progress?`                                      | `hs/hs_homeobject.cpp:98`              |
| 3   | `HomeObjectApplication lifetime unexpected! Shutdown in progress?`                                      | `hs/hs_homeobject.cpp:120`             |
| 4   | `Device {} is not supported, skipping`                                                                  | `hs/hs_homeobject.cpp:175`             |
| 5   | `Device {} detected as {}, but input type is {}, using input type`                                      | `hs/hs_homeobject.cpp:179`             |
| 6   | `We are starting for the first time on [{}], Formatting!!`                                              | `hs/hs_homeobject.cpp:208`             |
| 7   | `failed to put blob for pg={}, pg is disk down and not leader`                                          | `hs/hs_blob_manager.cpp:108`           |
| 8   | `failed to get blob for pg={}, shardID=0x{:x},pg={},shard=0x{:x}, not ready for traffic`                | `hs/hs_blob_manager.cpp:374`           |
| 9   | `traceID={}, shardID=0x{:x}, pg={}, shard=0x{:x}, Received a blob_put on an unknown pg={}, underlying ` | `hs/hs_blob_manager.cpp:528`           |
| 10  | `traceID={}, shardID=0x{:x}, pg={}, shard=0x{:x}, Received a blob_put on an unknown shard, underlying ` | `hs/hs_blob_manager.cpp:540`           |
| 11  | `index table is not found for pg={}, skip statistics refresh`                                           | `hs/hs_blob_manager.cpp:559`           |
| 12  | `failed to delete blob for pg={}, pg is disk down and not leader`                                       | `hs/hs_blob_manager.cpp:588`           |
| 13  | `shard_id={}, blob_id={} not found, probably already deleted, lsn={}`                                   | `hs/hs_blob_manager.cpp:679`           |
| 14  | `shard_id={}, blob_id={} already tombstoned, lsn={}`                                                    | `hs/hs_blob_manager.cpp:702`           |
| 15  | `replication message header is corrupted with crc error, lsn={}, traceID={}`                            | `hs/hs_blob_manager.cpp:736`           |
| 16  | `traceID={}, lsn={}, mes_type={} should not happen in blob message rollback`                            | `hs/hs_blob_manager.cpp:750`           |
| 17  | `Found delete_marker for shard_id={}, blob_id={}, skipping verification!`                               | `hs/hs_blob_manager.cpp:799`           |
| 18  | `Unsupported shard_info_superblk sb_version={}`                                                         | `hs/hs_shard_manager.cpp:55`           |
| 19  | `meta length {} exceeds max meta length {}, trace_id={}`                                                | `hs/hs_shard_manager.cpp:164`          |
| 20  | `failed to create shard with non-exist pg={}`                                                           | `hs/hs_shard_manager.cpp:170`          |
| 21  | `failed to create shard for pg={}, pg is disk down and not leader`                                      | `hs/hs_shard_manager.cpp:175`          |
| 22  | `failed to get repl dev instance for pg={}`                                                             | `hs/hs_shard_manager.cpp:182`          |
| 23  | `failed to create shard for pg={}, not leader`                                                          | `hs/hs_shard_manager.cpp:188`          |
| 24  | `failed to create shard for pg={}, not ready for traffic`                                               | `hs/hs_shard_manager.cpp:194`          |
| 25  | `failed to create shard for pg={}, pchunk_id= {} is selected for vchunk_id={} is selected, not enough ` | `hs/hs_shard_manager.cpp:217`          |
| 26  | `failed to seal shard for pg={}, pg is disk down and not leader`                                        | `hs/hs_shard_manager.cpp:311`          |
| 27  | `replication message header is corrupted with crc error, lsn={}, traceID={}`                            | `hs/hs_shard_manager.cpp:393`          |
| 28  | `replication message header is corrupted with crc error, lsn={}, traceID={}`                            | `hs/hs_shard_manager.cpp:416`          |
| 29  | `replication message header is corrupted with crc error, lsn={}, traceID={}`                            | `hs/hs_shard_manager.cpp:477`          |
| 30  | `pg={} is disk down, skip recover chunk state from shards`                                              | `hs/hs_shard_manager.cpp:626`          |
| 31  | `Shard {} not found in shard map during migration write, skipping`                                      | `hs/hs_shard_manager.cpp:651`          |
| 32  | `pg={} is disk down, skip add shard to map, shardID=0x{:x}`                                             | `hs/hs_shard_manager.cpp:686`          |
| 33  | `chunk_id={} not found in chunk_to_shards_map`                                                          | `hs/hs_shard_manager.cpp:740`          |
| 34  | `gc task_id={}, find no shard in move_from_chunk={}, skip update shard meta blk after gc!`              | `hs/hs_shard_manager.cpp:756`          |
| 35  | `on shards destroy with unknown pg={}`                                                                  | `hs/hs_shard_manager.cpp:807`          |
| 36  | `PG already exists with different info! pg={}, pg_info={}, hs_pg_info={}`                               | `hs/hs_pg_manager.cpp:97`              |
| 37  | `PG already exists! pg={}`                                                                              | `hs/hs_pg_manager.cpp:102`             |
| 38  | `Not support to create PG which pg_size={} < chunk_size={}`                                             | `hs/hs_pg_manager.cpp:109`             |
| 39  | `Failed to select chunks for pg={}`                                                                     | `hs/hs_pg_manager.cpp:116`             |
| 40  | `Simulating create repl dev error in creating pg`                                                       | `hs/hs_pg_manager.cpp:130`             |
| 41  | `Failed to remove repl device which group_id={}, error={}`                                              | `hs/hs_pg_manager.cpp:153`             |
| 42  | `Simulating raft message error in creating pg`                                                          | `hs/hs_pg_manager.cpp:179`             |
| 43  | `PG already exists, pg={}, trace_id={}, pg_info={}, hs_pg_info={}`                                      | `hs/hs_pg_manager.cpp:197`             |
| 44  | `Failed to select chunks for pg={}, trace_id={}`                                                        | `hs/hs_pg_manager.cpp:212`             |
| 45  | `Failed to get pg chunks, pg={}, trace_id={}`                                                           | `hs/hs_pg_manager.cpp:217`             |
| 46  | `Failed to get replication quorum for PG {}, repl_dev may not be ready`                                 | `hs/hs_pg_manager.cpp:453`             |
| 47  | `repl dev not found, ignore, group_id={}, trace_id={}`                                                  | `hs/hs_pg_manager.cpp:752`             |
| 48  | `mark pg destroyed with unknown pg={}`                                                                  | `hs/hs_pg_manager.cpp:796`             |
| 49  | `unknown pg={}`                                                                                         | `hs/hs_pg_manager.cpp:808`             |
| 50  | `destroy pg index table with unknown pg={}`                                                             | `hs/hs_pg_manager.cpp:825`             |
| 51  | `destroy pg superblk with unknown pg={}`                                                                | `hs/hs_pg_manager.cpp:857`             |
| 52  | `Failed to recover pg={} chunks,set pg_state to DISK_DOWN`                                              | `hs/hs_pg_manager.cpp:969`             |
| 53  | `ReplDev is null, cannot register data rpc handlers for pg={}`                                          | `hs/hs_pg_manager.cpp:1165`            |
| 54  | `PUSH_SCRUB_REQ RPC handler already registered for pg={}`                                               | `hs/hs_pg_manager.cpp:1176`            |
| 55  | `PUSH_SCRUB_RESULT RPC handler already registered for pg={}`                                            | `hs/hs_pg_manager.cpp:1183`            |
| 56  | `scrub req received with empty buffer for pg={}`                                                        | `hs/hs_pg_manager.cpp:1196`            |
| 57  | `received with invalid flatbuffer for pg={}`                                                            | `hs/hs_pg_manager.cpp:1203`            |
| 58  | `Failed to load scrub_blob request from flatbuffer for pg={}`                                           | `hs/hs_pg_manager.cpp:1211`            |
| 59  | `ScrubManager is not initialized in HS_PG::on_scrub_req_received for pg={}`                             | `hs/hs_pg_manager.cpp:1285`            |
| 60  | `PUSH_DEEP_BLOB_SM received with empty buffer for pg={}, buffer_size={}`                                | `hs/hs_pg_manager.cpp:1307`            |
| 61  | `scrub result received with invalid flatbuffer for pg={}, buffer_size={}`                               | `hs/hs_pg_manager.cpp:1312`            |
| 62  | `Failed to load scrub result from flatbuffer for pg={}`                                                 | `hs/hs_pg_manager.cpp:1318`            |
| 63  | `ScrubManager is not initialized in HS_PG::on_scrub_result_received for pg={}`                          | `hs/hs_pg_manager.cpp:1326`            |
| 64  | `lsn={}, mes_type={}, pg={}, should not happen in pg message rollback`                                  | `hs/hs_pg_manager.cpp:1416`            |
| 65  | `get pg tombstone blob count with unknown pg={}`                                                        | `hs/hs_pg_manager.cpp:1427`            |
| 66  | `get_blk_alloc_hints called for old shard message type={}, shard={}, pg={}, return committed_blk hint ` | `hs/replication_state_machine.cpp:219` |
| 67  | `not support msg type for {} in get_blk_alloc_hints`                                                    | `hs/replication_state_machine.cpp:229` |
| 68  | `do not have pg mapped by group_id={}`                                                                  | `hs/replication_state_machine.cpp:259` |
| 69  | `Invalid resync cursor; resetting donor cursor: {}`                                                     | `hs/replication_state_machine.cpp:362` |
| 70  | `Ignoring resync object after shard completion: lsn={}, next_shard=0x{:x}, shard_cursor=0x{:x}`         | `hs/replication_state_machine.cpp:501` |
| 71  | `Resync receiver has no shard cursor; requesting PG metadata: lsn={}`                                   | `hs/replication_state_machine.cpp:506` |
| 72  | `Invalid resync receiver cursor; requesting shard metadata: lsn={}, next_shard=0x{:x}, `                | `hs/replication_state_machine.cpp:510` |
| 73  | `Header is empty in on_fetch_data for lsn {}`                                                           | `hs/replication_state_machine.cpp:590` |
| 74  | `replication message header is corrupted with crc error, lsn={}, header={}`                             | `hs/replication_state_machine.cpp:601` |
| 75  | `msg type={}, should not happen in fetch_data rpc`                                                      | `hs/replication_state_machine.cpp:738` |
| 76  | `shardID=0x{:x}, pg={}, shard=0x{:x}, shard does not exist when handling on_no_space_left, `            | `hs/replication_state_machine.cpp:830` |
| 77  | `not support msg type for {} in handling on_no_space_left`                                              | `hs/replication_state_machine.cpp:841` |
| 78  | `can not get a valid chunk_id, skip handling on_no_space_left for lsn={}`                               | `hs/replication_state_machine.cpp:847` |
| 79  | `start handling no_space_left error for chunk_id={} , lsn={}`                                           | `hs/replication_state_machine.cpp:888` |
| 80  | `can not find any pg for group={}!`                                                                     | `hs/replication_state_machine.cpp:948` |
| 81  | `No chunk found for chunk_id={}`                                                                        | `hs/heap_chunk_selector.cpp:57`        |
| 82  | `gc: chunk is already in gc state, chunk_id={}`                                                         | `hs/heap_chunk_selector.cpp:64`        |
| 83  | `gc: chunk is inuse, chunk_id={}`                                                                       | `hs/heap_chunk_selector.cpp:69`        |
| 84  | `No pg found for pg={}`                                                                                 | `hs/heap_chunk_selector.cpp:101`       |
| 85  | `No chunk found for v_chunk_id={}`                                                                      | `hs/heap_chunk_selector.cpp:109`       |
| 86  | `No pg found for pg={}`                                                                                 | `hs/heap_chunk_selector.cpp:147`       |
| 87  | `No chunk found for v_chunk_id={}`                                                                      | `hs/heap_chunk_selector.cpp:154`       |
| 88  | `No pg found for pg={}`                                                                                 | `hs/heap_chunk_selector.cpp:171`       |
| 89  | `No pg found for pg={}`                                                                                 | `hs/heap_chunk_selector.cpp:189`       |
| 90  | `No pg found for pg={}`                                                                                 | `hs/heap_chunk_selector.cpp:226`       |
| 91  | `No chunk found for v_chunk_id={}`                                                                      | `hs/heap_chunk_selector.cpp:233`       |
| 92  | `pg_size={} is less than chunk_size={}`                                                                 | `hs/heap_chunk_selector.cpp:250`       |
| 93  | `PG had already created, pg={}`                                                                         | `hs/heap_chunk_selector.cpp:258`       |
| 94  | `Pdev has no enough space to create pg={} with num_chunk={}, available_num_chunk={}`                    | `hs/heap_chunk_selector.cpp:270`       |
| 95  | `pg={} had been recovered`                                                                              | `hs/heap_chunk_selector.cpp:396`       |
| 96  | `Unexpected empty pg={}`                                                                                | `hs/heap_chunk_selector.cpp:400`       |
| 97  | `No chunk found for p_chunk_id={}`                                                                      | `hs/heap_chunk_selector.cpp:409`       |
| 98  | `The pdev value is different, last_pdev_id={}, pdev_id={}`                                              | `hs/heap_chunk_selector.cpp:414`       |
| 99  | `PG chunks should be recovered beforhand, pg={}`                                                        | `hs/heap_chunk_selector.cpp:453`       |
| 100 | `pg={} had never been created`                                                                          | `hs/heap_chunk_selector.cpp:480`       |
| 101 | `No pg found for pg={}`                                                                                 | `hs/heap_chunk_selector.cpp:500`       |
| 102 | `No available chunk for pg={}, ctx=0x{:x}`                                                              | `hs/heap_chunk_selector.cpp:512`       |
| 103 | `No pg found for pg={}`                                                                                 | `hs/heap_chunk_selector.cpp:539`       |
| 104 | `No pg found for pg={}`                                                                                 | `hs/heap_chunk_selector.cpp:551`       |
| 105 | `No pdev found for pdev {}`                                                                             | `hs/heap_chunk_selector.cpp:561`       |
| 106 | `No pg found for pg_id {}`                                                                              | `hs/heap_chunk_selector.cpp:598`       |
| 107 | `PG not found for pg={} when getting index table`                                                       | `hs/index_kv.cpp:128`                  |
| 108 | `Resync blob was deleted during read; restarting snapshot: pg={}, shard_id=0x{:x}, blob={}`             | `hs/pg_blob_iterator.cpp:318`          |
| 109 | `Persisting donor-reported corrupted resync blob: pg={}, shard_id=0x{:x}, blob={}`                      | `hs/snapshot_receive_handler.cpp:223`  |

### ERROR (75)

| #  | Message                                                                                                                 | Source                                  |
|----|-------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| 1  | `Couldn't find shard id in shard map {}, trace_id=[{}]`                                                                 | `shard_manager.cpp:55`                  |
| 2  | `cannot find group id in repl_sm_map`                                                                                   | `hs/hs_homeobject.cpp:82`               |
| 3  | `can't extract host from uuid {}, endpoint={}; error={}`                                                                | `hs/hs_homeobject.cpp:108`              |
| 4  | `gc index table {} not found`                                                                                           | `hs/hs_homeobject.cpp:580`              |
| 5  | `GC is not enabled`                                                                                                     | `hs/hs_homeobject.cpp:591`              |
| 6  | `PG {} not found`                                                                                                       | `hs/hs_homeobject.cpp:616`              |
| 7  | `PG {} not found`                                                                                                       | `hs/hs_homeobject.cpp:643`              |
| 8  | `Failed to trigger snapshot: PG {} not found`                                                                           | `hs/hs_homeobject.cpp:653`              |
| 9  | `replication message header is corrupted with crc error, lsn={}, traceID={}`                                            | `hs/hs_blob_manager.cpp:288`            |
| 10 | `traceID={}, shardID=0x{:x}, pg={}, shard=0x{:x}, replication message header is corrupted with crc error`               | `hs/hs_blob_manager.cpp:519`            |
| 11 | `Failed to free blocks for tombstoned blob, error={}`                                                                   | `hs/hs_blob_manager.cpp:694`            |
| 12 | `Invalid header found: [header={}]`                                                                                     | `hs/hs_blob_manager.cpp:764`            |
| 13 | `Invalid shard_id in header: [header={}]`                                                                               | `hs/hs_blob_manager.cpp:770`            |
| 14 | `Invalid blob_id in header: [header={}]`                                                                                | `hs/hs_blob_manager.cpp:776`            |
| 15 | `Hash mismatch header, [header={}] [computed={:np}]`                                                                    | `hs/hs_blob_manager.cpp:787`            |
| 16 | `Failed to migrate shard_id={}: {}`                                                                                     | `hs/hs_shard_manager.cpp:661`           |
| 17 | `Chunk sizes are inconsistent, leader_chunk_size={}, local_chunk_size={}, trace_id={}`                                  | `hs/hs_pg_manager.cpp:204`              |
| 18 | `create PG message header is corrupted , lsn={}; header={}, trace_id={}`                                                | `hs/hs_pg_manager.cpp:258`              |
| 19 | `create PG message header is inconsistent with value, lsn={}, trace_id={}`                                              | `hs/hs_pg_manager.cpp:269`              |
| 20 | `PG replace member failed task_id={}, member_out={} member_in={}, trace_id={}`                                          | `hs/hs_pg_manager.cpp:383`              |
| 21 | `PG complete replace member failed member_out={} member_in={}, trace_id={}`                                             | `hs/hs_pg_manager.cpp:405`              |
| 22 | `PG clean replace member task failed, pg not found, task_id={}, member_out={}, member_in={}, trace_id={}`               | `hs/hs_pg_manager.cpp:437`              |
| 23 | `PG {} not found for reconcile_membership`                                                                              | `hs/hs_pg_manager.cpp:445`              |
| 24 | `Adding new member {} to pg={} membership, should not happen!`                                                          | `hs/hs_pg_manager.cpp:468`              |
| 25 | `Failed to list replace member tasks, error={}`                                                                         | `hs/hs_pg_manager.cpp:667`              |
| 26 | `Failed to destroy repl dev for group_id={}, error={}, trace_id={}`                                                     | `hs/hs_pg_manager.cpp:756`              |
| 27 | `Failed to pause pg={} state machine after {} ms`                                                                       | `hs/hs_pg_manager.cpp:776`              |
| 28 | `Resumed pg state machine, pg=`                                                                                         | `hs/hs_pg_manager.cpp:788`              |
| 29 | `create PG message header is corrupted , lsn={}, header={}`                                                             | `hs/hs_pg_manager.cpp:1393`             |
| 30 | `create PG message header is inconsistent with value, lsn={}`                                                           | `hs/hs_pg_manager.cpp:1403`             |
| 31 | `index table is not found for pg={}, skip statistics refresh`                                                           | `hs/hs_pg_manager.cpp:1433`             |
| 32 | `corrupted message in pre_commit, lsn={}`                                                                               | `hs/replication_state_machine.cpp:98`   |
| 33 | `corrupted message in rollback, lsn={}`                                                                                 | `hs/replication_state_machine.cpp:118`  |
| 34 | `on_error, message type={} with lsn={}, error={}`                                                                       | `hs/replication_state_machine.cpp:176`  |
| 35 | `Unknown message type, error unhandled , error={}, lsn={}`                                                              | `hs/replication_state_machine.cpp:202`  |
| 36 | `Failed to create pg snapshot data for snapshot read, {}`                                                               | `hs/replication_state_machine.cpp:371`  |
| 37 | `Failed to generate shard blob list for snapshot read, {}`                                                              | `hs/replication_state_machine.cpp:380`  |
| 38 | `Failed to create shard meta data for snapshot read, {}`                                                                | `hs/replication_state_machine.cpp:384`  |
| 39 | `Failed to create blob batch data for snapshot read, {}`                                                                | `hs/replication_state_machine.cpp:392`  |
| 40 | `Invalid resync object size: minimum_bytes={}, {}`                                                                      | `hs/replication_state_machine.cpp:438`  |
| 41 | `Corrupted resync object header: {}`                                                                                    | `hs/replication_state_machine.cpp:443`  |
| 42 | `Resync object payload size mismatch: actual_bytes={}, expected_bytes={}, {}`                                           | `hs/replication_state_machine.cpp:447`  |
| 43 | `Failed to reset existing PG before resync; requesting retry: pg={}, {}`                                                | `hs/replication_state_machine.cpp:474`  |
| 44 | `Failed to process resync PG metadata: error={}, {}`                                                                    | `hs/replication_state_machine.cpp:486`  |
| 45 | `Failed to process resync shard metadata: error={}, {}`                                                                 | `hs/replication_state_machine.cpp:527`  |
| 46 | `Failed to process resync shard batch: error={}, {}`                                                                    | `hs/replication_state_machine.cpp:542`  |
| 47 | `User snapshot context null group={}`                                                                                   | `hs/replication_state_machine.cpp:557`  |
| 48 | `FetchData fails to read blob, lsn={}, blob_id={}, shard_id={}, err_value={}, error={}`                                 | `hs/replication_state_machine.cpp:634`  |
| 49 | `pg not found for pg={}, shardID=0x{:x}`                                                                                | `hs/replication_state_machine.cpp:654`  |
| 50 | `IO error happens when reading data for blob_id={}, shardID=0x{:x}, pg={}, error={}`                                    | `hs/replication_state_machine.cpp:730`  |
| 51 | `shardID=0x{:x}, pg={}, shard=0x{:x}, replication message header is corrupted with crc error when `                     | `hs/replication_state_machine.cpp:819`  |
| 52 | `become leader but can not find any pg for group={}!`                                                                   | `hs/replication_state_machine.cpp:989`  |
| 53 | `become follower but can not find any pg for group={}!`                                                                 | `hs/replication_state_machine.cpp:1000` |
| 54 | `blob already exists, and conflict occurs, blob_id={}, existing pbas={}, new pbas={}, status={}`                        | `hs/index_kv.cpp:88`                    |
| 55 | `Failed to put to index table error {}`                                                                                 | `hs/index_kv.cpp:92`                    |
| 56 | `Failed to query blobs in index table for ret={} shard={} start_blob_id={}`                                             | `hs/index_kv.cpp:155`                   |
| 57 | `Cannot resume resync: pg={}, requested_obj={} is not in the snapshot shard list`                                       | `hs/pg_blob_iterator.cpp:103`           |
| 58 | `Invalid resync cursor: pg={}, current_obj={}, expected_obj={}, requested_obj={}`                                       | `hs/pg_blob_iterator.cpp:111`           |
| 59 | `Cannot create resync PG metadata: pg={} not found`                                                                     | `hs/pg_blob_iterator.cpp:191`           |
| 60 | `Failed to query resync shard blobs: pg={}, shard_seq=0x{:x}, error={}`                                                 | `hs/pg_blob_iterator.cpp:239`           |
| 61 | `Failed to read resync blob: pg={}, shard=0x{:x}, blob={}, blkid={}, error={}`                                          | `hs/pg_blob_iterator.cpp:293`           |
| 62 | `Cannot resolve resync blob verification failure: pg={} no longer exists, shard_id=0x{:x}, blob={}`                     | `hs/pg_blob_iterator.cpp:308`           |
| 63 | `Resync blob verification failed: pg={}, shard=0x{:x}, blob={}, blkid={}`                                               | `hs/pg_blob_iterator.cpp:325`           |
| 64 | `Failed to prefetch resync blob: pg={}, shard=0x{:x}, blob={}, blkid={}`                                                | `hs/pg_blob_iterator.cpp:398`           |
| 65 | `Resync batch cannot find prefetched blob: pg={}, shard_seq=0x{:x}, batch={}, blob={}, `                                | `hs/pg_blob_iterator.cpp:453`           |
| 66 | `Resync batch failed to retrieve blob: pg={}, shard_seq=0x{:x}, batch={}, blob={}, error={}`                            | `hs/pg_blob_iterator.cpp:465`           |
| 67 | `Incomplete resync batch: pg={}, shard_seq=0x{:x}, batch={}, examined_blobs={}, skipped_blobs={}, `                     | `hs/pg_blob_iterator.cpp:478`           |
| 68 | `Failed to process resync PG metadata: pg={}, error={}`                                                                 | `hs/snapshot_receive_handler.cpp:49`    |
| 69 | `Resync blob verification failed: pg={}, shard_id=0x{:x}, blob={}`                                                      | `hs/snapshot_receive_handler.cpp:215`   |
| 70 | `Failed to allocate resync blob blocks: pg={}, shard_id=0x{:x}, blob={}`                                                | `hs/snapshot_receive_handler.cpp:252`   |
| 71 | `Failed to write resync blob; freeing block: pg={}, shard_id=0x{:x}, blob={}, blkid={}`                                 | `hs/snapshot_receive_handler.cpp:280`   |
| 72 | `Failed to commit resync blob block: pg={}, shard_id=0x{:x}, blob={}, blkid={}`                                         | `hs/snapshot_receive_handler.cpp:289`   |
| 73 | `Failed to index resync blob: pg={}, shard_id=0x{:x}, blob={}, blkid={}`                                                | `hs/snapshot_receive_handler.cpp:298`   |
| 74 | `Failed to submit complete resync shard batch: pg={}, shard_id=0x{:x}, batch={}, expected_blobs={}, submitted_blobs={}` | `hs/snapshot_receive_handler.cpp:318`   |
| 75 | `Failed to write resync shard batch: pg={}, shard_id=0x{:x}, batch={}, error_code={}, error={}`                         | `hs/snapshot_receive_handler.cpp:322`   |

---

## Module: `blobmgr`

**Description:** Blob put/get/delete/seal operations with per-request structured context.

**Macros:** `BLOGT/BLOGD/BLOGI/BLOGW/BLOGE/BLOGC`

**Log prefix:** `[traceID={}, shardID=0x{:x}, pg={}, shard=0x{:x}, blob={}]`

**Source files:** `homestore_backend/hs_blob_manager.cpp`

### TRACE (9)

| # | Message                                                     | Source                       |
|---|-------------------------------------------------------------|------------------------------|
| 1 | `Put blob: header={} sgs={}`                                | `hs/hs_blob_manager.cpp:195` |
| 2 | `blob put commit, exist_already={}, status={}, pbas={}`     | `hs/hs_blob_manager.cpp:224` |
| 3 | `blob already exists in index table, skip it.`              | `hs/hs_blob_manager.cpp:265` |
| 4 | `blob put commit lsn={}, pbas={}`                           | `hs/hs_blob_manager.cpp:280` |
| 5 | `Blob has already been persisted, blk_num={}, blk_count={}` | `hs/hs_blob_manager.cpp:566` |
| 6 | `deleting blob`                                             | `hs/hs_blob_manager.cpp:582` |
| 7 | `Delete blob successful`                                    | `hs/hs_blob_manager.cpp:635` |
| 8 | `Recovered pg index table uuid {}`                          | `hs/index_kv.cpp:51`         |
| 9 | `Recovered gc index table uuid {}`                          | `hs/index_kv.cpp:62`         |

### DEBUG (11)

| #  | Message                                                                                            | Source                       |
|----|----------------------------------------------------------------------------------------------------|------------------------------|
| 1  | `Blob Put request: pg={}, group={}, shard=0x{:x}, length={}`                                       | `hs/hs_blob_manager.cpp:121` |
| 2  | `Blob Put request: Put blob success blkid={}`                                                      | `hs/hs_blob_manager.cpp:209` |
| 3  | `succeed to free blob data blk, lsn={}, blkid={}`                                                  | `hs/hs_blob_manager.cpp:317` |
| 4  | `try to commit put_blob message to a non-open shard, lsn={}, shard_sealed_lsn={}, skip it!`        | `hs/hs_blob_manager.cpp:321` |
| 5  | `Blob Get request: pg={}, group={}, shard=0x{:x}, blob={}, offset={}, len={}`                      | `hs/hs_blob_manager.cpp:380` |
| 6  | `Reading from blkid={} to buf={}`                                                                  | `hs/hs_blob_manager.cpp:418` |
| 7  | `Blob get success: blkid={}`                                                                       | `hs/hs_blob_manager.cpp:445` |
| 8  | `Reading partial data: offset={}, len={}, full_blkid={}, read_blkid={}, start_blk={}, num_blks={}` | `hs/hs_blob_manager.cpp:479` |
| 9  | `Blob partial get success: blkid={}`                                                               | `hs/hs_blob_manager.cpp:503` |
| 10 | `Picked p_chunk_id={}, reserved_blks={}`                                                           | `hs/hs_blob_manager.cpp:553` |
| 11 | `Blob Delete request: pg={}, group={}, Shard={}, Blob={}`                                          | `hs/hs_blob_manager.cpp:596` |

### WARN (5)

| # | Message                                                  | Source                       |
|---|----------------------------------------------------------|------------------------------|
| 1 | `failed to put blob for pg={}, not leader`               | `hs/hs_blob_manager.cpp:125` |
| 2 | `failed to put blob for pg={}, not ready for traffic`    | `hs/hs_blob_manager.cpp:131` |
| 3 | `failed to free blob data blk, err={}, lsn={}, blkid={}` | `hs/hs_blob_manager.cpp:314` |
| 4 | `failed to del blob, not leader`                         | `hs/hs_blob_manager.cpp:600` |
| 5 | `failed to del blob, not ready for traffic`              | `hs/hs_blob_manager.cpp:606` |

### ERROR (7)

| # | Message                                                                    | Source                       |
|---|----------------------------------------------------------------------------|------------------------------|
| 1 | `input user key length > max_user_key_length {}`                           | `hs/hs_blob_manager.cpp:96`  |
| 2 | `Failed to insert into index table, err {}`                                | `hs/hs_blob_manager.cpp:227` |
| 3 | `Blob not found in index during get blob`                                  | `hs/hs_blob_manager.cpp:384` |
| 4 | `Failed to get blob: err={}`                                               | `hs/hs_blob_manager.cpp:423` |
| 5 | `Invalid offset length requested in get blob offset={} len={} size={}`     | `hs/hs_blob_manager.cpp:433` |
| 6 | `Failed to read partial data: err={}`                                      | `hs/hs_blob_manager.cpp:487` |
| 7 | `replication message header is corrupted with crc error, lsn={} header={}` | `hs/hs_blob_manager.cpp:650` |

---

## Module: `shardmgr`

**Description:** Shard create/seal/recover operations with per-request structured context.

**Macros:** `SLOGT/SLOGD/SLOGI/SLOGW/SLOGE/SLOGC`

**Log prefix:** `[trace_id={}, shardID=0x{:x}, pg={}, shard=0x{:x}]`

**Source files:** `homestore_backend/hs_shard_manager.cpp`

### DEBUG (10)

| #  | Message                                                                            | Source                        |
|----|------------------------------------------------------------------------------------|-------------------------------|
| 1  | `Create shard request: pg={}, size={}`                                             | `hs/hs_shard_manager.cpp:199` |
| 2  | `vchunk_id={}`                                                                     | `hs/hs_shard_manager.cpp:230` |
| 3  | `Shard created success.`                                                           | `hs/hs_shard_manager.cpp:287` |
| 4  | `Seal shard request: is_open={}`                                                   | `hs/hs_shard_manager.cpp:302` |
| 5  | `Seal shard request: Shard sealed success, is_open={}`                             | `hs/hs_shard_manager.cpp:378` |
| 6  | `rollback create shard message, type={}, lsn= {}`                                  | `hs/hs_shard_manager.cpp:425` |
| 7  | `rollback seal shard message, type={}, lsn={}`                                     | `hs/hs_shard_manager.cpp:432` |
| 8  | `shard already exist, this should happen in log replay case,  skip creating shard` | `hs/hs_shard_manager.cpp:464` |
| 9  | `Commit done for creating shard`                                                   | `hs/hs_shard_manager.cpp:513` |
| 10 | `Commit done for sealing shard at lsn={}`                                          | `hs/hs_shard_manager.cpp:574` |

### WARN (7)

| # | Message                                             | Source                        |
|---|-----------------------------------------------------|-------------------------------|
| 1 | `no available chunk left to create shard for pg={}` | `hs/hs_shard_manager.cpp:205` |
| 2 | `pg={} not found`                                   | `hs/hs_shard_manager.cpp:305` |
| 3 | `failed to get repl dev instance for pg={}`         | `hs/hs_shard_manager.cpp:318` |
| 4 | `failed to seal shard, not leader`                  | `hs/hs_shard_manager.cpp:324` |
| 5 | `failed to seal shard, not ready for traffic`       | `hs/hs_shard_manager.cpp:330` |
| 6 | `failed to seal shard, vchunk id not found`         | `hs/hs_shard_manager.cpp:337` |
| 7 | `try to seal an unopened shard, current_state={}`   | `hs/hs_shard_manager.cpp:536` |

### ERROR (1)

| # | Message                                                            | Source                        |
|---|--------------------------------------------------------------------|-------------------------------|
| 1 | `got {} when creating shard at leader, failed to create shard {}!` | `hs/hs_shard_manager.cpp:280` |

---

## Module: `gcmgr`

**Description:** Garbage collection scheduling, chunk selection, blob copy, and GC task lifecycle.

**Macros:** `GCLOGT/GCLOGD/GCLOGI/GCLOGW/GCLOGE/GCLOGC, or LOGINFOMOD(gcmgr, ...)`

**Log prefix:** `[gc_task_id={}, pg_id={}, shard_id=0x{:x}]`

**Source files:** `homestore_backend/gc_manager.cpp`

### DEBUG (34)

| #  | Message                                                                                          | Source                   |
|----|--------------------------------------------------------------------------------------------------|--------------------------|
| 1  | `gc scan chunk_id={}, use_blks={}, available_blks={}, total_blks={}, defrag_blks={}, `           | `hs/gc_manager.cpp:256`  |
| 2  | `chunk_id={} is added to reserved chunk queue`                                                   | `hs/gc_manager.cpp:510`  |
| 3  | `chunk_id={} belongs to no pg, not eligible for gc`                                              | `hs/gc_manager.cpp:522`  |
| 4  | `chunk_id={} belongs to pg {}, which is not eligible for gc at this moment!`                     | `hs/gc_manager.cpp:530`  |
| 5  | `start emergent gc task : move_from_chunk_id={}, priority={}`                                    | `hs/gc_manager.cpp:543`  |
| 6  | `start gc task : move_from_chunk_id={}, priority={}`                                             | `hs/gc_manager.cpp:552`  |
| 7  | `{} pending gc tasks to be completed for pg={}, wait for 2 seconds!`                             | `hs/gc_manager.cpp:574`  |
| 8  | `all pending gc tasks for pg_id={} are completed`                                                | `hs/gc_manager.cpp:583`  |
| 9  | `decrease pending gc task num for pg_id={}, now it is {}`                                        | `hs/gc_manager.cpp:592`  |
| 10 | `pending gc task num for pg_id={} is already 0, no need to decrease it`                          | `hs/gc_manager.cpp:596`  |
| 11 | `increase pending gc task num for pg_id={}, now it is {}`                                        | `hs/gc_manager.cpp:603`  |
| 12 | `start handling recovered gc task: move_from_chunk_id={}, move_to_chunk_id={}, priority={}`      | `hs/gc_manager.cpp:616`  |
| 13 | `finish handling recovered gc task: move_from_chunk_id={}, move_to_chunk_id={}, priority={}`     | `hs/gc_manager.cpp:674`  |
| 14 | `remove tombstone when updating pg index after data copy blob_id={}, move_from_chunk={}, `       | `hs/gc_manager.cpp:740`  |
| 15 | `An already upated blob index found during recovery, which is expected. blob_id={}, `            | `hs/gc_manager.cpp:752`  |
| 16 | `will replace blob_id={}, move_from_chunk={}, move_to_chunk={} from blk_id={} to blk_id={}`      | `hs/gc_manager.cpp:772`  |
| 17 | `successfully update index table, ret={}, move_from_chunk={}, move_to_chunk={}, blob_id={}`      | `hs/gc_manager.cpp:801`  |
| 18 | `empty shard found in move_from_chunk={}, skip`                                                  | `hs/gc_manager.cpp:930`  |
| 19 | `{} valid blobs found in move_from_chunk={}`                                                     | `hs/gc_manager.cpp:933`  |
| 20 | `successfully read blob from move_from_chunk={}, blob_id={}, pba={}`                             | `hs/gc_manager.cpp:984`  |
| 21 | `successfully insert new key to gc index table for `                                             | `hs/gc_manager.cpp:1036` |
| 22 | `successfully copy blobs from move_from_chunk={} to move_to_chunk={}`                            | `hs/gc_manager.cpp:1073` |
| 23 | `all valid blobs are copied from move_from_chunk={} to move_to_chunk={}`                         | `hs/gc_manager.cpp:1076` |
| 24 | `successfully commit_blk in move_to_chunk={}, commit_blk_id={}`                                  | `hs/gc_manager.cpp:1092` |
| 25 | `no used blks in move_to_chunk={}, so no need to commit_blk`                                     | `hs/gc_manager.cpp:1095` |
| 26 | `remove tombstone successfully, ret={}, move_from_chunk={}, move_to_chunk={}`                    | `hs/gc_manager.cpp:1125` |
| 27 | `data copied successfully for move_from_chunk={} to move_to_chunk={}`                            | `hs/gc_manager.cpp:1128` |
| 28 | `clear all rreqs on chunk={} before resetting it`                                                | `hs/gc_manager.cpp:1148` |
| 29 | `reset chunk={} before using it for gc`                                                          | `hs/gc_manager.cpp:1152` |
| 30 | `vchunk_id={} has been updated from move_from_chunk={} to move_to_chunk={}, final state is `     | `hs/gc_manager.cpp:1272` |
| 31 | `start process gc task for move_from_chunk={} with priority={} `                                 | `hs/gc_manager.cpp:1295` |
| 32 | `task for move_from_chunk={} to move_to_chunk={} with priority={} start copying data`            | `hs/gc_manager.cpp:1319` |
| 33 | `gc task for move_from_chunk={} to move_to_chunk={} with priority={} start replacing blob index` | `hs/gc_manager.cpp:1371` |
| 34 | `{} blks are reclaimed in this gc task!`                                                         | `hs/gc_manager.cpp:1454` |

### INFO (15)

| #  | Message                                                                                     | Source                   |
|----|---------------------------------------------------------------------------------------------|--------------------------|
| 1  | `start gc actor for pdev={}`                                                                | `hs/gc_manager.cpp:126`  |
| 2  | `gc scheduler timer has started, interval is set to {} seconds`                             | `hs/gc_manager.cpp:152`  |
| 3  | `stop gc scheduler timer`                                                                   | `hs/gc_manager.cpp:163`  |
| 4  | `stop gc actor for pdev={}`                                                                 | `hs/gc_manager.cpp:176`  |
| 5  | `pdev gc actor not found for pdev_id={}, chunk={}`                                          | `hs/gc_manager.cpp:197`  |
| 6  | `create new gc actor for pdev_id: {}`                                                       | `hs/gc_manager.cpp:210`  |
| 7  | `pdev gc actor already exists for pdev_id: {}`                                              | `hs/gc_manager.cpp:212`  |
| 8  | `pdev_id={} already has {}/{} pending normal gc tasks, skipping submission this scan cycle` | `hs/gc_manager.cpp:369`  |
| 9  | `gc task for chunk_id={} on pdev_id={} has been submitted and successfully completed `      | `hs/gc_manager.cpp:403`  |
| 10 | `submitted gc task for chunk_id={} on pdev_id={}, garbage_ratio_pct={}, tier={}, `          | `hs/gc_manager.cpp:417`  |
| 11 | `pdev_id={} scan complete: submitted {} new gc tasks (total pending now ~{}), `             | `hs/gc_manager.cpp:426`  |
| 12 | `pdev gc actor for pdev_id={} has started, {} threads for normal gc and {} threads for egc` | `hs/gc_manager.cpp:485`  |
| 13 | `pdev gc actor for pdev_id={} has stopped`                                                  | `hs/gc_manager.cpp:500`  |
| 14 | `gc task superblk is not deleted to simulate recovery`                                      | `hs/gc_manager.cpp:1414` |
| 15 | `gc actor for pdev_id={} is destroyed`                                                      | `hs/gc_manager.cpp:1471` |

### WARN (26)

| #  | Message                                                                                                      | Source                   |
|----|--------------------------------------------------------------------------------------------------------------|--------------------------|
| 1  | `the disk for chunk {} is not found, probably lost, skip recovering gc mateblk for this chunk!`              | `hs/gc_manager.cpp:110`  |
| 2  | `gc scheduler timer is not running, no need to stop it`                                                      | `hs/gc_manager.cpp:157`  |
| 3  | `got false after add_gc_task for chunk_id={} on pdev_id={}, it means we cannot mark `                        | `hs/gc_manager.cpp:408`  |
| 4  | `pdev gc actor for pdev_id={} is already stopped, no need to stop again!`                                    | `hs/gc_manager.cpp:492`  |
| 5  | `pdev gc actor for pdev_id={} is not started yet or already stopped, cannot add gc task!`                    | `hs/gc_manager.cpp:515`  |
| 6  | `fail to submit gc task for chunk_id={}, priority={}`                                                        | `hs/gc_manager.cpp:560`  |
| 7  | `Divergence!!! existing pbas chunk={} should be equal to move_from_chunk={}, blob_id={}, `                   | `hs/gc_manager.cpp:765`  |
| 8  | `no shard found in move_from_chunk, chunk_id={}`                                                             | `hs/gc_manager.cpp:845`  |
| 9  | `can not range remove blobs with tombstone in pg index table, status={}`                                     | `hs/gc_manager.cpp:906`  |
| 10 | `blob_id={} is expected to be in move_from_chunk={}, but its pba={} is not, `                                | `hs/gc_manager.cpp:942`  |
| 11 | `fail to remove tombstone, ret={}`                                                                           | `hs/gc_manager.cpp:1122` |
| 12 | `fail to purge gc index for chunk={}`                                                                        | `hs/gc_manager.cpp:1168` |
| 13 | `the number of copied blobs number {} is not the same as the number of valid blobs number {} from gc index ` | `hs/gc_manager.cpp:1205` |
| 14 | `can not find copied blob in copied_blobs for move_to_chunk={}, blob_id={}`                                  | `hs/gc_manager.cpp:1219` |
| 15 | `pba of copied blob is not the same as that in gc index table for move_to_chunk={}, blob_id={}, `            | `hs/gc_manager.cpp:1227` |
| 16 | `copied blobs do not match those in gc index table, start printing copied blobs:`                            | `hs/gc_manager.cpp:1238` |
| 17 | `copied blob: move_to_chunk={}, blob_id={}, pba={}`                                                          | `hs/gc_manager.cpp:1242` |
| 18 | `start printing valid blobs from gc index table:`                                                            | `hs/gc_manager.cpp:1245` |
| 19 | `valid blob: move_to_chunk={}, blob_id={}, pba={}`                                                           | `hs/gc_manager.cpp:1248` |
| 20 | `copied blobs are not the same as the valid blobs got from gc index table`                                   | `hs/gc_manager.cpp:1252` |
| 21 | `move_from_chunk={} is expected to in GC state but not!`                                                     | `hs/gc_manager.cpp:1299` |
| 22 | `can not purge move_to_chunk={}`                                                                             | `hs/gc_manager.cpp:1326` |
| 23 | `failed to copy data from move_from_chunk={} to move_to_chunk={} with priority={}`                           | `hs/gc_manager.cpp:1332` |
| 24 | `failed to get valid blob indexes from gc index table for move_to_chunk={}`                                  | `hs/gc_manager.cpp:1340` |
| 25 | `copied blobs are not the same as the valid blobs got from gc index table for move_to_chunk={}`              | `hs/gc_manager.cpp:1346` |
| 26 | `expect gc index table and blk allocator to be flushed but failed!`                                          | `hs/gc_manager.cpp:1355` |

### ERROR (20)

| #  | Message                                                                                          | Source                   |
|----|--------------------------------------------------------------------------------------------------|--------------------------|
| 1  | `chunk {} not found when submit gc task!`                                                        | `hs/gc_manager.cpp:183`  |
| 2  | `chunk {} has no garbage to be reclaimed, skip gc for this chunk!`                               | `hs/gc_manager.cpp:190`  |
| 3  | `pdev gc actor not found for pdev_id: {}`                                                        | `hs/gc_manager.cpp:220`  |
| 4  | `gc_garbage_rate_threshold_low={} exceeds gc_garbage_rate_threshold={}, `                        | `hs/gc_manager.cpp:280`  |
| 5  | `pdev gc actor for pdev_id={} is already started, no need to start again!`                       | `hs/gc_manager.cpp:466`  |
| 6  | `Failed to query blobs in gc index table for move_to_chunk={}, index ret={}`                     | `hs/gc_manager.cpp:694`  |
| 7  | `Failed to update blob in pg index table, move_from_chunk={}, error_status={}, move_to_chunk={}` | `hs/gc_manager.cpp:793`  |
| 8  | `last shard in move_from_chunk={} has a state of OPEN in a normal gc task, which is unexpected!` | `hs/gc_manager.cpp:860`  |
| 9  | `Failed to query blobs in index table for status={}`                                             | `hs/gc_manager.cpp:925`  |
| 10 | `Failed to read blob from move_from_chunk={}, blob_id={}, err={}, `                              | `hs/gc_manager.cpp:976`  |
| 11 | `blob verification fails for move_from_chunk={}, blob_id={}, pba={}`                             | `hs/gc_manager.cpp:995`  |
| 12 | `Failed to write blob to move_to_chunk={}, blob_id={}, err={}, `                                 | `hs/gc_manager.cpp:1013` |
| 13 | `Failed to insert new key to gc index table for `                                                | `hs/gc_manager.cpp:1029` |
| 14 | `Failed to copy blob for move_to_chunk={}, will cancel this task`                                | `hs/gc_manager.cpp:1059` |
| 15 | `Failed to copy all blobs from move_from_chunk={} to move_to_chunk={}`                           | `hs/gc_manager.cpp:1069` |
| 16 | `fail to commit_blk for move_to_chunk={}, commit_blk_id={}`                                      | `hs/gc_manager.cpp:1088` |
| 17 | `Failed to query blobs after purging reserved chunk={} in gc index table, index ret={}`          | `hs/gc_manager.cpp:1180` |
| 18 | `gc index table is not empty for chunk={} after purging, valid_blob_indexes.size={}`             | `hs/gc_manager.cpp:1186` |
| 19 | `move_from_chunk={} to move_to_chunk={} with priority={} failed!`                                | `hs/gc_manager.cpp:1283` |
| 20 | `failed to replace blob index, move_from_chunk={} to move_to_chunk={} with priority={}`          | `hs/gc_manager.cpp:1404` |

---

## Module: `scrubmgr`

**Description:** Scrubber scheduling, shard/blob integrity verification, and repair operations.

**Macros:** `SCRUBLOGD/SCRUBLOGI/SCRUBLOGW/SCRUBLOGE/SCRUBLOGC`

**Log prefix:** `[pg_id={}, task_id={}]`

**Source files:** `homestore_backend/scrub_manager.cpp`

### DEBUG (43)

| #  | Message                                                                                             | Source                      |
|----|-----------------------------------------------------------------------------------------------------|-----------------------------|
| 1  | `batch scrub for peer {} is not active, skipping timeout check`                                     | `hs/scrub_manager.cpp:134`  |
| 2  | `batch scrub for peer {} has not timed out yet, skipping`                                           | `hs/scrub_manager.cpp:139`  |
| 3  | `pg={} is not eligible for any scrubbing`                                                           | `hs/scrub_manager.cpp:199`  |
| 4  | `receive scrub req: {}`                                                                             | `hs/scrub_manager.cpp:312`  |
| 5  | `handling meta scrub req for pg {}`                                                                 | `hs/scrub_manager.cpp:367`  |
| 6  | `handling blob scrub req for pg {}, scrub_type={}`                                                  | `hs/scrub_manager.cpp:373`  |
| 7  | `successfully sent scrub result to peer {} in pg {}, scrub_type:{}`                                 | `hs/scrub_manager.cpp:400`  |
| 8  | `commit lsn {} is less than scrub lsn {}, wait for 1 second before retrying, retry times {}/{}`     | `hs/scrub_manager.cpp:420`  |
| 9  | `handling blob scrub req for pg {}, req_id={}, scrub_lsn={}, scrub_type={}`                         | `hs/scrub_manager.cpp:469`  |
| 10 | `pg_id={}, req_id={}, scrub_lsn={}, shallow blob scrub completed, return {} blobs in range [{},{})` | `hs/scrub_manager.cpp:531`  |
| 11 | `add entry to blob scrub result: shard_id={}, blob_id={}`                                           | `hs/scrub_manager.cpp:595`  |
| 12 | `pg_id={}, req_id={}, deep blob scrub completed, found {} blobs in range [{},{}] to [{},{})`        | `hs/scrub_manager.cpp:603`  |
| 13 | `handling meta scrub req for pg {}, req_id={}, scrub_lsn={}`                                        | `hs/scrub_manager.cpp:629`  |
| 14 | `received meta scrub req for pg {}, req_id={}, scrub_lsn={}, start_shard_id={}, end_shard_id={}, `  | `hs/scrub_manager.cpp:655`  |
| 15 | `scrubbing pg meta of pg={}`                                                                        | `hs/scrub_manager.cpp:678`  |
| 16 | `add last entry, shard_id={}, blob_id={}`                                                           | `hs/scrub_manager.cpp:747`  |
| 17 | `meta scrub completed, checked {} shards in range [{},{}) to [{}, {}] in pg={}`                     | `hs/scrub_manager.cpp:751`  |
| 18 | `Starting handling {} scrub task, last_scrub_time={} =====`                                         | `hs/scrub_manager.cpp:830`  |
| 19 | `Starting META scrubbing`                                                                           | `hs/scrub_manager.cpp:887`  |
| 20 | `scrub task cancelled after meta scrub, skip blob scrub`                                            | `hs/scrub_manager.cpp:891`  |
| 21 | `meta scrub batch completed in range: {} to {}, scrub_lsn={}`                                       | `hs/scrub_manager.cpp:905`  |
| 22 | `no more shard to scrub, end meta scrub`                                                            | `hs/scrub_manager.cpp:909`  |
| 23 | `Starting {} blob scrubbing`                                                                        | `hs/scrub_manager.cpp:920`  |
| 24 | `scrub task cancelled during blob batch accumulation, stop`                                         | `hs/scrub_manager.cpp:929`  |
| 25 | `successfully complete {} scrub task!`                                                              | `hs/scrub_manager.cpp:981`  |
| 26 | `no active batch for peer {}, dropping result req_id={}`                                            | `hs/scrub_manager.cpp:1092` |
| 27 | `promise already fulfilled for peer {}, dropping result req_id={}`                                  | `hs/scrub_manager.cpp:1102` |
| 28 | `stale req_id from peer {}, expected={}, actual={}, dropping`                                       | `hs/scrub_manager.cpp:1109` |
| 29 | `peer {} completed batch, batch_scrub_result entries={}`                                            | `hs/scrub_manager.cpp:1162` |
| 30 | `successfully sent scrub req to peer {}, req_id={}, scrub_type={}`                                  | `hs/scrub_manager.cpp:1191` |
| 31 | `start scrubbing meta for shard range: {} to {}, last_blob_id={}, scrub_lsn={}`                     | `hs/scrub_manager.cpp:1201` |
| 32 | `complete meta range scrub: {}`                                                                     | `hs/scrub_manager.cpp:1231` |
| 33 | `start scrubbing blob for shard range: {} to {}, last_blob_id={}, scrub_lsn={}, scrub_type={}`      | `hs/scrub_manager.cpp:1276` |
| 34 | `complete blob range scrub: {}`                                                                     | `hs/scrub_manager.cpp:1307` |
| 35 | `scrub task cancelled, skip reconciliation`                                                         | `hs/scrub_manager.cpp:1327` |
| 36 | `no missing shard/blob in scrub report, no need to reconcile`                                       | `hs/scrub_manager.cpp:1335` |
| 37 | `reconcile check: shard {} confirmed absent on peer {}, removing from `                             | `hs/scrub_manager.cpp:1355` |
| 38 | `reconcile check: shard {} still present on peer {}, no change`                                     | `hs/scrub_manager.cpp:1361` |
| 39 | `reconcile check: shard_id={}, blob_id={} confirmed absent on peer {}, `                            | `hs/scrub_manager.cpp:1384` |
| 40 | `reconcile check: shard_id={}, blob_id={} still present on peer {}, no change`                      | `hs/scrub_manager.cpp:1390` |
| 41 | `reconciliation cleared all missing items after {} retries`                                         | `hs/scrub_manager.cpp:1409` |
| 42 | `successfully checked {} existence in peer {}, shard_id={}, blob_id={}, exists={}`                  | `hs/scrub_manager.cpp:1456` |
| 43 | `[pg={}] Shallow scrub merge completed!`                                                            | `hs/scrub_manager.cpp:1910` |

### INFO (26)

| #  | Message                                                                                                | Source                      |
|----|--------------------------------------------------------------------------------------------------------|-----------------------------|
| 1  | `scrub task is already cancelled, no need to cancel again`                                             | `hs/scrub_manager.cpp:46`   |
| 2  | `scrub task is cancelled`                                                                              | `hs/scrub_manager.cpp:56`   |
| 3  | `pg={} is eligible for deep scrub, submit scrub task`                                                  | `hs/scrub_manager.cpp:164`  |
| 4  | `deep scrub is completed for pg={}`                                                                    | `hs/scrub_manager.cpp:172`  |
| 5  | `pg={} is eligible for shallow scrub, submit scrub task`                                               | `hs/scrub_manager.cpp:185`  |
| 6  | `shallow scrub is completed for pg={}`                                                                 | `hs/scrub_manager.cpp:193`  |
| 7  | `scrub task queue is stopped, no need to handle scrub task anymore!`                                   | `hs/scrub_manager.cpp:245`  |
| 8  | `scrub manager started!`                                                                               | `hs/scrub_manager.cpp:273`  |
| 9  | `stop scrub scheduler timer`                                                                           | `hs/scrub_manager.cpp:277`  |
| 10 | `scrub manager stopped!`                                                                               | `hs/scrub_manager.cpp:308`  |
| 11 | `commit lsn {} is greater than or equal to scrub lsn {}, wait successfully`                            | `hs/scrub_manager.cpp:416`  |
| 12 | `submit a scrub task for pg={}, deep_scrub={}, trigger_type={}`                                        | `hs/scrub_manager.cpp:759`  |
| 13 | `cancel scrub task for pg={}`                                                                          | `hs/scrub_manager.cpp:822`  |
| 14 | `cleared SCRUBBING state for pg={}`                                                                    | `hs/scrub_manager.cpp:854`  |
| 15 | `added new scrub superblock for pg={}`                                                                 | `hs/scrub_manager.cpp:986`  |
| 16 | `cannot find pg={}!`                                                                                   | `hs/scrub_manager.cpp:988`  |
| 17 | `no scrub superblock found for pg={}, no need to remove`                                               | `hs/scrub_manager.cpp:1003` |
| 18 | `removed pg={} in scrub manager!`                                                                      | `hs/scrub_manager.cpp:1007` |
| 19 | `cannot find pg={}, destroy stale scrub superblock`                                                    | `hs/scrub_manager.cpp:1023` |
| 20 | `loaded scrub superblock for pg={}, last_deep_scrub_time={}, last_shallow_scrub_time={}`               | `hs/scrub_manager.cpp:1031` |
| 21 | `skip updating scrub superblock for pg={} since there is no scrub progress update`                     | `hs/scrub_manager.cpp:1061` |
| 22 | `{}`                                                                                                   | `hs/scrub_manager.cpp:1732` |
| 23 | `[pg={}] Meta scrub merge completed: {} corrupted shard metas, {} inconsistent shard metas, `          | `hs/scrub_manager.cpp:1827` |
| 24 | `{}`                                                                                                   | `hs/scrub_manager.cpp:1861` |
| 25 | `{}`                                                                                                   | `hs/scrub_manager.cpp:1956` |
| 26 | `[pg={}] Deep blob scrub merge completed: {} missing blobs, {} corrupted blobs, {} inconsistent blobs` | `hs/scrub_manager.cpp:2016` |

### WARN (20)

| #  | Message                                                                                       | Source                      |
|----|-----------------------------------------------------------------------------------------------|-----------------------------|
| 1  | `timeout waiting for scrub result from peer {}, req_id={}, retry={}/{}`                       | `hs/scrub_manager.cpp:144`  |
| 2  | `scrub manager is not started yet, drop this req, req_id={}!`                                 | `hs/scrub_manager.cpp:318`  |
| 3  | `a scrub task is already running for pg={}, no need to submit another one!`                   | `hs/scrub_manager.cpp:766`  |
| 4  | `pg={} is not in HEALTHY state (current_state={}), cannot submit scrub task!`                 | `hs/scrub_manager.cpp:786`  |
| 5  | `pg={} scrub submission already in-flight, skip!`                                             | `hs/scrub_manager.cpp:796`  |
| 6  | `pg={} scrub task queue is closed/stopped, skip!`                                             | `hs/scrub_manager.cpp:809`  |
| 7  | `no running scrub task for pg={}, no need to cancel!`                                         | `hs/scrub_manager.cpp:818`  |
| 8  | `cannot find hs_pg to clear SCRUBBING state for pg={}!`                                       | `hs/scrub_manager.cpp:857`  |
| 9  | `scrub superblk not found for pg {}`                                                          | `hs/scrub_manager.cpp:1068` |
| 10 | `reconciliation finished after {} retries but {} missing shards and {} missing blobs remain`  | `hs/scrub_manager.cpp:1405` |
| 11 | `[pg={}] MetaScrubReport::merge: empty map, skip`                                             | `hs/scrub_manager.cpp:1738` |
| 12 | `[pg={}] MetaScrubReport::merge: first entry is null or not META, skip`                       | `hs/scrub_manager.cpp:1745` |
| 13 | `[pg={}] MetaScrubReport::merge: null, wrong type, or mismatched range from peer {}, skip`    | `hs/scrub_manager.cpp:1751` |
| 14 | `[pg={}] find corruption for META shard={} peer={}`                                           | `hs/scrub_manager.cpp:1780` |
| 15 | `[pg={}] ShallowScrubReport::merge: empty map, skip`                                          | `hs/scrub_manager.cpp:1867` |
| 16 | `[pg={}] ShallowScrubReport::merge: first entry is null, skip`                                | `hs/scrub_manager.cpp:1873` |
| 17 | `[pg={}] ShallowScrubReport::merge: null, wrong type, or mismatched range from peer {}, skip` | `hs/scrub_manager.cpp:1886` |
| 18 | `[pg={}] DeepScrubReport::merge: empty map, skip`                                             | `hs/scrub_manager.cpp:1962` |
| 19 | `[pg={}] DeepScrubReport::merge: first entry is null, skip`                                   | `hs/scrub_manager.cpp:1968` |
| 20 | `[pg={}] find corruption for blob shard_id={}, blob_id={}, peer={}`                           | `hs/scrub_manager.cpp:1991` |

### ERROR (43)

| #  | Message                                                                                            | Source                      |
|----|----------------------------------------------------------------------------------------------------|-----------------------------|
| 1  | `deep scrub failed for pg={}`                                                                      | `hs/scrub_manager.cpp:169`  |
| 2  | `report for deep scrub cannot be casted to DeepScrubReport for pg={}`                              | `hs/scrub_manager.cpp:175`  |
| 3  | `shallow scrub failed for pg={}`                                                                   | `hs/scrub_manager.cpp:190`  |
| 4  | `Shallow scrub report is null!`                                                                    | `hs/scrub_manager.cpp:205`  |
| 5  | `Deep scrub report is null!`                                                                       | `hs/scrub_manager.cpp:215`  |
| 6  | `cannot find scrub context for pg_id={}, fail to add scrub result!`                                | `hs/scrub_manager.cpp:324`  |
| 7  | `scrub req is null, cannot handle it!`                                                             | `hs/scrub_manager.cpp:333`  |
| 8  | `cannot find hs_pg for pg {}, fail to handle scrub req!`                                           | `hs/scrub_manager.cpp:340`  |
| 9  | `repl_dev is null for pg {}, fail to handle scrub req!`                                            | `hs/scrub_manager.cpp:346`  |
| 10 | `fail to handle scrub req for pg {}, scrub_type={}, drop it!`                                      | `hs/scrub_manager.cpp:382`  |
| 11 | `failed to send scrub result to peer {} in pg {}, scrub_type:{}, error={}`                         | `hs/scrub_manager.cpp:395`  |
| 12 | `repl_dev is null, cannot wait for scrub lsn commit!`                                              | `hs/scrub_manager.cpp:407`  |
| 13 | `blob scrub req is null, cannot handle it!`                                                        | `hs/scrub_manager.cpp:453`  |
| 14 | `invalid scrub req type for local_scrub_blob, pg_id={}, req_id={}, scrub_type={}, scrub_lsn={}`    | `hs/scrub_manager.cpp:463`  |
| 15 | `req_id={} cannot find hs_pg for pg={}, fail to do deep blob scrub!`                               | `hs/scrub_manager.cpp:474`  |
| 16 | `pg_id={}, req_id={}, commit lsn is not advanced to scrub lsn {} after waiting for a while, fail ` | `hs/scrub_manager.cpp:479`  |
| 17 | `Failed to read blob for deep scrub, shard_id={}, blob_id={}, error={}`                            | `hs/scrub_manager.cpp:574`  |
| 18 | `Blob verification failed for deep scrub, shard_id={}, blob_id={}`                                 | `hs/scrub_manager.cpp:584`  |
| 19 | `meta scrub req is null, cannot handle it!`                                                        | `hs/scrub_manager.cpp:612`  |
| 20 | `invalid scrub req type for local_scrub_meta, pg_id={}, req_id={}, scrub_type={}, scrub_lsn={}`    | `hs/scrub_manager.cpp:623`  |
| 21 | `cannot find hs_pg for pg={}, fail to scrub meta!`                                                 | `hs/scrub_manager.cpp:633`  |
| 22 | `received incorrect meta scrub req, start_shard_id={} > end_shard_id={}`                           | `hs/scrub_manager.cpp:638`  |
| 23 | `[pg={}] failed to query index table, error={}`                                                    | `hs/scrub_manager.cpp:740`  |
| 24 | `cannot find scrub superblk for pg={}, fail to submit scrub task!`                                 | `hs/scrub_manager.cpp:772`  |
| 25 | `cannot find hs_pg for pg={}, fail to submit scrub task!`                                          | `hs/scrub_manager.cpp:779`  |
| 26 | `cannot find hs_pg for this pg, fail this scrub task!`                                             | `hs/scrub_manager.cpp:864`  |
| 27 | `meta scrub failed for batch in range: {} to {}, scrub_lsn={}`                                     | `hs/scrub_manager.cpp:900`  |
| 28 | `{} blob scrub failed for shard range: {} to {}, scrub_lsn={}`                                     | `hs/scrub_manager.cpp:939`  |
| 29 | `{} blob scrub batch failed for shard range: {} to {}, scrub_lsn={}`                               | `hs/scrub_manager.cpp:958`  |
| 30 | `replication device is not available, cannot send scrub req to peer {}, req_id={}, `               | `hs/scrub_manager.cpp:1172` |
| 31 | `failed to send scrub req to peer {}, req_id={}, error={}, scrub_type={}`                          | `hs/scrub_manager.cpp:1188` |
| 32 | `scrub meta batch is failed, error={}`                                                             | `hs/scrub_manager.cpp:1222` |
| 33 | `scrub meta batch is failed, receive nullptr scrub result`                                         | `hs/scrub_manager.cpp:1227` |
| 34 | `scrub blob batch is failed, error={}`                                                             | `hs/scrub_manager.cpp:1298` |
| 35 | `scrub blob batch is failed, receive nullptr scrub result`                                         | `hs/scrub_manager.cpp:1303` |
| 36 | `failed to check shard existence in peer {}, shard {}, error: {}`                                  | `hs/scrub_manager.cpp:1347` |
| 37 | `failed to check blob existence in peer {}, shard_id={}, blob_id={}, error: {}`                    | `hs/scrub_manager.cpp:1375` |
| 38 | `failed to check {} existence in peer {}, blob {}, error code: {}`                                 | `hs/scrub_manager.cpp:1442` |
| 39 | `invalid response for {} existence check from peer {}, blob {}, response size={}`                  | `hs/scrub_manager.cpp:1449` |
| 40 | `scrub_req::load called with null or empty buffer`                                                 | `hs/scrub_manager.cpp:1517` |
| 41 | `scrub_req::load: GetSizePrefixedScrubReq returned null`                                           | `hs/scrub_manager.cpp:1523` |
| 42 | `scrub_result::load called with null or empty buffer`                                              | `hs/scrub_manager.cpp:1587` |
| 43 | `scrub_result::load: GetSizePrefixedScrubResult returned null`                                     | `hs/scrub_manager.cpp:1593` |

---

## Module: `base`

**Description:** Sisl default module used by bare LOGINFO/LOGDEBUG/LOGWARN/LOGERROR/LOGTRACE/LOGCRITICAL calls (no
explicit module). Primarily HTTP manager and miscellaneous paths.

**Macros:** `LOGDEBUG/LOGINFO/LOGWARN/LOGERROR/LOGTRACE/LOGCRITICAL (no MOD suffix — sisl default)`

**Log prefix:** `bare message (no structured prefix)`

**Source files:
** `homestore_backend/hs_http_manager.cpp`, `homestore_backend/replication_state_machine.cpp`, `homestore_backend/hs_pg_manager.cpp`, `homestore_backend/hs_homeobject.cpp`, `homestore_backend/snapshot_receive_handler.cpp`, `homestore_backend/hs_shard_manager.cpp`, `homestore_backend/index_kv.cpp`, `memory_backend/mem_shard_manager.cpp`

### DEBUG (6)

| # | Message                                                                          | Source                         |
|---|----------------------------------------------------------------------------------|--------------------------------|
| 1 | `chunk_id={} is not in chunk_to_shards_map, add it`                              | `hs/hs_shard_manager.cpp:698`  |
| 2 | `Not a leader, no need to yield leadership`                                      | `hs/hs_pg_manager.cpp:1095`    |
| 3 | `cannot find a candidate leader except current leader {}, candidate_priority={}` | `hs/hs_pg_manager.cpp:1122`    |
| 4 | `GC job {} for chunk {} in PG {} with priority={}`                               | `hs/hs_http_manager.cpp:1042`  |
| 5 | `Failed to get from index table [route={}]`                                      | `hs/index_kv.cpp:106`          |
| 6 | `Creating Shard [{}]: in pg={} of Size [{}b]`                                    | `mem/mem_shard_manager.cpp:22` |

### INFO (42)

| #  | Message                                                                                    | Source                                |
|----|--------------------------------------------------------------------------------------------|---------------------------------------|
| 1  | `HomeObject DEBUG version: {}`                                                             | `hs/hs_homeobject.cpp:43`             |
| 2  | `HomeObject RELEASE version: {}`                                                           | `hs/hs_homeobject.cpp:45`             |
| 3  | `PG remove member, member={}, trace_id={}`                                                 | `hs/hs_pg_manager.cpp:512`            |
| 4  | `Setting up HomeObject HTTP routes`                                                        | `hs/hs_http_manager.cpp:47`           |
| 5  | `Received reconcile leader request for pg_id {}`                                           | `hs/hs_http_manager.cpp:129`          |
| 6  | `Checking candidate {} for pg_id {}`                                                       | `hs/hs_http_manager.cpp:152`          |
| 7  | `Received yield leadership request for pg_id {} to follower, candidate={}`                 | `hs/hs_http_manager.cpp:172`          |
| 8  | `Received snapshot creation request for pg_id={}, compact_lsn={}, wait_for_commit={}`      | `hs/hs_http_manager.cpp:200`          |
| 9  | `Flipping learner flag, pg_id={}, member_id={}, learner={}, commit_quorum={}, tid={}`      | `hs/hs_http_manager.cpp:404`          |
| 10 | `Remove member, pg_id={}, member_id={}, commit_quorum={}, tid={}`                          | `hs/hs_http_manager.cpp:431`          |
| 11 | `Clean replace member task, pg_id={}, task_id={}, commit_quorum={}, tid={}`                | `hs/hs_http_manager.cpp:460`          |
| 12 | `Reconcile membership for pg_id={}`                                                        | `hs/hs_http_manager.cpp:482`          |
| 13 | `list pg replace member tasks, count={}, tid={}`                                           | `hs/hs_http_manager.cpp:511`          |
| 14 | `Exit pg request received for group_id={}, peer_id={}, tid={}`                             | `hs/hs_http_manager.cpp:571`          |
| 15 | `Received trigger_pg_scrub request for pg_id={}, deep={}`                                  | `hs/hs_http_manager.cpp:634`          |
| 16 | `Received trigger_gc request for chunk_id {}`                                              | `hs/hs_http_manager.cpp:791`          |
| 17 | `Received trigger_gc request for pg_id {}`                                                 | `hs/hs_http_manager.cpp:854`          |
| 18 | `GC job {} stopping GC scan timer`                                                         | `hs/hs_http_manager.cpp:878`          |
| 19 | `GC job {} completed: total={}, success={}, failed={}`                                     | `hs/hs_http_manager.cpp:883`          |
| 20 | `GC job {} restarting GC scan timer`                                                       | `hs/hs_http_manager.cpp:885`          |
| 21 | `Received trigger_gc request for all chunks`                                               | `hs/hs_http_manager.cpp:889`          |
| 22 | `GC job {} no PGs found, marking as completed`                                             | `hs/hs_http_manager.cpp:904`          |
| 23 | `GC job {} will process {} PGs`                                                            | `hs/hs_http_manager.cpp:915`          |
| 24 | `GC job {} stopping GC scan timer`                                                         | `hs/hs_http_manager.cpp:916`          |
| 25 | `GC job {} completed: total={}, success={}, failed={}`                                     | `hs/hs_http_manager.cpp:927`          |
| 26 | `GC job {} restarting GC scan timer`                                                       | `hs/hs_http_manager.cpp:929`          |
| 27 | `query job {} status!`                                                                     | `hs/hs_http_manager.cpp:981`          |
| 28 | `query all job status!`                                                                    | `hs/hs_http_manager.cpp:988`          |
| 29 | `GC job {} draining pending GC tasks for PG {}`                                            | `hs/hs_http_manager.cpp:1011`         |
| 30 | `GC job {} processing PG {} with {} chunks`                                                | `hs/hs_http_manager.cpp:1017`         |
| 31 | `All GC tasks for PG {} have been processed`                                               | `hs/hs_http_manager.cpp:1058`         |
| 32 | `GC job {} resumed accepting requests for PG {}`                                           | `hs/hs_http_manager.cpp:1065`         |
| 33 | `Query scrub job {} status`                                                                | `hs/hs_http_manager.cpp:1076`         |
| 34 | `Query all scrub job status`                                                               | `hs/hs_http_manager.cpp:1098`         |
| 35 | `Cancel scrub job {}`                                                                      | `hs/hs_http_manager.cpp:1182`         |
| 36 | `Simulating a segv with dereferencing nullptr={}`                                          | `hs/hs_http_manager.cpp:1263`         |
| 37 | `Resuming snapshot receiver context from lsn={} pg={} shardID=0x{:x}, pg={}, shard=0x{:x}` | `hs/snapshot_receive_handler.cpp:400` |
| 38 | `Update snp_info sb, CP Flush {}`                                                          | `hs/snapshot_receive_handler.cpp:486` |
| 39 | `Found snapshot info meta blk`                                                             | `hs/snapshot_receive_handler.cpp:520` |
| 40 | `Snapshot info meta blk recovery completed`                                                | `hs/snapshot_receive_handler.cpp:531` |
| 41 | `Found snapshot shard list meta blk`                                                       | `hs/snapshot_receive_handler.cpp:535` |
| 42 | `Snapshot shard list meta blk recovery completed`                                          | `hs/snapshot_receive_handler.cpp:546` |

### WARN (1)

| # | Message                                                                                      | Source                                 |
|---|----------------------------------------------------------------------------------------------|----------------------------------------|
| 1 | `Found duplicate snapshot context superblk for group_id={}, current lsn={}, existing lsn={}` | `hs/replication_state_machine.cpp:797` |

### ERROR (4)

| # | Message                                                                                | Source                                 |
|---|----------------------------------------------------------------------------------------|----------------------------------------|
| 1 | `Failed to query blobs in index table for pg={}`                                       | `hs/hs_pg_manager.cpp:1459`            |
| 2 | `http server not available`                                                            | `hs/hs_http_manager.cpp:104`           |
| 3 | `setup routes failed, {}`                                                              | `hs/hs_http_manager.cpp:109`           |
| 4 | `failed to submit emergent gc task for chunk_id={} , lsn={}, will retry again if new ` | `hs/replication_state_machine.cpp:902` |

---
