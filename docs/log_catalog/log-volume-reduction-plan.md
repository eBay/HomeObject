# Log Volume Reduction Plan

## Background

SM generates excessive log volume — normally 1M+ across the entire SDS account,
peaking at ~3M. This causes logs from this and other SDS services to be throttled by the
ingestion limit, resulting in incomplete Sherlock coverage.

This plan samples one SM node, measures its log volume during a normal hour and a peak hour, and
designs a viable reduction strategy.

Current production log mode:

```
base:debug,cm_client:debug,blobmgr:debug,shardmgr:debug,replication:trace,gcmgr:debug (default)
```

Analysis approaches the problem from two angles:

1. The normal one-hour statistical breakdown
2. The read/write path per-operation analysis (see `blob-op-log-analysis.md`), which scales with request throughput.

---

## Observed Baseline Breakdown

Two sampling windows were taken from namespace `nuobject2sm`:

- **Normal hour**: 2026-09-15 00:13–01:13
- **Peak hour**: 2026-09-15 09:19–10:19

The normal-hour breakdown:

| Source                                | Lines/hr   | % of total | Pattern matched                   |
|---------------------------------------|------------|------------|-----------------------------------|
| GC scan (per-chunk debug)             | 21,600     | 43.5%      | `gc scan chunk_id`                |
| Raft heartbeat ("no entry")           | 25,000     | 50.4%      | `Raft channel: Received no entry` |
| **All other (read/write/background)** | **3,000**  | **6.1%**   | —                                 |
| **Total**                             | **49,600** | 100%       |                                   |

Both noise sources are independent of request traffic. Even at peak load, they remain dominant,
accounting for over 50% of total log volume — making them the highest-priority targets regardless of traffic pattern.

After filtering out the two dominant noise sources (`gc scan chunk_id` and `Raft channel: Received no entry`),
the remaining **3,000 lines/hr** at normal load break down as follows (TOP 20 by source location):

| Count/hr | File:line                     | Function                         |
|----------|-------------------------------|----------------------------------|
| 188      | `services.hpp:89`             | `pre_process`                    |
| 186      | `hs_blob_manager.cpp:373`     | `_get_blob`                      |
| 172      | `hs_blob_manager.cpp:411`     | `_get_blob_data`                 |
| 162      | `raft_repl_dev.cpp:2633`      | `get_cp_ctx`                     |
| 104      | `common.cpp:179`              | `set_lsn`                        |
| 98       | `raft_repl_dev.cpp:1261`      | `applier_create_req`             |
| 98       | `append_blk_allocator.cpp:90` | `alloc`                          |
| 92       | `raft_state_machine.cpp:312`  | `unlink_lsn_to_req`              |
| 88       | `raft_repl_dev.cpp:1288`      | `applier_create_req`             |
| 88       | `index_kv.cpp:106`            | `get_blob_from_index_table`      |
| 86       | `repl_log_store.cpp:23`       | `append`                         |
| 86       | `raft_repl_dev.cpp:1712`      | `handle_commit`                  |
| 79       | `common.cpp:243`              | `release_data`                   |
| 77       | `raft_state_machine.cpp:62`   | `localize_journal_entry_prepare` |
| 76       | `raft_state_machine.cpp:214`  | `commit_ext`                     |
| 74       | `raft_repl_dev.cpp:1303`      | `notify_after_data_written`      |
| 72       | `raft_repl_dev.cpp:1167`      | `on_push_data_received`          |
| 71       | `raft_repl_dev.cpp:2435`      | `raft_event`                     |
| 63       | `hs_blob_manager.cpp:546`     | `blob_put_get_blk_alloc_hints`   |
| 59       | `repl_log_store.cpp:59`       | `end_of_append_batch`            |
| 59       | `raft_state_machine.cpp:198`  | `pre_commit_ext`                 |

The same filter applied to the peak-hour window — after removing the two dominant noise sources — yields the following
TOP 20:

| Count/hr | File:line                     | Function                         |
|----------|-------------------------------|----------------------------------|
| 395      | `services.hpp:89`             | `pre_process`                    |
| 391      | `hs_blob_manager.cpp:411`     | `_get_blob_data`                 |
| 382      | `hs_blob_manager.cpp:373`     | `_get_blob`                      |
| 379      | `bitset.hpp:1175`             | `resize_impl`                    |
| 204      | `repl_log_store.cpp:59`       | `end_of_append_batch`            |
| 204      | `raft_state_machine.cpp:312`  | `unlink_lsn_to_req`              |
| 204      | `raft_state_machine.cpp:198`  | `pre_commit_ext`                 |
| 198      | `common.cpp:179`              | `set_lsn`                        |
| 194      | `repl_log_store.cpp:23`       | `append`                         |
| 188      | `raft_repl_dev.cpp:1288`      | `applier_create_req`             |
| 183      | `raft_repl_dev.cpp:1712`      | `handle_commit`                  |
| 179      | `raft_repl_dev.cpp:2435`      | `raft_event`                     |
| 173      | `raft_state_machine.cpp:62`   | `localize_journal_entry_prepare` |
| 163      | `raft_state_machine.cpp:214`  | `commit_ext`                     |
| 108      | `append_blk_allocator.cpp:90` | `alloc`                          |
| 100      | `index_kv.cpp:106`            | `get_blob_from_index_table`      |
| 98       | `raft_repl_dev.cpp:1167`      | `on_push_data_received`          |
| 93       | `common.cpp:243`              | `release_data`                   |
| 92       | `raft_repl_dev.cpp:1261`      | `applier_create_req`             |
| 88       | `raft_repl_dev.cpp:1303`      | `notify_after_data_written`      |

---

## P0 Changes — Eliminate Two Dominant Noise Sources

### Change 1: Restructure `gcmgr` log levels — INFO for task lifecycle, TRACE for scan/eligibility/per-blob

GC task logs are split into three tiers. Existing INFO / WARN / ERROR sites are unchanged.

**INFO** — task entry and exit, always visible without enabling `gcmgr:debug`:

Promote `gc_manager.cpp:1319` so the task-start log includes move_to_chunk

| Line                  | Message template                                                                                              | Change       |
|-----------------------|---------------------------------------------------------------------------------------------------------------|--------------|
| `gc_manager.cpp:616`  | `start handling recovered gc task: move_from_chunk_id={}, move_to_chunk_id={}, priority={}`                   | debug → info |
| `gc_manager.cpp:674`  | `finish handling recovered gc task: move_from_chunk_id={}, move_to_chunk_id={}, priority={}`                  | debug → info |
| `gc_manager.cpp:1272` | `vchunk_id={} has been updated from move_from_chunk={} to move_to_chunk={}, final state is ... is completed!` | debug → info |
| `gc_manager.cpp:1295` | `start process gc task for move_from_chunk={} with priority={} `                                              | debug → info |
| `gc_manager.cpp:1319` | `task for move_from_chunk={} to move_to_chunk={} with priority={} start copying data`                         | debug → info |

**TRACE** — scan per-chunk stats, eligibility checks, per-blob copy/index progress:

| Line                  | Message template                                                                            | Change        |
|-----------------------|---------------------------------------------------------------------------------------------|---------------|
| `gc_manager.cpp:256`  | `gc scan chunk_id={}, use_blks={}, available_blks={}, total_blks={}, defrag_blks={}, `      | debug → trace |
| `gc_manager.cpp:522`  | `chunk_id={} belongs to no pg, not eligible for gc`                                         | debug → trace |
| `gc_manager.cpp:530`  | `chunk_id={} belongs to pg {}, which is not eligible for gc at this moment!`                | debug → trace |
| `gc_manager.cpp:740`  | `remove tombstone when updating pg index after data copy blob_id={}, move_from_chunk={}, `  | debug → trace |
| `gc_manager.cpp:752`  | `An already upated blob index found during recovery, which is expected. blob_id={}, `       | debug → trace |
| `gc_manager.cpp:772`  | `will replace blob_id={}, move_from_chunk={}, move_to_chunk={} from blk_id={} to blk_id={}` | debug → trace |
| `gc_manager.cpp:801`  | `successfully update index table, ret={}, move_from_chunk={}, move_to_chunk={}, blob_id={}` | debug → trace |
| `gc_manager.cpp:984`  | `successfully read blob from move_from_chunk={}, blob_id={}, pba={}`                        | debug → trace |
| `gc_manager.cpp:1036` | `successfully insert new key to gc index table for `                                        | debug → trace |

**DEBUG** — others remain debug, including intermediate task milestones and per-shard progress:

| Line                        | Message template                                                                                 | Change       |
|-----------------------------|--------------------------------------------------------------------------------------------------|--------------|
| `gc_manager.cpp:510`        | `chunk_id={} is added to reserved chunk queue`                                                   | remain debug |
| `gc_manager.cpp:543`        | `start emergent gc task : move_from_chunk_id={}, priority={}`                                    | remain debug |
| `gc_manager.cpp:552`        | `start gc task : move_from_chunk_id={}, priority={}`                                             | remain debug |
| `gc_manager.cpp:574`        | `{} pending gc tasks to be completed for pg={}, wait for 2 seconds!`                             | remain debug |
| `gc_manager.cpp:583`        | `all pending gc tasks for pg_id={} are completed`                                                | remain debug |
| `gc_manager.cpp:592`        | `decrease pending gc task num for pg_id={}, now it is {}`                                        | remain debug |
| `gc_manager.cpp:596`        | `pending gc task num for pg_id={} is already 0, no need to decrease it`                          | remain debug |
| `gc_manager.cpp:603`        | `increase pending gc task num for pg_id={}, now it is {}`                                        | remain debug |
| add to `gc_manager.cpp:805` | `successfully update index table, ret={}, move_from_chunk={}, move_to_chunk={}`                  | debug (new)  |
| `gc_manager.cpp:930`        | `empty shard found in move_from_chunk={}, skip`                                                  | remain debug |
| `gc_manager.cpp:933`        | `{} valid blobs found in move_from_chunk={}`                                                     | remain debug |
| `gc_manager.cpp:1073`       | `successfully copy blobs from move_from_chunk={} to move_to_chunk={}`                            | remain debug |
| `gc_manager.cpp:1076`       | `all valid blobs are copied from move_from_chunk={} to move_to_chunk={}`                         | remain debug |
| `gc_manager.cpp:1092`       | `successfully commit_blk in move_to_chunk={}, commit_blk_id={}`                                  | remain debug |
| `gc_manager.cpp:1095`       | `no used blks in move_to_chunk={}, so no need to commit_blk`                                     | remain debug |
| `gc_manager.cpp:1125`       | `remove tombstone successfully, ret={}, move_from_chunk={}, move_to_chunk={}`                    | remain debug |
| `gc_manager.cpp:1128`       | `data copied successfully for move_from_chunk={} to move_to_chunk={}`                            | remain debug |
| `gc_manager.cpp:1148`       | `clear all rreqs on chunk={} before resetting it`                                                | remain debug |
| `gc_manager.cpp:1152`       | `reset chunk={} before using it for gc`                                                          | remain debug |
| `gc_manager.cpp:1371`       | `gc task for move_from_chunk={} to move_to_chunk={} with priority={} start replacing blob index` | remain debug |
| `gc_manager.cpp:1454`       | `{} blks are reclaimed in this gc task!`                                                         | remain debug |

---

### Change 2: Rate-limit "no entry" heartbeat log

```cpp
// Current — fires on every heartbeat APPEND_ENTRIES with 0 entries (~250ms interval):
RD_LOGT(NO_TRACE_ID, "Raft channel: Received no entry, leader committed lsn {}",
        raft_req->get_commit_idx());

// Proposed — N=14400: 250ms × 14400 = 1 hour cadence:
RD_LOGT_EVERY_N(14400, NO_TRACE_ID, "Raft channel: Received no entry, leader committed lsn {}",
                raft_req->get_commit_idx());
```

Due to HS `LOG_EVERY_N` resetting its counter every 5 minutes, the effective behavior is one log
per 5 minutes rather than one per 250ms — a 20× reduction in practice.

---

## P1 Changes — Reduce Logs on Normal Request Paths

For detailed per-operation log traces, see `blob-op-log-analysis.md`.

### Change 3: Reduce Logs on Normal Read/Write Paths

#### 3.1 blobmgr log changes

Keep only entry and exit logs at DEBUG; demote intermediate path detail to TRACE.
Promote the tombstone log (previously invisible under `homeobject:info`) to `blobmgr` debug so
DELETE commit is traceable without enabling the `homeobject` module.

| Line                      | Message template                                              | Change                               |
|---------------------------|---------------------------------------------------------------|--------------------------------------|
| `hs_blob_manager.cpp:317` | `succeed to free blob data blk, lsn={}, blkid={}`             | `blobmgr` debug → trace              |
| `hs_blob_manager.cpp:418` | `Reading from blkid={} to buf={}`                             | `blobmgr` debug → trace              |
| `hs_blob_manager.cpp:479` | `Reading partial data: offset={}, len={}, full_blkid={}, ...` | `blobmgr` debug → trace              |
| `hs_blob_manager.cpp:553` | `Picked p_chunk_id={}, reserved_blks={}`                      | `blobmgr` debug → trace              |
| `hs_blob_manager.cpp:686` | `shard_id={}, blob_id={} has been moved to tombstone, lsn={}` | `homeobject` trace → `blobmgr` debug |

#### 3.2 replication changes — promote selected TRACE to DEBUG, demote noisy DEBUG to TRACE

Promote batch-progress logs so they remain visible under `replication:debug` without enabling full
TRACE. Demote two low-value DEBUG sites that add noise without diagnostic benefit on hot paths.

| Line                          | Message template                                                                                              | Change        |
|-------------------------------|---------------------------------------------------------------------------------------------------------------|---------------|
| `raft_repl_dev.cpp:1280`      | `For Repl_key=[{}] alloc hints returned error={}, failing this req, data_channel: {}, is_proposer: {}`        | debug → trace |
| `raft_repl_dev.cpp:2707`      | `GC rreq: Releasing blkid={} freed successfully`                                                              | debug → trace |
| `home_raft_log_store.cpp:215` | `end_of_append_batch flushed upto start={} cnt={} lsn={}`                                                     | trace → debug |
| `raft_repl_dev.cpp:1554`      | `Data Channel: FetchData received: dsn={} lsn={}`                                                             | trace → debug |
| `raft_repl_dev.cpp:2435`      | `Raft channel: Received {} append entries on follower from leader, term {}, lsn {} ~ {}, my committed lsn {}` | trace → debug |

---

## P2 Changes — Move `base:DEBUG` logs to dedicated modules

This further reduces log volume and makes each module's log level independently controllable.

### Change 4: Move `base:DEBUG` logs to dedicated modules

Audit each `base:DEBUG` log site to determine whether it should be retained under a dedicated
module or suppressed entirely.

#### 4.1: Add `storagemgr` log module

SM gRPC entry logs record the in_req → out_req mapping and are needed for request tracing.
Add a dedicated `storagemgr` module so they remain visible when `base` is set to `info`.

| Line                  | Message template                                                                                                | Change                     |
|-----------------------|-----------------------------------------------------------------------------------------------------------------|----------------------------|
| `sm_lib.cpp:251`      | `Sending heartbeat for [sm:{svc_uuid}]`                                                                         | move to `storagemgr` debug |
| `sm_lib.cpp:356`      | `Loading pg status for [sm:{svc_uuid}]`                                                                         | move to `storagemgr` debug |
| `sm_lib.cpp:504`      | `Local IP Address {ip}`                                                                                         | move to `storagemgr` debug |
| `cluster_mgr.hpp:67`  | `Adjusting CM Client to: [{addr}]`                                                                              | move to `storagemgr` debug |
| `cluster_mgr.hpp:98`  | `Attempt to access CM endpoint: [{addr}]`                                                                       | move to `storagemgr` debug |
| `cluster_mgr.hpp:104` | `Fail to access CM endpoint: [{addr}]`                                                                          | move to `storagemgr` debug |
| `cluster_mgr.hpp:121` | `Attempt[{i}] to [{action}]`                                                                                    | move to `storagemgr` debug |
| `services.hpp:89`     | `Received a request, in_req_id: {client_req_id}, out_req_id: {server_req_id}`                                   | move to `storagemgr` debug |
| `services.hpp:103`    | `Received request with empty subcluster id, reqId: {client_req_id}, IGNORE!`                                    | move to `storagemgr` debug |
| `services.hpp:116`    | `Received invalid request due to mismatched subcluster id: {got}, expected: {expected}, reqId: {client_req_id}` | move to `storagemgr` debug |

#### 4.2: Reassign `base` log sites in HomeObject

| Line                       | Message template                                                                 | Change                            |
|----------------------------|----------------------------------------------------------------------------------|-----------------------------------|
| `hs_shard_manager.cpp:698` | `chunk_id={} is not in chunk_to_shards_map, add it`                              | `base` debug → `homeobject` info  |
| `hs_pg_manager.cpp:1095`   | `Not a leader, no need to yield leadership`                                      | `base` debug → `homeobject` info  |
| `hs_pg_manager.cpp:1122`   | `cannot find a candidate leader except current leader {}, candidate_priority={}` | `base` debug → `homeobject` info  |
| `hs_http_manager.cpp:1042` | `GC job {} for chunk {} in PG {} with priority={}`                               | `base` debug → `homeobject` info  |
| `index_kv.cpp:106`         | `Failed to get from index table [route={}]`                                      | `base` debug → `blobmgr` debug    |
| `mem_shard_manager.cpp:22` | `Creating Shard [{}]: in pg={} of Size [{}b]`                                    | `base` debug → `homeobject` debug |

#### 4.3: Add `homestore` log module

Add a `homestore` log module so HomeStore `base:DEBUG` sites can be independently controlled.
All HomeStore `base:DEBUG` logs move to `homestore:debug` by default — except two sites that
belong to other modules:

| Line                                | Message template                                                                                            | Change                             |
|-------------------------------------|-------------------------------------------------------------------------------------------------------------|------------------------------------|
| `lib/logstore/log_dev.cpp:795`      | `Found a logstore log_dev={} log_store={} with start lsn={}, Creating a new HomeLogStore instance`          | `base` debug → `homestore` debug   |
| `lib/meta/meta_blk_service.cpp:347` | `[type={}], meta blk size check passed!`                                                                    | `base` debug → `homestore` debug   |
| `lib/device/device_manager.cpp:275` | `Virtual device {} is already sized correctly, no new devices to add`                                       | `base` debug → `homestore` debug   |
| `lib/device/device_manager.cpp:520` | `total size of type {} in this homestore is {}`                                                             | `base` debug → `homestore` debug   |
| `lib/device/device_manager.cpp:527` | `size of all added pdevs={}, current_chunk_num={} of type {} in vdev {}`                                    | `base` debug → `homestore` debug   |
| `lib/device/device_manager.cpp:533` | `pdev {} is already added to vdev {}, skip it`                                                              | `base` debug → `homestore` debug   |
| `raft_state_machine.cpp:406`        | `save_snp_resync_data success, next obj_id={}`                                                              | `base` debug → `replication` debug |
| `append_blk_allocator.cpp:90`       | `chunk {} has successfully allocated nblks: {}, totally used blks: {}, available_blks: {}, actual blks: {}` | `base` debug → trace               |

#### 4.4 Set `base` log level to INFO (TBD)

Setting `base` to `info` would eliminate high-frequency noise such as `bitset.hpp:1175:resize_impl`,
but would also suppress tcmalloc stats and other `base`-module diagnostics. For now, `base` and
sisl logs are kept at `debug`; this can be revisited if further volume reduction is needed.

---

## Target Log Mode

After all P0 + P1 + P2 changes:

```
base:debug,cm_client:debug,blobmgr:debug,shardmgr:debug,replication:debug,gcmgr:debug,storagemgr:debug,homestore:debug
```

---

## Per-Operation Impact After P1 Changes

Comparison between the original mode (`replication:trace, blobmgr:debug`) and the target mode
(`replication:debug, blobmgr:debug`) with code changes applied. Source: `blob-op-log-analysis.md`.

### Summary table

| Operation        | Role     | Lines before | Lines after | Size before | Size after |
|------------------|----------|:------------:|:-----------:|:-----------:|:----------:|
| PUT blob         | Leader   |      12      |      7      |   ~2.8 KB   |  ~1.7 KB   |
| PUT blob         | Follower |      10      |      5      |   ~2.4 KB   |  ~1.2 KB   |
| GET blob full    | Local    |      3       |      2      |   ~660 B    |   ~460 B   |
| GET blob partial | Local    |      3       |      2      |   ~740 B    |   ~460 B   |
| DELETE blob      | Leader   |      8       |      3      |   ~1.8 KB   |   ~750 B   |
| DELETE blob      | Follower |      7       |      3      |   ~1.6 KB   |   ~700 B   |
| CREATE shard     | Leader   |      11      |      6      |   ~2.3 KB   |  ~1.3 KB   |
| CREATE shard     | Follower |      8       |      4      |   ~1.8 KB   |   ~900 B   |
| SEAL shard       | Leader   |      10      |      5      |   ~2.2 KB   |  ~1.2 KB   |
| SEAL shard       | Follower |      8       |      4      |   ~1.8 KB   |   ~900 B   |

