# HomeStore Log Catalog

Catalog of HomeStore log statements, organized by log module and severity.
Source files live in HomeStore `docs/log_catalog/` (auto-generated). This document
keeps the per-module totals and expands the two production-critical non-INFO
modules in the same layout as [log-catalog.md](log-catalog.md).

## Log Modules

HomeStore defines named log modules in `src/include/homestore/homestore_decl.hpp`:

```cpp
#define HOMESTORE_LOG_MODS \
    btree, device, blkalloc, cp, metablk, wbcache, logstore, transient, replication, journalvdev, solorepl
```

An implicit module **`base`** is used wherever bare `LOGINFO`/`LOGDEBUG`/etc. calls
appear (no `MOD` suffix). These resolve to `LOGINFOMOD(base, ...)` via sisl
(`sisl/include/sisl/logging/logging.h`). `base` is not listed in `HOMESTORE_LOG_MODS`.

Covered macros: `LOG*MOD`, `DLOG*MOD`, `HS_LOG`, `HS_PERIODIC_LOG`, `HS_LOG_EVERY_N`,
`RD_LOG*`, `REPL_STORE_LOG`, `BT_LOG`, `BT_NODE_LOG`, `BLKALLOC_LOG`,
`THIS_LOGSTORE_LOG`/`THIS_LOGDEV_LOG`, `CP_LOG`, and base `LOGINFO`/`LOGDEBUG`/….

| Module        | Primary files                                                                                                                                    | Macro family                                                                                  | Structured prefix                            |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|----------------------------------------------|
| `replication` | `lib/replication/repl_dev/raft_repl_dev.cpp`, `raft_state_machine.cpp`, `repl_log_store.cpp`, `home_raft_log_store.cpp`, `raft_repl_service.cpp` | `RD_LOGT/D/I/W/E/C`, `RD_LOG`, `RD_LOG*_EVERY_N`, `REPL_STORE_LOG`, `LOG*MOD(replication, …)` | RD: tid prefixed; REPL_STORE: store prefixed |
| `base`        | `lib/homestore.cpp`, `checkpoint/cp_mgr.cpp`, `device/*`, `blkalloc/*`, `logstore/*`, plus header helpers                                        | `LOGDEBUG`/`LOGINFO`/`LOGWARN`/`LOGERROR`/`LOGCRITICAL`                                       | bare message (no module prefix)              |
| `btree`       | `include/homestore/btree/**`, index btree                                                                                                        | `BT_LOG`, `BT_NODE_LOG`                                                                       | file+line+btree / node prefixed              |
| `blkalloc`    | `lib/blkalloc/*`                                                                                                                                 | `BLKALLOC_LOG`, `LOG*MOD(blkalloc, …)`                                                        | allocator name prefixed                      |
| `logstore`    | `lib/logstore/*`                                                                                                                                 | `THIS_LOGSTORE_LOG`, `THIS_LOGDEV_LOG`                                                        | store name / logdev id prefixed              |
| `wbcache`     | `lib/index/wb_cache.cpp`                                                                                                                         | `LOG*MOD(wbcache, …)`                                                                         | module prefixed                              |
| `device`      | `lib/device/*`                                                                                                                                   | `LOG*MOD(device, …)`                                                                          | module prefixed                              |
| `journalvdev` | `lib/blkalloc` / journal vdev                                                                                                                    | `LOG*MOD(journalvdev, …)`                                                                     | module prefixed                              |
| `metablk`     | `lib/meta/meta_blk_service.cpp`                                                                                                                  | `LOG*MOD(metablk, …)`                                                                         | module prefixed                              |
| `cp`          | `lib/checkpoint/*`                                                                                                                               | `CP_LOG`                                                                                      | cp_id prefixed                               |
| `solorepl`    | solo repl path                                                                                                                                   | `LOG*MOD(solorepl, …)`                                                                        | module prefixed                              |

## Severity Levels

All modules support the same 6 levels in ascending severity: TRACE < DEBUG < INFO < WARN < ERROR < CRITICAL.

## Production logmod

Production SM logmod (from HomeObject `docs/log-volume-reduction-plan.md`):

```
base:debug,cm_client:debug,pg_svc:info,shard_svc:info,data_svc:info,homeobject:info,blobmgr:debug,shardmgr:debug,wbcache:info,btree:info,nuraft_mesg:info,replication:trace,logstore:info
gcmgr:debug (default)
```

HomeStore modules in that string:

| Module            | Production level | Effect                                      |
|-------------------|------------------|---------------------------------------------|
| `wbcache`         | info             | INFO+ only                                  |
| `btree`           | info             | INFO+ only                                  |
| `logstore`        | info             | INFO+ only                                  |
| **`base`**        | **debug**        | DEBUG+ (no TRACE statements in this module) |
| **`replication`** | **trace**        | TRACE+ (every replication statement)        |

The two **non-INFO** HomeStore modes — and the only ones that emit below INFO in
production — are **`replication:trace`** and **`base:debug`**. Their statements
are listed in full below. Other HomeStore modules either stay at INFO in
production or are not set in this logmod string (they keep sisl defaults).

## Log Count Summary

Totals include `lib/` and `tests/`. The parenthesized number is `lib/` only.

| Module        | TRACE | DEBUG | INFO | WARN | ERROR | CRITICAL | Total (lib) |
|---------------|-------|-------|------|------|-------|----------|-------------|
| `base`        | -     | 73    | 1168 | 16   | 62    | 6        | 1325 (184)  |
| `blkalloc`    | 22    | 15    | 4    | 1    | 2     | -        | 44 (42)     |
| `btree`       | 15    | 20    | 3    | -    | 9     | -        | 47 (47)     |
| `cp`          | -     | 3     | 3    | -    | -     | -        | 6 (6)       |
| `device`      | 16    | 5     | 4    | -    | 4     | -        | 29 (29)     |
| `journalvdev` | 5     | 4     | 18   | -    | -     | -        | 27 (27)     |
| `logstore`    | 13    | 18    | 24   | 4    | 4     | -        | 63 (63)     |
| `metablk`     | 1     | 19    | 12   | 2    | -     | -        | 34 (34)     |
| `replication` | 43    | 85    | 124  | 27   | 61    | -        | 340 (312)   |
| `solorepl`    | -     | 1     | 2    | -    | -     | -        | 3 (3)       |
| `wbcache`     | 120   | 2     | 10   | -    | 1     | -        | 133 (133)   |

Under production logmod, statements that can fire in the **runtime library** (`lib/`):

- **`replication` at TRACE:** 312 statements (41 TRACE + 84 DEBUG + 103 INFO + 27 WARN + 57 ERROR).
- **`base` at DEBUG:** 184 statements (8 DEBUG + 115 INFO + 7 WARN + 54 ERROR). The 1325-row `base` total is dominated
  by tests (1141), mostly `LOGINFO`.

## Custom Log Macro Families

| Macro family                             | Expands to logmod | Arg layout               |
|------------------------------------------|-------------------|--------------------------|
| `LOG<L>MOD(mod, fmt, …)`                 | `mod`             | direct                   |
| `DLOG<L>MOD(mod, fmt, …)`                | `mod`             | debug-only               |
| `HS_LOG(lvl, mod, fmt, …)`               | `mod`             | direct                   |
| `HS_PERIODIC_LOG(lvl, mod, fmt, …)`      | `mod`             | rate-limited             |
| `HS_LOG_EVERY_N(lvl, mod, freq, fmt, …)` | `mod`             | every-N                  |
| `RD_LOG(lvl, tid, fmt, …)`               | `replication`     | tid prefixed             |
| `RD_LOGT/D/I/W/E/C(tid, fmt, …)`         | `replication`     | fixed level              |
| `RD_LOG_EVERY_N(lvl, freq, tid, fmt, …)` | `replication`     | every-N                  |
| `RD_LOG?_EVERY_N(freq, tid, fmt, …)`     | `replication`     | fixed level every-N      |
| `REPL_STORE_LOG(lvl, fmt, …)`            | `replication`     | replstore prefixed       |
| `BT_LOG(lvl, fmt, …)`                    | `btree`           | file+line+btree prefixed |
| `BT_NODE_LOG(lvl, node, fmt, …)`         | `btree`           | node prefixed            |
| `BLKALLOC_LOG(lvl, fmt, …)`              | `blkalloc`        | allocator name prefixed  |
| `THIS_LOGSTORE_LOG(lvl, fmt, …)`         | `logstore`        | store name prefixed      |
| `THIS_LOGDEV_LOG(lvl, fmt, …)`           | `logstore`        | logdev id prefixed       |
| `CP_LOG(lvl, cp_id, fmt, …)`             | `cp`              | cp_id prefixed           |
| `LOGINFO/DEBUG/…(fmt, …)`                | `base`            | no mod prefix            |
| `DLOGINFO/DEBUG/…(fmt, …)`               | `base`            | debug-only               |

---

## Module: `replication`

**Description:** Raft replication device, state machine, log store, and repl service. Production sets this module to *
*TRACE**, so every row below can emit.

**Macros:** `RD_LOGT/D/I/W/E/C`, `RD_LOG*_EVERY_N`, `REPL_STORE_LOG`, `LOG*MOD(replication, …)`

**Log prefix:** RD logs are tid-prefixed; `REPL_STORE_LOG` is store-prefixed.

**Source files:
** `lib/replication/repl_dev/raft_repl_dev.cpp`, `raft_repl_dev.h`, `raft_state_machine.cpp`, `common.cpp`, `lib/replication/log_store/repl_log_store.cpp`, `home_raft_log_store.cpp`, `lib/replication/service/raft_repl_service.cpp`, `generic_repl_svc.cpp`

**Total log statements:** 340 (312 in lib/, 28 in tests/)

### TRACE (43)

**lib/** (41)

| #  | Message                                                                                                  | Source                                                  |
|----|----------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| 1  | `append entry term={}, log_val_type={} size={}`                                                          | `lib/replication/log_store/home_raft_log_store.cpp:165` |
| 2  | `end_of_append_batch flushed upto start={} cnt={} lsn={}`                                                | `lib/replication/log_store/home_raft_log_store.cpp:215` |
| 3  | `log_entries lsn={}`                                                                                     | `lib/replication/log_store/home_raft_log_store.cpp:223` |
| 4  | `Num log entries start={} end={} num_entries={}`                                                         | `lib/replication/log_store/home_raft_log_store.cpp:228` |
| 5  | `log_entries_ext, start={} end={}, hint {}, adjusted range {} ~ {}, cnt {}`                              | `lib/replication/log_store/home_raft_log_store.cpp:252` |
| 6  | `packing lsn={} of size={}, avail_size in buffer={}`                                                     | `lib/replication/log_store/home_raft_log_store.cpp:320` |
| 7  | `unpacking nth_entry={} of size={}, lsn={}`                                                              | `lib/replication/log_store/home_raft_log_store.cpp:356` |
| 8  | `Raft Channel: Received append log entry rreq=[{}]`                                                      | `lib/replication/log_store/repl_log_store.cpp:23`       |
| 9  | `Raft Channel: Received write_at log entry rreq=[{}]`                                                    | `lib/replication/log_store/repl_log_store.cpp:38`       |
| 10 | `Raft Channel: end_of_append_batch start_lsn={} count={} num_data_to_be_written={} {}`                   | `lib/replication/log_store/repl_log_store.cpp:59`       |
| 11 | `Raft Channel: end_of_append_batch, I am proposer for lsn {}, only flushed log for it`                   | `lib/replication/log_store/repl_log_store.cpp:91`       |
| 12 | `[traceID={}] Setting lsn={} for request={}`                                                             | `lib/replication/repl_dev/common.cpp:179`               |
| 13 | `[traceID={}] m_pushed_data addr={}, m_rkey={}, m_lsn={}`                                                | `lib/replication/repl_dev/common.cpp:243`               |
| 14 | `Raft Channel: propose journal_entry=[{}]`                                                               | `lib/replication/repl_dev/raft_state_machine.cpp:37`    |
| 15 | `Raft Channel: Localizing Raft log_entry: server_id={}, term={}, journal_entry=[{}]`                     | `lib/replication/repl_dev/raft_state_machine.cpp:62`    |
| 16 | `Repl_key=[{}] already flushed, skip duplicate prepare`                                                  | `lib/replication/repl_dev/raft_state_machine.cpp:92`    |
| 17 | `Precommit rreq=[{}]`                                                                                    | `lib/replication/repl_dev/raft_state_machine.cpp:211`   |
| 18 | `Raft channel: Received Commit message rreq=[{}]`                                                        | `lib/replication/repl_dev/raft_state_machine.cpp:227`   |
| 19 | `Raft channel: erase lsn {}, rreq {}`                                                                    | `lib/replication/repl_dev/raft_state_machine.cpp:325`   |
| 20 | `Skipping data channel send since value size is 0`                                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:1091`       |
| 21 | `Data Channel: Data already received for rreq=[{}], ignoring this data`                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:1192`       |
| 22 | `Repl_key=[{}] already received`                                                                         | `lib/replication/repl_dev/raft_repl_dev.cpp:1261`       |
| 23 | `{} pending reqs's data are written`                                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:1349`       |
| 24 | `Fetching data from originator={}, remote: rreq=[{}], remote_blkid={}, my server_id={}`                  | `lib/replication/repl_dev/raft_repl_dev.cpp:1458`       |
| 25 | `Data Channel: FetchData from remote completed, time taken={} us`                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:1484`       |
| 26 | `Data Channel: FetchData received: fetch_req.size={}`                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:1526`       |
| 27 | `Data Channel: FetchData received: dsn={} lsn={}`                                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:1554`       |
| 28 | `Data Channel: FetchData handled, my_blkid={}`                                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:1559`       |
| 29 | `Data Channel: FetchData data read completed for {} buffers`                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:1584`       |
| 30 | `Data Channel: Data already received for rreq=[{}], skip and move on to next rreq.`                      | `lib/replication/repl_dev/raft_repl_dev.cpp:1629`       |
| 31 | `Data Channel: Data fetched from remote: rreq=[{}], data_size: {}, total_size: {}, local_blkid: {}`      | `lib/replication/repl_dev/raft_repl_dev.cpp:1662`       |
| 32 | `Found active peer {}, lag {}, my lsn {}, peer lsn {}, least_active_repl_idx {}, laggy={}`               | `lib/replication/repl_dev/raft_repl_dev.cpp:2115`       |
| 33 | `Raft channel: Received no entry, leader committed lsn {}`                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:2371`       |
| 34 | `Raft channel: Append entries callback flip completed, lsn {} ~ {}`                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:2378`       |
| 35 | `Raft channel: Received {} append entries on follower from leader, term {}, lsn {} ~ {} , my`            | `lib/replication/repl_dev/raft_repl_dev.cpp:2434`       |
| 36 | `Raft channel: term {}, lsn {}, skipping dup, last_commit_lsn {}`                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:2451`       |
| 37 | `Flushing durable commit lsn to {}`                                                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:2518`       |
| 38 | `No replace member in progress, return`                                                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:2533`       |
| 39 | `state_machine req map size is {};`                                                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:2691`       |
| 40 | `Skipping GC rreq [{}] because it is in state machine`                                                   | `lib/replication/repl_dev/raft_repl_dev.cpp:2696`       |
| 41 | `Raft Channel: Applying Raft log_entry upon recovery: server_id={}, term={}, lsn={}, journal_entry=[{}]` | `lib/replication/repl_dev/raft_repl_dev.cpp:2754`       |

**tests/** (2)

| # | Message                                                                          | Source                                          |
|---|----------------------------------------------------------------------------------|-------------------------------------------------|
| 1 | `[Replica={}] Read logical snapshot callback fetching lsn={} size={} pattern={}` | `tests/test_common/raft_repl_test_base.hpp:249` |
| 2 | `[Replica={}] Save logical snapshot got lsn={} data_size={} data_pattern={}`     | `tests/test_common/raft_repl_test_base.hpp:308` |

### DEBUG (85)

**lib/** (84)

| #  | Message                                                                                                    | Source                                                  |
|----|------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| 1  | `Store={} LogDev={}: Skipping truncating because of reserved logs entries is not enough or`                | `lib/replication/log_store/home_raft_log_store.cpp:61`  |
| 2  | `Opened new home log_dev={} log_store={}`                                                                  | `lib/replication/log_store/home_raft_log_store.cpp:99`  |
| 3  | `Opening existing home log_dev={} log_store={}`                                                            | `lib/replication/log_store/home_raft_log_store.cpp:103` |
| 4  | `Home Log store created/opened successfully`                                                               | `lib/replication/log_store/home_raft_log_store.cpp:111` |
| 5  | `Logstore is being physically removed`                                                                     | `lib/replication/log_store/home_raft_log_store.cpp:117` |
| 6  | `Compact with log holes from {} to={}`                                                                     | `lib/replication/log_store/home_raft_log_store.cpp:370` |
| 7  | `None-APP log: append entry term={}, log_val_type={} lsn={} size={}`                                       | `lib/replication/log_store/repl_log_store.cpp:13`       |
| 8  | `Raft Channel: effective_compact_lsn={}, raft compact_to_lsn={}, local truncation_upper_limit={}`          | `lib/replication/log_store/repl_log_store.cpp:120`      |
| 9  | `Reaper Thread: Doing GC`                                                                                  | `lib/replication/service/raft_repl_service.cpp:643`     |
| 10 | `become_follower_cb called!`                                                                               | `lib/replication/repl_dev/raft_repl_dev.h:369`          |
| 11 | `Raft channel: Commit new cluster conf , log_idx = {}`                                                     | `lib/replication/repl_dev/raft_state_machine.cpp:245`   |
| 12 | `Raft channel: Rollback cluster conf , log_idx = {}`                                                       | `lib/replication/repl_dev/raft_state_machine.cpp:273`   |
| 13 | `Raft channel: Rollback lsn {}, rreq=[{}]`                                                                 | `lib/replication/repl_dev/raft_state_machine.cpp:285`   |
| 14 | `Raft channel: last_commit_index {}`                                                                       | `lib/replication/repl_dev/raft_state_machine.cpp:315`   |
| 15 | `Step1. Replace member, quorum safety check, active_peers={}, active_peers_exclude_out/in_member={},`      | `lib/replication/repl_dev/raft_repl_dev.cpp:283`        |
| 16 | `Step5. Replace member, old member is removed, task_id={}, member={}`                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:412`        |
| 17 | `Member replacement is in progress. task_id={}, out_member={}, in_member={}`                               | `lib/replication/repl_dev/raft_repl_dev.cpp:522`        |
| 18 | `out member still exists in raft group, member={}`                                                         | `lib/replication/repl_dev/raft_repl_dev.cpp:647`        |
| 19 | `Wait for member removed timed out, please retry, timeout: {}, member={}`                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:654`        |
| 20 | `learner flag has already been set to {}, skip, member={}`                                                 | `lib/replication/repl_dev/raft_repl_dev.cpp:715`        |
| 21 | `Wait for flipping learner timed out, please retry, timeout: {}`                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:728`        |
| 22 | `create_snapshot last_idx={}/term={}`                                                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:890`        |
| 23 | `peer_repl_idx={}, minimum_repl_idx={}`                                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:958`        |
| 24 | `calculated truncation_upper_limit={},`                                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:963`        |
| 25 | `repl_key [{}], header size [{}] bytes, user_key size [{}] bytes, data size [{}] bytes`                    | `lib/replication/repl_dev/raft_repl_dev.cpp:1035`       |
| 26 | `Simulating push data failure, so that all the follower will have to fetch data`                           | `lib/replication/repl_dev/raft_repl_dev.cpp:1054`       |
| 27 | `Data Channel: Pushing data to follower {}, rreq=[{}]`                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:1117`       |
| 28 | `Data Channel: Data push completed for rreq=[{}]`                                                          | `lib/replication/repl_dev/raft_repl_dev.cpp:1133`       |
| 29 | `Data Channel: PushData received: time diff={} ms.`                                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:1167`       |
| 30 | `Data Channel: Data write completed for rreq=[{}], time_diff_data_log_us={},`                              | `lib/replication/repl_dev/raft_repl_dev.cpp:1232`       |
| 31 | `For Repl_key=[{}] alloc hints returned error={}, failing this req`                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:1271`       |
| 32 | `Repl_key=[{}] got no_space_left error on follower as lsn={}`                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:1274`       |
| 33 | `For Repl_key=[{}] alloc hints returned error={}, failing this req, data_channel: {}, is_proposer: {}`     | `lib/replication/repl_dev/raft_repl_dev.cpp:1278`       |
| 34 | `in follower_create_req: rreq={}, addr=0x{:x}`                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:1288`       |
| 35 | `Data written and blkid mapped: rkey=[{}]`                                                                 | `lib/replication/repl_dev/raft_repl_dev.cpp:1303`       |
| 36 | `Data write completed and blkid mapped: rreq=[{}]`                                                         | `lib/replication/repl_dev/raft_repl_dev.cpp:1346`       |
| 37 | `We haven't received data for {} out {} in reqs batch, will fetch and wait for {} ms, in_resync_mode()={}` | `lib/replication/repl_dev/raft_repl_dev.cpp:1376`       |
| 38 | `rreq=[{}] already errored out, ignoring the fetch`                                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:1408`       |
| 39 | `Data already received for rreq=[{}], ignoring the fetch`                                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:1412`       |
| 40 | `Data Channel : FetchData from remote: rreq.size={}, my server_id={}`                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:1441`       |
| 41 | `non-originator FetchData received: dsn={} lsn={} originator={}, my_server_id={}`                          | `lib/replication/repl_dev/raft_repl_dev.cpp:1551`       |
| 42 | `Data Channel: Error happens when fetching data. value={}, category={}, err_message={},`                   | `lib/replication/repl_dev/raft_repl_dev.cpp:1569`       |
| 43 | `Data Channel: FetchData completed for {} requests`                                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:1618`       |
| 44 | `Data Channel: Data Write completed rreq=[{}], data_write_latency_us={},`                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:1656`       |
| 45 | `Rolling back rreq: {}`                                                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:1686`       |
| 46 | `Releasing blkid={} freed successfully`                                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:1697`       |
| 47 | `Raft channel: Commit rreq=[{}]`                                                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:1712`       |
| 48 | `config commit on lsn {}`                                                                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:1760`       |
| 49 | `roll back config on lsn {}`                                                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:1770`       |
| 50 | `m_repl_svc_ctx doesn't exist, returning empty peer info`                                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:2022`       |
| 51 | `get_replication_quorum: found {} members in cluster config`                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:2046`       |
| 52 | `Not the leader, no need to reconcile`                                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:2063`       |
| 53 | `Found higher priority peer {}, priority {}`                                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:2078`       |
| 54 | `Current leader {} has highest priority {}, no need to reconcile`                                          | `lib/replication/repl_dev/raft_repl_dev.cpp:2083`       |
| 55 | `Raft channel: Reject append entries requested by flip, lsn {} ~ {}`                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:2382`       |
| 56 | `Raft channel: Received JoinedCluster, implies become_follower`                                            | `lib/replication/repl_dev/raft_repl_dev.cpp:2486`       |
| 57 | `Raft channel: Received BecomeFollower`                                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:2490`       |
| 58 | `Raft channel: Received BecomeLeader`                                                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:2495`       |
| 59 | `Replica out {} with lsn {}`                                                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:2549`       |
| 60 | `Replica in {} with lsn {}`                                                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:2553`       |
| 61 | `Checking replace member status, task_id={},replica_in={} with lsn={}, replica_out={} with lsn={}`         | `lib/replication/repl_dev/raft_repl_dev.cpp:2567`       |
| 62 | `Checking replace member status, new member has caught up, task_id={}, replica_in={} with lsn={},`         | `lib/replication/repl_dev/raft_repl_dev.cpp:2573`       |
| 63 | `Trigger complete_replace_member, task_id={}, replica_in={}, replica_out={}`                               | `lib/replication/repl_dev/raft_repl_dev.cpp:2581`       |
| 64 | `cp flush in raft repl dev, lsn={}, clsn={}, next_dsn={}, cp string:{}`                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:2623`       |
| 65 | `getting cp_ctx for raft repl dev {}, cp_lsn={}, clsn={}, next_dsn={}, cp string:{}`                       | `lib/replication/repl_dev/raft_repl_dev.cpp:2632`       |
| 66 | `legacy req with committed DSN, rreq=[{}] , dsn = {}, next_dsn = {}, gap= {}, elapsed_hours {}`            | `lib/replication/repl_dev/raft_repl_dev.cpp:2666`       |
| 67 | `StateMachine: rreq=[{}] is expired, elapsed_hours {};`                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:2686`       |
| 68 | `Removing rreq [{}]`                                                                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:2702`       |
| 69 | `GC rreq: Releasing blkid={} freed successfully`                                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:2707`       |
| 70 | `Replay log on restart, rreq=[{}]`                                                                         | `lib/replication/repl_dev/raft_repl_dev.cpp:2809`       |
| 71 | `create snapshot resync msg, dsn={}, crc={}`                                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:2834`       |
| 72 | `received snapshot resync msg, dsn={}, crc={}, received crc={}`                                            | `lib/replication/repl_dev/raft_repl_dev.cpp:2849`       |
| 73 | `Update next_dsn from {} to {}`                                                                            | `lib/replication/repl_dev/raft_repl_dev.cpp:2870`       |
| 74 | `Raft Channel: Resync mode, leader_committed_lsn={}, my_log_idx={}, diff={}`                               | `lib/replication/repl_dev/raft_repl_dev.cpp:2884`       |
| 75 | `enter quiescence state, waiting for all the pending req to be initialized`                                | `lib/replication/repl_dev/raft_repl_dev.cpp:2909`       |
| 76 | `wait for {} pending create_req requests to be completed`                                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:2913`       |
| 77 | `exit quiescence state, resume accepting new requests`                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:2925`       |
| 78 | `start cleaning all the in-memory rreqs, which has allocated blk on the emergent chunk={} before handling` | `lib/replication/repl_dev/raft_repl_dev.cpp:2929`       |
| 79 | `blkid={} freed successfully for handling no_space_left error`                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:2943`       |
| 80 | `all the necessary in-memory rreqs which has allocated blks on the emergent chunk have been cleaned up`    | `lib/replication/repl_dev/raft_repl_dev.cpp:2955`       |
| 81 | `got nullptr for initing req, rkey=[{}]`                                                                   | `lib/replication/repl_dev/raft_repl_dev.cpp:2968`       |
| 82 | `Rejecting new request in quiescence state, rkey=[{}]`                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:2975`       |
| 83 | `become_leader_cb: setting traffic_ready_lsn from {} to {}`                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:2994`       |
| 84 | `Not yet ready for traffic, committed to {} but gate is {}`                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:3004`       |

**tests/** (1)

| # | Message                                                        | Source                                          |
|---|----------------------------------------------------------------|-------------------------------------------------|
| 1 | `Found duplicate key_id={} in already_exist_key_ids, retrying` | `tests/test_common/raft_repl_test_base.hpp:123` |

### INFO (124)

**lib/** (103)

| #   | Message                                                                                                   | Source                                                  |
|-----|-----------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| 1   | `LogDev={}: Truncating log entries from {} to {}, compact_lsn={}, last_lsn={}`                            | `lib/replication/log_store/home_raft_log_store.cpp:77`  |
| 2   | `Store={} LogDev={}: Purging all logs in the log store, last_lsn={}`                                      | `lib/replication/log_store/home_raft_log_store.cpp:388` |
| 3   | `boost::uuids::to_string(group_id)`                                                                       | `lib/replication/service/generic_repl_svc.cpp:148`      |
| 4   | `Creating RAFT state manager for server_id={} group_id={}`                                                | `lib/replication/service/raft_repl_service.cpp:344`     |
| 5   | `Groupid={}, new member={} added with priority={}`                                                        | `lib/replication/service/raft_repl_service.cpp:399`     |
| 6   | `ReplDev group_id={} was destroyed, reclaim the stale resource`                                           | `lib/replication/service/raft_repl_service.cpp:478`     |
| 7   | `repl dev group_id={} not found, maybe already destroyed, trace_id={}`                                    | `lib/replication/service/raft_repl_service.cpp:608`     |
| 8   | `Reaper Thread: scheduling GC every {} seconds`                                                           | `lib/replication/service/raft_repl_service.cpp:635`     |
| 9   | `Reaper Thread: GC timer expired {} times, running once`                                                  | `lib/replication/service/raft_repl_service.cpp:641`     |
| 10  | `flush durable commit timer expired {} times, running once`                                               | `lib/replication/service/raft_repl_service.cpp:654`     |
| 11  | `replace member sync check timer expired {} times, running once`                                          | `lib/replication/service/raft_repl_service.cpp:663`     |
| 12  | `fetch pending data timer expired {} times, running once`                                                 | `lib/replication/service/raft_repl_service.cpp:689`     |
| 13  | `Fetcher Thread: Stopping timer`                                                                          | `lib/replication/service/raft_repl_service.cpp:703`     |
| 14  | `Reaper Thread: Stopping timers`                                                                          | `lib/replication/service/raft_repl_service.cpp:707`     |
| 15  | `ReplSvc is stopping, skipping GC`                                                                        | `lib/replication/service/raft_repl_service.cpp:748`     |
| 16  | `ReplDev group_id={} was destroyed, shutting down the raft group in delayed fashion now`                  | `lib/replication/service/raft_repl_service.cpp:760`     |
| 17  | `ReplDev group_id={} is UNREADY, skip flushing durable commit lsn`                                        | `lib/replication/service/raft_repl_service.cpp:788`     |
| 18  | `[traceID={}] For Repl_key=[{}] data already exists, skip`                                                | `lib/replication/repl_dev/common.cpp:143`               |
| 19  | `Resetting repl dev name from {} to {}`                                                                   | `lib/replication/repl_dev/raft_repl_dev.h:301`          |
| 20  | `Raft Channel: Log {} is expected to be handled by snapshot. Skipping commit.`                            | `lib/replication/repl_dev/raft_state_machine.cpp:222`   |
| 21  | `Raft Channel: Config {} is expected to be handled by snapshot. Skipping commit.`                         | `lib/replication/repl_dev/raft_state_machine.cpp:240`   |
| 22  | `Raft channel: server ids in new cluster conf : {}, my_id {}, group_id {}`                                | `lib/replication/repl_dev/raft_state_machine.cpp:265`   |
| 23  | `Started {} RaftReplDev group_id={}, replica_id={}, raft_server_id={} committed_lsn={},`                  | `lib/replication/repl_dev/raft_repl_dev.cpp:91`         |
| 24  | `Starting data channel, group_id={}, replica_id={}`                                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:101`        |
| 25  | `Resuming after slow down data channel flip`                                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:107`        |
| 26  | `Slow down data channel flip is enabled, scheduling to call later`                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:110`        |
| 27  | `repl dev is being shutdown!`                                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:209`        |
| 28  | `Start replace member, task_id={}, member_out={} member_in={}`                                            | `lib/replication/repl_dev/raft_repl_dev.cpp:218`        |
| 29  | `Step1. Replace member, leader is the member_out so yield leadership, task_id={}`                         | `lib/replication/repl_dev/raft_repl_dev.cpp:229`        |
| 30  | `Step1. Replace member, the intent has already been fulfilled, ignore it, task_id={},`                    | `lib/replication/repl_dev/raft_repl_dev.cpp:264`        |
| 31  | `Step2. Replace member, flip out member to learner, task_id={}`                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:308`        |
| 32  | `Step2. Replace member, flip out member to learner and set priority to 0, task_id={}`                     | `lib/replication/repl_dev/raft_repl_dev.cpp:316`        |
| 33  | `Step3. Replace member, propose to raft for HS_CTRL_START_REPLACE req, group_id={}, task_id={}`           | `lib/replication/repl_dev/raft_repl_dev.cpp:319`        |
| 34  | `Step4. Replace member, propose to raft to add new member, group_id={}, task_id={}`                       | `lib/replication/repl_dev/raft_repl_dev.cpp:359`        |
| 35  | `Step4. Replace member, proposed to raft to add member, task_id={}, member={}`                            | `lib/replication/repl_dev/raft_repl_dev.cpp:369`        |
| 36  | `repl dev is being shutdown!`                                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:379`        |
| 37  | `Complete replace member, task_id={}, member_out={}, member_in={}`                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:388`        |
| 38  | `Step5. Replace member, remove old member, task_id={}, member={}`                                         | `lib/replication/repl_dev/raft_repl_dev.cpp:397`        |
| 39  | `Step6. Replace member, propose to raft for HS_CTRL_COMPLETE_REPLACE req, group_id={}, task_id={}`        | `lib/replication/repl_dev/raft_repl_dev.cpp:416`        |
| 40  | `Complete replace member done, group_id={}, task_id={}, member_out={} member_in={}`                       | `lib/replication/repl_dev/raft_repl_dev.cpp:451`        |
| 41  | `repl dev is being shutdown!`                                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:461`        |
| 42  | `Member replacement fulfilled, but task still exists, wait for reaper thread to retry`                    | `lib/replication/repl_dev/raft_repl_dev.cpp:517`        |
| 43  | `Member to add failed, not leader`                                                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:529`        |
| 44  | `Proposed to raft to add member, member={}`                                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:558`        |
| 45  | `Remove member, member={}`                                                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:564`        |
| 46  | `repl dev is being shutdown!`                                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:566`        |
| 47  | `Remove member step1. Member has been removed, member={}`                                                 | `lib/replication/repl_dev/raft_repl_dev.cpp:580`        |
| 48  | `Remove member step2. Propose to raft for HS_CTRL_REMOVE_MEMBER req, group_id={}`                         | `lib/replication/repl_dev/raft_repl_dev.cpp:585`        |
| 49  | `Remove member done, group_id={}, member={}`                                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:607`        |
| 50  | `Member to remove is the leader so yield leadership`                                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:617`        |
| 51  | `Proposed to raft to remove member, member={}`                                                            | `lib/replication/repl_dev/raft_repl_dev.cpp:640`        |
| 52  | `member has been removed, member={}`                                                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:660`        |
| 53  | `Flip learner flag to {}, member={}`                                                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:667`        |
| 54  | `repl dev is being shutdown!`                                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:669`        |
| 55  | `Learner flag has been set to {}, member={}`                                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:684`        |
| 56  | `flip learner flag failed, not leader`                                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:692`        |
| 57  | `flip learner flag to {}, member={}`                                                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:697`        |
| 58  | `Clean replace member task, task={}, commit_quorum={}`                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:738`        |
| 59  | `repl dev is being shutdown!`                                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:740`        |
| 60  | `Clean replace member task failed, not leader`                                                            | `lib/replication/repl_dev/raft_repl_dev.cpp:745`        |
| 61  | `Task not found, task_id={}`                                                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:750`        |
| 62  | `Clean replace member task, propose to raft for HS_CTRL_CLEAN_REPLACE_TASK req, group_id={}, task_id={},` | `lib/replication/repl_dev/raft_repl_dev.cpp:759`        |
| 63  | `Clean replace member task done, group_id={}, task_id={}`                                                 | `lib/replication/repl_dev/raft_repl_dev.cpp:781`        |
| 64  | `Reset raft quorum size={}`                                                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:836`        |
| 65  | `Waiting for commit upto compact_lsn={}, current_commit_lsn={}`                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:908`        |
| 66  | `cp_flush completed before updating truncation boundary to lsn={}`                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:916`        |
| 67  | `Updating truncation boundary to lsn={}, current_truncation_boundary={}`                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:918`        |
| 68  | `Successfully created snapshot, result log_idx={}`                                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:929`        |
| 69  | `Manually compacting logs upto lsn={} after snapshot, current_compact_lsn={}`                             | `lib/replication/repl_dev/raft_repl_dev.cpp:939`        |
| 70  | `Compacted logs upto lsn={} after snapshot`                                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:942`        |
| 71  | `cp_flush completed after snapshot creation and log compaction`                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:947`        |
| 72  | `snapshot creation and compaction completed`                                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:948`        |
| 73  | `Initializing rreq failed error={}, failing this req`                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:1030`       |
| 74  | `rreq->to_string()`                                                                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:1110`       |
| 75  | `Data Channel: Error in pushing data to all followers: rreq=[{}] error={}`                                | `lib/replication/repl_dev/raft_repl_dev.cpp:1128`       |
| 76  | `Data Channel: Flip is enabled, skip on_push_data_received to simulate fetch remote data,`                | `lib/replication/repl_dev/raft_repl_dev.cpp:1171`       |
| 77  | `Raft repl start_replace_member commit, task_id={} member_out={} member_in={}`                            | `lib/replication/repl_dev/raft_repl_dev.cpp:1833`       |
| 78  | `Raft repl complete_replace_member commit, task_id={} member_out={} member_in={}`                         | `lib/replication/repl_dev/raft_repl_dev.cpp:1848`       |
| 79  | `Raft repl replace_member_task has been cleared.`                                                         | `lib/replication/repl_dev/raft_repl_dev.cpp:1863`       |
| 80  | `Raft repl remove_member commit, member={}`                                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:1868`       |
| 81  | `Raft repl clean_replace_member_task commit, task_id={}`                                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:1874`       |
| 82  | `Raft repl clean_replace_member_task: task not found, task_id={}`                                         | `lib/replication/repl_dev/raft_repl_dev.cpp:1884`       |
| 83  | `Raft repl clean_replace_member_task, callback to listener, task_id={}, member_out={}, member_in={}`      | `lib/replication/repl_dev/raft_repl_dev.cpp:1902`       |
| 84  | `Raft repl replace_member_task has been cleared, task_id={}`                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:1917`       |
| 85  | `Raft repl update truncation_upper_limit to {}`                                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:1949`       |
| 86  | `Not the leader, but have the highest priority, try to request leader, result: {}`                        | `lib/replication/repl_dev/raft_repl_dev.cpp:2060`       |
| 87  | `Current leader {} is different from expected leader {}, reconciling it, my id={}`                        | `lib/replication/repl_dev/raft_repl_dev.cpp:2088`       |
| 88  | `Yielded leadership`                                                                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:2092`       |
| 89  | `Saved config {}`                                                                                         | `lib/replication/repl_dev/raft_repl_dev.cpp:2227`       |
| 90  | `Saved state in binary format (size={} bytes): term={}, voted_for={},`                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:2241`       |
| 91  | `No existing state found, using default state`                                                            | `lib/replication/repl_dev/raft_repl_dev.cpp:2254`       |
| 92  | `Loaded state in binary format (size={} bytes): term={}, voted_for={},`                                   | `lib/replication/repl_dev/raft_repl_dev.cpp:2266`       |
| 93  | `Permanent destroy for raft repl dev group_id={}`                                                         | `lib/replication/repl_dev/raft_repl_dev.cpp:2318`       |
| 94  | `RaftReplDev leave group_id={}`                                                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:2357`       |
| 95  | `Raft repl dev is destroyed, ignore flush durable commit lsn`                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:2514`       |
| 96  | `Raft repl dev is destroyed or unready, ignore check replace member status`                               | `lib/replication/repl_dev/raft_repl_dev.cpp:2527`       |
| 97  | `Complete replace member, task_id={}, replica_in={}, replica_out={}`                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:2592`       |
| 98  | `Raft repl dev is destroyed, ignore cp flush`                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:2599`       |
| 99  | `m_repl_key_req_map size is {};`                                                                          | `lib/replication/repl_dev/raft_repl_dev.cpp:2656`       |
| 100 | `Raft Channel: repl dev is in UNREADY stage, skip log replay.`                                            | `lib/replication/repl_dev/raft_repl_dev.cpp:2731`       |
| 101 | `Raft Channel: Log {} is outdated and will be handled by baseline resync. Ignoring replay.`               | `lib/replication/repl_dev/raft_repl_dev.cpp:2736`       |
| 102 | `Pause state machine for group_id={}`                                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:3010`       |
| 103 | `Resume state machine execution for group_id={}`                                                          | `lib/replication/repl_dev/raft_repl_dev.cpp:3017`       |

**tests/** (21)

| #  | Message                                                                                           | Source                                          |
|----|---------------------------------------------------------------------------------------------------|-------------------------------------------------|
| 1  | `[Replica={}] Received commit on lsn={} dsn={} key={} value[blkid={} pattern={}]`                 | `tests/test_common/raft_repl_test_base.hpp:157` |
| 2  | `[Replica={}] Received pre-commit on lsn={} dsn={}`                                               | `tests/test_common/raft_repl_test_base.hpp:173` |
| 3  | `[Replica={}] Received rollback on lsn={}`                                                        | `tests/test_common/raft_repl_test_base.hpp:180` |
| 4  | `restarted repl dev for [Replica={}] Group={}`                                                    | `tests/test_common/raft_repl_test_base.hpp:184` |
| 5  | `[Replica={}] Received error={} on key={}`                                                        | `tests/test_common/raft_repl_test_base.hpp:190` |
| 6  | `[Replica={}] Received notify_committed_lsn={}`                                                   | `tests/test_common/raft_repl_test_base.hpp:196` |
| 7  | `[Replica={}] Received config rollback at lsn={}`                                                 | `tests/test_common/raft_repl_test_base.hpp:200` |
| 8  | `[Replica={}] Received no_space_left at lsn={}, reset latch lsn since we don`t really handle it.` | `tests/test_common/raft_repl_test_base.hpp:203` |
| 9  | `[Replica={}] Got snapshot callback term={} idx={}`                                               | `tests/test_common/raft_repl_test_base.hpp:212` |
| 10 | `[Replica={}] Read logical snapshot callback first message obj_id={} term={} idx={}`              | `tests/test_common/raft_repl_test_base.hpp:236` |
| 11 | `Snapshot is_last_obj is true`                                                                    | `tests/test_common/raft_repl_test_base.hpp:256` |
| 12 | `[Replica={}] Read logical snapshot callback obj_id={} term={} idx={} num_items={}`               | `tests/test_common/raft_repl_test_base.hpp:265` |
| 13 | `[Replica={}] Save logical snapshot callback return obj_id={}`                                    | `tests/test_common/raft_repl_test_base.hpp:294` |
| 14 | `[Replica={}] Save logical snapshot callback obj_id={} term={} idx={} is_last={} num_items={}`    | `tests/test_common/raft_repl_test_base.hpp:325` |
| 15 | `[Replica={}] Apply snapshot term={} idx={}`                                                      | `tests/test_common/raft_repl_test_base.hpp:334` |
| 16 | `[Replica={}] Last snapshot term={} idx={}`                                                       | `tests/test_common/raft_repl_test_base.hpp:345` |
| 17 | `[Replica={}] Group={} is being destroyed`                                                        | `tests/test_common/raft_repl_test_base.hpp:388` |
| 18 | `[Replica={}] Db write key={} data_size={} pattern={} block_size={}`                              | `tests/test_common/raft_repl_test_base.hpp:401` |
| 19 | `[{}]: Total {} keys committed, validating them`                                                  | `tests/test_common/raft_repl_test_base.hpp:419` |
| 20 | `Validating key={} value[blkid={} pattern={}]`                                                    | `tests/test_common/raft_repl_test_base.hpp:436` |
| 21 | `[Replica={}] Db write key={} data_size={} pattern={} block_size={}`                              | `tests/test_common/raft_repl_test_base.hpp:699` |

### WARN (27)

**lib/** (27)

| #  | Message                                                                                                    | Source                                                  |
|----|------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| 1  | `RaftLogStore is asked to apply pack on lsn={}, but current lsn={} is behind, will be filling`             | `lib/replication/log_store/home_raft_log_store.cpp:340` |
| 2  | `Unable to find group_id={}, may be repl_dev was destroyed, we will destroy the raft_group_config as well` | `lib/replication/service/raft_repl_service.cpp:320`     |
| 3  | `Groupid={}, add member={} failed with error={}`                                                           | `lib/replication/service/raft_repl_service.cpp:403`     |
| 4  | `Config is changing for group_id={} while adding member={}, retry operation in a second`                   | `lib/replication/service/raft_repl_service.cpp:407`     |
| 5  | `RaftReplDev for group_id={} is not found`                                                                 | `lib/replication/service/raft_repl_service.cpp:583`     |
| 6  | `ReplDev group_id={} not found while scheduling snapshot creation`                                         | `lib/replication/service/raft_repl_service.cpp:619`     |
| 7  | `[traceID={}] block allocation failure, repl_key=[{}], status=[{}]`                                        | `lib/replication/repl_dev/common.cpp:159`               |
| 8  | `not ready to read because there are some uncommitted logs in snapshot,`                                   | `lib/replication/repl_dev/raft_state_machine.cpp:367`   |
| 9  | `Ignoring error returned from nuraft add_member, member={}, err={}`                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:545`        |
| 10 | `Remove member not found in group error, ignoring, member={}`                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:631`        |
| 11 | `Propose to raft failed due to config_changing, attempt: {}`                                               | `lib/replication/repl_dev/raft_repl_dev.cpp:802`        |
| 12 | `propose to raft for HS_CTRL_UPDATE_TRUNCATION_BOUNDARY req failed, err={}`                                | `lib/replication/repl_dev/raft_repl_dev.cpp:994`        |
| 13 | `Raft channel: Not ready to accept writes, stage={}`                                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:1012`       |
| 14 | `Data Channel: PushData received with empty buffer, ignoring this call`                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:1144`       |
| 15 | `Data Channel: PushData received with size mismatch, header size {}, data size {}, received size {}`       | `lib/replication/repl_dev/raft_repl_dev.cpp:1153`       |
| 16 | `Data Channel: PushData received with empty buffer, ignoring this call`                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:1520`       |
| 17 | `Empty response from remote!`                                                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:1612`       |
| 18 | `Raft repl clean_replace_member_task: task_id mismatch, received={}, persisted={}, skip cleaning`          | `lib/replication/repl_dev/raft_repl_dev.cpp:1889`       |
| 19 | `Raft repl clean_replace_member_task: invalid member info, skip callback`                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:1907`       |
| 20 | `exp_truncation_upper_limit {} is no larger than cur_truncation_upper_limit {}`                            | `lib/replication/repl_dev/raft_repl_dev.cpp:1942`       |
| 21 | `get_replication_quorum: msg_service is null, returning empty member list`                                 | `lib/replication/repl_dev/raft_repl_dev.cpp:2048`       |
| 22 | `Excluding peer {} from active_peers, lag {}, my lsn {}, peer lsn {}, least_active_repl_idx {}`            | `lib/replication/repl_dev/raft_repl_dev.cpp:2120`       |
| 23 | `Loaded state from legacy JSON object format: term={}, voted_for={}, election_timer_allowed={},`           | `lib/replication/repl_dev/raft_repl_dev.cpp:2282`       |
| 24 | `Raft channel: Reject append entries on follower from leader due to latch_lsn {}, start_lsn {} ~ {}`       | `lib/replication/repl_dev/raft_repl_dev.cpp:2413`       |
| 25 | `Raft channel: Reject mixed app+conf append entries to avoid config flush bypassing the`                   | `lib/replication/repl_dev/raft_repl_dev.cpp:2427`       |
| 26 | `Checking replace member status, task_id={}, Replica in {} not found in the peers, add_member might`       | `lib/replication/repl_dev/raft_repl_dev.cpp:2557`       |
| 27 | `GC rreq: Releasing blkid={} canceled`                                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:2713`       |

### ERROR (61)

**lib/** (57)

| #  | Message                                                                                               | Source                                                  |
|----|-------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| 1  | `last_entry() out_of_range={}, {}`                                                                    | `lib/replication/log_store/home_raft_log_store.cpp:155` |
| 2  | `entry_at({}) index out_of_range start {} end {}`                                                     | `lib/replication/log_store/home_raft_log_store.cpp:270` |
| 3  | `term_at({}) index out_of_range start {} end {}`                                                      | `lib/replication/log_store/home_raft_log_store.cpp:289` |
| 4  | `Raft Channel: rreq=[{}] met some errors before`                                                      | `lib/replication/log_store/repl_log_store.cpp:103`      |
| 5  | `[traceID={}] Allocate blk for rreq failed error={}`                                                  | `lib/replication/repl_dev/common.cpp:44`                |
| 6  | `[traceID={}] Allocate blk for rreq failed error={}`                                                  | `lib/replication/repl_dev/common.cpp:51`                |
| 7  | `Raft Channel: Failed to propose rreq=[{}] result_code={}`                                            | `lib/replication/repl_dev/raft_state_machine.cpp:49`    |
| 8  | `Failed to localize journal entry rkey={} jentry=[{}], we return error and let Raft resend this req`  | `lib/replication/repl_dev/raft_state_machine.cpp:144`   |
| 9  | `Raft channel: Rollback lsn {} rreq not found`                                                        | `lib/replication/repl_dev/raft_state_machine.cpp:281`   |
| 10 | `lsn={} already in precommit list, exist_term={}, is_volatile={}`                                     | `lib/replication/repl_dev/raft_state_machine.cpp:335`   |
| 11 | `Failed to bind data service request for PUSH_DATA`                                                   | `lib/replication/repl_dev/raft_repl_dev.cpp:120`        |
| 12 | `Failed to bind data service request for FETCH_DATA`                                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:126`        |
| 13 | `repl dev is not ready, stage={}`                                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:213`        |
| 14 | `Step1. Replace member, I am not leader, can not handle the request, task_id={}`                      | `lib/replication/repl_dev/raft_repl_dev.cpp:233`        |
| 15 | `Step1. Replace member, task_id={} is not the same as existing task_id={}`                            | `lib/replication/repl_dev/raft_repl_dev.cpp:254`        |
| 16 | `Step1. Replace member invalid parameter, out member is not found, task_id={}`                        | `lib/replication/repl_dev/raft_repl_dev.cpp:270`        |
| 17 | `Step1. Replace member, quorum safety check failed, active_peers={},`                                 | `lib/replication/repl_dev/raft_repl_dev.cpp:292`        |
| 18 | `Simulating set member to learner failure`                                                            | `lib/replication/repl_dev/raft_repl_dev.cpp:303`        |
| 19 | `Step2. Replace member, failed to flip out member to learner {}, task_id={}`                          | `lib/replication/repl_dev/raft_repl_dev.cpp:311`        |
| 20 | `Initializing rreq failed, rreq=[{}], error={}`                                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:335`        |
| 21 | `Step3. Replace member, propose to raft for HS_CTRL_START_REPLACE req failed, task_id={}, err={}`     | `lib/replication/repl_dev/raft_repl_dev.cpp:345`        |
| 22 | `Simulating add member failure`                                                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:355`        |
| 23 | `Step4. Replace member, add member failed, err={}, task_id={}`                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:365`        |
| 24 | `repl dev is not ready, stage={}`                                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:383`        |
| 25 | `Simulating remove member failure`                                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:401`        |
| 26 | `Step5. Replace member, failed to remove member, task_id={}, member={}, err={}`                       | `lib/replication/repl_dev/raft_repl_dev.cpp:407`        |
| 27 | `Initializing rreq failed, rreq=[{}], error={}`                                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:433`        |
| 28 | `Step6. Replace member, propose to raft for HS_CTRL_COMPLETE_REPLACE req failed , task_id={}, err={}` | `lib/replication/repl_dev/raft_repl_dev.cpp:443`        |
| 29 | `get_replace_member_status failed, other membership mismatch, task_id={}, detail={},`                 | `lib/replication/repl_dev/raft_repl_dev.cpp:496`        |
| 30 | `get_replace_member_status failed, task_id mismatch, persisted={}, received={}`                       | `lib/replication/repl_dev/raft_repl_dev.cpp:508`        |
| 31 | `Add member failed, member={}, err={}`                                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:550`        |
| 32 | `Add member failed, member={}, err={}`                                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:555`        |
| 33 | `Remove member step1. Failed to remove member err={}, member={}`                                      | `lib/replication/repl_dev/raft_repl_dev.cpp:575`        |
| 34 | `Remove member step2. Failed to propose to raft for HS_CTRL_REMOVE_MEMBER req , err={}, member={}`    | `lib/replication/repl_dev/raft_repl_dev.cpp:599`        |
| 35 | `Replace member failed to remove member, member={}, err={}`                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:636`        |
| 36 | `Flip learner flag failed {}, member={}`                                                              | `lib/replication/repl_dev/raft_repl_dev.cpp:680`        |
| 37 | `invalid parameter, member is not found, member={}`                                                   | `lib/replication/repl_dev/raft_repl_dev.cpp:700`        |
| 38 | `Propose to raft to flip learner failed, err: {}`                                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:711`        |
| 39 | `Task id mismatched, persisted_task_id={}, received_task_id={}`                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:754`        |
| 40 | `Propose to raft for HS_CTRL_CLEAN_REPLACE_TASK req failed , task_id={}, err={}`                      | `lib/replication/repl_dev/raft_repl_dev.cpp:774`        |
| 41 | `Propose to raft to set priority failed, result: {}`                                                  | `lib/replication/repl_dev/raft_repl_dev.cpp:828`        |
| 42 | `Failed to create snapshot - another snapshot may be in progress`                                     | `lib/replication/repl_dev/raft_repl_dev.cpp:926`        |
| 43 | `Initializing rreq failed, rreq=[{}], error={}`                                                       | `lib/replication/repl_dev/raft_repl_dev.cpp:982`        |
| 44 | `data blks has already been allocated and committed, failing this req`                                | `lib/replication/repl_dev/raft_repl_dev.cpp:1045`       |
| 45 | `Data Channel: Creating rreq on applier has failed, will ignore the push and let Raft channel send`   | `lib/replication/repl_dev/raft_repl_dev.cpp:1183`       |
| 46 | `Not able to fetching data from originator={}, error={}, probably originator is down. Will`           | `lib/replication/repl_dev/raft_repl_dev.cpp:1489`       |
| 47 | `Raft Channel: unexpected log {} committed before config {} committed`                                | `lib/replication/repl_dev/raft_repl_dev.cpp:1765`       |
| 48 | `Raft Channel: Error in processing rreq=[{}] error={}`                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:1780`       |
| 49 | `Raft Channel: Error has been added for rreq=[{}] error={}`                                           | `lib/replication/repl_dev/raft_repl_dev.cpp:1783`       |
| 50 | `Raft Channel: Error in processing rreq=[{}] error={}`                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:1797`       |
| 51 | `Raft Channel: Error in processing rreq=[{}] error={}`                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:1813`       |
| 52 | `Error in becoming leader: {}`                                                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:2003`       |
| 53 | `Failed to deserialize state in binary format: {}, using default state`                               | `lib/replication/repl_dev/raft_repl_dev.cpp:2272`       |
| 54 | `State data in legacy JSON object format is corrupted, using default state`                           | `lib/replication/repl_dev/raft_repl_dev.cpp:2289`       |
| 55 | `Failed to complete replace member, next time will retry it, task_id={}, error={}`                    | `lib/replication/repl_dev/raft_repl_dev.cpp:2588`       |
| 56 | `Snapshot resync data validation failed, magic={}, version={}`                                        | `lib/replication/repl_dev/raft_repl_dev.cpp:2844`       |
| 57 | `Snapshot resync data crc mismatch, received_crc={}, computed_crc={}`                                 | `lib/replication/repl_dev/raft_repl_dev.cpp:2856`       |

**tests/** (4)

| # | Message                                               | Source                                          |
|---|-------------------------------------------------------|-------------------------------------------------|
| 1 | `invalid snapshot offset={}`                          | `tests/test_common/raft_repl_test_base.hpp:224` |
| 2 | `invalid snapshot offset={}`                          | `tests/test_common/raft_repl_test_base.hpp:228` |
| 3 | `invalid snapshot offset={}`                          | `tests/test_common/raft_repl_test_base.hpp:284` |
| 4 | `[Replica={}] Error in writing data: id={}, error={}` | `tests/test_common/raft_repl_test_base.hpp:715` |

---

## Module: `base`

**Description:** Bare `LOGINFO`/`LOGDEBUG`/… calls with no module argument. Production sets this module to **DEBUG**.
There are no TRACE statements. Most INFO rows are in tests, not the runtime library.

**Macros:** `LOGDEBUG`/`LOGINFO`/`LOGWARN`/`LOGERROR`/`LOGCRITICAL` (and `DLOG*` variants)

**Log prefix:** bare message (no structured prefix)

**Source files:** `lib/homestore.cpp`, `lib/checkpoint/cp_mgr.cpp`, `lib/device/*`, `lib/blkalloc/*`, `lib/logstore/*`,
btree headers, `lib/replication/repl_dev/raft_state_machine.cpp` (a few bare logs)

**Total log statements:** 1325 (184 in lib/, 1141 in tests/)

### DEBUG (73)

**lib/** (8)

| # | Message                                                                                            | Source                                                |
|---|----------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| 1 | `Found a logstore log_dev={} log_store={} with start lsn={}, Creating a new HomeLogStore instance` | `lib/logstore/log_dev.cpp:795`                        |
| 2 | `[type={}], meta blk size check passed!`                                                           | `lib/meta/meta_blk_service.cpp:347`                   |
| 3 | `Virtual device {} is already sized correctly, no new devices to add`                              | `lib/device/device_manager.cpp:275`                   |
| 4 | `total size of type {} in this homestore is {}`                                                    | `lib/device/device_manager.cpp:520`                   |
| 5 | `size of all added pdevs={}, current_chunk_num={} of type {} in vdev {}`                           | `lib/device/device_manager.cpp:527`                   |
| 6 | `pdev {} is already added to vdev {}, skip it`                                                     | `lib/device/device_manager.cpp:533`                   |
| 7 | `save_snp_resync_data success, next obj_id={}`                                                     | `lib/replication/repl_dev/raft_state_machine.cpp:406` |
| 8 | `chunk {} has successfully allocated nblks: {}, totally used blks: {}, available_blks: {}, actual` | `lib/blkalloc/append_blk_allocator.cpp:90`            |

**tests/** (65)

| #  | Message                                                                                            | Source                                            |
|----|----------------------------------------------------------------------------------------------------|---------------------------------------------------|
| 1  | `Not added to group yet`                                                                           | `tests/test_raft_repl_dev_dynamic.cpp:125`        |
| 2  | `Node1:\n {}`                                                                                      | `tests/test_btree_node.cpp:317`                   |
| 3  | `Node2:\n {}`                                                                                      | `tests/test_btree_node.cpp:318`                   |
| 4  | `Creating a hole with size of 11 for prefix compaction usecase`                                    | `tests/test_btree_node.cpp:372`                   |
| 5  | `After random insertion of {} objects`                                                             | `tests/test_btree_node.cpp:449`                   |
| 6  | `After random removal of {} objects`                                                               | `tests/test_btree_node.cpp:458`                   |
| 7  | `After update of {} entries`                                                                       | `tests/test_btree_node.cpp:469`                   |
| 8  | `blk inserted: {}, is_multi:{}, crc: {}, integer: {}`                                              | `tests/test_data_service.cpp:793`                 |
| 9  | `Restart homestore`                                                                                | `tests/test_log_store_long_run.cpp:546`           |
| 10 | `Recovered lsn {}:{} with log data of size {}`                                                     | `tests/test_log_store.cpp:337`                    |
| 11 | `Printing json dump of all logstores in logdev {}. \n {}`                                          | `tests/test_log_store.cpp:641`                    |
| 12 | `Printing json dump of log_dev={} logstore id {}, start_seq {}, end_seq {}, \n\n {}`               | `tests/test_log_store.cpp:666`                    |
| 13 | `truncating to offset: 0x{}, desc: {}`                                                             | `tests/test_journal_vdev.cpp:183`                 |
| 14 | `remove offset: 0x{}`                                                                              | `tests/test_journal_vdev.cpp:197`                 |
| 15 | `truncating before start offset, looping back to beginning devices at offset: 0x{}`                | `tests/test_journal_vdev.cpp:204`                 |
| 16 | `remove offset: 0x{}`                                                                              | `tests/test_journal_vdev.cpp:210`                 |
| 17 | `write: {}, read: {}, truncate: {}, truncate_loop_back: {}`                                        | `tests/test_journal_vdev.cpp:229`                 |
| 18 | `reading on offset: 0x{}, size: {}, start: 0x{}, tail: 0x{}`                                       | `tests/test_journal_vdev.cpp:302`                 |
| 19 | `writing to bytes offset: 0x{}, size: {}, write_count: {} start: 0x{}, tail: 0x{} crc: 0x{}`       | `tests/test_journal_vdev.cpp:346`                 |
| 20 | `All keys are in use, skipping operation generation. end_range_ {} start_range_ {}`                | `tests/test_index_crash_recovery.cpp:125`         |
| 21 | `Not enough keys are in use, skipping operation generation. in_use_key_cnt_ {} numOperations {}`   | `tests/test_index_crash_recovery.cpp:131`         |
| 22 | `Reapply: Inserting key {}`                                                                        | `tests/test_index_crash_recovery.cpp:424`         |
| 23 | `Reapply: Removing key {}`                                                                         | `tests/test_index_crash_recovery.cpp:427`         |
| 24 | `Reapply: Inserting key {}`                                                                        | `tests/test_index_crash_recovery.cpp:439`         |
| 25 | `Reapply: Removing key {}`                                                                         | `tests/test_index_crash_recovery.cpp:443`         |
| 26 | `Lets before crash print operations\n{}`                                                           | `tests/test_index_crash_recovery.cpp:597`         |
| 27 | `No operations generated, skipping round {}`                                                       | `tests/test_index_crash_recovery.cpp:632`         |
| 28 | `Lets before crash print operations\n{}`                                                           | `tests/test_index_crash_recovery.cpp:704`         |
| 29 | `Removing key {}`                                                                                  | `tests/test_index_crash_recovery.cpp:715`         |
| 30 | `Inserting key {}`                                                                                 | `tests/test_index_crash_recovery.cpp:726`         |
| 31 | `metrics: \n{}`                                                                                    | `tests/test_index_crash_recovery.cpp:1347`        |
| 32 | `\n{}:\nmetrics (interior, leaf, height):\ncompute ({}, {}, {})\nbtree ({}, {}, {})\nmetrics ({},` | `tests/test_index_crash_recovery.cpp:1355`        |
| 33 | `All appends completed for iteration={} and waiting is done, outstanding = {}`                     | `tests/log_store_benchmark.cpp:85`                |
| 34 | `Notify that append has reached limit outstanding = {}`                                            | `tests/log_store_benchmark.cpp:95`                |
| 35 | `Notify that append has completed, outstanding = {}`                                               | `tests/log_store_benchmark.cpp:103`               |
| 36 | `Appending log entry for iteration_ind={} ind={}`                                                  | `tests/log_store_benchmark.cpp:114`               |
| 37 | `Recovered lsn {}:{} with log data of size {}`                                                     | `tests/log_store_benchmark.cpp:126`               |
| 38 | `Before write, cap stats: used={} total={}`                                                        | `tests/test_solo_repl_dev.cpp:233`                |
| 39 | `Before write, cap stats: used={} total={}`                                                        | `tests/test_solo_repl_dev.cpp:260`                |
| 40 | `[{}] Validating replay of lsn={} blkid = {}`                                                      | `tests/test_solo_repl_dev.cpp:290`                |
| 41 | `[{}] Replay of lsn={} blkid={} validated successfully`                                            | `tests/test_solo_repl_dev.cpp:302`                |
| 42 | `[{}] Validating of blkid={} validated successfully`                                               | `tests/test_solo_repl_dev.cpp:325`                |
| 43 | `Write complete with cap stats: used={} total={}`                                                  | `tests/test_solo_repl_dev.cpp:341`                |
| 44 | `Read data blkid={} len={} data={}`                                                                | `tests/test_solo_repl_dev.cpp:356`                |
| 45 | `Starting GC for chunk {}`                                                                         | `tests/test_index_gc.cpp:165`                     |
| 46 | `Preload done for index {}`                                                                        | `tests/test_index_gc.cpp:214`                     |
| 47 | `GC done for index {}`                                                                             | `tests/test_index_gc.cpp:221`                     |
| 48 | `Step 1: Do forward sequential insert for [{},{}] entries`                                         | `tests/test_mem_btree.cpp:314`                    |
| 49 | `Tombstoned {} keys:\n{}`                                                                          | `tests/test_mem_btree.cpp:328`                    |
| 50 | `GC {} keys:\n{}`                                                                                  | `tests/test_mem_btree.cpp:417`                    |
| 51 | `GC {} keys:\n{}`                                                                                  | `tests/test_mem_btree.cpp:421`                    |
| 52 | `free size: {}, total size: {}, used size: {}, available blks: {}`                                 | `tests/test_meta_blk_mgr.cpp:198`                 |
| 53 | `rand load, io seq {}, type {}`                                                                    | `tests/test_meta_blk_mgr.cpp:458`                 |
| 54 | `Flip {} set`                                                                                      | `tests/test_meta_blk_mgr.cpp:709`                 |
| 55 | `Range remove from {} to {} returned {}`                                                           | `tests/btree_helpers/btree_test_helper.hpp:315`   |
| 56 | `Waiting for m_crash_recovered future`                                                             | `tests/test_common/homestore_test_common.hpp:224` |
| 57 | `Flip {} set`                                                                                      | `tests/test_common/homestore_test_common.hpp:253` |
| 58 | `Flip {} set`                                                                                      | `tests/test_common/homestore_test_common.hpp:262` |
| 59 | `Flip {} removed`                                                                                  | `tests/test_common/homestore_test_common.hpp:267` |
| 60 | `CP Flush completed`                                                                               | `tests/test_common/homestore_test_common.hpp:357` |
| 61 | `data already exists in mem db, key={}`                                                            | `tests/test_common/raft_repl_test_base.hpp:359`   |
| 62 | `write data task complete, id={}`                                                                  | `tests/test_common/raft_repl_test_base.hpp:713`   |
| 63 | `replace member task still exists`                                                                 | `tests/test_common/raft_repl_test_base.hpp:888`   |
| 64 | `replace member task has been cleaned up`                                                          | `tests/test_common/raft_repl_test_base.hpp:895`   |
| 65 | `peer_info: id={} can_vote={} priority={}`                                                         | `tests/test_common/raft_repl_test_base.hpp:900`   |

### INFO (1168)

**lib/** (115)

| #   | Message                                                                                                      | Source                                                |
|-----|--------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| 1   | `non sorted entry : {} -> {}`                                                                                | `include/homestore/btree/detail/simple_node.hpp:346`  |
| 2   | `node locked by file: {}, line: {}`                                                                          | `include/homestore/btree/detail/btree_node.hpp:585`   |
| 3   | `Filter callback rejected the update for key {}`                                                             | `include/homestore/btree/detail/variant_node.hpp:214` |
| 4   | `Attempt to insert duplicate entry {}`                                                                       | `include/homestore/btree/detail/variant_node.hpp:221` |
| 5   | `Attempt to update non-existent entry {}`                                                                    | `include/homestore/btree/detail/variant_node.hpp:227` |
| 6   | `reading node failed for bnodeid: {} reason: {}`                                                             | `include/homestore/btree/detail/btree_common.ipp:422` |
| 7   | `HomeStore DEBUG version: {}`                                                                                | `lib/homestore.cpp:138`                               |
| 8   | `HomeStore RELEASE version: {}`                                                                              | `lib/homestore.cpp:140`                               |
| 9   | `Homestore is loading with following services: {}`                                                           | `lib/homestore.cpp:169`                               |
| 10  | `HomeStore starting first_time_boot?={} dynamic_config_version={}, cache_size={}, static_config: {}`         | `lib/homestore.cpp:284`                               |
| 11  | `Homestore shutdown is started`                                                                              | `lib/homestore.cpp:324`                               |
| 12  | `Homestore is completed its shutdown`                                                                        | `lib/homestore.cpp:379`                               |
| 13  | `HomeStore starting with dynamic config version: {} static config: {}`                                       | `lib/homestore.cpp:438`                               |
| 14  | `Dirty buffer exceeded count {} critical {}`                                                                 | `lib/checkpoint/cp_mgr.cpp:43`                        |
| 15  | `Using cp_timer_ms option value: {}`                                                                         | `lib/checkpoint/cp_mgr.cpp:64`                        |
| 16  | `cp timer is set to {} usec`                                                                                 | `lib/checkpoint/cp_mgr.cpp:98`                        |
| 17  | `cp timer expired {} times, running once`                                                                    | `lib/checkpoint/cp_mgr.cpp:103`                       |
| 18  | `Stopping cp timer`                                                                                          | `lib/checkpoint/cp_mgr.cpp:125`                       |
| 19  | `Trigger cp flush at CP shutdown`                                                                            | `lib/checkpoint/cp_mgr.cpp:133`                       |
| 20  | `Trigger cp done`                                                                                            | `lib/checkpoint/cp_mgr.cpp:142`                       |
| 21  | `Starting CP IO fibers with count: {}`                                                                       | `lib/checkpoint/cp_mgr.cpp:360`                       |
| 22  | `CP watchdog timer setting to : {} seconds`                                                                  | `lib/checkpoint/cp_mgr.cpp:438`                       |
| 23  | `cp watchdog timer expired {} times, running once`                                                           | `lib/checkpoint/cp_mgr.cpp:443`                       |
| 24  | `cp progress percent {} is not changed. time elapsed {}, cp state={}`                                        | `lib/checkpoint/cp_mgr.cpp:493`                       |
| 25  | `Flip set_minimum_chunk_size is enabled, min_chunk_size now is {}`                                           | `lib/logstore/log_store_service.cpp:77`               |
| 26  | `Created logdev {}`                                                                                          | `lib/logstore/log_store_service.cpp:199`              |
| 27  | `log flush timer expired {} times, running once`                                                             | `lib/logstore/log_dev.cpp:192`                        |
| 28  | `skip log entries for store id {}-{}, ios {}`                                                                | `lib/logstore/log_dev.cpp:708`                        |
| 29  | `Removing log_dev={} log_store={}`                                                                           | `lib/logstore/log_dev.cpp:769`                        |
| 30  | `MetaBlkService create vdev was failed last time. Should retry it with init flag`                            | `lib/meta/meta_blk_service.cpp:80`                    |
| 31  | `Initialize MetaBlkStore with total size={}, used size={}, need_format={}`                                   | `lib/meta/meta_blk_service.cpp:89`                    |
| 32  | `Successfully loaded meta ssb from disk: {}`                                                                 | `lib/meta/meta_blk_service.cpp:156`                   |
| 33  | `Successfully write m_ssb to disk: {}`                                                                       | `lib/meta/meta_blk_service.cpp:212`                   |
| 34  | `[type={}], fixing mblk's context_sz from {} to read_sz: {}`                                                 | `lib/meta/meta_blk_service.cpp:340`                   |
| 35  | `[type={}] being registered after scanned from disk.`                                                        | `lib/meta/meta_blk_service.cpp:383`                   |
| 36  | `Gettting status with log_level: {}`                                                                         | `lib/meta/meta_blk_service.cpp:1469`                  |
| 37  | `Removing old dump dir: {}`                                                                                  | `lib/meta/meta_blk_service.cpp:1486`                  |
| 38  | `Error getting space for dir={}, error={}, skip dumping to file`                                             | `lib/meta/meta_blk_service.cpp:1495`                  |
| 39  | `Free Blks Slab distribution is not initialized, possibly first boot - setting with defaults`                | `lib/common/homestore_config.hpp:93`                  |
| 40  | `Some settings are defaultted or overridden explicitly in the code, saving the new settings`                 | `lib/common/homestore_config.hpp:105`                 |
| 41  | `Cancel resource manager timer.`                                                                             | `lib/common/resource_mgr.cpp:34`                      |
| 42  | `Resource manager is stopped, so not triggering truncate`                                                    | `lib/common/resource_mgr.cpp:56`                      |
| 43  | `Resource manager is triggering truncate`                                                                    | `lib/common/resource_mgr.cpp:72`                      |
| 44  | `resource audit timer is set to {} ms`                                                                       | `lib/common/resource_mgr.cpp:82`                      |
| 45  | `resource audit timer is set to 0, so not starting timer`                                                    | `lib/common/resource_mgr.cpp:84`                      |
| 46  | `resource audit timer expired {} times, running once`                                                        | `lib/common/resource_mgr.cpp:91`                      |
| 47  | `q depth increased to {}`                                                                                    | `lib/common/resource_mgr.cpp:225`                     |
| 48  | `Ignoring same-CP created+freed buf {} for allocator free`                                                   | `lib/index/wb_cache.cpp:674`                          |
| 49  | `Flush the vdev to ensure all cp information is created`                                                     | `lib/index/wb_cache.cpp:958`                          |
| 50  | `crash simulation is ongoing, so skip the cp flush`                                                          | `lib/index/wb_cache.cpp:968`                          |
| 51  | `Simulating crash after partially preflushing root-transition node {}`                                       | `lib/index/wb_cache.cpp:997`                          |
| 52  | `Crash simulation is ongoing; aid simulation by not flushing.`                                               | `lib/index/wb_cache.cpp:1044`                         |
| 53  | `Simulating crash while writing buffer {}, stored in file {}`                                                | `lib/index/wb_cache.cpp:1049`                         |
| 54  | `sb metrics interior {}, leaf: {} depth {}`                                                                  | `lib/index/index_service.cpp:107`                     |
| 55  | `cp dag is stored in file {}`                                                                                | `lib/index/index_cp.cpp:184`                          |
| 56  | `{}`                                                                                                         | `lib/index/index_cp.cpp:252`                          |
| 57  | `{}`                                                                                                         | `lib/index/index_cp.cpp:431`                          |
| 58  | `Opening device {} with {} mode.`                                                                            | `lib/device/physical_dev.cpp:89`                      |
| 59  | `Device {} opened with dev_id={} size={}`                                                                    | `lib/device/physical_dev.cpp:110`                     |
| 60  | `AppendBlkAllocator resets in-place, skip creating new instance for chunk {}`                                | `lib/device/virtual_dev.cpp:139`                      |
| 61  | `writing zero for chunk: {}, size: {}, offset: {}`                                                           | `lib/device/virtual_dev.cpp:161`                      |
| 62  | `Flip triggered: after_get_allocator_shared for chunk_id={}`                                                 | `lib/device/virtual_dev.cpp:763`                      |
| 63  | `Critical journal vdev size threshold reached. Triggering truncate.`                                         | `lib/device/journal_vdev.cpp:64`                      |
| 64  | `Journal vdev init done`                                                                                     | `lib/device/journal_vdev.cpp:137`                     |
| 65  | `sync_next_read size_rd {} chunk {} seek_cursor {} end_of_chunk {} {}`                                       | `lib/device/journal_vdev.cpp:409`                     |
| 66  | `{}`                                                                                                         | `lib/device/journal_vdev.cpp:806`                     |
| 67  | `Flip triggered: before_allocator_reset for chunk_id={}`                                                     | `lib/device/chunk.cpp:69`                             |
| 68  | `Flip triggered: after_allocator_reset for chunk_id={}`                                                      | `lib/device/chunk.cpp:78`                             |
| 69  | `Overridding HDD open flags from DIRECT_IO to BUFFERED_IO`                                                   | `lib/device/device_manager.cpp:82`                    |
| 70  | `Formatting Homestore on Device[dev_name={}, pdev_id={}] with first block as: [{}] total_super_blk_size={}`  | `lib/device/device_manager.cpp:142`                   |
| 71  | `Empty first block found on device {}, format it`                                                            | `lib/device/device_manager.cpp:211`                   |
| 72  | `Loading Homestore from Device={} with first block as: [{}]`                                                 | `lib/device/device_manager.cpp:219`                   |
| 73  | `newer generation number {} found in device {}, updating first block header`                                 | `lib/device/device_manager.cpp:232`                   |
| 74  | `Device {} has been formatted, pdev_id {}`                                                                   | `lib/device/device_manager.cpp:242`                   |
| 75  | `Skipping adding new devices to vdev {}, as it is dynamic or single pdev type`                               | `lib/device/device_manager.cpp:260`                   |
| 76  | `Virtual device {} is undersized, pdevs already added={}, qualified pdevs ={}, need to add new`              | `lib/device/device_manager.cpp:279`                   |
| 77  | `Added pdev[name={}, id={}] with total_chunk_num_in_pdev={} to vdev {}`                                      | `lib/device/device_manager.cpp:295`                   |
| 78  | `commit formatting first block with gen_number={}`                                                           | `lib/device/device_manager.cpp:313`                   |
| 79  | `HomeStore formatting is committed on all physical devices`                                                  | `lib/device/device_manager.cpp:332`                   |
| 80  | `{} chunks is created on pdev {} for vdev {}, pdev data size is {}`                                          | `lib/device/device_manager.cpp:381`                   |
| 81  | `{} chunks is created for vdev {}, expected {}`                                                              | `lib/device/device_manager.cpp:388`                   |
| 82  | `Virtal Dev={} of size={} successfully created`                                                              | `lib/device/device_manager.cpp:399`                   |
| 83  | `{} Virtual device is attempted to be created with num_chunks={}, it needs to be adjust to`                  | `lib/device/device_manager.cpp:437`                   |
| 84  | `{} Virtual device is attempted to be created with size={}, it needs to be rounded to new_size={}`           | `lib/device/device_manager.cpp:445`                   |
| 85  | `{} Virtual device is attempted to be created with chunk_size={}, it needs to be adjust to`                  | `lib/device/device_manager.cpp:457`                   |
| 86  | `{} Virtual device is attempted to be created with size={}, it needs to be rounded to new_size={}`           | `lib/device/device_manager.cpp:464`                   |
| 87  | `{} Virtual device is attempted to be created with chunk_size={}, it needs to be adjust to`                  | `lib/device/device_manager.cpp:481`                   |
| 88  | `{} Virtual device is attempted to be created with size={}, it needs to be rounded to new_size={}`           | `lib/device/device_manager.cpp:489`                   |
| 89  | `New Virtal Dev={} of size={} with id={} is attempted to be created with multi_pdev_opts={}. The params are` | `lib/device/device_manager.cpp:500`                   |
| 90  | `pdev {} should add {} chunks to vdev {} , expect_chunk_num_on_pdev={}, available_chunks_on_pdev={},`        | `lib/device/device_manager.cpp:542`                   |
| 91  | `Add pdev {} to vdev {}, chunks_on_pdev={}`                                                                  | `lib/device/device_manager.cpp:553`                   |
| 92  | `Repl devs load completed, calling upper layer on_repl_devs_init_completed`                                  | `lib/replication/service/generic_repl_svc.cpp:90`     |
| 93  | `Starting RaftReplService with server_uuid={} port={}`                                                       | `lib/replication/service/raft_repl_service.cpp:105`   |
| 94  | `Repl devs load completed, calling upper layer on_repl_devs_init_completed`                                  | `lib/replication/service/raft_repl_service.cpp:157`   |
| 95  | `Starting LogStore service, fist_boot = {}`                                                                  | `lib/replication/service/raft_repl_service.cpp:174`   |
| 96  | `Started LogStore service, log replay should already done till this point`                                   | `lib/replication/service/raft_repl_service.cpp:176`   |
| 97  | `Starting DataService`                                                                                       | `lib/replication/service/raft_repl_service.cpp:178`   |
| 98  | `Repl dev is unready, skip join group, group_id={}`                                                          | `lib/replication/service/raft_repl_service.cpp:185`   |
| 99  | `raft_repl_service has been destructed!`                                                                     | `lib/replication/service/raft_repl_service.cpp:249`   |
| 100 | `file change event for {}, deleted? {}`                                                                      | `lib/replication/service/raft_repl_service.cpp:256`   |
| 101 | `file {} deleted,`                                                                                           | `lib/replication/service/raft_repl_service.cpp:277`   |
| 102 | `There are {} restart requests pending, will not restart services now. filepath: {}`                         | `lib/replication/service/raft_repl_service.cpp:287`   |
| 103 | `Restarting Raft services for file change event on {}`                                                       | `lib/replication/service/raft_repl_service.cpp:291`   |
| 104 | `Block size mismatch total_size={} sgs_size={}`                                                              | `lib/replication/repl_dev/solo_repl_dev.cpp:137`      |
| 105 | `SoloReplDev skipping already committed log_entry lsn={}, m_commit_upto at lsn={}`                           | `lib/replication/repl_dev/solo_repl_dev.cpp:182`      |
| 106 | `System exiting with code [{}]`                                                                              | `lib/replication/repl_dev/raft_repl_dev.h:472`        |
| 107 | `Hit flip baseline_resync_restart_new_follower crashing`                                                     | `lib/replication/repl_dev/raft_state_machine.cpp:435` |
| 108 | `Raft repl dev destroy_group={}`                                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:883`      |
| 109 | `repl dev is being shutdown!`                                                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:1968`     |
| 110 | `repl dev is not active!`                                                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:1972`     |
| 111 | `repl dev is being shutdown!`                                                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:1982`     |
| 112 | `repl dev is not active!`                                                                                    | `lib/replication/repl_dev/raft_repl_dev.cpp:1986`     |
| 113 | `repl dev is being shutdown!`                                                                                | `lib/replication/repl_dev/raft_repl_dev.cpp:1994`     |
| 114 | `Flip triggered: inside_append_cp_flush`                                                                     | `lib/blkalloc/append_blk_allocator.cpp:134`           |
| 115 | `m_fb_cache total free blks: {}`                                                                             | `lib/blkalloc/varsize_blk_allocator.cpp:84`           |

**tests/** (1053)

| #    | Message                                                                                                     | Source                                            |
|------|-------------------------------------------------------------------------------------------------------------|---------------------------------------------------|
| 1    | `Loading data chunk details={}`                                                                             | `tests/test_pdev.cpp:108`                         |
| 2    | `Loading fast chunk details={}`                                                                             | `tests/test_pdev.cpp:113`                         |
| 3    | `creating {} data device files with each of size {}`                                                        | `tests/test_pdev.cpp:125`                         |
| 4    | `creating {} fast device files with each of size {}`                                                        | `tests/test_pdev.cpp:134`                         |
| 5    | `Restart device manager`                                                                                    | `tests/test_pdev.cpp:156`                         |
| 6    | `Step 1: Creating 4 data pdev chunks in one shot each of size={}`                                           | `tests/test_pdev.cpp:162`                         |
| 7    | `Step 2: Creating 4 data pdev chunks independetly of smaller size={}`                                       | `tests/test_pdev.cpp:166`                         |
| 8    | `Step 3: Creating 4 fast pdev chunks in one shot each of size={}`                                           | `tests/test_pdev.cpp:174`                         |
| 9    | `Step 4: Creating 4 fast pdev chunks independently of smaller size={}`                                      | `tests/test_pdev.cpp:178`                         |
| 10   | `Step 5: Restart device manager to successfully load the chunks`                                            | `tests/test_pdev.cpp:185`                         |
| 11   | `Step 6: Removing a bigger data chunk of size={} and fast chunk of size={}`                                 | `tests/test_pdev.cpp:188`                         |
| 12   | `Step 7: Restart device manager to successfully load the chunks`                                            | `tests/test_pdev.cpp:193`                         |
| 13   | `Step 8: Post restart, create 2 smaller data chunks of size={}, overall size is same as`                    | `tests/test_pdev.cpp:196`                         |
| 14   | `Step 9: Post restart, create 2 smaller fast chunks of size={}, overall size is same as`                    | `tests/test_pdev.cpp:202`                         |
| 15   | `Step 10: Restart device manager to successfully load the chunks`                                           | `tests/test_pdev.cpp:208`                         |
| 16   | `Creating random sized chunk of size={}`                                                                    | `tests/test_pdev.cpp:234`                         |
| 17   | `Creating random sized chunk of size={} didn't find suitable space`                                         | `tests/test_pdev.cpp:240`                         |
| 18   | `Removing random sized chunk of size={}`                                                                    | `tests/test_pdev.cpp:252`                         |
| 19   | `Test created {} chunks and removed {} chunks successfully, final available size={}`                        | `tests/test_pdev.cpp:261`                         |
| 20   | `CP={}, CPContext has {} values to be flushed/validated`                                                    | `tests/test_cp_mgr.cpp:57`                        |
| 21   | `Step 1: Simulate IO on cp session for {} records`                                                          | `tests/test_cp_mgr.cpp:150`                       |
| 22   | `Step 2: Trigger a new cp without waiting for it to complete`                                               | `tests/test_cp_mgr.cpp:156`                       |
| 23   | `Step 3: Simulate IO parallel to CP for {} records`                                                         | `tests/test_cp_mgr.cpp:159`                       |
| 24   | `Step 4: Trigger a back-to-back cp`                                                                         | `tests/test_cp_mgr.cpp:165`                       |
| 25   | `Step 5: Simulate rescheduled IO for {} records`                                                            | `tests/test_cp_mgr.cpp:169`                       |
| 26   | `Step 6: Trigger a cp to validate`                                                                          | `tests/test_cp_mgr.cpp:175`                       |
| 27   | `ReplaceMember test started replica={}`                                                                     | `tests/test_raft_repl_dev_dynamic.cpp:29`         |
| 28   | `Writing on leader num_io={} replica={}`                                                                    | `tests/test_raft_repl_dev_dynamic.cpp:50`         |
| 29   | `Wait for commits replica={}`                                                                               | `tests/test_raft_repl_dev_dynamic.cpp:55`         |
| 30   | `sync_for_verify_state replica={}`                                                                          | `tests/test_raft_repl_dev_dynamic.cpp:60`         |
| 31   | `Validate all data written so far by reading them replica={}`                                               | `tests/test_raft_repl_dev_dynamic.cpp:71`         |
| 32   | `data synced, sync_for_verify_state replica={}`                                                             | `tests/test_raft_repl_dev_dynamic.cpp:75`         |
| 33   | `Waiting for repl dev to get destroyed on out member replica={}`                                            | `tests/test_raft_repl_dev_dynamic.cpp:85`         |
| 34   | `Repl dev destroyed on out member replica={}`                                                               | `tests/test_raft_repl_dev_dynamic.cpp:87`         |
| 35   | `ReplaceMember test done replica={}`                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:96`         |
| 36   | `ReplaceMember test started replica={}`                                                                     | `tests/test_raft_repl_dev_dynamic.cpp:102`        |
| 37   | `Writing on leader num_io={} replica={}`                                                                    | `tests/test_raft_repl_dev_dynamic.cpp:118`        |
| 38   | `Wait for being added to group, replica ={}`                                                                | `tests/test_raft_repl_dev_dynamic.cpp:123`        |
| 39   | `sync_for_verify_start replica={}`                                                                          | `tests/test_raft_repl_dev_dynamic.cpp:131`        |
| 40   | `rollback triggered, sync_for_verify_start replica={}`                                                      | `tests/test_raft_repl_dev_dynamic.cpp:143`        |
| 41   | `Waiting for repl dev to get destroyed on out member replica={}`                                            | `tests/test_raft_repl_dev_dynamic.cpp:152`        |
| 42   | `Repl dev destroyed on in member replica={}`                                                                | `tests/test_raft_repl_dev_dynamic.cpp:155`        |
| 43   | `Validate all data written so far by reading them replica={}`                                               | `tests/test_raft_repl_dev_dynamic.cpp:161`        |
| 44   | `ReplaceMember test done replica={}`                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:167`        |
| 45   | `TwoMemberDown test started replica={}`                                                                     | `tests/test_raft_repl_dev_dynamic.cpp:171`        |
| 46   | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev_dynamic.cpp:176`        |
| 47   | `Shutdown replica 1`                                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:193`        |
| 48   | `Shutdown replica 2`                                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:198`        |
| 49   | `Sleep 10 seconds to waiting for leadership expiring`                                                       | `tests/test_raft_repl_dev_dynamic.cpp:201`        |
| 50   | `Replace member started, task_id={}`                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:209`        |
| 51   | `Replace member returned NOT_LEADER, retry {}/{}`                                                           | `tests/test_raft_repl_dev_dynamic.cpp:228`        |
| 52   | `Leader completed num_io={}`                                                                                | `tests/test_raft_repl_dev_dynamic.cpp:233`        |
| 53   | `Member in got all commits`                                                                                 | `tests/test_raft_repl_dev_dynamic.cpp:238`        |
| 54   | `Validate all data written so far by reading them replica={}`                                               | `tests/test_raft_repl_dev_dynamic.cpp:243`        |
| 55   | `Start replica 1`                                                                                           | `tests/test_raft_repl_dev_dynamic.cpp:258`        |
| 56   | `Start replica 2`                                                                                           | `tests/test_raft_repl_dev_dynamic.cpp:263`        |
| 57   | `TwoMemberDown test done replica={}`                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:269`        |
| 58   | `OutMemberDown test started replica={}`                                                                     | `tests/test_raft_repl_dev_dynamic.cpp:276`        |
| 59   | `Writing on leader num_io={} replica={}`                                                                    | `tests/test_raft_repl_dev_dynamic.cpp:292`        |
| 60   | `Shutdown replica 2`                                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:297`        |
| 61   | `Wait for commits replica={}`                                                                               | `tests/test_raft_repl_dev_dynamic.cpp:304`        |
| 62   | `sync_for_verify_state replica={}`                                                                          | `tests/test_raft_repl_dev_dynamic.cpp:309`        |
| 63   | `Validate all data written so far by reading them replica={}`                                               | `tests/test_raft_repl_dev_dynamic.cpp:312`        |
| 64   | `Shutdown replica 2`                                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:318`        |
| 65   | `data synced, sync for completing replace member, replica={}`                                               | `tests/test_raft_repl_dev_dynamic.cpp:322`        |
| 66   | `Start replica 2`                                                                                           | `tests/test_raft_repl_dev_dynamic.cpp:332`        |
| 67   | `Waiting for repl dev to get destroyed on out member replica={}`                                            | `tests/test_raft_repl_dev_dynamic.cpp:340`        |
| 68   | `Repl dev destroyed on out member replica={}`                                                               | `tests/test_raft_repl_dev_dynamic.cpp:342`        |
| 69   | `Simulate reaper thread to complete_replace_member`                                                         | `tests/test_raft_repl_dev_dynamic.cpp:355`        |
| 70   | `OutMemberDown test done replica={}`                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:364`        |
| 71   | `LeaderReplace test started replica={}`                                                                     | `tests/test_raft_repl_dev_dynamic.cpp:372`        |
| 72   | `Writing on leader num_io={} replica={}`                                                                    | `tests/test_raft_repl_dev_dynamic.cpp:386`        |
| 73   | `Replace old leader`                                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:392`        |
| 74   | `Replace member leader yield done`                                                                          | `tests/test_raft_repl_dev_dynamic.cpp:395`        |
| 75   | `Replace member old leader done`                                                                            | `tests/test_raft_repl_dev_dynamic.cpp:400`        |
| 76   | `Wait for commits replica={}`                                                                               | `tests/test_raft_repl_dev_dynamic.cpp:404`        |
| 77   | `Validate all data written so far by reading them replica={}`                                               | `tests/test_raft_repl_dev_dynamic.cpp:411`        |
| 78   | `data synced, sync_for_verify_state replica={}`                                                             | `tests/test_raft_repl_dev_dynamic.cpp:415`        |
| 79   | `Waiting for repl dev to get destroyed on out member replica={}`                                            | `tests/test_raft_repl_dev_dynamic.cpp:428`        |
| 80   | `Repl dev destroyed on out member replica={}`                                                               | `tests/test_raft_repl_dev_dynamic.cpp:430`        |
| 81   | `LeaderReplace test done replica={}`                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:440`        |
| 82   | `OneMemberRestart test started replica={}`                                                                  | `tests/test_raft_repl_dev_dynamic.cpp:447`        |
| 83   | `Restart replica 1,`                                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:460`        |
| 84   | `Writing on leader num_io={} replica={}`                                                                    | `tests/test_raft_repl_dev_dynamic.cpp:466`        |
| 85   | `Wait for commits replica={}`                                                                               | `tests/test_raft_repl_dev_dynamic.cpp:472`        |
| 86   | `sync_for_verify_state replica={}`                                                                          | `tests/test_raft_repl_dev_dynamic.cpp:477`        |
| 87   | `Validate all data written so far by reading them replica={}`                                               | `tests/test_raft_repl_dev_dynamic.cpp:480`        |
| 88   | `data synced, sync_for_verify_state replica={}`                                                             | `tests/test_raft_repl_dev_dynamic.cpp:484`        |
| 89   | `Waiting for repl dev to get destroyed on out member replica={}`                                            | `tests/test_raft_repl_dev_dynamic.cpp:497`        |
| 90   | `Repl dev destroyed on out member replica={}`                                                               | `tests/test_raft_repl_dev_dynamic.cpp:499`        |
| 91   | `OneMemberRestart test done replica={}`                                                                     | `tests/test_raft_repl_dev_dynamic.cpp:508`        |
| 92   | `ValidateRequest test started replica={}`                                                                   | `tests/test_raft_repl_dev_dynamic.cpp:512`        |
| 93   | `setup consensus.laggy_threshold to {}`                                                                     | `tests/test_raft_repl_dev_dynamic.cpp:515`        |
| 94   | `Shutdown replica 1`                                                                                        | `tests/test_raft_repl_dev_dynamic.cpp:533`        |
| 95   | `Writing on leader num_io={} replica={}`                                                                    | `tests/test_raft_repl_dev_dynamic.cpp:540`        |
| 96   | `test SERVER_NOT_FOUND`                                                                                     | `tests/test_raft_repl_dev_dynamic.cpp:549`        |
| 97   | `test replace_member already complete`                                                                      | `tests/test_raft_repl_dev_dynamic.cpp:551`        |
| 98   | `test QUORUM_NOT_MET`                                                                                       | `tests/test_raft_repl_dev_dynamic.cpp:553`        |
| 99   | `Start replica 1`                                                                                           | `tests/test_raft_repl_dev_dynamic.cpp:559`        |
| 100  | `ValidateRequest test done replica={}`                                                                      | `tests/test_raft_repl_dev_dynamic.cpp:563`        |
| 101  | `Object Life Counter\n:{}`                                                                                  | `tests/test_raft_repl_dev_dynamic.cpp:624`        |
| 102  | `creating {} device files with each of size {}`                                                             | `tests/log_dev_benchmark.cpp:59`                  |
| 103  | `Creating iomgr with {} threads`                                                                            | `tests/log_dev_benchmark.cpp:67`                  |
| 104  | `Initialize and start HomeBlks with app_mem_size = {}`                                                      | `tests/log_dev_benchmark.cpp:71`                  |
| 105  | `HomeBlks Init completed`                                                                                   | `tests/log_dev_benchmark.cpp:83`                  |
| 106  | `Append completed with log_idx = {} offset = {}`                                                            | `tests/log_dev_benchmark.cpp:104`                 |
| 107  | `Found a log with log_idx = {} offset = {}`                                                                 | `tests/log_dev_benchmark.cpp:110`                 |
| 108  | `Filling cache with {} slabs and {} entries per slab`                                                       | `tests/test_blk_cache_queue.cpp:74`               |
| 109  | `Step 1: Allocating 1000 blocks from random slabs and expect all to succeed`                                | `tests/test_blk_cache_queue.cpp:143`              |
| 110  | `Step 2: Free all allocated blks and expect all free to succeed`                                            | `tests/test_blk_cache_queue.cpp:156`              |
| 111  | `Step 3: Now all slots are back full, try freeing one additional blocks and expect to fail`                 | `tests/test_blk_cache_queue.cpp:167`              |
| 112  | `Step 4: Realloc 1000 more random blks and it should succeed`                                               | `tests/test_blk_cache_queue.cpp:174`              |
| 113  | `Step 1: Allocate all blocks from slab={} and above for count={} and expect to break higher slab`           | `tests/test_blk_cache_queue.cpp:199`              |
| 114  | `Step 2: Since all higher slab are allocated, so contiguous only alloc of slab={} is expected to fail`      | `tests/test_blk_cache_queue.cpp:204`              |
| 115  | `Step 3: Allocate from lower than slab={} and expect all to succeed`                                        | `tests/test_blk_cache_queue.cpp:208`              |
| 116  | `Step 1: Allocate all blocks from slab={} and above for count={} and expect to break higher slab`           | `tests/test_blk_cache_queue.cpp:227`              |
| 117  | `Step 2: Since all higher slab are allocated, so contiguous only alloc of slab={} is expected to fail`      | `tests/test_blk_cache_queue.cpp:232`              |
| 118  | `Step 3: Allocate from lower than slab={} and expect all to succeed`                                        | `tests/test_blk_cache_queue.cpp:236`              |
| 119  | `Step 4: Try to allocate the partial block in previous slab and ensure it fails`                            | `tests/test_blk_cache_queue.cpp:240`              |
| 120  | `Step 5: Try allocate only one from previous slab and it should succeed`                                    | `tests/test_blk_cache_queue.cpp:244`              |
| 121  | `Step 6: Try to allocate one more blk for previous slab and it should fail`                                 | `tests/test_blk_cache_queue.cpp:247`              |
| 122  | `Step 1: Allocate all entries from slab=2, all-but-1 from slab=1, all-but-3 from slab=0`                    | `tests/test_blk_cache_queue.cpp:258`              |
| 123  | `Step 2: Allocate one from slab=2, and see if result is from both the slabs`                                | `tests/test_blk_cache_queue.cpp:263`              |
| 124  | `Step 3: Put one block back on slab 1 and then repeat the allocation, it should fail since there are only`  | `tests/test_blk_cache_queue.cpp:266`              |
| 125  | `Step 4: Subsequent alloc from slab 1 and slab 0 are successful, validates if previous partial alloc is`    | `tests/test_blk_cache_queue.cpp:274`              |
| 126  | `after_write_cb: Write completed;`                                                                          | `tests/test_data_service.cpp:111`                 |
| 127  | `Write blk ids: {}`                                                                                         | `tests/test_data_service.cpp:115`                 |
| 128  | `Step 2: async read on blkid: {}`                                                                           | `tests/test_data_service.cpp:124`                 |
| 129  | `read completed;`                                                                                           | `tests/test_data_service.cpp:129`                 |
| 130  | `completed async_free_blk: {}`                                                                              | `tests/test_data_service.cpp:135`                 |
| 131  | `after_write_cb: Write completed;`                                                                          | `tests/test_data_service.cpp:149`                 |
| 132  | `Write blk ids: {}`                                                                                         | `tests/test_data_service.cpp:152`                 |
| 133  | `Step 2a: inject read delay and read on blkid: {}`                                                          | `tests/test_data_service.cpp:161`                 |
| 134  | `read completed;`                                                                                           | `tests/test_data_service.cpp:171`                 |
| 135  | `Step 3: started async_free_blk: {}`                                                                        | `tests/test_data_service.cpp:179`                 |
| 136  | `completed async_free_blk`                                                                                  | `tests/test_data_service.cpp:182`                 |
| 137  | `after_write_cb: Write completed;`                                                                          | `tests/test_data_service.cpp:196`                 |
| 138  | `Step 2: started async_free_blk: {}`                                                                        | `tests/test_data_service.cpp:199`                 |
| 139  | `completed async_free_blks`                                                                                 | `tests/test_data_service.cpp:202`                 |
| 140  | `after_write_cb: Write completed;`                                                                          | `tests/test_data_service.cpp:218`                 |
| 141  | `Step 2: async read on blkid: {}`                                                                           | `tests/test_data_service.cpp:229`                 |
| 142  | `Read completed;`                                                                                           | `tests/test_data_service.cpp:238`                 |
| 143  | `Step 2: write data to these two chunks.`                                                                   | `tests/test_data_service.cpp:284`                 |
| 144  | `Step 3: restart with missing data drive(pdev).`                                                            | `tests/test_data_service.cpp:314`                 |
| 145  | `Step 4: read the blk from missing data drive`                                                              | `tests/test_data_service.cpp:334`                 |
| 146  | `Step 5: read the blk from living data drive`                                                               | `tests/test_data_service.cpp:351`                 |
| 147  | `Step 6: write the blk to living data drive`                                                                | `tests/test_data_service.cpp:361`                 |
| 148  | `Step 7: write the blk to missing data drive`                                                               | `tests/test_data_service.cpp:372`                 |
| 149  | `Step 8: free the blk from missing data drive`                                                              | `tests/test_data_service.cpp:386`                 |
| 150  | `Step 9: free the blk from living data drive`                                                               | `tests/test_data_service.cpp:395`                 |
| 151  | `completed async_free_blks, bid freed: {}`                                                                  | `tests/test_data_service.cpp:482`                 |
| 152  | `removing bid from map: {}`                                                                                 | `tests/test_data_service.cpp:490`                 |
| 153  | `m_total_io_comp_cnt: {}`                                                                                   | `tests/test_data_service.cpp:506`                 |
| 154  | `getting crc for blk: {}, is_multi: {}, integer:{}`                                                         | `tests/test_data_service.cpp:718`                 |
| 155  | `read completed, bid: {}`                                                                                   | `tests/test_data_service.cpp:740`                 |
| 156  | `Step 1: run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_data_service.cpp:840`                 |
| 157  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_data_service.cpp:843`                 |
| 158  | `Step 3: I/O completed, do shutdown.`                                                                       | `tests/test_data_service.cpp:846`                 |
| 159  | `Step 1: run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_data_service.cpp:852`                 |
| 160  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_data_service.cpp:857`                 |
| 161  | `Step 3: Check used size before and after reset blk allocator.`                                             | `tests/test_data_service.cpp:860`                 |
| 162  | `Step 3: I/O completed, do shutdown.`                                                                       | `tests/test_data_service.cpp:872`                 |
| 163  | `Step 1: run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_data_service.cpp:878`                 |
| 164  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_data_service.cpp:881`                 |
| 165  | `Step 3: I/O completed, do shutdown.`                                                                       | `tests/test_data_service.cpp:884`                 |
| 166  | `Step 1: run on worker thread to schedule write for {} Bytes, and {} iovs`                                  | `tests/test_data_service.cpp:891`                 |
| 167  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_data_service.cpp:895`                 |
| 168  | `Step 3: I/O completed, do shutdown.`                                                                       | `tests/test_data_service.cpp:898`                 |
| 169  | `Step 1: run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_data_service.cpp:904`                 |
| 170  | `Step 3: Wait for I/O to complete.`                                                                         | `tests/test_data_service.cpp:907`                 |
| 171  | `Step 4: I/O completed, do shutdown.`                                                                       | `tests/test_data_service.cpp:910`                 |
| 172  | `Step 1: run on worker thread to schedule write for {} Bytes, then free blk.`                               | `tests/test_data_service.cpp:917`                 |
| 173  | `Step 3: Wait for I/O to complete.`                                                                         | `tests/test_data_service.cpp:921`                 |
| 174  | `Step 4: I/O completed, do shutdown.`                                                                       | `tests/test_data_service.cpp:924`                 |
| 175  | `Step 1: Run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_data_service.cpp:933`                 |
| 176  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_data_service.cpp:937`                 |
| 177  | `Step 3: I/O completed, do shutdown.`                                                                       | `tests/test_data_service.cpp:940`                 |
| 178  | `Step 1: Run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_data_service.cpp:946`                 |
| 179  | `Step 4: Wait for I/O to complete.`                                                                         | `tests/test_data_service.cpp:950`                 |
| 180  | `Step 5: I/O completed, do shutdown.`                                                                       | `tests/test_data_service.cpp:953`                 |
| 181  | `Step 1: find two chunks in different pdevs.`                                                               | `tests/test_data_service.cpp:1019`                |
| 182  | `Step 10: wait for read and verify done.`                                                                   | `tests/test_data_service.cpp:1021`                |
| 183  | `Step 11: I/O completed, do shutdown.`                                                                      | `tests/test_data_service.cpp:1023`                |
| 184  | `Scenario 1: Testing chunk_id={}`                                                                           | `tests/test_data_service.cpp:1061`                |
| 185  | `Scenario 1: cp_flush got shared_ptr for chunk_id={}`                                                       | `tests/test_data_service.cpp:1077`                |
| 186  | `Scenario 1: cp_flush proceeding after reset_block_allocator completed`                                     | `tests/test_data_service.cpp:1085`                |
| 187  | `Scenario 1: About to call reset() after cp_flush got shared_ptr`                                           | `tests/test_data_service.cpp:1096`                |
| 188  | `Scenario 1: cp_flush completed successfully`                                                               | `tests/test_data_service.cpp:1103`                |
| 189  | `Scenario 1: reset_block_allocator completed`                                                               | `tests/test_data_service.cpp:1111`                |
| 190  | `Scenario 1: PASSED - Order: get_ptr -> reset_block_allocator -> cp_flush`                                  | `tests/test_data_service.cpp:1123`                |
| 191  | `Scenario 2: Testing chunk_id={}`                                                                           | `tests/test_data_service.cpp:1142`                |
| 192  | `Scenario 2: cp_flush got shared_ptr for chunk_id={}`                                                       | `tests/test_data_service.cpp:1159`                |
| 193  | `Scenario 2: cp_flush proceeding after old allocator reset completed`                                       | `tests/test_data_service.cpp:1167`                |
| 194  | `Scenario 2: About to call reset() on old allocator`                                                        | `tests/test_data_service.cpp:1178`                |
| 195  | `Scenario 2: Old allocator reset() completed`                                                               | `tests/test_data_service.cpp:1185`                |
| 196  | `Scenario 2: cp_flush completed, reset thread proceeding`                                                   | `tests/test_data_service.cpp:1193`                |
| 197  | `Scenario 2: cp_flush completed successfully on old allocator`                                              | `tests/test_data_service.cpp:1200`                |
| 198  | `Scenario 2: reset_block_allocator completed (in-place reset)`                                              | `tests/test_data_service.cpp:1208`                |
| 199  | `Scenario 2: PASSED - Order: get_ptr -> allocator_reset (in-place) -> cp_flush on same allocator`           | `tests/test_data_service.cpp:1222`                |
| 200  | `Scenario 3: Testing chunk_id={}`                                                                           | `tests/test_data_service.cpp:1241`                |
| 201  | `Scenario 3: Inside cp_flush with lock held`                                                                | `tests/test_data_service.cpp:1256`                |
| 202  | `Scenario 3: cp_flush continuing (reset_block_allocator should be blocked on lock)`                         | `tests/test_data_service.cpp:1262`                |
| 203  | `Scenario 3: cp_flush is inside, about to call reset() (should block on m_sb_mtx)`                          | `tests/test_data_service.cpp:1273`                |
| 204  | `Scenario 3: cp_flush completed successfully`                                                               | `tests/test_data_service.cpp:1279`                |
| 205  | `Scenario 3: reset_block_allocator completed (after waiting for cp_flush lock)`                             | `tests/test_data_service.cpp:1287`                |
| 206  | `Scenario 3: PASSED - mutex protected m_sb from concurrent reset_block_allocator during cp_flush`           | `tests/test_data_service.cpp:1299`                |
| 207  | `Scenario 4: Testing chunk_id={}`                                                                           | `tests/test_data_service.cpp:1322`                |
| 208  | `Scenario 4: Simulate cp_flush acquiring shared_ptr to old allocator before reset`                          | `tests/test_data_service.cpp:1325`                |
| 209  | `Scenario 4: Simulate GC calling reset_block_allocator on the chunk`                                        | `tests/test_data_service.cpp:1329`                |
| 210  | `Scenario 4: Simulate gc_repl_reqs calling free() on old allocator reference after reset`                   | `tests/test_data_service.cpp:1335`                |
| 211  | `Scenario 4: Simulate cp_flush running on old allocator reference after reset and free`                     | `tests/test_data_service.cpp:1342`                |
| 212  | `Scenario 4: PASSED - free() + cp_flush on old allocator after reset is safe`                               | `tests/test_data_service.cpp:1345`                |
| 213  | `cur_lsn is {} for store {} log_dev {}`                                                                     | `tests/test_log_store_long_run.cpp:108`           |
| 214  | `Totally recovered {} non-truncated lsns and {} truncated lsns for store {} log_dev {} truncated_upto {}`   | `tests/test_log_store_long_run.cpp:180`           |
| 215  | `Recovered lsn {}:{} with log data of size {}`                                                              | `tests/test_log_store_long_run.cpp:195`           |
| 216  | `Log store test needs minimum 4 log stores for testing, setting them to 4`                                  | `tests/test_log_store_long_run.cpp:280`           |
| 217  | `Iterations completed {}`                                                                                   | `tests/test_log_store_long_run.cpp:562`           |
| 218  | `Finished test. Num iterations {} Elapsed {}`                                                               | `tests/test_log_store_long_run.cpp:566`           |
| 219  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:114`                |
| 220  | `going to write data with id={}`                                                                            | `tests/test_raft_repl_dev.cpp:123`                |
| 221  | `I am not leader, leader_uuid={} my_uuid={}, do nothing`                                                    | `tests/test_raft_repl_dev.cpp:128`                |
| 222  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:134`                |
| 223  | `data with id={} has been deleted from db`                                                                  | `tests/test_raft_repl_dev.cpp:147`                |
| 224  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:160`                |
| 225  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:167`                |
| 226  | `Restart all the homestore replicas`                                                                        | `tests/test_raft_repl_dev.cpp:171`                |
| 227  | `Post restart write the data again on the leader`                                                           | `tests/test_raft_repl_dev.cpp:178`                |
| 228  | `Validate all data written (including pre-restart data) by reading them`                                    | `tests/test_raft_repl_dev.cpp:181`                |
| 229  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:188`                |
| 230  | `Set flip to fake fetch data request on data channel`                                                       | `tests/test_raft_repl_dev.cpp:192`                |
| 231  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:199`                |
| 232  | `Homestore replica={} setup completed, all the push_data from leader are disabled`                          | `tests/test_raft_repl_dev.cpp:208`                |
| 233  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:210`                |
| 234  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:217`                |
| 235  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:226`                |
| 236  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:233`                |
| 237  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:244`                |
| 238  | `Set the max fetch to be fairly small to force multiple batches`                                            | `tests/test_raft_repl_dev.cpp:253`                |
| 239  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:268`                |
| 240  | `Set the max fetch back to previous value={}`                                                               | `tests/test_raft_repl_dev.cpp:272`                |
| 241  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:283`                |
| 242  | `Set flip to fake reject append entries in both data and raft channels. We slow down data channel`          | `tests/test_raft_repl_dev.cpp:287`                |
| 243  | `Write to leader and then wait for all the commits on all replica despite drop/slow_down`                   | `tests/test_raft_repl_dev.cpp:294`                |
| 244  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:298`                |
| 245  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:310`                |
| 246  | `Step 1: Wait for target follower={} to commit bootstrap config through lsn={}`                             | `tests/test_raft_repl_dev.cpp:321`                |
| 247  | `Target follower={} committed bootstrap config`                                                             | `tests/test_raft_repl_dev.cpp:326`                |
| 248  | `Step 2: Install append tracking and rejection flips on target follower={}`                                 | `tests/test_raft_repl_dev.cpp:333`                |
| 249  | `Step 3: Build the A,A,A,C,A,C log sequence on the leader`                                                  | `tests/test_raft_repl_dev.cpp:342`                |
| 250  | `Step 4: Release the first rejected batch and allow the follower retries`                                   | `tests/test_raft_repl_dev.cpp:361`                |
| 251  | `Step 5: Wait for app-log commits and verify the follower retry batches`                                    | `tests/test_raft_repl_dev.cpp:367`                |
| 252  | `Target follower={} received all expected retry batches`                                                    | `tests/test_raft_repl_dev.cpp:375`                |
| 253  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:397`                |
| 254  | `After one follower is shutdown, insert more entries`                                                       | `tests/test_raft_repl_dev.cpp:408`                |
| 255  | `Switch to a new leader and insert more entries`                                                            | `tests/test_raft_repl_dev.cpp:412`                |
| 256  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:417`                |
| 257  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:423`                |
| 258  | `Restart leader`                                                                                            | `tests/test_raft_repl_dev.cpp:432`                |
| 259  | `After original leader is shutdown, insert more entries into the new leader`                                | `tests/test_raft_repl_dev.cpp:437`                |
| 260  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:441`                |
| 261  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:448`                |
| 262  | `Aim of this test is to drop raft entry and ensure that they are retried. In addition, we drop raft entry`  | `tests/test_raft_repl_dev.cpp:451`                |
| 263  | `Set flip to fake drop append entries in raft channel of replica=2`                                         | `tests/test_raft_repl_dev.cpp:455`                |
| 264  | `Even after drop on replica=2, lets validate that data written is synced on all members (after retry to 2)` | `tests/test_raft_repl_dev.cpp:461`                |
| 265  | `Set flip to fake drop append entries in raft channel of replica=2 again`                                   | `tests/test_raft_repl_dev.cpp:466`                |
| 266  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:487`                |
| 267  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:494`                |
| 268  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:500`                |
| 269  | `Create 2 more ReplDevs`                                                                                    | `tests/test_raft_repl_dev.cpp:503`                |
| 270  | `Inserting {} entries on the leader and concurrently remove that repl_dev while IO is ongoing`              | `tests/test_raft_repl_dev.cpp:513`                |
| 271  | `After remove db replica={} num_db={}`                                                                      | `tests/test_raft_repl_dev.cpp:519`                |
| 272  | `Shutdown one of the followers (replica=1) and then remove dbs on other members. Expect replica=1 to`       | `tests/test_raft_repl_dev.cpp:523`                |
| 273  | `After restart replica={} num_db={}`                                                                        | `tests/test_raft_repl_dev.cpp:526`                |
| 274  | `Set zombie on group={}`                                                                                    | `tests/test_raft_repl_dev.cpp:533`                |
| 275  | `Remove last replica={} num_db={}`                                                                          | `tests/test_raft_repl_dev.cpp:537`                |
| 276  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:557`                |
| 277  | `Set the repl_req_timout_sec to be fairly small to force GC to kick in`                                     | `tests/test_raft_repl_dev.cpp:561`                |
| 278  | `Set flip to fake fetch data request on data channel`                                                       | `tests/test_raft_repl_dev.cpp:569`                |
| 279  | `After original leader is shutdown, insert more entries into the new leader`                                | `tests/test_raft_repl_dev.cpp:580`                |
| 280  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:585`                |
| 281  | `Set the repl_req_timeout back to previous value={}`                                                        | `tests/test_raft_repl_dev.cpp:589`                |
| 282  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:608`                |
| 283  | `Write on leader num_entries={}`                                                                            | `tests/test_raft_repl_dev.cpp:614`                |
| 284  | `Shutdown replica 1`                                                                                        | `tests/test_raft_repl_dev.cpp:618`                |
| 285  | `Write on leader num_entries={}`                                                                            | `tests/test_raft_repl_dev.cpp:623`                |
| 286  | `Got all commits for replica 0 and 2`                                                                       | `tests/test_raft_repl_dev.cpp:629`                |
| 287  | `Leader create snapshot and truncate`                                                                       | `tests/test_raft_repl_dev.cpp:633`                |
| 288  | `Start replica 1`                                                                                           | `tests/test_raft_repl_dev.cpp:643`                |
| 289  | `Write on leader num_entries={}`                                                                            | `tests/test_raft_repl_dev.cpp:649`                |
| 290  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:653`                |
| 291  | `BaselineTest done`                                                                                         | `tests/test_raft_repl_dev.cpp:656`                |
| 292  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:660`                |
| 293  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:670`                |
| 294  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:676`                |
| 295  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:686`                |
| 296  | `Restart leader`                                                                                            | `tests/test_raft_repl_dev.cpp:690`                |
| 297  | `Validate leader switched`                                                                                  | `tests/test_raft_repl_dev.cpp:694`                |
| 298  | `Resign and trigger a priority leader election`                                                             | `tests/test_raft_repl_dev.cpp:701`                |
| 299  | `Validate leader switched back to initial replica`                                                          | `tests/test_raft_repl_dev.cpp:709`                |
| 300  | `Post restart write the data again on the leader`                                                           | `tests/test_raft_repl_dev.cpp:713`                |
| 301  | `Validate all data written (including pre-restart data) by reading them`                                    | `tests/test_raft_repl_dev.cpp:716`                |
| 302  | `Follower priority={} decayed_priority={}`                                                                  | `tests/test_raft_repl_dev.cpp:740`                |
| 303  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:750`                |
| 304  | `Write on leader num_entries={}`                                                                            | `tests/test_raft_repl_dev.cpp:763`                |
| 305  | `After 100 entries written, truncation upper limit became {}`                                               | `tests/test_raft_repl_dev.cpp:776`                |
| 306  | `Shutdown replica 1`                                                                                        | `tests/test_raft_repl_dev.cpp:779`                |
| 307  | `Write on leader num_entries={}`                                                                            | `tests/test_raft_repl_dev.cpp:783`                |
| 308  | `Got all commits for replica 0 and 2`                                                                       | `tests/test_raft_repl_dev.cpp:788`                |
| 309  | `Trigger cp after writing 100 entries for replica 0 and 2`                                                  | `tests/test_raft_repl_dev.cpp:790`                |
| 310  | `After another 100 entries written, truncation upper limit {}`                                              | `tests/test_raft_repl_dev.cpp:800`                |
| 311  | `Start replica 1`                                                                                           | `tests/test_raft_repl_dev.cpp:806`                |
| 312  | `Write on leader num_entries={}`                                                                            | `tests/test_raft_repl_dev.cpp:811`                |
| 313  | `After another 50 entries written, truncation upper limit became {}`                                        | `tests/test_raft_repl_dev.cpp:825`                |
| 314  | `Shutdown replica 1 again`                                                                                  | `tests/test_raft_repl_dev.cpp:831`                |
| 315  | `Write on leader num_entries={}`                                                                            | `tests/test_raft_repl_dev.cpp:836`                |
| 316  | `Got all commits for replica 0 and 2`                                                                       | `tests/test_raft_repl_dev.cpp:841`                |
| 317  | `Trigger cp after writing 300 entries for replica 0 and 2`                                                  | `tests/test_raft_repl_dev.cpp:843`                |
| 318  | `After another 300 entries written, truncation upper limit {}`                                              | `tests/test_raft_repl_dev.cpp:854`                |
| 319  | `Start replica 1 again`                                                                                     | `tests/test_raft_repl_dev.cpp:859`                |
| 320  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:866`                |
| 321  | `Set the raft_logstore_reserve_threshold back to previous value={}`                                         | `tests/test_raft_repl_dev.cpp:870`                |
| 322  | `RaftLogTruncationTest done`                                                                                | `tests/test_raft_repl_dev.cpp:877`                |
| 323  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:882`                |
| 324  | `Setup SSL for the repl_dev`                                                                                | `tests/test_raft_repl_dev.cpp:886`                |
| 325  | `Restart all the replicas with SSL enabled`                                                                 | `tests/test_raft_repl_dev.cpp:907`                |
| 326  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:915`                |
| 327  | `Set the ssl_ca_file back to previous value={}`                                                             | `tests/test_raft_repl_dev.cpp:920`                |
| 328  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:928`                |
| 329  | `Initial leader is replica={}`                                                                              | `tests/test_raft_repl_dev.cpp:934`                |
| 330  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:939`                |
| 331  | `Yield leader`                                                                                              | `tests/test_raft_repl_dev.cpp:943`                |
| 332  | `Validate leader switched`                                                                                  | `tests/test_raft_repl_dev.cpp:948`                |
| 333  | `Trigger reconcile leader on follower, expected no change`                                                  | `tests/test_raft_repl_dev.cpp:954`                |
| 334  | `Validate leader unchanged`                                                                                 | `tests/test_raft_repl_dev.cpp:957`                |
| 335  | `Request leadership on replica=0`                                                                           | `tests/test_raft_repl_dev.cpp:964`                |
| 336  | `Validate leader switched back to initial replica`                                                          | `tests/test_raft_repl_dev.cpp:970`                |
| 337  | `Yield leader again`                                                                                        | `tests/test_raft_repl_dev.cpp:974`                |
| 338  | `Validate leader switched`                                                                                  | `tests/test_raft_repl_dev.cpp:977`                |
| 339  | `Yield leadership on replica={}`                                                                            | `tests/test_raft_repl_dev.cpp:984`                |
| 340  | `Validate leader switched back to initial replica, leader={}`                                               | `tests/test_raft_repl_dev.cpp:990`                |
| 341  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:996`                |
| 342  | `Step 0: Got RaftReplDev instance for group_id={}`                                                          | `tests/test_raft_repl_dev.cpp:1001`               |
| 343  | `Step 1: Setting legacy JSON object format state`                                                           | `tests/test_raft_repl_dev.cpp:1004`               |
| 344  | `Step 1: Written legacy state - term=100, voted_for=5, election_timer_allowed=true, catching_up=false`      | `tests/test_raft_repl_dev.cpp:1012`               |
| 345  | `Step 2: Reading state from legacy JSON object format`                                                      | `tests/test_raft_repl_dev.cpp:1016`               |
| 346  | `Step 2: Successfully read legacy state - term={}, voted_for={}, election_timer_allowed={},`                | `tests/test_raft_repl_dev.cpp:1025`               |
| 347  | `Step 3: Updating state values and saving in binary format`                                                 | `tests/test_raft_repl_dev.cpp:1032`               |
| 348  | `Step 3: Saving new state - term=150, voted_for=10, election_timer_allowed=false,`                          | `tests/test_raft_repl_dev.cpp:1038`               |
| 349  | `Step 4: Verifying JSON superblock fields`                                                                  | `tests/test_raft_repl_dev.cpp:1043`               |
| 350  | `Step 4: Confirmed 'nuraft_state' field exists and is array (size={} bytes)`                                | `tests/test_raft_repl_dev.cpp:1048`               |
| 351  | `Step 4: Confirmed legacy 'state' field is empty/null for rollback compatibility`                           | `tests/test_raft_repl_dev.cpp:1052`               |
| 352  | `Step 5: Reading back state from binary format (nuraft_state field)`                                        | `tests/test_raft_repl_dev.cpp:1055`               |
| 353  | `Step 5: Successfully read new state - term={}, voted_for={}, election_timer_allowed={},`                   | `tests/test_raft_repl_dev.cpp:1063`               |
| 354  | `Homestore replica={} setup completed`                                                                      | `tests/test_raft_repl_dev.cpp:1071`               |
| 355  | `group={} current_truncation_upper_limit={}`                                                                | `tests/test_raft_repl_dev.cpp:1083`               |
| 356  | `Trigger scheduled snapshot creation on follower1`                                                          | `tests/test_raft_repl_dev.cpp:1086`               |
| 357  | `After scheduled snapshot creation, group={} current_truncation_upper_limit={}`                             | `tests/test_raft_repl_dev.cpp:1090`               |
| 358  | `Re-schedule snapshot creation on follower1 with lower compact lsn`                                         | `tests/test_raft_repl_dev.cpp:1093`               |
| 359  | `After re-scheduled snapshot creation, group={} current_truncation_upper_limit={}`                          | `tests/test_raft_repl_dev.cpp:1097`               |
| 360  | `Re-schedule snapshot creation on follower1 with higher compact lsn`                                        | `tests/test_raft_repl_dev.cpp:1100`               |
| 361  | `After re-scheduled snapshot creation, group={} current_truncation_upper_limit={}, current_commit_idx={}`   | `tests/test_raft_repl_dev.cpp:1105`               |
| 362  | `Validate all data written so far by reading them`                                                          | `tests/test_raft_repl_dev.cpp:1111`               |
| 363  | `Object Life Counter\n:{}`                                                                                  | `tests/test_raft_repl_dev.cpp:1173`               |
| 364  | `got store {} trunc_upto {} {} {}`                                                                          | `tests/test_log_store.cpp:261`                    |
| 365  | `Unexpected out_of_range exception for lsn={}:{} upto {} trunc_upto {}`                                     | `tests/test_log_store.cpp:268`                    |
| 366  | `Totally recovered {} non-truncated lsns and {} truncated lsns for store {} log_dev {}`                     | `tests/test_log_store.cpp:303`                    |
| 367  | `Log store test needs minimum 4 log stores for testing, setting them to 4`                                  | `tests/test_log_store.cpp:439`                    |
| 368  | `No forward progress for device truncation yet.`                                                            | `tests/test_log_store.cpp:747`                    |
| 369  | `Do not expect forward progress for device truncation`                                                      | `tests/test_log_store.cpp:756`                    |
| 370  | `{}`                                                                                                        | `tests/test_log_store.cpp:864`                    |
| 371  | `Iteration {}`                                                                                              | `tests/test_log_store.cpp:899`                    |
| 372  | `Step 1: Prepare num records and create reqd log stores`                                                    | `tests/test_log_store.cpp:900`                    |
| 373  | `Step 2: Inserting randomly within a batch of 10 in parallel fashion as a burst`                            | `tests/test_log_store.cpp:903`                    |
| 374  | `Step 3: Wait for the Inserts to complete`                                                                  | `tests/test_log_store.cpp:906`                    |
| 375  | `Step 4: Read all the inserts one by one for each log store to validate if what is written is valid`        | `tests/test_log_store.cpp:909`                    |
| 376  | `Step 4.1: Iterate all inserts one by one for each log store and validate if what is written is valid`      | `tests/test_log_store.cpp:912`                    |
| 377  | `Step 4.2: Read all inserts and dump all logstore records into json`                                        | `tests/test_log_store.cpp:917`                    |
| 378  | `Step 4.2: Read some specific interval/filter of seq number in one logstore and dump it into json`          | `tests/test_log_store.cpp:920`                    |
| 379  | `Step 5: Truncate all of the inserts one log store at a time and validate log dev truncation is marked`     | `tests/test_log_store.cpp:924`                    |
| 380  | `Step 6: Restart homestore`                                                                                 | `tests/test_log_store.cpp:928`                    |
| 381  | `Step 7: Issue more sequential inserts after restarts with q depth of 15`                                   | `tests/test_log_store.cpp:933`                    |
| 382  | `Step 8: Wait for the previous Inserts to complete`                                                         | `tests/test_log_store.cpp:936`                    |
| 383  | `Step 9: Read all the inserts one by one for each log store to validate if what is written is valid`        | `tests/test_log_store.cpp:939`                    |
| 384  | `Iteration {}`                                                                                              | `tests/test_log_store.cpp:949`                    |
| 385  | `Step 1: Reinit the num records to start sequential write test`                                             | `tests/test_log_store.cpp:950`                    |
| 386  | `Step 2: Issue sequential inserts as a burst`                                                               | `tests/test_log_store.cpp:953`                    |
| 387  | `Step 3: In parallel to writes issue truncation upto completion`                                            | `tests/test_log_store.cpp:957`                    |
| 388  | `Still pending completions = {}, pending issued = {}`                                                       | `tests/test_log_store.cpp:968`                    |
| 389  | `Truncation has been issued and validated for {} times before all records are completely truncated`         | `tests/test_log_store.cpp:971`                    |
| 390  | `Step 4: Wait for the Inserts to complete`                                                                  | `tests/test_log_store.cpp:974`                    |
| 391  | `Step 5: Do a final truncation and validate`                                                                | `tests/test_log_store.cpp:977`                    |
| 392  | `Iteration {}`                                                                                              | `tests/test_log_store.cpp:987`                    |
| 393  | `Step 1: Reinit the num records to start sequential write test`                                             | `tests/test_log_store.cpp:988`                    |
| 394  | `Step 2: Issue randomy within a batch of 10 with 1 hole per batch`                                          | `tests/test_log_store.cpp:991`                    |
| 395  | `Step 3: Wait for the Inserts to complete`                                                                  | `tests/test_log_store.cpp:994`                    |
| 396  | `Step 4: Read all the inserts one by one for each log store to validate if what is written is valid`        | `tests/test_log_store.cpp:997`                    |
| 397  | `Step 4.1: Iterate all inserts one by one for each log store and validate if what is written is valid`      | `tests/test_log_store.cpp:1000`                   |
| 398  | `Step 5: Fill the hole and do validation if they are indeed filled`                                         | `tests/test_log_store.cpp:1003`                   |
| 399  | `Step 6: Do a final truncation and validate`                                                                | `tests/test_log_store.cpp:1006`                   |
| 400  | `Iteration {}`                                                                                              | `tests/test_log_store.cpp:1016`                   |
| 401  | `Step 1: Reinit the num records={} and insert them as batch of 10 with qdepth=500 and wait for all`         | `tests/test_log_store.cpp:1017`                   |
| 402  | `Step 2: Stop the workload on stores 0,1 and write num records={} on other stores, wait for their`          | `tests/test_log_store.cpp:1028`                   |
| 403  | `Step 2.{}.1: Write and wait for {}`                                                                        | `tests/test_log_store.cpp:1032`                   |
| 404  | `Step 2.{}.2: Do a truncation on all log stores and validate`                                               | `tests/test_log_store.cpp:1039`                   |
| 405  | `Step 3: Change data rate on stores 0,1 but still slower than other stores, write num_records={}`           | `tests/test_log_store.cpp:1043`                   |
| 406  | `Step 3.{}.1: Write and wait for {}`                                                                        | `tests/test_log_store.cpp:1048`                   |
| 407  | `Step 3.{}.2: Do a truncation on all log stores and validate`                                               | `tests/test_log_store.cpp:1054`                   |
| 408  | `Step 4: Write the data with similar variable data rate, but truncate run in parallel to writes`            | `tests/test_log_store.cpp:1058`                   |
| 409  | `Step 4.{}: Truncating ith time with 200us delay between each truncation`                                   | `tests/test_log_store.cpp:1064`                   |
| 410  | `Iteration {}`                                                                                              | `tests/test_log_store.cpp:1081`                   |
| 411  | `Step 1: Reinit the num records to start sequential write test`                                             | `tests/test_log_store.cpp:1082`                   |
| 412  | `Step 2: Issue sequential inserts with q depth of 30`                                                       | `tests/test_log_store.cpp:1085`                   |
| 413  | `Step 3: Wait for the Inserts to complete`                                                                  | `tests/test_log_store.cpp:1088`                   |
| 414  | `Step 4: Read all the inserts one by one for each log store to validate if what is written is valid`        | `tests/test_log_store.cpp:1091`                   |
| 415  | `Step 4.1: Iterate all inserts one by one for each log store and validate if what is written is valid`      | `tests/test_log_store.cpp:1094`                   |
| 416  | `Step 5: Restart homestore`                                                                                 | `tests/test_log_store.cpp:1097`                   |
| 417  | `Step 5a: Read all the inserts one by one for each log store to validate if what is written is valid`       | `tests/test_log_store.cpp:1102`                   |
| 418  | `Step 6: Restart homestore again to validate recovery on consecutive restarts`                              | `tests/test_log_store.cpp:1105`                   |
| 419  | `Step 7: Issue more sequential inserts after restarts with q depth of 15`                                   | `tests/test_log_store.cpp:1110`                   |
| 420  | `Step 8: Wait for the previous Inserts to complete`                                                         | `tests/test_log_store.cpp:1113`                   |
| 421  | `Step 9: Read all the inserts one by one for each log store to validate if what is written is valid`        | `tests/test_log_store.cpp:1116`                   |
| 422  | `Step 9.1: Iterate all inserts one by one for each log store and validate if what is written is valid`      | `tests/test_log_store.cpp:1119`                   |
| 423  | `Step 10: Restart homestore again to validate recovery after inserts`                                       | `tests/test_log_store.cpp:1122`                   |
| 424  | `Step 11: Truncate`                                                                                         | `tests/test_log_store.cpp:1127`                   |
| 425  | `Step 1: Delay the flush threshold and flush timer to very high value to ensure flush works fine`           | `tests/test_log_store.cpp:1133`                   |
| 426  | `Step 2: Reinit the 10 records to start sequential write test`                                              | `tests/test_log_store.cpp:1140`                   |
| 427  | `Step 3: Issue sequential inserts with q depth of 10`                                                       | `tests/test_log_store.cpp:1143`                   |
| 428  | `Step 4: Do a sync flush`                                                                                   | `tests/test_log_store.cpp:1146`                   |
| 429  | `Step 5: Reset the settings back`                                                                           | `tests/test_log_store.cpp:1149`                   |
| 430  | `Step 6: Wait for the Inserts to complete`                                                                  | `tests/test_log_store.cpp:1156`                   |
| 431  | `Step 7: Simulate flush_sync running parallel to regular flush. This is achieved by doing flip to delay`    | `tests/test_log_store.cpp:1160`                   |
| 432  | `Step 8: Reissue sequential inserts with q depth of 10`                                                     | `tests/test_log_store.cpp:1173`                   |
| 433  | `Step 9: Do a parallel sync flush`                                                                          | `tests/test_log_store.cpp:1177`                   |
| 434  | `Step 10: Wait for the Inserts to complete`                                                                 | `tests/test_log_store.cpp:1180`                   |
| 435  | `Step 1: Reinit the {} to start sequential write test`                                                      | `tests/test_log_store.cpp:1191`                   |
| 436  | `Step 2: Issue sequential inserts with q depth of 40`                                                       | `tests/test_log_store.cpp:1194`                   |
| 437  | `Step 3: Wait for the Inserts to complete`                                                                  | `tests/test_log_store.cpp:1197`                   |
| 438  | `Step 4: Read all the inserts one by one for each log store to validate if what is written is valid`        | `tests/test_log_store.cpp:1200`                   |
| 439  | `Step 4.1: Iterate all inserts one by one for each log store and validate if what is written is valid`      | `tests/test_log_store.cpp:1203`                   |
| 440  | `Step 5: Remove log store 0`                                                                                | `tests/test_log_store.cpp:1206`                   |
| 441  | `Step 6: Truncate all of the remaining log stores and validate log dev truncation is marked`                | `tests/test_log_store.cpp:1209`                   |
| 442  | `Step 7: Do IO on remaining log stores for records={}`                                                      | `tests/test_log_store.cpp:1213`                   |
| 443  | `Step 8: Remove log store 1`                                                                                | `tests/test_log_store.cpp:1218`                   |
| 444  | `Step 9: Truncate again, this time expected to have first log store delete is actually garbage collected`   | `tests/test_log_store.cpp:1221`                   |
| 445  | `Iteration {}`                                                                                              | `tests/test_log_store.cpp:1229`                   |
| 446  | `Created new log store -> id {}`                                                                            | `tests/test_log_store.cpp:1233`                   |
| 447  | `Written sync data for LSN -> {}`                                                                           | `tests/test_log_store.cpp:1240`                   |
| 448  | `Remove logstore -> i {}`                                                                                   | `tests/test_log_store.cpp:1259`                   |
| 449  | `{}`                                                                                                        | `tests/test_journal_vdev.cpp:372`                 |
| 450  | `Add log entries`                                                                                           | `tests/test_journal_vdev.cpp:426`                 |
| 451  | `Validate log entries`                                                                                      | `tests/test_journal_vdev.cpp:435`                 |
| 452  | `Restart homestore`                                                                                         | `tests/test_journal_vdev.cpp:446`                 |
| 453  | `Add log entries`                                                                                           | `tests/test_journal_vdev.cpp:466`                 |
| 454  | `Validate log entries`                                                                                      | `tests/test_journal_vdev.cpp:480`                 |
| 455  | `Inserting two entries`                                                                                     | `tests/test_journal_vdev.cpp:512`                 |
| 456  | `Inserting three entries`                                                                                   | `tests/test_journal_vdev.cpp:523`                 |
| 457  | `Restart homestore`                                                                                         | `tests/test_journal_vdev.cpp:536`                 |
| 458  | `Inserting one entry`                                                                                       | `tests/test_journal_vdev.cpp:545`                 |
| 459  | `Inserting one entry`                                                                                       | `tests/test_journal_vdev.cpp:552`                 |
| 460  | `Truncating two entries`                                                                                    | `tests/test_journal_vdev.cpp:559`                 |
| 461  | `Truncating one entry`                                                                                      | `tests/test_journal_vdev.cpp:567`                 |
| 462  | `Restart homestore`                                                                                         | `tests/test_journal_vdev.cpp:574`                 |
| 463  | `Truncating one entry`                                                                                      | `tests/test_journal_vdev.cpp:581`                 |
| 464  | `Truncating all entries`                                                                                    | `tests/test_journal_vdev.cpp:589`                 |
| 465  | `Testing with run_time: {}, num_io: {}, read/write percentage: {}/{}, truncate_watermark_percentage: {}`    | `tests/test_journal_vdev.cpp:646`                 |
| 466  | `Testing with fixed write size: {}`                                                                         | `tests/test_journal_vdev.cpp:650`                 |
| 467  | `Testing with min write size: {}, max write size: {}`                                                       | `tests/test_journal_vdev.cpp:652`                 |
| 468  | `Object Life Counter\n:{}`                                                                                  | `tests/test_index_btree.cpp:62`                   |
| 469  | `Index table recovered`                                                                                     | `tests/test_index_btree.cpp:75`                   |
| 470  | `Root bnode_id {} version {}`                                                                               | `tests/test_index_btree.cpp:76`                   |
| 471  | `Node size {}`                                                                                              | `tests/test_index_btree.cpp:93`                   |
| 472  | `Added index table to index service`                                                                        | `tests/test_index_btree.cpp:110`                  |
| 473  | `Teardown with Root bnode_id {} tree size: {} btree node count (interior = {} leaf= {})`                    | `tests/test_index_btree.cpp:116`                  |
| 474  | `SequentialInsert test start`                                                                               | `tests/test_index_btree.cpp:145`                  |
| 475  | `Step 1: Do Forward sequential insert for {} entries`                                                       | `tests/test_index_btree.cpp:149`                  |
| 476  | `Step 2: Query {} entries and validate with pagination of 75 entries`                                       | `tests/test_index_btree.cpp:154`                  |
| 477  | `Step 3: Do Reverse sequential insert of remaining {} entries`                                              | `tests/test_index_btree.cpp:159`                  |
| 478  | `Step 4: Query {} entries and validate with pagination of 90 entries`                                       | `tests/test_index_btree.cpp:163`                  |
| 479  | `Step 5: Query all entries and validate with no pagination`                                                 | `tests/test_index_btree.cpp:167`                  |
| 480  | `Step 6: Query all entries and validate with pagination of 80 entries`                                      | `tests/test_index_btree.cpp:170`                  |
| 481  | `Step 7: Get all entries 1-by-1 and validate them`                                                          | `tests/test_index_btree.cpp:173`                  |
| 482  | `Step 8: Do incorrect input and validate errors`                                                            | `tests/test_index_btree.cpp:178`                  |
| 483  | `SequentialInsert test end`                                                                                 | `tests/test_index_btree.cpp:183`                  |
| 484  | `Step 1: Do forward random insert for {} entries`                                                           | `tests/test_index_btree.cpp:196`                  |
| 485  | `TriggerCacheEviction test start`                                                                           | `tests/test_index_btree.cpp:212`                  |
| 486  | `Step 1: Do insert for {} entries`                                                                          | `tests/test_index_btree.cpp:214`                  |
| 487  | `TriggerCacheEviction test end`                                                                             | `tests/test_index_btree.cpp:228`                  |
| 488  | `SequentialRemove test start`                                                                               | `tests/test_index_btree.cpp:232`                  |
| 489  | `Step 1: Do Forward sequential insert for {} entries`                                                       | `tests/test_index_btree.cpp:235`                  |
| 490  | `Step 2: Query {} entries and validate with pagination of 75 entries`                                       | `tests/test_index_btree.cpp:239`                  |
| 491  | `Step 3: Do Forward sequential remove for {} entries`                                                       | `tests/test_index_btree.cpp:243`                  |
| 492  | `Step 4: Query {} entries and validate with pagination of 75 entries`                                       | `tests/test_index_btree.cpp:247`                  |
| 493  | `Step 5: Do Reverse sequential remove of remaining {} entries`                                              | `tests/test_index_btree.cpp:252`                  |
| 494  | `Step 6: Query the empty tree`                                                                              | `tests/test_index_btree.cpp:257`                  |
| 495  | `SequentialRemove test end`                                                                                 | `tests/test_index_btree.cpp:261`                  |
| 496  | `Step 1: Do forward sequential insert for {} entries`                                                       | `tests/test_index_btree.cpp:268`                  |
| 497  | `Step 2: Do remove one by one for {} entries`                                                               | `tests/test_index_btree.cpp:280`                  |
| 498  | `RangeUpdate test start`                                                                                    | `tests/test_index_btree.cpp:288`                  |
| 499  | `Step 1: Do Forward sequential insert for {} entries`                                                       | `tests/test_index_btree.cpp:291`                  |
| 500  | `Step 2: Do Range Update of random intervals between [1-50] for 100 times with random key ranges`           | `tests/test_index_btree.cpp:296`                  |
| 501  | `Step 2: Query {} entries and validate with pagination of 75 entries`                                       | `tests/test_index_btree.cpp:301`                  |
| 502  | `RangeUpdate test end`                                                                                      | `tests/test_index_btree.cpp:303`                  |
| 503  | `CpFlush test start`                                                                                        | `tests/test_index_btree.cpp:307`                  |
| 504  | `Do Forward sequential insert for {} entries`                                                               | `tests/test_index_btree.cpp:310`                  |
| 505  | `Query {} entries and validate with pagination of 75 entries`                                               | `tests/test_index_btree.cpp:320`                  |
| 506  | `Trigger checkpoint flush.`                                                                                 | `tests/test_index_btree.cpp:323`                  |
| 507  | `Query {} entries and validate with pagination of 75 entries`                                               | `tests/test_index_btree.cpp:326`                  |
| 508  | `Restarted homestore with index recovered`                                                                  | `tests/test_index_btree.cpp:337`                  |
| 509  | `Query {} entries`                                                                                          | `tests/test_index_btree.cpp:341`                  |
| 510  | `CpFlush test end`                                                                                          | `tests/test_index_btree.cpp:345`                  |
| 511  | `MultipleCpFlush test start`                                                                                | `tests/test_index_btree.cpp:349`                  |
| 512  | `Do Forward sequential insert for {} entries`                                                               | `tests/test_index_btree.cpp:352`                  |
| 513  | `Trigger checkpoint flush wait=false.`                                                                      | `tests/test_index_btree.cpp:356`                  |
| 514  | `Trigger checkpoint flush wait=false.`                                                                      | `tests/test_index_btree.cpp:361`                  |
| 515  | `Trigger checkpoint flush wait=false.`                                                                      | `tests/test_index_btree.cpp:368`                  |
| 516  | `Trigger checkpoint flush wait=true.`                                                                       | `tests/test_index_btree.cpp:371`                  |
| 517  | `Query {} entries and validate with pagination of 75 entries`                                               | `tests/test_index_btree.cpp:374`                  |
| 518  | `Restarted homestore with index recovered`                                                                  | `tests/test_index_btree.cpp:385`                  |
| 519  | `Query {} entries and validate with pagination of 1000 entries`                                             | `tests/test_index_btree.cpp:390`                  |
| 520  | `MultipleCpFlush test end`                                                                                  | `tests/test_index_btree.cpp:392`                  |
| 521  | `ThreadedCpFlush test start`                                                                                | `tests/test_index_btree.cpp:396`                  |
| 522  | `Do Forward sequential insert for {} entries`                                                               | `tests/test_index_btree.cpp:402`                  |
| 523  | `Do random removes for {} entries`                                                                          | `tests/test_index_btree.cpp:411`                  |
| 524  | `Trigger checkpoint flush wait=true.`                                                                       | `tests/test_index_btree.cpp:424`                  |
| 525  | `Trigger checkpoint flush wait=true done.`                                                                  | `tests/test_index_btree.cpp:426`                  |
| 526  | `Trigger checkpoint flush wait=true.`                                                                       | `tests/test_index_btree.cpp:435`                  |
| 527  | `Query {} entries and validate with pagination of 75 entries`                                               | `tests/test_index_btree.cpp:438`                  |
| 528  | `Restarted homestore with index recovered`                                                                  | `tests/test_index_btree.cpp:448`                  |
| 529  | `Query {} entries and validate with pagination of 1000 entries`                                             | `tests/test_index_btree.cpp:453`                  |
| 530  | `ThreadedCpFlush test end`                                                                                  | `tests/test_index_btree.cpp:455`                  |
| 531  | `=== Root Collapse CP Flush Test ===`                                                                       | `tests/test_index_btree.cpp:481`                  |
| 532  | `Using m_max_keys_in_node={}, m_min_keys_in_node={}`                                                        | `tests/test_index_btree.cpp:487`                  |
| 533  | `CP2 Phase - Step 1: Insert {} entries to create 3-child tree`                                              | `tests/test_index_btree.cpp:499`                  |
| 534  | `After insert: depth={}, interior={}, leaf={}`                                                              | `tests/test_index_btree.cpp:506`                  |
| 535  | `CP2 Phase - Step 2: Deleting {} entries to trigger merge to 1 child`                                       | `tests/test_index_btree.cpp:514`                  |
| 536  | `After merge: depth={}, interior={}, leaf={}`                                                               | `tests/test_index_btree.cpp:521`                  |
| 537  | `CP2 Phase - Step 3: Triggering CP2 to flush all merge changes`                                             | `tests/test_index_btree.cpp:524`                  |
| 538  | `CP2 completed - all buffers are now CLEAN for the next CP`                                                 | `tests/test_index_btree.cpp:526`                  |
| 539  | `CP3 Phase - Step 4: Removing non-existent key to trigger collapse`                                         | `tests/test_index_btree.cpp:529`                  |
| 540  | `Attempting to remove key {} (doesn't exist)`                                                               | `tests/test_index_btree.cpp:531`                  |
| 541  | `After collapse attempt: depth={}, interior={}, leaf={}, keys={}`                                           | `tests/test_index_btree.cpp:538`                  |
| 542  | `Root collapse occurred: depth {} → {}`                                                                     | `tests/test_index_btree.cpp:542`                  |
| 543  | `CP3 Phase - Step 5: Triggering CP3 to verify it completes successfully...`                                 | `tests/test_index_btree.cpp:548`                  |
| 544  | `CP3 completed successfully`                                                                                | `tests/test_index_btree.cpp:567`                  |
| 545  | `Index table recovered`                                                                                     | `tests/test_index_btree.cpp:581`                  |
| 546  | `Root bnode_id {} version {}`                                                                               | `tests/test_index_btree.cpp:582`                  |
| 547  | `Node size {}`                                                                                              | `tests/test_index_btree.cpp:608`                  |
| 548  | `Added index table to index service`                                                                        | `tests/test_index_btree.cpp:630`                  |
| 549  | `cleanup the dump map and index data? {}`                                                                   | `tests/test_index_btree.cpp:642`                  |
| 550  | `File {} removed successfully`                                                                              | `tests/test_index_btree.cpp:647`                  |
| 551  | `Error: failed to remove {}`                                                                                | `tests/test_index_btree.cpp:649`                  |
| 552  | `Teardown with Root bnode_id {} tree size: {} btree node count (interior = {} leaf= {})`                    | `tests/test_index_btree.cpp:653`                  |
| 553  | `Using seed {} to sow the random generation`                                                                | `tests/test_index_btree.cpp:687`                  |
| 554  | `No seed provided. Using randomly generated seed: {}`                                                       | `tests/test_index_btree.cpp:691`                  |
| 555  | `First time boot, formatting devices`                                                                       | `tests/test_device_manager.cpp:82`                |
| 556  | `Not first time boot, loading devices`                                                                      | `tests/test_device_manager.cpp:86`                |
| 557  | `creating {} data device files with each of size {}`                                                        | `tests/test_device_manager.cpp:110`               |
| 558  | `Step 1: Creating {} vdevs with combined size as {}`                                                        | `tests/test_device_manager.cpp:162`               |
| 559  | `Step 1a: Creating vdev of name={} with size={}`                                                            | `tests/test_device_manager.cpp:169`               |
| 560  | `Step 2: Validating all vdevs if they have created with correct number of chunks`                           | `tests/test_device_manager.cpp:183`               |
| 561  | `Step 3: Restarting homestore`                                                                              | `tests/test_device_manager.cpp:186`               |
| 562  | `Step 4: Post Restart validate if all vdevs are loaded with correct number of chunks`                       | `tests/test_device_manager.cpp:189`               |
| 563  | `Step 1: Creating {} vdevs with combined size as {}`                                                        | `tests/test_device_manager.cpp:203`               |
| 564  | `Step 1a: Creating vdev of name={} with size={}`                                                            | `tests/test_device_manager.cpp:210`               |
| 565  | `Step 2: Validating all vdevs if they have created with correct number of chunks`                           | `tests/test_device_manager.cpp:224`               |
| 566  | `Step 3a: Remove device to simulate device failure, file={}`                                                | `tests/test_device_manager.cpp:237`               |
| 567  | `Step 3b: Restart dmgr`                                                                                     | `tests/test_device_manager.cpp:239`               |
| 568  | `Step 4: Validate after one device is removed`                                                              | `tests/test_device_manager.cpp:242`               |
| 569  | `Step 5: Recreate file to simulate a new device`                                                            | `tests/test_device_manager.cpp:245`               |
| 570  | `Step 6: Restart and validate if new device can be added to vdevs`                                          | `tests/test_device_manager.cpp:249`               |
| 571  | `Step 7: Restart and validate again`                                                                        | `tests/test_device_manager.cpp:253`               |
| 572  | `Step 1: Creating {} vdevs with combined size as {}`                                                        | `tests/test_device_manager.cpp:275`               |
| 573  | `Step 1a: Creating vdev of name={} with size={}`                                                            | `tests/test_device_manager.cpp:282`               |
| 574  | `Step 2: Validating all vdevs if they have created with correct number of chunks`                           | `tests/test_device_manager.cpp:296`               |
| 575  | `Step 3a: Remove device to simulate device failure, file={}`                                                | `tests/test_device_manager.cpp:309`               |
| 576  | `Step 3a: Remove device to simulate device failure, file={}`                                                | `tests/test_device_manager.cpp:316`               |
| 577  | `Step 3b: Restart dmgr`                                                                                     | `tests/test_device_manager.cpp:319`               |
| 578  | `Step 4: Validate after one device is removed`                                                              | `tests/test_device_manager.cpp:322`               |
| 579  | `Step 5: Recreate files to simulate new devices`                                                            | `tests/test_device_manager.cpp:325`               |
| 580  | `Step 6: Restart and validate if new device can be added to vdevs`                                          | `tests/test_device_manager.cpp:330`               |
| 581  | `Step 7: Restart and validate again`                                                                        | `tests/test_device_manager.cpp:334`               |
| 582  | `Step 1: Creating {} vdevs with combined size as {}`                                                        | `tests/test_device_manager.cpp:356`               |
| 583  | `Step 1a: Creating vdev of name={} with size={}`                                                            | `tests/test_device_manager.cpp:363`               |
| 584  | `Step 2: Validating all vdevs if they have created with correct number of chunks`                           | `tests/test_device_manager.cpp:377`               |
| 585  | `Step 3a: Remove device to simulate device failure, file={}`                                                | `tests/test_device_manager.cpp:390`               |
| 586  | `Step 3a: Remove device to simulate device failure, file={}`                                                | `tests/test_device_manager.cpp:397`               |
| 587  | `Step 3b: Restart dmgr after removing devices`                                                              | `tests/test_device_manager.cpp:400`               |
| 588  | `Step 4: Validate after devices is removed`                                                                 | `tests/test_device_manager.cpp:403`               |
| 589  | `Step 5: Recreate file to simulate replacement with a new device, file={}`                                  | `tests/test_device_manager.cpp:406`               |
| 590  | `Step 6: Recreate file to simulate replacement with a new device, file={}`                                  | `tests/test_device_manager.cpp:413`               |
| 591  | `Step 7: Restart and validate again`                                                                        | `tests/test_device_manager.cpp:418`               |
| 592  | `Step 1: Creating vdev of name={} with size={}`                                                             | `tests/test_device_manager.cpp:437`               |
| 593  | `Step 1: Creating test_vdev with size={}`                                                                   | `tests/test_device_manager.cpp:459`               |
| 594  | `Step 2: Creating {} chunks`                                                                                | `tests/test_device_manager.cpp:474`               |
| 595  | `Step 3: Restarting homestore`                                                                              | `tests/test_device_manager.cpp:485`               |
| 596  | `Step 4: Creating additional {} chunks`                                                                     | `tests/test_device_manager.cpp:488`               |
| 597  | `Object Life Counter\n:{}`                                                                                  | `tests/test_index_crash_recovery.cpp:77`          |
| 598  | `Index table recovered, root bnode_id {} uuid {} ordinal {} version {}`                                     | `tests/test_index_crash_recovery.cpp:283`         |
| 599  | `Node size {}, max_keys_in_node {}, min_keys_in_node {}`                                                    | `tests/test_index_crash_recovery.cpp:320`         |
| 600  | `Creating new index table with uuid {} - init_device:{:s} bt: {} root id {}, num of // keys {}`             | `tests/test_index_crash_recovery.cpp:332`         |
| 601  | `Creating new index table with uuid {} - root id {}, num of keys {}`                                        | `tests/test_index_crash_recovery.cpp:335`         |
| 602  | `Added index table to index service with uuid {} - total tables in the system is currently {}`              | `tests/test_index_crash_recovery.cpp:343`         |
| 603  | `Populating shadow map`                                                                                     | `tests/test_index_crash_recovery.cpp:348`         |
| 604  | `Shadow map size {} - btree keys {} - root id {}`                                                           | `tests/test_index_crash_recovery.cpp:351`         |
| 605  | `Destroying index btree with uuid {} root id {}`                                                            | `tests/test_index_crash_recovery.cpp:358`         |
| 606  | `Reset btree with uuid {} - erase shadow map {}`                                                            | `tests/test_index_crash_recovery.cpp:372`         |
| 607  | `destroy btree - erase shadow map {}`                                                                       | `tests/test_index_crash_recovery.cpp:381`         |
| 608  | `Installed fresh btree with uuid {}`                                                                        | `tests/test_index_crash_recovery.cpp:393`         |
| 609  | `\n\n\n\n\n\n shutdown homestore for index service Test\n\n\n\n\n`                                          | `tests/test_index_crash_recovery.cpp:398`         |
| 610  | `\tSnapshot before crash\n{}`                                                                               | `tests/test_index_crash_recovery.cpp:405`         |
| 611  | `tree after recovered stored in {}`                                                                         | `tests/test_index_crash_recovery.cpp:411`         |
| 612  | `Diff between shadow map and snapshot map\n{}\n`                                                            | `tests/test_index_crash_recovery.cpp:418`         |
| 613  | `cleanup the dump map and index data? {}`                                                                   | `tests/test_index_crash_recovery.cpp:453`         |
| 614  | `File {} removed successfully`                                                                              | `tests/test_index_crash_recovery.cpp:458`         |
| 615  | `Error: failed to remove {}`                                                                                | `tests/test_index_crash_recovery.cpp:460`         |
| 616  | `Teardown with Root bnode_id {} tree size: {}`                                                              | `tests/test_index_crash_recovery.cpp:463`         |
| 617  | `Expect to have [{},{}) in tree and it is actually{}`                                                       | `tests/test_index_crash_recovery.cpp:480`         |
| 618  | `Sanity check passed for {} keys!`                                                                          | `tests/test_index_crash_recovery.cpp:505`         |
| 619  | `Before Crash: {} keys in shadow map and it is actually {} keys in tree - operations size {}`               | `tests/test_index_crash_recovery.cpp:510`         |
| 620  | `Visualize the tree before crash file {}`                                                                   | `tests/test_index_crash_recovery.cpp:515`         |
| 621  | `waiting for crash to recover`                                                                              | `tests/test_index_crash_recovery.cpp:521`         |
| 622  | `Visualize the tree file after recovery : {}`                                                               | `tests/test_index_crash_recovery.cpp:526`         |
| 623  | `Before Reapply: {} keys in shadow map and actually {} in trees operation size {}`                          | `tests/test_index_crash_recovery.cpp:533`         |
| 624  | `Visualize the tree after reapply {}`                                                                       | `tests/test_index_crash_recovery.cpp:538`         |
| 625  | `After reapply: {} keys in shadow map and actually {} in tress`                                             | `tests/test_index_crash_recovery.cpp:544`         |
| 626  | `Step 0: Fill up the tree with {} entries`                                                                  | `tests/test_index_crash_recovery.cpp:587`         |
| 627  | `Step 0-1: Flush all the entries so far`                                                                    | `tests/test_index_crash_recovery.cpp:609`         |
| 628  | `\n\n\n\n\n\nRound {} of {}\n\n\n\n\n\n`                                                                    | `tests/test_index_crash_recovery.cpp:617`         |
| 629  | `{}`                                                                                                        | `tests/test_index_crash_recovery.cpp:662`         |
| 630  | `{}`                                                                                                        | `tests/test_index_crash_recovery.cpp:681`         |
| 631  | `Step 1-{}: No flip set`                                                                                    | `tests/test_index_crash_recovery.cpp:685`         |
| 632  | `It is time to stop but let's finish this round and then stop!`                                             | `tests/test_index_crash_recovery.cpp:733`         |
| 633  | `\n\n\n\t\t\tProgress: {} rounds of total {} ({:.2f}%) completed - Elapsed time: {:.0f} seconds of`         | `tests/test_index_crash_recovery.cpp:755`         |
| 634  | `Step 1: Fill up the last quarter of the tree`                                                              | `tests/test_index_crash_recovery.cpp:798`         |
| 635  | `Step 2: Flush all the entries so far`                                                                      | `tests/test_index_crash_recovery.cpp:805`         |
| 636  | `Step 3: Fill the 3rd quarter of the tree, to make sure left child is split and we crash on flush of the`   | `tests/test_index_crash_recovery.cpp:811`         |
| 637  | `Step 4: Crash and reapply the missing entries to tree`                                                     | `tests/test_index_crash_recovery.cpp:817`         |
| 638  | `Step 5: Fill the 2nd quarter of the tree, to make sure left child is split and we crash on flush of the`   | `tests/test_index_crash_recovery.cpp:820`         |
| 639  | `inserting key {}`                                                                                          | `tests/test_index_crash_recovery.cpp:825`         |
| 640  | `Step 6: Simulate crash and then recover, reapply keys to tree`                                             | `tests/test_index_crash_recovery.cpp:831`         |
| 641  | `Step 7: Fill the 1st quarter of the tree, to make sure left child is split and we crash on flush of the`   | `tests/test_index_crash_recovery.cpp:834`         |
| 642  | `Step 8: Post crash we reapply the missing entries to tree`                                                 | `tests/test_index_crash_recovery.cpp:840`         |
| 643  | `Step 9: Query all entries and validate with pagination of 80 entries`                                      | `tests/test_index_crash_recovery.cpp:842`         |
| 644  | `Step 1-{}: Set flag {}`                                                                                    | `tests/test_index_crash_recovery.cpp:855`         |
| 645  | `Batch {} Operations:\n {} \n`                                                                              | `tests/test_index_crash_recovery.cpp:858`         |
| 646  | `Detailed Key Occurrences for Batch {}:\n {} \n`                                                            | `tests/test_index_crash_recovery.cpp:859`         |
| 647  | `\t\t\t\t\t\t\t\t\t\t\t\t\tupserting entry {}`                                                              | `tests/test_index_crash_recovery.cpp:862`         |
| 648  | `=== Testing flip point: {} - {} ===`                                                                       | `tests/test_index_crash_recovery.cpp:1149`        |
| 649  | `Step {}-0: Populate some keys and flush`                                                                   | `tests/test_index_crash_recovery.cpp:1152`        |
| 650  | `\n\n\n\n\n\n\n\n\n\n\n\n\n\nStep {}-1: Set crash flag {}`                                                  | `tests/test_index_crash_recovery.cpp:1161`        |
| 651  | `Step {}-1-1: Remove keys in batch {}/{} ({} to {})`                                                        | `tests/test_index_crash_recovery.cpp:1171`        |
| 652  | `Step {}-1-2: Trigger cp to crash`                                                                          | `tests/test_index_crash_recovery.cpp:1178`        |
| 653  | `\n\n\n\n\n\n\n\n\n\n\n\n\n\nStep {}-2: Set crash flag {}`                                                  | `tests/test_index_crash_recovery.cpp:1184`        |
| 654  | `Step {}-2-1: Remove keys in batch {}/{} ({} to {})`                                                        | `tests/test_index_crash_recovery.cpp:1193`        |
| 655  | `Step {}-2-2: Trigger cp to crash`                                                                          | `tests/test_index_crash_recovery.cpp:1200`        |
| 656  | `\n\n\n\n\n\n\n\n\n\n\n\n\n\nStep {}-3: Set crash flag {}`                                                  | `tests/test_index_crash_recovery.cpp:1206`        |
| 657  | `Step {}-3-1: Remove keys in batch {}/{} ({} to {})`                                                        | `tests/test_index_crash_recovery.cpp:1215`        |
| 658  | `Step {}-3-2: Trigger cp to crash`                                                                          | `tests/test_index_crash_recovery.cpp:1224`        |
| 659  | `\n\n\n\n\n\n\n\n\n\n\n\n\n\nStep {}-4: Set crash flag {} Remove another batch in ascending order`          | `tests/test_index_crash_recovery.cpp:1230`        |
| 660  | `Step {}-4-1: Remove keys in batch {}/{} ({} to {})`                                                        | `tests/test_index_crash_recovery.cpp:1240`        |
| 661  | `Step {}-4-2: Trigger cp to crash`                                                                          | `tests/test_index_crash_recovery.cpp:1247`        |
| 662  | `Step 1: Preload {} keys and flush baseline CP`                                                             | `tests/test_index_crash_recovery.cpp:1283`        |
| 663  | `Step 2: Set crash flip on split-at-parent`                                                                 | `tests/test_index_crash_recovery.cpp:1293`        |
| 664  | `Step 3: Insert {} extra keys to cause splits (txn_journal populated)`                                      | `tests/test_index_crash_recovery.cpp:1301`        |
| 665  | `Step 4: Destroy index table — meta superblock removed from disk`                                           | `tests/test_index_crash_recovery.cpp:1309`        |
| 666  | `Step 5: Trigger CP (crash expected after journal is written)`                                              | `tests/test_index_crash_recovery.cpp:1316`        |
| 667  | `Step 6: Waiting for crash-recovery`                                                                        | `tests/test_index_crash_recovery.cpp:1322`        |
| 668  | `Step 6: Recovery succeeded without aborting — destroyed table was handled gracefully`                      | `tests/test_index_crash_recovery.cpp:1324`        |
| 669  | `Step 7: Installing a fresh empty btree for teardown`                                                       | `tests/test_index_crash_recovery.cpp:1328`        |
| 670  | `Step {}-1: Init btree`                                                                                     | `tests/test_index_crash_recovery.cpp:1394`        |
| 671  | `Step {}-2: Set flag {}`                                                                                    | `tests/test_index_crash_recovery.cpp:1401`        |
| 672  | `Removing {} keys before crash`                                                                             | `tests/test_index_crash_recovery.cpp:1411`        |
| 673  | `Removing key {}`                                                                                           | `tests/test_index_crash_recovery.cpp:1414`        |
| 674  | `Step {}-3: Simulate crash and recover`                                                                     | `tests/test_index_crash_recovery.cpp:1418`        |
| 675  | `\n\tTesting scenario {}`                                                                                   | `tests/test_index_crash_recovery.cpp:1483`        |
| 676  | `\n\t\t\t\tTesting flip point: {}`                                                                          | `tests/test_index_crash_recovery.cpp:1487`        |
| 677  | `Step {}-{}-1: Populate keys and flush`                                                                     | `tests/test_index_crash_recovery.cpp:1489`        |
| 678  | `Step {}-{}-2: Set crash flag, remove keys in reverse order`                                                | `tests/test_index_crash_recovery.cpp:1493`        |
| 679  | `Removing entry {}`                                                                                         | `tests/test_index_crash_recovery.cpp:1496`        |
| 680  | `Step {}-{}-3: Trigger cp to crash`                                                                         | `tests/test_index_crash_recovery.cpp:1502`        |
| 681  | `Index table recovered ordinal={} uuid={}`                                                                  | `tests/test_index_crash_recovery.cpp:1542`        |
| 682  | `SetUp: created {} index tables`                                                                            | `tests/test_index_crash_recovery.cpp:1597`        |
| 683  | `Step 1: Set crash_flush_on_root flip (must precede inserts)`                                               | `tests/test_index_crash_recovery.cpp:1636`        |
| 684  | `Step 2: Insert {} entries into table 0 (root split fires the flip)`                                        | `tests/test_index_crash_recovery.cpp:1639`        |
| 685  | `Step 3: Insert {} entries into table 1 (root split, flip already consumed)`                                | `tests/test_index_crash_recovery.cpp:1642`        |
| 686  | `Step 4: Trigger CP (will crash during buffer flush)`                                                       | `tests/test_index_crash_recovery.cpp:1647`        |
| 687  | `Step 5: Waiting for crash recovery`                                                                        | `tests/test_index_crash_recovery.cpp:1653`        |
| 688  | `Step 6: Recovery succeeded - bug is fixed (SDSTOR-21880)`                                                  | `tests/test_index_crash_recovery.cpp:1656`        |
| 689  | `Index table recovered, root bnode_id {} uuid {} ordinal {} version {}`                                     | `tests/test_index_crash_recovery.cpp:1750`        |
| 690  | `Persistently consumed {} index free blks; remaining free blks={}`                                          | `tests/test_index_crash_recovery.cpp:1913`        |
| 691  | `Step 1: preload sparse keys [0, {}] step {} and flush the baseline CP`                                     | `tests/test_index_crash_recovery.cpp:1922`        |
| 692  | `Step 1b: persistently consume index free queue so crash-CP created nodes are near recovery queue head`     | `tests/test_index_crash_recovery.cpp:1926`        |
| 693  | `Step 2: set existing crash flip to crash after journal persistence but before parent flush`                | `tests/test_index_crash_recovery.cpp:1936`        |
| 694  | `Step 3: observe the first current-CP split node, then make that exact blkid created+freed`                 | `tests/test_index_crash_recovery.cpp:1939`        |
| 695  | `Found {} created+freed node candidates in the crash CP`                                                    | `tests/test_index_crash_recovery.cpp:1951`        |
| 696  | `Step 4: create many more split records in the same unflushed CP to force recovery repair allocations`      | `tests/test_index_crash_recovery.cpp:1953`        |
| 697  | `Step 5: crash and recover through the normal recovery/repair path`                                         | `tests/test_index_crash_recovery.cpp:1956`        |
| 698  | `Step 6: do sanity check`                                                                                   | `tests/test_index_crash_recovery.cpp:1962`        |
| 699  | `Step 7: fixed recovery detected; no same-CP created+freed blkid was freed, verify writes and reads`        | `tests/test_index_crash_recovery.cpp:1978`        |
| 700  | `Using seed {} to sow the random generation`                                                                | `tests/test_index_crash_recovery.cpp:1999`        |
| 701  | `No seed provided. Using randomly generated seed: {}`                                                       | `tests/test_index_crash_recovery.cpp:2003`        |
| 702  | `after_write_cb: Write completed;`                                                                          | `tests/test_append_blkalloc.cpp:112`              |
| 703  | `Step 2: async read on blkid: {}`                                                                           | `tests/test_append_blkalloc.cpp:120`              |
| 704  | `Read completed;`                                                                                           | `tests/test_append_blkalloc.cpp:128`              |
| 705  | `after_write_cb: Write completed;`                                                                          | `tests/test_append_blkalloc.cpp:141`              |
| 706  | `Step 2: started async_free_blk: {}`                                                                        | `tests/test_append_blkalloc.cpp:144`              |
| 707  | `completed async_free_blks`                                                                                 | `tests/test_append_blkalloc.cpp:149`              |
| 708  | `Step 1: run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_append_blkalloc.cpp:195`              |
| 709  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_append_blkalloc.cpp:198`              |
| 710  | `Step 3: I/O completed, do shutdown.`                                                                       | `tests/test_append_blkalloc.cpp:201`              |
| 711  | `Step 1: run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_append_blkalloc.cpp:207`              |
| 712  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_append_blkalloc.cpp:210`              |
| 713  | `Step 3: I/O completed, do shutdown.`                                                                       | `tests/test_append_blkalloc.cpp:213`              |
| 714  | `Step 1: run on worker thread to schedule write for {} Bytes, then free blk.`                               | `tests/test_append_blkalloc.cpp:219`              |
| 715  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_append_blkalloc.cpp:223`              |
| 716  | `Step 3: I/O completed, do shutdown.`                                                                       | `tests/test_append_blkalloc.cpp:226`              |
| 717  | `Step 1: run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_append_blkalloc.cpp:231`              |
| 718  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_append_blkalloc.cpp:234`              |
| 719  | `Step 3: I/O completed, trigger_cp and wait.`                                                               | `tests/test_append_blkalloc.cpp:237`              |
| 720  | `Step 4: cp completed, do shutdown.`                                                                        | `tests/test_append_blkalloc.cpp:240`              |
| 721  | `Step 1: run on worker thread to schedule write for {} Bytes, then free blk.`                               | `tests/test_append_blkalloc.cpp:246`              |
| 722  | `Step 2: Wait for I/O to complete.`                                                                         | `tests/test_append_blkalloc.cpp:250`              |
| 723  | `Step 3: I/O completed, trigger_cp and wait.`                                                               | `tests/test_append_blkalloc.cpp:253`              |
| 724  | `Step 4: cp completed, restart homestore.`                                                                  | `tests/test_append_blkalloc.cpp:256`              |
| 725  | `Step 5: Restarted homestore with data service recovered`                                                   | `tests/test_append_blkalloc.cpp:260`              |
| 726  | `Step 6: run on worker thread to schedule write for {} Bytes.`                                              | `tests/test_append_blkalloc.cpp:264`              |
| 727  | `Step 7: Wait for I/O to complete.`                                                                         | `tests/test_append_blkalloc.cpp:267`              |
| 728  | `Step 8: I/O completed, trigger_cp and wait.`                                                               | `tests/test_append_blkalloc.cpp:270`              |
| 729  | `Step 9: do shutdown.`                                                                                      | `tests/test_append_blkalloc.cpp:273`              |
| 730  | `Step 0: initialize BlkReadTracker instance.`                                                               | `tests/test_blk_read_tracker.cpp:35`              |
| 731  | `Step 1: set entries per record to 16`                                                                      | `tests/test_blk_read_tracker.cpp:69`              |
| 732  | `Step 2: read {} BlkId (no overlap), with nblks:{} to hash map.`                                            | `tests/test_blk_read_tracker.cpp:76`              |
| 733  | `Step 3: remove {} BlkIds, with nblks {} from hash map.`                                                    | `tests/test_blk_read_tracker.cpp:86`              |
| 734  | `Step 1: set entries per record to 16`                                                                      | `tests/test_blk_read_tracker.cpp:98`              |
| 735  | `Step 2: read olverlaped BlkIds to hash map.`                                                               | `tests/test_blk_read_tracker.cpp:101`             |
| 736  | `wait_on called on blkid: {};`                                                                              | `tests/test_blk_read_tracker.cpp:143`             |
| 737  | `Step 1: read blkid: {} into hash map.`                                                                     | `tests/test_blk_read_tracker.cpp:160`             |
| 738  | `Step 2: free blkid: {} to be completed on reading`                                                         | `tests/test_blk_read_tracker.cpp:164`             |
| 739  | `wait_on callback triggered on blkid: {};`                                                                  | `tests/test_blk_read_tracker.cpp:168`             |
| 740  | `Step 3: try to do read completed on blkid: {}.`                                                            | `tests/test_blk_read_tracker.cpp:171`             |
| 741  | `Step 4: assert that callback is triggered by read complete.`                                               | `tests/test_blk_read_tracker.cpp:175`             |
| 742  | `Step 1: set entries per record to {}.`                                                                     | `tests/test_blk_read_tracker.cpp:191`             |
| 743  | `Step 2: read on blkid: {}.`                                                                                | `tests/test_blk_read_tracker.cpp:195`             |
| 744  | `Step 3: free blkid: {}.`                                                                                   | `tests/test_blk_read_tracker.cpp:200`             |
| 745  | `wait_on callback triggered on blkid: {}.`                                                                  | `tests/test_blk_read_tracker.cpp:204`             |
| 746  | `Step 4: read on blkid: {}.`                                                                                | `tests/test_blk_read_tracker.cpp:208`             |
| 747  | `Step 5a: read on blkid: {} completed.`                                                                     | `tests/test_blk_read_tracker.cpp:211`             |
| 748  | `Step 5b: assert callback on free_bid should NOT be triggered.`                                             | `tests/test_blk_read_tracker.cpp:214`             |
| 749  | `Step 6a: read on blkid: {} completed.`                                                                     | `tests/test_blk_read_tracker.cpp:217`             |
| 750  | `Step 6b: assert callback should be triggered`                                                              | `tests/test_blk_read_tracker.cpp:220`             |
| 751  | `Step 1: set entries per record to {}.`                                                                     | `tests/test_blk_read_tracker.cpp:236`             |
| 752  | `Step 2: read 1 blkid: {}.`                                                                                 | `tests/test_blk_read_tracker.cpp:240`             |
| 753  | `Step 3: free blkid: {}.`                                                                                   | `tests/test_blk_read_tracker.cpp:244`             |
| 754  | `wait on callback triggered on blkid: {}`                                                                   | `tests/test_blk_read_tracker.cpp:249`             |
| 755  | `Step 4: read 2 blkid: {}.`                                                                                 | `tests/test_blk_read_tracker.cpp:253`             |
| 756  | `Step 5: read 2 completes`                                                                                  | `tests/test_blk_read_tracker.cpp:256`             |
| 757  | `Step 5a: assert callback should NOT be triggered.`                                                         | `tests/test_blk_read_tracker.cpp:259`             |
| 758  | `Step 6: read 1 completes`                                                                                  | `tests/test_blk_read_tracker.cpp:262`             |
| 759  | `Step 6a: assert callback should be triggered.`                                                             | `tests/test_blk_read_tracker.cpp:265`             |
| 760  | `Step 1: set entries per record to {}.`                                                                     | `tests/test_blk_read_tracker.cpp:281`             |
| 761  | `Step 2: read 1 blkid: {}.`                                                                                 | `tests/test_blk_read_tracker.cpp:286`             |
| 762  | `Step 3: read 2 blkid: {}.`                                                                                 | `tests/test_blk_read_tracker.cpp:291`             |
| 763  | `Step 4: free blkid: {}.`                                                                                   | `tests/test_blk_read_tracker.cpp:297`             |
| 764  | `wait_on callback triggered on blkid: {}`                                                                   | `tests/test_blk_read_tracker.cpp:301`             |
| 765  | `Step 5a: read complete on blkid: {}`                                                                       | `tests/test_blk_read_tracker.cpp:304`             |
| 766  | `Step 5b: assert callback not triggered yet.`                                                               | `tests/test_blk_read_tracker.cpp:307`             |
| 767  | `Step 6a: read complete on blkid: {}`                                                                       | `tests/test_blk_read_tracker.cpp:310`             |
| 768  | `Step 6b: assert that callback is triggered by read completes`                                              | `tests/test_blk_read_tracker.cpp:313`             |
| 769  | `Step 1: set entries per record to {}.`                                                                     | `tests/test_blk_read_tracker.cpp:331`             |
| 770  | `Step 2: read-1 on blkid: {}.`                                                                              | `tests/test_blk_read_tracker.cpp:335`             |
| 771  | `Step 3: free on blkid: {}.`                                                                                | `tests/test_blk_read_tracker.cpp:339`             |
| 772  | `wait on callback triggered on free_bid: {}`                                                                | `tests/test_blk_read_tracker.cpp:344`             |
| 773  | `Step 4: read-2 on blkid: {}.`                                                                              | `tests/test_blk_read_tracker.cpp:348`             |
| 774  | `Step 5a: read-1 completed on blkid: {}.`                                                                   | `tests/test_blk_read_tracker.cpp:351`             |
| 775  | `Step 5b: assert free blk callback should be triggered`                                                     | `tests/test_blk_read_tracker.cpp:354`             |
| 776  | `Step 6: read-2 completed on blkid: {}.`                                                                    | `tests/test_blk_read_tracker.cpp:357`             |
| 777  | `Step 1: set entries per record to {}.`                                                                     | `tests/test_blk_read_tracker.cpp:371`             |
| 778  | `Step 2: threaded insert issued.`                                                                           | `tests/test_blk_read_tracker.cpp:388`             |
| 779  | `Step 3: threaded insert joined.`                                                                           | `tests/test_blk_read_tracker.cpp:397`             |
| 780  | `Step 4: threaded remove issued.`                                                                           | `tests/test_blk_read_tracker.cpp:407`             |
| 781  | `Step 4: all threads joined.`                                                                               | `tests/test_blk_read_tracker.cpp:412`             |
| 782  | `Step 1: set entries per record to {}.`                                                                     | `tests/test_blk_read_tracker.cpp:417`             |
| 783  | `Step 2: threaded insert issued.`                                                                           | `tests/test_blk_read_tracker.cpp:432`             |
| 784  | `wait_on called on blkid: {};`                                                                              | `tests/test_blk_read_tracker.cpp:442`             |
| 785  | `Step 3: threaded wait_on issued on all bids.`                                                              | `tests/test_blk_read_tracker.cpp:455`             |
| 786  | `Step 3: threaded remove issued.`                                                                           | `tests/test_blk_read_tracker.cpp:464`             |
| 787  | `Step 4: all threads joined.`                                                                               | `tests/test_blk_read_tracker.cpp:469`             |
| 788  | `Step 5: all bids wait_on cb called.`                                                                       | `tests/test_blk_read_tracker.cpp:475`             |
| 789  | `Step 1: set entries per record to {}.`                                                                     | `tests/test_blk_read_tracker.cpp:485`             |
| 790  | `Step 2: randome threaded insert/remove/wait_on operation:`                                                 | `tests/test_blk_read_tracker.cpp:493`             |
| 791  | `Step 3: all threads joined.`                                                                               | `tests/test_blk_read_tracker.cpp:546`             |
| 792  | `Step 4: Test Passed.`                                                                                      | `tests/test_blk_read_tracker.cpp:550`             |
| 793  | `Step 1: Append and test {} records`                                                                        | `tests/test_home_raft_logstore.cpp:232`           |
| 794  | `Step 2: Rollback half of the records`                                                                      | `tests/test_home_raft_logstore.cpp:235`           |
| 795  | `Step 3: Post rollback add {} records`                                                                      | `tests/test_home_raft_logstore.cpp:238`           |
| 796  | `Step 4: Compact first 10% records = {}`                                                                    | `tests/test_home_raft_logstore.cpp:242`           |
| 797  | `Step 5: Post compaction add {} records`                                                                    | `tests/test_home_raft_logstore.cpp:245`           |
| 798  | `Step 6: Compaction 10% records={} beyond max appended entries test`                                        | `tests/test_home_raft_logstore.cpp:249`           |
| 799  | `Step 7: Post compaction add {} records`                                                                    | `tests/test_home_raft_logstore.cpp:252`           |
| 800  | `Step 8: Pack all records`                                                                                  | `tests/test_home_raft_logstore.cpp:255`           |
| 801  | `Step 9: Unpack all records on an empty logstore`                                                           | `tests/test_home_raft_logstore.cpp:258`           |
| 802  | `Step 10: Append more {} records to follower logstore`                                                      | `tests/test_home_raft_logstore.cpp:261`           |
| 803  | `Step 11: Unpack same leader records again after append inserted records`                                   | `tests/test_home_raft_logstore.cpp:264`           |
| 804  | `Step 12: Restart homestore and validate recovery`                                                          | `tests/test_home_raft_logstore.cpp:267`           |
| 805  | `Step 13: Post recovery do append test`                                                                     | `tests/test_home_raft_logstore.cpp:272`           |
| 806  | `Step 1: Append some records and then compact all`                                                          | `tests/test_home_raft_logstore.cpp:278`           |
| 807  | `Step 2: Write at start boundary when no logs exist, expect allowed`                                        | `tests/test_home_raft_logstore.cpp:282`           |
| 808  | `Step 3: Write at start boundary again, expect allowed`                                                     | `tests/test_home_raft_logstore.cpp:288`           |
| 809  | `Metrics: {}`                                                                                               | `tests/log_store_benchmark.cpp:188`               |
| 810  | `Received on_commit lsn={}`                                                                                 | `tests/test_solo_repl_dev.cpp:97`                 |
| 811  | `ReplDev restarted`                                                                                         | `tests/test_solo_repl_dev.cpp:130`                |
| 812  | `Received error={} on repl_dev`                                                                             | `tests/test_solo_repl_dev.cpp:134`                |
| 813  | `Repl dev init completed CB called`                                                                         | `tests/test_solo_repl_dev.cpp:163`                |
| 814  | `[{}] Write complete with lsn={} for size={} blkid={}`                                                      | `tests/test_solo_repl_dev.cpp:349`                |
| 815  | `Flip {} set`                                                                                               | `tests/test_solo_repl_dev.cpp:391`                |
| 816  | `Step 1: run on worker threads to schedule write for {} Bytes.`                                             | `tests/test_solo_repl_dev.cpp:402`                |
| 817  | `Step 2: Restart homestore and validate replay data.`                                                       | `tests/test_solo_repl_dev.cpp:406`                |
| 818  | `Step 1: run on worker threads to schedule write for random bytes ranging {}-{}.`                           | `tests/test_solo_repl_dev.cpp:411`                |
| 819  | `Step 2: Restart homestore and validate replay data.`                                                       | `tests/test_solo_repl_dev.cpp:419`                |
| 820  | `Step 1: run on worker threads to schedule write`                                                           | `tests/test_solo_repl_dev.cpp:424`                |
| 821  | `Step 2: Restart homestore and validate replay data.`                                                       | `tests/test_solo_repl_dev.cpp:427`                |
| 822  | `Step 1: run on worker threads to schedule write for random bytes ranging {}-{}.`                           | `tests/test_solo_repl_dev.cpp:432`                |
| 823  | `Step 2: Restart homestore and validate replay data.`                                                       | `tests/test_solo_repl_dev.cpp:440`                |
| 824  | `Step 1: run on worker threads to schedule write and truncate`                                              | `tests/test_solo_repl_dev.cpp:447`                |
| 825  | `Index table recovered`                                                                                     | `tests/test_index_gc.cpp:56`                      |
| 826  | `Root bnode_id {} version {}`                                                                               | `tests/test_index_gc.cpp:57`                      |
| 827  | `Node size {}`                                                                                              | `tests/test_index_gc.cpp:72`                      |
| 828  | `Added index table to index service`                                                                        | `tests/test_index_gc.cpp:102`                     |
| 829  | `Teardown with Root bnode_id {} tree size: {} btree node count (interior = {} leaf= {})`                    | `tests/test_index_gc.cpp:107`                     |
| 830  | `Created {} IO reactors`                                                                                    | `tests/test_index_gc.cpp:150`                     |
| 831  | `GC task {} started`                                                                                        | `tests/test_index_gc.cpp:206`                     |
| 832  | `done pct={}, elapsed={}, fiber idx={}`                                                                     | `tests/test_index_gc.cpp:225`                     |
| 833  | `GC task {} completed`                                                                                      | `tests/test_index_gc.cpp:229`                     |
| 834  | `Put task {} started`                                                                                       | `tests/test_index_gc.cpp:234`                     |
| 835  | `Put task {} completed`                                                                                     | `tests/test_index_gc.cpp:242`                     |
| 836  | `Get task {} started`                                                                                       | `tests/test_index_gc.cpp:247`                     |
| 837  | `Get task {} completed`                                                                                     | `tests/test_index_gc.cpp:258`                     |
| 838  | `Chunk GC test start`                                                                                       | `tests/test_index_gc.cpp:279`                     |
| 839  | `Chunk GC test passed`                                                                                      | `tests/test_index_gc.cpp:294`                     |
| 840  | `Using seed {} to sow the random generation`                                                                | `tests/test_index_gc.cpp:306`                     |
| 841  | `No seed provided. Using randomly generated seed: {}`                                                       | `tests/test_index_gc.cpp:310`                     |
| 842  | `Count {} is not a power of 2, rounding total count to {}`                                                  | `tests/test_blkalloc.cpp:81`                      |
| 843  | `Metrics after preallocate: {}`                                                                             | `tests/test_blkalloc.cpp:517`                     |
| 844  | `Alloced {} random blks and freed {} random blks in this thread`                                            | `tests/test_blkalloc.cpp:560`                     |
| 845  | `Total Alloced {} random blks and freed {} random blks in all thread`                                       | `tests/test_blkalloc.cpp:565`                     |
| 846  | `Step 0: Reserve {} blks to be not allocated`                                                               | `tests/test_blkalloc.cpp:577`                     |
| 847  | `Step 1: Pre allocate {} objects in {} threads`                                                             | `tests/test_blkalloc.cpp:585`                     |
| 848  | `Step 2: Free {} blks randomly in {} threads`                                                               | `tests/test_blkalloc.cpp:594`                     |
| 849  | `Step 3: Fill in the remaining {} blks to empty the device in {} threads`                                   | `tests/test_blkalloc.cpp:602`                     |
| 850  | `Step 4: Validate if further allocation result in space full error`                                         | `tests/test_blkalloc.cpp:612`                     |
| 851  | `Step 5: Free up {} blocks ({} previously reserved and 2 new) and make sure 2 more alloc is successful and` | `tests/test_blkalloc.cpp:615`                     |
| 852  | `Step 1: Pre allocate {}% of total blks which is {} blks in {} threads`                                     | `tests/test_blkalloc.cpp:648`                     |
| 853  | `For contiguous_unirandsize test, iters={} cannot be more than 1/{}th of total count={}. Adjusting`         | `tests/test_blkalloc.cpp:656`                     |
| 854  | `Step 2: Do alloc/free contiguous blks with completely random size ratio_range=[{}-{}] threads={} iters={}` | `tests/test_blkalloc.cpp:661`                     |
| 855  | `Step 1: Pre allocate {}% of total blks which is {} blks in {} threads`                                     | `tests/test_blkalloc.cpp:693`                     |
| 856  | `For contiguous_unirandsize test, iters={} cannot be more than 1/{}th of total count={}. Adjusting`         | `tests/test_blkalloc.cpp:701`                     |
| 857  | `Step 2: Do alloc/free contiguous blks with completely random size ratio_range=[{}-{}] threads={} iters={}` | `tests/test_blkalloc.cpp:706`                     |
| 858  | `Step 1: Pre allocate {}% of total blks which is {} blks in {} threads`                                     | `tests/test_blkalloc.cpp:734`                     |
| 859  | `Metrics after preallocate: {}`                                                                             | `tests/test_blkalloc.cpp:738`                     |
| 860  | `For contiguous_slabrandsize test, iters={} cannot be more than 1/{}th of total count={}. Adjusting`        | `tests/test_blkalloc.cpp:743`                     |
| 861  | `Step 2: Do alloc/free contiguous blks with on slab sized ratio_range=[{}-{}] threads={} iters={}`          | `tests/test_blkalloc.cpp:748`                     |
| 862  | `Step 1: Pre allocate 50% of total blks which is {} blks in {} threads`                                     | `tests/test_blkalloc.cpp:761`                     |
| 863  | `Step 2: Do alloc/free contiguous blks with completely random size for blks span={}, threads={} iters={}`   | `tests/test_blkalloc.cpp:766`                     |
| 864  | `Step 3: Reallocate to alloc all remaining count {} calculated remaining {}`                                | `tests/test_blkalloc.cpp:774`                     |
| 865  | `Step 1: Pre allocate {}% of total blks which is {} blks in {} threads`                                     | `tests/test_blkalloc.cpp:802`                     |
| 866  | `Step 2: Do alloc/free contiguous blks with completely random size ratio_range=[{}-{}] threads={}`          | `tests/test_blkalloc.cpp:812`                     |
| 867  | `Step 3: Reallocate to alloc all remaining count {} calculated remaining {}`                                | `tests/test_blkalloc.cpp:821`                     |
| 868  | `Step 1: Set the flip to force directly bypassing freeblk cache`                                            | `tests/test_blkalloc.cpp:845`                     |
| 869  | `Step 2: Alloc upto {}% of space which is {} blks in {} threads as scattered blks`                          | `tests/test_blkalloc.cpp:857`                     |
| 870  | `Step 3: Reallocate to alloc all remaining count {} calculated remaining {}`                                | `tests/test_blkalloc.cpp:864`                     |
| 871  | `Step 1: Do forward sequential insert for {} entries`                                                       | `tests/test_mem_btree.cpp:126`                    |
| 872  | `Step 2: Query {} entries and validate with pagination of 75 entries`                                       | `tests/test_mem_btree.cpp:130`                    |
| 873  | `Step 3: Do reverse sequential insert of remaining {} entries`                                              | `tests/test_mem_btree.cpp:135`                    |
| 874  | `Step 4: Query {} entries and validate with pagination of 90 entries`                                       | `tests/test_mem_btree.cpp:139`                    |
| 875  | `Step 5: Query all entries and validate with no pagination`                                                 | `tests/test_mem_btree.cpp:143`                    |
| 876  | `Step 6: Query all entries and validate with pagination of 80 entries`                                      | `tests/test_mem_btree.cpp:146`                    |
| 877  | `Step 7: Get all entries 1-by-1 and validate them`                                                          | `tests/test_mem_btree.cpp:149`                    |
| 878  | `Step 8: Do incorrect input and validate errors`                                                            | `tests/test_mem_btree.cpp:154`                    |
| 879  | `Step 1: Do forward sequential insert for {} entries`                                                       | `tests/test_mem_btree.cpp:162`                    |
| 880  | `Step 2: Query {} entries and validate with pagination of 75 entries`                                       | `tests/test_mem_btree.cpp:166`                    |
| 881  | `Step 3: Do forward sequential remove for {} entries`                                                       | `tests/test_mem_btree.cpp:170`                    |
| 882  | `Step 4: Query {} entries and validate with pagination of 75 entries`                                       | `tests/test_mem_btree.cpp:174`                    |
| 883  | `Step 5: Do reverse sequential remove of remaining {} entries`                                              | `tests/test_mem_btree.cpp:178`                    |
| 884  | `Step 6: Query the empty tree`                                                                              | `tests/test_mem_btree.cpp:183`                    |
| 885  | `Step 1: Do forward random insert for {} entries`                                                           | `tests/test_mem_btree.cpp:200`                    |
| 886  | `Step 1: Do forward sequential insert for {} entries`                                                       | `tests/test_mem_btree.cpp:210`                    |
| 887  | `Step 2: Do range update of random intervals between [1-50] for 100 times with random key ranges`           | `tests/test_mem_btree.cpp:215`                    |
| 888  | `Step 3: Query {} entries and validate with pagination of 75 entries`                                       | `tests/test_mem_btree.cpp:220`                    |
| 889  | `Step 1: Do forward sequential insert for {} entries`                                                       | `tests/test_mem_btree.cpp:227`                    |
| 890  | `Step 2: Do range remove for {} entries`                                                                    | `tests/test_mem_btree.cpp:231`                    |
| 891  | `Step 1: Do forward sequential insert for {} entries`                                                       | `tests/test_mem_btree.cpp:254`                    |
| 892  | `Step 2: Do remove one by one for {} entries`                                                               | `tests/test_mem_btree.cpp:266`                    |
| 893  | `Step 1: Do forward sequential insert for {} entries`                                                       | `tests/test_mem_btree.cpp:279`                    |
| 894  | `Step 2: Do range remove for maximum of {} iterations`                                                      | `tests/test_mem_btree.cpp:286`                    |
| 895  | `Step 2 - {}: Do Range Remove of maximum [{},{}] keys`                                                      | `tests/test_mem_btree.cpp:291`                    |
| 896  | `Step 1: Do forward sequential insert for {} entries`                                                       | `tests/test_mem_btree.cpp:301`                    |
| 897  | `Step 2: Do GC on the tree for keys in range [{}, {}]`                                                      | `tests/test_mem_btree.cpp:403`                    |
| 898  | `Starting iomgr with {} threads`                                                                            | `tests/test_mem_btree.cpp:433`                    |
| 899  | `Using seed {} to sow the random generation`                                                                | `tests/test_mem_btree.cpp:476`                    |
| 900  | `No seed provided. Using randomly generated seed: {}`                                                       | `tests/test_mem_btree.cpp:480`                    |
| 901  | `buf written: size: {}, data: {}`                                                                           | `tests/test_meta_blk_mgr.cpp:230`                 |
| 902  | `compression ratio limit changed to: {}`                                                                    | `tests/test_meta_blk_mgr.cpp:440`                 |
| 903  | `compression ratio limit changed to: {}`                                                                    | `tests/test_meta_blk_mgr.cpp:449`                 |
| 904  | `rand load test finished, total ops: {}, write ops: {}, remove ops:{}, update ops: {}, restart: {}`         | `tests/test_meta_blk_mgr.cpp:478`                 |
| 905  | `Registering client with type: {}`                                                                          | `tests/test_meta_blk_mgr.cpp:591`                 |
| 906  | `max_write_times {}`                                                                                        | `tests/test_meta_blk_mgr.cpp:834`                 |
| 907  | `iter {}, available_blks {}`                                                                                | `tests/test_meta_blk_mgr.cpp:843`                 |
| 908  | `iter {}, available_blks {}`                                                                                | `tests/test_meta_blk_mgr.cpp:854`                 |
| 909  | `skip_header_size_check changed to: {}`                                                                     | `tests/test_meta_blk_mgr.cpp:905`                 |
| 910  | `Invalid input for min/max wrt sz: defaulting to {}/{}`                                                     | `tests/test_meta_blk_mgr.cpp:990`                 |
| 911  | `Testing with run_time: {}, num_io: {}, overflow: {}, write/update/remove percentage: {}/{}/{},`            | `tests/test_meta_blk_mgr.cpp:993`                 |
| 912  | `Written sync data for LSN -> {}:{}`                                                                        | `tests/test_log_dev.cpp:153`                      |
| 913  | `Written async data for LSN -> {}:{}`                                                                       | `tests/test_log_dev.cpp:170`                      |
| 914  | `Flush data from {} to {}`                                                                                  | `tests/test_log_dev.cpp:174`                      |
| 915  | `truncate_validate upto {}`                                                                                 | `tests/test_log_dev.cpp:237`                      |
| 916  | `Iteration {}`                                                                                              | `tests/test_log_dev.cpp:267`                      |
| 917  | `Created new log store -> id {}`                                                                            | `tests/test_log_dev.cpp:272`                      |
| 918  | `Remove logstore -> i {}`                                                                                   | `tests/test_log_dev.cpp:282`                      |
| 919  | `Step 1: Create a single logstore to start rollback test`                                                   | `tests/test_log_dev.cpp:287`                      |
| 920  | `Step 2: Issue sequential inserts with q depth of 10`                                                       | `tests/test_log_dev.cpp:306`                      |
| 921  | `Step 3.0: Rollback last 0 entries and validate if pre-rollback entries are intact`                         | `tests/test_log_dev.cpp:310`                      |
| 922  | `Step 3: Rollback last 50 entries and validate if pre-rollback entries are intact`                          | `tests/test_log_dev.cpp:313`                      |
| 923  | `Step 4: Append 25 entries after rollback is completed`                                                     | `tests/test_log_dev.cpp:316`                      |
| 924  | `Step 5: Rollback again for 75 entries even before previous rollback entry`                                 | `tests/test_log_dev.cpp:319`                      |
| 925  | `Step 6: Append 25 entries after second rollback is completed`                                              | `tests/test_log_dev.cpp:322`                      |
| 926  | `Step 7: Restart homestore and ensure all rollbacks are effectively validated`                              | `tests/test_log_dev.cpp:325`                      |
| 927  | `Step 8: Post recovery, append another 25 entries`                                                          | `tests/test_log_dev.cpp:328`                      |
| 928  | `Step 9: Rollback again for 75 entries even before previous rollback entry`                                 | `tests/test_log_dev.cpp:331`                      |
| 929  | `Step 10: After 3rd rollback, append another 25 entries`                                                    | `tests/test_log_dev.cpp:334`                      |
| 930  | `Step 11: Truncate all entries`                                                                             | `tests/test_log_dev.cpp:337`                      |
| 931  | `Step 12: Restart homestore and ensure all truncations after rollbacks are effectively validated`           | `tests/test_log_dev.cpp:340`                      |
| 932  | `Step 13: Append 25 entries after truncation is completed`                                                  | `tests/test_log_dev.cpp:343`                      |
| 933  | `Step 14: Do another truncation to effectively truncate previous records`                                   | `tests/test_log_dev.cpp:346`                      |
| 934  | `Step 15: Validate if there are no rollback records`                                                        | `tests/test_log_dev.cpp:349`                      |
| 935  | `Step 1: Create a single logstore to start re-truncate test`                                                | `tests/test_log_dev.cpp:354`                      |
| 936  | `Step 2: Issue sequential inserts with q depth of 10`                                                       | `tests/test_log_dev.cpp:359`                      |
| 937  | `Step 3: Truncate all entries`                                                                              | `tests/test_log_dev.cpp:363`                      |
| 938  | `Step 4: Truncate again`                                                                                    | `tests/test_log_dev.cpp:370`                      |
| 939  | `Step 5: Read and verify all entries again`                                                                 | `tests/test_log_dev.cpp:376`                      |
| 940  | `Step 1: Create a single logstore to start truncate with exceeding LSN test`                                | `tests/test_log_dev.cpp:381`                      |
| 941  | `Step 2: Insert 500 entries`                                                                                | `tests/test_log_dev.cpp:386`                      |
| 942  | `Step 3: Read and verify all entries`                                                                       | `tests/test_log_dev.cpp:390`                      |
| 943  | `Step 4: Truncate 100 entries`                                                                              | `tests/test_log_dev.cpp:393`                      |
| 944  | `Step 5: Read and verify all entries`                                                                       | `tests/test_log_dev.cpp:401`                      |
| 945  | `Step 6: Truncate all with exceeding lsn`                                                                   | `tests/test_log_dev.cpp:404`                      |
| 946  | `Step 7 Read and verify all entries`                                                                        | `tests/test_log_dev.cpp:412`                      |
| 947  | `Step 8: Append 500 entries`                                                                                | `tests/test_log_dev.cpp:415`                      |
| 948  | `Step 9: Read and verify all entries`                                                                       | `tests/test_log_dev.cpp:420`                      |
| 949  | `Step 1: Create a single logstore to start truncate with overlapping LSN test`                              | `tests/test_log_dev.cpp:425`                      |
| 950  | `Step 2: Insert 500 entries`                                                                                | `tests/test_log_dev.cpp:444`                      |
| 951  | `Step 3: Read and verify all entries`                                                                       | `tests/test_log_dev.cpp:448`                      |
| 952  | `Step 4: Truncate 100 entries`                                                                              | `tests/test_log_dev.cpp:451`                      |
| 953  | `Step 5: Read and verify all entries`                                                                       | `tests/test_log_dev.cpp:459`                      |
| 954  | `Step 6: Restart and verify all entries`                                                                    | `tests/test_log_dev.cpp:462`                      |
| 955  | `Step 7: call log dev truncate again and read verify`                                                       | `tests/test_log_dev.cpp:470`                      |
| 956  | `Step 1: Create 3 log stores to start truncate across multiple stores test`                                 | `tests/test_log_dev.cpp:476`                      |
| 957  | `Step 2: Insert 100 entries to store {}`                                                                    | `tests/test_log_dev.cpp:483`                      |
| 958  | `Step 3: Insert 200 entries to store {}`                                                                    | `tests/test_log_dev.cpp:488`                      |
| 959  | `Step 4: Insert 200 entries to store {}`                                                                    | `tests/test_log_dev.cpp:493`                      |
| 960  | `Step 5: Read and verify all stores`                                                                        | `tests/test_log_dev.cpp:498`                      |
| 961  | `Step 6: Truncate 100 entries in store {}`                                                                  | `tests/test_log_dev.cpp:514`                      |
| 962  | `Step 7: Read and verify all stores`                                                                        | `tests/test_log_dev.cpp:518`                      |
| 963  | `Step 8: Truncate 500 entries in store {}`                                                                  | `tests/test_log_dev.cpp:534`                      |
| 964  | `Step 9: Read and verify all stores`                                                                        | `tests/test_log_dev.cpp:538`                      |
| 965  | `Step 10: Truncate 100 entries in store {}`                                                                 | `tests/test_log_dev.cpp:555`                      |
| 966  | `Step 11: Read and verify all stores`                                                                       | `tests/test_log_dev.cpp:559`                      |
| 967  | `Step 12: Truncate 300 entries in store {}`                                                                 | `tests/test_log_dev.cpp:576`                      |
| 968  | `Step 13: Read and verify all stores`                                                                       | `tests/test_log_dev.cpp:580`                      |
| 969  | `Step 14: Insert 100 entries in store {}`                                                                   | `tests/test_log_dev.cpp:597`                      |
| 970  | `Step 15: Read and verify all stores`                                                                       | `tests/test_log_dev.cpp:602`                      |
| 971  | `Step 16: Truncate 500 entries in store {}`                                                                 | `tests/test_log_dev.cpp:619`                      |
| 972  | `Step 17: Read and verify all stores`                                                                       | `tests/test_log_dev.cpp:623`                      |
| 973  | `Step 1: Create a single logstore to start truncate-logs-after-flush-and-restart test`                      | `tests/test_log_dev.cpp:642`                      |
| 974  | `Step 2: Insert 100 entries`                                                                                | `tests/test_log_dev.cpp:661`                      |
| 975  | `Step 3: Read and verify all entries`                                                                       | `tests/test_log_dev.cpp:665`                      |
| 976  | `Step 4: Append 100 entries`                                                                                | `tests/test_log_dev.cpp:669`                      |
| 977  | `Step 5: Read and verify all entries`                                                                       | `tests/test_log_dev.cpp:673`                      |
| 978  | `Step 6: restart and verify`                                                                                | `tests/test_log_dev.cpp:676`                      |
| 979  | `Step 7: Truncate 50 entries`                                                                               | `tests/test_log_dev.cpp:681`                      |
| 980  | `Step 8: restart and verify`                                                                                | `tests/test_log_dev.cpp:689`                      |
| 981  | `Restart homestore`                                                                                         | `tests/test_log_dev.cpp:788`                      |
| 982  | `Flip {} set`                                                                                               | `tests/btree_helpers/btree_test_helper.hpp:97`    |
| 983  | `Flip {} reset`                                                                                             | `tests/btree_helpers/btree_test_helper.hpp:101`   |
| 984  | `Preload Skipped`                                                                                           | `tests/btree_helpers/btree_test_helper.hpp:106`   |
| 985  | `Progress: iterations completed ({:.2f}%)- Elapsed time: {:.0f} seconds-`                                   | `tests/btree_helpers/btree_test_helper.hpp:143`   |
| 986  | `Preload Done`                                                                                              | `tests/btree_helpers/btree_test_helper.hpp:162`   |
| 987  | `{}{}`                                                                                                      | `tests/btree_helpers/btree_test_helper.hpp:482`   |
| 988  | `Failed to open file`                                                                                       | `tests/btree_helpers/btree_test_helper.hpp:490`   |
| 989  | `Mismatch in btree files`                                                                                   | `tests/btree_helpers/btree_test_helper.hpp:494`   |
| 990  | `Mismatch in btree files`                                                                                   | `tests/btree_helpers/btree_test_helper.hpp:508`   |
| 991  | `number of fibers {} num_iters_per_thread {} extra_iters {}`                                                | `tests/btree_helpers/btree_test_helper.hpp:554`   |
| 992  | `Progress: iterations completed ({:.2f}%)- Elapsed time: {:.0f} seconds of total`                           | `tests/btree_helpers/btree_test_helper.hpp:597`   |
| 993  | `ALL parallel jobs joined`                                                                                  | `tests/btree_helpers/btree_test_helper.hpp:615`   |
| 994  | `Saved shadow map to file: {}`                                                                              | `tests/btree_helpers/shadow_map.hpp:242`          |
| 995  | `http port = {}`                                                                                            | `tests/test_common/homestore_test_common.hpp:79`  |
| 996  | `random port generated = {}`                                                                                | `tests/test_common/homestore_test_common.hpp:88`  |
| 997  | `Set minimum chunk size {}`                                                                                 | `tests/test_common/homestore_test_common.hpp:233` |
| 998  | `sg_list of sg1 size: {} mismatch with sg2 size: {},`                                                       | `tests/test_common/homestore_test_common.hpp:318` |
| 999  | `sg_list num of iovs mismatch: sg1: {}, sg2: {}`                                                            | `tests/test_common/homestore_test_common.hpp:323` |
| 1000 | `iov_len of iov[{}] mismatch, sg1: {}, sg2: {}`                                                             | `tests/test_common/homestore_test_common.hpp:330` |
| 1001 | `memcmp return false for iovs[{}] between sg1 and sg2.`                                                     | `tests/test_common/homestore_test_common.hpp:335` |
| 1002 | `Taking input dev_list: {}`                                                                                 | `tests/test_common/homestore_test_common.hpp:396` |
| 1003 | `creating {} device files with each of size {}`                                                             | `tests/test_common/homestore_test_common.hpp:408` |
| 1004 | `Spdk with more than 2 threads will cause overburden test systems, changing nthreads to 2`                  | `tests/test_common/homestore_test_common.hpp:420` |
| 1005 | `Starting iomgr with {} threads, spdk: {}`                                                                  | `tests/test_common/homestore_test_common.hpp:424` |
| 1006 | `Initialize and start HomeStore with app_mem_size = {}`                                                     | `tests/test_common/homestore_test_common.hpp:435` |
| 1007 | `Successfully zeroed the 1st {} bytes of device {}`                                                         | `tests/test_common/homestore_test_common.hpp:527` |
| 1008 | `Repl dev init completed CB called`                                                                         | `tests/test_common/hs_repl_test_common.hpp:124`   |
| 1009 | `Device list from input={}`                                                                                 | `tests/test_common/hs_repl_test_common.hpp:176`   |
| 1010 | `Spawning Homestore replica={} instance`                                                                    | `tests/test_common/hs_repl_test_common.hpp:203`   |
| 1011 | `Starting Homestore replica={}`                                                                             | `tests/test_common/hs_repl_test_common.hpp:223`   |
| 1012 | `Stopping Homestore replica={}`                                                                             | `tests/test_common/hs_repl_test_common.hpp:240`   |
| 1013 | `Restarting Homestore replica={}`                                                                           | `tests/test_common/hs_repl_test_common.hpp:259`   |
| 1014 | `Replica={} has priority={}`                                                                                | `tests/test_common/hs_repl_test_common.hpp:322`   |
| 1015 | `Got listener for group_id={} replica={}`                                                                   | `tests/test_common/hs_repl_test_common.hpp:344`   |
| 1016 | `[Replica={}] start replace member out {} in {}`                                                            | `tests/test_common/raft_repl_test_base.hpp:367`   |
| 1017 | `[Replica={}] complete replace member out {} in {}`                                                         | `tests/test_common/raft_repl_test_base.hpp:373`   |
| 1018 | `[Replica={}] clean replace member task {} out {} in {}`                                                    | `tests/test_common/raft_repl_test_base.hpp:379`   |
| 1019 | `[Replica={}] remove member, member {}`                                                                     | `tests/test_common/raft_repl_test_base.hpp:384`   |
| 1020 | `not yet ready for traffic, waiting`                                                                        | `tests/test_common/raft_repl_test_base.hpp:415`   |
| 1021 | `Manually create snapshot got index {}`                                                                     | `tests/test_common/raft_repl_test_base.hpp:467`   |
| 1022 | `Manually truncated`                                                                                        | `tests/test_common/raft_repl_test_base.hpp:473`   |
| 1023 | `Truncation upper limit is {}`                                                                              | `tests/test_common/raft_repl_test_base.hpp:479`   |
| 1024 | `Waiting for repl dev to get destroyed`                                                                     | `tests/test_common/raft_repl_test_base.hpp:530`   |
| 1025 | `Writing on group_id={}`                                                                                    | `tests/test_common/raft_repl_test_base.hpp:547`   |
| 1026 | `Replica={} received {} commits but expected {}`                                                            | `tests/test_common/raft_repl_test_base.hpp:563`   |
| 1027 | `Replica={} has received {} commits as expected`                                                            | `tests/test_common/raft_repl_test_base.hpp:566`   |
| 1028 | `Switch the leader to replica_num = {}`                                                                     | `tests/test_common/raft_repl_test_base.hpp:578`   |
| 1029 | `Waiting for replica={} to become leader`                                                                   | `tests/test_common/raft_repl_test_base.hpp:597`   |
| 1030 | `Waiting for leader to be elected for group={}`                                                             | `tests/test_common/raft_repl_test_base.hpp:614`   |
| 1031 | `Waiting for leader to be elected`                                                                          | `tests/test_common/raft_repl_test_base.hpp:634`   |
| 1032 | `Writing {} entries since I am the leader my_uuid={}`                                                       | `tests/test_common/raft_repl_test_base.hpp:637`   |
| 1033 | `leader is not yet ready for traffic, waiting`                                                              | `tests/test_common/raft_repl_test_base.hpp:640`   |
| 1034 | `Run on worker threads to schedule append on repldev for {} Bytes.`                                         | `tests/test_common/raft_repl_test_base.hpp:646`   |
| 1035 | `{} entries were written on the leader_uuid={} my_uuid={}`                                                  | `tests/test_common/raft_repl_test_base.hpp:656`   |
| 1036 | `Waiting for leader to be elected`                                                                          | `tests/test_common/raft_repl_test_base.hpp:669`   |
| 1037 | `Writing data {} since I am the leader my_uuid={}`                                                          | `tests/test_common/raft_repl_test_base.hpp:680`   |
| 1038 | `Run on worker threads to schedule append on repldev for {} Bytes.`                                         | `tests/test_common/raft_repl_test_base.hpp:684`   |
| 1039 | `data_size larger than 0, go ahead, data_size= {}.`                                                         | `tests/test_common/raft_repl_test_base.hpp:690`   |
| 1040 | `wait_for_commit={}`                                                                                        | `tests/test_common/raft_repl_test_base.hpp:721`   |
| 1041 | `Restart homestore: replica_num = {}`                                                                       | `tests/test_common/raft_repl_test_base.hpp:753`   |
| 1042 | `Wait for replica={} to completely go down and removed from alive raft-groups`                              | `tests/test_common/raft_repl_test_base.hpp:757`   |
| 1043 | `Shutdown homestore: replica_num = {}`                                                                      | `tests/test_common/raft_repl_test_base.hpp:764`   |
| 1044 | `Wait for replica={} to completely go down and removed from alive raft-groups`                              | `tests/test_common/raft_repl_test_base.hpp:767`   |
| 1045 | `Start homestore: replica_num = {}`                                                                         | `tests/test_common/raft_repl_test_base.hpp:774`   |
| 1046 | `Start replace member task_id={}, out={}, in={}`                                                            | `tests/test_common/raft_repl_test_base.hpp:786`   |
| 1047 | `remove member, member={}`                                                                                  | `tests/test_common/raft_repl_test_base.hpp:814`   |
| 1048 | `Member {} already removed`                                                                                 | `tests/test_common/raft_repl_test_base.hpp:818`   |
| 1049 | `Got retry request, retrying remove_member for member={}`                                                   | `tests/test_common/raft_repl_test_base.hpp:823`   |
| 1050 | `flip learner to {}, member={}`                                                                             | `tests/test_common/raft_repl_test_base.hpp:833`   |
| 1051 | `clean replace member task, task_id={}`                                                                     | `tests/test_common/raft_repl_test_base.hpp:847`   |
| 1052 | `check replace member status, task_id={}, out={} in={}`                                                     | `tests/test_common/raft_repl_test_base.hpp:860`   |
| 1053 | `check replace member rollback result, task_id={}, out={} in={}`                                            | `tests/test_common/raft_repl_test_base.hpp:882`   |

### WARN (16)

**lib/** (7)

| # | Message                                                                                            | Source                                |
|---|----------------------------------------------------------------------------------------------------|---------------------------------------|
| 1 | `Homestore shutdown is called before init is completed`                                            | `lib/homestore.cpp:320`               |
| 2 | `try to remove invalid store_id {}-{}`                                                             | `lib/logstore/log_dev.cpp:774`        |
| 3 | `high watermark hit, used percentage: {}, high watermark percentage: {}`                           | `lib/common/resource_mgr.cpp:193`     |
| 4 | `interval_sec={} exceeds cleanup period ({}s) - time-based rate limiting may not work as expected` | `lib/common/homestore_assert.hpp:374` |
| 5 | `device size={} is not the multiple of physical page adjusted size to {}`                          | `lib/device/physical_dev.cpp:106`     |
| 6 | `Homestore is formatted with {} devices, but restarted with {} devices.`                           | `lib/device/device_manager.cpp:197`   |
| 7 | `Found a chunk id={}, which is expected to be part of vdev_id={}, but that vdev`                   | `lib/device/device_manager.cpp:591`   |

**tests/** (9)

| # | Message                                                                                  | Source                                            |
|---|------------------------------------------------------------------------------------------|---------------------------------------------------|
| 1 | `adjusted to min_io_size: {} and max_io_size: {} which must be multiple of blk_size: {}` | `tests/test_data_service.cpp:93`                  |
| 2 | `Tree did not split - need more entries. Skipping test.`                                 | `tests/test_index_btree.cpp:509`                  |
| 3 | `Root collapse did not occur - depth unchanged. Test may not verify the scenario.`       | `tests/test_index_btree.cpp:544`                  |
| 4 | `fail to purge gc index for chunk={}`                                                    | `tests/test_index_gc.cpp:178`                     |
| 5 | `Truncate issued upto {} but real upto lsn in log store is {}`                           | `tests/test_log_dev.cpp:233`                      |
| 6 | `Preload size={} is more than half of num_entries, setting preload_size to {}`           | `tests/btree_helpers/btree_test_helper.hpp:443`   |
| 7 | `Ignoring device_list as use_file is set to true`                                        | `tests/test_common/homestore_test_common.hpp:378` |
| 8 | `CrashSimulator::crash() is called - restarting homestore`                               | `tests/test_common/homestore_test_common.hpp:453` |
| 9 | `has already waited for repl dev to get destroyed for 10 times, so do a force leave`     | `tests/test_common/raft_repl_test_base.hpp:536`   |

### ERROR (62)

**lib/** (54)

| #  | Message                                                                                            | Source                                                |
|----|----------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| 1  | `root split failed btree name {}`                                                                  | `include/homestore/btree/btree.ipp:117`               |
| 2  | `check collapse read failed btree name {}`                                                         | `include/homestore/btree/btree.ipp:208`               |
| 3  | `Query type {} is not supported yet`                                                               | `include/homestore/btree/btree.ipp:260`               |
| 4  | `Validation failed for bnodeid: {} error: {}`                                                      | `include/homestore/btree/detail/btree_common.ipp:410` |
| 5  | `Exception during validation of node {}`                                                           | `include/homestore/index/index_table.hpp:128`         |
| 6  | `repair_root_node: skip invalid/unwritten buf {}`                                                  | `include/homestore/index/index_table.hpp:220`         |
| 7  | `repair_root_node: skip invalid/unwritten buf {}`                                                  | `include/homestore/index/index_table.hpp:226`         |
| 8  | `repair_root_node: skip unsafe edge repair for buf={} next_bnode={} ret={}`                        | `include/homestore/index/index_table.hpp:244`         |
| 9  | `set_root_from_committed_buf: reject invalid candidate {}`                                         | `include/homestore/index/index_table.hpp:277`         |
| 10 | `set_root_from_committed_buf: reject invalid candidate {}`                                         | `include/homestore/index/index_table.hpp:283`         |
| 11 | `set_root_from_committed_buf: candidate={} failed validation: {}`                                  | `include/homestore/index/index_table.hpp:290`         |
| 12 | `set_root_from_committed_buf: persisted root {} has level={} but SB depth={}`                      | `include/homestore/index/index_table.hpp:297`         |
| 13 | `repair_node: skip invalid/unwritten buf {}`                                                       | `include/homestore/index/index_table.hpp:352`         |
| 14 | `repair_node: skip buf {} whose persisted node_id={} does not match blkid={}`                      | `include/homestore/index/index_table.hpp:357`         |
| 15 | `Attempting to update superblk when it is already invalid`                                         | `include/homestore/index/index_table.hpp:498`         |
| 16 | `No devices provided to start homestore`                                                           | `lib/homestore.cpp:119`                               |
| 17 | `max_grpc_message_size {} is too small to hold max_data_size {}, max_snapshot_batch_size {} and`   | `lib/homestore.cpp:156`                               |
| 18 | `Total percentage of services on Device type {} is greater than 100.0f, total_pct_sum={}`          | `lib/homestore.cpp:216`                               |
| 19 | `No services are configured to be placed on any device type`                                       | `lib/homestore.cpp:223`                               |
| 20 | `Fast device is not configured but services are configured to be placed on fast device`            | `lib/homestore.cpp:229`                               |
| 21 | `last write is not completely written. footer magic {} footer start_log_idx {} header log indx {}` | `lib/logstore/log_stream.cpp:119`                     |
| 22 | `crc doesn't match {} log_dev={}`                                                                  | `lib/logstore/log_stream.cpp:140`                     |
| 23 | `Logdev not found to destroy {}`                                                                   | `lib/logstore/log_store_service.cpp:165`              |
| 24 | `Failed to read from Journal vdev log_dev={} {} {}`                                                | `lib/logstore/log_dev.cpp:313`                        |
| 25 | `Failed to read from Journal vdev log_dev={} {} {}`                                                | `lib/logstore/log_dev.cpp:343`                        |
| 26 | `Store Id {}-{} found but not opened yet, it will be discarded after logstore is started`          | `lib/logstore/log_dev.cpp:788`                        |
| 27 | `[type={}], total size read: {} mismatch from meta blk context_sz: {}`                             | `lib/meta/meta_blk_service.cpp:329`                   |
| 28 | `hs_compress_default indicates a failure trying to compress the data, ret: {}`                     | `lib/meta/meta_blk_service.cpp:686`                   |
| 29 | `[type={}], negative result: {} from decompress trying to decompress the`                          | `lib/meta/meta_blk_service.cpp:1181`                  |
| 30 | `Detected corrupted in-memory meta ssb: {}`                                                        | `lib/meta/meta_blk_service.cpp:1313`                  |
| 31 | `bid: {} not found in meta blk cache, corruption detected!`                                        | `lib/meta/meta_blk_service.cpp:1538`                  |
| 32 | `Can't serve this request, meta ssb is nullptr.`                                                   | `lib/meta/meta_blk_service.cpp:1594`                  |
| 33 | `Can't serve this request, in-memory meta ssb is not valid, : magic: {}, version: {},`             | `lib/meta/meta_blk_service.cpp:1600`                  |
| 34 | `dirty_buf_cnt {} of now is less then size {}`                                                     | `lib/common/resource_mgr.cpp:112`                     |
| 35 | `Crash simulation is ongoing; aid simulation by not flushing.`                                     | `lib/index/wb_cache.cpp:1089`                         |
| 36 | `ordinal {} is already reserved`                                                                   | `lib/index/index_service.cpp:79`                      |
| 37 | `ordinal {} doesn't exist`                                                                         | `lib/index/index_service.cpp:88`                      |
| 38 | `Error on async_write_zero: exception={}`                                                          | `lib/device/physical_dev.cpp:194`                     |
| 39 | `Creation of chunks failed because of space, removing {} partially created chunks`                 | `lib/device/physical_dev.cpp:300`                     |
| 40 | `nblks={} failed to alloc after trying to alloc on every chunks and devices`                       | `lib/device/virtual_dev.cpp:281`                      |
| 41 | `exception happened {}`                                                                            | `lib/device/virtual_dev.cpp:288`                      |
| 42 | `exception happened {}`                                                                            | `lib/device/virtual_dev.cpp:710`                      |
| 43 | `logdev not found log_dev={}`                                                                      | `lib/device/journal_vdev.cpp:198`                     |
| 44 | `tail {} less than start offset {} desc {}`                                                        | `lib/device/journal_vdev.cpp:561`                     |
| 45 | `Failed to read first block from device={}, error={}`                                              | `lib/device/device_manager.cpp:169`                   |
| 46 | `Found duplicate pdev: [{}] with count={}`                                                         | `lib/device/device_manager.cpp:184`                   |
| 47 | `Failed to read first block from device={}, error={}`                                              | `lib/device/device_manager.cpp:319`                   |
| 48 | `Failed to register listner, {} to watch file {}, Not monitoring cert files`                       | `lib/replication/service/raft_repl_service.cpp:265`   |
| 49 | `Failed to register listner, {} to watch file {}, Not monitoring cert files`                       | `lib/replication/service/raft_repl_service.cpp:270`   |
| 50 | `Simulate no space left on follower for testing purposes`                                          | `lib/replication/repl_dev/common.cpp:37`              |
| 51 | `Failed to initialize repl_req_ctx for destroying group, error={}`                                 | `lib/replication/repl_dev/raft_repl_dev.cpp:869`      |
| 52 | `RaftReplDev::destroy_group failed {}`                                                             | `lib/replication/repl_dev/raft_repl_dev.cpp:880`      |
| 53 | `chunk {} has no space left to serve request nblks: {}, available_blks: {}, actual`                | `lib/blkalloc/append_blk_allocator.cpp:77`            |
| 54 | `Can't serve request nblks: {} larger than max_blks_in_op: {}`                                     | `lib/blkalloc/append_blk_allocator.cpp:84`            |

**tests/** (8)

| # | Message                                                                                 | Source                                          |
|---|-----------------------------------------------------------------------------------------|-------------------------------------------------|
| 1 | `write offset already exists, off: 0x{}, size: {}, crc: 0x{}`                           | `tests/test_journal_vdev.cpp:327`               |
| 2 | `truncate_watermark_percentage need to be between [5, 95], change to defaut value: 80`  | `tests/test_journal_vdev.cpp:642`               |
| 3 | `CP3 TIMEOUT, CP did not complete within timeout!`                                      | `tests/test_index_btree.cpp:562`                |
| 4 | `Failed to query blobs after purging reserved chunk={} in gc index table, index ret={}` | `tests/test_index_gc.cpp:191`                   |
| 5 | `gc index table is not empty for chunk={} after purging, valid_blob_indexes.size={}`    | `tests/test_index_gc.cpp:197`                   |
| 6 | `Failed to get last_truncate_log_idx from logdev status for logdev_id {}`               | `tests/test_log_dev.cpp:251`                    |
| 7 | `Failed to get current_log_idx from logdev status for logdev_id {}`                     | `tests/test_log_dev.cpp:258`                    |
| 8 | `Failed to remove key {}`                                                               | `tests/btree_helpers/btree_test_helper.hpp:232` |

### CRITICAL (6)

**tests/** (6)

| # | Message                                                                                              | Source                                  |
|---|------------------------------------------------------------------------------------------------------|-----------------------------------------|
| 1 | `Unexpected out_of_range exception for lsn={}:{} upto {} trunc_upto {}`                              | `tests/test_log_store_long_run.cpp:162` |
| 2 | `num_logstores {} should be greater or equal than num_logdevs {} to make sure there is at least one` | `tests/test_log_store_long_run.cpp:598` |
| 3 | `Unexpected out_of_range exception for lsn={}:{}`                                                    | `tests/test_log_store.cpp:218`          |
| 4 | ``                                                                                                   | `tests/test_log_store.cpp:270`          |
| 5 | `Caught exception e {}`                                                                              | `tests/test_log_store.cpp:298`          |
| 6 | `Failed to read at upto {} lsn {} trunc_upto {}`                                                     | `tests/test_log_dev.cpp:217`            |
