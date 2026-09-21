# Blob Operation Log Analysis

**Log mode:**

```
base:debug,cm_client:debug,pg_svc:info,shard_svc:info,data_svc:info,homeobject:info,blobmgr:debug,shardmgr:debug,wbcache:info,btree:info,nuraft_mesg:info,replication:trace,logstore:info
```

## Effective Level Per Module

| Module        | Level | Effect                                 |
|---------------|-------|----------------------------------------|
| `homeobject`  | INFO  | TRACE and DEBUG suppressed             |
| `blobmgr`     | DEBUG | TRACE suppressed; DEBUG and above pass |
| `replication` | TRACE | All levels pass                        |

---

## Log Line Size Estimation Methodology

Each log line includes a structured header emitted by the sisl framework:

```
2024-01-15 12:34:56.789012 [replication] [TRACE] [thread-id] message...
```

Typical header overhead: **~80 bytes**. Message body varies by the number and length of format arguments. Estimates
below are conservative round-numbers based on actual format strings and typical field widths (UUIDs ~36 chars, blkids ~
20 chars, RD rreq dumps ~80 chars).

**BLOG macro prefix** (added to every blobmgr log line):

```
[traceID=<uuid>,shardID=0x<16-hex>,pg=<N>,shard=0x<16-hex>,blob=<N>]
```

Prefix length: ~80 bytes.

---

## PUT Blob

### Leader Path

Call chain: `_put_blob` → `async_alloc_write` → `push_data_to_all_followers` (×N
followers) → `propose_to_raft` → `pre_commit_ext` → `commit_ext` → `handle_commit` → `on_commit` → `on_blob_put_commit` → `local_add_blob_info`

| #  | File                     | Line | Module        | Level | Format string (condensed)                                                               | Est. size |
|----|--------------------------|------|---------------|-------|-----------------------------------------------------------------------------------------|-----------|
| 1  | `hs_blob_manager.cpp`    | 121  | `blobmgr`     | DEBUG | `[traceID=…] Blob Put request: pg={}, group={}, shard=0x{:x}, length={}`                | ~280 B    |
| 2  | `raft_repl_dev.cpp`      | 1035 | `replication` | DEBUG | `repl_key [{}], header size [{}] bytes, user_key size [{}] bytes, data size [{}] bytes` | ~300 B    |
| 3  | `raft_state_machine.cpp` | 37   | `replication` | TRACE | `Raft Channel: propose journal_entry=[{}]`                                              | ~270 B    |
| 4  | `raft_repl_dev.cpp`      | 1117 | `replication` | DEBUG | `Data Channel: Pushing data to follower {}, rreq=[{}]` (×2 for 3-node)                  | ~250 B ×2 |
| 5  | `raft_repl_dev.cpp`      | 1133 | `replication` | DEBUG | `Data Channel: Data push completed for rreq=[{}]`                                       | ~220 B    |
| 6  | `hs_blob_manager.cpp`    | 553  | `blobmgr`     | DEBUG | `[traceID=…] Picked p_chunk_id={}, reserved_blks={}`                                    | ~220 B    |
| 7  | `raft_state_machine.cpp` | 211  | `replication` | TRACE | `Precommit rreq=[{}]`                                                                   | ~200 B    |
| 8  | `raft_state_machine.cpp` | 227  | `replication` | TRACE | `Raft channel: Received Commit message rreq=[{}]`                                       | ~220 B    |
| 9  | `raft_repl_dev.cpp`      | 1712 | `replication` | DEBUG | `Raft channel: Commit rreq=[{}]`                                                        | ~210 B    |
| 10 | `raft_state_machine.cpp` | 325  | `replication` | TRACE | `Raft channel: erase lsn {}, rreq {}`                                                   | ~200 B    |
| 11 | `hs_blob_manager.cpp`    | 209  | `blobmgr`     | DEBUG | `[traceID=…] Blob Put request: Put blob success blkid={}`                               | ~210 B    |

**Suppressed on leader** (level too low):

- `hs_blob_manager.cpp:195` BLOGT "Put blob: header={} sgs={}" — blobmgr:TRACE
- `hs_blob_manager.cpp:224` BLOGT "blob put commit, exist_already=…" — blobmgr:TRACE
- `hs_blob_manager.cpp:280` LOGTRACEMOD(blobmgr) "blob put commit lsn=…" — blobmgr:TRACE
- `replication_state_machine.cpp:26` LOGT "applying raft log commit…" — homeobject:TRACE
- `replication_state_machine.cpp:101` LOGT "on_pre_commit…" — homeobject:TRACE

**Leader total (3-node cluster, 2 followers): ~2,830 B ≈ 2.8 KB**

---

### Follower Path

Call chain: APPEND_ENTRIES
callback → `localize_journal_entry_prepare` → `applier_create_req` → `blob_put_get_blk_alloc_hints` → wait for data
write → `on_push_data_received` → `pre_commit_ext` → `commit_ext` → `handle_commit`

| #  | File                     | Line | Module        | Level | Format string (condensed)                                                                                                                                         | Est. size |
|----|--------------------------|------|---------------|-------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| 1  | `raft_repl_dev.cpp`      | 2434 | `replication` | TRACE | `Raft channel: Received {} append entries on follower from leader, term {}, lsn {} ~ {}, my committed lsn {}, leader committed lsn {}`                            | ~280 B    |
| 2  | `raft_state_machine.cpp` | 62   | `replication` | TRACE | `Raft Channel: Localizing Raft log_entry: server_id={}, term={}, journal_entry=[{}]`                                                                              | ~270 B    |
| 3  | `hs_blob_manager.cpp`    | 553  | `blobmgr`     | DEBUG | `[traceID=…] Picked p_chunk_id={}, reserved_blks={}`                                                                                                              | ~220 B    |
| 4  | `raft_repl_dev.cpp`      | 1288 | `replication` | DEBUG | `in follower_create_req: rreq={}, addr=0x{:x}`                                                                                                                    | ~200 B    |
| 5  | `raft_repl_dev.cpp`      | 1167 | `replication` | DEBUG | `Data Channel: PushData received: time diff={} ms.`                                                                                                               | ~200 B    |
| 6  | `raft_repl_dev.cpp`      | 1232 | `replication` | DEBUG | `Data Channel: Data write completed for rreq=[{}], time_diff_data_log_us={}, data_write_latency_us={}, total_data_write_latency_us={}, local_blkid.num_pieces={}` | ~350 B    |
| 7  | `raft_state_machine.cpp` | 211  | `replication` | TRACE | `Precommit rreq=[{}]`                                                                                                                                             | ~200 B    |
| 8  | `raft_state_machine.cpp` | 227  | `replication` | TRACE | `Raft channel: Received Commit message rreq=[{}]`                                                                                                                 | ~220 B    |
| 9  | `raft_repl_dev.cpp`      | 1712 | `replication` | DEBUG | `Raft channel: Commit rreq=[{}]`                                                                                                                                  | ~210 B    |
| 10 | `raft_state_machine.cpp` | 325  | `replication` | TRACE | `Raft channel: erase lsn {}, rreq {}`                                                                                                                             | ~200 B    |

**Follower total: ~2,350 B ≈ 2.4 KB**

---

## GET Blob

GET is a pure local read — no raft replication, no follower path.

Call chain: `_get_blob` → `_get_blob_data` (full) or `_get_blob_data_partial` (partial offset/len)

### Full Read

| # | File                  | Line | Module    | Level | Format string (condensed)                                                                 | Est. size |
|---|-----------------------|------|-----------|-------|-------------------------------------------------------------------------------------------|-----------|
| 1 | `hs_blob_manager.cpp` | 380  | `blobmgr` | DEBUG | `[traceID=…] Blob Get request: pg={}, group={}, shard=0x{:x}, blob={}, offset={}, len={}` | ~280 B    |
| 2 | `hs_blob_manager.cpp` | 418  | `blobmgr` | DEBUG | `[traceID=…] Reading from blkid={} to buf={}`                                             | ~200 B    |
| 3 | `hs_blob_manager.cpp` | 445  | `blobmgr` | DEBUG | `[traceID=…] Blob get success: blkid={}`                                                  | ~180 B    |

**Full GET total: ~660 B**

### Partial Read (non-zero offset or len < blob size)

| # | File                  | Line | Module    | Level | Format string (condensed)                                                                                      | Est. size |
|---|-----------------------|------|-----------|-------|----------------------------------------------------------------------------------------------------------------|-----------|
| 1 | `hs_blob_manager.cpp` | 380  | `blobmgr` | DEBUG | `[traceID=…] Blob Get request: pg={}, group={}, shard=0x{:x}, blob={}, offset={}, len={}`                      | ~280 B    |
| 2 | `hs_blob_manager.cpp` | 479  | `blobmgr` | DEBUG | `[traceID=…] Reading partial data: offset={}, len={}, full_blkid={}, read_blkid={}, start_blk={}, num_blks={}` | ~280 B    |
| 3 | `hs_blob_manager.cpp` | 503  | `blobmgr` | DEBUG | `[traceID=…] Blob partial get success: blkid={}`                                                               | ~180 B    |

**Partial GET total: ~740 B**

---

## DELETE Blob

DELETE carries zero data bytes (`data.size == 0`, `HS_DATA_INLINED`). This means:

- No `push_data_to_all_followers` path (no block data to push)
- No `blob_put_get_blk_alloc_hints` call → no "Picked p_chunk_id" log
- No `on_push_data_received` / "Data write completed" on follower
- `on_blob_del_commit` happy-path `LOGD` suppressed by `homeobject:info`

### Leader Path

Call chain: `_del_blob` → `async_alloc_write` → `propose_to_raft` (skips data
channel) → `pre_commit_ext` → `commit_ext` → `handle_commit` → `on_commit` → `on_blob_del_commit`

| # | File                     | Line | Module        | Level | Format string (condensed)                                                              | Est. size |
|---|--------------------------|------|---------------|-------|----------------------------------------------------------------------------------------|-----------|
| 1 | `hs_blob_manager.cpp`    | 596  | `blobmgr`     | DEBUG | `[traceID=…] Blob Delete request: pg={}, group={}, Shard={}, Blob={}`                  | ~240 B    |
| 2 | `raft_repl_dev.cpp`      | 1035 | `replication` | DEBUG | `repl_key [{}], header size [{}] bytes, user_key size [{}] bytes, data size [0] bytes` | ~300 B    |
| 3 | `raft_repl_dev.cpp`      | 1091 | `replication` | TRACE | `Skipping data channel send since value size is 0`                                     | ~180 B    |
| 4 | `raft_state_machine.cpp` | 37   | `replication` | TRACE | `Raft Channel: propose journal_entry=[{}]`                                             | ~270 B    |
| 5 | `raft_state_machine.cpp` | 211  | `replication` | TRACE | `Precommit rreq=[{}]`                                                                  | ~200 B    |
| 6 | `raft_state_machine.cpp` | 227  | `replication` | TRACE | `Raft channel: Received Commit message rreq=[{}]`                                      | ~220 B    |
| 7 | `raft_repl_dev.cpp`      | 1712 | `replication` | DEBUG | `Raft channel: Commit rreq=[{}]`                                                       | ~210 B    |
| 8 | `raft_state_machine.cpp` | 325  | `replication` | TRACE | `Raft channel: erase lsn {}, rreq {}`                                                  | ~200 B    |

**Suppressed on leader** (level too low):

- `hs_blob_manager.cpp:582` BLOGT "deleting blob" — blobmgr:TRACE
- `hs_blob_manager.cpp:635` BLOGT "Delete blob successful" — blobmgr:TRACE
- `hs_blob_manager.cpp:686` LOGD "shard_id={}, blob_id={} has been moved to tombstone, lsn={}" — homeobject:DEBUG,
  filtered by homeobject:info

**Leader total: ~1,820 B ≈ 1.8 KB**

---

### Follower Path

Call chain: APPEND_ENTRIES
callback → `localize_journal_entry_prepare` → `applier_create_req` → `pre_commit_ext` → `commit_ext` → `handle_commit`

(No PushData path: inline data means follower gets everything through the raft log channel only)

| # | File                     | Line | Module        | Level | Format string (condensed)                                                                                                              | Est. size |
|---|--------------------------|------|---------------|-------|----------------------------------------------------------------------------------------------------------------------------------------|-----------|
| 1 | `raft_repl_dev.cpp`      | 2434 | `replication` | TRACE | `Raft channel: Received {} append entries on follower from leader, term {}, lsn {} ~ {}, my committed lsn {}, leader committed lsn {}` | ~280 B    |
| 2 | `raft_state_machine.cpp` | 62   | `replication` | TRACE | `Raft Channel: Localizing Raft log_entry: server_id={}, term={}, journal_entry=[{}]`                                                   | ~270 B    |
| 3 | `raft_repl_dev.cpp`      | 1288 | `replication` | DEBUG | `in follower_create_req: rreq={}, addr=0x{:x}`                                                                                         | ~200 B    |
| 4 | `raft_state_machine.cpp` | 211  | `replication` | TRACE | `Precommit rreq=[{}]`                                                                                                                  | ~200 B    |
| 5 | `raft_state_machine.cpp` | 227  | `replication` | TRACE | `Raft channel: Received Commit message rreq=[{}]`                                                                                      | ~220 B    |
| 6 | `raft_repl_dev.cpp`      | 1712 | `replication` | DEBUG | `Raft channel: Commit rreq=[{}]`                                                                                                       | ~210 B    |
| 7 | `raft_state_machine.cpp` | 325  | `replication` | TRACE | `Raft channel: erase lsn {}, rreq {}`                                                                                                  | ~200 B    |

**Follower total: ~1,580 B ≈ 1.6 KB**

---

## CREATE Shard

CREATE_SHARD is a **log-only** operation — it passes empty data (`sisl::sg_list{}`) to `async_alloc_write`,
so `op_code = HS_DATA_INLINED`. This means:

- No data channel push to followers (same as DELETE)
- No block allocation → `get_blk_alloc_hints` not called → no "Picked p_chunk_id" log
- `SLOG` macro prefix: `[trace_id={},shardID=0x{:x},pg={},shard=0x{:x}]` (~80 B overhead, routes to `shardmgr`)

### Leader Path

Call
chain: `_create_shard` → `async_alloc_write` → `propose_to_raft` → `pre_commit_ext` → `commit_ext` → `handle_commit` → `on_commit` → `on_shard_message_commit` (
CREATE_SHARD branch)

| #  | File                     | Line | Module        | Level | Format string (condensed)                                                              | Est. size |
|----|--------------------------|------|---------------|-------|----------------------------------------------------------------------------------------|-----------|
| 1  | `hs_shard_manager.cpp`   | 199  | `shardmgr`    | DEBUG | `[trace_id=…] Create shard request: pg={}, size={}`                                    | ~240 B    |
| 2  | `hs_shard_manager.cpp`   | 230  | `shardmgr`    | DEBUG | `[trace_id=…] vchunk_id={}`                                                            | ~180 B    |
| 3  | `raft_repl_dev.cpp`      | 1035 | `replication` | DEBUG | `repl_key [{}], header size [{}] bytes, user_key size [{}] bytes, data size [0] bytes` | ~300 B    |
| 4  | `raft_repl_dev.cpp`      | 1091 | `replication` | TRACE | `Skipping data channel send since value size is 0`                                     | ~180 B    |
| 5  | `raft_state_machine.cpp` | 37   | `replication` | TRACE | `Raft Channel: propose journal_entry=[{}]`                                             | ~270 B    |
| 6  | `raft_state_machine.cpp` | 211  | `replication` | TRACE | `Precommit rreq=[{}]`                                                                  | ~200 B    |
| 7  | `raft_state_machine.cpp` | 227  | `replication` | TRACE | `Raft channel: Received Commit message rreq=[{}]`                                      | ~220 B    |
| 8  | `hs_shard_manager.cpp`   | 513  | `shardmgr`    | DEBUG | `[trace_id=…] Commit done for creating shard`                                          | ~180 B    |
| 9  | `raft_repl_dev.cpp`      | 1712 | `replication` | DEBUG | `Raft channel: Commit rreq=[{}]`                                                       | ~210 B    |
| 10 | `raft_state_machine.cpp` | 325  | `replication` | TRACE | `Raft channel: erase lsn {}, rreq {}`                                                  | ~200 B    |
| 11 | `hs_shard_manager.cpp`   | 287  | `shardmgr`    | DEBUG | `[trace_id=…] Shard created success.`                                                  | ~160 B    |

**Suppressed on leader** (level too low):

- `replication_state_machine.cpp:26` LOGT "applying raft log commit…" — homeobject:TRACE
- `replication_state_machine.cpp:101` LOGT "on_pre_commit with lsn={}, msg type={}" — homeobject:TRACE (only reached for
  HS_DATA_LINKED/INLINED ops; shard is INLINED so this path is hit but filtered)

**Leader total: ~2,340 B ≈ 2.3 KB**

---

### Follower Path

Call chain: APPEND_ENTRIES callback → `localize_journal_entry_prepare` → `applier_create_req` (no `alloc_local_blks`
since `HS_DATA_INLINED`) → `pre_commit_ext` → `commit_ext` → `handle_commit` → `on_shard_message_commit`

| # | File                     | Line | Module        | Level | Format string (condensed)                                                                                                              | Est. size |
|---|--------------------------|------|---------------|-------|----------------------------------------------------------------------------------------------------------------------------------------|-----------|
| 1 | `raft_repl_dev.cpp`      | 2434 | `replication` | TRACE | `Raft channel: Received {} append entries on follower from leader, term {}, lsn {} ~ {}, my committed lsn {}, leader committed lsn {}` | ~280 B    |
| 2 | `raft_state_machine.cpp` | 62   | `replication` | TRACE | `Raft Channel: Localizing Raft log_entry: server_id={}, term={}, journal_entry=[{}]`                                                   | ~270 B    |
| 3 | `raft_repl_dev.cpp`      | 1288 | `replication` | DEBUG | `in follower_create_req: rreq={}, addr=0x{:x}`                                                                                         | ~200 B    |
| 4 | `raft_state_machine.cpp` | 211  | `replication` | TRACE | `Precommit rreq=[{}]`                                                                                                                  | ~200 B    |
| 5 | `raft_state_machine.cpp` | 227  | `replication` | TRACE | `Raft channel: Received Commit message rreq=[{}]`                                                                                      | ~220 B    |
| 6 | `hs_shard_manager.cpp`   | 513  | `shardmgr`    | DEBUG | `[trace_id=…] Commit done for creating shard`                                                                                          | ~180 B    |
| 7 | `raft_repl_dev.cpp`      | 1712 | `replication` | DEBUG | `Raft channel: Commit rreq=[{}]`                                                                                                       | ~210 B    |
| 8 | `raft_state_machine.cpp` | 325  | `replication` | TRACE | `Raft channel: erase lsn {}, rreq {}`                                                                                                  | ~200 B    |

Note: No PushData path (log-only), no block allocation, no "Picked p_chunk_id".

**Follower total: ~1,760 B ≈ 1.8 KB**

---

## SEAL Shard

SEAL_SHARD has identical replication structure to CREATE_SHARD (log-only, `HS_DATA_INLINED`, no data push, no block
allocation). The only differences are in the `shardmgr`-layer log messages.

### Leader Path

Call chain: `_seal_shard` → `async_alloc_write` → `propose_to_raft` → `pre_commit_ext` → `on_shard_message_pre_commit` (
noop in happy path) → `commit_ext` → `handle_commit` → `on_commit` → `on_shard_message_commit` (SEAL_SHARD
branch) → `flush_durable_commit_lsn` + state update

| #  | File                     | Line | Module        | Level | Format string (condensed)                                                              | Est. size |
|----|--------------------------|------|---------------|-------|----------------------------------------------------------------------------------------|-----------|
| 1  | `hs_shard_manager.cpp`   | 302  | `shardmgr`    | DEBUG | `[trace_id=…] Seal shard request: is_open={}`                                          | ~220 B    |
| 2  | `raft_repl_dev.cpp`      | 1035 | `replication` | DEBUG | `repl_key [{}], header size [{}] bytes, user_key size [{}] bytes, data size [0] bytes` | ~300 B    |
| 3  | `raft_repl_dev.cpp`      | 1091 | `replication` | TRACE | `Skipping data channel send since value size is 0`                                     | ~180 B    |
| 4  | `raft_state_machine.cpp` | 37   | `replication` | TRACE | `Raft Channel: propose journal_entry=[{}]`                                             | ~270 B    |
| 5  | `raft_state_machine.cpp` | 211  | `replication` | TRACE | `Precommit rreq=[{}]`                                                                  | ~200 B    |
| 6  | `raft_state_machine.cpp` | 227  | `replication` | TRACE | `Raft channel: Received Commit message rreq=[{}]`                                      | ~220 B    |
| 7  | `hs_shard_manager.cpp`   | 574  | `shardmgr`    | DEBUG | `[trace_id=…] Commit done for sealing shard at lsn={}`                                 | ~200 B    |
| 8  | `raft_repl_dev.cpp`      | 1712 | `replication` | DEBUG | `Raft channel: Commit rreq=[{}]`                                                       | ~210 B    |
| 9  | `raft_state_machine.cpp` | 325  | `replication` | TRACE | `Raft channel: erase lsn {}, rreq {}`                                                  | ~200 B    |
| 10 | `hs_shard_manager.cpp`   | 378  | `shardmgr`    | DEBUG | `[trace_id=…] Seal shard request: Shard sealed success, is_open={}`                    | ~220 B    |

**Leader total: ~2,220 B ≈ 2.2 KB**

---

### Follower Path

Same replication skeleton as CREATE_SHARD follower. `on_shard_message_pre_commit` for SEAL_SHARD checks header CRC and
returns true with no additional logging in the happy path.

| # | File                     | Line | Module        | Level | Format string (condensed)                                                            | Est. size |
|---|--------------------------|------|---------------|-------|--------------------------------------------------------------------------------------|-----------|
| 1 | `raft_repl_dev.cpp`      | 2434 | `replication` | TRACE | `Raft channel: Received {} append entries on follower...`                            | ~280 B    |
| 2 | `raft_state_machine.cpp` | 62   | `replication` | TRACE | `Raft Channel: Localizing Raft log_entry: server_id={}, term={}, journal_entry=[{}]` | ~270 B    |
| 3 | `raft_repl_dev.cpp`      | 1288 | `replication` | DEBUG | `in follower_create_req: rreq={}, addr=0x{:x}`                                       | ~200 B    |
| 4 | `raft_state_machine.cpp` | 211  | `replication` | TRACE | `Precommit rreq=[{}]`                                                                | ~200 B    |
| 5 | `raft_state_machine.cpp` | 227  | `replication` | TRACE | `Raft channel: Received Commit message rreq=[{}]`                                    | ~220 B    |
| 6 | `hs_shard_manager.cpp`   | 574  | `shardmgr`    | DEBUG | `[trace_id=…] Commit done for sealing shard at lsn={}`                               | ~200 B    |
| 7 | `raft_repl_dev.cpp`      | 1712 | `replication` | DEBUG | `Raft channel: Commit rreq=[{}]`                                                     | ~210 B    |
| 8 | `raft_state_machine.cpp` | 325  | `replication` | TRACE | `Raft channel: erase lsn {}, rreq {}`                                                | ~200 B    |

**Follower total: ~1,780 B ≈ 1.8 KB**

---

## Summary

| Operation          | Role            | Log lines                                                | Est. disk/op |
|--------------------|-----------------|----------------------------------------------------------|--------------|
| PUT blob           | Leader (3-node) | 12 lines (11 unique + 1 duplicate for 2nd follower push) | **~2.8 KB**  |
| PUT blob           | Follower        | 10 lines                                                 | **~2.4 KB**  |
| GET blob (full)    | Local only      | 3 lines                                                  | **~0.7 KB**  |
| GET blob (partial) | Local only      | 3 lines                                                  | **~0.7 KB**  |
| DELETE blob        | Leader          | 8 lines                                                  | **~1.8 KB**  |
| DELETE blob        | Follower        | 7 lines                                                  | **~1.6 KB**  |
| CREATE shard       | Leader          | 11 lines                                                 | **~2.3 KB**  |
| CREATE shard       | Follower        | 8 lines                                                  | **~1.8 KB**  |
| SEAL shard         | Leader          | 10 lines                                                 | **~2.2 KB**  |
| SEAL shard         | Follower        | 8 lines                                                  | **~1.8 KB**  |

> **Cluster-wide cost per operation** (leader + 2 followers):
> - PUT: 2.8 KB + 2 × 2.4 KB = **~7.6 KB**
> - GET: **~0.7 KB** (local only, no replication)
> - DELETE: 1.8 KB + 2 × 1.6 KB = **~5.0 KB**
> - CREATE shard: 2.3 KB + 2 × 1.8 KB = **~5.9 KB**
> - SEAL shard: 2.2 KB + 2 × 1.8 KB = **~5.8 KB**
