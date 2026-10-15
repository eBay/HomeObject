# Shard Lifecycle Control

**Status**: Proposed
**Date**: 2026-10-13
---

## Context and Problem Statement

In the initial design, DM was responsible for creating and sealing shards, and storing shard information in CM. When a
GW went down, DM would reclaim the shards held by the GW. Specifically, it would obtain shard information from CM and
then send a seal shard request to SM. However, we found that CM might lose some shard information under high
concurrency, leading to shards never being reclaimed and the chunk space used by the shards never being reused.
Therefore, SM should control the shard lifecycle and seal idle open shards autonomously.

## Proposal

SM will seal open shards that have not experienced write traffic for a long period, thereby preventing space waste.
The PG leader is responsible for deciding when to seal shards.

Once per scan interval, SM serially scans all PGs. Only the PG leader evaluates idle shards and proposes seal
operations.

**Details**

Leader's Responsibilities:

1. Scanning for Expired Shards:
    - Triggered by a new thread timer in `GCManager`
    - Scan by PG's in-memory last-write timestamp index
    - For each expired shard:
        - skip if the shard's chunk is in `ChunkState::GC`
          ( add `HeapChunkSelector::is_chunk_in_gc_state(pg_id, v_chunk_id)`;
        - skip if a seal is already in-flight for this shard
          (add a per-PG `boost::concurrent_flat_set<shard_id_t> pending_auto_seals_`, inserted after propose succeeds,
          cleared on commit/rollback)
    - Seal expired shards one at a time, fire-and-forget via existing `SEAL_SHARD_MSG`; recheck leadership immediately
      before proposing

2. Expired Shard Identification
    - Identifies open shards with no `put_blob` commit for longer than `shard_idle_timeout_sec`, based on the in-memory
      last-write timestamp
        - Write traffic through a GW-held shard indicates the GW is still active;
        - delete traffic does not, since deletes are routed by blob and may not imply GW liveness.
    - Add new configuration `shard_idle_timeout_sec`: idle threshold before auto-seal. Tune based on Gateway TPS if
      known; otherwise default to one week without write traffic (604800 seconds)
    - Add new configuration `shard_scan_interval_sec`: how often to run a full scan. Default to once per day (86400
      seconds)

All Members' Responsibilities:

1. In-Memory Last-Write Timestamp Index
    - Add a per-PG in-memory index on `HS_PG`:
      `boost::concurrent_flat_map<shard_id, last_write_time>`
    - Update the index on `put_blob` commit only; add on shard creation; remove on shard seal / delete
    - On SM boot, initialize index entries for all loaded open shards to `boot_timestamp`
2. Normal Seal Handling:
    - Reuse existing single-shard `seal_shard` flow

**Corner cases:**

Write Requests During Auto-Seal:

- Before shard sealing: double-check last write time and only seal shard if the index has not been updated by a recent
  `put_blob` commit.
- After shard sealing: Similar to user send requests on a sealed shard, the put request will be rejected and GW will
  attempt to create a new shard for the blob, and the delete request will process successfully
- During shard sealing: put requests with a smaller LSN are accepted; requests with a larger LSN are rejected.
  Same behavior as concurrent GW put and seal operations.

Raft failure:

- Hanging requests: since `seal_shard` is a Raft request, it may hang without quorum and stall shutdown. Mitigation: use
  asynchronous sealing (do not wait for log commit on the scan thread)
- Leader switch: may cause double-sealing and rollback issues. Corner case:
    - T1: scan finds shard S1 expired
    - T2: GW seals S1 with LSN 10 (committed)
    - T3: auto-seal proposes S1 again with LSN 20
    - T4: rollback of LSN 20 reverts S1 to OPEN, but S1 should remain SEALED
      update: does not exist: `SEAL_SHARD_MSG` pre_commit is a no-op, so the rollback handler has nothing to revert.
      State transitions only happen at commit. Double-sealing is addressed by the idempotency fix below.
- Best-effort sealing: auto-seal does not wait for commit. Under extreme conditions (slow Raft, leader switch), a seal
  proposed on one scan may still be in flight when the next scan proposes the same shard again. Seal commit must be
  idempotent: if `sealed_lsn` is already set, skip `release_chunk` and do not change `sealed_lsn` and :
  A in-flight dedup set (`pending_auto_seals_`) prevents re-proposing the same shard.

Seal vs GC:

- Auto-seal operates on OPEN shards; normal GC runs on SEALED shards — no overlap.
- For emergent GC: OPEN shards hitting `no_space_left` have recent write pressure and are unlikely to be selected for
  auto-seal.
    - If writes remain stuck (e.g. emergent GC blocked) longer than `shard_idle_timeout_sec`, the
      auto-seal scan need to skip those shards depends on `ChunkState::GC`.
- Could GC start after a shard is sealed, then a leader switch rolls back the seal, and new blobs written to that shard
  during GC cause data loss? **No.**

Seal vs BR (baseline resync):

- **Leader:** baseline resync sends whatever shard state the leader currently holds. Auto-seal uses the normal Raft path
  (`SEAL_SHARD_MSG`); no additional BR-specific handling is required.
- **Follower:** applies whatever the leader sends; independent of auto-seal. When processing a shard snapshot, refresh
  the follower's last-write timestamp index entry for that shard.

## Target Release

Implementation changes are in HomeObject; SM picks up the dependency in **SM 0.5.8**.