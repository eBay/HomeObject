# Blob Scrub Range Coverage Strategy

**Status**: Proposed  
**Date**: 2026-04-11  
**Proposer**: Jie

---

## Context

After completing PG meta and shard-level scrub, the scrub manager must perform a full coverage scan of all blobs in a PG.

### System Constants

| Parameter | Value |
|-----------|-------|
| Chunk size | 4 GB |
| Minimum blob size | 8 KB |
| Max blobs per shard | 4 GB ÷ 8 KB = **524,288** |
| Blob scrub result per blob | blob_id(8B) + shard_id(8B) + result(1B) + hash(32B) = **49 B** |
| Max in-memory result for one shard (3 replicas) | 49 × 524,288 × 3 ≈ **77.1 MB** |
| Typical HDD random IOPS (foreground active) | ~100 |
| Worst-case full-IOPS scrub time (max shard) | 524,288 ÷ 100 ≈ **5,243 s** |
| Worst-case scrub time at 10 % IOPS budget | 5,243 ÷ 0.1 = **52,430 s ≈ 14.6 h** |

### Index Structure

All entries in the index table are keyed as `{shard_id, blob_id}`, sorted with `shard_id` as the primary field. A range query that specifies a `shard_id` prefix can seek directly to that shard's start position. A range query by `blob_id` alone must scan the full index tree.

### The Three Scrub Constraints

1. **Coverage completeness** — every blob whose `blob_id ≤ last_blob_id` at scrub start must be visited; no silent omissions.
2. **Single-RPC latency** — each scrub request must complete within a normal RPC timeout; a single request cannot block on reading up to 4 GB of random disk data.
3. **IOPS fairness** — scrub must consume at most ~10 % of HDD random I/O capacity so foreground traffic is not degraded.

### Additional Engineering Invariants

- **Coverage must not depend on the leader's data integrity.** If the leader has lost blobs in a shard, its view of that shard's blob count is too low. A scrub range derived solely from the leader's local data will silently miss those blobs on followers.
- **The leader cannot buffer the full three-replica result set before comparing.** A full PG filled with minimum-size blobs produces 128M blobs × 49 B × 3 replicas ≈ 18.8 GB — far beyond any node's acceptable working memory for scrub.

---

## Problem Statement

**How to partition the `{shard_id, blob_id}` key space into scrub requests such that:**
- Every committed blob is covered regardless of the leader's own data state.
- Each request completes within a bounded latency window.
- Disk IOPS consumption stays within the configured budget.
- Leader peak memory during result comparison stays within a few tens of MB.

---

## Alternatives Considered

### Option A: Static blob_id Equal-Width Slicing (current implementation)

Partition `[0, last_blob_id]` into fixed-width intervals of size N. Issue one RPC per interval. The current code uses N = HDD\_IOPS × (timeout / 2) = 200 × 5 = 1,000 blobs.

**Implementation** (`scrub_manager.cpp:handle_pg_scrub_task`):
```cpp
const auto blob_scrub_range_size = HDD_IOPS * (SM_REQUEST_TIMEOUT.count() / 2);
while (blob_start <= last_blob_id) {
    uint64_t blob_end = std::min(blob_start + blob_scrub_range_size - 1, last_blob_id);
    // issue RPC for [blob_start, blob_end]
    blob_start = blob_end + 1;
}
```

**Problems:**

1. **No index locality.** The index is ordered by `{shard_id, blob_id}`. A range `[{0, start_blob_id}, {UINT64_MAX, end_blob_id}]` (as written in the current `local_scrub_blob`) crosses every shard boundary and forces a full index scan rather than a seek.

2. **Empty-interval RTT explosion.** In delete-intensive workloads, `blob_id` may be very sparse (consecutive valid IDs separated by millions of gaps). RTT count = `max_blob_id / N`, which can be 10–100× the actual blob count `blob_count / N`. Each RTT touches zero blobs but still pays network latency.

---

### Option B: Follower-Driven Full Scan

The leader sends `last_blob_id` once; each follower independently scans `[{0, 0}, {UINT64_MAX, last_blob_id}]`, streaming results every N blobs. The leader passively receives and aggregates.

**Problems:**

1. **No progress alignment.** Two followers are in different blob ranges at the same moment. The leader cannot release memory for any range until all replicas have completed it, which degenerates to buffering the full result set simultaneously.

2. **Ambiguous empty vs. lost message.** If a follower's sub-response is dropped, the leader cannot distinguish "no blobs in that range" from "the message was lost." This makes fault recovery non-deterministic.

3. **No flow control.** The leader cannot throttle follower throughput; one follower's unconsumed result buffer can exhaust node memory.

4. **Difficult cancellation.** A leader switch mid-scrub cannot stop followers already executing a full-table range scan; they must finish before discarding results.

---

### Option C: Per-Shard Static RPC (one RPC per shard)

Issue one scrub request per shard, scoped to `[{shard_id, 0}, {shard_id, last_blob_id}]`.

**Problems:**

1. **Prohibitive RPC count.** A PG can hold up to ~256K shards. One RPC per shard means up to 256K sequential RTTs, even for shards that contain a single blob.

2. **Leader data dependency.** The leader's shard list is its own view. If the leader has lost a shard, that shard is not scrubbed at all. Coverage correctness breaks in exactly the failure scenario scrub is designed to detect.

---

## Decision: Shard-Aware Bin-Packing with Streaming Sub-Responses

This design combines data produced during the shard scrub phase with a dynamic bin-packing algorithm and a streaming response protocol to satisfy all three constraints simultaneously.

### Step 1: Build shard_blob_count from the Shard Scrub Phase

During shard scrub, each replica already counts blobs per shard. After shard scrub completes, the leader takes the **maximum blob count across all replicas** for each shard and builds:

```cpp
std::map<shard_id_t, uint32_t> shard_blob_count;
```

Two special cases apply:

- **Empty shard** (`blob_count == 0`): use `max(1, blob_count) = 1`. This ensures empty shards still contribute to the batch size accumulator and each batch eventually seals, bounding batch count.

- **Missing shard** (exists on some replicas but not all): set `blob_count = MAX_BLOB_COUNT`. This forces the missing shard into its own isolated batch (see Step 2). Without a reliable blob count from cross-replica comparison, the only safe assumption is worst-case coverage.

**Key safety property**: Because the count comes from the most-populated replica rather than the leader's own view, even if the leader has lost blobs in a shard, the coverage range for that shard is still driven by the follower's larger count.

### Step 2: Bin-Pack Shards into Blob Scrub Batches

Set a configurable batch ceiling `MAX_BLOB_COUNT` (reference: ≤ 524,288, i.e. the maximum blobs per shard).

**Algorithm:**

```
batch_list = []
current_batch_shards = []
current_batch_count = 0

for (shard_id, count) in shard_blob_count:  // iterated in shard_id order
    effective_count = max(1, count)          // never let empty shards accumulate forever

    if effective_count >= MAX_BLOB_COUNT:
        // Oversized shard: flush any in-progress batch, then this shard forms its own batch
        if current_batch_shards is not empty:
            batch_list.append(current_batch_shards)
            current_batch_shards = []
            current_batch_count = 0
        batch_list.append([shard_id])        // solo batch
    else:
        if current_batch_count + effective_count > MAX_BLOB_COUNT:
            batch_list.append(current_batch_shards)
            current_batch_shards = []
            current_batch_count = 0
        current_batch_shards.append(shard_id)
        current_batch_count += effective_count

if current_batch_shards is not empty:
    batch_list.append(current_batch_shards)
```

finally, we get the first and the last shard in `batch_list` to get the final shard range `[start_shard_id, end_shard_id]` (since `shard_blob_count` is ordered by `shard_id`) for this scrub round.

**What this guarantees:**

- **Small-shard batching**: Many shards holding 1–2 blobs are packed into a single RPC. RTT count ≈ `Σ blob_count / MAX_BLOB_COUNT`, proportional to actual data volume, not ID span.
- **Oversized-shard isolation**: A shard exceeding `MAX_BLOB_COUNT` is placed alone. Its result set for three replicas is at most 524,288 × 49 B × 3 ≈ 77.1 MB — within acceptable memory.
- **Missing-shard isolation**: `MAX_BLOB_COUNT` can make sure missing shards always form solo batches and are covered by the worst-case range `[{shard_id, 0}, {shard_id, last_blob_id}]`.

### Step 3: Leader Issues Range Requests with Streaming Sub-Responses

For each batch `[start_shard_id, end_shard_id]`, the leader sends a scrub request to all followers containing:

```
{ start_shard_id, end_shard_id, last_blob_id }
```

`last_blob_id` is read once at scrub start and used as the scan ceiling throughout. Raft guarantees a follower's committed log never exceeds the leader's, so no per-replica negotiation is needed.

**Follower streaming protocol:**

1. The follower executes an index range query over `[{start_shard_id, 0}, {end_shard_id, last_blob_id}]`, leveraging the `{shard_id, blob_id}` composite key for a direct seek from `start_shard_id`. Leader do it also in the backgroud when waiting for followers' responses. The follower streams results back to the leader in batches of at most `MAX_BLOB_COUNT_PER_RPC` blobs (configurable, reference: 10 blobs per batch).
2. After reading every MAX_BLOB_COUNT_PER_RPC blobs (configurable), the follower returns the scrub results for those MAX_BLOB_COUNT_PER_RPC blobs to the leader. Suppose the last blob in the returned scrub result is `{shard_id_n, blob_id_n}`; the leader then issues the next sub-request with a range `[{shard_id_n, blob_id_n + 1}, {end_shard_id, last_blob_id}]`.
3. The follower executes a fresh index range query from `{shard_id_n, blob_id_n + 1}` and returns the next batch of results.
4. When the follower's response is empty, the leader marks that follower as complete for this batch.

This protocol bounds each individual RPC to at most MAX_BLOB_COUNT_PER_RPC blob reads — regardless of how large the shard is — keeping latency within any reasonable RPC timeout.

### Step 4: Leader Collects and Compares

The leader also performs local scrub concurrently. Once all followers have completed a whole batch { start_shard_id, end_shard_id, last_blob_id }, the leader merges the results from the three replicas and then releases memory for all the blobs in this batch as it is compared. Peak memory is bounded by the largest batch's result set (≤ 77.1 MB for a solo oversized shard).

If any follower fails to respond after 3 consecutive retries, the scrub task for that batch is aborted and reported as incomplete.

### Step 5: IOPS Throttling

Scrub is a background task and must not crowd out foreground traffic.

**Throttling budget:**

| Parameter | Value |
|-----------|-------|
| HDD random IOPS | 100 |
| Scrub IOPS budget (10 % of total) | 10 IOPS/s per disk |

**PBA sorting within each sub-response batch:**

Although true sequential reads are not achievable in production (foreground I/O continuously moves the disk head), sorting blobs by PBA before reading them within a single sub-response batch minimises unnecessary head movement. Reads are issued in PBA order with no artificial delay inserted between them — the goal is to read the N blobs in the batch as efficiently as the mixed-load HDD allows, not to impose fixed sleeps that turn a potentially semi-sequential workload into a deliberately random one.

**Token-bucket throttling at the scrub request queue:**

Rate limiting is enforced at the request-processing layer rather than between individual blob reads. A per-disk token bucket is maintained with a replenishment rate of **10 tokens/second** (matching the IOPS budget). Before the follower begins reading the blobs in a sub-response batch, it acquires **N tokens** from the bucket (where N = `MAX_BLOB_COUNT_PER_RPC`). If insufficient tokens are available, the request handler blocks until the bucket refills.

```
token_bucket.replenish_rate = 10 tokens/s   // = scrub IOPS budget
token_bucket.acquire(MAX_BLOB_COUNT_PER_RPC) // blocks if budget exhausted
read blobs in PBA order                      // no per-blob sleep
return scrub results to leader
```

This decouples two concerns:

- **Within a batch**: reads proceed at full disk speed in PBA order — no artificial pacing.
- **Between batches**: the token bucket enforces that no more than 10 blob reads per second are issued across all concurrently active scrub requests on the same disk.

**Relationship to `MAX_BLOB_COUNT_PER_RPC`:**

With N tokens consumed per batch and a replenishment rate of 10/s, at most `10 / N` batches can be processed per second. The end-to-end latency per sub-response is dominated by the actual disk read time for N PBA-sorted blobs plus the RPC round-trip:

```
latency ≈ disk_read_time(N blobs, PBA-sorted) + RPC_RTT
```

`MAX_BLOB_COUNT_PER_RPC` therefore controls both per-RPC latency and the granularity of IOPS accounting. A value of N = 10 means one batch per second at steady state, with each batch reading 10 blobs in sequence — leaving 90 IOPS/s (90 %) available for foreground traffic.

---

## Correctness Analysis

### Full Coverage

[{start_shard_id, 0}, {end_shard_id, last_blob_id}] can ensure full coverage of all blobs in shards [start_shard_id, end_shard_id] are covered at the moment scrub_lsn is committed.

### Safety Under Leader Data Loss

If the leader has lost blobs in shard S:
- Its local blob count for S is too low.
- But `shard_blob_count[S]` uses the **max across replicas**, so the batch range includes S with the correct (follower-derived) upper bound.
- The follower's index query `[{S, 0}, {S, last_blob_id}]` finds the blobs the leader is missing.

### Missing Shard Handling

If shard S is missing on one or more replicas:
- `shard_blob_count[S] = UINT64_MAX`, causing S to form its own batch.
- The follower's query range `[{S, 0}, {S, last_blob_id}]` covers the full possible extent of S.
- Cross-replica comparison during merge will flag S as having missing blobs.
- Unlike Option C, scrub of other healthy shards is **not blocked**; missing shards are treated as a worst-case solo batch and scrubbed independently.

### Streaming Cursor Continuity

The leader's next sub-request starts at the one exactly next of`{shard_id_m, blob_id_m}` — the last position returned by the follower. The follower's index query is `[{shard_id_m, blob_id_m + 1}, {end_shard_id, last_blob_id}]` with an inclusive lower bound, so no blob is skipped between sub-responses.

---

## Efficiency Analysis
the proposed solution eliminates the impacts of sparse blob_ids and shard_ids. The number of RPCs is proportional to the actual blob count, not the ID span. so, this solution is very frendly to delete-intensive workloads.

---

## Trade-offs and Limitations

1. **shard_blob_count is a snapshot, not a live count.** the upper limit of last_blob_id excludes those newly written blobs from the scrub, it is the snapshot at the moment scrub_lsn is committed.

2. **Missing shard increases RPC count.** A missing shard gets blob count of `MAX_BLOB_COUNT`, causing it to solo-batch and generate streaming sub-requests over its full possible range. This is intentional and conservative; a missing shard is a serious integrity concern that warrants thorough coverage.

3. **Stateless follower between sub-requests.** The follower does not retain any state between sub-requests, the scrub result is only determined by the received requests from leader and the pre-set `MAX_BLOB_COUNT`.

4. **Memory ceiling per batch.** A solo oversized batch (524,288 blobs × 3 replicas × 49 B) ≈ 77.1 MB of result data in the leader's memory simultaneously. This is a fixed ceiling and well within acceptable limits.

---

## Consequences

**Benefits**

- RTT count is proportional to actual blob count, not to `max_blob_id`. Delete-intensive workloads do not generate wasteful empty RTTs.
- Index access uses `shard_id`-prefix seeks, eliminating full-tree scans for each interval.
- Coverage is safe even when the leader has lost data — the max-across-replicas count drives the range.
- Missing shards are covered conservatively without blocking scrub of healthy shards.
- Per-RPC latency is bounded by the streaming batch(MAX_BLOB_COUNT_PER_RPC), independent of shard size.
- Leader peak memory is bounded at ≈ 77.1 MB, regardless of PG size.

**Costs**

- Implementation is coupled to the shard scrub phase; `shard_blob_count` must be available before blob scrub begins.
- Streaming sub-response protocol adds implementation complexity compared to a single bulk response per range.
