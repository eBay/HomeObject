# GC Priority Scheduling with Dual Watermarks

**Status**: Accepted  
**Date**: 2026-06-17  

---

## Context

The GC manager periodically scans all chunks per pdev and submits GC tasks for any chunk whose
garbage ratio (defragmented blocks / total blocks) exceeds a single threshold
(`gc_garbage_rate_threshold`, default 50%). Tasks are submitted in arbitrary iteration order until a
per-pdev quota (`max_task_num = 2 × (reserved_chunks − egc_reserved)`) is reached.

This design has two weaknesses:

1. **No priority ordering.** A chunk at 51% garbage and a chunk at 95% garbage are treated
   identically. When the quota is exhausted, high-garbage chunks may be skipped in favour of
   lower-garbage ones that happen to appear first in iteration order.

2. **Single threshold.** The threshold must be set conservatively high (50%) to avoid saturating
   the quota with marginally-dirty chunks, leaving genuinely dirty chunks under-served.

---

## Decision

### 1. Priority-based scheduling (top-K by garbage ratio, descending)

Before submitting any tasks, `scan_chunks_for_gc` now selects the top-K most garbage-heavy chunks
per pdev using a bounded min-heap (`std::priority_queue`) of capacity `K = max_task_num`. After
collection, the heap is drained into a vector and reversed so submission proceeds in descending
garbage-ratio order — the chunks with the most garbage are always scheduled first. This maximises
reclaimed space per GC cycle and makes scheduling deterministic: given the same chunk state, a
leader and its followers will produce an identical submission order.

`K` is fixed at `max_task_num` (`2 * (reserved_chunk_num_per_pdev - reserved_chunk_num_per_pdev_for_egc)`)
rather than the dynamic `remaining_capacity` so we always retain enough candidates if capacity
opens up later during the scan. Memory is bounded to O(K) regardless of how many chunks the pdev
holds.

### 2. Dual watermarks

Two configurable thresholds replace the single threshold:

| Config key | Default | Meaning |
|---|---|---|
| `gc_garbage_rate_threshold` | 50% | **High watermark.** Chunks above this threshold are *high-tier* and can consume the full task quota. |
| `gc_garbage_rate_threshold_low` | 30% | **Low watermark.** Chunks between the two watermarks are *low-tier* and are capped at half the task quota (`max_task_num / 2`). |

The submission loop enforces these invariants:

```
total_quota  = max_task_num          (hard cap, both tiers combined)
low_quota    = max_task_num / 2      (cap for low-tier chunks only)

for chunk in top_k_chunks iterated DESC by garbage_ratio:
    if total_submitted >= total_quota  → stop
    if chunk is low-tier AND low_tier_submitted >= low_quota → skip
    submit; update counters
```

Because chunks are iterated in descending order, all high-tier chunks are submitted before any
low-tier ones, so the low-tier cap never blocks a more-urgent chunk.

Both thresholds are marked `hotswap` and can be adjusted at runtime without restart.

---

## Consequences

### Leader–Follower GC Synchronisation

A key benefit of this change is improved synchronisation between the leader and its followers.
Under Raft replication, all replicas apply the same writes and deletions, so the garbage ratio of
any given vchunk will converge to the same value across all replicas. With priority-based
scheduling, the leader and followers will independently reach the same conclusion about which
chunks to GC and in what order — they see the same dirty state and rank chunks identically.

This synchronisation matters because emergent GC (eGC) is triggered when a replica runs out of
free space and must GC under pressure. If regular GC runs consistently on the dirtiest chunks
first, space is reclaimed more efficiently, and replicas are less likely to diverge in their free
space inventory. The result is fewer eGC events and lower tail latency for normal I/O.

### Operational impact

- **Lower effective threshold.** Because dirty chunks are prioritised, the high watermark can be
  reduced over time (e.g., from 50% to 40%) without increasing the number of tasks per cycle.
- **Coarse throttle for marginally-dirty chunks.** The low-tier cap ensures that chunks just above
  the low watermark do not crowd out more valuable work when the quota is tight.
- **Backward compatibility.** The existing `gc_garbage_rate_threshold` field retains its default
  value (50%) and its role; only its semantics are clarified as the *high* watermark. No on-disk
  format changes are required.
