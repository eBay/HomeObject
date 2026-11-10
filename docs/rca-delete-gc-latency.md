# Delete latency during GC: root cause analysis and fix design

* **Status:** fix implemented; pending review
* **Component:** HomeObject `homestore_backend`: GC Manager and PG manager
* **Revision analysed:** `fff21f794520022ffceb9032016c7705a549974b`, before the fix; the source links below point to it
* **Incident:** delete latency of 10-20 s observed on 2026-10-18 in `car-audi`; further logs from a run on 2026-11-02

## 1. Summary

**Problem.** During GC, some delete requests stalled for 10-20 seconds.

**Root cause (reproduced in tests, not yet proven for the incidents).** After every chunk GC task,
`update_pg_meta_after_gc()` recounted tombstones by scanning the whole blob index of the PG while holding the
instance-wide `_pg_lock` exclusively. `get_hs_pg()` takes the same lock in shared mode, so lookups of every other PG
waited for the scan. In a single-replica test the scan held the lock for 1.68 s at 5 million index entries, and for
8.67 s on average, 5.17 times longer, when 48 PGs held 5 million entries each (§4).

**Fix.** The scan no longer runs under `_pg_lock` or on the GC task path. A completed GC task only marks its PG. One
GC Manager worker thread then recounts that PG's tombstones, at most once per interval (default 300 s) and without
holding `_pg_lock`, and stores the result as an advisory hint (§5).

**Not established.** That this lock caused the production delays (the logs are circumstantial, §2); the latency
improvement in production; and the delay when several GC tasks queue for the lock.

## 2. Symptom and production evidence

Delete request latency rose while GC was active.

* **Dashboards.** Delete load was roughly constant, then normal GC activity started and latency rose. The GC panel is
  a rolling one-hour aggregate, so its edge does not show exactly when GC started. The latency panel is the p90 of
  successful SM (Storage Manager) delete requests, not disk latency; a value near 2 s may be capped by the histogram's
  largest finite bucket.
* **Original incident (2026-10-18), PG 56, trace `5967486512239909464`.** SM7 logged precommit at 09:08:14.745 and `handle_commit`
  at 09:08:16.044 (1.299 s). SM5 logged follower request creation at 09:08:14.745 and `handle_commit` at 09:08:35.228
  (20.483 s). The two intervals have different boundaries, and `handle_commit` is logged before the application
  commit callback, so 20.483 s is not the duration of this delete's HomeObject callback.
* **Later run (2026-11-02), PG 56, trace `713184303384713465`.** SM5 received the delete at 09:58:18.716 and logged
  `Blob Delete request` at 09:58:29.608: **10.892 s** on the same thread. The request entered Raft at about
  09:58:29.608, so the delay preceded its Raft proposal.
* **GC on the same SM.** GC task 590 for PG 141 logged a successful index replacement at 09:58:18.715, before its PG
  and shard metadata updates. Its worker logged LRU activity through 09:58:29.407 and a checkpoint-context event at
  09:58:29.608. The GC task and the slow delete overlap closely.

**Limits of this evidence.** The logs do not identify the lock, so they do not prove that GC caused the delay. The
10.892 s interval also includes executor scheduling and both the shard and PG lookup paths; GC updates shard metadata
under `_shard_lock`, and checkpoint processing can contend on `_pg_lock`. LRU messages alone do not show cache
saturation or a blocking read, and neither the follower delays nor the pre-Raft interval point to an election or gRPC
channel cause. The 2026-11-02 logs cannot by themselves establish the cause of the 2026-10-18 incident. Sherlock may
sample or drop log lines, so a missing line is not evidence that an event did not happen. The deployed revision was
not established.

## 3. Root cause

### 3.1 The code path

[`update_pg_meta_after_gc()`](https://github.com/eBay/HomeObject/blob/fff21f794520022ffceb9032016c7705a549974b/src/lib/homestore_backend/hs_pg_manager.cpp#L1577-L1654)
runs at the end of a chunk GC task, after the task has copied the chunk's valid blobs and updated the PG index, to
point the PG's vchunk at its new pchunk. It took `_pg_lock` exclusively for its whole body (PG metadata update, PG
superblock write, chunk selector update) and, inside that critical section, called
[`get_pg_tombstone_blob_count()`](https://github.com/eBay/HomeObject/blob/fff21f794520022ffceb9032016c7705a549974b/src/lib/homestore_backend/hs_pg_manager.cpp#L1455-L1498),
which scans the PG's entire blob index and counts tombstones. `get_hs_pg()` takes the same lock in shared mode, so a
long scan blocks lookups for every PG. Disk throughput alone does not bound the scan's wall time: cache misses,
synchronous reads, scheduling and contention all matter.

### 3.2 Why it repeats

The investigated environment has **one pdev, multiple PGs and multiple chunks**. The pdev's GC actor has a normal-GC
executor and an EGC executor, shared by all its PGs; concurrency is not limited per PG. The code defaults are four
normal-GC and two EGC threads per pdev (normal threads are capped by reserved-chunk capacity); the deployed values
must be verified. A chunk's GC state prevents duplicate tasks for the same chunk, but different chunks, including
chunks of the same PG, can have concurrent tasks. The per-PG pending-task count tracks and drains tasks; it is not a
concurrency cap.

Every chunk task on the normal metadata-update path therefore scanned the whole index of its PG, and the scans
serialized on `_pg_lock`. A task whose destination chunk is already recorded (crash recovery) skips that branch and its
scan. When a writer releases the lock, a waiting reader or another GC writer may acquire it next, depending on
scheduling and the `std::shared_mutex` implementation. Several GC writers can therefore compound a foreground
request's wait, but no acquisition order or total delay is guaranteed.

Deletes feed this path: they create tombstones and reclaimable space, which drives normal GC. That may explain why a
delete-heavy workload was affected; it is not evidence that only deletes take the lock or that GC is the only
bottleneck.

### 3.3 What the count is used for

`tombstone_blob_count` is a durable entity of the PG. It feeds `PGStats::num_tombstone_objects`, the
`tombstone_blob_count` metric gauge and the PG superblock (written at CP). It is not an input to GC scheduling, which
uses defragmentable blocks, chunk state and PG liveness. Blob delete increments it; nothing decrements it. After log
replay, `refresh_pg_statistics()` recounts it together with the active blob count, occupied blocks and
`last_committed_blob_id`. The fix relies on the count being advisory (§5.1).

## 4. Reproduction and measurements

### 4.1 Method

Two opt-in benchmarks (`HomeObjectFixture.DISABLED_*`, kept in the tree) build real PG indexes directly, without blob
payload writes or Raft replication. The entries are synthetic: 25% tombstones, the rest a repeated synthetic PBA,
spread over 16 synthetic shard IDs. Each benchmark calls the original scan, `get_pg_tombstone_blob_count()`, while
holding `_pg_lock` exclusively. Neither runs a real GC. Both ran on one replica in the same environment (Appendix A).

| Benchmark | Measures |
|---|---|
| `PgTombstoneScanBenchmark` | One populated PG: the scan time under the lock, and the latency of `get_hs_pg()` for another PG from a probe thread during the scan (5 scans per size). |
| `MultiPgTombstoneScanBenchmark` | N populated PGs (48 here): the scan time of each PG, in two sequential rounds. No concurrent lookup probe. |

### 4.2 Results

At the same 5,000,000 entries and 25% tombstones per scanned PG, populating 48 PGs instead of one raised the mean scan
time from **1.677066 s to 8.671741 s (5.17x)**. In both runs the scan made up almost the entire write-lock hold of the
test.

| Experiment | Entries per populated PG | Scan samples | Mean scan | Scan range |
|---|---:|---|---:|---:|
| Single populated PG, with another PG's lookup probe | 5,000,000 | 5 repetitions | 1.677066 s | 1.673597-1.682421 s |
| 48 populated PGs, second sequential round | 5,000,000 | 48, one per PG | 8.671741 s | 8.349350-8.966741 s |

**Cross-PG blocking (single populated PG).** At 1,000,000 entries, three scans took 345.815-347.534 ms. At 5,000,000
entries, including 1,250,000 tombstones, a probe thread looked up another PG repeatedly while each scan held the
lock:

| Iteration | Scan | Test write-lock hold | Other PG baseline maximum | Other PG concurrent maximum |
|---|---:|---:|---:|---:|
| 0 | 1.673712 s | 1.673721 s | 9 µs | 1.673778 s |
| 1 | 1.679804 s | 1.679823 s | 12 µs | 1.679892 s |
| 2 | 1.675795 s | 1.675808 s | 4 µs | 1.675896 s |
| 3 | 1.673597 s | 1.673610 s | 10 µs | 1.673677 s |
| 4 | 1.682421 s | 1.682434 s | 12 µs | 1.682508 s |

In all five samples the slowest other-PG lookup started 0–1 µs after the scan started and returned 65–101 µs after the
scan ended, so lookups of other PGs waited for the whole scan. Each baseline maximum is from 128 lookups made before
the scan. These figures are not latency percentiles, and lookup times include thread scheduling.

**48-PG scans (second round, PGs 1-48).** Every sample reports 5,000,000 entries and 1,250,000 tombstones per PG, for a
total of 240 million entries.

| Metric | Observed value |
|---|---:|
| Samples | 48, one per PG |
| Minimum scan | 8.349350 s, PG20 |
| Mean scan | 8.671741 s |
| Median scan | 8.675219 s |
| Maximum scan | 8.966741 s, PG48 |
| Sum of scan durations | 416.243565 s (6 min 56.244 s) |
| Sum of test lock-hold durations | 416.243715 s |
| Write-lock acquisition wait | 1-81 µs; 41 of 48 samples report 1 µs |
| Test lock hold minus scan | 1-8 µs per sample |

The writer's wait for `_pg_lock` was negligible, so the delay is inside the critical section. All 48 scans took
8.35-8.97 s, so the mean is not the result of a few outliers. These are distributions across PGs in one round, not
repeated measurements of one PG or foreground latency percentiles, and the sum of durations is scan time, not a
measured round duration. The record available for this document contains only this round: round 0, the first-PG
baseline, working-set sizes and metrics snapshots are not included.

### 4.3 What this shows, and what it does not

* **Shows:** the original path can hold the instance-wide PG lock for seconds per scan (about 1.7 s with one populated
  PG, about 8.7 s with 48 populated PGs, at 5 million entries each), and lookups of other PGs wait for the whole scan.
  This strengthens the explanation for second-scale foreground stalls, in the same order of magnitude as the
  production 10-20 s observations.
* **Does not show:** that this was the production root cause, or the delay with several GC writers. The 48-PG test
  has a single writer and no concurrent lookup probe.
* **Cache effects are not measured.** The 5.17x increase is consistent with the shared index cache being pressured by
  many PGs, but the records contain no cache-miss or physical-read counts, and cache state was not controlled. Both
  runs used the same environment, which makes their scan times comparable but does not separate cache misses from other
  scan-time resource effects. Do not describe the increase as a measured cache-miss penalty or an absolute worst case.
* **Scope of the timing.** Only the scan was timed, on synthetic index values; this is not the full
  `update_pg_meta_after_gc()` lock hold or a real GC. The index is validated by a full sweep after loading, which warms
  the cache, so the runs are not cold-cache measurements. The relationship between 1 and 5 million entries is not
  enough to extrapolate to larger indexes.

## 5. Fix design

### 5.1 Goals, non-goals and contract

**Goals**

* No tombstone scan under `_pg_lock`, and none on the GC task path.
* Do not rescan a PG's index more often than needed: coalesce the demand from many GC tasks.
* Bound the background cost: one scan at a time.

**Out of scope**

* Removing all index IO, cache or B-tree lock contention caused by the scan. Only the scan-length hold of the global PG
  lock is removed.
* Changes to PG ownership or lookup interfaces, `drain_pg_pending_gc_task()`, GC and CP recovery order, other
  statistics, the disk format, and PUT/DELETE accounting.
* A cancellable or paginated scan (§8).

**Statistical contract**

* Only `tombstone_blob_count` is refreshed online. The complete `refresh_pg_statistics()` is not called online: it also
  scans chunks and overwrites `last_committed_blob_id`, which scrub uses to determine its range.
* The count is a hint, not a correctness or scheduling input. Concurrent delete callbacks and GC mutations can make a
  scan omit or double-count live increments; the worker replaces the atomic with the scan result. No snapshot, delta
  reconciliation or durable event protocol is needed. The old scan was not exact either: operations that had already
  passed the PG lookup could still modify the index while it ran.
* Assumption, confirmed by the change author: no external system depends on the exact count.
* A recount, including the replay-time one, is accurate only when relevant IO and GC activity are quiescent, not merely
  because a restart occurred. There is no decrement path, and this change does not add one.

### 5.2 Overview

```text
GC task done -> mark PG -> pending refresh -> eligible after the interval
                                                    |
        GC timer tick, or a refresh finishing -> dispatch the earliest eligible PG
                                                    |
                         one in-flight refresh: scan (no _pg_lock) -> store hint

GC task done during a scan -> next pending refresh (kept after the running one finishes)
```

### 5.3 Marking

`pdev_gc_actor::on_gc_task_completed()` calls `GCManager::mark_pg_tombstone_refresh(pg_id)` for every completed task,
successful or failed, because a failed task may already have removed tombstones. Ordinary tasks reach it through
`gc_task_guard`; recovered tasks call it explicitly. The early return in `process_gc_task()` before the guard exists has
not modified the index and needs no mark. Nothing is marked in `add_gc_task()`: a running task needs no early refresh.

For ordinary tasks the mark is made before the task promise is fulfilled and before the per-PG pending GC counter is
decremented, so once `pg_destroy()` has drained a PG's GC tasks, all their marks have been made. GC task completion
does not wait for a refresh.

`mark_pg_tombstone_refresh()` registers a PG only if it exists, is ALIVE and has an index. The first mark of a pending
refresh records its time and makes the PG eligible one interval later (monotonic clock). Later marks merge into the same
pending refresh without postponing it.

### 5.4 Scheduling

* **State.** GC Manager keeps a `std::map` from PG ID to a per-PG refresh state, under one mutex. A state holds the PG's
  index UUID, the time its pending refresh started, its eligible time, the last success, and `valid` and `active`
  flags. It is in memory only; nothing is persisted. A separate single slot holds the in-flight refresh.
* **Dispatch.** The GC scan timer calls `dispatch_tombstone_refresh()` after `scan_chunks_for_gc()` (every
  `gc_scan_interval_sec`, default 60 s), and every finishing refresh dispatches the next, so other eligible PGs do not
  wait for another tick. Dispatch walks the map, takes the eligible PG with the earliest eligible time, moves it into
  the slot and claims its pending refresh. With tens of PGs this needs no priority queue, secondary index or per-PG
  future. If the slot is occupied or the worker is not running, dispatch does nothing. The timer never scans.
* **Worker.** A dedicated single-thread executor (`folly::IOThreadPoolExecutor`) owned by GC Manager, separate from the
  normal and EGC pools. `GCManager::start()` submits one resident worker that waits on a condition variable for the
  slot; dispatch fills the slot rather than queueing executor jobs.
* **At most one in-flight refresh** (selected or running); other PGs stay in the map. A GC task that completes while a
  scan runs creates the next pending refresh, which the running refresh must not clear.
* **Interval.** `hs_backend_config.gc_tombstone_refresh_interval_sec` (default 300, must be positive) is the coalescing
  window and the retry delay after a failed scan. It is not hot-swappable. It is a best-effort target, not a bound:
  timer granularity, other PGs' scans, scan duration and retries add delay.

**Freshness estimate.** At the benchmark's scan cost, one worker needs about **416 s** to scan all 48 PGs, excluding
other overhead. If every PG became pending together and scanning started after the 300 s interval, the last refresh
would finish about **716 s (11 min 56 s)** after the first completion. This is an estimate from the old-path benchmark,
not a measurement of the new worker, and 300 s must not be read as a five-minute freshness guarantee. The worker count
stays at one to bound background cost; removing the scan-length hold of the global PG lock is the primary objective.

### 5.5 Scan and store

`HSHomeObject::scan_pg_tombstones(pg_id, index)` is the scan extracted from `get_pg_tombstone_blob_count()`. It takes an
explicit index handle and returns the count or a btree status. Only `success` is a complete scan: `has_more` would mean
a partial count and is treated as a failure (the previous function accepted it). `get_pg_tombstone_blob_count()` remains
as the locked reference path used by the benchmarks; production code no longer calls it.

For the in-flight refresh the worker:

1. **Starts**, under `_pg_lock` in shared mode and then the refresh mutex. It checks that GC Manager is not stopping,
   the state is valid, the PG exists and is ALIVE, and its index UUID still matches the state's. It copies the index
   `shared_ptr` and marks the state active.
2. **Scans** the index holding neither lock. No raw PG pointer is kept across the scan.
3. **Stores** the result, again under `_pg_lock` in shared mode and the refresh mutex. It re-checks the same
   conditions and that the PG still holds the same index object, then updates only the atomic `tombstone_blob_count`
   and marks the PG's durable entities dirty. The existing CP writes it to the PG superblock; there is no forced CP and
   no direct superblock write.
4. **Finishes**, on every exit path (scope guard): clears `active`, frees the slot and dispatches the next eligible PG.

Lock order: `_pg_lock` before the refresh mutex whenever both are needed. Neither is held across a scan, a wait for an
active refresh, or an executor join.

### 5.6 Failure handling

A failed scan (btree error, missing index or exception) keeps the previous hint, logs the failure and leaves the PG
pending, with its eligible time moved to at least now plus one interval. Retries therefore neither spin nor starve other
PGs. The worker catches `std::runtime_error` and `std::bad_alloc` from a refresh; the scope guard in step 4 runs either
way, so an exception cannot leave a state active or the slot occupied.

### 5.7 PG destroy and shutdown

A shared index pointer keeps the object allocated but does not stop an explicit `index_table->destroy()`, and a raw PG
pointer from `get_hs_pg()` does not retain the PG. Neither is relied on: liveness is checked and active access is
registered under `_pg_lock` before the scan starts, and the result is re-checked before it is stored (§5.5).

`pg_destroy()` calls `GCManager::stop_pg_tombstone_refresh(pg_id)` after marking the PG destroyed and draining its GC
tasks, and before destroying its resources:

| Refresh phase | When the PG is destroyed |
|---|---|
| Pending only | The state is invalidated and removed; no wait. |
| Selected, not yet started | The state is invalidated; the worker skips it. |
| Scanning or storing | New starts are refused; `pg_destroy()` waits for the active refresh to end, then removes the state. |

A selected refresh must never act on a new PG that reuses the ID. The state's identity, its index UUID and the index
object are checked, not the PG ID alone, and a late finish does not erase or update the new PG's state. Because
`pg_destroy()` invalidates the state first, the UUID and index-object checks are a second line of defense.

`drain_pg_pending_gc_task()` and its counters are unchanged: `HttpManager::trigger_gc_for_pg()` also uses it before a
manual GC, so turning it into a permanent close would break that path.

**Shutdown.** `GCManager::stop()` marks the manager stopped and invalidates every state, wakes the worker, stops the GC
scan timer and the GC actors, and then joins the executor, which waits for an active scan. `HSHomeObject` calls it before
HomeStore shutdown. After `stop()`, marking and dispatch are no-ops and cannot recreate state. It is safe when GC was
never started (`enable_gc=false` starts no worker) and when called repeatedly.

**Recovery.** Recovered GC tasks complete before `GCManager::start()`. Their completion hooks register pending state,
which is not dispatched until the worker exists. The recovery GC and CP order and the replay-time
`refresh_pg_statistics()` are unchanged.

The scan is a full, non-interruptible sweep, so destroy and shutdown can wait for a scan in progress (the locked scans in
§4 took 1.7-8.7 s); no bounded exit time is claimed.

### 5.8 Changes to existing code

| Area | Change |
|---|---|
| `HSHomeObject::update_pg_meta_after_gc()` | The tombstone scan and its assignment are removed. Chunk mapping, occupied and reclaimed block accounting, the superblock write and the chunk selector update are unchanged; the superblock write persists the current hint. |
| `pdev_gc_actor::on_gc_task_completed()` | Calls `mark_pg_tombstone_refresh()`. This is the only new action on the GC task path. |
| `GCManager::start()`, `stop()`, GC scan timer | Start and stop the refresh worker; the timer callback also dispatches refreshes. |
| `HSHomeObject::pg_destroy()` | Calls `stop_pg_tombstone_refresh()` after the existing GC drain. |
| `HSHomeObject::scan_pg_tombstones()` | New private method. `GCManager` is a friend of `HSHomeObject`, so the worker can take `_pg_lock` and read the PG for the short start and store steps without changing PG ownership or lookup interfaces. |
| `hs_backend_config.fbs` | `gc_tombstone_refresh_interval_sec` appended to `HSBackendSettings` (default 300). |
| `conanfile.py` | Version 4.3.6 to 4.3.7. |

Fault and pause injection points exist in these paths only in `_PRERELEASE` builds.

### 5.9 Observability

Each completed refresh logs, at INFO in the `gcmgr` module, `Tombstone refresh pg=<id> count=<n> scan_us=<..>
dispatch_wait_us=<..> pending_age_us=<..>`: the scan time, the wait between selection and the start of the scan, and the
age of the pending refresh when the scan started. Failures are logged at error level. The time of the last successful
refresh is kept in memory only. No metric was added.

### 5.10 Alternatives not taken

| Alternative | Why not |
|---|---|
| Scan inline at GC task completion, outside `_pg_lock` | GC task completion would include a full index scan, repeated for every chunk task of the same PG. |
| Call `refresh_pg_statistics()` online | It also scans chunks and overwrites `last_committed_blob_id`, which scrub uses (§5.1). |
| Mark in `add_gc_task()`, or only on success | A running task needs no early refresh, and a failed task may already have removed tombstones (§5.3). |
| Priority queue, secondary index or per-PG future | Unnecessary for tens of PGs (§5.4). |
| A GC-count threshold | The interval already limits each PG to one scan per interval, so a count threshold is not needed. |
| More than one worker | Keeps background cost bounded; the main goal is to remove the global-lock hold, not to minimize refresh latency (§5.4). |
| Cancellable or paginated scan | Larger scope; deferred (§8). |
| Make `drain_pg_pending_gc_task()` a permanent close | Would break `trigger_gc_for_pg()` (§5.7). |

## 6. Behavior and compatibility

* **Hint freshness.** `PGStats::num_tombstone_objects`, the `tombstone_blob_count` gauge and the PG superblock value are
  no longer recounted when a GC task completes. After GC removes tombstones from the index, the value keeps its old,
  higher level until the next refresh: normally within one interval plus queueing and scan time (about 12 minutes in the
  48-PG estimate of §5.4), or until the recount after the next restart. After a crash the persisted value can be as old
  as the last CP.
* **GC.** Chunk selection, manual GC (including `trigger_gc_for_pg()`), GC task promises and pending counters, and the
  recovery persistence order are unchanged. GC task completion no longer includes a scan.
* **Format.** No on-disk change. The new setting is appended to the FlatBuffers settings schema without an explicit
  field ID, so existing settings files stay valid and default to 300 s.
* **Resources.** One more thread in GC Manager, idle except while scanning, and one small in-memory state per PG that has
  completed a GC task.

## 7. Validation

### 7.1 Tests

Unit tests are in `hs_gc_tombstone_refresh_tests.cpp` (`TombstoneRefreshTest`, `TombstoneRefreshDisabledTest`). They
drive eligibility with explicit monotonic timestamps instead of waiting for the interval, and hold the worker at chosen
points. The pause and failure injection points exist only in `_PRERELEASE` builds, so the tests that need them are
compiled only there. These tests and the existing GC tests, which cover chunk switching and crash recovery, pass
locally in a Debug `_PRERELEASE` build.

| Behavior | Tests |
|---|---|
| The first mark sets the eligible time and later marks do not postpone it | `CoalescesWithoutPostponingEligibility` |
| Earliest eligible PG first; one in-flight refresh; the next PG follows without waiting for a timer tick | `SelectsEarliestPGAndAdvancesWithOneSlot` |
| A GC task completing during a scan keeps the next pending refresh | `CompletionDuringScanRetainsNextBatch` |
| Successful and failed GC tasks mark but do not refresh inline | `SuccessfulGCMarksButDoesNotSynchronouslyRefresh`, `FailedGCTaskMarksRefresh` |
| Only the tombstone hint is stored, and CP persists it | `PublishesOnlyTombstoneHintAndPersistsWithCP` |
| A failed scan keeps the hint and retries after the interval while other PGs proceed; an exception releases active access | `FailedScanRetainsHintAndAllowsOtherPG`, `ScanExceptionReleasesAccessAndWorkerCanRetry` |
| PG destroy while pending, selected and active; same-ID re-creation | `PendingDestroyDiscardsDemand`, `SelectedWorkCannotPublishToRecreatedPG`, `ActiveScanDoesNotHoldGlobalLockAndDestroyWaits` |
| A valid state whose recorded index UUID no longer matches the PG's index is skipped: nothing is stored and no retry is queued | `StaleIndexIdentityIsSkippedWithoutRetry` |
| Lookups of other PGs proceed while a scan is paused | `ActiveScanDoesNotHoldGlobalLockAndDestroyWaits` |
| Manual GC drain does not cancel a refresh | `ManualGCDrainDoesNotCancelRefresh` |
| Shutdown drains active work and rejects new demand; repeated stop; GC disabled; demand registered before the worker starts | `ShutdownDrainsActiveWorkAndRejectsNewDemand`, `TombstoneRefreshDisabledTest.CanRegisterBeforeStartupAndStopWithoutWorker` |
| The replay-time recount does not depend on old pending state | `ReplayRefreshDoesNotDependOnOldPendingState` |

All names except the one marked `TombstoneRefreshDisabledTest.` are in `TombstoneRefreshTest`.

### 7.2 Not yet validated

* **Latency with the fix.** The benchmarks time only the old locked scan. For the new path the tests show that lookups
  of other PGs proceed while a scan is paused; foreground latency was not measured.
* **The production link.** That this lock caused the production delays remains unproven (§2, §4.3). It needs a real-GC
  comparison, controlled cold-cache and cache-pressure runs, a concurrent workload and the multi-writer tail. Results
  should record the HomeObject and HomeStore revisions, hardware, storage, cache settings, workload and insertion order;
  a restart alone does not make caches cold.
* **Deployed GC thread settings** (§3.2).

## 8. Known limitations and follow-ups

* The scan is a full, non-interruptible sweep, so `pg_destroy()` and shutdown can wait for it. A cancellable or
  paginated traversal would remove the wait; it is a separate scope decision.
* Hint freshness is best-effort. With many large PGs it can lag by more than the interval (§5.4).
* The scan still competes for index IO, cache and B-tree locks. Only the scan-length hold of the global PG lock is
  removed.

## Appendix A. Running the benchmarks

Both benchmarks are `DISABLED_` tests: run them with `--gtest_also_run_disabled_tests`, on an isolated host, from an
empty scratch directory (the settings, including `--override_config` values, are written to `--config_path`). The test
harness removes a shared-memory object, kills any process using its `--base_port`, and creates and removes fixed device
files under `/tmp`. Do not run it alongside another HomeObject test or against production devices.

### A.1 Single populated PG

```sh
/path/to/homestore_test_gc --executor immediate --config_path ./ \
  --replicas 1 --use_file=true --num_blobs 1000000 \
  --gtest_also_run_disabled_tests \
  --gtest_filter=HomeObjectFixture.DISABLED_PgTombstoneScanBenchmark
```

`--num_blobs` is the maximum total number of index entries (default 20), not a per-shard count; use `10000000` or
`100000000` for large runs, which need adequate index capacity and take a long time. The test measures 10,000, 100,000,
1,000,000 and 10,000,000 entries below the maximum, plus the maximum itself. Each size is checked (successful inserts,
total visited entries, tombstones) before it is measured. Log lines to read:

* `gc_scan_benchmark load`: the incremental `batch_load_us`.
* `gc_scan_benchmark scan`: five samples of `scan_us`, `lock_wait_us` and `test_lock_hold_us`. The last is the test's
  scan-only critical section, not the full GC critical section, and includes small measurement overheads.
* `gc_scan_benchmark reader`: the maximum of 128 baseline lookups of another PG, the number and maximum duration of
  lookups from the probe thread during the scan, whether the slowest one overlapped the scan, and its start and end
  offsets from the scan's start and end. `probe_count=0` (with `probe_max_us=-1`) means there was no sample. A
  non-overlapping maximum does not show that no lookup overlapped the scan.
* `gc_scan_benchmark other_pg`: a one-shot lookup of another PG while the writer holds the lock. `lookup_us` includes
  thread scheduling and is not a pure mutex wait.

Durations are observations, not pass or fail thresholds.

### A.2 48 populated PGs

```sh
/path/to/homestore_test_gc --executor immediate --config_path ./ \
  --replicas 1 --use_file=true --num_devs 2 --dev_size_mb 65536 \
  --chunks_per_pg 1 --chunk_size 20 --num_pgs 48 --num_blobs 5000000 \
  --base_port 4000 --override_config hs_backend_config.enable_gc=false \
  --gtest_also_run_disabled_tests \
  --gtest_filter=HomeObjectFixture.DISABLED_MultiPgTombstoneScanBenchmark
```

Background GC must be disabled, because the synthetic PBAs do not refer to real blob payloads. Here `--num_blobs` is the
number of entries **per PG**, so the example loads 240 million entries (25% tombstones, 16 synthetic shard IDs per PG).
`4000` is an example `--base_port`. The command creates two 64 GiB device files under `/tmp` (one fast, one data; they
may be sparse, but writes consume space), so provision for them; loading and validation take a long time. Log lines to
read (`multi_pg_gc_scan`):

* `loaded`: each PG's load and validation time and its `node_bytes` and `total_node_bytes` (live B-tree nodes times the
  node size: an indicator of working-set size, not resident cache bytes).
* `baseline`: PG 1 scanned right after it was loaded and validated, before the other PGs are loaded (warm-leaning).
* `sample` and `round_summary`: after all PGs are loaded, each PG's `scan_us`, `lock_wait_us` and `test_lock_hold_us` in
  two sequential rounds, and each round's maximum. Metrics snapshots are logged around each round.

The test does not measure cache misses or physical reads, and it simulates no concurrent lookup or multiple GC writers.
A working set larger than the cache does not prove that a given scan missed. Treat the rounds and the baseline as
observations, not as a guaranteed cold cache or an absolute worst case. Record the HomeObject and HomeStore revisions,
hardware, storage, cache settings and insertion order with the results (§7.2).
