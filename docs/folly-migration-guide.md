# ADR — Remove Folly from HomeObject (Upgrade to HomeStore v8 + sisl v14)

- **Status:** Proposed
- **Date:** 2026-08-25
- **Authors:** Jie
- **Related:**
  - Origin proposal: [eBay/HomeObject#429](https://github.com/eBay/HomeObject/issues/429) — *"Lifting homeobject onto HomeStore v8"* by @szmyd
  - Reference implementation: homeblocks commit `c43fa2d` — *"Remove Folly and redesign the public API onto the v8 coroutine stack"*

> **How to use this ADR.** Sections 1–5 are the review sections (context, target state,
> mechanical substitution tables, homeobject-specific design decisions, and constraints).
> §4 contains all decision records (D-1 through D-8); §§1–3 and §5 are reference/context.
> Section 6 is the migration plan; section 7 is the risk register; section 8 is consequences.
> Appendices A–C are mechanical reference material (type dictionary, sisl v14 breaking changes,
> file inventory) — consult them during implementation.

---

## 1. Context

HomeObject is a blob store built on HomeStore. It is currently pinned to the Folly-futures stack
(homestore `^7.5.2`, sisl `^13.2`, C++20). HomeStore v8 removes Folly entirely and redesigns
the public API onto stdexec coroutines (`sisl::async::task`) and `std::expected`.

homeblocks — sharing the same Folly baseline — has already completed this lift (commit `c43fa2d`)
and serves as the reference implementation. Continuing on v7 accrues rebase debt against every
subsequent sisl/iomgr/homestore release.

**Scope.** Everything required to make HomeObject compile, link, and pass its full test suite
against homestore `^8.0.0` and sisl `^14.8`. Out of scope: production rollout, on-disk format
changes (v8 preserves the format), consumer/SDK migration.

**What is different from homeblocks.** Most of the lift is mechanical reuse of the homeblocks
recipe. HomeObject has three areas homeblocks did not:

| Hard part | Why homeblocks didn't have it |
|-----------|-------------------------------|
| Real multi-member raft listener (`replication_state_machine`) | homeblocks used a solo repl_dev with a stub listener |
| Snapshot / baseline-resync | Did not exist in homeblocks |
| GC with executors in the work path | homeblocks GC was a single reactor timer |

These are the high-risk areas; everything else is mechanical substitution.

## 2. Baseline → Target

| | Baseline (`main`) | Target |
|---|---|---|
| homestore | `^7.5.2@oss/master` | `^8.0.0@oss/dev` |
| sisl | `^13.2.3@oss/master` | `^14.8@oss/dev` |
| iomgr | transitive | `^13.0@oss/dev` (explicit `requires`) |
| nuraft_mesg | transitive via homestore | `^5` transitive; identifier renames apply (see Appendix A) |
| C++ standard | 20 | **23** (`<expected>` requires C++23 in libstdc++) |
| Futures library | Folly | **removed** — `sisl::async::task<T>` (`exec::task<T>`) |
| Error carrier | `folly::Expected<T, E>` | `std::expected<T, E>` (manager surface stays richer — see D-3) |
| HomeObject version | `4.x` | `5.0` (API break) |

Total Folly touchpoints: **~259 across 30 files** (see Appendix C for per-file breakdown).

**Critical path:** Public API (`common.hpp`) → `replication_state_machine` → `hs_blob_manager`.
All PG/shard/blob operations are blocked until the state machine is done.

---

## 3. Mechanical Substitutions

The following are 1:1 substitutions with no design decision required. See Appendix A for the
full dictionary and Appendix B for sisl v14 macro expansions.

| Folly | Replacement |
|-------|-------------|
| `folly::Future<T>` / `SemiFuture<T>` | `sisl::async::task<T>` |
| `folly::makeFuture(x)` / `makeSemiFuture(x)` | `co_return x` |
| `folly::makeUnexpected(e)` | `co_return std::unexpected(e)` |
| `folly::Unit` | `std::monostate` |
| `folly::collectAll` / `collectAllUnsafe` | `sisl::async::when_all(vector<task<T>>)` |
| `folly::Init` | delete — sisl logging init already present |
| `folly::InlineExecutor` / `.via(...)` / `getGlobalCPUExecutor()` | delete — coroutines need no executor |
| `.get()` in tests | `detail::sync_get(...)` from internal `coro_helpers.hpp` — **off-reactor only** |
| `folly::small_vector<T,N>` | `boost::container::small_vector<T,N>` |
| `folly::Uri` | 5-line inline parser (see D-7) |
| homestore `.h` headers | `.hpp` (v8 renamed all; see Appendix A) |
| `ReplDev` / `ReplDevListener` / `BlkId` / … | snake_case renames (see Appendix A) |
| `r_cast` / `s_cast` / `uintptr_cast` / `Clock` / … | real casts + sisl-qualified names (see Appendix B) |

---

## 4. Decisions (homeobject-specific design)

### D-1. Async model: `sisl::async::task<T>` directly in public headers

**Decision:** Replace all `folly::Future<T>` / `folly::SemiFuture<T>` with
`sisl::async::task<T>`. All async manager methods become coroutines using `co_await` / `co_return`.
`sisl::async::task` is exposed in HomeObject's public headers (not erased behind a wrapper).

**Rationale:** This matches homeblocks and aligns with sisl/iomgr/homestore. Alternatives:
- `std::future` wrapper: blocks threads, defeats the lift.
- PIMPL type erasure: kills `when_all` composition, adds heap allocation per await.

**Implications:**
- `folly::Promise<T>` in `repl_result_ctx<T>` → `sisl::async::value_awaitable<T>` (held
  via `std::shared_ptr`; producer calls `.complete(v)` on the commit thread, consumer
  `co_await`s in the issuing coroutine). Lift this pattern verbatim from homeblocks.
- `when_all` does **not** short-circuit on child error; errors are values in the result vector.
- All `Executor::KeepAlive<>`, `_defer()`, `.via()`, `.thenValue()` patterns are deleted.
- Consumers (nuobject, etc.) gain a transitive dependency on stdexec headers; version bump
  to `5.0` signals this API break.

**Internal helper:** Add `src/lib/homestore_backend/coro_helpers.hpp` with `sync_get` and
`detach` wrappers — do **not** expose them in public headers (same as homeblocks).

### D-2. Public API surface: keep `Manager<E>::Result` / `AsyncResult` aliases

**Decision:** Preserve the `Manager<E>` template. Change the underlying types:

```cpp
// Before
template <typename T> using Result      = folly::Expected<T, E>;
template <typename T> using AsyncResult = folly::SemiFuture<Result<T>>;
using NullResult      = Result<folly::Unit>;
using NullAsyncResult = AsyncResult<folly::Unit>;

// After
template <typename T> using Result      = std::expected<T, E>;
template <typename T> using AsyncResult = sisl::async::task<Result<T>>;
using NullResult      = Result<std::monostate>;
using NullAsyncResult = AsyncResult<std::monostate>;
```

**Rationale:** Keeping the aliases preserves the call-site shape for consumers and lets us
change the substrate without touching every usage. Do **not** adopt nuraft_mesg's
`result`/`async_task` aliases at the HomeObject surface; the manager error types are richer
(see D-3).

### D-3. Error boundary: translate at homestore call site, preserve `current_leader`

**Decision:** HomeObject's manager error structs (`BlobError`, `ShardError`, `PGError`)
retain their `{code, std::optional<peer_id_t> current_leader}` shape unchanged.
At every `co_await` on a homestore operation, translate `std::error_condition` back into
the HomeObject error struct. For codes with no HomeObject equivalent, map to `UNKNOWN` and
**log `ec.message()`** at the translation site so observability is not lost.

```cpp
BlobError toBlobError(const std::error_condition& ec,
                      std::shared_ptr<homestore::repl_dev> rd) {
    if (ec == homestore::ReplServiceError::NOT_LEADER)
        return {BlobErrorCode::NOT_LEADER, rd->get_leader_id()};
    if (ec == homestore::ReplServiceError::CANCELLED)
        return {BlobErrorCode::REPLICATION_ERROR, std::nullopt};
    LOGERROR("Unmapped homestore error: {}", ec.message());
    return {BlobErrorCode::UNKNOWN, std::nullopt};
}

// At call sites that return a task (e.g. async_read):
auto r = co_await repl_dev_->async_read(blkid, sgs, size);
if (!r) co_return std::unexpected(toBlobError(r.error(), repl_dev_));

// Note: async_alloc_write is void-returning (a scheduling call).
// The result arrives via the ctx's embedded value_awaitable.
// repl_result_ctx<T>::promise_ is value_awaitable<std::expected<T, homestore::ReplServiceError>>,
// so co_await returns std::expected<T, E> — supports !r and .error().
repl_dev_->async_alloc_write(header, key, value, ctx);
auto commit_result = co_await ctx->promise_;   // blocks until on_commit fires
if (!commit_result) co_return std::unexpected(toBlobError(commit_result.error(), repl_dev_));
```

**Rationale:** HomeStore v8's flat `std::error_condition` cannot carry `current_leader`.
The NOT_LEADER redirect is a first-class client contract; dropping it would break caller
retry logic. The homeblocks recipe (adopt `std::error_condition` wholesale) does **not**
apply here — this is the main homeobject-specific divergence. The existing `toBlobError`
in `hs_blob_manager.cpp:26–63` is the model; `toShardError` / `toPGError` counterparts
must be added.

### D-4. GC / Scrub threading: iomgr named reactors

**Decision:** Replace `folly::IOThreadPoolExecutor` with dedicated iomgr named reactors.
Dispatch work with `iomanager.run_on_forget`. Per-chunk fan-out via `when_all`.

```cpp
// Initialization
for (int i = 0; i < num_gc_threads; ++i)
    iomanager.create_reactor("gc_worker_" + std::to_string(i),
                             iomgr::loop_type_t::io_loop);

// Dispatch — detach the task so it runs on the reactor event loop
iomanager.run_on_forget(iomgr::reactor_regex{"gc_worker_.*"},
    [this, chunk_id]() { sisl::async::detach(do_gc_coro(chunk_id)); });

// GC coroutine — co_await only, never sync_get
sisl::async::task<bool> do_gc_coro(chunk_id_t chunk_id) {
    auto r = co_await data_service().async_read(...);
    co_return true;
}
```

**Rationale:** GC internally `co_await`s homestore IO operations (`async_read`,
`async_free_blks`, etc.). IO completions are dispatched by iomgr reactors. Running GC on
a plain `std::thread` would make those completions unreachable — guaranteed deadlock.
Named reactors preserve the "dedicated GC worker pool" invariant while composing correctly
with the v8 coroutine stack.

Alternatives rejected:
- `std::thread` + `sync_get` bridge: IO completions cannot reach a non-reactor thread.
- moodycamel + `std::thread`: same problem; adds a dep not in the v8 stack.

### D-5. `sync_get` policy: off-reactor only, mandatory comment

**Decision:** `sync_get` is permitted **only on non-reactor threads**. Every call site must
carry a comment `// sync_get: off-reactor OK — <reason>`.

| Site | Verdict |
|------|---------|
| `main()` / test fixture | ✅ off-reactor |
| sisl httplib HTTP handler threads | ✅ off-reactor |
| GC named reactors | ❌ must `co_await` |
| `repl_dev_listener` callbacks | ❌ must `co_await` |
| Any `iomanager.run_on*` body | ❌ must `co_await` |

**Rationale:** `sync_get` parks the calling thread. On a reactor, this prevents the reactor
from processing IO completions — including the one the parked task is waiting for. There is
no safe sync-wait on a reactor; `co_await` is the only alternative.

**Enforcement:** `grep -rn "sync_get" src/` — every hit must have the comment.

### D-6. Concurrent containers

**Decision:**

| Folly type | Replacement | Rationale |
|---|---|---|
| `folly::ConcurrentHashMap<K,V>` | `boost::unordered::concurrent_flat_map<K,V>` | Lock-free reads; boost already in dep graph |
| `folly::MPMCQueue<T>` | `homestore::BoundedMPMCQueue<T>` | Ships with HomeStore v8; no new dep |
| `folly::EvictingCacheMap<K,V>` | Hand-rolled ~30-line LRU | Only site is `hs_http_manager`, 100-item cap |

Note: `concurrent_flat_map` uses `visit_all()` for iteration, not range-for.

### D-7. URI parsing: hand-roll, no new dependency

**Decision:** Remove `folly::Uri`. The only use site (`HSReplApplication::lookup_peer`)
has a fixed `http://<host>:<port>` format — use a 5-line inline parser:

```cpp
// Precondition: url has the form [scheme://]<host>:<port> (as produced by lookup_peer).
// IPv6 bracket notation ([::1]:8080) is not expected at this site.
std::pair<std::string, uint16_t> parse_endpoint(std::string_view url) {
    auto host_start = url.find("://");
    host_start = (host_start == std::string_view::npos) ? 0 : host_start + 3;
    auto colon = url.rfind(':');
    if (colon == std::string_view::npos || colon <= host_start)
        throw std::invalid_argument("parse_endpoint: malformed endpoint: " + std::string(url));
    return {std::string(url.substr(host_start, colon - host_start)),
            static_cast<uint16_t>(std::stoi(std::string(url.substr(colon + 1))))};
}
```

### D-8. Build strategy: CMake gate + memory-backend green trunk

**Decision:** Gate `homestore_backend` behind a CMake option so `main` stays buildable
(memory backend only) throughout the migration. `memory_test` must stay green at every merge.

```cmake
# CMakeLists.txt top-level
option(HO_BUILD_HOMESTORE_BACKEND "Build the homestore-backed library" ON)

# src/lib/CMakeLists.txt
if(HO_BUILD_HOMESTORE_BACKEND)
    add_subdirectory(homestore_backend)
endif()
```

Also required in the first PR:
- `CMakeLists.txt:4` → `set(CMAKE_CXX_STANDARD 23)`. Without this CMake silently overrides
  Conan's `-std=c++23` and `<expected>` fails to resolve.
- `conanfile.py`: remove `folly/...`; add explicit `stdexec/25.09`.

CI runs `HO_BUILD_HOMESTORE_BACKEND=OFF` from step 1 until the managers are fully migrated;
flip to `ON` once `homestore_test_*` passes end-to-end.

---

## 5. Non-goals

- No on-disk format change. v8 preserves the format; the lift is API-only.
- No consumer SDK migration (nuobject, callers) in this ADR.
- No new HTTP framework research. Follow the homeblocks Pistache → sisl httplib pattern.
- No refactor of `HeapChunkSelector` beyond the `.h` → `.hpp` rename.
- Do not adopt nuraft_mesg's `Result/AsyncResult` at the HomeObject public surface (D-2).

---

## 6. Migration Plan

Self-contained bottom-up sequence; each step must leave `memory_test` green before merging.

| Step | Scope | Est. | Notes |
|------|-------|------|-------|
| **1** | `conanfile.py` + `CMakeLists.txt` (C++23, CMake gate, dep bump). Get it *configuring* before touching code. | 0.5 d | |
| **2** | `common.hpp` public API + `pg/shard/blob` headers: `Manager<E>` aliases → `task`/`std::expected`. Memory backend warm-up: `makeUnexpected`→`co_return std::unexpected`, `ConcurrentHashMap`→`std::unordered_map + std::shared_mutex` (memory backend is single-threaded test code; `concurrent_flat_map` reserved for GC in step 8), drop `folly::Init` (also in `test_package/test_package.cpp`). Tests: `sync_get`, drop `GlobalExecutor`. | 3–4 d | Memory backend has no homestore deps — clean warm-up |
| **3** | `replication_state_machine.{hpp,cpp}`: re-derive **every** override against the v8 `repl_dev_listener` header (snake_case + signature churn — don't assume). `repl_result_ctx<T>::promise_` → `value_awaitable`. New v8 callbacks: add stub bodies now. | 2–3 d | **Critical path** — blocks steps 4–5 |
| **4** | `hs_blob_manager.cpp` (35 touches), `hs_shard_manager.cpp` (22 touches), `hs_pg_manager.cpp` (50 touches): convert to coroutines; add `toBlobError`/`toShardError`/`toPGError` translators (D-3). `hs_homeobject.{hpp,cpp}`. | 7–10 d | PGManager may split into a sub-PR |
| **5** | `index_kv.cpp`: `put`/`get`/`query`/`remove` → `status`; audit all `IndexTable::destroy()` call sites — it `co_await`s a CP flush internally, must not be called with `sync_get` on a reactor. | 1 d | |
| **6** | `hs_cp_callbacks.cpp`: `cp_flush` → `task<bool>`. | 1 d | |
| **7** | `snapshot_receive_handler.cpp`, `pg_blob_iterator.cpp`: convert to coroutines. Note: `read_snapshot_obj` and `apply_snapshot` are **sync** in v8 (return `int`/`bool`). | 2–3 d | |
| **8** | `gc_manager.{hpp,cpp}`, `scrub_manager.cpp`: iomgr named reactors, `BoundedMPMCQueue`, `concurrent_flat_map`, `when_all` fan-out. **Deadlock discipline**: all homestore calls inside GC must be `co_await`ed. | 3–4 d | Highest architectural risk |
| **9** | `hs_http_manager.{hpp,cpp}`: Pistache → sisl httplib; LRU map for `EvictingCacheMap`. | 1–2 d | |
| **10** | Test-side cleanup. Remove any remaining folly references. Final grep audit (see Appendix C). | 2–3 d | |

**Total estimate: 22–31 person-days.**

Per-area breakdown (from issue #429):

| Area | Est. |
|------|------|
| Public API + memory backend | 3–5 d |
| `replication_state_machine` | 2–3 d |
| `hs_blob_manager` | 3–4 d |
| `hs_shard_manager` | 2 d |
| `hs_pg_manager` | 2–3 d |
| GC manager | 3–4 d |
| Snapshot / baseline-resync | 2–3 d (step 7; budget real time — second-heaviest area) |
| HTTP manager | 1–2 d |
| CP callbacks | 1 d |
| Tests & fixtures | 2–3 d |

After step 7 (snapshot/resync complete), flip CI to `HO_BUILD_HOMESTORE_BACKEND=ON` once `homestore_test_*` passes end-to-end. Steps 5–7 must be complete before this — index_kv, cp_callbacks, and snapshot are required for a passing test run.

---

## 7. Risk Register

| # | Risk | Mitigation |
|---|------|-----------|
| 1 | **`sync_get` on reactor → deadlock** | `sync_get` parks the reactor's event loop. The parked reactor can no longer dispatch IO completions — including the one the task is waiting for — causing an unrecoverable deadlock. Permitted sites: `main()`, test fixtures, sisl httplib handler threads (all non-reactor). Forbidden: GC reactors, `repl_dev_listener` callbacks, any `run_on*` body — must `co_await`. Mandatory comment `// sync_get: off-reactor OK — <reason>` at every site; grep audit per PR. |
| 2 | **`IndexTable::destroy()` deadlock** | `destroy()` internally `co_await`s `cp_flush`; a reactor-thread caller that `sync_get`s the result will deadlock (same mechanism as risk 1). Audit every `.destroy()` site in step 5; must be `co_await`ed or called off-reactor. |
| 3 | **`uintptr_cast` wrong expansion** | Must expand to `reinterpret_cast<uint8_t*>`, not `uint32_t*` or `static_cast`. homeblocks had 5 latent pointer bugs from bad expansions. `grep -rn "uintptr_cast" src/` and hand-audit before step 4. |
| 4 | **`alloc_blks` truthiness flip** | v8 `has_value() == true` means success (opposite of v7). Every old `if (r) { /* error */ }` must become `if (!r) { /* error */ }`. `grep -rn "alloc_blks" src/` and hand-audit every call. |
| 5 | **`exec::task` lazy evaluation (silent no-op)** | A task not `co_await`ed / `sync_get`ted / `detach`ed silently does nothing — no error, no execution. Mark all `AsyncResult`-returning functions `[[nodiscard]]`; `std::ignore = task` is a bug, not a suppression. **Caution:** existing v7 call sites that intentionally discard futures (fire-and-forget) will become compile errors — audit each and replace with explicit `sisl::async::detach(...)` where fire-and-forget is intended. |
| 6 | **Buffer lifetime across `co_await`** | `sg_list` data pointed to by the coroutine frame must remain valid while the coroutine is suspended. Follow homeblocks `sgs_keepalive` pattern: keep buffer ownership in the frame until IO completes. |
| 7 | **New v8 listener callbacks silently skipped** | v8 adds `on_no_space_left`, `on_log_replay_done`, `on_become_leader`, `on_become_follower`, `on_config_rollback` with default no-op bodies. If real behavior is needed, silently-unimplemented callbacks will cause data loss or missed events. Add stubs in step 3; wire real behavior in steps 4/7. |
| 8 | **Coroutine purity** | Don't re-invert coroutines with `detach_then`-style callbacks inside an async function. `co_await` / `co_return` only; mixing callback-style continuations back in loses structured stack and error propagation. |
| 9 | **`repl_dev_listener` signature churn** | v8 changed every override to snake_case with modified signatures. Re-derive from the actual v8 header in step 3 — do not assume any signature matches v7. |

---

## 8. Consequences

- **API break for consumers.** `AsyncResult<T>` changes from
  `folly::SemiFuture<folly::Expected<T,E>>` to `sisl::async::task<std::expected<T,E>>`.
  Consumers must have stdexec on their include path. Version bump `4.x → 5.0`.
- **Manager error structs are stable.** `BlobError`, `ShardError`, `PGError` keep
  `{code, current_leader}`; NOT_LEADER redirect semantics preserved.
- **HomeObject no longer depends on Folly**, directly or transitively.
- **New explicit dependency: stdexec** in `conanfile.py`.
- **GC threading semantics unchanged.** Dedicated worker pool + rate limiter + per-chunk
  fan-out. Implementation swaps folly executor for iomgr named reactors.
- **`memory_test` becomes the CI canary** during the migration window.

---

## Appendix A — Folly → v8 Type Replacement Dictionary

| Folly (v7) | Replacement | Notes |
|---|---|---|
| `folly::Expected<T, E>` | `std::expected<T, E>` | C++23 |
| `folly::makeUnexpected(e)` | `std::unexpected(e)` | In coroutines: `co_return std::unexpected(e)` |
| `folly::Unit` | `std::monostate` | |
| `folly::Future<T>` / `SemiFuture<T>` | `sisl::async::task<T>` | |
| `folly::makeFuture(x)` / `makeSemiFuture(x)` | `co_return x` | Function becomes a coroutine |
| `.thenValue(fn)` / `.deferValue(fn)` | `co_await` + inline body | |
| `folly::Promise<T>` | `std::shared_ptr<sisl::async::value_awaitable<T>>` | Producer `.complete(v)`; consumer `co_await *ptr` |
| `folly::makePromiseContract<T>()` | Construct shared `value_awaitable<T>` | |
| `.setValue(v)` | `.complete(v)` | |
| `.getSemiFuture()` | `co_await *shared_ptr` | |
| `folly::collectAll` / `collectAllUnsafe` | `sisl::async::when_all(vector<task<T>>)` | No short-circuit on error |
| `folly::Init` | delete | sisl logging init already present |
| `folly::InlineExecutor` / `QueuedImmediateExecutor` / `.via(...)` / `getGlobalCPUExecutor()` | delete | Coroutines need no executor |
| `.get()` in tests | `detail::sync_get(...)` from `coro_helpers.hpp` | Off-reactor only |
| `folly::ConcurrentHashMap<K,V>` | `boost::unordered::concurrent_flat_map<K,V>` | Use `visit_all()` for iteration |
| `folly::MPMCQueue<T>` | `homestore::BoundedMPMCQueue<T>` | `homestore/src/lib/blkalloc/bounded_mpmc_queue.hpp` |
| `folly::EvictingCacheMap<K,V>` | Hand-rolled LRU (`std::list` + `std::unordered_map`) | Only site: `hs_http_manager`, 100 items |
| `folly::Uri` | 5-line inline parser (D-7) | Only site: `HSReplApplication::lookup_peer` |
| `folly::small_vector<T,N>` | `boost::container::small_vector<T,N>` | |
| `folly::IOThreadPoolExecutor` | `iomgr.create_reactor(name, io_loop)` | See D-4 |

### HomeStore v7 → v8 identifier renames

| v7 | v8 |
|----|-----|
| `homestore::ReplDev` | `homestore::repl_dev` |
| `homestore::ReplDevListener` | `homestore::repl_dev_listener` |
| `homestore::ReplApplication` | `homestore::repl_application` |
| `homestore::BlkId` | `homestore::blk_id` |
| `homestore::MultiBlkId` | `homestore::multi_blk_id` |
| `homestore::AsyncReplResult<T>` | `homestore::async_result<T>` |
| `homestore::AsyncReplResult<>` (void) | `homestore::async_status` |
| `homestore::NullReplResult` | `homestore::status` |
| `replication/repl_dev.h` | `replication/repl_dev.hpp` |
| `replication/repl_decls.h` | `replication/repl_decls.hpp` |
| `homestore/blk.h` | `homestore/blk.hpp` |
| `homestore/chunk_selector.h` | `homestore/chunk_selector.hpp` |
| `homestore/vchunk.h` | `homestore/vchunk.hpp` |

### nuraft_mesg types (transitive, `^5`)

These types are used in HomeObject's public API but are defined in nuraft_mesg.
Their underlying types are unchanged; no HomeObject-side rename is needed as long as
`using namespace nuraft_mesg` is not in scope and types are fully qualified.

| Type | Definition |
|------|------------|
| `peer_id_t` | `boost::uuids::uuid` (same underlying type as HomeObject's current `peer_id_t`) |
| `replica_id_t` | `boost::uuids::uuid` |
| `group_id_t` | `boost::uuids::uuid` |

> The existing `peer_id_t = boost::uuids::uuid` alias in `common.hpp` stays as-is;
> same underlying type, no ODR issue.

### Semantic changes (not just renames)

- **`alloc_blks` return value flipped.** v8 returns `status`; `has_value() == true` means
  success. Every `if (r) { /* was error */ }` → `if (!r) { /* error */ }`.
- **`apply_snapshot(...)` returns plain `bool`** (was async in v7).
- **`read_snapshot_obj(...)` returns plain `int`** (was async in v7).
- **`get_blk_alloc_hints(...)` returns `result<blk_alloc_hints>`**, not `ReplResult`.
- **New `repl_dev_listener` callbacks** (default no-op; add stubs in step 3):
  `on_no_space_left`, `on_log_replay_done`, `on_become_leader`, `on_become_follower`,
  `on_config_rollback`.

---

## Appendix B — sisl v14 Breaking Changes

### Removed cast macros

| Removed | Expand to | Warning |
|---------|-----------|---------|
| `r_cast<T>(x)` | `reinterpret_cast<T>(x)` | |
| `s_cast<T>(x)` | `static_cast<T>(x)` | |
| `uintptr_cast(p)` | `reinterpret_cast<uint8_t*>(p)` | **Must be `uint8_t*`.** homeblocks had 5 latent bugs from expanding as `uint32_t*`. |

```bash
# Audit before step 4
grep -rn "uintptr_cast\|r_cast\|s_cast" src/
```

### Renamed types / enums

| Old | New |
|-----|-----|
| `Clock` | `sisl::Clock` |
| `MetricsGroupWrapper` | `sisl::MetricsGroup` |
| `ReportFormat::kTextFormat` | `sisl::ReportFormat::TEXT_FORMAT` |

---

## Appendix C — Affected Files

| File | Touches | Step |
|------|--------:|------|
| `src/include/homeobject/common.hpp` | 7 | 2 |
| `src/lib/homeobject_impl.hpp` | 3 | 2 |
| `src/lib/homeobject_impl.cpp` | 3 | 2 |
| `src/lib/blob_manager.cpp` | 5 | 2 |
| `src/lib/shard_manager.cpp` | 6 | 2 |
| `src/lib/pg_manager.cpp` | 2 | 2 |
| `src/lib/memory_backend/mem_homeobject.hpp` | 3 | 2 |
| `src/lib/memory_backend/mem_blob_manager.cpp` | 2 | 2 |
| `src/lib/memory_backend/mem_shard_manager.cpp` | 1 | 2 |
| `src/lib/memory_backend/mem_pg_manager.cpp` | 8 | 2 |
| `src/lib/tests/fixture_app.cpp` | 2 | 2 |
| `src/lib/tests/BlobManagerTest.cpp` | 4 | 2 |
| `test_package/test_package.cpp` | 2 | 2 |
| `src/lib/homestore_backend/replication_state_machine.hpp` | 5 | 3 |
| `src/lib/homestore_backend/replication_state_machine.cpp` | 15 | 3 |
| `src/lib/homestore_backend/hs_homeobject.hpp` | 3 | 4 |
| `src/lib/homestore_backend/hs_homeobject.cpp` | 3 | 4 |
| `src/lib/homestore_backend/hs_blob_manager.cpp` | 35 | 4 |
| `src/lib/homestore_backend/hs_shard_manager.cpp` | 22 | 4 |
| `src/lib/homestore_backend/hs_pg_manager.cpp` | 50 | 4 |
| `src/lib/homestore_backend/index_kv.cpp` | 3 | 5 |
| `src/lib/homestore_backend/hs_cp_callbacks.cpp` | 2 | 6 |
| `src/lib/homestore_backend/snapshot_receive_handler.cpp` | 10 | 7 |
| `src/lib/homestore_backend/pg_blob_iterator.cpp` | 3 | 7 |
| `src/lib/homestore_backend/gc_manager.hpp` | 14 | 8 |
| `src/lib/homestore_backend/gc_manager.cpp` | 25 | 8 |
| `src/lib/homestore_backend/scrub_manager.cpp` | ~15 | 8 |
| `src/lib/homestore_backend/hs_http_manager.hpp` | 4 | 9 |
| `src/lib/homestore_backend/hs_http_manager.cpp` | 6 | 9 |
| `src/lib/homestore_backend/tests/hs_gc_tests.cpp` | 4 | 10 |
| `src/lib/homestore_backend/tests/test_homestore_backend_dynamic.cpp` | 2 | 10 |
| `src/lib/homestore_backend/tests/test_heap_chunk_selector.cpp` | 2 | 10 |
| `src/lib/homestore_backend/tests/hs_repl_test_helper.hpp` | 3 | 10 |

### Final grep audit (all steps complete)

```bash
# All must return zero matches
grep -rE 'folly|#include\s*<folly' src/ conanfile.py CMakeLists.txt
grep -rE 'homestore/(blk|chunk_selector|vchunk|replication/repl_dev|replication/repl_decls)\.h[>"]' src/
grep -rE '\b(r_cast|s_cast|uintptr_cast)\b' src/
grep -n "CXX_STANDARD 20" CMakeLists.txt
```
