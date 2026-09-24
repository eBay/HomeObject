# v5 upgrade Note

## Version Changes
- HomeObject package version: 5.0.0 (was 4.x)
- C++ standard: C++23 (was C++20)
- Conan: 2.x required (was 1.x)
- Upstream: sisl ^14.9, homestore ^8.3 (both already Folly-free); Folly and Pistache dropped entirely

This is a breaking change to the public API surface, not to the on-disk format. No data rewrite is required.

## Incompatibilities and Breaking Changes

### 1. Public API Types (`src/include/homeobject/common.hpp`)
- `Result<T>`: `folly::Expected<T, E>` → `std::expected<T, E>`
- `AsyncResult<T>`: `folly::SemiFuture<Result<T>>` → `sisl::async::task<Result<T>>` (a C++23 coroutine)
- `NullResult` / `NullAsyncResult` success value: `folly::Unit` → `std::monostate`

### 2. Async Execution Model
- All async manager methods are now coroutines (`co_await`/`co_return`) instead of Folly futures chained with `thenValue`/`via`/`collectAll`.
- `HomeObjectImpl::executor_` and `_defer()` removed; the `--executor immediate|cpu|io` CLI option is gone.
- Non-coroutine callers must drive an `AsyncResult` with `co_await`, `sisl::async::sync_get` (non-reactor threads only), or `detach`/`detach_then`.

### 3. Error Handling
- `result.hasError()` → `if (!result)`
- `folly::makeUnexpected(e)` → `std::unexpected(e)`

### 4. Concurrency Containers
- `folly::ConcurrentHashMap` → `boost::concurrent_flat_map` (no persistent iterators; use `cvisit`/`visit`/`try_emplace`/`erase_if`)
- `folly::small_vector` → `boost::container::small_vector`
- GC `folly::MPMCQueue<chunk_id_t>` → `sisl::BoundedMPMCQueue<chunk_id_t>` (non-blocking `write`/`read`; GC reserved-chunk logic reworked to not simulate blocking)
- Local `homestore_backend/MPMCPriorityQueue.hpp` removed → `sisl::MPMCPriorityQueue`
- `folly::EvictingCacheMap` → `sisl::LruMap`
- `folly::Uri` removed → manual `host:port` parsing

### 5. Thread Pools
- Scrub's and GC's `IOThreadPoolExecutor` pools are gone. GC now runs on two `std::jthread`s per pdev (normal/emergent); scrub tasks run as detached coroutines on iomgr workers.

### 6. HTTP Layer
- Pistache handlers replaced by `sisl`/`httplib` (`sisl::http_method` + `ioenvironment.get_http_server()->setup_route(...)`).
- Test HTTP client fixtures migrated from the Pistache client to `httplib::Client`.

### 7. HomeStore 8.x API Alignment
- Headers: `*.h` → `*.hpp` (`repl_dev.hpp`, `blk.hpp`, `chunk_selector.hpp`, `vchunk.hpp`, `crc.hpp`)
- Types: `ReplDevListener` → `repl_dev_listener`, `ReplDev` → `repl_dev`, `MultiBlkId` → `multi_blk_id`
- `async_alloc_write`'s last parameter changed from `bool part_of_batch` to `io_batch* batch = nullptr`

### 8. Build
- Conan 1.x → 2.x (`required_conan_version = ">=2.0"`)
- `check_min_cppstd(self, 23)`; root `CMakeLists.txt` sets `CMAKE_CXX_STANDARD 23`
- No direct or transitive dependency on `folly` or `pistache`

## Upgrade Plan
1. **Update toolchain**: install Conan 2.x and a C++23-capable compiler.
2. **Rebuild upstream packages in order**: sisl (^14.9) → iomgr (^13.0) → nuraft_mesg (^5.0) → homestore (^8.3), using `@oss/dev` channel when tracking this tree.
3. **Update downstream call sites**:
   - Replace `AsyncResult` consumption (`.get()`, `thenValue`) with `co_await`, `sisl::async::sync_get`, or `detach`/`detach_then`.
   - Replace `hasError()` checks with `if (!r)`.
   - Replace `folly::Unit` / `makeUnexpected` usages with `std::monostate` / `std::unexpected`.
4. **Memory-backend-only development**: build with `-o homeobject/*:with_homestore=False` (sets `HOMEOBJECT_MEMORY_ONLY`).
5. **Verify no Folly/Pistache remains**: `rg -i 'folly|pistache' HomeObject --glob '!docs/**'` should return nothing (comments in docs are fine).
6. **Run the full test suite** (`homestore_test_pg`/`_shard`/`_blob`/`_misc`/`_gc`/`_scrubber`, dynamic tests with `enable_http=true`) before publishing HomeObject 5.x.
