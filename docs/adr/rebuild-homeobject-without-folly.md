# Rebuild HomeObject on current HomeStore/sisl and remove Folly

**Status**: Accepted  
**Date**: 2026-09-16  
**Updated**: 2026-09-17  
**Scope**: HomeObject (`HomeObject/`), consuming local `HomeStore/` and `sisl/` in this workspace

---

## Context

sisl 和 HomeStore 已经完成 Folly 剥离，并把异步模型从 `folly::Future` / `folly::SemiFuture` 换成 C++23 协程 + `sisl::async::task`（底层是 stdexec `exec::task`）。HomeObject 4.3.4 仍停在旧栈：

| 组件 | HomeObject 当前依赖 | 本仓库实际代码 |
|---|---|---|
| sisl | `sisl/[^13.2]`，C++20 | `14.9.0`，C++23，无 Folly；HTTP 为 cpp-httplib，无 Pistache |
| HomeStore | `homestore/[^7.5.12]` | `8.3.0`，C++23，无 Folly；传递依赖 `iomgr/[^13.0]`、`nuraft_mesg/[^5.0]` |
| Conan | `>=1.60.0` | sisl/HomeStore 要求 `>=2.0`；上游包走 `@oss/dev` |
| 异步 API | `folly::SemiFuture` / `Expected` | `sisl::async::task` + `std::expected` |

HomeObject 不能只“换包版本”。公开头文件、manager 实现、HomeStore listener、GC/scrub、HTTP、测试夹具都直接依赖 Folly 类型和旧 HomeStore / Pistache API。继续链接 Folly 会把 Folly 重新引入已经干净的依赖图，也会和当前头文件对不上。

本 ADR 决定：以本目录下的 sisl 与 HomeStore 为唯一上游，把 HomeObject 重建到同一异步与错误模型，并清掉全部 Folly（以及 Pistache）依赖。采用 **C++23 + `std::expected` + `sisl::async::task` + Boost concurrent_flat_map + sisl 队列 + httplib HTTP**，公开 API 破坏性变更，版本到 5.x。

### 当前 Folly / 旧 HTTP 使用面（必须清零）

仓库内约 35 个 `.cpp/.hpp` 引用 Folly，外加 Pistache handler。按用途分组：

1. **公开 API 词汇表**（破坏性，下游必跟）  
   `src/include/homeobject/common.hpp`：`folly::Expected`、`folly::Unit`、`folly::SemiFuture`。  
   `BlobManager` / `PGManager` / `ShardManager` 的 `Result` / `AsyncResult` / `NullAsyncResult` 全部建立在这组 typedef 上。

2. **Future 编排与延迟执行**  
   `thenValue` / `via` / `collectAll` / `collectAllUnsafe` / `Promise` / `makeSemiFuture` / `makeFuture`：  
   core managers、memory backend、homestore backend（blob/pg/shard、replication SM、snapshot、GC、scrub、HTTP GC）、以及几乎所有测试。  
   `HomeObjectImpl::executor_` + `_defer()`：`folly::makeSemiFuture().via(executor_)`，由 `--executor immediate|cpu|io` 选择 `QueuedImmediateExecutor` / `getGlobalCPUExecutor` / `getGlobalIOExecutor`。

3. **并发容器与小对象**  
   - `folly::ConcurrentHashMap`：memory backend `btree_` / `index_`；`gc_manager` 的 `copied_blobs`、`m_pdev_gc_actors`；`scrub_manager` 的 `m_pg_scrub_ctx_map`、`m_pg_scrub_sb_map`  
   - `folly::small_vector`：`ho_repl_ctx::data_bufs_`  
   - `folly::MPMCQueue<chunk_id_t>`：**仍在用** — `gc_manager` `pdev_gc_actor::m_reserved_chunk_queue`（容量 `reserved_chunk_num_per_pdev`，调用 `blockingWrite` / `blockingRead` / `read`）。替换后改 GC 逻辑，不再模拟 blocking。  
   - `folly::EvictingCacheMap`：`hs_http_manager.hpp` 的 `gc_jobs_map_` / `scrub_jobs_map_`（容量 100）  
   - 本地 `homestore_backend/MPMCPriorityQueue.hpp`：scrub 任务队列，与 `sisl::MPMCPriorityQueue` 同源旧拷贝

4. **线程池**  
   - scrub：`m_scrub_executor`、`m_scrub_req_executor`（`IOThreadPoolExecutor`）  
   - GC：`m_gc_executor`、`m_egc_executor`（`IOThreadPoolExecutor`；正常 GC 与 emergent GC 两套池，带 rate limiter）  
   - `.via(getGlobalIOExecutor())` / `InlineExecutor`：scrub RPC、replication fetch、blob prefetch

5. **初始化与杂项**  
   - `folly::Init`：`fixture_app.cpp`、`hs_repl_test_helper.hpp`、`test_heap_chunk_selector.cpp`、`test_package.cpp`  
   - `folly::Uri`：`hs_homeobject.cpp` `lookup_peer`（先拼 `http://` 再拆 host/port）  
   - `folly::makeUnexpected` / `v.hasError()`：遍布实现与测试

6. **Pistache HTTP**（sisl 14 已删除）  
   `hs_http_manager.hpp` 全部 handler 仍是 `Pistache::Rest::Request` + `Pistache::Http::ResponseWriter`。sisl 现接口是 `sisl::http_handler` = `httplib::Server::Handler`（`httplib::Request const&`, `httplib::Response&`），经 `ioenvironment.get_http_server()->setup_route(...)` 注册。  
   测试侧同样未迁：`homeobj_fixture_http.hpp` 是 Pistache **client**；`hs_repl_test_helper.hpp` 的 `start_http_server` / `/metrics` 仍绑 Pistache method 与 `ResponseWriter`。`test_homestore_backend_dynamic.cpp` 无条件 include 该 client。

### HomeStore 接口已经变了，HomeObject 还没跟上

| HomeObject 现状 | 当前 HomeStore |
|---|---|
| `#include <homestore/replication/repl_dev.h>` 等 `.h` | 公开头全部是 `.hpp`（`blk.hpp`、`repl_dev.hpp`、`chunk_selector.hpp`、`vchunk.hpp`、`crc.hpp`） |
| `homestore::ReplDevListener` / `ReplDev` | `repl_dev_listener` / `repl_dev` |
| `homestore::ReplResult<T>` / `AsyncReplResult<>` | `homestore::result<T>` / `async_result<T>` / `async_status` |
| `folly::Future<std::error_code> on_fetch_data(...)` | `sisl::async::task<iomgr::io_result> on_fetch_data(...)` |
| `folly::Future<bool> CPCallbacks::cp_flush` | `sisl::async::task<bool> cp_flush` |
| `get_blk_alloc_hints` 返回 `ReplResult<blk_alloc_hints>` | `result<blk_alloc_hints>`（错误是 `std::error_condition`，可与 `ReplServiceError` 比较） |
| `create_snapshot` 返回 `AsyncReplResult<>` | `async_status`（协程，`co_return homestore::ok()`） |
| `MultiBlkId` | `multi_blk_id` |
| `async_alloc_write(..., false /* part_of_batch */, tid)` | `async_alloc_write(..., io_batch* batch = nullptr, tid)` |
| `create_repl_dev(...).thenValue(...)` | `co_await hs()->repl_service().create_repl_dev(...)` 得到 `result<shared<repl_dev>>` |
| `data_service().async_read(...).thenValue(...)` | `co_await data_service().async_read(...)` → `iomgr::io_result` |

HomeStore 测试用 `sisl::async::sync_get` 阻塞协程；生产路径用 `co_await`，非协程回调用 `sisl::async::detach` / `detach_then`。HomeObject 必须同一套。

sisl v14 删掉了 `r_cast` / `s_cast` / `uint32_cast` 等 15 个宏。HomeObject 仍在 `replication_state_machine.hpp`、`hs_blob_manager.cpp`、`index_kv.hpp` 等处使用。HomeStore 用 force-include `sisl_cast_compat.hpp` 过渡；HomeObject 同样 **先 force-include，随后在改过的文件里顺手展开**。

---

## Decision

### 1. 上游锁定：本地 sisl + 本地 HomeStore，不再走 Folly

- HomeObject 只消费本仓库的 `sisl` 14.9.x 和 `homestore` 8.3.x，以及它们已经无 Folly 的传递依赖：`iomgr/[^13.0]`、`nuraft_mesg/[^5.0]`。
- `conanfile.py` 禁止再直接或传递依赖 `folly` 或 `pistache`。验收：`conan graph info` / 链接命令行中无二者；源码 `rg folly` / `rg Pistache` 为零（注释允许 “formerly folly”）。
- C++ 标准升到 **23**。`check_min_cppstd(self, 23)`；`required_conan_version = ">=2.0"`。
- HomeObject 主版本 **5.0.0**。不在 4.x 做 ABI / Folly typedef shim。
- 开发阶段：Conan editable / `path=` 指向本仓库的 sisl 与 HomeStore；channel 与上游一致时用 `@oss/dev`。

### 2. 公开 API：`std::expected` + `sisl::async::task`

`src/include/homeobject/common.hpp` 只保留：

```cpp
#include <expected>
#include <variant>
#include <sisl/async/task.hpp>   // 调用方需要 C++23。stdexec 已由 sisl 14.9 requires("stdexec/25.09") 传到 sisl::sisl，memory-only 也会带上，不需要再经 homestore/iomgr。

namespace homeobject {

template < class E >
class Manager {
public:
    template < typename T >
    using Result = std::expected< T, E >;
    template < typename T >
    using AsyncResult = sisl::async::task< Result< T > >;

    using NullResult = Result< std::monostate >;
    using NullAsyncResult = AsyncResult< std::monostate >;
};

} // namespace homeobject
```

不要 `#include <sisl/result.hpp>`。那个头是 `std::expected<T, std::error_condition>`，HomeObject 业务错误仍是 `BlobError` / `PGError` / `ShardError`（带 `current_leader`），不能塞进 `error_condition`。

| 旧 | 新 |
|---|---|
| `folly::Expected<T, E>` | `std::expected<T, E>`：`hasError()` → `!r`；成功值 `*r` / `r.value()` |
| `folly::Unit` | `std::monostate` |
| `folly::makeUnexpected(e)` | `std::unexpected(e)` |
| `folly::SemiFuture<Result<T>>` | `sisl::async::task<Result<T>>` |
| `future.get()`（测试 / 下游同步调用） | `sisl::async::sync_get(std::move(task))`，**仅非 reactor 线程** |
| `thenValue` 链 | 函数写成 coroutine，`co_await` 下游。未 `co_await` / `detach` / `sync_get` 的 `task` **不会跑** |

HomeStore 的 `ReplServiceError` 仍经 `toPgError` / `toBlobError` / `toShardError` 翻译。增加 `std::error_condition` 重载：用 `r.error() == ReplServiceError::NOT_LEADER` 分支，或 `static_cast<ReplServiceError>(r.error().value())`（仅当 `r.error().category()` 是 replication category）。

### 3. `_defer` / `--executor`：删除，不保留 Folly 式 hop

`_defer()` 的唯一作用是把后续 `thenValue` 扔到 Folly executor。公开 API 改为 coroutine 之后：

- 删除 `HomeObjectImpl::executor_` 和 `_defer()`。
- 删除 `--executor immediate|cpu|io`（`SISL_OPTION_GROUP(homeobject, ...)`）。`MemoryTestCPU` / `MemoryTestIO` 合并为一条 `memory_test`，不保留被忽略的 flag。
- manager 正面（`get_shard` / `list_shards` 等）直接 `co_return` 同步 `Result`，或 `co_await` 真正的 backend `task`。不需要为“切线程”再包一层。
- 若某条路径必须离开调用线程，只在那一条上 `co_await` iomgr hop，不要恢复全局 executor 选择。

### 4. 内部异步：coroutine-first；Promise 用 `value_awaitable`

| 场景 | 替换 |
|---|---|
| 公开/内部 async 方法 | `sisl::async::task<T>` + `co_return` |
| Raft 提交后交回调用方（今日 `repl_result_ctx::promise_`） | `sisl::async::value_awaitable< Result<X> >`（单 waiter）。多 waiter 才用 `shared_awaitable` |
| 非协程回调里启动协程 | `sisl::async::detach` / `detach_then` |
| N 路并发 | `sisl::async::when_all(std::vector<task<T>>)` → `task<vector<T>>`。不 short-circuit。`T` **必须能默认构造**（子 task 抛异常时该槽 default-init）。元素类型只用 `expected<...>` / `iomgr::io_result` / `monostate`，不要用 Folly `Try` 替身 |
| 立刻成功的 CP flush | `co_return true;` |
| memory backend | 同样返回 `task`，函数写成 coroutine |

`repl_result_ctx`：

```cpp
template < typename T >  // T = Manager::Result<X>，例如 ShardManager::Result<ShardInfo>
struct repl_result_ctx : public ho_repl_ctx {
    sisl::async::value_awaitable< T > done_;

    sisl::async::task< T > result() { co_return co_await sisl::async::await_value_ref(done_); }

    void set_ok() { done_.complete(T{std::monostate{}}); } // NullResult：今日 setValue(folly::Unit())
    void set_ok(typename T::value_type v) { done_.complete(T{std::move(v)}); }
    void set_err(typename T::error_type e) { done_.complete(T{std::unexpected(std::move(e))}); }
};
```

规则：

- `complete` 收的是 `T`（即 `expected`），**不能** `complete(std::unexpected(...))`。无参 `set_ok()` 只对 `NullResult`（`expected<monostate, E>`）编译通过。
- `value_awaitable` 不可移动、恰好 complete 一次。漏 complete → 调用方永远挂起；两次 complete → UB。
- `on_commit` / `on_error` / rollback 路径都必须 complete。若调用方尚未 `co_await result()`，对象仍须活着（继续用 `intrusive_ptr`）。
- 默认单 waiter。不要无故上 `shared_awaitable`。

### 5. 执行器：去掉 Folly 线程池，落到 iomgr（GC 额外约束）

| 旧 | 新 |
|---|---|
| scrub 两个 `IOThreadPoolExecutor` | 在 iomgr worker 上 `detach` 的 `task`；reactor 上取任务用 `try_pop`，阻塞 `pop` 只留给非 reactor worker |
| GC / eGC 两个 `IOThreadPoolExecutor` | **默认：每个 pdev 两条 `std::jthread`（normal / emergent）跑 actor 循环**，循环里可以 `RateLimiter`。取 reserved chunk 用非阻塞 `read`：失败则本任务失败返回，不要阻塞等队列。循环内的磁盘 IO 仍 `co_await` HomeStore `async_read` / `async_alloc_write`（iomgr 负责 hop）。不要再引入 Folly 或第三套通用线程池库。验收以 `homestore_test_gc` 吞吐不显著差于现状为准。 |
| `.via(getGlobalIOExecutor())` | 删除；`co_await` HomeStore/iomgr IO 时由 iomgr hop |
| `.via(InlineExecutor)` | 删除 |
| `folly::Init` | 删除；测试只留 `SISL_OPTIONS_LOAD` + `SISL_LOGGING_INIT` |

### 6. 容器与杂项

| 旧 | 新 | 注意 |
|---|---|---|
| `folly::small_vector<T, N>` | `boost::container::small_vector<T, N>` | 与 HomeStore `blkid_list_t` 一致 |
| `folly::ConcurrentHashMap` | `boost::concurrent_flat_map<K, V>`（`<boost/unordered/concurrent_flat_map.hpp>`） | Boost 1.85，无新依赖。见下节 API。不要用 sisl `SimpleHashMap` |
| GC `folly::MPMCQueue<chunk_id_t>` | `sisl::BoundedMPMCQueue<chunk_id_t>`，并改 GC 逻辑 | **不要** mutex/cv blocking 包装。sisl 只有非阻塞 `write`/`read`/`sizeGuess`。`chunk_id_t` 是 trivially copyable。调用约定见下。 |

`m_reserved_chunk_queue` 调用约定（改 GC，不模拟 Folly blocking）：

| 旧 | 新 |
|---|---|
| `blockingWrite(id)`（`add_reserved_chunk`、回收路径、`on_gc_task_completed`） | `while (!q.write(id)) {}` — 满则自旋重试直到成功。队列容量是 reserved chunk 数，正常不应长期满；不要丢 chunk、不要 cv 等待 |
| `blockingRead(out)`（`process_gc_task` 取 `move_to_chunk`） | `if (!q.read(out)) { 本任务失败并返回 false; }` — 空则直接失败，不要阻塞。今日注释已写明线程数≈reserved 数时 `blockingRead` 实际立刻返回；没有 reserved chunk 就失败比挂起更清楚 |
| `read(out)`（`handle_recovered_gc_task` 扫队列找 `move_to`/`move_from`） | 保持非阻塞循环：`for (; q.read(chunk_id);)` |

| HomeObject `MPMCPriorityQueue` | `sisl::MPMCPriorityQueue`（`<sisl/fds/mcmp_priority_queue.hpp>`） | 删除本地头、CMake 条目、`MPMCPriorityQueueTest` |
| `folly::EvictingCacheMap<K,V>(100)` | `sisl::LruMap<K,V>`（`<sisl/fds/lru_map.hpp>`，容量 100） | 就是 list + hashmap 的有界缓存，无新依赖。`set` / `get` / 迭代与现用法一致；`get` miss 返回 `Value{}`（`shared_ptr` 为 `nullptr`）。**`get` 不刷新 LRU**（只有 `set` 会提到队首），和「只留最近 100 个 job」按写入计一致。容器本身不是线程安全的，继续用现有 `gc_job_mutex_` / `scrub_job_mutex_`。不要用 `sisl::cache` 的 `LruEvictor` / `SimpleHashMap`。 |
| `folly::Uri` | 手拆 `host:port` | `HomeObjectApplication::lookup_peer` 只保证 `std::string`。现有 fixture（`hs_repl_test_helper`）返回 `127.0.0.1:<port>`。解析规则：去掉可选的 `http://` / `https://` 前缀，按最后一个 `:` 拆 host 与端口；`[ipv6]:port` 若出现再补。不要加 `boost::urls`。 |

`boost::concurrent_flat_map` 对照（HomeObject 现网全是 iterator 风格）：

| Folly `ConcurrentHashMap` | `boost::concurrent_flat_map` |
|---|---|
| `auto it = m.find(k); if (it != m.end()) use(it->second);` | `bool hit = m.cvisit(k, [&](auto const& p) { use(p.second); });` |
| `auto [it, ok] = m.try_emplace(k, v);` | `bool ok = m.try_emplace(k, v);`（无 iterator） |
| `m.insert(k, v)` / `m.emplace(...)` | `m.emplace(k, v)` / `m.insert({k, v})` |
| `m.erase(k)` / `m.erase(it)` | `m.erase(k)` |
| `for (auto& e : m)` | 若回调里还要改同一 map：先快照。范例：`HomeStore/src/lib/replication/repl_dev/raft_repl_dev.cpp` 的 `RaftReplDev::gc_repl_reqs`、`clear_chunk_req`；`raft_state_machine.cpp` 的 `RaftStateMachine::iterate_repl_reqs` |
| `assign_if_equal(k, expected, desired)` | `m.visit(k, [&](auto& p) { if (p.second == expected) p.second = desired; });` |
| 持有 `it->second` 跨过下一次 map 操作 | 禁止。`shared_ptr` / `unique_ptr` 在回调里拷/移出去再用 |

单 key、不遍历时不必快照，用 `cvisit` / `visit` / `erase_if` / `try_emplace`（同文件 `link_lsn_to_req` / `unlink_lsn_to_req`）。

### 7. HTTP：Pistache → sisl / httplib

- handler 签名改为 `void(httplib::Request const&, httplib::Response&)`，经 `ioenvironment.get_http_server()->setup_route(sisl::http_method::Get, "/...", handler)` 注册（与 `sisl/http/http_server.hpp`、iomgr `with_http_server()` 一致）。
- 读 query / body、写 JSON / status 改用 httplib API，不要再 include Pistache。
- `trigger_gc_for_pg` 返回 `sisl::async::task<std::monostate>`；HTTP 线程用 `detach_then` 填 `Response`，**禁止**在 HTTP/iomgr 线程上 `sync_get`。

### 8. 对齐 HomeStore 8.x 头文件与调用

1. include：`repl_dev.h` → `repl_dev.hpp`，`repl_decls.h` → `repl_decls.hpp`，`blk.h` → `blk.hpp`，`chunk_selector.h` → `chunk_selector.hpp`，`vchunk.h` → `vchunk.hpp`，`crc.h` → `crc.hpp`。
2. 类型名：`ReplDevListener` → `repl_dev_listener`，`ReplDev` → `repl_dev`，`MultiBlkId` → `multi_blk_id`。
3. 虚函数按 `repl_dev.hpp` / `cp_mgr.hpp`：`on_fetch_data`、`create_snapshot`、`get_blk_alloc_hints`、`cp_flush`。
4. `create_repl_dev` / `replace_member` / `remove_member` / `flip_learner_flag`：`co_await`，失败 `co_return std::unexpected(toPgError(r.error()))`。
5. `async_read` / `async_write` / `async_free_blks`：`co_await`，类型为 `iomgr::io_result`。
6. **`async_alloc_write` 最后一个 batch 参数从 `bool part_of_batch` 改为 `io_batch* batch = nullptr`。** 现有 `false /* part_of_batch */` 全部改成省略或显式 `nullptr`。
7. sisl cast 宏：复制 HomeStore `sisl_cast_compat.hpp` 并 `-include`。

### 9. 构建与发布

`HomeObject/conanfile.py`：

- `self.requires("sisl/[^14.9]", transitive_headers=True)`（与上游 channel 对齐时加 `@oss/dev`）
- `self.requires("homestore/[^8.3]")`
- 不直接 requires folly / pistache（homestore 已拉 iomgr、nuraft_mesg）
- `check_min_cppstd(self, 23)`；`required_conan_version = ">=2.0"`
- 根 `CMakeLists.txt`：`CMAKE_CXX_STANDARD 23`

本地顺序：sisl → iomgr → nuraft_mesg → homestore → homeobject（Conan create / editable）。本 `no-folly` 超仓可 `add_subdirectory` 做开发，不替代 Conan 发布。

CI 在上游包未发布前，workflow 先 `conan create` 三个上游。

### 10. 不做的范围

- 不把 HomeObject 业务错误改成 `std::error_condition`。
- 不重写 GC 优先级调度、scrub 协议、on-disk superblk。
- 不合并 memory / homestore backend。
- 不引入 Folly / Abseil / 自研并发 hashmap；只用 `boost::concurrent_flat_map`。
- 不保留本地 `MPMCPriorityQueue` 分叉。
- 不为 4.x 提供 Folly typedef 兼容层。
- 不把 `sisl/result.hpp` 引进公开头。
- 不新增 `boost::urls` 或通用线程池库。

---

## 分阶段步骤

原则：Phase 0 只改构建，允许尚未编译通过。**Phase 1 起每个 phase 结束必须能编译并跑通该 phase 列出的测试**（未迁模块可暂时不加入 `add_subdirectory` / `add_test`，但禁止继续链接 Folly）。

### Phase 0 — 构建脚手架

- `CMakeLists.txt` / `conanfile.py`：C++23、sisl 14.9、homestore 8.3、Conan 2。
- 增加 sisl cast compat force-include。
- **不做** 全库 `.h` → `.hpp` 重命名（并进 Phase 2，与 listener / `async_*` 一起改，避免声明与函数体一半新一半旧）。

**完成标准**：`conan graph info` 无 folly、无 pistache；工具链按 C++23 配置成功。

### Phase 1 — 公开头 + core + memory（一次闭环）

公开 typedef 一变，core / memory 必须一起改，否则没有可编译的翻译单元。

**文件**：`common.hpp`；`homeobject_impl.hpp/.cpp`（删 `_defer` / executor）；`blob_manager.cpp` `pg_manager.cpp` `shard_manager.cpp`；memory backend 全部；`tests/fixture_app.cpp` `*ManagerTest.cpp` `BlobManagerTest.cpp`。

**怎么改**

1. Decision 2 的 typedef。
2. 删除 `_defer` / `--executor`；正面 API 写成 coroutine。
3. `makeUnexpected` → `std::unexpected`。
4. memory：`concurrent_flat_map`；拆掉 `WITH_SHARD` / `IF_BLOB_ALIVE` iterator 宏。
5. 测试：`.get()` → `sync_get`；`collectAll` → `sync_get(when_all(...))`；去掉 `folly::Init`。
6. 本阶段 **不编译** `homestore_backend`。在 `src/lib/CMakeLists.txt` 加 `option(HOMEOBJECT_MEMORY_ONLY "Build memory backend only" OFF)`，Phase 1 CI/本地打开它；Phase 2 起关掉。不要靠手改注释，合并时容易忘恢复。

**完成标准**：`homeobject_memory` 链接；`memory_test` 通过；一个只 include `homeobject/blob_manager.hpp` 的 TU **没有 Folly include path** 也能编译。

### Phase 2 — HomeStore listener 与复制路径

**文件**：`replication_state_machine.hpp/.cpp`；`hs_homeobject.hpp/.cpp`（含 `folly::Uri` → 手拆 host/port）；`hs_blob/pg/shard_manager.cpp`；`hs_cp_callbacks.cpp`；`index_kv.hpp/.cpp`；`heap_chunk_selector.h` include；`replication_message.hpp`。

**关键改写**

1. `repl_dev_listener`；`on_fetch_data` coroutine + `co_return`，删除 “bypass later thenValue” 的异常技巧。
2. `create_snapshot`：`co_return homestore::ok();`
3. `cp_flush`：`task<bool>`，`co_return true;`
4. `create_pg`：`co_await create_repl_dev`
5. blob get：`co_await async_read`
6. `async_alloc_write(..., nullptr, tid)`（不再传 `bool`）
7. `toPgError(std::error_condition)` 等重载
8. `repl_result_ctx` 按 Decision 4（含无参 `set_ok()`）

关掉 `HOMEOBJECT_MEMORY_ONLY`。`hs_homeobject.hpp` **直接 include** `gc_manager.hpp` / `scrub_manager.hpp`，并持有 `GCManager` / `ScrubManager` / `HttpManager`。只把三个 `.cpp` 移出 `target_sources` **不能** 让 `hs_homeobject.cpp` 编过。本阶段必须：

- 把 `gc_manager.hpp` / `scrub_manager.hpp` / `hs_http_manager.hpp` 里的 Folly 类型改成前向声明或已迁类型（`concurrent_flat_map`、`task`、`sisl::MPMCPriorityQueue`），使 include 不再拉 Folly；**或**
- 把这三个模块的头+实现一并做到能编译（HTTP 实现可仍是空 stub，头不能再出现 Pistache/Folly）。

`pg_blob_iterator.cpp` 若本阶段未改，也要从 `homeobject_homestore` 的 sources 拿掉，否则 `thenValue` / `async_read` 会编不过。

**完成标准**：`homestore_test_pg` / `_shard` / `_blob` / `_misc` 通过。dynamic / GC / scrub / `enable_http` 测试本阶段不跑。

### Phase 3 — snapshot、GC、scrub（工作量最大）

**snapshot_receive_handler.cpp**：`cp_fut` 改为 `co_await cp_mgr().trigger_cp_flush(...)` 或 `shared_awaitable<bool>`；`collect_all_futures` → `when_all`。空 fan-out 必须覆盖：`when_all({})` 会立刻 `co_return` 空 vector。

**gc_manager**（头+实现；含 Folly Promise，不只是容器）

- CHM → `concurrent_flat_map`；再改同一 map 时按 `gc_repl_reqs` 先快照。
- `m_reserved_chunk_queue`：`sisl::BoundedMPMCQueue`。`write` 失败自旋重试直到成功；`read` 失败则 GC 任务返回 `false`。不要 cv blocking 包装。
- 两个 `IOThreadPoolExecutor` → 每 pdev 两条 `std::jthread`（Decision 5）。
- `add_gc_task` / `process_gc_task` / `submit_gc_task` / `gc_task_guard` 上的 `folly::Promise<bool>` / `SemiFuture<bool>` → `value_awaitable<bool>` 或 `task<bool>`，完成规则与 `repl_result_ctx` 相同。
- `async_read` / `async_alloc_write` then 链 → `task` / `when_all`。
- `hs_gc_tests.cpp`：`collectAllUnsafe` → `sync_get(when_all(...))`。

**scrub_manager**

- 队列改 `sisl::MPMCPriorityQueue`；删除本地头、重复单测，以及头文件里残留的 `#include <folly/MPMCQueue.h>`。
- 去掉两个 Folly 线程池。
- Promise → `value_awaitable` + `task`；取消用错误值 complete。
- `when_all` 聚合 `expected`。

**完成标准**：`homestore_test_gc`、shard-race、`FetchDataWithOriginatorGC`、`homestore_test_scrubber` 通过。dynamic 测试 **仅** `enable_http=false` 的 ReplaceMember / BaselineResync / restart；CMake 上把 `homeobj_fixture_http.hpp` 从 dynamic 目标拿掉（或 `#if`），直到 Phase 4。

### Phase 4 — HTTP（服务端 + 测试客户端 + fixture 路由）

- `hs_http_manager.hpp/.cpp`：Pistache → httplib；`EvictingCacheMap` → `sisl::LruMap`（容量 100，外锁保留）。`hs_http_manager.cpp` 里不止 `trigger_gc_for_pg`（`InlineExecutor`、`thenError`、`collectAllUnsafe` 聚多 PG），全部改成 `task` + `when_all` + `detach_then`。
- `homeobj_fixture_http.hpp`：Pistache client → `httplib::Client`（`get`/`post`/`del` 同步即可）。
- `hs_repl_test_helper.hpp`：`start_http_server` / `/metrics` 改为 `sisl::http_method` + httplib handler，去掉 `Pistache::Rest::Request`。
- CMake 把 HTTP helper 加回 dynamic 目标。

**完成标准**：`enable_http=true` 的 dynamic 路径通过；`rg -i pistache HomeObject --glob '!docs/**'` 为空。

### Phase 5 — 测试夹具扫尾与发布

- `hs_repl_test_helper.hpp`、`test_package.cpp`、`homeobj_fixture.hpp`、各 `hs_*_tests.cpp`：无 `.get()` on SemiFuture，无 `hasError()`。
- `rg folly` / `rg -i pistache` 仅注释与 docs。
- CI Debug+Sanitize 与 Release 跑完整 `add_test`（已 `RUN_SERIAL`）。
- README / changelog：C++23、无 Folly、`AsyncResult` 是 coroutine。
- 下游 10 行迁移：`sync_get` / `co_await`；`if (!r)`。
- sisl 14.9、homestore 8.3 发布后再发 HomeObject 5.x。

---

## 文件级修改清单

### 构建

| 文件 | 修改 |
|---|---|
| `conanfile.py` | 5.0.0、cppstd 23、sisl 14.9、homestore 8.3、Conan 2、无 folly/pistache |
| `CMakeLists.txt`、`src/CMakeLists.txt`、`src/lib/CMakeLists.txt` | C++23；cast compat；`HOMEOBJECT_MEMORY_ONLY` |
| `.github/workflows/*` | 先编上游或指向 `@oss/dev` |

### 公开 API / core / memory

| 文件 | 修改 |
|---|---|
| `src/include/homeobject/common.hpp` | 去掉 Folly；`Result` / `AsyncResult` |
| `homeobject_impl.hpp/.cpp` | 删除 `_defer`、executor、`--executor` |
| `blob_manager.cpp` `pg_manager.cpp` `shard_manager.cpp` | coroutine；`std::unexpected` |
| `mem_homeobject.hpp/.cpp` `mem_*_manager.cpp` | `concurrent_flat_map` + coroutine |

### HomeStore backend

| 文件 | 修改 |
|---|---|
| `replication_state_machine.hpp/.cpp` | listener、on_fetch_data、create_snapshot、`repl_result_ctx`、small_vector |
| `hs_homeobject.hpp/.cpp` | 类型/include、`lookup_peer` 手拆 URI |
| `hs_blob_manager.cpp` `hs_pg_manager.cpp` `hs_shard_manager.cpp` | Future + `async_*` + `io_batch*` |
| `hs_cp_callbacks.cpp` | `task<bool>` |
| `index_kv.hpp/.cpp` `pg_blob_iterator.cpp` | expected + `async_read` |
| `snapshot_receive_handler.cpp` | `when_all` + CP |
| `gc_manager.hpp/.cpp` | flat_map、BoundedMPMCQueue（write 重试 / read 失败即失败）、`jthread`、`Promise<bool>` / `add_gc_task` |
| `scrub_manager.hpp/.cpp` | flat_map、sisl 优先队列、线程池、Promise；去掉残留 `MPMCQueue.h` |
| 删除 `MPMCPriorityQueue.hpp` | 改用 sisl |
| `hs_http_manager.hpp/.cpp` | httplib、`sisl::LruMap`、`when_all` / `detach_then`（不止 `trigger_gc_for_pg`） |
| `heap_chunk_selector.h` | include `.hpp` |
| `replication_message.hpp` | `crc.hpp` |

### 测试

| 文件 | 修改 |
|---|---|
| `tests/fixture_app.cpp` `*ManagerTest.cpp` `BlobManagerTest.cpp` | Init、`sync_get`、`when_all` |
| `homestore_backend/tests/hs_repl_test_helper.hpp` `homeobj_fixture.hpp` | Init、`hasError`、`.get()`；`/metrics` 去 Pistache |
| `homeobj_fixture_http.hpp` | Pistache client → `httplib::Client` |
| `hs_gc_tests.cpp` `hs_pg/shard/blob_tests.cpp` `test_homestore_backend_dynamic.cpp` `hs_scrubber_tests.cpp` | `collectAllUnsafe`、`hasError` |
| `test_heap_chunk_selector.cpp` `test_package.cpp` | Init |
| 删除 `test_mpmc_priority_queue.cpp` 与 `MPMCPriorityQueueTest` | 单测归 sisl |

---

## 语义陷阱

1. **Lazy vs eager**：未 `co_await` / `sync_get` / `detach` 的 `task` 不跑。
2. **不要在 iomgr / HTTP 线程上 `sync_get`**。
3. **`when_all` 吞异常**：失败必须在 `expected` / `io_result` 里；`T` 要能默认构造。`when_all({})` 立刻成功并返回空 vector，零 peer / 零任务路径必须测。
4. **`value_awaitable::complete` 是 noexcept**：错误放进 `expected` 再 complete；每条 commit/error/rollback 恰好一次。
5. **`std::expected` 没有 `hasError()` / `hasValue()`**。
6. **`toPgError` 必须吃 `std::error_condition`**，不能只留 `ReplServiceError` 重载。
7. **`sisl::MPMCPriorityQueue::pop()` 阻塞**；reactor 上用 `try_pop()`。不要和 `BoundedMPMCQueue` 搞混。
8. **`BoundedMPMCQueue` 没有 blocking API**。GC **不要** cv 包装：`write` 失败自旋重试到成功（禁止忽略返回值的单次 `write`）；`read` 失败直接让任务失败返回 `false`（禁止当永远成功）。
9. **`concurrent_flat_map` 无 iterator**；`visit_all` 持分片锁，回调内禁止再进同一 map。范例见 `raft_repl_dev.cpp` `gc_repl_reqs` / `clear_chunk_req`，`raft_state_machine.cpp` `iterate_repl_reqs`。
10. **`async_alloc_write` 的 `bool` 已不存在**，传 `false` 会编不过。

---

## Consequences

**正面**

- 与 sisl 14 / HomeStore 8 / iomgr 13 同一异步、错误、HTTP 模型。
- 产品构建不再带 Folly / Pistache。
- `on_fetch_data` / blob get 等控制流比 `thenValue` 链清楚。

**负面 / 风险**

- 公开 API 破坏，下游必须改 `sync_get` / `co_await`。
- GC actor 改为 `jthread` 后，rate limiter 与「reserved 队列空则任务失败」和今日 Folly 池 + `blockingRead` 不同；以 `homestore_test_gc` 为准。
- scrub 并发模型同样会变，用 `homestore_test_scrubber` 对比。
- `visit` 重入会死锁；memory backend 宏必须拆。
- 漏 `detach` / 漏 `complete` 是静默挂起，review 时专项查。

**回滚**

- 改动只进 5.x。失败则产品继续 4.3.x + homestore 7.x + sisl 13 + Folly。不提供双栈。

---

## 验收清单

- [ ] `rg '#include <folly' HomeObject --glob '!docs/**'` 为空
- [ ] `rg 'folly::' HomeObject --glob '!docs/**'` 为空
- [ ] `rg -i pistache HomeObject --glob '!docs/**'` 为空
- [ ] Conan 依赖图无 `folly`、无 `pistache`
- [ ] 仅 include 公开头、无 Folly `-I` 的 TU 能编译
- [ ] `CMAKE_CXX_STANDARD` 为 23；sisl `^14.9`、homestore `^8.3`
- [ ] 无 `_defer`、无 `--executor`、无本地 `MPMCPriorityQueue.hpp` / `MPMCPriorityQueueTest`
- [ ] `memory_test` 通过
- [ ] `homestore_test_pg/shard/blob/misc` 通过
- [ ] `homestore_test_gc`、shard-race、`FetchDataWithOriginatorGC` 通过
- [ ] `homestore_test_scrubber` 通过
- [ ] dynamic：ReplaceMember、BaselineResync、Follower/Leader restart（先 `enable_http=false`，Phase 4 后再跑 `true`）
- [ ] `HOMEOBJECT_MEMORY_ONLY` 在 5.x 默认 OFF
- [ ] Sanitize Debug 无新增 leak（GC/scrub 线程与 task 生命周期）
