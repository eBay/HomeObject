# HomeObject Blob Index Analyze

**Date:** 2026-05-06
**Purpose:** Reference for blob index sizing and capacity planning

---
## Execution Summary

### Scenario 1: 20TB Data with 2GB Memory Budget

**Configuration:**
- Total data: 20TB
- `io_mem_size`: 2GB
- **Fixed cache pool:** 2GB × 65% = **1.3 GB** (always allocated)

| Blob Size | Blob Count | Index Disk | Hard Memory | Cache Pool | Cache Coverage |
|-----------|------------|------------|-------------|------------|----------------|
| **8 KB**  | 2.56B      | 60.1 GB    | 177 MB      | 1.3 GB     | **2.16%** ⚠️  |
| **256 KB**| 80M        | 1.88 GB    | 5.51 MB     | 1.3 GB     | **69%** ✓      |


---

### Scenario 2: 128TB Data with 2GB Memory Budget

**Configuration:**
- Total data: 128TB
- `io_mem_size`: 2GB
- **Fixed cache pool:** 2GB × 65% = **1.3 GB** (always allocated)

| Blob Size | Blob Count | Index Disk | Hard Memory | Cache Pool | Cache Coverage |
|-----------|------------|------------|-------------|------------|----------------|
| **8 KB**  | 16.4B      | 385 GB     | 1.13 GB     | 1.3 GB     | **0.34%** ⚠️⚠️ |
| **256 KB**| 512M       | 12.0 GB    | 35.3 MB     | 1.3 GB     | **10.8%** ✓    |


---

## Action Items
### Shrink down the size of Index_vdev
#### [HDD SKU]
Now we use 45% of META drive (200GB*45% =90GB) on HDD SKUs. The number is generally correct as worst case we can consume up to 60GB.
Shrinking it down from 90GB to 60GB saves 60MB memory which is negligible.
#### [QLC SKU]
5% of QLC size makes it into 5721.96 GB,  as a result it consumes 18GB memory for allocator.
Changing it to 0.8% based on below calculation, as a result, Index size would be ~1TB with Hard Memory ~3GB.

```
Worst case assuming blob size is 8KB, the Index size should be 0.61% of DataSize

(DataSize/8K)*(4KB/167)*1.01 ==> DataSize *1.01/167 = DataSize * 0.61%.
```
### Configure mem_size for QLC nodes.

For QLC nodes we have ~35GB+ memory for a SM, bumping the app_mem_size allows more index be cached in memory, however,
the performance gap is still subject to performance evaluation.

## Quick Estimation Formulas

**For any blob size and data volume:**

```
1. Blob count (N)       = Data size / Avg blob size

2. Index disk space     ≈ (N / 167) × 4KB × 1.01
                          (1.01 factor accounts for internal nodes)

3. Hard memory          = Index disk / 4096 × 12 bytes
   (Allocator)            (Always resident, non-negotiable)

4. Soft memory          = min(Index disk × 10%, io_mem_size × 65%)
   (Working set)          (Capped by global memory budget)

5. Dirty buffer limit   = io_mem_size × 10%
   (Transient peak)       (Shared across all writes, not index-specific)
```

**Memory Components:**
- **Hard (Allocator):** Always resident in `folly::MPMCQueue`, holds ALL free blocks
    - Independent of `io_mem_size`, scales with index vdev size
    - Formula: `(index_disk / 4096) × 12 bytes`

- **Soft (Working Set):** LRU-managed btree node cache
    - **Capped by:** `io_mem_size × cache_size_percent / 100` (default 65%)
    - **Shared by:** All indexes + data service caches
    - **Not capped by:** Index vdev size

- **Transient (Dirty):** Temporary write buffers
    - **Capped by:** `io_mem_size × dirty_buf_percent / 100` (default 10%)
    - Freed after checkpoint flush

---
## Quick Reference

| Node Type | Fanout | Entry Size | Calculation |
|-----------|--------|------------|-------------|
| **Leaf**     | **167** | 24 bytes | (4096 - 72) / 24 = 167.67 → 167 |
| **Internal** | **125** | 32 bytes | (4096 - 72) / 32 = 125.75 → 125 |

---

## Detailed Breakdown

### Node Structure (4KB nodes)

```
Total node size:     4096 bytes
Header size:           72 bytes (persistent_hdr_t)
Data area:           4024 bytes
```

### Leaf Nodes (Store blob → physical address mappings)

**Entry Structure:**
```cpp
Key:   BlobRoute (shard_id + blob_id)     = 16 bytes
Value: MultiBlkId (physical block addr)    = 8 bytes
Total:                                     = 24 bytes
```

**Capacity:**
```
Fanout = floor(4024 / 24) = 167 entries per leaf
Wasted space = 4024 - (167 × 24) = 16 bytes (99.6% utilization)
```

### Internal Nodes (Store routing keys → child pointers)

**Entry Structure:**
```cpp
Key:   BlobRoute (separator key)           = 16 bytes
Value: BtreeLinkInfo (child pointer)       = 16 bytes
       - bnodeid_t (child node ID)         = 8 bytes
       - link_version (concurrency ctrl)   = 8 bytes
Total:                                     = 32 bytes
```

**Capacity:**
```
Fanout = floor(4024 / 32) = 125 children per internal node
Wasted space = 4024 - (125 × 32) = 24 bytes (99.4% utilization)
```

---

## Key Details

### Type Definitions
```cpp
// From HomeObject
using shard_id_t = uint64_t;   // 8 bytes
using blob_id_t = uint64_t;    // 8 bytes
using bnodeid_t = uint64_t;    // 8 bytes (HomeStore)

#pragma pack(1)
struct BlobRoute {
    shard_id_t shard;  // 8 bytes
    blob_id_t blob;    // 8 bytes
};  // Total: 16 bytes (packed)
```

### Internal Node Keys
- Internal nodes store **actual BlobRoute keys** (same as leaf keys)
- Keys are **separator keys**: the **last key** from the left child subtree
- NOT synthetic/aggregate keys - real blob identifiers

### Index Sizing Formula

For N blobs:
```
Leaf nodes     = ceil(N / 167)
Level 1        = ceil(Leaf nodes / 125)
Level 2        = ceil(Level 1 / 125)
...
Total size     = Total nodes × 4096 bytes
```

**Example:** 1 million blobs
```
Leaf:      5,989 nodes
Level 1:      48 nodes
Level 2:       1 node (root)
Total:     6,038 nodes = 23.59 MB
```

---

## Node Implementation

- **Node Type:** `SimpleNode` (fixed-size key/value pairs)
- **Packing:** Sequential, no per-entry overhead
- **Layout:** `[K₀V₀][K₁V₁][K₂V₂]...` (tightly packed)
- **Alignment:** None - entries packed contiguously

---

## Memory Overhead

For index vdev size S:
- **Allocator overhead:** ~0.2% of S
    - Uses `FixedBlkAllocator` (4KB fixed block size)
    - Maintains all free blocks in memory via `folly::MPMCQueue`
    - Each slot: 12 bytes (4B blk_num_t + 8B atomic sequence)
    - For 10GB index: ~22 MB allocator memory

**Total overhead per blob:** ~24-25 bytes (including tree structure overhead)

---

## Production Scaling Analysis

### Memory Cap Model

**Global Memory Budget:** `io_mem_size` (configuration parameter)
```cpp
// From homestore_decl.hpp
uint64_t io_mem_size() const {
    return (hugepage_size != 0) ? hugepage_size : app_mem_size;
}
```

**Index Working Set Cap:**
```cpp
// From resource_mgr.cpp:179
max_index_cache = io_mem_size × cache_size_percent / 100
                = io_mem_size × 65%  (default from homestore_config.fbs)
```

**Dirty Buffer Cap:**
```cpp
// From resource_mgr.cpp:233
max_dirty_buffers = io_mem_size × dirty_buf_percent / 100
                  = io_mem_size × 10%  (default)
```

**Key Points:**
1. **Global cap trumps index size:** Working set is limited by `io_mem_size × 65%`, NOT index vdev size
    - Example: 10GB index but `io_mem_size = 1GB` → max cache = 650MB (not 6.5GB)
    - LRU evictor (created in homestore.cpp:280) enforces this limit

2. **Allocator memory is separate:** Not counted in cache budget, always required
    - Hard memory = 0.3% of index disk (12 bytes per 4KB block)

3. **Shared budget:** `io_mem_size` serves ALL HomeStore components (index, data, log)
    - Index cache competes with data cache for this budget
    - Multi-index deployments share the same `cache_size_percent` pool

**In the tables below:**
- "Soft Memory" shows 65% of index disk (theoretical maximum)
- Actual working set = `min(index_disk × 65%, io_mem_size × 65%)`
- Configure `io_mem_size` large enough to avoid excessive cache thrashing

---

### Case 1: 256KB Blobs

| Data Size | Blob Count | Index Disk | Hard Memory (Allocator) | Soft Memory (Working Set)* |
|-----------|------------|------------|-------------------------|----------------------------|
| **1 TB**   | 4.0M      | 95.9 MB    | 282 KB                 | 10 MB (10% of index)       |
| **20 TB**  | 80.0M     | 1.88 GB    | 5.51 MB                | 188 MB (10% of index)      |
| **128 TB** | 512.0M    | 12.0 GB    | 35.3 MB                | 1.2 GB (10% of index)      |

\* *Actual = min(shown value, `io_mem_size × 65%`). Configure `io_mem_size` accordingly.*

**Calculation:**
```
Blob count    = Data size / 256KB
Index disk    = ceil(Blob count / 167) × 4KB + overhead (3-level tree)
Hard memory   = Index disk / 4KB blocks × 12 bytes (allocator queue)
Soft memory   = min(Index disk × 10%, io_mem_size × 65%)
```

---

### Case 2: 8KB Blobs (Worst Case)

| Data Size | Blob Count | Index Disk | Hard Memory (Allocator) | Soft Memory (Working Set)* |
|-----------|------------|------------|-------------------------|----------------------------|
| **1 TB**   | 128.0M    | 3.01 GB    | 8.83 MB                | 0.3 GB (10% of index)      |
| **20 TB**  | 2.56B     | 60.1 GB    | 177 MB                 | 6 GB (10% of index)        |
| **128 TB** | 16.4B     | 385 GB     | 1.13 GB                | 38 GB (10% of index)       |

\* *Actual = min(shown value, `io_mem_size × 65%`). Large deployments need substantial `io_mem_size`.*

**Calculation:**
```
Blob count    = Data size / 8KB
Index disk    = ceil(Blob count / 167) × 4KB + overhead (4-5 level tree)
Hard memory   = Index disk / 4KB blocks × 12 bytes (allocator queue)
Soft memory   = min(Index disk × 10%, io_mem_size × 65%)
```

---



## References

- **HomeStore:** `/Users/xiaoxchen/Code/HomeStore`
    - Btree: `src/include/homestore/btree/detail/simple_node.hpp`
    - Block allocator: `src/lib/blkalloc/fixed_blk_allocator.{h,cpp}`

- **HomeObject:** `/Users/xiaoxchen/Code/HomeObject`
    - Index KV: `src/lib/homestore_backend/index_kv.hpp`
    - Blob route: `src/lib/blob_route.hpp`

---

**Last Updated:** 2026-05-06
**Validated Against:** HomeStore commit d9cf5c10, HomeObject current
**Includes:** Production scaling for 1TB-128TB with 256KB and 8KB blob sizes
