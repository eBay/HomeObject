# Scrub FlatBuffers Schema Design

**Status**: Proposed  
**Date**: 2026-04-10  
**Proposer**: Jie  

---

## Context

The scrub manager needs to serialize two categories of messages over the wire between the leader and replicas:

1. **ScrubReq** — leader sends a scrub request to all replicas, telling them what range to check and at what LSN.
2. **ScrubResult** — each replica serializes its local scrub results and sends them back to the leader.

There are three scrub types (`ScrubType`):

| Type | Description |
|---|---|
| `META` |  Check PG metadata, shard existence, and shard metadata integrity |
| `SHALLOW_BLOB` |  Check blob existence (presence list) |
| `DEEP_BLOB` |  Check blob data integrity (hash comparison) |

`META` consolidates the former `PG_META`, `SHALLOW_SHARD`, and `DEEP_SHARD` types. A single `META` request covers a shard range; the response contains one `ScrubResultEntry` per shard, carrying the shard's blob count. The leader derives PG-level aggregates (total shard count, total blob count) directly from the entry vector without a separate PG summary entry.

The choice of FlatBuffers schema structure significantly impacts wire efficiency, schema evolution, and receiver-side dispatch complexity. Two FlatBuffers behaviors are central to this decision:

- **Absent fields**: A scalar or table field not explicitly set during serialization is omitted from the wire buffer entirely. On deserialization, it returns the field's declared default value (0 for integers, false for bools, null for vectors/strings/tables).
- **Default-value elision**: FlatBuffers automatically omits scalar fields whose value equals the declared default. A `ScrubStatus` field with value `NONE = 0` is never written to the buffer, which is free space savings for the common case.

---

## Decision: Single ScrubReq + Single ScrubResult with Generic ScrubResultEntry

The chosen design uses a single request table and a single response table for all three scrub types, exploiting FlatBuffers' absent-field semantics to encode type-specific meaning without proliferating tables.

1. A single `ScrubReq` table for all request types, carrying all correlation and range fields directly 
2. A `scrub_type` field in `ScrubReq` as the type discriminator.
3. A single `ScrubResult` table for all response types. It echoes only `req_id` (for request-response matching) and `issuer_uuid` (for sender identity) — no other request fields need to be retransmitted.
4. A vector of generic `ScrubResultEntry` records in `ScrubResult`, whose `shard_id`/`blob_id` fields carry type-specific identifiers or counters.

### Schema Summary

```
// ─── Common types ─────────────────────────────────────────────────────────────
namespace homeobject;

/// Per-entry scrub outcome.  NONE = 0 → FlatBuffers elides this field for healthy
/// entries; the wire format carries only errors, saving one byte per healthy blob.
enum ScrubStatus : uint8 { NONE = 0, IO_ERROR, MISMATCH, NOT_FOUND }

/// Controls which range fields in ScrubReq are meaningful and how shard_id / blob_id
/// in ScrubResultEntry are interpreted.
enum ScrubType : uint8 { META = 0, SHALLOW_BLOB, DEEP_BLOB }

// ─── Single request table ─────────────────────────────────────────────────────
/// Covers all three scrub types.  The receiver inspects scrub_type to determine
/// which range fields are meaningful.
table ScrubReq {
  pg_id:          uint16;
  /// Randomised per-request identifier used by the receiver to match a response to
  /// the outstanding request and to discard stale responses from a previous scrub
  /// epoch.  The leader generates a distinct req_id for every (replica, request) pair:
  /// two replicas receiving the same scrub round will each see a different req_id,
  /// and successive paging requests to the same replica also use fresh req_ids.
  req_id:         uint64;
  scrub_lsn:      int64;
  scrub_type:     ScrubType;   // discriminator; default = META (0)
  /// UUID of the node that issued this request.  Receivers use this to identify
  /// the request source and send scrub result back to the sender.
  issuer_uuid:    [ubyte:16];
  /// Shard range — set for all scrub types.
  start_shard_id: uint64;
  /// Inclusive lower bound on blob_id within start_shard_id — used as the streaming
  /// resume cursor for SHALLOW_BLOB / DEEP_BLOB requests.  Absent (→ 0) for META
  /// scrub and for the first request of a blob scrub batch.
  start_blob_id:  uint64;
  end_shard_id:   uint64;
  /// Inclusive upper bound on blob_id — absent (→ 0) for META scrub.
  last_blob_id:   uint64;
}
root_type ScrubReq;

// ─── Single response table ────────────────────────────────────────────────────

/// Generic result entry.  shard_id and blob_id carry type-specific identifiers or
/// counters; their meaning depends on the scrub_type from the request context
/// (looked up by the leader via req_id):
///
///   META         → shard_id identifies the shard, blob_id holds the shard's blob_count
///   SHALLOW_BLOB → shard_id identifies the shard, blob_id holds the blob's id
///   DEEP_BLOB    → shard_id identifies the shard, blob_id holds the blob's id
///
/// scrub_result defaults to NONE (0) and is elided for healthy entries.
/// hash is present only for DEEP_BLOB entries whose scrub_result == NONE;
/// absent (→ 0) for META, SHALLOW_BLOB, and for any corrupted blob.
table ScrubResultEntry {
  shard_id:        uint64;
  blob_id:      uint64;
  scrub_result: ScrubStatus;  // default NONE → elided for healthy entries (saves 1 B each)
  hash:         uint64;       // CRC64; present only when DEEP_BLOB and scrub_result == NONE
}

/// Single response table for all three scrub types.
/// Only req_id and issuer_uuid are echoed back — scrub_type and all range fields
/// are already held by the leader in its request context (keyed by req_id)
/// and need not be retransmitted.
table ScrubResult {
  req_id:        uint64;      // echo of ScrubReq.req_id; used for stale-response detection
  /// UUID of the node that produced this result.  The leader uses this to
  /// attribute each ScrubResult to the correct replica.
  issuer_uuid:   [ubyte:16];
  scrub_results: [ScrubResultEntry];
}
root_type ScrubResult;
```

### shard_id / blob_id / hash semantics by ScrubType

| `scrub_type` | `shard_id` | `blob_id` | `hash` |
|---|---|---|---|
| `META` | `shard_id` | `blob_count` | absent |
| `SHALLOW_BLOB` | `shard_id` | `blob_id` | absent |
| `DEEP_BLOB` | `shard_id` | `blob_id` | CRC64 (absent on error) |

`META` produces one entry per shard in the requested range. The leader computes PG-level aggregates (total shard count, total blob count) by counting entries and summing `blob_id` values — no separate summary entry is needed.

### ScrubReq field applicability

| `scrub_type` | `start_shard_id` | `start_blob_id` | `end_shard_id` | `last_blob_id` |
|---|---|---|---|---|
| `META` (first batch req)| 0 | absent → 0 | uint64_max | absent → 0 |
| `META` (other batch req)| set | absent → 0 | uint64_max | absent → 0 |
| `SHALLOW_BLOB` / `DEEP_BLOB` (first batch req) | set | absent → 0 | set | set |
| `SHALLOW_BLOB` / `DEEP_BLOB` (other batch req) | set | set | set | set |

For blob scrub streaming, the leader issues the first request with `start_blob_id` absent (cursor starts at 0). After each follower response, the leader advances the cursor to `{last_returned_shard_id, last_returned_blob_id + 1}` and sets both `start_shard_id` and `start_blob_id` in the next request.

---

## Alternatives Considered

### Option A: Type-specialized message tables (one table per scrub type)

Define a separate FBS table for each combination: `MetaScrubReq`, `ShallowBlobScrubReq`, `DeepBlobScrubReq`, and corresponding result tables.

**Pros:**
- Strict typing: no absent fields, no ambiguous overlap.
- Every field in a message is meaningful for that type.

**Cons:**
- Proliferates tables: 3 req types + 3 result types = 6 root tables across many `.fbs` files.
- Forces the transport layer to know the message type *before* parsing, requiring an out-of-band type tag (exactly what the in-band `ScrubType` field solves).
- Schema evolution requires changes in 6 places when shared fields (like `pg_id` or `req_id`) change.
- The C++ dispatch code becomes a large switch with per-type deserializer calls rather than a single deserializer + switch on `scrub_type`.

**FlatBuffers consideration:** This approach avoids absent-field ambiguity entirely but gains no benefit from FlatBuffers' structural sharing. It sacrifices DRY for type purity.

---

### Option B: FlatBuffer Union for polymorphic payload

Define a FlatBuffers `union` to hold the type-specific payload alongside a common header:

```
union ScrubPayload {
  MetaPayload,
  BlobPresencePayload,   // shallow blob
  BlobHashPayload        // deep blob
}

table ScrubMessage {
  scrub_info: ScrubInfo;
  payload:    ScrubPayload;
}
```

**Pros:**
- Retains a single wire message type while achieving strict type separation for the payload.
- FlatBuffers union dispatch is type-safe.

**Cons:**
- FlatBuffers unions add two fields per union (`payload_type` + `payload`), increasing header size even for simple cases.
- Union tables cannot share memory with other tables, preventing the structural sharing possible with embedded common fields.
- Code for union dispatch (`switch (msg->payload_type())`) is equivalent to switching on `scrub_type`, but with more boilerplate.
- Union semantics in FlatBuffers do not support default values — the union field is either present or null, adding a mandatory null-check path that the current scalar default approach avoids.
- Breaking the natural 1:1 correspondence between "scrub type" and "payload type" makes the schema harder to read.

**FlatBuffers consideration:** Union's `payload_type` is a `uint8` with no meaningful default — it cannot be absent meaningfully. This eliminates the clean "absent = default = NONE" semantic that the current design leverages.

---

### Option C: Inheritance-style nesting (deep extends shallow via embedded table)

Model deep scrub as a superset of shallow scrub by embedding the shallow result table inside the deep result table:

```
table ShallowBlobScrubResult { scrub_info: ScrubInfo; blobs: [BlobKey]; }
table DeepBlobScrubResult    { shallow_map: ShallowBlobScrubResult; results: [BlobHashEntry]; }

table ShallowShardScrubResult { scrub_info: ScrubInfo; shards: [uint64]; }
table DeepShardScrubResult    { shallow_map: ShallowShardScrubResult; problematic_shards: [ShardResultEntry]; }
```

**Pros:**
- Clear semantic layering: deep results include shallow results, which mirrors the conceptual relationship.
- No spurious absent fields — shallow fields are cleanly separated from deep fields.

**Cons:**
- Doubles the table nesting depth. Every deep-scrub deserialization must navigate through an extra indirection (`deep_map->shallow_map()->scrub_info()`).
- FlatBuffers nested table access adds pointer indirection per level, slightly increasing deserialization cost.
- The `scrub_type` discriminator still needs to exist in the transport layer — this approach moves complexity from schema level to the transport/dispatch level.
- Cannot share the same result table across shallow and deep requests from the leader perspective (leader must handle two types of results per granularity).

**FlatBuffers consideration:** The extra nesting provides cleaner absent-field semantics at the cost of indirection. Whether a deep shard map has any `problematic_shards` is cleanly expressed as an absent vector, but this benefit is equally achievable in the chosen design by omitting entries with `scrub_result = NONE`.

---

### Option D: Per-granularity result tables (2 response types, 1 request type)

Use a single `ScrubReq` for requests and separate `MetaScrubResult` and `BlobScrubResult` for responses. Each result table has its own entry type with named, type-specific fields:

```
table MetaScrubResultEntry { shard_id: uint64; blob_count: uint64; scrub_result: ScrubStatus; }
table BlobScrubResultEntry { shard_id: uint64; blob_id:    uint64; scrub_result: ScrubStatus; hash: uint64; }
table MetaScrubResult { scrub_info: ScrubInfo; meta_scrub_results: [MetaScrubResultEntry]; }
table BlobScrubResult { scrub_info: ScrubInfo; blob_scrub_results: [BlobScrubResultEntry]; }
```

**Pros:**
- Named fields (`shard_id`, `blob_count`, `blob_id`) make per-entry semantics self-documenting without a shard_id/blob_id convention.
- No implicit field-meaning mapping for the reader to learn.

**Cons:**
- Two separate root response types require two different RPC handlers or an out-of-band type tag, even though `scrub_type` is already carried in `ScrubReq` and available in the leader's request context.
- `issuer_uuid` and `req_id` must be duplicated across both `MetaScrubResult` and `BlobScrubResult`; any correlation field added in the future (e.g., `scrub_epoch`) requires changes in both tables.
- Schema drift: adding a correlation field requires changes in 2 result tables rather than one.

**FlatBuffers consideration:** The transport already carries `scrub_type` inside `ScrubReq`. Splitting the response into two types adds dispatch surface without adding information that isn't already available to the leader.

---

## Rationale for the Chosen Design

### 1. One request table, one response table

Having a single `ScrubReq` and a single `ScrubResult` means the transport layer needs exactly two message handlers, one for each direction. The `scrub_type` field in `ScrubReq` is the sole dispatch point on the request side; on the response side dispatch goes through the `req_id` → request context lookup. The `ScrubType` enum contains exactly three values (`META`, `SHALLOW_BLOB`, `DEEP_BLOB`). `META` consolidates the functionality of separate PG-meta and shard scrub requests, simplifying the protocol without sacrificing coverage.

### 2. Absent fields for type-specific range data are harmless, not ambiguous

`ScrubReq` carries `start_shard_id`, `start_blob_id`, `end_shard_id`, and `last_blob_id`. For `META` scrub, `start_blob_id` and `last_blob_id` are inapplicable and are left absent (deserialise as 0). For the first request of a blob scrub batch, `start_blob_id` is also absent, which the follower correctly interprets as "start from blob 0 within `start_shard_id`". Because `scrub_type` and the request sequence together always tell the receiver which range fields are meaningful, the absent-field default of 0 is never mistaken for an intentional value. This is the intended use of FlatBuffers absent fields: communicate "not applicable" without burning a byte.

### 3. `ScrubStatus.NONE = 0` provides free sparse encoding

For blob and shard scrub results, the overwhelming common case is `scrub_result = NONE` (no error found). Because FlatBuffers elides scalar fields equal to their declared default, healthy entries do not transmit `scrub_result` at all. A shallow blob scan of a 1M-blob PG with zero errors produces a result list where no `scrub_result` field is ever written — saving 1 byte per entry = 1 MB of wire data.

### 4. Generic shard_id/blob_id avoids multiple result entry types

Two named entry structs (`MetaScrubResultEntry` and `BlobScrubResultEntry`) become one `ScrubResultEntry`. The semantic mapping (see table above) is documented once in the schema comments and once in the C++ dispatch code. When the leader receives a `ScrubResult`, it looks up the request context via `req_id` to obtain `scrub_type` and then interprets `shard_id`/`blob_id` accordingly.

### 5. `uint64` hash instead of `[ubyte]`

Using a fixed-width `uint64` rather than `[ubyte]` removes the 4-byte vector-length prefix overhead and avoids a heap allocation on deserialization. CRC64/NVME fits in 8 bytes and offers stronger collision resistance than CRC32 for scrub purposes. If the hashing algorithm changes in the future, the field width can be revisited in a single schema change without altering the shape of `ScrubResultEntry`.

### 6. `req_id` for epoch deduplication; `issuer_uuid` for sender identity

The two fields serve distinct purposes and cannot replace each other:

- **`req_id: uint64`** — a randomly generated per-request identifier. The leader matches incoming `ScrubResult` messages to the outstanding request and discards any response whose `req_id` does not match, eliminating stale responses from a previous scrub epoch. It replaces the earlier variable-length `[ubyte]` UUID used solely for deduplication, at a fixed cost of 8 bytes with no heap allocation.
- **`issuer_uuid: [ubyte:16]`** — the UUID of the sending node. Every node needs to know which peer sent a given message in order to attribute results to the correct replica, enforce membership checks, and detect impersonation. A fixed-size 16-byte array is used instead of the earlier `[ubyte]` to eliminate the 4-byte length prefix and ensure constant-time access.

Both fields appear in `ScrubReq` (so the follower knows who the leader is) and in `ScrubResult` (so the leader knows which replica sent the result).

### 7. `ScrubResult` echoes only `req_id` and `issuer_uuid`

On the response side, the leader already holds every `ScrubReq` field in its outstanding-request context, keyed by `req_id`. Once the leader looks up the context to validate `req_id` against outstanding requests, `scrub_type` and all other fields are immediately available — echoing them back adds no information. The follower therefore echoes only `req_id` (stale-response detection) and `issuer_uuid` (sender identity). All other fields — `pg_id`, `scrub_type`, `scrub_lsn`, and all four range fields — are omitted, saving approximately 51 bytes per sub-response, which compounds across hundreds of sub-responses in the streaming blob scrub protocol.

---

## Consequences

- **Wire efficiency**: Healthy scrub results transmit no `scrub_result` bytes (elided as default = 0). `ScrubResult` echoes only `req_id` (8 B) and `issuer_uuid` (16 B), omitting all other request fields — saving ~51 bytes per sub-response. Deep blob hashes are only serialized when the blob is verified successfully. `uint64` hash avoids the vector-length prefix overhead of `[ubyte]`.
- **Transport simplicity**: One request handler, one response handler. On the request side, dispatch is a `switch` on `ScrubReq.scrub_type`. On the response side, the leader looks up the request context via `ScrubResult.req_id` and reads `scrub_type` from there.
- **Schema evolution**: Adding a new field to `ScrubReq` is localised to one table. Adding a new `ScrubType` value requires only a new entry in the `shard_id`/`blob_id` semantics table and the corresponding C++ handler. The three-value enum (`META`, `SHALLOW_BLOB`, `DEEP_BLOB`) keeps the dispatch switch compact.
- **shard_id/blob_id convention**: The implicit meaning of these fields per `ScrubType` must be maintained in code comments and enforced by code review, not by the schema itself. This is a documentation burden that the per-granularity approach (Option D) avoids.
- **Absent-field risk**: Callers must not interpret absent range fields as "zero ID" without first checking `scrub_type`. This convention must be maintained in code, not enforced by the schema.
