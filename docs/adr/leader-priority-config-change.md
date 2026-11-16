# Leader-Priority Raft Config Change

**Status**: Proposed
**Date**: 2026-06-17
**Proposer**: @xiaoxichen

---

## Context

CM (Cluster Manager) needs a way to change which member of a PG's Raft group is the
intended leader. Today the system has no end-to-end path for this: CM's leadership
intent is recorded only at PG creation (and is not even reliably enforced past
creation), and once a PG is running there is no API to relocate leadership without
removing and re-adding members.

This ADR designs the **internal basement** for that API — how a priority change is
proposed through Raft, committed on every replica, and converged into NuRaft's
cluster configuration. The user-facing gRPC schema is intentionally out of scope and
will be designed separately on top of this basement.

### Existing primitives

The following pieces are already present in the stack and frame the design space.

#### NuRaft

`nuraft::raft_server::set_priority(srv_id, prio, broadcast_when_leader_exists=false)`
(`NuRaft/include/libnuraft/raft_server.hxx:362-386`, impl
`NuRaft/src/handle_priority.cxx:35-108`) returns one of:
- `SET` — on the leader, with no demotion-to-zero: NuRaft clones the cluster_config
  (honoring `uncommitted_config_`), appends a `log_val_type::conf` log entry, sets
  `config_changing_=true`, broadcasts via `request_append_entries()`.
- `BROADCAST` — leader self-demoting to 0 or no live leader: NuRaft sends
  `priority_change_request` messages directly without consensus; the result may
  diverge across nodes (per `NuRaft/docs/leader_election_priority.md`).
- `IGNORED` — call not actionable.

On commit, every node fires `state_machine::commit_config(log_idx, new_conf)`; NuRaft
also calls `state_mgr::save_config(new_conf)` so the new cluster_config is persisted
on each node. `raft_server::get_srv_config(id)` reads from the **committed** config
only (`NuRaft/src/raft_server.cxx:1819-1822`), so polling for verification after a
`SET` is safe.

NuRaft's leader-election priority decay algorithm
(`NuRaft/docs/leader_election_priority.md`) decays the election `target_priority` by
0.8× per election timeout, down to 1. This makes "raft priority 0" effectively
unreachable in elections, which constrains the CM-to-raft priority mapping below.

#### HomeStore

- `raft_leader_priority = 100` (hardcoded constant,
  `HomeStore/src/lib/replication/service/raft_repl_service.h:40`).
- `compute_raft_follower_priority()` ≈ 65 (default), as
  `1 + ceil(100 × 0.8^max_wait_round)` with `max_wait_round=2`
  (`raft_repl_service.cpp:67-75`).
- `RaftReplDev::set_priority(member, prio, trace_id)` exists at
  `raft_repl_dev.cpp:820-832`, but is unwired from any public API and has several
  defects:
  - Treats `BROADCAST`/`IGNORED` results as a generic `NOT_LEADER` error, losing
    semantic distinction.
  - No `commit_quorum` plumbing.
  - No `wait_and_verify` polling.
  - No `retry_when_config_changing` wrapping (so single-shot failure on transient
    `CONFIG_CHANGING`).
- `RaftReplDev::flip_learner_flag` (`raft_repl_dev.cpp:664-733`) is the closest
  existing pattern: leader-only check, `reset_quorum_size(commit_quorum)`,
  `retry_when_config_changing` wrap, `wait_and_check` post-verification.
- `RaftReplDev::clean_replace_member_task` (`raft_repl_dev.cpp:735-782`) is the model
  for proposing a custom `HS_CTRL_*` journal entry from inside HomeStore.
- `RaftReplDev::reconcile_leader` (`raft_repl_dev.cpp:2054-2093`) is the existing
  leader-rebalance entry point. Today it reads NuRaft's live cluster_config and
  yields leadership to whichever peer has the highest committed priority. It has no
  notion of CM intent — it blindly trusts whatever priorities NuRaft happens to
  hold.
- `RaftStateMachine::commit_config(log_idx, new_conf)`
  (`raft_state_machine.cpp:223`) calls `RaftReplDev::handle_config_commit`, which
  bumps lsn and **discards** `new_conf`. There is no listener hook today through
  which HomeObject can learn about a NuRaft-auto-generated conf log commit.

#### HomeObject

- `PGMember.priority` is `int32` with documented semantics `< 0` Arbiter / `== 0`
  Follower / `> 0` Follower-or-Leader (`pg_manager.hpp:34`). In practice CM only
  ever sets `0` (follower) or `1` (intended leader); the Arbiter semantics are not
  implemented.
- `PG sb`'s `pg_members[i].priority` is written by `on_pg_start_replace_member`,
  `on_pg_complete_replace_member`, and `update_membership`; re-read at recovery
  (`hs_pg_manager.cpp:964`); surfaced via `get_stats()` and
  `PGInfo::is_equivalent_to`.
- `to_pg_member` / `to_replica_member_info` (`hs_pg_manager.cpp:326-340`) are
  identity-priority pass-throughs.
- `HSHomeObject::HS_PG::yield_leadership_to_follower`
  (`hs_pg_manager.cpp:1057-1094`) reads peer priorities from NuRaft's live
  replication status, not from PG sb.
- `HSHomeObject::_create_pg` (`hs_pg_manager.cpp:85-163`) calls
  `hs_repl_service().create_repl_dev(replica_set_uuid, peers)` where `peers` is a
  bare `std::set<peer_id_t>` (UUIDs only — no priorities reach the replication
  layer at this point). CM-supplied PGMember priorities reach PG sb only later
  via the `CREATE_PG_MSG` payload commit.
- `RaftReplService::create_repl_dev` (`raft_repl_service.cpp:375-413`) hardcodes
  the proposer at `raft_leader_priority=100` (implicit) and every other member at
  `compute_raft_follower_priority()`. CM intent does not influence NuRaft
  cluster_config at create time.

### CM-to-Raft priority mapping

Two priority value spaces exist:

| Space | Values | Source |
| --- | --- | --- |
| CM intent | `0` (follower), `1` (intended leader) | proto `pg.Member.priority` |
| NuRaft cluster_config | `100` (leader-grade), `compute_raft_follower_priority()` (~65, follower-grade) | NuRaft `srv_config.priority_` |

No mapping function exists between them today; `_create_pg` discards CM intent
entirely, and only the routing of the `create_pg` RPC at the CM-intended leader node
makes the runtime topology line up with CM intent (since the proposer becomes the
implicit leader at priority 100). This is CM-design discipline rather than enforced
invariant.

### Divergence at `replace_member`

`PgMemberChange` is by CM design a pure topology swap; CM does not express
leadership intent through it (CM uses the separate `CmLeaderChange` RPC for that).
The pg.Member proto field is present but is not meaningfully populated for
`PgMemberChange` requests. As a result:

- HomeObject `_replace_member` (`hs_pg_manager.cpp:286-323`) builds
  `out_replica` with only `id` set (priority defaults to `0`) and
  `in_replica.priority` defaulting to `0` (proto3 default).
- HomeStore `RaftReplDev::replace_member` Step 4 (`raft_repl_dev.cpp:351-361`)
  overrides the CM-supplied priority for NuRaft:
  `member_to_add.priority = out_srv_cfg.get()->get_priority();`
  This is **correct** for NuRaft — it preserves the runtime topology, which is
  what CM wants from a pure swap.
- `on_pg_start_replace_member` (`hs_pg_manager.cpp:362`) writes the CM-supplied
  priority (`0`) to PG sb for the in member. This is **incorrect**: if out was
  the intended leader (PG sb priority `1`), the in member does not inherit that
  designation in PG sb, so PG sb loses leader intent for the PG entirely.

This bug does not manifest today because no consumer reads PG sb priority for
leadership decisions. It will manifest once `reconcile_leader` is taught to drive
NuRaft from PG sb (Decision §4 below).

---

## Problem Statement

Design the basement that lets CM change Raft leader priority such that:

1. CM's leadership intent for a PG is recorded durably on **every replica**, so it
   survives leader failover and node restart.
2. NuRaft's cluster_config eventually converges to match CM's intent, so the
   intended leader is the one that wins elections.
3. The basement is composable with existing `replace_member` / `flip_learner_flag`
   flows and does not silently break their semantics.
4. Transient divergence between durable intent and live NuRaft state is bounded and
   recoverable without operator intervention.

This basement is the substrate for a separate gRPC API (`CmLeaderChange` →
`PGManager::set_leader_priority`, schema TBD).

---

## Decision

PG sb is the **source of truth for CM's leader intent**. NuRaft's cluster_config is
derived/reconciled state. A new HomeStore control journal entry,
`HS_CTRL_SET_LEADER_PRIORITY`, propagates intent to every replica's PG sb; an
enhanced `reconcile_leader` is the engine that converges NuRaft to match PG sb.
Transient PG-sb-vs-NuRaft divergence is tolerated; reconvergence is best-effort and
re-attempted on every subsequent reconcile trigger.

### Mapping

| CM PGMember.priority | NuRaft srv_config priority |
| --- | --- |
| `1` (intended leader) | `raft_leader_priority` = 100 |
| `0` (follower) | `compute_raft_follower_priority()` ≈ 65 |

Both ends of the map are leader-eligible in NuRaft's election; the 100 vs 65 gap
makes the `1` member the natural winner under NuRaft's decay algorithm. We do not
map to NuRaft priority 0 (effectively excluded from elections, breaks failover when
the intended leader dies).

### Piece 1 — `HS_CTRL_SET_LEADER_PRIORITY` journal entry

A new `journal_type_t::HS_CTRL_SET_LEADER_PRIORITY` value. The header payload
carries:
- `task_id` (string) — for caller-side idempotency and tracking, matching the
  pattern of `HS_CTRL_START_REPLACE` / `HS_CTRL_COMPLETE_REPLACE`.
- `target_leader_uuid` (peer_id_t) — the single member CM designates as the intended
  leader.

The entry has no user-data body.

### Piece 2 — Propose path

A new `RaftReplDev::set_leader_priority(task_id, target_leader_uuid, commit_quorum,
trace_id)` modeled on `clean_replace_member_task` (`raft_repl_dev.cpp:735-782`):
- Leader-only precondition; `commit_quorum==0` requires `is_leader()`.
- Validates `target_leader_uuid` is a current PG member (read from PG sb).
- Constructs the `repl_req_ctx`, calls `m_state_machine->propose_to_raft(...)`.
- Returns async success when the entry is accepted; commit-side reconciliation runs
  in the on_commit handler.

The matching service-level wrapper is `RaftReplService::set_leader_priority(...)`
mirroring `flip_learner_flag` shape.

### Piece 3 — `on_commit` handler

Fires on every replica via the `ReplDevListener` callback for the new journal type.
On every replica:
1. Look up the PG by group_id.
2. Walk `pg_sb_->get_pg_members_mutable()`. For each member: if `id ==
   target_leader_uuid` set `priority = 1`, else set `priority = 0`.
3. Persist the PG sb via `hs_pg->pg_sb_.write()` (same pattern as
   `on_pg_start_replace_member`).
4. On the leader only, additionally invoke `reconcile_leader` (Piece 4) so the
   operation feels complete to the caller by the time `set_leader_priority`'s async
   future resolves.

No `on_precommit` handler is needed: the operation only mutates PG sb in
`on_commit`. No rollback handler is needed: uncommitted entries leave PG sb
untouched, so there is nothing to undo.

### Piece 4 — Enhanced `reconcile_leader`

`RaftReplDev::reconcile_leader` (`raft_repl_dev.cpp:2054-2093`) becomes the
PG-sb-driven convergence engine.

**Trigger sources** (all existing or added by this work):
- Inline at the tail of the `HS_CTRL_SET_LEADER_PRIORITY` on_commit handler on the
  leader.
- Existing per-PG and all-PG HTTP/admin endpoints.
- A future periodic background reconciler thread (phase 2).

**New logic on the leader**:
1. Derive desired NuRaft priorities from PG sb using the mapping table above.
2. For each member whose live NuRaft priority
   (`raft_server->get_srv_config(srv_id)->get_priority()`) differs from desired,
   call `raft_server->set_priority(srv_id, desired)` wrapped in the existing
   `retry_when_config_changing()` helper (`raft_repl_dev.cpp:794-808`). Iterate in
   PG sb member order — ordering between demotions and promotions is irrelevant
   because transient states like `{100, 100, 65}` and `{65, 65, 65}` both leave the
   current leader in charge and converge correctly.
3. After convergence, if the current leader (self) is not the target, invoke
   `raft_server->yield_leadership(false /*immediate*/, target_srv_id)`.

**Follower-side logic** (unchanged): if my NuRaft priority becomes 100,
`request_leadership()`; else wait. This branch already exists in
`reconcile_leader`.

### Piece 5 — `replace_member` defense-in-depth fix

In `HSHomeObject::on_pg_start_replace_member` (`hs_pg_manager.cpp:342-377`), before
emplacing `to_pg_member(member_in)` into `pg->pg_info_.members`:

1. Look up the existing entry for `member_out.id` in `pg->pg_info_.members`.
2. If found, copy its `priority` onto the in `PGMember` (overriding the CM-supplied
   default-`0`).
3. Then proceed with the existing emplace + `sb_members` rewrite loop.

This propagates the "intended leader" designation in PG sb across a member swap,
mirroring what NuRaft already does for the runtime priority at
`raft_repl_dev.cpp:361`. Without this fix, swapping out a member that holds the
`priority=1` row in PG sb leaves the PG with no intended leader in PG sb, and
`reconcile_leader` would then push the cluster toward all-followers and trigger
election churn.

`on_pg_complete_replace_member` needs no change because
`pg->pg_info_.members.emplace(in)` is a no-op when the in member is already in the
set (added in `on_pg_start_replace_member`), so the inherited priority is preserved
through `update_membership`. Recovery is also unaffected because
`HS_CTRL_START_REPLACE` is in the raft log and is replayed on recovery — start
always runs before complete.

---

## Failure-mode walkthrough

| Failure point | Outcome |
| --- | --- |
| Leader crash before Piece 2 propose returns | CM retries (idempotent on `task_id`). No state change. |
| Leader crash after journal entry committed on majority but before on_commit handler runs on the leader | The entry is replayed during recovery; on_commit fires; new leader's reconcile_leader converges NuRaft. |
| Leader crash between on_commit (Piece 3) and reconcile_leader completion (Piece 4) | PG sb is durable on majority; NuRaft cluster_config may be partly unconverged. Any subsequent reconcile_leader trigger (HTTP, CM retry, future periodic thread) re-derives desired state from PG sb and finishes the job. Transient inconsistency is bounded by reconcile trigger cadence. |
| Single `set_priority` call inside Piece 4 fails with `CONFIG_CHANGING` | `retry_when_config_changing` retries up to 5 × 500ms. If still failing, the loop continues with the next member; the failed one is picked up by the next reconcile trigger. |
| New leader elected mid-convergence | New leader's PG sb has the same intent (committed entry replicated to majority). When the new leader is next asked to `reconcile_leader`, it resumes convergence. |
| `replace_member` swaps the current intended leader while a `HS_CTRL_SET_LEADER_PRIORITY` is in flight | Both entries serialize through the raft log. Whichever commits later wins for PG sb. Piece 5 propagates leader designation across the swap, so an in-flight set_priority on a since-removed member is benign — reconcile_leader at next trigger will read the updated PG sb (with the in member holding priority=1) and converge from there. |

---

## Out of Scope (Phase 2 candidates)

- **Periodic background reconciler thread**. Phase 1 relies on
  on-commit-tail + existing HTTP/CM/replace_member triggers. A timer-driven
  reconciler closes the long-tail window without requiring CM retries.
- **Fixing the `create_pg`-time priority gap**
  (`RaftReplService::create_repl_dev` takes only UUIDs). Today this is masked by
  CM's discipline of routing `create_pg` at the intended-leader node. Tightening
  this to an enforced invariant requires extending the `create_repl_dev` interface
  and `create_group` path to carry per-member priorities.
- **Decoupling leader intent from per-member priority** in PG sb. A top-level
  `intended_leader_uuid` field on PG sb would be a cleaner data model than
  encoding intent in per-member priority rows, but requires PG sb schema
  versioning. Phase 1 keeps the per-member encoding (with Piece 5's fix to keep
  it consistent across membership changes).
- **Arbiter member semantics** (`PGMember.priority < 0`). Documented in
  `pg_manager.hpp:34` but never implemented; orthogonal to this work.

---

## Alternatives Considered

### Alternative A — Lean on NuRaft's auto-generated conf log + new listener hook

Let CM's intent flow as a `raft_server::set_priority` call directly on the leader;
NuRaft's `log_val_type::conf` entry handles consensus; add a new
`ReplDevListener::on_config_committed(new_conf)` so HomeObject can mirror the new
priorities into PG sb on commit on every replica.

Rejected because:
- The leader is the *only* node that knows CM's intent until commit; if the leader
  fails mid-flight the new leader has no record of what was being attempted, so
  any partial NuRaft state is opaque.
- BROADCAST semantics (leader self-demote-to-0) are inherently inconsistent across
  nodes per NuRaft docs and would silently corrupt PG sb.
- Adding a new listener hook to HomeStore's existing `commit_config` path is a
  larger API surface change than introducing one custom journal type.

### Alternative B — Drop PG-sb leader priority entirely; use NuRaft cluster_config as the only source

Treat NuRaft's cluster_config as authoritative; remove `PGMember.priority` from PG
sb; CM queries NuRaft directly for leadership intent.

Rejected because:
- NuRaft cluster_config is per-node state; recovering a node from scratch via
  baseline resync requires the leader's cluster_config to be intact, which it is
  not always at the moment CM wants to express new intent.
- Loses the audit trail of "what did CM intend" separate from "what is NuRaft
  currently doing".
- Doesn't compose with the existing PG-sb-based recovery and inspection flows.

### Alternative C — Fix `replace_member` Step 4 in HomeStore to honor CM-supplied priority

Remove the override at `raft_repl_dev.cpp:361` and pass `member_in.priority`
through to NuRaft directly.

Rejected because:
- CM does not populate `member_in.priority` for `PgMemberChange` (by design — CM
  uses the separate `CmLeaderChange` RPC for leadership intent).
- The current inheritance behavior is the correct behavior for a pure topology
  swap; removing it would break the common case to no benefit.
- Even if CM started populating it, applying the new priority at Step 4 (before
  the in member has finished baseline resync) would cause NuRaft election churn
  during catch-up. Piece 5 + Piece 4 together apply CM intent only after the swap
  is fully complete, avoiding that window.

---

## Open Implementation Decisions (deferred to code stage)

- **Multi-member priority update inside Piece 4**: N sequential
  `raft_server::set_priority()` calls (1 conf log entry each, retry-on-`CONFIG_CHANGING`
  per call) vs. directly constructing a cluster_config with all priority changes
  and `append_entries` a single hand-built `log_val_type::conf` entry. The latter
  produces one log entry total and avoids the retry loop but bypasses NuRaft's
  `config_changing_` / `uncommitted_config_` bookkeeping. Trade-off to revisit
  when implementing Piece 4.
- Whether to add `wait_and_check` polling after each `set_priority` call to fully
  serialize on commit (matching `flip_learner_flag`'s `wait_and_verify=true`) vs.
  relying on `retry_when_config_changing`'s backoff alone.
- Whether `get_replication_status` filters dead peers (relevant for whether the
  pre-existing `reconcile_leader` behavior actually misfires for unreachable
  peers; informs whether Piece 4 needs to skip those peers explicitly).
