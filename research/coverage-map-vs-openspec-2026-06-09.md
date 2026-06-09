---
topic: "Coverage map — report findings vs in-flight OpenSpec changes"
date: "2026-06-09"
sources: "research/technical-pg-redis-stability-performance-2026-06-09.md + openspec/changes/{fix-crash-safety,harden-shared-memory-error-paths,surgical-list-writeback}"
---

# Coverage Map — Findings vs OpenSpec Changes

Cross-references every finding in the stability/performance report against the three open OpenSpec changes. Verified by reading each change's proposal/design/tasks/specs **and** confirming against the current source state observed by the analysis agents.

## Status of each change (confirmed against current source)

| Change | Artifacts | Implementation state | Evidence |
| --- | --- | --- | --- |
| **harden-shared-memory-error-paths** | proposal, design, tasks, 2 specs | **IMPLEMENTED** (core code landed; tests/verification deferred) | tasks.md core items `[x]`; agents observed the fixes in current source (hash value-len cap at `shared_hash.c:279`, DSA rollback `PG_TRY` in `populate_shared_from_scratch`, `PG_TRY` in `pg_redis_shmem_dsa`, bounded `find_bucket`, drain `PG_CATCH` frees DSA spill) |
| **fix-crash-safety** | proposal, design, tasks, 3 specs | **DESIGNED ONLY — not implemented** | tasks.md 0/checked; agents found every target bug still live (self-deadlock, SPI under `startup_lock` at `persistence.c:1008`, old `fetch_add`-before-READY ring at `dirty_ring.c:157`, `HASH_PARTITION` still set) |
| **surgical-list-writeback** | proposal only | **PROPOSAL ONLY** | no design/tasks; agent saw full list release+rebuild still at `shared_store.c:582` |

## Coverage of report findings

Legend: ✅ Covered · 🟡 Partial · ❌ Not covered. "Impl?" = is the covering code already in the tree.

### Critical

| # | Finding (file:line) | Status | By change | Impl? | Notes |
| --- | --- | --- | --- | --- | --- |
| C1 | Shared lock-order inversion + self-deadlock on cold-load first-DSA-touch (`shmem.c:223`, `persistence.c:1008`) | ✅ Covered | fix-crash-safety (D1 eager DSA attach, D3 CAS-loaded no-LWLock cold load, D5 drop HASH_PARTITION) | ❌ designed only | This is fix-crash-safety's centerpiece. Implement it and C1 is resolved. |
| C2 | Async "single consumer" not enforced — sync_flush producers drain concurrently with BGW → blind `read_head` write → wedge/dup/torn (`dirty_ring.c:197-235`) | 🟡 Partial | fix-crash-safety (moves SPI out from under lock; two-phase reservation fixes the **producer**/`write_head` side) | ❌ designed only | **Gap:** the two-phase protocol fixes producer races, but no change adds a **consumer-side lock** to serialize concurrent drainers (BGW + sync_flush producer) on `read_head`. The blind-write race remains unaddressed. |
| C3 | `read_head`/slots freed before persisting xact commits → batch loss on abort/crash (`dirty_ring.c:226-232`) | 🟡 Partial | fix-crash-safety spec **requires** "BGW advances read_head only after durable write commits" | ❌ designed only; harden explicitly **deferred** the data-loss aspect | Requirement exists in fix-crash-safety's spec but is **not** in the design's task list as a two-phase drain; the implemented harden change does the opposite (advances first, frees DSA only). Verify the implementation actually does post-commit advance. |
| C4 | No `SubXactCallback` → savepoint / PL/pgSQL EXCEPTION rollback persists discarded data (all modes) | ❌ Not covered | — | — | **No change touches subtransaction handling.** fix-crash-safety uses `BeginInternalSubTransaction` for the *drain*, which is unrelated to tracking user savepoints. Top gap. |

### High

| # | Finding (file:line) | Status | By change | Impl? | Notes |
| --- | --- | --- | --- | --- | --- |
| H1 | Session `run_flush` no `PG_TRY` → stuck `in_dirty_set` → silent durability loss (`persistence.c:169,303`) | ❌ Not covered | — | — | All three changes are shared/async-focused; the session-mode pre-commit flush error path is untouched. |
| H2 | Shared scratch-copy RMW loses concurrent updates — INCR + lists (`pg_redis.c:537-575,949`) | 🟡 Partial | surgical-list-writeback (applies list deltas in place instead of rebuilding from stale scratch → less clobber) | ❌ proposal only | **Partial for lists only, and only as a side effect of a perf change.** INCR/string lost-update is **not** addressed; the read-release-mutate-reacquire gap remains. The real fix (single-lock atomic paths like HSET) is not planned for strings/INCR. |
| H3 | `lock_partitions` not power-of-two (`pg_redis.c:255-264`, `shmem.c:157`) | ✅ Covered | fix-crash-safety (D5 drops `HASH_PARTITION` → power-of-two requirement disappears; external locks tolerate any count) | ❌ designed only | Resolved by removing HASH_PARTITION rather than by a check_hook. The report's check_hook suggestion becomes moot. |
| H4 | Shared **list** value_len uint16 truncation > 64 KB (`shared_list.c:36`) | ❌ Not covered | — | — | harden fixed the **hash** path (same class of bug) and implemented the "reject > UINT16_MAX" decision — but **did not apply it to the list path.** Clear, isolated gap; same fix pattern. |
| H5 | Post-rollback in-memory inserts never removed → GET returns rolled-back values (`persistence.c:202`) | ❌ Not covered | — | — | No abort-reconciliation in any change. Related to C4. |
| H6 | Cached SPI plans / static load flags no teardown on DROP/DISCARD (`persistence.c:51-56`) | ❌ Not covered | — | — | fix-crash-safety changes the load flag to an atomic state machine for cold-start coordination, but does not add plan/flag teardown on extension drop. |
| H7 | Async over-claim past capacity → spurious slot-stuck ERROR (`dirty_ring.c:130-157`) | ✅ Covered | fix-crash-safety (D4 two-phase reservation: full-check then CAS slot EMPTY→WRITING, advance write_head only after READY) | ❌ designed only | The new protocol eliminates over-claim and the ~10 ms spin-ERROR. |
| H8 | DSA spill leak on **publish** error (`dirty_ring.c:315`) | 🟡 Partial | harden covers the **drain-side** spill leak (implemented); fix-crash-safety returns RING_FULL instead of ereport for sync_flush | harden: ✅ / fix-crash-safety: ❌ | **Gap:** the **producer** encode→publish pair is still not wrapped in `PG_TRY` to free `dsa_payload` when publish ERRORs in `'block'` mode after the spin budget. |

### Medium / Performance (condensed)

| # | Finding | Status | By change | Notes |
| --- | --- | --- | --- | --- |
| M1 | FLUSHALL SPI TRUNCATE + DSA frees under all partition locks | 🟡 Partial / **conflict** | harden D3 **accepts** it; fix-crash-safety "no SPI under any LWLock" invariant **forbids** it | The two changes disagree. Must reconcile when both land (see Conflicts). |
| M2 | Flush/drain not in dedicated MemoryContext (~2× peak) | ❌ Not covered | — | |
| M3 | HSET→HDEL→HSET tombstone ordering divergence | ❌ Not covered | — | |
| M4 | `version` never reloaded (rewinds durable counter) | ❌ Not covered | — | fix-crash-safety *uses* version for sync_flush revalidation but doesn't fix the reload rewind. |
| M5 | Snapshot not binary-safe (JSON on raw bytes) | ❌ Not covered | — | |
| M6 | Shared-mode abort not reconciled | ❌ Not covered | — | Related to C4/H5. |
| M7 | No SIGUSR1/procsignal handler in bgworker | ❌ Not covered | — | fix-crash-safety touches bgworker (DB GUC) but not signals. |
| M8 | No job-overlap guard (FOR UPDATE SKIP LOCKED) | ❌ Not covered | — | |
| M9 | Documented async crash window wrong (ring-fill, not flush_interval) | ❌ Not covered | — | Doc fix. |
| M10 | INCR overflow UB (`pg_redis.c:571`) | ❌ Not covered | — | |
| P1 | No fast path GET/SET/INCR/EXISTS (materialize double-copy + double-lock) | 🟡 Partial | surgical-list-writeback (lists) + existing surgical_apply_hash (hashes) cover the **writeback** O(N) rebuild | Read-path materialize double-copy and INCR two-lock **not** covered. |
| P2 | Cache storage/persistence mode enum (strcmp 4–6×/cmd) | ❌ Not covered | — | |
| P3 | Raise `lock_partitions` default/cap | ❌ Not covered | — | fix-crash-safety keeps default 16. |
| P4 | Async publish spins ~10 ms under partition lock | ✅ Covered | fix-crash-safety (D2 moves sync_flush drain out of lock; reservation returns RING_FULL not spin) | designed only. |
| P5 | tombstone dedup O(d×k), DatumVec lazy init, TLV stack buf, 1 KB memset, inline key arrays | ❌ Not covered | — | Minor perf. |

## What the changes fix that the report did NOT find

The OpenSpec changes are not strictly a subset of the report — they caught real bugs the agents missed (credit to whoever wrote them):

- **`estimate_max_entries` int overflow at ≥ 2 GB shared memory** → silent clamp to 1024 entries then PANIC on growth (fix-crash-safety D6). The agents read `shmem.c` but did not flag this.
- **`DatumGetByteaPP` leaves inline-compressed TOAST values compressed** → TLV tag check fails, values silently dropped after restart (fix-crash-safety D7 / persistence-collection-tables spec). Missed by the agents.
- **bgworker hardcodes database `postgres`** → infinite FATAL/restart loop on clusters without it (fix-crash-safety D8). The async agent flagged bgworker lifecycle generally but not this.
- **drain `PG_CATCH` leaks SPI nesting / snapshot** (fix-crash-safety D9) — the report noted the DSA side but not the `_SPI_curid` / `PopActiveSnapshot` leak.
- **`find_bucket` unbounded probe loop** defensive guard (harden D7, implemented).

## Conflicts & sequencing to resolve

1. **M1 conflict — FLUSHALL SPI under all locks.** harden-shared-memory-error-paths *accepts* holding all partition locks across the TRUNCATE (admin op, design D3). fix-crash-safety introduces a hard invariant: *no SPI under any pg_redis LWLock* (storage-shared-keyspace spec). These are contradictory. If fix-crash-safety lands, `pg_redis_persistence_flushall`'s TRUNCATE must move out from under the locks — but it isn't currently in fix-crash-safety's task list. **Add it explicitly, or the invariant ships violated.**
2. **shmem.c `pg_redis_shmem_dsa()` overlap.** harden added a `PG_TRY` around the **lazy** attach (implemented). fix-crash-safety **removes the lazy path** (eager attach, D1). fix-crash-safety supersedes harden's task 5 — sequence fix-crash-safety after harden and delete the now-dead lazy `PG_TRY`.
3. **Dirty-ring overlap (C2/C3/H7/H8).** harden hardened the *current* one-phase ring (drain `PG_CATCH` frees DSA). fix-crash-safety *replaces* the reservation protocol (two-phase). Land fix-crash-safety's ring rewrite, then re-verify harden's drain `PG_CATCH` still applies to the new protocol — and **add the missing consumer-side serialization for C2** and confirm C3's post-commit `read_head` advance is actually implemented (the spec requires it; the task list doesn't build it).

## Gaps covered by NO change (suggested roadmap)

Ranked, stability-first. None of these are addressed by any of the three changes:

1. **C4 — SubXactCallback / savepoint rollback** (Critical, all modes). The single most important uncovered gap.
2. **H1 — session `run_flush` PG_TRY** (High, durability wedge).
3. **H2 — shared lost-update for INCR/strings** (High; lists only partially via surgical writeback).
4. **H4 — shared list value_len > 64 KB guard** (High; trivial — mirror the hash fix already in harden).
5. **H5 / M6 — post-rollback & shared-abort reconciliation** (High).
6. **H6 — SPI plan / load-flag teardown** (High).
7. **C2 — consumer-side drain serialization** (Critical; fix-crash-safety must be extended).
8. **Medium integrity:** version reload (M4), snapshot binary-safety (M5), HSET/HDEL ordering (M3), flush MemoryContext (M2).
9. **Medium robustness:** bgworker SIGUSR1 (M7), job overlap guard (M8), INCR overflow (M10), async crash-window doc (M9).
10. **Performance not in any change:** mode-enum caching (P2), read-path/INCR atomic fast paths (P1 remainder), lock_partitions tuning (P3), misc micro-opts (P5).
