# ADR-002: Split-Brain Resolution Refinement

## Status

Proposed (2026-05-19). Incorporates two rounds of agent review.

## Context

`src/v2/analyze/split_brain.rs` resolves split-brain by comparing timelines and checking which primary a replica's `wal_receiver.sender_host` points at. The check is too permissive: a `sender_host` match alone counts as "following," even when the streaming connection is dead and the row is stale.

The tool is run **after** a failover, when the cluster has been left in a stable but degraded state. The class of incident this ADR addresses is **slow fencing**: the old primary was supposed to be demoted/shut down as part of the failover, but the fence didn't take (or hasn't taken yet by the time scans resume). The result is a zombie primary persisting alongside the new one, sometimes accepting writes, sometimes not, depending on whether any replica is still acking for it. Scans observe the residue, not the failover-in-flight.

### Operational definition of "true primary"

The true primary is the one whose **sync quorum is satisfied and that is actively committing**. Under `synchronous_standby_names = 'ANY 1 (A, B)'`, this can be the lower-TL primary if it still has a replica flushing for it and the higher-TL primary's quorum is unsatisfied (e.g. its only available replica is the other one, which is stuck on the old primary).

**Active flushing replica evidence on a lower-TL primary correctly identifies it as the true primary.** The existing `ReplicaOverridesTimeline` variant (proposed renamed to `LowerTimelineHasQuorum` — see §4) codifies this case. The problem is observation: a stale `wal_receiver` row pointing at a no-longer-serving primary looks identical to an actively-streaming one in the current code. The gate added below is the test that separates the two.

### Cluster assumptions

- 1 primary + 2 replicas per cluster (replicas A and B). The resolver assumes this topology; >2 replicas is out of scope for v1.
- `synchronous_standby_names = 'ANY 1 (A, B)'` — quorum is satisfiable by either replica alone
- repmgr-set `application_name` equals the node name
- `wal_sender_timeout = 5min` (300_000 ms). Keepalives are sent at `wal_sender_timeout / 2` ≈ 150 s.
- Scanner role has `pg_read_server_files` (the tool is run by DBAs, so this privilege is in place)

## Case matrix

Three-node inventory: `db001` (was pre-failover primary), `db002` (was replica A, now promoted to new primary on TL=N+1), `db003` (was replica B, still a replica with state varying).

The resolver is invoked only when ≥2 primaries are observed. In post-failover steady state with slow fencing, that means `db001` is a zombie primary (`pg_is_in_recovery()=false` because the fence didn't take) alongside `db002` (true new primary). The interesting variation is in `db003`'s state.

Whether `db001` is actively committing depends entirely on whether `db003` is flushing for it: `synchronous_standby_names = 'ANY 1 (A, B)'` on `db001` is satisfiable by `db003`'s ack (A = `db002`, now the primary itself; B = `db003`). On `db002` (new primary), the same setting is unsatisfiable unless `db003` reattaches: A = itself can't be its own standby, and B = `db003` is stuck elsewhere.

| # | `db003`'s `wal_receiver` | `db003` flushing? | `db001` committing? | True primary (operational) | Verdict + findings |
|---|---|---|---|---|---|
| C-a | `sender_host=db002`, `status=streaming/catchup`, recent receipt | Yes, for `db002` | No (no replica acking it) | `db002` | `Both` (timeline + flushing replica agree) + `BidirectionalFlushingConfirmed(db002, db003)` + `PrimaryQuorumUnsatisfied(db001)` |
| C-b | `sender_host=db001`, `status=streaming/catchup`, recent receipt, `received_tli=N` | Yes, for `db001` | Yes | **`db001`** — flushing-replica evidence makes this correct under the operational definition | `LowerTimelineHasQuorum` + `BidirectionalFlushingConfirmed(db001, db003)` + `PrimaryQuorumUnsatisfied(db002)` |
| C-c | Same as C-b, but `db003`'s `flushed_lsn` is past the TL=N→N+1 fork LSN | Yes, for `db001` | Yes | **`db001`** (same as C-b) | C-b verdict + separate top-level `DivergentReplicaWal(db003, …)` |
| C-d | `sender_host=db001` but `status ≠ streaming/catchup`, OR `last_msg_receipt_time` aged out | No — gate rejects | No (without `db003` acking, `db001` can't satisfy its quorum) | `db002` | `HigherTimeline` (no flushing replica anywhere) + `ReplicaWalReceiverStale(db003, db001)` |
| C-e | `sender_host=db002`, actively flushing for `db002`, but `db001`'s `pg_stat_replication` still has a stale row for `db003` (within `wal_sender_timeout` of disconnect) | Yes, for `db002` | No (`db001`'s claimed replica is actually elsewhere) | `db002` | `Both` (replica's `wal_receiver` is authoritative; `db001`'s stale `pg_stat_replication` row is filtered because the replica-side match fails first) + `BidirectionalFlushingConfirmed(db002, db003)` |
| C-f | `wal_receiver` absent, or `status=stopped/starting`, or blocked on `restore_command` (archive corruption) | No | No (without `db003`, `db001`'s quorum is unsatisfied) | `db002` | `HigherTimeline`; replica-stuck condition surfaced separately (existing archive-failure mechanism) |

The design's correctness rests on three facts visible in the matrix:

1. C-b and C-c are correctly resolved by the renamed `LowerTimelineHasQuorum` variant once the gate has confirmed `db003` is actively flushing for `db001`. The lower-TL primary is the true one *because* a replica is genuinely flushing for it.
2. C-d, C-e, and the inactive subcases of C-f require the gate to *reject* stale/one-sided evidence that the current code accepts. This is what the gate adds.
3. The verdict in C-b/C-c is the same whether the higher-TL primary (`db002`) is quorum-satisfied or not — but flagging `PrimaryQuorumUnsatisfied(db002)` in the findings tells the operator "the failover hasn't completed: the new primary has no replicas yet."

**Gate precedence is asymmetric, not symmetric AND:** the replica's `wal_receiver` is the authoritative side (it names exactly one sender). The primary's `pg_stat_replication` is corroborating only. If the replica-side gate fails for primary X, no amount of primary-side state on X can rescue the match. This is what makes C-e resolve cleanly — `db001`'s stale row is filtered because `db003`'s `wal_receiver` doesn't name `db001`.

Cases the design does **not** attempt to disambiguate from a single scan: scans that overlap an in-progress failover, where streaming connections are still within `wal_sender_timeout` of disconnect. The tool is typically run in stable post-failover state where this isn't an issue; verdicts produced during such windows are capped at `BestEffort` and re-converge once `wal_sender_timeout` elapses.

## Decision

### 1. Flushing-liveness gate

Replace the current `sender_host == primary.ip` match in `build_replica_following_map`. A replica counts as following a primary only when all of:

- **Replica side (authoritative):**
  - `wal_receiver.sender_host` matches the primary. The comparison is `==` against `primary.ip_address.to_string()`; in environments where `primary_conninfo` uses a hostname, this comparison fails. Out of scope for v1; flag as a known limitation. (Current production uses IPs.)
  - `wal_receiver.sender_port == 5432`
  - `wal_receiver.status` ∈ {`"streaming"`, `"catchup"`}. `catchup` is genuinely-following mid-recovery and must not be rejected.
  - `wal_receiver.last_msg_receipt_time` is within `freshness_threshold` of the scan-start timestamp (see below).

- **Primary side (corroborating, only checked if replica side passes):**
  - The primary's `pg_stat_replication` has a row whose `application_name` equals the replica's node name. `application_name == ""` is **rejected** as unmatchable (postgres default when client doesn't set one; matches indiscriminately otherwise).
  - The row's `state` ∈ {`"streaming"`, `"catchup"`}, and explicitly `state ≠ "backup"` (which is a `pg_basebackup` client, not a replication consumer).
  - The row's `reply_time` is within `freshness_threshold` of the scan-start timestamp.

If the replica side passes but the primary side does not, emit `PrimaryDoesNotSeeReplica(primary, replica)` as a finding and do not count the replica as following (avoids endorsing a one-sided claim).

**Freshness threshold derivation per primary:**

```
freshness_threshold = wal_sender_timeout_ms / 2 + 30_000 ms
```

For the production `wal_sender_timeout = 5min`, this yields ~180 s — comfortably above the keepalive cadence of ~150 s, with 30 s of slack for scan jitter. `wal_sender_timeout_ms` is parsed from each primary's `configuration["wal_sender_timeout"]`, which `pg_settings` returns as raw milliseconds with no unit suffix; default 60_000 ms if missing/malformed.

Note that `pg_stat_replication.reply_time` (primary side) and `wal_receiver.last_msg_receipt_time` (replica side) have asymmetric update cadences — primary side updates on `wal_receiver_status_interval` (~10 s), replica side on keepalive (~150 s in our config). A symmetric threshold is generous on the primary side; this is acceptable for v1.

Rejected as gate inputs (kept available as findings only): raw `flush_lsn` freshness (zombie rows hold fresh values until `wal_sender_timeout` fires), `flush_lag` (stops updating on idle clusters), archive recency (under TL fork the two primaries write different filenames; they do not collide).

### 2. Sanity gates → Refuse

Before resolving, check:

- **`pg_control_system().system_identifier` consistency** across ALL nodes (primaries and replicas). A replica with a foreign `system_identifier` indicates a reseed/restore from an unrelated cluster; this is escalation-worthy regardless of split-brain. Mismatch → `Confidence::Refuse` with `SystemIdentifierMismatch(nodes)`.
- **`synchronous_commit` durability** on every candidate primary. The `ANY 1 (A, B)` no-divergence claim depends on the standby actually fsyncing before ack. Refuse if any primary has `synchronous_commit` ∈ {`local`, `off`, `remote_write`, empty}. `remote_write` is included because it does not wait for fsync on the standby. Valid values: `on`, `remote_apply`, `remote_flush`.

In addition to setting `Confidence::Refuse`, **replicas with a `system_identifier` not matching the cluster's reference sysid are excluded from §1 gate input** — their replication evidence is treated as not endorsing any candidate, and they do not contribute to `observed` in the `PrimaryQuorumUnsatisfied` derivation (§4). Exclusion happens **before** `build_replica_following_map`, so excluded replicas never appear as followers in the map.

The **reference sysid** is the sysid agreed by ≥ 2 candidate primaries (i.e. the majority class in the 1+2 topology, where 2 of 2 primaries must agree). If candidate primaries themselves disagree (e.g. db001 sysid=X, db002 sysid=Y, no majority class), **all replicas are excluded** and the resolution falls back to timeline-only with `Confidence::Refuse`.

The `SystemIdentifierMismatch { nodes }` finding's `nodes` payload names every node whose sysid differs from the reference (or, when there's no reference, every primary involved in the disagreement plus every replica).

Exclusion and Refuse are orthogonal: exclusion prevents a foreign-cluster replica from acting as the deciding vote in the default timeline-based pick; Refuse tells the operator not to act on that pick regardless.

Not Refuse-worthy:
- `synchronous_standby_names` inconsistency across primaries — surface as `SyncStandbyNamesDiverged` finding, do not refuse.

### 3. Confidence states

Add a `Confidence` field to `SplitBrainInfo`:

```rust
enum Confidence {
    BestEffort,    // single-pass scan; gate passed; verdict is internally consistent
    Conflicting,   // signals partially contradict (verdict still chosen)
    Refuse,        // sanity gate failed; verdict not actionable
}
```

`Verified` is deliberately omitted from v1. Promoting `BestEffort` to `Verified` requires a two-pass stability check (deferred — §6).

**`Refuse` vs `Indeterminate`:** these are different axes.
- `Confidence::Refuse` means *the tool declines to interpret the evidence* because a sanity gate failed. The `resolution` field still carries the timeline-based pick for completeness, but downstream consumers should treat it as not-actionable.
- `SplitBrainResolution::Indeterminate` (kept) means *the evidence itself is inconclusive* (e.g., equal timelines with no replica evidence). Confidence is then `BestEffort`.

### 4. Findings list and variant rename

Add `findings: Vec<SplitBrainFinding>` to `SplitBrainInfo`. Order: sanity-gate failures, then contradictions, then corroboration. Cap at ~5 surfaced items.

**Rename `SplitBrainResolution::ReplicaOverridesTimeline` to `LowerTimelineHasQuorum`.** The new name describes the outcome (which primary won and why) rather than the mechanism (which signal beat which). Operators reading the verdict at 3 AM should not have to translate "replica overrides timeline" into "the new primary is quorum-blocked, fence the wrong thing and you lose."

v1 finding categories:

- `SystemIdentifierMismatch { nodes }` — sanity gate
- `SynchronousCommitWeakened { primary, value }` — sanity gate
- `SyncStandbyNamesDiverged { primaries }` — informational, not Refuse
- `ReplicaWalReceiverStale { replica, claimed_sender }` — gate rejected stale replica-side evidence
- `PrimaryDoesNotSeeReplica { primary, replica }` — one-sided claim rejected
- `BidirectionalFlushingConfirmed { primary, replica }` — positive corroboration
- `ReplicaInCatchup { replica, primary }` — informational, gate passed
- `PrimaryQuorumUnsatisfied { primary, required, observed }` — derivation rule below

**Derivation rule for `PrimaryQuorumUnsatisfied`:**

1. Parse `synchronous_standby_names` on the primary into `{ method, count, members }` (e.g. `ANY 1 (A, B)` → method=ANY, count=1, members={A, B}). Treat unparseable as method=ANY, count=∞ (defensive: emit no finding rather than a wrong one).
2. Build the set of replicas that **bidirectionally-gate-pass** for this primary (full §1 gate, both sides), excluding replicas filtered by §2 (foreign sysid).
3. `observed = |members ∩ gated_followers|`.
4. Emit if `observed < count`.

**Required for `LowerTimelineHasQuorum`:** when the resolution is `LowerTimelineHasQuorum`, the higher-TL (stale) primary by definition has zero gated followers (that's the precondition for the variant firing), so `observed = 0 < count` always holds. Implementations MUST emit `PrimaryQuorumUnsatisfied` for the stale primary in this case — the short-string contract (item 3) renders this finding inline, and without it the verdict text degenerates to the paradox the rename was supposed to fix.

**Short-string contract for `format_reason`:**

Operator-facing output (`reason.short`) must surface enough information for incident triage. The following are mandated, not polish — without them the design has no operational effect even when the resolver is internally correct:

1. **`Confidence::Refuse` overrides the resolution variant in the short string.** Lead with `REFUSE/` and name the failed sanity gate (e.g. `REFUSE/SplitBrain: system_identifier mismatch (db003 vs db001/db002)`). The resolution-variant text MUST NOT appear when Refuse fires, to prevent operators from acting on a winner pick that is not actionable. **Carve-out:** `DivergentReplicaWal` (item 4) is orthogonal to whether the timeline-pick is actionable; if both Refuse and DivergentReplicaWal apply, the rebuild instruction still surfaces, concatenated after the Refuse text (e.g. `REFUSE/SplitBrain: system_identifier mismatch (db003) | rebuild db003 from db001`).
2. **`LowerTimelineHasQuorum` short string MUST name the action**, not the mechanism. Acceptable: `SplitBrain: db001 has quorum (lower TL=N), fence db002 (TL=N+1, quorum-blocked)`. Not acceptable: `SplitBrain: replica overrides timeline (N < N+1)` — that phrasing is paradox-shaped and was the trigger for the rename in the first place.
3. **`PrimaryQuorumUnsatisfied` MUST appear inline in the short string** when present, since it explains why the higher-TL primary lost. Without it the verdict reads as a paradox.
4. **`DivergentReplicaWal`, when present, MUST surface inline in the SplitBrain short string** (concatenated after the resolution-variant text, e.g. with ` | `), and MUST name the remediation: `rebuild <replica> from <true-primary>`. Evidence-only phrasing ("divergent WAL past fork @ LSN") is insufficient — it reads as background context and operators may reattach the divergent replica via `repmgr standby follow`, leaving silent corruption. The action verb is **rebuild** (tear down + new basebackup), not pg_rewind; the choice between basebackup-from-archive vs. fresh basebackup is operator judgment based on archive integrity and is not encoded in the tool's output.
5. **`SystemIdentifierMismatch` and `SynchronousCommitWeakened` are escalation-worthy** independent of the split-brain verdict; they should drive an alerting path distinct from routine SplitBrain rendering.

Phrasing is implementation detail; the *minimum information content* listed above is spec.

**Action-text ownership.** The writer (`format_reason`) derives the short string from `SplitBrainInfo.resolution` and `SplitBrainInfo.findings` — no new fields on the resolver types are required. The variant→action mapping is:

| Resolution | Short-string template |
|---|---|
| `Both` | `SplitBrain: {true} has quorum (TL={hi}), demote {stale} (TL={lo}, quorum unsatisfied)` |
| `LowerTimelineHasQuorum` | `SplitBrain: {true} has quorum (lower TL={lo}), fence {stale} (TL={hi}, quorum-blocked)` |
| `HigherTimeline` | `SplitBrain: {true} has quorum (TL={hi}), demote {stale} (TL={lo}, no live replicas)` |
| `ReplicaFollowing` | `SplitBrain: {true} has quorum (TL={tl}), demote {stale} (same TL)` |
| `Indeterminate` | `SplitBrain: cannot determine true primary (insufficient evidence)` |

Findings concatenation:
- `PrimaryQuorumUnsatisfied` is the source of the "quorum unsatisfied/blocked/no live replicas" inline text and is consumed by the template above, not separately appended.
- `DivergentReplicaWal`, if present, is appended after the resolution text with ` | rebuild <replica> from <true-primary>`.
- Other findings appear in `details_json`, not in `short`.

### 5. New data to collect

Additions to `HEALTH_CHECK_PRIMARY_QUERY`:

- `system_identifier` from `pg_control_system()`, cast to text: `(SELECT system_identifier::text FROM pg_control_system())` to avoid `bigint`→JSON safe-int issues.
- Timeline-history file contents for the current TL:

  ```sql
  (WITH cc AS (SELECT timeline_id FROM pg_control_checkpoint())
   SELECT CASE
     WHEN cc.timeline_id = 1 THEN NULL
     ELSE pg_read_file(
       'pg_wal/' || lpad(upper(to_hex(cc.timeline_id)), 8, '0') || '.history',
       0,
       (1024 * 1024)::bigint,
       true
     )
   END
   FROM cc)
  ```

  The CTE evaluates `pg_control_checkpoint()` once; calling it twice in the same CASE risked a (negligible but real) race during a TL bump where the filename and the existence check disagree. The 4-arg form `pg_read_file(path, offset, length, missing_ok)` returns NULL if the file doesn't exist (e.g. during a fresh promotion window between TL bump and history-file write); the 1-arg form throws and would abort the entire `jsonb_build_object`. The 1 MiB length cap is far more than any realistic history file (typically <1 KiB) but bounds memory allocation.

  The filename is 8-digit uppercase hex padded (postgres writes via `%08X`); decimal padding will silently miss TLs ≥ 10. Privileges: requires `pg_read_server_files`, which is granted in production.

Additions to `HEALTH_CHECK_REPLICA_QUERY`:

- `system_identifier::text` from `pg_control_system()` — for cross-node sanity gate. (Replicas inherit `system_identifier` from their basebackup source; mismatch detects a replica reseeded from a foreign cluster.)

Other:

- Pass scan-start `DateTime<Utc>` from `analyze_clusters` through `analyze()` into `resolve_split_brain()` as a parameter, for the freshness gate. This is a small but load-bearing plumbing change.

### 6. Two-pass stability check (deferred)

Future enhancement: re-scan after ≥ `wal_sender_timeout`. If primary set, timelines, follower map, and `pg_replication_slots.active` set are unchanged, promote `BestEffort` to `Verified`. Out of scope for this ADR.

### 7. Divergent-WAL as a separate finding

`DivergentReplicaWal { replica_node, replica_received_tli, replica_flushed_lsn, fork_tli, fork_lsn }` — emitted as **both** a top-level finding on the broader analyze report AND included in `SplitBrainInfo.findings` when a split-brain verdict is being produced. The dual emission is so the writer's `format_reason` for the `SplitBrain` reason has direct access (the writer currently sees only the `SplitBrainInfo`, not top-level findings; duplicating the finding avoids a signature change to `format_reason`).

Triggered when a replica's `received_tli` is older than the cluster's highest primary TL AND its `flushed_lsn` is past the corresponding fork LSN. The fork LSN is read from the **higher-TL primary's** timeline-history file (which records the previous timeline's switch-LSN), parsed client-side. History file format is tab-separated `<previous_tli>\t<switch_lsn>\t<reason>` per line; ignore lines beginning with `#`.

When `DivergentReplicaWal` is present, §4's short-string contract item 4 mandates the rebuild remediation surfaces inline (this is what the dual emission enables).

## Out of scope

- Scoring/weighted-aggregation. Rejected during brainstorm.
- Multi-dimensional verdict (separate `committing`/`intended`/`divergent` fields). Single-winner is correct under the operational definition.
- repmgr metadata integration. Possible follow-up; would shift authority from postgres state to repmgr.
- In-flight-failover detection. The tool runs in stable state; transient-window scans are capped at `BestEffort`.
- Cross-cluster signals (Pacemaker, etcd, VIP state).
- Topologies with >2 replicas. Resolver assumes 1+2.
- Hostname-form `primary_conninfo`. Current production uses IPs; flag as known limitation.
- Visual/layout polish for the report (colorization, column ordering, table styling). The *information content* mandated by §4's short-string contract is in scope; how it's visually formatted is not.

## Consequences

- Verdicts previously expressed as authoritative are now explicitly `BestEffort` until two-pass stability lands.
- `LowerTimelineHasQuorum` (renamed from `ReplicaOverridesTimeline`) is harder to trigger: stale replica evidence that previously qualified is filtered by the gate. C-b and C-c (the legitimate cases) still resolve correctly.
- Renaming `ReplicaOverridesTimeline` is a breaking API change for `SplitBrainResolution` consumers; the writer and any external readers must be updated in the same PR.
- Adding `system_identifier` and timeline-history collection makes `HEALTH_CHECK_PRIMARY_QUERY` modestly larger; deployment must have `pg_read_server_files` granted (already true in production).
- Existing tests assert on `SplitBrainInfo` literals (six tests in `split_brain.rs`, one in `analyze.rs`); each needs the new `confidence` and `findings` fields. Mechanical but not free.
- `Indeterminate` is preserved as an evidence-state outcome; no single-pass tiebreaker is added.
