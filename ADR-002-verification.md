# ADR-002 end-to-end verification

Read-only assessment, 2026-09-10. No repo file was modified except the creation of this one.
Spec: `docs/adr/002-split-brain-resolution-refinement.md` (268 lines).
Implementation reviewed at working copy `1a2c4dd5` (parent `933e0721` "wip commit 12").

Method: 25 verification agents (one per ADR section block, one per case-matrix row, one per
postgres-behaviour family, plus concepts-doc / deferral / identity-matching deep dives), then a
completeness critic and an adversarial critic that re-attacked every critical and high finding.
`cargo test --all` passes (194 tests). `cargo clippy --all-targets` emits only the two known
`timeline_history.rs` dead-code warnings.

---

## 0. Evidence base

Distinguishing what is measured from what is argued matters more than usual here, because the ADR
itself says no one has captured the dangerous states.

**Real prod measurements (user-run psql, prod-pg-app001, 2026-09-10):**

| Fact | Value |
|---|---|
| version | PostgreSQL 15.14 (the ADR's annotations cite PG17) |
| `synchronous_standby_names` | `ANY 1 ( prod_pg_app001_db002, prod_pg_app001_db003 )` |
| `synchronous_commit` | `remote_apply` |
| `wal_sender_timeout` | `300000`, `pg_settings.unit` = `ms` |
| `wal_receiver_status_interval` | `10`, unit `s` |
| `pg_stat_replication.application_name` | `prod_pg_app001_db002`, `prod_pg_app001_db003`; both `sync_state=quorum` |
| host / repmgr node name | `prod-pg-app001-db002` (hyphens) vs app_name (underscores) |
| `pg_control_checkpoint().timeline_id` | 15 |
| history file | `lpad(upper(to_hex(15)),8,'0')` -> `0000000F.history`, exists; `pg_ls_dir` lists `00000008..0000000F.history` |
| history contents | ancestors only, `<parent_tli>\t<switch_lsn>\t<reason>`; last line `14  16A/BB0000A0` is the current TL's own fork |
| `pg_stat_wal_receiver` on primary | 0 rows; viewdef ends `WHERE s.pid IS NOT NULL` |
| promoted primary LSNs | `replay = 16A/BB0000A0` (= its own fork), `receive = NULL` |
| postmaster start / last promote | 2026-06-03 (98d uptime) / 2026-05-21 -- restart came **after** promotion |
| `SET synchronous_commit=false; SHOW` | returns `off` (aliases normalise) |
| `SET synchronous_commit='remote_flush'` | `ERROR: invalid value`; `HINT: Available values: local, remote_write, remote_apply, on, off.` |
| replica | `status=streaming`, `sender_host=10.81.17.1` (IP), `sender_port=5432`, `slot_name` empty |
| scanner role | connects as "operator"; each operator has superuser |

**Captured fixture:** `tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json` -- a healthy 3-node dev
cluster on 15.14; user confirms fixtures are up to date. One field in it is internally inconsistent
(see L-8), so it is not uniformly faithful.

**Config-as-code (`~/work/infra`, Ansible).** This is the strongest evidence in the document,
because it establishes fleet properties *by construction* rather than by sampling.

- All 321 rendered `result_manifests/*/*/postgres/manifest_postgresql.conf`:
  `synchronous_standby_names = 'ANY 1 ( X, X )'` with exactly two members, **self-excluded on all
  321**; `synchronous_commit = remote_apply` on all 321; `wal_sender_timeout = 5min` and
  **`wal_receiver_timeout = 5min`** on all 321.
- `application_name` has exactly one definition fleet-wide, symlinked into all five environments:
  `ansible/environments/proact/global_postgres_all.yml:450`
  `pg_replica_application_name: "{{inventory_hostname.split('.')[0]|replace('-', '_')}}"`
  -- with the comment "'-' cannot be used in postgres application_name". No override exists
  anywhere; no postgres node has host_vars. 214/214 rendered `primary_conninfo` values match their
  own directory FQDN under that transform, zero mismatches.
- `node_name` is the Ansible `inventory_hostname` (FQDN): `ansible/postgres-inventory-scan.yml:15`
  POSTs `{"host": "{{inventory_hostname}}"}` to `https://database.fnox.se/api/v1/inventory_scans/host`,
  stored as `pginv_server.servername` and served as the portal's `node_name`.
- Topology, from three independent sources that agree exactly (321 manifests; 1074 inventory hosts;
  the portal's own 1074-node response): **358 clusters, every one exactly 3 nodes, exactly 1
  primary + 2 replicas, node numbers always {db001, db002, db003}.** Citus coordinators and workers
  are each their own separate 3-node repmgr cluster with their own portal `cluster_id`.
- Replication path: no NAT, VIP, proxy or LB between postgres nodes; pgbouncer listens on 6432 and
  is not in the replication path; 214/214 `primary_conninfo` use IPv4 literals; one NIC per VM whose
  static IP is `ansible_host`; `pg_hba` pins replication to those /32s.
- `pg_read_server_files` is **never granted anywhere in the repo**. Ops domain users are provisioned
  `SUPERUSER` (`postgres_setup_dynamic/tasks/7-setup-postgres-domain-users.yml:6`), so `pg_read_file`
  succeeds by superuser bypass, not by the grant the ADR names.

**PostgreSQL source:** local checkout at `~/work/postgres`. The working tree is REL_18_BETA1, but
tagged reads (`git show REL_15_14:...`) give the deployed version; all source citations below are
from the `REL_15_14` tag.

**Not available:** any captured split-brain scan. Every claim about a two-primary cluster is
therefore reasoning over code paths, not observation. Nothing in this document asserts otherwise.

---

## 1. Section-by-section classification

Each row is one load-bearing statement. `impl` = implemented as specified, `div` = diverges,
`unver` = unverifiable as written, `self-c` = self-contradictory, `stale` = overtaken by a later
revision, `def-ok` = deferred and consistently stated.

### Context / operational definition / cluster assumptions (9-30)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 27 | "repmgr-set `application_name` equals the node name" | **div** | False on prod and in the fixture: app_name `prod_pg_app001_db002` vs `Node.name` FQDN from the portal API (`src/v2/node.rs:14`). True only inside repmgr's own namespace. Root of F1. |
| 19 | true primary = "sync quorum satisfied and actively committing" | **div** | Code elects on "has a live bidirectional follower" only. `determine_true_primary` (`split_brain.rs:352-393`) never reads quorum state or commit activity; `findings` feed confidence only. |
| 26 | `ANY 1 (A, B)`, quorum satisfiable by either replica | **impl** | Matches prod verbatim (modulo names). |
| 28 | `wal_sender_timeout=5min`; "keepalives sent at `wal_sender_timeout/2` ~150 s" | **div** | Keepalives fire only after half the timeout *without a standby reply*; prod replies every 10 s, so no 150 s cadence exists. See F7. |
| 29 | scanner role has `pg_read_server_files` | **unver -> resolved** | Asserted three times, verified nowhere in code. User confirms operators are superusers, so it holds in practice -- by role privilege, not by the stated grant. |
| 25 | ">2 replicas out of scope" | **impl** | Guard at `analyze.rs:323`. Unreachable in a 3-node fleet, but consistent. *An earlier agent claim that the split-brain early return makes this dead code was refuted: it needs >=4 nodes regardless of ordering.* |
| 58 | transient-window verdicts "capped at `BestEffort`" | **impl** | `BestEffort` is the top of the lattice with `Verified` absent, so cap and default coincide. *Agent claim of divergence refuted as wordplay.* |
| 13 | "a `sender_host` match alone counts as following" (the problem statement) | **stale** | Describes pre-ADR code; the gate now exists. Harmless historical framing. |

### Case matrix (31-59)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 39-47 | C-a..C-g enumerate db003's states exhaustively | **div** | No row covers the §1-mandated third outcome (replica side passes, primary side fails -> `PrimaryDoesNotSeeReplica`, not following) -- which is what all four "live" rows actually produce on fleet naming. |
| 41-47 | each row's "Verdict + findings" is what §1-§4 determine | **div** | Every row mismatches; see §2 below. Findings columns are inconsistent between sibling rows (C-a lists `PrimaryQuorumUnsatisfied(db001)`, C-e omits it for the identical db001 condition; the code emits it in both). |
| 43 | C-c: db002 "provably client-acked nothing ... (`sync_commit=on`)" | **div** | Premise is `on`; fleet is `remote_apply` (stronger, so the inference survives) but the proof is stated over a config that does not exist here, and it silently assumes non-empty SSN (F3). |
| 43, 237 | "Its fork is empty" / "committed nothing on its fork" | **div** | False as stated: an isolated primary commits locally and only withholds the ack (F4). The defensible claim is "acknowledged nothing". |
| 45, 56 | C-e resolves by asymmetric precedence; replica side fails first | **impl** | `build_replica_following_map` checks the replica side first and `continue`s (`split_brain.rs:281-300`); the primary-side row cannot rescue a failed match. |
| 44 | C-d is safe: db002 is the true primary | **unver** | C-d is C-b after the link ages out. Nothing in the row excludes "db003 was flushing for db001 past the fork, then the link died" -- in which case C-d is C-g with a timer. The row asserts safety it does not derive. |
| 46 | C-f: replica-stuck condition "surfaced separately (existing archive-failure mechanism)" | **div** | False. `analyze.rs:317-321` returns early on split brain, so `check_archive`, `check_streaming` and every other per-node check never run. Nothing surfaces it. |
| 47 | C-g: resolver mis-picks `HigherTimeline` | **impl** | Correct and honestly stated (`split_brain.rs:485-502`). |
| 49-54 | "correctness rests on three facts" then lists four | **self-c** | Off-by-one in the doc. |
| 33-47 | matrix state space fixes db001 at TL=N, db002 at TL=N+1 | **div** | No equal-timeline row, yet `resolve_with_equal_timelines` is a real branch -- and F5 makes equal timelines *likely* right after a promotion. |

### §1 Flushing-liveness gate (62-90)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 67-68 | `sender_host == primary.ip_address`, `sender_port == 5432` | **impl** | `split_brain.rs:281-283`. Prod confirms IP form. |
| 69 | status in {`streaming`,`catchup`}; "catchup ... must not be rejected" | **div** | `catchup` is not a `pg_stat_wal_receiver.status` value. Real set (`walreceiver.c:1373-1383`): stopped, starting, streaming, waiting, restarting, stopping. The replica-side arm accepts an impossible string, and `waiting`/`restarting` are silently rejected without the ADR ever considering them. |
| 70, 75 | freshness measured "of the scan-start timestamp" | **div (code is right)** | Code compares intra-node against each node's own `current_time`. That is strictly better -- it removes scanner/db clock skew from a 180 s budget. The ADR is stale here, not the code. |
| 73 | primary row's `application_name` equals the replica's node name | **div** | F1. Never true on this fleet. |
| 73 | empty `application_name` rejected | **impl** | `split_brain.rs:306`. |
| 74 | state in {streaming,catchup}, `state != backup` | **impl** | `split_brain.rs:308-311`; `Backup` is excluded by omission from the `matches!`. |
| 75, 87 | `reply_time` freshness; cadence asymmetry "acceptable for v1" | **div** | `reply_time` is the *standby's* clock, so `p_health.current_time - reply_time` is an inter-node comparison -- contradicting the code's own doc comment (`split_brain.rs:242-244`) that freshness is intra-node and skew-immune. Half the gate is skew-exposed. |
| 77 | one-sided claim -> `PrimaryDoesNotSeeReplica`, not counted | **impl** | `split_brain.rs:335-337`. |
| 82, 85 | `threshold = wal_sender_timeout/2 + 30_000`; pg_settings returns raw ms | **impl** | `split_brain.rs:266`, `parse_wal_sender_timeout:609`. Prod confirms `setting=300000, unit=ms`. |
| 85-87 | 180 s is "comfortably above the keepalive cadence of ~150 s" | **div** | The 150 s cadence does not exist (F7), and the replica-side row's lifetime is governed by `wal_receiver_timeout` (standby GUC, default 60 s), which the scanner does not even collect. The derivation describes a mechanism that is not the operative one. |
| 89 | rejected gate inputs (flush_lsn, flush_lag, archive recency) | **impl** | None is used as a gate input. |
| 67 | hostname-form `primary_conninfo` out of scope | **impl** | Consistent with line 257; not flagged in code, only in the ADR. |

### §2 Sanity gates -> Refuse (91-108)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 95 | sysid consistency across ALL nodes | **impl** | `mismatched_sysid_nodes:583` covers primaries and replicas. Nodes with `Role::Unknown*` are silently skipped -- undiscussed, minor. |
| 100 | reference sysid = agreed by >= 2 candidate primaries | **impl** | `reference_sysid:566`. "Majority class" and ">= 2 wins" coincide at 2 primaries; they diverge at 4 split 2-2, which is out of scope. |
| 98, 100 | excluded replicas never reach the gate; no-reference -> all excluded + Refuse | **impl** | `resolve_split_brain:141-146`, exclusion precedes `build_replica_following_map`. |
| 96 | Refuse if `synchronous_commit` in {local, off, remote_write, empty} | **impl** | `WEAKENED_SYNCHRONOUS_COMMIT:13`, loop at `:160-175`. Boolean aliases normalise (`SHOW` returns `off`), so the denylist is not bypassable -- *an agent concern about alias evasion is refuted by prod.* |
| 96 | "Valid values: `on`, `remote_apply`, `remote_flush`" | **div** | `remote_flush` does not exist. Prod: `HINT: Available values: local, remote_write, remote_apply, on, off.` No behavioural effect (code encodes only the deny side). |
| 96 | the gate protects the "no-divergence" invariant | **div** | **F3.** It tests the consequent and never the antecedent. Empty `synchronous_standby_names` disables the wait entirely (`SyncStandbysDefined()`, `syncrep.c:91-92`) regardless of `synchronous_commit`. No Refuse, no finding -- while the writer still prints "quorum unsatisfied" about that primary. |
| 106-107 | SSN inconsistency "not Refuse-worthy ... each primary evaluates its own SSN locally" | **div** | The locality argument is sound, but the conclusion is used to skip *all* SSN inspection, which is how F3 slips through. Local evaluation is exactly why an empty local SSN is dangerous. |

### §3 Confidence states (109-126)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 113-118 | three states with the stated meanings | **impl** | Enum at `split_brain.rs:73-78`; ordering `Refuse < Conflicting < BestEffort` pinned by test `confidence_ordering_matches_severity_rank`. |
| 113 | BestEffort = "gate passed; verdict is internally consistent" | **div** | Reachable with the gate having rejected everything: `HigherTimeline` with no findings at all yields `BestEffort` via `unwrap_or` (`:389`). |
| 109-126 | (how multiple findings combine) | **unver** | The ADR never specifies aggregation. `min()` at `:384-391` is unspecified behaviour, not a divergence. |
| 116-117 | `PrimaryQuorumUnsatisfied` -> Conflicting on the elected primary, BestEffort otherwise | **unver** | Not in the ADR at all; introduced by commit `87e51e5`. The rule is defensible but unsanctioned, and it interacts badly with F9: under `HigherTimeline` the elected primary is *always* quorum-unsatisfied, so the verdict is permanently `Conflicting` while the text says "has quorum". |
| 116, 140 | `Conflicting` = contradiction vs `ReplicaInCatchup` = "informational, gate passed" | **self-c** | An explicitly informational finding is mapped to the contradiction state (`:413`). Invisible in practice (F10). |
| 121 | `Verified` omitted, needs two-pass (§6) | **def-ok** | Consistent everywhere. |
| 123-125 | Refuse vs Indeterminate are different axes; Indeterminate -> BestEffort | **impl** | Both Indeterminate constructors set `BestEffort` (`:376`, `:557`). |

### §4 Findings list, rename, derivation (127-151)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 129 | ordering sanity-gate / contradiction / corroboration; cap ~5 | **impl** | *Largely refuted as a finding.* Only 0-1 findings ever reach `short`; `details_json` carries all by design (line 179); the theoretical max in a 3-node split brain is ~7. The one real deviation -- quorum findings appended last -- has no effect. |
| 131 | rename to `LowerTimelineHasQuorum` | **impl** (code) / **stale** (docs) | Code fully renamed. `SPEC.md:401` still uses the old name; `README.md:231` prints `SplitBrain: replica overrides timeline (7 < 8)` -- verbatim the string line 157 calls "Not acceptable". The enum doc comment at `:33` still says "Replica evidence overrides timeline". |
| 135-141 | seven finding categories with named payloads | **impl** | All present. `PrimaryDoesNotSeeReplica`/`BidirectionalFlushingConfirmed`/`ReplicaInCatchup` use a `ReplicationLink` newtype rather than two fields -- equivalent. |
| 145 | "unparseable -> method=ANY, count=infinity (emit no finding rather than a wrong one)" | **self-c** | `count=infinity` makes `observed < count` unconditionally true, i.e. emit *always*; the parenthetical says emit *never*. The sentinel that yields the stated intent is `count = 0`. Code does the parenthetical (`continue` at `:650`). Seeded-lead item 3: **upheld.** |
| 145 | (empty SSN) | **unver** | The rule never mentions empty SSN, which `parse()` maps to `None` identically to unparseable. Gap, not divergence. |
| 146 | step 2 excludes foreign-sysid replicas | **impl** | Via `filtered_replicas`. |
| 147 | `observed = |members ∩ gated_followers|` | **div** | **F2.** The two sides are in different namespaces -- `members` from SSN (application-name form), `gated` holds `node_name` (FQDN). Structurally 0 on every real primary. Independent of F1: fixing the gate does not fix this. |
| 148 | "Emit if `observed < count`" | **impl** | `:665`. Method (ANY vs FIRST) is ignored; harmless for `ANY 1`, wrong for `FIRST 2 (a,b,c)`, which does not occur here. |
| 150 | "Implementations MUST emit `PrimaryQuorumUnsatisfied` for the stale primary" | **div** | Silently fails whenever the stale primary's SSN is empty or unparseable. Seeded-lead item 4: **upheld** -- the MUST holds only when that primary's SSN parses. The repo's own tests sit in exactly that state (builder default leaves SSN unset). |

### §4 Short-string contract (152-180)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 156 | Refuse leads with `REFUSE/`, resolution text suppressed | **impl** | `split_brain_reason:655-659`, test `refuse_overrides_resolution_text`. |
| 156 | (fallback "sanity gate failed") | **impl** | *Refuted as a live defect:* Refuse arises from only three findings; two render a gate name and the third has no emitter. Dead code today -- a trap for later, not a finding now. |
| 157, 171 | `LowerTimelineHasQuorum` names the action | **impl** | `build.rs:704-711` matches the ADR's "Acceptable" example. |
| 158, 177 | `PrimaryQuorumUnsatisfied` MUST appear inline / "consumed by the template" | **div (partial)** | **F9.** The digest claim that hardcoding violates item 3 is *refuted* -- line 177 explicitly says the finding is consumed by the template. What survives: the parenthetical is asserted unconditionally even when the finding was never derived, and its subject is wrong under `HigherTimeline` (the finding names the *elected* primary; the parenthetical is attached to the *stale* one). Seeded-lead items 1 and 2: **upheld**. |
| 172 | `HigherTimeline` -> "{true} has quorum ... (no live replicas)" | **self-c** | `HigherTimeline` fires precisely when *no* primary has a gated follower, so the elected primary demonstrably does not have quorum. The template asserts the opposite of the variant's own precondition. |
| 173 | `ReplicaFollowing` -> "... has quorum (TL={tl}) ..." | **self-c** | The variant carries no timeline, and line 166 forbids adding fields. The two sentences cannot both be satisfied; the writer drops the TL (`build.rs:719-722`). |
| 168-174 | `{stale}` is a single node | **div** | `stale_primaries` is a vec; the writer renders only `.first()`. A three-way split brain names two zombies in the PRIMARY column and demotes one. |
| 159-161, 178 | `DivergentReplicaWal` rendering deferred | **def-ok** | *Refuted as self-contradictory:* item 4 is scoped by its own revision note, item 1 marks the carve-out dormant, line 178 and §7:247 agree. Consistent in four places plus the plan. |
| 162 | sysid/sync_commit findings "should drive an alerting path distinct from routine rendering" | **unver** | No distinct path exists; the statement names no observable, so it cannot be checked. |
| 179 | other findings in `details_json` | **impl** | `serde_json::to_string(info)`; test `findings_appear_in_details_json`. |

### §5 New data to collect (181-226)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 185, 208 | `system_identifier::text` on both queries | **impl** | Both queries; avoids the bigint/JSON issue as stated. |
| 188-200 | timeline-history SQL | **impl** | Shipped query is semantically identical (`WHEN timeline_id = 1` inside the CTE-scoped `FROM cc` is the same as `cc.timeline_id`). |
| 202 | 4-arg `pg_read_file` returns NULL; 1-arg would abort the object | **impl** | Correct, and prod confirms the 4-arg call works. |
| 202 | CTE avoids a TL-bump race | **div (rationale)** | The defensive choice is right; the stated reason is not. `pg_control_checkpoint()` reports the last *completed* checkpoint, so it cannot bump mid-query in the way described. |
| 202 | missing_ok covers "a fresh promotion window between TL bump and history-file write" | **div (rationale)** | PG writes the history file *before* installing the new timeline. The window described is inverted. |
| 204 | 8-digit uppercase hex; "decimal padding will silently miss TLs >= 10" | **impl** | Prod: TL 15 -> `0000000F.history`, exists. Confirmed. |
| 209-213 | add absolute replay/receive LSN; NULL never `0/0` | **impl** | Present as `Option<String>`. The NULL claim is true -- but the annotation cites PG17 for a PG15 fleet (see L-6). |
| 215 | "NULL is close to unobservable in practice -- confirmed on a promoted primary, where `pg_last_wal_receive_lsn() IS NULL` returns `f`" | **div** | Prod returns **`t`** on the first primary checked. Refuted by measurement. |
| 219 | "both LSNs ... are zeroed by a postmaster restart and do not survive one" | **div** | Prod: postmaster restarted 2026-06-03, after the 2026-05-21 promotion. `replay` survived holding the pre-restart promotion LSN; `receive` did not. The two fields behave differently across a restart; the ADR treats them as one. This also makes line 215 unsustainable: any node restarted since promotion has `receive = NULL`. |
| 217 | "both return 6FD/7C0000A0 ... both pointers freeze at the promotion LSN" | **unver** | Does not reproduce on prod (only `replay` is set). The cited measurement is from a node in a state we cannot re-observe; it does not generalise. |
| 221 | "capture only ... so the next real C-g is diagnosable" | **div** | **F6.** The two fields reach no output: not the info-level event, not `details_json` (built from the verdict, not node health), not the CSV. Only the debug-level raw dump, and the default level is info. A scan during a real C-g records nothing. |
| 225 | plumb scan-start `DateTime<Utc>` into `resolve_split_brain()` | **stale** | No such parameter. The intra-node design the code chose is better; the ADR should be corrected, not the code. |

### §6 / §7 / Out of scope / Consequences (227-268)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 227-230 | two-pass check deferred | **def-ok** | Nothing half-built; `Verified` absent as stated. |
| 233, 247 | `DivergentReplicaWal` defined but detection/confidence/remediation deferred | **def-ok** | Variant exists, maps to Refuse (`:399`), has no emitter anywhere in the repo. Stated consistently. |
| 7 vs 245 | "a conservative Refuse-only floor is **shippable today** (§7)" vs "which is why **no** conservative Refuse-only floor **is shipped** in the interim" | **self-c** | The shippable/shipped reading does not rescue it: §7's reason is that shipping the floor would "add over-caution to safe cases and false confidence to the dangerous one" -- it argues the floor is a *bad idea*, not merely unbuilt. Following line 7's own "(§7)" pointer lands on the opposite recommendation. |
| 237 | remediation: keep lower TL, rebuild the higher-TL node | **impl (as spec)** | Matches the concepts doc and the operational policy (rebuild, not `pg_rewind`). |
| 239 | the finding must drive a verdict-flip, not ride along | **def-ok** | Honest and consistent with C-g. |
| 241-243 | the 3-node proof | **div** | **F3/F4.** The reduction assumes non-empty SSN on db002 and equates "committed" with "acknowledged". Both assumptions are unstated and one is false. |
| 245 | detection deferred because danger and detectability are anti-correlated | **impl** | Sound, and the honest core of the ADR. |
| 47, 54, 239 | (what the tool does *today* in C-g) | **div** | **F13.** Every reference site says what is deferred and why; none states the interim behaviour, which is an explicit demote instruction aimed at the node the ADR says holds the acked writes. |
| 256 | ">2 replicas out of scope" | **impl** | Consistent. |
| 258 | `SyncStandbyNamesDiverged` out of scope; "each primary's SSN structurally excludes itself" | **div** | Prod SSN on the primary lists the two *replicas* and not itself, so the premise holds today -- but only pre-failover. After promotion nothing rewrites SSN, so the new primary's SSN does list itself. The justification is written over the wrong moment in the incident. |
| 263 | verdicts now explicitly BestEffort | **impl** | |
| 264 | "C-b and C-c (the legitimate cases) still resolve correctly" | **div** | They do not, on fleet naming. See F1. |
| 267 | "six tests in `split_brain.rs`, one in `analyze.rs`" | **div** | Five full-literal `SplitBrainInfo` assertions in `split_brain.rs` (31 tests total), one in `analyze.rs`. Off by one; trivial. |
| 268 | "no single-pass tiebreaker is added" | **div** | A tiebreaker *is* applied to `true_primary`: `primaries_with_highest_timeline[0]` after a stable sort = whichever primary the scan pipeline delivered first. Nondeterministic across runs, and the PRIMARY column renders it as a pick. |

### docs/concepts/split-brain.md (in scope as spec)

| Line | Statement | Class | Justification |
|---|---|---|---|
| 15 | "an *isolated* primary ... physically cannot commit" | **div** | **F4.** False. `xact.c:1515` calls `SyncRepWaitForLSN` *after* the commit record is flushed and clog marked; `syncrep.c:328-329` warns "The transaction has already committed locally, but might not have been replicated to the standby." It cannot *acknowledge*. |
| 44 | "Past that gate, the inference holds" | **div** | Not past the empty-SSN hole (F3). |
| 50, 72, 84 | the lower-TL / observability branch of the model | **div** | Describes behaviour the resolver cannot exhibit on fleet naming (F1). |
| 85 | "db003 unobservable -> **`Refuse`** (decline to auto-resolve)" | **div** | The sharpest doc defect. Stated in the present indicative as tool behaviour; the tool does the opposite. Not excusable as aspirational, because :87 in the next paragraph correctly says detection is deferred. Contradicts ADR:47, which admits the mis-pick. |
| 55-64 | anchor divergence to the inter-primary fork, not the winner | **impl (as design)** | Correct, well argued, and better than anything in the ADR. `_fork_lsn_for` (`timeline_history.rs:41`) implements exactly this shape and its semantics match prod history files. |
| 89-95 | wedge log signature | **div (minor)** | Message text is right and honestly sourced to BUG #8294, but it is `ereport(LOG)`, not `FATAL`. Anyone grepping for FATAL misses it. |
| 7, 15, 42, 70 | reasons throughout in terms of `synchronous_commit = on` | **stale** | Fleet is `remote_apply`. Stronger, so conclusions survive, but the doc describes a config that does not exist. |

---

## 2. Case matrix vs the code

Traced by hand-executing `resolve_split_brain` then `split_brain_reason`. "Synthetic" = the test
suite's naming (`application_name == node_name`); "Fleet" = the naming prod actually uses.

| Row | ADR expects | Synthetic names | Fleet names | Match? |
|---|---|---|---|---|
| C-a | `Both` + `BidirectionalFlushingConfirmed` + `PrimaryQuorumUnsatisfied(db001)` | `Both`; quorum finding **absent** unless SSN is set (builder leaves it unset) | `HigherTimeline`, "demote db001" | No |
| C-b | `LowerTimelineHasQuorum`, keep **db001** | matches ADR exactly | **`HigherTimeline`, "demote db001"** -- verdict inverts | No |
| C-c | C-b verdict; `DivergentReplicaWal` informational | as C-b | as C-b -- inverts | No |
| C-d | `HigherTimeline` + `ReplicaWalReceiverStale` | matches | matches | Partly (see row note above: safety not derived) |
| C-e | `Both` | `Both` | `HigherTimeline` | No |
| C-f | `HigherTimeline`; stuck replica surfaced elsewhere | `HigherTimeline`, **no finding at all** | same | No -- nothing surfaces it (`analyze.rs:317`) |
| C-g | mis-pick, must be overridden by Refuse | `HigherTimeline`, "demote db001" | same | Yes as described; the override does not exist |

**The headline:** C-b and C-c -- the ADR's canonical "keeping the lower timeline is correct" rows --
produce, on real fleet naming, the exact C-g data-loss output, from a benign and fully observable
state. The dangerous output is not confined to the exotic wedged case.

Determinacy: C-c, C-d, C-f and C-g are **not** determinate under §1-§4 as written. Their outcomes
are asserted, not derived.

---

## 3. Findings, ranked

Severity = can this produce a wrong verdict or operator text pointing at a destructive action.
"Code" = provable by reading. "Fleet" = requires evidence; stated separately and honestly.

### F1 -- Cross-namespace identity compare disables the entire gate. CRITICAL
**Code: yes. Fleet: evidenced.**
`split_brain.rs:307` compares `conn.application_name` (`prod_pg_app001_db002`) to
`replica.node_name` (`prod-pg-app001-db002.sto1.example.com`, straight from the portal API via
`node.rs:14`, never normalised on the resolver path). They can never be equal under either possible
inventory form, since one uses underscores and the other hyphens.

Consequences, all by reading: the primary-side gate never passes -> every gate-passing replica emits
`PrimaryDoesNotSeeReplica` -> `replicas_following` is permanently empty -> `Both`,
`LowerTimelineHasQuorum`, `ReplicaFollowing`, `BidirectionalFlushingConfirmed` and `ReplicaInCatchup`
are unreachable -> every split brain resolves as `HigherTimeline` (or `Indeterminate` on equal
timelines) -> the writer prints `demote <lower-TL node>`. That is a destructive instruction against
the node that, in C-b/C-c, holds the acknowledged writes.

The test suite cannot catch it: `split_brain.rs` builders set `application_name == node_name`, and
`analyze.rs:1276-1294` fabricates `with_followers(&["dev-pg-app001-db003.sto3.example.com"])` -- an
`application_name` postgres would never emit. The tests certify the broken comparison.

**Now confirmed by construction, not by sampling.** Ansible defines the application name once,
fleet-wide, as a deliberate transform of the very string db-scan stores as `node_name`:

```
# ansible/environments/proact/global_postgres_all.yml:445-450
# (ex. dev-pg-app001-db001.sto1.fnox.se -> dev-pg-app001-db001)
# Then replace '-' with '_' since '-' cannot be used in postgres application_name
pg_replica_application_name: "{{inventory_hostname.split('.')[0]|replace('-', '_')}}"
```

The same `inventory_hostname` is POSTed to the portal (`postgres-inventory-scan.yml:15`) and comes
back as `node_name`. So `==` between the two endpoints of a documented, mandatory, fleet-wide
transform is dead code by construction, in all five environments, for all 358 clusters.

**Fix -- resolver, plus ADR text, plus tests. Mirror the Ansible transform.** Compare
`conn.application_name` against `replica.node_name.split('.').next().replace('-', "_")`. This is a
one-line pure function of data already in hand, it reproduces the config-as-code rule exactly
(including the `it` environment, whose domain is `.it.fnox.se` -- `split('.')[0]` is
domain-agnostic, which is why that env had to override the sibling SSN variable but not this one),
and critically **it also fixes F2**, because SSN members are generated by the same expression.

*Alternative, independently validated:* match `conn.client_addr` against `replica.ip_address`,
mirroring the replica-side gate. The infra review confirms this join is safe -- no NAT/VIP/proxy in
the replication path, 214/214 `primary_conninfo` are IPv4 literals, one NIC per VM, `pg_hba` pins
the /32s. It is a fine belt-and-braces addition but it does **not** fix F2, since SSN contains no
addresses. Prefer the name transform; add the address check only if you want both.
*Rejected:* reusing the writer's helpers -- `normalize_application_name` yields `db002` while
`extract_db_number` yields `db002@sto2`, so neither is the fleet's canonical form, and it would make
the analyzer depend on the writer against the repo's no-new-abstractions rule.

ADR lines 27 and 73 should be rewritten to state the actual transform rather than the false
equality. The tests must be re-based on fleet-shaped names, or they will keep certifying the bug.

### F2 -- The quorum intersection is a second, independent namespace bug. CRITICAL
**Code: yes. Fleet: evidenced.**
`emit_quorum_findings:660-663` intersects SSN `members` (application-name form, prod:
`prod_pg_app001_db002`) with `gated` (node names). Always empty, so `observed` is pinned at 0 and
`PrimaryQuorumUnsatisfied` fires for *every* primary including the elected one -- which via
`determine_confidence_level:404` pins confidence at `Conflicting` on every split brain.

**Important interaction:** fixing F1 with `client_addr` does **not** fix F2, because SSN contains no
addresses. Fixing F1 with the Ansible name transform *does* -- infra generates SSN members with the
same `split('.')[0]|replace('-','_')` expression
(`global_postgres_all.yml:321`, and the `it` override at `gdc/it/.../postgres_all.yml:44`), so once
`node_name` is normalised the intersection is exact. This is the decisive argument for choosing the
name transform over the address join. Doing F1 alone by address leaves the quorum derivation
silently returning 0 on every primary.

### F3 -- The §2 sanity gate tests the consequent, not the antecedent. CRITICAL
**Code: yes. Fleet: ESCALATED -- the fleet has a supported procedure that deliberately creates the
unsafe state, and it is the procedure used on exactly the clusters this resolver runs against.**

Steady state is safe: all 321 rendered manifests carry a non-empty SSN. But rendered steady state is
not the whole story. `ansible/postgres-kickstart-standalone-db001-primary.yml:73-84` strips the GUC
from a live primary and reloads:

```yaml
- name: Remove synchronous_standby_names option from postgres.conf
  ansible.builtin.lineinfile:
    path: "/var/lib/pgsql/{{pg_version}}/data/postgresql.conf"
    state: absent
    regexp: "^synchronous_standby_names"
```

Line 26 states the intent: "Removing standby_sync from db001 config to allow it to take writes
without replicas". It also sets `MIN_ATTACHED_REPLICAS=0` (lines 86-91) so `pg-cluster-health` keeps
reporting OK. With the line gone, SSN falls back to `''`, `SyncStandbysDefined()` is false, and
`synchronous_commit = remote_apply` becomes a no-op wait -- the primary acks client commits with
zero replicas.

**The correlation is the finding.** That playbook was added (commit `cc1be2409a`) to get a cluster
taking writes again "after various failed failover events that we have seen" -- i.e. forked-timeline
post-failover states, which is precisely when db-scan's split-brain resolver is invoked. Empty SSN
and split brain are therefore *not independent events whose joint probability can be discounted*:
the operational response to a split brain is what creates the empty-SSN state. A scan run against a
mid-remediation cluster hits both at once, and the ADR's "an isolated primary acked nothing"
invariant is false exactly when it matters most. A second path exists in planned maintenance
(`postgres-version-lr-upgrade-part3.yml:178`, play named "Disable synchronous replication ... in
db001"). There is no companion playbook that restores SSN; recovery is manual
(`docs/alerts/postgresSynchronousStandbyNamesMisconfigured.md:20-21`).

**Partial mitigation, stated fairly.** The runbook for the standalone procedure requires the rest
of the cluster to be down first:

```
# To allow a primary to act as standalone without replicas in a failure scenario, set
# MIN_ATTACHED_REPLICAS=0, then modify postgresql.conf and comment out
# synchronous_standby_names option. ...
# Doing this is DANGEROUS, make sure other nodes of the cluster are *down* with postgres
# stopped & disabled first.
```

If that precondition is honoured, db-scan sees one primary, the resolver is never invoked, and F3
does not bite. So the dangerous combination needs the precondition to have been *violated* -- a node
that was supposed to be stopped is still running. That is not a stretch: it is exactly the "slow
fencing / the fence didn't take" failure the ADR was written for (line 15). F3's severity therefore
rests on a compliance assumption, not on an independent coincidence -- weaker than "guaranteed to
co-occur", stronger than "unrelated events". The resolver should not depend on that assumption
holding, which is the argument for the gate.

Not reachable via repmgr failover or `ALTER SYSTEM` -- only via these deliberate operator procedures.
No live node was checked, so this is "reachable by a supported, documented path", not "currently
true somewhere".
The whole safety argument is "quorum-sync means an isolated primary acked nothing". That holds only
if `synchronous_standby_names` names at least one standby. The gate reads only
`synchronous_commit` (`split_brain.rs:13,169`). With SSN empty, `SyncStandbysDefined()`
(`syncrep.c:91-92`) is false and the commit path fast-exits without waiting -- so a primary with
`synchronous_commit=on` and no SSN acks locally and immediately. The resolver produces no Refuse and
no finding (`parse()` -> `None` -> `continue` at `:650`), yet the writer still prints "quorum
unsatisfied" about that primary. The implication is inverted: the text asserts the safety property
in exactly the configuration where it does not hold.

Note the asymmetry: the single-primary path already treats empty SSN as Critical
(`check_writes_unprotected`, tests at `analyze.rs:1014`, `:1153`), but `analyze.rs:317-321` returns
early on split brain, so none of it runs. This looks unintentional.

**Fix -- resolver + ADR text.** In the same loop as the `synchronous_commit` check, treat
`parse(ssn) == None` on a candidate primary as a sanity-gate failure: new finding (e.g.
`SyncQuorumDisabled { primary }`), map to `Confidence::Refuse`, give it an arm in `format_refuse`.
*Rejected:* handling it inside `emit_quorum_findings` -- that function's job is counting, and a
Refuse-worthy condition emitted from a counting helper is easy to miss; also it would still leave
§2's text claiming a guarantee it does not provide.

### F4 -- "An isolated primary physically cannot commit" is false. HIGH (spec/doc)
**Code: n/a. Source: confirmed.**
`xact.c:1515-1516` calls `SyncRepWaitForLSN` *after* the commit record is written, flushed and clog
is marked; `syncrep.c:327-329` warns on the cancel path: "The transaction has already committed
locally, but might not have been replicated to the standby." An isolated primary commits locally and
those effects become visible to other sessions; it withholds only the client acknowledgement. The
wait is also abandonable (query cancel, backend crash, shutdown, promotion).

So concepts:15 is wrong as written, and ADR:43/237's "its fork is empty" / "committed nothing on its
fork" is wrong as written. The defensible claim -- "acknowledged nothing" -- is what the rest of the
argument actually needs, and §7:235 states it correctly. This matters because the remediation is
"discard/rebuild the higher TL": discarding a branch that contains locally-committed, unacked, but
*visible* transactions is a defensible decision, and the current phrasing hides that it is a
decision at all.
**Fix -- ADR text and concepts text only.** Replace "cannot commit" / "committed nothing" with
"cannot acknowledge" / "acknowledged nothing", and add one sentence noting the discarded branch may
hold locally-committed unacked transactions.

### F5 -- The resolver's only discriminator lags promotion. HIGH
**Code: yes. Fleet: unknown -- needs a capture right after a promotion.**
`get_timeline` reads `timeline_id` from `pg_control_checkpoint()`, which reports the last
**completed** checkpoint. PG15 `xlog.c:5769-5776` (`PerformRecoveryXLogAction`): "In promotion, only
create a lightweight end-of-recovery record instead of a full checkpoint. A checkpoint is requested
later, after we're fully out of recovery mode and already accepting queries."

So a freshly promoted primary serves on TL=N+1 while reporting TL=N, until the requested checkpoint
completes. In the ADR's exact target window -- post-failover, fence not yet taken -- both primaries
can report TL=N, which routes to `resolve_with_equal_timelines`: a branch the entire case matrix
never considers. The same stale value also builds the `.history` filename, so the tool would read
the *old* timeline's history.
**Fix -- scanner + ADR text.** Capture a promotion-independent timeline alongside the control-file
one (`pg_walfile_name(pg_current_wal_lsn())` on a primary gives the serving TL directly), prefer it
in `get_timeline`, and document the window in the ADR. *Rejected:* forcing a checkpoint from the
scanner -- unacceptable side effect on a production primary.
**Evidence that would settle it:** on a node within a few minutes of promotion, compare
`pg_control_checkpoint().timeline_id` against `pg_walfile_name(pg_current_wal_lsn())`.

### F6 -- §5's capture-first captures nothing observable. HIGH
**Code: yes (verified by grep).**
`last_wal_replay_lsn` / `last_wal_receive_lsn` land in `ReplicaHealthCheckResult` and go nowhere:
the info-level completion event omits them, `details_json` is built from the verdict rather than
node health, and the CSV has no node-health columns. They appear only in the debug-level raw JSON
dump; the default level is info. Commit 11's stated purpose -- "gather the evidence so the next real
C-g is diagnosable" -- is therefore unmet: a scan taken during a real C-g records nothing.
**Fix -- writer/scanner.** Emit both LSNs plus the control-file TLI in the replica's info-level
completion event (that is where `apply_lag_bytes` already goes). Smallest change that makes the
stated purpose true. *Rejected:* adding CSV columns -- changes the report schema for a diagnostic
that is only interesting during an incident.

### F7 -- The freshness threshold is right by luck: correct number, wrong mechanism, wrong GUC. MEDIUM
**Source: confirmed on REL_15_14. Fleet: confirmed -- and it rescues the number.**
*Downgraded from HIGH after the infra review; my earlier "60 s window" claim was wrong -- that is
the postgres default, and this fleet sets `wal_receiver_timeout = 5min` on all 321 nodes.*

What actually happens on an idle-but-healthy link:
- Primary-initiated keepalives are **fully suppressed**. `WalSndKeepaliveIfNecessary`
  (`walsender.c:3670-3697`) fires only if `last_processing >= last_reply_timestamp +
  wal_sender_timeout/2`, and `last_reply_timestamp` is reset by *any* standby message
  (`walsender.c:2001-2005`). Standbys send a status reply every `wal_receiver_status_interval`
  (10 s), so the 150 s ping time is never reached.
- With no WAL to ship, the primary sends nothing at all (`XLogSendPhysical` returns early at
  `walsender.c:2905-2910`).
- The heartbeat is therefore **standby-initiated**: `walreceiver.c:536-560` -- "If we haven't heard
  anything from the server for more than **`wal_receiver_timeout / 2`**, ping the server" -- and the
  primary answers at `walsender.c:2143` (`if (replyRequested) WalSndKeepalive(false, ...)`), which
  is what refreshes `last_msg_receipt_time`.

So the real cadence is `wal_receiver_timeout/2` on the standby = 150 s on this fleet, and the 180 s
threshold does sit comfortably above it. **ADR:28/85/87 credit the wrong side and the wrong GUC**,
and `split_brain.rs:266` derives the replica-side threshold from the *primary's*
`wal_sender_timeout`. It yields the right number only because all 321 nodes set both GUCs to 5min.
Change `wal_receiver_timeout` alone and the gate silently mis-sizes, with no test to catch it.

Related and still standing: `reply_time` is the *standby's* clock, so the primary-side freshness
check is inter-node despite the code comment at `:242-244` claiming intra-node skew-immunity.

**Fix -- ADR text now; scanner later.** Rewrite ADR:85-87 to describe the standby-initiated ping.
Optionally collect `wal_receiver_timeout` (`health_check_replica.rs:109-112` collects four keys and
not this one) and derive the replica-side threshold from it, so the coupling is explicit rather than
coincidental. *Rejected:* changing the constant without changing the derivation -- the number is
already correct; the defect is that nobody would notice when it stops being correct.

### F8 -- The concepts doc states a Refuse the tool does not perform. HIGH (doc)
concepts:85 says an unobservable db003 yields `Refuse`. It yields `HigherTimeline` + "demote". The
next paragraph correctly says detection is deferred, so :85 reads as description. It contradicts
ADR:47, which admits the mis-pick.
**Fix -- concepts text.** Mark :85 as the target state and state the current behaviour beside it.

### F9 -- Hardcoded quorum parenthetical (the seeded lead). HIGH
**Partially upheld; one sub-claim refuted.**
- Item 3 "violated because `format_resolution` never reads `findings`": **refuted.** ADR:177 says the
  finding is consumed by the template, so hardcoding conforms to the letter of the spec.
- Sub-claim 1 (only emitter; silent when SSN empty/unparseable): **upheld.** The "stale primary has
  enough gated followers of its own" branch is *not* reachable while the variant fires -- that part
  of the lead is overstated.
- Sub-claim 2 (empty SSN not covered by the `SynchronousCommitWeakened` gate, so a locally-acking
  primary is described as "quorum unsatisfied"): **upheld**, and promoted to F3.
- Sub-claim 3 (step 1 self-contradictory): **upheld.**
- Sub-claim 4 (the MUST holds only when SSN parses): **upheld.**
- Additional: under `HigherTimeline` the parenthetical's subject is wrong -- the finding names the
  *elected* primary, the text attaches quorum failure to the *stale* one, and the template
  simultaneously asserts the elected primary "has quorum".
- Provenance: the plan's own sketch (`plans/2026-05-20-...md:125-128`) computes a `quorum_blocked`
  boolean and never uses it. The intent was to condition the parenthetical; it was lost in
  transcription, and the shipped code dropped the dead binding.

**Fix -- writer, primarily.** Derive each parenthetical from `info.findings` for the node it is
attached to; when no corresponding finding exists, omit the parenthetical rather than assert it. Fix
the `HigherTimeline` template so it does not claim the elected primary "has quorum" while a
`PrimaryQuorumUnsatisfied` finding names it. *Rejected:* fixing this only in ADR text (blessing the
hardcode) -- it is the operator-facing justification for a destructive action, so it should be
evidence-backed; *also rejected:* making the resolver stuff pre-rendered text into a new field --
line 166 explicitly forbids new resolver fields and the writer already owns action text.

### F10 -- Confidence is invisible to the operator. HIGH
`info.confidence` is read in exactly one place in the writer (the Refuse branch), so `Conflicting`
and `BestEffort` render byte-identically. A verdict the resolver has flagged as internally
contradictory looks exactly like a clean one on screen; the distinction survives only in
`details_json`. Combined with F2 (every split brain is `Conflicting`), the state carries no
information today and will carry none after F2 is fixed unless it is rendered.
**Fix -- writer.** Prefix or annotate the short string on `Conflicting`. The ADR mandates rendering
only for `Refuse`, so this needs an ADR sentence too.

### F11 -- Indeterminate hides a nondeterministic tiebreaker. MEDIUM
`primaries_with_highest_timeline[0]` after a stable sort is whichever primary the scan pipeline
delivered first, so `true_primary` under `Indeterminate` varies between runs on identical cluster
state. ADR:268 says no tiebreaker is added; the PRIMARY column renders `{true} vs {stale}`, which
reads as a pick.
**Fix -- resolver (sort by node name for determinism) + ADR text.**

### F12 -- Line 7 contradicts §7 on the Refuse-only floor. MEDIUM (doc)
Covered above. The rest of the §6/§7 deferral is stated consistently across all nine reference sites
plus the plan -- several agent claims to the contrary were refuted.

### F13 -- No reference site states the interim C-g behaviour. MEDIUM (doc)
Every site says what is deferred and why; none says that until detection lands, C-g prints
`SplitBrain: <db002> has quorum (TL=N+1), demote <db001> (TL=N, no live replicas)` at
Conflicting/BestEffort confidence. That sentence is the one an on-call reader needs.
**Fix -- ADR text (row C-g and fact 4).**

### Lower-severity, factual
- **L-1** `remote_flush` is not a `synchronous_commit` value (ADR:96). Prod error text confirms. No
  behavioural effect.
- **L-2** `catchup` is not a `pg_stat_wal_receiver.status` value (ADR:69). The replica-side arm
  accepts an impossible string; `waiting` and `restarting` are silently rejected and never discussed.
- **L-3** `README.md:231` prints `SplitBrain: replica overrides timeline (7 < 8)` -- verbatim the
  phrasing ADR:157 lists as "Not acceptable". `SPEC.md:401` still uses the old variant name. The
  enum doc comment at `split_brain.rs:33` still describes the old mechanism.
- **L-4** ADR:49 says "three facts", lists four.
- **L-5** ADR:267 test count is five, not six.
- **L-6** The "(Validated 2026-09-10)" NULL-vs-`0/0` claim is *true*, but cites PG17 for a PG15
  fleet. The guard exists in 15.14 too, so the claim survives its wrong citation.
- **L-7** `ReplicaFollowing`'s template needs a timeline the variant does not carry, while line 166
  forbids adding fields -- internally unsatisfiable; writer drops the TL.
- **L-8** The fixture is internally inconsistent: `timeline_id: 11` with a ten-entry history (both
  consistent with `0000000B.history`) but `last_archived_wal` `00000011...`, whose TLI field is hex
  `0x11` = 17. One field was hand-edited. This does not weaken F1, which rests on three
  corroborating fixture fields *plus* prod psql *plus* two pieces of source.
- **L-9** The writer interpolates raw FQDNs into the REASON column while normalising the same node to
  `db002@sto2` in the PRIMARY column two cells earlier.
- **L-10** concepts:89-95 labels the wedge signature `FATAL`; it is `ereport(LOG)`.
- **L-11** ADR:29/204/266 say the scanner has `pg_read_server_files`. That grant does not exist
  anywhere in infra. It works because every ops domain user is provisioned `SUPERUSER`
  (`postgres_setup_dynamic/tasks/7-setup-postgres-domain-users.yml:6`), and db-scan connects as a
  *personal* operator account (`~/.config/db-scan/config.yml`), falling back to the bootstrap
  `postgres` superuser on non-cert dev nodes. The capability is real; the stated mechanism is
  false. Rewrite to name the actual invariant, since "grant `pg_read_server_files`" is what a
  future reader would try to verify.
- **L-12** ADR:257's "hostname-form `primary_conninfo` out of scope" is now moot: 214/214 rendered
  `primary_conninfo` use IPv4 literals, and the value comes from inventory `ansible_host`. It is
  structurally enforced, not merely the current convention.
- **L-13** `cluster.rs:31` emits a Cluster at exactly 3 accumulated nodes. That -- not the fleet
  shape -- is why `analyze.rs:323`'s `replicas.len() > 2` guard is dead. It also means a
  hypothetical oversized cluster would silently drop surplus nodes and produce a confident wrong
  answer rather than `UnexpectedTopology`. Latent; the fleet is 358/358 three-node clusters.
- **L-14** `Node::cluster_name()` takes the first three hyphen segments, so the nine citus nodes
  `dev-ct-cluster001-{coordinator,worker001,worker002}-dbNNN` all render the display name
  `dev-ct-cluster001`. Grouping is by portal `cluster_id`, so this is *not* a false split brain --
  but three distinct clusters share one label, and `--filter`/watch matching (`main.rs:256,274`)
  will match all three. Cosmetic, dev-only.

### Explicitly refuted (reported so they are not re-litigated)
The `>2 replicas` guard being dead code; the dual-acking `Both` branch being reachable (both need
>=4 nodes); the `"sanity gate failed"` fallback being live (no emitter exists); `BestEffort` being a
default rather than a cap; the findings cap/ordering being violated; the §4/§7 `DivergentReplicaWal`
deferral being self-contradictory; boolean aliases evading the `synchronous_commit` denylist.

---

## 4. Fleet evidence: what is settled and what is not

**Settled by measurement:** the naming mismatch (F1/F2); PG version; SSN/`synchronous_commit`/
`wal_sender_timeout` values and units; hex history-file naming; history file format and that the
current TL's fork is its last line; `pg_stat_wal_receiver` returning zero rows (so the scanner's
`COALESCE` is dead and there is no deserialisation trap); enum alias normalisation; `remote_flush`
not existing; `pg_read_server_files` satisfied via superuser; `receive_lsn` NULL on a real primary.

**Settled by source (REL_15_14):** sync-rep ordering and the cancel warning (F4); `SyncStandbysDefined`
fast-exit (F3); walreceiver status vocabulary (L-2); the promotion checkpoint deferral (F5).

**Settled by config-as-code (`~/work/infra`):** F1 fleet-wide by construction; the exact transform
to use for the fix; SSN/`synchronous_commit`/both timeouts uniform across 321 nodes; SSN always
self-excluding (so ADR:258's premise holds, and it holds post-failover too, since the config is
per-node and role-independent); topology 358/358 three-node 1+2; no NAT/VIP/proxy in the
replication path, so the `client_addr` join is safe; `primary_conninfo` always IP-form;
`pg_read_server_files` never granted but superuser makes it moot; and F3's empty-SSN state
reachable by two documented operator procedures.

**Not settled, and I am not substituting argument for evidence:**
- Whether any node *currently* has empty SSN, and whether the kickstart playbook has ever run. The
  infra review shows the path exists and is supported; it did not check live state or run history.
- The size of F5's window in practice. Needs a post-promotion capture.
- What `pg_stat_wal_receiver` actually contains on a wedged replica -- the ADR's central §7 premise.
  Still unobserved, exactly as §7 admits.
- F7's 150 s cadence is derived from source, not measured on an idle link.

**Worth running next:**
- `git -C ~/work/infra log --oneline -- ansible/postgres-kickstart-standalone-db001-primary.yml` and
  any run history for it -- that tells you whether F3 is theoretical or has already happened.
- On a quiet cluster: sample `SELECT now() - last_msg_receipt_time FROM pg_stat_wal_receiver` every
  10 s for 10 minutes; the max should stay under ~152 s (confirms F7's corrected model).
- At the next promotion: `pg_control_checkpoint().timeline_id` vs
  `pg_walfile_name(pg_current_wal_lsn())` every 15 s for five minutes (sizes F5's window).

---

## 5. Overall call on the ADR's health

**It needs a revision pass before more code is written against it -- but it is still the right
document, and the code is not the spec.**

The reasoning layer is in good shape. The §7 rework, the anchor-to-the-fork argument in the concepts
doc, the danger/detectability anti-correlation, and the honesty about having no captured C-g are
better than most ADRs get. The deferral is stated consistently in nine places. Where agents claimed
the ADR contradicts itself, the adversarial pass refuted most of it.

What has actually gone wrong is narrower and more specific: **the ADR's model of node identity is
wrong, and everything downstream inherited it.** One false assumption at line 27 -- that
`application_name` equals the node name -- is implemented literally at `split_brain.rs:307`, and it
disables the gate, the quorum derivation, four of five resolution variants, and five of seven matrix
rows. The tests encode the same false assumption, so the suite is green. That is not spec drift; it
is a spec defect faithfully implemented.

Three things follow:

1. **The code is not ahead of the spec.** Only two places should be settled in the code's favour:
   intra-node freshness (line 225) and the `format_refuse` shape. Everywhere else the ADR is either
   right and unimplemented, or wrong and implemented.
2. **The revision pass is mostly not about §7.** It is: fix line 27's identity model and §1's line
   73; add the empty-SSN antecedent to §2; correct "cannot commit" to "cannot acknowledge"
   throughout the ADR and the concepts doc; add an equal-timelines row to the matrix and note the
   promotion window; state the interim C-g behaviour; and delete the line 7 / line 245 contradiction.
   §6 and §7 need no design change.
3. **Priority order for the follow-up session:** F1+F2 together as one change, using the Ansible
   name transform (it closes both). Then F3, whose severity rose once infra showed the unsafe state
   is manufactured by the standard remediation for the very incident class the resolver handles.
   Then F9+F10 (the operator-facing text). Then the doc corrections. F5 needs a measurement before
   it needs a fix; F7 needs only an ADR rewrite.

**On the main risk to this assessment.** In the first pass I flagged that eight findings and five
matrix rows rested on a single premise -- that `application_name` never equals `Node.name`. That
premise is now closed off in the strongest available way: Ansible defines the application name as
an explicit transform of the same `inventory_hostname` that becomes `node_name`, in one file
symlinked into all five environments, with no override anywhere and 214/214 rendered configs
agreeing. F1 and F2 are no longer inferences from a sample; they are properties of the
configuration management system.

What remains uncertain is narrower and honestly held: whether F3's empty-SSN path has ever actually
been executed, how wide F5's promotion window really is, and what a wedged replica exposes -- the
last of which the ADR itself flags as the thing nobody has captured.


---

# Appendix A -- complete finding inventory (raw agent output)

Preserved verbatim from the 27-agent verification run. Severity/classification are as the
agent reported them, BEFORE the adversarial critic's adjudication -- section 3 of this document
is the adjudicated view and takes precedence where they disagree.


## ADR-002 section 4, lines 127-151: findings list, ordering/cap, ReplicaOverridesTimeline -> LowerTimelineHasQuorum rename, the seven v1 finding categories, and the four-step PrimaryQuorumUnsatisfied derivation rule (verified against SplitBrainFinding, emit_quorum_findings, and sync_standby_names::parse)

### [critical | diverges] ADR lines 147
- **Claim:** Step 3: `observed = |members ∩ gated_followers|`
- **Justification:** The two sides of the intersection carry different name forms on the real fleet, so `observed` is structurally pinned at 0 for every primary.
- **Code:** `src/v2/analyze/split_brain.rs:660-663 `let observed = members.iter().filter(|m| gated.iter().any(|g| g == *m)).count() as u32;` -- `members` come from `parse(synchronous_standby_names)` (line 650), which on the captured fleet yields `["dev_pg_app001_db002", "dev_pg_app001_db003"]`, while `gated` is populated from `replica.node_name` at src/v2/analyze/split_brain.rs:322 `.push(replica.node_name.clone());`, i.e. `dev-pg-app001-db002.sto2.example.com`. Cross-check: tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json carries `"synchronous_standby_names": "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"` alongside `"node_name": "dev-pg-app001-db002.sto2.example.com"`. The bridge exists but is not used here: src/v2/writer/build.rs:422 `fn normalize_application_name(app_name: &str) -> String`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** Already largely settled by tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json. To close the remaining gap (no 2-primary capture exists), on any fleet node run: `SELECT current_setting('synchronous_standby_names');` and `SELECT application_name, state FROM pg_stat_replication;` and compare both against the `node_name` db-scan's inventory carries for the same host. A capture of an actual 2-primary scan is what is missing, not the naming evidence.
- **Fix (resolver):** In `emit_quorum_findings` (src/v2/analyze/split_brain.rs:636-675), compare SSN members and gated followers on a normalized key rather than raw strings: map each member through the application-name normalizer (`dev_pg_app001_db002` -> `db002`) and each gated node_name through the node-name normalizer (`dev-pg-app001-db002.sto2.example.com` -> `db002`), then intersect. Both normalizers already exist as `normalize_application_name` (src/v2/writer/build.rs:422) and the `db` token split inside `extract_db_number` (src/v2/writer/build.rs:408); move a single shared `db_token(&str) -> &str` helper into a place both the analyzer and writer can call, or duplicate the four-line split in the resolver. Add a regression test whose primary uses SSN `ANY 1 ( dev_pg_app001_db002 )` and whose replica node_name is `dev-pg-app001-db002.sto2.example.com`, asserting NO `PrimaryQuorumUnsatisfied`. Note this fix is necessary but not sufficient: the same normalization is missing at src/v2/analyze/split_brain.rs:307, so `gated` is itself always empty on the fleet; both compares must be fixed together or `observed` stays 0.
- **Alternatives rejected:** (a) Normalizing in the writer is wrong: `observed`/`required` are computed in the resolver and drive `determine_confidence_level` (src/v2/analyze/split_brain.rs:404-410); by the time the writer sees the finding the count is already wrong. (b) Normalizing inside `sync_standby_names::parse` is wrong: the parser's job is to reproduce what postgres sees, and postgres genuinely matches SSN members against `application_name`, not against node_name. The impedance mismatch belongs to the resolver, which is the only place that holds both forms.

### [high | diverges] ADR lines 150
- **Claim:** "Implementations MUST emit `PrimaryQuorumUnsatisfied` for the stale primary in this case" (when resolution is `LowerTimelineHasQuorum`)
- **Justification:** `emit_quorum_findings` returns early whenever the stale primary's `synchronous_standby_names` is empty/unset or unparseable, so the MUST silently fails; the repo's own test at split_brain.rs:1298 hits exactly this state.
- **Code:** `src/v2/analyze/split_brain.rs:650-652 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue; };` -- combined with line 646-649 `.get("synchronous_standby_names").map_or("", String::as_str)` and sync_standby_names.rs:26-28 `if s.is_empty() { return None; }`. Concrete in-tree instance: src/v2/analyze/split_brain.rs:1298 `fn gate_accepts_catchup_status_and_emits_replica_in_catchup()` asserts `SplitBrainResolution::LowerTimelineHasQuorum` (line 1330) with stale primary `db002` built by `primary(2, "db002", IP_DB2, 12)`, whose `PrimaryHealthBuilder` default (src/v2.rs:59-70) sets no `synchronous_standby_names` -- so no `PrimaryQuorumUnsatisfied` is emitted and no assertion catches it.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `SELECT current_setting('synchronous_standby_names');` on every primary in the fleet, to establish whether any production primary runs with an empty/unset SSN. If all fleet primaries always carry a parseable `ANY n (...)`, the fleet-reachability of this specific gap drops to low even though the code path is live.
- **Fix (combination):** Two-part. (1) resolver: in `emit_quorum_findings` (src/v2/analyze/split_brain.rs:636), replace the blanket `continue` with an explicit three-way split -- empty SSN => genuinely no sync requirement, emit nothing (correct today, but make it explicit and comment it); unparseable non-empty SSN => emit a distinct signal rather than nothing (see the separate step-1 finding); parsed => current logic. (2) tests: add an assertion to `gate_accepts_catchup_status_and_emits_replica_in_catchup` (split_brain.rs:1298) -- or a new test -- that when the resolution is `LowerTimelineHasQuorum` and the stale primary has a parseable SSN with count>=1, `PrimaryQuorumUnsatisfied { primary: <stale> }` is present. (3) adr-text: line 150's "so `observed = 0 < count` always holds" must be qualified with "provided the stale primary's SSN parses to count >= 1"; as written the MUST is unconditional and unachievable. Note also the parse-accepts-`ANY 0` edge (sync_standby_names.rs:50 accepts count=0, which postgres itself rejects at config load): `observed 0 < 0` is false, another way the MUST fails, though not fleet-reachable.
- **Alternatives rejected:** Do NOT synthesize the finding in `resolve_with_different_timelines` at src/v2/analyze/split_brain.rs:457 just because the variant fired. `required`/`observed` would then be fabricated numbers with no SSN behind them, and `determine_confidence_level` (line 404) would consume them as if measured. Also rejected: fixing it only in the writer -- the writer's `format_resolution` already hardcodes "quorum-blocked" for this variant, which masks the missing finding in `short` while leaving `details_json` (writer/build.rs:660 serializes `info` wholesale) with no supporting evidence for the claim in `short`.

### [medium | self-contradictory] ADR lines 145
- **Claim:** Step 1: "Treat unparseable as method=ANY, count=infinity (defensive: emit no finding rather than a wrong one)."
- **Justification:** count=infinity makes step 4's `observed < count` unconditionally true, which emits a finding for every primary -- the exact opposite of the parenthetical's stated intent.
- **Code:** `src/v2/analyze/split_brain.rs:650-652 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue; };` -- the code implements the parenthetical (emit nothing), not the literal rule. Step 4 is src/v2/analyze/split_brain.rs:665 `if observed < count {`.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Rewrite ADR-002 line 145's final sentence. The sentinel that actually produces "emit no finding" under step 4's `observed < count` predicate is count = 0, not count = infinity. Preferred wording: "Treat an unparseable non-empty value as *unknown quorum*: skip the primary entirely, emitting no `PrimaryQuorumUnsatisfied` (defensive -- we would rather say nothing than assert a requirement we could not read)." Dropping the fabricated `{method: ANY, count: N}` triple removes the contradiction rather than papering over it, and matches what src/v2/analyze/split_brain.rs:650 does. If instead the intent was to flag the unreadable config, that needs a separate finding variant, not an infinite count.
- **Alternatives rejected:** Changing the code to match the literal rule (count = infinity) is rejected: it would emit `PrimaryQuorumUnsatisfied { required: u32::MAX, observed: 0 }` for every primary whose SSN the parser cannot handle, which given the parser's known brittleness (mixed-case `Any`, bare-count `2 (a,b,c)`) turns a parser gap into a fleet-wide false-positive quorum alarm. Also rejected: leaving the ADR text as-is on the grounds that the code is right -- the ADR is the spec a reimplementer follows, and this clause is actively misleading.

### [high | unverifiable] ADR lines 143-148
- **Claim:** The four-step derivation rule (implicitly: everything the resolver needs to know about `synchronous_standby_names`)
- **Justification:** The rule never mentions EMPTY `synchronous_standby_names`, which `parse()` maps to `None` identically to unparseable, and which also silently voids the cluster's quorum-sync safety argument with no sanity gate firing.
- **Code:** `src/v2/analyze/sync_standby_names.rs:26-28 `let s = input.trim(); if s.is_empty() { return None; }` -- indistinguishable at the call site from an unparseable value, both hitting src/v2/analyze/split_brain.rs:651 `else { continue; }`. The sanity-gate loop that could have caught it only inspects `synchronous_commit`: src/v2/analyze/split_brain.rs:164-174, with `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` at line 13.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `SELECT name, setting FROM pg_settings WHERE name IN ('synchronous_standby_names','synchronous_commit');` on every primary in the fleet. If any primary reports an empty `synchronous_standby_names` with a non-`local`/`off` `synchronous_commit`, that is a live instance of a cluster believed to be quorum-protected that is not, and the tool reports nothing today.
- **Fix (combination):** (1) adr-text: add a step 0 to the derivation rule at ADR-002 line 145 distinguishing the three input states -- empty (no sync standbys configured: quorum requirement is vacuously satisfied, emit no `PrimaryQuorumUnsatisfied`), unparseable non-empty (unknown, emit nothing per the corrected step 1), parsed (steps 2-4). (2) resolver/ADR-002 section 2: `synchronous_standby_names = ''` combined with `synchronous_commit = remote_apply` disables synchronous replication just as completely as `synchronous_commit = local`, yet only the latter trips `SynchronousCommitWeakened` (split_brain.rs:169). Since the cluster's write-divergence safety argument rests on quorum sync being live, an empty SSN on a primary deserves a sanity-gate finding of its own. Adding that is a section-2 change (new finding variant + Refuse mapping in `determine_confidence_level` at split_brain.rs:396) and should be raised as an ADR amendment before implementing, not slipped into section 4. (3) tests: add a `parse("")` vs `parse("garbage")` distinction test in sync_standby_names.rs so the two states stop being observationally identical.
- **Alternatives rejected:** Making `parse` return `Some(Quorum { count: 0, members: vec![] })` for the empty string is rejected -- it collapses "no sync configured" and "sync configured but unreadable" into one value at the type level, which is precisely the ambiguity this finding is about. The distinction has to survive to the call site.

### [medium | diverges] ADR lines 129
- **Claim:** "Cap at ~5 surfaced items."
- **Justification:** No cap exists anywhere -- not in the resolver, not in the writer, not in the plan; `serde_json::to_string(info)` serializes the whole vec.
- **Code:** `src/v2/writer/build.rs:660 `let details = serde_json::to_string(info).unwrap_or_else(|_| "{}".to_owned());` -- no truncation. `rg -n "truncate|MAX_FINDINGS|\.take\(" src/` returns only unrelated hits (src/v2/node.rs:27, src/v2/scan.rs:114/169/203/257). The plan (docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md) never mentions a cap, so this was dropped silently rather than deliberately deferred.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Count findings produced by a real 2-primary scan. Upper bound by reading: 1 sysid + 2 sync-commit + (2 primaries x 2 replicas x up to 2 link findings) + 2 quorum = ~13, so the cap is not hypothetical on a 3-node cluster. A capture of an actual split-brain scan would settle the realistic count.
- **Fix (writer):** Cap at render time in src/v2/writer/build.rs, not in the resolver. Concretely: in `split_brain_reason` (writer/build.rs:654), serialize a bounded projection of `info` -- e.g. build a `#[derive(Serialize)]` shadow struct that carries `true_primary`, `stale_primaries`, `resolution`, `confidence`, and `findings.iter().take(5)`, plus a `findings_omitted: usize` count so the truncation is visible rather than silent. Order the take by the ADR's precedence (sanity gates, then contradictions, then corroboration) at that point, which also fixes the ordering finding below. Alternatively, if the intent is that `details_json` stay complete and only the operator-visible summary be bounded, amend ADR-002 line 129 to say so explicitly -- "surfaced" is currently doing undefined work.
- **Alternatives rejected:** Truncating `SplitBrainInfo.findings` in the resolver (e.g. after src/v2/analyze/split_brain.rs:381 `split_brain_info.findings.extend_from_slice(findings);`) is worse and mildly dangerous: `confidence` is derived by folding over the full findings vec at split_brain.rs:384-389 (`.map(|f| determine_confidence_level(f, true_primary)).min()`), so a cap applied there silently changes the verdict's confidence -- dropping a `SystemIdentifierMismatch` or a `DivergentReplicaWal` past position 5 would turn a `Refuse` into a `BestEffort`. Capping in the resolver would also discard evidence from `details_json`, which is the incident-forensics record. Also rejected: capping inside `emit_quorum_findings` -- it only produces one finding per primary, at most 2-3 on this topology, so it is not the source of volume.

### [low | diverges] ADR lines 129
- **Claim:** "Order: sanity-gate failures, then contradictions, then corroboration."
- **Justification:** Sanity gates are correctly first, but contradictions and corroboration are interleaved per (primary, replica) pair inside one loop, so corroboration routinely precedes contradiction.
- **Code:** `src/v2/analyze/split_brain.rs:152-180 establishes the outer order (sysid at 155, `SynchronousCommitWeakened` at 170, then `findings.extend(following_findings);` at 179, then quorum at 180). Inside `build_replica_following_map` the pushes are per-pair, not per-category: `ReplicaWalReceiverStale` at line 294 (contradiction), `BidirectionalFlushingConfirmed` at 324 (corroboration), `ReplicaInCatchup` at 329 (informational), `PrimaryDoesNotSeeReplica` at 335 (contradiction) -- all inside the same `for replica in replicas` body starting at line 268. Two replicas on one primary, the first gate-passing and the second failing the primary-side check, yields [BidirectionalFlushingConfirmed, PrimaryDoesNotSeeReplica]: corroboration before contradiction. No test asserts ordering -- every findings assertion in the file uses `.iter().any(...)`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Fix (combination):** Sort at the point of consumption rather than restructuring the loop. Add `fn finding_rank(f: &SplitBrainFinding) -> u8` next to `determine_confidence_level` (src/v2/analyze/split_brain.rs:395) returning 0 for the two sanity gates + `DivergentReplicaWal`, 1 for `ReplicaWalReceiverStale`/`PrimaryDoesNotSeeReplica`/`PrimaryQuorumUnsatisfied`, 2 for `BidirectionalFlushingConfirmed`/`ReplicaInCatchup`, and apply `sort_by_key` (stable, preserving per-pair order within a rank) immediately after src/v2/analyze/split_brain.rs:381. Separately, ADR-002 line 129's three buckets do not cover all seven categories it lists at lines 135-141: `ReplicaInCatchup` is labelled "informational" and `PrimaryQuorumUnsatisfied` "derivation rule below", and neither is a sanity gate, a contradiction, or corroboration -- assign both a bucket in the ADR text or the ordering rule stays unverifiable for 2 of 7 variants.
- **Alternatives rejected:** Restructuring `build_replica_following_map` into three passes over the same (primary, replica) product is rejected -- it triples the loop for a presentational property and would need the gate result cached anyway. Sorting in the writer is rejected because ADR line 129 states the order as a property of `SplitBrainInfo.findings` itself, which is what `details_json` serializes.

### [medium | diverges] ADR lines 145
- **Claim:** Step 1 parsing of `synchronous_standby_names` -- "we accept a pragmatic subset matching repmgr-generated configs"
- **Justification:** `parse()` misreads several spellings postgres accepts; the `*` wildcard case is the one that flips behaviour on a healthy cluster, producing a false `PrimaryQuorumUnsatisfied`.
- **Code:** `src/v2/analyze/sync_standby_names.rs:59-70 `fn split_members` treats every token as a literal name -- `parse("ANY 1 (*)")` returns `Quorum { method: Any, count: 1, members: ["*"] }`, and src/v2/analyze/split_brain.rs:660-663 then counts how many gated followers equal the literal string `"*"`, which is zero, so src/v2/analyze/split_brain.rs:665 `if observed < count` fires. Postgres semantics: `*` matches ANY standby's `application_name`, so with one live standby the quorum is satisfied and the finding is a false positive. Because that finding maps to `Confidence::Conflicting` when it lands on the elected primary (split_brain.rs:404-409), the whole verdict is downgraded.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `SELECT current_setting('synchronous_standby_names') FROM pg_settings LIMIT 1;` across the fleet, checking for any `*` member. The one captured cluster uses an explicit two-name list (`ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )`), so `*` is unattested on this fleet; repmgr does not generate `synchronous_standby_names` at all (it is operator-set in postgresql.conf), so the likelihood turns entirely on operator convention, not on repmgr.
- **Fix (scanner):** In `emit_quorum_findings` (src/v2/analyze/split_brain.rs:660), special-case the wildcard before intersecting: if `members` contains `"*"`, set `observed = gated.len() as u32` rather than intersecting, since postgres treats `*` as matching every connected standby. Do this in the resolver, not the parser, so `Quorum.members` keeps reproducing the literal config. Add a `parse("ANY 1 (*)")` unit test in sync_standby_names.rs asserting `members == ["*"]`, and a resolver test asserting no `PrimaryQuorumUnsatisfied` when one gated follower exists and SSN is `ANY 1 (*)`.
- **Alternatives rejected:** Expanding `*` to the full follower list inside `parse` is rejected -- the parser has no access to the follower set and would have to take it as an argument, coupling a pure config parser to scan state. Rejected also: treating `*` as unparseable (returning None) -- that silently suppresses a legitimate quorum check rather than evaluating it.

### [medium | diverges] ADR lines 145
- **Claim:** Step 1 parsing -- keyword and whitespace handling ("Postgres also supports ... full SQL identifier rules; we accept a pragmatic subset")
- **Justification:** `parse()` requires an exact-case keyword followed by exactly one ASCII space; three postgres-legal spellings fall through to the legacy branch and yield count=1 plus garbage member strings, understating `required` instead of failing loudly.
- **Code:** `src/v2/analyze/sync_standby_names.rs:32-46. Case A, bare count (postgres makes FIRST optional, so `2 (a,b,c)` means FIRST 2): neither `strip_prefix("ANY ")` (line 32) nor `strip_prefix("FIRST ")` (line 35) matches, so line 41-45 returns `Quorum { First, 1, members: ["2 (a", "b", "c)"] }` -- required understated 2 -> 1, members garbage. Case B, mixed case (`Any 1 (a,b)`; postgres keywords are case-insensitive but line 32 only tries `"ANY "` and `"any "`): same legacy fallthrough, `members: ["Any 1 (a", "b)"]`, count 1. Case C, tab or multiple spaces after the keyword (`"ANY\t1 (a,b)"`): same. Case D, quoted names containing commas (`ANY 1 ("db, one", "db two")`): sync_standby_names.rs:61 `.split(',')` splits inside the quotes, yielding `["db", "one", "db two"]`. Handled correctly and worth recording: no space before the paren (`ANY 1(a,b)`) works via `split_once('(')` at line 49; trailing/leading whitespace works via `trim()` at line 25 and `trim()`/`strip_suffix` at line 51; the legacy bare list `db001, db002` correctly becomes FIRST 1 (postgres default num_sync is 1).`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `SELECT current_setting('synchronous_standby_names');` on every primary. The single captured value is `ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )`, which parses correctly, so all four cases here are currently unattested on this fleet.
- **Fix (scanner):** Rewrite the prefix handling in src/v2/analyze/sync_standby_names.rs:30-46 to (a) split off the first whitespace-delimited token with `split_once(char::is_whitespace)` instead of `strip_prefix("ANY ")`, (b) compare it with `eq_ignore_ascii_case("any")` / `eq_ignore_ascii_case("first")`, and (c) add a third arm: if the input starts with a digit and contains `'('`, treat it as the FIRST-implicit form (`2 (a,b,c)` -> FIRST 2). Keep the bare-name-list arm as the final fallback but guard it against inputs containing `'('` so a malformed parenthesized form returns None rather than silently degrading to count=1 with garbage members. Add unit tests for `"2 (a,b,c)"`, `"Any 1 (a,b)"`, `"ANY\t1 (a,b)"`, and `"first 2(a,b)"`. The quoted-name-with-comma case (Case D) needs a quote-aware splitter in `split_members`; given no observed instance, note it in the module doc rather than building the splitter.
- **Alternatives rejected:** Rejected: leaving this alone on the grounds that the one captured cluster parses fine. The failure is silent and directionally unsafe -- count=1 with unmatchable garbage members always yields `observed 0 < 1`, so a misparse looks exactly like a genuinely quorum-blocked primary, and the resulting finding is the one ADR-002 line 158 designates as the explanation of the verdict. Rejected also: making the fallback return None on any input containing a digit -- that breaks legitimate names like `db001`.

### [low | stale] ADR lines 131
- **Claim:** "Rename `SplitBrainResolution::ReplicaOverridesTimeline` to `LowerTimelineHasQuorum`" ... "Not acceptable: `SplitBrain: replica overrides timeline (N < N+1)`"
- **Justification:** The rename landed in code, but three places still carry the old name or the exact phrasing the ADR declares unacceptable -- including the README's advertised sample output.
- **Code:** `Code is correct: src/v2/analyze/split_brain.rs:35 `LowerTimelineHasQuorum {`, and all match arms updated (src/v2/writer/build.rs:246, 704). Stale survivors: (1) src/v2/analyze/split_brain.rs:33 `/// Replica evidence overrides timeline - replicas are following a lower-timeline primary` -- the doc comment on the renamed variant still describes the mechanism the rename existed to kill; (2) SPEC.md:401 `4. **ReplicaOverridesTimeline**: Replicas follow lower-timeline primary` -- old identifier; (3) README.md:231 `CRITICAL prod-pg-app123 db001@sto1<sup>7</sup> vs db002@sto2<sup>8</sup> db003@sto3->db001@sto1 -   -    SplitBrain: replica overrides timeline (7 < 8)` -- this is the literal string ADR-002 line 157 lists as "Not acceptable", presented as the tool's sample output.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (combination):** Three text edits, no behaviour change. (1) src/v2/analyze/split_brain.rs:33-34: replace the doc comment with an outcome-shaped one, e.g. `/// The lower-timeline primary holds quorum -- replicas are actively flushing for it, so the higher-TL primary was isolated after promotion and cannot ack writes.` (2) SPEC.md:401: rename to `LowerTimelineHasQuorum`. (3) README.md:231: regenerate the sample row with what `format_resolution` (src/v2/writer/build.rs:708-711) actually emits today -- `SplitBrain: db001@sto1 has quorum (lower TL=7), fence db002@sto2 (TL=8, quorum-blocked)`. Verify the column widths still line up after the substitution, since that block is a fixed-width table.
- **Alternatives rejected:** Rejected: treating README.md:231 as harmless illustration. ADR-002 line 157 singles out that exact sentence as the paradox-shaped phrasing that triggered the rename; leaving it as the README's headline example is the one place a reader most likely forms their mental model of the output. Rejected: `rg`-and-replace across docs blindly -- ADR-002 lines 21, 131, 264, 265 legitimately mention the old name in rename-history context and must be left alone.

### [info | implemented] ADR lines 135-141
- **Claim:** The seven v1 finding categories, with the payload shapes named at lines 135-141
- **Justification:** All seven variants exist and every named field is present; the three link-shaped ones use a `ReplicationLink` newtype whose serde output is key-identical to the struct variants the ADR writes.
- **Code:** `src/v2/analyze/split_brain.rs:82-109. `SystemIdentifierMismatch { nodes: Vec<NodeName> }` (83-85), `SynchronousCommitWeakened { primary, value }` (86-89), `ReplicaWalReceiverStale { replica, claimed_sender }` (90-93), `PrimaryQuorumUnsatisfied { primary, required: u32, observed: u32 }` (97-101) all match field-for-field. The three the ADR writes as `{ primary, replica }` / `{ replica, primary }` are newtypes: line 94 `PrimaryDoesNotSeeReplica(ReplicationLink)`, line 95 `BidirectionalFlushingConfirmed(ReplicationLink)`, line 96 `ReplicaInCatchup(ReplicationLink)`, over `pub struct ReplicationLink { pub primary: NodeName, pub replica: NodeName }` (111-115). Serde externally-tagged encoding of a newtype-over-struct is `{"PrimaryDoesNotSeeReplica":{"primary":...,"replica":...}}` -- byte-identical to the struct-variant form, so `details_json` (src/v2/writer/build.rs:660) matches the ADR. The only cosmetic delta is `ReplicaInCatchup`, which the ADR writes `{ replica, primary }` and the code emits as `{primary, replica}` -- key order only, same keys.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No fix required. If exact JSON key order for `ReplicaInCatchup` matters to a downstream consumer, the cheaper change is ADR-002 line 140 -> `ReplicaInCatchup { primary, replica }`, matching the other two link findings and the code, rather than adding a bespoke struct variant.
- **Alternatives rejected:** Rejected: converting the three newtypes back to struct variants to match the ADR literally -- `ReplicationLink` is used at four construction sites (split_brain.rs:325, 330, 336) and in `determine_confidence_level`'s match (412-414), and the serialized output is already correct, so the change is churn with no observable effect.

### [info | implemented] ADR lines 146, 148
- **Claim:** Step 2: "excluding replicas filtered by section 2 (foreign sysid)"; Step 4: "Emit if `observed < count`"
- **Justification:** Sysid exclusion happens before the gate and so cannot reach `observed`; step 4's predicate is literal; discarding `method` is correct because postgres blocks commits below num_sync under both ANY and FIRST.
- **Code:** `Step 2: src/v2/analyze/split_brain.rs:146-150 builds `filtered_replicas` by `.filter(|r| !mismatched_nodes.contains(&r.node_name))`, passed to `build_replica_following_map` at line 178, whose output map is the sole input to `emit_quorum_findings` at line 180 -- so an excluded replica can never appear in `gated` (split_brain.rs:656-659). This satisfies ADR line 98's stronger phrasing ("Exclusion happens **before** `build_replica_following_map`"). Step 4: src/v2/analyze/split_brain.rs:665 `if observed < count {`. Method is discarded at line 650 via `Quorum { count, members, .. }`. That is correct, not a gap: for `FIRST 2 (a,b,c)` postgres selects the first 2 *connected* listed standbys by priority and blocks commits when fewer than 2 are connected -- identical unblocking threshold to `ANY 2 (a,b,c)`. Method changes *which* standbys are chosen, not how many must be present, and `PrimaryQuorumUnsatisfied` only asserts the latter.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Optional clarification only: add a sentence to ADR-002 after line 148 recording that `method` is deliberately unused by step 4, with the reason (both ANY and FIRST block commits when fewer than num_sync listed standbys are streaming), so a future reader does not 'fix' the resolver by branching on it. Mirror the note as a code comment at src/v2/analyze/split_brain.rs:650 where `..` discards it.
- **Alternatives rejected:** Rejected: branching on `method` in `emit_quorum_findings` to model FIRST's priority ordering. It would change nothing about the emit predicate and would introduce a wrong distinction -- under FIRST, a primary with the *lowest-priority* standby connected and num_sync=1 is still able to ack, exactly as under ANY.

### [info | implemented] ADR lines 150
- **Claim:** "when the resolution is `LowerTimelineHasQuorum`, the higher-TL (stale) primary by definition has zero gated followers (that's the precondition for the variant firing)"
- **Justification:** Verified rather than assumed: the guard requires the highest-TL primary's follower list to be empty, so the members-intersect-gated set is empty regardless of what SSN lists -- the 'all members are gate-passing followers' escape hatch genuinely cannot co-exist with the variant.
- **Code:** `src/v2/analyze/split_brain.rs:449 `if !replicas_following_stale.is_empty() && replicas_following_highest.is_empty() {` -- `replicas_following_highest` is read at lines 430-433 keyed on `highest_tl_node.node_name`, and line 456 `stale_primaries: vec![highest_tl_node.node_name.clone()],` confirms that same node is the one the MUST refers to. Since `emit_quorum_findings` looks up `gated` by the identical key (split_brain.rs:656 `replicas_following.get(&p.node_name)`), `gated` is empty for the stale primary and `observed = 0` unconditionally. So the ADR's premise about `observed` holds; what fails is the separate assumption that a `count` exists at all (see the step-1 / MUST findings above).`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No fix. Recorded so a later reader does not re-open this branch of the argument. Note one adjacent, separately-scoped divergence found while checking: at src/v2/analyze/split_brain.rs:456 the `LowerTimelineHasQuorum` arm sets `stale_primaries` to a one-element vec containing only the highest-TL node, so with three primaries (TL 5 / TL 4 / TL 3, TL 4 winning) the TL 3 primary is neither `true_primary` nor listed stale and disappears from the verdict entirely -- unreachable on the documented 3-node / 2-primary topology, and belongs to the section-1 resolver review rather than section 4.

### [info | deferred-correct] ADR lines 133-141
- **Claim:** The v1 finding list at lines 135-141 (seven categories) versus the `SplitBrainFinding` enum (eight variants)
- **Justification:** `DivergentReplicaWal` is the eighth variant, absent from the v1 list, and its deferral is stated consistently in the ADR, the plan, and the code -- nothing outside tests constructs it.
- **Code:** `src/v2/analyze/split_brain.rs:102-108 defines `DivergentReplicaWal { replica_node, replica_received_tli, replica_flushed_lsn, fork_tli, fork_lsn }`. It is only ever matched, never constructed in non-test code: src/v2/analyze/split_brain.rs:399 (maps to `Confidence::Refuse`) and src/v2/writer/build.rs:682 (maps to `None` in `format_refuse`). The sole construction is inside the test module (which begins at split_brain.rs:677) at line 782, an rstest case. Deferral is stated in ADR-002 lines 156 and 159-160 and in docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:21-23 ("this commit becomes **capture-only**").`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No fix. Optionally add `DivergentReplicaWal` to ADR-002's list at line 141 with an explicit "(defined, not emitted -- see section 7)" marker, so a reader diffing the enum against the list does not have to reconstruct the deferral from section 7.
- **Alternatives rejected:** Rejected: removing the unused variant. It is load-bearing for the deferred design and already wired into `determine_confidence_level` with the correct `Refuse` mapping, so deleting it would have to be undone when the verdict-flip lands.


## ADR-002 §4 lines 152-180: short-string contract for `format_reason`, action-text ownership table, findings-concatenation rules (verified against `split_brain_reason`/`format_refuse`/`format_resolution` in src/v2/writer/build.rs:654-727 and their tests at build.rs:742-819)

### [high | diverges] ADR lines 158, 177
- **Claim:** "`PrimaryQuorumUnsatisfied` MUST appear inline in the short string when present, since it explains why the higher-TL primary lost."
- **Justification:** format_resolution never reads info.findings at all, so a PrimaryQuorumUnsatisfied naming the TRUE primary is never rendered -- and under HigherTimeline that finding is always present (when SSN parses), producing a short string that asserts the opposite of the finding.
- **Code:** `src/v2/writer/build.rs:692-694 `fn format_resolution(info: &SplitBrainInfo) -> String {` / `let stale = info.stale_primaries.first().map_or("", String::as_str);` -- `info.findings` is never touched in the whole function (695-727). Emitter: src/v2/analyze/split_brain.rs:665-671 `if observed < count { findings.push(SplitBrainFinding::PrimaryQuorumUnsatisfied {...})` runs over ALL primaries, not just stale ones. Confidence side effect: src/v2/analyze/split_brain.rs:404-409 `if primary == true_primary { Confidence::Conflicting }`. Existing test proving the state: src/v2/analyze/split_brain.rs:1536-1564 `quorum_unsatisfied_on_elected_primary_is_conflicting` yields true_primary=db002 + PrimaryQuorumUnsatisfied{db002, required:1, observed:0}; feeding exactly that SplitBrainInfo to split_brain_reason prints "SplitBrain: db002 has quorum (TL=12), demote db001 (TL=11, no live replicas)".`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** The finding-side is already evidenced by the naming forms in tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json: SSN members are `dev_pg_app001_db002` while gated followers are FQDN node_names, so `members ∩ gated` is empty and `observed=0` for EVERY primary including the true one on real fleet data. What is not evidenced is the >=2-primaries precondition -- no captured split-brain run exists. To settle: capture one scan JSON of a real 2-primary cluster, or run the resolver over a synthetic AnalyzedCluster built from the captured node shapes (FQDN node_name + underscore application_name) and print reason.short.
- **Fix (writer):** In format_resolution, before the match, compute `let true_pq = info.findings.iter().find_map(|f| match f { PrimaryQuorumUnsatisfied { primary, required, observed } if *primary == info.true_primary => Some((*required, *observed)), _ => None });` and when it is Some, replace the `{true} has quorum` clause with `{true} elected on TL={hi} but quorum unsatisfied ({observed}/{required})` (and drop `has quorum` entirely). This makes the short string self-consistent with details_json and stops the tool from asserting quorum for a node it recorded as quorum-less.
- **Alternatives rejected:** Appending the finding as a trailing clause ("; db002 quorum 0/1") was considered: it satisfies the letter of item 3 but leaves the contradictory "has quorum" lead intact in the same sentence, which is worse than silence. Fixing it in the resolver (e.g. downgrading to Indeterminate) would change verdicts and belongs to a different section.

### [high | self-contradictory] ADR lines 172
- **Claim:** Table row: `HigherTimeline` -> `SplitBrain: {true} has quorum (TL={hi}), demote {stale} (TL={lo}, no live replicas)`
- **Justification:** HigherTimeline fires only when NO primary has a gated follower, so under `ANY 1 (A, B)` the elected primary provably does NOT satisfy its quorum; the row asserts "has quorum" for a node that fails the ADR's own operational definition of true primary (line 22: "sync quorum is satisfied and that is actively committing") in the same sentence that admits "no live replicas".
- **Code:** `src/v2/analyze/split_brain.rs:484-498 -- the HigherTimeline arm is the `} else {` branch reached only when both `replicas_following_highest` and `replicas_following_stale` are empty: `// No replica evidence - trust timeline`. Writer faithfully prints it: src/v2/writer/build.rs:715-717 `"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)"`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** None to establish the contradiction (it is derivable from the variant precondition). To size the operational risk: a captured 2-primary scan where no replica gate-passes.
- **Fix (adr-text):** Rewrite the HigherTimeline row to drop the quorum claim: `SplitBrain: {true} elected on TL={hi} (no live replicas anywhere -- quorum unverified), {stale} on TL={lo}` and drop the bare `demote` verb for this variant, since the variant's own precondition means the tool has zero positive evidence about either node's write status. Case C-g (line 47) is exactly this state and the ADR itself says the pick is wrong there; the text must not add false confidence on top of an admittedly unreliable pick. Then update build.rs:715-717 to match.
- **Alternatives rejected:** Leaving the row and relying on Confidence::Conflicting to warn the operator does not work: terminal.rs:111 prints only `view.reason.short`, and only Refuse alters the short string (build.rs:655), so Conflicting is invisible in the default sink.

### [high | diverges] ADR lines 177, 170-172
- **Claim:** Findings concatenation: "`PrimaryQuorumUnsatisfied` is the source of the 'quorum unsatisfied/blocked/no live replicas' inline text and is consumed by the template above, not separately appended." (i.e. the hardcoded per-variant parenthetical stands in for the finding)
- **Justification:** The writer prints the parenthetical unconditionally without checking that a PrimaryQuorumUnsatisfied for that stale primary exists; the finding can be absent while the text claims it, and the empty-SSN case inverts the safety implication (a primary with `synchronous_standby_names=''` acks every commit locally, i.e. the exact opposite of quorum-blocked, and is NOT caught by the SynchronousCommitWeakened gate).
- **Code:** `src/v2/writer/build.rs:700-703 `"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, quorum unsatisfied)"` and 708-711 `"... fence {} (TL={}, quorum-blocked)"` -- neither consults info.findings. Emitter gap: src/v2/analyze/split_brain.rs:650-652 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue; };` with src/v2/analyze/sync_standby_names.rs:26-28 `if s.is_empty() { return None; }`. Gate that does not cover it: src/v2/analyze/split_brain.rs:13 `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` keys on synchronous_commit only, so `synchronous_commit=on` + `SSN=''` produces no finding at all. Corroboration that the check was intended and lost: docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:125-127 computes `let quorum_blocked = info.findings.iter().any(...)` and then never uses it in any match arm.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** psql on every primary of every cluster: `SELECT name, setting FROM pg_settings WHERE name IN ('synchronous_standby_names','synchronous_commit');` -- specifically on a repmgr-promoted node right after promotion, to see whether SSN is set by the promotion path or left empty. Equivalently grep stored scan JSON for `"synchronous_standby_names": ""`. Today the only capture (tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json:23) has SSN set on the single primary, so empty-SSN is unobserved. If any primary is found with empty SSN, this becomes critical: the cluster's 'isolated primaries cannot ack' invariant -- the whole basis for 'demote the stale primary' being safe -- does not hold for that node.
- **Fix (combination):** Verify the four sub-claims as follows. (1) CONFIRMED that emit_quorum_findings (split_brain.rs:636) is the only non-test constructor of PrimaryQuorumUnsatisfied (rg shows only split_brain.rs:666 plus test literals), and CONFIRMED it emits nothing for empty/unparseable SSN. The 'stale primary has enough gated followers of its own' escape is PARTLY OVERSTATED: it is impossible under LowerTimelineHasQuorum (split_brain.rs:449 `if !replicas_following_stale.is_empty() && replicas_following_highest.is_empty()` makes zero followers on the stale/higher-TL primary a precondition), and under `Both` it needs >=2 replicas (2 primaries + 2 replicas, one gate-passing for each) -- reachable in code, outside the ADR's stated 1+2 topology. That 4-node `Both` case is the worst instance: the stale primary has a live acking sync standby and the tool still prints 'demote {stale} (quorum unsatisfied)'. (2) CONFIRMED and it is the strongest sub-claim. Fix: make the parenthetical conditional -- render `quorum unsatisfied ({observed}/{required})` only when the matching finding exists; otherwise render `quorum state unknown (synchronous_standby_names empty/unparseable)`. That requires the ADR table rows to gain an else-branch, so the table text must change too.
- **Alternatives rejected:** Fixing only the resolver (emitting PrimaryQuorumUnsatisfied for empty SSN with required=0) is wrong: with SSN empty the quorum genuinely is satisfied-by-vacuity, so the finding would be a lie in the other direction. The honest signal for empty SSN is a separate 'no sync standbys configured -> local-only acks' finding, but inventing a new finding variant exceeds §4's scope; conditioning the writer text is the minimal correct fix.

### [medium | self-contradictory] ADR lines 150
- **Claim:** "Implementations MUST emit `PrimaryQuorumUnsatisfied` for the stale primary in this case [LowerTimelineHasQuorum] ... `observed = 0 < count` always holds."
- **Justification:** The MUST is unconditional but derivation step 1 (line 145) and the code both skip emission when the stale primary's synchronous_standby_names is empty or unparseable, so the MUST holds only when that SSN parses -- and a just-promoted higher-TL primary is precisely the node most likely to have SSN unset.
- **Code:** `src/v2/analyze/split_brain.rs:646-652 `let synchronous_standby_names = h.configuration.get("synchronous_standby_names").map_or("", String::as_str);` then `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue; };`. Precondition side is correct: src/v2/analyze/split_brain.rs:449 guarantees the higher-TL primary has zero gated followers when LowerTimelineHasQuorum fires.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** Same query as the empty-SSN item: `SHOW synchronous_standby_names;` on a freshly repmgr-promoted primary.
- **Fix (adr-text):** Qualify line 150: 'MUST emit ... whenever the stale primary's synchronous_standby_names parses to a Quorum with count >= 1. When it does not parse (empty or malformed), no quorum claim may be made and the short string MUST say so instead of asserting quorum-blocked.' This is the same conditional the writer needs for the parenthetical, so land both together.
- **Alternatives rejected:** Making the resolver emit a synthetic finding with required=count-unknown to satisfy the MUST literally would push an unfounded claim into details_json; qualifying the spec is the honest fix.

### [medium | self-contradictory] ADR lines 156, 159, 161, 178
- **Claim:** Item 1 carve-out: "`DivergentReplicaWal` was to surface inline even under Refuse ... That is deferred ... nothing emits it today, so the carve-out is dormant" versus item 4: "`DivergentReplicaWal`, when present, MUST surface inline in the SplitBrain short string and MUST set `Confidence::Refuse`."
- **Justification:** Item 1 defers the inline surfacing; item 4 keeps 'MUST surface inline' as live and even supplies the exact string to render at line 161. The code splits the difference in the dangerous direction: the Refuse mapping is live while the rendering is absent, so the first ever emission yields the bare fallback 'REFUSE/SplitBrain: sanity gate failed' with no evidence text.
- **Code:** `src/v2/analyze/split_brain.rs:397-399 `| SplitBrainFinding::DivergentReplicaWal { .. } => Confidence::Refuse,` (wiring live) versus src/v2/writer/build.rs:681-684 `| SplitBrainFinding::DivergentReplicaWal { .. } => None,` ... `.unwrap_or_else(|| "sanity gate failed".to_owned());`. 'Nothing emits it today' VERIFIED: rg over the repo finds constructions only at src/v2/analyze/split_brain.rs:782 (an rstest case) -- no production construction site anywhere. Enumeration of Refuse paths: determine_confidence_level (split_brain.rs:395-415) makes only SystemIdentifierMismatch, SynchronousCommitWeakened and DivergentReplicaWal Refuse-level, and confidence is `.min().unwrap_or(BestEffort)` over findings (split_brain.rs:384-389), so Refuse implies at least one of those three is in info.findings; two of the three are rendered, so the fallback is unreachable today and becomes reachable the moment a DivergentReplicaWal emitter lands.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (combination):** Either (a) add the DivergentReplicaWal arm to format_refuse now, rendering exactly the evidence-only string ADR line 161 specifies (`divergent committed WAL -- {replica_node} flushed to {replica_flushed_lsn} past TL={fork_tli} fork @ {fork_lsn}; acked writes may exist only on lower TL`) so the live Refuse wiring can never degrade to 'sanity gate failed'; or (b) make item 1 and item 4 agree that BOTH the Refuse mapping and the rendering are deferred, and remove the Refuse arm at split_brain.rs:399 too. (a) is preferable: it costs six lines, matches §7's 'conservative Refuse-only floor is shippable today', and removes a trap where the safety gate fires with no operator-readable reason.
- **Alternatives rejected:** Leaving the fallback and relying on details_json is not acceptable for this finding: terminal.rs:111 prints only reason.short, so the operator's only channel would say 'sanity gate failed' for the one gate the ADR calls load-bearing against acknowledged-data loss.

### [low | self-contradictory] ADR lines 145
- **Claim:** Derivation step 1: "Treat unparseable as method=ANY, count=infinity (defensive: emit no finding rather than a wrong one)."
- **Justification:** With count=infinity the emit test `observed < count` is ALWAYS true, so the rule as written emits a finding on every unparseable SSN -- the exact opposite of its own stated intent; count=0 (or an explicit skip) is what produces 'emit no finding'. The code implements the intent, not the rule.
- **Code:** `src/v2/analyze/split_brain.rs:650-652 `let Some(Quorum { .. }) = parse(...) else { continue; };` (skips entirely) versus src/v2/analyze/split_brain.rs:665 `if observed < count {`.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Replace step 1's parenthetical with: 'Treat empty or unparseable synchronous_standby_names as no-quorum-claim-possible: emit no PrimaryQuorumUnsatisfied finding, and (per the short-string contract) suppress the quorum parenthetical for that primary.' Drop the count=infinity device entirely -- it is a sentinel that inverts the rule it is attached to.
- **Alternatives rejected:** Keeping count=infinity and flipping the emit test to `observed < count && count.is_finite()` re-states the same skip with more machinery.

### [low | diverges] ADR lines 166, 173
- **Claim:** Table row: `ReplicaFollowing` -> `SplitBrain: {true} has quorum (TL={tl}), demote {stale} (same TL)`; plus "no new fields on the resolver types are required".
- **Justification:** The code omits `(TL={tl})` because `SplitBrainResolution::ReplicaFollowing { replicas_following_true }` carries no timeline and SplitBrainInfo exposes none -- so the ADR row is unimplementable and its own 'no new fields required' claim is false for exactly this row. The code (and the plan) are right to omit it; the ADR text is wrong.
- **Code:** `src/v2/writer/build.rs:719-722 `SplitBrainResolution::ReplicaFollowing { .. } => format!("SplitBrain: {} has quorum, demote {} (same TL)", info.true_primary, stale)` versus the variant definition src/v2/analyze/split_brain.rs:23-26 `ReplicaFollowing { replicas_following_true: Vec<NodeName> }`. The plan already encodes the same omission at docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:144-147. Fleet-shaped instance: src/v2/analyze.rs:1313-1327 produces exactly this variant.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Change the ReplicaFollowing row to `SplitBrain: {true} has quorum, demote {stale} (same TL)` and soften line 166 to 'no new fields are required for the variants that carry timelines; ReplicaFollowing carries none, so its row omits the TL'. Alternatively add `timeline: i32` to ReplicaFollowing and restore `(TL={tl})` -- but that contradicts line 166 and buys the operator little, since 'same TL' already says the timelines are equal.
- **Alternatives rejected:** Recovering the TL in the writer from AnalyzedCluster nodes was considered: format_resolution takes only &SplitBrainInfo, so it would need a signature change to thread the cluster through, which is more churn than the information is worth.

### [low | unverifiable] ADR lines 162
- **Claim:** "`SystemIdentifierMismatch` and `SynchronousCommitWeakened` are escalation-worthy independent of the split-brain verdict; they should drive an alerting path distinct from routine SplitBrain rendering."
- **Justification:** No alerting concept exists anywhere in src (rg -i 'alert|escalat|paging' over src/ returns nothing), and the statement names no sink, channel, exit code, severity field, or artifact -- so 'distinct alerting path' has no observable referent and the item is not falsifiable as written. The only distinction the code makes is that these two are the only findings format_refuse can name.
- **Code:** `src/v2/writer/build.rs:671-676 (the only two rendered arms) and the sink inventory: src/v2/writer/terminal.rs:111 `view.reason.short,` and src/v2/writer/csv.rs:65-66 `view.reason.short, view.reason.details_json.replace('"', "\"\"")` -- two sinks, both routing everything through the same ReasonView.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Either name the artifact ('these two findings MUST set a dedicated Reason variant / a non-zero exit code / a distinct CSV column so an external alert rule can key on them') or move item 5 to a Consequences/Future-work note. As written it can never be marked done or not-done.
- **Alternatives rejected:** Treating the REFUSE/ prefix as the 'distinct path' would be a post-hoc reading: SystemIdentifierMismatch and SynchronousCommitWeakened reach the operator through the same short-string field as every other verdict, and item 5 explicitly says 'distinct from routine SplitBrain rendering'.

### [low | diverges] ADR lines 156-157
- **Claim:** Item 1 example output `REFUSE/SplitBrain: system_identifier mismatch (db003 vs db001/db002)` and item 2 example `... db001 has quorum (lower TL=N), fence db002 ...` (short node names).
- **Justification:** The split-brain short strings interpolate raw node_name, which on real fleet data is an FQDN (`dev-pg-app001-db002.sto2.example.com`), while every other operator string in the same file passes node names through extract_db_number; also the mismatch example renders `nodes.join(", ")` rather than the ADR's `X vs Y/Z` form.
- **Code:** `src/v2/writer/build.rs:702 `info.true_primary, true_primary_timeline, stale, stale_timeline` (raw names) and 672 `Some(format!("system_identifier mismatch ({})", nodes.join(", ")))`, versus src/v2/writer/build.rs:505-506 `extract_db_number(chained_replica), extract_db_number(upstream_replica)` and 623/642. Real names confirmed at src/v2/analyze.rs:1314-1318 and tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Fix (writer):** Wrap the four interpolations in format_resolution and the two in format_refuse with extract_db_number (build.rs:408), matching the rest of the file. Side benefit: terminal.rs:40-48 sizes the REASON column to max short.len(), so three FQDNs in one line widens the whole table.
- **Alternatives rejected:** Changing the ADR examples to FQDNs instead is worse -- it would make the documented examples unreadable and diverge from the case matrix, which uses db001/db002/db003 throughout.

### [low | diverges] ADR lines 156
- **Claim:** Item 1: "Lead with `REFUSE/` and name the failed sanity gate." (rendering of SynchronousCommitWeakened)
- **Justification:** When the finding's value is the empty string -- an explicit member of the weakened list, present to catch a missing/blank setting -- the rendered gate text is `synchronous_commit= on db001`, which an operator reads as 'synchronous_commit = on', i.e. the exact inverse of the finding being reported.
- **Code:** `src/v2/writer/build.rs:674-675 `Some(format!("synchronous_commit={} on {}", value, primary))` combined with src/v2/analyze/split_brain.rs:13 `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` and split_brain.rs:164-167 `.get("synchronous_commit").map_or("", String::as_str)`.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Whether the empty value can occur at all: the collector selects synchronous_commit from pg_settings (src/v2/scan/health_check_primary.rs:163-175), and that GUC always has a value, so the path is likely only reachable when the whole configuration map is empty. Confirm by checking whether any stored scan has a primary with an empty or absent `configuration.synchronous_commit`.
- **Fix (writer):** Render the empty case distinctly, e.g. `let shown = if value.is_empty() { "<unset>" } else { value };` then `format!("synchronous_commit={shown} on {primary}")`.
- **Alternatives rejected:** Dropping "" from WEAKENED_SYNCHRONOUS_COMMIT would silently disable the missing-setting guard, which is a resolver-behaviour change outside §4.

### [info | implemented] ADR lines 179
- **Claim:** Findings concatenation: "Other findings appear in `details_json`, not in `short`."
- **Justification:** details_json is serde_json::to_string of the whole SplitBrainInfo, so every finding variant serialises (externally-tagged enum: struct variants keep their field names, the three newtype variants emit the ReplicationLink object), and short carries none of them -- with the caveat that only the CSV sink prints details_json.
- **Code:** `src/v2/writer/build.rs:660 `let details = serde_json::to_string(info).unwrap_or_else(|_| "{}".to_owned());`; Serialize derives at src/v2/analyze/split_brain.rs:81-115; sinks at src/v2/writer/csv.rs:66 (prints it) and src/v2/writer/terminal.rs:111 (prints only short). Test: src/v2/writer/build.rs:799-818 `findings_appear_in_details_json`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No change required. Worth recording in the ADR that details_json reaches the operator only via the CSV sink; anything the terminal user must see has to be in short. That constraint is what makes the item-3 and HigherTimeline findings above high rather than low.

### [info | implemented] ADR lines 156
- **Claim:** Item 1: "`Confidence::Refuse` overrides the resolution variant in the short string ... The resolution-variant text MUST NOT appear when Refuse fires."
- **Justification:** split_brain_reason branches on Refuse before any resolution formatting, so no resolution text can leak; the fallback-string hazard is tracked separately above and is unreachable today.
- **Code:** `src/v2/writer/build.rs:655-659 `let short = if matches!(info.confidence, Confidence::Refuse) { format_refuse(info) } else { format_resolution(info) };`; test at src/v2/writer/build.rs:772-796 asserts `short.starts_with("REFUSE/")` and `!short.contains("has quorum")`.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | implemented] ADR lines 157, 171
- **Claim:** Item 2 / table row: `LowerTimelineHasQuorum` -> `SplitBrain: {true} has quorum (lower TL={lo}), fence {stale} (TL={hi}, quorum-blocked)`
- **Justification:** Character-for-character identical to the ADR's Acceptable example modulo substitutions, and the substitutions are bound correctly: true_primary_timeline holds the LOWER timeline and stale_timeline the higher, so {lo}/{hi} are not swapped.
- **Code:** `src/v2/writer/build.rs:708-711 `"SplitBrain: {} has quorum (lower TL={}), fence {} (TL={}, quorum-blocked)"` with src/v2/analyze/split_brain.rs:457-460 `LowerTimelineHasQuorum { true_primary_timeline: stale_tl, stale_timeline: highest_tl, ... }` (stale_tl is `primaries_with_lower_timeline[0].1`). Test: src/v2/writer/build.rs:748-770 asserts `fence db002` and `lower TL=11`. The rejected phrasing 'replica overrides timeline' appears nowhere in the writer.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [low | diverges] ADR lines 168-174
- **Claim:** The template's `{stale}` is a single node (all five rows assume exactly one stale primary).
- **Justification:** The writer names only stale_primaries.first(); with three or more primaries the resolver populates stale_primaries with all of them, so the operator is told to demote one zombie and never learns about the others from the short string.
- **Code:** `src/v2/writer/build.rs:693 `let stale = info.stale_primaries.first().map_or("", String::as_str);` versus src/v2/analyze/split_brain.rs:467-471 which collects every lower-TL primary into stale_primaries.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Cluster sizes on the fleet: count nodes per cluster in the inventory DB. Only the single 3-node capture exists today.
- **Fix (writer):** Join the names (`info.stale_primaries.join(", ")`) or append `(+N more)` when len > 1. Note the TL interpolation only makes sense for the first entry, so the honest minimal fix is `demote {first} (TL={lo}, ...)` plus a trailing `; also stale: {rest}`.
- **Alternatives rejected:** Leaving it is defensible under the ADR's 1-primary-plus-2-replicas assumption (3+ primaries cannot occur in a 3-node cluster), which is why this is low rather than medium; resolve_split_brain itself imposes no node-count limit (src/v2/analyze.rs:317-318 calls it for any primaries.len() > 1).


## ADR-002 section 3 "Confidence states" (lines 109-126): Confidence enum, determine_confidence_level, min-based aggregation in determine_true_primary, and the ordering tests

### [high | diverges] ADR lines 113-118
- **Claim:** BestEffort means "single-pass scan; gate passed; verdict is internally consistent"
- **Justification:** BestEffort is the *default* for an empty findings list, so it is exactly what the no-evidence path produces -- the gate never passed, it never even ran.
- **Code:** `src/v2/analyze/split_brain.rs:384-389 `let confidence = split_brain_info.findings.iter().map(|f| determine_confidence_level(f, true_primary)).min().unwrap_or(Confidence::BestEffort);`; the silent drop-outs that produce zero findings are src/v2/analyze/split_brain.rs:272 `let Some(wr) = &r_health.wal_receiver else {` (no wal_receiver at all -> `continue`, no finding) and :286-296 (replica-side gate failed and `wr.sender_host` names no candidate primary -> `continue`, no finding); src/v2/analyze/split_brain.rs:650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else {` suppresses the only remaining finding source when SSN is empty/unparseable. Pinned by test src/v2/analyze/split_brain.rs:988-1010 `higher_timeline_wins_when_no_replica_evidence` asserting `findings: vec![]` + `confidence: Confidence::BestEffort` for a `HigherTimeline` verdict.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** `SHOW synchronous_standby_names;` on all three nodes of a cluster (the scan captures this GUC only for the node that is primary at scan time -- in tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json nodes[1]/[2] `configuration` holds only hot_standby, primary_conninfo, primary_slot_name, recovery_target_timeline). If the promoted node's SSN is empty or unparseable, the zero-findings BestEffort path is live on the fleet; if every node has `ANY 1 (...)` in postgresql.conf, the fleet lands on Conflicting instead (see the Conflicting-is-unconditional finding).
- **Fix (resolver):** Give determine_true_primary a confidence floor that is independent of the findings list. Pass `replicas_following` (or a precomputed `any_gate_passed: bool`) alongside `findings`, and compute `confidence = min(findings_min, floor)` where `floor = Conflicting` when no replica passed the bidirectional gate for any candidate primary and the resolution is HigherTimeline or Indeterminate, `BestEffort` otherwise. Alternatively, if the ADR prefers the label to stay BestEffort, amend line 115 to say "gate passed OR no gate input existed" -- but that guts the comment's meaning, so the code fix is preferable.
- **Alternatives rejected:** Emitting a synthetic `NoReplicaEvidence` finding and mapping it to Conflicting: rejected because it adds a v1 finding category the ADR does not list (section 4, lines 135-141) and would leak into the short-string contract and details JSON.

### [high | self-contradictory] ADR lines 7, 123-124
- **Claim:** Refuse is the state for "the tool declines to interpret the evidence"; the docs assert the tool refuses when the replica is unobservable -- docs/concepts/split-brain.md:85 "db003 unobservable (timeline-wedged, no wal_receiver): you cannot prove db002's fork is empty -> Refuse (decline to auto-resolve)", and ADR line 7 "a conservative Refuse-only floor is shippable today (SS7)"
- **Justification:** No code path can produce Refuse for the unobservable/wedged replica; ADR line 245 says the opposite of line 7 ("which is why no conservative 'Refuse-only floor' is shipped in the interim"), and the concepts doc states a behaviour the binary does not have.
- **Code:** `src/v2/analyze/split_brain.rs:397-399 -- only `SystemIdentifierMismatch`, `SynchronousCommitWeakened` and `DivergentReplicaWal` map to `Confidence::Refuse`, and `DivergentReplicaWal` is never constructed outside tests (`grep -rn DivergentReplicaWal src/` -> split_brain.rs:102 decl, :399 mapping, :782 unit-test case, writer/build.rs:682 `=> None`). In the wedged state the resolver takes src/v2/analyze/split_brain.rs:272 `let Some(wr) = &r_health.wal_receiver else { continue; }` and falls through to HigherTimeline, which src/v2/writer/build.rs:716 renders as `"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)"`.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** A capture of the wedged state (ADR SS7's control-file capture): `select * from pg_stat_wal_receiver;` (expected empty) plus `select timeline_id from pg_control_checkpoint(); select pg_last_wal_replay_lsn();` on the wedged replica, taken while the standby log shows "new timeline N+1 forked off current database system timeline N before current recovery point X/X".
- **Fix (adr-text):** Reconcile the three texts. Either (a) delete "a conservative Refuse-only floor is shippable today (SS7)" from ADR line 7 and change docs/concepts/split-brain.md:85 to "...-> should be Refuse; not implemented, detection deferred (ADR-002 SS7)", or (b) implement the floor in the resolver (same hook as the previous finding: no gate input anywhere + a replica present but exposing no wal_receiver -> Refuse). Do not leave (a) and (b) both un-done: today the concepts doc promises operators a refusal the tool never issues.
- **Alternatives rejected:** Leaving it to SS7's deferral note alone: the deferral is stated in SS7 but is directly contradicted by the ADR's own revision header and by the concepts doc, which is the document an on-call reader reaches for first.

### [high | diverges] ADR lines 116-117
- **Claim:** The Conflicting/Refuse split for PrimaryQuorumUnsatisfied rests on "A quorum-blocked primary cannot have ack'd writes ... When it's a stale primary, that's the proof behind the resolution -- benign"
- **Justification:** The code comment converts a present-tense observation (blocked at scan time) into a past-tense guarantee (never acked); ADR line 47 (C-g) states the exact counterexample -- "No, now (but it *did* ack TL=N writes past X earlier)".
- **Code:** `src/v2/analyze/split_brain.rs:400-409 `// A quorum-blocked primary cannot have ack'd writes. When it's a stale primary, that's the proof behind the resolution -- benign.` / `if primary == true_primary { Confidence::Conflicting } else { Confidence::BestEffort }`. `observed` comes from the gate map only: src/v2/analyze/split_brain.rs:659-664 `let observed = members.iter().filter(|m| gated.iter().any(|g| g == *m)).count() as u32;``
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** A captured C-g run (see previous finding). Short of that, the claim is only checkable as a logical one: PrimaryQuorumUnsatisfied is derived from the scanner's gated-follower set at one instant, so it can never certify what the primary acked before the scan.
- **Fix (combination):** Narrow the comment to what the finding supports ("cannot ack *new* writes at scan time; says nothing about writes acked before the scan") and stop treating the stale-primary case as positive proof: keep the BestEffort mapping only when that stale primary also has zero acked-write evidence, i.e. when some replica is observably following someone else on a different TL (the ADR's 3-node proof, concepts doc "The 3-node proof"). When no replica is observable at all, the stale primary's quorum-unsatisfied finding is not proof of anything and must not be the arm that keeps confidence at BestEffort. Record the rule in ADR section 3 alongside the enum.
- **Alternatives rejected:** Reverting to the pre-87e51e5 unconditional Refuse: that re-breaks LowerTimelineHasQuorum, which is what the commit correctly fixed. The problem is the *justification*, not the elected/stale split itself.

### [medium | diverges] ADR lines 125
- **Claim:** "SplitBrainResolution::Indeterminate (kept) means the evidence itself is inconclusive (e.g., equal timelines with no replica evidence). Confidence is then BestEffort."
- **Justification:** Indeterminate arises exactly when no replica passed the gate for any candidate primary, which forces observed=0 for every primary; with a parseable synchronous_standby_names on the elected primary the code then yields Conflicting, not BestEffort.
- **Code:** `src/v2/analyze/split_brain.rs:549-559 (equal-timeline fallthrough) `resolution: SplitBrainResolution::Indeterminate,` / `confidence: Confidence::BestEffort,` -- but that seed is overwritten at :384-391 by the min over findings, and src/v2/analyze/split_brain.rs:665 `if observed < count {` fires for the elected primary, which :404-406 maps to `Confidence::Conflicting`. The only Indeterminate test (src/v2/analyze/split_brain.rs:1121-1140 `equal_timelines_no_replica_evidence_is_indeterminate`) uses `PrimaryHealthBuilder` defaults, which set `synchronous_commit` but no `synchronous_standby_names` (src/v2.rs:61, :148), so it never exercises the SSN-present case.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None for the SSN-present half: tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json carries `"synchronous_standby_names": "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"`, which src/v2/analyze/sync_standby_names.rs parses (see its `parses_any_1_a_b` test with the same spacing).
- **Fix (adr-text):** Amend ADR line 125 to "Confidence is then BestEffort or Conflicting -- Indeterminate does not by itself imply Refuse", since Indeterminate co-occurs by construction with PrimaryQuorumUnsatisfied on the elected primary whenever sync rep is configured. Add a test `indeterminate_with_ssn_is_conflicting` next to equal_timelines_no_replica_evidence_is_indeterminate so the pairing is pinned either way. Worth noting separately (no fix proposed): the Indeterminate struct simultaneously sets `true_primary` to an arbitrary first primary and says the verdict cannot be determined, so "verdict is internally consistent" is thin here too.
- **Alternatives rejected:** Suppressing PrimaryQuorumUnsatisfied when the resolution is Indeterminate: rejected, the finding is genuinely informative there and section 4 line 148 mandates emission on `observed < count` unconditionally.

### [medium | diverges] ADR lines 116
- **Claim:** Conflicting means "signals partially contradict (verdict still chosen)"
- **Justification:** HigherTimeline fires precisely when no primary has a gated follower, so the elected primary is always quorum-unsatisfied there; with SSN configured, Conflicting becomes the unconditional outcome for the ADR's own routine post-failover states (C-d/C-f) and stops signalling contradiction.
- **Code:** `src/v2/analyze/split_brain.rs:487-501 -- the HigherTimeline arm is the `else` reached only when both `replicas_following_highest` and `replicas_following_stale` are empty; then src/v2/analyze/split_brain.rs:659-666 gives `observed = 0` for the elected primary and :404-406 maps it to `Confidence::Conflicting`. Same collapse for LowerTimelineHasQuorum on real fleet names, because `members` are application-name form (`dev_pg_app001_db003`) while `gated` holds node names (`dev-pg-app001-db003.sto3.example.com`), so `observed` is 0 for every primary.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Already evidenced by tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json: SSN members are underscore/application-name form and node_name is the hyphenated FQDN, so `members intersect gated` is empty regardless of actual replication health -> PrimaryQuorumUnsatisfied on every primary including the elected one -> Conflicting on every non-Refuse split-brain verdict.
- **Fix (combination):** Two coupled fixes: (1) make the quorum derivation name-aware (normalize application-name vs node-name forms in the resolver, as writer/build.rs already does via normalize_application_name/extract_db_number) so `observed` reflects reality; (2) once (1) lands, decide explicitly whether quorum-unsatisfied on the elected primary in a HigherTimeline verdict is a contradiction or the expected post-failover state -- ADR line 53 already calls it informational ("the failover hasn't completed: the new primary has no replicas yet"), which argues for BestEffort there and Conflicting only when the elected primary's quorum is unsatisfied *despite* a replica claiming to follow it. Note that the same rule currently gives benign C-f and data-destroying C-g identical confidence.
- **Alternatives rejected:** Only fixing the confidence rule and leaving the name comparison alone: the field would still be constant on fleet data, just constant at a different value.

### [medium | unverifiable] ADR lines 109-126
- **Claim:** Section 3 enumerates the three confidence states and their triggers (gate passed / signals partially contradict / sanity gate failed)
- **Justification:** The load-bearing "judge the finding by who it names" rule for PrimaryQuorumUnsatisfied appears nowhere in the ADR (section 3 says nothing about per-finding mapping; section 4 line 141 defers to a derivation rule that covers emission only), so its correctness cannot be checked against the spec.
- **Code:** `src/v2/analyze/split_brain.rs:404-409 `SplitBrainFinding::PrimaryQuorumUnsatisfied { primary, .. } => { if primary == true_primary { Confidence::Conflicting } else { Confidence::BestEffort } }`; the rationale exists only in the code comment at :400-403 and in commit 87e51e5's message ("Judge the finding by who it names").`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Add a finding-to-confidence table to ADR section 3 (one row per v1 finding category from section 4 lines 135-141), stating explicitly: SystemIdentifierMismatch/SynchronousCommitWeakened -> Refuse; PrimaryQuorumUnsatisfied -> Conflicting if it names the elected true primary, BestEffort otherwise, with the reasoning from 87e51e5 and the caveat that it only speaks to writes ack-able *now*; ReplicaWalReceiverStale/PrimaryDoesNotSeeReplica -> Conflicting; BidirectionalFlushingConfirmed/ReplicaInCatchup -> BestEffort; DivergentReplicaWal -> Refuse (dormant). Also state that the aggregate is the minimum over findings.
- **Alternatives rejected:** Leaving the rule in the code comment: it is the single most surprising piece of confidence logic and it is invisible to anyone reading the ADR to decide whether a verdict is actionable.

### [medium | diverges] ADR lines 116
- **Claim:** Conflicting = "signals partially contradict", while section 4 line 140 classifies ReplicaInCatchup as "informational, gate passed"
- **Justification:** ReplicaInCatchup is only ever emitted together with BidirectionalFlushingConfirmed for the same link (i.e. both sides of the gate passed), yet it drags the verdict to Conflicting; nothing contradicts anything.
- **Code:** `src/v2/analyze/split_brain.rs:411-413 `| SplitBrainFinding::ReplicaInCatchup(_) => Confidence::Conflicting,`; emission site src/v2/analyze/split_brain.rs:317-330 -- the finding is pushed inside the `Some(row)` arm right after `BidirectionalFlushingConfirmed`, `if matches!(row.state, ReplicationState::Catchup)`. ADR line 68 also insists `catchup` "is genuinely-following mid-recovery and must not be rejected".`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `select application_name, state from pg_stat_replication;` on the new primary during the window after a replica is re-pointed (state='catchup' before it reaches 'streaming'). The captured healthy cluster shows only streaming, so the catchup path is unexercised by real data.
- **Fix (resolver):** Move `SplitBrainFinding::ReplicaInCatchup(_)` from the Conflicting arm (split_brain.rs:411-413) into the BestEffort arm (:414) and update the rstest case `replica_in_catchup` (split_brain.rs:802-806) to expect BestEffort. If instead the intent is that mid-catchup evidence is weaker, say so in ADR section 4 line 140 and drop the words "informational, gate passed".
- **Alternatives rejected:** Changing the ADR to call catchup a contradiction: it conflicts with ADR line 68, which deliberately admits catchup through the gate, and post-failover reattach (catchup) is the normal state the tool is run in.

### [low | unverifiable] ADR lines 109-126
- **Claim:** Section 3 defines the three states but never says how a verdict with several findings is scored
- **Justification:** The ADR specifies no aggregation; the code picks worst-finding-wins via `min()` over a derived `Ord`, which is a defensible reading but not a checkable one.
- **Code:** `src/v2/analyze/split_brain.rs:384-389 `.map(|f| determine_confidence_level(f, true_primary)).min().unwrap_or(Confidence::BestEffort);`, resting on the derive at :71 `#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Serialize)]` and pinned by the tests at :746-756 (`severity_rank`) and :821-836 (`confidence_ordering_matches_severity_rank`).`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** State in section 3: "Confidence is the minimum over per-finding confidences (worst finding wins); an empty findings list yields BestEffort", and note that this makes per-finding mapping the only lever -- context-dependent severity (as with PrimaryQuorumUnsatisfied, and as C-c will need for DivergentReplicaWal) must be encoded inside the per-finding function, since the aggregator cannot see the finding set.
- **Alternatives rejected:** Documenting it only in the test doc comment (split_brain.rs:746-748), where it currently lives: that comment is about not breaking the ordering, not about the policy.

### [low | self-contradictory] ADR lines 113-118
- **Claim:** C-c: "DivergentReplicaWal(db003, ...) is informational, not Refuse ... the verdict is confident" (line 43), while the enum's Refuse is "sanity gate failed"
- **Justification:** DivergentReplicaWal maps unconditionally to Refuse and confidence is a min, so the moment detection lands as originally specified, C-c yields Refuse -- the opposite of what the case matrix promises; SS7 only reconciles this by keeping the finding dormant.
- **Code:** `src/v2/analyze/split_brain.rs:397-399 `| SplitBrainFinding::DivergentReplicaWal { .. } => Confidence::Refuse,` combined with the `min()` at :388. ADR line 247 acknowledges the mapping and defers: "The DivergentReplicaWal variant already maps to Confidence::Refuse in determine_confidence_level, so nothing emits it today".`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Add one sentence to section 3 (or to the C-c row): when DivergentReplicaWal detection returns, its confidence must be context-dependent in the same shape as PrimaryQuorumUnsatisfied -- Refuse only when the replica's allegiance is unprovable (C-g), BestEffort when a replica is observably acking the lower-TL primary (C-c, where the 3-node proof holds). As written, the unconditional Refuse arm plus min() cannot express C-c.
- **Alternatives rejected:** Doing nothing because the finding is dormant: the mapping is the first thing a future implementer will wire detection into, and it currently encodes the behaviour C-c forbids.

### [low | diverges] ADR lines 113-118
- **Claim:** The enum is declared `BestEffort, Conflicting, Refuse` (in that order) in the ADR snippet
- **Justification:** The code declares the reverse order (Refuse, Conflicting, BestEffort) because Ord is derived and the aggregator is min(); with the ADR's literal order, min() would return the *most* confident value.
- **Code:** `src/v2/analyze/split_brain.rs:71-77 `#[derive(... Ord, PartialOrd ...)] pub enum Confidence { Refuse, Conflicting, BestEffort, }`; guarded by src/v2/analyze/split_brain.rs:821-836 `confidence_ordering_matches_severity_rank`.`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Reverse the variants in the ADR snippet and add "variant order is load-bearing: Ord is derived and confidence is aggregated with min(), so the least-confident variant must be declared first". The code is right; only the ADR snippet is misleading.
- **Alternatives rejected:** Reordering the code to match the ADR and switching to max()/an explicit rank function: needless churn, and the existing test already prevents a silent reorder.

### [low | diverges] ADR lines 117, 123-124
- **Claim:** Refuse means "sanity gate failed; verdict not actionable"
- **Justification:** Refuse is also produced by DivergentReplicaWal, which section 2 does not list as a sanity gate; the writer's fallback then prints the literal "sanity gate failed" for a cause that is not one.
- **Code:** `src/v2/analyze/split_brain.rs:397-399 (three variants -> Refuse) vs ADR section 2 lines 95-96 (two sanity gates: system_identifier, synchronous_commit); src/v2/writer/build.rs:676-684 maps DivergentReplicaWal to `None` and falls back to `.unwrap_or_else(|| "sanity gate failed".to_owned());``
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Reword the Refuse comment to "safety model broken (sanity gate failed, or committed-write divergence detected); verdict not actionable" -- which is exactly how commit 87e51e5 describes the post-fix meaning ("Refuse now only means what it should: the safety model is broken"). The writer's "sanity gate failed" fallback becomes reachable only when DivergentReplicaWal detection lands, and is section 4's problem.
- **Alternatives rejected:** Removing DivergentReplicaWal from the Refuse arm: SS7 line 247 deliberately keeps that wiring dormant-but-present.

### [info | implemented] ADR lines 113-118
- **Claim:** BestEffort is the top of the lattice and positive corroboration keeps a verdict there
- **Justification:** BidirectionalFlushingConfirmed -> BestEffort is inert under min() (it can only fail to lower, never raise), which is consistent with the ADR's ceiling since Verified is deferred -- but it means a fully corroborated verdict and a verdict with no evidence at all carry the identical label.
- **Code:** `src/v2/analyze/split_brain.rs:414 `SplitBrainFinding::BidirectionalFlushingConfirmed(_) => Confidence::BestEffort,` with the aggregation at :388 `.min()`; the no-evidence twin is :389 `.unwrap_or(Confidence::BestEffort)`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No change to the arm itself (removing it would be a behaviour-preserving refactor but the explicit arm documents intent and is mutation-tested). The information gap it exposes -- corroborated vs evidence-free are indistinguishable -- is addressed by the confidence-floor fix in the first finding, not here.
- **Alternatives rejected:** Adding a fourth 'Corroborated' state between Conflicting and BestEffort: that is Verified by another name and is explicitly deferred (ADR line 121, SS6 line 229).

### [info | deferred-correct] ADR lines 121
- **Claim:** "Verified is deliberately omitted from v1. Promoting BestEffort to Verified requires a two-pass stability check (deferred -- SS6)."
- **Justification:** The enum has exactly three variants, and every other mention of Verified is consistently future-tense.
- **Code:** `src/v2/analyze/split_brain.rs:73-77 (three variants, no Verified); no occurrence of `Verified` anywhere in src/ (`grep -rn Verified src/` returns nothing). Consistent with ADR line 229 (SS6 future enhancement), line 263 (Consequences), and docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:234 ("Two-pass stability check (`Verified` confidence) -- ADR-002 SS6").`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | implemented] ADR lines 123-124
- **Claim:** "The resolution field still carries the timeline-based pick for completeness, but downstream consumers should treat it as not-actionable"
- **Justification:** Confidence is computed after the resolution is built and never mutates it, and the one downstream consumer suppresses the resolution text under Refuse.
- **Code:** `src/v2/analyze/split_brain.rs:361-392 -- the resolution is built first, then `split_brain_info.confidence = confidence;` at :391 with no touch to `resolution`; src/v2/writer/build.rs:655-658 `let short = if matches!(info.confidence, Confidence::Refuse) { format_refuse(info) } else { format_resolution(info) };`. Note as an aside: this is the *only* consumer of the field anywhere in src/ (`grep -rn '\.confidence' src/`), so Conflicting and BestEffort are indistinguishable in operator-facing text and differ only inside the serialized details JSON.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 


## ADR-002 section 2, "Sanity gates to Refuse" (lines 91-108), plus the docs/concepts/split-brain.md statements that section 2 is load-bearing for

### [critical | diverges] ADR lines 96
- **Claim:** "**`synchronous_commit` durability** on every candidate primary. The `ANY 1 (A, B)` no-divergence claim depends on the standby actually fsyncing before ack. Refuse if any primary has `synchronous_commit` in {local, off, remote_write, empty}." -- i.e. the whole quorum-sync safety argument is gated on ONE GUC's value.
- **Justification:** The gate tests the consequent (synchronous_commit is durable) and never the antecedent (synchronous_standby_names names at least one standby). With SSN empty, `synchronous_commit=on` waits only for LOCAL fsync -- SyncStandbysDefined() is false so SyncRepWaitForLSN returns immediately -- so an isolated primary acks client commits, which is exactly the state the ADR asserts is structurally impossible. Nothing in the code notices.
- **Code:** `src/v2/analyze/split_brain.rs:13 `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` and :169 `if WEAKENED_SYNCHRONOUS_COMMIT.contains(&v) {` -- the loop at :160-175 reads only `configuration.get("synchronous_commit")`, never `synchronous_standby_names`. The one place SSN is read in this module bails on empty: split_brain.rs:650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else {` + :651 `continue;` (sync_standby_names.rs:26-28 returns None for ""). The single-primary path that WOULD catch it is bypassed: analyze.rs:317 `if primaries.len() > 1 {` returns at :320 before analyze.rs:334 `check_sync_commit(primary, &mut verdict);`, and checks.rs:365 `fn is_standby_names_empty(...)` is only reachable from that bypassed path.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Fleet-wide: `SELECT name, setting, source, sourcefile FROM pg_settings WHERE name IN ('synchronous_standby_names','synchronous_commit');` on every node, flagging any node where synchronous_standby_names is ''. Also locate the dump referenced by src/v2/analyze.rs:1154 ("Mirrors the real dump: synchronous_commit=on, synchronous_standby_names=\"\"") and land it under tests/fixtures/ -- that comment is the only in-repo evidence that the empty-SSN config exists on this fleet, and it is second-hand.
- **Fix (combination):** Make the section-2 gate a conjunction. In resolve_split_brain, in the same loop as the synchronous_commit check (split_brain.rs:160-175), also read `configuration["synchronous_standby_names"]` per candidate primary and treat empty/whitespace (equivalently `sync_standby_names::parse(..) == None`) as a Refuse-class finding -- add `SplitBrainFinding::SyncStandbyNamesEmpty { primary }`, map it to Confidence::Refuse in determine_confidence_level (alongside :397-399), and add one arm to writer/build.rs format_refuse (:670-683) so the short string reads e.g. "no sync standbys configured on <node>: commits ack locally" instead of falling through to the literal "sanity gate failed". The predicate already exists as checks.rs:365 is_standby_names_empty; this is the cheap absolute check, not the SSN-drift normalization the ADR defers at line 258. Then reword ADR line 96 to state the gate as durable synchronous_commit AND non-empty synchronous_standby_names, and carve the empty case out of line 107.
- **Alternatives rejected:** (a) Handling it inside emit_quorum_findings by synthesizing a PrimaryQuorumUnsatisfied when parse() returns None: wrong severity channel -- determine_confidence_level (:404-410) maps that finding to Conflicting (elected primary) or BestEffort (stale primary), so the demote instruction would still render; and it overloads a finding whose contract is "SSN lists standbys, too few are live". (b) Writer-only fix: confidence stays BestEffort and the serialized details still describe a safe cluster. (c) Dropping the analyze.rs:317 early return so check_sync_commit runs for split-brain clusters: much larger blast radius (changes the cluster verdict shape for every split brain) and the SplitBrain reason string would still say BestEffort/demote.

### [high | diverges] ADR lines 106-107
- **Claim:** "Not Refuse-worthy: `synchronous_standby_names` inconsistency across primaries -- detection deferred (see Out of scope). Not Refuse-worthy regardless: each primary evaluates its own SSN locally, so divergence cannot break the per-primary quorum reasoning the resolver depends on."
- **Justification:** The reasoning is sound for POLICY divergence (ANY 1 (db002,db003) vs ANY 1 (db001,db003)) and unsound for the degenerate member of the same set: SSN='' on one primary. "Each primary evaluates its own SSN locally" is true and is precisely why an empty SSN is fatal -- that primary locally evaluates "no sync standbys required" and acks alone. The deferral rationale at line 258 ("requires normalization, because ... each primary's SSN structurally excludes itself") also does not cover the empty case: is-empty needs no normalization and is already implemented at checks.rs:365. So the ADR's own justification for not gating on SSN does not extend to the one SSN state that breaks the safety model.
- **Code:** `src/v2/analyze/split_brain.rs:650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else {` -- empty and unparseable are collapsed into the same silent `continue`; ADR line 145 blesses that for unparseable, nobody blessed it for empty. Contrast src/v2/analyze/checks.rs:365 `fn is_standby_names_empty(health: &PrimaryHealthCheckResult) -> bool {`, which exists because the single-primary path treats empty SSN as Critical SyncCommitOff (analyze.rs:1152-1158).`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Same fleet query as the previous finding; additionally `SELECT setting FROM pg_settings WHERE name='synchronous_standby_names'` captured on a node during/after a repmgr promotion, to establish whether promotion ever leaves SSN empty on the newly promoted node.
- **Fix (adr-text):** Split the bullet in two. Keep "SSN policy divergence (method/count/member set) is not Refuse-worthy; detection deferred, see Out of scope" with the existing local-evaluation argument. Add "SSN EMPTY on any candidate primary IS Refuse-worthy: an empty SSN makes synchronous_commit=on mean local flush only, so that primary acks alone and the ANY 1 (A,B) no-divergence premise of this section does not hold for it." Line 258's out-of-scope entry should say explicitly that it defers only the policy comparison, not the emptiness check.
- **Alternatives rejected:** Leaving the bullet and relying on the reader to notice the gap: the bullet is the stated licence for the resolver not checking SSN at all, and the implementation took that licence literally (split_brain.rs:650). Deleting the bullet entirely would lose the correct policy-divergence argument, which is worth keeping.

### [high | self-contradictory] ADR lines 96 (asserted in docs/concepts/split-brain.md:44; same overclaim at ADR 235)
- **Claim:** "(If `synchronous_commit` is weakened, this inference breaks -- which is exactly why weakened `synchronous_commit` is its own hard `Refuse` gate, ADR-002 section 2. Past that gate, the inference holds.)" -- concepts doc, restating what section 2 guarantees.
- **Justification:** docs/concepts/split-brain.md:15 states the invariant WITH its precondition ("Under `ANY 1 (A, B)` with `synchronous_commit = on` ..."); line 44 then drops the precondition and asserts that passing the section-2 gate alone restores the flushed-past-fork -> acknowledged-writes inference. Passing the section-2 gate is necessary but not sufficient: with SSN empty the gate passes and the inference is false. The two statements in the same document disagree about what the gate buys.
- **Code:** `src/v2/analyze/split_brain.rs:160-175 (the gate is value-only) and :13; no code reads synchronous_standby_names for gate purposes anywhere in the module.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Change docs/concepts/split-brain.md:44 to "Past that gate, the inference holds only if the primary actually has sync standbys configured -- an empty synchronous_standby_names makes synchronous_commit=on a local-flush-only ack. That is a second precondition of the same gate (ADR-002 section 2)." Apply the same qualifier to the parenthetical in ADR line 235, which repeats the claim for the DivergentReplicaWal design.
- **Alternatives rejected:** Fixing only the ADR line 235 parenthetical: the concepts doc is the one the ADR points readers at for the domain model (ADR line 12), so the unqualified version would remain the canonical statement of the invariant.

### [medium | diverges] ADR lines 95
- **Claim:** "A replica with a foreign `system_identifier` indicates a reseed/restore from an unrelated cluster; this is escalation-worthy **regardless of split-brain**."
- **Justification:** The sysid comparison exists only inside resolve_split_brain, which analyze() calls only when `primaries.len() > 1`. A repo-wide grep shows system_identifier is consumed in exactly one place outside the scanners and test builders (split_brain.rs:569/595/596). So in the overwhelmingly common 1-primary cluster a foreign-sysid replica is collected, stored, serialized -- and never compared to anything.
- **Code:** `src/v2/analyze.rs:317 `if primaries.len() > 1 {` (the only route to the check; returns at :320); src/v2/analyze/split_brain.rs:583 `fn mismatched_sysid_nodes(` is private to the module and called only from :144. No sysid check in src/v2/analyze/checks.rs.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `SELECT system_identifier::text FROM pg_control_system();` on every node of every cluster, grouped by cluster_id -- one row per cluster with count(distinct) > 1 would show the state is real. The one captured cluster (tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json lines 16/100/145) has identical sysids on all three nodes.
- **Fix (combination):** Either (a) add a per-cluster sysid consistency check to the single-primary path in analyze() (next to check_sync_commit at analyze.rs:334) emitting a NodeVerdict, reusing the reference = primary's sysid; or (b) narrow ADR line 95 to "escalation-worthy, but only evaluated inside the split-brain path in v1" and record the 1-primary case as a known gap in Out of scope. (a) is small -- one comparison against primaries[0]'s sysid -- but it adds a verdict variant, so it is a scope decision, not a bug fix; (b) costs nothing and keeps the ADR honest.
- **Alternatives rejected:** Moving mismatched_sysid_nodes wholesale into analyze() so it runs for every cluster: it currently returns Vec<NodeName> shaped for SplitBrainFinding and derives its reference from >=2 primaries, which is meaningless with one primary; the single-primary check needs a different reference rule (the primary is the reference), so sharing the function would force a parameterisation nobody needs.

### [low | implemented] ADR lines 96
- **Claim:** "Refuse if any primary has `synchronous_commit` in {local, off, remote_write, **empty**}" -- absent/empty treated as weakened.
- **Justification:** `map_or("", String::as_str)` on a missing key plus "" in the denylist makes absent fail-closed, which is the right direction for a safety gate. But the resulting operator string is the problem: the value is interpolated raw, so an empty value renders as "synchronous_commit= on db001", which an operator reads as "synchronous_commit = on" -- the SAFE value -- while the tool is refusing precisely because it does not know the value.
- **Code:** `src/v2/analyze/split_brain.rs:164-167 `let v = h.configuration.get("synchronous_commit").map_or("", String::as_str);`; src/v2/writer/build.rs:675 `Some(format!("synchronous_commit={} on {}", value, primary))` -> "REFUSE/SplitBrain: synchronous_commit= on dev-pg-app001-db001...".`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None needed to confirm the string; to confirm unreachability on the fleet: `SELECT count(*) FROM pg_settings WHERE name='synchronous_commit';` returns 1 on every node, and HEALTH_CHECK_PRIMARY_QUERY (health_check_primary.rs:163-176) selects it unconditionally with no serde default on `configuration`, so an absent key is a deserialization error rather than an empty string.
- **Fix (writer):** In writer/build.rs:674-676, special-case the empty value: `if value.is_empty() { format!("synchronous_commit not reported by {}", primary) } else { format!("synchronous_commit={} on {}", value, primary) }`. Leave the resolver's fail-closed behaviour alone.
- **Alternatives rejected:** Making the resolver store a sentinel like "<missing>" in the finding: that leaks presentation into the analysis type and changes the serialized details JSON, which downstream consumers read.

### [low | diverges] ADR lines 96
- **Claim:** "Valid values: `on`, `remote_apply`, `remote_flush`."
- **Justification:** PostgreSQL has no user-settable synchronous_commit value called remote_flush. The accepted enum is off, local, remote_write, on, remote_apply (plus the hidden boolean aliases true/false/yes/no/1/0). `remote_flush` is the internal C enum constant SYNCHRONOUS_COMMIT_REMOTE_FLUSH, which is the value `on` maps to -- so the ADR lists the same setting twice under two names, one of which postgres would reject with "invalid value for parameter". No code effect: the constant is a denylist, not an allowlist, so a value that cannot be set cannot be missed.
- **Code:** `src/v2/analyze/split_brain.rs:13 `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` -- the implementation correctly encodes only the deny side and never references remote_flush; grep for "remote_flush" over src/ returns nothing.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `SELECT enumvals FROM pg_settings WHERE name='synchronous_commit';` on any fleet node settles the value set in one row. `SET synchronous_commit='false'; SHOW synchronous_commit;` settles the alias-normalisation question (expected: "off").
- **Fix (adr-text):** ADR line 96: change "Valid values: `on`, `remote_apply`, `remote_flush`." to "Values that satisfy the gate: `on` (= remote flush) and `remote_apply`. The full postgres enum is off, local, remote_write, on, remote_apply; boolean aliases (true/false/yes/no/1/0) are normalised by pg_settings to on/off, so the denylist sees canonical names."
- **Alternatives rejected:** Adding "remote_flush" to WEAKENED_SYNCHRONOUS_COMMIT or to an allowlist in code: it would be dead code, since postgres cannot produce that string in pg_settings.setting.

### [low | diverges] ADR lines 95
- **Claim:** "...consistency across **ALL** nodes (primaries and replicas)." -- with respect to nodes whose role is Unknown / UnknownPrimary / UnknownReplica.
- **Justification:** Two separate things. (1) Nodes whose health check failed carry no sysid, so they are silently outside "ALL nodes" -- the ADR says nothing about them. Benign for the verdict (an Unknown node also contributes no replication evidence: build_replica_following_map skips non-Role::Replica at :269-271), but it means a Refuse can be issued on an incomplete comparison without saying so. (2) The Unknown arms in mismatched_sysid_nodes are unreachable at the production call site: Cluster::primaries()/replicas() filter on `matches!(self, Role::Primary{..})` / `Role::Replica{..}`, so the slices passed in at split_brain.rs:144 can only contain those two variants.
- **Code:** `src/v2/analyze/split_brain.rs:597 `Role::Unknown | Role::UnknownPrimary | Role::UnknownReplica => None,`; src/v2/cluster.rs:70 `self.nodes.iter().filter(|n| n.role.is_primary())` and :75 `.filter(|n| n.role.is_replica())` with scan.rs:333-339 defining those as the exact-variant matches.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None for the code claim (readable). For the fleet claim: count scans where a cluster reached resolve_split_brain with a third node in Role::Unknown* -- i.e. how often the sysid comparison runs on 2 of 3 nodes.
- **Fix (adr-text):** ADR line 95: add "Nodes whose health check failed (Role::Unknown*) expose no system_identifier and are excluded from the comparison; the gate is therefore over all REACHABLE nodes. A cluster with an unreachable node cannot be proven sysid-consistent." No code change -- the filter_map arm is the correct behaviour, it is just undocumented.
- **Alternatives rejected:** Deleting the Unknown arms as dead code: they are exhaustiveness arms on Role and keep the function total if the call site ever widens; removing them buys nothing and violates the project's do-not-clean-adjacent-code rule.

### [info | implemented] ADR lines 98
- **Claim:** "replicas with a `system_identifier` not matching the cluster's reference sysid are excluded from section 1 gate input ... Exclusion happens **before** `build_replica_following_map`, so excluded replicas never appear as followers."
- **Justification:** Exclusion is a filter on the replica slice built at :146-150 and the filtered slice is what is passed to build_replica_following_map at :177-178; the unfiltered `replicas` is never passed to it. Excluded replicas therefore produce no map entry and also no ReplicaWalReceiverStale / PrimaryDoesNotSeeReplica findings, which matches "their replication evidence is treated as not endorsing any candidate".
- **Code:** `src/v2/analyze/split_brain.rs:146-150 `let filtered_replicas = replicas.iter().filter(|r| !mismatched_nodes.contains(&r.node_name)).copied().collect::<Vec<_>>();` then :177-178 `let (replicas_following, following_findings) = build_replica_following_map(&timeline_info, filtered_replicas.as_slice());``
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | implemented] ADR lines 100
- **Claim:** "If candidate primaries themselves disagree ... **all replicas are excluded** and the resolution falls back to timeline-only with `Confidence::Refuse`." -- both halves.
- **Justification:** Half one: reference_sysid returns None (no sysid reaches count >= 2), so mismatched_sysid_nodes's closure `reference == Some(sid)` is false for every node and returns all primaries + all replicas; the filter at :148 then empties the replica slice. Half two: the SystemIdentifierMismatch finding maps to Confidence::Refuse and the fold takes the minimum, so Refuse wins; with an empty follower map determine_true_primary falls to the no-replica-evidence branch (HigherTimeline, or Indeterminate when timelines tie).
- **Code:** `src/v2/analyze/split_brain.rs:588 `let matches = |sid: &str| reference == Some(sid);`; :397-399 `SystemIdentifierMismatch { .. } | ... => Confidence::Refuse`; :384-389 `.map(|f| determine_confidence_level(f, true_primary)).min().unwrap_or(Confidence::BestEffort)` with Confidence declared Refuse-first at :73-77 so min() == Refuse; :484-502 is the timeline-only branch.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | implemented] ADR lines 100
- **Claim:** "The **reference sysid** is the sysid agreed by >= 2 candidate primaries (i.e. the majority class in the 1+2 topology, where 2 of 2 primaries must agree)."
- **Justification:** For every input the pipeline can produce, ">= 2 wins" and "majority class" are the same rule: a Cluster is only emitted at exactly 3 nodes, so primaries is 2 or 3. 2 primaries: agree -> count 2 -> reference; disagree -> both count 1 -> None. 3 primaries: 3-0 and 2-1 both yield the majority; 1-1-1 yields None. They diverge only at >= 4 primaries (2-2, or 2-1-1 where 2 is not a majority), which the pipeline cannot construct -- and there the tie-break is nondeterministic, since max_by_key returns the LAST maximum and HashMap iteration order is randomised, so the reference sysid and hence the SystemIdentifierMismatch node list would vary run to run on identical input.
- **Code:** `src/v2/analyze/split_brain.rs:574-578 `counts.into_iter().filter(|(_, count)| *count >= 2).max_by_key(|(_, count)| *count).map(|(sid, _)| sid.to_owned())`; bounded by src/v2/cluster.rs:31 `if nodes[&cluster_id].len() == 3 {` -- clusters are dispatched only at exactly three nodes.`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** Optional and not recommended now: if the 3-node assumption is ever relaxed, change the filter to `*count * 2 > primaries.len()` so the rule is a true majority and ties resolve to None (exclude all replicas) rather than to an arbitrary class. Until then the current code is correct for every reachable input and changing it would add untestable paths.
- **Alternatives rejected:** Adding a BTreeMap or sort to make the >= 4 tie deterministic: it would make a wrong answer (picking one of two tied classes as "the majority") stable rather than correct, and the case is unreachable.

### [info | implemented] ADR lines 102
- **Claim:** "The `SystemIdentifierMismatch { nodes }` finding's `nodes` payload names every node whose sysid differs from the reference (or, when there's no reference, every primary involved in the disagreement plus every replica)."
- **Justification:** Both branches fall out of the same closure: with a reference, only nodes whose sysid differs are collected; with reference None, `reference == Some(sid)` is false for all, so every primary and every replica with a readable sysid is named. Confirmed by tests at :868-886 (flags only the divergent node) and :888-902 (None reference flags all).
- **Code:** `src/v2/analyze/split_brain.rs:588-605, esp. :599-603 `if matches(sid) { None } else { Some(n.node_name.clone()) }`; rendered at src/v2/writer/build.rs:672 `Some(format!("system_identifier mismatch ({})", nodes.join(", ")))`.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | implemented] ADR lines 95-96
- **Claim:** "Mismatch -> `Confidence::Refuse` with `SystemIdentifierMismatch(nodes)`" and "the gate applies on every candidate primary" for synchronous_commit.
- **Justification:** Both gates emit before anything else so they sit first in the findings vec (ADR line 129's ordering requirement), both map to Refuse, and the confidence fold takes the minimum so a single gate failure forces Refuse regardless of corroborating findings. The synchronous_commit loop iterates the whole primaries slice with no filtering beyond as_primary(), which cannot skip anything at the call site since Cluster::primaries() yields only Role::Primary.
- **Code:** `src/v2/analyze/split_brain.rs:152-158 (sysid finding pushed first), :160 `for p in primaries {`, :169-174 (one finding per weakened primary), :397-399 (both -> Refuse), :384-391 (min fold). Tests: :758-768 and :921-950.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | implemented] ADR lines 104
- **Claim:** "Exclusion and Refuse are orthogonal: exclusion prevents a foreign-cluster replica from acting as the deciding vote in the default timeline-based pick; Refuse tells the operator not to act on that pick regardless."
- **Justification:** The two effects are computed from the same mismatched_nodes list but applied independently -- the filter at :148 changes the pick, the finding at :155 changes the confidence -- and the writer honours the second by suppressing the resolution text entirely under Refuse, so no demote/fence instruction reaches the operator on a sysid failure.
- **Code:** `src/v2/analyze/split_brain.rs:146-158; src/v2/writer/build.rs:655-659 `let short = if matches!(info.confidence, Confidence::Refuse) { format_refuse(info) } else { format_resolution(info) };``
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | implemented] ADR lines 98
- **Claim:** "...and they do not contribute to `observed` in the `PrimaryQuorumUnsatisfied` derivation (section 4)."
- **Justification:** Literally true and structurally guaranteed: observed is computed by intersecting SSN members with the follower map, and an excluded replica cannot be in that map. But the claim is close to vacuous on real data for a reason section 2 does not own: SSN members are application_name form (fixture: dev_pg_app001_db002) while the map keys and values are node_name form (dev-pg-app001-db002.sto2.example.com), so the intersection is empty for every replica, excluded or not, and observed is always 0.
- **Code:** `src/v2/analyze/split_brain.rs:660-663 `let observed = members.iter().filter(|m| gated.iter().any(|g| g == *m)).count() as u32;` against tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json:23 `"synchronous_standby_names": "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"` and :93 `"node_name": "dev-pg-app001-db002.sto2.example.com"`. The upstream raw compare is split_brain.rs:307 `&& conn.application_name == replica.node_name`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** None -- the captured fixture already shows both forms side by side. A useful addition: one capture of pg_stat_replication.application_name and pg_settings.synchronous_standby_names from a node that has been through a repmgr promotion, to confirm the forms do not converge post-failover.
- **Fix (resolver):** Owned by the section 1 / section 4 audit, not this one -- recorded here only because it makes this section-2 guarantee untestable in practice. The fix is to normalise both sides (writer/build.rs already has normalize_application_name/extract_db_number) before the compares at split_brain.rs:307 and :662.
- **Alternatives rejected:** Fixing only the SSN-member compare at :662 without :307: the map would still be empty because the primary-side gate rejects every replica first, so observed would remain 0.


## ADR-002 lines 9-30: Context, "Operational definition of true primary", Cluster assumptions (plus the line-58 transient-window claim, which is the same premise)

### [critical | diverges] ADR lines 27 (and 73)
- **Claim:** "repmgr-set `application_name` equals the node name" (assumption bullet), operationalised in ADR §1 as "the primary's `pg_stat_replication` has a row whose `application_name` equals the replica's node name" (line 73)
- **Justification:** The captured fleet data falsifies the assumption outright: application_name is `dev_pg_app001_db002`, node_name is `dev-pg-app001-db002.sto2.example.com`. The resolver does a raw `==`, so the primary-side gate can never pass on real data, no replica ever enters `replicas_following`, and every split brain falls through to `HigherTimeline`.
- **Code:** `src/v2/analyze/split_brain.rs:307 `                    && conn.application_name == replica.node_name` (inside the primary-side gate at :305-315). Fixture: tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json node 1437 `"node_name": "dev-pg-app001-db002.sto2.example.com"` vs primary row `"application_name": "dev_pg_app001_db002"`. Consequence path: src/v2/analyze/split_brain.rs:484-502 (`HigherTimeline`, stale_primaries = lower-TL nodes) then src/v2/writer/build.rs:712-718 `"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)"`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** None for the naming mismatch itself -- tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json already carries both forms in one capture (node_name, pg_stat_replication.application_name, and wal_receiver.conninfo `application_name=dev_pg_app001_db002`). To confirm it is fleet-wide rather than one cluster: `SELECT application_name, client_addr FROM pg_stat_replication;` on each primary, compared against the inventory node_name.
- **Fix (combination):** In `build_replica_following_map` (src/v2/analyze/split_brain.rs:305-315) stop comparing across namespaces. Recommended: mirror the replica-side gate, which already matches on IP (`wr.sender_host == primary.ip_address.to_string()`, :281), by matching the primary row on `conn.client_addr == Some(replica.ip_address.to_string())`; the fixture shows client_addr 127.2.12.151 / 127.3.12.151, exactly the replicas' ip_address values. Keep the state and reply_time checks; the `!application_name.is_empty()` guard can stay but stops being load-bearing once identity is IP-based. Then correct ADR line 27 to state the real repmgr form (underscored, domain-stripped) and rewrite §1 line 73 to describe whatever match the code performs. Note the split_brain unit tests and the analyze.rs integration test at src/v2/analyze.rs:1276-1294 (`with_followers(&["dev-pg-app001-db003.sto3.example.com"])`) fabricate an application_name Postgres would never emit, so they must be re-based on fleet-shaped names or they will keep certifying the broken compare.
- **Alternatives rejected:** (a) Reuse writer/build.rs `normalize_application_name` (:422) and `extract_db_number` (:408): they do not produce a common string -- `dev_pg_app001_db002` -> `db002` but `dev-pg-app001-db002.sto2.example.com` -> `db002@sto2` -- so a new shared canonical form would have to be invented and the analyzer would take a dependency on the writer. (b) Parse `application_name=` out of the replica's `wal_receiver.conninfo` / `configuration["primary_conninfo"]` (both captured) and compare that to conn.application_name: correct and namespace-exact, but adds a conninfo parser; keep as fallback if client_addr proves unreliable under NAT.

### [high | diverges] ADR lines 19 (restated in docs/concepts/split-brain.md:50)
- **Claim:** The operational definition of the true primary: "the one whose **sync quorum is satisfied and that is actively committing**"
- **Justification:** The code's election predicate is strictly "has at least one live bidirectional follower". It never checks that the follower is a `synchronous_standby_names` member before electing, and never observes commit activity at all -- `determine_true_primary` reads only `replicas_following`; `emit_quorum_findings` runs after the map is built and can only attach a finding. So a primary elected as "true" can be one the resolver itself has just flagged quorum-unsatisfied.
- **Code:** `src/v2/analyze/split_brain.rs:352-393 `fn determine_true_primary(timeline_info, replicas_following, findings)` -- `findings` is only used for confidence (:384-391), never for the pick; src/v2/analyze/split_brain.rs:404-410 downgrades `PrimaryQuorumUnsatisfied` on the elected primary from Refuse to `Confidence::Conflicting`; src/v2/writer/build.rs:692-727 `format_resolution` prints "{true} has quorum" for every variant without inspecting `info.findings`. Nothing in split_brain.rs reads `sync_state`, `current_wal_lsn`, or any second sample (grep for sync_state/current_wal_lsn in that file returns nothing).`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** A captured split-brain scan (two Role::Primary nodes in one cluster) is required to show the divergence in the wild; none exists. Minimum capture: the full scan JSON for a cluster during slow fencing, plus `SELECT application_name, sync_state, state, reply_time FROM pg_stat_replication;` on both primaries and `SELECT * FROM pg_stat_wal_receiver;` on the surviving replica.
- **Fix (combination):** Two coherent options; pick one and make ADR and code agree. (i) Make the code match the definition: require the gate-passing follower to be a member of the electing primary's `synchronous_standby_names` before `LowerTimelineHasQuorum` (or `ReplicaFollowing`) may name that primary true -- i.e. reuse the intersection already computed in `emit_quorum_findings`, and fall back to `Indeterminate` when the only follower is async. (ii) Keep the live-follower proxy and make ADR line 19 state it explicitly as a proxy, valid only under assumption bullets 1+2 (exactly one replica, and it is SSN-listed), and stop the writer asserting "{true} has quorum" when `findings` contains `PrimaryQuorumUnsatisfied { primary: true_primary }`. Note the ADR is internally split on this: line 53 endorses quorum state not changing the pick, while line 19 defines the winner by quorum.
- **Alternatives rejected:** Adding an "actively committing" probe (two `pg_current_wal_lsn()` samples) was considered and rejected as out of proportion: the scanner takes one sample per node per run, a second sample would change the scan contract, and on an idle cluster a non-advancing LSN does not mean "cannot commit". `pg_stat_replication.sync_state` is the cheap, already-captured stand-in for "counted toward quorum".

### [high | diverges] ADR lines 26 (derivation at 143-150)
- **Claim:** Cluster assumption "`synchronous_standby_names = 'ANY 1 (A, B)'`" underpins the §4 quorum derivation `observed = |members ∩ gated_followers|`
- **Justification:** `members` come from parsing SSN and are therefore in application-name form (`dev_pg_app001_db002`), while `gated` is keyed by `node_name` (FQDN). The intersection is empty for every real cluster, so `observed` is always 0 and `PrimaryQuorumUnsatisfied` fires unconditionally for both primaries -- the derivation carries no information. This is a second, independent cross-namespace compare: fixing the primary-side gate (finding 1) does not fix it.
- **Code:** `src/v2/analyze/split_brain.rs:660-663 `let observed = members.iter().filter(|m| gated.iter().any(|g| g == *m)).count() as u32;` where `gated` is filled at :320-322 with `replica.node_name`; fixture SSN: `"synchronous_standby_names": "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Already evidenced by the fixture (SSN member strings vs node_name strings in the same capture).
- **Fix (resolver):** Drop the SSN parse for this purpose and count what Postgres has already decided: for each primary, `observed` = number of `pg_stat_replication` rows that passed the §1 gate and whose `sync_state` is `Quorum` or `Sync` (src/v2/scan/health_check_primary.rs:60-69 already models it; src/v2/analyze/checks.rs:373-383 already uses that signal for the single-primary quorum check). `required` still comes from `parse(ssn).count`. If the SSN membership set must be kept, have `build_replica_following_map` return the matched `application_name` alongside the node name and intersect on that instead.
- **Alternatives rejected:** Keying `replicas_following` by application_name instead of node_name was rejected: the same map values are used verbatim as operator-facing node identifiers in `SplitBrainResolution::*::replicas_following_true` (src/v2/analyze/split_brain.rs:460, 479, 538), so switching the key namespace would push the wrong form into the report.

### [high | diverges] ADR lines 58
- **Claim:** "verdicts produced during such windows are capped at `BestEffort` and re-converge once `wal_sender_timeout` elapses"
- **Justification:** No cap exists. `BestEffort` is the *maximum* of the three-value `Confidence` enum (Refuse < Conflicting < BestEffort) and is the initial value of every resolution; confidence is only ever reduced by `.min()` over findings. There is no transient-window detection anywhere, and no re-scan comparison. Worse, for anything short of `Refuse` the confidence never reaches the operator: the terminal writer prints only `reason.short`, and only `Refuse` gets a prefix.
- **Code:** `src/v2/analyze/split_brain.rs:71-77 (`enum Confidence { Refuse, Conflicting, BestEffort }` with derived Ord) and :384-391 `.map(|f| determine_confidence_level(f, true_primary)).min().unwrap_or(Confidence::BestEffort)`; src/v2/writer/build.rs:654-659 `let short = if matches!(info.confidence, Confidence::Refuse) { format_refuse(info) } else { format_resolution(info) };` -- `Conflicting` renders identically to full confidence. Confidence reaches output only inside `details_json`, which is emitted by the CSV writer (src/v2/writer/csv.rs:66), not the terminal.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** To show the transient window actually produces verdicts on this fleet: one `--watch` run captured across a controlled failover, retaining each iteration's short string. Nothing today records per-iteration output.
- **Fix (combination):** Either delete the sentence (it names a mitigation that cannot exist while BestEffort is the ceiling) or implement it: add a confidence level above BestEffort (the ADR already contemplates `Verified` at line 229), keep BestEffort as the transient-window ceiling, and render a marker in `reason.short` for every non-top confidence -- e.g. prefix `CONFLICTING/` the way `REFUSE/` is prefixed in src/v2/writer/build.rs:686. Without the second half, an operator sees "demote db001" with no indication the resolver is unsure.
- **Alternatives rejected:** Suppressing the resolution text for `Conflicting` the way it is suppressed for `Refuse` was rejected as too blunt: `Conflicting` is the routine outcome (any stale wal_receiver row produces it, src/v2/analyze/split_brain.rs:411-413), so blanking the verdict would blank most real reports.

### [medium | diverges] ADR lines 15, 58
- **Claim:** "The tool is run **after** a failover ... Scans observe the residue, not the failover-in-flight" and "The tool is typically run in stable post-failover state where this isn't an issue"
- **Justification:** The binary ships a `--watch <secs>` mode whose explicit purpose is to re-scan unhealthy clusters on a short operator-chosen interval until they turn healthy -- i.e. to be pointed at a cluster *during* remediation. Nothing rate-limits the interval against `wal_sender_timeout`, so the in-flight window the ADR declines to disambiguate is a first-class, designed-for usage rather than an edge case.
- **Code:** `src/main.rs:128-181 `async fn run_watch_mode(...)` -- loop with `tokio::time::sleep(interval)`, `cluster_filter = Some(scan_result.clusters_to_rescan);` and `println!("\nAll clusters healthy. Exiting watch mode.")` at :152; interval comes from `args.watch.map(Duration::from_secs)` (src/main.rs:83) with no lower bound.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** Whether DBAs actually run `--watch` during a failover: check the runbook / shell history for `db-scan --watch`. The capability is code-evident; the usage is not.
- **Fix (adr-text):** Amend the Context paragraph (line 15) and the closing paragraph (line 58) to acknowledge `--watch`: state that the resolver's guarantees are defined for a settled cluster, and that watch-mode iterations inside `wal_sender_timeout` of a disconnect can flip verdicts between iterations. If the ADR wants to keep the premise, the enforcement belongs in the tool -- e.g. suppress or mark the SplitBrain short string when the scan is a watch iteration started less than `wal_sender_timeout` after the previous one.
- **Alternatives rejected:** Making watch mode skip split-brain resolution entirely was rejected: watching a cluster through a split brain is exactly when an operator wants the output; the fix is honest labelling, not suppression.

### [high | unverifiable] ADR lines 29 (repeated at 204 and 266)
- **Claim:** "Scanner role has `pg_read_server_files` (the tool is run by DBAs, so this privilege is in place)"
- **Justification:** Asserted three times in the ADR, never checked anywhere in the code or tests -- grep for `pg_read_server_files` finds only those three ADR lines. The privilege is consumed unconditionally by the primary health-check query, and a permission failure is not degraded: it fails the whole query, so the node is recorded as `Role::UnknownPrimary` and disappears from `cluster.primaries()`. In a split brain both primaries are on TL>1, so both would drop out and the cluster would be reported `NoPrimary` -- the split brain would not be detected at all.
- **Code:** `src/v2/scan/health_check_primary.rs:150-159 `'timeline_history', (SELECT CASE WHEN timeline_id = 1 THEN NULL ELSE pg_read_file('pg_wal/' || lpad(upper(to_hex(timeline_id)), 8, '0') || '.history', 0, (1024*1024)::bigint, true) END FROM cc)` -- the `missing_ok` argument covers a missing file, not a missing grant; failure path src/v2/scan/health_check_primary.rs:258-272 `role: Role::UnknownPrimary`; filter src/v2/scan.rs:333-335 `pub fn is_primary(&self) -> bool { matches!(self, Role::Primary { .. }) }`; early return src/v2/analyze.rs:311-314 (`ClusterVerdict::NoPrimary`) which also bypasses the per-node error checks at :342-346.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** Run as the scanner role on one fleet primary: `SELECT current_user, pg_has_role(current_user, 'pg_read_server_files', 'member') AS in_role, has_function_privilege(current_user, 'pg_read_file(text,bigint,bigint,boolean)', 'execute') AS can_exec;` -- if `in_role` and `can_exec` are both false the assumption is wrong and this becomes critical.
- **Fix (combination):** Make the assumption self-verifying rather than asserted: either (a) run the `has_function_privilege` probe once per scan and record the result, or (b) make timeline_history failure non-fatal so a privilege error degrades to `timeline_history: NULL` instead of losing the node -- split it out of the single jsonb_build_object into its own statement whose error is caught and stored in `AnalyzedNode.errors`. (b) is the one that protects the verdict, because it keeps both primaries visible and therefore keeps the split brain detectable.
- **Alternatives rejected:** Adding a startup preflight that aborts the whole scan when the grant is missing was rejected: it turns a per-node degradation into a fleet-wide outage of the tool, and timeline_history is only needed for fork-LSN work that is itself deferred (§7).

### [medium | diverges] ADR lines 25
- **Claim:** "1 primary + 2 replicas per cluster ... The resolver assumes this topology; >2 replicas is out of scope for v1"
- **Justification:** The resolver has no topology guard: `resolve_split_brain` asserts only `primaries.len() >= 2` and loops over however many replicas it is handed. The `replicas.len() > 2` guard in `analyze()` sits *after* the split-brain branch returns, so it is unreachable on the split-brain path. What actually bounds the topology is an unrelated pipeline rule -- `cluster_builder` dispatches a cluster the moment it has received exactly 3 nodes -- and that rule mis-handles a >3-node cluster: it analyses an arbitrary 3-node subset (whichever health checks completed first) and silently strands the remainder. The 3-node proof the safety argument rests on would then be applied to a sample, not the cluster.
- **Code:** `src/v2/analyze.rs:316-328 -- `if primaries.len() > 1 { let split_brain_info = resolve_split_brain(&primaries, &replicas); ... return ...; }` precedes `if replicas.len() > 2 { ... UnexpectedTopology ... }`; src/v2/analyze/split_brain.rs:136-139 `assert!(primaries.len() >= 2, "resolve_split_brain requires at least 2 primaries")` is the only shape check; src/v2/cluster.rs:31 `if nodes[&cluster_id].len() == 3 {` followed by `nodes.remove(&cluster_id)` at :32.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Count nodes per cluster in the inventory the scanner reads: `SELECT cluster_id, count(*) FROM <inventory nodes table> GROUP BY 1 HAVING count(*) <> 3;` If that returns no rows, the assumption holds today and this drops to low; if it returns rows, those clusters are being analysed from a partial, non-deterministic subset.
- **Fix (combination):** Enforce the assumption where the ADR says it lives -- in the resolver: have `resolve_split_brain` return `Indeterminate` (or cap confidence) when `primaries.len() + replicas.len() != 3`, since the ADR's C-a..C-g matrix and the 3-node fork-emptiness proof are only defined for that shape. Separately fix `cluster_builder` to dispatch on "all inventory nodes for this cluster_id received" rather than the literal `== 3`, so a 4-node cluster is either analysed whole or reported incomplete instead of silently sampled.
- **Alternatives rejected:** Moving the existing `replicas.len() > 2` / `UnexpectedTopology` check above the split-brain branch was rejected as insufficient: with the `== 3` dispatch rule a 4-node cluster never presents 3 replicas to `analyze()` in the first place, so the check would still not fire -- the defect is upstream in cluster assembly.

### [medium | diverges] ADR lines 28 (derivation at 85-87)
- **Claim:** "`wal_sender_timeout = 5min` (300_000 ms). Keepalives are sent at `wal_sender_timeout / 2` ~ 150 s" (and the §5 restatement "replica side [updates] on keepalive (~150 s in our config)")
- **Justification:** The setting value is right (fixture: `"wal_sender_timeout": "300000"`, raw ms, parses cleanly) and the derived threshold matches the code exactly: 300000/2 + 30000 = 180000 ms. The keepalive *cadence* claim is wrong about Postgres. A walsender sends a keepalive when half of `wal_sender_timeout` has elapsed **since the standby last replied**, not on a 150 s clock; because the standby reports every `wal_receiver_status_interval` (10 s default) that trigger effectively never fires on a healthy link. What actually refreshes `last_msg_receipt_time` on an idle link is the sender's reply to the walreceiver's own ping, which the receiver issues at `wal_receiver_timeout / 2` (30 s under the default 60 s) -- a setting this scanner does not collect. So the 180 s threshold is justified by the wrong mechanism, and it is looser than the real inter-message gap, widening exactly the stale-row acceptance window the gate was added to close.
- **Code:** `src/v2/analyze/split_brain.rs:266 `let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;` and :609-613 `fn parse_wal_sender_timeout(...) -> i64 { cfg.get("wal_sender_timeout").and_then(|s| s.parse().ok()).unwrap_or(60_000) }`; applied to the replica clock at :284-286 `wr.last_msg_receipt_time.is_some_and(|t| (r_health.current_time - t).num_milliseconds() <= threshold_ms)`. `wal_receiver_timeout` is absent from the replica settings list at src/v2/scan/health_check_replica.rs:105-114 (`'hot_standby','primary_conninfo','primary_slot_name','recovery_target_timeline'`).`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** On a fleet replica: `SELECT name, setting, unit, source FROM pg_settings WHERE name IN ('wal_receiver_timeout','wal_receiver_status_interval','wal_sender_timeout');` -- this decides whether the 180 s window is even reachable, because at the 60 s default the walreceiver exits and `pg_stat_wal_receiver` empties long before 180 s, whereas at 300000 a dead row can sit there for the full window. Then, to pin the real cadence: capture `pg_stat_wal_receiver` on an idle replica at 10 s intervals for 5 minutes and observe how often `last_msg_receipt_time` advances.
- **Fix (combination):** Correct ADR lines 28 and 85-87 to state the actual mechanism (walsender keepalive is triggered by standby silence of `wal_sender_timeout/2`; the idle-link refresh is driven by the replica's `wal_receiver_timeout/2` ping) and re-derive the freshness threshold from `wal_receiver_timeout` on the replica side, keeping `wal_sender_timeout` for the primary side. Add `wal_receiver_timeout` and `wal_receiver_status_interval` to the pg_settings list in HEALTH_CHECK_REPLICA_QUERY (src/v2/scan/health_check_replica.rs:108-113) so the derivation can be validated against captured data instead of asserted.
- **Alternatives rejected:** Simply tightening the constant (e.g. 60 s) was rejected: without the `wal_receiver_timeout` capture there is no evidence for any number, and a threshold below the true inter-message gap would reject live streams -- a false 'no live replicas' is the destructive direction (it produces `HigherTimeline` + 'demote').

### [low | stale] ADR lines 13
- **Claim:** "`src/v2/analyze/split_brain.rs` resolves split-brain by comparing timelines and checking which primary a replica's `wal_receiver.sender_host` points at. The check is too permissive: a `sender_host` match alone counts as 'following,' even when the streaming connection is dead and the row is stale."
- **Justification:** Written in the present tense about the pre-ADR code. All twelve commits have landed: the gate now additionally requires sender_port, status in {streaming, catchup}, a fresh `last_msg_receipt_time`, and a corroborating primary-side row. A reader auditing today's code against this paragraph would be told the opposite of what the code does.
- **Code:** `src/v2/analyze/split_brain.rs:281-286 `let replica_passes = wr.sender_host == primary.ip_address.to_string() && wr.sender_port == 5432 && matches!(wr.status.as_str(), "streaming" | "catchup") && wr.last_msg_receipt_time.is_some_and(...)``
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Put the paragraph in the past tense ("resolved ... The check was too permissive") or add a status marker noting the gate landed, so the Context is not read as a description of current behaviour.
- **Alternatives rejected:** Deleting the paragraph was rejected -- it is the motivation for the whole ADR and should stay, just tensed correctly.

### [low | diverges] ADR lines 11 (pointer); docs/concepts/split-brain.md:7, 15, 42, 70
- **Claim:** docs/concepts/split-brain.md: "Three nodes, `synchronous_standby_names = 'ANY 1 (A, B)'`, `synchronous_commit = on`" -- the flushed-past-fork -> acknowledged-writes inference and the 3-node fork-emptiness proof are both stated as resting on `synchronous_commit = on`
- **Justification:** The fleet runs `synchronous_commit = remote_apply`, not `on`. The direction is safe -- remote_apply waits for apply, which is strictly stronger than flush, so the inference holds a fortiori -- and the code's weakened-value list correctly excludes it. But the concept doc states a cluster fact that the only captured data contradicts, and ADR-002's own assumption list (lines 25-29) omits `synchronous_commit` entirely even though it is the single most load-bearing assumption behind §2's Refuse gate.
- **Code:** `tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json `"synchronous_commit": "remote_apply"`; src/v2/analyze/split_brain.rs:13 `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` and :169-174 (remote_apply correctly not flagged).`
- **Reachable in code:** False | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Already evidenced by the fixture; to confirm fleet-wide: `SELECT name, setting FROM pg_settings WHERE name = 'synchronous_commit';` on each primary.
- **Fix (adr-text):** In docs/concepts/split-brain.md say `synchronous_commit >= on` (i.e. one of on / remote_flush / remote_apply, with the fleet currently on remote_apply) wherever the inference is stated, and add a `synchronous_commit >= on` bullet to ADR-002's Cluster assumptions list at lines 25-29 with a pointer to the §2 gate that enforces it.
- **Alternatives rejected:** Leaving it as 'on' as a simplification was rejected because the doc explicitly grounds a data-loss inference on the exact value, and the value is the one thing §2 refuses on.

### [info | implemented] ADR lines 21
- **Claim:** "Active flushing replica evidence on a lower-TL primary correctly identifies it as the true primary. The existing `ReplicaOverridesTimeline` variant (proposed renamed to `LowerTimelineHasQuorum`) codifies this case."
- **Justification:** The rename landed and the branch exists exactly as described: a lower-TL primary with gate-passing followers and a higher-TL primary with none elects the lower-TL node. Worth recording as info because this branch is the C-b/C-c safety mechanism and, given finding 1, it can never fire on fleet-shaped data -- the code is correct and dead simultaneously.
- **Code:** `src/v2/analyze/split_brain.rs:33-39 `LowerTimelineHasQuorum { true_primary_timeline, stale_timeline, replicas_following_true }` and :449-464 `if !replicas_following_stale.is_empty() && replicas_following_highest.is_empty() { ... true_primary: stale_node.node_name.clone() ... }`; lower-TL primaries are included in the gate loop at :255-259.`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | implemented] ADR lines 26
- **Claim:** Cluster assumption "`synchronous_standby_names = 'ANY 1 (A, B)'` -- quorum is satisfiable by either replica alone"
- **Justification:** The fixture value `ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )` is exactly this shape, both replicas are listed, and the parser handles the spacing: `ANY ` prefix, count 1, and the trailing `)` strip yields both members. The shape assumption itself holds against captured data; only the member *string form* is wrong (finding 3).
- **Code:** `src/v2/analyze/sync_standby_names.rs:24-57 `pub fn parse(input: &str) -> Option<Quorum>` -- `strip_prefix("ANY ")`, `rest.split_once('(')`, `rest.trim().strip_suffix(')')`; consumed at src/v2/analyze/split_brain.rs:650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue; };``
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 


## ADR-002 §1 "Flushing-liveness gate" (lines 62-90), verified against build_replica_following_map in src/v2/analyze/split_brain.rs

### [critical | diverges] ADR lines 73
- **Claim:** "The primary's `pg_stat_replication` has a row whose `application_name` equals the replica's node name." (relies on the Cluster-assumption "repmgr-set `application_name` equals the node name", line 30)
- **Justification:** The equality is implemented literally, but on real fleet data the two strings can never be equal, so the corroborating side always fails and no replica is ever counted as following.
- **Code:** `src/v2/analyze/split_brain.rs:307 `&& conn.application_name == replica.node_name` (inside the primary_row find at 305-315); contrast src/v2/writer/build.rs:422 `fn normalize_application_name` and :408 `fn extract_db_number`, which exist precisely to bridge the two forms`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Already settled by tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json: node_name "dev-pg-app001-db002.sto2.example.com" (line 93) vs pg_stat_replication.application_name "dev_pg_app001_db002" (line 35). No further capture needed. To confirm it is fleet-wide rather than one cluster: `SELECT application_name FROM pg_stat_replication` on each primary compared with the inventory node_name.
- **Fix (resolver):** In build_replica_following_map, stop comparing raw strings. Preferred: match the pg_stat_replication row against the application_name the replica itself advertises -- the replica already reports it in `wal_receiver.conninfo` and in `configuration["primary_conninfo"]` (`application_name=dev_pg_app001_db003` in both, fixture lines 117/173). Parse `application_name=` out of the replica's own conninfo and compare exactly; that is convention-free and needs no naming heuristic. Fallback if that is rejected: compare `normalize_application_name(&conn.application_name)` (yields "db002") against the db-token of `replica.node_name` (the `split('-').find(starts_with("db")).split('.').next()` form already used by find_replica_timeline in writer/build.rs:379-384). Whichever is chosen, add a split_brain.rs test whose node_name is the FQDN form and whose application_name is the underscore form -- today's tests use "db003" for both (split_brain.rs:1348-1352, 1366-1378) so the mismatch is invisible.
- **Alternatives rejected:** Fixing it in writer/build.rs is too late: by then the verdict, the follower map and the quorum counts are already wrong. Substring/suffix matching is worse than the conninfo parse -- "db002" is a suffix of "db0020" and of any node whose name embeds another's.

### [high | diverges] ADR lines 85,87
- **Claim:** "`pg_stat_replication.reply_time` (primary side) and `wal_receiver.last_msg_receipt_time` (replica side) have asymmetric update cadences -- primary side updates on `wal_receiver_status_interval` (~10 s), replica side on keepalive (~150 s in our config). A symmetric threshold is generous on the primary side" and "~180 s -- comfortably above the keepalive cadence of ~150 s"
- **Justification:** The replica-side receipt cadence is not driven by the primary's wal_sender_timeout/2; it is driven by the replica's own wal_receiver_timeout/2, a GUC the scanner never collects, so the 30 s margin the ADR claims is unverified and can be negative.
- **Code:** `src/v2/analyze/split_brain.rs:266 `let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;` applied to the replica-side check at :285 `(r_health.current_time - t).num_milliseconds() <= threshold_ms`; src/v2/scan/health_check_replica.rs:108-113 -- the replica config query pulls only hot_standby, primary_conninfo, primary_slot_name, recovery_target_timeline`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** On each replica: `SELECT name, setting FROM pg_settings WHERE name IN ('wal_receiver_timeout','wal_receiver_status_interval');`. The gate is safe iff wal_receiver_timeout is in (0, 2*threshold] -- with threshold 180000 ms that means 1..=360000. Value 0 (ping disabled) or >360000 breaks it. Nothing in any capture we hold records this today.
- **Fix (combination):** Add 'wal_receiver_timeout' (and 'wal_receiver_status_interval') to the name IN (...) list of HEALTH_CHECK_REPLICA_QUERY (health_check_replica.rs:108-113), then compute the replica-side threshold as max(wal_sender_timeout/2, wal_receiver_timeout/2) + 30_000, treating wal_receiver_timeout=0 as "no ping, cadence unbounded" -> skip the receipt-time check and emit a finding instead of silently rejecting. Correct ADR line 87 to say the replica-side cadence is wal_receiver_timeout/2 (standby-initiated ping, answered by the sender), not wal_sender_timeout/2.
- **Alternatives rejected:** Just widening the constant slack (e.g. +120 s instead of +30 s) hides the dependency and still breaks at wal_receiver_timeout=0. Deriving the replica-side threshold from the primary's setting -- what the code does now -- is the misattribution itself.

### [medium | diverges] ADR lines 70,75 (and 225)
- **Claim:** "`wal_receiver.last_msg_receipt_time` is within `freshness_threshold` of the scan-start timestamp" / "The row's `reply_time` is within `freshness_threshold` of the scan-start timestamp" (and §5's "Pass scan-start `DateTime<Utc>` from `analyze_clusters` through `analyze()` into `resolve_split_brain()` as a parameter, for the freshness gate. This is a small but load-bearing plumbing change.")
- **Justification:** The code measures age intra-node against each node's own now(); no scan-start value exists anywhere in the tree. The code's design is the correct one and the ADR should be amended -- the ADR's design is strictly more permissive and reintroduces scanner<->db clock skew.
- **Code:** `src/v2/analyze/split_brain.rs:132-135 `pub(super) fn resolve_split_brain(primaries: &[&AnalyzedNode], replicas: &[&AnalyzedNode]) -> SplitBrainInfo` (no timestamp parameter); :285 and :313 use `r_health.current_time` / `p_health.current_time`; src/v2/analyze.rs:318 `let split_brain_info = resolve_split_brain(&primaries, &replicas);`. `rg -n 'scan_start' src/` returns nothing.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** To size the gap the ADR's version would open: record the fleet-scan wall clock and per-node current_time in one capture and compute max(node.current_time) - scan_start. (The healthy fixture is useless here -- all three nodes carry the identical sanitized current_time 2025-09-20T18:57:24.600000Z.)
- **Fix (adr-text):** Rewrite ADR lines 70 and 75 to read "within `freshness_threshold` of that node's own `current_time` (captured by `now()` in the same health-check statement)", and delete/invert the §5 bullet at line 225 -- record it as "rejected: scan-start plumbing would make the gate more permissive the longer the scan runs and would import scanner<->db clock skew". Note that ADR line 215 (the 2026-06-07 revision) already prescribes the intra-node form -- "must source freshness separately (`wal_receiver` presence, `last_msg_receipt_time`, `lag.last_transaction_replay_at` against `current_time`)" -- so lines 70/75/225 are the leftovers.
- **Alternatives rejected:** Implementing §5 as written (plumbing scan-start into resolve_split_brain) is the wrong fix: it loosens a safety gate and adds a cross-clock comparison to the one side (replica) that is currently skew-free. Keeping both timestamps and taking the min would be strictly worse than the current code and adds a parameter for nothing.

### [medium | diverges] ADR lines 87 (ADR); split_brain.rs:242-244 (code doc)
- **Claim:** Code doc: "Freshness uses each node's own `current_time` against that same node's recorded timestamps, so the comparison is intra-node and immune to scanner<->db clock skew that would otherwise eat into a tight threshold." -- and the ADR's implicit assumption that a symmetric threshold on `reply_time` is merely "generous" (line 87)
- **Justification:** `pg_stat_replication.reply_time` is stored on the primary but its VALUE is the standby's clock reading, so the primary-side check is a cross-node comparison; only the replica-side check is genuinely intra-node.
- **Code:** `src/v2/analyze/split_brain.rs:312-314 `&& conn.reply_time.is_some_and(|t| { (p_health.current_time - t).num_milliseconds() <= threshold_ms })`; doc claim at :242-244`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** Per cluster, compare the three nodes' `current_time` values from one scan (they are captured within seconds of each other), or run `SELECT now()` on db001/db002/db003 back-to-back and diff. Skew must exceed ~180 s to flip a gate outcome. Also worth checking chronyd/ntp state on Harvester VMs that have been suspended/migrated.
- **Fix (combination):** Narrow the doc comment at split_brain.rs:242-244: the replica-side check is intra-node (last_msg_receipt_time is written by the walreceiver's own clock -- PG docs: "Receipt time of last message received from origin WAL sender"), but the primary-side check compares the primary's now() against a timestamp generated on the standby (PG docs: reply_time = "Send time of last reply message received from standby server"), so it is exposed to primary<->replica skew. Add one sentence to ADR line 87 saying the same, and note that the 30 s slack term is the only thing absorbing skew.
- **Alternatives rejected:** Switching the primary-side freshness input to `backend_start` (primary clock) does not work -- it is the connection start, not a liveness signal. Dropping the primary-side freshness check entirely would weaken the corroborating side more than skew does.

### [medium | diverges] ADR lines 67 (and Out of scope, 257)
- **Claim:** "`wal_receiver.sender_host` matches the primary. The comparison is `==` against `primary.ip_address.to_string()`; in environments where `primary_conninfo` uses a hostname, this comparison fails. Out of scope for v1; flag as a known limitation."
- **Justification:** The first half is implemented exactly; the "flag as a known limitation" half exists only in the ADR -- there is no code comment, no finding, and no operator-facing text, and the failure is silent rather than loud.
- **Code:** `src/v2/analyze/split_brain.rs:281 `let replica_passes = wr.sender_host == primary.ip_address.to_string()` and :293 `if wr.sender_host == primary.ip_address.to_string() {` (the guard that suppresses ReplicaWalReceiverStale for non-matching hosts). `rg -n 'hostname|primary_conninfo|limitation' src/v2/analyze/ src/v2/writer/` returns nothing.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Across the whole inventory, not just this one cluster: `SELECT setting FROM pg_settings WHERE name='primary_conninfo'` on every replica (or `SELECT sender_host FROM pg_stat_wal_receiver`), and check whether every value parses as an IPv4 literal. The healthy fixture shows host=127.1.12.151 for both replicas, i.e. one cluster's worth of evidence for the ADR's "current production uses IPs".
- **Fix (resolver):** Minimal, in-scope fix: when `wr.sender_host` does not parse as an `Ipv4Addr` and does not equal any candidate primary's ip_address, emit a finding (reuse ReplicaWalReceiverStale, or add a dedicated variant) so the operator sees "replica db003 names sender host X which matches no candidate primary" instead of nothing. At minimum, add the ADR's limitation as a comment at split_brain.rs:281 so the next reader knows the compare is IP-literal-only. Also note the compare is exact-string, so a non-canonical literal (leading zeros) or IPv6 fails the same way; `AnalyzedNode.ip_address` is `Ipv4Addr` (src/v2/scan.rs:240), so IPv6 replication networks are unrepresentable anywhere in the tool.
- **Alternatives rejected:** Resolving the hostname in the scanner (DNS) would make the verdict depend on the scanner host's resolver and on DNS state at scan time -- worse than a loud finding. Comparing against `client_addr` on the primary side instead does not help, because the replica side is authoritative and fails first.

### [low | unverifiable] ADR lines 85
- **Claim:** "`wal_sender_timeout_ms` is parsed from each primary's `configuration[\"wal_sender_timeout\"]` ... default 60_000 ms if missing/malformed."
- **Justification:** Missing/malformed is handled exactly as specified, but the ADR never says what to do with the legal Postgres value 0 ("disabled"), which parses successfully and yields a 30 s threshold.
- **Code:** `src/v2/analyze/split_brain.rs:609-613 `cfg.get("wal_sender_timeout").and_then(|s| s.parse().ok()).unwrap_or(60_000)`; consumed at :266 `(parse_wal_sender_timeout(...) / 2) + 30_000``
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** `SELECT setting FROM pg_settings WHERE name='wal_sender_timeout'` on every primary; the one captured cluster has "300000" (fixture line 22), so 0 is not present there.
- **Fix (combination):** Treat 0 as "timeouts disabled" rather than as a number: in parse_wal_sender_timeout, map Some("0") to the 60_000 default (or make the caller skip the freshness conjunct and emit a finding). Add one sentence to ADR line 85 stating the chosen behaviour for 0.
- **Alternatives rejected:** Leaving it as-is is defensible only while the fleet is uniformly 300000; the failure is silent and points in the destructive direction, so it is worth two lines of code.

### [low | diverges] ADR lines 77 (with matrix rows C-d/C-f, lines 47/50)
- **Claim:** §1 specifies only PrimaryDoesNotSeeReplica as the gate's finding; the case matrix attaches ReplicaWalReceiverStale to C-d ("`status != streaming/catchup`, OR `last_msg_receipt_time` aged out") and attaches no such finding to C-f ("`status=stopped/starting`").
- **Justification:** The code emits ReplicaWalReceiverStale for ANY replica-side failure once sender_host matches -- including a port mismatch and including the C-f stopped/starting states the matrix routes elsewhere -- so the finding's name overstates what was observed and C-f gains an unlisted finding that drags confidence to Conflicting.
- **Code:** `src/v2/analyze/split_brain.rs:288-299: `if !replica_passes { ... if wr.sender_host == primary.ip_address.to_string() { findings.push(SplitBrainFinding::ReplicaWalReceiverStale { ... }) } continue; }`; confidence mapping at :411-413`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None -- readable from the code. Tests split_brain.rs:1193-1243 assert exactly this behaviour for status="stopped" and sender_port=5433.
- **Fix (adr-text):** Either broaden the ADR: say in §1 that any replica-side failure with a matching sender_host emits ReplicaWalReceiverStale (and add it to matrix row C-f), or narrow the code to emit it only for the staleness/status causes and use a distinct signal for a port mismatch. The doc change is the cheaper and more honest one, since the operator does want to know the replica names this primary but is not usably attached to it.
- **Alternatives rejected:** Suppressing the finding for non-staleness causes would make a port mismatch invisible, which is worse; the current behaviour errs safe.

### [medium | implemented] ADR lines 75
- **Claim:** "The row's `reply_time` is within `freshness_threshold` of the scan-start timestamp" -- i.e. the primary-side freshness conjunct is a real gate
- **Justification:** The conjunct exists and is correct in shape, but no test exercises it: the test builders make every freshness comparison negative, so the primary-side freshness branch has never been shown to reject anything.
- **Code:** `src/v2/analyze/split_brain.rs:312-314; src/v2.rs:198 `reply_time: Some(Utc::now())` (no setter exists) against src/v2.rs:63 and :252 `current_time: DateTime::<Utc>::UNIX_EPOCH``
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** None -- `rg -n 'current_time' src/v2.rs` shows the builders hardcode UNIX_EPOCH and expose no with_current_time; `rg -n 'reply_time' src/` shows no setter either.
- **Fix (tests):** Add `with_current_time` to PrimaryHealthBuilder/ReplicaHealthBuilder and `with_follower_reply_time` to PrimaryHealthBuilder, then add two tests: (a) reply_time aged past threshold with a realistic current_time -> PrimaryDoesNotSeeReplica and no follower; (b) reply_time 60 s old with wal_sender_timeout=300000 -> still a follower. Today every gate test passes freshness through a ~56-year *negative* interval (current_time=1970 vs Utc::now()), so a sign or operand-order regression on the primary side would not be caught by anything except gate_rejects_stale_last_msg_receipt, which only covers the replica side.
- **Alternatives rejected:** Changing the builder default current_time to Utc::now() would silently flip several existing tests' meaning; adding explicit setters and two targeted tests is the surgical option.

### [info | implemented] ADR lines 67,68
- **Claim:** "`wal_receiver.sender_host` matches the primary. The comparison is `==` against `primary.ip_address.to_string()`" and "`wal_receiver.sender_port == 5432`"
- **Justification:** Both conjuncts are present verbatim and both hold on captured fleet data.
- **Code:** `src/v2/analyze/split_brain.rs:281-282 `wr.sender_host == primary.ip_address.to_string() && wr.sender_port == 5432``
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Settled: fixture line 11 ip_address "127.1.12.151" vs lines 115-116/160-161 sender_host "127.1.12.151", sender_port 5432. The hardcoded 5432 is consistent with the rest of the tool -- src/v2/db.rs:58 `.port(5432)` -- so a non-5432 cluster could not be scanned at all and the gate conjunct is not the binding constraint.
- **Fix (none):** 

### [info | implemented] ADR lines 69,74
- **Claim:** "`wal_receiver.status` in {streaming, catchup}. `catchup` is genuinely-following mid-recovery and must not be rejected." / "The row's `state` in {streaming, catchup}, and explicitly `state != backup`"
- **Justification:** Both sides use whitelists that admit catchup and exclude backup; Startup/Stopping/Unknown are excluded by the same whitelist, which is the conservative reading the ADR implies and is complete for PG 15.
- **Code:** `src/v2/analyze/split_brain.rs:283 `matches!(wr.status.as_str(), "streaming" | "catchup")` and :308-311 `matches!(conn.state, ReplicationState::Streaming | ReplicationState::Catchup)`; enum at src/v2/scan/health_check_primary.rs:27-35; catchup also raises ReplicaInCatchup at split_brain.rs:328-333`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None. PG 15's pg_stat_replication.state domain is exactly startup/catchup/streaming/backup/stopping, all five of which the enum names, so the `#[serde(other)] Unknown` catch-all is unreachable on this fleet (pg_version "15.14" on all three nodes) -- it would only be hit by a future major. Tests: gate_rejects_state_backup (split_brain.rs:1362-1388), gate_accepts_catchup_status_and_emits_replica_in_catchup (:1299), gate_rejects_status_not_streaming (:1193).
- **Fix (none):** 

### [info | implemented] ADR lines 72,77
- **Claim:** "Primary side (corroborating, only checked if replica side passes)" ... "If the replica side passes but the primary side does not, emit `PrimaryDoesNotSeeReplica(primary, replica)` as a finding and do not count the replica as following"
- **Justification:** The `continue` on replica-side failure enforces the asymmetric precedence, and the None arm emits the finding without touching the follower map.
- **Code:** `src/v2/analyze/split_brain.rs:288-300 (`if !replica_passes { ...; continue; }`) and :317-338 (`match primary_row { Some(row) => { following.entry(...).push(...) ... } None => findings.push(SplitBrainFinding::PrimaryDoesNotSeeReplica(...)) }`)`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None. Also correct per §1's C-e argument: since the replica-side conjunct requires sender_host == this primary's IP, at most one primary can reach the primary-side check for a given replica, so a stale pg_stat_replication row on the other primary is filtered before it is ever consulted.
- **Fix (none):** 

### [info | implemented] ADR lines 73
- **Claim:** "`application_name == \"\"` is rejected as unmatchable (postgres default when client doesn't set one; matches indiscriminately otherwise)."
- **Justification:** The explicit is_empty guard is present and ordered before the equality, matching the ADR's stated intent.
- **Code:** `src/v2/analyze/split_brain.rs:306 `!conn.application_name.is_empty()``
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None. The guard is strictly redundant today -- `conn.application_name == replica.node_name` can only be true for an empty application_name if node_name is also empty, which the inventory never produces -- but it is cheap and it documents the ADR's reasoning. Test: gate_rejects_empty_application_name (split_brain.rs:1344-1360).
- **Fix (none):** 

### [info | implemented] ADR lines 82,85
- **Claim:** "freshness_threshold = wal_sender_timeout_ms / 2 + 30_000 ms" ... derived per primary; "`pg_settings` returns [wal_sender_timeout] as raw milliseconds with no unit suffix"
- **Justification:** The formula is transcribed exactly, recomputed per primary inside the outer loop, and the units claim is true of the pg_settings.setting column and confirmed by captured data.
- **Code:** `src/v2/analyze/split_brain.rs:265-266 `// wal_sender_timeout can differ between primaries` / `let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;`; source of the value: src/v2/scan/health_check_primary.rs:163-176 `SELECT jsonb_object_agg(name, setting) FROM pg_settings WHERE name IN (... 'wal_sender_timeout' ...)``
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Settled two ways: fixture line 22 `"wal_sender_timeout": "300000"` (so 300000/2+30000 = 180000 ms as the ADR says), and the PG 15 docs for pg_settings, where `setting` is "Current value of the parameter" and `unit` is the separate "Implicit unit of the parameter" -- the suffix form ("5min") is what SHOW/current_setting() return, not pg_settings.setting. The query reads pg_settings, so the ADR's claim holds. Note the threshold derived from the primary is also applied to the replica-side check (:285); that is what §1 intends (one freshness_threshold per primary), but see the wal_receiver_timeout finding for why the replica side needs its own basis.
- **Fix (none):** 

### [info | implemented] ADR lines 89
- **Claim:** "Rejected as gate inputs (kept available as findings only): raw `flush_lsn` freshness (zombie rows hold fresh values until `wal_sender_timeout` fires), `flush_lag` (stops updating on idle clusters), archive recency (under TL fork the two primaries write different filenames; they do not collide)."
- **Justification:** None of the three appears anywhere in split_brain.rs, and each stated reason is substantially true of Postgres.
- **Code:** ``rg -n 'flush_lsn|flush_lag|archiv' src/v2/analyze/split_brain.rs` returns nothing; the fields remain captured (src/v2/scan/health_check_primary.rs:135,138 and :116 archiver) and are used only by unrelated checks (src/v2/analyze/checks.rs:37-59 archive lag, :173 replay/flush LSN for replica lag)`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Reasons checked against the PG 15 docs: for the lag columns, "If the standby server has entirely caught up with the sending server and there is no more WAL activity, the most recently measured lag times will continue to be displayed for a short time and then show NULL" -- so the ADR's "stops updating on idle clusters" is right in substance (stale value, then NULL), though it understates that the column goes NULL rather than merely freezing. flush_lsn: a walsender row survives a silently-broken TCP connection until wal_sender_timeout terminates it, and its LSN columns freeze at their last value, so the ADR's rationale holds. Archive recency: after a fork the two primaries write disjoint filenames (different TL prefix), so both keep archiving successfully and recency discriminates nothing -- true, though the ADR states the fact ("they do not collide") without spelling out that this is why it is useless as a liveness input.
- **Fix (none):** Optional, text-only: expand ADR line 89's archive clause to "...write different filenames, so both keep archiving successfully and recency cannot distinguish the live primary from the zombie", and change "stops updating" to "holds its last value briefly, then reads NULL".


## ADR-002 lines 31-59: the case matrix as a document (completeness of C-a..C-g over the state space, internal consistency of the "Verdict + findings" column, the "three facts" list, C-c's proof, C-e's asymmetric precedence, C-g's mis-pick claim)

### [critical | diverges] ADR lines 39-47
- **Claim:** The C-a..C-g enumeration covers db003's states. Every row where db003's wal_receiver names a primary and looks live (C-a, C-b, C-c, C-e) is stated to resolve as `Both` / `LowerTimelineHasQuorum` with `BidirectionalFlushingConfirmed`.
- **Justification:** No row covers the §1-mandated third outcome (replica side passes, primary side fails -> `PrimaryDoesNotSeeReplica`, NOT counted as following), and on captured fleet data that is the outcome every one of those four rows actually produces.
- **Code:** `src/v2/analyze/split_brain.rs:307 `&& conn.application_name == replica.node_name` (raw compare); :335 `None => findings.push(SplitBrainFinding::PrimaryDoesNotSeeReplica(`. Fixture tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json: `"application_name": "dev_pg_app001_db002"` vs `"node_name": "dev-pg-app001-db002.sto2.example.com"`. The bridging helpers exist but only in the writer: src/v2/writer/build.rs:376 `let normalized = normalize_application_name(app_name);``
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** On any cluster: `SELECT application_name, client_addr, state FROM pg_stat_replication;` on the primary, next to the scanner's stored `node_name` for the same node -- confirms the two forms never compare equal. Already evidenced in the captured fixture; a live re-check costs one query.
- **Fix (combination):** Two parts. (a) Resolver: make the primary-side lookup compare normalized forms instead of `conn.application_name == replica.node_name` -- reuse the same db-number extraction the writer already has (build.rs `normalize_application_name`/`find_replica_timeline`) rather than a second ad-hoc rule, and add a split_brain.rs unit test whose node_name is `dev-pg-app001-db003.sto3.example.com` while application_name is `dev_pg_app001_db003` (today's tests use `db003` for both, so they cannot see this). (b) ADR: add an explicit matrix row for "replica side passes, primary side fails -> `PrimaryDoesNotSeeReplica`, replica not counted, verdict falls through to `HigherTimeline`", because that outcome is defined in §1 but modelled nowhere in the matrix.
- **Alternatives rejected:** Loosening the primary-side gate to a substring/suffix match: rejected, it would re-admit the empty-application_name and indiscriminate-match hazard §1 explicitly closes. Fixing only the ADR text: rejected, the text would then document a verdict the code cannot reach on real hostnames.

### [critical | diverges] ADR lines 33-47
- **Claim:** The matrix is exhaustive over db003's state; the replica set is always exactly {db003}, so at most one primary can ever have a live flushing follower.
- **Justification:** No row covers two primaries each holding a gate-passing follower. The code does not reject that input, resolves it as `Both`, and the writer then prints a hardcoded, false 'quorum unsatisfied' justification for demoting a primary that is quorum-satisfied.
- **Code:** `src/v2/analyze/split_brain.rs:449 `if !replicas_following_stale.is_empty() && replicas_following_highest.is_empty() {` -- false when both are non-empty, so :465 `} else if !replicas_following_highest.is_empty() {` wins and yields `Both`. src/v2/writer/build.rs:701 `"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, quorum unsatisfied)"`. The topology guard that would prevent the input exists only on the single-primary path: src/v2/analyze.rs:323 `if replicas.len() > 2 {` sits *after* the split-brain early return at :317-321.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Inventory query against the scanner's own cluster/node tables: `SELECT cluster_id, count(*) FROM nodes GROUP BY 1 HAVING count(*) > 3;` -- if no cluster exceeds 3 nodes, reachable_on_fleet is 'no' and this drops to a documentation-only item.
- **Fix (combination):** Resolver: before picking, count primaries with a non-empty gated-follower list; if >1, this is dual-acking -- the one state where the quorum-sync invariant the whole ADR leans on is actually broken -- so force `Confidence::Refuse` (a new finding, or reuse of the sanity-gate path) instead of `Both`. Separately, add the row to the matrix and state that §7's '3-node proof' is conditional on exactly-one-replica, not a property of the design. If the 1+2 assumption is meant to be load-bearing, enforce it on the split-brain path too (mirror analyze.rs:323 before analyze.rs:318).
- **Alternatives rejected:** Writer-only fix (stop hardcoding 'quorum unsatisfied' and read info.findings instead): necessary but insufficient -- the verdict itself, not just its parenthetical, is unsafe when both branches hold acked writes.

### [high | unverifiable] ADR lines 43
- **Claim:** C-c: "in this 3-node cluster db002's only candidate acker is db003, and db003 is observably acking db001 on TL=N, so db002 provably client-acked nothing on TL=N+1 (sync_commit=on). Its fork is empty -> fencing db002 is safe and the verdict is confident."
- **Justification:** The observation is instantaneous; the claim quantifies over the whole interval since db002's promotion. A db003 that acked db002 on TL=N+1 earlier and was then rebuilt from db001 (this fleet's stated remediation is tear-down-and-basebackup) satisfies the observation exactly while db002's fork holds acked writes. Nothing in a single scan distinguishes the two histories.
- **Code:** `No code corresponds -- this is a proof stated in the ADR and relied on by §7:242 ("we can *prove* db002's fork is empty ... no Refuse") and docs/concepts/split-brain.md:70. The resolver captures no history: src/v2/analyze/split_brain.rs:281-315 reads only current `wal_receiver`/`pg_stat_replication` state.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** On db003 during a real occurrence: `SELECT receive_start_tli, receive_start_lsn, conninfo FROM pg_stat_wal_receiver;` plus db003's postmaster start time / basebackup marker (repmgr clone timestamp or backup_label). On db002: `pg_waldump` from the fork LSN forward, looking for COMMIT records past X.
- **Fix (adr-text):** Downgrade 'provably' to a stated assumption with its precondition named: the proof holds only if db003's allegiance has been continuous since the fork. Say what would establish that (a second scan, or db003's `pg_stat_wal_receiver.receive_start_tli`/`receive_start_lsn` showing a start point at or before the fork on TL=N, which today is captured but unused). The 'sync_commit=on' parenthetical is also wrong for this fleet -- the fixture shows `remote_apply`, which is strictly stronger, so the inference survives; fix the wording to say 'on/remote_flush/remote_apply per the §2 gate' rather than pinning it to `on`.
- **Alternatives rejected:** Leaving it as-is because §7 already flags C-g as the risky row: rejected -- C-c is the row that explicitly authorises a *confident* fence of db002, so an unqualified 'provably' here is what turns into the destructive instruction.

### [high | diverges] ADR lines 37, 43
- **Claim:** C-c / row-preamble: db001's and db002's commit ability is fully determined by whether db003 is flushing for them, because `synchronous_standby_names = 'ANY 1 (A, B)'` holds on both primaries.
- **Justification:** If `synchronous_standby_names` is cleared on db002 (the standard operator move to unblock a quorum-blocked new primary) it commits asynchronously and can ack writes with no standby -- the matrix's entire 'db001 committing? / fork is empty' column collapses -- and the code emits nothing at all in that state, so the operator gets no hint.
- **Code:** `src/v2/analyze/split_brain.rs:650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else {` followed by `continue;` -- and src/v2/analyze/sync_standby_names.rs:26 `if s.is_empty() { return None; }`. Empty SSN therefore produces no `PrimaryQuorumUnsatisfied` and no other finding.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** `SELECT name, setting, source, pending_restart FROM pg_settings WHERE name = 'synchronous_standby_names';` on both candidate primaries during an incident, plus the same for `synchronous_commit` -- check `source` to catch an ALTER SYSTEM / session override applied mid-incident.
- **Fix (combination):** Resolver: distinguish 'SSN unparseable' (keep today's defensive silence) from 'SSN empty on a candidate primary'. Empty SSN means that primary can ack writes unilaterally, which voids the no-divergence argument, so it belongs with the §2 sanity gates (Refuse) or at minimum a dedicated finding. ADR: state in the matrix preamble that every row assumes a non-empty `ANY 1 (...)` on both candidate primaries, and add the empty-SSN state as an out-of-matrix Refuse.
- **Alternatives rejected:** Treating empty SSN as method=ANY count=infinity (the §4 rule's defensive default for unparseable input): rejected -- that emits `PrimaryQuorumUnsatisfied` for a primary that is in fact freely committing, which is the opposite of the truth.

### [high | unverifiable] ADR lines 47
- **Claim:** C-g: the wedged db003 "likely cannot establish a wal_receiver at all", therefore the resolver mis-picks `HigherTimeline` because "at scan time no replica is live-following anyone".
- **Justification:** The premise is explicitly unconfirmed (no captured run; the concepts doc says the receiver may be "empty or only transiently populated" and sources the log signature from a 2013 upstream bug report). If the wedged replica is caught with a transient row naming db002, the gate can pass and the verdict becomes `Both` + `BidirectionalFlushingConfirmed` -- a *more* confident rendering of the same destructive action than the row predicts.
- **Code:** `src/v2/analyze/split_brain.rs:272 `let Some(wr) = &r_health.wal_receiver else { continue; };` (the no-receiver path the row assumes) vs :317-333, which on a transiently-populated fresh row records the follower and pushes `BidirectionalFlushingConfirmed`. docs/concepts/split-brain.md:95 "the precise `pg_stat_wal_receiver` contents in this state are likewise unconfirmed".`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** A capture of the wedged state: on db003, `SELECT * FROM pg_stat_wal_receiver;` sampled every 5 s for a minute plus the server-log tail; simultaneously on db002, `SELECT application_name, state, reply_time FROM pg_stat_replication;`. That is exactly §5's capture-first evidence; until it exists both C-g1 and C-g2 are hypotheses.
- **Fix (adr-text):** Split C-g into C-g1 (no `wal_receiver` -> `HigherTimeline`, as written) and C-g2 (transient/retrying receiver toward db002 that passes the freshness+status gate -> `Both`, BestEffort, corroboration asserted). C-g2 is strictly worse than C-g1 and any future verdict-flip design must cover it; recording only C-g1 will produce a fix that misses the harder half. Mark the whole row's premise as inferred-not-observed, consistent with §7.
- **Alternatives rejected:** Adding a 'retrying receiver' heuristic to the gate now (e.g. requiring `latest_end_time` to advance): rejected -- single-scan data cannot show advancement, and §7 already decided capture-first over guessing.

### [high | diverges] ADR lines 35, 39-47
- **Claim:** The matrix's state space is 'db003's wal_receiver' (present/absent, sender, status, freshness) -- i.e. db003 is always observed.
- **Justification:** A db003 that is unreachable or whose health-check query errors is dropped from the resolver's input entirely, producing exactly C-f's verdict and text ('no live replicas') from an absence of evidence rather than evidence of absence -- while the real state may be C-b (db003 alive and acking db001). No row covers it and no finding marks it.
- **Code:** `src/v2/scan.rs:121 `role: Role::Unknown,` (connection failure) and src/v2/scan/health_check_replica.rs:155 `role: Role::UnknownReplica,` (query failure); src/v2/cluster.rs:74 `self.nodes.iter().filter(|n| n.role.is_replica())` excludes both; src/v2/analyze.rs:317-321 returns the SplitBrain verdict before any unreachable-node accounting. Resulting text: src/v2/writer/build.rs:716 `... demote {} (TL={}, no live replicas)`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Rate of scan failures per node from existing scan output (any node landing in `Role::Unknown`/`UnknownReplica`). Also check whether §5's additions can fail on their own: `SELECT has_function_privilege('<scanner_role>','pg_control_system()','execute');` on a replica -- a revoked EXECUTE turns every replica into `UnknownReplica`.
- **Fix (combination):** Resolver: pass the count of non-Primary/non-Replica nodes into `resolve_split_brain` (they are already in `cluster.nodes`) and, when any exist, cap confidence at `Conflicting` or emit an 'incomplete evidence' finding -- a `HigherTimeline` pick derived from a replica nobody could reach must not read the same as one derived from a replica observed to have no receiver. ADR: add the row, and note that C-f's verdict is only sound when db003 was actually scanned.
- **Alternatives rejected:** Rendering it in the writer from `AnalyzedNode.errors`: rejected -- the confidence value, not just the prose, needs to change, and confidence is computed in the resolver.

### [high | diverges] ADR lines 33, 39-47
- **Claim:** The matrix's inventory fixes db001 on TL=N and db002 on TL=N+1; every row is a different-timeline case.
- **Justification:** Equal timelines between two primaries (independent double promotion from TL=N-1 -- both pick the same next TL) is a distinct resolver path with its own outcomes, is exercised by an existing integration test, and has no matrix row and no safety analysis; the fork-anchoring method the design depends on ('read X from the higher-TL primary's .history') is undefined when there is no higher TL.
- **Code:** `src/v2/analyze/split_brain.rs:361 `} else if timeline_info.primaries_with_highest_timeline.len() > 1 {` -> `resolve_with_equal_timelines` (:508-561), which returns `ReplicaFollowing` or `Indeterminate` with `true_primary` = `primaries_with_highest_timeline[0]` (:545), sort-order dependent. Writer: src/v2/writer/build.rs:719-722 `"SplitBrain: {} has quorum, demote {} (same TL)"`. Exercised by src/v2/analyze.rs:1256 `analyze_pipeline_wires_split_brain_into_cluster_verdict` (both primaries timeline 13).`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Compare `pg_control_checkpoint().timeline_id` and the `.history` tail on both candidate primaries during a real double-promotion; if both report TL=N with different switch LSNs for N-1->N, the same-TL fork is confirmed observable.
- **Fix (combination):** ADR: add an equal-timeline row (or an explicit out-of-scope statement and why), covering that (a) the two branches share a TL number but not content, (b) `pg_control_checkpoint().timeline_id` cannot distinguish which primary a replica is on, so the '3-node proof' loses its 'a replica is on one timeline at a time' step, and (c) `Indeterminate` still populates `true_primary`/`stale_primaries`. Resolver/writer: for `Indeterminate` the short string correctly says 'cannot determine', but `stale_primaries` still names a node in details_json -- decide whether that is intended.
- **Alternatives rejected:** Treating equal TLs as impossible and asserting: rejected -- `resolve_with_equal_timelines` exists precisely because it is not, and §3:125 already acknowledges the state ('equal timelines with no replica evidence').

### [high | diverges] ADR lines 35, 37
- **Claim:** "The resolver is invoked only when >=2 primaries are observed ... that means db001 is a zombie primary alongside db002 ... The interesting variation is in db003's state." (implicitly: exactly two candidate primaries)
- **Justification:** db003 observed as a third primary is reachable and unmodelled: the replica set is then empty, §7's '3-node proof' premise ('exactly two candidate primaries and one replica') is silently false, and the operator-facing string names only one of the two stale primaries.
- **Code:** `src/v2/analyze.rs:317 `if primaries.len() > 1 {` (no upper bound); src/v2/analyze/split_brain.rs:486-490 collects *all* lower-TL primaries into `stale_primaries`, but src/v2/writer/build.rs:693 `let stale = info.stale_primaries.first().map_or("", String::as_str);` renders only the first.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** None for the writer half (readable from the code). For fleet reachability: count historical scans where a cluster reported >2 nodes with `pg_is_in_recovery() = false`.
- **Fix (combination):** Writer: join all `stale_primaries` into the short string rather than taking `.first()` -- under-reporting the demote list is operator-facing text that omits a node still accepting writes. ADR: state the two-candidate-primary precondition where the proof is used (matrix preamble and §7), since nothing in the code enforces it.
- **Alternatives rejected:** Refusing outright when >2 primaries: defensible, but a bigger behavioural change than the matrix gap warrants; naming all stale primaries plus documenting the precondition is the minimal correct step.

### [medium | diverges] ADR lines 46
- **Claim:** C-f: "replica-stuck condition surfaced separately (existing archive-failure mechanism)".
- **Justification:** In a split-brain cluster no other check runs at all -- `analyze()` returns immediately after setting the SplitBrain verdict -- and the archive check is primary-side `pg_stat_archiver` only, which cannot observe a replica blocked on `restore_command`.
- **Code:** `src/v2/analyze.rs:317-321 (`if primaries.len() > 1 { ... return AnalyzedCluster { cluster, verdict }; }`) precedes src/v2/analyze.rs:333 `check_archive(primary, &mut verdict);`; src/v2/analyze/checks.rs:23-48 reads `health.configuration.get("archive_mode")` and `health.archiver` from the *primary* only.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Delete the parenthetical or replace it with the truth: in the split-brain branch no per-node checks run, so a db003 stuck on `restore_command` is invisible in the verdict and appears only as 'no live replicas'. If the information is wanted, the fix is in the resolver (a finding for 'replica present, receiver absent') rather than resurrecting the archive check, which measures a different thing on a different node.
- **Alternatives rejected:** Running `check_archive` in the split-brain path: rejected -- it inspects the primary's archiver, so it still would not surface a replica's restore_command stall; it would add noise without covering the claim.

### [medium | self-contradictory] ADR lines 44, 45, 46
- **Claim:** Per-row "Verdict + findings" columns: C-d lists only `HigherTimeline` + `ReplicaWalReceiverStale`; C-f lists no findings; C-e lists only `Both` + `BidirectionalFlushingConfirmed`.
- **Justification:** §4's derivation rule mandates `PrimaryQuorumUnsatisfied` for every primary with observed < count, which in C-d and C-f is *both* primaries and in C-e is db001 -- C-a lists that same finding for the identical quorum state, so the column is internally inconsistent, and the omission hides that C-d/C-f actually land on `Confidence::Conflicting`, not BestEffort.
- **Code:** `src/v2/analyze/split_brain.rs:665 `if observed < count {` (emits for every such primary) and :404-409 -- `PrimaryQuorumUnsatisfied { primary, .. }` returns `Confidence::Conflicting` when `primary == true_primary`, combined via `.min()` at :388.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Make the findings column complete and add a Confidence column. Minimally: C-d and C-f gain `PrimaryQuorumUnsatisfied(db001)` + `PrimaryQuorumUnsatisfied(db002)` and Confidence=Conflicting; C-e gains `PrimaryQuorumUnsatisfied(db001)` and Confidence=BestEffort. Without the Confidence column the matrix cannot be read against §4.1's Refuse-overrides-resolution rule at all.
- **Alternatives rejected:** Stating once above the table that `PrimaryQuorumUnsatisfied` is implied wherever quorum is unmet: weaker, because the rows disagree with each other today and a reader cannot tell which omissions are deliberate.

### [medium | diverges] ADR lines 41-47
- **Claim:** The matrix's sender_host axis has two values, db001 and db002.
- **Justification:** A replica whose `sender_host` matches no candidate primary's `ip_address` (VIP, cascading source, second NIC, hostname-form conninfo) is dropped silently -- no follower, no finding of any kind -- and the verdict falls to `HigherTimeline`; in a C-b-shaped cluster that flips 'keep db001' into 'demote db001' with zero operator-visible reason.
- **Code:** `src/v2/analyze/split_brain.rs:281 `let replica_passes = wr.sender_host == primary.ip_address.to_string()` and the emission guard at :293 `if wr.sender_host == primary.ip_address.to_string() {` -- both false, so the `continue` at :299 leaves no trace. Sub-case: a fresh stream on a non-5432 port fails :282 `&& wr.sender_port == 5432` while the guard at :293 is true, so a healthy stream is reported as `ReplicaWalReceiverStale`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** `SELECT sender_host, sender_port, conninfo FROM pg_stat_wal_receiver;` on every replica, compared against the scanner's stored `ip_address` for each primary. The captured fixture shows a direct match (sender_host 127.1.12.151 = db001's ip_address, port 5432), so this is currently latent rather than active.
- **Fix (combination):** Resolver: after the primary loop, emit a finding for any replica whose `wal_receiver` is present and fresh but whose `sender_host` matched no candidate primary (name it and the host), so the evidence loss is visible; and do not label a port mismatch as `ReplicaWalReceiverStale` (it is a topology/config mismatch, not staleness). ADR: add the 'sender_host is not a candidate primary' state to the matrix -- §1 flags hostname-form conninfo as a known limitation, but the matrix never shows what the tool *outputs* in that state (silence).
- **Alternatives rejected:** Resolving sender_host through DNS in the resolver: out of scope per the ADR and adds a network dependency to analysis; naming the unmatched host in a finding gets the operator the same information.

### [medium | diverges] ADR lines 58
- **Claim:** "Cases the design does not attempt to disambiguate: scans that overlap an in-progress failover ... verdicts produced during such windows are capped at `BestEffort` and re-converge once `wal_sender_timeout` elapses."
- **Justification:** 'Capped at BestEffort' describes no behaviour: BestEffort is the maximum confidence in v1 (Verified is deliberately omitted, §3:121) and nothing detects an in-flight-failover window, so a transient-window verdict is indistinguishable from a stable-state one. The sentence reads as a mitigation for the design's stated blind spot but implements nothing.
- **Code:** `src/v2/analyze/split_brain.rs:73-77 `pub enum Confidence { Refuse, Conflicting, BestEffort }` (no Verified) and :389 `.unwrap_or(Confidence::BestEffort)` -- the default, not a cap.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Rewrite as what is true today: verdicts from an overlapping-failover window are *not* distinguishable from stable-state verdicts, and the mitigation is operator discipline (re-run after `wal_sender_timeout`), not a confidence cap. Re-instate the 'capped' language only when §6's two-pass check and a `Verified` level exist.
- **Alternatives rejected:** Adding a real cap now (e.g. downgrade to Conflicting when any freshness value is within one keepalive of the threshold): plausible but unrequested scope, and it needs the scan-start timestamp §5 mandates but the resolver does not take.

### [medium | self-contradictory] ADR lines 43
- **Claim:** C-c: `DivergentReplicaWal(db003, ...)` is "informational, not Refuse".
- **Justification:** §4 item 4 states the same finding "MUST set `Confidence::Refuse`" unconditionally, and the code implements that unconditional mapping; C-c is the one row where the ADR says the opposite. Dormant only because nothing emits the finding.
- **Code:** `src/v2/analyze/split_brain.rs:399 `| SplitBrainFinding::DivergentReplicaWal { .. } => Confidence::Refuse,` -- no per-case discrimination. Emission sites: none in production code (the variant appears only at :102 definition, :399, a test at :782, and build.rs:682).`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Reconcile the two statements explicitly: either §4.4 gains the C-c carve-out ('Refuse unless the replica's allegiance to the lower-TL primary is observable', matching §7:242-243), or C-c is rewritten to say the finding is suppressed rather than emitted-as-informational. As written, whoever implements the deferred detection hits a spec fork at exactly the point where over-caution (needless Refuse on the safe row) versus false confidence is decided.
- **Alternatives rejected:** Changing `determine_confidence_level` now to be conditional: rejected -- the finding is unreachable today, so this is a text conflict, and §7 explicitly defers the confidence mapping until designed from a captured occurrence.

### [medium | diverges] ADR lines 43
- **Claim:** C-c: "Its fork is empty -> fencing db002 is safe" (with §7:237 "committed nothing on its fork -- it is the empty branch").
- **Justification:** 'Empty of client-acked writes' is not 'empty'. Under sync replication an unacked commit is still written and locally committed; fencing/rebuilding db002 discards those records. The matrix states the stronger, false claim without the qualifier the remediation depends on.
- **Code:** `No code corresponds -- nothing inspects db002's WAL past the fork. docs/concepts/split-brain.md:76 "db002 was isolated and committed nothing on its fork ... it is the empty branch" carries the same overstatement.`
- **Reachable in code:** False | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** `pg_waldump` on db002 from the fork LSN X forward, counting COMMIT records -- shows directly whether an isolated higher-TL primary's fork contains committed-but-unacked transactions in a real occurrence.
- **Fix (adr-text):** Qualify to 'empty of client-acknowledged writes' in both the C-c cell and §7's remediation paragraph, and note that locally-committed-but-unacked transactions on db002's fork are discarded by the rebuild -- that is a real operator decision (salvage via pg_waldump/archive before teardown), not a no-op.
- **Alternatives rejected:** Leaving it, on the grounds that the ADR scopes itself to acknowledged writes: rejected -- the destructive instruction ('discard/rebuild the higher TL') is justified by the word 'empty', so the qualifier belongs next to the instruction.

### [low | stale] ADR lines 49-54
- **Claim:** "The design's correctness rests on three facts visible in the matrix:" followed by four numbered items.
- **Justification:** Item 4 (C-g) was added by the 2026-06-07 revision that introduced the row (header, line 7) and the lead-in count was left at three. Worse than a count typo: items 1-3 are supporting facts, item 4 is a known *unfixed* hole, so filing it under 'the design's correctness rests on' reads as if C-g is handled when the same item says the resolver picks the wrong primary.
- **Code:** ``
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Change the lead-in to 'three facts ... and one known gap', keep 1-3 under the correctness heading, and move item 4 under its own heading ('The gap the design does not close') pointing at §7. Purely editorial, but this is the one sentence a reader skims to decide whether C-g is covered.
- **Alternatives rejected:** Just changing 'three' to 'four': rejected -- it keeps an unfixed hole listed as a load-bearing correctness fact.

### [info | implemented] ADR lines 45, 56
- **Claim:** C-e: "`db001`'s stale `pg_stat_replication` row is filtered because the replica-side match fails first" / "If the replica-side gate fails for primary X, no amount of primary-side state on X can rescue the match."
- **Justification:** Verified against the control flow: the replica-side predicate is evaluated first and `continue`s before `p_health.replication` is ever read, and the stale-finding guard also suppresses a finding for db001 because db003's `sender_host` names db002.
- **Code:** `src/v2/analyze/split_brain.rs:281-300 -- `let replica_passes = wr.sender_host == primary.ip_address.to_string() ...`; `if !replica_passes { if wr.sender_host == primary.ip_address.to_string() { ...ReplicaWalReceiverStale... } continue; }` -- the primary-side `find` at :305 is unreachable for the db001/db003 pair.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No fix. One wording caveat worth folding into the C-e row: 'the replica's wal_receiver is authoritative' is true for *rejection* only -- §1 still lets the corroborating primary side veto an accepted match (`PrimaryDoesNotSeeReplica`), which is the mechanism behind the critical finding above.

### [info | implemented] ADR lines 47
- **Claim:** C-g: "Resolver mis-picks `HigherTimeline` -> `db002`, because at scan time no replica is live-following anyone. Acting on that demotes db001 and destroys acknowledged transactions."
- **Justification:** Accurate description of today's code for the no-wal_receiver premise: the replica is skipped before any gate, the following map is empty, the different-timelines resolver falls through to `HigherTimeline`, the writer prints a demote instruction, and nothing sets Refuse.
- **Code:** `src/v2/analyze/split_brain.rs:272 `let Some(wr) = &r_health.wal_receiver else { continue; };`; :492-501 `resolution: SplitBrainResolution::HigherTimeline { ... }`; src/v2/writer/build.rs:716 `"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)"`. Confidence lands at `Conflicting` at worst (PrimaryQuorumUnsatisfied on the elected db002, :404-409), so `format_refuse` is never reached.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** A captured C-g run (same capture as the C-g premise finding) -- without it the fleet reachability of the state is unknown, though the code path is confirmed.
- **Fix (none):** No fix here -- the row correctly describes the code. Residual: the row's stated safeguard ('`DivergentReplicaWal(db003) -> Refuse` must override the pick') is deferred everywhere else in the ADR, so the row should read 'must, once §7's detection lands' to avoid being taken as an implemented guarantee.


## ADR-002 sections 6 (lines 227-230), 7 (231-248), Out of scope (249-260), Consequences (261-268)

### [critical | diverges] ADR lines 241-243
- **Claim:** "db002's quorum can be satisfied *only* by db003 ... So 'is db002's fork empty of acked writes?' reduces to 'is db003 acking db002?'" -- the 3-node proof that licenses a confident (non-Refuse) lower-TL verdict.
- **Justification:** The reduction silently assumes a non-empty synchronous_standby_names on db002; with SSN empty/unset, synchronous_commit=on degrades to local flush and an isolated db002 commits and acks freely, so its fork is NOT empty. Neither the ADR nor the code guards this.
- **Code:** `src/v2/analyze/split_brain.rs:650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else {` -- empty/unparseable SSN `continue`s, so no PrimaryQuorumUnsatisfied is emitted and confidence stays BestEffort; src/v2/writer/build.rs:709 `"SplitBrain: {} has quorum (lower TL={}), fence {} (TL={}, quorum-blocked)"` prints "quorum-blocked" unconditionally, never inspecting findings.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** On each candidate primary, immediately after a repmgr promotion: SELECT name, setting, source, sourcefile, sourceline FROM pg_settings WHERE name = 'synchronous_standby_names'; The only captured value is the steady-state primary's (tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json, db001 = 'ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )'); nothing shows what a freshly promoted node carries.
- **Fix (combination):** In emit_quorum_findings, distinguish "SSN absent or empty" from "SSN unparseable". For an empty SSN on a candidate primary emit a new sanity-gate finding (e.g. SyncQuorumDisabled { primary }), map it to Confidence::Refuse in determine_confidence_level (split_brain.rs:396-414), and give it an arm in writer/build.rs format_refuse. Separately amend ADR-002 line 241 to state the precondition: the reduction holds only while every candidate primary has a non-empty SSN with count >= 1.
- **Alternatives rejected:** Patching only format_resolution (drop the "quorum-blocked" parenthetical when no PrimaryQuorumUnsatisfied is present) is worse: it hides the hole instead of naming it and still leaves the verdict at BestEffort after the safety argument has failed.

### [high | diverges] ADR lines 266
- **Claim:** "Adding `system_identifier` and timeline-history collection makes `HEALTH_CHECK_PRIMARY_QUERY` modestly larger; deployment must have `pg_read_server_files` granted (already true in production)."
- **Justification:** The stated consequence is query size. The real consequence of a missing grant is that the entire single-statement primary health check errors, the node becomes UnknownPrimary, primaries() drops it, and `primaries.len() > 1` never fires -- split-brain detection is silently disabled on exactly the promoted (TL>1) clusters this ADR exists for.
- **Code:** `src/v2/scan/health_check_primary.rs:268 `role: Role::UnknownPrimary,` (any Err from the one jsonb_build_object query); src/v2/scan.rs:333-335 `matches!(self, Role::Primary { .. })` excludes UnknownPrimary from primaries(); src/v2/analyze.rs:316 `if primaries.len() > 1 {`.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** As the scanner role on every node: SELECT current_user, pg_has_role(current_user,'pg_read_server_files','member') AS member, has_function_privilege(current_user,'pg_read_file(text,bigint,bigint,boolean)','execute') AS can_exec; Also settle whether the ACL error is raised at parse analysis (fires even on TL=1, where the CASE short-circuits) or at execution (fires only on TL>1) by running the exact HEALTH_CHECK_PRIMARY_QUERY as a role lacking the grant.
- **Fix (combination):** Scanner: move the timeline_history read out of the monolithic jsonb_build_object into its own query whose failure degrades to None, so an ACL error costs one field instead of the node's role. ADR: replace the "modestly larger" wording with the real blast radius -- a failed primary health check demotes the node to UnknownPrimary and suppresses split-brain resolution entirely.
- **Alternatives rejected:** Adding #[serde(default)] to timeline_history does not help: the failure is a server-side ERROR on the whole statement, not a missing JSON key.

### [high | diverges] ADR lines 268
- **Claim:** "`Indeterminate` is preserved as an evidence-state outcome; no single-pass tiebreaker is added."
- **Justification:** The equal-timeline / no-evidence branch does add a tiebreaker -- it names primaries_with_highest_timeline[0] as true_primary and the rest as stale_primaries -- and the report's primary column renders that pick as "X vs Y" for every split-brain resolution, Indeterminate included, while reason.short says "cannot determine true primary".
- **Code:** `src/v2/analyze/split_brain.rs:544-556 `// Cannot determine - mark first as "true" but resolution is indeterminate` / `let first = timeline_info.primaries_with_highest_timeline[0].0;`; src/v2/writer/build.rs:199-206 builds PrimaryView::SplitBrain with no per-resolution branch; src/v2/writer/view.rs:76 `format!("{} vs {}", true_primary.render(mode), stale_strs.join(","))`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** A captured equal-timeline two-primary scan. Note this is not a corner case on this fleet: build_replica_following_map compares conn.application_name (dev_pg_app001_db003) to replica.node_name (dev-pg-app001-db003.sto3.example.com), so the primary-side gate never matches on real data and replicas_following is always empty -- making Indeterminate the default equal-TL outcome. Both name forms are evidenced in tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json.
- **Fix (writer):** In writer/build.rs build_split_brain_view (around line 175-207), branch on info.resolution: for SplitBrainResolution::Indeterminate emit a PrimaryView that lists the candidates without a winner/loser ordering (e.g. a new PrimaryView::SplitBrainUndecided(Vec<NodeView>) rendered as "db001?,db002?"), so the table never implies a demote target the resolver refused to name.
- **Alternatives rejected:** Making SplitBrainInfo.true_primary an Option is more correct but is a breaking type change rippling through classify.rs, capture.rs and the CSV schema; the writer fix removes the operator-facing hazard with a change confined to one match.

### [high | diverges] ADR lines 241
- **Claim:** "db002's quorum can be satisfied *only* by db003 (a peer primary is not its standby; a primary is not its own)."
- **Justification:** Postgres satisfies synchronous_standby_names by matching the application_name of any connected walsender, including a pg_basebackup / repmgr-clone stream. Section 1 deliberately filters state=backup out of the gate, so the tool can report "no observed acker" while postgres is in fact acking through a connection the gate discarded -- and this fleet's remediation policy is exactly "tear down and re-basebackup", so such connections are created on purpose.
- **Code:** `src/v2/analyze/split_brain.rs:304 `// - \`state=backup\`: pg_basebackup clients, not replication consumers.` and the following filter at :305-315 (`matches!(conn.state, ReplicationState::Streaming | ReplicationState::Catchup)`), which is also the input to emit_quorum_findings' `observed`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** During a repmgr standby clone of db003, on the source primary: SELECT application_name, state, sync_state, backend_start FROM pg_stat_replication; plus grep the generated primary_conninfo / repmgr.conf for the application_name the clone uses. If it equals the SSN member name (dev_pg_app001_db003) the proof's "only db003" premise is false during every rebuild window.
- **Fix (adr-text):** Amend ADR-002 line 241 to scope the proof: db002's quorum can be satisfied by any walsender whose application_name is an SSN member, which includes basebackup/clone streams; therefore "no gated follower observed" does not imply "provably no acked writes", and the confident verdict at line 242 additionally requires that no pg_stat_replication row on db002 carries an SSN member name in ANY state (not just streaming/catchup). Consider having the resolver check that broader condition before allowing a non-Refuse lower-TL verdict.
- **Alternatives rejected:** Loosening the gate to count state=backup as a follower is wrong: a basebackup is not a durable replica and must not endorse a primary as "true". The asymmetry needs to live in the proof/verdict, not in the follower map.

### [high | diverges] ADR lines 256
- **Claim:** "Topologies with >2 replicas. Resolver assumes 1+2." (out of scope)
- **Justification:** The exclusion is declared but not enforced. analyze() runs the split-brain resolver on any primary count >= 2 and any replica count, and returns before the >2-replica UnexpectedTopology check ever executes -- so that check is unreachable whenever split-brain fires. With two replicas, section 7's proof ("exactly two candidate primaries and one replica") fails: both primaries can have an acker and genuinely diverge, yet the resolver still emits a BestEffort single-winner verdict with a demote instruction.
- **Code:** `src/v2/analyze.rs:316-320 `if primaries.len() > 1 { let split_brain_info = resolve_split_brain(&primaries, &replicas); ... return AnalyzedCluster { cluster, verdict }; }` followed at :322-327 by the now-dead `if replicas.len() > 2 { ... UnexpectedTopology ... }`; src/v2/analyze/split_brain.rs:136-139 asserts only `primaries.len() >= 2`.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Node counts per cluster from the inventory API the scanner reads (src/database_portal.rs:52 `get_config().database_portal_url`): curl -s "$DATABASE_PORTAL_URL" | jq '[.[] | {cluster: .name, n: (.nodes|length)}] | map(select(.n != 3))' -- if that is empty, the 1+2 assumption is evidenced and this drops to low.
- **Fix (resolver):** Guard the assumption where it is relied on: in resolve_split_brain, if replicas.len() > 1 (i.e. more than the single replica the 3-node proof allows), force Confidence::Refuse with a finding naming the unsupported topology, rather than emitting a single-winner BestEffort verdict. Alternatively move the replicas.len() > 2 check above the split-brain branch in analyze.rs -- but that silences a Critical split-brain into an Unknown-tier verdict, which is worse.
- **Alternatives rejected:** Reordering the two checks in analyze.rs demotes a real split-brain from Critical (rank 27) to UnexpectedTopology (Unknown tier, rank 1) -- it would hide the incident rather than qualify the verdict.

### [high | diverges] ADR lines 263
- **Claim:** PrimaryQuorumUnsatisfied on the *elected* primary yields Confidence::Conflicting, but the short string still asserts that primary has quorum.
- **Justification:** Consequences claims verdicts are "now explicitly BestEffort", but confidence is only visible in details_json: format_resolution branches solely on Refuse-vs-not, so a Conflicting verdict renders identically to a BestEffort one. In the specific Conflicting case the resolver defines, the text directly contradicts the finding it was derived from.
- **Code:** `src/v2/analyze/split_brain.rs:404-408 `if primary == true_primary { Confidence::Conflicting }`; src/v2/writer/build.rs:655-658 `let short = if matches!(info.confidence, Confidence::Refuse) { format_refuse(info) } else { format_resolution(info) };` -- so with confidence Conflicting the output is still e.g. `SplitBrain: db001 has quorum (lower TL=11), fence db002 ...` for a db001 the resolver just flagged as quorum-unsatisfied. Test src/v2/analyze/split_brain.rs:1536 constructs the state.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** A captured two-primary scan. Reachability is amplified by the SSN-member vs node_name mismatch (split_brain.rs:653-656 intersects application-name-form members against FQDN node names), which makes observed = 0 for every primary with a parseable SSN on real fleet data -- both name forms are evidenced in tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json.
- **Fix (writer):** In writer/build.rs split_brain_reason (line 654), prefix the short string when confidence is Conflicting (e.g. `CONFLICTING/` plus the contradicting finding), or at minimum suppress the "has quorum" phrasing for the elected primary when a PrimaryQuorumUnsatisfied finding names it.
- **Alternatives rejected:** Leaving it to details_json is not sufficient: the ADR's own section 4 premise is that reason.short is what an operator triages from at 3 AM.

### [medium | self-contradictory] ADR lines 7, 245
- **Claim:** Status note: "a conservative `Refuse`-only floor is shippable today (§7)" vs section 7: "which is why no conservative 'Refuse-only floor' is shipped in the interim."
- **Justification:** Line 7 cites section 7 as authority for the floor being available; section 7 rejects it on the merits ("Shipping the trigger now would add over-caution to safe cases and false confidence to the dangerous one"), not as a scheduling choice. Line 245 is the correct one and matches the code -- nothing constructs DivergentReplicaWal and no floor exists.
- **Code:** `src/v2/analyze/split_brain.rs:102-108 defines DivergentReplicaWal; the only non-test references are the match arms at split_brain.rs:399 and src/v2/writer/build.rs:682. No construction site outside the rstest case at split_brain.rs:782.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Rewrite the tail of the 2026-06-07 status note (line 7) to match section 7's conclusion: "...the precise trigger and the verdict-flip are deferred until designed from real data; a Refuse-only floor built on the wal_receiver trigger is explicitly rejected, not merely postponed, because it would fire on the safe case (C-c) and miss the dangerous one (C-g)."
- **Alternatives rejected:** Softening section 7 instead would be wrong -- its anti-correlation argument is the substantive one and is corroborated by docs/concepts/split-brain.md:87.

### [medium | self-contradictory] ADR lines 156, 159-161, 178
- **Claim:** "`DivergentReplicaWal`, when present, MUST surface inline in the SplitBrain short string ... Until then, surface evidence + `Refuse`, not an action. Render the raw facts inline -- e.g. `REFUSE/SplitBrain: divergent committed WAL -- db003 flushed past TL=N fork @ <lsn>; ...`" versus line 156/178 "rendering is deferred ... the carve-out is dormant".
- **Justification:** Line 161's "Until then" prose prescribes a concrete present-tense rendering while lines 156 and 178 say rendering is deferred. The code trio makes the dormant state a trap rather than a no-op: determine_confidence_level maps DivergentReplicaWal to Refuse, so the first person to wire detection immediately routes into format_refuse, which maps that variant to None and prints the bare literal "sanity gate failed" -- naming no gate, in the one state where acked writes may exist only on the lower TL.
- **Code:** `src/v2/analyze/split_brain.rs:399 `| SplitBrainFinding::DivergentReplicaWal { .. } => Confidence::Refuse,`; src/v2/writer/build.rs:682 `| SplitBrainFinding::DivergentReplicaWal { .. } => None,` and :684 `.unwrap_or_else(|| "sanity gate failed".to_owned());`.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (combination):** Either (a) add the format_refuse arm now, rendering exactly the evidence string line 161 already specifies from the variant's own fields (replica_node, replica_flushed_lsn, fork_tli, fork_lsn) -- the variant carries everything needed, so this is a self-contained ~5-line change that removes the trap; or (b) if the deferral is to stay total, delete the mandated example string from line 161 and drop the DivergentReplicaWal arm from determine_confidence_level so the variant is inert rather than half-wired. Option (a) is preferable: it makes lines 156/159-161/178 consistent and leaves the dormant path safe.
- **Alternatives rejected:** Leaving the confidence mapping in place while the writer renders nothing is the current state and is the worst of the three: it guarantees an information-free Refuse the moment detection lands.

### [medium | self-contradictory] ADR lines 258
- **Claim:** "in this cluster's topology each primary's SSN structurally excludes itself -- so the naive string compare fires on every split-brain even when the policy is identical (`ANY 1 (db002, db003)` on db001 vs. `ANY 1 (db001, db003)` on db002 is the same policy, different strings)."
- **Justification:** The ADR's own case matrix says the opposite: line 37 states db002 runs "the same setting" and that "A = itself can't be its own standby", i.e. db002's SSN member set includes db002. The captured fixture agrees with line 37's model -- db001's SSN is 'ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )', a static two-standby list, and no per-node SSN variation is observable because the replica health check does not capture synchronous_standby_names at all. If the string is cluster-wide identical, a naive compare would NOT fire on identical policy, so the stated cost/benefit justifying the exclusion collapses.
- **Code:** `tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json (db001 configuration.synchronous_standby_names = 'ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )'); src/v2/scan/health_check_replica.rs:60-115 -- HEALTH_CHECK_REPLICA_QUERY captures primary_conninfo, primary_slot_name, hot_standby, recovery_target_timeline but never synchronous_standby_names, so no second SSN sample exists anywhere.`
- **Reachable in code:** False | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** One query settles it: on db002 and db003 (currently replicas), SELECT setting, source, sourcefile FROM pg_settings WHERE name='synchronous_standby_names'; If all three nodes carry the identical string, line 258's premise is false and line 37 is right.
- **Fix (adr-text):** Rewrite the SyncStandbyNamesDiverged bullet's justification. Either drop the self-exclusion premise and keep the exclusion on the honest ground ("informational-only finding, no consumer, no observed drift incident"), or -- if the fixture check confirms an identical cluster-wide string -- note that a plain string compare would in fact be correct and cheap, and re-evaluate. Also add synchronous_standby_names to HEALTH_CHECK_REPLICA_QUERY so the premise becomes checkable at all.
- **Alternatives rejected:** Implementing normalization now is unwarranted -- the bullet's conclusion (do not emit) may well be right; it is only the reasoning that is falsified, and reasoning is what a future implementer will reuse.

### [medium | unverifiable] ADR lines 235
- **Claim:** "under `synchronous_commit = on`, the lower-TL primary does not ack a client commit until the standby has flushed it (weakened `synchronous_commit` is a separate Refuse gate, §2). So the finding detects committed-write divergence." -- with docs/concepts/split-brain.md:44 stating "Past that gate, the inference holds."
- **Justification:** synchronous_commit is a USERSET GUC. The gate reads only the cluster-level pg_settings value; ALTER ROLE / ALTER DATABASE SET and per-session or per-transaction SET are invisible to it. A primary reporting remote_apply can still be acking writes locally-only for some sessions, which breaks the flushed-past-fork => acknowledged inference in both directions. The concepts doc's flat "past that gate, the inference holds" is an overclaim about a state nobody has checked.
- **Code:** `src/v2/analyze/split_brain.rs:285-299 reads `h.configuration.get("synchronous_commit")` (the pg_settings value) and compares against WEAKENED_SYNCHRONOUS_COMMIT (split_brain.rs:13); no per-role/per-database override is consulted.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** On each candidate primary: SELECT r.rolname, d.datname, s.setconfig FROM pg_db_role_setting s LEFT JOIN pg_roles r ON r.oid = s.setrole LEFT JOIN pg_database d ON d.oid = s.setdatabase WHERE array_to_string(s.setconfig,',') ILIKE '%synchronous_commit%'; That settles role/database overrides. Per-session SET is not observable post hoc at all -- if it is a real risk here, it needs log_statement or an application-code audit, and the ADR should say so.
- **Fix (combination):** Add the pg_db_role_setting lookup to HEALTH_CHECK_PRIMARY_QUERY and extend the section 2 gate to Refuse when any role/database override weakens synchronous_commit. In docs/concepts/split-brain.md:44, replace "Past that gate, the inference holds" with "Past that gate the inference holds for sessions that use the cluster default; per-role, per-database and per-session overrides are not observed by the scanner."
- **Alternatives rejected:** Documenting only, without the query, leaves the strongest safety inference in the design resting on an unchecked assumption.

### [medium | diverges] ADR lines 257
- **Claim:** "Hostname-form `primary_conninfo`. Current production uses IPs; flag as known limitation." (out of scope)
- **Justification:** The exclusion is real but its stated mitigation -- flagging the limitation -- exists only in ADR prose. In code, a hostname-form sender_host fails the equality compare, and because the ReplicaWalReceiverStale guard uses the same failing compare, the replica disappears from the follower map with zero findings. The verdict then falls through to HigherTimeline at BestEffort and prints "demote X (no live replicas)" -- the C-g failure shape, produced by a config difference, with no operator signal. A test locks the silence in.
- **Code:** `src/v2/analyze/split_brain.rs:281 `let replica_passes = wr.sender_host == primary.ip_address.to_string()` and :291 the identical guard before emitting ReplicaWalReceiverStale; test `gate_silent_when_sender_host_differs_no_stale_finding` at split_brain.rs:1391; src/v2/writer/build.rs:716 `"... demote {} (TL={}, no live replicas)"`. Nothing in src/ contains a hostname/known-limitation notice (grep for "known limitation" returns nothing).`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Across a full scan's replica captures, check every configuration.primary_conninfo for a non-IP host: the current fixture shows host=127.1.12.151 on both replicas. Equivalently on each replica: SELECT setting FROM pg_settings WHERE name='primary_conninfo'; and confirm the host= token parses as an inet.
- **Fix (resolver):** In build_replica_following_map, when wr.sender_host fails to parse as an IP address, emit a finding (e.g. ReplicaSenderHostNotIp { replica, sender_host }) mapped to Confidence::Conflicting, so the known limitation is visible in the verdict rather than only in the ADR. This is the minimal change that turns a silent miss into a flagged one without attempting hostname resolution.
- **Alternatives rejected:** Resolving hostnames in the analyzer would add DNS I/O to a pure function and is explicitly out of scope; suppressing the HigherTimeline verdict entirely would over-fire on the normal case where a replica simply follows the other primary.

### [medium | stale] ADR lines 247
- **Claim:** "Collect db003's timeline and applied LSN from the **control file**, independent of `wal_receiver` (§5), so the wedged state becomes observable."
- **Justification:** Section 5's 2026-09-10 revision explicitly corrects this shorthand (line 219: the LSNs "are read from shared memory -- they are zeroed by a postmaster restart and do not survive one"), but scopes the correction to "this section". Line 247 was left standing with the uncorrected claim, and it matters: pg_last_wal_receive_lsn -- which line 211 designates "the ack-relevant one" -- returns NULL on a wedged replica that has been restarted, because its walreceiver never succeeds in the new postmaster's life. That is a plausible operator response to the FATAL loop described in docs/concepts/split-brain.md:89-95. Only pg_last_wal_replay_lsn (set by startup recovery over local WAL) and the genuine control-file timeline_id survive, so "becomes observable" is true for the applied position and false for the received one.
- **Code:** `src/v2/scan/health_check_replica.rs:63 `'timeline_id', (SELECT timeline_id FROM pg_control_checkpoint()),` (genuinely control-file) versus :88-89 `'last_wal_replay_lsn', pg_last_wal_replay_lsn()::text, 'last_wal_receive_lsn', pg_last_wal_receive_lsn()::text` (shared memory).`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Restart a standby whose primary_conninfo points at a timeline it cannot follow, then: SELECT pg_is_in_recovery(), (SELECT timeline_id FROM pg_control_checkpoint()), pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn(), (SELECT count(*) FROM pg_stat_wal_receiver); This is the C-g capture section 7 is waiting for and it settles which of the two LSNs survives.
- **Fix (adr-text):** Amend line 247 to name the sources precisely -- control-file timeline_id plus the shared-memory applied LSN -- and to state that the received LSN is lost across a postmaster restart, so detection must key off (control timeline_id == lower TL) AND (pg_last_wal_replay_lsn > fork LSN), never on the received position alone.
- **Alternatives rejected:** Widening section 5 line 219's disclaimer to cover the whole ADR is weaker: line 247 is where the detection design is specified, and a future implementer reading only section 7 would pick the fragile field.

### [medium | self-contradictory] ADR lines 43, 247
- **Claim:** Matrix row C-c: "`DivergentReplicaWal(db003, ...)` is **informational, not Refuse**" versus section 7: "The `DivergentReplicaWal` variant already maps to `Confidence::Refuse` in `determine_confidence_level`".
- **Justification:** Both statements are current text. The moment any detection emits the finding in the C-c shape (which the original trigger did -- line 245 says so), the confidence mapping flips a verdict the matrix declares confident into Refuse. Section 7 acknowledges the effect obliquely ("would add over-caution to safe cases") but the C-c row is not annotated, so the matrix reads as a spec the code cannot satisfy.
- **Code:** `src/v2/analyze/split_brain.rs:399 `| SplitBrainFinding::DivergentReplicaWal { .. } => Confidence::Refuse,` -- unconditional, with no informational path; the rstest case at split_brain.rs:781-790 locks it in.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Annotate the C-c row with the 2026-06-07 revision: since DivergentReplicaWal is unconditionally Refuse-mapped, the C-c "informational" reading requires either a separate informational variant or a condition on the mapping (e.g. not Refuse when a gated follower is observably acking the lower-TL primary). Say which, so the follow-up does not have to rediscover the conflict.
- **Alternatives rejected:** Making determine_confidence_level conditional now is premature -- section 7 defers the whole design pending a real capture; the fix here is to record the constraint, not to build it.

### [low | stale] ADR lines 233
- **Claim:** "`DivergentReplicaWal { replica_node, replica_received_tli, replica_flushed_lsn, fork_tli, fork_lsn }` stays a defined finding variant, but its detection, confidence handling, and remediation are deferred."
- **Justification:** Two problems. (a) "confidence handling ... deferred" contradicts line 247 of the same section, which says the Refuse mapping is already in place -- and the code confirms line 247. (b) The retained field names encode the rejected wal_receiver-based source: section 7 and the plan both say the position must come from the control-file/applied LSN, not wr.flushed_lsn, and the timeline must be the control-file timeline_id, not received_tli. A follow-up filling replica_flushed_lsn from last_wal_receive_lsn inherits a name/semantics mismatch in the one finding where the distinction is load-bearing.
- **Code:** `src/v2/analyze/split_brain.rs:102-108 (variant with replica_received_tli / replica_flushed_lsn); docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:72 "Source the replica position from the control-file LSN (this commit's new fields), not `wr.flushed_lsn`".`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (combination):** Drop "confidence handling" from line 233's deferral list (it is not deferred; line 247 is correct). Rename the variant's fields to match the corrected source -- replica_control_tli and replica_applied_lsn -- or add a doc comment on split_brain.rs:102 stating that the fields must be populated from control-file timeline_id and pg_last_wal_replay_lsn, never from wal_receiver.
- **Alternatives rejected:** Leaving the names alone and relying on the ADR is what created the trap in the first place; the variant is the artifact the implementer will actually read.

### [low | diverges] ADR lines 267
- **Claim:** "Existing tests assert on `SplitBrainInfo` literals (six tests in `split_brain.rs`, one in `analyze.rs`); each needs the new `confidence` and `findings` fields."
- **Justification:** Counted at the pre-ADR revision (jj 37ace1f033a7) and today: five tests in split_brain.rs, not six; one in analyze.rs; and one unmentioned in classify.rs. The total (seven) is right, so the classify.rs test appears to have been attributed to split_brain.rs. All seven do carry the new fields today, so the substantive part of the bullet is satisfied.
- **Code:** `Today: src/v2/analyze/split_brain.rs:999, 1024, 1054, 1082, 1132 (five tests: higher_timeline_wins_when_no_replica_evidence, timeline_and_replica_evidence_agree, replica_following_lower_timeline_overrides, equal_timelines_resolved_by_replica_following, equal_timelines_no_replica_evidence_is_indeterminate); src/v2/analyze.rs:1313 (analyze_pipeline_wires_split_brain_into_cluster_verdict); src/v2/analyze/classify.rs:153 (cluster_verdict_split_brain_to_reason). Pre-ADR the same seven sites existed at 37ace1f033a7 split_brain.rs:379/402/428/452/498, analyze.rs:1250, classify.rs:153.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Change line 267 to "five tests in split_brain.rs, one in analyze.rs, one in classify.rs".
- **Alternatives rejected:** Not worth touching the tests; the count is the error.

### [low | diverges] ADR lines 229
- **Claim:** "If primary set, timelines, follower map, and `pg_replication_slots.active` set are unchanged, promote `BestEffort` to `Verified`."
- **Justification:** One of the four proposed stability inputs is vacuous on this fleet: the captured cluster has replication_slots = [] on the primary and wal_receiver.slot_name = null on both replicas -- streaming without slots. The active-slot set is therefore constant-empty and contributes nothing to the stability comparison. The field itself is already captured, so this is a design flaw in section 6, not a gap.
- **Code:** `src/v2/scan/health_check_primary.rs:95-104 ReplicationSlot { ..., active: bool, ... } and the query at :216-227 -- capture exists; tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json db001 `"replication_slots": []`, db002/db003 `"slot_name": null` and `"primary_slot_name": ""`.`
- **Reachable in code:** False | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Already evidenced by the fixture. To generalise beyond the one captured cluster: SELECT count(*) FROM pg_replication_slots; on each primary across the fleet.
- **Fix (adr-text):** Drop pg_replication_slots.active from section 6's stability tuple, or qualify it as "where slots are in use (not this fleet)". Replace it with a signal that actually varies here -- e.g. wal_receiver.pid and last_msg_receipt_time advancing on each replica between passes.
- **Alternatives rejected:** Keeping it costs nothing at runtime but makes the stability check look stronger than it is, which is exactly the failure mode section 7 warns about elsewhere.

### [low | unverifiable] ADR lines 254, 58
- **Claim:** "In-flight-failover detection. The tool runs in stable state; transient-window scans are capped at `BestEffort`." (out of scope; same claim at line 58)
- **Justification:** No cap exists. BestEffort is simply the top of the enum because Verified is deliberately absent (line 121), so every non-Refuse, non-Conflicting verdict is BestEffort whether or not a failover is in flight. The statement is vacuously true today and describes a mechanism that is not implemented; if section 6 lands, nothing in the described stability check distinguishes a transient window from a stable one except the >= wal_sender_timeout re-scan interval.
- **Code:** `src/v2/analyze/split_brain.rs:73-77 `pub enum Confidence { Refuse, Conflicting, BestEffort }` -- no Verified; :385-389 `.map(|f| determine_confidence_level(f, true_primary)).min().unwrap_or(Confidence::BestEffort)`.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Reword to "transient-window scans cannot be promoted above BestEffort, because promotion requires the two-pass stability check (section 6) whose re-scan interval exceeds wal_sender_timeout" -- which states the actual mechanism instead of implying an active cap.
- **Alternatives rejected:** Implementing a real cap is unnecessary: the deferral of Verified already provides the property, it is just described backwards.

### [info | deferred-correct] ADR lines 227-230
- **Claim:** Section 6 as a whole: two-pass stability check deferred; `Verified` omitted from v1 (line 121).
- **Justification:** The deferral is stated consistently in ADR line 121, ADR line 229, and the plan's out-of-scope list, and nothing is half-built: `Verified` appears nowhere in src/. The Confidence enum leaves room -- derived Ord over declaration order with min() folding means a `Verified` appended after `BestEffort` slots in as the most-confident value, and the exhaustive severity_rank match in the guard test forces any new variant to be ranked. The one design note worth recording: confidence is currently derived purely from findings, and resolve_split_brain takes neither a scan-start timestamp nor prior-scan state, so section 6 needs a post-fold promotion step plus a signature change -- the min() fold cannot express it.
- **Code:** `src/v2/analyze/split_brain.rs:70-77 (enum + derives); :385-389 (min fold); :750-756 test severity_rank exhaustive match; :822-836 confidence_ordering_matches_severity_rank. src/v2/writer.rs:41 clusters_to_rescan and src/main.rs:151-161 are a watch-mode loop that keeps no prior-pass state -- not a partial two-pass implementation.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | implemented] ADR lines 247
- **Claim:** "Defer the `DivergentReplicaWal` emission ... nothing emits it today" and "Decision: capture-first. Collect db003's timeline and applied LSN ... (§5)".
- **Justification:** Both halves check out. No non-test construction site for DivergentReplicaWal exists, and the capture landed: the replica query now carries the control-file timeline plus both absolute LSNs as Option<String>, with round-trip tests including the explicit-null case.
- **Code:** `src/v2/scan/health_check_replica.rs:30-33 `pub last_wal_replay_lsn: Option<String>, pub last_wal_receive_lsn: Option<String>,`; :63 `'timeline_id', (SELECT timeline_id FROM pg_control_checkpoint()),`; :88-89 query fields; :251-265 deserialization tests. Grep for DivergentReplicaWal in src/ yields only the definition (split_brain.rs:102), two match arms (split_brain.rs:399, writer/build.rs:682) and one rstest case (split_brain.rs:782).`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | deferred-correct] ADR lines 251-253, 255, 259
- **Claim:** Out of scope: scoring/weighted aggregation; multi-dimensional verdict; repmgr metadata integration; cross-cluster signals (Pacemaker, etcd, VIP); visual/layout polish.
- **Justification:** Verified absent. Confidence is a lattice min over findings, not a weighted score; SplitBrainInfo carries a single true_primary with no separate committing/intended/divergent fields; grep across src/ finds no repmgr, pacemaker, etcd or VIP integration (the only hit is a doc comment in sync_standby_names.rs about repmgr-generated config strings). The test-only severity_rank helpers in split_brain.rs and classify.rs are ordering locks, not production scoring.
- **Code:** `src/v2/analyze/split_brain.rs:385-389 `.min()`; :60-68 SplitBrainInfo fields; grep -rn 'repmgr|pacemaker|etcd' src/ --include=*.rs -> src/v2/analyze/sync_standby_names.rs:9 only.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 

### [info | unverifiable] ADR lines 242-243
- **Claim:** "`Refuse` is correct only when db003's allegiance is **unprovable** -- the wedged C-g state."
- **Justification:** The dichotomy (observable -> confident, unprovable -> Refuse) omits a third state the code already produces: observable but inadmissible. A replica with a foreign system_identifier is excluded from gate input before build_replica_following_map, so its wal_receiver evidence is discarded even though it is present. The code lands on Refuse anyway, via SystemIdentifierMismatch, so the composition is safe today -- but by a different mechanism than the proof, and the proof does not say so.
- **Code:** `src/v2/analyze/split_brain.rs:250-256 filters mismatched replicas before the follower map; :396-399 SystemIdentifierMismatch -> Refuse; test sysid_mismatch_sets_refuse_and_emits_finding at :922.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** A capture in which one node's pg_control_system().system_identifier differs from the other two; none exists (the fixture has 6968745321024393216 on all three nodes).
- **Fix (adr-text):** Add a third bullet to the section 7 dichotomy: when db003 is excluded by the sysid gate its allegiance is observable but inadmissible, and Refuse arrives from the sysid gate rather than from the divergence reasoning -- so the safe outcome does not depend on the proof in that branch.
- **Alternatives rejected:** No code change: the current composition already errs safe.

### [low | stale] ADR lines 235, 211
- **Claim:** Section 7 and docs/concepts/split-brain.md reason throughout in terms of `synchronous_commit = on`, and line 211 designates the received LSN "the ack-relevant one under `synchronous_commit=on`".
- **Justification:** The fleet runs remote_apply, not on. Section 2 accepts both so there is no behavioural divergence, but the designation of which LSN is ack-relevant does change: under remote_apply the primary waits for the standby to *apply*, so pg_last_wal_replay_lsn is the ack-relevant position, not pg_last_wal_receive_lsn. That happens to favour the design (the applied position is also the one that survives a postmaster restart), but a follow-up implementing detection off "the ack-relevant one" as line 211 defines it would pick the weaker field.
- **Code:** `tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json db001 configuration.synchronous_commit = "remote_apply"; src/v2/analyze/split_brain.rs:13 `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` accepts remote_apply.`
- **Reachable in code:** False | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Note in section 5 line 211 and section 7 line 235 that the fleet's actual setting is remote_apply, under which the applied (replay) position is the ack-relevant one; keep the `on` reasoning as the weakest accepted case.
- **Alternatives rejected:** Rewriting all the `on` prose to remote_apply would lose the point that `on` is the floor section 2 permits.

### [info | implemented] ADR lines 264-265
- **Claim:** "Renaming `ReplicaOverridesTimeline` is a breaking API change for `SplitBrainResolution` consumers; the writer and any external readers must be updated in the same PR." / "`LowerTimelineHasQuorum` ... is harder to trigger: stale replica evidence that previously qualified is filtered by the gate."
- **Justification:** Both hold. The variant is renamed at the definition and consumed under the new name in the writer; the gate adds status, port, and freshness checks on the replica side plus a corroborating primary-side row, all of which stale evidence fails.
- **Code:** `src/v2/analyze/split_brain.rs:34-38 `LowerTimelineHasQuorum { ... }`; src/v2/writer/build.rs:246 and :705-710 consume it; the gate at split_brain.rs:281-315; jj commit f14cdf3454b1 "refactor(analyze): rename ReplicaOverridesTimeline to LowerTimelineHasQuorum".`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** 


## ADR-002 §5 "New data to collect" (lines 181-226): primary/replica query additions, timeline-history SQL, replica absolute LSNs, the 2026-09-10 validation/measurement paragraphs, and the scan-start plumbing bullet.

### [high | diverges] ADR lines 221
- **Claim:** "Capture only for now (§7): gather the evidence so the next real C-g is diagnosable" -- and §7's "Decision: capture-first. Collect db003's timeline and applied LSN from the control file ... so the wedged state becomes observable".
- **Justification:** The values are read from Postgres and then dropped in-process: nothing durable (terminal view, CSV, file, DB) ever contains them, so a real C-g scan at default log level produces no captured evidence.
- **Code:** `src/v2/scan/health_check_replica.rs:184 `tracing::debug!(text = %json_text, "Raw JSONB text result");` is the only place the values appear; the info-level completion event src/v2/scan/health_check_replica.rs:123-130 logs `timeline_id`/`wal_receiver_status`/`apply_lag_bytes`/`conflicts_count`/`primary_conninfo` and omits both new LSNs. Default level is info: src/config.rs:298 `.unwrap_or_else(|| "info".to_owned());`. Output is only ClusterView: src/v2/writer/csv.rs:45 `"status,cluster,primary,replicas,lag_bytes,reason,details_json"`, and details_json is built from the verdict (src/v2/writer/build.rs:279), never from node health.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Run a scan against a cluster at default log level and grep the output for `last_wal_receive_lsn` -- expect zero hits; repeat with RUST_LOG=debug to confirm the only path.
- **Fix (scanner):** Add the two LSNs (and a `timeline_history` present/absent + entry-count marker on the primary side) to the existing info-level completion events -- src/v2/scan/health_check_replica.rs:123-130 gains `last_wal_replay_lsn = data.last_wal_replay_lsn.as_deref(), last_wal_receive_lsn = data.last_wal_receive_lsn.as_deref()`, and src/v2/scan/health_check_primary.rs:237-243 gains `timeline_history_len = data.timeline_history.as_ref().map(String::len)`. Alternatively state explicitly in §5 that capture-first requires running the scan with `RUST_LOG=debug` (and say so in the runbook), so the deferral in §7 has a terminating condition.
- **Alternatives rejected:** Persisting the raw health JSON to a file or a results DB, or adding node-level columns to the CSV: much larger scope than 'capture only', and the CSV row is cluster/verdict-shaped, not node-shaped. Promoting the existing raw-JSON debug log to info: dumps the whole payload for every node on every scan, unusable at fleet size.

### [high | self-contradictory] ADR lines 215
- **Claim:** "A zero pointer is only reachable before any walreceiver has run (receive) or anything has been replayed (replay) within the current postmaster's life. Every node in this fleet is built as a standby or promoted from one, so NULL is close to unobservable in practice" / "The hazard is staleness, not nullity."
- **Justification:** Line 219 in the same section says both pointers are shared-memory and 'zeroed by a postmaster restart', which makes NULL routine after any restart -- and the restart case coincides with C-g, the state the capture exists for.
- **Code:** `src/v2/scan/health_check_replica.rs:33 `pub last_wal_receive_lsn: Option<String>,` (doc comment at :31-32 correctly says 'Survives walreceiver death and promotion within one postmaster'); src/v2/scan/health_check_replica.rs:89 `'last_wal_receive_lsn', pg_last_wal_receive_lsn()::text,``
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** On any fleet standby: `SELECT pg_postmaster_start_time(), pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();` immediately after a `pg_ctl restart` with primary_conninfo pointing at an unreachable host -- expect receive NULL, replay non-NULL.
- **Fix (adr-text):** Replace 'NULL is close to unobservable in practice' with the conditional truth: NULL is unobservable only while the postmaster that did the standby work is still running. A wedged replica that has been restarted (the normal operator reflex on a stuck node) has never started a walreceiver in the current postmaster's life, so `pg_last_wal_receive_lsn()` is NULL exactly in C-g; `pg_last_wal_replay_lsn()` recovers to a useful value because restart recovery replays local pg_wal. State that consequence explicitly: the received/flushed pointer is the one that does NOT survive the case it was added for, and detection must therefore be designed on the applied pointer (or on a durable control-file position, see the pg_control_recovery finding).
- **Alternatives rejected:** Leaving the two paragraphs as-is and treating 219 as the qualifier: 215 makes an unconditional practical claim ('close to unobservable') that a reader will carry into the deferred detection design, and 219 is four paragraphs later under a 'Terminology' heading nobody reads as a correction to the nullity argument.

### [high | unverifiable] ADR lines 204
- **Claim:** "Privileges: requires `pg_read_server_files`, which is granted in production." (and by implication, that adding the file read to HEALTH_CHECK_PRIMARY_QUERY is safe)
- **Justification:** The grant is asserted, not evidenced, and the failure mode is all-or-nothing: a permission error on pg_read_file aborts the whole jsonb_build_object, the node becomes UnknownPrimary, and UnknownPrimary is filtered out of `primaries()`, so a two-primary split brain silently collapses to the single-primary path.
- **Code:** `src/v2/scan/health_check_primary.rs:153-156 `ELSE pg_read_file('pg_wal/' || lpad(upper(to_hex(timeline_id)), 8, '0') || '.history', 0, (1024 * 1024)::bigint, true)`; on error src/v2/scan/health_check_primary.rs:268 `role: Role::UnknownPrimary,`; src/v2/cluster.rs:70 `self.nodes.iter().filter(|n| n.role.is_primary())` and src/v2/scan.rs:334 `matches!(self, Role::Primary { .. })` exclude it; src/v2/analyze.rs:315 `if primaries.len() > 1 {` then never fires; src/v2/writer/build.rs:361-362 maps UnknownPrimary to None in the view, so nothing is shown.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** On every production node as the scan role: `SELECT pg_has_role(current_user,'pg_read_server_files','member'), has_function_privilege(current_user,'pg_read_file(text,bigint,bigint,boolean)','execute'), has_function_privilege(current_user,'pg_control_system()','execute');` -- all three must be true on all nodes, not just 'in production' generically.
- **Fix (scanner):** Make the history read self-disabling instead of fatal: add a guard arm to the CASE in src/v2/scan/health_check_primary.rs:151-157, e.g. `WHEN NOT pg_has_role(current_user, 'pg_read_server_files', 'member') THEN NULL` before the ELSE (superusers satisfy pg_has_role implicitly). One line, no new abstraction, and it converts 'the primary vanishes from the scan' into 'timeline_history is NULL on that node'. Note the same all-or-nothing exposure applies to the new `pg_control_system()` call added by this section (health_check_primary.rs:160, health_check_replica.rs:64), which is also EXECUTE-restricted by default.
- **Alternatives rejected:** Running the history read as a separate, failure-tolerant query: an extra round trip per primary plus a second result-merging path, for a value nothing consumes yet. Catching the error in Rust: the error is raised by the server for the whole statement, so there is nothing to catch per-field.

### [medium | diverges] ADR lines 211
- **Claim:** "`pg_last_wal_receive_lsn()` -- received/flushed position (the ack-relevant one under `synchronous_commit=on`)"
- **Justification:** True as written but mis-specified for this fleet: the captured cluster runs `synchronous_commit = remote_apply`, under which the ack criterion is apply, so `pg_last_wal_replay_lsn()` -- not the receive pointer -- is the ack-relevant one.
- **Code:** `tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json `"synchronous_commit": "remote_apply"` (primary configuration block); src/v2/scan/health_check_replica.rs:88-89 captures both, so the data is there either way. The plan already uses the apply pointer: docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:71 "A replica on the *lower* timeline (control-TLI = N) with applied LSN > X is proof of acked writes".`
- **Reachable in code:** False | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `SELECT name, setting FROM pg_settings WHERE name='synchronous_commit';` on every production primary, to confirm remote_apply is fleet-wide and not just this one dev cluster.
- **Fix (adr-text):** Qualify line 211: under the fleet's actual `synchronous_commit = remote_apply`, acknowledgement implies apply, so `pg_last_wal_replay_lsn()` is the exact ack boundary and `pg_last_wal_receive_lsn()` is an upper bound (received-but-unapplied WAL was never acked). This is the load-bearing sentence for the deferred §7 trigger, and it currently points a future implementer at the weaker, restart-fragile pointer. Note also that this makes the applied pointer doubly preferable: it is both exact under remote_apply and reconstructible after a restart from local WAL replay.
- **Alternatives rejected:** Changing §7's `synchronous_commit = on` framing instead: that is a broader edit across §2/§7 and outside this section; the minimal correct change is to stop labelling the receive pointer as 'the ack-relevant one' without naming the fleet's setting.

### [medium | unverifiable] ADR lines 217
- **Claim:** "Measured on a promoted primary (`pg_is_in_recovery() = f`): `pg_last_wal_replay_lsn()` and `pg_last_wal_receive_lsn()` both return `6FD/7C0000A0`, which is exactly the TL 21 -> 22 switch point in the `.history` captured from that cluster."
- **Justification:** Neither the value `6FD/7C0000A0` nor any TL 21->22 `.history` exists anywhere in the repo; the only checked-in artifact from that cluster is a TL-22 replica capture, which is consistent with but does not pin the quoted number.
- **Code:** `src/v2/scan/health_check_replica.rs:214,228-229 `"timeline_id": 22 ... "receive_start_lsn": "6FD/7D000000", "receive_start_tli": 22` (labelled at :205-206 'verbatim capture from a live replica'); a repo-wide search for `7C0000A0` matches only ADR-002 line 217.`
- **Reachable in code:** False | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** On the promoted primary of that cluster: `SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn();` plus `SELECT pg_read_file('pg_wal/00000016.history');` (TL 22 = 0x16), stored as a fixture.
- **Fix (adr-text):** Either attach the supporting capture (the promoted primary's `.history` contents and the two function outputs) as a fixture next to the existing replica capture, or downgrade the sentence to 'observed once on <cluster>, capture not retained'. As written it reads as reproducible evidence for the general 'both pointers freeze at the promotion LSN' rule, and the general rule is what a future implementer will build on.
- **Alternatives rejected:** Deleting the paragraph: the observation is genuinely useful (it is the only direct evidence that promotion does not clear receivedUpto); the problem is the missing artifact, not the observation.

### [medium | diverges] ADR lines 217
- **Claim:** "Both pointers freeze at the promotion LSN -- a promoting standby finishes replaying what it received, so the two converge on the fork point and then stop advancing."
- **Justification:** The convergence holds only for trigger promotion of a standby with no recovery target, and 'freeze' is falsified outright by line 219: a postmaster restart of the promoted primary zeroes both, so they do not freeze, they vanish.
- **Code:** `src/v2/scan/health_check_replica.rs:30-33 (the struct doc comments hedge correctly -- 'can be stale -- frozen at promotion on a former standby' -- but the fields are only collected on replicas; HEALTH_CHECK_PRIMARY_QUERY at src/v2/scan/health_check_primary.rs:145-229 never collects them, so nothing in the tool can observe a promoted primary's frozen pointers anyway).`
- **Reachable in code:** False | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Promote a standby that is deliberately behind in replay (pause recovery with `pg_wal_replay_pause()`, let the walreceiver run on, then promote) and read both functions -- expect replay < receive, contradicting 'converge'.
- **Fix (adr-text):** Bound the claim to its preconditions. Convergence requires (a) promotion by trigger with no `recovery_target_*` set -- with a recovery target (default `recovery_target_action = promote`) replay stops at the target while receivedUpto stays far ahead, so replay < receive; and (b) the postmaster not having restarted since promotion -- otherwise both read NULL, not the promotion LSN. Also add the observation that the tool cannot see this on primaries at all today, since HEALTH_CHECK_PRIMARY_QUERY does not collect either function; if the promoted-primary reading is wanted as C-g evidence, say so and add it, otherwise mark the paragraph explicitly as background rather than a capture requirement.
- **Alternatives rejected:** Adding the two functions to the primary query to match the paragraph: the fork LSN of a promoted primary is already available from its `.history` (captured), so the extra fields would duplicate a value we can get durably; the cheaper fix is to scope the prose.

### [medium | diverges] ADR lines 221
- **Claim:** "The control-file `timeline_id` (already captured) plus an absolute applied LSN let us place the replica relative to a primary's fork LSN even with no live receiver."
- **Justification:** The data is necessary but not sufficient as characterised: the applied LSN is not a control-file value (line 219 concedes this), the durable control-file position `pg_control_recovery().min_recovery_end_location` is not captured, the replica's own `.history` is not captured so its lineage cannot be confirmed beyond system_identifier, and no LSN comparison is wired anywhere.
- **Code:** `src/v2/scan/health_check_replica.rs:61-115 -- the replica query captures `pg_control_checkpoint().timeline_id` (:63) and the two shared-memory pointers (:88-89) but nothing from `pg_control_recovery()` and no `.history`; the comparator exists but is unused for this: src/v2/analyze/checks.rs:340 `pub(super) fn pg_lsn_diff(lsn1: &str, lsn2: &str) -> Option<u64> {`.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** On a standby: `SELECT * FROM pg_control_recovery();` before and after a postmaster restart, to confirm min_recovery_end_location survives where the two shared-memory pointers do not.
- **Fix (combination):** Two changes. (1) Scanner: add `(SELECT min_recovery_end_location::text FROM pg_control_recovery())` and `min_recovery_end_timeline` to HEALTH_CHECK_REPLICA_QUERY -- these are genuine control-file values, they survive a postmaster restart, and they are the only 'control-file position' that matches the words §5 and §7 use. (2) ADR text: list what placement actually needs -- replica control-file TLI, a replica position (applied, or min_recovery_end_location as the durable floor), the fork LSN from the higher-TL primary's `.history` (already captured, still unparsed in production code), and a same-lineage check via system_identifier -- and say plainly that only three of the four are captured today.
- **Alternatives rejected:** Capturing the replica's own `.history` file too: on a wedged replica the interesting history file is the higher-TL one it never fetched, so the primary-side capture is the right source; adding a replica-side pg_read_file would extend the privilege blast radius (see the pg_read_server_files finding) to replicas for little gain.

### [low | implemented] ADR lines 204
- **Claim:** "The filename is 8-digit uppercase hex padded (postgres writes via `%08X`); decimal padding will silently miss TLs >= 10."
- **Justification:** The claim is correct and the shipped SQL implements it; the fixture's TL-11 cluster with `last_archived_wal` `00000011...` is the trap, and it is the fixture that is wrong -- TL 11 in %08X is `0000000B`, and `00000011` is hex 17.
- **Code:** `src/v2/scan/health_check_primary.rs:154 `'pg_wal/' || lpad(upper(to_hex(timeline_id)), 8, '0') || '.history',` -- to_hex(11)='b' -> '0000000B', which is what Postgres names the file. Contradicting fixture data: tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json:15 `"timeline_id": 11,` with tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json:79 `"last_archived_wal": "00000011000004850000003F"`.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** On the fixture's source cluster: `SELECT timeline_id FROM pg_control_checkpoint();` alongside `SELECT last_archived_wal FROM pg_stat_archiver;` -- the TLI prefix must equal lpad(upper(to_hex(timeline_id)),8,'0').
- **Fix (tests):** Fix the fixture, not the code. The three fields disagree: timeline_id 11 (decimal) implies the history file `0000000B.history` and WAL segments `0000000B........`, but the archived segment name encodes TLI 0x11 = 17, while the fixture's `timeline_history` contains exactly 10 switch records (TL 1..10 -> current TL 11). Pick one lineage and make all three agree -- most likely `"last_archived_wal": "0000000B000004850000003F"`, keeping the logid/segno fields (0x485/0x3F), which are already hex-consistent with `current_wal_lsn` 48F/6957B540. Add a one-line comment in the fixture or in the timeline_history tests recording that the TLI field is hex, so the next person does not 'fix' `to_hex` instead.
- **Alternatives rejected:** Treating the fixture as ground truth and changing the SQL to decimal padding: that would break every cluster at TL >= 10 (silently, because missing_ok=true turns the wrong filename into NULL) -- i.e. it would introduce exactly the bug line 204 warns about. Also rejected: leaving it, since the fixture is the repo's designated 'real captured data' and this field actively teaches the wrong encoding.

### [low | diverges] ADR lines 209
- **Claim:** "today the only LSN we keep is the *difference* `pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())` stored as `lag.apply_lag_bytes` -- the gap, not the positions."
- **Justification:** The pre-existing replica payload already keeps four absolute LSNs inside the wal_receiver row; the accurate statement (which the section's own Rationale paragraph makes) is that no absolute LSN survives when `wal_receiver` is absent.
- **Code:** `src/v2/scan/health_check_replica.rs:71-79 `receive_start_lsn::text, ... written_lsn::text, flushed_lsn::text, ... latest_end_lsn::text`; struct fields at src/v2/scan/health_check_replica.rs:40-47.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Reword to 'the only LSNs we keep outside the `wal_receiver` row are the receive-replay difference in `lag.apply_lag_bytes`; every absolute position we have today (`receive_start_lsn`, `written_lsn`, `flushed_lsn`, `latest_end_lsn`) disappears with the row -- which is the C-g case'. Same correction applies to the plan at docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:25 and :61-62.
- **Alternatives rejected:** Leaving it as shorthand: the sentence is the stated justification for adding the fields, and as written it is checkably false, which invites a reviewer to reject the addition as redundant.

### [low | diverges] ADR lines 225
- **Claim:** "Pass scan-start `DateTime<Utc>` from `analyze_clusters` through `analyze()` into `resolve_split_brain()` as a parameter, for the freshness gate. This is a small but load-bearing plumbing change."
- **Justification:** No such parameter exists anywhere in the chain; the code compares each node's own `current_time` against that node's own timestamps, which is strictly better than the ADR's proposal.
- **Code:** `src/v2/analyze/split_brain.rs:132-135 `pub(super) fn resolve_split_brain(primaries: &[&AnalyzedNode], replicas: &[&AnalyzedNode],) -> SplitBrainInfo`; call site src/v2/analyze.rs:318 `let split_brain_info = resolve_split_brain(&primaries, &replicas);`; intra-node comparisons at src/v2/analyze/split_brain.rs:285 `(r_health.current_time - t).num_milliseconds() <= threshold_ms` and :313 `(p_health.current_time - t).num_milliseconds() <= threshold_ms`, documented at :242-244.`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Rewrite the bullet to record the decision the code actually made and why it is better: freshness is measured intra-node, `now()` from the same query that read `last_msg_receipt_time`/`reply_time`, so (a) scanner-to-node clock skew cannot eat into a ~180 s threshold (wal_sender_timeout/2 + 30 s), and (b) a long fleet scan cannot make late-scanned nodes look artificially fresh. Point (b) is the decisive one: with a scan-start reference, `scan_start - last_msg_receipt_time` shrinks (or goes negative) for nodes scanned minutes later, so a scan-start anchor is *less* conservative than intra-node -- it would let genuinely stale wal_receiver rows through the gate that is the whole point of this ADR. Drop 'load-bearing plumbing change' entirely; no plumbing is needed.
- **Alternatives rejected:** Implementing the ADR as written (adding the parameter): it would make the safety gate strictly weaker and reintroduce clock-skew sensitivity. The one thing a shared reference would give -- comparing two nodes at the same instant -- is not needed, because each side of the follow-link is judged against its own clock and no cross-node timestamp arithmetic is done.

### [low | implemented] ADR lines 213
- **Claim:** "(Validated 2026-09-10.) Both functions return SQL NULL, never `0/0` ... PG17 `xlogfuncs.c` guards each with `if (recptr == 0) PG_RETURN_NULL();` ahead of `PG_RETURN_LSN(recptr)`. `Option<String>` is therefore the right shape"
- **Justification:** The behaviour claim is true and the guard is present in the version the fleet actually runs, but the citation names PG17 while every node is 15.14 -- a true claim with the wrong-version citation, not a false claim.
- **Code:** `src/v2/scan/health_check_replica.rs:30,33 `pub last_wal_replay_lsn: Option<String>,` / `pub last_wal_receive_lsn: Option<String>,` with the NULL round-trip covered at src/v2/scan/health_check_replica.rs:256-266. Verified upstream in REL_15_STABLE src/backend/access/transam/xlogfuncs.c: both `pg_last_wal_receive_lsn` (GetWalRcvFlushRecPtr) and `pg_last_wal_replay_lsn` (GetXLogReplayRecPtr) contain `if (recptr == 0) PG_RETURN_NULL();` before `PG_RETURN_LSN(recptr);`. Fleet version: tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json `"pg_version": "15.14"` on all three nodes.`
- **Reachable in code:** False | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Change 'PG17 xlogfuncs.c' to 'PG 15 xlogfuncs.c (REL_15_STABLE; the guard is unchanged through 17)'. Also fix the stale contradiction it was written to correct: docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:35 still says 'may be 0/0 with no receiver', and the code comment at src/v2/scan/health_check_replica.rs:257 says 'PG17 returns SQL NULL' -- both should name 15.
- **Alternatives rejected:** Leaving the version out entirely: the whole value of the '(Validated)' annotation is that a reader can re-check it against the deployed source, and 15 vs 17 is the difference between a check they can run and one they cannot.

### [low | deferred-correct] ADR lines 186-204
- **Claim:** Timeline-history collection is a §5 addition whose parsed fork LSN underpins the deferred §7 detection ("Read the switch-LSN X from the higher-TL primary's `.history`").
- **Justification:** The capture and the parser both landed, and nothing in §5 claims a consumer -- but the parser is entirely inert in production code (its two dead-code warnings are the evidence), so the §5 capture currently exists only to be read by a human.
- **Code:** `src/v2/scan/health_check_primary/timeline_history.rs:17 `pub fn timeline_history_entries(&self) -> Vec<TimelineHistoryEntry>` -- called only from :42 inside `_fork_lsn_for` (underscore-named to suppress dead_code, :41) and from the module's own tests; `TimelineHistoryEntry.reason` (:11) is read only in tests. `mod timeline_history;` is private (src/v2/scan/health_check_primary.rs:19), so no external crate can reach them either.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Say in §5 what §5 says for the replica LSNs: the primary-side history capture is capture-only, nothing consumes it, and the two dead-code warnings in timeline_history.rs are the intentional marker of that state (the `_fork_lsn_for` underscore prefix is the same marker). Without that sentence, §5 reads as if the history is already feeding fork-LSN logic, and §7's 'the wiring stays dormant' is stated only about the finding variant, not about the parser.
- **Alternatives rejected:** Wiring `_fork_lsn_for` into something to clear the warnings: §7 explicitly defers the detection until a real C-g is captured, so inventing a consumer now would contradict the deferral. Deleting the parser: it is the piece a future C-g investigation needs first.

### [info | implemented] ADR lines 188-200
- **Claim:** The quoted timeline-history SQL block, character for character (ADR writes a self-contained scalar subquery with its own `WITH cc` and a CTE-qualified `WHEN cc.timeline_id = 1`).
- **Justification:** The shipped query hoists `cc` to statement level and drops the `cc.` qualifier; both differences are semantically inert (single range table, no ambiguity) and the hoist strengthens the ADR's own single-evaluation rationale, since a CTE referenced twice is never inlined.
- **Code:** `src/v2/scan/health_check_primary.rs:146 `WITH cc AS (SELECT timeline_id FROM pg_control_checkpoint())` shared by :149 `'timeline_id', (SELECT timeline_id FROM cc),` and :150-159; :152 `WHEN timeline_id = 1 THEN NULL` vs the ADR's `WHEN cc.timeline_id = 1`; :155 `0, (1024 * 1024)::bigint, true` matches the ADR's 4-arg form exactly.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Optional: update the quoted block to the shipped shape (statement-level `WITH cc`, unqualified column) so the ADR and the code are byte-comparable, and note that sharing `cc` with the `timeline_id` output is what guarantees materialisation. No code change -- the shipped form is the better of the two.
- **Alternatives rejected:** Changing the code to match the ADR verbatim: it would give each field its own `pg_control_checkpoint()` call, which is the thing the rationale paragraph argues against.

### [info | unverifiable] ADR lines 202
- **Claim:** "The CTE evaluates `pg_control_checkpoint()` once; calling it twice in the same CASE risked a (negligible but real) race during a TL bump where the filename and the existence check disagree."
- **Justification:** The hygiene is right (pg_control_checkpoint() is volatile, so two calls in one statement may disagree), but the specific race is not reachable: this query only runs against a node already serving as a primary, and a timeline_id increment happens at end-of-recovery -- a node cannot promote again while it is already a primary.
- **Code:** `src/v2/scan/health_check_primary.rs:146 `WITH cc AS (SELECT timeline_id FROM pg_control_checkpoint())`; the query is dispatched only on the primary path, src/v2/scan/health_check_primary.rs:232 `pub(super) async fn check(client: Client, node: Arc<Node>, ...)` producing `Role::Primary` at :251.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Soften the justification to what actually holds: pg_control_checkpoint() is volatile, so two calls in one statement are not guaranteed to agree, and the CTE removes the question -- rather than asserting a TL-bump race that cannot occur on an already-promoted node. Note too that with `missing_ok = true` the worst outcome of a disagreement is a NULL history, not an error, so 'race' overstates the consequence.
- **Alternatives rejected:** Deleting the rationale: the single-evaluation property is worth recording, since a future edit that inlines the CTE back into two subqueries would be a silent regression of it.

### [info | implemented] ADR lines 202
- **Claim:** "The 4-arg form `pg_read_file(path, offset, length, missing_ok)` returns NULL if the file doesn't exist ... the 1-arg form throws and would abort the entire `jsonb_build_object`. The 1 MiB length cap is far more than any realistic history file (typically <1 KiB) but bounds memory allocation."
- **Justification:** Both semantics are correct for PG 15 and the shipped call uses exactly the 4-arg form with a 1 MiB cap; the TL=1 branch returns NULL and the parser handles it.
- **Code:** `src/v2/scan/health_check_primary.rs:153-156 `ELSE pg_read_file(... , 0, (1024 * 1024)::bigint, true)`; NULL handling at src/v2/scan/health_check_primary/timeline_history.rs:18-20 `let Some(history) = &self.timeline_history else { return Vec::new(); };` with the TL=1 case covered by the test at :101-105.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No change. One caveat worth a half-sentence if the section is being edited anyway: `missing_ok` covers a missing file only -- a permission error still aborts the statement (see the pg_read_server_files finding), so 'returns NULL if the file doesn't exist' should not be read as 'the read cannot fail'.

### [info | implemented] ADR lines 185, 208
- **Claim:** `system_identifier` from `pg_control_system()` cast to text on both queries, "to avoid `bigint`->JSON safe-int issues" and "for cross-node sanity gate".
- **Justification:** Present in both queries with the cast, typed as String, and genuinely consumed by the sanity gate -- the only §5 addition that has a live consumer.
- **Code:** `src/v2/scan/health_check_primary.rs:160 and src/v2/scan/health_check_replica.rs:64 `'system_identifier', (SELECT system_identifier::text FROM pg_control_system()),`; consumed at src/v2/analyze/split_brain.rs:569 `if let Some(sid) = p.role.as_primary().map(|h| h.system_identifier.as_str())` and :595-596. The cast is load-bearing: the fixture value `6968745321024393216` exceeds 2^53, and without `::text` serde would also fail to deserialize a JSON number into `String`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No change.

### [low | implemented] ADR lines 219
- **Claim:** "Terminology: ... `pg_control_checkpoint().timeline_id` does come from the control file, but both LSNs are read from shared memory -- they are zeroed by a postmaster restart and do not survive one."
- **Justification:** True (GetXLogReplayRecPtr / GetWalRcvFlushRecPtr read shared memory), and the struct doc comments carry the caveat -- but §7 and the concepts doc still use the 'from the control file' shorthand for these same LSNs, so the correction is only half-propagated.
- **Code:** `src/v2/scan/health_check_replica.rs:28-33 doc comments ('can be stale -- frozen at promotion', 'Survives walreceiver death and promotion within one postmaster, so `Some(_)` is a high-water mark'). Un-propagated shorthand: docs/adr/002-split-brain-resolution-refinement.md §7 'Collect db003's timeline and applied LSN from the **control file**' and docs/concepts/split-brain.md:87 'reading the replica's position from the **control file** (`pg_control_checkpoint().timeline_id`, `pg_last_wal_replay_lsn()`)' (the concepts doc does append the shared-memory caveat in the same sentence).`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Propagate the correction into §7's 'Decision: capture-first' sentence (say 'control-file timeline plus the shared-memory applied position', or capture `pg_control_recovery().min_recovery_end_location` and make the shorthand true). Leaving §7 saying 'from the control file' while §5 says the opposite is the kind of drift that a follow-up implementer resolves in favour of whichever paragraph they read first.
- **Alternatives rejected:** Editing only the concepts doc: it already carries the caveat inline; §7 is the one that states it flatly.

### [low | stale] ADR lines 209-221
- **Claim:** Fleet-capture coverage for the two new replica fields (implied by §5's capture-first framing).
- **Justification:** The repo's designated real-fleet fixture predates the fields and does not contain them at all, so the only fleet-shaped evidence for their content is the inline capture in the replica unit test.
- **Code:** `tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json contains no `last_wal_replay_lsn`/`last_wal_receive_lsn` keys on either replica (they deserialize to None only because serde treats missing `Option<T>` as None), while the same fixture *does* carry the §5 primary-side additions (`system_identifier`, `timeline_history` at lines 15-18). Coverage that does exist: src/v2/scan/health_check_replica.rs:241-242 and the two tests at :246-266.`
- **Reachable in code:** False | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** A fresh scan capture of the dev cluster with RUST_LOG=debug, taking the raw replica JSONB verbatim.
- **Fix (tests):** Refresh NON_FAILOVER_CLUSTER.json from a real scan so the replica nodes carry the two LSNs (a streaming replica must report both non-NULL and roughly equal to `wal_receiver.flushed_lsn` -- the current fixture's silence is not a valid observation of NULL). While doing so, reconcile the timeline_id / last_archived_wal / timeline_history inconsistency noted separately; the fixture's history LSNs (0/3000000, 0/5000000, 1/2000000 ...) are suspiciously round compared with a real switch point like 6FD/7C0000A0, which suggests the primary-side §5 fields were hand-authored rather than captured.
- **Alternatives rejected:** Adding `#[serde(default)]`-style compatibility shims or a second fixture: the file is already the single include_str! source for the healthy-cluster tests (src/v2.rs:30), so refreshing it in place is the minimal change.


## docs/concepts/split-brain.md (as delegated spec) + docs/concepts/README.md, cross-checked against ADR-002, the resolver/writer code, and stale statements in SPEC.md / README.md / CHANGELOG.md / TODO.md

### [critical | diverges] ADR lines docs/concepts/split-brain.md:50, 72, 80-87
- **Claim:** The concepts doc's whole causal model -- 'the verdict follows the flushing replica', C-b/C-c, LowerTimelineHasQuorum, and 'Observability is the hinge' (what verdict you get depends on what you can see of db003) -- describes behaviour the resolver cannot exhibit on real fleet node naming.
- **Justification:** Per ground truth #2, the primary-side gate compares the two different name forms the fleet actually uses, so no replica is ever gated; the doc's entire lower-TL branch is dead on real data.
- **Code:** `src/v2/analyze/split_brain.rs:307 `&& conn.application_name == replica.node_name` -- fixture has application_name "dev_pg_app001_db002" (tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json:23-27 block) vs node_name "dev-pg-app001-db002.sto2.example.com"; with primary_row=None the replica is never inserted into `following` (src/v2/analyze/split_brain.rs:315-337), so resolve_with_different_timelines falls to the final else (src/v2/analyze/split_brain.rs:484-503) and returns HigherTimeline, rendered as "SplitBrain: {true} has quorum (TL={hi}), demote {stale} (TL={lo}, no live replicas)" (src/v2/writer/build.rs:716)`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Already evidenced by tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json. To confirm the same shapes hold on a split-brain node: `SELECT application_name, client_addr, state FROM pg_stat_replication;` on both candidate primaries, compared against the node_name the nodes API returns.
- **Fix (resolver):** In build_replica_following_map, compare the primary-side row using the same normalisation the writer already has: normalize_application_name()/extract_db_number() in src/v2/writer/build.rs. Match `conn.application_name` against the normalised replica node_name (strip domain, map '-' to '_'), or compare extracted db-numbers. Same normalisation must be applied in emit_quorum_findings when intersecting SSN members with gated followers, since SSN members are also in application-name form. Add a regression test built from NON_FAILOVER_CLUSTER.json's two name forms rather than the toy db003/db003 pair used today.
- **Alternatives rejected:** Rewriting docs/concepts/split-brain.md to describe the higher-TL-always outcome: rejected -- the doc is right about the domain and the code is wrong; documenting the bug entrenches the data-destroying verdict. Matching on ip/client_addr instead of application_name: rejected -- SSN quorum membership in postgres is by application_name, so the quorum derivation would still be wrong.

### [high | diverges] ADR lines docs/concepts/split-brain.md:15, 70, 76
- **Claim:** "an *isolated* primary -- one with no live standby acking it -- physically cannot commit", and consequently "db002 ... **provably committed nothing on TL=N+1.** Its fork is empty" / "it is the empty branch. The divergent node to *rebuild* is db002".
- **Justification:** Postgres sync rep withholds the client acknowledgement, not the commit: RecordTransactionCommit flushes the commit record and marks the xact committed locally before SyncRepWaitForLSN, which is why cancelling the wait yields "The transaction has already committed locally, but might not have been replicated to the standby". An isolated primary's fork therefore contains real commit records that are visible to other sessions on that node and survive its crash -- it is not an "empty branch". ADR-002 states the careful version (C-c: "provably client-acked nothing"); the concepts doc states the unsound version, and the doc is the one that concludes "rebuild db002".
- **Code:** `src/v2/analyze/split_brain.rs:400 `// A quorum-blocked primary cannot have ack'd writes. When it's a stale` -- the code comment inherits the same over-claim and is the stated reason PrimaryQuorumUnsatisfied on a non-elected primary is downgraded to Confidence::BestEffort (src/v2/analyze/split_brain.rs:404-408)`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** On a candidate isolated primary: `SELECT pg_current_wal_insert_lsn();` compared with the switch LSN in its own `<TL>.history`, plus `SELECT * FROM pg_last_committed_xact();` (needs track_commit_timestamp=on, which is not among the GUCs the scanner captures today). Any commit timestamp later than the promotion time proves the fork is not empty.
- **Fix (adr-text):** Rewrite docs/concepts/split-brain.md:15 to "an isolated primary cannot *acknowledge* a client commit; it still writes and locally commits the transaction, and other sessions on that node can see it". Rewrite lines 70 and 76 to say "client-acked nothing on TL=N+1" / "holds no client-acknowledged writes on its fork", and drop the phrase "empty branch". Add one sentence at line 76 stating explicitly that rebuilding db002 can still discard locally-committed, never-acked transactions, so the rebuild is a data-discarding action even in the 'safe' case. Reword the comment at src/v2/analyze/split_brain.rs:400 to "cannot currently ack writes".
- **Alternatives rejected:** Leaving the shorthand and relying on ADR-002 C-c's precise wording: rejected -- ADR-002:11 delegates exactly this topic ("why flushed-past-fork implies acknowledged writes") to the concepts doc, so the concepts doc is the normative text for it, and it is the text that names the destructive action.

### [high | diverges] ADR lines docs/concepts/split-brain.md:66-72, 84
- **Claim:** The "3-node proof": "If db003 is observably on TL=N (acking db001), then db002 had no acker and ... provably committed nothing on TL=N+1" -- used to license a *confident*, non-Refuse "keep db001, fence db002" verdict.
- **Justification:** The premise is a point-in-time observation; the conclusion quantifies over the whole interval since db002's promotion. The doc applies historical reasoning in the mirror direction (C-g: db003 "*did* ack TL=N writes past X earlier") but point-in-time reasoning here, from the same evidence type. Two concrete falsifiers: (a) db003 acked db002 after promotion and was then rebuilt from db001 by basebackup -- which is this cluster's documented remediation policy -- leaving it observably on TL=N with the contradicting evidence destroyed; (b) postgres satisfies SSN quorum by application_name, so any walsender named `dev_pg_app001_db003` (pg_receivewal, a backup agent, a `pg_basebackup -X stream` session) can ack for db002 without being db003-the-standby.
- **Code:** `src/v2/analyze/split_brain.rs:449-461 -- the LowerTimelineHasQuorum arm sets `confidence: Confidence::BestEffort` with no safety qualifier, and src/v2/writer/build.rs:709 renders `"SplitBrain: {} has quorum (lower TL={}), fence {} (TL={}, quorum-blocked)"``
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** On db003: `SELECT * FROM pg_stat_wal_receiver;` plus the contents of `pg_wal/*.history` (`SELECT pg_read_file('pg_wal/' || lpad(upper(to_hex(timeline_id)),8,'0') || '.history') FROM pg_control_checkpoint();`) -- a TL=N+1 line in db003's history, or a basebackup_start timestamp after db002's promotion, falsifies the proof. On db002: `SELECT * FROM pg_last_committed_xact();` with track_commit_timestamp=on.
- **Fix (adr-text):** State the missing premise at docs/concepts/split-brain.md:68-72: the proof holds only if db003 has not been on TL=N+1 at any point since db002's promotion, and only if no non-standby walsender can carry an SSN member name. Downgrade the conclusion from "provably" to "provably, given that db003's timeline history shows no TL=N+1 lineage". Note in the same paragraph that the tool cannot currently check that premise (nothing reads db003's timeline history; PrimaryHealthCheckResult::_fork_lsn_for at src/v2/scan/health_check_primary/timeline_history.rs:41 is dead) and so the 'confident' verdict is confident only under an unchecked assumption.
- **Alternatives rejected:** Making the resolver check db003's history file: rejected as the fix location here -- the immediate defect is that the spec asserts a proof it does not have; wiring a check is the §7 capture-first work and should not be smuggled in as a doc fix. Removing the 3-node proof entirely: rejected -- the instantaneous version is genuinely useful and ADR-002 §7 depends on it.

### [medium | diverges] ADR lines docs/concepts/split-brain.md:44
- **Claim:** "(If `synchronous_commit` is weakened, this inference breaks -- which is exactly why weakened `synchronous_commit` is its own hard `Refuse` gate, ADR-002 §2. Past that gate, the inference holds.)"
- **Justification:** "Past that gate, the inference holds" is stronger than the gate can support on two axes. Scope: synchronous_commit is settable per-role (ALTER ROLE ... SET), per-database (ALTER DATABASE ... SET) and per-session/per-transaction (SET synchronous_commit = local), none of which appear in the cluster-level pg_settings value the gate reads. Time: the gate reads the value at scan time, but the inference is about writes acked minutes to hours earlier, and the GUC is SIGHUP-reloadable. A primary running synchronous_commit=local while the writes were acked, then reloaded to 'on' before the scan, passes the gate and defeats the inference.
- **Code:** `src/v2/analyze/split_brain.rs:164-174 -- `let v = h.configuration.get("synchronous_commit").map_or("", String::as_str); if WEAKENED_SYNCHRONOUS_COMMIT.contains(&v)` reads one scalar per primary from `configuration`, i.e. the cluster-level pg_settings value only`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** On each candidate primary: `SELECT setconfig FROM pg_db_role_setting;` and `SELECT setting, source, sourcefile FROM pg_settings WHERE name = 'synchronous_commit';` -- any per-role/per-db override containing synchronous_commit, or source='session', falsifies the blanket claim.
- **Fix (adr-text):** Replace "Past that gate, the inference holds" with a bounded statement: the gate rules out a *cluster-default* weakening observed at scan time; it does not rule out per-role/per-database/per-session overrides, nor a weakening that was in force when the writes were acked and has since been reloaded away. If the stronger claim is wanted, the scanner must additionally capture `SELECT setdatabase, setrole, setconfig FROM pg_db_role_setting` and `SELECT source, sourcefile, pending_restart FROM pg_settings WHERE name='synchronous_commit'`, which is a §5 collection change, not a doc change.
- **Alternatives rejected:** Widening WEAKENED_SYNCHRONOUS_COMMIT: rejected -- the value set is correct; the problem is which scopes and which point in time are observed, which no value list can fix.

### [high | diverges] ADR lines docs/concepts/split-brain.md:15, 17
- **Claim:** The line-15 invariant "an isolated primary ... cannot [ack] ... This is why write divergence is normally *structurally* prevented" is stated with `synchronous_standby_names` non-empty as an unstated precondition, and neither ADR-002 nor the code ever checks it.
- **Justification:** With an empty synchronous_standby_names, synchronous_commit='on' degrades to local flush and the primary acks freely with zero standbys -- the invariant the entire subsystem rests on simply does not apply. Ground truth #4: parse("") returns None so emit_quorum_findings skips the primary entirely, meaning a primary with sync rep effectively disabled produces *no* PrimaryQuorumUnsatisfied finding and is silently treated as if the invariant held. ADR-002:107 compounds this by declaring synchronous_standby_names issues "Not Refuse-worthy regardless". A promoted node whose SSN was cleared by failover tooling is exactly the node the concepts doc calls "the empty branch".
- **Code:** `src/v2/analyze/sync_standby_names.rs:26-29 `let s = input.trim(); if s.is_empty() { return None; }` feeding src/v2/analyze/split_brain.rs:650-652 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue; };``
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** On a freshly promoted node in this fleet: `SELECT name, setting FROM pg_settings WHERE name IN ('synchronous_standby_names','synchronous_commit');` immediately after a repmgr promotion, to establish whether repmgr clears or preserves SSN on promotion.
- **Fix (combination):** Doc: state the precondition explicitly at docs/concepts/split-brain.md:15 -- the invariant requires a non-empty synchronous_standby_names naming at least `count` reachable members; with SSN empty the primary is asynchronous and the no-divergence argument is void. Code: in the §2 sanity-gate loop (src/v2/analyze/split_brain.rs:159-174) treat an empty/whitespace synchronous_standby_names on a candidate primary the same way empty synchronous_commit is treated -- it is the same failure (durability not actually enforced). Reusing SynchronousCommitWeakened with value "synchronous_standby_names=''" is the minimal change; a distinct finding variant is cleaner but adds a writer arm.
- **Alternatives rejected:** Making sync_standby_names::parse("") return Quorum{count:0}: rejected -- that would make emit_quorum_findings report a *satisfied* quorum for an asynchronous primary, which is worse than silence. Handling it only in emit_quorum_findings: rejected -- the correct consequence is a Refuse-class sanity gate, not an informational quorum finding.

### [medium | diverges] ADR lines docs/concepts/split-brain.md:42, 57, 87
- **Claim:** "compare the replica's **applied** LSN against the inter-primary fork X" (line 57), and "a replica whose **flushed/applied LSN** is past the fork X ... is *proof* that the lower-TL primary client-acknowledged writes" (line 42).
- **Justification:** Contradicts ADR-002 §5, which designates `pg_last_wal_receive_lsn()` as "received/flushed position (**the ack-relevant one** under `synchronous_commit=on`)" and lists replay separately. Under the doc's own stated `synchronous_commit = on`, the ack is gated on the standby *flush*, and applied always trails flushed, so anchoring on applied under-detects -- the false-negative direction on a safety gate whose entire job is to stop a destructive action. The doc treats "flushed/applied" as interchangeable at line 42 and then silently picks the weaker of the two at lines 57 and 87.
- **Code:** `src/v2/scan/health_check_replica.rs:88-89 `'last_wal_replay_lsn', pg_last_wal_replay_lsn()::text, 'last_wal_receive_lsn', pg_last_wal_receive_lsn()::text` -- the scanner captures both, so the doc's choice is a spec decision, not a data limitation; neither field is read by any consumer yet`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None needed to settle the spec question; the postgres semantics are settled. To confirm the gap is non-trivial on this fleet: `SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn());` on db003 under load.
- **Fix (adr-text):** Fix docs/concepts/split-brain.md:42 to name one field, not two: under synchronous_commit in {on, remote_flush} the ack-relevant position is the *received/flushed* LSN (`pg_last_wal_receive_lsn()`); under remote_apply it is the *applied* LSN (`pg_last_wal_replay_lsn()`). Change line 57 and line 87 to say "the ack-relevant LSN for the primary's synchronous_commit setting", and add a one-line table mapping the three valid settings to the field. Because this fleet runs remote_apply (see the stale-example finding), state which field applies here.
- **Alternatives rejected:** Always using the received LSN as the conservative choice: rejected -- it over-fires under remote_apply, and ADR-002 §7 already argues that over-caution on safe cases is a real cost. Always using applied: rejected -- that is the current text and it is the unsafe direction under `on`.

### [medium | self-contradictory] ADR lines docs/concepts/split-brain.md:53, 72, 84
- **Claim:** "This is why, when db003's allegiance is observable, the verdict can be **confident** ... not merely conservative" (line 72/84) -- i.e. C-c yields no Refuse.
- **Justification:** Three-way disagreement on one behaviour. The concepts doc and ADR-002 case-matrix row C-c ("`DivergentReplicaWal(db003, ...)` is **informational, not Refuse**") say C-c is confident; ADR-002 §4 item 4 says "`DivergentReplicaWal`, when present, ... **MUST set `Confidence::Refuse`**"; the code implements the unconditional-Refuse side. If detection is ever built to the concepts doc's spec (anchored on the inter-primary fork, which fires in C-c by construction -- ADR-002:245 says so explicitly), the code will Refuse on exactly the case both narrative texts call provably safe.
- **Code:** `src/v2/analyze/split_brain.rs:396-399 `SplitBrainFinding::SystemIdentifierMismatch { .. } | SplitBrainFinding::SynchronousCommitWeakened { .. } | SplitBrainFinding::DivergentReplicaWal { .. } => Confidence::Refuse,` -- unconditional, no C-c carve-out`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** True | **Destructive text:** False
- **Fix (combination):** Pick one and make all three agree. The concepts doc's position is the defensible one only if the 3-node-proof premise gap is closed first (see that finding); until then ADR-002 §4.4's unconditional Refuse is the safe reading. Minimal resolution: strike "informational, not Refuse" from ADR-002 C-c and soften docs/concepts/split-brain.md:72/84 from "confident" to "the safety signal is explainable, but the tool still declines to auto-resolve", leaving split_brain.rs:396-399 as-is. If instead the confident reading wins, determine_confidence_level needs the finding's payload compared against the resolution (Refuse only when true_primary is the higher-TL node), which is the verdict-flip ADR-002 §7 defers.
- **Alternatives rejected:** Leaving it until the finding is emitted: rejected -- the deferral is why the contradiction is cheap to fix now; once an emitter exists the disagreement becomes a live wrong-verdict bug rather than a paragraph edit.

### [medium | implemented] ADR lines docs/concepts/split-brain.md:38, 46-53
- **Claim:** The verdict-vs-safety split is defined at "Two questions, not one" (lines 46-53) and ADR-002:11 relies on that definition.
- **Justification:** The split IS defined there, clearly and correctly, and the ADR's structural honouring of it is real: `resolution` carries question 1 and `confidence` carries question 2 (ADR-002:124). The code preserves that shape. The caveat is that the operator-facing rendering collapses it: any safety-axis Refuse that is not one of the two sanity gates prints the content-free literal "sanity gate failed", and the resolution text is suppressed, so the operator learns neither the pick nor the safety reason. Reported as implemented-with-a-rendering-gap rather than diverges, because nothing emits a safety-axis finding today.
- **Code:** `src/v2/writer/build.rs:672-684 -- format_refuse maps ReplicaWalReceiverStale, PrimaryDoesNotSeeReplica, BidirectionalFlushingConfirmed, ReplicaInCatchup, PrimaryQuorumUnsatisfied and DivergentReplicaWal all to `None`, then `.unwrap_or_else(|| "sanity gate failed".to_owned())``
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (writer):** Add a DivergentReplicaWal arm to format_refuse rendering the evidence string ADR-002 §4 item 4 already specifies verbatim: "divergent committed WAL -- {replica} flushed past TL={fork_tli} fork @ {fork_lsn}; acked writes may exist only on lower TL". Keep the "sanity gate failed" fallback for the genuinely-unreachable arms but make it name the finding discriminant so it is never content-free.
- **Alternatives rejected:** Adding a separate `safety: Option<...>` field to SplitBrainInfo to make the two axes distinct types: rejected -- ADR-002:252 puts multi-dimensional verdicts out of scope, and the resolution/confidence pair already encodes the split adequately.

### [low | stale] ADR lines docs/concepts/split-brain.md:78
- **Claim:** "the tool's job is only to name the divergent node and the canonical source: it does not attempt an in-place reconciliation."
- **Justification:** Residue of the pre-2026-06-07 rebuild-pointer framing. ADR-002 §4 item 4 as revised defers all remediation naming -- "surface evidence + `Refuse`, not an action ... and stop" -- and the writer names no divergent node and no canonical source. The doc states as present-tense tool behaviour something the ADR explicitly deferred in the same revision the rest of the section was updated for.
- **Code:** `src/v2/writer/build.rs:665-686 (format_refuse) and src/v2/writer/build.rs:691-724 (format_resolution) -- neither emits a rebuild target or a canonical-source name; format_resolution's parentheticals are hardcoded per variant`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Change docs/concepts/split-brain.md:78 to future/conditional voice: "when the remediation string returns (ADR-002 §7), the tool's job will be only to name the divergent node and the canonical source". Add "today it surfaces evidence and Refuses without naming an action".
- **Alternatives rejected:** Implementing the naming now: rejected -- ADR-002 §7 defers it precisely because naming a direction before the verdict-flip exists prints a backwards, data-destroying instruction.

### [low | stale] ADR lines docs/concepts/split-brain.md:87
- **Claim:** "reading the replica's position from the **control file** (`pg_control_checkpoint().timeline_id`, `pg_last_wal_replay_lsn()`) ... (ADR-002 §7)"
- **Justification:** Partially updated for 2026-09-10 -- it does carry the shared-memory/high-water-mark caveat -- but three things are behind the revision. (a) It still calls both positions "control file", the exact shorthand ADR-002:219 flags as loose, and omits the operative consequence: the LSNs are zeroed by a postmaster restart and do not survive one, so the 'survives with no receiver' selling point is weaker than stated. (b) It never mentions the nullity result (SQL NULL, never 0/0). (c) It never mentions the finding that a non-NULL replay LSN is not evidence a node is a replica and freezes at the promotion LSN. It also cites "(ADR-002 §7)" for the staleness point, which lives in ADR-002 §5 (line 215).
- **Code:** `src/v2/scan/health_check_replica.rs:30-33 `pub last_wal_replay_lsn: Option<String>, ... pub last_wal_receive_lsn: Option<String>` -- Option shape matches the nullity finding; src/v2/scan/health_check_replica.rs:63 `'timeline_id', (SELECT timeline_id FROM pg_control_checkpoint())` is the only genuinely control-file-sourced value`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** At docs/concepts/split-brain.md:87 change "from the control file (pg_control_checkpoint().timeline_id, pg_last_wal_replay_lsn())" to "from the control file (`pg_control_checkpoint().timeline_id`) and shared memory (`pg_last_wal_replay_lsn()`)", add "and are zeroed by a postmaster restart", and repoint the staleness citation from §7 to §5. Add one sentence carrying the 2026-09-10 promoted-primary result: both LSNs freeze at the promotion LSN, so a non-NULL replay LSN says nothing about whether the node is in recovery.
- **Alternatives rejected:** Leaving it and relying on ADR-002 §5: rejected -- ADR-002:11 makes the concepts doc the normative source for the domain model, and a reader who only reads the concepts doc would conclude the position survives a restart.

### [low | diverges] ADR lines docs/concepts/split-brain.md:7, 15, 42, 70
- **Claim:** Running example: "Three nodes, `synchronous_standby_names = 'ANY 1 (A, B)'`, `synchronous_commit = on`", with the whole inference chain at lines 15, 42, 44, 70 conditioned on `= on`.
- **Justification:** The fleet runs `synchronous_commit = remote_apply`, not `on` (ground truth #1, confirmed in the fixture). docs/concepts/README.md sets the rule "we must keep them grounded in a running example", and the running example does not match the only captured cluster. This is not cosmetic: `on` acks on standby flush and `remote_apply` acks on standby apply, which is precisely what decides the received-vs-applied LSN question in the anchoring finding above.
- **Code:** `tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json `"synchronous_commit": "remote_apply"` and `"synchronous_standby_names": "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"`; ADR-002:96 lists on/remote_apply/remote_flush as all valid`
- **Reachable in code:** False | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Change the running example at docs/concepts/split-brain.md:7 to `synchronous_commit = remote_apply` to match the captured cluster, and generalise lines 15/42/44/70 to "synchronous_commit strong enough to wait for the standby (on, remote_flush, remote_apply)" with a parenthetical noting which position each waits on. Optionally use the real name forms (dev_pg_app001_db00N) in the example, which would also have made the resolver's name-form bug visible when the doc was written.
- **Alternatives rejected:** Keeping `on` as a simplifying abstraction: rejected -- the doc's own README makes grounding in a running example the rule, and the difference between `on` and `remote_apply` changes which LSN the safety check must read.

### [low | diverges] ADR lines docs/concepts/split-brain.md:89
- **Claim:** "When db003 is re-pointed at db002 while already past X on TL=N, db002 refuses to stream it"
- **Justification:** Misattributes the refusal. The primary streams whatever is requested; the FATAL "new timeline N+1 forked off current database system timeline N before current recovery point X/X" is raised by the *standby's* startup process after it fetches the timeline history and finds the switchpoint behind its own recovery point. The doc's very next sentence gets this right ("The standby logs a FATAL"), so the two halves of one paragraph disagree. It matters for triage: the operator will look for the error on the wrong host.
- **Code:** ``
- **Reachable in code:** False | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** The §7 capture would settle it directly: server logs from both hosts during a real wedged state, plus `SELECT * FROM pg_stat_wal_receiver;` on the wedged standby.
- **Fix (adr-text):** Change docs/concepts/split-brain.md:89 to "db003 refuses to follow db002 -- it is ahead of where TL=N+1 forked" and note the FATAL appears in db003's server log, not db002's.
- **Alternatives rejected:** None -- single-clause correction.

### [low | self-contradictory] ADR lines 7
- **Claim:** ADR-002:7 (revision header): "a conservative `Refuse`-only floor is shippable today (§7)."
- **Justification:** ADR-002:245 says the opposite in the section the header points at: "which is why **no** conservative 'Refuse-only floor' is shipped in the interim." docs/concepts/split-brain.md:87 independently sides with §7 ("divergence detection is deferred behind capturing that evidence first"), and the code ships no floor -- nothing constructs DivergentReplicaWal outside tests. The concepts doc and the code agree; the revision header is the outlier. Reporting from this section because the concepts doc is the tiebreaker the ADR itself delegates to.
- **Code:** `src/v2/analyze/split_brain.rs:102 (variant definition), :399 (confidence mapping) and :782 (test construction) are the only three occurrences -- no production emitter exists`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Strike "a conservative `Refuse`-only floor is shippable today (§7)" from ADR-002:7 and replace with "no interim floor is shipped; see §7 for why a partial trigger would be worse than none".
- **Alternatives rejected:** Shipping the floor to make the header true: rejected -- §7's argument against it (fires on the safe case, misses the dangerous one) is sound and is the whole point of the 2026-06-07 revision.

### [low | stale] ADR lines README.md:231, 290
- **Claim:** README.md sample output: "SplitBrain: replica overrides timeline (7 < 8)", and README.md:290 "**Override Case**: Replicas override timeline (isolated failed promotion)".
- **Justification:** README.md:231 prints verbatim the string ADR-002:157 lists as **Not acceptable** ("`SplitBrain: replica overrides timeline (N < N+1)` -- that phrasing is paradox-shaped and was the trigger for the rename"). The code no longer produces it. README is the operator-facing entry point, so the paradox phrasing the rename was meant to eliminate still teaches itself to every new reader.
- **Code:** `src/v2/writer/build.rs:709 `"SplitBrain: {} has quorum (lower TL={}), fence {} (TL={}, quorum-blocked)"` -- the actual output for this verdict`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Update the README.md:231 sample row to the current output, e.g. `SplitBrain: db001 has quorum (lower TL=7), fence db002 (TL=8, quorum-blocked)`, and rename the README.md:290 bullet from "Override Case: Replicas override timeline" to "Lower Timeline Has Quorum: the higher-TL primary is quorum-blocked; the lower-TL primary still has a flushing replica".
- **Alternatives rejected:** Leaving the README as illustrative: rejected -- ADR-002:265 requires "the writer and any external readers must be updated in the same PR", and a sample output is the most externally-read consumer there is.

### [low | stale] ADR lines SPEC.md:401, 405-412
- **Claim:** SPEC.md §7.2 item 4: "**ReplicaOverridesTimeline**: Replicas follow lower-timeline primary", plus §7.3 "Higher timeline primary can be safely shut down and marked for rebuild / Tool should recommend this action".
- **Justification:** SPEC.md is the only file outside ADR-002 still carrying the pre-rename variant name (the concepts doc does not -- it uses LowerTimelineHasQuorum-consistent language throughout, and the term ReplicaOverridesTimeline appears nowhere in it). §7.3's remediation direction happens to agree with the 2026-06-07 correction (rebuild the higher-TL node), but "Tool should recommend this action" contradicts ADR-002 §4 item 4, which defers all remediation text, and the two §7.3 conditions are garbled ("follows lower timeline on same timeline" / "with higher timeline itself"). Negative results for the rest of the sweep: CHANGELOG.md contains no stale split-brain claim (only "add Confidence and findings to SplitBrainInfo") and no DivergentReplicaWal claim anywhere; TODO.md's split-brain content is a refactoring note with no behavioural assertions; no file in the repo claims DivergentReplicaWal is implemented.
- **Code:** `src/v2/analyze/split_brain.rs:34 `LowerTimelineHasQuorum {` -- the only name that exists in code; no rebuild recommendation is produced anywhere in src/v2/writer/build.rs:691-724`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** In SPEC.md:401 rename the strategy to LowerTimelineHasQuorum and describe it as "the higher-TL primary is quorum-blocked; a replica is actively flushing for the lower-TL primary". In §7.3, either delete the two garbled bullets or replace them with a pointer to ADR-002's case matrix and docs/concepts/split-brain.md, and change "Tool should recommend this action" to note that remediation text is deferred (ADR-002 §7).
- **Alternatives rejected:** Deleting SPEC.md §7 entirely in favour of the ADR: rejected -- out of scope for a rename cleanup and SPEC.md §7.1/7.4 still carry non-duplicated requirements.


## Deferral consistency of ADR-002 sections 6 and 7 across every reference site (ADR status block, matrix rows C-c/C-g, fact 4, S4 items 1/4 + concatenation bullet, S5 capture-first, S6, S7, docs/concepts/split-brain.md, docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md, and the dormant code wiring)

### [critical | diverges] ADR lines 47, 54
- **Claim:** C-g: "`DivergentReplicaWal(db003) -> Refuse` must override the pick." and fact 4: "The only thing standing between that pick and acknowledged-data loss is `DivergentReplicaWal -> Refuse`." No reference site states what the tool actually prints in C-g today.
- **Justification:** Every site says WHAT is deferred and WHY; not one states the interim operator-facing output in the deferred case, which today is an explicit demote instruction aimed at the node holding the acked writes.
- **Code:** `src/v2/analyze/split_brain.rs:485 "// No replica evidence - trust timeline" -> :495 "resolution: SplitBrainResolution::HigherTimeline {"; src/v2/writer/build.rs:716 "\"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)\""`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** A capture of the wedged state. On a cluster reproducing C-g (db003 re-pointed at the higher-TL primary while past the fork, walreceiver FATAL 'new timeline N+1 forked off current database system timeline N before current recovery point'), run per node: SELECT pg_is_in_recovery(), (SELECT timeline_id FROM pg_control_checkpoint()), pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn(), (SELECT count(*) FROM pg_stat_wal_receiver), (SELECT row_to_json(w) FROM pg_stat_wal_receiver w); plus the higher-TL primary's pg_wal/*.history. That settles both whether pg_stat_wal_receiver is empty and whether the control-file LSN is past the fork.
- **Fix (combination):** (a) ADR text: add one sentence to row C-g and to fact 4 stating the CURRENT behaviour explicitly -- 'Until detection lands, C-g resolves to HigherTimeline with Confidence::Conflicting and prints "SplitBrain: db002 has quorum (TL=N+1), demote db001 (TL=N, no live replicas)". Nothing today stands between that pick and acknowledged-data loss.' The present wording ('must override the pick', 'the only thing standing between') reads as shipped behaviour. (b) Consider a resolver guard: in resolve_with_different_timelines the else branch (split_brain.rs:484-501) fires precisely when NO replica gate-passes for anyone -- the C-g precondition. Emitting Confidence::Conflicting there unconditionally, or a HigherTimelineNoFollowers finding, would at minimum stop the text reading as a confident demote. Note the finding must be tied to the resolution shape, not to DivergentReplicaWal, because the latter is deferred.
- **Alternatives rejected:** Rejected 'just build the detection now': S7's argument (the only trigger available today keys off wal_receiver, which the wedged replica does not expose) is sound, and the orchestrator confirms no captured C-g exists. Rejected 'change the HigherTimeline template alone': the template is correct for C-d/C-f where HigherTimeline is genuinely right; the problem is that C-g is indistinguishable from C-f with current data.

### [high | self-contradictory] ADR lines 7, 245
- **Claim:** Status block: "a conservative `Refuse`-only floor is shippable today (§7)." versus S7: "which is why no conservative \"Refuse-only floor\" is shipped in the interim."
- **Justification:** Both sentences were introduced in the SAME commit (jj wrsstmwvvpol / 9276b78 'docs: correct ADR-002 divergent-WAL design'), so this is not stale text left standing across revisions -- the revision shipped both halves of a contradiction, and the status block cites S7 as its own source.
- **Code:** `No code corresponds -- nothing emits or renders the finding; src/v2/writer/build.rs:682 "| SplitBrainFinding::DivergentReplicaWal { .. } => None," is the only writer-side handling, and it renders nothing.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Delete the trailing clause of line 7 and replace with S7's actual position: '...deferred until designed from real data. No interim Refuse-only floor ships: the only trigger available today keys off pg_stat_wal_receiver, which is anti-correlated with the dangerous case (S7).' S7's text survives, because (i) it carries the argument and the status block only carries the conclusion, (ii) four other sites (S4 item 1, S4 concatenation bullet, plan Commit 11/12, docs/concepts/split-brain.md:87) agree with S7, and (iii) the code matches S7. 'Shippable today' is also stronger than a scheduling choice: S7 denies the premise, arguing any floor buildable today would add false confidence in C-g.
- **Alternatives rejected:** Rejected keeping line 7 and softening S7: the code ships no floor, so S7 is the description that matches reality, and line 7 is the first thing a reader sees -- it currently invites a future implementer to build exactly the wal_receiver-keyed trigger S7 rejects.

### [high | self-contradictory] ADR lines 159-161
- **Claim:** S4 item 4 heading: "**`DivergentReplicaWal`, when present, MUST surface inline in the SplitBrain short string** and MUST set `Confidence::Refuse`." plus its bullet 2, which specifies the interim render verbatim: "Render the raw facts inline -- e.g. `REFUSE/SplitBrain: divergent committed WAL -- db003 flushed past TL=N fork @ <lsn>; acked writes may exist only on lower TL` -- and stop."
- **Justification:** Item 4 mandates a renderer in the present tense (MUST, with a concrete template) while item 1's carve-out (line 156) says the carve-out 'is dormant' and the concatenation bullet (line 178) says 'rendering is deferred (S7); nothing emits it today'. All three were written in the same 2026-06-07 commit. The 2026-06-07 rewrite deferred only the *remediation verb*, leaving the *inline surfacing* MUST standing.
- **Code:** `src/v2/writer/build.rs:682 "| SplitBrainFinding::DivergentReplicaWal { .. } => None," and :684 ".unwrap_or_else(|| \"sanity gate failed\".to_owned());" -- the writer has no DivergentReplicaWal renderer at all, so under a literal reading of item 4 the completed Commit 12 is non-conforming.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Rewrite item 4's heading to be conditional on detection landing: 'DivergentReplicaWal is deferred in full (S7). When detection lands it MUST surface inline and MUST set Confidence::Refuse; the interim rendering is evidence-only, per the template below.' Keep bullet 2's template -- it is valuable design guidance and it is the *correct* interim rendering -- but mark it future, not v1 spec. Item 1's carve-out wording ('deferred along with the finding's detection; nothing emits it today, so the carve-out is dormant') is the phrasing that should survive, because it is the only one of the three that is simultaneously true of the ADR's intent and of the code.
- **Alternatives rejected:** Rejected implementing item 4 in the writer instead: adding a renderer for a finding nothing emits grows dead code and does not resolve the contradiction with line 178. Rejected deleting bullet 2's template: it is the corrected design and losing it invites the original backwards 'rebuild <replica> from <true-primary>' string to be reinvented.

### [high | diverges] ADR lines 247
- **Claim:** S7: "The `DivergentReplicaWal` variant already maps to `Confidence::Refuse` in `determine_confidence_level`, so nothing emits it today -- that wiring stays dormant until the detection is built from real data."
- **Justification:** The wiring is not neutral. A blanket DivergentReplicaWal -> Refuse contradicts matrix row C-c, which says the finding is 'informational, not Refuse' in the observable case, and S7 lines 242-243 which say Refuse is correct ONLY when db003's allegiance is unprovable. The dormant arm pre-commits the mapping the ADR itself says is wrong for C-c, and a test pins it.
- **Code:** `src/v2/analyze/split_brain.rs:399 "| SplitBrainFinding::DivergentReplicaWal { .. } => Confidence::Refuse,"; pinned by src/v2/analyze/split_brain.rs:781-790 "#[case::divergent_replica_wal(... Confidence::Refuse)]"`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** True | **Destructive text:** False
- **Fix (adr-text):** S7 line 247 should say what the dormant wiring actually encodes and flag it as a decision still open: 'determine_confidence_level currently maps the variant unconditionally to Refuse. That is correct only for the unobservable (C-g) case; per C-c, an observable-allegiance emission must NOT force Refuse. Whoever builds detection must split the mapping (or gate it on observability) before the first emitter lands -- and update the finding_to_confidence rstest case at split_brain.rs:781, which currently pins the unconditional mapping.' Leave the code alone: with no emitter, changing the arm now is speculative, and the rstest is the right place for the warning to bite.
- **Alternatives rejected:** Rejected changing determine_confidence_level now to take an observability flag: that is designing the deferred feature, which S7 explicitly says must wait for a real capture. Rejected deleting the arm: an exhaustive match means it must handle the variant somehow, and Refuse is the safe default of the two.

### [medium | diverges] ADR lines 156, 247
- **Claim:** The dormant wiring as a trap: a Refuse mapping with no emitter and no renderer. S4 item 1: "The resolution-variant text MUST NOT appear when Refuse fires, to prevent operators from acting on a winner pick that is not actionable."
- **Justification:** Refuse suppression is implemented only for the short string. build_split_brain_views is confidence-blind, so the report's primary column still renders 'true_primary vs stale' -- i.e. it still names the winner the resolver refuses to stand behind. This is reachable TODAY via the sysid gate, and it is what makes the DivergentReplicaWal day-one failure concrete.
- **Code:** `src/v2/writer/build.rs:200-205 "let primary = PrimaryView::SplitBrain { true_primary: NodeView { display: extract_db_number(&info.true_primary), ... }, stale };" (no Confidence check anywhere in build_split_brain_views); rendered by src/v2/writer/view.rs:76-77 "format!(\"{} vs {}\", true_primary.render(mode), stale_strs.join(\",\"))"`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** A scan of a cluster with a foreign-sysid replica (the reachable-today Refuse path) to confirm the rendered row shows 'REFUSE/SplitBrain: system_identifier mismatch (...)' in the reason column while the primary column still reads 'db002 vs db001'. Reproducible offline by feeding a mismatched-sysid fixture through the writer.
- **Fix (writer):** Day-one failure, precisely: the moment an emitter pushes DivergentReplicaWal into SplitBrainInfo.findings, (1) determine_confidence_level (split_brain.rs:399) flips confidence to Refuse with no other code change and no compile error -- the match at build.rs:677-682 already lists the variant, so exhaustiveness gives no warning; (2) split_brain_reason routes to format_refuse, whose find_map returns None for the variant, so the operator sees the literal string 'REFUSE/SplitBrain: sanity gate failed' -- the least informative possible text for the most dangerous state, and the divergence facts (replica_flushed_lsn, fork_lsn) appear only in details_json; (3) if a real sanity gate is also present, find_map returns the sysid/sync_commit text first and the divergence is absent from `short` entirely; (4) the primary column still renders 'db002 vs db001', pointing at demoting the node that holds the acked writes. Minimum fix when detection lands: add the DivergentReplicaWal arm to format_refuse per S4 line 161's template, hoist it ahead of the other gates in find_map ordering, and make build_split_brain_views suppress or mark the winner pick under Confidence::Refuse. Independently of the deferral, the last item should land now, since Refuse is already reachable via the sysid gate.
- **Alternatives rejected:** Rejected leaving the writer alone and relying on details_json: S4 line 154 states the short-string information content is 'mandated, not polish'. Rejected a `todo!()`/panic in the DivergentReplicaWal arm as a tripwire: it would turn a rendering gap into a crash on the most operationally sensitive report.

### [medium | stale] ADR lines plan 207-221 (vs plan 81, 155, 182, 204)
- **Claim:** Plan Commit 12 commit message: "appends DivergentReplicaWal as a rebuild instruction ... the DivergentReplicaWal finding is surfaced inline with rebuild guidance."
- **Justification:** The plan's own Commit 12 goal (line 81) says '(Revised 2026-06-07: the DivergentReplicaWal concatenation is dropped ... the writer has nothing to render for it yet)', and three checkbox items in the same section are struck out as deferred -- but the prescribed commit message was never updated, so it instructs the implementer to record a change that did not happen.
- **Code:** `src/v2/writer/build.rs:654-690 -- split_brain_reason/format_refuse contain no rebuild string and no DivergentReplicaWal rendering, so the prescribed message would be false if used verbatim.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (plan-text):** Rewrite the Commit 12 message block to drop both DivergentReplicaWal sentences: 'feat(writer): split-brain short-string contract per ADR-002 S4 / The format_reason SplitBrain arm now derives action-text via the variant-to-template mapping, inlines PrimaryQuorumUnsatisfied via the per-variant parenthetical, and overrides the resolution text with REFUSE/<gate> when Confidence::Refuse. DivergentReplicaWal rendering is deferred (ADR-002 S7).' The plan text at line 81 survives; the message block is the stale half.
- **Alternatives rejected:** Rejected leaving it as historical record: the plan is an executable instruction document with live checkboxes, not an archive, and this block is the literal text a future implementer would paste.

### [medium | stale] ADR lines plan 5, 7
- **Claim:** Plan header: "**Goal:** Implement ADR-002 -- refine the split-brain resolver with ... divergent-WAL detection, and an operator-actionable short-string contract" and "**Architecture:** ... resolver core (gate + sanity gates + findings) -> divergent-WAL -> writer integration."
- **Justification:** Both lines still name divergent-WAL detection as a deliverable of this plan, contradicting the plan's own Commit 11 revision note (line 23, 'this commit becomes capture-only') and Commit 12 note (line 81). A reader who stops at the header comes away believing detection is in scope.
- **Code:** `No code corresponds -- no detection exists; src/v2/analyze/split_brain.rs contains no emitter for DivergentReplicaWal (rg finds only the variant definition at :102, the confidence arm at :399, and the rstest at :782).`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (plan-text):** Replace 'divergent-WAL detection' with 'divergent-WAL evidence capture (detection deferred, ADR-002 S7)' in the Goal, and change the Architecture arrow chain to '... -> divergent-WAL capture-only -> writer integration'. The Commit 11/12 revision notes survive; the header is the stale half.
- **Alternatives rejected:** Rejected adding a global 'see revision notes' banner instead: the header is the summary a reader trusts, and the file already carries per-commit revision notes that this header contradicts.

### [medium | stale] ADR lines plan 228
- **Claim:** Plan post-implementation: "Decide whether the top-level `DivergentReplicaWal` finding emission (outside the SplitBrain reason) needs to land in this PR or a follow-up -- the writer correctness is already satisfied by the dual-emission inside `SplitBrainInfo.findings`."
- **Justification:** This is an open checkbox presupposing that emission exists and that a 'dual-emission' design is in force. The dual-emission paragraph was DELETED from the ADR in the 2026-06-07 revision (jj diff -r wrsstmwvvpol shows old S7 line 212, 'emitted as both a top-level finding ... AND included in SplitBrainInfo.findings', removed). The plan retained the reference to a design the ADR no longer contains.
- **Code:** `src/v2/analyze/split_brain.rs -- no top-level or SplitBrainInfo-level emission of DivergentReplicaWal exists, so there is no 'dual-emission' to be satisfied by.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (plan-text):** Strike the checkbox and replace with: 'Top-level DivergentReplicaWal emission: moot -- the finding is deferred in full (ADR-002 S7). Revisit emission placement (top-level vs SplitBrainInfo.findings) when detection is designed from a captured C-g.'
- **Alternatives rejected:** Rejected leaving it as an open question: an unchecked box implies a live decision, and answering it as written would produce an emitter -- the exact thing S7 defers.

### [medium | self-contradictory] ADR lines 233 (vs plan 72)
- **Claim:** S7 defines the variant as "`DivergentReplicaWal { replica_node, replica_received_tli, replica_flushed_lsn, fork_tli, fork_lsn }`" while the plan's deferred design says "Source the replica position from the control-file LSN (this commit's new fields), not `wr.flushed_lsn` -- the wedged replica may have no `wal_receiver`."
- **Justification:** The payload field names replica_received_tli / replica_flushed_lsn are the pg_stat_wal_receiver columns that S7's own data-gap argument rules out as the source. S7 restates the payload unchanged while arguing its source is unusable, and never flags the mismatch.
- **Code:** `src/v2/analyze/split_brain.rs:102-108 "DivergentReplicaWal { replica_node: NodeName, replica_received_tli: i32, replica_flushed_lsn: String, fork_tli: i32, fork_lsn: String, }" -- compare src/v2/scan/health_check_replica.rs:29-33, the capture-first fields "last_wal_replay_lsn" / "last_wal_receive_lsn", which are top-level on ReplicaHealthCheckResult precisely so they survive wal_receiver == None.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Add to S7 after the variant restatement: 'The payload field names date from the original wal_receiver-sourced design. When detection lands, replica_received_tli should be sourced from pg_control_checkpoint().timeline_id and replica_flushed_lsn from last_wal_receive_lsn (S5), and the fields renamed accordingly (e.g. replica_control_tli / replica_received_lsn) so the payload does not misdescribe its own provenance.' The plan's line 72 wording survives -- it is the corrected design; S7's restatement is the half that lags.
- **Alternatives rejected:** Rejected renaming the fields in code now: the variant is dormant and unemitted, so a rename is churn with no behavioural effect, and the final shape should be settled against a real capture.

### [low | self-contradictory] ADR lines plan 35 (vs plan 45)
- **Claim:** Plan SQL comment: "'last_wal_receive_lsn', pg_last_wal_receive_lsn()::text, -- received/flushed (ack-relevant); may be 0/0 with no receiver"
- **Justification:** Ten lines below, the same plan says "`Option` because both functions return SQL NULL -- never `0/0` -- when their position is zero (PG17 `xlogfuncs.c` ..., validated 2026-09-10)". The SQL comment carries the pre-validation wording that the ADR corrected in jj umssqtnqtzzm/775f90e (old ADR line 211 said 'may be stale or 0/0'; it was replaced).
- **Code:** `src/v2/scan/health_check_replica.rs:255-265 "fn replica_health_check_handles_null_wal_positions()" with the comment "// PG17 returns SQL NULL, never 0/0, when the position is zero (xlogfuncs.c)." -- the shipped test encodes the corrected behaviour, so only the plan comment is wrong.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (plan-text):** Change the SQL comment to '-- received/flushed (ack-relevant); NULL, never 0/0, when zero'. The prose at plan line 45 survives.
- **Alternatives rejected:** Rejected treating this as harmless: a future detector written from the plan's SQL snippet would add a dead `== "0/0"` sentinel branch that never fires, masking the real hazard (a non-NULL stale high-water mark, ADR lines 215-217).

### [low | self-contradictory] ADR lines 247
- **Claim:** S7: "The `DivergentReplicaWal` variant already maps to `Confidence::Refuse` in `determine_confidence_level`, **so** nothing emits it today."
- **Justification:** Non-sequitur: the existence of a confidence mapping does not entail the absence of an emitter. The two clauses are independent facts joined by a causal 'so'. The effect is to make the wiring read as verified-inert when in fact the ADR is asserting the absence of an emitter without pointing at anything.
- **Code:** `src/v2/analyze/split_brain.rs:399 (the mapping) is unrelated to the absence of an emitter; the absence is verifiable only by rg -- the sole non-test hits repo-wide are :102 (definition), :399 (mapping), and src/v2/writer/build.rs:682 (renders None).`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Split the sentence: 'No code constructs DivergentReplicaWal today (the only references are the variant definition, the determine_confidence_level arm, and a writer match arm that renders nothing). The variant maps unconditionally to Confidence::Refuse; that mapping stays dormant -- see the C-c caveat above -- until detection is built from real data.'
- **Alternatives rejected:** Rejected deleting the sentence: naming the dormant wiring is useful; the problem is only the false causal link and the unstated C-c conflict.

### [info | deferred-correct] ADR lines 121, 227-229, 263
- **Claim:** S6 two-pass stability check: "Future enhancement ... Out of scope for this ADR", cross-referenced by "`Verified` is deliberately omitted from v1. Promoting `BestEffort` to `Verified` requires a two-pass stability check (deferred -- S6)" and "Verdicts previously expressed as authoritative are now explicitly `BestEffort` until two-pass stability lands."
- **Justification:** All four sites (ADR S3, S6, Consequences; plan 'Out of scope (follow-up work)' line 234) say the same thing, the code has no Verified variant, and the Ord derivation makes BestEffort the ceiling so 'capped at BestEffort' (lines 58, 254) is automatically true. No divergence found -- unlike S7, S6's deferral is stated consistently everywhere.
- **Code:** `src/v2/analyze/split_brain.rs:72-78 "pub enum Confidence { Refuse, Conflicting, BestEffort, }" -- no Verified; repo-wide rg for 'Verified' finds only rustls ServerCertVerified in src/v2/db.rs.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No change needed. Worth noting only as the contrast case: S6 is what a consistently-stated deferral looks like -- one normative sentence, repeated without elaboration, with no dormant wiring behind it. S7's inconsistencies all stem from the opposite: the variant, the confidence mapping, the payload shape and a render template were all specified before the deferral, and the deferral was applied to some of them and not others.

### [low | diverges] ADR lines 233, 247 (S7 deferral) vs code
- **Claim:** timeline_history.rs carries `#[expect(clippy::used_underscore_items, reason = "will be used shortly")]` over the tests for `_fork_lsn_for`, the fork-LSN reader S7's deferred design needs.
- **Justification:** 'Shortly' contradicts an indefinite deferral gated on a capture that has never occurred and may not. The suppression reason is the only in-code statement about the S7 timeline, and it disagrees with every doc site (ADR S7, plan Commit 11, docs/concepts/split-brain.md:87 'deferred behind capturing that evidence first').
- **Code:** `src/v2/scan/health_check_primary/timeline_history.rs:50 "#[expect(clippy::used_underscore_items, reason = \"will be used shortly\")]"; the underscore-prefixed dead reader is at :41 "pub fn _fork_lsn_for(&self, from_tli: i32) -> Option<String> {"`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Change the reason string to point at the gate rather than a timeline: reason = \"fork-LSN reader for the deferred DivergentReplicaWal detection; see ADR-002 S7 (blocked on a captured C-g)\". Correspondingly, S7's capture-first paragraph should mention that the client-side fork-LSN parser already exists and is deliberately unwired, since S7 currently lists only the S5 data capture as what has landed. The dead-code warnings themselves are out of scope per the audit brief; only the misleading reason string and the ADR's silence about the parser are in scope.
- **Alternatives rejected:** Rejected deleting _fork_lsn_for: the plan (line 75) explicitly says 'The old lsn_to_u64 helper and the per-replica fork comparison remain a fine starting point mechanically', so keeping the parser is deliberate -- it is only the 'shortly' that misstates the deferral.

### [info | unverifiable] ADR lines 213, 217
- **Claim:** S5 capture-first validation: "PG17 `xlogfuncs.c` guards each with `if (recptr == 0) PG_RETURN_NULL();`" and "Measured on a promoted primary ...: `pg_last_wal_replay_lsn()` and `pg_last_wal_receive_lsn()` both return `6FD/7C0000A0`, which is exactly the TL 21 -> 22 switch point in the `.history` captured from that cluster."
- **Justification:** The only captured fleet data in the repo (tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json) reports pg_version 15.14 on all three nodes and a timeline history topping out at TL=11, not TL 21->22. The measurement is from some cluster not represented in the repo, and the source citation is for a major version the fleet does not run. The capture-only decision itself is correctly and consistently stated -- this is about the evidence backing its validation note.
- **Code:** `src/v2/scan/health_check_replica.rs:255-265 encodes the NULL-never-0/0 claim as a unit test on hand-written JSON, not on captured output; tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json is the only real capture and its timeline_history ends at previous_tli 10.`
- **Reachable in code:** False | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** On a 15.x node in this fleet: SELECT version(); then SELECT pg_last_wal_receive_lsn() IS NULL, pg_last_wal_replay_lsn() IS NULL; on a freshly restarted primary that has never had a walreceiver in the current postmaster. To pin the promoted-primary claim, capture on the specific cluster: SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn(); plus the matching pg_wal/*.history, and add it as a fixture so the '6FD/7C0000A0 == TL 21->22 switch point' assertion is checkable in-repo.
- **Fix (adr-text):** Change 'PG17 xlogfuncs.c' to name the version actually verified and note the fleet version, e.g. 'xlogfuncs.c (unchanged from 9.x through 17; the fleet runs 15.14 per tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json)'. Attribute the 6FD/7C0000A0 measurement to the cluster and date it came from, or land the capture as a fixture. The guard's existence in PG15 is highly likely but is a plausibility argument until someone runs the query above.
- **Alternatives rejected:** Rejected deleting the measurement: it is the strongest evidence in S5 that these pointers freeze at promotion, which is the staleness hazard the whole section turns on. It needs sourcing, not removal.


## Deep dive: primary-side identity matching in split_brain.rs (ADR-002 §"Cluster assumptions" line 27, §1 line 73, case matrix rows C-a/C-b/C-c/C-e, §4 short-string contract) -- blast radius and refutation attempt

### [critical | diverges] ADR lines 27, 73
- **Claim:** "repmgr-set `application_name` equals the node name" (line 27), operationalised in §1 as "The primary's `pg_stat_replication` has a row whose `application_name` equals the replica's node name" (line 73).
- **Justification:** The assumption is false on the real fleet, and the code implements it literally, so the primary-side gate can never match and `replicas_following` is permanently empty.
- **Code:** `src/v2/analyze/split_brain.rs:307 `&& conn.application_name == replica.node_name` (inside `build_replica_following_map`'s `p_health.replication.iter().find(...)` at :305). Provenance chain, no normalisation at any hop: src/v2/node.rs:13-14 `#[serde(rename = "node_name")] pub name: String` <- portal JSON; src/v2/db.rs:57 `cfg.host(&node.name)` (so it must be a resolvable hostname); src/v2/scan.rs:118/173, src/v2/scan/health_check_primary.rs:248/265, src/v2/scan/health_check_replica.rs:135/152 all `node_name: node.name.clone()`; src/v2/analyze.rs:318 `resolve_split_brain(&primaries, &replicas)` passes AnalyzedNode through untouched.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Half of this is already settled by evidence, not inference. Fleet side (settled): /tmp/nodes_response.json, the tool's own 1-day cache of the database-portal inventory (src/database_portal.rs:8 `CACHE_PATH`), 1074 nodes -- every single node_name is FQDN form `<env>-<type>-<cluster>-db<NNN>.sto{1,2,3}.fnox.se`, 3 dots, zero underscores; 1074/1074 have repmgr_enabled=true; the label->underscore transform is injective over all 1074 (1074 distinct forms), so a deterministic mapping is well-defined and collision-free. Postgres side (one cluster only): tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json shows application_name `dev_pg_app001_db002` in the primary's pg_stat_replication AND, independently, `application_name=dev_pg_app001_db002` inside the replica's own `configuration.primary_conninfo`. To close the remaining gap across the other 357 clusters, run on any two prod primaries (e.g. prod-pg-app007-db001.sto1.fnox.se and a prod-pg-f*c*-db001): `SELECT application_name, client_addr, client_hostname FROM pg_stat_replication;` and on a replica `SHOW primary_conninfo;` plus `grep node_name /etc/repmgr.conf`. Confirming the underscore form on one prod cluster makes this fleet-wide.
- **Fix (combination):** Two edits. (a) Resolver identity: at split_brain.rs:307 replace the name equality with the IP identity already used everywhere else in the analyzer -- `conn.client_addr.as_deref() == Some(replica.ip_address.to_string().as_str())` -- keeping the existing `!conn.application_name.is_empty()` reject so §1's 'unmatchable empty app name' rule survives. checks.rs:194, :219, :246 already pair pg_stat_replication rows to AnalyzedNodes by `n.ip_address.to_string() == client_addr`, and the fixture confirms it holds (primary row client_addr 127.2.12.151 == db002's ip_address 127.2.12.151). (b) ADR text: rewrite line 27 and line 73 to state the real relationship -- application_name is the node_name's first DNS label with '-' replaced by '_' -- and record which identity the gate actually uses. Without (b) the next implementer re-introduces (a).
- **Alternatives rejected:** Scanner-side rewrite of ReplicationConnection.application_name at capture time: rejected -- it destroys the raw captured value that capture.rs archives and that writer/build.rs:145 `normalize_application_name` and :660 `group_connections_by_identity` consume, and it silently corrupts historical captures. Adding an `application_name` column to the portal inventory: correct long-term single-source-of-truth but is a change outside this repo (the cached response has no such field: keys are cluster_id, cpu_amount, id, ip_address, kernel, memory_mb, node_name, pg_version, point_in_time_recovery, read_only, repmgr_enabled) and does not repair existing captures. A shared normalisation helper module: rejected under repo CLAUDE.md ('Do not introduce traits, abstractions, or helper files unless explicitly asked'); if name matching is preferred over IP matching, a single 3-line private `fn to_application_name(node_name: &str) -> String { node_name.split('.').next().unwrap_or(node_name).replace('-', "_") }` inside split_brain.rs is the CLAUDE.md-compatible shape -- it is peer to the existing private `parse_wal_sender_timeout` at :610, not a new abstraction. IP matching is preferred for the gate specifically because the gate is verdict-critical and an app-name convention drift would silently re-break it.

### [critical | diverges] ADR lines 172, 39-40
- **Claim:** §4 variant->action mapping, `HigherTimeline` row: "SplitBrain: {true} has quorum (TL={hi}), demote {stale} (TL={lo}, no live replicas)" (line 172), and C-b/C-c's finding that the LOWER-TL primary is the true primary because it has a flushing replica (lines 39-40).
- **Justification:** Because the gate never matches, C-b and C-c both fall through to HigherTimeline, and the writer then prints a demote instruction against exactly the node the ADR says holds the acknowledged writes.
- **Code:** `src/v2/writer/build.rs:712-718 `SplitBrainResolution::HigherTimeline { .. } => format!("SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)", info.true_primary, true_primary_timeline, stale, stale_timeline)`; reached because src/v2/writer/build.rs:655 `if matches!(info.confidence, Confidence::Refuse)` is false (confidence is Conflicting, see the PrimaryQuorumUnsatisfied finding). Fall-through path: src/v2/analyze/split_brain.rs:465-503 -- `replicas_following_highest` empty and `replicas_following_stale` empty means both the LowerTimelineHasQuorum branch (:465) and the Both branch (:479) are skipped and the HigherTimeline else-branch (:487-503) fires.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Reachability on the fleet needs one captured split-brain with a live replica following the lower-TL primary. There is no captured run of C-b/C-c (the ADR says so for C-g; the same is true here). What is verifiable today without such a capture: on a healthy cluster, confirm the gate's primary side rejects a genuinely-streaming replica -- take tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json, duplicate db002 as a second Primary role with a lower timeline_id, and run it through resolve_split_brain; expect replicas_following empty and resolution HigherTimeline. That is a fixture exercise, not a fleet capture, and it settles reachable_in_code only.
- **Fix (resolver):** Fixing the :307 identity comparison is necessary and sufficient for this specific text: with the gate matching, C-b/C-c reach `LowerTimelineHasQuorum` (:453-467) and the writer emits the 'fence {higher-TL}' string at build.rs:702-711 instead. No change to format_resolution is required for this finding. Do NOT patch this at the writer -- suppressing the demote text would hide a wrong verdict rather than fix it.
- **Alternatives rejected:** Writer-side suppression (e.g. refuse to print 'demote' when findings contain PrimaryDoesNotSeeReplica) was considered: it converts a wrong actionable verdict into a wrong non-actionable one and leaves LowerTimelineHasQuorum permanently dead, so it treats the symptom. ADR-text-only (documenting HigherTimeline as unreliable) rejected: the ADR already documents C-b/C-c as the cases the design exists to get right.

### [high | diverges] ADR lines 39-40, 46
- **Claim:** C-b/C-c "`LowerTimelineHasQuorum` + `BidirectionalFlushingConfirmed(db001, db003)`" and design fact 1: "C-b and C-c are correctly resolved by the renamed `LowerTimelineHasQuorum` variant once the gate has confirmed `db003` is actively flushing for `db001`" (line 46).
- **Justification:** `LowerTimelineHasQuorum`, `Both`, `ReplicaFollowing`, `BidirectionalFlushingConfirmed` and `ReplicaInCatchup` are all constructed only inside the `Some(row)` arm or gated on a non-empty following map, so all five are dead code on the fleet.
- **Code:** `src/v2/analyze/split_brain.rs:317-336 -- the `match primary_row { Some(row) => { following.entry(...).push(...); findings.push(BidirectionalFlushingConfirmed(...)); ... ReplicaInCatchup ... } None => findings.push(PrimaryDoesNotSeeReplica(...)) }`. :319-322 is the only insertion into `following` in the whole function. Downstream consumers that therefore never fire: :453-467 (LowerTimelineHasQuorum), :479-486 (Both), :514-536 (ReplicaFollowing, via `replicas_following.get(&primary.node_name)` at :517).`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** None beyond the identity evidence already gathered; this is pure code reading given a failing :307 comparison.
- **Fix (resolver):** Same single-line identity fix at :307. No other change revives these variants.
- **Alternatives rejected:** None -- there is exactly one insertion site into the following map.

### [high | diverges] ADR lines 38, 42
- **Claim:** C-a expects "`Both` (timeline + flushing replica agree) + `BidirectionalFlushingConfirmed(db002, db003)` + `PrimaryQuorumUnsatisfied(db001)`" and C-e expects "`Both` ... + `BidirectionalFlushingConfirmed(db002, db003)`" (lines 38, 42).
- **Justification:** Both rows produce HigherTimeline + PrimaryDoesNotSeeReplica(db002, db003) + PrimaryQuorumUnsatisfied on BOTH primaries instead; the C-e asymmetric-precedence argument still holds mechanically but is untestable because the primary side now rejects everything, not just stale rows.
- **Code:** `src/v2/analyze/split_brain.rs:334-336 `None => findings.push(SplitBrainFinding::PrimaryDoesNotSeeReplica(ReplicationLink::new(&primary.node_name, &replica.node_name)))`. In C-a/C-e the replica-side gate at :281-288 passes for db002 (sender_host == db002.ip, status streaming, fresh), so the `continue` at :299 is not taken and the None arm is reached.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (resolver):** Same :307 fix. Note the verdict for C-a/C-e stays db002 either way (the timeline fallback happens to agree), so this is a findings/confidence divergence not a wrong-winner divergence -- which is precisely why it would never be noticed in a real incident: the tool looks right for the common case and is wrong only in C-b/C-c.
- **Alternatives rejected:** n/a

### [high | diverges] ADR lines 143-150
- **Claim:** §4 derivation rule step 3 "`observed = |members ∩ gated_followers|`" (line 147) and line 150's MUST-emit rule for the stale primary only.
- **Justification:** The intersection is a raw string compare between synchronous_standby_names members (application-name form) and node_names (FQDN form), so `observed` is 0 for every primary -- including the elected true primary -- independent of the gate bug, and the two mismatches compound.
- **Code:** `src/v2/analyze/split_brain.rs:660-663 `let observed = members.iter().filter(|m| gated.iter().any(|g| g == *m)).count() as u32;` where `gated` at :656-659 is `replicas_following.get(&p.node_name)` (node_names) and `members` at :651 comes from `parse(synchronous_standby_names)` -- on the fleet, `ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )`. Even if the :307 gate were fixed in isolation, `g == *m` would still be false for every pair.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Already settled: fixture line `"synchronous_standby_names": "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"` against node_names `dev-pg-app001-db00{2,3}.sto{2,3}.example.com`. To confirm SSN uses the same convention on prod: `SELECT name, setting FROM pg_settings WHERE name = 'synchronous_standby_names';` on any prod primary.
- **Fix (resolver):** In emit_quorum_findings, compare on a normalised form rather than raw: `let observed = members.iter().filter(|m| gated.iter().any(|g| to_application_name(g) == **m)).count()`, with a private `fn to_application_name(node_name: &str) -> String { node_name.split('.').next().unwrap_or(node_name).replace('-', "_") }` local to split_brain.rs. Verified injective over all 1074 inventory node_names, so no cross-node collisions. This edit is required IN ADDITION TO the :307 gate fix -- fixing only :307 leaves PrimaryQuorumUnsatisfied firing for every primary.
- **Alternatives rejected:** Reducing both sides to the db-number ('db002'), which is what writer/build.rs:375-393 `find_replica_timeline` does: rejected as the primary recommendation because it is only unique within a cluster and discards information for no benefit -- the full-label transform is exact and verified collision-free fleet-wide. Threading the matched row's application_name out of build_replica_following_map into the map: rejected -- it changes the map's value type, and those values are also rendered as `replicas_following_true` by writer/build.rs:250-266 via extract_db_number, which expects the hyphen/FQDN form.

### [high | diverges] ADR lines 115-121, 158
- **Claim:** §3 "`Conflicting` // signals partially contradict (verdict still chosen)" (lines 115-121) combined with §4 item 3 "`PrimaryQuorumUnsatisfied` MUST appear inline in the short string when present, since it explains why the higher-TL primary lost. Without it the verdict reads as a paradox" (line 158).
- **Justification:** PrimaryQuorumUnsatisfied is now always emitted for the elected primary too, which pins confidence to Conflicting on every split brain; but format_resolution never inspects findings, so the one PQU that would contradict the verdict is the one the operator never sees, and Conflicting has no other rendering effect anywhere.
- **Code:** `src/v2/analyze/split_brain.rs:404-410 `PrimaryQuorumUnsatisfied { primary, .. } => { if primary == true_primary { Confidence::Conflicting } else { Confidence::BestEffort } }`, min'd at :383-387 over the derived Ord of `enum Confidence { Refuse, Conflicting, BestEffort }` (:73-77), so Conflicting wins. The fleet cannot reach Refuse via the sanity gates in the normal case (fixture `synchronous_commit: remote_apply`, not in WEAKENED_SYNCHRONOUS_COMMIT at :13). Rendering: src/v2/writer/build.rs:655 is the ONLY read of `info.confidence` in the whole writer -- `grep -n confidence src/v2/writer/` returns only :655 plus test literals -- so Conflicting renders byte-for-byte identically to BestEffort. And format_resolution (build.rs:692-725) takes only `info.resolution` and `info.stale_primaries`, never `info.findings`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** None -- verified by reading; `rg -n "confidence" src/v2/writer/` and `rg -n "details" src/v2/writer/terminal.rs` are the two checks.
- **Fix (combination):** Primary fix is the two resolver edits above, which stop PQU firing for the elected primary. Independently of that, the ADR should decide whether Conflicting is allowed to be invisible: today it is a field nothing renders. Either §3 should say so explicitly, or §4 should add a rule that PrimaryQuorumUnsatisfied naming the ELECTED primary must break out of the fixed per-variant parenthetical in format_resolution -- e.g. append ' [WARNING: elected primary also has 0/N gated followers]'. The details_json path (build.rs:663, surfaced only in CSV at src/v2/writer/csv.rs:66) does carry the finding, but src/v2/writer/terminal.rs:111 and :128 print only `view.reason.short`, so the terminal operator -- the 3 AM path the rename in §4 was written for -- never sees it.
- **Alternatives rejected:** Making PQU-on-elected-primary a Refuse: rejected, the ADR's comment at split_brain.rs:398-402 argues explicitly and correctly why it is Conflicting not Refuse ('shaky, but not a safety-model break'), and escalating it would mask the actual bug behind a blanket refusal on every split brain.

### [high | diverges] ADR lines 172
- **Claim:** Assessment requested by the orchestrator: whether writer normalisation makes the REPORT look coherent while the RESOLVER verdict is wrong. (Against §4 line 172's 'no live replicas' parenthetical.)
- **Justification:** The writer's normalisation is NOT on the split-brain replica path, so the report does not show correct pairings -- it shows none; but the empty replicas column silently corroborates the false 'no live replicas' text, producing two mutually-confirming false signals, which is the worse outcome the orchestrator was probing for, arriving by a different route.
- **Code:** `src/v2/writer/build.rs:207 `let replicas = build_split_brain_replicas(info);` -- the Reason::SplitBrain arm bypasses build_replicas_view/normalize_application_name entirely. build.rs:272-274 `SplitBrainResolution::HigherTimeline { .. } | SplitBrainResolution::Indeterminate => { ReplicasView::None }`, rendered as `"-"` by src/v2/writer/view.rs:112. Meanwhile the PRIMARY column IS normalised (build.rs:202 `display: extract_db_number(&info.true_primary)`, view.rs:71-77 `"{} vs {}"`), so the row reads e.g. `db002@sto2 vs db001@sto1 | - | SplitBrain: ... (TL=11, no live replicas)` -- a self-consistent, entirely believable row. README.md documents the correct split-brain replicas cell as `db003@n→db001@n`; on the fleet that cell is now always `-` for split brains.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** None.
- **Fix (resolver):** No writer change needed; the :307 and emit_quorum_findings fixes restore SplitBrainFollowing pairs. Worth noting in the ADR that the replicas column being '-' on a split brain is now a tell for 'the gate matched nothing', not for 'there are no followers' -- i.e. it is not independent corroboration of the reason text and an operator should not read it as such.
- **Alternatives rejected:** Making build_split_brain_replicas fall back to build_replicas_view (which does normalise) for HigherTimeline/Indeterminate: rejected -- it would paper over the empty following map with pairings derived from a different source, making the row look MORE authoritative while the verdict stays wrong.

### [medium | diverges] ADR lines 157, 170-174
- **Claim:** §4 short-string examples and the variant->action table use short node identifiers: "`SplitBrain: db001 has quorum (lower TL=N), fence db002 (TL=N+1, quorum-blocked)`" (line 157) and the `{true}` / `{stale}` placeholders (lines 170-174).
- **Justification:** format_resolution interpolates the raw node_name, which on the fleet is a 35-character FQDN, while every other cell in the same row is passed through extract_db_number -- so the reason column is inconsistent with the primary column and roughly triples the terminal's reason column width.
- **Code:** `src/v2/writer/build.rs:695 `let stale = info.stale_primaries.first().map_or("", String::as_str);` and :698-724 interpolating `info.true_primary` / `stale` raw, versus build.rs:202 and :256 which wrap the same strings in `extract_db_number`. Column width: src/v2/writer/terminal.rs:48 `max_reason = max_reason.max(view.reason.short.len());`. Rendered result on the fleet: `SplitBrain: prod-pg-f1c1-db002.sto2.fnox.se has quorum (TL=12), demote prod-pg-f1c1-db001.sto1.fnox.se (TL=11, no live replicas)`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (writer):** Wrap the four interpolations in build.rs:698-724 with the extract_db_number already imported in that file: `extract_db_number(&info.true_primary)` and `extract_db_number(stale)`. This is independent of the identity bug and safe to land separately. The existing tests at build.rs:766/787 pass literal "db001"/"db002" as node names, and extract_db_number returns those unchanged (no '-' part starting with 'db'), so the tests keep passing without modification -- which is also why the divergence was invisible.
- **Alternatives rejected:** Changing the ADR examples to FQDN form: rejected -- the ADR's whole §4 rationale is 3 AM legibility, and the primary column already establishes `dbNNN@zone` as the house identifier format.

### [info | implemented] ADR lines 27
- **Claim:** Refutation attempt on the lead: is the fixture a hand-edited anonymised artifact whose application_name/node_name shape difference is a templating artifact rather than fleet reality?
- **Justification:** The lead survives every refutation I could construct: node_name is FQDN on 1074/1074 real inventory records, so no matching path exists for an underscore application_name, and three independent artifacts corroborate the underscore convention.
- **Code:** `Refutation checks run and their outcomes -- (1) No alternative matching path in the resolver: `conn.application_name == replica.node_name` at split_brain.rs:307 is the sole primary-side identity test; there is no alias table, no fuzzy compare, no fallback arm. (2) analyze() does not rewrite node_name: src/v2/analyze.rs:318 passes `&primaries, &replicas` straight through; `rg -n node_name src/v2/analyze.rs` shows only `type NodeName = String` (:30) and verdict pushes. (3) Fixture is anonymised (127.x IPs, example.com) BUT the anonymiser preserved cross-field relationships -- wal_receiver.sender_host 127.1.12.151 == db001's ip_address 127.1.12.151, and the replica's own `configuration.primary_conninfo` contains `application_name=dev_pg_app001_db002` matching the primary's pg_stat_replication row captured from a different node. A relationship-preserving anonymiser that had been fed two equal values would have emitted two equal values. (4) Independent corroboration from the code itself: writer/build.rs:422-430 `normalize_application_name` (documented '// Application names are like: dev_pg_app001_db002') and :375-393 `find_replica_timeline` exist solely to bridge `dev_pg_app001_db002` to `dev-pg-app001-db002.sto...`; that code is pointless if the forms are equal. (5) Structural: src/v2/db.rs:57 `cfg.host(&node.name)` means node.name must resolve in DNS, and underscores are not legal in hostnames -- so node_name cannot be the underscore form regardless of FQDN-vs-shortname.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** One prod capture: `SELECT application_name, client_addr FROM pg_stat_replication;` on prod-pg-app007-db001.sto1.fnox.se (or any prod-pg-f*c*-db001), plus `SHOW primary_conninfo;` on its replica.
- **Fix (none):** No fix. Recording the refutation attempt and its failure. One honest residual: the underscore application_name is evidenced on exactly one captured cluster (dev-pg-app001). The 1074/1074 FQDN node_name result makes the mismatch certain for any cluster whose application_name is not the literal FQDN, and repmgr_enabled=true on 1074/1074 makes a uniform repmgr template overwhelmingly likely, but 'overwhelmingly likely' is not 'captured'. The prod query in the first finding's evidence_needed closes it.
- **Alternatives rejected:** n/a

### [low | implemented] ADR lines 70, 27
- **Claim:** §1's known-limitation hedge: "The comparison is `==` against `primary.ip_address.to_string()`; in environments where `primary_conninfo` uses a hostname, this comparison fails. Out of scope for v1; flag as a known limitation. (Current production uses IPs.)" (line 70).
- **Justification:** The replica-side IP identity assumption was checked against production and is documented with its failure mode, while the primary-side name identity assumption on line 27 got no check and no hedge -- and it is the one that is false; the asymmetry is the process failure behind the bug.
- **Code:** `src/v2/analyze/split_brain.rs:281 `let replica_passes = wr.sender_host == primary.ip_address.to_string()` -- holds on the fixture (sender_host 127.1.12.151 == db001 ip_address 127.1.12.151, and `conninfo` shows `host=127.1.12.151`, an IP not a hostname), so the hedged assumption is the one that is TRUE. Contrast split_brain.rs:307, whose assumption is unhedged in the ADR and false. Note also that every other analyzer check pairs pg_stat_replication to nodes by IP, not name: src/v2/analyze/checks.rs:194, :219, :246.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (adr-text):** When correcting line 27, add the same style of hedge and the same evidence citation the sender_host limitation got, and state which capture it was checked against. Also worth a sentence in §1 noting that the resolver is the only place in the analyzer that identifies a replication row by name rather than by client_addr, so the ADR's choice there is a deliberate exception, not the house convention.
- **Alternatives rejected:** n/a

### [medium | diverges] ADR lines 27, 73
- **Claim:** Test coverage: whether the suite can catch any of the above.
- **Justification:** The test builder bakes the false assumption into its API and documents it as intended, and no test anywhere uses fleet-shaped identifiers, so every finding above is invisible to `cargo test`.
- **Code:** `src/v2.rs:95-101 -- `/// Add pg_stat_replication rows with explicit application_names /// matching given replica node names. Used by tests that need the /// flushing-liveness gate's primary side to corroborate.` then `pub fn with_followers(mut self, followers: &[&str])`, and src/v2.rs:182 `application_name: name.clone()`. Call sites pass the same literal used for the node name: e.g. split_brain.rs:1044-1046 `primary_with_followers(1, "db001", IP_DB1, 11, &["db003"])` with `replica_following(3, "db003", IP_DB1, 11)`. The one real-data fixture, src/v2.rs:30 `include_str!("../tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json")`, is used at src/v2/analyze.rs:373 only, and it has a single primary so `primaries.len() > 1` at analyze.rs:317 is false and resolve_split_brain is never entered on real data.`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** None | **Destructive text:** None
- **Evidence needed:** None.
- **Fix (tests):** Add one regression test in split_brain.rs that uses fleet-shaped identifiers: node_name `dev-pg-app001-db003.sto3.example.com`, application_name `dev_pg_app001_db003`, synchronous_standby_names `ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )`, and assert the C-b outcome (resolution == LowerTimelineHasQuorum, BidirectionalFlushingConfirmed present, no PrimaryQuorumUnsatisfied naming the elected primary). This requires a `with_followers`-adjacent way to set application_name independently of node_name; the smallest change is to keep with_followers as-is and pass the underscore strings explicitly at the new call site, since with_followers already takes arbitrary strings -- no builder change needed, only the docstring at src/v2.rs:95-97 is wrong and should say 'application_names' without the '=node names' claim.
- **Alternatives rejected:** Rewriting all existing split_brain tests to fleet-shaped names: rejected as unnecessary churn under CLAUDE.md §3; one targeted regression test pins the behaviour. Building the second primary into the healthy fixture: rejected -- it would turn the only real-data capture into a synthetic one.

### [info | unverifiable] ADR lines 213
- **Claim:** Correction to orchestrator ground truth 1 (scope note): the statement 'pg_version is "15.14" on all three nodes. NOT Postgres 17' is correct about the fixture but should not be read as a statement about the fleet.
- **Justification:** The real inventory is majority PG17, so §5's PG17-sourced NULL-vs-zero-LSN validation is representative of most of the fleet -- but 228 of 1074 nodes run PG13/14/15 and the ADR cites only PG17 source, so the claim is unverified for those.
- **Code:** `No code claim. Evidence from /tmp/nodes_период_response.json (the tool's own portal cache, src/database_portal.rs:8): pg_version distribution over 1074 nodes -- 17.6:733, 15.14:120, 17.10:112, 15.18:63, 14.19:24, 14.23:15, 15.19:3, 13.22:3, 17.11:1. That is 846 nodes on PG17 and 228 on PG13/14/15. ADR line 213: '(Validated 2026-09-10.) Both functions return SQL NULL, never 0/0 ... PG17 xlogfuncs.c guards each with if (recptr == 0) PG_RETURN_NULL();'. Cluster shape also confirmed: 1074 nodes / 358 clusters = exactly 3 per cluster, read_only true on 716 = exactly 2 per cluster, matching the ADR's '1 primary + 2 replicas' assumption at line 25.`
- **Reachable in code:** False | **On fleet:** unknown | **Wrong verdict:** None | **Destructive text:** None
- **Evidence needed:** `SELECT version(), pg_last_wal_receive_lsn() IS NULL FROM (SELECT 1) x;` on a 13.22 and a 14.19 node.
- **Fix (adr-text):** Outside my assignment; flagging for whoever owns §5/§7. Either widen the validation citation or scope line 213 to PG16+. Settle it with `SELECT version(), pg_last_wal_receive_lsn() IS NULL, pg_last_wal_replay_lsn() IS NULL;` on one PG13 node and one PG14 node (there are 3 nodes on 13.22 and 39 on 14.x).
- **Alternatives rejected:** n/a


## Completeness critic: coverage audit of the 25-agent digest against all 268 lines of docs/adr/002-split-brain-resolution-refinement.md, plus unauthorised code behaviours and audit-methodology risk

### [high | diverges] ADR lines 171 (with 25, 150, 256)
- **Claim:** §4 table row `LowerTimelineHasQuorum` -> "SplitBrain: {true} has quorum (lower TL={lo}), fence {stale} (TL={hi}, quorum-blocked)", and the whole ADR's implicit two-primary scope (§Cluster assumptions:25 and Out-of-scope:256 scope out >2 REPLICAS, never >2 PRIMARIES).
- **Justification:** UNCOVERED BY DIGEST. With >=3 primaries at >=3 distinct timelines the LowerTimelineHasQuorum arm prints a timeline that does not belong to the primary it elects, and silently drops every other zombie primary from stale_primaries.
- **Code:** `src/v2/analyze/split_brain.rs:426 `let stale_tl = timeline_info.primaries_with_lower_timeline[0].1;` -- index 0 is the HIGHEST of the lower TLs (descending sort at :195 `primary_timelines.sort_by_key(|b| std::cmp::Reverse(b.1));`), but the elected node is chosen by the break at :444 `stale_with_followers = Some(*stale_node);`, which can be a different, lower-TL entry. The mismatched value is then published at :458 `true_primary_timeline: stale_tl,` and rendered verbatim at src/v2/writer/build.rs:708-711 `"SplitBrain: {} has quorum (lower TL={}), fence {} (TL={}, quorum-blocked)"`. Second defect, same arm: src/v2/analyze/split_brain.rs:456 `stale_primaries: vec![highest_tl_node.node_name.clone()],` -- only the highest-TL node, whereas the sibling arms collect all of them (:467-471, :488-492).`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** To settle reachable_on_fleet: whether three simultaneous primaries at three distinct timelines can occur. Query across a captured multi-primary scan: `SELECT cluster_id, count(*) FILTER (WHERE NOT pg_is_in_recovery()) AS primaries, count(DISTINCT timeline_id) FROM <scan snapshot> GROUP BY 1 HAVING count(*) FILTER (WHERE NOT pg_is_in_recovery()) >= 3;`. Equivalently, add a fixture under tests/fixtures/ capturing any cluster where all three nodes report pg_is_in_recovery()=false.
- **Fix (resolver):** In resolve_with_different_timelines, stop using the precomputed `stale_tl`. Capture the elected node's own timeline alongside the node in the search loop -- change `stale_with_followers: Option<&AnalyzedNode>` to `Option<(&AnalyzedNode, i32)>` and bind both from the `for (stale_node, tl) in &timeline_info.primaries_with_lower_timeline` iterator at :439, then set `true_primary_timeline` from that captured `tl`. Separately, build `stale_primaries` for this arm the same way the Both/HigherTimeline arms do: every primary in `primaries_with_highest_timeline` plus every `primaries_with_lower_timeline` entry except the elected one, so a second live zombie primary is still named for fencing. Add a regression test with three primaries at three distinct timelines (e.g. db001 TL=10 with a gate-passing follower, db002 TL=11 no followers, db003 TL=12 no followers) asserting `true_primary_timeline == 10` and `stale_primaries` containing both db002 and db003; no such test exists today -- src/v2/analyze/split_brain.rs:1097 `equal_timelines_three_primaries_one_has_follower` uses three EQUAL timelines and :1418 `extract_timeline_info_partitions_by_highest` uses 11/12/12, i.e. only one lower-TL entry.
- **Alternatives rejected:** Fixing this in the writer (deriving the elected primary's TL by re-looking-up the node) was considered and rejected: the writer has no access to the primaries slice, only to SplitBrainInfo, so the wrong number is already baked in by the time format_resolution runs. Adding a >2-primaries guard in analyze.rs was also rejected -- it would suppress a real split-brain rather than report it, and analyze.rs:317 `if primaries.len() > 1 {` deliberately has no upper bound.

### [high | diverges] ADR lines 75, 87
- **Claim:** §1: "The row's `reply_time` is within `freshness_threshold`" (:75), plus the §1 note that the two cadences differ but "A symmetric threshold is generous on the primary side; this is acceptable for v1" (:87). The implementation's own doc comment escalates this into a safety claim.
- **Justification:** The digest flags the intra-node doc comment (s1, ADR:87 / split_brain.rs:242-244) but supplies NO postgres source for what reply_time actually is -- it is accepted on plausibility. I settled it from REL_15_14 source: reply_time is the STANDBY's clock, so the primary-side check is inter-node, not intra-node.
- **Code:** `src/v2/analyze/split_brain.rs:243 `/// timestamps, so the comparison is intra-node and immune to scanner<->db clock skew` versus :313 `(p_health.current_time - t).num_milliseconds() <= threshold_ms` where `t` is `conn.reply_time` (:312). Postgres provenance, REL_15_14 (fleet is 15.14/15.18/17.x): the standby stamps the reply with its OWN clock -- src/backend/replication/walreceiver.c, XLogWalRcvSendReply: `pq_sendint64(&reply_message, GetCurrentTimestamp());`; the primary reads it verbatim off the wire -- src/backend/replication/walsender.c:2098 `replyTime = pq_getmsgint64(&reply_message);` -> :2163 `walsnd->replyTime = replyTime;` -> exposed as pg_stat_replication.reply_time at :3624 `values[11] = TimestampTzGetDatum(replyTime);`. `p_health.current_time` is the primary's own `now()` -- src/v2/scan/health_check_primary.rs:148 `'current_time', (SELECT now()),`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** `SELECT node_name, now() AS db_clock FROM <each node>` in one scan pass, then compute max pairwise skew per cluster; or directly `SELECT application_name, reply_time, now(), now() - reply_time FROM pg_stat_replication;` on a primary -- a consistently negative or large-positive `now() - reply_time` on a demonstrably healthy stream is the signature.
- **Fix (combination):** Two parts. (1) Correct the doc comment at src/v2/analyze/split_brain.rs:242-244: the replica-side check (:285) IS intra-node, but the primary-side check (:313) subtracts a standby-generated timestamp from a primary-generated one and is therefore exposed to primary<->replica clock skew. (2) Decide the intended behaviour and record it in ADR §1. The skew is directional and fails CLOSED in the dangerous direction: a replica clock BEHIND its primary by more than threshold_ms (180 s on the fleet, wal_sender_timeout=300000) makes a live healthy stream read as stale, so primary_row is None, `PrimaryDoesNotSeeReplica` is pushed (:335), the replica never enters `following` (:319-322), and C-b/C-c collapse into the HigherTimeline arm (:487-501) -- the writer then prints "demote <lower-TL primary>" (build.rs:715-717) against the node holding the acknowledged writes. A replica clock AHEAD yields a negative difference, which passes the `<=` and fails open. Minimal fix: clamp the primary-side conjunct to a non-negative difference and widen it, or drop reply_time from the gate and keep it as a finding only -- ADR:56 already declares the primary side "corroborating only", so a skew-sensitive input has no business being able to veto a gate the replica side passed.
- **Alternatives rejected:** Passing a scan-start timestamp (ADR:225) does not fix this: the skew is primary-clock vs replica-clock, not scanner-vs-db, so substituting the scanner's clock for `p_health.current_time` swaps one cross-node comparison for another. Comparing reply_time against the replica's `current_time` instead was rejected -- the scanner has no join from a pg_stat_replication row back to the replica's ReplicaHealthCheckResult that does not itself go through the broken application_name equality at :307.

### [high | unverifiable] ADR lines 27, 73
- **Claim:** META (Q5): the single most likely way this audit is WRONG. Roughly eight critical/high digest findings and six of seven matrix rows rest on ONE premise -- that `pg_stat_replication.application_name` and `AnalyzedNode.node_name` are different string forms, making split_brain.rs:307 always false on the fleet.
- **Justification:** I strengthened half the premise to fleet-wide certainty and left the other half resting on a single anonymised dev fixture. If that half is wrong, the audit's critical mass collapses.
- **Code:** `src/v2/analyze/split_brain.rs:307 `&& conn.application_name == replica.node_name`. HALF ONE, NOW SETTLED FLEET-WIDE (new evidence, not in the digest): the portal inventory that populates `node_name` (src/v2/node.rs `#[serde(rename = "node_name")] pub name: String`) covers 1074 nodes in /tmp/nodes_response.json -- 0 of 1074 node_name values contain an underscore, 1074 of 1074 contain a hyphen, 1074 of 1074 are FQDNs (samples: `acce-pg-f3c020-db003.sto3.fnox.se`, `prod-pg-app003-db003.sto3.fnox.se`). So `replica.node_name` is NEVER the underscore short form anywhere on the fleet. HALF TWO, STILL SINGLE-SOURCED: that `application_name` is the underscore short form rests on tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json (one anonymised 3-node dev cluster) plus the circumstantial existence of src/v2/writer/build.rs:422 `fn normalize_application_name`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** One query, run against 5-10 clusters spanning prod/acce and spanning pg_version 15.x and 17.x: `SELECT current_setting('cluster_name', true) AS cn, application_name, state FROM pg_stat_replication;` executed on each primary, compared against that node's portal `node_name`. Settle in the same pass with `SHOW synchronous_standby_names;`. If application_name comes back as `dev_pg_app001_db002`-shaped on every cluster, the audit's naming findings are confirmed fleet-wide; if it comes back FQDN-shaped anywhere, those findings are cluster-specific and their severity drops.
- **Fix (tests):** Capture a second real fixture from a DIFFERENT cluster -- ideally a prod one, ideally with >=2 primaries -- before acting on any of the naming findings. Note that the two reported joins are not independent and must be settled together: split_brain.rs:307 (`application_name` vs `node_name`) and split_brain.rs:660-663 (`members` from `synchronous_standby_names` vs `gated` node_names) both fail if and only if repmgr writes the underscore short form, and both succeed if repmgr writes the FQDN -- because repmgr generates the SSN member list from the same application_name it sets. Any fix must change both call sites or neither; fixing :307 alone leaves `observed` structurally 0 and turns `PrimaryQuorumUnsatisfied` into a permanent false positive on every primary.
- **Alternatives rejected:** Treating the fixture as decisive was rejected: 1 of 358 clusters, and it is the only cluster ever captured. Treating `normalize_application_name`'s existence as decisive was also rejected -- it lives in the writer, whose input is `pg_stat_replication` rows for SINGLE-primary clusters, and could have been written for a naming convention that has since changed.

### [medium | diverges] ADR lines 113-118 (with 53, 141)
- **Claim:** §3 defines the three confidence states as properties of the evidence state -- `Conflicting` = "signals partially contradict (verdict still chosen)" (:116), `BestEffort` = "single-pass scan; gate passed; verdict is internally consistent" (:115). No ADR statement authorises a finding's confidence to depend on WHICH NODE the finding names.
- **Justification:** The digest marks the RATIONALE unverifiable (s3, medium) but nobody flagged the sharper half: the `else` branch maps a contradiction-class finding to the TOP of the lattice, where `.min()` makes it indistinguishable from no finding at all.
- **Code:** `src/v2/analyze/split_brain.rs:404-409 `if primary == true_primary { Confidence::Conflicting } else { Confidence::BestEffort }`, folded at :388-389 `.map(|f| determine_confidence_level(f, true_primary)).min().unwrap_or(Confidence::BestEffort)` over the Refuse-first Ord at :73-77. Because `BestEffort` is the maximum, `PrimaryQuorumUnsatisfied` on a non-elected primary contributes exactly nothing to the fold -- the same result as if the finding had never been emitted. Every other non-corroborating finding is `Conflicting` (:411-413).`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (combination):** Either (a) write the rule into ADR §3 explicitly -- add a sentence stating that findings are scored by `min()` over `determine_confidence_level`, that `PrimaryQuorumUnsatisfied` naming a non-elected primary is corroborating rather than contradicting (it is the proof behind the resolution, per ADR:53), and that corroborating findings map to `BestEffort` -- or (b) introduce a distinct neutral rank so that a finding which is neither corroboration nor contradiction cannot occupy the same lattice position as `BidirectionalFlushingConfirmed`. (a) is the smaller change and matches the intent already written in the code comment at :400-403 and in commit 87e51e5.
- **Alternatives rejected:** Mapping the non-elected case to `Conflicting` was rejected: it would make every C-a/C-b resolution Conflicting by construction (ADR:41-42 both expect PrimaryQuorumUnsatisfied on the loser), which is the exact regression 87e51e5 fixed. Removing the identity condition entirely was rejected for the same reason.

### [medium | unverifiable] ADR lines 67, 137, 257 (with 255)
- **Claim:** §1:67 hedges only the hostname-form failure of the sender_host comparison, and §Out of scope:257 repeats it as "Current production uses IPs; flag as known limitation." §4:137 defines `ReplicaWalReceiverStale { replica, claimed_sender }` as the record of gate-rejected replica-side evidence.
- **Justification:** UNCOVERED ANGLE. The digest covers the hostname case and the emission guard, but nobody asked whether `sender_host` on the fleet is the node IP or a VIP -- and the code preserves no raw `sender_host` anywhere in the output, so a fleet-wide mismatch would be invisible in every artifact the tool produces.
- **Code:** `src/v2/analyze/split_brain.rs:293 `if wr.sender_host == primary.ip_address.to_string() {` -- the guard; :294-297 sets `claimed_sender: primary.node_name.clone()`, i.e. the resolved node name, discarding the raw string. When the guard is false the loop hits :299 `continue;` with no finding of any kind. The raw value never reaches the writer: `grep -n sender_host src/v2/writer/` returns nothing, and the split-brain path bypasses the replicas view entirely -- src/v2/writer/build.rs:272-274 maps `HigherTimeline | Indeterminate` to `ReplicasView::None`. ADR:255 lists "Cross-cluster signals (Pacemaker, etcd, VIP state)" as out of scope, which establishes that VIPs exist in this environment.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** `SELECT sender_host, sender_port, status FROM pg_stat_wal_receiver;` on a sample of replicas across prod clusters, compared against the portal `ip_address` for the cluster's primary. Also `SHOW primary_conninfo;` on the same nodes -- if `host=` names a VIP or a hostname on any cluster, the §1 gate is a no-op there by a mechanism entirely separate from the application_name join.
- **Fix (writer):** When a split-brain cluster produces zero `BidirectionalFlushingConfirmed` findings, include each replica's raw `wal_receiver.sender_host` and `status` in `details_json` so a gate that is failing for an unanticipated reason (VIP, hostname, non-5432 port, cascaded upstream) leaves a diagnostic trace rather than a silent `HigherTimeline` + empty findings. This is a writer-side change because the resolver already has the information and discards it; the alternative -- emitting `ReplicaWalReceiverStale` unconditionally -- was explicitly rejected in the code comment at :289-292 for good reason (it would fire once per non-matching primary for every replica).
- **Alternatives rejected:** Dropping the guard at :293 was rejected: with P primaries and R replicas it produces P*R-R spurious findings per scan, exactly what the comment at :289-292 says. Widening the comparison to accept a VIP was rejected as premature -- there is no evidence yet that a VIP appears in sender_host.

### [medium | implemented] ADR lines 164
- **Claim:** §4 closing caveat: "Phrasing is implementation detail; the *minimum information content* listed above is spec."
- **Justification:** UNCOVERED BY DIGEST, and it invalidates two of the digest's `diverges` calls. This sentence is the escape hatch every §4 short-string finding must be tested against, and nobody applied it.
- **Code:** `src/v2/writer/build.rs:702 `info.true_primary, true_primary_timeline, stale, stale_timeline` (raw FQDNs) versus src/v2/writer/build.rs:408 `fn extract_db_number`. The digest raises this twice as a divergence -- s4b ("lines 156-157", short node names, `diverges`/low) and the naming agent ("lines 157, 170-174", `diverges`/medium). Under ADR:164 node-name RENDERING is phrasing, not information content: the information content mandated by items 1-5 (which primary won, which to demote/fence, the timelines, the quorum state, the failed sanity gate) is all present in build.rs:698-724.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** No code change. Downgrade the two short-name findings to `low`/`info` in the audit's output and record in the audit summary that ADR:164 immunises them. If long FQDNs in the reason column are genuinely a problem, that is a §Out-of-scope:259 visual-polish item ("Visual/layout polish for the report ... how it's visually formatted is not [in scope]"), not an ADR-002 conformance defect. The one thing worth fixing on operational grounds -- terminal column width, since src/v2/writer/terminal.rs:48 `max_reason = max_reason.max(view.reason.short.len());` sizes the whole table off the longest reason -- should be filed separately as a UX issue, not as an ADR divergence.
- **Alternatives rejected:** Wrapping true_primary/stale in `extract_db_number` inside format_resolution was considered; it is a one-line change and probably desirable, but presenting it as an ADR-002 conformance fix is wrong given ADR:164 and ADR:259, and it would make the reason string ambiguous across the two datacentre suffixes if `extract_db_number` collapses them.

### [low | unverifiable] ADR lines 129
- **Claim:** §4: "Cap at ~5 surfaced items."
- **Justification:** UNDER-SPECIFIED, AND THE DIGEST OVER-CALLED IT. "Surfaced" is never defined. The digest (s4a) classifies it `diverges`/medium; on either reading of "surfaced" the code is already conformant or the ceiling is unreachable.
- **Code:** `src/v2/writer/build.rs:660 `let details = serde_json::to_string(info).unwrap_or_else(|_| "{}".to_owned());` -- no truncation, confirming the digest's mechanical observation. But if "surfaced" means the operator-facing short string, exactly ONE finding is ever surfaced: src/v2/writer/build.rs:669-683 `.find_map(...)` returns the first match, and format_resolution (:692-727) surfaces none. If it means details_json, the ceiling on a 3-node cluster is bounded and small: 1 sysid + P synchronous_commit + up to 2 per (primary, replica) pair from build_replica_following_map (:294, :324, :329, :335) + P quorum = 13 for P=3, R=1.`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Replace "Cap at ~5 surfaced items" with the actual contract, which §4's own bullets at :176-179 already state correctly: at most one sanity-gate finding reaches `short` (via format_refuse), the resolution template supplies the rest of `short`, and ALL findings go to `details_json` uncapped. Then delete the cap sentence, or rewrite it as a note that the findings vector is structurally bounded by (1 + 2P + 2PR) and needs no cap in the 1+2 topology. Downgrade the digest's `diverges`/medium to `unverifiable`/low.
- **Alternatives rejected:** Implementing a `.take(5)` on the findings vector was rejected: it would truncate `details_json`, which is the only place findings reach a human at all (src/v2/writer/csv.rs:66), and it would make the `.min()` confidence fold depend on emission order -- a Refuse-level finding at position 6 would be silently dropped from the fold, converting a Refuse into a BestEffort. That is strictly worse than no cap.

### [low | diverges] ADR lines docs/concepts/split-brain.md:89-95 (in scope per the assignment; ADR-002:11 incorporates this doc)
- **Claim:** docs/concepts/split-brain.md:89: "When db003 is re-pointed at db002 while already past X on TL=N, db002 refuses to stream it" (and :89-95, the wedge log signature).
- **Justification:** Q2 ANSWER. The digest's concepts agent classifies this `diverges` with a COMPLETELY EMPTY code citation and no postgres source -- a divergence verdict on zero evidence. I settled it from source; the doc is directionally right but attributes the refusal to the wrong process, and the digest's verdict was reached by plausibility.
- **Code:** `No code corresponds (nothing in src/ models the wedge). Postgres source, REL_15_14 and master, src/backend/access/transam/xlogrecovery.c:4181: `ereport(LOG, errmsg("new timeline %u forked off current database system timeline %u before current recovery point %X/%08X", ...)); return false;` -- inside the standby's own timeline-history validation, immediately after `if (currentTle->end < replayLSN)`. It is the STANDBY that declines to adopt the new target timeline, logged at LOG on the standby, and it happens before any streaming request is made. db002 does not 'refuse'; db003 never asks. This also corroborates the digest's postgres-agent refutation of the FATAL claim.`
- **Reachable in code:** False | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Fix (plan-text):** Edit docs/concepts/split-brain.md:89 to attribute the refusal correctly: the re-pointed standby validates the new timeline's history against its own replay position and declines to switch, emitting `new timeline N+1 forked off current database system timeline N before current recovery point X/X` at LOG severity on the STANDBY's log (xlogrecovery.c:4181). Adjust the surrounding sentence at :89-95 which currently hedges that "the precise pg_stat_wal_receiver contents in this state are likewise unconfirmed" -- that hedge is correct and should stay, but the log-line severity and the emitting node are now settled and should be stated as fact with the source reference.
- **Alternatives rejected:** Leaving the text as loose prose was rejected because this exact sentence is the mechanism the whole C-g row depends on; if an operator greps for FATAL after a suspected wedge and finds nothing, they will conclude the wedge did not happen.

### [low | unverifiable] ADR lines 140 (with 69, 74)
- **Claim:** §4:140 `ReplicaInCatchup { replica, primary }` -- "informational, gate passed"; §1:69 defines `catchup` on the REPLICA side (`wal_receiver.status`) and §1:74 defines it on the PRIMARY side (`state`). The ADR never says which side triggers the finding.
- **Justification:** UNCOVERED BY DIGEST. The digest notes the emission site in passing (s1, lines 69/74, `implemented`) but nobody asked whether the ADR pins down which side decides, and it does not.
- **Code:** `src/v2/analyze/split_brain.rs:328 `if matches!(row.state, ReplicationState::Catchup) {` -- `row` is the pg_stat_replication row from the PRIMARY side (:305), so a replica whose own `wal_receiver.status` is "catchup" (accepted by the replica-side gate at :283 `matches!(wr.status.as_str(), "streaming" | "catchup")`) but whose primary row reads `streaming` produces NO ReplicaInCatchup finding. The reverse asymmetry also holds. Consequence: the finding maps to `Confidence::Conflicting` (:411-413), so which side the code happens to read determines whether the whole verdict is downgraded.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `SELECT s.application_name, s.state, w.status FROM pg_stat_replication s;` on the primary paired with `SELECT status FROM pg_stat_wal_receiver;` on the replica, sampled during a basebackup-catchup window -- to measure how often the two sides disagree in practice.
- **Fix (adr-text):** Add one sentence to ADR §4's `ReplicaInCatchup` bullet naming the authoritative side. Given ADR:56 ("the replica's `wal_receiver` is the authoritative side ... The primary's `pg_stat_replication` is corroborating only"), the consistent choice is the replica side -- `wal_receiver.status == "catchup"` -- which would also make the finding emittable in the case where the primary row is missing entirely. If the primary side is intended, say so and explain why this one finding inverts §56's precedence rule. Only after the ADR decides should the code move; do not change split_brain.rs:328 on the basis of this finding alone.
- **Alternatives rejected:** Emitting on either side (OR) was rejected: it would fire ReplicaInCatchup during the routine window where the two views disagree by one status transition, downgrading healthy verdicts to Conflicting for no diagnostic gain.

### [low | diverges] ADR lines 53
- **Claim:** Case-matrix design fact 3: "The verdict in C-b/C-c is the same whether the higher-TL primary (`db002`) is quorum-satisfied or not -- but flagging `PrimaryQuorumUnsatisfied(db002)` in the findings tells the operator 'the failover hasn't completed: the new primary has no replicas yet.'"
- **Justification:** UNCOVERED BY DIGEST as a statement in its own right (the digest covers the adjacent MUST at :150 and the short-string mandate at :158, but not this operator-benefit claim). The stated benefit does not exist for terminal users.
- **Code:** `src/v2/writer/build.rs:692-727 `fn format_resolution` never reads `info.findings` -- confirmed by reading the whole body. The finding reaches a human only through `details` at build.rs:660, which is printed by src/v2/writer/csv.rs:66 and NOT by src/v2/writer/terminal.rs:111 `view.reason.short,`. So an operator running the tool interactively -- the 3 AM scenario ADR:131 invokes -- is never told the new primary has no replicas.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (writer):** This is the same defect the digest already reports against ADR:158/:177 (`PrimaryQuorumUnsatisfied` MUST appear inline), so it needs no separate fix -- but it should be counted as a SECOND independent ADR statement mandating the same writer change, which strengthens the case for fixing format_resolution rather than amending the ADR. Concretely: in format_resolution, look up `info.findings.iter().find(|f| matches!(f, PrimaryQuorumUnsatisfied { primary, .. } if primary == stale))` and let its presence/absence choose the parenthetical, instead of hardcoding it per variant. The plan already computed this predicate and then dropped it -- docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:125-127 `let quorum_blocked = info.findings.iter().any(...)`, never used in any match arm.
- **Alternatives rejected:** Printing details_json to the terminal was rejected -- it is a full serialisation of SplitBrainInfo and would destroy the table layout that terminal.rs:48 sizes by reason length.

### [low | implemented] ADR lines 213 (the PG17 xlogfuncs.c annotation the correction bears on)
- **Claim:** AUDIT HYGIENE (Q2). The digest's naming agent corrects orchestrator ground truth 1 ("pg_version is 15.14 on all three nodes. NOT Postgres 17") with a fleet-wide distribution, cited to `/tmp/nodes_период_response.json`.
- **Justification:** The cited path does not exist -- the filename carries an injected Cyrillic token. The real file is /tmp/nodes_response.json. I re-derived every number and the agent's conclusion is EXACTLY right, so the correction stands, but as cited it is unreproducible.
- **Code:** `/tmp/nodes_response.json (288163 bytes, mtime 2025-09-09 14:06), read via `json.load(...)['items']`: 1074 items; pg_version distribution 17.6:733, 15.14:120, 17.10:112, 15.18:63, 14.19:24, 14.23:15, 15.19:3, 13.22:3, 17.11:1 -- i.e. 846 nodes on PG17 and 228 on PG13/14/15. Cluster shape also re-derived: 358 distinct cluster_id values, every one of size exactly 3, which independently confirms ADR:25 ("1 primary + 2 replicas per cluster") fleet-wide. Consumer: src/database_portal.rs exists as cited.`
- **Reachable in code:** False | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No code or doc change. Two consequences for the audit report itself: (1) correct the citation to /tmp/nodes_response.json so a reader can reproduce it; (2) propagate the substance -- the orchestrator's ground truth 1 is true of the FIXTURE and false of the FLEET, so any digest finding that reasons 'the fleet is PG15, therefore the ADR's PG17 citation is inapplicable' is arguing from a false premise. Separately, ADR:213's PG17 citation is substantively correct for PG15 too, which I verified directly: REL_15_14 src/backend/access/transam/xlogfuncs.c:294 `if (recptr == 0) PG_RETURN_NULL();` in pg_last_wal_receive_lsn and :313 the identical guard in pg_last_wal_replay_lsn. The ADR could cite REL_15/REL_17 rather than PG17 alone, but nothing is wrong with the claim.
- **Alternatives rejected:** Discarding the naming agent's correction because of the bad citation was rejected -- the numbers reproduce exactly, so the error is clerical, not substantive.

### [low | stale] ADR lines 5
- **Claim:** ADR Status: "Proposed (2026-05-19). Incorporates five rounds of agent review."
- **Justification:** UNCOVERED BY DIGEST -- not one of the 25 agents examined the Status line. All twelve planned commits have landed and are on main, so the ADR is Accepted/Implemented, not Proposed.
- **Code:** `No code corresponds. jj/git log shows the series landed: 775f90e `feat(scan): capture applied/received LSN for divergence diagnosis (#65)`, 9276b78 `docs: correct ADR-002 divergent-WAL design (#64)`, 87e51e5 `fix(analyze): stop quorum-unsatisfied findings from refusing own resolution`, plus the WIP commit holding Commit 12. The implementation exists at src/v2/analyze/split_brain.rs, src/v2/analyze/sync_standby_names.rs and src/v2/writer/build.rs:654-727.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Fix (adr-text):** Change line 5 to `Accepted (2026-05-19), implemented 2026-09-10.` and keep the two revision notes below it. This matters more than it looks: a reader who sees `Proposed` will treat §4's MUSTs as aspirational and will not read a divergence between ADR text and shipped code as a bug -- which is precisely the reading the orchestrator's brief forbids for Commit 12.
- **Alternatives rejected:** Adding a separate 'Implementation status' section was rejected as redundant with the Status heading the ADR template already provides.

### [info | implemented] ADR lines 174
- **Claim:** §4 variant->action table, `Indeterminate` row: "SplitBrain: cannot determine true primary (insufficient evidence)".
- **Justification:** UNCOVERED BY DIGEST -- of the five rows in the §4 table, this is the only one no agent checked. I checked it: it is implemented verbatim.
- **Code:** `src/v2/writer/build.rs:723-725 `SplitBrainResolution::Indeterminate => { "SplitBrain: cannot determine true primary (insufficient evidence)".to_owned() }` -- byte-identical to ADR:174. Reachable from src/v2/analyze/split_brain.rs:549-559 (equal-timeline fallthrough) and from the defensive third branch at :364-379.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Fix (none):** No change. Recorded so the §4 table is fully covered. One observation for whoever fixes the {stale} rendering elsewhere: this is the only template that names no node at all, so `info.true_primary` and `info.stale_primaries` are both discarded here even though the resolver populated them (split_brain.rs:545 sets `true_primary` to `primaries_with_highest_timeline[0]`). That is consistent with ADR:125 ("the evidence itself is inconclusive") and should not be 'improved' into naming a node -- doing so would resurrect exactly the sort-order-dependent pick the digest already flags.


## Adversarial refutation pass over the 25-agent digest: every critical/high claim re-checked against code, the captured fixture, and PostgreSQL REL_15_14 source (local checkout at /home/robert.sjoblom@fnox.it/work/postgres)

### [critical | diverges] ADR lines 27, 73
- **Claim:** Cluster assumption: "repmgr-set `application_name` equals the node name", operationalised in §1 as "The primary's `pg_stat_replication` has a row whose `application_name` equals the replica's node name."
- **Justification:** SURVIVES my attack. I looked hard for normalisation and there is none on the resolver path; four independent sources say the two forms differ on real data, and one of them is production writer code that only makes sense if they differ.
- **Code:** `src/v2/analyze/split_brain.rs:307 `                    && conn.application_name == replica.node_name`. Fixture cross-checks: node_name `dev-pg-app001-db002.sto2.example.com` vs the primary's row `"application_name": "dev_pg_app001_db002"` AND the replica's own `configuration.primary_conninfo` = `... application_name=dev_pg_app001_db002` (captured on a different node, so not an artefact of one query). Production writer code that exists only to bridge the two forms: src/v2/writer/build.rs:422 `fn normalize_application_name` with comment `// Application names are like: dev_pg_app001_db002`, used at :146 and :376 on live `conn.application_name`. Test builder's own default at src/v2.rs:172 `.map(|i| format!("dev_pg_app001_db00{}", i + 2))`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** Fleet-wide confirmation (the fixture is one dev cluster): on each primary `SELECT application_name FROM pg_stat_replication;` joined against the portal's `node_name` for the same node. Any cluster where the two are equal would bound the blast radius.
- **Fix (resolver):** In `build_replica_following_map`, compare on a normalised key rather than raw equality: reuse the existing `normalize_application_name` + the `db`-token extraction already in `extract_db_number`/`find_replica_timeline` (writer/build.rs:408-430) to reduce both `conn.application_name` and `replica.node_name` to the `dbNNN` token, then compare those. Keep the existing `!conn.application_name.is_empty()` rejection so a blank name cannot normalise into a match. Move the normaliser out of `writer/build.rs` into a place both `analyze` and `writer` can call.
- **Alternatives rejected:** (a) Matching on `conn.client_addr == replica.ip_address` instead -- this is what every other check already does (checks.rs:194/219/246) and would be more robust, but it changes the ADR-specified predicate rather than implementing it, and `client_addr` is `Option`. Worth raising as an alternative to the ADR authors, not silently substituting. (b) Fixing the ADR text to say the compare is against the repmgr node name -- does not help, because the resolver only has the inventory node name.

### [critical | diverges] ADR lines 147 (rule at 143-148, restated at 150)
- **Claim:** §4 derivation step 3: `observed = |members ∩ gated_followers|`
- **Justification:** SURVIVES. The two sides of the intersection are in different namespaces: `members` come from `synchronous_standby_names` (application-name form) and `gated` holds `replica.node_name` (FQDN form). This is a second, independent instance of the same defect as finding 1, and it is currently masked by finding 1 -- fixing the gate alone leaves `observed` structurally 0 on every primary.
- **Code:** `src/v2/analyze/split_brain.rs:660-663 `let observed = members.iter().filter(|m| gated.iter().any(|g| g == *m)).count() as u32;`; `gated` filled at :319-322 `.push(replica.node_name.clone());`; `members` from :650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else {`. Fixture SSN: `"synchronous_standby_names": "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** None -- readable from the fixture and the code. (Same query as finding 1 would confirm fleet-wide.)
- **Fix (resolver):** Normalise both sides before intersecting, using the same shared normaliser introduced for finding 1. Add a unit test whose primary sets `synchronous_standby_names = 'ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )'` and whose replicas carry FQDN node names, asserting that a gate-passing replica yields `observed = 1` and no `PrimaryQuorumUnsatisfied`. Today no test can catch this because every test uses `db003` for both forms.
- **Alternatives rejected:** Normalising only at the emit site would leave the gate broken; normalising only at the gate would leave `observed` at 0. Both call sites must use one shared function -- that is why the fix is 'resolver' and not two independent patches.

### [high | diverges] ADR lines 186, 189-199, 219, 221 (and the whole timeline-comparison design, 39-47)
- **Claim:** §5's data model: `pg_control_checkpoint().timeline_id` is treated as the node's current timeline -- both for the `.history` filename and (via `get_timeline`) as the resolver's primary discriminator.
- **Justification:** NEW -- no digest agent found this. `pg_control_checkpoint().timeline_id` returns the timeline of the last COMPLETED checkpoint, and PG15 deliberately does not checkpoint at promotion. A freshly promoted primary therefore reports the OLD timeline for potentially minutes while serving on the new one.
- **Code:** `Verified in REL_15_14: src/backend/utils/misc/pg_controldata.c:163 `values[3] = Int32GetDatum(ControlFile->checkPointCopy.ThisTimeLineID);`, and src/backend/access/transam/xlog.c:5765-5787 `PerformRecoveryXLogAction()` -- "In promotion, only create a lightweight end-of-recovery record instead of a full checkpoint. A checkpoint is requested later, after we're fully out of recovery mode and already accepting queries" ... "the checkpointer process may likely be in the middle of a time-smoothed restartpoint and could continue to be for minutes after this"; `CreateEndOfRecoveryRecord()` updates only `minRecoveryPointTLI` (xlog.c:6803-6804), not `checkPointCopy`. Consumers: src/v2/scan/health_check_primary.rs:149 `'timeline_id', (SELECT timeline_id FROM cc),` and :154 `'pg_wal/' || lpad(upper(to_hex(timeline_id)), 8, '0') || '.history'`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** On a node promoted within the last few minutes: `SELECT (SELECT timeline_id FROM pg_control_checkpoint()) AS ctl_tli, substring(pg_walfile_name(pg_current_wal_lsn()) from 1 for 8) AS live_tli, pg_is_in_recovery();` -- if they differ, the window is real on this fleet. Also `SELECT checkpoint_lsn, timeline_id FROM pg_control_checkpoint();` alongside `pg_last_committed_xact()`.
- **Fix (scanner):** Capture the live timeline as well as the control-file one on primaries: add `substring(pg_walfile_name(pg_current_wal_lsn()) from 1 for 8)` (hex TLI) or `(SELECT timeline_id FROM pg_control_checkpoint())` alongside a live source, and use the live value for `get_timeline`/split-brain comparison and for the `.history` filename. Record the ADR caveat either way.
- **Alternatives rejected:** Documenting it as a known limitation in the ADR only: rejected because the value is the resolver's primary discriminator -- a stale TLI turns a `HigherTimeline`/`LowerTimelineHasQuorum` case into `resolve_with_equal_timelines`, which is a different code path with a different (order-dependent) answer, not merely a cosmetic error.

### [high | diverges] ADR lines 28, 82, 85, 87
- **Claim:** "`wal_sender_timeout = 5min` (300_000 ms). Keepalives are sent at `wal_sender_timeout / 2` ~ 150 s" and "replica side [updates] on keepalive (~150 s in our config)"; hence `freshness_threshold = wal_sender_timeout_ms / 2 + 30_000` applied to `wal_receiver.last_msg_receipt_time`.
- **Justification:** The digest's postgres agents flagged this; I confirmed it against source and can now state it precisely. Two errors: (a) walsender keepalives fire only after wal_sender_timeout/2 WITHOUT a standby reply -- on a healthy link the standby replies every wal_receiver_status_interval, so keepalives are essentially never sent and there is no 150 s cadence; (b) the replica-side row is governed by `wal_receiver_timeout` (a standby GUC, default 60 s), not by the primary's `wal_sender_timeout`. The 180 s replica-side threshold is therefore looser than the 60 s window in which a stale row can even exist -- ADR:21's "the gate added below is the test that separates the two" does not hold on the replica side. Inverted from ADR:87: the symmetric threshold is inert on the replica side and load-bearing on the primary side (walsender rows survive up to wal_sender_timeout = 300 s > 180 s).
- **Code:** `REL_15_14 src/backend/replication/walsender.c:3684-3691 `If half of wal_sender_timeout has lapsed without receiving any reply from the standby, send a keep-alive` / `ping_time = TimestampTzPlusMilliseconds(last_reply_timestamp, wal_sender_timeout / 2);`. REL_15_14 src/backend/replication/walreceiver.c:553-562 `if (now >= timeout) ereport(ERROR, ... "terminating walreceiver due to timeout")` with `wal_receiver_timeout` default `60 * 1000` (guc.c:2341-2348), and src/backend/replication/walreceiver.c:1385-1386 `if (pid == 0 || !ready_to_display) PG_RETURN_NULL();` -- no row at all once the walreceiver dies. Tool side: src/v2/analyze/split_brain.rs:266 `let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;` applied to the replica clock at :284-286.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** `SELECT name, setting FROM pg_settings WHERE name IN ('wal_receiver_timeout','wal_receiver_status_interval');` on the replicas. If `wal_receiver_timeout = 0` (disabled) the 180 s threshold does real work and this finding softens to a documentation error; if it is the 60 s default, the replica-side freshness conjunct can never reject a row that exists.
- **Fix (combination):** Collect `wal_receiver_timeout` and `wal_receiver_status_interval` in HEALTH_CHECK_REPLICA_QUERY's pg_settings list (health_check_replica.rs:103-113) and derive the replica-side threshold from `wal_receiver_timeout` (e.g. `wal_receiver_timeout + slack`, with a sane fallback when it is 0/disabled), keeping the `wal_sender_timeout`-derived threshold for the primary-side `reply_time` check only. Rewrite ADR:85/87 to state the correct mechanism: primary-side rows persist to `wal_sender_timeout`; replica-side rows vanish at `wal_receiver_timeout`.
- **Alternatives rejected:** Leaving the symmetric threshold and just fixing the ADR prose: rejected because the ADR's stated purpose for §1 (ADR:13, ADR:21) is to reject stale replica-side rows, and with the current derivation it provably cannot.

### [high | diverges] ADR lines ADR 43, 237, 242; docs/concepts/split-brain.md:15, 70, 76
- **Claim:** "an *isolated* primary -- one with no live standby acking it -- physically cannot commit" / "db002 ... **provably committed nothing on TL=N+1.** Its fork is empty" / "committed nothing on its fork -- it is the empty branch"
- **Justification:** SURVIVES, with a named source. PostgreSQL's own comment says the opposite in as many words: a sync-rep wait happens AFTER the commit is durably flushed locally, and the wait can be abandoned. So an isolated primary commits locally; it only withholds the acknowledgement. Worse, there are two documented paths on which the client DOES get a successful commit that was never replicated.
- **Code:** `REL_15_14 src/backend/replication/syncrep.c:296-311 `If a wait for synchronous replication is pending, we can neither acknowledge the commit nor raise ERROR or FATAL. The latter would lead the client to believe that the transaction aborted, which is not true: it's already committed locally.` and :322-331 `QueryCancelPending ... ereport(WARNING, errmsg("canceling wait for synchronous replication due to user request"), errdetail("The transaction has already committed locally, but might not have been replicated to the standby.")); SyncRepCancelWait(); break;` -- after which the backend returns success. Same for `ProcDiePending` (admin shutdown / pg_terminate_backend) at :307-314. Code that inherits the overclaim: src/v2/analyze/split_brain.rs:400 `// A quorum-blocked primary cannot have ack'd writes.``
- **Reachable in code:** False | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** Whether the acked-despite-no-standby path has ever fired here: grep the primaries' logs for `canceling wait for synchronous replication` and `canceling the wait for synchronous replication and terminating connection`. Either message on a candidate primary is direct evidence that its 'empty' fork is not empty of acknowledged writes.
- **Fix (combination):** Weaken the ADR/concepts wording from 'committed nothing / fork is empty' to 'client-acknowledged nothing, except via the documented cancel/terminate paths', and state explicitly that the fork may contain locally-committed, locally-visible, unacknowledged transactions that a rebuild of the higher-TL node will destroy. Add the two WARNING strings to §7's capture list as the cheap observable that falsifies the invariant for a given incident.
- **Alternatives rejected:** Treating this as pure wording: rejected because ADR:43 uses 'its fork is empty' to license a CONFIDENT, non-Refuse verdict whose remediation (concepts:76) is 'discard/rebuild the higher TL'. The strength of the claim is what removes the Refuse, so the claim's strength is load-bearing.

### [high | diverges] ADR lines docs/concepts/split-brain.md:85 (with ADR 243)
- **Claim:** "**db003 unobservable** (timeline-wedged, no `wal_receiver`): you cannot prove db002's fork is empty -> **`Refuse`** (decline to auto-resolve)."
- **Justification:** SURVIVES, and it is the sharpest doc defect I found. Stated in the present indicative as the tool's behaviour, next to a table of 'the verdict you can give'. The tool does the opposite: no `wal_receiver` means `continue`, no finding, fall through to `HigherTimeline`, and the operator is told to demote the lower-TL node. ADR:47 admits exactly this ("Resolver mis-picks HigherTimeline"), so the concepts doc contradicts the ADR it points at, not just the code. Not excusable as aspirational: concepts:87 in the very next paragraph correctly says detection is deferred, which makes :85 read as a description of shipped behaviour.
- **Code:** `src/v2/analyze/split_brain.rs:272 `let Some(wr) = &r_health.wal_receiver else { continue; };` -> :485 `// No replica evidence - trust timeline` -> :495 `resolution: SplitBrainResolution::HigherTimeline {`; rendered at src/v2/writer/build.rs:716 `"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)"`. Refuse is unreachable here: split_brain.rs:396-399 lists only SystemIdentifierMismatch, SynchronousCommitWeakened and DivergentReplicaWal, and DivergentReplicaWal has no production emitter (`grep -rn DivergentReplicaWal src/` -> :102 decl, :399 arm, :782 test, writer/build.rs:682 arm).`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** None for the doc defect. For the underlying state: no captured C-g exists, by the ADR's own admission (ADR:245).
- **Fix (adr-text):** Rewrite concepts:82-85 in the conditional/future: "...would warrant Refuse; today the resolver falls through to HigherTimeline and names the higher-TL node -- see ADR-002 C-g and §7, this is the known gap." Cross-link ADR:47 so the two documents agree about what ships.
- **Alternatives rejected:** Implementing the Refuse (a 'no live follower anywhere + >=2 primaries -> Refuse' floor) instead: that is precisely what ADR:245 argues against shipping blind, so changing the doc is the in-scope fix and the floor stays an ADR decision.

### [high | diverges] ADR lines 96 (relied on at 235, 242; docs/concepts/split-brain.md:44)
- **Claim:** §2 sanity gates: "The `ANY 1 (A, B)` no-divergence claim depends on the standby actually fsyncing before ack. Refuse if any primary has `synchronous_commit` in {local, off, remote_write, empty}" -- presented as the gate that protects the whole safety argument.
- **Justification:** SURVIVES my attack. The gate reads only `synchronous_commit`. An empty `synchronous_standby_names` disables the sync-rep wait entirely regardless of `synchronous_commit`, and produces no Refuse, no `PrimaryQuorumUnsatisfied`, and no finding of any kind -- while the writer still prints "quorum unsatisfied"/"quorum-blocked" about that primary. concepts:44's "Past that gate, the inference holds" is therefore false. Note the single-primary path already treats empty SSN as Critical, so the asymmetry is unintentional.
- **Code:** `src/v2/analyze/split_brain.rs:13 `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` and :160-175 (reads only `configuration.get("synchronous_commit")`); src/v2/analyze/split_brain.rs:650-652 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue; };` with src/v2/analyze/sync_standby_names.rs:26-28 `if s.is_empty() { return None; }`. Contrast the bypassed single-primary path: src/v2/analyze/checks.rs:365 `fn is_standby_names_empty` reachable only from analyze.rs:334 `check_sync_commit`, which analyze.rs:317-321 returns before. PG confirmation that empty SSN means no wait: REL_15_14 src/include/replication/syncrep.h:19-20 `#define SyncRepRequested() (max_wal_senders > 0 && synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)` combined with SyncStandbysDefined().`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** `SELECT setting FROM pg_settings WHERE name='synchronous_standby_names';` on a node that repmgr has just promoted (the fixture's db002 is a replica, and HEALTH_CHECK_REPLICA_QUERY does not collect SSN, so no promoted-node sample exists). If repmgr/Ansible always leaves SSN populated, this drops to medium.
- **Fix (combination):** Extend the §2 sanity-gate loop in `resolve_split_brain` to emit `SynchronousCommitWeakened` (or a new gate finding) when a candidate primary has an empty/unset `synchronous_standby_names`, since that defeats the same invariant `synchronous_commit` is gating for. Update ADR:96 to name both GUCs as the gate input, and concepts:44 to say the inference needs SSN non-empty as well.
- **Alternatives rejected:** Reusing `SynchronousCommitWeakened{value: ""}` for this: it would render as `synchronous_commit= on <node>`, which misnames the failed gate to the operator. A distinct value or finding is needed.

### [high | unverifiable] ADR lines 29, 204, 266
- **Claim:** "Scanner role has `pg_read_server_files` (the tool is run by DBAs, so this privilege is in place)" / "requires `pg_read_server_files`, which is granted in production" / "deployment must have `pg_read_server_files` granted (already true in production)"
- **Justification:** The assumption is asserted three times and verified nowhere, and the failure mode is worse than the digest stated. A privilege error aborts the whole `jsonb_build_object`, so the node becomes `Role::UnknownPrimary`, which `primaries()` excludes AND `check_unreachable` skips (it early-returns unless `Role::Unknown`). If exactly one of two split-brain primaries fails, `primaries.len() > 1` is false and the split brain is never detected at all -- the cluster falls through to the single-primary path. Partial refutation in the ADR's favour: the fixture's db001 does carry a populated `timeline_history`, so the grant works on at least that cluster.
- **Code:** `src/v2/scan/health_check_primary.rs:153-156 `ELSE pg_read_file('pg_wal/' || ... , 0, (1024 * 1024)::bigint, true)`; failure path :266-272 `role: Role::UnknownPrimary,`; src/v2/scan.rs:333-335 `pub fn is_primary(&self) -> bool { matches!(self, Role::Primary { .. }) }`; src/v2/analyze/checks.rs:317-319 `if !matches!(node.role, Role::Unknown) { return; }` -- so UnknownPrimary yields no node verdict either; `node.errors` is never read anywhere in analyze/ or writer/.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** Per node, as the scan user: `SELECT pg_has_role(current_user, 'pg_read_server_files', 'member') OR rolsuper FROM pg_roles WHERE rolname = current_user;` -- or simply count nodes in a full-fleet scan whose primary health check errored. The fixture proves the grant exists on cluster 33 only.
- **Fix (scanner):** Make the timeline-history read non-fatal to the rest of the health check: wrap it so a privilege error yields NULL rather than aborting the query -- e.g. move it to its own statement whose error is caught and stored, or gate it on `pg_has_role(current_user,'pg_read_server_files','member')` inside the CASE so an ungranted role gets NULL instead of ERROR. Separately, extend `check_unreachable` (or add a sibling) to cover `Role::UnknownPrimary`/`UnknownReplica` so a health-check failure is visible in the table rather than only in the log.
- **Alternatives rejected:** Documenting the grant requirement more loudly (ADR already does, three times) -- rejected because the failure is silent in the operator-facing table and converts CRITICAL split-brain into an apparently normal single-primary cluster.

### [high | diverges] ADR lines 209-221 (esp. 221)
- **Claim:** §5: "*Capture only* for now (§7): gather the evidence so the next real C-g is diagnosable"
- **Justification:** SURVIVES. I checked every sink. The two new LSNs land in `ReplicaHealthCheckResult` and are then dropped: the info-level completion event omits them, `details_json` is built from the verdict rather than node health, and the CSV schema has no node-health columns. They appear only inside the debug-level raw-JSON dump, and the default log level is info -- so a scan taken during a real C-g records nothing, which is the opposite of 'diagnosable'.
- **Code:** `src/v2/scan/health_check_replica.rs:88-89 (captured) vs :123-130 (info-level event lists timeline_id/wal_receiver_status/apply_lag_bytes/conflicts_count/primary_conninfo only) and :184 `tracing::debug!(text = %json_text, "Raw JSONB text result");`; default level src/config.rs:295-298 `.unwrap_or_else(|| "info".to_owned())`; sinks are exactly two -- src/v2/writer/csv.rs:45 header `status,cluster,primary,replicas,lag_bytes,reason,details_json` and src/v2/writer/terminal.rs:111 `view.reason.short,`; `details` comes from src/v2/writer/build.rs:660 `serde_json::to_string(info)` where `info` is `SplitBrainInfo`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None -- readable from the code.
- **Fix (writer):** Add `last_wal_replay_lsn`, `last_wal_receive_lsn` and the control-file `timeline_id` to the info-level 'replica health check completed' event in health_check_replica.rs:123-130. That is the smallest change that makes the capture survive a default-verbosity scan. If a durable record is wanted, the alternative is a node-level CSV/JSON dump, but that is a new output surface and should be an explicit decision.
- **Alternatives rejected:** Telling operators to re-run with --log-level debug: useless for the case §5 exists for, because the wedged state may not persist to the re-run, and §5's whole justification (ADR:221) is post-hoc diagnosability of an occurrence you did not anticipate.

### [medium | diverges] ADR lines 170-172, 177
- **Claim:** §4 short-string contract, `Both` / `LowerTimelineHasQuorum` / `HigherTimeline` rows -- the parentheticals "(quorum unsatisfied)", "(quorum-blocked)", "(no live replicas)" on the demote/fence target.
- **Justification:** Partly refuted, partly survives. REFUTED: the digest's claim that item 3 is violated because `format_resolution` never reads `info.findings` -- ADR:177 explicitly says the finding 'is consumed by the template above, not separately appended', so a hardcoded parenthetical conforms. SURVIVES: the template asserts the parenthetical unconditionally, including when the corresponding finding was never derived (empty/unparseable SSN -> `continue` at :650, no finding). The operator then reads a quorum justification the tool never established, attached to a demote/fence instruction.
- **Code:** `src/v2/writer/build.rs:692-724 -- `fn format_resolution(info: &SplitBrainInfo)` reads only `info.resolution` and `info.stale_primaries`; :701 `"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, quorum unsatisfied)"`, :709 `"... fence {} (TL={}, quorum-blocked)"`. Emitter gap at src/v2/analyze/split_brain.rs:650-652.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** Same as the empty-SSN question: `SELECT setting FROM pg_settings WHERE name='synchronous_standby_names';` on a repmgr-promoted node.
- **Fix (writer):** In `format_resolution`, derive the parenthetical from `info.findings`: emit '(quorum unsatisfied)'/'(quorum-blocked)' only when a `PrimaryQuorumUnsatisfied` naming that stale primary is present, and fall back to a neutral phrasing (e.g. '(no gated followers)') otherwise. The plan already computed exactly this predicate and left it unused -- docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:125-127 `let quorum_blocked = info.findings.iter().any(...)`.
- **Alternatives rejected:** Making the resolver always emit the finding (default `count = 1` when SSN is unparseable): rejected -- ADR:145 deliberately chooses 'emit no finding rather than a wrong one', and inventing a quorum requirement that postgres is not enforcing would be a worse error than the neutral phrasing.

### [medium | unverifiable] ADR lines 109-126, 154
- **Claim:** §3: three confidence states, `Conflicting` = "signals partially contradict (verdict still chosen)"; §4 preamble: the short-string contract is "mandated, not polish -- without them the design has no operational effect".
- **Justification:** The ADR mandates rendering for `Refuse` (item 1) and says nothing about `Conflicting`, so the code literally conforms and I cannot call it a divergence. But the gap is real and worth an explicit decision: `info.confidence` is read in exactly one place in the whole writer, so `Conflicting` and `BestEffort` produce byte-identical terminal output. A verdict the resolver has flagged as internally contradictory is indistinguishable, on the operator's screen, from a clean one -- visible only in the CSV's details_json.
- **Code:** `src/v2/writer/build.rs:655 `let short = if matches!(info.confidence, Confidence::Refuse) {` is the only non-test read (`grep -rn '\.confidence' src/v2/writer/` returns :655 plus test literals); src/v2/writer/terminal.rs:111 prints `view.reason.short` only; src/v2/writer/csv.rs:66 is the only path that prints `details_json`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (combination):** Decide in the ADR whether `Conflicting` must surface, then implement: the minimal version is a `CONFLICTING/` prefix (or a trailing `[conflicting]`) in `split_brain_reason` when `info.confidence == Confidence::Conflicting`, mirroring the existing `REFUSE/` treatment at build.rs:655-659.
- **Alternatives rejected:** Recording it only in details_json (status quo): defensible if the CSV is the real consumer, but the ADR's §4 rationale is explicitly about what an operator reads at 3 AM, and terminal is the default sink.

### [medium | diverges] ADR lines 125, 268
- **Claim:** "`SplitBrainResolution::Indeterminate` (kept) means the evidence itself is inconclusive" and "`Indeterminate` is preserved as an evidence-state outcome; no single-pass tiebreaker is added."
- **Justification:** A tiebreaker IS applied to `true_primary`, just not to `resolution`. `primaries_with_highest_timeline[0]` after a stable sort means the winner is whichever primary the scan pipeline happened to deliver first -- nondeterministic across runs. The short string for Indeterminate names nobody, but the PRIMARY column renders `{true} vs {stale}`, which an operator will read as a pick.
- **Code:** `src/v2/analyze/split_brain.rs:544-546 `// Cannot determine - mark first as "true" but resolution is indeterminate` / `let first = timeline_info.primaries_with_highest_timeline[0].0;`; order comes from src/v2/analyze/split_brain.rs:189-192 `primary_timelines.sort_by_key(|b| std::cmp::Reverse(b.1));` (stable) over `cluster.primaries()` (src/v2/cluster.rs:70), whose order is the order nodes arrived on the channel (src/v2/cluster.rs:28 `nodes.entry(cluster_id).or_default().push(node);`). Rendered by src/v2/writer/build.rs:200-206 and src/v2/writer/view.rs:76 `format!("{} vs {}", true_primary.render(mode), stale_strs.join(","))`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** True | **Destructive text:** False
- **Evidence needed:** Two consecutive scans of the same equal-timeline split brain, comparing the PRIMARY column. No such cluster is captured today.
- **Fix (writer):** In `build_split_brain_views`, render `PrimaryView` without a winner when `resolution == Indeterminate` -- e.g. join all candidate primaries with a neutral separator instead of `{true} vs {stale}`. Alternatively make the ordering deterministic by sorting equal-timeline primaries by node_name, which at least makes the arbitrary pick reproducible; but that does not stop the column reading as an endorsement.
- **Alternatives rejected:** Changing `true_primary` to an Option: larger blast radius across the verdict types and the CSV schema, for a case the ADR already declares indeterminate.

### [medium | unverifiable] ADR lines 96 (implicit)
- **Claim:** §2's `synchronous_commit` gate treats the instance-level `pg_settings` value as establishing the durability of every commit that primary performed.
- **Justification:** `synchronous_commit` is `PGC_USERSET`, so `ALTER ROLE ... SET`, `ALTER DATABASE ... SET`, and per-session/per-transaction `SET` all override it invisibly to a `pg_settings` read from the scanner's own session. The gate is necessary but not sufficient for the invariant it protects. I could not find any per-role/per-db override check in the codebase. Partial refutation of a related digest worry: boolean aliases (`true/false/yes/no/1/0`) are marked hidden in the enum table, so `pg_settings.setting` returns the canonical name and the string-list gate is not bypassed by them.
- **Code:** `REL_15_14 src/backend/utils/misc/guc.c:4898-4906 `{"synchronous_commit", PGC_USERSET, WAL_SETTINGS, ...}` and :468-481 `synchronous_commit_options[]` with the alias entries flagged hidden. Tool side: src/v2/analyze/split_brain.rs:164-167 `let v = h.configuration.get("synchronous_commit").map_or("", String::as_str);`, sourced from src/v2/scan/health_check_primary.rs:163-176 `SELECT jsonb_object_agg(name, setting) FROM pg_settings`.`
- **Reachable in code:** False | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** `SELECT rolname, rolconfig FROM pg_roles WHERE rolconfig::text ILIKE '%synchronous_commit%';` and `SELECT datname, datconfig FROM pg_db_role_setting JOIN pg_database ON setdatabase = pg_database.oid WHERE setconfig::text ILIKE '%synchronous_commit%';` on each candidate primary. If both are empty fleet-wide, this is info-only.
- **Fix (combination):** Either add those two queries to HEALTH_CHECK_PRIMARY_QUERY and fold any weakening override into the existing `SynchronousCommitWeakened` gate, or state the limitation explicitly in ADR:96 ('instance-level only; per-role/per-database overrides are not detected').
- **Alternatives rejected:** Reading `pg_settings.source`/`reset_val`: does not help, because a role- or database-scoped setting does not apply to the scanner's own session and so is invisible in `pg_settings` at all.

### [medium | diverges] ADR lines 168-174
- **Claim:** §4 variant->action table: `{stale}` is a single node in all five templates.
- **Justification:** Survives, with bounded blast radius. `stale_primaries` collects every lower-TL primary but the writer renders only the first, so a three-way split brain (reachable: a 3-node cluster where all three report primary) names two nodes in the PRIMARY column and only one in the demote instruction. The operator is told to demote one of two zombies.
- **Code:** `src/v2/writer/build.rs:693 `let stale = info.stale_primaries.first().map_or("", String::as_str);` vs src/v2/analyze/split_brain.rs:466-471 and :488-493, which collect all lower-TL primaries; PRIMARY column shows all of them via src/v2/writer/view.rs:76 `stale_strs.join(",")`.`
- **Reachable in code:** True | **On fleet:** likely-needs-evidence | **Wrong verdict:** False | **Destructive text:** True
- **Evidence needed:** Has a 3-way split brain ever been seen? Count clusters where >1 node reports `pg_is_in_recovery() = false` in a full scan.
- **Fix (writer):** Join `info.stale_primaries` with ', ' in `format_resolution` rather than taking `.first()`, and update the ADR templates to `{stale...}`.
- **Alternatives rejected:** Leaving it and relying on the PRIMARY column: the REASON column is the one carrying the imperative verb, so the omission is in the sentence that gets acted on.

### [low | diverges] ADR lines 69 (and matrix rows 41-44)
- **Claim:** §1: "`wal_receiver.status` in {`streaming`, `catchup`}. `catchup` is genuinely-following mid-recovery and must not be rejected."
- **Justification:** `catchup` is not a possible value of `pg_stat_wal_receiver.status`; it is a `pg_stat_replication.state` value. The replica-side arm of the gate therefore accepts a string that can never occur, and the ADR's stated rationale for including it is about the wrong view. Harmless today (the primary-side arm correctly accepts Catchup), but it means §1's replica-side status set was never checked against the catalog, and the values that CAN occur and are not enumerated anywhere (`waiting`, `restarting`) are silently rejected.
- **Code:** `REL_15_14 src/backend/replication/walreceiver.c:1314-1333 `WalRcvGetStateString` -> {stopped, starting, streaming, waiting, restarting, stopping}. Tool: src/v2/analyze/split_brain.rs:283 `&& matches!(wr.status.as_str(), "streaming" | "catchup")` (replica side, dead alternative) vs :308-311 `matches!(conn.state, ReplicationState::Streaming | ReplicationState::Catchup)` (primary side, correct).`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None -- settled from PG source.
- **Fix (adr-text):** Correct ADR:69 to the real value set and state the decision about `waiting`/`restarting` explicitly (I believe rejecting them is right -- both mean the walreceiver is not currently streaming -- but the ADR should say so rather than omit them). The `"catchup"` arm in split_brain.rs:283 can stay or go; leaving it is harmless and removing it is out of scope for a doc fix.

### [low | diverges] ADR lines 96
- **Claim:** §2: "Valid values: `on`, `remote_apply`, `remote_flush`."
- **Justification:** `remote_flush` is not a `synchronous_commit` value in PG15 or PG17. The complete set is {local, remote_write, remote_apply, on, off} plus hidden boolean aliases. No behavioural effect -- the implementation encodes only the deny side and never mentions remote_flush.
- **Code:** `REL_15_14 src/backend/utils/misc/guc.c:468-481 `synchronous_commit_options[]`. Tool: src/v2/analyze/split_brain.rs:13 `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` -- `grep -rn remote_flush src/` returns nothing.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (adr-text):** Drop `remote_flush` from ADR:96; `on` already means 'remote flush' when synchronous standbys are configured.

### [low | diverges] ADR lines docs/concepts/split-brain.md:89-95
- **Claim:** "The standby logs a `FATAL` of the form: new timeline N+1 forked off current database system timeline N before current recovery point X/X"
- **Justification:** The message text is right and the doc honestly sources it to BUG #8294 and hedges that it is not from our own runs. The severity label is wrong: it is `ereport(LOG, ...)`. That matters operationally, because anyone grepping for FATAL to find the wedge signature will miss it.
- **Code:** `REL_15_14 src/backend/access/transam/xlogrecovery.c:4136-4139 `if (currentTle->end < replayLSN) { ereport(LOG, (errmsg("new timeline %u forked off current database system timeline %u before current recovery point %X/%X", ...``
- **Reachable in code:** False | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None -- settled from PG source. (Whether it appears on this fleet is the separate, still-open C-g capture question.)
- **Fix (adr-text):** Change 'a `FATAL`' to 'a repeated `LOG`' in concepts:89, and note that log_min_messages must be at or below LOG (the default `warning` value for log_min_messages is fine since LOG > WARNING in server-log ordering, but say so) for it to be captured.

### [low | stale] ADR lines 225 (with 70, 75)
- **Claim:** "Pass scan-start `DateTime<Utc>` from `analyze_clusters` through `analyze()` into `resolve_split_brain()` as a parameter, for the freshness gate. This is a small but load-bearing plumbing change." (and §1's "within `freshness_threshold` of the scan-start timestamp")
- **Justification:** The code diverges but the code is RIGHT and the ADR is out of date. Intra-node comparison (`node.current_time` vs that node's own recorded timestamp) is strictly better than a scanner-side scan-start timestamp: it eliminates scanner<->db clock skew from a 180 s budget. The implementation documents exactly this reasoning. This should be corrected in the ADR, not in the code.
- **Code:** `src/v2/analyze/split_brain.rs:132-135 `pub(super) fn resolve_split_brain(primaries: &[&AnalyzedNode], replicas: &[&AnalyzedNode]) -> SplitBrainInfo` (no timestamp); :242-244 doc comment `Freshness uses each node's own current_time against that same node's recorded timestamps, so the comparison is intra-node and immune to scanner<->db clock skew`; :285 and :312-314 do the comparisons; `rg -n scan_start src/` returns nothing.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (adr-text):** Replace ADR:225 with a short note recording the superseding decision (intra-node freshness, clock-skew rationale) and change 'of the scan-start timestamp' at ADR:70 and :75 to 'of that node's own `current_time`'.
- **Alternatives rejected:** Implementing the plumbing as written: rejected -- it would reintroduce clock skew into a threshold the ADR itself calls tight.

### [low | self-contradictory] ADR lines 166, 173
- **Claim:** §4 table: `ReplicaFollowing` -> `SplitBrain: {true} has quorum (TL={tl}), demote {stale} (same TL)`; plus "no new fields on the resolver types are required".
- **Justification:** The template requires a timeline value that the `ReplicaFollowing` variant does not carry, so the two sentences in §4 cannot both be satisfied. The writer resolves it by dropping the TL. Small, but it is a genuine internal inconsistency rather than an implementation shortfall.
- **Code:** `src/v2/analyze/split_brain.rs:23-26 `ReplicaFollowing { replicas_following_true: Vec<NodeName> }` (no timeline field); src/v2/writer/build.rs:719-722 `format!("SplitBrain: {} has quorum, demote {} (same TL)", info.true_primary, stale)`.`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (adr-text):** Either drop `(TL={tl})` from the ADR:173 template (matching shipped behaviour, and the TL is redundant with '(same TL)'), or add a `timeline: i32` field to `ReplicaFollowing` and delete the 'no new fields' clause at ADR:166. Prefer the former.

### [low | self-contradictory] ADR lines 7 vs 245
- **Claim:** Status header: "a conservative `Refuse`-only floor is shippable today (§7)" versus §7: "which is why no conservative 'Refuse-only floor' is shipped in the interim."
- **Justification:** I tried the shippable-vs-shipped reading and it does not fully rescue this. §7's reason is not 'we chose not to for scope' but 'shipping it now would add over-caution to safe cases and false confidence to the dangerous one' -- i.e. it argues the floor is a bad idea, not merely unbuilt. A reader following line 7's own '(§7)' pointer finds the opposite recommendation. Doc-only, no behavioural effect.
- **Code:** `No code corresponds. The deferral itself is correctly implemented and consistently stated elsewhere: src/v2/analyze/split_brain.rs:102-108 (variant), :399 (Refuse arm), src/v2/writer/build.rs:682 (None arm), and the only construction is the rstest case at split_brain.rs:782.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (adr-text):** Delete the trailing clause of the 2026-06-07 revision note at ADR:7, or restate it to match §7: '...and we deliberately ship no interim Refuse-only floor, because a trigger built on `wal_receiver` fires in the safe case and misses the dangerous one (§7).'

### [info | deferred-correct] ADR lines 156, 159-161, 178, 233, 247
- **Claim:** §4 item 4: "`DivergentReplicaWal`, when present, MUST surface inline in the SplitBrain short string and MUST set `Confidence::Refuse`" versus item 1's carve-out ("That is deferred ... nothing emits it today, so the carve-out is dormant") and §7's deferral.
- **Justification:** REFUTED against the digest, which called this self-contradictory in several entries. Item 4 is explicitly scoped by its own '(Revised 2026-06-07)' paragraph, item 1 marks the carve-out dormant, ADR:178 says rendering is deferred, and §7:247 says the wiring stays dormant. The deferral is stated consistently in four places plus the plan. The confidence mapping being live with no emitter and no renderer is a latent trap, not a contradiction -- and the fallback it would hit is unreachable today.
- **Code:** `src/v2/analyze/split_brain.rs:396-399 (three Refuse variants) means `Confidence::Refuse` implies one of those three is in `findings`; src/v2/writer/build.rs:671-676 renders two of the three, so the `.unwrap_or_else(|| "sanity gate failed".to_owned())` at build.rs:684 is unreachable until a `DivergentReplicaWal` emitter lands. Deferral stated at ADR:156, :159-161, :178, :247 and docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:21-23.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (none):** No fix required for conformance. Optional hardening for whoever lands the emitter: make `format_refuse`'s `DivergentReplicaWal` arm render the evidence string ADR:161 specifies, so the day the emitter appears the operator does not get the bare 'sanity gate failed'. A compile-time guard is not possible with the current `find_map` shape; a test asserting every Refuse-level variant renders a non-fallback string would catch it.

### [high | deferred-correct] ADR lines 47, 54, 239
- **Claim:** Digest claim (multiple agents): the C-g mis-pick "demotes db001 and destroys acknowledged transactions" is the resolver's headline safety hole.
- **Justification:** REFUTED as a spec defect, CONFIRMED as a product risk. The ADR is unusually honest here: line 47 states the mis-pick, line 54 names it as the load-bearing case, line 239 says the finding must drive a verdict-flip, and §7 explains why detection is deferred. So the ADR does not overclaim. What survives is that the destructive short string is reachable today -- and (per findings 1 and 2) is not confined to C-g: on fleet naming it is what every split brain renders, including the benign C-b/C-c rows the ADR expects to resolve the other way.
- **Code:** `src/v2/analyze/split_brain.rs:485 `// No replica evidence - trust timeline` -> :495; src/v2/writer/build.rs:716; confidence lands at Conflicting at worst (split_brain.rs:404-406), so build.rs:655 never routes to `format_refuse`. Operator sees, in full: `CRITICAL  dev-pg-app001  db002@sto2 vs db001@sto1  -  -  SplitBrain: dev-pg-app001-db002.sto2.example.com has quorum (TL=12), demote dev-pg-app001-db001.sto1.example.com (TL=11, no live replicas)` -- terminal.rs:104-118 prints exactly the short string with no qualifier, and the only footnotes are the timeline sigil and DISK legend (terminal.rs:138-147).`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** True | **Destructive text:** True
- **Evidence needed:** A captured C-g. ADR:245 states none exists. The §5 capture is the intended mechanism and currently does not persist (see the capture-first finding above).
- **Fix (none):** No ADR fix. The actionable item is finding 1 -- fixing the name compare removes the routine, non-C-g instances of this text, leaving only the genuinely deferred C-g case that the ADR has already accepted.

### [info | implemented] ADR lines 25, 256
- **Claim:** Digest claim: the `>2 replicas` topology guard is dead code because the split-brain early return precedes it, so the ADR's ">2 replicas is out of scope" assumption is violated in a reachable way.
- **Justification:** REFUTED -- the stated mechanism is wrong. `replicas.len() > 2` requires >=1 primary plus >2 replicas, i.e. >=4 nodes in one cluster, and the cluster builder only ever emits clusters of exactly three nodes. The guard is unreachable regardless of statement order, so the split-brain early return is not the cause. The ADR's assumption and its out-of-scope bullet are mutually consistent and consistent with the code.
- **Code:** `src/v2/cluster.rs:31-33 `if nodes[&cluster_id].len() == 3 { let cluster_nodes = nodes.remove(&cluster_id).unwrap();` -- clusters are dispatched at exactly three nodes; src/v2/analyze.rs:310-313 returns `NoPrimary` before the topology guard when there are zero primaries, so the 3-replica case cannot reach it either. Guard at src/v2/analyze.rs:323-328.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (none):** No fix needed for the ADR claim. Adjacent observation, out of ADR scope and reported not fixed: a cluster with more than three nodes in the portal is silently truncated -- the first three to arrive form the cluster and the remainder are logged as 'incomplete' at cluster.rs:49-50 and dropped.

### [info | implemented] ADR lines 39-47, 449-465 in code
- **Claim:** Digest claim (matrix-consistency, rated critical): when both `replicas_following_stale` and `replicas_following_highest` are non-empty, the `Both` branch wins and the lower-TL primary with real quorum is demoted.
- **Justification:** REFUTED as reachable. Two non-empty follower sets require at least two replicas alongside at least two primaries -- four nodes -- which the cluster builder cannot produce. The branch ordering is a latent design question, not a reachable defect, and the ADR's 3-node scope (line 241: 'exactly two candidate primaries and one replica') is what makes it moot.
- **Code:** `src/v2/analyze/split_brain.rs:449 `if !replicas_following_stale.is_empty() && replicas_following_highest.is_empty() {` then :465 `} else if !replicas_following_highest.is_empty() {`; bounded by src/v2/cluster.rs:31 `if nodes[&cluster_id].len() == 3 {`.`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (none):** None. If the 3-node cap is ever lifted, this branch precedence becomes a genuine safety question and should be revisited alongside the ADR's 1+2 assumption.

### [info | implemented] ADR lines 156
- **Claim:** Digest claim (rated high/critical by several agents): the writer falls back to the literal string "sanity gate failed", giving the operator no gate name.
- **Justification:** REFUTED as reachable today. `Confidence::Refuse` can only arise from one of three findings; two of them render a specific gate name and the third has no production emitter anywhere in the repo. The fallback is dead code until a `DivergentReplicaWal` emitter lands, at which point it becomes live -- worth a guard, not a finding.
- **Code:** `src/v2/analyze/split_brain.rs:396-399 `SystemIdentifierMismatch { .. } | SynchronousCommitWeakened { .. } | DivergentReplicaWal { .. } => Confidence::Refuse,`; :384-389 `.map(|f| determine_confidence_level(f, true_primary)).min().unwrap_or(Confidence::BestEffort)`; src/v2/writer/build.rs:671-675 renders the first two; `grep -rn DivergentReplicaWal src/` -> :102, :399, :782 (test), build.rs:682 only.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (none):** None required. See the `DivergentReplicaWal` entry for the optional hardening.

### [info | implemented] ADR lines 58, 254
- **Claim:** Digest claim: "verdicts produced during such windows are capped at `BestEffort`" diverges because `BestEffort` is the `unwrap_or` default rather than a cap.
- **Justification:** REFUTED. With `Verified` deliberately absent from v1 (ADR:121) and no finding mapping above `BestEffort`, `BestEffort` IS the top of the lattice -- every verdict is capped there, which is exactly what ADR:263 states as a consequence. Default and cap coincide by construction; calling that a divergence is wordplay. The residual 're-converge once wal_sender_timeout elapses' clause is an untested claim about repeat scans, not a code divergence.
- **Code:** `src/v2/analyze/split_brain.rs:73-77 `pub enum Confidence { Refuse, Conflicting, BestEffort, }` with derived Ord at :71; :384-389 `.min().unwrap_or(Confidence::BestEffort)`; :414 `BidirectionalFlushingConfirmed(_) => Confidence::BestEffort,` is the highest any finding maps to. Ordering pinned by the test at :821-836.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (none):** 

### [low | implemented] ADR lines 129
- **Claim:** §4: "Order: sanity-gate failures, then contradictions, then corroboration. Cap at ~5 surfaced items."
- **Justification:** Largely REFUTED. (a) The cap: only 0 or 1 finding ever reaches `short`, and details_json carries all of them by design (ADR:179), so 'surfaced items' is trivially within ~5; the theoretical maximum in a 3-node split brain is about 7 findings total. (b) The ordering: within one replica the gate emits at most one finding across all primaries (a `wal_receiver` names exactly one sender), so the corroboration-before-contradiction interleaving the digest constructed needs >=2 replicas and is unreachable. The one real ordering deviation -- quorum findings appended after corroboration -- has no behavioural effect, because `format_refuse`'s `find_map` only ever selects sanity-gate findings and those are pushed first.
- **Code:** `src/v2/analyze/split_brain.rs:152-180 (outer order: sysid :155, SynchronousCommitWeakened :170, following_findings :179, quorum :180); `format_refuse`'s selector at src/v2/writer/build.rs:669-684; details serialised whole at :660.`
- **Reachable in code:** True | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (adr-text):** Optional: drop the 'Cap at ~5 surfaced items' sentence from ADR:129, since §4's own findings-concatenation rules already bound `short` to a single item and explicitly send everything else to details_json.

### [low | diverges] ADR lines 202
- **Claim:** §5: "The 4-arg form `pg_read_file(...)` returns NULL if the file doesn't exist (e.g. during a fresh promotion window between TL bump and history-file write)" and "The CTE evaluates `pg_control_checkpoint()` once; calling it twice ... risked a ... race during a TL bump".
- **Justification:** The defensive choices (missing_ok, single CTE evaluation) are correct and I am not disputing them; the stated rationale is inverted. PG writes the history file BEFORE installing the new timeline, with a source comment about minimising exactly the opposite window. And because `pg_control_checkpoint()` reports the last completed checkpoint (see the checkpoint-lag finding), it cannot 'bump' mid-query in the way described.
- **Code:** `REL_15_14 src/backend/access/transam/xlog.c:5425-5436 `Write the timeline history file, and have it archived. After this point ... the timeline will appear as "taken" ... To minimize the window for that, try to do as little as possible between here and writing the end-of-recovery record.` followed at :5442 by `XLogCtl->InsertTimeLineID = newTLI;`. Shipped SQL: src/v2/scan/health_check_primary.rs:146-159.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None -- settled from PG source.
- **Fix (adr-text):** Replace the rationale at ADR:202 with the real ones: `missing_ok` covers a node whose `pg_wal` lacks the history file (restored from basebackup, cleaned pg_wal, or a privilege-independent absence), and the single-CTE form is chosen for readability/one evaluation rather than to close a promotion race that does not exist in this direction.

### [low | diverges] ADR lines 157, 168-174
- **Claim:** §4 examples and the variant->action table use short node identifiers, e.g. `SplitBrain: db001 has quorum (lower TL=N), fence db002 (TL=N+1, quorum-blocked)`.
- **Justification:** Real but cosmetic-plus: the writer interpolates raw FQDNs into the reason while normalising the very same strings for the PRIMARY column two lines earlier, so one row shows `db002@sto2` and `dev-pg-app001-db002.sto2.example.com` for the same node, and the REASON column drives the whole table's width.
- **Code:** `src/v2/writer/build.rs:698-724 interpolate `info.true_primary` / `stale` raw, versus :202 `display: extract_db_number(&info.true_primary),` and :256; column width from src/v2/writer/terminal.rs:48 `max_reason = max_reason.max(view.reason.short.len());`.`
- **Reachable in code:** True | **On fleet:** evidenced-yes | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (writer):** Wrap `info.true_primary` and `stale` in `extract_db_number` inside `format_resolution` and `format_refuse`, matching build.rs:202/256. ADR:259 puts visual polish out of scope but ADR:157's examples are normative about identifier form, so this is contract, not styling.
- **Alternatives rejected:** Changing the ADR examples to FQDNs: worse for the stated 3-AM-triage goal, and inconsistent with every other column in the table.

### [low | stale] ADR lines 131, 157 (rename mandate)
- **Claim:** Documentation drift outside the ADR: README.md and SPEC.md still carry the pre-rename mechanism language.
- **Justification:** Confirmed. README.md:231 prints, as sample tool output, the exact string ADR:157 lists as 'Not acceptable'. SPEC.md:401 still uses the old variant name. Code is fully renamed. Doc-only, but README is the first thing a new operator reads and it teaches the paradox phrasing the rename exists to kill.
- **Code:** `README.md:231 `... SplitBrain: replica overrides timeline (7 < 8)`; SPEC.md:401 `4. **ReplicaOverridesTimeline**: Replicas follow lower-timeline primary`; actual output src/v2/writer/build.rs:709 `"SplitBrain: {} has quorum (lower TL={}), fence {} (TL={}, quorum-blocked)"`; variant src/v2/analyze/split_brain.rs:35 `LowerTimelineHasQuorum {`.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None.
- **Fix (plan-text):** Update README.md:231 to the real `fence`-phrased output and README.md:290 / SPEC.md:401 to `LowerTimelineHasQuorum`. Note that the README sample also shows `db003@sto3->db001@sto1` in the REPLICAS cell, which only occurs for the follower-carrying variants -- worth checking the sample row is still representative once finding 1 is fixed.
- **Alternatives rejected:** Also renaming the stale doc comment at src/v2/analyze/split_brain.rs:33 (`/// Replica evidence overrides timeline ...`): correct to do, but it is a code comment on the renamed variant and belongs with the code fix, not the doc sweep.

### [low | diverges] ADR lines 116, 140
- **Claim:** §4: `ReplicaInCatchup { replica, primary }` -- "informational, gate passed"; §3: `Conflicting` -- "signals partially contradict".
- **Justification:** An explicitly informational, gate-PASSED finding is mapped to `Conflicting`, which §3 defines as contradiction. Real but invisible: `Conflicting` changes nothing an operator sees (see the confidence-visibility finding), so the effect is confined to the `confidence` field in details_json.
- **Code:** `src/v2/analyze/split_brain.rs:411-413 `| SplitBrainFinding::ReplicaInCatchup(_) => Confidence::Conflicting,`; emission at :328-333, inside the `Some(row)` arm immediately after `BidirectionalFlushingConfirmed`, i.e. the gate has passed.`
- **Reachable in code:** True | **On fleet:** unknown | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** None for the mapping. Whether catchup occurs on this fleet: `SELECT application_name, state FROM pg_stat_replication WHERE state <> 'streaming';` across primaries.
- **Fix (resolver):** Map `ReplicaInCatchup(_)` to `Confidence::BestEffort` alongside `BidirectionalFlushingConfirmed`, matching ADR:140's 'informational, gate passed'. One-line change in `determine_confidence_level`; the existing severity_rank test at split_brain.rs:746-756 will need its expectation updated.
- **Alternatives rejected:** Changing ADR:140 to call catchup a contradiction: contradicts ADR:69's insistence that catchup 'must not be rejected', so the code is the side that should move.

### [info | unverifiable] ADR lines 204 (the hex-filename claim leans on this fixture)
- **Claim:** Ground-truth / fixture integrity: tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json as the reference capture.
- **Justification:** Reported so nobody treats the fixture as uniformly faithful. The fixture is internally inconsistent in one field: db001 reports `timeline_id: 11` and a `timeline_history` with exactly ten entries (previous_tli 1..10), both consistent with TL 11 and file `0000000B.history` -- but `archiver.last_archived_wal` is `00000011000004850000003F`, whose TLI field is hex 0x11 = 17. One of those was hand-edited. This does NOT weaken the naming evidence (finding 1), which rests on three mutually corroborating fields plus two independent pieces of source code; it does mean 'the fixture says X' is not by itself sufficient for a claim resting on a single field.
- **Code:** `tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json: `"timeline_id": 11`, `"timeline_history": "1\t0/3000000...10\t3/E000000..."`, `"last_archived_wal": "00000011000004850000003F"`. Consumed by src/v2.rs:29-30 `include_str!("../tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json")`.`
- **Reachable in code:** False | **On fleet:** no | **Wrong verdict:** False | **Destructive text:** False
- **Evidence needed:** Re-capture one cluster with the anonymiser and diff field-by-field against the portal, to establish which fields it rewrites and which it passes through. That would settle both this and the fleet-wide half of finding 1.
- **Fix (tests):** Either correct `last_archived_wal` to `0000000B...` so the fixture is self-consistent, or add a header comment recording which fields are synthetic. Not urgent, but a reviewer using this file as ground truth should not have to rediscover the inconsistency.


---

# Appendix B -- case-matrix traces (raw)


## Row C-c (docs/adr/002-split-brain-resolution-refinement.md:43) -- db003 wal_receiver points at db001, status streaming, rece

- **Determinate under sections 1-4:** False
- **Matches ADR:** False | **Severity:** critical | **Destructive text:** True

**ADR expects (resolution):**

`SplitBrainResolution::LowerTimelineHasQuorum { true_primary_timeline: N, stale_timeline: N+1, replicas_following_true: [db003] }`, true_primary = db001, stale_primaries = [db002]. Confidence must NOT be Refuse ("the verdict is confident", ADR:43; "confident ... no Refuse", ADR:242). Short string per ADR:154 must name the action: "SplitBrain: db001 has quorum (lower TL=N), fence db002 (TL=N+1, quorum-blocked)".


**ADR expects (findings):**

`BidirectionalFlushingConfirmed(db001, db003)` + `PrimaryQuorumUnsatisfied(db002)` (inherited from C-b, ADR:42; ADR:145-151 mandates the latter whenever LowerTimelineHasQuorum fires) + `DivergentReplicaWal(db003, ...)` as informational only. ADR:53 additionally requires PrimaryQuorumUnsatisfied(db002) to be operator-visible.


**Determinacy:**

Split verdict. The C-b-inherited half IS determinate: SS1 (gate) + SS4 (PrimaryQuorumUnsatisfied derivation rule) + the SS4 variant->template table pin down LowerTimelineHasQuorum(db001) + BidirectionalFlushingConfirmed(db001,db003) + PrimaryQuorumUnsatisfied(db002,1,0) with no freedom left. The C-c-specific delta is NOT determinate and is self-contradictory:

(a) ADR:43 says "`DivergentReplicaWal(db003, ...)` is **informational, not Refuse**". ADR:159 (SS4 item 4, itself carrying the same 2026-06-07 revision stamp) says "**`DivergentReplicaWal`, when present, MUST surface inline in the SplitBrain short string** and MUST set `Confidence::Refuse`." Those are opposite outcomes for the same input row, with no stated exception mechanism in SS4.
(b) SS7 (ADR:242) sides with the row -- "When db003's allegiance is **observable** (e.g. C-c ...) ... no Refuse" -- so the discriminator (allegiance observable vs. unprovable) exists, but only in SS7 prose. It is not a rule SS1-SS4 expose, and structurally cannot be: determine_confidence_level (src/v2/analyze/split_brain.rs:395-415) is keyed on the finding *variant* alone and is handed only `finding` and `true_primary`. It has no access to whether db003's allegiance was observable, so a single variant->Confidence map can never express "informational in C-c, Refuse in C-g".
(c) The row's premise "(`sync_commit=on`)" is not the fleet value (fixture: remote_apply). The safety inference survives (remote_apply is stronger than on, and SS2's valid set includes it), so this is doc drift, not a hole -- but it does mean SS5's annotation of `pg_last_wal_receive_lsn()` as "the ack-relevant one under `synchronous_commit=on`" names the wrong field for this fleet: under remote_apply the ack-relevant position is `pg_last_wal_replay_lsn()`. Capture-only today, so no behavioural effect.
(d) Bonus self-contradiction found while checking determinacy: ADR:11 says "a conservative `Refuse`-only floor is shippable today (SS7)"; ADR:245 says "...which is why no conservative \"Refuse-only floor\" is shipped in the interim." Directly opposed, same revision.

Classification: the C-b-inherited verdict is `implemented` under synthetic names and `diverges` under fleet names; the DivergentReplicaWal-informational claim is `self-contradictory` (ADR:43 vs ADR:159) and, as wired, `diverges`.


**Code, synthetic names -- resolution:**

SplitBrainInfo { true_primary: "db001", stale_primaries: ["db002"], resolution: LowerTimelineHasQuorum { true_primary_timeline: 11, stale_timeline: 12, replicas_following_true: ["db003"] }, confidence: BestEffort, findings: [BidirectionalFlushingConfirmed(ReplicationLink{primary:"db001", replica:"db003"}), PrimaryQuorumUnsatisfied{primary:"db002", required:1, observed:0}] }. This is exactly the shape asserted by the existing test src/v2/analyze/split_brain.rs:1041 replica_following_lower_timeline_overrides (which omits SSN and therefore omits the quorum finding) plus split_brain.rs:1502 quorum_unsatisfied_on_stale_primary_does_not_refuse.


**Code, synthetic names -- findings:**

In order: [1] BidirectionalFlushingConfirmed(db001, db003) -- pushed at split_brain.rs:325 during build_replica_following_map; [2] PrimaryQuorumUnsatisfied{primary:"db002", required:1, observed:0} -- pushed at split_brain.rs:669 by emit_quorum_findings. Order is fixed by split_brain.rs:170-171 (following-map findings extended before quorum findings) and split_brain.rs:373 (both appended after the resolution's own empty findings vec). NOTE: this order is "corroboration, then contradiction", the reverse of ADR:135 "Order: sanity-gate failures, then contradictions, then corroboration" -- cosmetic here (no sanity-gate finding fires in C-c) but the ordering rule is not implemented anywhere. DivergentReplicaWal is absent: nothing in src/ constructs it (only src/v2/analyze/split_brain.rs:782 in a test rstest case, plus the two match arms at split_brain.rs:399 and src/v2/writer/build.rs:682).


**Code, synthetic names -- confidence:**

Confidence::BestEffort. min() over {BestEffort (BidirectionalFlushingConfirmed, split_brain.rs:414), BestEffort (PrimaryQuorumUnsatisfied on a non-elected primary, split_brain.rs:400-406)} with the derive-order Ord Refuse < Conflicting < BestEffort (split_brain.rs:69-75). Matches ADR:43's "the verdict is confident". If a future emitter added DivergentReplicaWal here, split_brain.rs:397-399 `| SplitBrainFinding::DivergentReplicaWal { .. } => Confidence::Refuse` would drag min() to Refuse, contradicting ADR:43.


**Code, synthetic names -- SHORT STRING:**

"SplitBrain: db001 has quorum (lower TL=11), fence db002 (TL=12, quorum-blocked)"

Emitted by src/v2/writer/build.rs:708-711. Matches ADR:154's mandated form. Caveat: the "quorum-blocked" parenthetical is hardcoded per variant and format_resolution never inspects info.findings (build.rs:692-727), so the identical string is emitted when PrimaryQuorumUnsatisfied is ABSENT -- e.g. if db002's synchronous_standby_names is empty or unparseable, split_brain.rs:650 `let Some(Quorum{..}) = parse(...) else { continue }` emits nothing and the operator still reads "quorum-blocked" as an assertion the tool never derived. Medium on its own.

Counterfactual for the ADR:159 reading (DivergentReplicaWal present -> Refuse): build.rs:655 routes to format_refuse, whose match at build.rs:682 maps DivergentReplicaWal to None, so the fallback at build.rs:684 fires and the entire operator-facing string becomes "REFUSE/SplitBrain: sanity gate failed" -- no divergence evidence (violates ADR:159 "MUST surface inline"), no LSN, no "keep db001", and per ADR:152 the resolution text is deliberately suppressed. The wiring would get this row wrong in both directions at once.


**Code, FLEET names -- resolution:**

SplitBrainInfo { true_primary: "dev-pg-app001-db002.sto2.example.com", stale_primaries: ["dev-pg-app001-db001.sto1.example.com"], resolution: HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }, confidence: Conflicting, findings: [PrimaryDoesNotSeeReplica(ReplicationLink{primary:"dev-pg-app001-db001.sto1.example.com", replica:"dev-pg-app001-db003.sto3.example.com"}), PrimaryQuorumUnsatisfied{primary:"dev-pg-app001-db001.sto1.example.com", required:1, observed:0}, PrimaryQuorumUnsatisfied{primary:"dev-pg-app001-db002.sto2.example.com", required:1, observed:0}] }.

The verdict INVERTS: the ADR says db001 is the true primary; the code names db002. LowerTimelineHasQuorum becomes unreachable on this fleet, because split_brain.rs:307 can never match an application-name-form string against an FQDN node_name, so `following` is always empty and resolve_with_different_timelines always falls through to the HigherTimeline else-branch at split_brain.rs:484. C-a, C-b, C-c and C-e all collapse onto the same HigherTimeline output. Note this is the exact mis-pick the ADR reserves for C-g (ADR:47, "Resolver mis-picks HigherTimeline -> db002 ... destroys acknowledged transactions") -- reached here in the benign, fully-observable row, where the safety net (DivergentReplicaWal -> Refuse) is deferred and cannot fire.


**Code, FLEET names -- SHORT STRING:**

"SplitBrain: dev-pg-app001-db002.sto2.example.com has quorum (TL=12), demote dev-pg-app001-db001.sto1.example.com (TL=11, no live replicas)"

Emitted by src/v2/writer/build.rs:715-718. Three separate problems in one line: (1) it instructs demotion of db001, the node that in C-c holds client-acknowledged writes that db003 flushed past the fork and that db002 does not have; (2) the parenthetical "no live replicas" is factually false -- db003 IS live-streaming db001, the replica-side gate at split_brain.rs:281-287 passed; the claim comes from the hardcoded variant template, not from evidence; (3) confidence is Conflicting, not Refuse, so build.rs:655 does not suppress the text and nothing warns the operator off. Also note format_resolution interpolates raw node_name (build.rs:702/710/717/721) while the rest of the report renders via extract_db_number (build.rs:122/195/202/250), so the short string alone carries un-shortened FQDNs -- cosmetic, but it is a second place the naming forms were not reconciled.


**Mismatch:**

Two independent mismatches.

HEADLINE (fleet naming, critical). ADR C-c requires true_primary = db001 and "fence db002". With the real naming captured in tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json the code produces true_primary = db002 and "demote db001". Root cause is a single line: src/v2/analyze/split_brain.rs:307 `&& conn.application_name == replica.node_name` -- a raw equality between "dev_pg_app001_db003" and "dev-pg-app001-db003.sto3.example.com". The primary-side corroboration of ADR SS1 can therefore never pass on this fleet, PrimaryDoesNotSeeReplica fires instead, `following` stays empty, and resolve_with_different_timelines falls through to HigherTimeline. src/v2/writer/build.rs already contains both bridging helpers (normalize_application_name:422, extract_db_number:408); the resolver does not call them. The bug is invisible to the suite because every split_brain.rs test uses node_name == application_name (e.g. primary_with_followers(1,"db001",...,&["db003"]) at :712 paired with replica named "db003"). The same defect silently poisons the SS4 quorum derivation: emit_quorum_findings (:663-667) intersects application-name-form SSN members with FQDN-form `gated`, so observed == 0 unconditionally and PrimaryQuorumUnsatisfied fires for every primary on every real cluster, including the true one -- which is what drags confidence to Conflicting.

What is verified vs. not: the naming mismatch is FLEET-CONFIRMED from captured data (fixture node_name vs. pg_stat_replication.application_name vs. SSN members, all three forms present in one file). What is NOT captured is a real split-brain occurrence, so "this exact string will be printed during an incident" is an inference from the code path, not an observation. The settling capture is a scan taken while two primaries are up; the settling query on any live primary today is `SELECT application_name FROM pg_stat_replication;` compared against that node's `node_name` in the inventory -- the fixture already answers it in the negative.

SECONDARY (the C-c-specific claim, latent). ADR:43's "DivergentReplicaWal is informational, not Refuse" is not implemented and is contradicted by the ADR's own ADR:159 "MUST set `Confidence::Refuse`". split_brain.rs:399 maps the variant unconditionally to Refuse. Nothing emits it (confirmed: only the test case at split_brain.rs:782 constructs one), so this is not reachable today -- but the question the assignment poses answers YES: the current wiring would get C-c wrong the moment SS7's detection lands, and would do so twice over, once by flipping a confident verdict to Refuse and once by rendering it as the content-free "REFUSE/SplitBrain: sanity gate failed" (build.rs:682 -> :684). The deeper structural point: determine_confidence_level is keyed on finding variant alone and has no observability input, so it cannot express the C-c/C-g distinction that SS7 (ADR:242) makes load-bearing. This is not "deferred-correct" -- the deferral is stated inconsistently across ADR:11, ADR:43, ADR:159 and ADR:245.

TERTIARY (low, doc drift). C-c's premise "(`sync_commit=on`)" and docs/concepts/split-brain.md:7 "`synchronous_commit = on`" both mis-state the fleet, which runs remote_apply. The safety inference survives (remote_apply is strictly stronger and is in SS2's valid set, and WEAKENED_SYNCHRONOUS_COMMIT at split_brain.rs:13 correctly excludes it), but ADR SS5's label of `pg_last_wal_receive_lsn()` as "the ack-relevant one under `synchronous_commit=on`" names the wrong field for remote_apply, where the ack condition is apply, i.e. `pg_last_wal_replay_lsn()`. Both are captured, detection is deferred, so no behavioural effect -- but if the deferred trigger is later built on receive_lsn it will over-fire (received-but-not-applied WAL read as acked), which is over-caution, not data loss.


**Code path:**

Common entry: src/v2/analyze.rs:318 `let split_brain_info = resolve_split_brain(&primaries, &replicas);` -- no name normalization anywhere on this path.

SYNTHETIC NAMES (node_name "db001"/"db002"/"db003", pg_stat_replication.application_name "db003", SSN "ANY 1 (db002, db003)" / "ANY 1 (db001, db003)", builder defaults synchronous_commit="on", wal_sender_timeout absent):
1. split_brain.rs:135 extract_timeline_info -> highest=12 (db002), lower=[(db001,11)].
2. split_brain.rs:137-138 reference_sysid -> Some("6968745321024393216") (2 of 2 primaries agree); mismatched_sysid_nodes -> [] (db003 shares the default). findings = [] (:146).
3. split_brain.rs:153-167 synchronous_commit loop: "on" not in WEAKENED_SYNCHRONOUS_COMMIT (:13) -> no finding.
4. split_brain.rs:169 build_replica_following_map:
   - :266 `let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;` -> 60_000/2+30_000 = 60_000 ms (builder sets no wal_sender_timeout; :605-609 default).
   - primary db002 first (highest-TL is chained first, :255-259): :281 `wr.sender_host == primary.ip_address.to_string()` -> "127.1.12.151" != "127.2.12.151" -> replica_passes=false; :291 same compare false -> NO ReplicaWalReceiverStale; continue (:300).
   - primary db001: :281-287 replica side passes (host match, port 5432, status "streaming", and `(r_health.current_time - t).num_milliseconds()` is a large NEGATIVE because the builder sets current_time=UNIX_EPOCH and last_msg_receipt_time=Utc::now(); negative <= 60_000 so the freshness check passes).
   - :302-316 primary-side row search: :307 `&& conn.application_name == replica.node_name` -> "db003" == "db003" -> match; state Streaming; reply_time likewise "fresh" via the same negative-delta path.
   - :325-331 push BidirectionalFlushingConfirmed(db001, db003); following["db001"] = ["db003"]; row.state != Catchup so no ReplicaInCatchup.
5. split_brain.rs:171 emit_quorum_findings (:637-...): db001 -> parse("ANY 1 (db002, db003)") = Quorum{count:1, members:[db002,db003]}, gated=["db003"], observed=1, 1<1 false -> nothing. db002 -> members [db001,db003], gated=[], observed=0 < 1 -> PrimaryQuorumUnsatisfied{db002,1,0}.
6. split_brain.rs:173 -> :357-360 `primaries_with_highest_timeline.len()==1 && !lower.is_empty()` -> resolve_with_different_timelines (:421).
   - :432 replicas_following_highest = following.get("db002") -> None -> [].
   - :439-447 db001 has followers -> replicas_following_stale=["db003"], stale_with_followers=db001.
   - :449 both conditions hold -> :455-464 LowerTimelineHasQuorum branch.
7. :373 findings appended; :375-381 confidence = min(BestEffort from :414 BidirectionalFlushingConfirmed, BestEffort from :400-406 PrimaryQuorumUnsatisfied where primary "db002" != true_primary "db001") = BestEffort.
8. src/v2/writer/build.rs:654-659 split_brain_reason -> not Refuse -> format_resolution (:692) -> LowerTimelineHasQuorum arm :704-711.

FLEET NAMES (node_name "dev-pg-app001-dbNNN.stoN.example.com", application_name "dev_pg_app001_db003", SSN "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )", synchronous_commit=remote_apply, wal_sender_timeout=300000 -- all from tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json):
1-3. Same as above except :266 threshold_ms = 300000/2 + 30_000 = 180_000 ms, and "remote_apply" is correctly not in WEAKENED_SYNCHRONOUS_COMMIT.
4. build_replica_following_map, primary db001: replica side PASSES identically (:281-287, sender_host 127.1.12.151 == db001.ip_address).
   Then split_brain.rs:307 `&& conn.application_name == replica.node_name` compares "dev_pg_app001_db003" == "dev-pg-app001-db003.sto3.example.com" -> **FALSE**. No other row matches -> primary_row = None.
   -> :335-337 `None => findings.push(SplitBrainFinding::PrimaryDoesNotSeeReplica(ReplicationLink::new(&primary.node_name, &replica.node_name)))`. db003 is NOT inserted into `following`. (build.rs:422 normalize_application_name and build.rs:408 extract_db_number exist and would bridge the two forms; the resolver never calls them.)
5. emit_quorum_findings: :650 parse succeeds for both primaries; members are in application-name form, `gated` is empty for both, so :663-667 `members.iter().filter(|m| gated.iter().any(|g| g == *m))` -> observed = 0 for BOTH. Emits PrimaryQuorumUnsatisfied{db001-FQDN,1,0} then PrimaryQuorumUnsatisfied{db002-FQDN,1,0} (primaries slice order).
6. resolve_with_different_timelines: :432 replicas_following_highest = [] ; :439-447 no lower-TL primary has followers -> replicas_following_stale = []. :449 false, :465 false -> falls through to the `else` at :484-501 -> `SplitBrainResolution::HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }`, true_primary = db002 FQDN, stale_primaries = [db001 FQDN].
7. Confidence = min(Conflicting from :410-412 PrimaryDoesNotSeeReplica, BestEffort from PQU(db001) != true_primary, Conflicting from :406 PQU(db002) == true_primary) = **Conflicting** -- NOT Refuse, so nothing suppresses the resolution text.
8. build.rs:655 `matches!(info.confidence, Confidence::Refuse)` false -> format_resolution -> HigherTimeline arm :712-718.


## Row C-a (docs/adr/002-split-brain-resolution-refinement.md:41): db003's wal_receiver names db002, status streaming/catchup, 

- **Determinate under sections 1-4:** True
- **Matches ADR:** False | **Severity:** high | **Destructive text:** False

**ADR expects (resolution):**

true_primary = db002 (TL=N+1), stale_primaries = [db001] (TL=N), resolution = SplitBrainResolution::Both { true_primary_timeline: N+1, stale_timeline: N, replicas_following_true: [db003] }. Confidence is NOT stated by the row; §3's prose ("BestEffort // single-pass scan; gate passed; verdict is internally consistent") implies BestEffort but the ADR never specifies how Confidence combines across a findings vector -- the min() rule at src/v2/analyze/split_brain.rs:377-380 is code-only. Short string per §4's variant table: "SplitBrain: {true} has quorum (TL={hi}), demote {stale} (TL={lo}, quorum unsatisfied)".


**ADR expects (findings):**

Exactly two, in this order per §4 ("sanity-gate failures, then contradictions, then corroboration"): BidirectionalFlushingConfirmed(db002, db003) and PrimaryQuorumUnsatisfied(db001, required=1, observed=0). The row explicitly does NOT list PrimaryQuorumUnsatisfied(db002) (db003 gate-passes for db002, so observed=1 >= 1) and does NOT list PrimaryDoesNotSeeReplica. §4's ordering rule would actually put PrimaryQuorumUnsatisfied (a contradiction) BEFORE BidirectionalFlushingConfirmed (corroboration), which is the reverse of the row's own left-to-right listing -- a minor self-inconsistency in the ADR.


**Determinacy:**

The row's three asserted columns (true primary, resolution variant, findings) ARE derivable from §§1-4 -- but only because §1 is applied on top of a stated cluster assumption that the captured fleet data falsifies.

Derivation chain:
- §1 replica side: sender_host==db002.ip, port==5432, status in {streaming,catchup}, last_msg_receipt_time fresh -> passes. Determinate.
- §1 primary side: "The primary's pg_stat_replication has a row whose application_name equals the replica's node name." Whether this passes is determined only by the Cluster-assumptions bullet at docs/adr/002-split-brain-resolution-refinement.md:27, "repmgr-set `application_name` equals the node name." Under that premise the primary side passes -> db003 counts as following db002 -> Both + BidirectionalFlushingConfirmed. So the row is determinate GIVEN line 27.
- §4 quorum derivation: needs synchronous_standby_names present and parseable. Line 26 fixes it as 'ANY 1 (A, B)' on both primaries. db001: gated={} -> observed 0 < 1 -> emit. db002: gated={db003} -> observed 1 -> no emit. Determinate.

Two parts are NOT pinned down:
1. Confidence. §3 defines the three states but never gives a combination rule over findings, and the C-a row gives no Confidence value. That sub-part is unverifiable, not implemented/diverges.
2. Line 27 is not a rule, it is a factual claim about the fleet, and it is FALSE. tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json has node_name "dev-pg-app001-db002.sto2.example.com" and pg_stat_replication.application_name "dev_pg_app001_db002" (also visible inside the replica's own primary_conninfo: "application_name=dev_pg_app001_db002"). Different separator, different suffix. Under the real naming, §1's own primary-side rule REJECTS the match and the ADR's rules derive PrimaryDoesNotSeeReplica + not-following -- i.e. the ADR text contradicts the ADR row. Classification for the row as written: diverges (and the premise at line 27 is itself stale/wrong relative to captured data).


**Code, synthetic names -- resolution:**

TWO sub-variants, because the ADR row depends on synchronous_standby_names being set and the test builder does not set it.

(1a) EXACTLY as the existing unit test does it (PrimaryHealthBuilder::new() defaults, src/v2.rs:59-61 sets only archive_mode=on and synchronous_commit=on -- NO synchronous_standby_names key):
- split_brain.rs:307 "db003" == "db003" -> primary_row = Some -> line 322 following{db002:[db003]}, line 327 BidirectionalFlushingConfirmed(db002,db003); row.state is Streaming so no ReplicaInCatchup.
- emit_quorum_findings: h.configuration.get("synchronous_standby_names") is absent -> map_or("") -> sync_standby_names.rs:26-28 parse("") returns None -> split_brain.rs:650 `else { continue }` for BOTH primaries -> ZERO quorum findings.
- resolve_with_different_timelines: replicas_following_highest=[db003] non-empty -> line 470-483 Both.
RESULT: true_primary="db002", stale_primaries=["db001"], resolution=Both{true_primary_timeline:12, stale_timeline:11, replicas_following_true:["db003"]}, findings=[BidirectionalFlushingConfirmed(db002,db003)], confidence=BestEffort.
This is byte-for-byte the assertion in split_brain.rs:1013-1038 `timeline_and_replica_evidence_agree`. PrimaryQuorumUnsatisfied(db001) is ABSENT -> the ADR row is violated under builder defaults.

(1b) With synchronous_standby_names = "ANY 1 (db002, db003)" set on BOTH primaries (the ADR's stated cluster config, line 26):
- db002: members={db002,db003} ∩ gated{db003} -> observed=1, not < 1 -> no finding.
- db001: gated={} -> observed=0 < 1 -> PrimaryQuorumUnsatisfied{primary:"db001", required:1, observed:0}.
RESULT: resolution=Both{12,11,["db003"]}, findings=[BidirectionalFlushingConfirmed(db002,db003), PrimaryQuorumUnsatisfied{db001,1,0}], confidence = min(BestEffort, BestEffort) = BestEffort (split_brain.rs:401-407: PQU on a primary != true_primary maps to BestEffort).
This matches the ADR row's content, modulo finding ORDER (code emits corroboration before the contradiction; §4 mandates the reverse).


**Code, synthetic names -- findings:**

1a (builder defaults, = existing test): [BidirectionalFlushingConfirmed(ReplicationLink{primary:"db002", replica:"db003"})]. 1b (SSN set): [BidirectionalFlushingConfirmed(ReplicationLink{primary:"db002", replica:"db003"}), PrimaryQuorumUnsatisfied{primary:"db001", required:1, observed:0}].


**Code, synthetic names -- confidence:**

BestEffort in both sub-variants (1a: min over [BestEffort]; 1b: min over [BestEffort, BestEffort]). Fleet-name trace gives Conflicting instead -- see code_actual_resolution_fleet_names.


**Code, synthetic names -- SHORT STRING:**

SplitBrain: db002 has quorum (TL=12), demote db001 (TL=11, quorum unsatisfied)

Identical in 1a and 1b. writer/build.rs:700-703 hardcodes the "quorum unsatisfied" parenthetical on the Both arm and never reads info.findings, so the string asserts db001's quorum is unsatisfied even in 1a where the resolver never parsed an SSN and emitted no such finding. §4 item 3 ("PrimaryQuorumUnsatisfied MUST appear inline in the short string when present") is satisfied only by coincidence of wording; the converse -- it must NOT be asserted when absent -- is violated.


**Code, FLEET names -- resolution:**

Names: db001="dev-pg-app001-db001.sto1.example.com", db002="dev-pg-app001-db002.sto2.example.com", db003="dev-pg-app001-db003.sto3.example.com"; pg_stat_replication.application_name="dev_pg_app001_db003"; SSN="ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )" (verbatim from tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json).

- split_brain.rs:307 `conn.application_name == replica.node_name` -> "dev_pg_app001_db003" == "dev-pg-app001-db003.sto3.example.com" -> FALSE. No row matches -> primary_row = None.
- split_brain.rs:335 -> PrimaryDoesNotSeeReplica(ReplicationLink{primary:"dev-pg-app001-db002.sto2.example.com", replica:"dev-pg-app001-db003.sto3.example.com"}). db003 is NOT inserted into `following`. replicas_following is EMPTY.
- emit_quorum_findings: SSN parses fine (sync_standby_names.rs:24-57 handles the spaces inside the parens; members = ["dev_pg_app001_db002","dev_pg_app001_db003"]). gated is empty for BOTH primaries -> observed=0 < 1 for both -> PrimaryQuorumUnsatisfied on db001 AND on db002.
- resolve_with_different_timelines (split_brain.rs:422): replicas_following_highest=[] and replicas_following_stale=[] -> falls through both branches to the else at line 486-501 -> HigherTimeline.

RESULT:
  true_primary  = "dev-pg-app001-db002.sto2.example.com"
  stale_primaries = ["dev-pg-app001-db001.sto1.example.com"]
  resolution    = HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }   <-- ADR says Both
  findings (in emitted order) = [
      PrimaryDoesNotSeeReplica(db002 -> db003),                                       <-- not in the ADR row
      PrimaryQuorumUnsatisfied{ primary: db001, required: 1, observed: 0 },
      PrimaryQuorumUnsatisfied{ primary: db002, required: 1, observed: 0 },           <-- not in the ADR row; ADR says db002's quorum IS satisfied
  ]
  BidirectionalFlushingConfirmed is NEVER emitted.                                    <-- ADR row demands it
  confidence = min(Conflicting, BestEffort, Conflicting) = Conflicting                <-- ADR §3 implies BestEffort
    (PrimaryDoesNotSeeReplica -> Conflicting, split_brain.rs:409-411; PQU(db001) != true_primary -> BestEffort, :404;
     PQU(db002) == true_primary -> Conflicting, :402.)

Corollary, and the real headline: with fleet naming the `following` map can never be non-empty, so SplitBrainResolution::Both, ::LowerTimelineHasQuorum and ::ReplicaFollowing are all UNREACHABLE on real data. Every fleet split-brain degenerates to HigherTimeline or Indeterminate. §1's entire flushing-liveness gate -- the stated purpose of ADR-002 -- is a no-op on production input.


**Code, FLEET names -- SHORT STRING:**

SplitBrain: dev-pg-app001-db002.sto2.example.com has quorum (TL=12), demote dev-pg-app001-db001.sto1.example.com (TL=11, no live replicas)

(writer/build.rs:712-718, HigherTimeline arm. Note format_resolution uses info.true_primary and stale RAW -- unlike every other short-string producer in the file it does not call extract_db_number (build.rs:408), so the operator gets full FQDNs where ChainedReplica/DiskIoErrors/FilesystemErrors get "db002@sto2". Separate low-severity cosmetic divergence.)

The sentence is self-contradictory against its own details_json: it says db002 "has quorum" while findings carries PrimaryQuorumUnsatisfied{primary: db002, observed: 0}, and it says "no live replicas" while db003 is observably streaming from db002 with a 40 ms-old last_msg_receipt_time. Both untruths come from format_resolution hardcoding the parenthetical per variant instead of reading info.findings.


**Mismatch:**

Three separate mismatches, ranked.

(A) HEADLINE -- the gate's primary side never matches on real data. split_brain.rs:307 `&& conn.application_name == replica.node_name` compares the underscore/short form ("dev_pg_app001_db003") against the hyphen/FQDN form ("dev-pg-app001-db003.sto3.example.com"). They are never equal on this fleet. The codebase already contains the correct join: writer/build.rs:375-386 `find_replica_timeline` normalizes with normalize_application_name (build.rs:422) and then strips the FQDN off the node_name before comparing -- so the project demonstrably knows the two forms differ; the resolver just doesn't use it. Consequence for C-a: resolution is HigherTimeline, not Both; BidirectionalFlushingConfirmed is never emitted; a spurious PrimaryDoesNotSeeReplica and a spurious PrimaryQuorumUnsatisfied(db002) appear; confidence drops BestEffort -> Conflicting. The unit tests cannot see any of this because split_brain.rs:1015-1016 builds application_name "db003" and node_name "db003" (ground truth 3 confirmed).
  For row C-a the picked winner is still db002, which is the right node, so C-a itself is not destructive. But the identical code path at rows C-b and C-c -- where the true primary is the LOWER-TL db001 precisely because db003 is live-flushing for it -- also collapses to HigherTimeline, emitting "demote dev-pg-app001-db001... (TL=N, no live replicas)". Under synchronous_commit=remote_apply that is an instruction to demote the node that holds acknowledged writes. I rate the ROW high; I rate the ROOT CAUSE critical and recommend the parent treat it as such, because C-b/C-c are the destructive instances of the same single line.

(B) PrimaryQuorumUnsatisfied(db001) is absent under builder defaults. src/v2.rs:59-61 seeds only archive_mode and synchronous_commit; there is no synchronous_standby_names default, so parse("") -> None (sync_standby_names.rs:26-28) and split_brain.rs:650 `continue`s for every primary. The existing test timeline_and_replica_evidence_agree (split_brain.rs:1013-1038) therefore asserts findings == [BidirectionalFlushingConfirmed] and passes. This is a TEST-FIXTURE gap, not a code bug: the real fleet does carry SSN in `configuration`, so on real input the finding would be emitted (were the gate not broken by (A)). Classification: the code implements §4's derivation correctly; the C-a row is simply not covered by any test that supplies an SSN. Related latent test bug, out of scope but worth flagging: split_brain.rs:1469 sets `.with_synchronous_standby_names("ANY 1 (db002, db003")` -- unbalanced paren -> parse returns None at sync_standby_names.rs:51 -> the assertion at :1494-1498 "db001 has db003 following -> quorum satisfied; no finding for db001" is vacuous. db003 is in fact NOT following db001 there (db001 was built with no pg_stat_replication rows), so with a balanced paren that assertion would fail.

(C) Commit 12's short-string contract is hardcoded, not derived. writer/build.rs:692-727 format_resolution pins the quorum parenthetical to the resolution variant and never inspects info.findings, so: in synthetic-1a it prints "quorum unsatisfied" for db001 with no finding behind it; in the fleet trace it prints "has quorum" for a node the same struct flags as PrimaryQuorumUnsatisfied. §4's "Action-text ownership" paragraph explicitly says the writer "derives the short string from SplitBrainInfo.resolution AND SplitBrainInfo.findings" -- findings are not read at all. Also, format_refuse (build.rs:666-687) maps everything except SystemIdentifierMismatch/SynchronousCommitWeakened to None and falls back to the literal "sanity gate failed"; C-a never reaches Refuse so this doesn't bite here, but it means any future Refuse driven by a third finding type prints a contentless string.

Two secondary observations, info only:
- Freshness is never exercised positively by the builders. PrimaryHealthBuilder/ReplicaHealthBuilder set current_time = DateTime::<Utc>::UNIX_EPOCH (src/v2.rs:63, :252) while reply_time / last_msg_receipt_time = Utc::now() (src/v2.rs:198, :262). (current_time - t) is therefore a large NEGATIVE millisecond count, which trivially satisfies `<= threshold_ms` at split_brain.rs:285 and :311. The gate passes for the wrong reason in every test; there is no test proving a genuinely fresh-but-not-future timestamp passes.
- §5's "Pass scan-start DateTime<Utc> ... into resolve_split_brain() as a parameter. This is a small but load-bearing plumbing change" is not implemented (analyze.rs:318 passes two arguments). The intra-node substitute is arguably better (immune to scanner<->db clock skew, as the doc comment at split_brain.rs:240-243 argues), but the ADR text was not updated to say so -- stale spec text.

Query that would settle (A) on live hardware, since no captured split-brain run exists: on any primary, `SELECT application_name FROM pg_stat_replication;` compared against the `node_name` column the scanner loads for the same host. If the two strings are not byte-identical -- and the captured fixture says they are not -- split_brain.rs:307 can never match.


**Code path:**

Input state used (both traces): db001 Primary TL=11, ip 127.1.12.151, sysid 6968745321024393216, configuration{wal_sender_timeout:"300000", synchronous_commit:"remote_apply", synchronous_standby_names:"ANY 1 ( <A>, <B> )"}, replication=[] (C-a: "no replica acking it"). db002 Primary TL=12, ip 127.2.12.151, same sysid/config, replication=[{application_name:<db003 form>, state:Streaming, reply_time: current_time-40ms}]. db003 Replica TL=12, ip 127.3.12.151, same sysid, wal_receiver{sender_host:"127.2.12.151", sender_port:5432, status:"streaming", last_msg_receipt_time: current_time-40ms}.

Path (identical up to the primary-side gate):
- src/v2/analyze.rs:318 `resolve_split_brain(&primaries, &replicas)` -- primaries in cluster order [db001, db002]; NO scan-start timestamp parameter (ground truth 6 confirmed; §5's "Pass scan-start DateTime<Utc> ... into resolve_split_brain() as a parameter" is NOT implemented -- freshness is intra-node).
- split_brain.rs:184 extract_timeline_info -> sort desc -> highest=12/db002, lower=[(db001,11)].
- split_brain.rs:559 reference_sysid -> Some("6968745321024393216") (2 primaries agree).
- split_brain.rs:577 mismatched_sysid_nodes -> [] -> findings starts empty.
- split_brain.rs:160-174 synchronous_commit loop: "remote_apply" is not in WEAKENED_SYNCHRONOUS_COMMIT (line 13: ["local","off","remote_write",""]) -> no SynchronousCommitWeakened.
- split_brain.rs:245 build_replica_following_map; line 270 threshold_ms = 300000/2 + 30000 = 180000.
  - primary db002 (highest first, lines 256-260), replica db003:
    - line 281 `let replica_passes = wr.sender_host == primary.ip_address.to_string() && wr.sender_port == 5432 && matches!(wr.status.as_str(), "streaming" | "catchup") && wr.last_msg_receipt_time.is_some_and(...)` -> TRUE in both traces.
    - line 305-313 primary-side: `.find(|conn| !conn.application_name.is_empty() && conn.application_name == replica.node_name && ...)`. Line 307 is the RAW compare. THIS IS WHERE THE TWO TRACES DIVERGE.
  - primary db001, replica db003: line 281 sender_host "127.2.12.151" != "127.1.12.151" -> replica_passes=false; line 293 guard `if wr.sender_host == primary.ip_address.to_string()` also false -> nothing emitted (correct, no spurious ReplicaWalReceiverStale).
- split_brain.rs:180 findings.extend(emit_quorum_findings(primaries, &replicas_following)); line 650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue };`, line 660 observed = |members ∩ gated|, line 665 `if observed < count`.
- split_brain.rs:352 determine_true_primary -> line 366 (1 highest, >=1 lower) -> line 422 resolve_with_different_timelines.
- split_brain.rs:381 `split_brain_info.findings.extend_from_slice(findings)` -- resolution-local findings (always vec![]) then ALL accumulated findings appended, so the emitted order is: sysid, sync_commit, following-map findings (corroboration and contradictions interleaved), quorum findings last.
- split_brain.rs:377-380 confidence = findings.map(determine_confidence_level).min(); Confidence derives Ord with variant order Refuse < Conflicting < BestEffort (lines 69-76), so min() = most severe.
- writer/build.rs:654 split_brain_reason -> :655 not Refuse -> :692 format_resolution.


## Row C-b (ADR-002 docs/adr/002-split-brain-resolution-refinement.md:42) -- db003's wal_receiver names db001 (lower TL=N), sta

- **Determinate under sections 1-4:** True
- **Matches ADR:** False | **Severity:** critical | **Destructive text:** True

**ADR expects (resolution):**

`SplitBrainResolution::LowerTimelineHasQuorum` with true_primary = db001 (TL=N, the LOWER timeline), stale_primaries = [db002] (TL=N+1), replicas_following_true = [db003]. Operator action: keep/promote-nothing on db001, FENCE db002.


**ADR expects (findings):**

Exactly two, per row C-b: [BidirectionalFlushingConfirmed(db001, db003), PrimaryQuorumUnsatisfied{primary: db002, required: 1, observed: 0}]. The ADR does not state an order beyond §4's "sanity-gate failures, then contradictions, then corroboration" (which would actually put PrimaryQuorumUnsatisfied before BidirectionalFlushingConfirmed if quorum-unsatisfied counts as a contradiction -- another minor under-specification). Confidence is not specified by the ADR.


**Determinacy:**

Determinate for the verdict and the finding set, with two under-specified corners that do not change this row's outcome.

WHAT THE RULES PIN DOWN:
- Row C-b names only the replica-side inputs (sender_host=db001, status in {streaming,catchup}, recent receipt, received_tli=N). The primary-side inputs required by the ADR §1 gate (db001's pg_stat_replication row for db003, state in {streaming,catchup}, fresh reply_time, non-empty application_name) are NOT in the row's columns. They are pinned indirectly: column 3 "db003 flushing? Yes, for db001" plus the post-matrix fact 1 ("C-b and C-c are correctly resolved ... once the gate has confirmed db003 is actively flushing for db001", ADR line ~50) means the full bidirectional gate passes, and the cluster assumption "repmgr-set `application_name` equals the node name" (ADR line ~33) pins the join key. So `Both`-sides-pass is derived, not merely asserted -> LowerTimelineHasQuorum + BidirectionalFlushingConfirmed(db001, db003) is determinate.
- PrimaryQuorumUnsatisfied(db002) is derived by §4's four-step rule: db002's gated-follower set is empty (db003's wal_receiver names exactly one sender and it is db001), so observed = |members ∩ {}| = 0 < count = 1. Payload {required: 1, observed: 0} is determinate.

CORNERS THE RULES DO NOT PIN (flagged, immaterial to C-b's outcome):
1. SELF-CONTRADICTORY, §4 derivation step 1: "Treat unparseable as method=ANY, count=∞ (defensive: emit no finding rather than a wrong one)". count=∞ combined with step 4 "Emit if observed < count" emits the finding ALWAYS, which is the exact opposite of the stated intent. The code follows the intent, not the letter (src/v2/analyze/split_brain.rs:650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue; };`). Also note the rule conflates "unparseable" with "empty/unset", which are different states -- empty SSN means no sync quorum exists at all, so no finding is genuinely right there.
2. SELF-CONTRADICTORY / UNVERIFIABLE, §4 "Required for LowerTimelineHasQuorum": "Implementations MUST emit `PrimaryQuorumUnsatisfied` for the stale primary in this case" is unconditional, but the derivation rule directly above it makes emission conditional on SSN parsing. When SSN is empty/unset the two clauses give opposite answers. This is precisely the state the existing unit test sits in (see mismatch_explanation).
3. UNVERIFIABLE: the ADR never states which `Confidence` C-b lands at, and §3 gives no finding->confidence mapping. `determine_confidence_level` + `min()` (src/v2/analyze/split_brain.rs:384-414) is a code-only invention. BestEffort is inferable from §3's prose ("gate passed; verdict is internally consistent") but is not derived by the spec.
4. AMBIGUOUS (immaterial): the ADR disagrees with itself about db002's SSN membership -- the matrix prose (line ~37) says db002's SSN still lists itself ("A = itself can't be its own standby"), while Out of scope says db002 has `ANY 1 (db001, db003)`. observed = 0 under either reading, so C-b's finding payload is unaffected.


**Code, synthetic names -- resolution:**

MATCHES the ADR. Input: db001 = primary TL=11 ip 127.1.12.151, SSN "ANY 1 (db002, db003)", synchronous_commit=on, wal_sender_timeout absent (-> 60000 default) or 300000, sysid 6968745321024393216, pg_stat_replication = [{application_name: "db003", state: Streaming, reply_time: Some(now)}]; db002 = primary TL=12 ip 127.2.12.151, SSN "ANY 1 (db001, db003)", empty pg_stat_replication, same sysid; db003 = replica node_name "db003", wal_receiver{sender_host: "127.1.12.151", sender_port: 5432, status: "streaming", last_msg_receipt_time: Some(now), received_tli: 11}, same sysid.

Output: SplitBrainInfo { true_primary: "db001", stale_primaries: ["db002"], resolution: LowerTimelineHasQuorum { true_primary_timeline: 11, stale_timeline: 12, replicas_following_true: ["db003"] } }.

NOTE on the as-written unit test `replica_following_lower_timeline_overrides` (src/v2/analyze/split_brain.rs:1041-1068): it uses the SAME topology but the builder default configuration contains ONLY archive_mode=on and synchronous_commit=on (src/v2.rs:59-61) -- there is NO synchronous_standby_names key. So parse("") -> None -> `continue` at split_brain.rs:650, and the test's asserted findings are `[BidirectionalFlushingConfirmed(db001, db003)]` with NO PrimaryQuorumUnsatisfied. The test is not a counterexample to the ADR's cluster assumption; it simply does not set up C-b's stated `ANY 1 (A, B)` config, so it silently enshrines the §4 "MUST emit" violation for the SSN-unset case.


**Code, synthetic names -- findings:**

findings (exact order as built): [
  BidirectionalFlushingConfirmed(ReplicationLink{primary: "db001", replica: "db003"}),
  PrimaryQuorumUnsatisfied{primary: "db002", required: 1, observed: 0}
]
Order comes from split_brain.rs:179-181: no sysid mismatch, no SynchronousCommitWeakened, then following_findings (build_replica_following_map iterates highest-TL primaries first: db002/db003 fails the replica-side host compare silently, then db001/db003 passes both sides), then emit_quorum_findings over `primaries` in slice order (db001 -> observed 1, no finding; db002 -> observed 0, finding). determine_true_primary then does `split_brain_info.findings.extend_from_slice(findings)` onto an initially-empty vec (split_brain.rs:381), so the order is preserved verbatim.

With the as-written test defaults (SSN unset): findings = [BidirectionalFlushingConfirmed(db001, db003)] only -- ADR §4's "Implementations MUST emit PrimaryQuorumUnsatisfied for the stale primary in this case" is violated, and the short string still prints "quorum-blocked" with no finding backing it (format_resolution hardcodes the parenthetical and never reads info.findings, src/v2/writer/build.rs:704-711).


**Code, synthetic names -- confidence:**

Confidence::BestEffort. min() over determine_confidence_level (split_brain.rs:384-394): BidirectionalFlushingConfirmed -> BestEffort (line 414); PrimaryQuorumUnsatisfied{primary: "db002"} vs true_primary "db001" -> not equal -> BestEffort (lines 403-409). min(BestEffort, BestEffort) = BestEffort. Not Refuse, so split_brain_reason takes the format_resolution branch (build.rs:655-659).


**Code, synthetic names -- SHORT STRING:**

SplitBrain: db001 has quorum (lower TL=11), fence db002 (TL=12, quorum-blocked)

(build.rs:704-711; `stale` = info.stale_primaries.first() = "db002".) This is character-for-character the ADR §4 "Acceptable" example modulo TL numbers, and it names the correct action: fence the HIGHER-TL node, keep the lower-TL node that holds the acked writes.


**Code, FLEET names -- resolution:**

DIVERGES -- the verdict INVERTS. Input identical to above but with the naming captured in tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json: node_name "dev-pg-app001-db001.sto1.example.com" (ip 127.1.12.151, TL=11), "dev-pg-app001-db002.sto2.example.com" (ip 127.2.12.151, TL=12), "dev-pg-app001-db003.sto3.example.com" (ip 127.3.12.151, replica); db001's pg_stat_replication row has application_name "dev_pg_app001_db003"; SSN = "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"; synchronous_commit=remote_apply; wal_sender_timeout=300000 (-> threshold 180000 ms); db003's wal_receiver{sender_host "127.1.12.151", sender_port 5432, status "streaming", last_msg_receipt_time fresh}.

Replica side of the gate PASSES for db001 (split_brain.rs:281-286 -- host/port/status/freshness all match). Primary side FAILS at split_brain.rs:307 `&& conn.application_name == replica.node_name`: "dev_pg_app001_db003" != "dev-pg-app001-db003.sto3.example.com". So primary_row = None -> PrimaryDoesNotSeeReplica pushed (line 335) and db003 is NEVER inserted into `following`.

`replicas_following` is therefore EMPTY. In resolve_with_different_timelines, replicas_following_stale is empty and replicas_following_highest is empty, so both the LowerTimelineHasQuorum branch (line 449) and the Both branch (line 465) are skipped and control falls through to the final else:

  resolution = HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }
  true_primary = "dev-pg-app001-db002.sto2.example.com"
  stale_primaries = ["dev-pg-app001-db001.sto1.example.com"]

findings (exact order): [
  PrimaryDoesNotSeeReplica(ReplicationLink{primary: db001-FQDN, replica: db003-FQDN}),
  PrimaryQuorumUnsatisfied{primary: db001-FQDN, required: 1, observed: 0},
  PrimaryQuorumUnsatisfied{primary: db002-FQDN, required: 1, observed: 0}
]
The two quorum findings are a SECOND, independent instance of the same name-join bug: emit_quorum_findings intersects SSN `members` (application-name form, "dev_pg_app001_db003") with `gated` (node_name form) at split_brain.rs:660-664, so observed is structurally 0 for EVERY primary on this fleet, forever -- even for a primary whose quorum is genuinely satisfied.

Confidence = Conflicting: PrimaryDoesNotSeeReplica -> Conflicting (line 412); PrimaryQuorumUnsatisfied{db001} != true_primary -> BestEffort; PrimaryQuorumUnsatisfied{db002} == true_primary -> Conflicting (line 405). min = Conflicting. Crucially this is NOT Refuse, so format_refuse never runs and the operator gets an actionable-looking pick.


**Code, FLEET names -- SHORT STRING:**

SplitBrain: dev-pg-app001-db002.sto2.example.com has quorum (TL=12), demote dev-pg-app001-db001.sto1.example.com (TL=11, no live replicas)

(build.rs:712-719.) Three separate falsehoods in one line, all of them pointing the operator at the destructive action:
- "db002 has quorum" -- the findings vector in the same SplitBrainInfo contains PrimaryQuorumUnsatisfied{db002, required 1, observed 0}. The short string and its own details_json contradict each other, because format_resolution hardcodes the parenthetical per variant and never inspects info.findings.
- "no live replicas" -- db003 is streaming from db001 right now with a fresh last_msg_receipt_time; the resolver saw it pass the replica-side gate and discarded it on a string compare.
- "demote db001" -- db001 is the node whose ANY 1 quorum is satisfied by db003 under synchronous_commit=remote_apply, i.e. the only node that can be holding client-acked writes.
Also note format_resolution is the only short-string builder in build.rs that does NOT pass its node names through extract_db_number (contrast lines 122, 195, 202, 250, 505-506, 623, 642), so the fleet string carries raw FQDNs where the ADR's example shows "db001"/"db002". Cosmetic next to the inversion, but it is why the ADR's illustrative text never matches real output.


**Mismatch:**

HEADLINE: with the real fleet naming, C-b -- the ADR's canonical "keeping the LOWER timeline is correct" row -- resolves to `HigherTimeline` and emits "demote <db001>". That is the C-g data-loss outcome reached from a benign, fully-observable C-b state, and it is reachable with nothing more exotic than the naming already captured in tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json.

ROOT CAUSE, and which side is right: ADR §1's primary-side gate says "The primary's `pg_stat_replication` has a row whose `application_name` equals the replica's node name", resting on the cluster assumption at ADR line ~33: "repmgr-set `application_name` equals the node name". The code implements that faithfully (split_brain.rs:307). THE ADR ASSUMPTION IS FALSIFIED BY OUR OWN CAPTURED DATA: in the fixture, node_name = "dev-pg-app001-db002.sto2.example.com" while pg_stat_replication.application_name = "dev_pg_app001_db002" (hyphens+FQDN vs underscores+bare). So the ADR is wrong and the code inherits the error; the fixture is ground truth. No deferral covers this -- Out of scope lists "Hostname-form `primary_conninfo`" as the known naming limitation, which is the replica-side host compare, a DIFFERENT comparison that actually works on this fleet (sender_host is an IP). The application-name-form mismatch has no carve-out anywhere in the ADR.

WHY THE TEST SUITE CANNOT SEE IT: every split-brain test forces application_name == node_name. `replica_following_lower_timeline_overrides` (split_brain.rs:1044) uses primary_with_followers(..., &["db003"]) against a replica named "db003"; even the "realistic names" integration test analyze.rs:1281 does `with_followers(&["dev-pg-app001-db003.sto3.example.com"])`, i.e. it hand-feeds the FQDN as an application_name, which is not a value this fleet ever produces. writer/build.rs already owns the bridge (normalize_application_name at build.rs:422, extract_db_number at build.rs:408) and every other analyze path avoids the join entirely (checks.rs iterates health.replication directly), so split_brain.rs is the single place that depends on a name equality that does not hold in production.

SECONDARY DIVERGENCES ON THIS ROW:
1. emit_quorum_findings has the SAME name-join bug on the SSN member set (split_brain.rs:660-664): `members` are application-name form from postgresql.conf, `gated` are node_names, so `observed` is 0 for every primary on this fleet unconditionally. Even if the §1 gate were fixed in isolation, C-b would then produce PrimaryQuorumUnsatisfied for db001 -- the node the verdict just declared "has quorum" -- flipping Confidence to Conflicting and putting the short string in direct contradiction with its own findings. Both joins must be fixed together.
2. §4's "Implementations MUST emit PrimaryQuorumUnsatisfied for the stale primary [under LowerTimelineHasQuorum]" is violated whenever synchronous_standby_names is empty/unset/unparseable, because emission is gated behind `parse(...) else { continue }` (split_brain.rs:650). The as-written unit test locks this in (asserts findings == [BidirectionalFlushingConfirmed] exactly). Verdict on who is right: the CODE is right for empty SSN (no sync quorum exists, so "quorum unsatisfied" would be a fabricated finding) and the ADR's unconditional MUST is wrong; but the ADR's own step-1 gloss ("unparseable -> count=∞ ... emit no finding") is internally contradictory, so the spec needs repair either way. Severity of this one alone: medium.
3. `LowerTimelineHasQuorum` never actually consults synchronous_standby_names -- it is derived purely from the follower map (split_brain.rs:449). The variant can fire, and the short string can print "has quorum", on a cluster with SSN empty (fully asynchronous replication), where a flushing replica proves nothing about acked writes. §4's rename argument ("Operators reading the verdict at 3 AM") assumes the word "quorum" is load-bearing; in the code it is decoration. Medium.
4. The §4 short-string contract item 3 ("PrimaryQuorumUnsatisfied MUST appear inline in the short string when present") is not implemented as a derivation: format_resolution hardcodes "quorum unsatisfied" / "quorum-blocked" / "no live replicas" per variant and never reads info.findings (build.rs:692-724). It coincidentally satisfies the contract in the synthetic C-b case and actively violates it in the fleet case, where the string asserts "has quorum" for a node carrying PrimaryQuorumUnsatisfied.

REACHABILITY, kept separate as instructed:
- reachable_in_code: YES, verified by reading. Every branch above is a plain string compare with no guard; no unobserved state is required.
- reachable_on_fleet: NOT PROVEN. The naming mismatch is captured evidence (the fixture). What is NOT captured is a two-primary post-failover scan on this fleet -- there is no split-brain fixture in tests/fixtures. So "the code will emit this string on our fleet" rests on the fixture's naming carrying over into a split-brain scan, which is very likely but is an inference, not a capture.
- WHAT WOULD SETTLE IT, exactly: on any current node, `SELECT application_name FROM pg_stat_replication;` compared against the inventory node_name for the same host -- if they differ by so much as the domain suffix, the §1 primary-side gate can never pass in production and EVERY split-brain scan on this fleet degenerates to timeline-only. That single query is cheap, runs on a healthy cluster, and needs no split-brain to reproduce. Follow it with `SHOW synchronous_standby_names;` on both a primary and a promoted node to confirm the member-form half of the bug and to settle the ADR's internal disagreement about whether db002's SSN lists itself.


**Code path:**

RUN 1 -- synthetic names (application_name == node_name), SSN set per ADR cluster assumption:
1. src/v2/analyze.rs:317-318 `if primaries.len() > 1 { let split_brain_info = resolve_split_brain(&primaries, &replicas);` -- primaries in Cluster::primaries() node order = [db001, db002] (src/v2/cluster.rs:69-71).
2. src/v2/analyze/split_brain.rs:132 resolve_split_brain -> :137 extract_timeline_info -> highest_timeline = 12 (db002), primaries_with_lower_timeline = [(db001, 11)].
3. :139-141 reference_sysid = Some("6968745321024393216") (both primaries agree); mismatched_sysid_nodes = [] -> no SystemIdentifierMismatch.
4. :161-174 synchronous_commit loop: "on" not in WEAKENED_SYNCHRONOUS_COMMIT (:13) -> no SynchronousCommitWeakened.
5. :177 build_replica_following_map. :266 `let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;` = 180000 for wal_sender_timeout=300000. Iteration order is highest-TL primaries then lower (:255-260).
   5a. primary db002 x replica db003: :281 `wr.sender_host == primary.ip_address.to_string()` -> "127.1.12.151" != "127.2.12.151" -> replica_passes = false; :292 the same compare guards the stale finding, so NOTHING is emitted (correct -- avoids a spurious ReplicaWalReceiverStale).
   5b. primary db001 x replica db003: :281-287 host match, port 5432, status "streaming", freshness ok -> replica_passes = true. :306-313 primary-side find: application_name "db003" non-empty AND == replica.node_name "db003" AND state Streaming AND reply_time fresh -> Some(row). :318-322 insert into `following`: {db001 -> ["db003"]}. :324 push BidirectionalFlushingConfirmed(db001, db003). :331 row.state is Streaming not Catchup -> no ReplicaInCatchup.
6. :180 emit_quorum_findings over [db001, db002]: db001 SSN "ANY 1 (db002, db003)" -> :650 parse Some{count 1, members ["db002","db003"]}; gated = ["db003"]; :660-664 observed = 1; :665 1 < 1 false -> no finding. db002 SSN -> parse ok; gated = [] -> observed 0; 0 < 1 -> push PrimaryQuorumUnsatisfied{db002, 1, 0}.
7. :182 determine_true_primary -> :357-360 highest count == 1 && lower non-empty -> resolve_with_different_timelines (:428). replicas_following_highest (db002) = []; loop :440-447 finds followers for db001 -> replicas_following_stale = ["db003"], stale_with_followers = db001. :449 `if !replicas_following_stale.is_empty() && replicas_following_highest.is_empty()` -> TRUE -> :453-462 LowerTimelineHasQuorum{true_primary_timeline: 11, stale_timeline: 12, replicas_following_true: ["db003"]}, true_primary db001, stale_primaries ["db002"].
8. :381 extend_from_slice(findings) -> [BidirectionalFlushingConfirmed(db001,db003), PrimaryQuorumUnsatisfied{db002,1,0}]. :384-390 min over determine_confidence_level = BestEffort.
9. src/v2/writer/build.rs:532 `split_brain_reason(info)` -> :655 `matches!(info.confidence, Confidence::Refuse)` false -> :658 format_resolution -> :704-711 LowerTimelineHasQuorum arm -> "SplitBrain: db001 has quorum (lower TL=11), fence db002 (TL=12, quorum-blocked)".

RUN 2 -- real fleet naming (node_name FQDN-with-hyphens, application_name underscores-no-domain, SSN members in application-name form):
Steps 1-4 identical (sysids agree; synchronous_commit=remote_apply is not weakened).
5'. :266 threshold_ms = 300000/2 + 30000 = 180000.
   5a'. db002 x db003: :281 "127.1.12.151" != "127.2.12.151" -> silent skip, as in Run 1.
   5b'. db001 x db003: :281-287 replica side PASSES (sender_host 127.1.12.151 == db001.ip, port 5432, status "streaming", fresh). :307 `conn.application_name == replica.node_name` -> "dev_pg_app001_db003" != "dev-pg-app001-db003.sto3.example.com" -> the .find() predicate is false for every row -> primary_row = None. :335 push PrimaryDoesNotSeeReplica(db001, db003). `following` stays EMPTY -- this is the pivot.
6'. :180 emit_quorum_findings: both primaries parse SSN Some{count 1, members ["dev_pg_app001_db002","dev_pg_app001_db003"]}; gated is [] for both (and would be node_name-form even if non-empty), so :660-664 observed = 0 for both; :665 fires twice -> PrimaryQuorumUnsatisfied{db001,1,0} then PrimaryQuorumUnsatisfied{db002,1,0}.
7'. :182 -> :357 same shape -> resolve_with_different_timelines. replicas_following_highest = []; the :440-447 loop finds no entry for db001 -> replicas_following_stale = []. :449 FALSE (both empty). :465 `else if !replicas_following_highest.is_empty()` FALSE. -> final else :486-500 HigherTimeline{true_primary_timeline: 12, stale_timeline: 11}, true_primary = db002-FQDN, stale_primaries = [db001-FQDN].
8'. :381 findings = [PrimaryDoesNotSeeReplica(db001,db003), PrimaryQuorumUnsatisfied{db001,1,0}, PrimaryQuorumUnsatisfied{db002,1,0}]. :384-390 min(Conflicting, BestEffort, Conflicting) = Conflicting -- NOT Refuse.
9'. build.rs:655 false -> :658 format_resolution -> :712-719 HigherTimeline arm -> "SplitBrain: dev-pg-app001-db002.sto2.example.com has quorum (TL=12), demote dev-pg-app001-db001.sto1.example.com (TL=11, no live replicas)".

SIDE OBSERVATION (info, not part of C-b): the freshness predicate at :285-287 is one-sided -- `(r_health.current_time - t).num_milliseconds() <= threshold_ms` accepts arbitrarily NEGATIVE deltas, i.e. a last_msg_receipt_time in the node's own future passes unconditionally. This is also why the unit tests' gate passes at all: the builders set current_time = DateTime::<Utc>::UNIX_EPOCH (src/v2.rs:63, src/v2.rs:252) while last_msg_receipt_time / reply_time are Some(Utc::now()) (src/v2.rs:198, src/v2.rs:262), so every test evaluates a delta of roughly -55 years. The freshness gate ADR §1 calls load-bearing is therefore never actually exercised in the positive direction by these fixtures.


## Row C-e (ADR-002 case matrix, docs/adr/002-split-brain-resolution-refinement.md:45)

- **Determinate under sections 1-4:** True
- **Matches ADR:** False | **Severity:** high | **Destructive text:** False

**ADR expects (resolution):**

`SplitBrainResolution::Both`, true primary db002, stale primary db001. Rationale in the row and at ADR line 56: "the replica's `wal_receiver` is the authoritative side ... If the replica-side gate fails for primary X, no amount of primary-side state on X can rescue the match. This is what makes C-e resolve cleanly -- `db001`'s stale row is filtered because `db003`'s `wal_receiver` doesn't name `db001`."


**ADR expects (findings):**

Exactly one finding per the row: `BidirectionalFlushingConfirmed(db002, db003)`. (Per §4's derivation rule the row should also carry `PrimaryQuorumUnsatisfied{primary: db001, required: 1, observed: 0}`, as C-a does; the row omitting it is an ADR self-contradiction, not a code defect.) Confidence is not stated in the row; §3 implies BestEffort since no sanity gate fired.


**Determinacy:**

Sections 1-4 DO pin down a unique outcome for C-e -- but only under the ADR's own "Cluster assumptions" bullet at line 27 ("repmgr-set `application_name` equals the node name"), and the outcome they derive is NOT the one the row states.

Derivation under §1 + §4:
- §1 replica side is authoritative and is evaluated first: db003's `wal_receiver.sender_host` names db002, so the db001 candidacy fails on the replica side and the primary-side corroboration for db001 is never consulted. Determinate: db001 gets no following-entry, and (because `PrimaryDoesNotSeeReplica` is defined in §1 as "replica side passes but primary side does not") db001 also gets no finding. The stale row on db001 is genuinely inert.
- §1 for db002: both sides fresh -> following + `BidirectionalFlushingConfirmed(db002, db003)`. Determinate.
- §4 derivation rule for `PrimaryQuorumUnsatisfied` is mechanical: parse SSN, observed = |members ∩ gated_followers|, emit if observed < count. With the assumed cluster-wide `ANY 1 (A, B)` (ADR line 22, and the ADR reasons explicitly about db001's SSN at line 37), db001 has zero gated followers -> observed = 0 < 1 -> the rule REQUIRES `PrimaryQuorumUnsatisfied(db001)`. The C-e row's findings column omits it. C-a -- identical db001 condition ("No (no replica acking it)") -- lists it. So the row's findings column is an outcome the ADR states but does not derive, and it contradicts both §4's rule and its own sibling row C-a.

Two genuinely under-specified points:
1. The row does not fix the naming form of `application_name` vs `node_name`; §1 says the primary-side row's `application_name` must equal "the replica's node name", leaning on the line-27 assumption. That assumption is FALSE on this fleet (see mismatch_explanation), and it is the single input that flips the outcome. Under the true fleet forms the rules pin down a DIFFERENT unique outcome (HigherTimeline), so the row is determinate but wrong-by-assumption rather than ambiguous.
2. The resolution-variant mapping (follower map -> `Both`/`HigherTimeline`/...) is not stated anywhere in §1-§4; it is inherited pre-existing behaviour. `Both` is the natural reading and matches the code, so I do not classify the variant as unverifiable, but the ADR never derives it.


**Code, synthetic names -- resolution:**

`SplitBrainResolution::Both { true_primary_timeline: 12, stale_timeline: 11, replicas_following_true: ["db003"] }`, true_primary = "db002", stale_primaries = ["db001"]. Matches the ADR row's variant and winner exactly.


**Code, synthetic names -- findings:**

With SSN explicitly set to the ADR's assumed `ANY 1 (db002, db003)` on both primaries, findings in order:
1. `BidirectionalFlushingConfirmed(ReplicationLink { primary: "db002", replica: "db003" })`  (split_brain.rs:324)
2. `PrimaryQuorumUnsatisfied { primary: "db001", required: 1, observed: 0 }`  (split_brain.rs:665-669; db001 has zero gated followers)
db002 gets no quorum finding: members {db002, db003} ∩ gated {db003} = 1, not < 1.

With the tests_common default (SSN unset, src/v2.rs:59-61 sets only archive_mode and synchronous_commit), findings = [`BidirectionalFlushingConfirmed(db002, db003)`] only, because split_brain.rs:650 `continue`s on the empty SSN. That single-element list is exactly what the ADR row claims -- so the existing test `timeline_and_replica_evidence_agree` (split_brain.rs:1013-1038, which asserts precisely that literal) "confirms" the row only by virtue of an unset config key.


**Code, synthetic names -- confidence:**

`Confidence::BestEffort`. min over: `BidirectionalFlushingConfirmed` -> BestEffort (split_brain.rs:414); `PrimaryQuorumUnsatisfied{primary: "db001"}` with true_primary "db002" -> primary != true_primary -> BestEffort (split_brain.rs:403-409). Same result in the SSN-unset variant (single BestEffort finding).


**Code, synthetic names -- SHORT STRING:**

SplitBrain: db002 has quorum (TL=12), demote db001 (TL=11, quorum unsatisfied)

(build.rs:700-703, the `Both` arm. Identical in both the SSN-set and SSN-unset variants -- the "quorum unsatisfied" clause is a literal in the format string and is emitted even when `emit_quorum_findings` computed nothing at all, so the text asserts a quorum state the tool never evaluated.)


**Code, FLEET names -- resolution:**

`SplitBrainResolution::HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }`, true_primary = "dev-pg-app001-db002.sto2.example.com", stale_primaries = ["dev-pg-app001-db001.sto1.example.com"].

Findings in order:
1. `PrimaryDoesNotSeeReplica(ReplicationLink { primary: "dev-pg-app001-db002.sto2.example.com", replica: "dev-pg-app001-db003.sto3.example.com" })` -> Conflicting (split_brain.rs:335, :412)
2. `PrimaryQuorumUnsatisfied { primary: "dev-pg-app001-db001.sto1.example.com", required: 1, observed: 0 }` -> BestEffort
3. `PrimaryQuorumUnsatisfied { primary: "dev-pg-app001-db002.sto2.example.com", required: 1, observed: 0 }` -> Conflicting (primary == true_primary, split_brain.rs:403-407)

Confidence = `Conflicting`.

Two independent naming failures, both from the raw compares:
- Gate level, split_brain.rs:307-308 `conn.application_name == replica.node_name`: "dev_pg_app001_db003" vs "dev-pg-app001-db003.sto3.example.com" -> never equal on this fleet, so NO replica can ever gate-pass for ANY primary. `Both`, `LowerTimelineHasQuorum` and `ReplicaFollowing` are unreachable on real fleet data; every multi-primary scan degrades to `HigherTimeline` or `Indeterminate`.
- Quorum level, split_brain.rs:660-664 `members.iter().filter(|m| gated.iter().any(|g| g == *m))`: SSN members are the application-name form (fixture: `ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )`) while `gated` holds node_names (FQDN). Even if the gate were fixed, `observed` would still be 0 for every primary, so `PrimaryQuorumUnsatisfied` fires unconditionally on this fleet -- including against the elected primary, which drags confidence to Conflicting on every real split brain.

The bridging helpers exist and are unused by the resolver: `normalize_application_name` (build.rs:422-430, "dev_pg_app001_db002" -> "db002") and `extract_db_number` (build.rs:408-420, FQDN -> "db002@sto2").


**Code, FLEET names -- SHORT STRING:**

SplitBrain: dev-pg-app001-db002.sto2.example.com has quorum (TL=12), demote dev-pg-app001-db001.sto1.example.com (TL=11, no live replicas)

(build.rs:712-718, the `HigherTimeline` arm.) Two operator-facing falsehoods in one line, both contradicted by the tool's own output in the same record: "no live replicas" is emitted while db003 is observably streaming from the elected primary with a ~37 ms-old `last_msg_receipt_time`, and "has quorum" is emitted for the very node the findings list records as `PrimaryQuorumUnsatisfied{required: 1, observed: 0}`. `format_resolution` never inspects `info.findings` (build.rs:692-727), so nothing can reconcile the two.


**Mismatch:**

Three separate mismatches, in increasing order of importance.

(1) ADR self-contradiction, low. Under §4's `PrimaryQuorumUnsatisfied` derivation rule and the cluster-wide `ANY 1 (A, B)` assumption (ADR:22, reasoned about at ADR:37), C-e must also carry `PrimaryQuorumUnsatisfied(db001)` -- sibling row C-a lists exactly that for the identical db001 condition. The row omits it. The code emits it. The code is right; the row's findings column is stale/self-contradictory. Classification: self-contradictory (ADR), implemented (code).

(2) The part of the row that IS derived is implemented correctly. The asymmetric-precedence claim at ADR:56 holds literally in code: db001's stale `pg_stat_replication` row is unreachable because the only read of a primary's replication list is split_brain.rs:305, gated behind the replica-side `continue` at :299, and `PrimaryDoesNotSeeReplica` (:335) sits inside the same post-gate match. `PrimaryDoesNotSeeReplica` therefore CANNOT fire spuriously for db001 in C-e. It can and does fire for db002 -- not spuriously in the "one-sided claim" sense the ADR intends, but as a pure naming artifact (see 3).

(3) Headline, high. The row's outcome depends entirely on ADR:27 "repmgr-set `application_name` equals the node name", and that assumption is FALSE on this fleet. tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json: node_name = "dev-pg-app001-db003.sto3.example.com", `pg_stat_replication.application_name` = "dev_pg_app001_db003", SSN = "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )". With those real forms, C-e resolves to `HigherTimeline` + `PrimaryDoesNotSeeReplica(db002, db003)` + `PrimaryQuorumUnsatisfied` on BOTH primaries at `Conflicting`, not `Both` + `BidirectionalFlushingConfirmed` at BestEffort. The ADR is not merely unimplemented here -- the assumption it rests on is contradicted by captured fleet data, so ADR:27 should be corrected and §1's primary-side gate respecified in terms of a normalized comparison. Ground truths 2 and 3 in the brief are confirmed exactly as stated; I found no error in them.

Why the test suite cannot see it: every gate test passes node-name form into `with_followers` (e.g. split_brain.rs:1015 `primary_with_followers(2, "db002", IP_DB2, 12, &["db003"])`), and even the FQDN integration test does `with_followers(&["dev-pg-app001-db003.sto3.example.com"])` (analyze.rs:1281). The builder's own DEFAULT app-name generator produces the real underscore form -- src/v2.rs:167-173 `format!("dev_pg_app001_db00{}", i + 2)` -- so the fixture-faithful path exists in the builder and is bypassed by every gate test that matters.

Scope note kept separate from the row: for C-e's own state the wrong-variant outcome is not destructive (see destructive_text_possible). The severe consequence lands on rows C-b/C-c, where the same all-gates-fail collapse turns "keep db001" into `HigherTimeline` -> "demote db001 ... no live replicas". That is another row's assignment; I flag it because C-e and C-b become byte-identical output under fleet naming, which is what makes the C-e string untrustworthy rather than merely wrong.

Not verifiable from the repo, stated as such: whether the deployed repmgr actually sets `application_name` to the underscore short form on a POST-FAILOVER cluster (the fixture is a healthy non-failover cluster). Settle it with, on each node: `SELECT application_name, state, reply_time FROM pg_stat_replication;` plus `SHOW synchronous_standby_names;` plus the repmgr.conf `node_name`/`conninfo application_name`, captured while two primaries are live. If that capture shows FQDN application_names, the ADR:27 assumption stands and (3) collapses to a fixture-only artifact.


**Code path:**

Input state (both runs): db001 = primary TL=11, db002 = primary TL=12, db003 = replica whose `wal_receiver` names db002's IP, port 5432, status "streaming", fresh `last_msg_receipt_time`; db001 carries the C-e stale `pg_stat_replication` row for db003 (state Streaming, reply_time still fresh -- "within `wal_sender_timeout` of disconnect"); db002 carries a live row for db003. All three share sysid 6968745321024393216. Relevant builder defaults (src/v2.rs): `synchronous_commit` = "on" (v2.rs:61), NO `synchronous_standby_names` key, NO `wal_sender_timeout` key, `current_time` = UNIX_EPOCH (v2.rs:63 and v2.rs:252) while `reply_time`/`last_msg_receipt_time` = `Utc::now()` (v2.rs:198, v2.rs:262), so every freshness delta is large and NEGATIVE and the freshness gate passes vacuously in unit tests.

1. src/v2/analyze.rs:318 `let split_brain_info = resolve_split_brain(&primaries, &replicas);` -- `primaries`/`replicas` are in cluster-node order (analyze.rs:301-302, cluster.rs:69-76), i.e. [db001, db002] / [db003].
2. split_brain.rs:132 `resolve_split_brain` -> :189 `extract_timeline_info`: highest = 12, highest node = db002, highest list = [db002], lower list = [(db001, 11)].
3. split_brain.rs:566 `reference_sysid` -> Some("6968745321024393216") (2 of 2 primaries agree); :583 `mismatched_sysid_nodes` -> [] ; `filtered_replicas` = [db003]; `findings` starts empty (:152-158).
4. split_brain.rs:160-175 synchronous_commit loop: "on" (synthetic) / "remote_apply" (fleet); `WEAKENED_SYNCHRONOUS_COMMIT` at :13 is ["local","off","remote_write",""] -> no finding either way.
5. split_brain.rs:245 `build_replica_following_map`, primaries iterated highest-TL first then lower-TL (:254-259), so db002 then db001.
   5a. primary = db002: :266 `let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;` -> 60_000/2+30_000 = 60_000 ms synthetic (:609 default), 300_000/2+30_000 = 180_000 ms with the fleet's `wal_sender_timeout` = "300000".
       :272 `let Some(wr) = &r_health.wal_receiver else { continue };` -> present.
       :281 `let replica_passes = wr.sender_host == primary.ip_address.to_string() && wr.sender_port == 5432 && matches!(wr.status.as_str(), "streaming" | "catchup") && wr.last_msg_receipt_time.is_some_and(...)` -> TRUE (fleet: 127.2.12.151 == 127.2.12.151, receipt age ~37 ms vs 180_000).
       :305 `let primary_row = p_health.replication.iter().find(|conn| { !conn.application_name.is_empty() && conn.application_name == replica.node_name && ... })` -- RAW equality, no normalization. SYNTHETIC: "db003" == "db003" -> Some(row) -> :324 push `BidirectionalFlushingConfirmed(db002, db003)`, insert into `following`; row.state is Streaming so no `ReplicaInCatchup`. FLEET: "dev_pg_app001_db003" == "dev-pg-app001-db003.sto3.example.com" -> false -> None -> :335 push `PrimaryDoesNotSeeReplica(db002, db003)` and NO entry in `following`.
   5b. primary = db001: :281 `wr.sender_host` ("127.2.12.151") != db001.ip ("127.1.12.151") -> `replica_passes` = false; :288 enters the reject branch; :293 `if wr.sender_host == primary.ip_address.to_string()` is also false -> NO `ReplicaWalReceiverStale`; `continue` at :299. **db001's `p_health.replication` is read only at :305, which is unreachable after this `continue` -- the only read of a primary's `pg_stat_replication` anywhere in the resolver is split_brain.rs:305 (grep: single hit).** So the stale row on db001 provably cannot produce a following-entry or any finding, and `PrimaryDoesNotSeeReplica` cannot fire for db001 because its push site (:335) is inside the post-replica-gate match. ADR line 56's asymmetric-precedence claim: CONFIRMED in code.
6. split_brain.rs:636 `emit_quorum_findings`, iterating `primaries` in caller order [db001, db002]; :650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue };`. With the builder default (SSN unset) this `continue`s for both primaries and emits NOTHING. With SSN set (the ADR's assumed cluster config, and the fixture's `ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )`) it emits per :660-668 `observed = |members ∩ gated|`.
7. split_brain.rs:352 `determine_true_primary` -> highest list len == 1 and lower non-empty -> :422 `resolve_with_different_timelines`. SYNTHETIC: `replicas_following_highest` = ["db003"], `replicas_following_stale` = [] -> first branch skipped, :465 `} else if !replicas_following_highest.is_empty() {` taken -> :476 `SplitBrainResolution::Both`. FLEET: both lists empty -> falls to the final `else` (:484-500) -> `SplitBrainResolution::HigherTimeline`.
8. split_brain.rs:381 `split_brain_info.findings.extend_from_slice(findings);` -> final order = [gate findings..., quorum findings...]; :383-391 confidence = min over :395 `determine_confidence_level`.
9. src/v2/writer/build.rs:532 `split_brain_reason(info)` -> :654-659 not Refuse -> :692 `format_resolution`, which uses `info.true_primary` and `info.stale_primaries.first()` RAW (no `extract_db_number`, build.rs:408) and hardcodes the quorum parenthetical per variant (:700-718) without ever inspecting `info.findings`.


## Row C-d (docs/adr/002-split-brain-resolution-refinement.md:44) -- `sender_host=db001` but `status != streaming/catchup`, OR 

- **Determinate under sections 1-4:** False
- **Matches ADR:** False | **Severity:** critical | **Destructive text:** True

**ADR expects (resolution):**

ADR-002:44 states: true primary (operational) = `db002`; verdict = `HigherTimeline` "(no flushing replica anywhere)". Supporting columns: "db003 flushing? No -- gate rejects"; "db001 committing? No (without db003 acking, db001 can't satisfy its quorum)". Line 52 reinforces: "C-d, C-e, and the inactive subcases of C-f require the gate to *reject* stale/one-sided evidence that the current code accepts."


**ADR expects (findings):**

Exactly one finding is listed in the row: `ReplicaWalReceiverStale(db003, db001)`. No `PrimaryQuorumUnsatisfied` is listed for either primary, and no Confidence value is given. This is in direct conflict with the ADR's own §4 derivation rule (ADR:187-192: parse SSN; observed = |members INTERSECT gated_followers|; "Emit if observed < count"), which under the ADR's own cluster assumption `synchronous_standby_names = 'ANY 1 (A, B)'` (ADR:31) derives observed=0 < 1 for BOTH db001 and db002 in C-d, since no replica is gated for anyone. Compare C-a, which does list `PrimaryQuorumUnsatisfied(db001)` in the same situation for one primary.


**Determinacy:**

Partially determinate. What §1-4 DO pin down: (a) the replica-side gate rejects db003 for db001 (§1's status set and freshness clause), so no replica is a gated follower; (b) with no follower evidence, the pre-existing resolver falls through to timeline -> `HigherTimeline`, true_primary=db002. Those two are derivable. What the rules do NOT pin down, so the row asserts them rather than deriving them: (1) CONFIDENCE. The ADR gives a finding->Confidence mapping only for the two §2 sanity gates and for `DivergentReplicaWal` (§7). Nothing in §3 or §4 says what `ReplicaWalReceiverStale` or `PrimaryQuorumUnsatisfied` map to, and nothing specifies min()-over-findings as the aggregation rule. The code's `Conflicting` (split_brain.rs:395-419, aggregated at :384-390) is an implementation invention -- unverifiable against the spec. (2) THE FINDINGS VECTOR. §4's derivation rule mandates two `PrimaryQuorumUnsatisfied` findings the row omits -> self-contradictory, not merely under-specified. (3) EMISSION MULTIPLICITY of `ReplicaWalReceiverStale`. §1 describes the gate's pass/fail conditions but never says the finding is emitted per-(primary,replica) pair nor that it is suppressed when `sender_host` names a different primary. That guard (split_brain.rs:293) is a code invention documented only in a code comment. (4) NAME vs TRIGGER. §4 defines `ReplicaWalReceiverStale` as "gate rejected *stale* replica-side evidence", but the row's first subcase (`status != streaming/catchup`) is not staleness, and the code additionally emits it for `sender_port != 5432` (test `gate_rejects_wrong_sender_port`, split_brain.rs:1218ff), which the row never mentions. (5) FINDINGS ORDER/CAP. §4 mandates "sanity-gate failures, then contradictions, then corroboration. Cap at ~5"; no cap exists in the code (no truncate/take in split_brain.rs), and `PrimaryQuorumUnsatisfied`'s bucket in that taxonomy is unstated.


**Code, synthetic names -- resolution:**

`SplitBrainResolution::HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }`; `true_primary: "db002"`, `stale_primaries: ["db001"]`. Findings vector, in order: with the ADR's cluster assumption SSN=`ANY 1 (db002, db003)` set on both primaries -> [ReplicaWalReceiverStale { replica: "db003", claimed_sender: "db001" }, PrimaryQuorumUnsatisfied { primary: "db001", required: 1, observed: 0 }, PrimaryQuorumUnsatisfied { primary: "db002", required: 1, observed: 0 }]. Confidence = Conflicting. With the bare `PrimaryHealthBuilder` default (no SSN key) -- which is what the two existing C-d tests `gate_rejects_stale_last_msg_receipt` (split_brain.rs:1161) and `gate_rejects_status_not_streaming` (split_brain.rs:1193) actually construct -> findings = [ReplicaWalReceiverStale { replica: "db003", claimed_sender: "db001" }] only, Confidence = Conflicting. Neither test asserts on confidence or on the absence of quorum findings, so the builder default silently hides the two `PrimaryQuorumUnsatisfied` findings the ADR's §4 rule requires. Both subcases (status="stopped"; last_msg_receipt_time = UNIX_EPOCH - 600s against threshold 60_000 ms) produce byte-identical output.


**Code, synthetic names -- findings:**

[ReplicaWalReceiverStale { replica: "db003", claimed_sender: "db001" }, PrimaryQuorumUnsatisfied { primary: "db001", required: 1, observed: 0 }, PrimaryQuorumUnsatisfied { primary: "db002", required: 1, observed: 0 }] -- the two quorum findings collapse to zero if `synchronous_standby_names` is unset, which is the builder default. Emitted exactly once each; `ReplicaWalReceiverStale` cannot fire twice because the :293 guard keys on `primary.ip_address` and a wal_receiver row names one sender.


**Code, synthetic names -- confidence:**

Confidence::Conflicting. Derivation: min(Conflicting from ReplicaWalReceiverStale, BestEffort from PrimaryQuorumUnsatisfied{db001, != true_primary}, Conflicting from PrimaryQuorumUnsatisfied{db002, == true_primary}) with Refuse < Conflicting < BestEffort (split_brain.rs:70-76, :384-390). NOT Refuse -- so `format_refuse` is not taken and the destructive resolution text is printed. The ADR specifies no confidence for this row, so this value is unverifiable against the spec rather than implemented or divergent.


**Code, synthetic names -- SHORT STRING:**

SplitBrain: db002 has quorum (TL=12), demote db001 (TL=11, no live replicas)


**Code, FLEET names -- resolution:**

Identical resolver outcome: `HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }`, true_primary = "dev-pg-app001-db002.sto2.example.com", stale_primaries = ["dev-pg-app001-db001.sto1.example.com"], Confidence = Conflicting. Findings: [ReplicaWalReceiverStale { replica: "dev-pg-app001-db003.sto3.example.com", claimed_sender: "dev-pg-app001-db001.sto1.example.com" }, PrimaryQuorumUnsatisfied { primary: "dev-pg-app001-db001.sto1.example.com", required: 1, observed: 0 }, PrimaryQuorumUnsatisfied { primary: "dev-pg-app001-db002.sto2.example.com", required: 1, observed: 0 }]. C-d is the one row where fleet naming changes NOTHING structurally: the replica-side gate compares IP to IP (`wr.sender_host` "127.1.12.151" vs `primary.ip_address` -- both real values, fixture-confirmed), fails on status/freshness, and `continue`s at :299 before ever reaching the `conn.application_name == replica.node_name` compare at :306. The SSN members are in application-name form (`dev_pg_app001_db002`) and the gated set is empty, so `members INTERSECT gated` = 0 either way. Real threshold is 180_000 ms (wal_sender_timeout="300000") rather than the tests' 60_000 ms -- so an operator's "aged out" window on the fleet is 3x the one every unit test exercises. THE DIFFERENCE THAT MATTERS IS ADJACENT, NOT INTERNAL: because the primary-side gate uses raw equality, fleet-named C-b and C-c (`db003` genuinely, freshly streaming from db001) can never reach `LowerTimelineHasQuorum`. They hit :306, `"dev_pg_app001_db003" != "dev-pg-app001-db003.sto3.example.com"`, fall to :316 `PrimaryDoesNotSeeReplica`, leave `following` empty, and land in the SAME :485 else-branch -> the SAME `HigherTimeline` verdict and the SAME short-string template as C-d, differing only by one finding buried in details_json. On the real fleet, C-d's output shape IS the output shape of every split-brain row.


**Code, FLEET names -- SHORT STRING:**

SplitBrain: dev-pg-app001-db002.sto2.example.com has quorum (TL=12), demote dev-pg-app001-db001.sto1.example.com (TL=11, no live replicas)


**Mismatch:**

Four mismatches, ordered by consequence.

(1) THE ROW'S SAFETY ARGUMENT DOES NOT HOLD; C-d IS C-g ON A TIMER. [self-contradictory / high-value] C-d is literally C-b (ADR:42) after the streaming link drops and the keepalive ages past `freshness_threshold`. Nothing in the row's preconditions excludes "db003 was flushing for db001 past the fork X, then the connection died". If that happened, db001+db003 hold client-acknowledged TL=N writes that db002 lacks, and the tool prints `demote db001`. The ADR itself makes this argument for C-g (ADR:47 and §7: "at scan time the lower-TL primary has no *live* follower ... so the resolver's live-follower logic falls through to `HigherTimeline` and picks the wrong primary ... demoting it destroys the acked writes"), but assigns C-d the opposite, routine treatment. The row's third column, "db001 committing? No (without db003 acking, db001 can't satisfy its quorum)", is a PRESENT-TENSE claim; acked-write divergence is a PAST-TENSE fact. The code makes the same conflation: a single field, `last_msg_receipt_time`, crossing a 180 s threshold flips `true_primary` from db001 to db002 and the short string from `fence db002` to `demote db001` -- a verdict reversal in the data-destroying direction, driven by a clock, on otherwise identical inputs. Crucially, C-g's stated excuse does not apply here: §7 defers detection because "a timeline-wedged replica ... likely has *no* `wal_receiver` at all", but in C-d the `wal_receiver` row IS present and DOES carry `received_tli` and `flushed_lsn` -- the exact fields §7 says would settle it. C-d is a row where the dangerous state is observable with data already captured and the ADR nonetheless marks it safe.

(2) THE SHORT STRING ASSERTS "db002 has quorum" WHILE THE SAME STRUCT CARRIES `PrimaryQuorumUnsatisfied { primary: db002, required: 1, observed: 0 }`. [diverges from §4 item 3; ADR self-contradictory] ADR:230 mandates "`PrimaryQuorumUnsatisfied` MUST appear inline in the short string when present, since it explains why the higher-TL primary lost. Without it the verdict reads as a paradox." In C-d it is present for BOTH primaries, and `format_resolution` (build.rs:692-731) never inspects `info.findings` -- the parenthetical is hardcoded per variant. Worse, the §4 variant table at ADR:169-175 mandates the very template that produces the false clause. So the ADR contradicts itself: item 3 requires the finding inline; the template asserts "{true} has quorum" with no slot for it. The code implements the table. Result: the operator-facing line states the elected primary has quorum when the tool's own evidence says it has zero of one required standby -- and, per §7's 3-node proof, db002 in C-d cannot ack a single write. `Confidence::Conflicting` would have flagged this, but build.rs:655 only branches on `Refuse`, so Conflicting is invisible in `short`.

(3) THE ROW'S FINDINGS LIST IS INCOMPLETE AGAINST §4's OWN DERIVATION RULE. [self-contradictory] Under the ADR's cluster assumption (ADR:31, `ANY 1 (A, B)`), observed=0 < 1 for both primaries in C-d, so §4's rule (ADR:187-192) derives two `PrimaryQuorumUnsatisfied` findings the row does not list. The code emits them (correctly, per the rule). The row is wrong, not the code. This is invisible in the test suite only because `PrimaryHealthBuilder::new()` (src/v2.rs:59-61) does not set `synchronous_standby_names` at all, so `parse("")` -> None -> `continue` at split_brain.rs:650 (ground truth 4) -- the two existing C-d tests never exercise the quorum path.

(4) UNVERIFIABLE / DOC DRIFT. [unverifiable, low] `Confidence::Conflicting` is not derivable from the ADR (no finding->confidence mapping outside §2 and §7; no min() aggregation rule stated). The §4 "cap at ~5 surfaced items" is unimplemented. The short string renders raw node names, while every other display path in build.rs uses `extract_db_number` (build.rs:122, :195, :202, :256, :505) -- so on the fleet the SplitBrain line prints two 36-character FQDNs where the rest of the report prints `db001@sto1`; §4's own examples use the short form.

WHAT IS CORRECTLY IMPLEMENTED: the `HigherTimeline` pick and the single `ReplicaWalReceiverStale(db003, db001)` emission match the row exactly, for both subcases and once only, with the primary-side gate correctly skipped per the "asymmetric, not symmetric AND" precedence rule (ADR:50). Absence of `DivergentReplicaWal` is deferred-correct per §7 ("nothing emits it today"), consistently stated in ADR:7, ADR:222-229 and docs/concepts/split-brain.md:78.

WHAT WOULD SETTLE (1): no captured run of the state exists, so this is reachable-in-code, unverified-on-fleet. The capture that settles it, taken during a real two-primary scan: on db003, `SELECT status, sender_host, received_tli, flushed_lsn, last_msg_receipt_time, now() FROM pg_stat_wal_receiver;` plus `SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), timeline_id FROM pg_control_checkpoint();` and on db002 the TL=N->N+1 `.history` fork LSN X. If `received_tli = N` and `flushed_lsn > X`, C-d carried acknowledged TL=N writes and `demote db001` was the data-destroying instruction. All of these fields except the fork comparison are ALREADY captured by the current scanner (§5 landed, commit 775f90e), so the check is a resolver change, not a capture change.


**Code path:**

1. src/v2/analyze.rs:301 `let primaries: Vec<_> = cluster.primaries().collect();` -> :317 `if primaries.len() > 1` -> :318 `resolve_split_brain(&primaries, &replicas)`. Note: `primaries` is in cluster node order (db001, db002), NOT timeline order -- this fixes the order of the quorum findings later.
2. src/v2/analyze/split_brain.rs:132 `resolve_split_brain` -> :139 `extract_timeline_info` (:184) -> highest_timeline=12 (db002), primaries_with_highest=[db002], primaries_with_lower=[db001].
3. :141 `reference_sysid` (:562) -> both primaries carry the same sysid -> `Some("6968745321024393216")`; :142 `mismatched_sysid_nodes` (:581) -> empty (db003 matches) -> :152 `findings = Vec::new()`; no replica excluded at :146.
4. :160-175 synchronous_commit loop -> value is `on` (synthetic) / `remote_apply` (fleet); neither is in `WEAKENED_SYNCHRONOUS_COMMIT` (:13 `["local", "off", "remote_write", ""]`) -> no `SynchronousCommitWeakened`.
5. :177 `build_replica_following_map` (:245). Outer loop order is highest-TL first then lower-TL (:259-263).
   5a. primary = db002: :266 `threshold_ms = (parse_wal_sender_timeout(..)/2) + 30_000` = 180_000 (fleet, wal_sender_timeout="300000") or 60_000 (synthetic builder default -- key absent, `parse_wal_sender_timeout` :592 returns 60_000). :272 `let Some(wr) = &r_health.wal_receiver` -> present. :281 `wr.sender_host == primary.ip_address.to_string()` -> "127.1.12.151" != "127.2.12.151" -> `replica_passes = false`. :288 taken; :293 guard `if wr.sender_host == primary.ip_address.to_string()` -> FALSE -> NO finding (silent); :299 continue.
   5b. primary = db001: :281 sender_host matches 127.1.12.151, :282 port 5432 ok, then EITHER :283 `matches!(wr.status.as_str(), "streaming" | "catchup")` -> false (subcase "status != streaming/catchup") OR :284-286 `wr.last_msg_receipt_time.is_some_and(|t| (r_health.current_time - t).num_milliseconds() <= threshold_ms)` -> false (subcase "aged out"). Either way `replica_passes = false`. :288 taken; :293 guard TRUE -> :294 push `ReplicaWalReceiverStale { replica: db003, claimed_sender: db001 }`; :299 continue.
   5c. The primary-side corroboration block (:302-315, `conn.application_name == replica.node_name`) is NEVER REACHED in C-d -- the `continue` at :299 short-circuits it. This is why the raw-equality naming bug (ground truth 2) is inert for this row. `following` map is empty.
   EMISSION COUNT: exactly ONE. The outer loop visits every primary, but the :293 guard compares against `primary.ip_address`, and a `wal_receiver` row names exactly one `sender_host`, so at most one candidate primary can match per replica. Both subcases of C-d land on the same guard, as does `sender_port != 5432`. A replica with NO `wal_receiver` (C-f) exits at :272 before the guard and emits nothing -- consistent with the C-f row.
6. :179 `emit_quorum_findings` (:636). :650 `let Some(Quorum { count, members, .. }) = parse(ssn) else { continue }`. With SSN configured (`ANY 1 (db002, db003)` synthetic / `ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )` fleet): `gated` is empty for both primaries -> :663 `observed = 0`; :665 `0 < 1` -> push `PrimaryQuorumUnsatisfied` for db001 then db002 (caller order). With the bare `PrimaryHealthBuilder` default (src/v2.rs:59-61 sets only `archive_mode=on` and `synchronous_commit=on` -- NO `synchronous_standby_names`), `parse("")` returns None (sync_standby_names.rs:26-28) and :650 `continue`s -> ZERO quorum findings. This is exactly what the two existing C-d tests hit.
7. :181 `determine_true_primary` (:352) -> :355 `primaries_with_highest_timeline.len() == 1 && !primaries_with_lower_timeline.is_empty()` -> true -> :357 `resolve_with_different_timelines` (:422). :449 `!replicas_following_stale.is_empty() && replicas_following_highest.is_empty()` -> false (no stale followers). :465 `else if !replicas_following_highest.is_empty()` -> false. :485 else branch -> :493-499 `SplitBrainInfo { true_primary: db002, stale_primaries: [db001], resolution: HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }, confidence: BestEffort, findings: vec![] }`.
8. :381 `split_brain_info.findings.extend_from_slice(findings)` -> :384-390 `.map(determine_confidence_level).min()`. Per :395-419: `ReplicaWalReceiverStale` -> Conflicting (:411-413); `PrimaryQuorumUnsatisfied{db002}` with db002 == true_primary -> Conflicting (:402-408); `PrimaryQuorumUnsatisfied{db001}` -> BestEffort. Enum order (:70-76) is Refuse < Conflicting < BestEffort -> min = `Confidence::Conflicting`.
9. src/v2/writer/build.rs:532 `split_brain_reason(info)` -> :654-664; :655 `matches!(info.confidence, Confidence::Refuse)` -> FALSE -> :692 `format_resolution` -> :712-718 the `HigherTimeline` arm, formatting `info.true_primary` and `stale = info.stale_primaries.first()` RAW (no `extract_db_number`). `Confidence::Conflicting` is never rendered anywhere in the writer (only Refuse is inspected, build.rs:655).


## Row C-f -- "wal_receiver absent, or status=stopped/starting, or blocked on restore_command (archive corruption)" (docs/adr/0

- **Determinate under sections 1-4:** False
- **Matches ADR:** False | **Severity:** high | **Destructive text:** True

**ADR expects (resolution):**

`SplitBrainResolution::HigherTimeline`, true primary db002 (TL=N+1), stale db001 (TL=N). Row text: "db002 | `HigherTimeline`; replica-stuck condition surfaced separately (existing archive-failure mechanism)".


**ADR expects (findings):**

The row lists NO SplitBrainFinding at all -- the findings cell contains only the resolution variant plus the prose "replica-stuck condition surfaced separately (existing archive-failure mechanism)". Implied confidence is therefore BestEffort (nothing in the row maps to Conflicting or Refuse). This conflicts with S4's derivation rule, which mandates PrimaryQuorumUnsatisfied for every primary with observed < count -- i.e. for BOTH db001 and db002 in C-f.


**Determinacy:**

Sections 1-4 determine the *gate* outcome for C-f but not the *verdict* the row asserts.

DETERMINED by S1: db003 fails the replica-side gate in every C-f sub-case, so `replicas_following` is empty for both primaries. That much is derivable.

NOT DETERMINED, three ways:

(a) The true-primary pick is not derivable from the ADR's own operational definition. ADR line 21: "The true primary is the one whose sync quorum is satisfied and that is actively committing." The row's own columns say db003 is not flushing and db001 is not committing; db002 likewise has no acker (ADR line 33: "On db002 (new primary), the same setting is unsatisfiable unless db003 reattaches"). So NEITHER primary meets the definition. The row asserts "db002" anyway. Sections 1-4 contain no rule "when nobody has quorum, fall back to highest TL" -- that rule exists only in the code (split_brain.rs:485-500) and in the S4 rendering table's `HigherTimeline` row, which is a *formatting* table, not a decision rule. Asserted, not derived => unverifiable.

(b) C-f (wal_receiver = None) and C-g are the SAME observable state under the data currently collected, and the ADR assigns them OPPOSITE true primaries. C-g (line 47) describes db003 as "wedged -- it cannot roll forward ... and likely cannot establish a wal_receiver at all", true primary db001, and says picking db002 "destroys acknowledged transactions". C-f's None sub-case is literally "wal_receiver absent", true primary db002, stated with no hedge. Section 7 confirms there is no discriminator: "a timeline-wedged replica (C-g ...) likely has *no* wal_receiver at all ... we have no captured run of the wedged state". Two rows of the same matrix therefore claim different answers for one input, and the ADR defers the tiebreak. Self-contradictory across rows; C-f's confidence is unearned.

(c) The findings column ("none") contradicts S4's own derivation rule. S4 step 4 is unconditional -- "Emit if observed < count" -- and under the cluster assumption `ANY 1 (A, B)` (ADR line 26) C-f has observed=0 < 1 for BOTH primaries. The row lists no findings. C-d has the same defect. C-a lists PrimaryQuorumUnsatisfied(db001) but not for db002, which is consistent there because db003 gates for db002. So C-f/C-d are stale relative to the S4 rule, not consistent with it.

(d) The row's compensating control does not exist -- see mismatch_explanation. That is a factual claim in the row, and it is false, which removes the only stated justification for emitting no finding about db003.


**Code, synthetic names -- resolution:**

`SplitBrainResolution::HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }`; `true_primary: "db002"`, `stale_primaries: ["db002"->no, ["db001"]]` -- precisely: true_primary="db002", stale_primaries=["db001"]. Matches the ADR row's variant and winner.


**Code, synthetic names -- findings:**

With tests_common builder DEFAULTS (no `synchronous_standby_names` key; `synchronous_commit="on"`; shared sysid "6968745321024393216"):

- wal_receiver=None sub-case: `findings: []` (empty vector).
- status="stopped"/"starting", sender_host=db001 IP sub-case: `findings: [ReplicaWalReceiverStale { replica: "db003", claimed_sender: "db001" }]`.

With `synchronous_standby_names = "ANY 1 (db002, db003)"` set explicitly (which is what ADR line 26's cluster assumption actually implies, and what the fleet really has):

- wal_receiver=None: `findings: [PrimaryQuorumUnsatisfied { primary: "db001", required: 1, observed: 0 }, PrimaryQuorumUnsatisfied { primary: "db002", required: 1, observed: 0 }]` (order follows the `primaries` slice order).
- status=stopped: `findings: [ReplicaWalReceiverStale { replica: "db003", claimed_sender: "db001" }, PrimaryQuorumUnsatisfied { primary: "db001", required: 1, observed: 0 }, PrimaryQuorumUnsatisfied { primary: "db002", required: 1, observed: 0 }]`.

This is the single most load-bearing default in the whole exercise: because `PrimaryHealthBuilder::new()` (src/v2.rs:56-80) omits `synchronous_standby_names`, EVERY existing split_brain.rs test that does not call `.with_synchronous_standby_names(...)` exercises the `parse("") -> None -> continue` path at split_brain.rs:650 and therefore sees zero quorum findings and BestEffort confidence. The C-f test in the suite (`gate_silent_when_wal_receiver_missing`, split_brain.rs:1246) is exactly such a test. Its behaviour is unreachable on the real fleet.


**Code, synthetic names -- confidence:**

Builder defaults, wal_receiver=None: `Confidence::BestEffort` (min over an EMPTY findings iterator -> `.unwrap_or(Confidence::BestEffort)` at split_brain.rs:390).
Builder defaults, status=stopped: `Confidence::Conflicting` (ReplicaWalReceiverStale maps to Conflicting, split_brain.rs:411-413).
With SSN set (either sub-case): `Confidence::Conflicting`, driven by `PrimaryQuorumUnsatisfied { primary: "db002" }` where db002 == true_primary (split_brain.rs:404-406).

Confidence::Conflicting has NO operator-visible effect anywhere: `grep Confidence::` shows the only non-test consumer is build.rs:655, which branches on `Refuse` alone. Conflicting reaches the operator only inside `details_json` (SplitBrainInfo is Serialize).


**Code, synthetic names -- SHORT STRING:**

Identical in ALL four synthetic permutations above (None/stopped x SSN-absent/SSN-present), because `format_resolution` hardcodes the parenthetical per variant and never reads `info.findings`:

SplitBrain: db002 has quorum (TL=12), demote db001 (TL=11, no live replicas)

Emitted by src/v2/writer/build.rs:716.


**Code, FLEET names -- resolution:**

`SplitBrainResolution::HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }`, `true_primary: "dev-pg-app001-db002.sto2.example.com"`, `stale_primaries: ["dev-pg-app001-db001.sto1.example.com"]`.

findings (wal_receiver=None sub-case), with the fixture's real config (`synchronous_standby_names = "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"`, `synchronous_commit = "remote_apply"`, `wal_sender_timeout = "300000"`, sysid 6968745321024393216 on all three):
```
[ PrimaryQuorumUnsatisfied { primary: "dev-pg-app001-db001.sto1.example.com", required: 1, observed: 0 },
  PrimaryQuorumUnsatisfied { primary: "dev-pg-app001-db002.sto2.example.com", required: 1, observed: 0 } ]
```
findings (status="stopped"/"starting", sender_host="127.1.12.151"): the same two, preceded by
`ReplicaWalReceiverStale { replica: "dev-pg-app001-db003.sto3.example.com", claimed_sender: "dev-pg-app001-db001.sto1.example.com" }`.

`Confidence::Conflicting` in both sub-cases.

RESOLUTION AND SHORT STRING ARE UNCHANGED vs the synthetic run. The application_name-vs-node_name mismatch (ground truth 2/3) is INERT in C-f, in both places it could have bitten: (a) the primary-side `conn.application_name == replica.node_name` compare is never reached because the replica-side gate short-circuits at split_brain.rs:272/281; (b) the `members ∩ gated` intersection at split_brain.rs:660 is trivially empty because `gated` is empty, so it yields observed=0 -- the same answer the correct comparison would give. C-f is therefore NOT the row that exposes the naming bug (C-a/C-b/C-e are, where a genuinely gated db003 would fail to intersect the app-name-form SSN members and produce a spurious PrimaryQuorumUnsatisfied on a primary that actually has quorum). I flag that as an adjacent finding, not this row's.

What DOES differ synthetic-vs-fleet is confidence and findings, and only because of a test-builder default, not because of naming: builder-default C-f(None) gives `[]` / BestEffort; real fleet C-f(None) gives two PrimaryQuorumUnsatisfied / Conflicting.


**Code, FLEET names -- SHORT STRING:**

SplitBrain: dev-pg-app001-db002.sto2.example.com has quorum (TL=12), demote dev-pg-app001-db001.sto1.example.com (TL=11, no live replicas)

Note: `format_resolution` (build.rs:692-724) interpolates `info.true_primary` and `stale` RAW. Unlike the `Reason::DiskIoErrors`/`FilesystemErrors` short strings (build.rs:625, 641) and unlike the SplitBrain NodeView path (build.rs:198, 202, 250, 256), it does NOT call `extract_db_number()`, so the short string carries full FQDNs while the structured view next to it says "db002@sto2". Cosmetic (ADR S4: "Phrasing is implementation detail"), reported as info only.


**Mismatch:**

The code matches the row on the two things the row gets right (variant `HigherTimeline`, winner db002) and mismatches on everything else the row asserts.

**1. The row's findings column is empty; the code emits two `PrimaryQuorumUnsatisfied` on real fleet config, and S4 mandates them.** ADR S4 derivation step 4 is unconditional ("Emit if `observed < count`"). In C-f neither primary has a gated follower, so observed=0 < 1 for both. The code does exactly that (split_brain.rs:665). The row says the findings are just `HigherTimeline`. Classification: the ROW is stale/self-inconsistent against S4, the CODE is right. Same defect in row C-d.

**2. The emitted short string affirmatively states the opposite of a finding in the same struct.** On fleet config the record is `{ resolution: HigherTimeline, findings: [.., PrimaryQuorumUnsatisfied { primary: db002, required: 1, observed: 0 }], confidence: Conflicting }` and the operator line is `"SplitBrain: dev-pg-app001-db002... has quorum (TL=12), demote dev-pg-app001-db001... (TL=11, no live replicas)"`. "has quorum" is false: the resolver's own finding says db002's quorum is unsatisfied, 0 of 1. This is a direct violation of ADR S4 short-string contract item 3: "**`PrimaryQuorumUnsatisfied` MUST appear inline in the short string** when present". It is present (twice) and appears inline zero times. Cause is ground-truth item 5, confirmed: `format_resolution` hardcodes the parenthetical per variant and never reads `info.findings` (build.rs:692-724). Classification: **diverges**; the ADR is right and the code is wrong. Also note `Confidence::Conflicting` is invisible to the operator -- build.rs:655 branches on `Refuse` only -- so nothing on the rendered line hedges the "demote" instruction.

**3. The row's compensating control does not exist. This is the claim I would flag loudest.** The row says the replica-stuck condition is "surfaced separately (existing archive-failure mechanism)". Verified false on three independent counts:
   - `check_archive` (src/v2/analyze/checks.rs:23-26) begins `let Role::Primary { health } = &primary.role else { return; }` -- it never looks at a replica. It reads `pg_stat_archiver` (`archived_count`/`failed_count`), which measures WAL being *pushed to* the archive by a primary. A replica blocked on `restore_command` is failing to *fetch from* the archive. Different direction, different node role, different catalog. The archive-failure mechanism cannot fire for this condition even in principle.
   - There is no capture of `restore_command` state at all. `grep -rn "restore_command" src/` returns nothing. `ReplicaHealthCheckResult` (src/v2/scan/health_check_replica.rs:20-34) has no recovery-failure or restore field. So the "blocked on restore_command (archive corruption)" sub-case of C-f is not observable by this tool by any path.
   - The mechanism that *would* surface it, `check_streaming` -> `NodeVerdict::NotStreaming` (src/v2/analyze/checks.rs:295-302, fires exactly on `health.wal_receiver.is_none()`), is unreachable in a split-brain cluster: `analyze()` returns early at src/v2/analyze.rs:317-321 (`if primaries.len() > 1 { ... return AnalyzedCluster { cluster, verdict }; }`) before the `for node in cluster.nodes()` loop at src/v2/analyze.rs:342-346 that calls it. In a split-brain cluster the ONLY verdict produced is `ClusterVerdict::SplitBrain`; there are no node verdicts. And the writer drops db003 too -- `HigherTimeline => ReplicasView::None` at build.rs:272-274. Net: in C-f the operator report contains no mention of db003 whatsoever.

**4. The row asserts a verdict that the ADR elsewhere says is the data-destroying one, for an input the tool cannot distinguish.** In the `wal_receiver = None` sub-case, the state C-f describes and the state C-g describes are byte-identical in the resolver's inputs: no wal_receiver on db003, two primaries on N and N+1. C-g (ADR line 47) says the correct answer is db001 and that picking db002 "destroys acknowledged transactions"; S7 confirms the discriminator does not exist ("a timeline-wedged replica (C-g ...) likely has *no* wal_receiver at all", "we have no captured run of the wedged state"). C-f states db002 flatly, with no cross-reference to C-g and no hedge. Classification: **self-contradictory** between rows C-f and C-g; the C-f cell should carry the C-g caveat.

**What is NOT a mismatch, stated explicitly so it is not double-counted:** the application_name/node_name naming bug does not change C-f's outcome in either direction (see code_actual_resolution_fleet_names). And Commit 12's `format_refuse` "sanity gate failed" fallback is never reached in C-f, because C-f never reaches Refuse.

**Evidence that would settle the open part (item 4).** I cannot verify from code whether the fleet ever enters C-g rather than benign C-f, and there is no captured instance. The capture that would settle it, given that ADR S5's new fields have already landed: on a cluster in this state, from db003, `SELECT pg_is_in_recovery(), pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), timeline_id FROM pg_control_checkpoint();` together with `pg_stat_wal_receiver` (expected: zero rows) and the contents of `pg_wal/*.history` from both db001 and db002; then compare db003's `pg_last_wal_receive_lsn()` against the TL N->N+1 switch LSN in db002's history file. Receive LSN strictly greater than the fork LSN while control-file `timeline_id` = N is the C-g signature and the thing that flips the verdict to db001. Until such a capture exists, C-f's confident "db002" is unverifiable, not implemented.


**Code path:**

Entry: `resolve_split_brain(&[&db001,&db002], &[&db003])` -- src/v2/analyze/split_brain.rs:132 `pub(super) fn resolve_split_brain(`.

1. src/v2/analyze/split_brain.rs:177 `extract_timeline_info` -- sorts desc: highest_timeline=N+1, highest_timeline_node=db002, primaries_with_highest_timeline=[(db002,N+1)], primaries_with_lower_timeline=[(db001,N)].
2. split_brain.rs:555 `reference_sysid` -- both primaries share sysid, count 2 >= 2 => Some(sysid). `mismatched_sysid_nodes` => []. filtered_replicas=[db003]. findings=[].
3. split_brain.rs:157-170 synchronous_commit loop -- "on" (synthetic) / "remote_apply" (fleet); neither is in `WEAKENED_SYNCHRONOUS_COMMIT` (split_brain.rs:13) => no `SynchronousCommitWeakened`.
4. split_brain.rs:245 `build_replica_following_map`. Threshold computed at split_brain.rs:265: `(parse_wal_sender_timeout(...)/2) + 30_000` = 60_000 ms synthetic (key absent -> `.unwrap_or(60_000)` at split_brain.rs:612), 180_000 ms fleet (wal_sender_timeout="300000").
   - **wal_receiver=None sub-case (the row's None path):** split_brain.rs:272 `let Some(wr) = &r_health.wal_receiver else {` -> `continue` for BOTH primaries, BEFORE the threshold is ever used and BEFORE any name comparison. Returns (`{}`, `[]`). No finding of any kind. Confirmed by the existing test `gate_silent_when_wal_receiver_missing`, split_brain.rs:1246-1270.
   - **status="stopped"/"starting" sub-case, sender_host=db001's IP:** for primary=db002 split_brain.rs:281 `let replica_passes = wr.sender_host == primary.ip_address.to_string()` is false on the very first conjunct and the stale-guard `if wr.sender_host == primary.ip_address.to_string()` also fails => silent. For primary=db001 the host/port conjuncts pass, `matches!(wr.status.as_str(), "streaming" | "catchup")` fails, stale-guard passes => split_brain.rs:294 pushes `ReplicaWalReceiverStale { replica: db003, claimed_sender: db001 }`. Map still `{}`.
   - Note: the raw `conn.application_name == replica.node_name` compare (split_brain.rs:~305, the naming bug of ground-truth item 2) is NEVER REACHED in C-f, because the replica-side gate short-circuits first. C-f is immune to the naming mismatch on the follower-map side.
5. split_brain.rs:636 `emit_quorum_findings`. split_brain.rs:650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else {` -> `continue`.
   - Synthetic/builder default: `PrimaryHealthBuilder::new()` (src/v2.rs:56-80) seeds configuration with ONLY `archive_mode=on` and `synchronous_commit=on`. No `synchronous_standby_names` => `map_or("")` => `parse("")` returns None (src/v2/analyze/sync_standby_names.rs:26-29) => continue => NO quorum findings.
   - Fleet: `"ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )"` parses to count=1, members=["dev_pg_app001_db002","dev_pg_app001_db003"]. `gated` is empty for both primaries => observed=0 (split_brain.rs:660-663) => `if observed < count` (split_brain.rs:665) fires for BOTH => two `PrimaryQuorumUnsatisfied`. The app-name-vs-node-name mismatch in the `members ∩ gated` intersection is inert here because `gated` is empty.
6. split_brain.rs:352 `determine_true_primary` -> `primaries_with_highest_timeline.len()==1 && !lower.is_empty()` -> split_brain.rs:422 `resolve_with_different_timelines`.
   - `replicas_following_highest` = [] ; loop over lower primaries finds no entry => `replicas_following_stale` = [], `stale_with_followers` = None.
   - Both `if` guards false => split_brain.rs:485 `// No replica evidence - trust timeline` => split_brain.rs:495 `resolution: SplitBrainResolution::HigherTimeline {` with true_primary_timeline=N+1, stale_timeline=N; true_primary=db002; stale_primaries=[db001].
7. split_brain.rs:377-390: findings extended, then `confidence = findings.iter().map(determine_confidence_level).min().unwrap_or(Confidence::BestEffort)`. `Confidence` derives Ord over declaration order Refuse < Conflicting < BestEffort (split_brain.rs:70-76), so min() = worst.
   - split_brain.rs:404-409: `PrimaryQuorumUnsatisfied { primary, .. } => if primary == true_primary { Conflicting } else { BestEffort }`. The db002 entry IS the elected primary => Conflicting.
8. Writer: src/v2/writer/build.rs:528-533 `Reason::SplitBrain => ... split_brain_reason(info)`; build.rs:655 `let short = if matches!(info.confidence, Confidence::Refuse)` -- Conflicting is NOT Refuse, so it falls to build.rs:692 `format_resolution`, arm build.rs:712-719, format string build.rs:716 `"SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)"`. `format_resolution` never inspects `info.findings` (ground-truth item 5, confirmed).
9. Report view: src/v2/writer/build.rs:272-274 `SplitBrainResolution::HigherTimeline { .. } | SplitBrainResolution::Indeterminate => ReplicasView::None` -- db003 does not appear in the replicas view either.


## Row C-g (ADR-002 docs/adr/002-split-brain-resolution-refinement.md:47) -- db003 flushed past the TL=N fork while acking db00

- **Determinate under sections 1-4:** False
- **Matches ADR:** False | **Severity:** critical | **Destructive text:** True

**ADR expects (resolution):**

The row's "True primary (operational)" column says **db001** ("it holds acked writes that exist nowhere else"). The Verdict column concedes the resolver mis-picks `HigherTimeline` -> db002 and states that `DivergentReplicaWal(db003) -> Refuse` "must override the pick". So the ADR's required end state for C-g is: either true_primary=db001, or (as the shippable floor) `Confidence::Refuse` with the resolution-variant text suppressed and the divergence evidence rendered inline (§4 item 4, revised text: `REFUSE/SplitBrain: divergent committed WAL -- db003 flushed past TL=N fork @ <lsn>; acked writes may exist only on lower TL`). §7 and §4 item 1 then defer the emission, the confidence handling and the verdict-flip entirely ("nothing emits it today ... the carve-out is dormant").


**ADR expects (findings):**

`DivergentReplicaWal { replica_node: db003, replica_received_tli, replica_flushed_lsn, fork_tli, fork_lsn }` -> `Confidence::Refuse`. The row names no other finding. §4's derivation rule independently requires `PrimaryQuorumUnsatisfied` for any primary whose gated-follower intersection is below `count`, which in C-g is BOTH primaries (db003 gate-fails for everyone), so the ADR also implies `PrimaryQuorumUnsatisfied{db001,1,0}` and `PrimaryQuorumUnsatisfied{db002,1,0}`.


**Determinacy:**

Split the row in two.

(a) What §1-§4 DO determine, uniquely: db003 has no `wal_receiver`, so the §1 replica-side gate fails for both primaries; the follower map is empty; §4's quorum rule gives observed=0 < count=1 for both primaries; the timeline rule gives the higher-TL node. The code outcome is fully pinned and deterministic -- `HigherTimeline` -> db002, two `PrimaryQuorumUnsatisfied` findings, `Confidence::Conflicting`.

(b) What the row ASSERTS but §1-§4 do NOT derive: "True primary = db001". No rule in §1-§4 can name db001 -- naming it requires knowing db003 acked past the fork earlier, which is precisely the §7 machinery the ADR defers ("Defer the `DivergentReplicaWal` emission, its confidence mapping, and the verdict-flip until designed from a captured occurrence"). Strictly: the C-g "true primary" cell and the "must override" clause are **unverifiable**, not implemented and not implementable under sections 1-4 as written.

(c) §4 is additionally self-contradictory about C-g's short string, so even the operator text is underdetermined:
  - §4 item 3: "`PrimaryQuorumUnsatisfied` MUST appear inline in the short string when present". In C-g it is present -- twice, including for the ELECTED primary db002.
  - §4 variant table: `HigherTimeline` -> `SplitBrain: {true} has quorum (TL={hi}), demote {stale} (TL={lo}, no live replicas)` -- a fixed template that asserts the true primary "has quorum" and cannot render a `PrimaryQuorumUnsatisfied` naming that same true primary.
  These two mandates cannot both be satisfied for C-g. `HigherTimeline` fires exactly when nobody is gate-following anyone, so under `ANY 1 (...)` the elected primary provably does NOT have quorum -- the template's own words are false in every case that can reach it.
  - §4 item 4 ("`DivergentReplicaWal` ... MUST surface inline", with a concrete evidence template) vs the Findings-concatenation bullet ("`DivergentReplicaWal` rendering is deferred (§7); nothing emits it today") is a second self-contradiction inside §4.


**Code, synthetic names -- resolution:**

`SplitBrainResolution::HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }`, `true_primary: "db002"`, `stale_primaries: ["db001"]`.

Input constructed with the tests_common builders (src/v2.rs:39-215, 235-350):
  db001 = NodeBuilder::new("db001").with_id(1).with_primary(PrimaryHealthBuilder::new().with_timeline(11).build())
  db002 = ... .with_timeline(12) ...
  db003 = NodeBuilder::new("db003").with_id(3).with_replica(ReplicaHealthBuilder::new().with_timeline(11).without_wal_receiver().build())
This is byte-for-byte the shape of the existing test `gate_silent_when_wal_receiver_missing` (src/v2/analyze/split_brain.rs:1246), which already asserts `info.true_primary == "db002"`. C-g is therefore already covered by a passing test -- under a name that hides what it means.

Builder defaults that matter: `PrimaryHealthBuilder::new()` seeds ONLY `archive_mode=on` and `synchronous_commit=on` (src/v2.rs:59-61) -- there is NO default `synchronous_standby_names`. `ReplicaHealthBuilder::new()` defaults `sender_host=127.1.12.151`, `sender_port=5432`, `status=streaming`, `last_msg_receipt_time=Some(now)`, `has_wal_receiver=true`, and `last_wal_replay_lsn/last_wal_receive_lsn = None` with NO builder setters for either (src/v2.rs:245-246, 264-265) -- the fields added by commit 775f90e are unsettable from tests.


**Code, synthetic names -- findings:**

Two sub-runs, because the builder default decides it:

(a) Pure builder defaults (no `synchronous_standby_names` key): `findings: []`. `emit_quorum_findings` (split_brain.rs:636) reads `configuration.get("synchronous_standby_names").map_or("", ...)` -> `""` -> `parse("")` returns None (sync_standby_names.rs:26-28) -> `let Some(Quorum{..}) = ... else { continue }` (split_brain.rs:650) -> nothing emitted for either primary.

(b) Synthetic names WITH the ADR's SSN set (`.with_synchronous_standby_names("ANY 1 (db002, db003)")` on both primaries), in emission order:
  1. `PrimaryQuorumUnsatisfied { primary: "db001", required: 1, observed: 0 }`
  2. `PrimaryQuorumUnsatisfied { primary: "db002", required: 1, observed: 0 }`
  (order = the `primaries` slice order from `cluster.primaries()`, i.e. node order in the cluster.)

In BOTH sub-runs there is no `ReplicaWalReceiverStale` (the `wal_receiver: None` early-continue at split_brain.rs:272 precedes every finding site), no `PrimaryDoesNotSeeReplica` (primary side never consulted), no `BidirectionalFlushingConfirmed`, no `SystemIdentifierMismatch` (all three nodes share the builder default sysid 6968745321024393216), no `SynchronousCommitWeakened` (`on` is not in WEAKENED_SYNCHRONOUS_COMMIT, split_brain.rs:13), and -- the point of the row -- no `DivergentReplicaWal` (grep: the variant is constructed nowhere outside the enum definition at split_brain.rs:102 and a test at :782).

Sub-variant worth recording: if the wedged db003 does expose a `wal_receiver` in `status="starting"` pointing at db002's IP (the ADR only says "likely cannot establish a wal_receiver at all"), the gate fails at split_brain.rs:281 and the sender_host guard at :293 fires, adding `ReplicaWalReceiverStale { replica: db003, claimed_sender: db002 }` -> Conflicting. Resolution, true_primary and short string are unchanged.


**Code, synthetic names -- confidence:**

(a) builder defaults: `Confidence::BestEffort` -- findings is empty, so `.min()` on the empty iterator hits `.unwrap_or(Confidence::BestEffort)` (split_brain.rs:384-389). The tool asserts BestEffort confidence in a data-destroying instruction.
(b) with SSN set: `Confidence::Conflicting` -- `min(BestEffort, Conflicting)` over `determine_confidence_level` (split_brain.rs:395): `PrimaryQuorumUnsatisfied{db001}` != true_primary -> BestEffort (:408); `PrimaryQuorumUnsatisfied{db002}` == true_primary -> Conflicting (:406). Ord is Refuse < Conflicting < BestEffort (declaration order, split_brain.rs:71-78).

Never `Refuse`. Note the regression: before commit 87e51e5 ("fix(analyze): stop quorum-unsatisfied findings from refusing own resolution") `PrimaryQuorumUnsatisfied` mapped unconditionally to `Refuse`, so sub-run (b) would have produced `Confidence::Refuse` and the short string `REFUSE/SplitBrain: sanity gate failed` -- ugly, but NOT an instruction to demote db001. That commit's reasoning is sound in isolation ("a quorum-blocked primary cannot have ack'd writes" -- true, NOW) but it removed the only thing that was accidentally holding C-g back from printing a destructive action. The commit did not consider C-g, where the danger is writes acked EARLIER on the other node.


**Code, synthetic names -- SHORT STRING:**

SplitBrain: db002 has quorum (TL=12), demote db001 (TL=11, no live replicas)

Identical in both sub-runs (a) and (b): `split_brain_reason` (writer/build.rs:654) branches only on `confidence == Refuse` (:655); Conflicting and BestEffort both fall through to `format_resolution` (:692) -> `HigherTimeline` arm (:712-718). `format_resolution` never inspects `info.findings`, so the `PrimaryQuorumUnsatisfied{db002, required:1, observed:0}` sitting in the same struct is contradicted by the words "db002 has quorum" in the very same sentence. That finding is visible only in `details_json` (serde_json of the whole `SplitBrainInfo`, :660).


**Code, FLEET names -- resolution:**

Identical structurally: `SplitBrainResolution::HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }`, `true_primary: "dev-pg-app001-db002.sto2.example.com"`, `stale_primaries: ["dev-pg-app001-db001.sto1.example.com"]`.

Findings (fleet SSN `ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )` from tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json parses fine: count=1, members in app-name form):
  1. `PrimaryQuorumUnsatisfied { primary: "dev-pg-app001-db001.sto1.example.com", required: 1, observed: 0 }`
  2. `PrimaryQuorumUnsatisfied { primary: "dev-pg-app001-db002.sto2.example.com", required: 1, observed: 0 }`
Confidence: `Conflicting`.

Caveat, stated as an evidence gap rather than a fact: db002's post-promotion `synchronous_standby_names` has never been captured (the fixture's db002 is a replica, and HEALTH_CHECK_REPLICA_QUERY does not collect SSN -- health_check_replica.rs:103-110). If repmgr leaves it empty on the promoted node, finding 2 disappears and confidence rises to `BestEffort`. The short string is byte-identical either way.

C-g is the one row where the raw-equality naming bug (`conn.application_name == replica.node_name`, split_brain.rs:307) does NOT change the answer: the replica-side gate short-circuits at :272 before the primary side is ever consulted. But the bug is why C-g's OUTPUT is reachable on the fleet regardless of whether the C-g STATE ever occurs -- see mismatch_explanation.


**Code, FLEET names -- SHORT STRING:**

SplitBrain: dev-pg-app001-db002.sto2.example.com has quorum (TL=12), demote dev-pg-app001-db001.sto1.example.com (TL=11, no live replicas)

(`format_resolution` interpolates `info.true_primary` and `stale_primaries[0]` raw -- unlike the DiskIoErrors/FilesystemErrors/ChainedReplica arms and `PrimaryView`, it does not call `extract_db_number` (writer/build.rs:408). Minor, separate, low severity: the split-brain short string is the only FQDN-verbose reason line in the writer.)


**Mismatch:**

WHAT THE TOOL OUTPUTS TODAY FOR C-g, plainly: `SplitBrain: <db002> has quorum (TL=12), demote <db001> (TL=11, no live replicas)`, at Conflicting or BestEffort confidence, logged via `tracing::error!(... "SPLIT BRAIN DETECTED")` (writer/build.rs:295-310). db001 is the node the ADR says "holds acked writes that exist nowhere else". Yes -- it is an instruction to destroy acknowledged writes, in the imperative, naming the node.

1. HALF-MATCH ON THE DESCRIPTIVE CLAUSE. The row's "Resolver mis-picks `HigherTimeline` -> db002" is exactly what the code does (verified by hand-execution and by the existing passing test at split_brain.rs:1246). The row's required end state (true_primary=db001, or Refuse) is absent. The deferral itself is stated consistently in §7, §4 item 1, the plan (docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:21-49) and the code comment, so the DEFERRAL is deferred-correct -- but the row's own text ("must override the pick") omits the word "deferred" and reads as a shipped mitigation.

2. THE ADR DEFEATS ITS OWN SAFETY RATIONALE. §4 item 4 defers the rebuild string because "Emitting a rebuild direction before that verdict-flip exists would print a backwards, data-destroying instruction." The §4 variant table then mandates, for the very same row, `demote {stale}` -- a backwards, data-destroying instruction, delivered through the resolution text instead of the rebuild text. Suppressing the rebuild verb while keeping "demote db001" buys nothing. This is the headline defect and it is a spec defect as much as a code defect: `format_resolution` implements the mandated template faithfully.

3. §4 item 3 IS VIOLATED, ON THIS ROW SPECIFICALLY. `PrimaryQuorumUnsatisfied{db002,1,0}` is present and MUST appear inline; `format_resolution` (build.rs:692-727) hardcodes the parenthetical per variant and never reads `info.findings`, so a `PrimaryQuorumUnsatisfied` naming the TRUE primary is structurally unrenderable. The operator sees "db002 has quorum" while the JSON says observed=0. If the mandate were honoured the line would read something like "db002 quorum 0/1" and no competent operator would demote db001 on it.

4. FLEET BLAST RADIUS -- keep separate from "C-g happened". Reachable in code: certain. Reachable on the fleet AS ROW C-g: unknown; there is no captured instance of the wedged state (the ADR says so, and I found none under tests/fixtures/). BUT because of the raw-equality naming bug (split_brain.rs:307: `conn.application_name == replica.node_name`, `dev_pg_app001_db003` vs `dev-pg-app001-db003.sto3.example.com`), NO replica on this fleet can ever pass the primary side of the gate, so `replicas_following` is always empty and EVERY fleet split-brain -- C-a, C-b, C-c, C-e included -- lands on the same `HigherTimeline` branch and emits the same "demote the lower-TL node" sentence. On the real fleet, C-b/C-c (where the ADR says db001 IS the true primary, provably, with a live acker) produce C-g's output verbatim, differing only by an extra `PrimaryDoesNotSeeReplica` finding buried in details_json. The C-g text is therefore reachable on the fleet today without the C-g state ever occurring. Settling query, per node: `SELECT application_name FROM pg_stat_replication;` on the primary vs the replica's `node_name` in the inventory -- the fixture already shows they differ.

5. DETECTABILITY CLAIM IN THE ROW, ASSESSED AGAINST THE NEW LSN FIELDS. The row asserts "a wedged replica with no `wal_receiver` exposes no `received_tli`/`flushed_lsn`, so the §5-as-original data cannot prove 'past fork'". That row text is now STALE with respect to the 2026-09-10 validation in §5: `pg_last_wal_receive_lsn()` is read from shared memory and survives walreceiver death within one postmaster ("Some(_) is a high-water mark, not a live position", health_check_replica.rs:31-33), and it is the flush-synced position -- the ack-relevant one. So a wedged-but-not-restarted db003 DOES expose a usable "how far did I flush" number, and the row's flat "cannot prove" no longer holds. Three things still block a decision, and I classify the remaining gap unverifiable, not closed:
   (a) No branch attribution. The captured LSN carries no timeline. The only TL for the replica is `pg_control_checkpoint().timeline_id`; on a replica re-pointed at db002 with `recovery_target_timeline=latest` that may already read N+1 while the high-water LSN is a TL=N position. Nothing in the captured data ties the number to a branch. Settling capture: on a real wedged replica, `SELECT timeline_id FROM pg_control_checkpoint();`, `SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();`, `SELECT * FROM pg_stat_wal_receiver;`, plus `ls pg_wal/*.history` and the contents of the highest-numbered history file -- captured together, once.
   (b) Restart erases it. §5's own Terminology note says both LSNs "are zeroed by a postmaster restart and do not survive one". Restarting a wedged replica is a plausible first operator move, so the field is likeliest to be NULL exactly after someone has poked the node. This directly contradicts §7's decision sentence, "Collect db003's timeline and applied LSN from the **control file**", and the plan's title "Capture replica control-file position" -- neither LSN comes from the control file. Self-contradictory within the ADR; anyone designing the follow-up detection off §7's wording will assume a restart-durability the data does not have.
   (c) Nothing can compare them yet. `_fork_lsn_for` (health_check_primary/timeline_history.rs:41) is underscore-dead-code with no caller; the higher-TL primary's `.history` is captured but never parsed in anger. A usable LSN->u64 comparator already exists at src/v2/analyze/checks.rs:341 (`parse_lsn`), so the plan's "no `lsn_to_u64`" framing understates what is already available -- the missing piece is wiring, not arithmetic.
   Minor evidence nit: §5's validation cites "PG17 `xlogfuncs.c`" while the fleet is PG 15.14 on all three nodes. The `if (recptr == 0) PG_RETURN_NULL();` guard is the same in 15, so the conclusion stands, but the citation is from the wrong major version for this fleet.

6. LATENT TRAP FOR THE FOLLOW-UP. `determine_confidence_level` maps `DivergentReplicaWal` -> `Refuse` (split_brain.rs:399) but `format_refuse` maps it to `None` (build.rs:682) and falls back to the literal "sanity gate failed" (:684). The day someone wires up emission without touching the writer, C-g's operator line becomes `REFUSE/SplitBrain: sanity gate failed` -- no replica named, no LSN, no fork -- violating §4 item 4's explicit evidence template. Safer than today's text, but it discards exactly the evidence §7's capture-first decision was taken to obtain.


**Code path:**

1. src/v2/analyze.rs:317 `if primaries.len() > 1 {` -> :318 `resolve_split_brain(&primaries, &replicas)` (note: no scan-start timestamp parameter, per ground truth 6).
2. split_brain.rs:141 `extract_timeline_info` -> :189-212: sorts desc -> highest_timeline=12, highest_timeline_node=db002, primaries_with_highest_timeline=[(db002,12)], primaries_with_lower_timeline=[(db001,11)].
3. split_brain.rs:143 `reference_sysid` -> :566-580: both primaries report 6968745321024393216, count 2 >= 2 -> Some(sysid). :144 `mismatched_sysid_nodes` -> :583-607: db003 shares it -> `[]`. :146-150 filtered_replicas=[db003]. :152 `findings = Vec::new()`.
4. split_brain.rs:160-175 synchronous_commit loop: fleet value `remote_apply` / builder default `on`; `WEAKENED_SYNCHRONOUS_COMMIT` (:13) = ["local","off","remote_write",""] -> no `SynchronousCommitWeakened`.
5. split_brain.rs:177-178 `build_replica_following_map` -> :245. Outer loop chains highest-then-lower (:255-259): db002, then db001. Inner loop, replica db003: :272 `let Some(wr) = &r_health.wal_receiver else { continue; };` -> `wal_receiver` is None (C-g: wedged, no receiver) -> `continue` for BOTH primaries. Returns (empty map, empty findings). The freshness gate at :281-289, the stale-flag guard at :293 and the primary-side `conn.application_name == replica.node_name` compare at :307 are NEVER REACHED on this row.
   (Replica-side data path: HEALTH_CHECK_REPLICA_QUERY's `wal_receiver` scalar subquery over an empty `pg_stat_wal_receiver` yields SQL NULL -> `Option<WalReceiverInfo>` = None, health_check_replica.rs:64-85.)
6. split_brain.rs:180 `emit_quorum_findings` -> :636. Per primary: :645 read `synchronous_standby_names`; :650 `let Some(Quorum { count, members, .. }) = parse(ssn) else { continue };`
   - builder default (key absent) -> `""` -> sync_standby_names.rs:26-28 returns None -> continue -> NO findings.
   - SSN present -> sync_standby_names.rs:32 strips "ANY ", :49-51 splits count/members -> count=1, members len 2. :656-660 `gated` = map lookup = `[]` -> :661-664 `observed = 0`. :665 `0 < 1` -> push `PrimaryQuorumUnsatisfied` for db001, then db002.
7. split_brain.rs:182 `determine_true_primary` -> :352. :357-359 `primaries_with_highest_timeline.len()==1 && !lower.is_empty()` -> true -> :360 `resolve_with_different_timelines` -> :422.
   :430-433 `replicas_following_highest` = [] ; :439-447 loop finds no followers for db001 -> `replicas_following_stale` = [] ; :449 first arm false (LowerTimelineHasQuorum NOT taken) ; :465 second arm false (Both NOT taken) ; :484 else -> :492-500 `HigherTimeline { true_primary_timeline: 12, stale_timeline: 11 }`, true_primary=db002, stale_primaries=[db001], findings=vec![].
8. split_brain.rs:381 `findings.extend_from_slice` -> the two quorum findings (or none). :383-389 confidence = min over `determine_confidence_level` (:395): db001 finding -> :408 BestEffort; db002 finding -> :405-406 Conflicting (primary == true_primary). min = Conflicting; empty case -> :389 `unwrap_or(BestEffort)`.
9. writer/build.rs:279 `build_reason_view` -> :486 `format_reason` -> :528-533 `Reason::SplitBrain` arm -> :654 `split_brain_reason`. :655 `matches!(info.confidence, Confidence::Refuse)` is FALSE (Conflicting/BestEffort) -> :658 `format_resolution` -> :692. :693 `stale = stale_primaries.first()` = db001. :712-718 `HigherTimeline` arm -> `format!("SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)", ...)`. :660 details_json = full serde of `SplitBrainInfo` (this is the only place the contradicting `PrimaryQuorumUnsatisfied{db002,1,0}` surfaces).
10. Operator surfaces: `ReasonView.short` in the report (build.rs:279-284) and `tracing::error!(reason = %reason_str, ... "SPLIT BRAIN DETECTED")` (build.rs:295-310).


---

# Appendix C -- postgres behaviour claims (raw)


## ADR-002 §5 "(Validated 2026-09-10.)" block: pg_last_wal_replay_lsn() / pg_last_wal_receive_lsn() nullity, staleness and promotion behaviour on the dep

### [high | partially-correct | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:221 (with :7, :247 and docs/concepts/split-brain.md:87)
- **Claim:** "The control-file `timeline_id` (already captured) plus an absolute applied LSN let us place the replica relative to a primary's fork LSN even with no live receiver."
- **Postgres evidence:** Reasoning from memory of the PostgreSQL source; no PG source tree, no psql, no pg_config exists in this environment (verified: `which psql pg_config postgres` empty, /usr/include/postgresql and /usr/share/postgresql absent). PG15 src/backend/access/transam/xlogfuncs.c pg_last_wal_replay_lsn() -> GetXLogReplayRecPtr(NULL), which in PG15 lives in the (new-in-15) src/backend/access/transam/xlogrecovery.c and reads XLogRecoveryCtl->lastReplayedEndRecPtr. That value is an LSN with NO timeline attached. The timeline it belongs to is XLogRecoveryCtl->lastReplayedTLI, which PG15 exposes through NO SQL function. pg_control_checkpoint().timeline_id is checkPointCopy.ThisTimeLineID -- the TLI as of the last checkpoint/restartpoint, updated on a completely different cadence (restartpoint interval, minutes to hours) from lastReplayedEndRecPtr (per record). So the pair the ADR proposes is not a coherent (TLI, LSN) point: an LSN from now, a TLI from the last restartpoint. On a replica that has replayed a TL-switch record but not yet completed a restartpoint, the two disagree, and an LSN compared against the wrong branch's fork point is meaningless.
- **PG15 vs PG17:** Same on both. PG17 also has no SQL-visible last-replayed TLI; pg_control_recovery() exists identically in 15 and 17.
- **Query to settle:** `SELECT (pg_control_checkpoint()).timeline_id AS ckpt_tli, (pg_control_recovery()).min_recovery_end_lsn, (pg_control_recovery()).min_recovery_end_timeline, pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn(), pg_is_in_recovery();  -- run on a 15.14 standby immediately after a TL switch, before the next restartpoint, and confirm ckpt_tli lags min_recovery_end_timeline`
- **Impact if wrong:** The whole capture-first decision (§7 line 247) rests on this pair being sufficient to diagnose the next real C-g. If it is not, the next wedged-replica incident is captured but still not diagnosable, and any future DivergentReplicaWal trigger built on it compares an LSN against a fork point on a branch the node may not be on -- which is the exact input to a verdict-flip that decides whether db001 or db002 gets demoted. ADDITIONAL FIELD NEEDED: pg_control_recovery().min_recovery_end_lsn and min_recovery_end_timeline. minRecoveryPoint/minRecoveryPointTLI is the one durable, timeline-QUALIFIED replay position in the control file; it survives a postmaster restart (unlike both shared-memory LSNs), and it is a conservative lower bound so 'min_recovery_end_lsn > fork_lsn on min_recovery_end_timeline == N' is genuine proof of replay past the fork. Note it is reset to 0/0 once recovery completes, so it is meaningful exactly on the node that matters (the still-in-recovery wedged replica). Adding it is a one-line change to a query that already calls pg_control_checkpoint() and pg_control_system(), so the pg_read_server_files/privilege story is already solved. Capturing pg_control_checkpoint().prev_timeline_id and redo_lsn as well is nearly free.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:88 ('last_wal_replay_lsn', pg_last_wal_replay_lsn()::text) paired with :64 ('timeline_id', (SELECT timeline_id FROM pg_control_checkpoint()))

### [high | annotation-does-not-support-claim | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:211 vs :221
- **Claim:** L211: "`pg_last_wal_receive_lsn()` -- received/flushed position (**the ack-relevant one** under `synchronous_commit=on`)" vs L221: "The control-file `timeline_id` ... plus **an absolute applied LSN** let us place the replica relative to a primary's fork LSN"
- **Postgres evidence:** From memory of PG semantics (no server available to check here): under synchronous_commit = on / remote_flush the primary releases the client when the standby reports FLUSH of the commit record (pg_stat_replication.flush_lsn / the standby's flushedUpto). Replay is not waited for. So on those settings the applied LSN is strictly <= the ack-relevant position and can lag it by an unbounded amount (replay conflict, recovery_min_apply_delay, a slow redo). §7 defines the danger criterion as 'flushed_lsn is past the fork' = proof of acked writes; substituting the applied LSN under-reports it. The fleet's captured configuration is synchronous_commit = remote_apply (tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json), under which the ack DOES wait for replay and the applied LSN happens to be ack-relevant -- but ADR §2 explicitly admits `on` and `remote_flush` as valid non-Refuse values, so the resolver must not assume remote_apply.
- **PG15 vs PG17:** No difference; synchronous_commit levels are identical in 15 and 17.
- **Query to settle:** `SELECT name, setting FROM pg_settings WHERE name='synchronous_commit';  -- on every candidate primary; then compare pg_last_wal_receive_lsn() and pg_last_wal_replay_lsn() on a standby under recovery_min_apply_delay='60s' to see the gap the rationale ignores`
- **Impact if wrong:** A future DivergentReplicaWal trigger written to §5's rationale would compare the APPLIED LSN against the fork and conclude 'not past the fork' on a replica that has flushed (and therefore acked) past it. That is a false negative on a committed-write-divergence safety gate: the resolver keeps its HigherTimeline pick, the operator demotes db001, and acknowledged transactions are destroyed. The error is in the unsafe direction. Fix: state that the fork comparison uses last_wal_receive_lsn under `on`/`remote_flush`, and that only under remote_apply does the applied LSN suffice.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:88-89 -- both are captured, so the data is there; the defect is that §5's rationale names the wrong one as the fork-comparison input.

### [high | refuted | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:215
- **Claim:** "Every node in this fleet is built as a standby or promoted from one, so NULL is close to unobservable in practice -- confirmed on a promoted primary, where `pg_last_wal_receive_lsn() IS NULL` returns `f`."
- **Postgres evidence:** From memory of PG15 src/backend/replication/walreceiverfuncs.c. WalRcvShmemInit() zeroes WalRcvData once, at postmaster start. flushedUpto is first written by RequestXLogStreaming(), which the STARTUP process calls only when the recovery state machine actually decides to try XLOG_FROM_STREAM. Therefore pg_last_wal_receive_lsn() returns NULL for the entire window between postmaster start and the first streaming request -- and forever if streaming is never requested. Concretely NULL is reachable on: (a) a standby doing archive-only recovery (restore_command, no primary_conninfo); (b) matrix row C-f, 'blocked on restore_command (archive corruption)' -- the startup process loops in the archive source and may never reach the stream source; (c) any standby in the first seconds after restart; (d) a node restarted by `repmgr standby follow` / `repmgr node rejoin` before its walreceiver is requested. The ADR's supporting measurement ('IS NULL returns f' on a promoted primary) is evidence about a node that streamed continuously for its whole postmaster life -- it says nothing about the states the capture is for.
- **PG15 vs PG17:** Identical. RequestXLogStreaming()'s initialisation of flushedUpto is unchanged between 15 and 17.
- **Query to settle:** `On a 15.14 standby: stop it, remove primary_conninfo (leave only restore_command + standby.signal), start it, then `SELECT pg_last_wal_receive_lsn() IS NULL, pg_last_wal_replay_lsn(), pg_is_in_recovery();` -- expect receive IS NULL = t while pg_is_in_recovery() = t.`
- **Impact if wrong:** The section concludes the hazard is 'staleness, not nullity' and therefore that Option<String> needs no further thought. But in the exact state the capture exists to diagnose -- a wedged replica, matrix C-g, quite likely restarted by the follow attempt that wedged it -- None is a live possibility, and None is indistinguishable from 'we captured nothing'. Anyone designing the §7 trigger off this paragraph will treat None as 'no divergence' rather than 'no evidence', which is the unsafe reading.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:31-33 doc comment "Received position. Survives walreceiver death and promotion within one postmaster, so `Some(_)` is a high-water mark, not a live position." -- the comment documents only the Some(_) hazard and not the None-in-the-target-state case.

### [high | partially-correct | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:215
- **Claim:** "A non-NULL value is a **high-water mark** from some earlier point in the postmaster's life, **not** evidence of a live receiver or of where the node is now." (and L215 "`receivedUpto` survives intact from the node's standby days")
- **Postgres evidence:** From memory of PG15 src/backend/replication/walreceiverfuncs.c RequestXLogStreaming(): it first rounds the start pointer DOWN to a segment boundary (`if (XLogSegmentOffset(recptr, wal_segment_size) != 0) recptr -= XLogSegmentOffset(...)`), and then, guarded by `if (walrcv->receiveStart == 0 || walrcv->receivedTLI != tli)`, does `walrcv->flushedUpto = recptr; walrcv->receivedTLI = tli; walrcv->latestChunkStart = recptr;`. So flushedUpto is OVERWRITTEN -- possibly backwards, to a segment boundary -- on the first streaming request of a postmaster's life AND on every streaming request whose timeline differs from receivedTLI. It is not a monotonic high-water mark; it is 'the last value some walreceiver start or receive set'. The replay side IS monotonic within one postmaster (lastReplayedEndRecPtr only advances), so the ADR's 'high-water mark' wording is right for replay and wrong for receive. Separately: the field has been named flushedUpto, not receivedUpto, since PG13 (the same commit that renamed GetWalRcvWriteRecPtr -> GetWalRcvFlushRecPtr); 'receivedUpto' is PG12-and-earlier vocabulary, which matters in a paragraph whose whole authority comes from source-level precision.
- **PG15 vs PG17:** Identical logic in 15 and 17; the flushedUpto rename predates both (PG13).
- **Query to settle:** `On a test 15.14 cluster: record `SELECT pg_last_wal_receive_lsn()` on a standby, then re-point it at a freshly promoted node on a new TL and re-read the same value plus `SELECT received_tli, flushed_lsn FROM pg_stat_wal_receiver;` -- confirm whether the receive LSN moves backwards to a segment boundary.`
- **Impact if wrong:** The C-g diagnosis is 'did db003 flush past the TL=N fork?'. If db003 was re-pointed and its startup process requested streaming on TL=N+1 even once, flushedUpto was reset to the segment-aligned requested start, which is at or below its replay point and BELOW the true TL=N high-water mark. The captured value then says 'not past the fork' when the node genuinely flushed (and acked) past it -- false negative on the safety gate, unsafe direction. Whether the wedged replica's startup process actually requests TL=N+1 (vs. refusing the switch in rescanLatestTimeLine and staying on TL=N, in which case receivedTLI==tli and no reset happens) is precisely what no captured run tells us.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:31-33 "so `Some(_)` is a high-water mark, not a live position" -- the doc comment asserts monotonicity that the shared-memory field does not have.

### [high | annotation-does-not-support-claim | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:219 vs :7, :247 and docs/concepts/split-brain.md:87
- **Claim:** L219: "both LSNs are read from shared memory -- they are zeroed by a postmaster restart and do not survive one." vs L7: "detecting the dangerous case reliably needs **control-file evidence we do not yet capture**" and L247: "Collect db003's timeline and applied LSN **from the control file**, independent of `wal_receiver` (§5)" and concepts:87: "reading the replica's position from the **control file** (`pg_control_checkpoint().timeline_id`, `pg_last_wal_replay_lsn()`), which survives with no receiver"
- **Postgres evidence:** L219's factual content is right (both functions read shared memory, not pg_control) and I concur from memory of the PG15 source. The problem is that L219 is a one-line correction that silently invalidates the decision it is embedded in and was not propagated: §7 (L247) still calls the added LSNs 'from the control file', and docs/concepts/split-brain.md:87 goes further and lists pg_last_wal_replay_lsn() as a control-file read that 'survives with no receiver' -- then in the same sentence says the positions 'persist in shared memory for the life of the postmaster', contradicting itself inside one sentence. The 2026-06-07 revision (L7) states the capture requirement as CONTROL-FILE evidence; §5 as implemented adds two shared-memory reads. By L219's own correction, the control-file evidence L7 asked for is still not captured.
- **PG15 vs PG17:** Version-independent; this is a documentation-consistency defect.
- **Query to settle:** `SELECT * FROM pg_control_recovery();  -- run once on a 15.14 standby to confirm min_recovery_end_lsn/min_recovery_end_timeline are populated and the scanner role can execute it`
- **Impact if wrong:** The ADR now reads as if the capture gap is closed ('capture-first: done'), so nobody adds the durable control-file position. When the next C-g happens on a node that restarted, both captured LSNs are zero-or-reseeded and the incident is still undiagnosable -- the exact outcome the 2026-06-07 revision was written to prevent. Minimum fix: add pg_control_recovery() to HEALTH_CHECK_REPLICA_QUERY, and correct L247 and concepts:87 to match L219.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:88-89 -- the two additions are shared-memory reads; the only control-file additions in the same commit series are timeline_id (:64, pre-existing) and system_identifier (:65).

### [high | unverifiable-offline | confidence=medium] ADR lines docs/adr/002-split-brain-resolution-refinement.md:215
- **Claim:** "Promotion terminates the walreceiver *without* restarting the postmaster, so `receivedUpto` survives intact from the node's standby days." -- and the implicit fleet claim that this is how promotion happens here
- **Postgres evidence:** The PostgreSQL half is confirmed from memory: promotion is handled entirely inside the running postmaster. The startup process notices the promote signal, calls XLogShutdownWalRcv() -> ShutdownWalRcv(); the walreceiver exits through WalRcvDie(), which clears walrcv->pid, walrcv->latch and sets walRcvState = WALRCV_STOPPED but does NOT touch flushedUpto/receivedTLI; the postmaster transitions PM_RECOVERY -> PM_RUN. No fork of a new postmaster, no shmem re-init. pg_ctl promote, pg_promote() and a promote_trigger_file are the same code path. The 'fast vs fallback' promotion distinction (fast = write an end-of-recovery record; fallback = full checkpoint) is about the checkpoint, not the postmaster, and pg_ctl promote has no -m mode at all. The FLEET half is unverifiable here: there is no repmgr.conf, no ansible/inventory and no runbook anywhere in this repo (grep for repmgr across docs/ returns only prose mentions in ADR-002 and the plan). repmgr standby promote by default shells out to `pg_ctl promote` and then polls pg_is_in_recovery(), but it uses `service_promote_command` instead when that is set in repmgr.conf, and a site that set that to a systemd 'restart' would zero both LSNs. More importantly for this ADR: the node the capture is ABOUT is not the promoted node -- it is db003, the re-pointed replica, and `repmgr standby follow` (the follow_command repmgrd runs on the surviving standbys) restarts the standby in the default repmgr 5 configuration.
- **PG15 vs PG17:** Postgres side identical in 15 and 17. The repmgr question is version-independent but deployment-specific.
- **Query to settle:** `Not a psql query: `grep -E '^(service_promote_command|service_restart_command|service_start_command|follow_command|promote_command)' /etc/repmgr.conf` on a fleet node, plus `SELECT pg_postmaster_start_time(), pg_last_wal_receive_lsn();` on db003 after the next `repmgr standby follow` to see whether the postmaster start time moved.`
- **Impact if wrong:** If the fleet's failover path restarts db003 (via standby follow / node rejoin), then by L219's own statement both captured LSNs are zeroed and re-seeded, and the capture-first data for the one case it exists to serve is worthless -- or worse, re-seeded to a segment-aligned value below the true flush point (see the RequestXLogStreaming finding). The ADR draws a survival guarantee from the promoted node's behaviour and applies it to a different node with a different lifecycle.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:31-32 "Survives walreceiver death and promotion within one postmaster"

### [medium | partially-correct | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:217
- **Claim:** "Both pointers freeze at the promotion LSN -- a promoting standby finishes replaying what it received, so the two converge on the fork point and then stop advancing."
- **Postgres evidence:** From memory of PG15 src/backend/access/transam/xlogrecovery.c WaitForWALToBecomeAvailable(), XLOG_FROM_STREAM branch: when the promote trigger is seen it does not exit immediately; the comment there is explicit -- 'After being triggered, we still want to replay all the WAL that was already streamed. It's in pg_wal now, so we just treat this as a failure, and the state machine will move on to replay the streamed WAL from pg_wal, and then recheck the trigger and exit replay.' So in the ordinary streaming case the ADR is right and the measurement is unsurprising. It is NOT a general law. Counterexamples: (1) DEAD WALRECEIVER + ARCHIVE. The same state machine keeps restoring from restore_command after the trigger ('we still finish replaying as much as we can from archive and pg_wal before failover'), so replay advances past a stale flushedUpto -- on the promoted node pg_last_wal_replay_lsn() > pg_last_wal_receive_lsn(), and the receive LSN is NOT the fork point. (2) RECOVERY TARGET. With any recovery_target_* reached, recovery stops at the target (recovery_target_action default 'pause', then promote on resume); flushedUpto can be far ahead of the target, so receive > replay and receive is not the fork point. (3) PARTIAL TRAILING RECORD. flushedUpto is a flush position of received BYTES, not a record boundary; recovery ends at the last COMPLETE record, so replay can be up to one record short of receive. (4) TIMELINE RE-SEED. If a walreceiver was started on a different TLI before promotion, RequestXLogStreaming reset flushedUpto to a segment boundary, so receive can be far behind. (5) FAST vs FALLBACK promotion changes only whether an end-of-recovery record or a checkpoint is written -- it does not change the LSN at which replay stops, and pg_ctl promote exposes no -m mode, so the fast/smart framing in the question does not produce a counterexample.
- **PG15 vs PG17:** The 'replay what was already streamed' behaviour is the same in 15 and 17. PG15 is where this code moved from xlog.c to xlogrecovery.c, so a PG17 file citation still points at the right file for 15.
- **Query to settle:** `On a 15.14 test standby: `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type='walreceiver';` then feed further WAL only via restore_command, then promote, then `SELECT pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn(), pg_is_in_recovery();` and diff both against the switch point in the new .history -- expect replay > receive and receive != switch point.`
- **Impact if wrong:** A single n=1 measurement is generalised into 'both pointers freeze at the promotion LSN'. Any later code that treats last_wal_receive_lsn on a former standby as 'the fork point' will be wrong in cases (1)-(4). Nothing consumes these fields today (grepped: no reader outside src/v2/scan/health_check_replica.rs and the src/v2.rs test builder), so this is a latent-design defect, not a live wrong verdict.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:28-29 "Applied position. Non-NULL on anything that has replayed, and can be stale -- frozen at promotion on a former standby."

### [medium | partially-correct | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:215 and :219
- **Claim:** "A zero pointer is only reachable before any walreceiver has run (receive) or **anything has been replayed** (replay) within the current postmaster's life" / "they are zeroed by a postmaster restart and do not survive one."
- **Postgres evidence:** From memory of PG15 src/backend/access/transam/xlogrecovery.c PerformWalRecovery(): before applying a single record it seeds the shared state -- if (RedoStartLSN < CheckPointLoc) { XLogRecoveryCtl->lastReplayedEndRecPtr = RedoStartLSN; lastReplayedTLI = RedoStartTLI; } else { lastReplayedEndRecPtr = xlogreader->EndRecPtr; lastReplayedTLI = CheckPointTLI; }. So the replay pointer is non-zero from the MOMENT RECOVERY BEGINS, before anything has been replayed -- and hot-standby connections are not accepted until consistency is reached, well after that. The correct rule (and the PG15 manual's own wording for pg_last_wal_replay_lsn, 'When the server has been started normally without recovery, the function returns NULL') is: NULL iff this postmaster never entered recovery at all. Two consequences the ADR misses: a cleanly-restarted, never-standby primary returns NULL forever; a CRASH-recovered never-standby primary returns non-NULL. And the practical corollary contradicts L219's implication: a standby that restarts does NOT expose a NULL replay LSN to any client, because it is seeded before connections open.
- **PG15 vs PG17:** PerformWalRecovery() with this seeding exists in 15 and 17; in PG14 and earlier the equivalent code is in xlog.c StartupXLOG(). No behavioural difference for this claim.
- **Query to settle:** `On a scratch 15.14 primary that was never a standby: (a) clean restart -> `SELECT pg_last_wal_replay_lsn() IS NULL;` expect t; (b) `pg_ctl -m immediate stop` then start -> same query, expect f with pg_is_in_recovery() = f.`
- **Impact if wrong:** Low blast radius on the replay side (the error makes NULL rarer, not commoner, so nothing unsafe follows). It matters because L215 and L219 are used together to argue 'nullity is a non-issue', and that argument is built on a mechanism the ADR states incorrectly -- the same sentence also carries the receive-side claim, which IS wrong in the unsafe direction (see the C-f / restarted-standby finding).
- **Code depending on it:** src/v2/scan/health_check_replica.rs:28 "Non-NULL on anything that has replayed" -- should read 'on anything that has entered recovery, including crash recovery'.

### [info | confirmed | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:213
- **Claim:** "Both functions return SQL **NULL**, never `0/0`, when their position is zero: PG17 `xlogfuncs.c` guards each with `if (recptr == 0) PG_RETURN_NULL();` ahead of `PG_RETURN_LSN(recptr)`. `Option<String>` is therefore the right shape, and there is no zero-LSN sentinel to special-case."
- **Postgres evidence:** Reasoning from memory of the source, not from a checked tree (no PG source available in this environment). PG15 src/backend/access/transam/xlogfuncs.c contains both wrappers with the identical guard: pg_last_wal_receive_lsn() { recptr = GetWalRcvFlushRecPtr(NULL, NULL); if (recptr == 0) PG_RETURN_NULL(); PG_RETURN_LSN(recptr); } and pg_last_wal_replay_lsn() { recptr = GetXLogReplayRecPtr(NULL); if (recptr == 0) PG_RETURN_NULL(); PG_RETURN_LSN(recptr); }. The guard is not a PG17 addition -- it dates to the 9.x pg_last_xlog_receive_location/pg_last_xlog_replay_location functions and survived the PG10 rename. Corroborated by the PG15 manual, Table 'Recovery Information Functions': pg_last_wal_receive_lsn 'If streaming replication is disabled, or if it has not yet started, the function returns NULL'; pg_last_wal_replay_lsn 'When the server has been started normally without recovery, the function returns NULL'. Independently corroborated in-repo by the query shape at src/v2/scan/health_check_replica.rs:88-89, which casts to ::text -- a NULL pg_lsn casts to SQL NULL and lands as JSON null, which the existing unit test at :260-264 exercises.
- **PG15 vs PG17:** Holds identically on 15.14. The only 15-vs-17 wrinkle is the callee: in PG15 GetXLogReplayRecPtr lives in the new xlogrecovery.c (split out of xlog.c in 15) rather than xlog.c, but the SQL wrapper and its zero guard are in xlogfuncs.c on both.
- **Query to settle:** `SELECT version(), pg_last_wal_replay_lsn() IS NULL AS replay_null, pg_last_wal_receive_lsn() IS NULL AS recv_null, pg_last_wal_replay_lsn()::text AS replay_text;  -- on a freshly initdb'd, cleanly started 15.14 instance; expect replay_null = t and replay_text NULL, not the string '0/0'`
- **Impact if wrong:** If the guard were absent on 15.14, the fields would arrive as Some("0/0") and every future 'is it past the fork' comparison would treat a zero pointer as a real position at the very start of WAL -- i.e. 'definitely not past the fork', a false negative on the safety gate. The claim holding is load-bearing for the Option<String> shape. Note the PLAN doc (docs/superpowers/plans/2026-05-20-split-brain-resolution-refinement.md:35) still carries the pre-validation comment 'may be 0/0 with no receiver'; that comment is refuted by this validation and did not make it into the code.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:30 and :33 (`Option<String>`), and the test at :258-265 that nulls the field

### [low | annotation-does-not-support-claim | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:213
- **Claim:** The annotation itself: "(Validated 2026-09-10.) ... **PG17** `xlogfuncs.c` guards each with ..." offered as validation for a fleet running 15.14
- **Postgres evidence:** tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json records "pg_version": "15.14" on all three nodes (lines 10, 94, 139). PG17 is the only version named anywhere in the ADR (grep for a version string across the file returns line 213 only). Citing the source of a version the fleet does not run is a category error in an annotation whose whole purpose is to be a checkable citation -- even though, here, the guard happens to be identical on 15 (see the preceding claim), so the conclusion survives.
- **PG15 vs PG17:** This claim IS the pg15-vs-pg17 issue. The cited guard is identical on both, so the annotation is wrong about its source but right about its conclusion.
- **Query to settle:** `SELECT version();  -- on the node the 2026-09-10 measurement was actually taken on, and record it in the annotation`
- **Impact if wrong:** The citation reads as verified-for-our-deployment when it is verified-for-a-version-we-do-not-run. That habit is what lets a genuinely version-sensitive claim through later. Fix is one word: cite PG15 xlogfuncs.c (or 'PG 9.1 through 17, unchanged'), and state which node the 2026-09-10 measurement was taken on and at what version.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:88-89 -- the query runs on 15.14 nodes, not PG17

### [info | confirmed | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:217
- **Claim:** "a non-NULL `pg_last_wal_replay_lsn()` is **not** evidence that a node is a replica (`pg_is_in_recovery()` is what distinguishes)"
- **Postgres evidence:** Confirmed, and understated. From memory of the PG15 source plus the PG15 manual's own wording ('When the server has been started normally without recovery, the function returns NULL'): lastReplayedEndRecPtr is seeded in PerformWalRecovery() for ANY recovery, and nothing resets it when recovery ends. So a node that merely CRASH-recovered -- never a standby, never promoted -- also returns a non-NULL replay LSN forever. The ADR reaches the right conclusion via the narrower promotion path only. The code already does the right thing: role dispatch is `SELECT pg_is_in_recovery()` at src/v2/scan.rs:224, so the replay LSN is never used as a role signal.
- **PG15 vs PG17:** Same on both.
- **Query to settle:** `On a scratch 15.14 primary: `pg_ctl -m immediate stop`, start, then `SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn();` -- expect f and a non-NULL LSN on a node that was never a standby.`
- **Impact if wrong:** None today. Worth broadening the sentence so the next reader does not infer 'non-NULL replay LSN => this node was once a standby', which is also false.
- **Code depending on it:** src/v2/scan.rs:224 -- role is decided by pg_is_in_recovery(), not by LSN nullity, so nothing depends on the weaker reasoning

### [low | unverifiable-offline | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:217
- **Claim:** "Measured on a promoted primary (`pg_is_in_recovery() = f`): `pg_last_wal_replay_lsn()` and `pg_last_wal_receive_lsn()` both return `6FD/7C0000A0`, which is exactly the TL 21 -> 22 switch point in the `.history` captured from that cluster."
- **Postgres evidence:** No artefact in the repo backs this. The only committed fleet capture, tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json, is a healthy TL=11 cluster and contains NEITHER last_wal_replay_lsn nor last_wal_receive_lsn (grep returns no hits; the fields deserialize to None because serde's missing_field forwards deserialize_option to visit_none). There is no TL 21/22 .history anywhere in the repo, and no fixture from the promoted cluster at all. The nearest trace is a hand-written unit-test literal at src/v2/scan/health_check_replica.rs:241-242 ("6FD/8F96BC00" for both fields) -- same 6FD segment range, so plausibly transcribed from the same cluster, but a different LSN and not the claimed switch point. The value itself is internally plausible: 0x7C0000A0 is 160 bytes into segment 7C, past the 40-byte long page header, i.e. a credible record boundary near a segment start.
- **PG15 vs PG17:** Unknown -- the ADR does not say which version the measured node ran, which is exactly the gap the PG17-vs-15.14 citation problem creates.
- **Query to settle:** `SELECT version(), pg_is_in_recovery(), pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn(), (SELECT timeline_id FROM pg_control_checkpoint()), pg_read_file('pg_wal/' || lpad(upper(to_hex((SELECT timeline_id FROM pg_control_checkpoint()))), 8, '0') || '.history', 0, 1048576, true);  -- re-run on the same promoted node and commit the output as a fixture`
- **Impact if wrong:** The single measurement is the entire empirical basis for the general rule in the next sentence ('both pointers freeze at the promotion LSN'). It is not reproducible from anything checked in, so a future reader cannot re-derive it or notice if it was a special case. Fix: commit the promoted-cluster capture (or the .history plus the two LSN readings) as a fixture, and record the pg_version of the node measured.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:241-242 (the only in-repo LSN literals from that cluster)

### [low | confirmed | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:209-211 (scope of the §5 addition)
- **Claim:** §5 adds the absolute LSNs to HEALTH_CHECK_REPLICA_QUERY only; the observation that anchors the section ("Measured on a promoted primary") describes data the tool itself never collects.
- **Postgres evidence:** Verified by reading the code, not by Postgres semantics. src/v2/scan/health_check_replica.rs:88-89 adds both functions. HEALTH_CHECK_PRIMARY_QUERY (src/v2/scan/health_check_primary.rs:150-228) adds neither -- for a non-recovery node it keeps only 'current_wal_lsn' (pg_current_wal_lsn()). Role dispatch is `SELECT pg_is_in_recovery()` (src/v2/scan.rs:224), so any promoted/zombie primary takes the primary path and its frozen promotion LSNs are dropped. Both functions are legal on a non-recovery backend in PG15 (no in-recovery restriction), so this is a scope decision, not a Postgres limitation.
- **PG15 vs PG17:** No difference; both functions are callable outside recovery on 15 and 17.
- **Query to settle:** `SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), pg_last_wal_receive_lsn(), pg_current_wal_lsn();  -- on a promoted primary, to confirm all four are readable in one round trip`
- **Impact if wrong:** Benign for C-g as specified (db003 is in recovery, so it takes the replica path). It matters for the zombie primary db001: the tool captures where db001 is WRITING now but not where it was promoted from, so a future fork-relative comparison can only anchor on the .history file. If the intent is 'make the next C-g diagnosable', adding the same two columns to the primary query costs nothing and captures the promotion point of both primaries directly rather than inferring it from .history.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:161 ('current_wal_lsn', (SELECT pg_current_wal_lsn()::text)) -- the primary path's only LSN


## ADR-002 Postgres behaviour audit: synchronous_commit acknowledgement semantics, the real GUC value set, and the "isolated primary acked nothing" invar

### [critical | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:96 (the sanity gate) and :100 ("Not Refuse-worthy: synchronous_standby_names inconsistency"); relied on at :237, :242; docs/concepts/split-brain.md:15, :70, :76
- **Claim:** "The ANY 1 (A, B) no-divergence claim depends on the standby actually fsyncing before ack. Refuse if any primary has synchronous_commit in {local, off, remote_write, empty}." -- i.e. the gate treats synchronous_commit alone as sufficient to establish the quorum-sync invariant.
- **Postgres evidence:** PG15 and PG17 docs, runtime-config-wal.html, synchronous_commit, verbatim: "If synchronous_standby_names is empty, the only meaningful settings are on and off; remote_apply, remote_write and local all provide the same local synchronization level as on." Reinforced in source: syncrep.c SyncRepWaitForLSN fast-exits on `!SyncRepRequested() || !WalSndCtl->sync_standbys_defined`, and sync_standbys_defined is derived from SynchronousStandbyNames being non-empty (set by SyncRepUpdateSyncStandbysDefined on config reload), not from connectivity. So synchronous_commit=remote_apply with synchronous_standby_names='' acks after a purely local flush, with zero replication requirement.
- **PG15 vs PG17:** Identical. The sentence is byte-for-byte the same in the PG15 and PG17 docs, and syncrep.c's fast-exit predicate is semantically the same in REL_15_STABLE and REL_17_STABLE (the SYNC_STANDBY_INIT/SYNC_STANDBY_DEFINED bitmask refactor is PG18-only; PG15 uses the plain `sync_standbys_defined` bool).
- **Query to settle:** `SELECT name, setting, reset_val, source, sourcefile, sourceline FROM pg_settings WHERE name IN ('synchronous_commit','synchronous_standby_names','max_wal_senders'); -- on every candidate primary. Empty-string setting for synchronous_standby_names on any primary means the sec.2 gate's premise is void for that node.`
- **Impact if wrong:** Reachable in code today. A candidate primary with synchronous_commit=remote_apply and synchronous_standby_names='' passes the sec.2 gate (no SynchronousCommitWeakened), emits no PrimaryQuorumUnsatisfied (emit_quorum_findings `continue`s on the unparseable/empty SSN), so determine_confidence_level sees no Refuse-worthy finding and returns BestEffort. If that primary is db002 (higher TL), the writer prints "fence db002 (TL=N+1, quorum-blocked)" while db002 has in fact been client-acking writes the whole time on its own fork. That is the destructive-instruction case the ADR exists to prevent, produced with full confidence. Compounding this, sec.2 line 100 explicitly declares synchronous_standby_names problems "Not Refuse-worthy" -- true for cross-primary *divergence*, false for *emptiness*, and the ADR never distinguishes the two. Not reachable on the current fleet: tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json has SSN = "ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )". But repmgr promote/standby-follow paths and manual incident response both rewrite SSN, so this is exactly the state a slow-fence incident can produce.
- **Code depending on it:** src/v2/analyze/split_brain.rs:13 `const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];` and :162-177 (the gate loop reads only `configuration.get("synchronous_commit")`); src/v2/analyze/split_brain.rs:650 `let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else { continue; };`; src/v2/analyze/split_brain.rs:400-407 (comment "A quorum-blocked primary cannot have ack'd writes" -> Confidence::BestEffort for the stale primary); src/v2/writer/build.rs:707-709 `"SplitBrain: {} has quorum (lower TL={}), fence {} (TL={}, quorum-blocked)"`

### [critical | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:235, :237, :242; docs/concepts/split-brain.md:15, :70
- **Claim:** "the lower-TL primary does not ack a client commit until the standby has flushed it" / "an isolated primary ... physically cannot commit" / db002 "provably client-acked nothing on TL=N+1".
- **Postgres evidence:** PG15 syncrep.c, SyncRepWaitForLSN wait loop, verbatim: `if (QueryCancelPending) { QueryCancelPending = false; ereport(WARNING, (errmsg("canceling wait for synchronous replication due to user request"), errdetail("The transaction has already committed locally, but might not have been replicated to the standby."))); SyncRepCancelWait(); break; }`. After the break, RecordTransactionCommit returns normally and the COMMIT reports success to the client. So a single pg_cancel_backend() (or a client Ctrl-C) turns a blocked commit on the isolated primary into a client-acknowledged commit that no standby ever saw.
- **PG15 vs PG17:** Identical in both; the WARNING/errdetail strings are unchanged between REL_15_STABLE and current master.
- **Query to settle:** `Cannot be settled after the fact from SQL -- the evidence is in the server log. Grep the isolated primary's log for 'canceling wait for synchronous replication due to user request' and 'canceling the wait for synchronous replication and terminating connection due to administrator command' between the fork timestamp and the scan. This should be added to the sec.7 capture list.`
- **Impact if wrong:** The whole 3-node proof (ADR:240-243, concepts:66-72) is stated as a proof and it is not one: it is an argument about the *default* path only. One cancelled commit on the isolated higher-TL primary and "its fork is empty" is false, in the strongest sense (client was told COMMIT succeeded). The tool then prints a confident fence/rebuild instruction for the node holding those writes. Mitigating fact, verified: statement_timeout cannot trigger this. PG15 postgres.c finish_xact_command() calls disable_statement_timeout() *before* CommitTransactionCommand(), so a routine statement timeout never reaches the sync-rep wait. It takes a deliberate cancel, a SIGTERM, a fast shutdown, or postmaster death. That makes it operator-triggered rather than automatic -- but "operator cancels a hung commit on a wedged primary" is a completely ordinary incident-response action, and it is unobservable to the scanner after the fact.
- **Code depending on it:** src/v2/analyze/split_brain.rs:400-401 (comment: "A quorum-blocked primary cannot have ack'd writes.") and :405-408 returning Confidence::BestEffort when the quorum-unsatisfied primary is not the elected true_primary; src/v2/writer/build.rs:707-709 (the "fence {stale} (TL={hi}, quorum-blocked)" string)

### [high | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:96 (the gate is written as a per-primary instance property)
- **Claim:** Implicit: reading synchronous_commit once per primary from pg_settings establishes the durability level of every commit that primary performed.
- **Postgres evidence:** synchronous_commit is declared PGC_USERSET (guc.c / guc_tables.c, same in PG15 and PG17). PG15 docs, warm-standby.html sec.27.2.8, verbatim: "synchronous_commit can be set by individual users, so it can be configured in the configuration file, for particular users or databases, or dynamically by applications, in order to control the durability guarantee on a per-transaction basis." And runtime-config-wal.html: "This parameter can be changed at any time; the behavior for any one transaction is determined by the setting in effect when it commits. ... issue SET LOCAL synchronous_commit TO OFF within the transaction." Sync-rep participation is decided per backend at commit time via SyncRepRequested() = `(max_wal_senders > 0 && synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)` (syncrep.h, PG15).
- **PG15 vs PG17:** Identical. PGC_USERSET in both; the doc sentence is present in both PG15 and PG17.
- **Query to settle:** `SELECT setting, reset_val, boot_val, source, sourcefile, sourceline, context FROM pg_settings WHERE name='synchronous_commit'; SELECT r.rolname, d.datname, s.setconfig FROM pg_db_role_setting s LEFT JOIN pg_roles r ON r.oid=s.setrole LEFT JOIN pg_database d ON d.oid=s.setdatabase WHERE array_to_string(s.setconfig,',') ILIKE '%synchronous_commit%'; -- the second query is the one the scanner does not run and should.`
- **Impact if wrong:** A session on the isolated primary doing `SET synchronous_commit = local` (or `off`, or `SET LOCAL ... TO OFF`) commits and is acked immediately with no standby involvement whatsoever. Those are durable, client-acknowledged writes on the fork the ADR calls empty. The resolver can never see this: (a) pg_settings.setting is the *scanner session's* value, not a fleet-wide fact -- pg_settings docs: setting = "Current value of the parameter", and "The change only affects the value used by the current session"; (b) role-level and database-level defaults set via ALTER ROLE/ALTER DATABASE live in pg_db_role_setting and apply to the *app* role, not the scanner role, so the scanner reads 'remote_apply' while the app commits with 'local'; (c) a SET issued in a since-closed app session leaves no trace anywhere queryable. This is undetectable-by-construction, not a bug to fix -- but the ADR presents the gate as establishing an invariant it cannot establish, and nothing in the ADR or the code says so. Minimum honest fix: capture reset_val/source/sourcefile plus the full contents of pg_db_role_setting, and downgrade the sec.2 gate's stated guarantee from "the standby fsynced before ack" to "the instance default did not disable sync rep".
- **Code depending on it:** src/v2/analyze/split_brain.rs:165-169 (`h.configuration.get("synchronous_commit")`); src/v2/scan/health_check_primary.rs:~163-178 (`SELECT jsonb_object_agg(name, setting) FROM pg_settings WHERE name IN (... 'synchronous_commit' ...)`)

### [high | refuted | confidence=certain] ADR lines docs/concepts/split-brain.md:15 ("an *isolated* primary -- one with no live standby acking it -- physically cannot commit"), :70 ("provably committed nothing on TL=N+1. Its fork is empty"), :76; docs/adr/002-split-brain-resolution-refinement.md:237, :242
- **Claim:** An isolated primary under quorum sync commits nothing / its fork is empty.
- **Postgres evidence:** PG15 xact.c RecordTransactionCommit(): the commit record is XLogFlush()ed and `TransactionIdCommitTree(xid, nchildren, children)` marks clog COMMITTED *before* the sync-rep wait. The wait's own comment, verbatim: "Wait for synchronous replication, if required. ... Note that at this stage we have marked clog, but still show as running in the procarray and continue to hold locks." followed by `if (wrote_xlog && markXidCommitted) SyncRepWaitForLSN(XactLastRecEnd, true);`. And PG15 docs warm-standby.html sec.27.2.8, verbatim: "If primary restarts while commits are waiting for acknowledgment, those waiting transactions will be marked fully committed once the primary database recovers. There is no way to be certain that all standbys have received all outstanding WAL data at time of the crash of the primary. ... The guarantee we offer is that the application will not receive explicit acknowledgment of the successful commit of a transaction until the WAL data is known to be safely received by all the synchronous standbys."
- **PG15 vs PG17:** Identical. Same xact.c ordering and same doc paragraph in PG15 and PG17.
- **Query to settle:** `On the isolated primary before fencing: SELECT pg_current_wal_lsn(); plus the higher-TL primary's .history fork LSN X, then `SELECT * FROM pg_ls_waldir()` / pg_waldump between X and current to see whether any XLOG_XACT_COMMIT records exist past the fork. A commit record past X on TL=N+1 falsifies "the fork is empty" directly.`
- **Impact if wrong:** Postgres guarantees exactly one thing here, and the docs state it precisely: no *explicit acknowledgment* to the application. It guarantees nothing about local commit, local durability, or eventual visibility. An isolated primary that is restarted (or whose waiting backends are killed) ends up with those transactions fully committed, visible to every subsequent session, and present in its WAL and its archive. So "discard/rebuild the higher TL, its fork is empty" (ADR:237, concepts:76) destroys durable, readable data -- just not *acked* data. The ADR's argument survives only in its weaker form: "no writes were acknowledged to a client on the isolated fork". Both documents should be rewritten to say that, and the rebuild instruction should carry the caveat, because an operator reading "the fork is empty" will not take a dump before tearing db002 down. Note this also interacts with archiving: archive_command ships the isolated primary's TL=N+1 segments regardless of sync rep, so the unacked-but-committed WAL also leaves the node.
- **Code depending on it:** src/v2/analyze/split_brain.rs:399-408 (determine_confidence_level; the "cannot have ack'd writes" comment and the BestEffort branch); src/v2/writer/build.rs:707-709

### [high | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:237, :242; docs/concepts/split-brain.md:15, :70
- **Claim:** (Same invariant, second failure path.) Nothing on the isolated fork can become client-visible without a standby ack.
- **Postgres evidence:** PG15 syncrep.c, ProcDiePending branch, verbatim: `ereport(WARNING, (errcode(ERRCODE_ADMIN_SHUTDOWN), errmsg("canceling the wait for synchronous replication and terminating connection due to administrator command"), errdetail("The transaction has already committed locally, but might not have been replicated to the standby."))); whereToSendOutput = DestNone; SyncRepCancelWait(); break;` and the postmaster-death branch `if (rc & WL_POSTMASTER_DEATH) { ProcDiePending = true; whereToSendOutput = DestNone; SyncRepCancelWait(); break; }`. Plus PG15 docs sec.27.2.8: "Users will stop waiting if a fast shutdown is requested."
- **PG15 vs PG17:** Identical in both branches.
- **Query to settle:** `Same as the previous claim: pg_waldump for XLOG_XACT_COMMIT records past the fork LSN on the higher-TL node, before it is torn down. There is no post-hoc SQL for this once the node is rebuilt.`
- **Impact if wrong:** pg_terminate_backend, a SIGTERM to the postmaster, `pg_ctl stop -m fast`, or postmaster death all break the wait. `whereToSendOutput = DestNone` means the *client* is never told, so no acknowledged-write invariant is violated -- but ProcArrayEndTransaction runs immediately afterwards in CommitTransaction(), so the transaction becomes MVCC-visible to every other session on that node from that instant. These are the writes that are durable and readable but unacked. They still exist only on the higher-TL fork. Rebuilding db002 destroys them silently. Note that fencing a zombie primary *is* commonly done with a fast shutdown, so the fence itself is one of the mechanisms that converts blocked commits into committed-and-visible ones.
- **Code depending on it:** src/v2/analyze/split_brain.rs:399-408; src/v2/writer/build.rs:707-709

### [medium | partially-correct | confidence=high] ADR lines docs/concepts/split-brain.md:42 ("a replica whose flushed/applied LSN is past the fork X on the lower timeline is *proof* that the lower-TL primary client-acknowledged writes in (X, flushed_lsn]"); docs/adr/002-split-brain-resolution-refinement.md:235
- **Claim:** Replica flushed past the inter-primary fork LSN is proof of client-acknowledged writes in (X, flushed_lsn].
- **Postgres evidence:** Reasoning from PG source structure rather than a single quotable sentence, so flagging that plainly. A primary emits WAL past any given LSN for reasons that involve no client commit at all: XLOG_CHECKPOINT_ONLINE at every checkpoint_timeout, XLOG_RUNNING_XACTS from the logging of standby snapshots (LOG_SNAPSHOT_INTERVAL_MS, ~15s, on the bgwriter), autovacuum/autoanalyze, HOT-prune records (XLOG_HEAP2_PRUNE), full-page images, and XLOG_SWITCH. RecordTransactionCommit only writes XLOG_XACT_COMMIT when `markXidCommitted` -- i.e. when a top-level xid was assigned -- and only *those* records reach SyncRepWaitForLSN (`if (wrote_xlog && markXidCommitted) SyncRepWaitForLSN(...)`, PG15 xact.c). So "standby flushed past X" proves the lower-TL primary *generated and replicated* WAL past X, which is true of an entirely idle primary. It does not prove a client commit was acked in that range.
- **PG15 vs PG17:** Same in both. Background WAL generation on an idle primary exists in PG15 and PG17 alike.
- **Query to settle:** `Not a query -- pg_waldump. On the lower-TL primary: pg_waldump -p <pgdata>/pg_wal -t <N> -s <X> | grep -c 'XACT_COMMIT'. Zero means the range past the fork contains no client transactions at all, and the "acked writes" inference does not apply.`
- **Impact if wrong:** The error direction is conservative (a false "acked writes diverged" produces Refuse / keep-lower-TL rather than a destructive pick), so this is not a data-loss path. But it is stated as a *proof* in the concepts doc and will be cited as one when sec.7 is designed, and it means the naive trigger "replica flushed_lsn > fork_lsn" will fire on essentially every split-brain where the zombie primary stayed up for one checkpoint interval -- exactly the over-caution ADR:245 says it wants to avoid. The sound version of the inference needs a *commit record* past X, not an LSN past X: scan pg_waldump output for XLOG_XACT_COMMIT in (X, flushed_lsn]. Worth saying explicitly in the doc, because the current phrasing invites building the cheap LSN comparison.
- **Code depending on it:** Nothing today -- DivergentReplicaWal is not emitted (src/v2/analyze/split_brain.rs:398 maps it to Refuse but no code constructs it). It becomes load-bearing the moment sec.7's detection is built, and the ADR mandates that detection drive a verdict-flip (ADR:239).

### [medium | partially-correct | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:211 ("pg_last_wal_receive_lsn() -- received/flushed position (the ack-relevant one under synchronous_commit=on)"); the same "synchronous_commit = on" framing at :235 and docs/concepts/split-brain.md:7, :15, :42, :70
- **Claim:** pg_last_wal_receive_lsn() is the ack-relevant replica position; the safety argument is framed throughout as "synchronous_commit = on".
- **Postgres evidence:** Correct for `on`. PG15/PG17 docs, runtime-config-wal.html: "When set to on, commits wait until replies from the current synchronous standby(s) indicate they have received the commit record of the transaction and flushed it to durable storage." And pg_last_wal_receive_lsn() is documented as the last WAL location "received and synced to disk by streaming replication" -- so it is the flush pointer, matching `on` exactly. But the fleet does not run `on`: tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json:27 has "synchronous_commit": "remote_apply", on PG 15.14. For remote_apply the docs say commits "wait until replies ... indicate they have received the commit record of the transaction and applied it, so that it has become visible to queries on the standby(s), and also written to durable storage on the standbys" -- i.e. remote_apply is strictly stronger than on (assign_synchronous_commit in syncrep.c maps REMOTE_WRITE->SYNC_REP_WAIT_WRITE, REMOTE_FLUSH(=ON)->SYNC_REP_WAIT_FLUSH, REMOTE_APPLY->SYNC_REP_WAIT_APPLY, and syncrep.h defines SYNC_REP_WAIT_WRITE 0 < SYNC_REP_WAIT_FLUSH 1 < SYNC_REP_WAIT_APPLY 2).
- **PG15 vs PG17:** No difference. The remote_apply and on definitions are byte-identical in the PG15 and PG17 docs; the SyncRepWaitMode ordering is unchanged.
- **Query to settle:** `On each candidate primary: SELECT application_name, state, sync_state, sent_lsn, write_lsn, flush_lsn, replay_lsn FROM pg_stat_replication; -- under remote_apply the ack boundary is replay_lsn. On the replica: SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), pg_is_in_recovery();`
- **Impact if wrong:** Two separate consequences. (1) Good news for the safety argument: remote_apply implies everything `on` implies, so every sentence written for `on` holds a fortiori on this fleet. The docs' "synchronous_commit = on" framing is therefore safe but inaccurate for our deployment, and should say remote_apply (or "on or stronger"). (2) Bad news for the evidence: under remote_apply the pointer that proves an ack is pg_last_wal_replay_lsn(), not pg_last_wal_receive_lsn(). Since receive >= replay always, building the sec.7 trigger on receive_lsn over-claims: a replica can have received-and-flushed WAL past the fork that was never applied and therefore never contributed to any ack. ADR:211's parenthetical picks the looser pointer and labels it "the ack-relevant one", which will steer the implementer wrong. Both are already captured (ADR sec.5), so the fix is a one-word change in the ADR: under remote_apply, use replay_lsn as the ack-proving pointer and receive_lsn only as an upper bound.
- **Code depending on it:** src/v2/scan/health_check_replica.rs (the pg_last_wal_receive_lsn()/pg_last_wal_replay_lsn() capture added by ADR sec.5); nothing consumes them yet, so this bites when sec.7 detection is written.

### [low | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:96 ("Valid values: on, remote_apply, remote_flush.")
- **Claim:** synchronous_commit has a value called remote_flush.
- **Postgres evidence:** PG15 and PG17 docs, runtime-config-wal.html, verbatim: "Valid values are remote_apply, on (the default), remote_write, local, and off." There is no remote_flush. The name the ADR reached for is the internal C enumerator: PG15 src/include/access/xact.h, verbatim: `typedef enum { SYNCHRONOUS_COMMIT_OFF, SYNCHRONOUS_COMMIT_LOCAL_FLUSH, SYNCHRONOUS_COMMIT_REMOTE_WRITE, SYNCHRONOUS_COMMIT_REMOTE_FLUSH, SYNCHRONOUS_COMMIT_REMOTE_APPLY } SyncCommitLevel;` followed by `#define SYNCHRONOUS_COMMIT_ON SYNCHRONOUS_COMMIT_REMOTE_FLUSH`. So remote_flush is what `on` *is*, internally -- not a spelling a user can set. `SET synchronous_commit = 'remote_flush'` errors with "invalid value for parameter". Confirmed against the accepted-name table itself: PG15 guc.c and PG17 guc_tables.c both contain, verbatim, `static const struct config_enum_entry synchronous_commit_options[] = { {"local", ...}, {"remote_write", ...}, {"remote_apply", ...}, {"on", ...}, {"off", ...}, {"true", ...}, {"false", ...}, {"yes", ...}, {"no", ...}, {"1", ...}, {"0", ...}, {NULL, 0, false} };` -- no remote_flush entry.
- **PG15 vs PG17:** No difference. The value set is identical in 15 and 17, and the config_enum_entry table is character-identical between REL_15_STABLE guc.c and REL_17_STABLE guc_tables.c (the table moved file in PG16 without changing content).
- **Query to settle:** `SELECT enumvals FROM pg_settings WHERE name='synchronous_commit'; -- returns {local,remote_write,remote_apply,on,off}. Or just: SET synchronous_commit = 'remote_flush';`
- **Impact if wrong:** No behavioural effect today. Two second-order concerns worth naming. First, it is evidence the sec.2 gate was written from the C enum rather than from the GUC, which is the same confusion that produced the "empty" entry (next claim). Second, the code's denylist is open-ended: any value not in {local, off, remote_write, ""} passes the gate. That is safe against PG15/PG17's fixed 5-value enum, but it means a typo'd or future value fails open rather than closed. If the gate is ever rewritten, invert it to an allowlist of {on, remote_apply} -- which is what the ADR was trying to state.
- **Code depending on it:** src/v2/analyze/split_brain.rs:13 -- the implementation ignored the ADR's allowlist and used a denylist instead, so the error never reached the code. The complete correct allowlist is {on, remote_apply}; the denylist {local, off, remote_write} is equivalent given the real 5-value set.

### [low | annotation-does-not-support-claim | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:96 ("synchronous_commit in {local, off, remote_write, empty}")
- **Claim:** "empty" is a synchronous_commit value that a primary can have.
- **Postgres evidence:** synchronous_commit is declared `(enum)` in PG15 and PG17. An enum GUC always holds one of its five values; pg_settings.setting for it is produced by config_enum_lookup_by_value(), which returns a name from synchronous_commit_options, never the empty string. There is no code path by which SHOW synchronous_commit or pg_settings.setting yields ''.
- **PG15 vs PG17:** No difference; enum GUC semantics are unchanged.
- **Query to settle:** `SELECT vartype, setting FROM pg_settings WHERE name='synchronous_commit'; -- vartype is 'enum' and setting is never ''.`
- **Impact if wrong:** The code's behaviour is defensible -- "" is reachable only when the key is absent from the captured configuration map, i.e. when the scan failed to read the setting, and refusing in that case is right. But the ADR describes it as a postgres value, so the finding is emitted as `SynchronousCommitWeakened { primary, value: "" }` and the writer renders it (src/v2/writer/build.rs:674-676) as the literal operator-facing string "synchronous_commit= on db001" -- a REFUSE reason with a blank value, which reads as a bug rather than as "we could not read this setting". The ADR should say "or the setting is missing from the scan" and the writer should render that case as "synchronous_commit not captured on db001".
- **Code depending on it:** src/v2/analyze/split_brain.rs:13 (`""` as the fourth denylist entry) together with :165-169 `h.configuration.get("synchronous_commit").map_or("", String::as_str)`

### [info | confirmed | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:96 (implicit: the gate compares the raw pg_settings string against a fixed lowercase list)
- **Claim:** (Requested enumeration.) The full accepted value set for synchronous_commit on PG15 and PG17, including boolean aliases, and what SHOW / pg_settings.setting return for each spelling.
- **Postgres evidence:** Checked source, both branches. PG15 src/backend/utils/misc/guc.c and PG17 src/backend/utils/misc/guc_tables.c, `synchronous_commit_options[]`, verbatim and identical: {"local", SYNCHRONOUS_COMMIT_LOCAL_FLUSH, false}, {"remote_write", SYNCHRONOUS_COMMIT_REMOTE_WRITE, false}, {"remote_apply", SYNCHRONOUS_COMMIT_REMOTE_APPLY, false}, {"on", SYNCHRONOUS_COMMIT_ON, false}, {"off", SYNCHRONOUS_COMMIT_OFF, false}, {"true", SYNCHRONOUS_COMMIT_ON, true}, {"false", SYNCHRONOUS_COMMIT_OFF, true}, {"yes", SYNCHRONOUS_COMMIT_ON, true}, {"no", SYNCHRONOUS_COMMIT_OFF, true}, {"1", SYNCHRONOUS_COMMIT_ON, true}, {"0", SYNCHRONOUS_COMMIT_OFF, true}. The third field is `hidden`. So eleven spellings are ACCEPTED: local, remote_write, remote_apply, on, off, true, false, yes, no, 1, 0. Only five are canonical. SHOW and pg_settings.setting go through config_enum_lookup_by_value(), which returns the FIRST table entry whose val matches -- 'on' precedes 'true'/'yes'/'1' and 'off' precedes 'false'/'no'/'0' in the table, so yes: an enum GUC does normalise. Setting true/yes/1 reports back as 'on'; false/no/0 reports back as 'off'. pg_settings.enumvals is built by config_enum_get_options(), which skips hidden entries, so enumvals = {local,remote_write,remote_apply,on,off} -- the six aliases are accepted but not advertised. (The lookup/get_options function bodies I am citing from memory of guc.c; the table itself I fetched and quote verbatim. The normalisation behaviour is independently checkable with the query below.)
- **PG15 vs PG17:** Byte-identical tables in PG15 and PG17. The only change between them is that the table moved from guc.c to guc_tables.c in PG16.
- **Query to settle:** `SET synchronous_commit = '0'; SHOW synchronous_commit; -- expect 'off'. SET synchronous_commit = 'yes'; SHOW synchronous_commit; -- expect 'on'. SELECT enumvals FROM pg_settings WHERE name='synchronous_commit'; -- expect {local,remote_write,remote_apply,on,off}.`
- **Impact if wrong:** The gate is correct only because pg_settings normalises. `synchronous_commit = 0` in postgresql.conf reaches split_brain.rs as "off" and is caught. That is a load-bearing assumption nobody wrote down. Two ways it could be broken later: switching the capture to pg_file_settings (whose `setting` column returns the RAW text as written in the file, so '0' stays '0' and the gate would silently fail open on an off-equivalent primary), or reading a config file directly. Worth a one-line comment at split_brain.rs:13 stating that the list relies on pg_settings enum normalisation. No change needed otherwise.
- **Code depending on it:** src/v2/analyze/split_brain.rs:169 `if WEAKENED_SYNCHRONOUS_COMMIT.contains(&v)` -- a raw, case-sensitive, exact string compare with no normalisation of its own.

### [info | confirmed | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:96 ("remote_write is included because it does not wait for fsync on the standby")
- **Claim:** remote_write must be refused because it does not wait for fsync on the standby.
- **Postgres evidence:** PG15 and PG17 docs, runtime-config-wal.html, verbatim: "When set to remote_write, commits will wait until replies from the current synchronous standby(s) indicate they have received the commit record of the transaction and written it to their file systems. This setting ensures data preservation if a standby instance of PostgreSQL crashes, but not if the standby suffers an operating-system-level crash because the data has not necessarily reached durable storage on the standby." Corroborated in source: syncrep.c assign_synchronous_commit maps SYNCHRONOUS_COMMIT_REMOTE_WRITE -> SYNC_REP_WAIT_WRITE (syncrep.h: SYNC_REP_WAIT_WRITE 0, below SYNC_REP_WAIT_FLUSH 1).
- **PG15 vs PG17:** Identical wording in both.
- **Query to settle:** `None needed.`
- **Impact if wrong:** Nothing -- the ADR's justification is essentially the docs' own wording. Worth noting the nuance the ADR glosses: under remote_write the standby HAS the WAL in its OS page cache, so the divergence-detection inference is only slightly weaker than under `on` (the replica's flush pointer lags its write pointer). Refusing is still the right call because pg_last_wal_receive_lsn() reports the flushed position and would understate what remote_write acked. This is a conservative gate, not a wrong one.
- **Code depending on it:** src/v2/analyze/split_brain.rs:13 ("remote_write" in WEAKENED_SYNCHRONOUS_COMMIT); test at src/v2/analyze/split_brain.rs:1431-1450

### [info | confirmed | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:239, :242-243; docs/concepts/split-brain.md:15, :68-72 -- the premise that db002, isolated with SSN set, simply stops
- **Claim:** (Requested.) What happens when synchronous_standby_names is set but no listed standby is connected -- does the commit block forever, or ack?
- **Postgres evidence:** It blocks, indefinitely. PG15 syncrep.c SyncRepWaitForLSN fast-exits only on `!SyncRepRequested() || !WalSndCtl->sync_standbys_defined`; sync_standbys_defined tracks whether the GUC string is non-empty (SyncRepUpdateSyncStandbysDefined, called on config reload), not whether any walsender exists. With no standby connected the backend enqueues and waits on its latch with no timeout. PG15 docs, warm-standby.html sec.27.2.8, verbatim: "Such transaction commits may never be completed if any one of the synchronous standbys should crash." And, concretely: "If you need to re-create a standby server while transactions are waiting, make sure that the commands pg_backup_start() and pg_backup_stop() are run in a session with synchronous_commit = off, otherwise those requests will wait forever for the standby to appear." The escape hatches the docs name are exactly the ones that break the invariant: "you should decrease the number of synchronous standbys ... in synchronous_standby_names (or disable it) and reload the configuration file on the primary server."
- **PG15 vs PG17:** Identical behaviour and identical doc text in PG15 and PG17.
- **Query to settle:** `On a suspected-isolated primary, before touching it: SELECT pid, state, wait_event_type, wait_event, xact_start, query FROM pg_stat_activity WHERE wait_event = 'SyncRep'; -- non-empty means backends are blocked in SyncRepWaitForLSN and nothing was acked; empty while SSN is set and pg_stat_replication is empty means either no write traffic or something took it off the sync path.`
- **Impact if wrong:** This is the ADR's strongest ground and it holds: the *default* behaviour of an isolated primary with SSN set is an unbounded block, not an ack. Every counterexample in this audit is a deviation from that default (SSN emptied, per-session override, cancel, terminate, crash). Worth recording in the ADR as the precise statement -- "the default path acks nothing; here are the five ways off the default path" -- rather than the current absolute "physically cannot commit". Note also the documented remedy ("disable it and reload") is precisely the incident-response move that converts a blocked zombie primary into an acking one, which is why the empty-SSN gate gap above is critical and not theoretical.
- **Code depending on it:** src/v2/analyze/split_brain.rs:400-401 (the "quorum-blocked primary cannot have ack'd writes" comment) -- this claim is the one part of that comment that is genuinely true.

### [info | refuted | confidence=high] ADR lines docs/concepts/split-brain.md:15, :70; docs/adr/002-split-brain-resolution-refinement.md:237 -- specifically the "visibility-before-ack" attack surface requested in the assignment
- **Claim:** (Hypothesis under test, from the assignment.) Other sessions can SEE the effects of a transaction that is still waiting for its sync-rep ack, giving a visibility-before-ack window.
- **Postgres evidence:** Postgres deliberately closes this window. PG15 xact.c places SyncRepWaitForLSN inside RecordTransactionCommit, and CommitTransaction() calls ProcArrayEndTransaction *after* RecordTransactionCommit returns; the in-source comment on the wait, verbatim: "Note that at this stage we have marked clog, but still show as running in the procarray and continue to hold locks." A snapshot taken by another session therefore sees the xid as in-progress, so MVCC hides the rows, and the locks are still held so writers block rather than proceed. The one plausible bypass -- pg_xact_status(), which reads clog -- is explicitly guarded: PG15 xid8funcs.c, verbatim: "Like when doing visiblity checks on a row, check whether the transaction is still in progress before looking into the CLOG. Otherwise we would incorrectly return \"committed\" for a transaction that is committing and has already updated the CLOG, but hasn't removed its XID from the proc array yet." followed by `if (TransactionIdIsInProgress(xid)) status = "in progress";`. The same comment appears verbatim on all three SyncRepWaitForLSN call sites in twophase.c (EndPrepare, RecordTransactionCommitPrepared, RecordTransactionAbortPrepared), so 2PC is covered identically.
- **PG15 vs PG17:** Same ordering and same guard in both; the xid8funcs.c comment is present in PG15 and unchanged in PG17.
- **Query to settle:** `Two sessions on a primary with SSN set and no standby: session A does BEGIN; INSERT; COMMIT (blocks). Session B runs SELECT count(*) FROM t; -- expect the old count -- and SELECT pg_xact_status(<A's xid8>); -- expect 'in progress'. Then cancel A and re-run both.`
- **Impact if wrong:** Result is in the ADR's favour: there is no steady-state visibility-before-ack leak. The window opens only when the wait terminates abnormally -- cancel, terminate, fast shutdown, postmaster death -- which are the claims above. That is a useful sharpening: the ADR does not need to defend against a continuous leak, only against four discrete events, all of which leave a log line. One residual I did NOT verify and am flagging as open: EndPrepare calls MarkAsPrepared() before SyncRepWaitForLSN, so a transaction may appear in pg_prepared_xacts while its PREPARE is still waiting for the ack. I did not read the MarkAsPrepared ordering myself, and pg_prepared_xacts exposure of a not-yet-acked prepare is narrow, but it is the one place I could not rule the window out.
- **Code depending on it:** Nothing. Reported because the assignment asked for it and because the negative result narrows where the real risk is.

### [info | refuted | confidence=high] ADR lines docs/concepts/split-brain.md:15, :70; docs/adr/002-split-brain-resolution-refinement.md:237 -- the "empty branch" claim, tested against unlogged and temporary tables
- **Claim:** (Attack vector from the assignment.) Unlogged and temporary tables let an isolated primary produce durable state without a sync-rep wait.
- **Postgres evidence:** For unlogged tables the wait still happens. A write to an unlogged table assigns a top-level xid, so markXidCommitted is true and RecordTransactionCommit writes an XLOG_XACT_COMMIT record, making wrote_xlog true; the guard is `if (wrote_xlog && markXidCommitted) SyncRepWaitForLSN(XactLastRecEnd, true);` (PG15 xact.c). Both conditions hold, so the commit blocks on an isolated primary exactly like a logged write. The xact.c comment's mention of "an xid ... assigned due to temporary/unlogged tables" refers to the no-WAL-at-all case, not to a bypass. Separately, unlogged relation *contents* are never replicated and are truncated at crash recovery, so they exist on neither branch's standby by construction. Temporary tables are session-local and vanish at disconnect.
- **PG15 vs PG17:** Same in both.
- **Query to settle:** `On an isolated primary: CREATE UNLOGGED TABLE u(i int); INSERT INTO u VALUES (1); -- expect the INSERT's implicit commit to block in wait_event='SyncRep'.`
- **Impact if wrong:** Strengthens the ADR rather than weakening it: unlogged writes do not sneak past the quorum gate. The only residual is that unlogged table contents live on the node and nowhere else, so a rebuild of the higher-TL primary loses them -- but they were never on any other branch either, and a crash would have truncated them anyway, so there is no divergence question. Irrelevant to the acked-write argument. Worth one sentence in the concepts doc only if someone asks.
- **Code depending on it:** None.

### [info | partially-correct | confidence=high] ADR lines docs/concepts/split-brain.md:15, :70 -- tested against sequence advances
- **Claim:** (Attack vector from the assignment.) nextval() advances a sequence and hands the value to the client without a sync-rep wait, so an isolated primary can burn sequence values that never reach the other branch.
- **Postgres evidence:** Checked source. PG15 sequence.c nextval_internal() pre-logs in blocks of `#define SEQ_LOG_VALS 32` ("We don't want to log each fetching of a value from a sequence, so we pre-log a few fetches in advance. In the event of crash we can lose (skip over) as many values as we pre-logged."), and when it does log it explicitly acquires an xid: `if (logit && RelationNeedsWAL(seqrel)) GetTopTransactionId();` with the comment "If something needs to be WAL logged, acquire an xid, so this transaction's commit will trigger a WAL flush and wait for syncrep." So the WAL-logging nextval() DOES go through SyncRepWaitForLSN and blocks on an isolated primary.
- **PG15 vs PG17:** Same in both; SEQ_LOG_VALS is 32 and the GetTopTransactionId() call with that comment is present in PG15 and PG17.
- **Query to settle:** `On an isolated primary: CREATE SEQUENCE s; SELECT nextval('s') FROM generate_series(1,40); -- expect it to return values until the pre-logged window is exhausted and then block in wait_event='SyncRep'.`
- **Impact if wrong:** Bounded and benign, which is worth recording so nobody re-raises it. On an isolated primary a session can consume at most the remainder of the pre-logged window (up to SEQ_LOG_VALS = 32 values per sequence, plus whatever a session-level CACHE n already holds) before the next nextval() must WAL-log and blocks. Those values were already covered by an EARLIER pre-log record that the standbys received, so the other branch's copy of the sequence already sits at or above that high-water mark: the failure mode is skipped values, not duplicated ones. No duplicate-key hazard from rebuilding either branch. Does not break the ADR argument; the client-visible-but-unreplicated values are lost, not divergent. setval() always logs and therefore always waits.
- **Code depending on it:** None.

### [info | refuted | confidence=high] ADR lines docs/concepts/split-brain.md:15, :70 -- tested against two-phase commit
- **Claim:** (Attack vector from the assignment.) Prepared (two-phase) transactions escape the sync-rep wait.
- **Postgres evidence:** Checked source. PG15 twophase.c has exactly three SyncRepWaitForLSN call sites, all with the same guarding comment: EndPrepare -- "Note that at this stage we have marked the prepare, but still show as running in the procarray (twice!) and continue to hold locks." then `SyncRepWaitForLSN(gxact->prepare_end_lsn, false);`; RecordTransactionCommitPrepared -- `SyncRepWaitForLSN(recptr, true);`; RecordTransactionAbortPrepared -- `SyncRepWaitForLSN(recptr, false);`. So PREPARE, COMMIT PREPARED and ROLLBACK PREPARED all wait for the sync quorum.
- **PG15 vs PG17:** Same three call sites in both.
- **Query to settle:** `SELECT count(*) FROM pg_prepared_xacts; -- on both candidate primaries, before any rebuild. Non-zero on the node about to be torn down is a stop sign the ADR does not currently raise.`
- **Impact if wrong:** 2PC does not create a bypass. It does inherit every abnormal-termination hazard above: a cancelled COMMIT PREPARED is acked-but-unreplicated exactly like a cancelled COMMIT. It adds one thing the ADR does not mention: prepared transactions persist in pg_twophase on the node's disk, so rebuilding the higher-TL primary destroys any prepare that only ever existed there, and an external transaction manager holding that gid will find it gone. Low practical relevance unless this cluster uses XA. Also flagging the unverified residual noted above: MarkAsPrepared runs before the wait in EndPrepare, so pg_prepared_xacts may list a prepare whose ack has not returned.
- **Code depending on it:** None.

### [low | partially-correct | confidence=high] ADR lines docs/concepts/split-brain.md:15 ("physically cannot commit"), :76 ("discard/rebuild the higher TL"); docs/adr/002-split-brain-resolution-refinement.md:237
- **Claim:** (Attack vector from the assignment.) ALTER SYSTEM and other configuration changes on the isolated primary.
- **Postgres evidence:** Reasoning from mechanism, stated plainly: ALTER SYSTEM writes postgresql.auto.conf through a temp-file-plus-rename, not through WAL. It is not a transactional catalog change, it is not replicated by physical replication, and it returns success to the client without any involvement of SyncRepWaitForLSN. By contrast CREATE ROLE / ALTER ROLE ... SET / ALTER DATABASE ... SET are ordinary catalog updates, are WAL-logged, assign an xid, and therefore do wait. I did not fetch a doc sentence for the ALTER SYSTEM file-write path; it is well-established behaviour but I am flagging it as reasoned rather than quoted.
- **PG15 vs PG17:** Same in both.
- **Query to settle:** `On each candidate primary: SELECT name, setting, sourcefile, sourceline FROM pg_settings WHERE sourcefile LIKE '%postgresql.auto.conf'; and SELECT * FROM pg_file_settings WHERE name IN ('synchronous_commit','synchronous_standby_names'); -- note pg_file_settings.setting returns the RAW file text and is NOT enum-normalised.`
- **Impact if wrong:** Falsifies the literal sentence "an isolated primary physically cannot commit" -- an isolated primary can accept and acknowledge ALTER SYSTEM all day. Operationally minor (no user data), but not zero: postgresql.auto.conf on the node being rebuilt commonly carries the recovery/replication settings repmgr wrote, and tearing the node down loses them. The genuinely interesting case is the interaction with the critical finding above: ALTER SYSTEM SET synchronous_standby_names = '' followed by SELECT pg_reload_conf() is the documented, sanctioned way to unblock a primary whose standbys are gone (PG15 docs sec.27.2.8: "you should decrease the number of synchronous standbys ... (or disable it) and reload the configuration file on the primary server"), it takes effect immediately, it is acked, and if reverted before the scan it leaves no trace in pg_settings. That is the single most likely real-world route to acked writes on the isolated fork.
- **Code depending on it:** None directly; it bears on the ADR:237 / concepts:76 rebuild instruction.

### [low | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:96 (the gate's input set)
- **Claim:** (Gap found while enumerating, not an ADR claim.) synchronous_commit is the only instance-level setting that can disable the sync-rep wait.
- **Postgres evidence:** PG15 src/include/replication/syncrep.h, verbatim: `#define SyncRepRequested() \ (max_wal_senders > 0 && synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)`. max_wal_senders = 0 disables sync replication entirely regardless of synchronous_commit or synchronous_standby_names -- SyncRepWaitForLSN returns immediately and every commit is acked after a local flush.
- **PG15 vs PG17:** Same macro in both.
- **Query to settle:** `SELECT setting FROM pg_settings WHERE name='max_wal_senders'; -- '0' on any candidate primary voids the sec.2 gate's premise for that node.`
- **Impact if wrong:** A third route past the sec.2 gate, at zero additional collection cost since the value is already in the configuration map. max_wal_senders = 0 on a machine that is a live primary in a replicated cluster is unlikely but not impossible (a bad restart from a stripped config, a node brought up standalone during a fence attempt). Adding `max_wal_senders == 0` and `synchronous_standby_names.is_empty()` to the same finding at split_brain.rs:169 would close two of the three gate holes found in this audit for a few lines of code. Note the macro also confirms the denylist's correctness from the other direction: `> SYNCHRONOUS_COMMIT_LOCAL_FLUSH` is exactly {remote_write, on, remote_apply}, so local and off are correctly refused and remote_write correctly requires the extra fsync argument.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:~165-176 -- 'max_wal_senders' IS already captured in the pg_settings IN-list; src/v2/analyze/split_brain.rs:162-177 does not read it.


## ADR-002 §1 freshness-gate timing model, audited against PostgreSQL 15 walsender/walreceiver source (REL_15_STABLE, fetched and read directly) and PG 1

### [high | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:79-90 (formula at :82, rationale at :85, cadence note at :87); assumption at :28
- **Claim:** "freshness_threshold = wal_sender_timeout_ms / 2 + 30_000 ms" applied to "`wal_receiver.last_msg_receipt_time`" (:70). The threshold is derived from the PRIMARY's `wal_sender_timeout`, but the timestamp it gates is produced and bounded entirely by the REPLICA's `wal_receiver_timeout` / `wal_receiver_status_interval`.
- **Postgres evidence:** Read directly from PG REL_15_STABLE source (curl'd, not memory). walreceiver.c:639-666 -- while streaming, `if (wal_receiver_timeout > 0)`: `timeout = TimestampTzPlusMilliseconds(last_recv_timestamp, wal_receiver_timeout); if (now >= timeout) ereport(ERROR, errmsg("terminating walreceiver due to timeout"))`, and at `last_recv_timestamp + (wal_receiver_timeout / 2)` it sets `requestReply = true; ping_sent = true;` then `XLogWalRcvSendReply(requestReply, requestReply)`. walsender.c:2165-2167 `/* Send a reply if the standby requested one. */ if (replyRequested) WalSndKeepalive(false, InvalidXLogRecPtr);` -- so the ping is what actually elicits primary->standby traffic on an idle link. walreceiver.c:551-552 `last_recv_timestamp = GetCurrentTimestamp(); ping_sent = false;` on every received message -- the same event that sets `lastMsgReceiptTime` (walreceiver.c:1330-1343 ProcessWalSndrMessage). Nothing in the primary's `wal_sender_timeout` enters this loop. Docs (PG15 runtime-config-replication): wal_receiver_timeout default 60 s, wal_receiver_status_interval default 10 s (unit: seconds).
- **PG15 vs PG17:** Verified against REL_15_STABLE, so this is the fleet's exact behaviour. Note PG 16+ refactored this loop into `WalRcvComputeNextWakeup` with WALRCV_WAKEUP_{PING,TERMINATE,REPLY}; I confirmed that symbol does NOT exist in PG 15 walreceiver.c. Semantics (ping at wal_receiver_timeout/2, terminate at wal_receiver_timeout) are the same in both.
- **Query to settle:** `On every replica: SELECT name, setting, unit, source FROM pg_settings WHERE name IN ('wal_receiver_timeout','wal_receiver_status_interval'); -- and add both names to the pg_settings IN-list in src/v2/scan/health_check_replica.rs:108-113 so the resolver can assert threshold > wal_receiver_timeout instead of assuming it.`
- **Impact if wrong:** The design's real soundness condition is `threshold > replica.wal_receiver_timeout` (i.e. `primary.wal_sender_timeout/2 + 30s > replica.wal_receiver_timeout`). On the fleet that is 180s > 60s and holds, so the gate is safe today -- by coincidence of two independently-set GUCs on two different machines, not by construction. It breaks in both directions: raise `wal_receiver_timeout` above 180s (or set it to 0) on any replica and the gate starts falsely rejecting live followers (see the wal_receiver_timeout=0 claim); the invariant is stated nowhere in the ADR, checked nowhere in code, and the replica's GUC is not even captured by the scanner.
- **Code depending on it:** src/v2/analyze/split_brain.rs:266 `let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;` feeding src/v2/analyze/split_brain.rs:284-286 `wr.last_msg_receipt_time.is_some_and(|t| (r_health.current_time - t).num_milliseconds() <= threshold_ms)`

### [high | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:28 and :85 and :87
- **Claim:** "`wal_sender_timeout = 5min` (300_000 ms). Keepalives are sent at `wal_sender_timeout / 2` ≈ 150 s." / "comfortably above the keepalive cadence of ~150 s" / "replica side on keepalive (~150 s in our config)"
- **Postgres evidence:** PG 15 walsender.c:3691-3719, WalSndKeepaliveIfNecessary verbatim: `if (wal_sender_timeout <= 0 || last_reply_timestamp <= 0) return; if (waiting_for_ping_response) return; /* If half of wal_sender_timeout has lapsed WITHOUT RECEIVING ANY REPLY FROM THE STANDBY, send a keep-alive message ... */ ping_time = TimestampTzPlusMilliseconds(last_reply_timestamp, wal_sender_timeout / 2); if (last_processing >= ping_time) { WalSndKeepalive(true, InvalidXLogRecPtr); ... }`. Three things follow: (a) it is not a periodic cadence -- it is keyed off `last_reply_timestamp`, the time of the last message received FROM the standby; (b) the standby sends a status reply at least every `wal_receiver_status_interval` (walreceiver.c:1172 `if (!force && wal_receiver_status_interval <= 0) return;` and :1188-1191 `!TimestampDifferenceExceeds(sendTime, now, wal_receiver_status_interval * 1000)`), default 10 s, so `last_reply_timestamp` is refreshed ~every 10 s and the 150 s condition is NEVER reached on a healthy connection; (c) `waiting_for_ping_response` latches, so even on a dead link exactly ONE unsolicited keepalive is sent (at 150 s), never a stream of them. Corroborated in-tree by walsender.c:2443-2452 (WalSndCheckTimeOut comment): "This rarely affects the default configuration, under which clients spontaneously send a message every standby_message_timeout = wal_sender_timeout/6 = 10s." The divisor is /2; the cap the prompt refers to is walsender.c:2410-2412 `long sleeptime = 10000; /* 10 s */` in WalSndComputeSleeptime, which is the wake-up floor used only when `wal_sender_timeout <= 0` -- it is a scheduler granularity, not a send cadence.
- **PG15 vs PG17:** Verified on REL_15_STABLE; WalSndKeepaliveIfNecessary is unchanged in substance from 9.5 through 17, so the refutation holds on 15.14 and on 17.
- **Query to settle:** `Empirical: on an idle cluster, SELECT now() - last_msg_receipt_time AS age, status FROM pg_stat_wal_receiver; sampled every 5 s for 10 min. Predicted max age ~= wal_receiver_timeout/2 (~30 s), NOT ~150 s.`
- **Impact if wrong:** ~150 s is not the right number for wal_sender_timeout=300 s. The real driver of `last_msg_receipt_time` on an idle link is wal_receiver_timeout/2 = ~30 s (default), and on any link with write traffic it is sub-second (fixture: last_msg_receipt_time is 37 ms before current_time). The formula's numerator therefore has no causal relationship to the quantity being measured; 180 s is a right-ish answer reached by a wrong derivation, which is exactly the kind of thing that silently stops being right when someone tunes a GUC.
- **Code depending on it:** src/v2/analyze/split_brain.rs:266 (the /2 + 30_000 formula exists solely to clear this asserted 150 s cadence)

### [high | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:85
- **Claim:** "this yields ~180 s -- comfortably above the keepalive cadence of ~150 s, with 30 s of slack for scan jitter"
- **Postgres evidence:** Taken on its own terms the arithmetic is not 'comfortable': 30 s of slack against a claimed 150 s period is 0.2 of one period, i.e. the design tolerates ZERO missed or delayed keepalives -- if one 150 s keepalive is lost, the next observation of `last_msg_receipt_time` is ~300 s old and the gate falsely rejects a healthy follower. What actually saves it is that the premise is wrong (previous claim): with wal_receiver_timeout=60000 the ping period is 30 s and, per walreceiver.c:645-651, the walreceiver ERRORs out and the process dies at 60 s of silence -- so a live `pg_stat_wal_receiver` row can never show an age above ~60 s + NAPTIME_PER_CYCLE (walreceiver.c:99 `#define NAPTIME_PER_CYCLE 100 /* max sleep time between cycles (100ms) */`). Real margin is 180 - 60 = 120 s, i.e. 4 ping periods, not 30 s. Scan-jitter framing is also misdirected: the comparison at split_brain.rs:285 is `r_health.current_time - t`, both taken inside the same node, so scanner queueing/fan-out delay does not enter the replica-side arithmetic at all.
- **PG15 vs PG17:** Holds on 15.14; NAPTIME_PER_CYCLE and the ping/terminate arithmetic are identical in 17.
- **Query to settle:** `SELECT setting FROM pg_settings WHERE name='wal_receiver_timeout'; on each replica, then assert (wal_sender_timeout_of_primary/2 + 30000) > that value.`
- **Impact if wrong:** Quantified answer to 'what happens to a scan landing in the wrong part of the cycle': under the fleet's actual config, nothing -- the row cannot be stale enough to trip the gate. Under the ADR's own stated model it would be a coin-flip on one dropped packet. Under a hypothetical replica with wal_receiver_timeout=600000 (10 min), the ping period is 300 s, ages oscillate 0->300 s, and a scan landing in the last 120 s of each cycle falsely rejects a healthy follower -- a 40% false-rejection duty cycle on an idle cluster.
- **Code depending on it:** src/v2/analyze/split_brain.rs:266, :284-286

### [high | refuted | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:70 (gate input) and :47 (case C-d, "OR `last_msg_receipt_time` aged out")
- **Claim:** "`wal_receiver.last_msg_receipt_time` is within `freshness_threshold`" is the authoritative half of the gate -- ADR:20 calls it "the test that separates the two" (live stream vs stale row).
- **Postgres evidence:** With the fleet's threshold (180 s) and default `wal_receiver_timeout` (60 s), the replica-side freshness predicate is unfalsifiable. Proof from source: `pg_stat_wal_receiver` has a row only while a walreceiver process exists (system_views.sql:907-925, `FROM pg_stat_get_wal_receiver() s WHERE s.pid IS NOT NULL`); the walreceiver kills itself at `last_recv_timestamp + wal_receiver_timeout` (walreceiver.c:643-651); and `last_recv_timestamp` is reset by the same message events that set `lastMsgReceiptTime` (walreceiver.c:551-552 vs :1334-1342). Therefore for any row that exists, `now() - last_msg_receipt_time < wal_receiver_timeout` = 60 s < 180 s, always. C-d's "`last_msg_receipt_time` aged out" branch is unreachable, and the 'authoritative' side of the gate degenerates to exactly `sender_host == ip && sender_port == 5432 && status in (streaming, catchup)` -- i.e. the check ADR:64 set out to replace, plus a port equality.
- **PG15 vs PG17:** Holds on 15.14. Same in 17 (the view's `WHERE s.pid IS NOT NULL` filter and the terminate-on-timeout path are unchanged).
- **Query to settle:** `Kill the primary's network ungracefully (iptables DROP) on a lab standby and poll: SELECT now(), pid, status, now()-last_msg_receipt_time FROM pg_stat_wal_receiver; -- predicted: the row VANISHES at ~wal_receiver_timeout, it never appears as an aged-out streaming row.`
- **Impact if wrong:** Not a wrong verdict on its own -- it fails toward accepting real followers, which is the safe direction for C-b/C-c. But the ADR's safety argument for the whole gate rests on this predicate, and it does not do the work claimed. The one case it WOULD catch is a walreceiver whose row is immortal (wal_receiver_timeout=0), which is precisely the config where the primary also stops sending -- see the next claim. Worth noting the inversion: the 'corroborating' primary side is the only side whose freshness predicate can actually fire, because the zombie `pg_stat_replication` row outlives the threshold (300 s row lifetime vs 180 s threshold).
- **Code depending on it:** src/v2/analyze/split_brain.rs:281-286 (the `replica_passes` conjunction) and the `ReplicaWalReceiverStale` emission at :293-298

### [medium | partially-correct | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:87 ("A symmetric threshold is generous on the primary side; this is acceptable for v1") and :56 (gate precedence)
- **Claim:** asymmetric cadences -- "primary side updates on `wal_receiver_status_interval` (~10 s)" -- and a symmetric 180 s threshold is merely "generous".
- **Postgres evidence:** The ~10 s cadence claim is CORRECT for the primary side: walreceiver.c:1172,:1188-1191 sends a reply whenever `wal_receiver_status_interval` (default 10 s, docs) has elapsed, even fully idle, and PG15 docs add "There are additional cases where updates are sent while ignoring this parameter; for example ... when synchronous_commit is set to remote_apply" -- which is this fleet's setting (fixture: "synchronous_commit": "remote_apply"), plus the `walrcv->force_reply` path at walreceiver.c:609-618. So reply_time is normally <=10 s old, often sub-second. The understatement: `reply_time` freezes at the last reply and the walsender survives until `last_reply_timestamp + wal_sender_timeout` (walsender.c:2454-2478 WalSndCheckTimeOut, 300 s here). So a connection can be dead for up to 180 s and still pass the primary-side check -- 18x the natural cadence, a 170 s false-pass window -- and the row remains visible-but-failing for the following 120 s.
- **PG15 vs PG17:** Holds on 15.14; wal_receiver_status_interval semantics and WalSndCheckTimeOut are unchanged in 17.
- **Query to settle:** `On a lab primary after freezing the standby VM: SELECT now(), application_name, state, now()-reply_time AS reply_age FROM pg_stat_replication; poll every 10 s -- predicted: reply_age grows linearly, row disappears at ~wal_sender_timeout (300 s).`
- **Impact if wrong:** The precedence order DOES save us, and I confirmed it in code: src/v2/analyze/split_brain.rs:288-300 returns via `continue` before the `pg_stat_replication` lookup at :305, so a stale primary-side row can never manufacture a follower on its own. C-e (ADR:45) therefore resolves as written. The residual exposure is only where BOTH sides are frozen at the same instant (partition with wal_receiver_timeout=0 on the replica), where the AND provides no independence -- both sides pass for the first 180 s and then the primary side fails first at 180 s (replica row is immortal, primary row is not). So 'generous' is right in direction but off by an order of magnitude in size, and the ADR should say 'the primary side accepts up to 180 s of death against a 10 s cadence'.
- **Code depending on it:** src/v2/analyze/split_brain.rs:312-314 `conn.reply_time.is_some_and(|t| (p_health.current_time - t).num_milliseconds() <= threshold_ms)`

### [medium | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:75 ("The row's `reply_time` is within `freshness_threshold` of the scan-start timestamp")
- **Claim:** Implicit in the implementation's doc comment (split_brain.rs:242-244): "Freshness uses each node's own `current_time` against that same node's recorded timestamps, so the comparison is intra-node and immune to scanner<->db clock skew".
- **Postgres evidence:** True for the replica side, FALSE for the primary side. `pg_stat_replication.reply_time` is not a primary-side timestamp: PG15 docs define it as "Send time of last reply message received from standby server", and the source confirms it is carried in the wire message from the standby -- walreceiver.c:1200-1206 `pq_sendbyte(&reply_message, 'r'); ... pq_sendint64(&reply_message, GetCurrentTimestamp());` (STANDBY clock) -> walsender.c:2120 `replyTime = pq_getmsgint64(&reply_message);` -> walsender.c:2186 `walsnd->replyTime = replyTime;` -> exposed unmodified at walsender.c:3644-3647. By contrast `last_msg_receipt_time` IS standby-local: walreceiver.c:1334 `TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();` (while `last_msg_send_time` at :1341 is the primary's clock, taken from the message header written by walsender.c:3675 `pq_sendint64(&output_message, GetCurrentTimestamp())`).
- **PG15 vs PG17:** Verified on REL_15_STABLE; identical in 17. The doc wording "Send time of last reply message received from standby server" is the same in both docs.
- **Query to settle:** `On the primary: SELECT application_name, reply_time, now() AS primary_now, now()-reply_time AS apparent_age FROM pg_stat_replication; and simultaneously on the standby: SELECT now(); -- any apparent_age materially below zero or above the round-trip time is clock skew, not staleness. Longer term: chronyc tracking / timedatectl on all three nodes.`
- **Impact if wrong:** A replica whose clock runs slow by more than 180 s makes every live connection fail the primary-side gate -> `PrimaryDoesNotSeeReplica` -> replica not counted as following -> in case C-b the verdict flips from `LowerTimelineHasQuorum(db001)` to `HigherTimeline(db002)`, i.e. the destructive direction. Fleet evidence bounds the current risk tightly: in tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json the round trip is visible -- db001 sends at 18:57:24.562477 (primary clock), db002 records receipt at 18:57:24.562819 (standby clock, 342 us later), replies at 18:57:24.563432 (standby clock), and db001 stores exactly that value as reply_time. One-way delta of 342 us bounds |skew| below ~1 ms. So this is a correctness hole in the stated safety argument, not a live hazard -- but the doc comment should not claim immunity it does not have, because it is the thing a future reader will rely on when tightening the threshold.
- **Code depending on it:** src/v2/analyze/split_brain.rs:313 `(p_health.current_time - t).num_milliseconds()` where `t` is `conn.reply_time` -- primary's now() minus a standby-generated timestamp; and the doc comment at :242-244 that asserts this cannot happen

### [high | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:82,85 (formula and "default 60_000 ms if missing/malformed")
- **Claim:** The formula `wal_sender_timeout_ms / 2 + 30_000` is the right threshold for any parsed value, including the timeout-disabled case.
- **Postgres evidence:** `wal_sender_timeout = 0` is legal (guc.c min is 0; PG15 docs: "A value of zero disables the timeout mechanism"). `parse_wal_sender_timeout` parses "0" successfully (it is not malformed), so the fallback does not apply and the formula yields 0/2 + 30_000 = 30 s -- the TIGHTEST threshold the formula can produce, in exactly the configuration where the primary is least chatty: walsender.c:3699 `if (wal_sender_timeout <= 0 ... ) return;` means unsolicited keepalives are disabled entirely. Primary->standby traffic on an idle cluster then depends solely on the standby's own ping at `wal_receiver_timeout/2` = 30 s (default), so `now() - last_msg_receipt_time` sweeps 0 -> 30 s and the predicate `<= 30_000` is marginal by construction; a scan landing just before a ping rejects a perfectly healthy follower.
- **PG15 vs PG17:** Holds identically on 15.14 and 17; the `wal_sender_timeout <= 0` early return in WalSndKeepaliveIfNecessary is present in both.
- **Query to settle:** `SELECT name, setting, unit, boot_val, source FROM pg_settings WHERE name='wal_sender_timeout'; on every candidate primary -- confirm no node has 0. A unit test asserting parse+formula on "0" would pin the behaviour.`
- **Impact if wrong:** False `ReplicaWalReceiverStale` -> the replica stops counting as a follower of the lower-TL primary -> C-b/C-c collapse to `HigherTimeline` -> operator is pointed at demoting/fencing the primary that holds the acked writes. Reachable in code today; NOT reachable on the current fleet (fixture shows "wal_sender_timeout": "300000" on db001). The formula is inverted for the disabled case: with timeouts off the walsender never reaps zombie rows at all, so the correct response is a threshold derived from the replica's cadence, or an explicit refusal to gate on freshness.
- **Code depending on it:** src/v2/analyze/split_brain.rs:608-612 `fn parse_wal_sender_timeout` (`.and_then(|s| s.parse().ok()).unwrap_or(60_000)`) and :266

### [high | partially-correct | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:70,85 (the replica-side freshness input and its threshold)
- **Claim:** Implied: a `wal_receiver` row that is fresher than the threshold means the streaming connection is live.
- **Postgres evidence:** Sound at default `wal_receiver_timeout`, unsound at `wal_receiver_timeout = 0`. With 0, walreceiver.c:639 `if (wal_receiver_timeout > 0)` skips BOTH the terminate check and the ping, so the walreceiver never dies and never solicits a keepalive. The standby still replies every `wal_receiver_status_interval`, which keeps the primary's `last_reply_timestamp` fresh, which means walsender.c:3705-3712 never fires an unsolicited keepalive either. On a genuinely idle cluster no WAL is produced (bgwriter's periodic LogStandbySnapshot is itself gated on new important WAL), so the primary sends the standby NOTHING and `last_msg_receipt_time` freezes on a healthy connection. After 180 s the gate declares a live follower stale.
- **PG15 vs PG17:** Holds on 15.14 as read. In PG 16+ the equivalent guard is `if (wal_receiver_timeout <= 0) wakeup[reason] = TIMESTAMP_INFINITY;` inside WalRcvComputeNextWakeup -- same semantics, different shape; PG 15 has the inline form quoted above.
- **Query to settle:** `SELECT setting FROM pg_settings WHERE name='wal_receiver_timeout'; on db002 and db003 (and every replica in the fleet). If any returns '0' or anything >= 180000, this is live, not hypothetical.`
- **Impact if wrong:** Same destructive direction as the wal_sender_timeout=0 case: false `ReplicaWalReceiverStale(db003, db001)` -> C-b degrades to C-d -> `HigherTimeline(db002)` -> fence db001, which is the node holding acked writes. Reachable in code. Reachable on fleet: UNKNOWN and currently unknowable from a scan, because `wal_receiver_timeout` is not in the replica health check's pg_settings IN-list (src/v2/scan/health_check_replica.rs:108-113 collects only hot_standby, primary_conninfo, primary_slot_name, recovery_target_timeline). I am explicitly not asserting this happens on the fleet.
- **Code depending on it:** src/v2/analyze/split_brain.rs:284-286 and :293-298

### [medium | partially-correct | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:89 ("zombie rows hold fresh values until `wal_sender_timeout` fires") and :45 (C-e, "within `wal_sender_timeout` of disconnect")
- **Claim:** "raw `flush_lsn` freshness (zombie rows hold fresh values until `wal_sender_timeout` fires)" -- the mechanism the gate is built to defeat.
- **Postgres evidence:** CORRECT for `pg_stat_replication`, and only for UNGRACEFUL peer loss. Ungraceful (partition, VM freeze): the walsender blocks on nothing, keeps its backend and therefore its row (PG15 docs: "one row per WAL sender process"), and all reported columns freeze at their last values -- sent/write/flush/replay_lsn and reply_time keep the values from the last reply, which on a caught-up cluster equal the primary's current LSN, i.e. they look perfectly fresh. The row is reaped at `last_reply_timestamp + wal_sender_timeout` (walsender.c:2454-2478, `ereport(COMMERROR, errmsg("terminating walsender process due to replication timeout")); WalSndShutdown();`) = 300 s here. Graceful (standby shut down / repointed by a restart): the socket closes, ProcessRepliesIfAny sees EOF/'X', the walsender exits within milliseconds and the row disappears -- there is no zombie window at all. REFUTED for `pg_stat_wal_receiver`: that row does not persist for `wal_sender_timeout`; it persists for at most the REPLICA's `wal_receiver_timeout` and then vanishes entirely (walreceiver.c:643-651 plus system_views.sql:925 `WHERE s.pid IS NOT NULL`).
- **PG15 vs PG17:** Verified on REL_15_STABLE; identical in 17.
- **Query to settle:** `Lab: (a) graceful -- pg_ctl stop the standby, then on the primary SELECT count(*) FROM pg_stat_replication; expect 0 within ~1 s. (b) ungraceful -- iptables -I INPUT -s <standby> -j DROP, then poll SELECT now()-reply_time, flush_lsn FROM pg_stat_replication; expect frozen flush_lsn, growing reply_time age, row gone at ~300 s. Simultaneously on the standby poll SELECT * FROM pg_stat_wal_receiver; expect the row gone at ~60 s.`
- **Impact if wrong:** Two consequences. (1) The comment misdirects the next maintainer to the primary's knob when tuning the replica-side gate -- the same conflation that produced the formula. (2) C-e's stale-row window is much narrower than 'wal_sender_timeout' implies whenever the repoint was done by repmgr with a standby restart (graceful close -> no stale row at all), so C-e as written describes the partition case only. Neither changes a verdict; the gate's design intent survives on the primary side.
- **Code depending on it:** src/v2/analyze/split_brain.rs:276-280, whose comment states the refuted half as fact: "a dead `wal_receiver` keeps its values until `wal_sender_timeout` fires, so without this check `status=\"streaming\"` alone would pass on a connection that just died" -- wrong GUC (`wal_receiver_timeout`), wrong side (replica, not primary), wrong default (60 s, not 300 s).

### [medium | refuted | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:70 and :75 (both say "within `freshness_threshold` of the scan-start timestamp"); see also :225
- **Claim:** "`wal_receiver.last_msg_receipt_time` is within `freshness_threshold` of the scan-start timestamp" / "The row's `reply_time` is within `freshness_threshold` of the scan-start timestamp" / ":225 Pass scan-start `DateTime<Utc>` from `analyze_clusters` through `analyze()` into `resolve_split_brain()` as a parameter"
- **Postgres evidence:** Not a Postgres question, but it changes the timing model so it belongs here. The implementation uses each node's own `now()` instead: `resolve_split_brain` takes no scan-start parameter, and the comparisons are `(r_health.current_time - t)` (split_brain.rs:285) and `(p_health.current_time - t)` (split_brain.rs:313), where `current_time` comes from `'current_time', (SELECT now())` in each node's health-check query (health_check_primary.rs:147, health_check_replica.rs:61). Postgres `now()` is transaction start time, so it is the instant that node's health-check transaction began.
- **PG15 vs PG17:** n/a -- `now()` = transaction start timestamp in both.
- **Query to settle:** `n/a -- settled by reading src/v2/analyze/split_brain.rs:245-330 and src/v2/analyze.rs wiring; the ADR text is what needs changing.`
- **Impact if wrong:** The implemented choice is BETTER than the ADR's on the replica side -- it removes scanner->db clock skew and scan fan-out delay from an already-tight comparison -- and the code comment at :242-244 correctly says so. It does not help the primary side, because `reply_time` is standby-generated (separate claim). The ADR text at :70/:75/:225 is stale relative to the code and should be rewritten to describe the intra-node comparison, otherwise the next person 'fixes' the code back to the worse design. Note the current fixture cannot referee this: all three nodes report current_time as exactly 2025-09-20T18:57:24.600000Z, which is normalized, while the surrounding sub-millisecond timestamps look genuinely captured.
- **Code depending on it:** src/v2/analyze/split_brain.rs:285 and :313; the absent parameter on `resolve_split_brain`

### [low | partially-correct | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:85
- **Claim:** "`wal_sender_timeout_ms` is parsed from each primary's `configuration[\"wal_sender_timeout\"]`, which `pg_settings` returns as raw milliseconds with no unit suffix; default 60_000 ms if missing/malformed."
- **Postgres evidence:** Correct for this GUC, over-general as a statement about pg_settings. `pg_settings.setting` returns the value in the GUC's own base unit, reported in `pg_settings.unit`; `wal_sender_timeout` is declared GUC_UNIT_MS with boot value 60000, so `setting` is a bare integer string of milliseconds -- matching the fixture's "wal_sender_timeout": "300000" for a 5min setting, and matching the code's 60_000 fallback (which is exactly PG's documented default: "The default value is 60 seconds"). But the general form is false: `wal_receiver_status_interval` is GUC_UNIT_S, so its `setting` is "10" meaning 10 SECONDS, and `SHOW wal_sender_timeout` (or `current_setting()`) returns '5min', not '300000'.
- **PG15 vs PG17:** Same in 15.14 and 17 (GUC_UNIT_MS on wal_sender_timeout, default 60000, min 0).
- **Query to settle:** `SELECT name, setting, unit, boot_val FROM pg_settings WHERE name IN ('wal_sender_timeout','wal_receiver_timeout','wal_receiver_status_interval'); -- expect ('wal_sender_timeout','300000','ms',...) and ('wal_receiver_status_interval','10','s',...).`
- **Impact if wrong:** No behavioural defect today. The risk is the silent fallback: any future change to how configuration is collected that yields a unit-suffixed string degrades the threshold from 180 s to 60 s without any finding being emitted. If ADR-002 wants a general rule it should say 'pg_settings.setting is expressed in pg_settings.unit; wal_sender_timeout's unit is ms'.
- **Code depending on it:** src/v2/analyze/split_brain.rs:608-612; src/v2/scan/health_check_primary.rs:145-176 (the query reads `setting` from pg_settings, which is the correct column -- reading via SHOW/current_setting would return '5min', fail `.parse::<i64>()`, and silently fall back to 60_000, i.e. a 60 s threshold with no diagnostic)

### [info | partially-correct | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:89
- **Claim:** "`flush_lag` (stops updating on idle clusters)" -- given as the reason flush_lag is rejected as a gate input.
- **Postgres evidence:** The conclusion is right and the mechanism is slightly stronger than stated: on an idle, fully caught-up standby the lag columns are not merely frozen, they are actively CLEARED to NULL. walsender.c:2237-2240 `clearLagTimes = (applyPtr == sentPtr && flushPtr == sentPtr && writePtr == prevWritePtr && flushPtr == prevFlushPtr && applyPtr == prevApplyPtr);` and :2179-2184 `if (flushLag != -1 || clearLagTimes) walsnd->flushLag = flushLag;` with LagTrackerRead returning -1 (rendered as NULL) when there is no new sample; the in-tree comment says this "avoids displaying stale lag data until more WAL traffic arrives."
- **PG15 vs PG17:** Same in 15.14 and 17.
- **Query to settle:** `On an idle cluster: SELECT application_name, write_lag, flush_lag, replay_lag FROM pg_stat_replication; -- expect NULLs.`
- **Impact if wrong:** None. Rejecting flush_lag is correct; the ADR could sharpen 'stops updating' to 'is set to NULL when the standby is caught up and idle', which is a stronger argument for rejection.
- **Code depending on it:** nothing -- flush_lag is correctly not a gate input; src/v2/scan/health_check_primary.rs:195 captures it for display only

### [info | confirmed | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:213
- **Claim:** "**(Validated 2026-09-10.)** Both functions return SQL **NULL**, never `0/0`, when their position is zero: PG17 `xlogfuncs.c` guards each with `if (recptr == 0) PG_RETURN_NULL();` ahead of `PG_RETURN_LSN(recptr)`."
- **Postgres evidence:** Out of my assigned section, but the schema asks the PG15-vs-PG17 question and this is the ADR's only version-annotated claim, so I checked it: PG REL_15_STABLE src/backend/access/transam/xlogfuncs.c:288-298 `pg_last_wal_receive_lsn(PG_FUNCTION_ARGS) { XLogRecPtr recptr; recptr = GetWalRcvFlushRecPtr(NULL, NULL); if (recptr == 0) PG_RETURN_NULL(); PG_RETURN_LSN(recptr); }` and :307-317 the identical shape for `pg_last_wal_replay_lsn` with `GetXLogReplayRecPtr(NULL)`.
- **PG15 vs PG17:** Identical in both. The PG17 citation is accurate but does not by itself establish the fleet's behaviour; direct PG 15 verification does.
- **Query to settle:** `On a promoted primary: SELECT pg_last_wal_receive_lsn() IS NULL, pg_last_wal_replay_lsn() IS NULL; -- ADR:215 already records 'f' for the first, consistent with the source.`
- **Impact if wrong:** None -- the annotation cites PG17 but the guard is byte-identical in 15.14, so `Option<String>` is the right shape on the fleet. The annotation should say 'unchanged from PG 12 through 17' rather than naming a version the fleet does not run; as written, a reader on 15.14 has to re-verify it (I did).
- **Code depending on it:** src/v2/scan/health_check_replica.rs:29-33 (`last_wal_replay_lsn` / `last_wal_receive_lsn` typed as `Option<String>`)


## Postgres behaviour audit: timeline-history file naming, content format, and the pg_read_file capture (ADR-002 §5 + src/v2/scan/health_check_primary.rs

### [high | refuted | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:202 -- "returns NULL if the file doesn't exist (e.g. during a fresh promotion window between TL bump and history-file write)"
- **Claim:** There is a promotion window in which the timeline has been bumped but the .history file has not yet been written, so pg_read_file could see a missing file.
- **Postgres evidence:** Reasoning from memory of src/backend/access/transam/xlog.c (StartupXLOG) and timeline.c -- I could NOT read those .c files here; the only Postgres source on this machine is a headers-only pg_query vendor tree (/home/robert.sjoblom@fnox.it/9th/pg-migration-lint/target/debug/build/pg_query-7436f49853ae04a4/out/src/postgres/include, PG_VERSION 17.7), which contains no xlog.c/timeline.c/genfile.c. From memory the order is the reverse of the ADR's: StartupXLOG calls writeTimeLineHistory(newTLI, recoveryTargetTLI, EndRecPtr, reason) FIRST -- with the in-tree comment "Write the timeline history file, and have it archived. ... To minimize the window for that, try to do as little as possible between here and writing the end-of-recovery record" -- and writeTimeLineHistory() fsyncs and durable_rename()s the file before returning. Only afterwards is newTLI published (XLogCtl->InsertTimeLineID), the end-of-recovery record written, and (later, asynchronously) a checkpoint on the new TL created. The filename in this query comes from pg_control_checkpoint().timeline_id = ControlFile->checkPointCopy.ThisTimeLineID, which is the LAST source to learn the new TL. So the file is durable strictly before the query can name it.
- **PG15 vs PG17:** Holds on 15.14. The fast-promotion path (end-of-recovery record + deferred checkpoint) and the writeTimeLineHistory-before-TL-publication ordering are unchanged between 15 and 17; the recovery code moved from xlog.c to xlogrecovery.c in 15 but the sequence did not change.
- **Query to settle:** `-- on a lab standby, run in a tight loop across `pg_ctl promote`: SELECT clock_timestamp(),        (SELECT timeline_id FROM pg_control_checkpoint())                       AS control_tli,        substring(pg_walfile_name(pg_current_wal_lsn()) FROM 1 FOR 8)           AS insert_tli_hex,        (pg_stat_file('pg_wal/' || lpad(upper(to_hex((SELECT timeline_id FROM pg_control_checkpoint()) + 1)), 8, '0') || '.history', true)).size AS next_history_size; -- expect: next_history_size becomes non-NULL BEFORE control_tli advances.`
- **Impact if wrong:** The missing_ok=true flag itself is harmless either way, so no behaviour breaks. What breaks is the reasoning: the ADR's stated safety window is fictional, and the REAL skew runs the opposite way (see the next claim). An operator or future implementer who trusts this sentence will conclude the captured timeline_history is at worst absent, when it can instead be silently the WRONG (parent) timeline's file.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:155 -- "0, (1024 * 1024)::bigint, true" (the missing_ok=true argument)

### [high | refuted | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:186 ("Timeline-history file contents for the current TL") and 189-199 (the SQL block)
- **Claim:** pg_control_checkpoint().timeline_id names the node's CURRENT timeline, so 'pg_wal/<that TL>.history' is the current timeline's history file.
- **Postgres evidence:** pg_control_checkpoint().timeline_id is ControlFile->checkPointCopy.ThisTimeLineID -- the timeline of the LAST COMPLETED CHECKPOINT (this is the field pg_controldata prints as "Latest checkpoint's TimeLineID"), not the running/insert timeline. Reasoning from memory of xlog.c: after a fast promotion StartupXLOG writes an end-of-recovery record (which updates only minRecoveryPoint/minRecoveryPointTLI) and then issues RequestCheckpoint(CHECKPOINT_FORCE) -- FORCE without IMMEDIATE or WAIT, i.e. an asynchronous SPREAD checkpoint. Until the checkpointer finishes it (up to checkpoint_timeout * checkpoint_completion_target, i.e. minutes on default settings), pg_control_checkpoint().timeline_id still reports the PRE-promotion TL. The same lag exists on a standby, whose control file is updated by restartpoints replaying the upstream's checkpoints. I could not read xlog.c here to quote it.
- **PG15 vs PG17:** Holds on 15.14. checkPointCopy.ThisTimeLineID semantics and the deferred post-promotion checkpoint are the same in 15 and 17. Neither version offers a plain SQL 'current timeline' function; pg_walfile_name(pg_current_wal_lsn()) is the available proxy on a primary (it errors during recovery).
- **Query to settle:** `-- on every primary, right now, and again within 60s of any promotion: SELECT (SELECT timeline_id FROM pg_control_checkpoint())             AS control_checkpoint_tli,        substring(pg_walfile_name(pg_current_wal_lsn()) FROM 1 FOR 8) AS insert_tli_hex,        (SELECT checkpoint_time FROM pg_control_checkpoint())         AS last_ckpt,        now() - (SELECT checkpoint_time FROM pg_control_checkpoint()) AS ckpt_age; -- control_checkpoint_tli must equal to_number(insert_tli_hex, 'XXXXXXXX')`
- **Impact if wrong:** Two things break at once, and both are load-bearing. (1) SECTION 7: on a freshly promoted higher-TL primary the query reads the PARENT timeline's .history, which contains no line for the node's actual current timeline -- i.e. exactly the fork line §7 needs to compare a replica's flushed_lsn against is the one missing, and the capture looks successful (non-NULL string, well-formed) while being the wrong file. (2) THE VERDICT ITSELF: get_timeline() (src/v2/analyze.rs:354-359) returns this same field, and extract_timeline_info (src/v2/analyze/split_brain.rs:189-200) ranks primaries by it. During the post-promotion checkpoint window the new primary reports the OLD TL, so a zombie primary and the true new primary look EQUAL-timeline -- collapsing HigherTimeline/Both into ReplicaFollowing or Indeterminate. Reachable in code: yes. Reachable on our fleet: unknown -- it depends on how long the post-promotion spread checkpoint takes on our hardware, and we have no captured post-failover run to measure it. ADR-002 scopes out "in-flight failover", but "the first few minutes after promotion" is precisely when a post-failover scan gets run.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:146 -- "WITH cc AS (SELECT timeline_id FROM pg_control_checkpoint())", consumed at :149 ('timeline_id' JSON key) and :154 (the .history filename)

### [high | partially-correct | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:204 ("Privileges: requires pg_read_server_files, which is granted in production"), :31 ("Scanner role has pg_read_server_files ... this privilege is in place"), :262 ("deployment must have pg_read_server_files granted (already true in production)")
- **Claim:** The capture requires pg_read_server_files, and that privilege is in place in production.
- **Postgres evidence:** Two separate gates, and the ADR names only one. (a) PATH CHECK: genfile.c convert_and_check_filename() short-circuits for members of pg_read_server_files, but for everyone else it still ALLOWS a relative path with no parent reference below the data directory (path_is_relative_and_below_cwd). 'pg_wal/0000000B.history' passes that check for ANY role -- so pg_read_server_files is not needed for the path. (b) EXECUTE PRIVILEGE: src/backend/catalog/system_functions.sql REVOKEs EXECUTE ON FUNCTION pg_read_file(text,bigint,bigint,boolean) FROM public and GRANTs it TO pg_read_server_files. That is the gate that actually bites. pg_monitor (ROLE_PG_MONITOR = 3373 in the vendored catalog/pg_authid_d.h:47) is a DIFFERENT predefined role from pg_read_server_files (ROLE_PG_READ_SERVER_FILES = 4569, pg_authid_d.h:51) and does not confer it. I am citing system_functions.sql from memory -- that file is not in the vendored tree. Separately: there is NO captured evidence the grant exists on our fleet. The only fixture (tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json) got its timeline_history value hand-added in commit c8d83c8 "chore: add timeline_history to JSON fixture", not from a scan.
- **PG15 vs PG17:** Holds on 15.14. The pg_read_server_files predefined role and the system_functions.sql GRANTs date from PG 11; convert_and_check_filename's relative-path allowance is unchanged in 15 and 17.
- **Query to settle:** `-- run as the scanner role on every node: SELECT current_user,        has_function_privilege(current_user, 'pg_read_file(text,bigint,bigint,boolean)', 'execute') AS can_exec_4arg,        pg_has_role(current_user, 'pg_read_server_files', 'member') AS in_read_server_files,        pg_has_role(current_user, 'pg_monitor', 'member')           AS in_pg_monitor;  SELECT pg_read_file('pg_wal/' || lpad(upper(to_hex((SELECT timeline_id FROM pg_control_checkpoint()))), 8, '0') || '.history', 0, 1048576, true) IS NOT NULL AS history_readable;`
- **Impact if wrong:** missing_ok=true suppresses ONLY ENOENT. An insufficient-privilege error (or any other error) aborts the whole statement, so execute_primary_health_check returns Err and the node becomes Role::UnknownPrimary. Role::is_primary() (src/v2/scan.rs:333-335) matches only Role::Primary, so that node drops out of cluster.primaries(). In a real split-brain where the grant is missing on one node, primaries.len() falls to 1 and analyze() (src/v2/analyze.rs:317-321) never reaches resolve_split_brain -- the split-brain verdict is silently NOT produced. A privilege misconfiguration therefore converts "two primaries" into "looks like one primary". The blast radius of the history read is the ENTIRE primary health check, not just the history field.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:153-156 (the pg_read_file call, inside the single jsonb_build_object statement) -> src/v2/scan/health_check_primary.rs:288-292 (execute_primary_health_check -> query_one -> errors::pg_err) -> :253-266 (Err arm sets Role::UnknownPrimary)

### [high | confirmed | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:204 applied to tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json (timeline_id 11 at :15, last_archived_wal "00000011000004850000003F" at :79, timeline_history at :18)
- **Claim:** The trap: the fixture is on timeline_id 11 and its last archived WAL is 00000011000004850000003F -- is 11 rendered there as hex 0B or as the digits 11, and what is the real history filename on that node?
- **Postgres evidence:** The ADR's %08X rule wins; the fixture is wrong. Per xlog_internal.h:166-170 (vendored PG 17.7) XLogFileName is snprintf("%08X%08X%08X", tli, log, seg), and IsXLogFileName (:180-184) requires strspn(fname,"0123456789ABCDEF")==24 -- the TLI field is UPPERCASE HEX. Decoding "00000011000004850000003F": TLI = 0x11 = 17 decimal, logid = 0x485, segment = 0x3F (segment start LSN 485/3F000000). TLI 17 cannot coexist with a control file reporting TL 11 -- timeline IDs only increase, so an archived file can be from an OLDER TL, never a newer one. Three further internal contradictions confirm the archiver block is synthetic, not captured: archived_count is 1523 (~1523 segments ever archived) but the named file is segment #1,860,159 of its timeline; last_archived_time 18:55:00 is 2m24s before current_time 18:57:24, yet current_wal_lsn 48F/6957B540 is ~42 GB past that segment's start; and the 10-line timeline_history (prev_tli 1..10) matches TL 11, not TL 17. Someone wrote the DECIMAL digits "11" into a hex field.
- **PG15 vs PG17:** Identical on 15.14. XLogFileName/TLHistoryFileName have used %08X since 9.0; the vendored 17.7 header is authoritative for both.
- **Query to settle:** `-- on dev-pg-app001-db001: SELECT (SELECT timeline_id FROM pg_control_checkpoint()) AS tli,        lpad(upper(to_hex((SELECT timeline_id FROM pg_control_checkpoint()))), 8, '0') || '.history' AS expected_history_file,        last_archived_wal,        substring(last_archived_wal FROM 1 FOR 8) AS archived_tli_hex   FROM pg_stat_archiver; SELECT d FROM pg_ls_dir('pg_wal') d WHERE d LIKE '%.history' ORDER BY 1;`
- **Impact if wrong:** RESOLUTION: the node's real history file is 0000000B.history (11 = 0x0B), and the shipped SQL computes exactly that -- it is correct. Nothing in the codebase parses a TLI out of last_archived_wal (grep for last_archived_wal finds only the struct field at health_check_primary.rs:90 and the SQL at :209), so there is no live bug. The danger is evidentiary: the orchestrator's ground-truth note calls this file "real captured fleet data". For the archiver block it is not. A reader who takes last_archived_wal at face value would conclude that a node on TL 11 writes "00000011" and could "simplify" line 154 to lpad(timeline_id::text,8,'0') -- which would read 00000011.history, i.e. TIMELINE 17's file, on a node whose real fork data lives in 0000000B.history. That is precisely the decimal-padding failure ADR line 204 warns about, and it would return NULL (missing_ok) or, worse on a long-lived cluster that has actually reached TL 17, silently return the wrong fork LSN.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:154 -- "'pg_wal/' || lpad(upper(to_hex(timeline_id)), 8, '0') || '.history'"

### [medium | confirmed | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:210-212, 253 and docs/concepts/split-brain.md:21,29,57 -- "records, in its .history file, the switch LSN X where TL=N ended" / "fork_lsn"
- **Claim:** The .history file's second column is the fork LSN; section 7 compares a replica's flushed_lsn against it.
- **Postgres evidence:** timeline.c writeTimeLineHistory() appends exactly one line per fork with snprintf(buffer, sizeof(buffer), "%s%u\t%X/%X\t%s\n", (srcfd < 0) ? "" : "\n", parentTLI, LSN_FORMAT_ARGS(switchpoint), reason) -- three TAB-separated columns: parent TLI in DECIMAL, switchpoint as UPPERCASE HEX "hi/lo" with NO zero padding, then the reason. The reason text is chosen at promotion; with no recovery_target_* set the branch is snprintf(reason, sizeof(reason), "no recovery target specified") -- the exact string in the fixture. The switchpoint argument is EndRecPtr, the end of the last WAL record read/replayed on the OLD timeline. That single LSN is simultaneously the END of the old timeline and the START of the new one -- they are the same number, not adjacent numbers. (Quoting timeline.c and xlogrecovery.c from memory; those .c files are not on this machine.)
- **PG15 vs PG17:** Identical on 15.14. The writeTimeLineHistory format string and the "no recovery target specified" text are unchanged from 9.x through 17.
- **Query to settle:** `-- dump the raw bytes so the separators are unambiguous: SELECT encode(pg_read_binary_file('pg_wal/' || lpad(upper(to_hex((SELECT timeline_id FROM pg_control_checkpoint()))), 8, '0') || '.history'), 'escape'); -- and on a promoted primary, compare the last switchpoint to the frozen pointers: SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();`
- **Impact if wrong:** THE OFF-BY-ONE: because end-of-old == start-of-new is one LSN, the divergence test must be STRICT: flushed_lsn > fork_lsn. A >= test would fire on every ordinary promotion, since a promoting standby finishes replaying what it received and its receive/replay pointers land exactly ON the switch point -- the ADR's own 2026-09-10 measurement says so ("both return 6FD/7C0000A0, which is exactly the TL 21 -> 22 switch point"). Under >= that node would be flagged as holding divergent acknowledged writes and would force Refuse on a perfectly healthy cluster. The prose in ADR §7 and docs/concepts/split-brain.md:42 correctly says "past the fork", so the spec is right; nothing implements the comparison yet. SECOND TRAP, live in the code: switch_lsn is kept as an opaque String (timeline_history.rs:10, :45) and "%X/%X" is not zero-padded, so LEXICOGRAPHIC comparison against a flushed_lsn string is wrong -- "3/E000000" sorts after "3/10000000" but is numerically smaller. A numeric hex LSN parser already exists at src/v2/analyze/checks.rs:346-347 (u64::from_str_radix on each half); §7 must reuse it rather than compare strings.
- **Code depending on it:** src/v2/scan/health_check_primary/timeline_history.rs:41-46 -- "pub fn _fork_lsn_for(&self, from_tli: i32) -> Option<String>"

### [medium | confirmed | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:186 ("for the current TL") and 253 ("Collect db003's timeline and applied LSN from the control file") / :243 ("the higher-TL primary's fork LSN")
- **Claim:** Reading <current TL>.history on the higher-TL primary yields that primary's own fork point.
- **Postgres evidence:** Confirmed, but via the LAST LINE, not via a line for the current TL. The file for timeline N contains one line per ANCESTOR only -- there is no line whose first column is N. timeline.c readTimeLineHistory() makes this explicit: after parsing the file it does "Create one more entry for the 'tip' of the timeline, which has no entry in the history file" and lcons()es a synthetic entry for targetTLI with end = InvalidXLogRecPtr. The file's LAST line is <parentTLI>\t<switchpoint>, and that switchpoint IS where the current timeline forked off. The repo's own fixture agrees: timeline_id 11 with exactly 10 lines, prev_tli 1..10, last line "10\t3/E000000\tno recovery target specified". (readTimeLineHistory quoted from memory; timeline.c is not on this machine.)
- **PG15 vs PG17:** Identical on 15.14. The ancestors-only file layout and the synthetic tip entry are unchanged across 15 and 17.
- **Query to settle:** `SELECT (SELECT timeline_id FROM pg_control_checkpoint()) AS tli,        count(*) AS history_lines   FROM regexp_split_to_table(          pg_read_file('pg_wal/' || lpad(upper(to_hex((SELECT timeline_id FROM pg_control_checkpoint()))), 8, '0') || '.history', 0, 1048576, true),          E'\n') AS l  WHERE btrim(l) <> '' AND btrim(l) NOT LIKE '#%'; -- history_lines is the number of ANCESTORS; it equals tli-1 only if no TLIs were ever skipped.`
- **Impact if wrong:** Two consequences for §7. (1) _fork_lsn_for(lower_tl) is the CORRECT accessor -- but only when called on the HIGHER-TL primary's PrimaryHealthCheckResult. Called on the lower-TL primary's own result it always returns None, because that node's file has no line for its own timeline. Both ADR §7 and docs/concepts/split-brain.md:57 already say to read X from the higher-TL primary, so the design is right; whoever wires it must not accidentally read the lower-TL node's own capture. (2) Do NOT derive the current timeline's fork as _fork_lsn_for(tl - 1). The parent is not guaranteed to be tl-1: at promotion newTLI = findNewestTimeLine(recoveryTargetTLI) + 1, so recovering to TL 3 when TLs 4 and 5 already exist in the archive yields TL 6 with parent 3. The only safe way to get "my own fork point" is the LAST entry of the file, which the current API does not expose.
- **Code depending on it:** src/v2/scan/health_check_primary/timeline_history.rs:44 -- ".find(|e| e.previous_tli == from_tli)"

### [low | partially-correct | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:202 -- "The CTE evaluates pg_control_checkpoint() once; calling it twice in the same CASE risked a (negligible but real) race during a TL bump where the filename and the existence check disagree."
- **Claim:** Two pg_control_checkpoint() calls in one CASE could race so that the filename and the existence check disagree.
- **Postgres evidence:** The volatility half is right; the race description is not. pg_control_checkpoint() is declared provolatile => 'v' in pg_proc.dat (it re-reads global/pg_control via get_controlfile() on every call, so the planner may not fold two calls together and two calls CAN return different values within one statement). But the SQL contains NO existence check -- the only other use of the value is the literal comparison "= 1". The branch can only differ across the TL 1 -> 2 transition, a once-per-cluster-lifetime event; for any TL >= 2 both calls take the ELSE branch and the worst outcome is reading the immediately-previous timeline's file. And missing_ok=true makes even the TL-1 case benign (it would read 00000001.history and get NULL rather than erroring). Note also that in the ADR's own quoted form, cc.timeline_id appearing twice is a COLUMN reference, not a function call -- referencing it twice never re-evaluates the function, so the CTE is not what prevents double evaluation there.
- **PG15 vs PG17:** Holds on 15.14. pg_control_checkpoint has been VOLATILE since it was added in 9.6; CTE auto-inlining (PG 12+) is blocked here both by the volatile function and, in the shipped form, by the CTE being referenced twice.
- **Query to settle:** `SELECT proname, provolatile FROM pg_proc WHERE proname IN ('pg_control_checkpoint','pg_control_system'); -- expect provolatile = 'v' for both`
- **Impact if wrong:** No behavioural impact -- the CTE is harmless and mildly beneficial. But the ADR undersells the shipped code: the SHIPPED query hoists the CTE to statement level, so the emitted 'timeline_id' JSON key (line 149) and the .history filename (line 154) provably come from ONE control-file read. The ADR's quoted form nests the CTE inside the history subselect only, which would leave the 'timeline_id' key free to be a second, independent read. That is a genuine (if small) consistency guarantee the shipped code has and the ADR text does not describe -- the ADR §5 snippet is stale relative to what landed.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:146 -- "WITH cc AS (SELECT timeline_id FROM pg_control_checkpoint())"

### [info | confirmed | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:191/193 ("WHEN cc.timeline_id = 1", "upper(to_hex(cc.timeline_id))") vs src/v2/scan/health_check_primary.rs:152/154 ("WHEN timeline_id = 1", "upper(to_hex(timeline_id))")
- **Claim:** The ADR-quoted SQL and the shipped SQL differ in qualifying the CTE column.
- **Postgres evidence:** Semantically identical. In the shipped subquery "SELECT CASE ... END FROM cc" the range table has exactly one entry (cc) exposing exactly one column (timeline_id), so unqualified timeline_id resolves unambiguously to cc.timeline_id. Postgres resolves unqualified column names at the innermost query level first, and the enclosing level (SELECT jsonb_build_object(...) with no FROM clause) contributes no columns, so there is nothing to shadow it and no ambiguity error is possible.
- **PG15 vs PG17:** Identical on 15.14; name resolution rules unchanged.
- **Query to settle:** `EXPLAIN (VERBOSE) <the shipped HEALTH_CHECK_PRIMARY_QUERY>;  -- confirms timeline_id binds to cc`
- **Impact if wrong:** None. The only real ADR-vs-shipped difference is the CTE's scope (statement-level in the shipped code, subquery-level in the ADR snippet) -- covered in the CTE-race claim above. Worth noting the guard is redundant given missing_ok=true: on a never-promoted cluster 00000001.history does not exist and the read would return NULL anyway.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:152 -- "WHEN timeline_id = 1 THEN NULL"

### [low | partially-correct | confidence=high] ADR lines src/v2/scan/health_check_primary/timeline_history.rs:1-46 (the parser) -- audited against ADR-002 §5/§7
- **Claim:** Does anything in the codebase parse the history file, and does that parser agree with the real format?
- **Postgres evidence:** Yes -- timeline_history.rs:17-38 is the only parser, and it agrees with writeTimeLineHistory's "%u\t%X/%X\t%s\n" for every line Postgres actually writes. splitn(3, char::is_whitespace) on a single-tab line yields exactly the three real columns, and the two real-world oddities are both handled: writeTimeLineHistory prepends a bare "\n" when copying from a parent file ("insert an extra newline just in case the parent file failed to end with one"), and the recovery_target_time / recovery_target_lsn reason strings embed their own trailing "\n" via snprintf(reason, ..., "%s %s\n", ...) -- both produce blank lines, which line 25 filters. Comment lines starting with '#' are skipped, matching Postgres's own readTimeLineHistory. Divergences are cosmetic: previous_tli is i32 where TimeLineID is uint32 (unreachable above 2^31, and note the SQL side is actually SAFER -- to_hex32 casts to uint32, so it renders a "negative" TLI correctly while the Rust side would not).
- **PG15 vs PG17:** Format identical on 15.14 and 17; the module doc link at :3 points at /docs/current/ (17 today) rather than /docs/15/, which is harmless since the format has not changed.
- **Query to settle:** `SELECT encode(pg_read_binary_file('pg_wal/' || lpad(upper(to_hex((SELECT timeline_id FROM pg_control_checkpoint()))), 8, '0') || '.history'), 'escape'); -- confirms literal \t separators, unpadded uppercase hex LSNs, and the exact reason text`
- **Impact if wrong:** No live impact: grep shows NO production caller of timeline_history_entries() or _fork_lsn_for() -- the only references are inside the module's own #[cfg(test)] block (that is the source of the two known dead-code warnings). Two documentation/robustness nits for whoever wires §7: (1) the doc comment at :15-16 says None means "TL=1, no history file yet", but None actually collapses THREE distinct states -- TL=1 (the CASE guard), missing_ok returning NULL for an absent file, and a pre-existing capture with no such key (#[serde(default)] at health_check_primary.rs:110-111). A NULL history on a TL>1 node is a signal, not a synonym for TL=1. (2) The 1 MiB length cap truncates silently rather than erroring, so a truncated read would yield a garbage final entry -- unreachable in practice (~45 bytes/line means ~23k timelines) but the failure is silent if it ever happens.
- **Code depending on it:** src/v2/scan/health_check_primary/timeline_history.rs:27 -- "let mut parts = line.splitn(3, char::is_whitespace);"

### [info | confirmed | confidence=certain] ADR lines docs/adr/002-split-brain-resolution-refinement.md:204 -- "The filename is 8-digit uppercase hex padded (postgres writes via %08X); decimal padding will silently miss TLs >= 10."
- **Claim:** Timeline history files are named with 8-digit zero-padded uppercase hex via %08X, and decimal padding would miss TLs >= 10.
- **Postgres evidence:** Read directly, not from memory. /home/robert.sjoblom@fnox.it/9th/pg-migration-lint/target/debug/build/pg_query-7436f49853ae04a4/out/src/postgres/include/access/xlog_internal.h:218-221: TLHistoryFileName(char *fname, TimeLineID tli) { snprintf(fname, MAXFNAMELEN, "%08X.history", tli); }. Corroborated by IsTLHistoryFileName at :224-229, which requires strlen == 8 + strlen(".history") && strspn(fname, "0123456789ABCDEF") == 8 -- i.e. exactly 8 UPPERCASE hex digits. XLogFileName at :166-170 uses the same %08X for the TLI field of WAL segment names. The shipped expression lpad(upper(to_hex(timeline_id)), 8, '0') reproduces %08X exactly; upper() is load-bearing because to_hex() returns lowercase. The ">= 10" boundary is right: decimal and hex renderings coincide only for 1..9.
- **PG15 vs PG17:** The header I read is PG 17.7 (pg_config.h:633 PG_VERSION "17.7"). TLHistoryFileName/XLogFileName have used %08X unchanged since 9.0, so this holds on 15.14; if you want it nailed down for 15 specifically, the settling query below reads the actual filenames off a fleet node.
- **Query to settle:** `SELECT lpad(upper(to_hex(11)), 8, '0') || '.history' AS expected;  -- 0000000B.history SELECT d FROM pg_ls_dir('pg_wal') d WHERE d LIKE '%.history' ORDER BY 1;`
- **Impact if wrong:** The decimal-padding warning is coherent as a RATIONALE note explaining why to_hex()+upper() are there, not as a description of a live hazard -- the shipped SQL cannot exhibit it. It is not idle, though: this fleet is on TL 11, past the >= 10 boundary, so the warning is exactly one refactor away from mattering, and the fixture's last_archived_wal already contains the decimal-rendered form (see the fixture claim above), which is how someone would talk themselves into that refactor.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:154 -- "'pg_wal/' || lpad(upper(to_hex(timeline_id)), 8, '0') || '.history'"

### [info | confirmed | confidence=high] ADR lines docs/adr/002-split-brain-resolution-refinement.md:202 -- "The 4-arg form pg_read_file(path, offset, length, missing_ok) returns NULL if the file doesn't exist ...; the 1-arg form throws and would abort the entire jsonb_build_object."
- **Claim:** The 4-arg pg_read_file exists, missing_ok=true returns NULL instead of erroring, and the 1-arg form would abort the enclosing jsonb_build_object.
- **Postgres evidence:** The distinct C entry points are visible in the vendored header: utils/fmgrprotos.h:1940 declares pg_read_file_off_len_missing (the 4-arg (text,bigint,bigint,boolean) form), :1520 pg_read_file_off_len (3-arg), :2328 pg_read_file_all (1-arg), :2792 pg_read_file_all_missing (2-arg (text,boolean)). The missing_ok variants dispatch to read_text_file(..., missing_ok) which PG_RETURN_NULL()s on ENOENT instead of ereport(ERROR). The 1-arg form passes missing_ok = false, so a missing file raises "could not open file ... for reading: No such file or directory"; because the call sits inside a single SELECT jsonb_build_object(...), that error aborts the whole statement. Overload resolution is unambiguous: with four arguments only one candidate matches, and the untyped literal 0 coerces to bigint.
- **PG15 vs PG17:** Holds on 15.14. The missing_ok overloads were added in PG 11; PG 15 docs list pg_read_file ( filename text [, offset bigint, length bigint ] [, missing_ok boolean ] ) -> text. The header I read is 17.7, so the PG-15 presence is from memory of the 15 docs -- the settling query confirms it in one line.
- **Query to settle:** `SELECT p.oid::regprocedure FROM pg_proc p WHERE p.proname = 'pg_read_file' ORDER BY 1; -- expect pg_read_file(text,bigint,bigint,boolean) among the rows on 15.14 SELECT pg_read_file('pg_wal/FFFFFFFF.history', 0, 1024, true) IS NULL AS missing_ok_returns_null;`
- **Impact if wrong:** Correct as stated, with one important scope limit already covered above: missing_ok covers ENOENT ONLY. Insufficient privilege, EACCES, or an encoding-validation failure still aborts the whole statement and drops the node to Role::UnknownPrimary. The ADR reads as if missing_ok makes the history read non-fatal in general; it does not.
- **Code depending on it:** src/v2/scan/health_check_primary.rs:153-156 -- "ELSE pg_read_file(... , 0, (1024 * 1024)::bigint, true)"


## Postgres behaviour audit of ADR-002's quorum/topology safety reasoning (case-matrix line 37, §7 3-node proof, SyncStandbyNamesDiverged out-of-scope bu

### [high | refuted | confidence=certain] ADR lines 37 (case-matrix preamble): "On db002 (new primary), the same setting is unsatisfiable unless db003 reattaches: A = itself can't be its own standby, and B = db003 is stuck elsewhere."
- **Claim:** db002's post-promotion synchronous_standby_names lists db002 itself as member A, so db002's only usable acker is db003.
- **Postgres evidence:** Two independent sources. (1) Fleet config, decisive: /home/robert.sjoblom@fnox.it/work/infra/ansible/environments/gdc/it/group_vars/postgres_all/postgres_all.yml:44 renders SSN as `ANY 1 ( {% for host in repmgr_other_postgres_nodes %}...{% endfor %} )` -- the loop is over OTHER nodes, so every node's SSN is self-excluding by construction. The rendered proof, three nodes of one cluster under /home/robert.sjoblom@fnox.it/work/infra/ansible/result_manifests/it/: db001 -> `ANY 1 ( it_pg_app001_db002, it_pg_app001_db003 )`; db002 -> `ANY 1 ( it_pg_app001_db001, it_pg_app001_db003 )`; db003 -> `ANY 1 ( it_pg_app001_db001, it_pg_app001_db002 )`. db002 does NOT list itself; it lists db001 and db003. (2) Nothing rewrites SSN on failover: repmgr.conf.j2 and promote_replica.sh.j2 contain no synchronous_standby_names handling (promotion is a bare `repmgr standby promote`), and /home/robert.sjoblom@fnox.it/work/infra/docs/alerts/postgresSynchronousStandbyNamesMisconfigured.md states it outright: "Postgres itself only consults the value on the primary, but the alert evaluates it on every node so a wrong value on a standby is caught before it becomes active on failover." So db002's pre-set self-excluding SSN simply becomes active at promotion. Postgres-side: PG 15.14 src/backend/replication/syncrep.c:874-905 SyncRepGetStandbyPriority has no self-identity concept at all; it only walks SyncRepConfig->member_names comparing `pg_strcasecmp(standby_name, application_name) == 0 || strcmp(standby_name, "*") == 0`.
- **PG15 vs PG17:** Irrelevant to the refutation -- this is a fleet-config fact, not a version-dependent postgres behaviour. The matching code is identical: REL_15_14 syncrep.c:893-894 and REL_17_RC1 syncrep.c:836-837 are the same two lines.
- **Query to settle:** `Run on all three nodes of a real cluster, including standbys: SELECT inet_server_addr(), pg_is_in_recovery(), current_setting('synchronous_standby_names'); -- expect each node to list the OTHER two.`
- **Impact if wrong:** The acker pool for db002 is {db001, db003}, not {db003}. That doubles the premise of the §7 proof. Today the conclusion still holds during the two-primary window for a different reason (db001, being a primary, has no walreceiver, so it is not a walsender client of db002) -- but the ADR never states that reason, it states a false one. An operator or future author who reasons from line 37 will conclude db002 can never ack until db003 returns, which is wrong the moment db001 is demoted-and-followed.
- **Code depending on it:** src/v2/analyze/split_brain.rs:660 (emit_quorum_findings computes `observed` from the SSN member set; line 37 is the ADR's model of what that set contains on the promoted node)

### [high | partially-correct | confidence=high] ADR lines 241 (§7, "The 3-node proof"): "db002's quorum can be satisfied *only* by db003 (a peer primary is not its standby; a primary is not its own)." Restated verbatim at docs/concepts/split-brain.md:68.
- **Claim:** The exclusivity of db003 as db002's acker is a postgres guarantee.
- **Postgres evidence:** Split the parenthetical. (a) "a primary is not its own [standby]": a real postgres guarantee, but vacuous -- SyncRepGetCandidateStandbys (PG 15.14 syncrep.c:769-845) iterates only WalSndCtl->walsnds, i.e. processes serving an inbound replication connection; a postmaster never connects to itself. It is also moot here because db002's SSN does not name db002 (see previous claim). (b) "a peer primary is not its standby": NOT a postgres guarantee. Postgres has no notion of "peer primary"; SyncRepGetStandbyPriority (syncrep.c:893) matches a bare string against the walsender's application_name GUC. db001's name IS in db002's SSN. Any inbound replication connection asserting application_name=it_pg_app001_db001 becomes a quorum candidate -- a demoted-and-followed db001, or equally `pg_receivewal --synchronous` with PGAPPNAME set. The only thing making (b) true at scan time is the topology snapshot: both nodes are primaries, so neither runs a walreceiver toward the other. That is a scan-time observation, and the proof needs an all-time claim ("db002 client-acked nothing on TL=N+1 since the fork").
- **PG15 vs PG17:** Holds identically on 15.14 and 17: the candidate-collection loop and the name match are unchanged (REL_15_14 syncrep.c:769-845/874-905 vs REL_17_RC1 syncrep.c:~710-790/827-850).
- **Query to settle:** `On the higher-TL primary: SELECT * FROM pg_stat_replication; plus SELECT * FROM repmgr.events WHERE event IN ('standby_follow','standby_promote','repmgrd_failover_promote') ORDER BY event_timestamp DESC LIMIT 20; -- a standby_follow on the lower-TL node after the promotion timestamp falsifies 'its fork is empty'.`
- **Impact if wrong:** Concrete data-loss history the proof excludes by assumption: db002 promotes at fork X; an operator runs a partial fence, `repmgr standby follow` on db001, so db001 attaches to db002 with application_name it_pg_app001_db001, which IS in db002's SSN -> db002's quorum is satisfied and it acks writes on TL=N+1; db001 is later re-promoted (repmgrd flap, manual pg_ctl promote) -> two primaries again, and now BOTH forks hold acked writes. A single scan showing db003 streaming db001 would still yield the 'confident, no Refuse' verdict of C-c ("db002 provably client-acked nothing"), which is false in that history. There is no captured run of this, and no field in the scan that could detect it.
- **Code depending on it:** src/v2/analyze/split_brain.rs:665 (`if observed < count`) -- the resolver never checks whether the *other* candidate primary was ever a standby of this one, because the proof asserts it cannot have been

### [critical | refuted | confidence=certain] ADR lines 145-148 (§4 derivation rule for PrimaryQuorumUnsatisfied): "observed = |members ∩ gated_followers|"; and the Consequences bullet at 264: "C-b and C-c (the legitimate cases) still resolve correctly."
- **Claim:** The SSN member set and the resolver's follower set (node_name values) are the same namespace, so their intersection is meaningful.
- **Postgres evidence:** They can never coincide, and postgres's own grammar is why. PG 15.14 syncrep_scanner.l defines unquoted SSN identifiers as ident_start=[A-Za-z\200-\377_], ident_cont=[A-Za-z\200-\377_0-9\$] -- no '-' and no '.'. A hyphen or dot falls through to the catch-all `.` rule returning JUNK, so `synchronous_standby_names = 'ANY 1 (dev-pg-app001-db002.sto2.example.com)'` is rejected by check_synchronous_standby_names (syncrep.c:1089-1116) as a syntax error. FQDN node_names are therefore not merely absent from SSN, they are unrepresentable unquoted. The Ansible template does the required transform explicitly: group_vars postgres_all.yml:44 `regex_replace('^(.*)(\.it\.fnox\.se)$','\1')|replace('-','_')`. Fixture confirms: SSN members ['dev_pg_app001_db002','dev_pg_app001_db003'] vs node_names ['dev-pg-app001-db001.sto1.example.com', ...] -> intersection is the empty set. Independently, the same namespace error at split_brain.rs:307 (`conn.application_name == replica.node_name`) keeps `gated` empty, so `observed` is 0 twice over.
- **PG15 vs PG17:** syncrep_scanner.l identifier classes are unchanged 15 -> 17 (the 17 diff is confined to error reporting and the yyscan_t reentrancy conversion). Same conclusion on both.
- **Query to settle:** `On a primary: SELECT current_setting('synchronous_standby_names'), array_agg(application_name), array_agg(sync_state) FROM pg_stat_replication; -- then compare against the scanner's node_name field for the same nodes. Cheaper fix than any comparison: pg_stat_replication.sync_state IS postgres's own answer (it is 'quorum'/'sync' only when the walsender matched an SSN member and is a current candidate, walsender.c:3535-3541,3613-3617). It is already captured as PgSyncSettings (health_check_primary.rs:141) and split_brain.rs never reads it (0 occurrences).`
- **Impact if wrong:** Two effects on real fleet data, both reachable in code today. (1) emit_quorum_findings emits PrimaryQuorumUnsatisfied for EVERY primary with a parseable SSN, unconditionally, including the elected true primary -> determine_confidence_level (split_brain.rs:404-410) then returns Conflicting, so no real split-brain verdict can ever be BestEffort. (2) Worse, because split_brain.rs:307 fails identically, `replicas_following` is always empty on fleet data, so resolve_with_different_timelines can never take the LowerTimelineHasQuorum branch. Exactly in matrix rows C-b and C-c -- where the ADR says the correct answer is 'keep db001, fence db002' -- the tool falls through to HigherTimeline and writer/build.rs:712 prints "SplitBrain: db002 has quorum (TL=N+1), demote db001 (TL=N, no live replicas)". That is an operator-facing instruction to demote the node holding the acknowledged writes, in the one case the ADR was written to get right. The Consequences bullet at line 264 is false as shipped.
- **Code depending on it:** src/v2/analyze/split_brain.rs:660-668 and src/v2/analyze/split_brain.rs:307

### [high | refuted | confidence=certain] ADR lines 69 (§1 gate, primary side is line 74): "`wal_receiver.status` ∈ {`"streaming"`, `"catchup"`}. `catchup` is genuinely-following mid-recovery and must not be rejected." plus line 74 accepting `state` ∈ {streaming, catchup} on the primary side, and line 140's ReplicaInCatchup "informational, gate passed".
- **Claim:** A standby whose walsender is in `catchup` counts as a live follower for quorum purposes, so a lower-TL primary with a catchup-only follower is 'actively committing'.
- **Postgres evidence:** PG 15.14 syncrep.c:805-809, inside SyncRepGetCandidateStandbys: `/* Must be streaming or stopping */ if (state != WALSNDSTATE_STREAMING && state != WALSNDSTATE_STOPPING) continue;`. WALSNDSTATE_CATCHUP (the state rendered as 'catchup' by WalSndGetStateString, walsender.c:3448) is excluded. SyncRepGetSyncRecPtr then bails at syncrep.c:628-633 with `num_standbys < SyncRepConfig->num_sync` -> returns false -> no commit waiter is released. So under ANY 1 (A,B) with the sole standby in catchup, the primary cannot ack a single client commit. Note the mirror-image asymmetry: WALSNDSTATE_STOPPING *does* count for postgres but the resolver rejects it (split_brain.rs:308-311 admits only Streaming|Catchup) -- that error is in the safe direction.
- **PG15 vs PG17:** Unchanged: REL_17_RC1 syncrep.c:750-751 has the identical STREAMING/STOPPING filter and :578 the identical count test.
- **Query to settle:** `On a primary with a catching-up standby: SELECT application_name, state, sync_state FROM pg_stat_replication; -- state='catchup' comes back with sync_state='potential', never 'quorum'. Confirm the primary is blocked with: SET synchronous_commit=on; BEGIN; CREATE TEMP TABLE t(i int); COMMIT; -- hangs.`
- **Impact if wrong:** Wrong verdict in the destructive direction, and a test currently locks it in. A catchup-only db003 makes the resolver flip to LowerTimelineHasQuorum and writer/build.rs:702 prints "SplitBrain: db001 has quorum (lower TL=N), fence db002 (TL=N+1, quorum-blocked)" -- fencing the genuinely-promoted primary on the strength of a standby that postgres refuses to count, on a primary that by the ADR's own operational definition (line 19: "quorum is satisfied and that is actively committing") is not the true primary. Reachable in code: yes, and asserted by a passing test. Reachable on fleet: not today, only because the namespace bug above keeps `gated` empty -- fixing the namespace bug without also splitting the catchup handling turns this latent bug live.
- **Code depending on it:** src/v2/analyze/split_brain.rs:283 and src/v2/analyze/split_brain.rs:308-311; the behaviour is pinned by the test `gate_accepts_catchup_status_and_emits_replica_in_catchup` at split_brain.rs:1299, which asserts `info.true_primary == "db001"` and `SplitBrainResolution::LowerTimelineHasQuorum` from a pure-catchup row (split_brain.rs:1325-1331)

### [high | partially-correct | confidence=certain] ADR lines 27 (Cluster assumptions): "repmgr-set `application_name` equals the node name"
- **Claim:** The application_name a standby presents equals the node name the scanner records.
- **Postgres evidence:** True for repmgr's OWN notion of node_name, false for the scanner's. repmgr.conf.j2:27 sets `node_name='{{ansible_hostname|replace('-','_')}}'` -> `it_pg_app001_db002`, and that is what repmgr writes as application_name into primary_conninfo on `standby clone`/`standby follow`. Ansible renders the same string independently (result_manifests/it/it-pg-app001-db002.../manifest_postgresql.conf: `primary_conninfo = '... application_name=it_pg_app001_db002'`), so the two agree and the value is stable across failover. The scanner's `node_name` is the FQDN (`dev-pg-app001-db002.sto2.example.com` in the fixture). The ADR sentence does not say which 'node name' it means, and the implementation resolved the ambiguity the wrong way.
- **PG15 vs PG17:** Not version-dependent.
- **Query to settle:** `grep -h "^node_name" /etc/repmgr/15/repmgr.conf on each node, and SELECT application_name FROM pg_stat_replication on the primary; compare to the node_name the scanner stores for the same host.`
- **Impact if wrong:** This one ambiguous ADR sentence is the licence for the raw equality compare that disables the entire primary-side gate on real data. Note src/v2/writer/build.rs already carries the bridge -- normalize_application_name (build.rs:422) maps `dev_pg_app001_db002` -> `db002` and extract_db_number (build.rs:408) maps the FQDN -> `db002@sto2` -- so the writer can join the two namespaces and the resolver cannot. The ADR should state the two forms explicitly and name the transform.
- **Code depending on it:** src/v2/analyze/split_brain.rs:307 `&& conn.application_name == replica.node_name`

### [info | confirmed | confidence=certain] ADR lines 258 (Out of scope, SyncStandbyNamesDiverged): "in this cluster's topology each primary's SSN structurally excludes itself -- so the naive string compare fires on every split-brain even when the policy is identical (`ANY 1 (db002, db003)` on db001 vs. `ANY 1 (db001, db003)` on db002 is the same policy, different strings)"
- **Claim:** Each node's SSN structurally excludes itself, making a naive string compare across primaries fire spuriously on every split-brain.
- **Postgres evidence:** Confirmed byte-for-byte, including the ADR's illustrative strings. group_vars postgres_all.yml:44 loops over `repmgr_other_postgres_nodes`; the rendered manifests give exactly the ADR's example: db001 -> `ANY 1 ( it_pg_app001_db002, it_pg_app001_db003 )`, db002 -> `ANY 1 ( it_pg_app001_db001, it_pg_app001_db003 )`. A survey of all 321 rendered postgresql.conf manifests under result_manifests/ finds exactly one SSN shape after name-masking: `ANY 1 ( NAME, NAME )` -- 321/321. So a naive whole-string compare between two candidate primaries is guaranteed to differ, always, with identical policy. The deferral is well founded, and this is 'deferred-correct'.
- **PG15 vs PG17:** Not version-dependent.
- **Query to settle:** `Already settled offline from rendered config; to reconfirm live: SELECT current_setting('synchronous_standby_names') on each node of any cluster.`
- **Impact if wrong:** Nothing breaks; this is the ADR being right. Worth recording because it is the exact premise that ADR line 37 contradicts -- lines 37 and 258 cannot both be true, and 258 is the true one. Recommend fixing line 37 rather than 258.
- **Code depending on it:** n/a -- nothing emits SyncStandbyNamesDiverged; the deferral is consistent across the ADR (line 107 and line 258)

### [info | confirmed | confidence=certain] ADR lines 37 (implicitly, via 'the same setting' on both primaries) and 145 (§4 step 1: parse into {method, count, members})
- **Claim:** The real fixture SSN 'ANY 1 ( dev_pg_app001_db002, dev_pg_app001_db003 )' -- with spaces inside the parens -- parses, and the parsed member strings are byte-identical to the pg_stat_replication.application_name values.
- **Postgres evidence:** Traced sync_standby_names::parse (src/v2/analyze/sync_standby_names.rs:24-70) by hand and by a faithful re-implementation run against the fixture: `strip_prefix("ANY ")` -> `trim_start` -> `split_once('(')` -> `count_str.trim().parse()` = 1 -> `rest.trim().strip_suffix(')')` -> `split_members` trims each -> ['dev_pg_app001_db002','dev_pg_app001_db003'], byte-identical to the fixture's two application_name values (equality test returned true). Postgres agrees that these strings match: syncrep_scanner.l ignores `[ \t\n\r\f\v]+` between tokens, and the fixture's pg_stat_replication rows both carry sync_state='quorum', which walsender.c:3613-3617 only assigns when sync_standby_priority != 0 (i.e. SyncRepGetStandbyPriority found the name in member_names) AND the walsender is in the current candidate list AND the method is SYNC_REP_QUORUM. So the fleet capture is direct evidence that postgres itself matched these member strings to these application_names.
- **PG15 vs PG17:** Whitespace handling and the sync_state derivation are identical in 15.14 and 17.
- **Query to settle:** `n/a -- settled by the fixture's sync_state='quorum' rows.`
- **Impact if wrong:** Nothing -- the parser is correct for the fleet's only SSN shape. The failure is downstream, at the intersection (claim 3), not in parsing.
- **Code depending on it:** src/v2/analyze/sync_standby_names.rs:49-56

### [info | refuted | confidence=certain] ADR lines 145 (§4 step 1) and the implicit contract of emit_quorum_findings at split_brain.rs:650-668, which discards `method`
- **Claim:** Ignoring ANY vs FIRST when comparing an intersection size against `count` can give a wrong answer.
- **Postgres evidence:** For the 'can this primary ack at all' question the method is genuinely irrelevant, and PG 15.14 proves it: both modes funnel through the same test in SyncRepGetSyncRecPtr, syncrep.c:628-633 -- `if (!(*am_sync) || num_standbys < SyncRepConfig->num_sync) return false;`. SyncRepGetCandidateStandbys collects every matched, STREAMING/STOPPING, valid-flush walsender for both modes and only afterwards, in priority mode, truncates to num_sync (syncrep.c:827-838) -- truncation cannot make the count fall below num_sync. Arithmetic: `ANY 1 (A,B)` needs 1 ack (any one); `FIRST 2 (A,B,C)` needs 2 acks, and specifically from the two lowest-priority-number CONNECTED candidates -- not necessarily A and B; with only A and C up, sync_standbys=[A,C], length 2 >= 2, satisfied. The method changes only WHICH LSN releases waiters (SyncRepGetOldestSyncRecPtr for priority vs SyncRepGetNthLatestSyncRecPtr for quorum, syncrep.c:648-659), never the count threshold. Fleet plausibility of any method-sensitive shape: zero -- 321/321 rendered manifests are `ANY 1 ( a, b )`, and there is a Prometheus alert (postgresSynchronousStandbyNamesMisconfigured) enforcing that exact shape on every node including standbys.
- **PG15 vs PG17:** REL_17_RC1 syncrep.c:578 carries the same `num_standbys < SyncRepConfig->num_sync` test; conclusion identical.
- **Query to settle:** `n/a for this fleet; to re-verify the shape invariant fleet-wide: SELECT setting FROM pg_settings WHERE name='synchronous_standby_names' scraped by the existing postgres_exporter query (roles/postgres_exporter/templates/queries.yml.j2:193-206).`
- **Impact if wrong:** None -- discarding `method` is the correct simplification for this check, and it is worth saying so in the ADR so a future reviewer does not 'fix' it. The genuine wrong-answer shapes are all method-independent: (a) duplicate member names -- `ANY 2 (a, a, b)` or `FIRST 2 (a, a, b)` with only `a` connected gives observed=2 in the tool (the filter counts list positions, split_brain.rs:660-663) while postgres has one candidate and blocks; (b) the `*` wildcard -- postgres matches every walsender (syncrep.c:894 `strcmp(standby_name, "*") == 0`) while the tool's intersection yields 0, a spurious PrimaryQuorumUnsatisfied. Neither shape exists on this fleet.
- **Code depending on it:** src/v2/analyze/split_brain.rs:650 (destructures `Quorum { count, members, .. }`, discarding method)

### [medium | partially-correct | confidence=high] ADR lines 241 (§7) and docs/concepts/split-brain.md:68: "A replica is on one timeline at a time."
- **Claim:** 'A replica is on one timeline at a time' is the fact that makes db003 unable to satisfy two primaries' quorums simultaneously.
- **Postgres evidence:** The conclusion is right; the stated reason is the wrong fact. The load-bearing guarantee is that a standby has exactly ONE walreceiver: WalRcvData is a single shmem struct (walreceiverfuncs.c:44-51, `size = add_size(size, sizeof(WalRcvData))`, one ShmemInitStruct("Wal Receiver Ctl")), so pg_stat_wal_receiver returns at most one row and a standby streams from exactly one upstream. That is a hard postgres guarantee and it alone settles the question. Cascading does not create a loophole in the other direction either: am_cascading_walsender = RecoveryInProgress() (walsender.c:268,417,732) and SyncRepGetStandbyPriority returns 0 for any cascading walsender (syncrep.c:882-885, comment "synchronous cascade replication is not allowed"), so a cascaded standby can never contribute to a standby-upstream's quorum. By contrast, 'one timeline at a time' is a weaker and looser statement: a replica's pg_wal legitimately holds segments from several timelines, and pg_control_checkpoint().timeline_id (the field §5 captures) is the last checkpoint's TL, which can lag pg_stat_wal_receiver.received_tli -- precisely the field the C-g capture leans on.
- **PG15 vs PG17:** WalRcvData singleton and the cascading-priority-zero rule are unchanged in 17 (REL_17_RC1 syncrep.c:827 `if (am_cascading_walsender) return 0;`).
- **Query to settle:** `SELECT count(*) FROM pg_stat_wal_receiver; -- always 0 or 1. For the temporal gap there is no query; it needs repmgr.events history or the standby's log.`
- **Impact if wrong:** Second-order but real: the proof also uses a point-in-time observation ('db003 is streaming db001 right now') to support an all-time claim ('db002 client-acked nothing since the fork'). One-walreceiver-at-a-time forbids simultaneity, not sequence. The gap is closed only by the wedge property -- a replica that advanced onto TL=N+1 past X cannot subsequently stream from a TL=N primary -- which is argued in docs/concepts/split-brain.md:89-95 but never invoked in the §7 proof itself. §7 should cite the wedge property explicitly, or the proof reads as a non sequitur.
- **Code depending on it:** src/v2/scan/health_check_replica.rs (single wal_receiver field per replica, which encodes the one-walreceiver guarantee) and the C-g capture design in ADR §5 lines 209-221

### [info | partially-correct | confidence=high] ADR lines 19 (Operational definition) and docs/concepts/split-brain.md:15: "an *isolated* primary -- one with no live standby acking it -- physically cannot commit"
- **Claim:** Two primaries cannot simultaneously appear quorum-satisfied, so a populated pg_stat_replication implies the primary is committing.
- **Postgres evidence:** The safety half is confirmed; the observability half is not. Confirmed: a commit waiter is released only when the standby's reported flush LSN passes the commit LSN (SyncRepGetSyncRecPtr -> SyncRepWakeQueue), and a standby with a dead connection sends no more replies, so an isolated primary genuinely cannot ack. Not confirmed: the *count* test can pass on both primaries at once. A walsender whose TCP peer vanished without a FIN stays in WALSNDSTATE_STREAMING with a valid flush pointer until wal_sender_timeout fires -- 300000 ms on this fleet -- and SyncRepGetCandidateStandbys (syncrep.c:800-820) accepts it on pid!=0 + STREAMING/STOPPING + priority!=0 + valid flush. So for up to five minutes db001 can show `sync_state='quorum'` for db003 while db003 is actually attached to db002, and db002 can show the same. Both primaries look quorum-satisfied; neither is necessarily committing.
- **PG15 vs PG17:** Identical candidate filter in 17 (REL_17_RC1 syncrep.c:745-760).
- **Query to settle:** `On the suspected zombie: SELECT application_name, state, sync_state, flush_lsn, reply_time, now()-reply_time AS reply_age FROM pg_stat_replication; -- a reply_age approaching wal_sender_timeout/2 with state='streaming' is the stale-but-counted row.`
- **Impact if wrong:** This validates the ADR's design rather than undermining it: it is exactly why §1's freshness gate and the asymmetric 'replica wal_receiver is authoritative' precedence (line 56) are necessary, and it is the mechanism behind matrix row C-e. Worth stating positively in the ADR: 'quorum populated' and 'actively committing' are different predicates, and only the second is the operational definition. As written, line 19 bundles them with an 'and', which is correct but easy to misread as one condition.
- **Code depending on it:** src/v2/analyze/split_brain.rs:281-290 (the freshness half of the replica-side gate) and :312-314 (reply_time freshness on the primary side)

### [low | refuted | confidence=certain] ADR lines 69 (§1): "`wal_receiver.status` ∈ {`"streaming"`, `"catchup"`}"; and 44 (C-d): "`status` ≠ streaming/catchup"
- **Claim:** 'catchup' is a possible value of pg_stat_wal_receiver.status.
- **Postgres evidence:** It is not. PG 15.14 walreceiver.h:45-54 defines WalRcvState = {WALRCV_STOPPED, WALRCV_STARTING, WALRCV_STREAMING, WALRCV_WAITING, WALRCV_RESTARTING, WALRCV_STOPPING} and walreceiver.c:1315-1333 WalRcvGetStateString renders them as exactly {stopped, starting, streaming, waiting, restarting, stopping}. 'catchup' is a *walsender* state (walsender.c:3448, WALSNDSTATE_CATCHUP), i.e. pg_stat_replication.state, a different view on the other end of the connection. The ADR conflates the two columns.
- **PG15 vs PG17:** Same enum and same strings in 17.
- **Query to settle:** `SELECT DISTINCT status FROM pg_stat_wal_receiver; across the fleet -- will never return 'catchup'.`
- **Impact if wrong:** No behavioural effect (a branch that can never be taken), but two side effects worth noting. The ADR's §1 rationale sentence -- "`catchup` is genuinely-following mid-recovery and must not be rejected" -- is attached to the wrong column, and the test at split_brain.rs:1321 `.with_wal_receiver_status("catchup")` builds a state postgres cannot produce, so that test is not evidence about real behaviour. Note also that 'waiting' and 'restarting' are real values the gate correctly rejects, and 'stopping' is real and correctly rejected on the replica side.
- **Code depending on it:** src/v2/analyze/split_brain.rs:283 `&& matches!(wr.status.as_str(), "streaming" | "catchup")` -- the `"catchup"` arm is unreachable against real postgres output

### [low | refuted | confidence=high] ADR lines 73 (§1 primary side): "`application_name == ""` is **rejected** as unmatchable (postgres default when client doesn't set one; matches indiscriminately otherwise)."
- **Claim:** An empty application_name is the postgres default for a client that doesn't set one, and it matches SSN entries indiscriminately.
- **Postgres evidence:** Both halves are wrong; the resulting behaviour (reject it) is harmless. (a) Only `*` matches indiscriminately: syncrep.c:893-894 is `pg_strcasecmp(standby_name, application_name) == 0 || strcmp(standby_name, "*") == 0`. An empty application_name matches nothing unless the SSN literally contains `*`, and the fleet's SSN never does. (b) A walreceiver never presents an empty application_name: libpqwalreceiver sets fallback_application_name=walreceiver, visible in the fixture's own conninfo string (`... application_name=dev_pg_app001_db002 fallback_application_name=walreceiver ...`), so an unset application_name surfaces as 'walreceiver', not ''.
- **PG15 vs PG17:** Unchanged in 17 (REL_17_RC1 syncrep.c:836-837).
- **Query to settle:** `SELECT application_name FROM pg_stat_replication; -- on this fleet always the underscore node name; an unset one would read 'walreceiver'.`
- **Impact if wrong:** The guard is correct to keep (an empty name can never equal a real node name anyway) but the ADR's stated reason is wrong, and the wrong reason could lead a future author to add a matching special case for '' or to assume `*` semantics apply to empty strings. Fix the parenthetical, keep the guard.
- **Code depending on it:** src/v2/analyze/split_brain.rs:306 `!conn.application_name.is_empty()`

### [low | partially-correct | confidence=certain] ADR lines 145 (§4 step 1): "Parse `synchronous_standby_names` on the primary into `{ method, count, members }` ... Treat unparseable as method=ANY, count=∞ (defensive: emit no finding rather than a wrong one)."
- **Claim:** The parser accepts the same language postgres does, so anything postgres accepts is either parsed correctly or falls into the defensive unparseable path.
- **Postgres evidence:** There is a third outcome the ADR does not allow for: silently mis-parsed, neither correct nor rejected. PG 15.14 syncrep_gram.y:62-67 accepts four forms -- bare `standby_list` (= FIRST 1), `NUM '(' list ')'` (= FIRST N, no keyword), `ANY NUM '(' list ')'`, `FIRST NUM '(' list ')'` -- with ANY/FIRST scanned case-insensitively by explicit character classes (syncrep_scanner.l: `[Aa][Nn][Yy]`, `[Ff][Ii][Rr][Ss][Tt]`) and whitespace being any of `[ \t\n\r\f\v]`. sync_standby_names.rs:32-38 only recognises the literal prefixes "ANY ", "any ", "FIRST ", "first " -- exact case, exact single space. Verified by simulation: 'Any 1 (a, b)' -> Some(First, 1, ['Any 1 (a', 'b)']); 'ANY\t1 (a,b)' -> Some(First, 1, ['ANY\t1 (a', 'b)']); '2 (A,B,C)' -> Some(First, 1, ['2 (A','B','C)']). Each returns Some with garbage members, so the defensive path at split_brain.rs:650 is never taken and observed=0 -> spurious PrimaryQuorumUnsatisfied. Also unhandled: postgres's quoted-identifier rules (syncrep_scanner.l xd state -- `""` un-doubles to `"`, and `[^"]+` lets a quoted name contain commas), whereas split_members (sync_standby_names.rs:59-64) splits on ',' before dequoting and uses trim_matches('"'). Finally postgres matches names with pg_strcasecmp (case-insensitive) while the resolver compares with `==` (case-sensitive).
- **PG15 vs PG17:** The grammar is the same language in 17; the 17 diff to syncrep_gram.y/.l is reentrancy plumbing and error text, not accepted syntax.
- **Query to settle:** `n/a -- settled from rendered config; the exporter query at roles/postgres_exporter/templates/queries.yml.j2:199-206 already tracks the shape fleet-wide.`
- **Impact if wrong:** Bounded and, on this fleet, unreachable: all 321 rendered manifests are `ANY 1 ( a, b )` with canonical uppercase and single spaces, and postgresSynchronousStandbyNamesMisconfigured alerts on any deviation on any node. Rate: reachable in code, not plausible on fleet. The one worth a cheap guard is case-insensitivity, since it is a one-line change and matches postgres semantics exactly.
- **Code depending on it:** src/v2/analyze/sync_standby_names.rs:31-46 and :59-64

### [info | confirmed | confidence=certain] ADR lines 107 (§2, Not Refuse-worthy): "each primary evaluates its own SSN locally, so divergence cannot break the per-primary quorum reasoning the resolver depends on"
- **Claim:** SSN is evaluated purely locally per primary, so cross-node SSN divergence cannot corrupt per-primary quorum reasoning.
- **Postgres evidence:** Correct. SyncRepConfig is a process-local pointer assigned from the node's own GUC extra (syncrep.c:1147-1151 assign_synchronous_standby_names), and every quorum decision reads only that node's SyncRepConfig plus that node's WalSndCtl->walsnds. There is no cross-node exchange of SSN in the replication protocol. A standby's own SSN value is inert while it is in recovery -- the alert runbook says the same thing in operational terms ("Postgres itself only consults the value on the primary") -- and becomes active at promotion with no rewrite.
- **PG15 vs PG17:** Unchanged.
- **Query to settle:** `On each standby: SELECT current_setting('synchronous_standby_names'); -- add 'synchronous_standby_names' to the pg_settings IN-list in HEALTH_CHECK_REPLICA_QUERY (src/v2/scan/health_check_replica.rs:108-113) to capture it.`
- **Impact if wrong:** None. One adjacent gap worth recording: the scanner does not capture synchronous_standby_names on replicas -- health_check_replica.rs:105-114 requests only hot_standby, primary_conninfo, primary_slot_name, recovery_target_timeline -- so a scan of a healthy cluster cannot see what SSN a standby will activate on promotion. That is the single field that would have let the ADR settle claim 1 from captured data rather than by assumption; adding it costs one line.
- **Code depending on it:** src/v2/analyze/split_brain.rs:646-651 (reads each primary's own configuration["synchronous_standby_names"])


## Postgres behaviour audit: pg_stat_wal_receiver -- what it exposes and when (fleet = PG 15.14; ADR annotations cite PG 17)

### [high | refuted | confidence=high] ADR lines docs/adr/002-...md:28 and :87 ("Keepalives are sent at `wal_sender_timeout / 2` ~= 150 s"; "replica side [updates] on keepalive (~150 s in our config)")
- **Claim:** The replica-side `last_msg_receipt_time` update cadence is driven by the primary's keepalive at `wal_sender_timeout / 2` (~150 s here), which is why a 180 s freshness threshold is 'comfortably above' it.
- **Postgres evidence:** PG 15 REL_15_STABLE src/backend/replication/walsender.c, WalSndKeepaliveIfNecessary(): the ping is scheduled off `last_reply_timestamp` -- the time of the last message received FROM the standby -- not off the last message sent. PG 15 walreceiver.c XLogWalRcvSendReply() returns early only if positions are unchanged AND less than `wal_receiver_status_interval` has passed, so an idle standby still replies every 10 s (default). A standby that replies every 10 s keeps `last_reply_timestamp` fresh, so the sender's 150 s ping never fires in a healthy pair. What actually keeps the receiver-side timestamps moving on an idle cluster is the STANDBY-driven ping: walreceiver.c main loop, `if (!ping_sent) { timeout = last_recv_timestamp + wal_receiver_timeout/2; if (now >= timeout) { requestReply = true; ping_sent = true; } }`, answered by walsender.c ProcessStandbyReplyMessage: `if (replyRequested) WalSndKeepalive(false, InvalidXLogRecPtr);`. Both timestamps are then set by walreceiver.c ProcessWalSndrMessage() for every 'w' and 'k' message: `walrcv->lastMsgSendTime = sendTime;` (the SENDER's clock, from the message) and `walrcv->lastMsgReceiptTime = GetCurrentTimestamp()` (the RECEIVER's clock). Fetched verbatim from raw.githubusercontent REL_15_STABLE. PG 15 docs runtime-config-replication: wal_receiver_timeout default 60 s, wal_receiver_status_interval default 10 s.
- **PG15 vs PG17:** No difference. The keepalive scheduling, the wal_receiver_timeout/2 ping and ProcessWalSndrMessage are unchanged between 15 and 17; the defaults (60 s / 10 s) are the same.
- **Query to settle:** `On each replica: SELECT name, setting, unit, source FROM pg_settings WHERE name IN ('wal_receiver_timeout','wal_receiver_status_interval','wal_retrieve_retry_interval'); then, to measure the real idle cadence: SELECT now() - last_msg_receipt_time AS age, now() - last_msg_send_time FROM pg_stat_wal_receiver; sampled every 5 s for 10 min on a cluster with no write traffic.`
- **Impact if wrong:** Answers the assignment's idle-cluster question: YES, they keep advancing on an idle cluster, but at ~wal_receiver_timeout/2 (~30 s default), NOT at 150 s, and ONLY while `wal_receiver_timeout > 0`. If `wal_receiver_timeout = 0` on a replica, no standby ping is sent, the receiver never self-terminates, and `last_msg_receipt_time` freezes indefinitely on a healthy idle pair -- then a genuinely-following replica fails the 180 s gate, C-b/C-c fall through to `HigherTimeline`, and the tool tells the operator to demote the lower-TL primary that holds the acked writes. `wal_receiver_timeout` is not captured by HEALTH_CHECK_REPLICA_QUERY (src/v2/scan/health_check_replica.rs:105-114 lists only hot_standby, primary_conninfo, primary_slot_name, recovery_target_timeline), so we cannot tell from any capture we hold.
- **Code depending on it:** src/v2/analyze/split_brain.rs:284 (`wr.last_msg_receipt_time.is_some_and(|t| (r_health.current_time - t).num_milliseconds() <= threshold_ms)`) with threshold from split_brain.rs:268 (`parse_wal_sender_timeout(...)/2 + 30_000`)

### [high | refuted | confidence=high] ADR lines docs/adr/002-...md:70, :82-:87, and matrix row C-d at :44 ("OR `last_msg_receipt_time` aged out")
- **Claim:** `freshness_threshold = wal_sender_timeout_ms / 2 + 30_000` applied to `wal_receiver.last_msg_receipt_time` separates a live stream from a stale `wal_receiver` row, because 'a dead wal_receiver keeps its values until wal_sender_timeout fires'.
- **Postgres evidence:** Two PG 15 facts kill this. (1) A dead walreceiver leaves NO row, not a stale one: walreceiver.c WalRcvDie() sets `walrcv->pid = 0` and `walrcv->ready_to_display = false`, and pg_stat_get_wal_receiver() does `if (pid == 0 || !ready_to_display) PG_RETURN_NULL();` while the view filters `WHERE s.pid IS NOT NULL` (system_views.sql, REL_15_STABLE, verbatim). (2) The replica-side row's maximum staleness is governed by the REPLICA's `wal_receiver_timeout`, not the primary's `wal_sender_timeout`: walreceiver.c main loop raises `ereport(ERROR, errmsg("terminating walreceiver due to timeout"))` once `now >= last_recv_timestamp + wal_receiver_timeout`, and the process exits, removing the row. With the 60 s default and a 180 s threshold, any row that EXISTS is at most ~60 s old, so the freshness predicate can never reject it.
- **PG15 vs PG17:** No difference; identical WalRcvDie / timeout logic and identical view WHERE clause in REL_17_STABLE.
- **Query to settle:** `SHOW wal_receiver_timeout; on every replica. If it is 0, claim 1's freeze hazard is live and this check matters; if it is 60000, this predicate is dead code and C-d's second disjunct should be struck from the ADR.`
- **Impact if wrong:** The replica-side half of the §1 gate provides less protection than the ADR claims: C-d's `last_msg_receipt_time` aged-out sub-case is unreachable while wal_receiver_timeout is at its default, so the gate's real discriminators are only the sender_host/sender_port/status checks plus primary-side corroboration. The genuinely-persistent zombie row is on the PRIMARY side (pg_stat_replication survives up to wal_sender_timeout = 300 s here), and there the same 180 s threshold leaves a 0-180 s window in which a disconnected standby's row still passes -- the ADR acknowledges this at :87 but derives the number from the wrong side. Net: the threshold formula is well-shaped for the primary side and arbitrary for the replica side it is documented to protect. The only configuration where the replica-side check does real work is wal_receiver_timeout = 0 plus a black-holed TCP connection.
- **Code depending on it:** src/v2/analyze/split_brain.rs:276-287 -- the comment "a dead `wal_receiver` keeps its values until `wal_sender_timeout` fires, so without this check `status=\"streaming\"` alone would pass on a connection that just died" is factually wrong, and the predicate it justifies is a no-op on default settings.

### [high | partially-correct | confidence=high] ADR lines docs/adr/002-...md:245 and :221 ("a timeline-wedged replica (C-g ...) likely has *no* `wal_receiver` at all: it cannot establish streaming past the fork"); docs/concepts/split-brain.md:85-:87
- **Claim:** A timeline-wedged replica cannot establish streaming past the fork, therefore it exposes no `wal_receiver` row, therefore `received_tli`/`flushed_lsn` are unavailable and data-loss danger is anti-correlated with wal_receiver-based detectability.
- **Postgres evidence:** The wedge mechanism is derivable and confirmed. With `recovery_target_timeline=latest` (fixture confirms), PG 15 xlogrecovery.c rescanLatestTimeLine() refuses to adopt TL=N+1 when the fork precedes the replica's recovery point and returns false at LOG level, so `recoveryTargetTLI` stays N; WaitForWALToBecomeAvailable then cycles archive -> pg_wal -> stream with a `wal_retrieve_retry_interval` sleep (5 s default). Each stream attempt requests `START_REPLICATION ... TIMELINE N`, and the higher-TL primary's walsender rejects it: walsender.c StartReplication(), `if (!XLogRecPtrIsInvalid(switchpoint) && switchpoint < cmd->startpoint) ereport(ERROR, errmsg("requested starting point %X/%X on timeline %u is not in this server's history"), errdetail("This server's history forked from timeline %u at %X/%X."))` (verbatim, REL_15_STABLE). BUT visibility is not gated on streaming succeeding: WalReceiverMain sets `walrcv->pid = MyProcPid; walrcv->walRcvState = WALRCV_STREAMING; walrcv->ready_to_display = false;` and seeds `walrcv->lastMsgSendTime = walrcv->lastMsgReceiptTime = walrcv->latestWalEndTime = now;` ("Initialise to a sanish value") BEFORE connecting, then sets `ready_to_display = true` immediately AFTER a successful `walrcv_connect()` and BEFORE walrcv_identify_system / WalRcvFetchTimeLineHistoryFiles / walrcv_startstreaming. All fetched verbatim from REL_15_STABLE walreceiver.c.
- **PG15 vs PG17:** Same in 17. The ready_to_display-before-START_REPLICATION ordering and the StartReplication ERROR are unchanged; PG 17 formats LSNs the same way in this message.
- **Query to settle:** `Only a reproduction settles the duty cycle. Recipe: 3-node lab, promote db002, keep db001 writing on TL=N with db003 acking past the fork, then repoint db003 at db002 and poll `SELECT clock_timestamp(), * FROM pg_stat_wal_receiver;` at 200 ms for 60 s alongside `SELECT clock_timestamp(), application_name, state, backend_start FROM pg_stat_replication;` on db002 and the db003 server log. Record: fraction of samples with a row, the status value, received_tli/flushed_lsn in the transient row (RequestXLogStreaming sets `flushedUpto = recptr` segment-aligned, so these may report the REQUESTED start, not the replica's true position), and the db002-side state.`
- **Impact if wrong:** The claim is right about the steady state and wrong about the structure. Correct statement: 'usually zero rows, on a ~5 s duty cycle a row IS present'. During each retry the view shows exactly one row with status='streaming', sender_host = the higher-TL primary, sender_port 5432, and last_msg_receipt_time equal to the walreceiver's start instant -- i.e. always fresh -- for a replica that has never received a byte from that primary. So the §1 replica-side gate PASSES for db002 in the dangerous C-g state. What prevents the disaster today is the primary-side corroboration, not the ADR's stated reasoning: db002's walsender never reaches `WalSndSetState(WALSNDSTATE_CATCHUP)` (that call is inside StartReplication, after the ERROR), so pg_stat_replication.state = 'startup', which src/v2/analyze/split_brain.rs:307-312 rejects -> `PrimaryDoesNotSeeReplica` -> no follower counted. If anyone ever relaxes the primary-side check to 'a row exists', C-g flips from 'no evidence' to 'positive evidence for the data-destroying pick'. Second correction: 'no wal_receiver' does not imply 'wedged' -- a standby with a working restore_command and a shared archive can replay TL=N indefinitely with zero rows in this view, fully caught up and NOT wedged, and equally not acking anything (archive recovery has no replication connection, so it cannot satisfy any primary's sync quorum). `wal_receiver IS NULL` is therefore not a wedge detector in either direction, which weakens §7's framing that the control-file capture alone closes the gap.
- **Code depending on it:** src/v2/analyze/split_brain.rs:272 (`let Some(wr) = &r_health.wal_receiver else { continue; }`) and :281-287 -- a C-g replica that is caught mid-retry passes every replica-side condition for the WRONG primary.

### [high | refuted | confidence=certain] ADR lines docs/adr/002-...md:70 ("`wal_receiver.last_msg_receipt_time` is within `freshness_threshold`") read as evidence of an active stream; matrix C-a/C-b at :41-:42 ("recent receipt")
- **Claim:** A fresh `last_msg_receipt_time` is evidence that the replica is actively receiving WAL from the named sender.
- **Postgres evidence:** PG 15 walreceiver.c WalReceiverMain seeds the shared-memory fields before any message arrives: `/* Initialise to a sanish value */ walrcv->lastMsgSendTime = walrcv->lastMsgReceiptTime = walrcv->latestWalEndTime = now;`, and this happens inside the same spinlock section that sets pid and WALRCV_STREAMING, i.e. before `walrcv_connect()`. `ready_to_display` (which controls whether the row appears at all) is set true right after connect. So the earliest observable value of last_msg_receipt_time is 'when this walreceiver process started', not 'when WAL last arrived'.
- **PG15 vs PG17:** Identical in 17.
- **Query to settle:** `Same reproduction as the C-g row above; also cheaply demonstrable by pointing a standby's primary_conninfo at a port with no listener-turned-listener that rejects replication, then sampling `SELECT status, last_msg_receipt_time, now() - last_msg_receipt_time FROM pg_stat_wal_receiver;`.`
- **Impact if wrong:** In any walreceiver crash-loop -- which is precisely the C-g state, and also the ordinary 'primary refuses the connection' state -- the freshness predicate is satisfied on every sample, because the process restarts every ~5 s and re-stamps the field. Combined with status being 'streaming' from before the connect, the replica-side gate reports maximal confidence exactly when the replica is receiving nothing. The gate's actual proof-of-liveness is the primary-side pg_stat_replication row, which the ADR labels 'corroborating only' (:56, :72).
- **Code depending on it:** src/v2/analyze/split_brain.rs:284-286

### [high | annotation-does-not-support-claim | confidence=certain] ADR lines docs/adr/002-...md:29 ("Scanner role has `pg_read_server_files`") -- the only privilege assumption stated anywhere in the ADR; also :204 and :266
- **Claim:** The scanner's privilege requirements for the new data collection are covered by `pg_read_server_files`.
- **Postgres evidence:** PG 15 walreceiver.c pg_stat_get_wal_receiver() masks every column except pid for unprivileged roles, verbatim: `if (!has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS)) { /* Only superusers and roles with privileges of pg_read_all_stats can see details. Other users only get the pid value to know whether it is a WAL receiver, but no details. */ MemSet(&nulls[1], true, sizeof(bool) * (tupdesc->natts - 1)); }`. `pg_read_server_files` does not imply `pg_read_all_stats`; they are unrelated predefined roles (pg_monitor grants pg_read_all_stats, pg_read_all_settings, pg_stat_scan_tables). pg_control_system() / pg_control_checkpoint() are separately REVOKEd from PUBLIC.
- **PG15 vs PG17:** Same masking in 17 (has_privs_of_role / ROLE_PG_READ_ALL_STATS unchanged). In PG <= 13 the macro is DEFAULT_ROLE_READ_ALL_STATS and the check is is_member_of_role -- same effect.
- **Query to settle:** `As the scanner role on a replica: SELECT current_user, pg_has_role(current_user,'pg_read_all_stats','USAGE') AS has_stats, rolsuper FROM pg_roles WHERE rolname = current_user; and SELECT * FROM pg_stat_wal_receiver;  -- if conninfo/status come back NULL, the role is masked.`
- **Impact if wrong:** An unprivileged scanner role gets ONE row with pid set and 14 NULLs. That deserialises to an error, not to `wal_receiver: None` -- execute_replica_health_check returns Err, check() sets `Role::UnknownReplica` (health_check_replica.rs:155), and `Cluster::replicas()` filters on `matches!(self, Role::Replica{..})` (src/v2/cluster.rs:74, src/v2/scan.rs:337), so the replica vanishes from `resolve_split_brain`'s input entirely. Every primary then has zero gated followers -> `HigherTimeline` -> 'demote the lower-TL primary'. A privilege downgrade thus converts into the destructive verdict rather than into a visible gate failure (a per-node error IS recorded, but the split-brain verdict never sees it). Not reachable on the fleet as configured: the captured fixture shows `conninfo` populated on both replicas, which proves the production role has pg_read_all_stats or is superuser. The ADR should state that requirement next to pg_read_server_files.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:37-53 -- `pid`, `status`, `receive_start_lsn`, `receive_start_tli`, `written_lsn`, `flushed_lsn`, `received_tli`, `latest_end_lsn`, `sender_host`, `sender_port`, `conninfo` are all non-Option, so a masked row (`{"pid":123,"status":null,...}`) fails serde at health_check_replica.rs:192

### [medium | refuted | confidence=certain] ADR lines docs/adr/002-...md:69 ("`wal_receiver.status` in {\"streaming\", \"catchup\"}. `catchup` is genuinely-following mid-recovery and must not be rejected"); matrix C-a/C-b/C-c/C-d at :41-:44 also say "status=streaming/catchup"
- **Claim:** `catchup` is a value of `pg_stat_wal_receiver.status` and must be accepted by the replica-side gate.
- **Postgres evidence:** PG 15 src/include/replication/walreceiver.h, verbatim: `typedef enum { WALRCV_STOPPED, WALRCV_STARTING, WALRCV_STREAMING, WALRCV_WAITING, WALRCV_RESTARTING, WALRCV_STOPPING } WalRcvState;` and pg_stat_get_wal_receiver() maps these to exactly six strings: stopped, starting, streaming, waiting, restarting, stopping. `catchup` is a WALSENDER state (WALSNDSTATE_CATCHUP) and appears only in pg_stat_replication.state, whose full set is startup, catchup, streaming, backup, stopping. The ADR imports the primary-side vocabulary into the replica-side gate.
- **PG15 vs PG17:** Identical enum and identical string set in 17.
- **Query to settle:** `SELECT DISTINCT status FROM pg_stat_wal_receiver; on any replica returns 'streaming'. Definitive: no PG release maps any WalRcvState to 'catchup'.`
- **Impact if wrong:** No wrong verdict from the dead arm itself, and the ADR's INTENT ('do not reject a replica that is still catching up') is satisfied by accident: a walreceiver reports 'streaming' from the moment it starts regardless of how far behind it is, and the primary side correctly accepts ReplicationState::Catchup. But the ADR sentence is a factual error that will mislead the next person to touch the gate, and the matrix rows C-a..C-d encode the same error four more times. Fix: replica-side status set is {streaming} plus a decision on waiting/restarting (next claim).
- **Code depending on it:** src/v2/analyze/split_brain.rs:283 `&& matches!(wr.status.as_str(), "streaming" | "catchup")` -- the `"catchup"` arm is unreachable. The conflation is visible in the code itself: the primary side is modelled with a correct typed enum (src/v2/scan/health_check_primary.rs:27-35 = Startup/Catchup/Streaming/Backup/Stopping) while the replica side is a raw String compared against that enum's value names.

### [medium | refuted | confidence=high] ADR lines docs/adr/002-...md:44 (C-d, "status != streaming/catchup ... -> `ReplicaWalReceiverStale(db003, db001)`") and :46 (C-f, "status=stopped/starting")
- **Claim:** The non-passing replica-side statuses are `stopped` and `starting`, and rejecting on status justifies emitting `ReplicaWalReceiverStale`.
- **Postgres evidence:** `stopped` and `starting` are structurally unobservable in the view. WalRcvDie() sets state=WALRCV_STOPPED together with `pid = 0` and `ready_to_display = false` in one spinlock section, and during WALRCV_STARTING the pid is still 0 (WalReceiverMain sets pid and flips the state to WALRCV_STREAMING in the same section, with ready_to_display=false until after connect). The view's `WHERE s.pid IS NOT NULL` therefore filters both out. The statuses that ARE observable and are NOT 'streaming' are `waiting` (WalRcvWaitForStartPosition -- the walreceiver has reached the end of a timeline and is waiting for the startup process to hand it a new start position), `restarting` (RequestXLogStreaming on a live receiver), and `stopping` (ShutdownWalRcv on a live receiver). All three keep pid != 0 and ready_to_display == true.
- **PG15 vs PG17:** Identical in 17.
- **Query to settle:** `Cannot be produced on demand from SQL; observe during a controlled `repmgr standby follow` / promotion by polling `SELECT clock_timestamp(), pid, status FROM pg_stat_wal_receiver;` at 100 ms on the follower across the switch, and record how long status is non-'streaming' while a row is present.`
- **Impact if wrong:** Two problems. (a) The C-f row lists two impossible values and omits the three real ones; the outcome it predicts is still right, because stopped/starting manifest as 'wal_receiver absent', which C-f also lists -- so no behavioural error, doc drift only. (b) The real one: `waiting` and `restarting` are exactly the states a healthy replica passes through during a TIMELINE SWITCH, i.e. during the post-failover window this tool is designed to run in. A scan that lands there rejects a genuinely-following replica, mislabels it 'stale' (the row is not stale -- it is mid-handoff, with a fresh receipt time), and drops the follower, so C-b/C-c fall through to `HigherTimeline` and the tool recommends demoting the lower-TL primary. The windows are short (sub-second to seconds), so the probability per scan is low, but the direction of the error is toward the destructive recommendation.
- **Code depending on it:** src/v2/analyze/split_brain.rs:283 rejects them, and :288-298 then labels the rejection `ReplicaWalReceiverStale { replica, claimed_sender }`

### [medium | partially-correct | confidence=certain] ADR lines docs/adr/002-...md:67 ("The comparison is `==` against `primary.ip_address.to_string()`; in environments where `primary_conninfo` uses a hostname, this comparison fails. Out of scope for v1 ... (Current production uses IPs.)")
- **Claim:** `sender_host` matches the primary's inventory IP on this fleet, and the only failure mode of the `==` comparison is a hostname-form `primary_conninfo`.
- **Postgres evidence:** PG 15 libpqwalreceiver.c libpqrcv_get_senderinfo(): `ret = PQhost(conn->streamConn); if (ret && strlen(ret) != 0) *sender_host = pstrdup(ret);` -- PQhost returns 'the verbatim host value provided by user, or hostaddr in lieu of that', for whichever entry of a multi-host list actually connected. There is no name resolution, no reverse lookup, no canonicalisation. PG 15 docs, pg_stat_wal_receiver.sender_host: 'Host of the PostgreSQL instance this WAL receiver is connected to. This can be a host name, an IP address, or a directory path if the connection is via Unix socket.'
- **PG15 vs PG17:** Identical in 17; PQhost semantics unchanged.
- **Query to settle:** `On each replica: SELECT sender_host, sender_port, conninfo FROM pg_stat_wal_receiver; compared against SHOW primary_conninfo; and against the inventory ip_address for the node named there.`
- **Impact if wrong:** Confirmed against the fixture: on both replicas `primary_conninfo` contains `host=127.1.12.151` and `sender_host` is exactly `127.1.12.151`, which equals db001's inventory `ip_address` -- so the `==` holds today and the ADR's 'production uses IPs' is accurate. The understatement is the risk: the comparison is against the INVENTORY address, so it breaks for any conninfo host string that is not byte-identical to it -- a VIP or floating address, a second interface/replication subnet, an IPv6 literal formatted differently, a Unix-socket directory path, or a multi-host conninfo where a different entry won. In all of those the replica-side gate fails for every primary, `ReplicaWalReceiverStale` is emitted against whichever primary the string happens to name (or silently nothing if it names neither), and the resolver has no follower evidence -> `HigherTimeline`. Worth noting as a scoping fix: the ADR's known limitation should be 'sender_host is a verbatim conninfo string and need not equal the inventory IP', not 'hostnames are out of scope'. Separately, an INFO-level fixture nit: src/v2/scan/health_check_replica.rs:219 has `conninfo ... host=10.81.12.151` while :222 has `sender_host: 127.1.12.151` in the same captured row -- an anonymisation inconsistency that a future reader could mistake for evidence that the two legitimately differ.
- **Code depending on it:** src/v2/analyze/split_brain.rs:281 and :293 (`wr.sender_host == primary.ip_address.to_string()`); also src/v2/analyze/checks.rs:253

### [low | refuted | confidence=certain] ADR lines n/a -- implementation question posed by the assignment against docs/adr/002-...md:70 (the gate reads `wal_receiver` at all)
- **Claim:** The scanner's `SELECT COALESCE(to_jsonb(t), '{}'::jsonb) FROM (SELECT ... FROM pg_stat_wal_receiver) t` needs the COALESCE to handle an absent walreceiver, and an empty object could reach `WalReceiverInfo`.
- **Postgres evidence:** The view is `... FROM pg_stat_get_wal_receiver() s WHERE s.pid IS NOT NULL;` -- fetched verbatim and byte-identical in REL_15_STABLE and REL_17_STABLE system_views.sql. pg_stat_get_wal_receiver() is a single-record (non-set-returning) function that does `if (pid == 0 || !ready_to_display) PG_RETURN_NULL();`, so in FROM position it yields exactly one all-NULL row when there is no receiver, and the view's WHERE turns that into ZERO rows. The view therefore never returns a row with a NULL pid. A scalar subquery over zero rows evaluates to SQL NULL and the select-list COALESCE is never evaluated; jsonb_build_object stores JSON null for the key.
- **PG15 vs PG17:** No difference -- both branch definitions of the view carry `WHERE s.pid IS NOT NULL` and the same 15 columns. Note the column list requires PG >= 13 (`written_lsn`/`flushed_lsn` replaced `received_lsn` in 13), which is satisfied.
- **Query to settle:** `On a replica with the walreceiver stopped (e.g. during a restore_command stretch): SELECT count(*) FROM pg_stat_wal_receiver;  -- expect 0. Contrast: SELECT count(*), pid FROM pg_stat_get_wal_receiver() GROUP BY pid;  -- expect 1 row with pid NULL.`
- **Impact if wrong:** Answering the assignment directly: zero rows, never one-row-with-NULLs (that shape exists only if you query `pg_stat_get_wal_receiver()` directly, bypassing the view). The COALESCE is dead -- `to_jsonb` of a FROM-clause whole-row var cannot be NULL, and an all-NULL row cannot survive the view's WHERE. It is not a live bug and not version-dependent (15 and 17 verified identical). It IS a latent trap with a bad blast radius: if the `{}` branch ever fired, serde would fail on the missing non-Option `pid`/`status`/... fields, the whole replica health check would error, and the node would become `Role::UnknownReplica` and disappear from `Cluster::replicas()` -- the same silent path to `HigherTimeline` described in the privilege finding. The pattern is correct 30 lines below at :95 (`COALESCE(jsonb_object_agg(...), '{}')`, where the aggregate genuinely returns NULL over zero rows) and was copied to a place where it does nothing. Also worth recording as safe-by-construction: no LIMIT is needed because the view yields at most one row (PG 15 docs: 'Only one row, showing statistics about the WAL receiver'), so the scalar subquery cannot raise 'more than one row returned by a subquery used as an expression'.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:66 (`SELECT COALESCE(to_jsonb(t), '{}'::jsonb)`) and :24 (`pub wal_receiver: Option<WalReceiverInfo>`)

### [info | confirmed | confidence=certain] ADR lines docs/adr/002-...md:213 ("**(Validated 2026-09-10.)** Both functions return SQL NULL, never `0/0` ... PG17 `xlogfuncs.c` guards each with `if (recptr == 0) PG_RETURN_NULL();`")
- **Claim:** The NULL-not-zero behaviour of pg_last_wal_receive_lsn()/pg_last_wal_replay_lsn() is a PG 17 fact.
- **Postgres evidence:** Verified on the fleet's actual major version: REL_15_STABLE src/backend/access/transam/xlogfuncs.c, verbatim -- `pg_last_wal_receive_lsn`: `recptr = GetWalRcvFlushRecPtr(NULL, NULL); if (recptr == 0) PG_RETURN_NULL(); PG_RETURN_LSN(recptr);` and `pg_last_wal_replay_lsn`: `recptr = GetXLogReplayRecPtr(NULL); if (recptr == 0) PG_RETURN_NULL(); PG_RETURN_LSN(recptr);`.
- **PG15 vs PG17:** Identical; the guard predates 15.
- **Query to settle:** `SELECT pg_last_wal_receive_lsn() IS NULL, pg_last_wal_replay_lsn() IS NULL FROM (SELECT 1) x;  -- on a fresh-restarted standby before any receive, expect t.`
- **Impact if wrong:** No action needed -- flagging only because the annotation cites PG 17 while the fleet is 15.14, and this is the one PG-17-cited claim in the ADR that I could check for a version gap. There is none. Same guard in both branches, so `Option<String>` is the right shape on 15.14 too.
- **Code depending on it:** src/v2/scan/health_check_replica.rs:30 and :33 (`Option<String>`), and the test at :256

### [low | refuted | confidence=high] ADR lines docs/concepts/split-brain.md:89-:95 ("The standby logs a `FATAL` of the form: new timeline N+1 forked off current database system timeline N before current recovery point X/X")
- **Claim:** The wedge signature in the standby log is a FATAL.
- **Postgres evidence:** In PG 15 that exact message text is emitted by `rescanLatestTimeLine()` in src/backend/access/transam/xlogrecovery.c at **LOG** level; the function returns false and recovery continues looping through archive -> pg_wal -> stream with a wal_retrieve_retry_interval sleep. The FATALs with adjacent wording are different sites: `InitWalRecovery()` raises `ereport(FATAL, errmsg("requested timeline %u does not contain minimum recovery point %X/%X on timeline %u"))` (confirmed present in REL_15_STABLE xlogrecovery.c) and `"requested timeline %u is not a child of this server's history"` -- both are server-START-time checks, not running-standby checks. The running-standby FATAL that a wedged replica actually produces every retry comes from the SENDER: walsender.c `ereport(ERROR, errmsg("requested starting point %X/%X on timeline %u is not in this server's history"))`, which surfaces in the standby log as the walreceiver's fatal exit.
- **PG15 vs PG17:** Same in 17; PG 18+ reformats the LSN as %X/%08X but the level and wording are unchanged.
- **Query to settle:** `Not settleable by SQL. In the C-g reproduction, capture the standby log with log_min_messages=debug1 and record the exact prefix (LOG:/FATAL:) of every line containing 'timeline'. I could not retrieve rescanLatestTimeLine's body verbatim -- raw.githubusercontent truncated xlogrecovery.c before it -- so this rests on the source as I know it plus the pgsql-bugs thread for BUG #8294, not on a fetched quote.`
- **Impact if wrong:** An operator or a future log-scraper told to grep for FATAL will miss the running-standby signature entirely, because the 'forked off ... before current recovery point' line is LOG. The doc already flags the message as taken from BUG #8294 rather than from one of our own runs, which is honest; the level is the part that is wrong. Recommend the doc list all three: LOG 'new timeline ... forked off ... before current recovery point' (loops, every rescan), FATAL 'requested starting point ... is not in this server's history' (walreceiver, every ~5 s retry), and the start-time FATALs.
- **Code depending on it:** No code depends on it; it is operator-facing guidance in the concepts doc.

### [info | refuted | confidence=certain] ADR lines docs/adr/002-...md:7 ("a conservative `Refuse`-only floor is shippable today (§7)") vs :245 ("which is why no conservative 'Refuse-only floor' is shipped in the interim")
- **Claim:** The ADR states a single, consistent position on whether a Refuse-only floor for DivergentReplicaWal ships in the interim.
- **Postgres evidence:** n/a -- this is an internal ADR contradiction, not a Postgres behaviour claim. Reported because I read §7 end-to-end for the detectability-gap assessment and both sentences are load-bearing for what §7 authorises.
- **PG15 vs PG17:** n/a
- **Query to settle:** `n/a -- resolve by editing one of the two sentences.`
- **Impact if wrong:** Outside my assignment (the §7-consistency auditor probably has it), so treat as a duplicate if already reported. The 2026-06-07 revision header (:7) promises a shippable Refuse-only floor; the body of §7 (:245) explicitly rejects one on the grounds that it would 'add over-caution to safe cases and false confidence to the dangerous one'. The body's reasoning is the sounder of the two and is what the code implements.
- **Code depending on it:** Nothing emits DivergentReplicaWal today, so no code disagrees with either sentence; the risk is that a future implementer picks the wrong sentence.


---

# Appendix D -- infra / config-as-code evidence (raw)


## On an IDLE but healthy streaming link, does pg_stat_wal_receiver.last_msg_receipt_time keep advancing, and does ADR-002's 180 s freshness gate therefore hold?

**Confidence:** high

Yes, it keeps advancing, and the 180 s gate holds on this fleet -- but for a reason ADR-002 states incorrectly, and the gate is keyed off the wrong GUC on the wrong node. F7 is a doc-accuracy defect plus a latent (currently non-firing) correctness bug, NOT a second live critical bug.

Mechanism, established from REL_15_14 source:

(1) Primary-initiated keepalives ARE fully suppressed on a healthy link, exactly as you suspected. `WalSndKeepaliveIfNecessary()` (walsender.c:3670-3697) returns early unless `last_processing >= last_reply_timestamp + wal_sender_timeout/2`. `last_reply_timestamp` is reset to now on ANY message received from the standby (walsender.c:2001-2005, `if (received) { last_reply_timestamp = last_processing; waiting_for_ping_response = false; }`). The standby sends a routine status reply every `wal_receiver_status_interval` (10 s, default, commented out on all 321 manifests) via walreceiver.c:583 -> `XLogWalRcvSendReply()` (walreceiver.c:1074-1128), whose rate limit is `TimestampDifferenceExceeds(sendTime, now, wal_receiver_status_interval * 1000)`. So `last_reply_timestamp` is never older than ~10 s, the 150 s ping_time is never reached, and `WalSndKeepalive()` is never called from that path. `WalSndComputeSleeptime()` (walsender.c:2386-2416) merely schedules a wakeup at `last_reply_timestamp + wal_sender_timeout/2`, which keeps getting pushed forward every 10 s.

(2) With keepalives suppressed and no WAL to ship, the primary sends the standby NOTHING. `XLogSendPhysical()` returns at walsender.c:2905-2910 (`if (SendRqstPtr <= sentPtr) { WalSndCaughtUp = true; return; }`) without touching the socket, and `WalSndLoop()` (walsender.c:2495-2580) then just calls `WalSndCheckTimeOut()` / `WalSndKeepaliveIfNecessary()` and sleeps. Every other `WalSndKeepalive()` call site is logical-decoding-only (walsender.c:1515, 1621, via `ProcessPendingWrites`) or shutdown (`WalSndDone`, walsender.c:3149). The single remaining path is walsender.c:2143, `if (replyRequested) WalSndKeepalive(false, InvalidXLogRecPtr);` -- a reply to a standby ping.

(3) So the heartbeat is driven from the STANDBY, not the primary. walreceiver.c:553-583: on the WL_TIMEOUT branch (every NAPTIME_PER_CYCLE = 100 ms, walreceiver.c:98), if `now >= last_recv_timestamp + wal_receiver_timeout/2` and `!ping_sent`, it sets `requestReply = true; ping_sent = true;` and calls `XLogWalRcvSendReply(requestReply, requestReply)`. The primary answers immediately with a 'k' message, the standby processes it in `XLogWalRcvProcessMsg` case 'k' (walreceiver.c:851-871) -> `ProcessWalSndrMessage()` (walreceiver.c:1244-1258), which is the ONLY writer of `walrcv->lastMsgReceiptTime` (= GetCurrentTimestamp()) and `walrcv->lastMsgSendTime` (= the sender's sendTime); the only two callers are the 'w' and 'k' cases. Receipt of any data also resets `last_recv_timestamp` and clears `ping_sent` (walreceiver.c:465-466).

(4) PAYOFF: on an idle-but-healthy cluster the refresh cadence of `last_msg_receipt_time` is `wal_receiver_timeout / 2` = 150 s, plus up to 100 ms of walreceiver loop granularity, plus one RTT. Worst-case age is therefore ~150.2 s on a LAN. The 180 s gate clears it with ~30 s to spare, so NO false negative, no fall-through to HigherTimeline, no spurious demote instruction. The comparison is also skew-immune: split_brain.rs:284-286 subtracts `last_msg_receipt_time` from `r_health.current_time`, which is the replica's own `SELECT now()` (health_check_replica.rs:62), not the scanner's clock -- so the 30 s of slack is not eaten by clock drift. (Note ADR-002 lines 70 and 75 say "of the scan-start timestamp", which the code does not do; the code's choice is the safer one.)

The latent bug: `threshold_ms = parse_wal_sender_timeout(&p_health.configuration) / 2 + 30_000` (split_brain.rs:266) derives the replica-side threshold from the PRIMARY's `wal_sender_timeout`, but the cadence it must cover is `wal_receiver_timeout / 2` on the REPLICA. These coincide only because this fleet renders 5min for both from the same template defaults. Three ways it breaks: (a) `parse_wal_sender_timeout` falls back to 60_000 if the key is missing/unparseable (split_brain.rs:609-613), giving a 60 s threshold against a 150 s cadence -- systematic false negative on every idle replica, i.e. exactly the destructive route you were probing; (b) any node overriding `postgres_wal_receiver_timeout` above the primary's `wal_sender_timeout` (independent Ansible vars, postgresql15.conf.template:430 and :500) produces the same inversion; (c) `wal_receiver_timeout = 0` on a standby means no ping is ever sent while the primary's keepalive stays suppressed by the 10 s status replies, so `last_msg_receipt_time` would freeze indefinitely on an idle link. None of (a)-(c) is currently true on the fleet.

(5) Zombie-row lifetimes vs the gate -- gate is STRICTER than both, which is the safe direction:
  - pg_stat_wal_receiver: the view is `WHERE s.pid IS NOT NULL` (system_views.sql:924-925) and `pg_stat_get_wal_receiver` returns NULL when `pid == 0 || !ready_to_display` (walreceiver.c:1384-1387). The process only dies at `last_recv_timestamp + wal_receiver_timeout` = 300 s (walreceiver.c:556-566, ERROR "terminating walreceiver due to timeout"). So a silently-dead link (partition, frozen VM -- no TCP FIN/RST) keeps a row with a FROZEN `last_msg_receipt_time` for up to 300 s. 180 s < 300 s: the gate rejects the zombie about 120 s before Postgres reaps the row. Good. A clean TCP close instead trips walreceiver.c:471-480 (`len < 0` -> endofwal) and the row vanishes at once.
  - pg_stat_replication: the row lives as long as the walsender backend, killed by `WalSndCheckTimeOut()` at `last_reply_timestamp + wal_sender_timeout` = 300 s (walsender.c:2434-2455), with `reply_time` frozen at the last reply. 180 s < 300 s: again stricter. And since the healthy primary-side cadence is 10 s, the primary leg of the gate carries ~170 s of slack, which matches ADR-002's own "generous on the primary side" note.

(6) Corrected ADR-002 sentence (replacing "For the production `wal_sender_timeout = 5min`, this yields ~180 s -- comfortably above the keepalive cadence of ~150 s, with 30 s of slack for scan jitter."):

"For the production `wal_sender_timeout = 5min` this yields 180 s. Note that on an idle-but-healthy physical link the primary sends nothing on its own initiative: the standby's routine status update every `wal_receiver_status_interval` (10 s) keeps the walsender's `last_reply_timestamp` fresh, so `WalSndKeepaliveIfNecessary()` never reaches `last_reply_timestamp + wal_sender_timeout/2` and primary-initiated keepalives are suppressed entirely. What actually refreshes `last_msg_receipt_time` is standby-driven: after `wal_receiver_timeout / 2` of silence the walreceiver sends a status reply with `requestReply = 1`, and the walsender answers it with a keepalive. The replica-side cadence is therefore `wal_receiver_timeout / 2` = 150 s (plus up to 100 ms of walreceiver loop granularity and one RTT), and 180 s clears it by ~30 s. This holds only because the fleet sets `wal_receiver_timeout = wal_sender_timeout = 5min` on all 321 nodes; the replica-side leg should properly be derived from the replica's own `wal_receiver_timeout`, and the 60_000 ms fallback used when `wal_sender_timeout` is missing would yield a 60 s threshold -- below the 150 s cadence -- and would reject every healthy idle replica."

Also correct ADR-002 line 28 ("Keepalives are sent at `wal_sender_timeout / 2` ~= 150 s") and line 87 ("replica side on keepalive (~150 s in our config)"): the number is right, the mechanism and the governing GUC are not.


**Evidence:**

- `~/work/postgres @ REL_15_14:src/backend/replication/walsender.c:3670-3697`

```
static void
WalSndKeepaliveIfNecessary(void)
{
	TimestampTz ping_time;

	if (wal_sender_timeout <= 0 || last_reply_timestamp <= 0)
		return;

	if (waiting_for_ping_response)
		return;

	/*
	 * If half of wal_sender_timeout has lapsed without receiving any reply
	 * from the standby, send a keep-alive message to the standby requesting
	 * an immediate reply.
	 */
	ping_time = TimestampTzPlusMilliseconds(last_reply_timestamp,
											wal_sender_timeout / 2);
	if (last_processing >= ping_time)
	{
		WalSndKeepalive(true, InvalidXLogRecPtr);
```

  Keepalive fires only if NO standby message arrived for wal_sender_timeout/2. A standby replying every 10 s suppresses it permanently.

- `~/work/postgres @ REL_15_14:src/backend/replication/walsender.c:1999-2005`

```
/*
	 * Save the last reply timestamp if we've received at least one reply.
	 */
	if (received)
	{
		last_reply_timestamp = last_processing;
		waiting_for_ping_response = false;
	}
```

  ANY message from the standby (including the plain 10 s status update) resets the keepalive clock, so the 150 s threshold is never reached on a healthy link.

- `~/work/postgres @ REL_15_14:src/backend/replication/walsender.c:2905-2910`

```
/* Do we have any work to do? */
	Assert(sentPtr <= SendRqstPtr);
	if (SendRqstPtr <= sentPtr)
	{
		WalSndCaughtUp = true;
		return;
	}
```

  With no new WAL, XLogSendPhysical writes nothing to the socket. Combined with suppressed keepalives, the primary sends the standby literally nothing while idle.

- `~/work/postgres @ REL_15_14:src/backend/replication/walsender.c:2142-2144`

```
/* Send a reply if the standby requested one. */
	if (replyRequested)
		WalSndKeepalive(false, InvalidXLogRecPtr);
```

  The ONLY primary->standby traffic on an idle physical link: an answer to the standby's own ping. The heartbeat is standby-driven.

- `~/work/postgres @ REL_15_14:src/backend/replication/walreceiver.c:553-583`

```
if (wal_receiver_timeout > 0)
					{
						TimestampTz now = GetCurrentTimestamp();
						TimestampTz timeout;

						timeout =
							TimestampTzPlusMilliseconds(last_recv_timestamp,
														wal_receiver_timeout);

						if (now >= timeout)
							ereport(ERROR,
									(errcode(ERRCODE_CONNECTION_FAILURE),
									 errmsg("terminating walreceiver due to timeout")));

						/*
						 * We didn't receive anything new, for half of
						 * receiver replication timeout. Ping the server.
						 */
						if (!ping_sent)
						{
							timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
																  (wal_receiver_timeout / 2));
							if (now >= timeout)
							{
								requestReply = true;
								ping_sent = true;
							}
						}
					}

					XLogWalRcvSendReply(requestReply, requestReply);
```

  THE decisive code. The idle-link heartbeat period is wal_receiver_timeout/2 = 150 s on this fleet -- a REPLICA-side GUC. Same code shows the walreceiver process itself dies at wal_receiver_timeout = 300 s.

- `~/work/postgres @ REL_15_14:src/backend/replication/walreceiver.c:1244-1258`

```
static void
ProcessWalSndrMessage(XLogRecPtr walEnd, TimestampTz sendTime)
{
	WalRcvData *walrcv = WalRcv;

	TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();

	/* Update shared-memory status */
	SpinLockAcquire(&walrcv->mutex);
	if (walrcv->latestWalEnd < walEnd)
		walrcv->latestWalEndTime = sendTime;
	walrcv->latestWalEnd = walEnd;
	walrcv->lastMsgSendTime = sendTime;
	walrcv->lastMsgReceiptTime = lastMsgReceiptTime;
```

  Sole writer of last_msg_receipt_time / last_msg_send_time; called only from XLogWalRcvProcessMsg case 'w' (line 843) and case 'k' (line 865). No WAL and no keepalive means no update.

- `~/work/postgres @ REL_15_14:src/backend/replication/walreceiver.c:1086-1105`

```
if (!force && wal_receiver_status_interval <= 0)
		return;
...
	if (!force
		&& writePtr == LogstreamResult.Write
		&& flushPtr == LogstreamResult.Flush
		&& !TimestampDifferenceExceeds(sendTime, now,
									   wal_receiver_status_interval * 1000))
		return;
```

  Even with nothing to report, the standby sends a status reply once per wal_receiver_status_interval (10 s). This is what keeps the primary's last_reply_timestamp fresh and suppresses primary keepalives.

- `~/work/postgres @ REL_15_14:src/backend/catalog/system_views.sql:924-925 and walreceiver.c:1384-1387`

```
FROM pg_stat_get_wal_receiver() s
    WHERE s.pid IS NOT NULL;
---
	if (pid == 0 || !ready_to_display)
		PG_RETURN_NULL();
```

  The pg_stat_wal_receiver row exists exactly as long as the walreceiver process, i.e. up to 300 s after the link goes silent. The 180 s gate is stricter than the row's lifetime -- the safe direction.

- `~/work/postgres @ REL_15_14:src/backend/replication/walsender.c:2437-2455`

```
timeout = TimestampTzPlusMilliseconds(last_reply_timestamp,
										  wal_sender_timeout);

	if (wal_sender_timeout > 0 && last_processing >= timeout)
	{
...
		ereport(COMMERROR,
				(errmsg("terminating walsender process due to replication timeout")));

		WalSndShutdown();
```

  pg_stat_replication zombie rows survive up to wal_sender_timeout = 300 s with a frozen reply_time. 180 s gate is stricter -- safe direction.

- `~/work/db-scan/src/v2/analyze/split_brain.rs:265-266, 284-286`

```
// `wal_sender_timeout` can differ between primaries
        let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;
...
                && wr.last_msg_receipt_time.is_some_and(|t| {
                    (r_health.current_time - t).num_milliseconds() <= threshold_ms
                });
```

  The replica-side freshness threshold is derived from the PRIMARY's wal_sender_timeout, but the cadence it must cover is the REPLICA's wal_receiver_timeout/2. They match at 150 s vs 180 s only by fleet configuration coincidence. Comparison base is the replica's own now(), so no clock skew.

- `~/work/db-scan/src/v2/analyze/split_brain.rs:608-613`

```
/// Parses `wal_sender_timeout` from `pg_settings.setting`.
fn parse_wal_sender_timeout(cfg: &HashMap<String, String>) -> i64 {
    cfg.get("wal_sender_timeout")
        .and_then(|s| s.parse().ok())
        .unwrap_or(60_000) // pg default
}
```

  If the setting is ever missing from the capture, the threshold becomes 60 s -- BELOW the 150 s idle cadence -- and every healthy idle replica is rejected by the gate. This is the live path from F7 to a destructive demote instruction.

- `~/work/infra/ansible/roles/postgres_server/templates/postgresql15.conf.template:430,500`

```
wal_sender_timeout = {{ postgres_wal_sender_timeout|default('5min') }}	# in milliseconds; 0 disables
...
wal_receiver_timeout = {{ postgres_wal_receiver_timeout|default('5min') }}             # time that receiver waits for
```

  The two values come from two independent Ansible variables that merely share a default. Nothing structurally ties wal_receiver_timeout to wal_sender_timeout; no override for either exists anywhere in the repo today.

- `~/work/infra/ansible/result_manifests/prod/prod-pg-app001-db001.sto1.fnox.se/postgres/manifest_postgresql.conf:423,455,466`

```
wal_sender_timeout = 5min	# in milliseconds; 0 disables
#wal_receiver_status_interval = 10s	# send replies at least this often
wal_receiver_timeout = 5min             # time that receiver waits for
```

  Rendered fleet reality. wal_receiver_status_interval is commented on all 321 manifests (verified: 321/321 commented, 0 uncommented), so it is the 10 s default -- which is precisely what suppresses primary keepalives.

- `~/work/infra/ansible/roles/repmgr_setup/templates/repmgr.conf.j2:353-354`

```
#monitoring_history=no                  # Whether to write monitoring data to the "montoring_history" table
#monitor_interval_secs=2                # Interval (in seconds) at which to write monitoring data
```

  repmgrd does NOT write monitoring rows on this fleet, so there is no background WAL generator keeping the link busy. Genuine WAL silence longer than 150 s is realistic, which is why the idle-path analysis matters.


**Caveats:** What this does NOT establish. (1) I did not observe an actual idle-link timing trace on a real node; the ~150 s cadence and the ~150.2 s worst case are derived from reading REL_15_14 source, not measured. A confirming observation would be: on a quiet cluster, sample `SELECT now() - last_msg_receipt_time FROM pg_stat_wal_receiver` every 10 s for 10 minutes and check the maximum stays under ~152 s. (2) I did not verify that any prod cluster is ever genuinely WAL-silent for more than 150 s. I established that repmgrd monitoring_history is off so it is not a background WAL source, but application traffic, autovacuum and log-shipping activity could keep WAL flowing continuously, in which case the idle path is never exercised at all. That is an argument the gate is even safer, not less. (3) The RTT and loop-granularity components of the worst case are my inference (NAPTIME_PER_CYCLE = 100 ms is from source; RTT is assumed LAN-scale). A pathologically slow or saturated link could add seconds, but would need ~30 s of added latency to breach the gate, at which point the link is not healthy. (4) `wal_receiver_status_interval` is commented out in all 321 manifests, so I am relying on the compiled-in default of 10 s; I did not confirm it from live pg_settings (unlike wal_sender_timeout, which prod psql confirms as 300000). If it were ever set to 0 on a node, the dynamic inverts: the standby stops sending routine replies, the primary's keepalive at wal_sender_timeout/2 comes back into play, and the cadence would then genuinely be 150 s from the primary side -- the same 150 s, so the gate result is unchanged. (5) Config paths that could differ: postgres_wal_receiver_timeout or postgres_wal_sender_timeout being overridden per-host or per-group (none exist in the repo now, but the two variables are independent); a node running wal_receiver_timeout = 0, which would freeze last_msg_receipt_time indefinitely on an idle link and break the gate outright. (6) I read PG 15 only. The infra repo carries postgresql13/14/17/18 templates with the same defaults, and the walsender/walreceiver logic is long-stable, but I did not diff other majors.


**Impact on the audit:** RESCOPES F7 from "possible second critical bug" down to "doc-accuracy defect plus a latent correctness bug that does not currently fire". Specifically: F7 does NOT produce a false negative on this fleet today, so it is NOT a second independent route to the destructive demote output that F1 causes. F1 remains the sole live critical defect. But F7 should not be closed as a pure nit, for two reasons. First, ADR-002 lines 28, 85 and 87 state the mechanism wrongly (they credit a primary-initiated keepalive at wal_sender_timeout/2; the real refresh is a standby-initiated ping at wal_receiver_timeout/2, and primary keepalives are provably suppressed on a healthy link), so the design's own justification for the 180 s number is unsound even though the number lands correctly. Second, split_brain.rs:266 derives the replica-side threshold from the wrong GUC on the wrong node, and split_brain.rs:611 has a 60_000 ms fallback that would yield a 60 s threshold against a 150 s cadence -- rejecting every healthy idle replica, emptying the follower map, falling through to HigherTimeline, and printing a demote instruction. Recommend recording F7 as (a) a correctness fix: key the replica-side leg off the replica's own wal_receiver_timeout (replica_wal_receiver_timeout_ms / 2 + 30_000), and either drop the 60 s fallback or make a missing/unparseable timeout a Refuse rather than a silent 60 s; and (b) an ADR text correction. On the other half of F7: the gate at 180 s is STRICTER than both zombie-row lifetimes (300 s for pg_stat_wal_receiver and for pg_stat_replication), which is the correct direction and confirms the ADR's stated intent of catching dead rows before Postgres reaps them. Note also a smaller doc/code divergence uncovered en route: ADR-002 lines 70 and 75 say freshness is measured against "the scan-start timestamp", but the code measures against each node's own SELECT now() -- the code is safer (skew-immune) and the ADR text should follow the code, not the reverse.


## Where does db-scan's inventory `node_name` come from, and is there already a canonical FQDN <-> application-name mapping anywhere?

**Confidence:** high

`node_name` is the Ansible `inventory_hostname` (the FQDN), and YES -- infra already has exactly one canonical FQDN -> application-name mapping, defined in a single file that is symlinked into every environment: `ansible/environments/proact/global_postgres_all.yml:450`, `pg_replica_application_name: "{{inventory_hostname.split('.')[0]|replace('-', '_')}}"`. db-scan should mirror that rule (strip the DNS domain, then `-` -> `_`), not invent a new normalisation.

Details:

(1) The portal does NOT live in ~/work/infra. What infra contains is only its *surroundings*: k8s RBAC/ServiceAccount for a `database-portal` workload on cluster `infra001` (`jsonnet/proact/rbac/database-portal.libsonnet`, wired in at `jsonnet/proact/common.jsonnet:1225`), the provisioning of its databases/roles (`ansible/environments/proact/infra/group_vars/postgres_timescale/postgres_timescale*.yml`), and a doc naming it (`docs/new_servicedb.md:5`, "requested through the database portal at <https://database.fnox.se>"). No application source, no migrations, no API schema. The portal's source repo is elsewhere -- not determinable from infra.

(2) infra DOES contain the feed. `ansible/postgres-inventory-scan.yml` runs against `hosts: postgres_all` and POSTs `{"host": "{{inventory_hostname}}"}` to `https://database.fnox.se/api/v1/inventory_scans/host`. So the identity the portal is handed is Ansible `inventory_hostname` -- the FQDN form. I confirmed the receiving end via the database-portal MCP: db `postgres_inventory` on `infra-pg-infra006` has `pginv_hosts_to_scan(host_name text)` (the POST target) and `pginv_server(id, instance_id, servername, ipaddress, postgres_version, ...)` FK-ing `pginv_instance(id, instance_name, instance_env)`. That is a field-for-field match with db-scan's `Node { id, cluster_id, node_name, pg_version, ip_address }`, so API `node_name` = `pginv_server.servername`. The `host_name -> servername` hop is inferred (the portal code isn't here), but the shapes and the observed FQDN values make it the only plausible reading. Not a DNS lookup, not a separate CMDB: it is inventory_hostname, which infra also uses as a DNS name.

(3) There is no *reverse* (application-name -> FQDN) mapping anywhere in infra, and no custom Ansible filter/lookup plugin for it (`ansible/filter_plugins/` holds only `kafka_health.py`; `ansible/roles/postgres_setup_dynamic/filter_plugins/` only `postgres_privileges.py`). The forward mapping appears four times, all the same rule: `global_postgres_all.yml:450` (the definition, with an explanatory comment at 447-449 spelling out `dev-pg-app001-db001.sto1.fnox.se -> dev_pg_app001_db001`), `global_postgres_all.yml:321` (synchronous_standby_names), `ansible/environments/gdc/it/group_vars/postgres_all/postgres_all.yml:44` (same for `.it.fnox.se`), and `ansible/roles/repmgr_setup/templates/repmgr.conf.j2:27` via `ansible_hostname|replace('-','_')` (short hostname, same result). Fleet-wide proof from the rendered manifests: all 214 `primary_conninfo ... application_name=` occurrences across the 321 nodes are the underscore short-name form; zero are FQDN/hyphen form.

(4) Naming is NOT uniformly `<env>-<type><nnn>-db<nnn>.<zone>.<domain>`. 312 of 321 have a 4-part short name; 9 have five (`dev-ct-cluster001-coordinator-db001`, `dev-ct-cluster001-worker001-db001`, `dev-ct-cluster001-worker002-db001`, x3 zones each). Type segment varies well beyond `app`: `appNNN`, `ts-metricsNNN`, `pg-infraNNN`, `pg-publicNNN`, `pg-f3cNNN`, `pg-baseNNN`, `pg-lonNNN`, plus citus. Domain varies: `sto1|sto2|sto3.fnox.se` (318) and `it.fnox.se` (3). BUT the writer's parser survives all of it: I checked every one of the 321 -- no non-final hyphen segment starts with "db", every name has exactly one `db<nnn>` segment, every cluster prefix has exactly 3 nodes, and `db001/db002/db003` each appear exactly 107 times, so `dbNNN` is unique within every cluster. `postgres_recovery_nodes` (e.g. `dev-pg-recovery-node001.sto1.fnox.se`, `infra-recovery-node001.sto1.fnox.se`) have NO `db` segment and would break `extract_db_number`, but they are not children of `postgres_all` in any environment, so they are never posted to the portal and have no rendered postgres manifest -- out of scope.

Consequence for F1: db-scan already contains a working cross-namespace join it just doesn't use in the resolver. `src/v2/writer/build.rs:375-393` (`find_replica_timeline`) reduces BOTH sides to the `dbNNN` token before comparing, and `normalize_application_name` (build.rs:422-430) does the app-name half. Meanwhile `src/v2/analyze/split_brain.rs:307` does `conn.application_name == replica.node_name`. Two safe fixes, in order of preference: (a) mirror the Ansible rule exactly -- `node_name.split('.').next() -> replace('-', "_")` -- which is a total function, reversible, and provably matches all 214 rendered app names; or (b) reuse the existing `dbNNN` reduction, which is also unambiguous here because dbNNN is unique per cluster and clusters are grouped by portal `cluster_id` (`src/v2/cluster.rs:28-32`), not by the string prefix. (a) is strictly better: it is the fleet's own rule, it survives a hypothetical 4-node cluster, and it does not depend on the `db` prefix convention.


**Evidence:**

- `infra: ansible/environments/proact/global_postgres_all.yml:447-450`

```
# Use inventory_hostname instead of ansible_hostname to avoid having to gather facts
# split hostname on '.', and use the first potion to get the short-name
# (ex. dev-pg-app001-db001.sto1.fnox.se -> dev-pg-app001-db001)
# Then replace '-' with '_' since '-' cannot be used in postgres application_name
# (ex. dev-pg-app001-db001 -> dev_pg_app001_db001)
pg_replica_application_name: "{{inventory_hostname.split('.')[0]|replace('-', '_')}}"
```

  THE canonical mapping, with the fleet's own worked example in the comment. This is the rule db-scan should mirror. The file is symlinked into all five environments' postgres_all group_vars, so it is fleet-wide.

- `infra: ansible/postgres-inventory-scan.yml:12-15`

```
url: "https://database.fnox.se/api/v1/inventory_scans/host"
...
  "host": "{{inventory_hostname}}"
```

  Direct evidence that what the portal is fed is Ansible inventory_hostname -- the FQDN. Playbook targets `hosts: postgres_all`. This is the origin of node_name.

- `infra: ansible/environments/gdc/it/group_vars/postgres_all/global_postgres_all.yml (symlink)`

```
global_postgres_all.yml -> ../../../../proact/global_postgres_all.yml
```

  The mapping is not proact-only. gdc/it and all four proact envs symlink the same file, so one rule covers all 321 nodes including the .it.fnox.se ones.

- `infra: ansible/environments/proact/global_postgres_all.yml:321`

```
postgres_synchronous_standby_names: "ANY 1 ( {% for host in repmgr_other_postgres_nodes %}{{ host|regex_replace('^(.*)(\\.sto[1-3]\\.fnox\\.se)$', '\\1')|replace('-', '_') }}{% if not loop.last %}, {% endif %}{% endfor %} )"
```

  SSN members are produced by the SAME transform applied to FQDNs from the Ansible inventory. Confirms SSN member namespace == application_name namespace == transform(node_name).

- `infra: ansible/roles/repmgr_setup/templates/repmgr.conf.j2:27`

```
node_name='{{ansible_hostname|replace('-', '_')}}'
```

  repmgr's own node_name is the same underscore form (ansible_hostname is already the short name). Note the collision of terminology: repmgr node_name = underscore form, portal node_name = FQDN. Opposite conventions, same word.

- `infra: ansible/environments/gdc/it/group_vars/postgres_all/postgres_all.yml:44`

```
postgres_synchronous_standby_names: "ANY 1 ( {% for host in repmgr_other_postgres_nodes %}{{ host|regex_replace('^(.*)(\\.it\\.fnox\\.se)$', '\\1')|replace('-', '_') }}{% if not loop.last %}, {% endif %}{% endfor %} )"
```

  The it env uses the same rule with its own domain suffix. Rendered output confirms: it/it-pg-app001-db002.../manifest_postgresql.conf:497 has application_name=it_pg_app001_db002.

- `infra: ansible/roles/postgres_server/templates/postgresql15.conf.template:459`

```
primary_conninfo = 'user={{pg_replica_replication_user}} connect_timeout=2 host={{pg_replica_master_ip}} port={{pg_listen_port}} application_name={{ pg_replica_application_name|default(inventory_hostname) }}'
```

  How the mapped value reaches pg_stat_replication.application_name. Identical line in postgresql13/14/17/18.conf.template and postgres_server/tasks/templates/postgresql.auto.conf.j2:20. Note the `default(inventory_hostname)` fallback -- unused today, but it would emit the raw FQDN.

- `infra: ansible/result_manifests/*/*/postgres/manifest_postgresql.conf (aggregate, 214 matches over 321 nodes)`

```
96 application_name=prod_pg_appN_dbN / 30 dev_pg_appN_dbN / 18 prod_ts_metricsN_dbN / 16 acce_pg_appN_dbN / 10 infra_pg_infraN_dbN / 6 prod_pg_fNcN_dbN / 4 dev_ct_clusterN_workerN_dbN / 2 it_pg_appN_dbN / ... (digits normalised to N)
```

  Fleet-wide: every rendered application_name is the underscore short-name form; zero FQDN or hyphenated forms. 214 = 321 nodes minus 107 primaries, i.e. one per replica. transform(node_name) == application_name holds on 100% of the fleet.

- `infra: ansible/result_manifests (321 postgres node dirs, shape tally)`

```
144 prod-pg-appN-dbN.stoN.fnox.se / 45 dev-pg-appN-dbN / 27 prod-ts-metricsN-dbN / 24 acce-pg-appN-dbN / 15 infra-pg-infraN-dbN / 9 prod-pg-fNcN-dbN / 6 infra-pg-publicN-dbN / 6 dev-ct-clusterN-workerN-dbN / 3 dev-ct-clusterN-coordinator-dbN / 3 it-pg-appN-dbN.it.fnox.se / 3 prod-pg-lonN-dbN / ...
```

  The strict <env>-<type><nnn>-db<nnn>.<zone>.<domain> pattern does NOT hold universally: 9 citus nodes have a 5-part short name and 3 it nodes use it.fnox.se instead of stoN.fnox.se. A pattern-matching normaliser must not assume 4 parts or a stoN zone; the split('.')[0] + replace rule makes no such assumption.

- `infra: ansible/result_manifests (verified over all 321)`

```
hosts where a NON-final hyphen part starts with 'db': (none) ; nodes per cluster prefix != 3: (none) ; db001 x107, db002 x107, db003 x107
```

  The writer's `split('-').find(|p| p.starts_with("db"))` heuristic is currently safe on the whole fleet, and dbNNN is unique within every cluster -- so the fallback fix (b) also works. But it is a convention, not an enforced invariant.

- `infra: ansible/environments/proact/dev/inventory:259-266 and infra/inventory:418-429`

```
[postgres_recovery_nodes]
dev-pg-recovery-node001.sto1.fnox.se ansible_host=10.81.12.81
...
infra-recovery-node001.sto1.fnox.se ansible_host=10.81.22.161
```

  The only postgres-adjacent hosts with NO dbNNN segment. They would break extract_db_number -- but postgres_all:children lists only postgres_f3base/f3lon/f3dbs/clusters/clusters_public/timescale/citus_nodes, never postgres_recovery_nodes, so they are never posted to the portal and never appear in db-scan's inventory.

- `portal DB schema, postgres_inventory on infra-pg-infra006 (read via database-portal MCP)`

```
pginv_server(id integer, instance_id integer, servername varchar(50), ipaddress varchar(15), postgres_version varchar(10), ...) FK instance_id -> pginv_instance(id); pginv_instance(id, instance_name varchar(50), instance_env text CHECK IN ('dev','acce','prod','infra','infra-public','backup')); pginv_hosts_to_scan(id, host_name text, job_status, ...)
```

  The portal's backing store. pginv_server is db-scan's Node one-for-one (servername=node_name, instance_id=cluster_id, ipaddress=ip_address, postgres_version=pg_version); pginv_instance is the cluster. pginv_hosts_to_scan.host_name is what the Ansible playbook POSTs. There is NO application_name column anywhere -- the portal cannot supply the mapping, db-scan must compute it.

- `portal API, search_databases (read via database-portal MCP)`

```
{"environment":"dev","cluster_id":1574,"cluster_name":"dev-ct-cluster001-coordinator"}, {"cluster_id":1575,"cluster_name":"dev-ct-cluster001-worker001"}, {"cluster_id":1576,"cluster_name":"dev-ct-cluster001-worker002"}
```

  The portal models the citus coordinator and each worker as SEPARATE clusters. db-scan groups by cluster_id (src/v2/cluster.rs:28), so the 5-part citus names create no dbNNN collision -- but Node::cluster_name() mislabels all 9 as 'dev-ct-cluster001', disagreeing with the portal's own instance_name.

- `db-scan: src/v2/analyze/split_brain.rs:306-307`

```
!conn.application_name.is_empty()
    && conn.application_name == replica.node_name
```

  F1 itself. Left side is 'prod_pg_app001_db002', right side is 'prod-pg-app001-db002.sto1.fnox.se'. Never equal on this fleet -- confirmed by the 214/214 rendered application_name values above.

- `db-scan: src/v2/writer/build.rs:375-393 and 422-430`

```
fn find_replica_timeline(app_name: &str, nodes: &[AnalyzedNode]) -> Option<i32> {
    let normalized = normalize_application_name(app_name);
    nodes.iter().find(|n| {
        n.node_name.split('-').find(|p| p.starts_with("db")).and_then(|p| p.split('.').next()) == Some(normalized.as_str())
    })
```

  db-scan ALREADY solves the same cross-namespace join elsewhere, by reducing both sides to the dbNNN token. The resolver's raw == is the outlier, not the norm -- which strengthens the case that F1 is an oversight rather than a design assumption.

- `db-scan: README.md:74-77`

```
**Node Naming Convention**:
Nodes must follow the naming pattern: `{env}-pg-{app}-{db}.{zone}.{domain}`
```

  db-scan's own documented assumption is narrower than reality: the fleet also has ts-metrics, pg-infra, pg-public, pg-base, pg-lon, pg-f3c and 5-part citus names. Also note TODO.md:141 claims Node carries 'prod-pg-app048-db001, no domain' -- that is WRONG; the portal supplies the full FQDN including the sto1/sto2/sto3 site code, so the 'which DC' TODO is already answerable without SSH.


**Caveats:** - The portal's application source and its `host_name -> servername` write path are NOT in ~/work/infra; I read the portal's *database schema* via the database-portal MCP tool, not its code. That the API's `node_name` is literally `pginv_server.servername` is inferred from field-for-field shape match plus the observed FQDN values, not from source.
- I did not verify that the portal stores or exposes an application_name column -- it does not appear in `pginv_server` or `pginv_instance`, so there is no server-side mapping db-scan could just ask for.
- `pginv_instance.instance_env` has CHECK `IN ('dev','acce','prod','infra','infra-public','backup')` -- note it does NOT include `it`. So the 3 `it-pg-app001-db00N.it.fnox.se` nodes are probably absent from the portal feed, and a `backup` env exists in the portal with no rendered postgres manifests. Either way both follow the same naming rule, so neither changes the fix.
- Config path that could differ: `postgresql{13..18}.conf.template` and `postgres_server/tasks/templates/postgresql.auto.conf.j2` all use `pg_replica_application_name|default(inventory_hostname)`. If `global_postgres_all.yml` were ever not loaded for a group, application_name would fall back to the raw FQDN (hyphens, dots). Today that cannot happen -- the file is symlinked into all five environments' `postgres_all` group_vars -- and no rendered manifest shows the fallback form. A `.split('.')[0] + replace('-','_')` normaliser is robust to that fallback anyway only if applied to both sides; a `dbNNN` reduction would be.
- The 9 citus nodes: `Node::cluster_name()` (src/v2/node.rs:23-30, first 3 hyphen parts) yields `dev-ct-cluster001` for all 9, whereas the portal's own `instance_name` splits them into three clusters (`...-coordinator`, `...-worker001`, `...-worker002`, cluster_ids 1574/1575/1576). That is a pre-existing display-label bug, unrelated to F1 -- grouping is by `cluster_id`, so it does not create a dbNNN collision.
- I did not read the ADR-002 text itself in this pass; the F1/F2 framing comes from the task brief and from ADR-002-verification.md line references I saw in grep output.


**Impact on the audit:** CONFIRMS F1 and hardens it from "the two strings look different" to "infra provably guarantees they are different on 100% of the fleet." The chain is now fully cited: Ansible inventory_hostname (FQDN) -> POSTed to the portal (postgres-inventory-scan.yml:15) -> stored as pginv_server.servername -> served as API node_name -> Node.name; while the SAME inventory_hostname is passed through `split('.')[0]|replace('-','_')` (global_postgres_all.yml:450) -> primary_conninfo application_name -> pg_stat_replication.application_name. `==` between the two endpoints of a deliberate, documented, fleet-wide transform is dead code by construction, so the split_brain.rs:307 primary-side corroborating gate never fires on any real primary in any of the five environments. That also transitively confirms F2 (members-from-SSN intersected against gated followers holding node_name) -- SSN members are generated by the identical transform (global_postgres_all.yml:321), so that intersection is structurally empty for the same reason.

RESCOPES the fix. The audit should NOT recommend inventing a normalisation or a config-driven pattern (README.md:77's "GOOD FIRST PR IF YOU EXTRACT THIS INTO CONFIG" is a trap here). infra has exactly one canonical rule and db-scan should mirror it verbatim: `node_name.split('.').next().unwrap_or(node_name).replace('-', "_")`, compared against application_name. Verified to reproduce all 214 rendered application_names across all 321 nodes, all 5 envs, including the 5-part citus names and the it.fnox.se domain. The alternative -- reusing the existing dbNNN reduction at build.rs:375-393 -- also works today (dbNNN is unique within every one of the 107 clusters, and grouping is by portal cluster_id not by name prefix) and has the merit of already existing, but it depends on the "db" prefix convention rather than on a rule infra actually enforces.

ADDS one finding the audit did not have: db-scan and infra use the term "node_name" for OPPOSITE conventions. The portal's node_name is the FQDN; repmgr's node_name (repmgr.conf.j2:27) is the underscore short form. Any ADR-002 wording that says "node_name" without qualifying which one is ambiguous, and that ambiguity is a plausible root cause of F1 -- someone reading repmgr's node_name would reasonably expect it to equal application_name, and it does.

FLAGS a stale claim in the audit's surrounding docs: TODO.md:141 asserts Node carries "prod-pg-app048-db001, no domain". It carries the full FQDN. Minor, but it is load-bearing for the "which DC" TODO, which is already solved by parsing the sto1/sto2/sto3 label.

NOT ESTABLISHED: nothing here bears on the DivergentReplicaWal verdict-flip question or on whether the resolver picks the right primary once the gate is fixed. Fixing F1 makes the gate live for the first time, which means gate behaviour on real fleet data has never actually been exercised -- the audit should say so rather than assume the gate is correct-but-unreachable.


## Is the "1 primary + 2 replicas" assumption true of every postgres cluster db-scan scans, and are all clusters 3 nodes? Is the `replicas.len() > 2` guard in analyze() dead?

**Confidence:** high

Yes on both counts, but the guard is dead for a different -- and worse -- reason than the audit assumes.

TOPOLOGY: Every cluster db-scan can scan is exactly 3 nodes, exactly 1 primary + 2 replicas, all named db001/db002/db003. I verified this against three independent sources that agree exactly:

(a) Rendered manifests: the 321 postgres nodes group into 107 clusters, ALL exactly size 3, ALL exactly {db001, db002, db003}. Zero exceptions.

(b) The Ansible inventories: the union of `postgres_all` hosts across proact/{acce,dev,infra,prod}/inventory.yaml is 1074 hosts / 358 clusters. Every cluster is exactly 3 hosts with exactly 1 in a `*_primaries_*` group and 2 in a `*_replicas_*` group. No exceptions (1093 hosts are in some postgres_* group; the 19 extra are `postgres_recovery_nodes`, which are NOT under postgres_all).

(c) db-scan's actual input, the database-portal node list (cached at /tmp/nodes_response.json, fetched 2026-09-09): 1074 items, 358 distinct cluster_ids, EVERY cluster_id has exactly 3 nodes, exactly 1 with read_only=false and 2 with read_only=true, repmgr_enabled=true on all 1074, node numbers exactly {db001, db002, db003} (358 of each). The portal node-name set is set-equal to the Ansible postgres_all set -- zero symmetric difference.

NODE TYPES (question 2): 18 shape-classes exist; all are ordinary 3-node repmgr clusters under `postgres_all`, and all inherit the same sync config. There are no standalone or 2-node postgres instances in the scanned set. Types: pg-app (`postgres_clusters`), pg-base (`postgres_f3base`), pg-f3c (`postgres_f3dbs`, by far the largest -- 235 prod clusters), pg-lon (`postgres_f3lon`), pg-infra + pg-public (`postgres_clusters`/`postgres_clusters_public`), ts-metrics (`postgres_timescale`), and Citus (`postgres_citus_coordinators` / `postgres_citus_workers`). Citus does NOT break the model: the coordinator and each worker is its own separate 3-node repmgr cluster with its own `pg_cluster_regex` and its own portal cluster_id (1574 coordinator, 1575 worker001, 1576 worker002). "recovery" nodes (19 of them) are NOT postgres clusters -- they sit in a sibling `postgres_recovery_nodes` group, have no rendered postgresql.conf, and are absent from the portal, so db-scan never sees them.

The sync setup is a single global definition, not per-type: `postgres_synchronous_commit: remote_apply` and `postgres_synchronous_standby_names` are defined once in environments/proact/global_postgres_all.yml (lines 320-321), which is symlinked into every environment's group_vars/postgres_all/. Nothing in any group_vars overrides either value -- I grepped all of environments/. The SSN member list is derived from `pg_cluster_other_nodes` = cluster members minus self, which is why it is structurally always 2 members and always self-excluded.

SCAN SELECTION (question 3): db-scan applies NO env or type filter. src/main.rs:237-289 fetches the full portal list and filters only by (i) an optional `--cluster <regex>` CLI flag (src/config.rs:106) and (ii) a watch-mode rescan set of previously-unhealthy cluster names. Default run = all 358 clusters. Cluster membership is decided by the portal's `cluster_id` field, not by name parsing.

THE GUARD IS DEAD, BUT NOT BECAUSE OF TOPOLOGY: `replicas.len() > 2` at src/v2/analyze.rs:323 is unreachable because src/v2/cluster.rs:31 only ever emits a Cluster when the accumulator hits exactly 3 nodes. Cluster.nodes().len() == 3 is an invariant of the pipeline, independent of what the fleet looks like. With exactly 1 primary already established at that point, replicas can be at most 2. The only other Cluster constructors are #[cfg(test)] fixtures (src/v2.rs:34, src/v2.rs:447).

This matters: the guard is not a safety net. If a 4th node were ever added to a cluster_id, cluster_builder would emit a Cluster containing an ARBITRARY 3 of the 4 (whichever arrive first from the concurrent scan stage) and strand the 4th in the "incomplete clusters" warning at cluster.rs:50. db-scan would silently analyze a random 3-node subset and report a confident verdict rather than firing UnexpectedTopology. Removing the dead guard is fine; relying on it is not.


**Evidence:**

- `/home/robert.sjoblom@fnox.it/work/db-scan/src/v2/cluster.rs:9-10,31`

```
/// Listens for incoming Nodes, groups them by `cluster_id`, and sends complete Clusters
/// to the provided cluster channel. A Cluster is considered complete when it has 3 Nodes.
...
        if nodes[&cluster_id].len() == 3 {
```

  Hard 3-node invariant on every Cluster that reaches analyze(). This, not the fleet topology, is what makes the >2-replicas guard unreachable. It also means a 4-node cluster_id would silently yield an arbitrary 3-node Cluster plus a stranded node, never an UnexpectedTopology verdict.

- `/home/robert.sjoblom@fnox.it/work/db-scan/src/v2/analyze.rs:323-328`

```
if replicas.len() > 2 {
        verdict.cluster_verdict = Some(ClusterVerdict::UnexpectedTopology {
            replica_count: replicas.len(),
        });
        return AnalyzedCluster { cluster, verdict };
    }
```

  The guard under audit. Reached only when primaries.len() == 1 and cluster.nodes().len() == 3, so replicas.len() <= 2 always. Dead code in production; only src/v2/analyze/classify.rs:146 constructs the variant, by hand, in a unit test.

- `/home/robert.sjoblom@fnox.it/work/infra/ansible/environments/proact/global_postgres_all.yml:457-463`

```
pg_cluster_regex: "^(:?dev|acce|prod|infra|it)\\-{{ pg_replica_cluster_type|default('pg') }}\\-{{ pg_replica_cluster_name }}{{ pg_cluster_id }}\\-db\\d+\\.(:?sto1|sto2|sto3|it)\\.fnox\\.se$"

# Find all hosts in the current postgres cluster
pg_cluster_hosts_name: "{{ groups[pg_replica_cluster_group] | map('regex_search', pg_cluster_regex) | select('string') | list | sort }}"

# Variable that contains 'other' nodes in the same cluster as the current node
pg_cluster_other_nodes: "{{ pg_cluster_hosts_name | difference([inventory_hostname]) | sort }}"
```

  Cluster membership in config-as-code is derived by regex over the inventory group. Note it matches `-db\d+` -- Ansible does not itself forbid a db004; the 3-node property is a fact about the inventory contents, not a constraint the regex enforces.

- `/home/robert.sjoblom@fnox.it/work/infra/ansible/environments/proact/global_postgres_all.yml:320-321`

```
postgres_synchronous_commit: remote_apply
postgres_synchronous_standby_names: "ANY 1 ( {% for host in repmgr_other_postgres_nodes %}{{ host|regex_replace('^(.*)(\\.sto[1-3]\\.fnox\\.se)$', '\\1')|replace('-', '_') }}{% if not loop.last %}, {% endif %}{% endfor %} )"
```

  Single global definition, symlinked into every env's group_vars/postgres_all/ (verified: environments/proact/{acce,prod,dev,infra}/group_vars/postgres_all/global_postgres_all.yml -> ../../../global_postgres_all.yml). No group_vars anywhere overrides either key. SSN members = pg_cluster_other_nodes, hence structurally always exactly 2 and always self-excluded -- this extends the '321 manifests' finding to all 1074 scanned nodes by derivation, including the 756 f3c nodes that have no rendered manifest.

- `/home/robert.sjoblom@fnox.it/work/infra/ansible/postgres-3-ha.yml:12-14`

```
- name: Fail if other hosts of the cluster not included
      fail:
        msg: "Missing hosts"
      when: pg_cluster_other_nodes[0] not in ansible_play_hosts_all or pg_cluster_other_nodes[1] not in ansible_play_hosts_all
```

  Ansible itself hardcodes indices [0] and [1] -- exactly two peers. Same pattern at postgres-1-install.yml:14, postgres-ipa-alias.yml:14, upgrade-timescale.yml:13, and in pgbackrest config at global_postgres_all.yml:609,613,746,747 (pg2-host / pg3-host only). The 1+2 shape is baked into the automation, so a 4th node would not be a supported configuration.

- `/home/robert.sjoblom@fnox.it/work/db-scan/src/main.rs:253-282`

```
.filter(|n| {
            let passes_cli_filter = match &get_config().cluster {
                Some(re) => { ... re.is_match(&n.cluster_name()) ... }
                None => true,
            };
```

  The only selection applied to the portal node list: an optional --cluster regex and the watch-mode rescan set. No env filter, no type filter -- a default run scans all 358 clusters returned by the portal.

- `/home/robert.sjoblom@fnox.it/work/db-scan/src/v2/node.rs:24-30`

```
pub fn cluster_name(&self) -> String {
        self.name
            .split('-')
            .take(3)
            .collect::<Vec<&str>>()
            .join("-")
    }
```

  Side finding, not topology: taking only 3 hyphen segments collapses all 9 Citus nodes (dev-ct-cluster001-coordinator/-worker001/-worker002, portal cluster_ids 1574/1575/1576) onto the single name "dev-ct-cluster001". Grouping is by cluster_id so the 3 clusters stay separate, but the display name is duplicated and the watch-mode exact-match filter (main.rs:274) and --cluster regex will pull in all three whenever one is unhealthy.

- `/home/robert.sjoblom@fnox.it/work/infra/ansible/environments/proact/dev/group_vars/postgres_citus_coordinators.yml:74`

```
pg_cluster_regex: "^(:?dev|acce|prod|infra|it)\\-{{ pg_replica_cluster_type|default('pg') }}\\-{{ pg_replica_cluster_name }}{{ pg_cluster_id }}\\-coordinator\\-db\\d+\\.(:?sto1|sto2|sto3|it)\\.fnox\\.se$"
```

  Citus is the only type that overrides pg_cluster_regex, and it does so to NARROW the cluster to the coordinator's own 3 nodes (the worker equivalent at postgres_citus_workers.yml:74 scopes to -worker{{citus_node_id}}-db\d+). Confirms Citus is 3 separate 3-node repmgr clusters, not one 9-node cluster.

- `/home/robert.sjoblom@fnox.it/work/infra/ansible/environments/proact/infra/inventory.yaml:509-544`

```
postgres_recovery_nodes:
      hosts:
        infra-recovery-node001.sto1.fnox.se:
```

  The 19 recovery nodes (7 in dev/inventory.yaml:500-520, 12 here) live in a group that is a sibling of postgres_all, not a child. They are the exact 19 result_manifests directories that have no postgres/manifest_postgresql.conf, and none appear in the portal response. They are not clusters and db-scan never scans them.

- `/home/robert.sjoblom@fnox.it/work/infra/ansible/environments/gdc/it/inventory:117-122`

```
[postgres_clusters_primaries_v17]
it-pg-app001-db001.it.fnox.se ansible_host=10.105.6.200

[postgres_clusters_replicas_v17]
it-pg-app001-db002.it.fnox.se ansible_host=10.105.6.201
it-pg-app001-db003.it.fnox.se ansible_host=10.105.6.202
```

  The `it` environment is a separate Ansible environment (gdc, .it.fnox.se domain, own postgres_all group_vars at gdc/it/group_vars/postgres_all/postgres_all.yml:44). Its 3 nodes DO have rendered manifests but are absent from the portal response, so db-scan does not currently scan them. Also 1+2, so it would not violate the assumption if added.


**Caveats:** 1. RESCOPES THE GUARD ARGUMENT, DOESN'T JUST CONFIRM IT. The audit's reasoning ("unreachable because clusters are always 3 nodes") is true but incidental. The real reason is src/v2/cluster.rs:31. If the audit recommends removing the guard on topology grounds, that recommendation is right by accident -- and it should be paired with a note that cluster_builder's `== 3` silently drops surplus nodes, so oversized clusters produce a confident wrong answer rather than UnexpectedTopology. That is a separate, live defect that the dead guard was presumably meant to cover.

2. THE PORTAL SNAPSHOT IS A CACHE, NOT A LIVE FETCH. My cluster_id / read_only evidence comes from /tmp/nodes_response.json, mtime 2026-09-09 14:06, written by src/database_portal.rs (1-day TTL). It is one day old and could in principle differ from the live API. Mitigating: its node-name set is byte-for-byte set-equal to the Ansible postgres_all set (1074 = 1074, empty symmetric difference both ways), so two independent sources agree.

3. THE 321 MANIFESTS ARE NOT THE WHOLE SCANNED FLEET -- this is the one place the already-established facts overreach. 756 of the 1074 scanned nodes have NO rendered manifest: 696 prod-pg-f3c*, 39 dev-pg-f3c*, 21 acce-pg-f3c*. Only 18 f3c nodes (6 clusters) are represented in result_manifests. So "all 321 nodes have SSN = ANY 1 (two members) / remote_apply / 5min timeouts" is a direct observation covering 30% of the scanned fleet. It extends to the other 70% by DERIVATION (single global definition, symlinked everywhere, no overrides found), not by observation. That is an inference, and I have labeled it as one. If the audit needs observed rather than derived coverage of f3c, the manifests cannot supply it.

4. NOTHING IN ANSIBLE HARD-BLOCKS A 4-NODE CLUSTER. pg_cluster_regex matches `-db\d+`. The playbook guard indexes pg_cluster_other_nodes[0] and [1], which would still pass with 3 peers; pgbackrest would just silently omit the 4th from pg2/pg3. So "always 3" is an observed property of today's inventory, not an enforced invariant. Low risk, but it is not a proof.

5. Env coverage: db-scan sees acce/dev/infra/prod (885 prod, 108 dev, 57 acce, 24 infra). The `it` environment is managed by Ansible but is not in the portal, so it is out of db-scan's reach today.


**Impact on the audit:** CONFIRMS ADR-002's topology assumption (docs/adr/002-split-brain-resolution-refinement.md:25, "1 primary + 2 replicas per cluster... >2 replicas is out of scope for v1"). The assumption holds for 358/358 scanned clusters across three independent sources. The 3-node proof at ADR-002:241 -- "in the split-brain scope there are exactly two candidate primaries and one replica (db003)" -- rests on a real fleet invariant, not wishful thinking. Nothing in the ADR needs to change on topology grounds.

RESCOPES the audit's dead-guard finding. The audit is right that `replicas.len() > 2` (src/v2/analyze.rs:323) never fires, but wrong about why, and the correct why is more interesting. The guard is unreachable because src/v2/cluster.rs:31 only emits a Cluster at exactly 3 accumulated nodes -- a pipeline invariant that holds regardless of fleet shape. Two consequences the audit should absorb: (i) the guard would be dead even if the fleet grew 4-node clusters tomorrow, so "it's safe because the fleet is 3-node" understates the deadness; (ii) more importantly, the guard is not the safety net it appears to be. A 4th node on a cluster_id makes cluster_builder emit an arbitrary first-3-by-arrival subset and strand the rest at cluster.rs:50 (a tracing::warn, not a verdict), so db-scan would emit a confident split-brain verdict computed from an incomplete, nondeterministic view of the cluster. If the audit recommends deleting the dead guard, it should simultaneously recommend that cluster_builder reject or flag len > 3 rather than silently truncating -- otherwise the cleanup removes the only textual trace that oversized topologies were ever considered.

DOES NOT AFFECT F1. This investigation is orthogonal to the application_name/node_name mismatch. It does incidentally corroborate F1's mechanism: environments/proact/global_postgres_all.yml:450 defines `pg_replica_application_name: "{{inventory_hostname.split('.')[0]|replace('-', '_')}}"`, which is exactly the strip-domain-then-hyphens-to-underscores transform F1 describes, applied fleet-wide to all 1074 scanned nodes rather than to a subset. Whatever fix F1 lands must therefore work for all 18 name shapes, including the Citus 4-segment names (dev_ct_cluster001_worker001_db001) and the `it` env's .it.fnox.se domain if it is ever onboarded -- a naive "strip .stoN.fnox.se" would miss the latter.


## Can any operational path leave a postgres node with an EMPTY synchronous_standby_names on this fleet?

**Confidence:** high

Yes -- empty synchronous_standby_names is reachable by at least four in-repo paths, two of which are deliberate operator procedures that run on a live, write-accepting primary. It is NOT reachable via repmgr failover, ALTER SYSTEM, or any of the other roles.

PATH A (deliberate, post-failover; the important one). /home/robert.sjoblom@fnox.it/work/infra/ansible/postgres-kickstart-standalone-db001-primary.yml:73-84 strips the GUC line out of the running config and reloads:
  - name: Remove synchronous_standby_names option from postgres.conf
    ansible.builtin.lineinfile:
      path: "/var/lib/pgsql/{{pg_version}}/data/postgresql.conf"
      state: absent
      regexp: "^synchronous_standby_names"
With the line gone, SSN falls back to the PG default '' and synchronous_commit = remote_apply becomes a no-op wait -- the primary commits with zero replicas. The playbook says so itself at line 26: "Removing standby_sync from db001 config to allow it to take writes without replicas". It also sets MIN_ATTACHED_REPLICAS=0 (lines 86-91) so pg-cluster-health keeps reporting OK. Crucially, the scenario this playbook exists for (prompt lines 8-21) is a *failed failover with a forked timeline* -- i.e. exactly the state db-scan's split-brain resolver is invoked on. Added 2025-09-11 in cc1be2409a: "Add two 'kickstart' playbooks meant for getting a cluster to being able to take writes again after various failed failover events that we have seen." There is no companion playbook that restores SSN; recovery is manual (docs/alerts/postgresSynchronousStandbyNamesMisconfigured.md:20-21).

PATH B (deliberate, planned maintenance). ansible/postgres-version-lr-upgrade-part3.yml:178 -- play literally named "Disable synchronous replication and reconfigure pg-cluster-health in db001", targeting the live from-version db001 primary. Its pre_task (195-199) deletes /opt/postgres_first_configuration_done, then re-runs role postgres_server (202). Because the template gates the GUC on that marker file, the freshly rendered postgresql.conf omits SSN entirely, and the role's `notify: reload postgres` applies it. Same trick at postgres-version-lr-upgrade-part2.yml:200 (on a node explicitly "bootstrapped in write mode to support logical replication", pg_replica: false at line 193). Both leave the node empty-SSN until a later postgres_server run (part5:211 / part6:217,236).

PATH C (structural, every new node). In roles/postgres_server/tasks/main.yml the conf is templated at line 11 but the marker file is only touched at line 87 -- so the FIRST postgres_server run on any node always renders without SSN and without synchronous_commit. The gate is intentional (template comment: "This logic ensures that our 'bootstrap' process works by ensuring we don't enable synchronous replication before our replicas has come online"). It closes only when postgres_server runs a second time (postgres-3-ha.yml:24, cleura_postgres-3.yml:10, whose comment states "Run configure postgres once more, since this is not the first setup, synchronous replication will be enabled now").

PATH D (degradation, not emptiness). SSN is derived from repmgr_other_postgres_nodes (global_postgres_all.yml:321 -> pg_cluster_other_nodes at :463). Dropping a member yields 'ANY 1 ( x )' -- still synchronous, quorum 1 of 1 -- which the runbook lists as a known cause of the alert. An empty list would render 'ANY 1 (  )', a parse error, not a silent empty.

RULED OUT. (1) repmgr does not rewrite SSN: event_notification_command and event_notifications are both commented out in roles/repmgr_setup/templates/repmgr.conf.j2:153,157; promote_command (:328) resolves to /var/lib/pgsql/scripts/repmgr/promote_replica.sh (global_postgres_all.yml:483), which only calls `repmgr standby promote` and touches no config; follow_command (:337) is a plain `repmgr standby follow`. No repmgr helper script (promote_replica, perform_switchover, node_check, cluster_crosscheck, repmgr_service_control) contains any reference to synchronous*, standby_names, ALTER SYSTEM, or postgresql.conf. (2) No ALTER SYSTEM anywhere in the repo writes SSN; postgresql.auto.conf is overwritten from an empty template on every postgres_server run (roles/postgres_server/tasks/empty_postgresql_auto_conf.yml). (3) postgres_recovery_node, postgres_citus_node, postgres_replica, postgres_setup_dynamic, postgres_failover and repmgr_setup contain no SSN logic at all; postgres_recovery_node has no postgresql.conf template (its templates are cleanup.sh.j2, config.yml.j2, selinux/). (4) db-sync.yml (hosts: db_sync, minio client), bankbackup.yml and dirtyfrag-*.yml (hosts: k8s) do not touch postgres. (5) postgres-restart-vms-db002-db003.yml only *documents* SSN in a header comment and is designed never to take both replicas down at once. (6) postgres-rebuild-dynamic.yml forces the marker true (line 53) and re-renders "to ensure quorum sync etc on current primary" (484-499), so the rebuild path restores rather than clears.

METHODOLOGICAL POINT: the 321-manifest aggregate cannot falsify this. postgres-generate-result-manifests.yml:69-71 hardcodes postgres_first_configuration_done.stat.exists: true before rendering the same template, so every manifest is produced through the "done" branch by construction. The manifests are evidence about intended steady state only; they are structurally blind to paths B and C, and blind by definition to path A (a runtime lineinfile edit).

DETECTION: an empty SSN is visible but not blocked -- roles/postgres_exporter/templates/queries.yml.j2:193-206 exports pg_synchronous_standby_names_matches, and alert postgresSynchronousStandbyNamesMisconfigured fires on == 0 after for: 10m at severity: warning (environments/proact/infra/group_vars/prometheus/alerts.yml:1004-1011, plus gdc/it). That alert was added 2026-05-22 (d99d67f5e9), nine months after the standalone playbook -- consistent with this state having been hit and gone unnoticed.


**Evidence:**

- `ansible/postgres-kickstart-standalone-db001-primary.yml:73-78`

```
- name: Remove synchronous_standby_names option from postgres.conf
      ansible.builtin.lineinfile:
        path: "/var/lib/pgsql/{{pg_version}}/data/postgresql.conf"
        state: absent
        regexp: "^synchronous_standby_names"
      become: yes
```

  Direct, deliberate removal of the GUC from the live primary's config, followed by a reload at lines 80-84. SSN reverts to the PG default ''. The repo says so; this is not inference.

- `ansible/postgres-kickstart-standalone-db001-primary.yml:23-31`

```
This playbook will then kickstart the cluster back into working order (being able to take writes again) by;
          1. Stopping repmgr on all nodes
          2. Stopping postgres and pgbouncer on db002 & db003
          3. Removing standby_sync from db001 config to allow it to take writes without replicas
...
          !! DO NOT RUN THIS PLAYBOOK AGAINST CLUSTERS IN ANY OTHER SCENARIO THAN DEFINED ABOVE - BAD THINGS WILL HAPPEN !!
```

  The stated purpose is to let a primary ack writes with zero attached standbys. This is exactly the premise ADR-002 assumes cannot happen. The triggering scenario (prompt lines 8-21) is a failed failover with a forked timeline -- the resolver's own domain.

- `ansible/roles/postgres_server/templates/postgresql15.conf.template:438-445`

```
{# This logic ensures that our 'bootstrap' process works by ensuring we don't enable #}
{# synchronous replication before our replicas has come online. #}
{% if postgres_synchronous_standby_names is defined and postgres_first_configuration_done.stat.exists == True %}
# Standby servers that provide synchronous replication: 
# comma-separated list of application_name from standby(s); '*' = all
synchronous_standby_names = '{{ postgres_synchronous_standby_names }}'
{% else %}
#synchronous_standby_names = ''	# standby servers that provide sync rep
```

  SSN is rendered only when a marker FILE exists on the node. Delete the file, re-run the role, and the GUC vanishes from the config. Identical gate in the 13/14/17/18 templates (lines 378/445/494/521).

- `ansible/roles/postgres_server/templates/postgresql15.conf.template:301-309`

```
{% if postgres_synchronous_commit is defined and postgres_first_configuration_done.stat.exists == True %}

# synchronization level; off, local, remote_write, remote_apply, or on
synchronous_commit = {{ postgres_synchronous_commit }}

{% else %}
#synchronous_commit = on		# synchronization level;
```

  The same marker gates synchronous_commit. Both halves of ADR-002's durability premise (remote_apply AND a non-empty quorum list) disappear together in the same rendering, falling back to plain 'on'.

- `ansible/postgres-version-lr-upgrade-part3.yml:178-202`

```
- name: Disable synchronous replication and reconfigure pg-cluster-health in db001
...
  pre_tasks:
    - name: remove postgres_first_configuration_done to avoid quorom replication config
      file:
        path: "/opt/postgres_first_configuration_done"
        state: absent
      become: true
  roles:
    - role: postgres_cluster_health
    - role: postgres_server
```

  A scheduled major-version-upgrade step that deliberately runs the production primary with empty SSN and MIN_ATTACHED_REPLICAS=0 (lines 188-191) for the duration of the logical-replication migration window. Same pattern at part2.yml:200.

- `ansible/roles/postgres_server/tasks/main.yml:11-21, 87-93`

```
- name: setting up {{pg_conf_path}}/{{ pg_version }}/{{pg_data_dir}}/postgresql.conf
    template:
      src: "postgresql{{pg_version}}.conf.template"
...
    notify: reload postgres
...
  - name: Create state files to track first configuration success
    file:
      state: touch
      path: "/opt/postgres_first_configuration_done"
    when: 
      - postgres_first_configuration_done.stat.exists == False
```

  Ordering proof: the config is rendered (line 11) BEFORE the marker is created (line 87), and the render is followed by an actual reload. So any run that starts with the marker absent leaves the node live with empty SSN until postgres_server runs a second time.

- `ansible/postgres-generate-result-manifests.yml:69-71`

```
postgres_first_configuration_done:
      stat:
        exists: true
```

  The 321 rendered manifests are generated with the marker forced true, through the same template (roles/result_manifest_postgres/tasks/main.yml:23-25). The manifest corpus therefore cannot exhibit the empty state and cannot be used as evidence against F3.

- `ansible/roles/postgres_cluster_health/templates/pg-cluster-health.service.j2:34-40`

```
# To allow a primary to act as standalone without replicas in a failure scenario, set 
# MIN_ATTACHED_REPLICAS=0, then modify postgresql.conf and comment out 
# synchronous_standby_names option. Reload postgres and restart pg-cluster-health
# for the change to take effect.

# Doing this is DANGEROUS, make sure other nodes of the cluster are *down* with postgres
# stopped & disabled first.
```

  The standalone/no-sync mode is a first-class documented operating mode of this fleet, written into the role template itself -- not an accident. Note the operational precondition ('make sure other nodes are down') is exactly what a split-brain violates.

- `docs/alerts/postgresSynchronousStandbyNamesMisconfigured.md:12-18`

```
Common causes:

- Host was kickstarted as standalone and `synchronous_standby_names` was
  stripped (`postgres-kickstart-standalone-db001-primary.yml`). Rebuild as HA.
- A replica was removed without updating `repmgr_other_postgres_nodes`, leaving
  only one name in the list.
- Manual edit to `postgresql.conf` that bypassed Ansible.
```

  The fleet's own runbook lists stripped SSN as a *common* cause, i.e. an expected real-world state, and acknowledges out-of-band manual edits as a third route Ansible cannot prevent.

- `ansible/roles/repmgr_setup/templates/repmgr.conf.j2:153-157, 328, 337`

```
#event_notification_command=''          # An external program or script which
#event_notifications=''                 # A commas-separated list of notification
...
promote_command='{{repmgr_promote_command}}'
follow_command='/usr/pgsql-{{pg_version}}/bin/repmgr standby follow -f /etc/repmgr/{{pg_version}}/repmgr.conf --log-to-file --upstream-node-id=%n'
```

  repmgr event hooks are disabled (commented out), and neither promote nor follow rewrites SSN. promote_command resolves to promote_replica.sh (global_postgres_all.yml:483), which contains only a `repmgr standby promote` call -- grep for synchronous/standby_names/ALTER SYSTEM/postgresql.conf across all repmgr_setup script templates returns nothing. Failover itself never clears SSN.

- `ansible/roles/postgres_exporter/templates/queries.yml.j2:196-199`

```
SELECT
           setting,
           CASE WHEN setting ~ '^ANY 1 \( \w+, \w+ \)$' THEN 1 ELSE 0 END AS matches
       FROM pg_settings WHERE name = 'synchronous_standby_names'
```

  Empty SSN is detectable (alert fires at == 0 after 10m, severity warning -- alerts.yml:1004-1011), but nothing prevents or auto-reverts it. Detection was only added 2026-05-22, nine months after the standalone playbook landed (2025-09-11).

- `ansible/environments/proact/global_postgres_all.yml:314-321`

```
# primary stops responding and automatic failover kicks in. It ensures that
# should a failed primary come back online, it will not be able to accept
# commits
postgres_synchronous_commit: remote_apply
postgres_synchronous_standby_names: "ANY 1 ( {% for host in repmgr_other_postgres_nodes %}{{ host|regex_replace('^(.*)(\\.sto[1-3]\\.fnox\\.se)$', '\\1')|replace('-', '_') }}{% if not loop.last %}, {% endif %}{% endfor %} )"
```

  The infra repo states the same safety argument ADR-002 relies on ('a failed primary will not be able to accept commits') -- and that argument is conditional on this variable actually reaching the running config, which paths A/B/C break.


**Caveats:** What this does NOT establish:

1. No live-node verification. This is a repo-only, read-only investigation -- I ran no psql and no ansible. I have not shown that any node currently has empty SSN, only that Ansible contains supported, documented procedures that produce it. The 2026-09-10 prod psql sample referenced in my brief covered application_name values, not synchronous_standby_names.

2. Execution history unknown. Whether postgres-kickstart-standalone-db001-primary.yml has ever actually been run against prod is not determinable from the repo. What IS in the repo: the commit that added it says it addresses "various failed failover events that we have seen" (cc1be2409a, 2025-09-11), and the alert runbook lists stripped SSN as a "common cause" (docs/alerts/...:14-15). Both are strong circumstantial support; neither is a run record. This is the main reason I said "high" rather than "certain".

3. Duration of exposure is not bounded by the repo. I found no scheduled/CI enforcement re-running postgres_server (no Jenkins job or cron in the repo invokes postgres-2-setup / postgres-3-ha / postgres-deploy; postgres-deploy.yml is a manual meta-playbook). So a stripped SSN persists until a human re-runs the role. But I cannot rule out out-of-band automation living outside this repo.

4. Path C severity is the mildest. The bootstrap window is closed by the standard sequence (postgres-2-setup -> postgres-3-ha) and by cleura_postgres-3.yml, so on a normal build it is minutes-to-hours during initial provisioning, before the cluster carries real traffic. It matters mainly because it proves the mechanism and because paths B/D reuse it deliberately on live nodes.

5. Manual edits are out of scope of any repo analysis. The runbook itself names "Manual edit to postgresql.conf that bypassed Ansible" as a cause. Ansible being authoritative is an operational convention, not an enforced invariant -- nothing in the config sets allow_alter_system=off (it appears only in commented-out documentation blocks in the PG17/18 templates).

6. PG semantics asserted, not tested here: that a removed SSN line yields the default '' and that synchronous_commit=remote_apply then does not wait, is standard PostgreSQL behaviour and is what the playbook's own comment relies on ("to allow it to take writes without replicas"). I did not empirically verify it on PG 15.14.

7. Config-path differences: I verified the gate in all five templates (13/14/15/17/18) and the SSN variable in both proact and gdc/it. The cleura environment has only group_vars/all and defines no postgres_synchronous_standby_names of its own -- if any cleura postgres host renders the template without inheriting proact's global_postgres_all.yml, the `is defined` half of the gate fails and SSN is omitted regardless of the marker file. Cleura hosts are generated dynamically (cleura_generate_inventory.yml) and have no result_manifests directory, so I could not confirm which vars they actually resolve; this is a possible fifth path I am flagging as unresolved rather than claiming.


**Impact on the audit:** CONFIRMS F3 and escalates it from "latent spec defect" to a live fleet risk. The ADR's safety argument -- "an isolated primary acked nothing" -- has an unstated precondition (non-empty synchronous_standby_names) that this fleet deliberately violates in at least two operator procedures, and the resolver never checks it.

The escalation turns on a correlation, not just reachability: the primary path to empty SSN (postgres-kickstart-standalone-db001-primary.yml) is a *post-failed-failover recovery* playbook, explicitly written for forked-timeline scenarios "that we have seen" (commit cc1be2409a). That is precisely the cluster state db-scan is asked to resolve. So the empty-SSN condition and the split-brain condition are not independent events whose joint probability can be discounted -- the operational response to the second one *creates* the first. A scan run against a cluster mid-kickstart, or against a cluster left in standalone mode after one, will apply a durability premise that is false for that node.

Path B (postgres-version-lr-upgrade-part3.yml:178) additionally means a planned major-version upgrade puts a prod primary into empty-SSN state on purpose, for the length of the LR migration window.

Rescoping recommendation: F3 should require the resolver to read synchronous_standby_names (and synchronous_commit) per node and refuse the "isolated primary acked nothing" inference when SSN is empty or synchronous_commit is not remote_apply/on -- treating it as an unknown-durability node rather than a safe one. Note this is the same class of gap as the DivergentReplicaWal gate: a safety premise assumed rather than measured.

Also rescopes the *evidence base* of the audit generally: the 321-manifest aggregate cannot be used to close questions of this shape. postgres-generate-result-manifests.yml:69-71 forces postgres_first_configuration_done true, so the manifests are rendered through the steady-state branch by construction and are blind to every bootstrap-gated and runtime-edited divergence. Any other audit finding that was dismissed on "all 321 manifests say X" should be re-examined for the same blindness -- in particular anything gated on that marker file (synchronous_commit is gated by it too, at template line 303).

What would close the remaining gap: a scan capture (or psql sweep) of pg_settings.synchronous_standby_names across the fleet, which would turn the "high" confidence here into direct observation of whether any node is currently in the empty state.


## What database role does db-scan connect as, and does it actually have the privileges ADR-002 assumes (`pg_read_server_files` for the `pg_read_file` timeline-history capture)?

**Confidence:** high

db-scan connects as a PERSONAL operator account (the FreeIPA domain user, e.g. `robert.sjoblom`) on all 264 cert/LDAP-auth nodes, and as the bootstrap `postgres` superuser on the 57 remaining dev nodes. Both paths are SUPERUSER, so `pg_read_file` works -- but ADR-002's stated mechanism is wrong: `pg_read_server_files` is NEVER granted anywhere in ~/work/infra. The privilege is satisfied because SUPERUSER bypasses the check, not because the role holds `pg_read_server_files`.

Details per sub-question:

1. Provisioning. "Operator" = an entry in `pg_domain_users_ops`, rendered by `postgres_setup_dynamic/tasks/7-setup-postgres-domain-users.yml` line 6: `role_attr_flags: "{% if item.superuser|default(False) == True %}SUPERUSER{% else %}{% endif %}"`. `robert.sjoblom` carries `superuser: True, state: present` in BOTH environment trees (proact -> acce/dev/infra/prod, and gdc -> it). Every `state: present` ops user in both lists is `superuser: true`; the only `superuser: false` entry (jonas.falck, gdc) is `state: absent`. There is no `pg_read_server_files`, no `pg_read_all_stats`, and no `pg_monitor` grant to any domain user anywhere in the repo -- a repo-wide grep for those three predefined roles returns exactly two hits, both about `pgmonitor` (grant `pg_monitor`, revoke `pg_read_all_settings`).

2. pg_hba. Rendered per-node `manifest_pg_hba.conf` exists for all 321 nodes. All 321 carry four ops lines (`10.100.124.0/24`, `10.110.124.0/24`, `10.140.124.0/24`, `10.255.124.0/24`) with `hostssl all all <net> <method>`. Method by env: prod 189/189 `ldap ... clientcert=verify-full`, acce 36/36 ldap, infra 24/24 ldap, it 3/3 `pam clientcert=verify-ca`, dev 12 ldap + 57 `scram-sha-256`. The LDAP search filter pins `gidNumber=1605201109` (ops group). So the operator can connect from any of the four ops networks on every node, and 264/321 additionally require a client cert whose CN matches the DB username (`verify-full`).

3. Service vs personal. Personal. `~/.config/db-scan/config.yml` line 2 is `user: robert.sjoblom`; `src/v2/db.rs:64` uses `args.pguser` on the cert path and `args.default_user` (line 11 of the same config: `user: postgres`) otherwise, selected by `Node::requires_cert()` (src/v2/node.rs:32-38). There is no service account. `pgmonitor` is never referenced anywhere in db-scan's source.

4. Non-superuser path. A non-superuser monitoring role DOES exist on the fleet (`pgmonitor`, member of `pg_monitor`, `pg_read_all_settings` explicitly revoked, conn_limit 10) -- and `pg_read_file` would certainly fail for it. But db-scan cannot reach it: `pgmonitor`'s only HBA lines are `hostssl all pgmonitor 127.0.0.1/32 cert` and `::1/128`, i.e. localhost-only, and db-scan never names it. So no configured non-superuser path exists today. The risk is a future config change: `pguser`/`default_user` are free-form strings with no privilege assertion at startup.

5. Failure-mode check. The mechanism is real and I confirmed every link: a privilege error inside the query makes `execute_primary_health_check` return Err -> `role: Role::UnknownPrimary` (health_check_primary.rs:258-272) -> `Cluster::primaries()` filters on `role.is_primary()` (cluster.rs:69-71) -> `analyze()` only calls `resolve_split_brain` when `primaries.len() > 1` (analyze.rs:317). A connect failure is worse in the same direction (`Role::Unknown`, scan.rs:121). BUT the plausibility of the specific "two-primary split brain presents as single-primary" outcome is LOW, for a structural reason: a privilege error is a property of the ROLE, not the node, and db-scan uses one credential per cluster, so a privilege failure hits all three nodes of a cluster identically -> `primaries.is_empty()` -> `ClusterVerdict::NoPrimary` (analyze.rs:311), which is loud, not silent.
   The one structural exception, which I do consider a real (if narrow) hazard: the query short-circuits with `WHEN timeline_id = 1 THEN NULL`, and Postgres does not evaluate the unselected CASE arm. So a TL=1 node never calls `pg_read_file` and succeeds even without the privilege, while a TL>=2 node fails. In a cluster's FIRST-EVER failover with a non-privileged role, the old primary (TL=1) would classify as `Role::Primary` and the newly promoted primary (TL=2) as `Role::UnknownPrimary` -> exactly one primary -> split brain silently missed. That is a deterministic asymmetry, not a coin flip. It is gated entirely on the role not being superuser, which config-as-code says is not the case today.


**Evidence:**

- `~/work/infra/ansible/roles/postgres_setup_dynamic/tasks/7-setup-postgres-domain-users.yml:2-7`

```
- name: Create domain users (without @fnox.se suffix)
    postgresql_user:
      name: "{{item.name}}"
      state: "{{item.state|default('present')}}"
      role_attr_flags: "{% if item.superuser|default(False) == True %}SUPERUSER{% else %}{% endif %}"
    loop: "{{pg_domain_users + pg_domain_users_extra|default([])}}"
```

  This is the only place operator DB roles are created. The role attribute is literally SUPERUSER or empty -- there is no branch that grants pg_read_server_files, pg_monitor, or pg_read_all_stats.

- `~/work/infra/ansible/environments/proact/global_postgres_all.yml:1345,1373-1375,1427`

```
pg_domain_users_ops:
...
  - name: robert.sjoblom
    superuser: True
    state: present
...
pg_domain_users: "{{ pg_domain_users_ops }}"
```

  The account db-scan authenticates as is provisioned SUPERUSER across the whole proact tree (acce/dev/infra/prod). Every state:present ops entry in this list is superuser:True.

- `~/work/infra/ansible/environments/gdc/global_config.yml:239,264-266,292`

```
pg_domain_users_ops:
...
  - name: robert.sjoblom
    superuser: true
    state: present
...
pg_domain_users: "{{ pg_domain_users_ops }}"
```

  Same for the gdc tree (the `it` environment). Both environment trees agree; there is no env where the operator is a non-superuser.

- `~/work/db-scan/src/v2/db.rs:52-71`

```
fn pg_cfg(node: &Node) -> Config {
...
    if node.requires_cert() {
        cfg.ssl_mode(SslMode::Require)
            .user(&args.pguser)
            .password(args.pgpassword.expose_secret());
    } else {
        cfg.ssl_mode(SslMode::Prefer)
            .user(&args.default_user)
            .password(&args.default_pass);
    }
```

  Two credential paths, both chosen purely by node name. `pguser` is the personal operator account; `default_user` is a fallback. No service account, and no privilege check at connect time.

- `~/.config/db-scan/config.yml:1-2,10-11,15`

```
postgres:
  user: robert.sjoblom
...
defaults:
  user: postgres
...
  user: robert_sjoblom
```

  The live resolved config: pguser = the personal FreeIPA operator account (superuser per infra), default_user = the bootstrap `postgres` superuser. Note the third value is the SSH user (underscore form), used only for disk checks -- not a DB role.

- `~/work/infra/ansible/result_manifests/prod/prod-pg-app001-db002.sto2.fnox.se/postgres/manifest_pg_hba.conf:27-28`

```
#Fortnox Vaxjo Office ops
hostssl    all			all			10.100.124.0/24			ldap ldaptls=1 ldapbasedn="cn=users,cn=accounts,dc=fnox,dc=se" ldapbinddn="uid=ldap_search,cn=users,cn=accounts,dc=fnox,dc=se" ldapbindpasswd="<redacted>" ldapsearchfilter="(&(uid=$username)(gidNumber=1605201109))" clientcert=verify-full
```

  Rendered proof the operator can reach any DB on any prod node from an ops network, authenticating by LDAP password plus a client cert whose CN must equal the DB username (verify-full). Present on 189/189 prod, 36/36 acce, 24/24 infra nodes; `it` uses `pam clientcert=verify-ca` on 3/3.

- `~/work/infra/ansible/environments/proact/global_postgres_all.yml:241,247,263-267`

```
pg_hba_ldap_ops_gid: 'ldapsearchfilter="(&(uid=$username)(gidNumber=1605201109))"'
...
pg_hba_ldap_ops_method: "{{ pg_hba_ldap_base }} {{ pg_hba_ldap_ops_gid }} clientcert=verify-full"
...
pg_hba_ops_networks:
  - { type: hostssl, network: '10.100.124.0/24', comment: 'Fortnox Vaxjo Office ops', method: '{{ pg_hba_ldap_ops_method }}' }
```

  The source variable that produces the rendered ops HBA lines. Access is gated on membership of the ops POSIX group, matching the same population as pg_domain_users_ops.

- `~/work/infra/ansible/roles/postgres_setup_dynamic/tasks/3.1-create-user.yml:2-10,16-21,31-37`

```
- name: Create pgmonitor user
    postgresql_user:
      db: postgres
      name: pgmonitor
      conn_limit: 10
...
  - name: Make pgmonitor member of pg_monitor
...
  - name: Revoke pgmonitor membership of pg_read_all_settings
```

  The only non-superuser monitoring role on the fleet. It is deliberately narrowed (pg_read_all_settings revoked) and its only HBA entries are 127.0.0.1/32 and ::1/128 cert -- db-scan connects remotely and never names it, so this is not a reachable path for db-scan.

- `~/work/db-scan/docs/adr/002-split-brain-resolution-refinement.md:29,204,266`

```
- Scanner role has `pg_read_server_files` (the tool is run by DBAs, so this privilege is in place)
...
Privileges: requires `pg_read_server_files`, which is granted in production.
...
deployment must have `pg_read_server_files` granted (already true in production).
```

  All three ADR assertions are factually wrong about the mechanism: grep across ~/work/infra finds zero grants of pg_read_server_files. The capability is real, but it comes from SUPERUSER on the operator account.

- `~/work/db-scan/src/v2/scan/health_check_primary.rs:145-159`

```
WITH cc AS (SELECT timeline_id FROM pg_control_checkpoint())
SELECT jsonb_build_object(
...
    'timeline_history', (
        SELECT CASE
            WHEN timeline_id = 1 THEN NULL
            ELSE pg_read_file(
                'pg_wal/' || lpad(upper(to_hex(timeline_id)), 8, '0') || '.history',
                0, (1024 * 1024)::bigint, true
            )
        END
        FROM cc
    ),
```

  The privileged call is inside an unselected-arm CASE. Postgres does not evaluate the ELSE arm when timeline_id = 1, so a hypothetical unprivileged role fails on TL>=2 nodes but succeeds on TL=1 nodes -- the only path by which a privilege error could hide one primary rather than all three.

- `~/work/db-scan/src/v2/scan/health_check_primary.rs:258-271`

```
Err(err) => {
            let kind = errors::extract_kind(&err);
            tracing::error!(error = ?err, "primary health check failed");
...
                role: Role::UnknownPrimary,
                errors: vec![kind],
```

  Confirms the failure mode's first link: ANY error in the jsonb_build_object -- including a pg_read_file permission error -- demotes a real primary to UnknownPrimary. It is logged and recorded in `errors`, so it is not literally silent, but the role is lost.

- `~/work/db-scan/src/v2/cluster.rs:69-71 and ~/work/db-scan/src/v2/analyze.rs:311-321`

```
pub fn primaries(&self) -> impl Iterator<Item = &AnalyzedNode> {
        self.nodes.iter().filter(|n| n.role.is_primary())
    }
...
    if primaries.is_empty() {
        verdict.cluster_verdict = Some(ClusterVerdict::NoPrimary);
        return AnalyzedCluster { cluster, verdict };
    }

    // Multiple primaries - Critical (split brain)
    if primaries.len() > 1 {
        let split_brain_info = resolve_split_brain(&primaries, &replicas);
```

  Closes the failure chain: UnknownPrimary is excluded from primaries(), and resolve_split_brain only runs at len() > 1. It also shows the mitigating branch -- a uniform (all-node) privilege failure yields the loud NoPrimary verdict, not a silent single-primary one.

- `~/work/infra/ansible/environments/proact/dev/group_vars/postgres_clusters/postgres_clusters.yml:218-225,377`

```
# Cluster 006 and 010 contains sensitive data, therefore they get pg_hba_serverlines_pam for LDAP login
hba_serverlines: |
  {% if pg_cluster_id in ['006', '010', '011'] %}
...
pg_domain_users_enabled: "{% if pg_cluster_id in ['006'] %}true{% else %}false{% endif %}"
```

  Side finding: dev clusters 006/010/011 require LDAP+client-cert for ops (matching db-scan's CERT_CLUSTERS_IN_DEV exactly), but the domain user is only CREATED on 006. On dev-pg-app010/011 the role `robert.sjoblom` is not provisioned by Ansible at all, so db-scan's cert path there would fail at connect (Role::Unknown), not at privilege check.

- `~/work/infra/ansible/result_manifests/dev/dev-pg-f3c034-db001.sto1.fnox.se/postgres/manifest_pg_hba.conf:16`

```
hostssl    all			all			10.100.124.0/24			ldap ldaptls=1 ... ldapsearchfilter="(&(uid=$username)(gidNumber=1605201109))" clientcert=verify-full
```

  Second side finding: dev-pg-f3c034-db00{1,2,3} require LDAP + client cert, but `Node::requires_cert()` returns false for them (CERT_CLUSTERS_IN_DEV lists only app006/010/011), so db-scan would offer user `postgres` with no client cert and be rejected. 12 dev nodes require cert; db-scan's hardcoded list covers only 9.


**Caveats:** What this does NOT establish:

- Runtime state. I verified the DESIRED state in config-as-code, not the live catalog. I did not connect to any database. If someone ran `ALTER ROLE robert.sjoblom NOSUPERUSER` by hand, Ansible would revert it only on the next run of the `create_pg_domain_users` tag, and nothing in the repo proves the last run succeeded on every node. `ansible/postgres_domain_user_audit.yml` exists and appears purpose-built to check exactly this drift, but I did not run it (side effects) and there is no captured output of it in the repo.
- The `postgres` role's superuser status is inferred, not quoted. The rendered `pg_users.yml` lists `- name: postgres` with a password and no `role_attr_flags`, and Ansible's `postgresql_user` with no flags does not alter them -- so it remains the initdb bootstrap superuser. That is an inference from module semantics, not a repo assertion. (By contrast `repmgr` IS explicitly `role_attr_flags: superuser` in the same file.)
- Other privileged calls in the same query. `pg_control_checkpoint()` and `pg_control_system()` are also execution-privileged in stock Postgres, so the query's privilege footprint is broader than the single `pg_read_server_files` ADR-002 names. I did not verify PG 15's exact default ACL for these offline -- flagging it as worth confirming, not asserting it.
- Config paths that could differ. `pguser` and `default_user` are plain strings resolvable from CLI (`--pguser`), env (`PGUSER`/`DEFAULT_USER`), or the config file, with the file lowest-priority (src/config.rs:274-293). Nothing validates the resulting role's privileges at startup, and `--print-config` would show the name but not the grants. Another operator running db-scan with a different `PGUSER` is out of scope of what I checked; all *listed* ops accounts are superuser, but a non-listed name would simply fail to connect.
- Whether dev-pg-f3c034 / dev-pg-app010 / dev-pg-app011 are actually in db-scan's scan set is not determinable from either repo -- the inventory comes from the database-portal API at runtime.


**Impact on the audit:** RESCOPES one ADR-002 finding and REFUTES (downgrades to Low) the privilege-driven variant of the F1-adjacent detection-gap hypothesis. Neither touches F1 itself (application_name vs node_name), which is unaffected by this line of inquiry.

1. RESCOPED -- ADR-002's `pg_read_server_files` assumption (lines 29, 204, 266) is a documentation defect, not a functional one. The assertion "granted in production" is false as written: the grant does not exist anywhere in config-as-code. The capability is nonetheless present because every ops account is provisioned SUPERUSER. Recommended rewrite: state the actual invariant -- "the scanner connects as a FreeIPA ops domain account provisioned SUPERUSER (ansible/environments/proact/global_postgres_all.yml pg_domain_users_ops), or as the bootstrap `postgres` superuser on non-cert dev nodes; `pg_read_file` succeeds by superuser bypass. If the tool is ever pointed at a non-superuser role, the timeline-history capture breaks." This is a low-severity correction but it matters, because the current wording would lead a future maintainer to believe a targeted grant exists that they could rely on or narrow to.

2. REFUTED as stated / KEPT as a narrow variant -- the "pg_read_file privilege error hides a primary and a two-primary split brain reads as single-primary" scenario. The chain is real and I confirmed every link in code (health_check_primary.rs:258-271 -> cluster.rs:69-71 -> analyze.rs:317), but its trigger is not: a privilege error is a property of the role, and db-scan uses one credential per cluster, so all three nodes fail identically and the verdict is the loud `ClusterVerdict::NoPrimary`, not a silent single-primary. Report this as Low likelihood / High impact rather than Critical.

   The one variant worth keeping in the audit is new and I do not think it was previously identified: the `WHEN timeline_id = 1 THEN NULL` short-circuit makes the privilege dependency TIMELINE-CONDITIONAL. Under a non-superuser role, a TL=1 node succeeds and a TL>=2 node fails, so a cluster's first-ever failover would present exactly one primary and the split brain would be missed deterministically. This is latent today (role is superuser) but it is a genuine landmine for any future least-privilege change -- which is precisely the change ADR-002's wording invites. Concrete mitigation to recommend: make the timeline-history capture fail soft rather than aborting the whole `jsonb_build_object` (wrap the read so a permission error yields NULL, or capture it in a separate query whose failure only nulls that field), so a privilege regression can never cost the tool a primary classification. Optionally add a startup assertion on the connecting role's capability.

3. NEW, out of ADR-002's scope but same detection-gap family -- two inventory/credential mismatches that also cost primaries silently, both via `Role::Unknown` on connect failure: (a) `CERT_CLUSTERS_IN_DEV` in src/v2/node.rs:5 lists 3 dev clusters, but 4 dev clusters (12 nodes) render cert-requiring ops HBA -- dev-pg-f3c034 is missing, so db-scan offers `postgres` with no client cert and is rejected; (b) dev-pg-app010 and dev-pg-app011 require LDAP auth but have `pg_domain_users_enabled: false`, so the operator role is never provisioned there. These are the same "node silently drops out of primaries()" failure shape as item 2, with a far more plausible trigger, and they argue for surfacing Unknown/UnknownPrimary counts in the split-brain verdict rather than letting them vanish.


## Exactly how is the replication `application_name` set on this fleet, and is the underscore/short form universal?

**Confidence:** certain

`application_name` is set by exactly one Ansible variable, `pg_replica_application_name`, defined once in a single file that every postgres environment symlinks into `group_vars/postgres_all/`. Its value is `{{inventory_hostname.split('.')[0]|replace('-', '_')}}` -- i.e. take the inventory FQDN, keep the first dot-separated label (strip the domain), and replace every `-` with `_`. There is no other assignment of that variable anywhere in the repo, no group_vars override, and no host_vars for any postgres node. The underscore/short form is therefore universal by construction, not by sampling.

Mechanism, end to end:
1. Definition: `ansible/environments/proact/global_postgres_all.yml:450`, with the transformation spelled out in the comments at lines 445-449 ("dev-pg-app001-db001.sto1.fnox.se -> dev-pg-app001-db001" then "-> dev_pg_app001_db001").
2. Consumption: the per-major-version postgresql.conf templates render it into `primary_conninfo`, guarded by `{% if pg_replica|default(false) == true %}` (postgresql15.conf.template:456). PG13/14/15/17/18 templates all carry the identical line (398/464/459/514/541). The out-of-band path `ansible/roles/postgres_server/tasks/templates/postgresql.auto.conf.j2:20` and the recovery playbook `ansible/postgres-kickstart-failed-standby-follow.yml:107` use the same variable.
3. Universality by symlink: `proact/{dev,acce,infra,prod}/group_vars/postgres_all/global_postgres_all.yml` and `gdc/it/group_vars/postgres_all/global_postgres_all.yml` are all symlinks to the one `proact/global_postgres_all.yml`. So all five environments (dev/acce/infra/prod/it) get the identical expression.
4. No override is possible in the current tree: repo-wide search for `pg_replica_application_name` returns exactly one assignment (plus four template consumers and one playbook). The four `host_vars/` directories contain no postgres nodes at all. Strong corroborating signal: the `it` environment DID have to override the sibling variable `postgres_synchronous_standby_names` (gdc/it/group_vars/postgres_all/postgres_all.yml:44) because that expression hardcodes the `\.sto[1-3]\.fnox\.se` domain -- but it did NOT need to override `pg_replica_application_name`, because `.split('.')[0]` is domain-agnostic. The authors hit the domain issue and the app-name expression survived it unchanged.
5. Rendered cross-check: there is no rendered repmgr.conf under result_manifests (only manifest_postgresql.conf x321, manifest_pgbackrest.conf x321, manifest_pg_hba.conf x321, and pgbouncer manifests), but manifest_postgresql.conf carries the rendered `primary_conninfo`. 214 of the 321 manifests contain one (the other 107 are exactly the `-db001` configured primaries, where the `pg_replica` guard suppresses the block). Across those 214: 214 distinct application_name values, 0 containing `-`, 0 containing `.`, all matching `^[a-z0-9_]+$`, and for every single one the value equals its own manifest-directory FQDN's first label with `-`->`_` (I compared each of the 214 against its directory name; zero mismatches). Example from a non-`sto` domain: `it-pg-app001-db002.it.fnox.se` renders `application_name=it_pg_app001_db002`.
6. repmgr `node_name`: `ansible/roles/repmgr_setup/templates/repmgr.conf.j2:27` sets `node_name='{{ansible_hostname|replace('-', '_')}}'` -- same underscore short form (ansible_hostname is the domainless hostname). Corroborated by repmgr_setup/tasks/main.yml lines 28, 55 and 84, which detect node registration by grepping `repmgr cluster show` output for `{{ansible_hostname|replace('-', '_')}}`. repmgr.conf sets no explicit `application_name`, so repmgr's own regenerated `primary_conninfo` (written by `standby clone` / `standby follow`, wired up at repmgr.conf.j2:337) defaults application_name to node_name -- the same underscore form. That last step is repmgr's documented default behaviour, inferred, not stated in the repo.


**Evidence:**

- `~/work/infra/ansible/environments/proact/global_postgres_all.yml:445-450`

```
# Use inventory_hostname instead of ansible_hostname to avoid having to gather facts
# split hostname on '.', and use the first potion to get the short-name
# (ex. dev-pg-app001-db001.sto1.fnox.se -> dev-pg-app001-db001)
# Then replace '-' with '_' since '-' cannot be used in postgres application_name
# (ex. dev-pg-app001-db001 -> dev_pg_app001_db001)
pg_replica_application_name: "{{inventory_hostname.split('.')[0]|replace('-', '_')}}"
```

  The single, authoritative definition. Domain stripped via split('.')[0]; hyphens -> underscores. The comment states the intent explicitly: '-' cannot be used in a postgres application_name.

- `~/work/infra/ansible/roles/postgres_server/templates/postgresql15.conf.template:456-459`

```
{% if pg_replica|default(false) == true %}
# Specifies a connection string which is used for the standby server to connect
# with the primary.
primary_conninfo = 'user={{pg_replica_replication_user}} connect_timeout=2 host={{pg_replica_master_ip}} port={{pg_listen_port}} application_name={{ pg_replica_application_name|default(inventory_hostname) }}'
```

  The only place application_name reaches PostgreSQL for PG15 (the prod version). Identical line in postgresql13.conf.template:398, 14:464, 17:514, 18:541. The `|default(inventory_hostname)` fallback is dead on this fleet because the variable is always defined via group_vars.

- `~/work/infra/ansible/environments/proact/global_postgres_all.yml:430`

```
pg_replica: "{% if inventory_hostname != pg_replica_master_hostname %}True{% else %}False{% endif %}"
```

  Explains why only 214 of 321 rendered manifests contain primary_conninfo: the 107 `-db001` nodes are the configured primaries and the guarded block is skipped.

- `~/work/infra/ansible/environments/gdc/it/group_vars/postgres_all/global_postgres_all.yml (symlink)`

```
ansible/environments/gdc/it/group_vars/postgres_all/global_postgres_all.yml -> ../../../../proact/global_postgres_all.yml
ansible/environments/proact/dev/group_vars/postgres_all/global_postgres_all.yml -> ../../../global_postgres_all.yml
ansible/environments/proact/acce/group_vars/postgres_all/global_postgres_all.yml -> ../../../global_postgres_all.yml
ansible/environments/proact/infra/group_vars/postgres_all/global_postgres_all.yml -> ../../../global_postgres_all.yml
ansible/environments/proact/prod/group_vars/postgres_all/global_postgres_all.yml -> ../../../global_postgres_all.yml
```

  All five postgres environments (dev, acce, infra, prod, it) load the same physical file. One definition, fleet-wide.

- `~/work/infra/ansible/environments/gdc/it/group_vars/postgres_all/postgres_all.yml:44`

```
postgres_synchronous_standby_names: "ANY 1 ( {% for host in repmgr_other_postgres_nodes %}{{ host|regex_replace('^(.*)(\\.it\\.fnox\\.se)$', '\\1')|replace('-', '_') }}{% if not loop.last %}, {% endif %}{% endfor %} )"
```

  The `it` env had to override the sibling sync-names variable (whose upstream form at global_postgres_all.yml:321 hardcodes \.sto[1-3]\.fnox\.se) but did NOT override pg_replica_application_name -- because split('.')[0] is domain-agnostic. Evidence the app-name expression is robust across domains, and that overrides here are visible in-repo when they exist.

- `~/work/infra/ansible/result_manifests/it/it-pg-app001-db002.it.fnox.se/postgres/manifest_postgresql.conf`

```
primary_conninfo = 'user=replicator connect_timeout=2 host=10.105.6.200 port=5432 application_name=it_pg_app001_db002'
```

  Rendered proof for the one environment with a non-sto domain. Directory (= inventory_hostname) is it-pg-app001-db002.it.fnox.se; application_name is it_pg_app001_db002.

- `~/work/infra/ansible/result_manifests/ (aggregate over all 5 envs)`

```
214 rendered `primary_conninfo` lines; 214 distinct application_name values; grep -c '-' => 0; grep -c '\.' => 0; grep -vE '^[a-z0-9_]+$' => NONE; per-file comparison of application_name against <manifest-dir FQDN first label with '-'->'_'> => 0 mismatches
```

  Rendered output confirms the template analysis on every standby in the fleet. Not one node produces a hyphenated or FQDN-shaped application_name.

- `~/work/infra/ansible/roles/repmgr_setup/templates/repmgr.conf.j2:27`

```
node_name='{{ansible_hostname|replace('-', '_')}}'
```

  repmgr node_name is the same underscore short form (ansible_hostname is domainless). This is why repmgr's event log shows prod_pg_app001_db002, and why repmgr-regenerated primary_conninfo keeps the same application_name form after a failover.

- `~/work/infra/ansible/roles/repmgr_setup/tasks/main.yml:28`

```
shell: "/usr/pgsql-{{pg_version}}/bin/repmgr -f /etc/repmgr/{{pg_version}}/repmgr.conf cluster show | grep -q {{ansible_hostname|replace('-', '_')}}"
```

  Ansible itself matches repmgr cluster-show output against the underscore form (also lines 55, 84) -- independent confirmation that the underscore form is what repmgr reports.

- `~/work/db-scan/src/v2/analyze/split_brain.rs:306-307`

```
!conn.application_name.is_empty()
    && conn.application_name == replica.node_name
```

  The raw == on the resolver's primary-side corroborating gate. Left side is the PG-reported underscore short form; right side is AnalyzedNode.node_name.

- `~/work/db-scan/src/v2/node.rs:12-14`

```
#[serde(rename = "node_name")]
    pub name: String,
```

  node_name comes verbatim from the database-portal API and is propagated unchanged to AnalyzedNode.node_name (src/v2/scan.rs:118, 146, 173).

- `~/work/db-scan/tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json:35 and :93`

```
"application_name": "dev_pg_app001_db002",
...
"node_name": "dev-pg-app001-db002.sto2.example.com",
```

  The repo's own captured healthy-cluster fixture already contains both forms side by side -- the mismatch is present in real captured data, not hypothetical.

- `~/work/db-scan/src/v2/writer/build.rs:422-429`

```
fn normalize_application_name(app_name: &str) -> String {
    // Application names are like: dev_pg_app001_db002
    if let Some(db_part) = app_name.split('_').next_back()
        && db_part.starts_with("db")
    {
        return db_part.to_owned();
```

  The writer layer already knows application_name is underscore-delimited and normalizes it before display. The resolver does not. The knowledge exists in the codebase; it just was not applied at split_brain.rs:307.


**Caveats:** What this does NOT establish:
1. It does not prove what the live running config on each box is -- result_manifests is Ansible's rendered output, i.e. what Ansible WOULD write. Drift (manual edits to postgresql.auto.conf, a repmgr `standby follow` that rewrote primary_conninfo, a node never converged) is not visible here. However, drift can only move application_name toward repmgr's node_name, which is the SAME underscore form (repmgr.conf.j2:27), so drift cannot produce the FQDN form either. Prod psql on 2026-09-10 independently confirmed the underscore form live.
2. `ansible_hostname` (used for repmgr node_name) is a gathered fact, not derivable from the repo. I found no role that sets the OS hostname, so I cannot prove from config-as-code that ansible_hostname == inventory_hostname's first label. This affects only the repmgr node_name claim, not application_name (which uses inventory_hostname and is fully repo-derivable). Prod repmgr event-log evidence covers the gap.
3. That repmgr's regenerated primary_conninfo defaults application_name to node_name is repmgr's documented behaviour, INFERRED -- repmgr.conf.j2 does not set application_name explicitly and I did not find a repo artifact asserting it.
4. There is no rendered repmgr.conf anywhere under result_manifests, so I could not aggregate node_name across the fleet the way I did application_name. The only manifest kinds present are manifest_postgresql.conf (321), manifest_pgbackrest.conf (321), manifest_pg_hba.conf (321), manifest_pgbouncer.ini (318), manifest_pgbouncer.database.ini (318), manifest_pgbouncer_pg_hba.conf (318), manifest_pgbouncer_user_config.ini (45).
5. Config path that could differ: a future host_vars file or a more-specific group_vars group could override pg_replica_application_name and Ansible would honour it. No such override exists today (repo-wide search returns exactly one assignment, and no postgres node has a host_vars file at all). The `|default(inventory_hostname)` fallback in the templates would yield the FQDN form -- but it is unreachable while the group_vars definition exists, since postgres_all covers every postgres node.
6. The 107 manifests with no rendered application_name are all `-db001` primaries; I verified none of them is a non-db001 node. Recovery nodes (infra-recovery-nodeNNN) have no postgres manifest and are not among the 321.


**Impact on the audit:** CONFIRMS F1 and upgrades it from a sampling-based observation to a structural, fleet-wide certainty. The comparison at src/v2/analyze/split_brain.rs:307 (`conn.application_name == replica.node_name`) can NEVER be true on this fleet for any node, in any environment, in any cluster. Not "usually false" -- always false, by construction:
  - left side is always `^[a-z0-9_]+$` (env_type_clusterNNN_dbNNN), produced by a single group_vars expression symlinked into all 5 environments;
  - right side is always the hyphenated FQDN from the database-portal API;
  - the two character sets are disjoint on the separator, so no host, group, environment or override can make them equal.

Concrete rescoping this enables:
1. F1's blast radius is 100% of nodes, not "some". The primary-side corroborating gate in `find_replicas_streaming_from_primary` is dead code in production: `primary_row` is always `None`. Whatever the resolver does on the None branch is what it has ALWAYS done in prod, for every scan, since the gate was written. The audit should re-examine that branch's behaviour as the de facto only behaviour, not as an edge case.
2. It also means the gate has never contributed evidence to any recorded verdict. Any ADR-002 claim of the form "the primary-side check corroborates the replica-side check" is unsupported on this fleet -- the replica-side gate (wal_receiver sender_host/sender_port/status/last_msg_receipt_time, split_brain.rs:281-286) is doing 100% of the work. Note the replica-side gate matches on IP (`wr.sender_host == primary.ip_address.to_string()`), not on name, which is why the resolver still functions at all.
3. The fix is unambiguous and low-risk because the naming rule is exact and total: normalize node_name with `name.split('.').next().unwrap().replace('-', "_")` and compare to application_name. This is the same transformation Ansible performs at global_postgres_all.yml:450, applied in reverse direction. It is guaranteed lossless here because the first FQDN label is unique fleet-wide (all 214 rendered application_names are distinct). db-scan already encodes half of this knowledge at src/v2/writer/build.rs:422 (`normalize_application_name`), so the fix is consistent with existing code, not a new abstraction.
4. Supports adding a regression test directly from the existing captured fixture tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json, which already contains node_name "dev-pg-app001-db002.sto2.example.com" alongside application_name "dev_pg_app001_db002" -- real captured data that exercises the bug with no synthetic setup needed.
5. Secondary note for the ADR, not a new finding: repmgr node_name (repmgr.conf.j2:27) uses the same underscore form, so after a repmgr-driven failover rewrites primary_conninfo the application_name form is unchanged. There is no post-failover state in which the FQDN form appears, i.e. no scenario where the current `==` accidentally starts working.


## Is `pg_stat_replication.client_addr` a reliable identity for a replica on this fleet -- i.e. is `client_addr == inventory ip_address` a safe join, and where could it break?

**Confidence:** high

Yes. On this fleet `client_addr` == inventory `ip_address` is a safe join, and the repo proves it rather than merely suggesting it. Replication is node-to-node TCP to port 5432 using a literal IPv4 address that Ansible takes straight from the inventory (`ansible_host`), and the primary's `pg_hba.conf` only accepts replication from that exact set of /32 addresses -- so a rewritten source address would not produce a wrong match, it would produce no replication connection at all.

Four independent legs:

(1) NO ADDRESS REWRITE IN THE REPLICATION PATH. There is no NAT, VIP, proxy or LB between postgres nodes. A repo-wide grep for MASQUERADE/SNAT/DNAT over ansible/roles, ansible/environments and tofu/ returns only the RKE2 cilium pod network, a Redis comment and a Postfix comment -- nothing touching postgres. The `iptables` role has no `nat` table content at all (ansible/roles/iptables/defaults/main.yml, filter rules only). `keepalived` is used by exactly one playbook, `ansible/storagelbs.yml` (hosts: storagelbs) -- storage load balancers, not postgres. `haproxy_setup_dynamic` has zero references to 5432 or postgres. pgbouncer IS installed on the DB nodes but is not in the replication path: all 318 rendered pgbouncer configs listen on 6432 and point upstream at `host=127.0.0.1`, while replication targets port 5432 directly. The "proxy" seen in labels (`prod-pg-app001-proxy.fnox.se`) is a FreeIPA DNS alias plus a CoreDNS rewrite inside RKE2 -- name resolution for application pods, not a packet path, and not used by replication at all.

(2) primary_conninfo host= IS ALWAYS AN IP, FLEET-WIDE. The variable resolves to the peer's inventory address, and the rendered output confirms it: 214 of 321 nodes carry a `primary_conninfo` line (the other 107 are all the db001 designated primaries, gated by `{% if pg_replica %}`), and 214/214 have `host=<IPv4 literal>`. Zero hostname-form hosts in any environment. The ADR's "hostname form is out of scope" caveat is therefore vacuous on the current fleet.

(3) ONE ADDRESS PER NODE, AND IT IS THE INVENTORY ADDRESS. VMs get exactly one NIC whose static IP is `ansible_host` (ansible/roles/vm_deploy/defaults/main.yml:26-33); `vm_extra_networks` is referenced only in the role's README and its own task guard and is defined for no host anywhere; `network_interfaces:` (the multi-NIC role) is defined in no environment file, leaving the role default `[]`. There is no dedicated replication subnet -- the 10.81/10.82/10.83 third octet encodes the site (sto1/sto2/sto3), not a second interface, so `10.81.17.1 / 10.82.17.1 / 10.83.17.1` are three different nodes of one cluster, not three addresses of one node. I verified this mechanically: across all 321 rendered `manifest_pg_hba.conf`, every `hostssl replication` line is /32 (963 lines, zero exceptions, zero CIDR wildcards), and for 321/321 nodes the node's OWN inventory `ansible_host` appears in its own replication HBA list. Every replication /32 in the fleet resolves to some inventory `ansible_host`. And the 321 postgres `ansible_host` values are unique -- no two postgres nodes share an IP, so the join is 1:1.

(4) NO SLOTS. `pg_replica_primary_slot` is defined in no environment file (only referenced in the five postgresql*.conf templates), so 0 of 321 rendered configs contain an uncommented `primary_slot_name`. repmgr's `use_replication_slots` is left commented out (default no). This matches the fixture and prod psql showing empty slot_name -- slot name is not an available alternative identity here.

Bonus corroboration for the fix's symmetry: the codebase already performs exactly this join elsewhere -- src/v2/analyze/checks.rs:194, :224 and :334 match `n.ip_address.to_string() == client_addr`, and split_brain.rs:281 matches `wr.sender_host == primary.ip_address.to_string()`. The F1 fix is consistent with existing, working code, not a new pattern. Also note repmgr rewrites `primary_conninfo` into postgresql.auto.conf during failover, and its own conninfo is `host={{ansible_default_ipv4.address}}` (repmgr.conf.j2:38) -- still an IP, still the single-NIC address, so the invariant survives an automatic failover window.


**Evidence:**

- `ansible/environments/proact/global_postgres_all.yml:443`

```
pg_replica_master_ip: "{{ hostvars[pg_replica_master_hostname]['ansible_host'] }}"
```

  The host= in primary_conninfo IS the peer's inventory ansible_host. Same field the db-scan inventory ip_address corresponds to. Not a hostname, not a VIP.

- `ansible/roles/postgres_server/templates/postgresql15.conf.template:459`

```
primary_conninfo = 'user={{pg_replica_replication_user}} connect_timeout=2 host={{pg_replica_master_ip}} port={{pg_listen_port}} application_name={{ pg_replica_application_name|default(inventory_hostname) }}'
```

  Replica dials the primary's inventory IP directly on 5432. Identical line exists in postgresql13.conf.template:398, 14:464, 17:514, 18:541 -- every PG major on the fleet.

- `ansible/result_manifests/prod/prod-pg-app001-db002.sto2.fnox.se/postgres/manifest_postgresql.conf`

```
primary_conninfo = 'user=replicator connect_timeout=2 host=10.81.17.1 port=5432 application_name=prod_pg_app001_db002'
```

  Rendered proof. Aggregated: 214/321 nodes have primary_conninfo (the 107 without are all db001 primaries) and 214/214 use an IPv4 literal for host=. Zero hostname-form hosts fleet-wide, so the ADR's 'hostname form out of scope' caveat does not apply to any current node.

- `ansible/roles/postgres_server/templates/pg_hba.conf.j2:55-60`

```
{% if pg_replication_hba_generation|default(False) == True %}

#Replication access from dbs in cluster
{% for postgres_host in pg_cluster_hosts_ip %}
hostssl    replication			{{ pg_replica_replication_user }}			{{ postgres_host }}/32			{{ hba_generated_auth_method|default('md5') }}
{% endfor %}
```

  THE DECISIVE CONSTRAINT. The primary accepts replication only from exact /32 inventory addresses. If anything rewrote the source address (NAT, proxy, second NIC, VIP), the walsender connection would be rejected by HBA -- so a streaming row in pg_stat_replication is itself proof that client_addr is the unrewritten inventory IP. A mismatch cannot silently produce a wrong join; it produces no connection.

- `ansible/environments/proact/global_postgres_all.yml:466`

```
pg_cluster_hosts_ip: "{{ pg_cluster_hosts_name | map('extract', hostvars, 'ansible_host') }}"
```

  The /32 allow-list is literally the map of cluster members to their inventory ansible_host. Same source field as primary_conninfo host= and (per NetBox) the portal's ip_address.

- `ansible/result_manifests/prod/prod-pg-app001-db002.sto2.fnox.se/postgres/manifest_pg_hba.conf:46-48`

```
hostssl    replication			replicator			10.81.17.1/32			scram-sha-256
hostssl    replication			replicator			10.82.17.1/32			scram-sha-256
hostssl    replication			replicator			10.83.17.1/32			scram-sha-256
```

  Exactly one address per cluster member (81/82/83 = sto1/sto2/sto3 site octet, not multi-homing). Fleet aggregate over all 321 manifests: 963 replication lines, 100% /32, zero non-/32; and for 321/321 nodes the node's own ansible_host is in its own list. The 321 postgres ansible_host values are unique -- the IP->node join is 1:1.

- `ansible/roles/vm_deploy/defaults/main.yml:26-33`

```
networks:
    - name: "{{ vm_network }}"
      ip: "{{ ansible_host }}"
      netmask: "{{ vm_netmask }}"
      gateway: "{{ vm_gateway }}"
      domain: "{{ freeipaclient_domain }}"
      type: "static"
      device_type: "vmxnet3"
      connected: true
```

  Single NIC, static, IP == ansible_host. vm_extra_networks (vm.yml:23) is defined for no host in any environment; network_interfaces: appears in no environment file (role default [] at ansible/roles/network-interfaces/defaults/main.yml:4). No dedicated replication network exists.

- `ansible/roles/iptables/defaults/main.yml:6-15`

```
iptables_default_head: |
  -P INPUT ACCEPT
  -P FORWARD ACCEPT
  -P OUTPUT ACCEPT
  -A INPUT -m state --state RELATED,ESTABLISHED -j ACCEPT
```

  Filter table only, no nat table. A repo-wide grep for MASQUERADE/SNAT/DNAT across roles, environments and tofu hits only rke2 cilium pod networking, a redis.conf comment and a postfix comment -- nothing on the postgres path.

- `ansible/storagelbs.yml:2-9`

```
- hosts: storagelbs
  become: yes
  roles:
    - role: certificates
    - role: keepalived
    - role: haproxy_install
    - role: haproxy_setup_dynamic
```

  keepalived/HAProxy are confined to the storagelbs group. No VIP or floating IP is applied to postgres hosts anywhere in the repo, and haproxy_setup_dynamic contains no reference to 5432 or postgres.

- `ansible/result_manifests/prod/prod-pg-app001-db002.sto2.fnox.se/pgbouncer/manifest_pgbouncer.database.ini:2`

```
* = host=127.0.0.1 port=5432 auth_user=pgbouncer
```

  pgbouncer is a colocated client-side pooler, not a replication hop. Fleet aggregate: 318/318 pgbouncer configs listen on 6432 and 336/336 database entries point at 127.0.0.1. Replication uses port 5432 directly and never transits it.

- `ansible/roles/postgres_server/templates/postgresql15.conf.template:461-464`

```
{% if pg_replica_primary_slot is defined and pg_replica_primary_slot != '' %}
primary_slot_name = '{{ pg_replica_primary_slot }}'
{% else %}
#primary_slot_name = ''			# replication slot on sending server
```

  pg_replica_primary_slot is defined in no environment file, so the else-branch always renders: 0 of 321 rendered configs have an uncommented primary_slot_name. Confirms the fixture/prod observation of empty slot_name. No slot identity is available as an alternative join key.

- `ansible/roles/repmgr_setup/templates/repmgr.conf.j2:38`

```
conninfo='host={{ansible_default_ipv4.address}} user={{repmgr_pg_user}} dbname=repmgr connect_timeout=2 sslmode=require'
```

  repmgr's own node conninfo is the node's default-route source IP -- the same single-NIC address. So when repmgr rewrites primary_conninfo into postgresql.auto.conf during a failover, host= remains an IP and the client_addr invariant survives the failover window (the exact window a split-brain resolver runs in).

- `ansible/environments/proact/prod/inventory.yaml:157-158`

```
prod-pg-app007-db001.sto1.fnox.se:
                      ansible_host: 10.81.17.7
```

  Ties infra to db-scan: the Node fixture at src/v2/node.rs:51-53 is name 'prod-pg-app007-db001.sto1.example.com' with ip_address 127.1.17.7 -- the anonymised form of 10.81.17.7 (10.8X -> 127.X). The portal's ip_address is the inventory ansible_host.

- `ansible/netbox_add_instance.yml:102`

```
primary_ip4: "{{ ansible_host }}"
```

  NetBox records one primary IPv4 per VM, sourced from ansible_host. If the database-portal inventory derives from NetBox (likely but NOT verified from this repo), ip_address == ansible_host by construction.


**Caveats:** WHAT THIS DOES NOT ESTABLISH:

1. I did not verify the database-portal's ingestion path. The claim "inventory ip_address == ansible_host" is INFERRED, not read from the portal. Two supports: ansible/netbox_add_instance.yml:102 sets NetBox `primary_ip4: {{ ansible_host }}` (one IP per VM), and the db-scan fixture pairs prod-pg-app007-db001.sto1 with 127.1.17.7, the anonymised form of that host's ansible_host 10.81.17.7 (inventory.yaml:157-158). Both point the same way, but I did not query the portal or read its ETL. If the portal ever sourced ip_address from DNS resolution or from a NetBox interface record other than primary_ip4, this could drift. Cheap to close: one database-portal query comparing node_name/ip_address against the inventory pairs.

2. Everything here is config-as-code and rendered output -- I ran nothing against a live host. I did not read `ip addr` on any node, so I cannot rule out an address added out-of-band (manual second NIC, a container/bridge address). Mitigating: any such extra address would have to be the packet source for replication to matter, and if it were, the /32 hostssl replication rule would reject the connection. So an out-of-band address breaks replication rather than breaking the join.

3. Rendered manifests are a point-in-time snapshot of what Ansible last produced. If a node's live postgresql.conf/pg_hba.conf has drifted from its manifest (hand-edit, or a repmgr write into postgresql.auto.conf that Ansible has not yet cleared -- see ansible/roles/postgres_server/tasks/templates/postgresql.auto.conf.j2:10-14, which explicitly exists to empty that drift), the live values could differ. For host= specifically this is low-risk: repmgr's rewrite also uses an IP (repmgr.conf.j2:38).

4. CONFIG PATHS THAT COULD DIFFER FROM THE MAIN FINDING:
   - The `it` environment (3 nodes, gdc/it) uses an INI-format inventory at ansible/environments/gdc/it/inventory:118-122 rather than inventory.yaml, and sits in a different subnet (10.105.6.200-202, all three nodes in one /24 rather than split across sto1/2/3 site octets). Same single-IP, /32, IP-literal shape -- I verified its conninfo and HBA directly -- but it is a distinct provisioning path (gdc, not proact) and a different physical DC. If a future NAT or inter-site firewall appeared anywhere, this env is the likeliest place.
   - Citus clusters take a different HBA branch (pg_hba.conf.j2:61-67, `hostssl all all {{ hostvars[...].ansible_host }}/32 trust`). Still ansible_host, still /32, so the identity argument holds, but citus nodes are a separate code path I did not audit for replication topology.
   - `pg_replication_hba_generation` is set True at global_postgres_all.yml:185 and re-affirmed in two group_vars, and all 321 rendered manifests contain generated replication lines -- so no node currently falls back to hand-written `hba_serverlines` replication entries. A future node that did could in principle use a wider CIDR. Zero non-/32 replication lines exist today.

5. I checked whether any postgres node shares an IP with another (none do -- 321 unique). The single fleet-wide duplicate ansible_host is 127.0.0.2 on two non-postgres hosts, irrelevant here.

6. Out of scope for this question and NOT investigated: whether application_name itself could be repaired instead (that is the other half of F1), what repmgr writes for application_name during a failover rewrite, and whether the resolver's downstream verdict logic handles a None join correctly. I flagged the None-handling cases in impact_on_audit but did not read the resolver's consumption of them.


**Impact on the audit:** CONFIRMS the F1 fix as specified, and REMOVES two caveats the ADR currently carries.

1. The recommended fix is sound and should be adopted as written. Matching `conn.client_addr` to the replica's inventory `ip_address` is a correct, 1:1, fleet-wide-valid join. It is strictly better than the current raw `application_name == node_name` compare, which is broken for the reason the audit found (underscore short-name vs hyphenated FQDN). It also makes the primary-side gate symmetric with the replica-side gate that already does `wr.sender_host == primary.ip_address.to_string()` (src/v2/analyze/split_brain.rs:281), and reuses a join pattern already present in src/v2/analyze/checks.rs:194, :224, :334.

2. RESCOPE: the ADR's "production uses IPs, hostname form is out of scope" caveat can be dropped as a live concern. It is not merely the current convention -- it is structurally enforced. `host=` is templated from `hostvars[...]['ansible_host']` on every PG major, and 214/214 rendered conninfo lines are IPv4 literals with zero hostname-form hosts in any of the five environments. The ADR should say so and cite the template, rather than hedging.

3. STRENGTHEN the argument: the pg_hba /32 allow-list makes this a safety invariant, not a coincidence of naming. Because the primary only accepts `hostssl replication` from the exact inventory /32s (963 lines fleet-wide, zero exceptions), a client_addr that failed to match an inventory ip_address could not have established a streaming connection in the first place. The failure mode "client_addr is a NAT/proxy address and silently joins to the wrong node" is not reachable on this fleet. This is a stronger guarantee than the ADR currently claims for any identity field, and worth stating explicitly since a split-brain resolver's verdict hangs on it.

4. WHERE IT STILL BREAKS -- five residual cases the implementation must handle, none of which invalidate the join but three of which could produce a wrong VERDICT if treated as "replica absent":
   (a) NULL client_addr. pg_stat_replication rows for local/unix-socket walsenders have client_addr NULL. Already guarded in checks.rs:188 and :219 via `let Some(...) else`; the new split_brain code must do the same and must NOT treat NULL as a failed identity match.
   (b) Non-streaming walsenders. `pg_basebackup`/repmgr `standby clone` (see ansible/roles/postgres_replica/tasks/perform_base_backup.yml:24, which runs pg_basebackup -X stream from a cluster peer) creates rows with a legitimate cluster-member client_addr but state='backup'. Filter on streaming state before treating a match as an attached replica -- otherwise a node being rebuilt counts as a healthy quorum member. This matters directly given the established "replica remediation = rebuild from basebackup" workflow.
   (c) Cascading replication. If a replica streams from another replica, the primary has no row for it and the join correctly yields None. This is "not connected to THIS primary", not "identity mismatch", and must not be conflated. checks.rs:253-255 already contemplates a sender_host that is another replica.
   (d) Unmatched client_addr. An address present in pg_stat_replication but absent from the db-scan inventory (a node the portal has not ingested yet, a temporary standby) yields no match. This should surface as an explicit anomaly, not be silently dropped -- silently dropping it under-counts attached replicas and can flip a quorum-satisfied verdict to unsatisfied.
   (e) IPv6. Not reachable today (host= is an IPv4 literal and Node.ip_address is typed Ipv4Addr at src/v2/node.rs:16), but if IPv6 were ever enabled, client_addr would render as `::ffff:10.x.x.x` or a native v6 address and the string compare would fail. Worth a one-line note, not a code change.

5. NOT AFFECTED: slot_name remains unusable as an identity (0/321 nodes configure a slot, confirmed against both template and rendered output), so client_addr is the only viable IP-grade identity and there is no fallback to design.

---

# Appendix E -- raw prod evidence (psql, 2026-09-10)

Captured by the user on prod-pg-app001 during this review. Preserved here because the session
scratchpad is not durable.

# Prod evidence captured 2026-09-10 (psql, prod-pg-app001)

Source: user ran queries on prod-pg-app001-db001 (primary) and prod-pg-app001-db002 (replica).

- version: PostgreSQL 15.14 (NOT 17)
- synchronous_standby_names = 'ANY 1 ( prod_pg_app001_db002, prod_pg_app001_db003 )'  [underscores]
- synchronous_commit = remote_apply
- wal_sender_timeout = 300000, pg_settings.unit = 'ms'      -> ADR "raw ms, no suffix" CONFIRMED
- wal_receiver_status_interval = 10, unit 's'
- cluster_name = '' (unset)
- pg_stat_replication.application_name: prod_pg_app001_db002 / prod_pg_app001_db003, both sync_state=quorum
- shell prompt shows host prod-pg-app001-db002 (hyphens) -> app_name uses underscores, host uses hyphens
- current_user=postgres, pg_has_role(pg_read_server_files)=t  [superuser; says nothing about the scanner role]
- pg_control_checkpoint().timeline_id = 15
- history read via lpad(upper(to_hex(15)),8,'0') -> 0000000F.history, EXISTS. HEX naming CONFIRMED.
- pg_ls_dir shows 00000008..0000000F.history (hex)
- history contents: lines for TL 1..14 only (ancestors), tab-separated <parent_tli> <switch_lsn> <reason>
  last line "14  16A/BB0000A0" = fork point of the CURRENT TL 15. So current TL's own fork IS the last line.
- pg_stat_wal_receiver on primary: 0 rows. viewdef ends "WHERE s.pid IS NOT NULL". -> COALESCE in scanner is dead; NULL -> Option::None. No deserialisation trap.
- ON THE PROMOTED PRIMARY:
    pg_is_in_recovery = f
    pg_last_wal_replay_lsn  = 16A/BB0000A0   (NOT NULL) == exactly the TL14->15 switch LSN
    pg_last_wal_receive_lsn = NULL           <-- receive_null = t
  ADR line ~215 says "confirmed on a promoted primary, where pg_last_wal_receive_lsn() IS NULL returns f".
  PROD RETURNS t. The ADR's stated measurement is contradicted on prod.
  ADR line ~217 says both freeze at the promotion LSN and both returned 6FD/7C0000A0. Prod: only replay is set.
- synchronous_commit enum aliases normalise: SET LOCAL synchronous_commit=false; SHOW -> 'off'.
  -> denylist covering "off" is sufficient; alias-evasion concern REFUTED.
- SET LOCAL synchronous_commit='remote_flush' -> ERROR: invalid value.
  HINT: Available values: local, remote_write, remote_apply, on, off.
  -> ADR section 2 "Valid values: on, remote_apply, remote_flush" is FACTUALLY WRONG. remote_flush does not exist.
- Replica prod-pg-app001-db002: wal_receiver status=streaming, sender_host=10.81.17.1 (IP), sender_port=5432,
  slot_name empty, received_tli=15, flushed_lsn=190/FC0132C0
  primary_conninfo: host=10.81.17.1 ... application_name=prod_pg_app001_db002
  pg_last_wal_replay_lsn == pg_last_wal_receive_lsn == 190/FC0DA968
- User states: "db-scan fixtures are up to date"

# Inventory naming (from code, not psql)
src/v2/node.rs: Node.name is deserialised from the database-portal API field "node_name".
Node::fixture() = "prod-pg-app007-db001.sto1.example.com"  -> FQDN with hyphens.
env() and cluster_name() both split on '-', confirming the hyphen+FQDN form is the expected shape.

=> application_name (underscores, short) can never equal node_name (hyphens, FQDN) under EITHER
   possible inventory form. The mismatch does not depend on the unanswered short-vs-FQDN question.

# Local checks
cargo test --all: 194 passed, 0 failed.
timeline_history.rs HAS tests incl. fork_lsn_returns_switch_point (parser exists and works; it is
unused by production code, which is the source of the two known dead-code warnings).
