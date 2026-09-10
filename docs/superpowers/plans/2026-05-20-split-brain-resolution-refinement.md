# Split-Brain Resolution Refinement Implementation Plan

> **Format:** One commit per checkbox. Steps use checkbox (`- [ ]`) syntax for tracking. User implementing manually — code snippets show key changes, not full files. Test code shown where it adds clarity.

**Goal:** Implement ADR-002 — refine the split-brain resolver with a flushing-liveness gate, structured findings, sanity gates with `Refuse` semantics, divergent-WAL detection, and an operator-actionable short-string contract.

**Architecture:** Twelve commits, bottom-up. Foundation (data collection + types) → resolver core (gate + sanity gates + findings) → divergent-WAL → writer integration. The resolver gains new fields and a richer findings vec but the existing variant set extends rather than breaks (one rename, two new fields).

**Tech Stack:** Rust, tokio-postgres, serde, chrono. Existing test infrastructure: `NodeBuilder` / `PrimaryHealthBuilder` / `ReplicaHealthBuilder` in `src/v2.rs` (the test-helpers module, gated `#[cfg(test)]`). Use `pretty_assertions::assert_eq` throughout.

**Spec:** `docs/adr/002-split-brain-resolution-refinement.md`. Open this side-by-side while implementing.

**Conventions in this codebase:**
- Tests live alongside code (`#[cfg(test)] mod tests` at the bottom of each module).
- Errors via `anyhow` + `anyhow::Context`; structured kinds via `errors::extract_kind`.
- Tracing macros (`tracing::info!`, `tracing::debug!`, etc.) on every IO boundary.
- Clippy lints are kept clean on touched lines (per the project's `feedback_fix_clippy` memory).

---

## Commit 11: Capture replica control-file position (DivergentReplicaWal detection deferred)

> **Revised 2026-06-07 (see ADR-002 §7).** The original Commit 11 ("detect `DivergentReplicaWal` via `wal_receiver`, emit → Refuse, render rebuild-from-true-primary") is **dropped** for now. Review found it was anchored to the wrong primary, stated the remediation backwards, and — critically — keyed off `pg_stat_wal_receiver`, which the *dangerous* case (a timeline-wedged replica, matrix C-g) likely doesn't expose at all. Data-loss danger and `wal_receiver`-based detectability are anti-correlated, and we have no captured run of the wedged state. So this commit becomes **capture-only**: collect control-file evidence so the next real C-g is diagnosable, and design the detection + verdict-flip from that data later. The deferred detection design is preserved at the end of this section.

**Goal:** Add the replica's **absolute applied/received LSN** to the replica health check. The control-file timeline is *already* captured (`timeline_id` ← `pg_control_checkpoint()`); the gap is that the only LSN kept today is the receive−replay *difference* (`lag.apply_lag_bytes`), not the positions. No detection, no finding emission, no confidence change.

**Files:**
- Modify: `src/v2/scan/health_check_replica.rs` — `HEALTH_CHECK_REPLICA_QUERY` and `ReplicaHealthCheckResult`.
- Modify: `src/v2.rs` test helpers (`ReplicaHealthBuilder`) — expose the new fields for later test use.

- [ ] **Add the absolute LSNs to `HEALTH_CHECK_REPLICA_QUERY`.** The query already selects `timeline_id` from `pg_control_checkpoint()` and feeds `pg_last_wal_receive_lsn()`/`pg_last_wal_replay_lsn()` into the `apply_lag_bytes` diff — but discards the positions. Surface them as text:

```sql
'last_wal_replay_lsn', pg_last_wal_replay_lsn()::text,   -- applied position; present without a receiver
'last_wal_receive_lsn', pg_last_wal_receive_lsn()::text, -- received/flushed (ack-relevant); may be 0/0 with no receiver
```

- [ ] **Add the fields to `ReplicaHealthCheckResult`** (top-level, *not* inside `WalReceiverInfo` — these must survive when `wal_receiver` is `None`):

```rust
pub last_wal_replay_lsn: Option<String>,
pub last_wal_receive_lsn: Option<String>,
```

  `Option` because both functions return SQL NULL -- never `0/0` -- when their position is zero (PG17 `xlogfuncs.c`: `if (recptr == 0) PG_RETURN_NULL();`, validated 2026-09-10). Keep them nullable rather than inventing a sentinel. In practice NULL is near-unobservable on this fleet; the real trap is a non-NULL *stale* value, so read ADR-002 §7 before comparing these positions to anything.

- [ ] **Expose them on `ReplicaHealthBuilder`** (`src/v2.rs`) so later detection tests can construct the wedged state. No production wiring beyond the struct — nothing reads these yet.

That is the whole commit: capture only. No `lsn_to_u64`, no detection block, no `DivergentReplicaWal` emission. Those land in a follow-up once we have a real C-g capture to design against (see *Deferred* below).

- [ ] **Test:** a deserialization test confirming the two new LSN fields round-trip from the query JSON (present, and explicit `null` -> `None`; there is no `0/0` case). The fields carry no `#[serde(default)]`, so an *absent* key is a hard error rather than `None` -- acceptable, because `jsonb_build_object` always emits the key. No resolver/analyze test — nothing consumes them yet.

- [ ] **Commit message:**

```
feat(scan): capture replica applied/received LSN for divergence diagnosis

ADR-002 §7 (revised). The dangerous split-brain case — a timeline-wedged
replica holding acked writes on the lower TL — is exactly the state where
pg_stat_wal_receiver is likely empty, so received_tli/flushed_lsn are
unavailable. We already record the control-file timeline; add the absolute
pg_last_wal_replay_lsn()/pg_last_wal_receive_lsn() (today only their diff is
kept, as apply_lag_bytes) so a real occurrence is diagnosable. Capture only:
no detection or verdict change yet — see the deferred design in ADR-002 §7.
```

### Deferred: DivergentReplicaWal detection + verdict-flip (design notes)

Not implemented; recorded so the eventual follow-up starts from the corrected design, not the original (wrong) one. Build only after a real C-g capture confirms what a wedged replica actually exposes.

- **Anchor detection to the inter-primary fork, independent of the default verdict.** Read the switch-LSN X from the higher-TL primary's `.history` (objective: where TL=N+1 split from TL=N — using its history is fine; it is *not* the same as treating that primary as canonical). A replica on the *lower* timeline (control-TLI = N) with applied LSN > X is proof of acked writes on the lower TL. Do **not** anchor to whatever the resolver currently calls `true_primary`: by default that's the higher-TL primary (the loser), and "divergence relative to it" would suppress this very signal — and even after the flip, db003 sits *on* the lower-TL true primary's lineage, so a true-primary anchor would never fire.
- **Source the replica position from the control-file LSN** (this commit's new fields), not `wr.flushed_lsn` — the wedged replica may have no `wal_receiver`.
- **It's a verdict-flip, not just a finding.** With no live follower at scan time the resolver falls to `HigherTimeline` and picks the higher TL; the acked-write evidence must drive the verdict to lower-TL-canonical (keep lower TL, rebuild the higher-TL node), or force `Refuse` when db003's allegiance is unprovable. The divergent node to *rebuild* is then the higher-TL primary (off the canonical lineage), not db003 (which re-points to the lower-TL primary).
- **3-node proof of safety:** db002's only candidate acker is db003; if db003 is observably on TL=N (control timeline + applied LSN past the fork), db002 provably acked nothing on its fork → the lower-TL pick is *confident*, not merely conservative.
- The old `lsn_to_u64` helper and the per-replica fork comparison remain a fine starting point mechanically; only the *anchor*, *source LSN*, and *verdict effect* change.

---

## Commit 12: Writer integration — short-string contract

**Goal:** Implement the `format_reason` SplitBrain arm per ADR-002 §4. Use the action-text mapping table; override with `REFUSE/` text when `Confidence::Refuse`. **(Revised 2026-06-07: the `DivergentReplicaWal` concatenation is dropped — that finding is deferred per §7, so the writer has nothing to render for it yet.)**

**Files:**
- Modify: `src/v2/writer/build.rs:528-564` — the `Reason::SplitBrain` arm of `format_reason`
- Possibly: `src/v2/writer/view.rs` — if `ReasonView` needs new fields (likely not; should fit in `short`)

- [x] **Replace the existing match arm.** Sketch (adapt to the actual surrounding signatures):

```rust
Reason::SplitBrain(info) => {
    let short = if matches!(info.confidence, Confidence::Refuse) {
        format_refuse(info)
    } else {
        format_resolution(info)
    };

    // DivergentReplicaWal concatenation deferred (§7) — nothing emits that finding yet.

    ReasonView { short, details_json: serde_json::to_string(info).unwrap_or_default() }
}
```

- [x] **Helper: `format_refuse`** picks the first sanity-gate finding:

```rust
fn format_refuse(info: &SplitBrainInfo) -> String {
    let gate = info.findings.iter().find_map(|f| match f {
        SplitBrainFinding::SystemIdentifierMismatch { nodes } => {
            Some(format!("system_identifier mismatch ({})", nodes.join(", ")))
        }
        SplitBrainFinding::SynchronousCommitWeakened { primary, value } => {
            Some(format!("synchronous_commit={} on {}", value, primary))
        }
        _ => None,
    }).unwrap_or_else(|| "sanity gate failed".to_owned());
    format!("REFUSE/SplitBrain: {}", gate)
}
```

- [x] **Helper: `format_resolution`** — per the §4 mapping table:

```rust
fn format_resolution(info: &SplitBrainInfo) -> String {
    let stale = info.stale_primaries.first().cloned().unwrap_or_default();
    let quorum_blocked = info.findings.iter().any(|f| matches!(
        f, SplitBrainFinding::PrimaryQuorumUnsatisfied { primary, .. } if primary == &stale
    ));

    match &info.resolution {
        SplitBrainResolution::Both { true_primary_timeline, stale_timeline, .. } => format!(
            "SplitBrain: {} has quorum (TL={}), demote {} (TL={}, quorum unsatisfied)",
            info.true_primary, true_primary_timeline, stale, stale_timeline
        ),
        SplitBrainResolution::LowerTimelineHasQuorum {
            true_primary_timeline, stale_timeline, ..
        } => format!(
            "SplitBrain: {} has quorum (lower TL={}), fence {} (TL={}, quorum-blocked)",
            info.true_primary, true_primary_timeline, stale, stale_timeline
        ),
        SplitBrainResolution::HigherTimeline { true_primary_timeline, stale_timeline } => format!(
            "SplitBrain: {} has quorum (TL={}), demote {} (TL={}, no live replicas)",
            info.true_primary, true_primary_timeline, stale, stale_timeline
        ),
        SplitBrainResolution::ReplicaFollowing { .. } => format!(
            "SplitBrain: {} has quorum, demote {} (same TL)",
            info.true_primary, stale
        ),
        SplitBrainResolution::Indeterminate => {
            "SplitBrain: cannot determine true primary (insufficient evidence)".to_owned()
        }
    }
}
```

- [ ] ~~**Helper: `find_divergent_replica_wal`**~~ — deferred (§7); no `DivergentReplicaWal` rendering in this commit.

- [x] **Tests** in `writer/build.rs`:

```rust
#[test]
fn lower_tl_short_string_names_action() {
    let info = SplitBrainInfo {
        true_primary: "db001".to_owned(),
        stale_primaries: vec!["db002".to_owned()],
        resolution: SplitBrainResolution::LowerTimelineHasQuorum {
            true_primary_timeline: 11,
            stale_timeline: 12,
            replicas_following_true: vec!["db003".to_owned()],
        },
        confidence: Confidence::BestEffort,
        findings: vec![
            SplitBrainFinding::PrimaryQuorumUnsatisfied {
                primary: "db002".to_owned(), required: 1, observed: 0,
            },
        ],
    };
    let view = format_reason(&Reason::SplitBrain(info));
    assert!(view.short.contains("fence db002"));
    assert!(view.short.contains("lower TL=11"));
}

// divergent_wal_appends_rebuild_instruction: deferred (§7) — no DivergentReplicaWal rendering yet.

#[test]
fn refuse_overrides_resolution_text() {
    let info = SplitBrainInfo {
        true_primary: "db001".to_owned(),
        stale_primaries: vec!["db002".to_owned()],
        resolution: SplitBrainResolution::HigherTimeline {
            true_primary_timeline: 12, stale_timeline: 11,
        },
        confidence: Confidence::Refuse,
        findings: vec![SplitBrainFinding::SystemIdentifierMismatch {
            nodes: vec!["db003".to_owned()],
        }],
    };
    let view = format_reason(&Reason::SplitBrain(info));
    assert!(view.short.starts_with("REFUSE/"));
    assert!(view.short.contains("system_identifier mismatch"));
    // The resolution text must NOT appear:
    assert!(!view.short.contains("has quorum"));
}

// refuse_with_divergent_wal_still_appends_rebuild: deferred (§7) along with the carve-out.
```

- [ ] **Commit message:**

```
feat(writer): split-brain short-string contract per ADR-002 §4

The format_reason SplitBrain arm now derives action-text via the
variant-to-template mapping, inlines PrimaryQuorumUnsatisfied, appends
DivergentReplicaWal as a rebuild instruction, and overrides the
resolution variant text with REFUSE/<gate> when Confidence::Refuse.

This closes the operator-handling gap that motivated the ADR: in the
LowerTimelineHasQuorum case the short string now names the action
(fence the quorum-blocked primary) rather than the mechanism, and the
DivergentReplicaWal finding is surfaced inline with rebuild guidance.
```

---

## Post-implementation

- [x] Run `cargo clippy --all-targets` and fix lints on touched lines (per the `feedback_fix_clippy` memory).
- [ ] Decide whether the top-level `DivergentReplicaWal` finding emission (outside the SplitBrain reason) needs to land in this PR or a follow-up — the writer correctness is already satisfied by the dual-emission inside `SplitBrainInfo.findings`.

---

## Out of scope (follow-up work)

- Two-pass stability check (`Verified` confidence) — ADR-002 §6.
- repmgr metadata integration — ADR-002 out-of-scope.
- Hostname-form `primary_conninfo` support — ADR-002 out-of-scope.
- Visual/layout polish for the report.

These are deliberately left for separate work per the ADR.
