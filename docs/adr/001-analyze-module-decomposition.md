# ADR-001: Analyze Module Decomposition

## Status

Accepted (2026-04-29)

## Context

`src/v2/analyze/mod.rs` is 2583 lines with 40+ helper functions. It handles:
- Split-brain resolution (timeline analysis, replica evidence)
- Health classification by topology (full redundancy, one replica down, no replicas)
- Individual node checks (archive failure, sync commit, lag, disk errors)
- Verdict-to-reason mapping

Problems:
1. **Poor locality**: Split-brain logic (~180 lines) is buried in the monolith. Understanding or testing it requires navigating 2500 lines.
2. **Primitive obsession**: Health checks return raw `Option<(i64, Option<String>)>` and `bool`. No intermediate types capture decisions.
3. **Duplicated domain logic**: `writer/build.rs` re-implements checks like `!node_name.contains("-db001")` for failover detection because it can't access verdicts.

## Decision

### 1. Extract Split-Brain Resolution Module

**Extract to:** `src/v2/analyze/split_brain.rs`

**What moves:**
- `resolve_split_brain(primaries: &[&AnalyzedNode], replicas: &[&AnalyzedNode]) -> SplitBrainInfo`
- Helper functions: `extract_timeline_info`, `build_replica_following_map`, `determine_true_primary`, `resolve_with_different_timelines`, `resolve_with_equal_timelines`
- Types: `TimelineInfo`, `SplitBrainInfo`, `SplitBrainResolution`

**Interface:** Keep current signature. The module accepts `&[&AnalyzedNode]` — no narrowing. This preserves flexibility if resolution logic needs more node data in the future.

**Re-exports:** `analyze/mod.rs` re-exports `SplitBrainInfo` and `SplitBrainResolution` for downstream consumers.

### 2. Introduce Health Verdict Types

**New types in analyze module:**

```rust
enum NodeVerdict {
    ArchiveFailure { failed_count: i64, last_wal: Option<String> },
    SyncCommitOff,
    IsFailoverNode,
    HighLag { bytes: u64 },
    DiskIoErrors { io: u32, block: u32 },
    FilesystemErrors { count: u32 },
}

enum ClusterVerdict {
    SplitBrain(SplitBrainInfo),
    WritesBlocked,
    WritesUnprotected,
    NoPrimary,
    ChainedReplication { replica: String, upstream: String },
}

struct Verdict {
    node_verdicts: Vec<(String, NodeVerdict)>,
    cluster_verdict: Option<ClusterVerdict>,
}
```

**Location:** `Verdict` lives on `AnalyzedCluster` so it flows through the pipeline and is accessible to writer.

**Flow:**
1. `analyze()` runs checks → collects `Verdict`
2. `Verdict::to_health()` maps verdicts to `ClusterHealth` with appropriate `Reason`
3. Writer accesses `cluster.verdict` for display details instead of re-implementing domain logic

### 3. Defer Topology Analyzer Extraction

Topology-specific functions (`analyze_full_redundancy`, `analyze_one_replica_down`, `analyze_no_replicas`) are 20-70 lines each. Once they become "collect verdicts" functions after #2, further extraction may be unnecessary.

**Revisit after Verdict types are implemented.** If `mod.rs` remains hard to navigate, extract topology analyzers to separate files.

## Consequences

**Positive:**
- Split-brain logic has locality — one file to understand, one test module
- Verdicts eliminate primitive obsession and duplicated domain logic
- Writer becomes a pure view transformation (pattern-match on verdicts)
- Detection (what conditions exist) is separated from classification (severity)

**Negative:**
- More types to maintain (`NodeVerdict`, `ClusterVerdict`, `Verdict`)
- `AnalyzedCluster` grows (gains `verdict` field)

**Neutral:**
- No interface change at `resolve_split_brain` call sites
- `Reason` enum stays as-is; `Verdict::to_health()` maps to it

## Implementation Order

1. Extract split-brain module (isolated change, no downstream impact)
2. Introduce Verdict types (larger structural change, benefits from cleaner analyze.rs)
3. Revisit topology analyzer extraction if needed
