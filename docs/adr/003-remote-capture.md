# ADR-003: Remote Capture of Pipeline State

## Status

Proposed (2026-05-26).

## Context

db-scan runs against the production postgres fleet and produces per-cluster verdicts (split-brain classification, replica health, etc.). The analyzer's correctness is hard to test in the abstract: real production states -- slow fences, stale `wal_receiver` rows, divergent replicas mid-rebuild -- are the inputs we most want test coverage for, and they are exactly the inputs we cannot synthesize confidently from documentation.

We want to optionally persist the full pipeline state of each db-scan run to an internal postgres so production cluster scenarios accumulate as a corpus. The corpus is the value; any individual capture is not.

### Non-goals

- **Guaranteed delivery.** Captures may be lost when the destination is unreachable or when a one-shot run exits before the upload completes. The corpus accumulates across runs.
- **Sanitization.** Source and destination are owned by the same org. The trust boundary lives at fixture extraction, not at capture.
- **Schema management.** db-scan does not create or migrate the destination table.
- **Transport flexibility.** Postgres only. No HTTP, S3, or pluggable sinks.

## Decision

### 1. Pipeline shape

A new stage, `capture`, sits between `analyze` and `write`:

```
DatabasePortal → Scan → Cluster → Analyze → Capture → Write
```

`capture` is a passthrough with a side effect:

1. Forward each `ClusterHealth` downstream unchanged.
2. Buffer each one as it passes through.
3. When the upstream channel closes, flush the buffer to the capture DB.
4. Return, dropping the downstream sender (and thereby signalling Write to finish).

When capture is disabled (no `capture_client` in `PipelineContext`), the stage degrades to pure forwarding: no buffer, no flush. The stage is always present in the pipeline; behavior is gated on `Option<Arc<Client>>` in the context.

### 2. Granularity: one row per `ClusterHealth`, run_id shared across rows

A single db-scan run produces N `ClusterHealth` values (one per cluster in the fleet). The capture table stores **one row per cluster, all sharing a single `run_id`** generated server-side. This means:

- The corpus is queryable per-cluster without unpacking JSONB arrays.
- A run is reconstructible by grouping on `run_id`.

The `hostname` column carries the **cluster name**, not the machine running db-scan. We care which cluster a row describes, not which operator ran the scan.

### 3. Flush mechanics

The capture client is an `Arc<tokio_postgres::Client>` connected once at startup and held in `PipelineContext` for the process lifetime.

The flush is **inline-awaited** with a 5 s timeout:

```rust
let _ = tokio::time::timeout(
    Duration::from_secs(5),
    flush_to_db(buf, Arc::clone(client))
).await;
```

**Why inline.** `Write` (stdout) finishes in milliseconds. If the flush were spawned and `capture` returned immediately, the pipeline would close out and the tokio runtime would cancel the in-flight insert on process exit -- giving the flush effectively zero time. Holding `analyzed_tx` alive through the flush keeps `Write`'s channel open and keeps the runtime alive for up to 5 s, which is enough headroom for a healthy capture DB to acknowledge the insert.

**Why 5 s and not 10 s.** Watch mode runs on a tight cadence; 5 s is a balance between giving the DB time to accept the write and not stalling the next cycle. If the capture DB is healthy, inserts complete in tens of milliseconds and the timeout never fires. If unhealthy, 5 s is long enough to confirm pathology and short enough to not be operationally costly.

**Failure mode.** On timeout or insert error, log at `error!` level and continue. The diagnostic -- db-scan's primary job -- is unaffected.

### 5. Schema (internally-owned)

```sql
CREATE TABLE db_scan.db_scan_captures (
    id              BIGSERIAL    NOT NULL,
    run_id          UUID         NOT NULL,
    captured_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    binary_version  TEXT         NOT NULL,
    binary_git_sha  TEXT,
    hostname        TEXT         NOT NULL,
    blob            JSONB        NOT NULL,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT db_scan_captures_pkey PRIMARY KEY (id)
);
```

db-scan does not own or migrate this schema. The DDL above is the operator contract.

### 6. Config & CLI

Capture is **off by default**. Enabled per-deployment by providing a `capture:` block in the config:

```yaml
capture:
  enabled: true
  postgres:
    host: capture.example.com
    port: 5432
    dbname: db_scan_captures
    user: capture_writer
    # certs and password inherited from top-level postgres: block
```

When the block is absent or `enabled: false`, the capture client is `None` in `PipelineContext` and the stage forwards without buffering.

`--no-capture` overrides config-enabled capture for a single run.

### 7. Serialization

`ClusterHealth` and its full transitive type graph (`Cluster`, `AnalyzedNode`, `Verdict`, `ClusterVerdict`, `NodeVerdict`, `Reason`, `SplitBrainInfo`, `Confidence`, leaf types) derive `Serialize`. The blob is `serde_json::Value` produced via `json!(c)`, sent as `JSONB`.

No envelope struct (`Capture { metadata, analyzed: Vec<...> }`) -- the metadata that matters per-cluster (cluster name, scan time, binary version) is either a top-level column or implicit in the row.

### 8. Watch mode

The capture client is owned by `PipelineContext` and lives for the process lifetime. Each pipeline cycle gets a fresh `Arc` clone. There is no reconnect-on-error in v1; if the capture DB restarts mid-watch, subsequent inserts fail and are logged. Acceptable for v1 -- db-scan processes are typically short-lived enough that this is rare.

## Out of scope

- **Retry / on-disk queue.** Dropped during design. The corpus tolerates loss; an on-disk queue adds operational surface area (disk pressure, schema migration, cleanup) for a corpus that doesn't need it.
- **Reconnect-on-error.** If the v1 single-`Client` pattern proves fragile under long watch runs, revisit.
- **Sanitization.** Sanitize at fixture extraction if/when fixtures are committed publicly.
- **Multi-sink transport.** Postgres only.
- **Capturing `args`.** Risk of `--pgpassword <secret>` leakage. Flag-relevant context can be inferred from the data shape (e.g., presence of disk-check data).
- **Per-run aggregate metadata row.** A `runs` table summarizing each `run_id` (start time, host, binary version, count) is a natural follow-up but not v1.

## Consequences

- The capture stage is always present in the pipeline; correctness of the disabled-capture path depends on the `Option<Arc<Client>>` check at the call site, not on conditional pipeline construction.
- The flush is inline-awaited, so a slow capture DB delays the *end* of a pipeline cycle by up to 5 s. In watch mode this delays the next cycle's start. Accepted trade-off -- see §3.
- Adding `Serialize` to the analyzer type tree is a wide but mechanical diff. New types added to the tree must remember to derive it, or capture will fail to compile -- this is the desired failure mode (loud, at build time).
- `binary_git_sha` is in the schema but unwired. A future change to `build.rs` is the natural completion point.
- The capture DB is a soft dependency: failure to connect at startup logs a warning and disables capture for the run; failure mid-run logs and continues. db-scan's diagnostic output is never gated on capture health.
