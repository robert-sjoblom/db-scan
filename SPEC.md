# db-scan Specification

**Version:** 2.0
**Status:** Draft
**Last Updated:** 2026-01-20

## Executive Summary

db-scan is a PostgreSQL cluster health monitoring tool designed for Fortnox's infrastructure. It performs concurrent health checks across PostgreSQL clusters, classifies their health states, and provides actionable diagnostics for operators. The tool supports three operating modes: single-scan CLI, watch mode with terminal UI, and long-running service mode with HTTP API.

---

## 1. Architecture Overview

### 1.1 Pipeline Model

**CLI Mode (single scan):**
```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌──────────────┐   ┌──────────┐   ┌────────┐
│ Portal      │──▶│ Prometheus  │──▶│ Node        │──▶│ Cluster      │──▶│ Analysis │──▶│ Output │
│ Discovery   │   │ Thresholds  │   │ Scanner     │   │ Aggregation  │   │          │   │        │
└─────────────┘   └─────────────┘   └─────────────┘   └──────────────┘   └──────────┘   └────────┘
```

**Service Mode (continuous):**
```
                                    ┌─────────────────────────────────────────────────────┐
                                    │                   Scan Loop                         │
┌──────────────────┐                │  ┌─────────┐   ┌─────────┐   ┌──────────┐   ┌────┐  │
│ Data Source      │───────────────▶│  │ Scanner │──▶│ Cluster │──▶│ Analysis │──▶│Out │  │
│ Registry         │  nodes +       │  └─────────┘   └─────────┘   └──────────┘   └────┘  │
│                  │  thresholds    └─────────────────────────────────────────────────────┘
│ ┌──────────────┐ │                                      │
│ │ Portal       │ │                                      │
│ │ (background) │ │                                      ▼
│ └──────────────┘ │                              ┌───────────────┐
│ ┌──────────────┐ │                              │ State Tracker │
│ │ Prometheus   │ │                              │ (durations,   │
│ │ (background) │ │                              │  transitions) │
│ └──────────────┘ │                              └───────────────┘
└──────────────────┘                                      │
        ▲                                                 │
        │                         ┌───────────────────────┼───────────────────────┐
        │                         │                       ▼                       │
   ┌────┴────┐              ┌─────┴─────┐          ┌──────────────┐        ┌──────┴──────┐
   │ HTTP    │◀─────────────│ /scan     │          │ /metrics     │        │ /health     │
   │ Server  │  on-demand   │ (trigger) │          │ (prometheus) │        │ (liveness)  │
   └─────────┘              └───────────┘          └──────────────┘        └─────────────┘
```

### 1.2 Core Design Principles

1. **Graceful Degradation**: Partial visibility is better than total failure. One unreachable node, failed data source, or cluster scan does not block monitoring of others.

2. **Stateful Service, Stateless Scans**: The service maintains state (data source health, cluster state durations), but each scan cycle is independent. State is reconstructable from a fresh start.

3. **Availability Over Accuracy**: When data sources are degraded, prefer serving potentially stale data with explicit freshness warnings over failing entirely.

4. **Observable by Default**: Service mode exposes metrics, health endpoints, and structured logs suitable for production monitoring.

---

## 2. Data Source Management

### 2.1 Data Source Registry

The service maintains a registry of external data sources, each with independent health tracking:

```rust
struct DataSourceHealth {
    last_success: Option<Instant>,
    last_attempt: Instant,
    consecutive_failures: u32,
    last_error: Option<String>,
    state: DataSourceState,
}

enum DataSourceState {
    Healthy,                    // Recent successful fetch
    Degraded { since: Instant }, // Failures but have cached data
    Unavailable,                // No cached data available
}
```

### 2.2 Database Portal

**Purpose**: Node inventory (IP, cluster_id, node_name, pg_version)

**Source**: REST API at configured endpoint (default: `https://database.example.com/api/v1/nodes`)

**Fetch Strategy**:
- **Service mode**: Background task fetches every 5 minutes (configurable)
- **CLI mode**: Fetch once at startup, cache to file for subsequent runs within 24h
- **File cache**: `/tmp/nodes_response.json`

**Failure Handling**:
- On failure, retain last-known-good data
- Mark source as `Degraded`
- Include staleness warning in scan output
- After 1 hour of failures, escalate logging severity

**Data Contract**:
```rust
struct PortalData {
    nodes: Vec<Node>,
    fetched_at: Instant,
    freshness: DataFreshness,
}

enum DataFreshness {
    Fresh,                      // Fetched within expected interval
    Stale { age: Duration },    // Older than expected but usable
    Unknown,                    // No successful fetch yet (startup)
}
```

### 2.3 Prometheus

**Purpose**:
- Dynamic lag thresholds (WAL generation rate history)
- Filesystem metrics for backup progress

**Fetch Strategy**:
- **Service mode**: Background task fetches every 60 seconds
- **CLI mode**: Fetch once at startup

**Failure Handling**:
- On failure, retain last-known thresholds
- Fall back to static defaults if no cached data (80MB based on 16MB/s × 5s)
- Mark source as `Degraded`
- Continue scans with reduced accuracy

**Data Contract**:
```rust
struct PrometheusData {
    lag_thresholds: HashMap<ClusterId, LagThreshold>,
    filesystem_metrics: HashMap<ClusterId, FileSystemMetrics>,
    fetched_at: Instant,
    freshness: DataFreshness,
}

struct LagThreshold {
    value_bytes: u64,
    source: ThresholdSource,
}

enum ThresholdSource {
    Calculated { p95_rate: f64, tolerance_secs: u64 },
    Static { reason: &'static str },
}
```

### 2.4 Freshness Propagation

Each scan receives a `ScanContext` that includes freshness metadata:

```rust
struct ScanContext {
    nodes: Vec<Node>,
    portal_freshness: DataFreshness,

    lag_thresholds: HashMap<ClusterId, LagThreshold>,
    prometheus_freshness: DataFreshness,

    scan_requested_at: Instant,
}
```

Analysis and output stages use this to:
- Annotate results with data source warnings
- Adjust confidence in health classifications
- Include freshness in metrics labels

---

## 3. State Tracking

### 3.1 Cluster State History

The service tracks state transitions for each cluster:

```rust
struct ClusterStateHistory {
    current_state: ClusterHealth,
    current_state_since: Instant,
    previous_state: Option<ClusterHealth>,
    transition_count_24h: u32,
}
```

**Use Cases**:
- "This cluster has been Degraded for 10 minutes"
- "This cluster has flapped 5 times in the last hour"
- State duration exposed as Prometheus metric

### 3.2 State Persistence

**In-memory only**: State is not persisted to disk. On restart:
- All clusters start with unknown duration
- First scan establishes baseline
- Metrics show `state_duration_seconds` as time since service start until second scan

**Rationale**: Simplicity. External systems (Prometheus, logs) provide historical data if needed.

### 3.3 Transition Events

State changes emit structured events:

```rust
struct StateTransition {
    cluster_id: String,
    from: ClusterHealth,
    to: ClusterHealth,
    timestamp: Instant,
    duration_in_previous_state: Duration,
}
```

Events are:
- Logged at INFO level
- Exposed via `/events` SSE endpoint (optional)
- Counted in Prometheus metrics

---

## 4. Scanning

### 4.1 Connection Strategy

- **Concurrency**: Async tasks per node via tokio
- **mTLS**: Required for production and specific dev clusters (`dev-pg-app006`, `dev-pg-app010`, `dev-pg-app011`)
- **Certificate Hot-Reload**: Detect certificate file changes and reload without restart during long-running sessions

### 4.2 Retry Logic

- **Attempts**: 3 total (initial + 2 retries)
- **Delay**: 500ms between retries
- **Failure Handling**: Mark node as `Unknown` after all attempts exhausted; continue scanning remaining nodes

### 4.3 Adaptive Rate Limiting

- **Initial Behavior**: Start with unlimited concurrency
- **Back-off Trigger**: Increase in connection failure rate
- **Back-off Strategy**: Reduce concurrent connections progressively
- **Recovery**: Gradual ramp-up; double concurrency every successful interval until restored to maximum; reset to reduced state on new failures

### 4.4 Role Detection

- **Method**: `SELECT pg_is_in_recovery()`
- **Primary**: Not in recovery → execute primary health check
- **Replica**: In recovery → execute replica health check

### 4.5 Health Check Queries

#### Primary Health Check Returns:
- Timeline ID
- Uptime
- Current WAL LSN
- Configuration (synchronous_commit, synchronous_standby_names, wal_level)
- pg_stat_replication data for all replica connections

#### Replica Health Check Returns:
- WAL receiver status and state
- Replication lag (bytes, calculated from LSN difference)
- WAL receiver timeline (which primary it follows)
- Recovery conflicts by database
- Configuration (hot_standby, primary_conninfo)

### 4.6 Inferring Unreachable Node State

When a node is unreachable but the primary is accessible:

- **Query primary's pg_stat_replication** to determine replica state
- **Distinguish failure modes**:
  - "Unreachable but streaming per primary" → likely network issue from db-scan's perspective
  - "Unreachable and not in pg_stat_replication" → actual replica failure
- **Display both observations**: Show db-scan's direct result alongside primary's reported state

---

## 5. Cluster Aggregation

### 5.1 Topology Profiles

Clusters are assigned to topology profiles that define expected characteristics:

- **Profile Definition**: Specified in YAML topology configuration
- **Profile Assignment**: Clusters tagged with profile via database portal metadata
- **Example Profiles**:
  - `standard-3node`: 1 primary + 2 replicas (default Fortnox configuration)
  - `reporting-2node`: 1 primary + 1 replica
  - `legacy-5node`: Extended replication for legacy applications

### 5.2 Unexpected Topology Handling

- **Do not silently skip**: Clusters with node counts not matching their profile
- **Dedicated output section**: List all topology anomalies with:
  - Cluster name
  - Expected node count (from profile)
  - Actual node count
  - List of nodes found

### 5.3 Configuration Validation

- **synchronous_standby_names validation**: Verify configuration matches actual connected replicas
- **Pattern support**: Configurable patterns in topology config to support:
  - Explicit names (db002, db003)
  - Patroni-style dynamic names
  - Hostname-based names
- **Mismatch handling**: Flag as warning requiring investigation

---

## 6. Health Classification

### 6.1 Health States

```rust
enum ClusterHealth {
    Healthy { failover: bool, cluster: AnalyzedCluster },
    Degraded { reasons: Vec<Reason>, cluster: AnalyzedCluster },
    Critical { reasons: Vec<Reason>, cluster: AnalyzedCluster },
    Unknown { cluster: AnalyzedCluster, reachable_nodes: usize },
}
```

### 6.2 Classification Logic

#### Healthy
- Exactly 1 primary matching expected role
- Expected number of replicas (per topology profile) all streaming
- Replication lag below dynamic threshold
- No rebuilding replicas
- No chained replication
- `failover: true` if primary is not the canonical primary (db001)

#### Degraded Reasons
- `OneReplicaDown`: Fewer replicas than expected but at least one streaming
- `HighReplicationLag`: Lag exceeds dynamic threshold
- `RebuildingReplica`: Replica has no WAL receiver (actively rebuilding)
- `ChainedReplica`: Replica streaming from another replica instead of primary
- `PostgresVersionMismatch`: Nodes report different major PostgreSQL versions

#### Critical Reasons
- `NoPrimary`: Zero primaries found
- `WritesBlocked`: Primary with synchronous_commit=on but no replicas
- `WritesUnprotected`: Primary with synchronous_commit=off and no replicas
- `SplitBrain`: Multiple primaries detected
- `UnexpectedReplicaSource`: Primary reports replication connections from unknown sources

#### Unknown
- `NoNodesReachable`: Cannot connect to any nodes
- `UnexpectedTopology`: Node count doesn't match profile expectation

### 6.3 Compound Severity Escalation

When multiple degraded conditions exist simultaneously:

- **Two or more Degraded reasons** → Escalate to **Critical**
- **Rationale**: Compound failures represent higher risk than single issues
- **Example**: OneReplicaDown + HighReplicationLag on remaining replica = Critical

### 6.4 Dynamic Lag Threshold

**Data Source**: Prometheus integration (pg_stat_replication metrics history)

**Calculation**:
- Query last 7 days of WAL generation data per cluster
- Calculate 95th percentile of WAL bytes/second
- Threshold = 95th percentile rate × configured lag tolerance seconds (default: 5s)

**Fallback**: When Prometheus unavailable, use static threshold (80MB based on 16MB/s × 5s)

**Caching**: 30-60 second TTL on Prometheus queries to avoid excessive load during watch mode

### 6.5 Lag Diagnostic Heuristics

When lag exceeds threshold, provide breakdown:

- **send_lag**: Network transfer delay (network bottleneck indicator)
- **flush_lag**: Disk write delay (I/O bottleneck indicator)
- **replay_lag**: Transaction application delay (CPU/lock contention indicator)

**Display Logic**:
- Below threshold: Single lag number
- Above threshold: Automatically expand to show component breakdown

---

## 7. Split-Brain Analysis

### 7.1 Detection

Multiple nodes return `pg_is_in_recovery() = false`

### 7.2 Resolution Strategies

1. **HigherTimeline**: Different timeline IDs → higher timeline is true primary
2. **ReplicaFollowing**: Equal timelines → determine which primary replicas are streaming from
3. **Both**: Timeline and replica evidence agree (high confidence)
4. **ReplicaOverridesTimeline**: Replicas follow lower-timeline primary
5. **Indeterminate**: Cannot determine (equal timelines, no replica evidence)

### 7.3 Timeline Relationship Analysis

When replica follows lower timeline on same timeline:
- Higher timeline primary can be safely shut down and marked for rebuild
- Tool should recommend this action

When replica follows lower timeline with higher timeline itself:
- Manual decision required
- Tool should present both options with implications

### 7.4 Recommended Actions with Approval

- **Default (interactive)**: Display recommended action, prompt operator for confirmation
- **Script mode (--generate-script)**: Write remediation commands to script file for review and manual execution
- **Never auto-execute** without explicit operator approval

---

## 8. Plugin System

### 8.1 Architecture

- **Communication**: External processes via stdin/stdout JSON
- **Discovery**: Plugins specified in configuration file
- **Language Agnostic**: Plugins can be written in any language

### 8.2 Data Contract

- **Input**: Full `ClusterHealth` struct as JSON (maximum flexibility)
- **Output**: JSON response containing:
  - Optional health classification override with justification
  - Optional suggestions/annotations
  - Optional recommended actions

### 8.3 Override Capability

Plugins can reclassify health states:
- Must provide justification string
- Original classification preserved alongside override
- Use case: Maintenance windows, known acceptable states

### 8.4 Execution Model

- **Timeout**: None (trust plugins to behave)
- **Failure Handling**: Operator responsibility to fix misbehaving plugins
- **Ordering**: Plugins execute in configuration order; each receives output from previous

---

## 9. Operating Modes

### 9.1 CLI Mode (Default)

Single scan, exit with results.

```bash
db-scan --cluster prod-pg-app007
```

- Fetches data sources once
- Runs scan pipeline
- Outputs to terminal/CSV
- Exits

### 9.2 Watch Mode

Repeated scans with terminal UI.

```bash
db-scan --watch 30s
```

- Polls data sources before each scan
- Tracks state transitions in memory
- Terminal UI shows current state + recent transitions
- No HTTP server
- Ctrl+C to exit

**Transition Visualization**:
- **Changed clusters highlighted**: Visual distinction for clusters that changed state since last scan
- **Event log**: Chronological log of state transitions alongside current state table
  - Format: `[14:32:15] prod-pg-app007: Healthy → Degraded (OneReplicaDown)`

### 9.3 Service Mode

Long-running daemon with HTTP API.

```bash
db-scan --service --port 8080
```

**Lifecycle**:
1. Start HTTP server
2. Start background data source fetchers
3. Wait for first successful data source fetch (with timeout)
4. Begin periodic scan loop (configurable interval, default 30s)
5. Serve requests until SIGTERM

**HTTP Endpoints**:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Liveness probe (200 if running) |
| `/ready` | GET | Readiness probe (200 if data sources healthy) |
| `/metrics` | GET | Prometheus metrics |
| `/scan` | POST | Trigger immediate scan (returns scan ID) |
| `/scan/{id}` | GET | Get scan result by ID |
| `/clusters` | GET | Current state of all clusters |
| `/clusters/{name}` | GET | Detailed state for one cluster |
| `/events` | GET | SSE stream of state transitions |
| `/sources` | GET | Data source health status |

### 9.4 Scan Triggering

**Periodic**: Runs on configured interval (default 30s)

**On-demand**: `POST /scan` triggers immediate scan
- Returns scan ID immediately
- Client polls `/scan/{id}` for result
- Concurrent on-demand requests are coalesced (return same scan ID)

**Rate limiting**: Minimum 5s between scans to prevent abuse

### 9.5 Graceful Shutdown

On SIGTERM:
1. Stop accepting new HTTP requests
2. Cancel any in-progress scan
3. Flush pending metrics
4. Exit within 10s timeout

---

## 10. Metrics Exposition

### 10.1 Prometheus Metrics

All metrics prefixed with `dbscan_`.

**Cluster Health**:
```
dbscan_cluster_health_state{cluster="prod-pg-app007", state="healthy"} 1
dbscan_cluster_health_state{cluster="prod-pg-app007", state="degraded"} 0
dbscan_cluster_health_state{cluster="prod-pg-app007", state="critical"} 0
dbscan_cluster_health_state{cluster="prod-pg-app007", state="unknown"} 0

dbscan_cluster_state_duration_seconds{cluster="prod-pg-app007"} 3600
dbscan_cluster_failover{cluster="prod-pg-app007"} 0
dbscan_cluster_replication_lag_bytes{cluster="prod-pg-app007", replica="db002"} 1048576
```

**Data Sources**:
```
dbscan_datasource_healthy{source="portal"} 1
dbscan_datasource_healthy{source="prometheus"} 0
dbscan_datasource_last_success_seconds{source="portal"} 45
dbscan_datasource_last_success_seconds{source="prometheus"} 120
dbscan_datasource_consecutive_failures{source="prometheus"} 3
```

**Scan Performance**:
```
dbscan_scan_duration_seconds{} 2.5
dbscan_scan_nodes_total{} 150
dbscan_scan_nodes_reachable{} 148
dbscan_scan_clusters_total{} 50
```

**State Transitions**:
```
dbscan_state_transitions_total{cluster="prod-pg-app007", from="healthy", to="degraded"} 2
```

### 10.2 Metric Labels

Consistent labeling:
- `cluster`: Cluster name (e.g., "prod-pg-app007")
- `source`: Data source name (e.g., "portal", "prometheus")
- `state`: Health state (e.g., "healthy", "degraded", "critical", "unknown")
- `replica`: Replica node name when applicable

---

## 11. Output

### 11.1 Terminal Output

- **Table format**: Cluster name, health state, primary node, replica states, lag
- **Color coding**: Green (Healthy), Yellow (Degraded), Red (Critical), Gray (Unknown)
- **Failover annotation**: Healthy clusters with failover show "Healthy (Failover)" in green
- **Per-cluster timing**: Show scan duration for each cluster

### 11.2 CSV Output

- **Content**: Full details including all health metrics, configuration values, split-brain resolution methods
- **Stateless**: Each run produces independent snapshot; no historical accumulation

### 11.3 Filtering

- **--quiet**: Only output non-Healthy clusters (Degraded/Critical/Unknown)
- **--show-healthy**: Include healthy clusters (default: true)
- **--show-failover**: Highlight failed-over healthy clusters

---

## 12. Configuration

### 12.1 Service Configuration

```yaml
# config.yaml
service:
  port: 8080
  scan_interval: 30s
  min_scan_interval: 5s  # Rate limit for on-demand scans

data_sources:
  portal:
    url: "https://database.example.com/api/v1/nodes"
    refresh_interval: 5m
    timeout: 30s

  prometheus:
    url: "https://prometheus.example.com"
    refresh_interval: 60s
    timeout: 10s
    lag_threshold:
      lookback: 7d
      percentile: 95
      tolerance_seconds: 5
      static_fallback_bytes: 83886080  # 80MB
```

### 12.2 Topology Configuration (YAML)

```yaml
# topology.yaml
profiles:
  standard-3node:
    expected_nodes: 3
    canonical_primary_pattern: "-db001"
    standby_names_pattern: "ANY 1 (db002, db003)"

  reporting-2node:
    expected_nodes: 2
    canonical_primary_pattern: "-db001"
    standby_names_pattern: "db002"

defaults:
  profile: standard-3node
  lag_tolerance_seconds: 5

overrides:
  prod-pg-reporting:
    profile: reporting-2node
```

### 12.3 Configuration Reloading

**Service mode**:
- SIGHUP triggers config reload
- Only safe settings reloaded (intervals, thresholds)
- Structural changes (port, data source URLs) require restart
- Reload status exposed at `/config/reload` endpoint

**CLI/Watch mode**:
- Config loaded once at startup
- Changes require restart

---

## 13. PostgreSQL Version Handling

### 13.1 Version Mismatch Detection

- **Flag as Degraded warning**: When nodes in same cluster report different major versions
- **Use case**: During rolling upgrades this is expected; otherwise indicates issues
- **Display**: Include version information in output for mismatched clusters

### 13.2 Query Compatibility

- Health check queries should work across supported PostgreSQL versions
- No version-specific query variants in initial implementation

---

## 14. Error Handling

### 14.1 Connection Failures

- Retry with backoff
- Mark node as Unknown after exhausted retries
- Continue with remaining nodes
- Infer state from primary when possible

### 14.2 Query Failures

- Mark node with appropriate Unknown variant (UnknownPrimary, UnknownReplica)
- Log error details at debug level
- Include error summary in output

### 14.3 Data Source Failures

| Source | Failure Behavior | Escalation |
|--------|------------------|------------|
| Database Portal | Use cached nodes, mark Degraded | After 1h: WARN → ERROR log level |
| Prometheus | Use cached/static thresholds | After 5m: WARN → ERROR log level |
| Plugin | Skip plugin output, log warning | N/A |

### 14.4 Service Health Degradation

The `/ready` endpoint reflects overall health:

```
200 OK        - All data sources healthy, recent scan succeeded
503 Degraded  - One or more data sources unhealthy, or last scan failed
```

Response body includes details:
```json
{
  "ready": false,
  "checks": {
    "portal": { "healthy": true, "last_success_ago": "45s" },
    "prometheus": { "healthy": false, "consecutive_failures": 3 },
    "last_scan": { "healthy": true, "ago": "12s" }
  }
}
```

---

## 15. Security Considerations

### 15.1 Credential Handling

- PostgreSQL credentials via environment variables (PGUSER, PGPASSWORD)
- Certificate paths via environment variables (PGSSLKEY, PGSSLCERT, PGSSLROOTCERT)
- No credentials stored in configuration files

### 15.2 Connection Security

- mTLS required for production environments
- Certificate validation enabled
- No fallback to unencrypted connections

### 15.3 Output Sanitization

- Connection strings containing credentials must be redacted in logs and output
- Plugin JSON output should not include raw credentials

---

## 16. Performance Characteristics

| Operation | Expected Performance |
|-----------|---------------------|
| Node discovery | O(1) cached lookup |
| Scanning | O(n) concurrent (all nodes in parallel) |
| Clustering | O(3n) linear pass |
| Analysis | O(1) per cluster |
| Total scan | 5-10 seconds for 100+ clusters typical |

---

## 17. Implementation Status & TODO

### 17.1 CLI Mode (Current Implementation)

**Status:** Core functionality complete

Implemented:
- Node scanning with role detection
- Cluster aggregation (3-node clusters)
- Health classification (Healthy, Degraded, Critical, Unknown)
- Split-brain detection and resolution
- Terminal output with color coding
- CSV export
- Prometheus integration for backup progress
- Type-safe pipeline architecture

### 17.2 TODO: Service Mode

**Status:** Not yet implemented (Sections 2, 3, 9.3-9.5, 10)

Required implementation:
- [ ] Data source registry with health tracking (Section 2.1)
- [ ] Background fetchers for Portal and Prometheus (Sections 2.2, 2.3)
- [ ] Data freshness tracking and propagation (Section 2.4)
- [ ] Cluster state history and transition tracking (Section 3.1)
- [ ] State transition events (Section 3.3)
- [ ] HTTP server with endpoints (Section 9.3):
  - [ ] `/health` - Liveness probe
  - [ ] `/ready` - Readiness probe with data source status
  - [ ] `/metrics` - Prometheus metrics exposition
  - [ ] `/scan` - Trigger on-demand scan
  - [ ] `/scan/{id}` - Get scan result
  - [ ] `/clusters` - Current state of all clusters
  - [ ] `/clusters/{name}` - Detailed cluster state
  - [ ] `/events` - SSE stream of transitions
  - [ ] `/sources` - Data source health status
- [ ] Periodic scan loop with configurable interval
- [ ] Scan coalescing for concurrent on-demand requests
- [ ] Rate limiting (minimum 5s between scans)
- [ ] Graceful shutdown on SIGTERM
- [ ] Prometheus metrics (Section 10.1):
  - [ ] `dbscan_cluster_health_state`
  - [ ] `dbscan_cluster_state_duration_seconds`
  - [ ] `dbscan_cluster_failover`
  - [ ] `dbscan_cluster_replication_lag_bytes`
  - [ ] `dbscan_datasource_*` metrics
  - [ ] `dbscan_scan_*` metrics
  - [ ] `dbscan_state_transitions_total`

### 17.3 TODO: Watch Mode

**Status:** Not yet implemented (Section 9.2)

Required implementation:
- [ ] Terminal UI with refresh
- [ ] State transition visualization
- [ ] Event log display
- [ ] Highlighted changed clusters
- [ ] Ctrl+C handling

### 17.4 TODO: Configuration System

**Status:** Not yet implemented (Section 12)

Required implementation:
- [ ] YAML configuration loading (service.yaml)
- [ ] Topology configuration (topology.yaml)
  - [ ] Profile definitions
  - [ ] Per-cluster overrides
- [ ] Configuration validation
- [ ] SIGHUP reload support (service mode)
- [ ] Configuration reload endpoint

### 17.5 TODO: Advanced Features

**Status:** Not yet implemented

Required implementation:
- [ ] Topology profiles (Section 5.1)
- [ ] Unexpected topology handling (Section 5.2)
- [ ] synchronous_standby_names validation (Section 5.3)
- [ ] Dynamic lag thresholds from Prometheus history (Section 6.4)
- [ ] Lag diagnostic heuristics breakdown (Section 6.5)
- [ ] Compound severity escalation (Section 6.3)
- [ ] Inferring unreachable node state from primary (Section 4.6)
- [ ] Adaptive rate limiting (Section 4.3)
- [ ] Certificate hot-reload (Section 4.1)
- [ ] Plugin system (Section 8)
- [ ] PostgreSQL version mismatch detection (Section 13.1)
- [ ] Split-brain remediation suggestions (Section 7.4)

### 17.6 TODO: Pipeline Enhancements

**Status:** Deferred

Optional improvements:
- [ ] Bounded channels with backpressure
- [ ] Fan-out/fan-in support for parallel processing
- [ ] Additional metrics (item counts, throughput)
- [ ] Graceful shutdown with cancellation tokens
- [ ] Partial results on stage failure

---

## 18. Future Considerations

### 18.1 Alert Integration (Planned)

- Export health states to monitoring systems
- Webhook support for state change notifications
- Integration with PagerDuty/Slack/etc.

### 18.2 Generalization Path

- Topology configuration enables non-Fortnox deployments
- Core logic remains general; conventions are configuration
- Fortnox-specific defaults preserved for backward compatibility

---

## Appendix A: CLI Reference

```
db-scan [OPTIONS]

Options:
  -c, --cluster <PATTERN>     Filter clusters by name (substring match)
  -l, --log-level <LEVEL>     Log level [default: info]
  -w, --watch <INTERVAL>      Enable watch mode with specified interval (e.g., "30s")
  -s, --service               Run as long-running service with HTTP API
  -p, --port <PORT>           HTTP server port for service mode [default: 8080]
  -q, --quiet                 Only show non-Healthy clusters
      --show-healthy          Include healthy clusters in output [default: true]
      --show-failover         Highlight failed-over healthy clusters
      --generate-script       Output remediation as script instead of interactive prompts
      --config <PATH>         Path to configuration file
  -h, --help                  Print help
  -V, --version              Print version

Environment Variables:
  PGUSER              PostgreSQL username
  PGPASSWORD          PostgreSQL password
  PGSSLKEY            Path to client key file
  PGSSLCERT           Path to client certificate file
  PGSSLROOTCERT       Path to CA certificate file
  DATABASE_PORTAL_URL Base URL for database portal API
  PROMETHEUS_URL      Base URL for Prometheus (optional)
```

---

## Appendix B: Health Check Query Outputs

### Primary Health Check JSON Structure

```json
{
  "timeline_id": 11,
  "uptime_seconds": 864000,
  "current_wal_lsn": "0/5A000000",
  "configuration": {
    "synchronous_commit": "remote_apply",
    "synchronous_standby_names": "ANY 1 (db002, db003)",
    "wal_level": "replica"
  },
  "replication": [
    {
      "application_name": "db002",
      "client_addr": "10.0.1.2",
      "state": "streaming",
      "sent_lsn": "0/5A000000",
      "write_lsn": "0/5A000000",
      "flush_lsn": "0/5A000000",
      "replay_lsn": "0/5A000000",
      "sync_state": "quorum"
    }
  ]
}
```

### Replica Health Check JSON Structure

```json
{
  "wal_receiver": {
    "status": "streaming",
    "sender_host": "10.0.1.1",
    "sender_port": 5432,
    "received_lsn": "0/5A000000",
    "received_tli": 11
  },
  "replay_lag_bytes": 0,
  "configuration": {
    "hot_standby": true,
    "primary_conninfo": "host=10.0.1.1 port=5432 ..."
  },
  "conflicts": {}
}
```

---

## Appendix C: Plugin Protocol

### Input (stdin)

```json
{
  "version": "1.0",
  "cluster_health": {
    "status": "Degraded",
    "reasons": ["HighReplicationLag"],
    "cluster": { /* full AnalyzedCluster struct */ }
  }
}
```

### Output (stdout)

```json
{
  "override": {
    "status": "Healthy",
    "justification": "Maintenance window in progress, lag expected"
  },
  "suggestions": [
    "Consider increasing wal_sender_timeout during maintenance"
  ],
  "actions": [
    {
      "description": "Cancel long-running query on db002",
      "command": "psql -h db002 -c 'SELECT pg_cancel_backend(12345)'"
    }
  ]
}
```

---

## Appendix D: Open Questions

### Service Mode & HTTP API

1. **Authentication for HTTP endpoints?**
   - Internal network only?
   - mTLS?
   - Bearer token?

2. **Retention for `/scan/{id}` results?**
   - Keep last N scans?
   - TTL-based expiry?

3. **SSE vs WebSocket for `/events`?**
   - SSE simpler, sufficient for one-way events
   - WebSocket if we want bidirectional (e.g., subscribe to specific clusters)

4. **Should watch mode use the same HTTP server internally?**
   - Could simplify code (watch mode = service mode without external listener)
   - Or keep them separate for simplicity

### Pipeline Implementation

5. **Bounded vs unbounded channels?**
   - Currently using unbounded channels for simplicity
   - Should we add bounded channel variants for backpressure control?
   - What capacity would be appropriate?

6. **Fan-out/fan-in patterns?**
   - Current linear pipeline is sufficient for CLI mode
   - Service mode may benefit from fan-out (multiple concurrent scans)
   - Would complicate pipeline API - defer until needed

7. **Metrics beyond timing?**
   - Item counts per stage (nodes processed, clusters analyzed)?
   - Throughput metrics (items/second)?
   - Channel buffer sizes and backpressure indicators?
   - Defer for now - timing is sufficient for v1

8. **Advanced error propagation?**
   - Current: fail-fast on any stage error
   - Future: graceful shutdown with cancellation tokens
   - Partial results when some stages fail?
   - How to handle transient vs permanent failures?

---

## Revision History

| Version | Date       | Author         | Changes |
|---------|------------|----------------|---------|
| 1.0     | 2026-01-20 | Robert Sjöblom | Initial specification based on requirements interview |
| 2.0     | 2026-01-20 | Robert Sjöblom | Added service mode, data source management, state tracking, metrics exposition |
| 2.1     | 2026-01-27 | Robert Sjöblom | Added implementation status section (17) with TODO items; expanded open questions with pipeline considerations |
