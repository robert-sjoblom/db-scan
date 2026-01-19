# db-scan Specification

**Version:** 1.0
**Status:** Draft
**Last Updated:** 2026-01-20

## Executive Summary

db-scan is a PostgreSQL cluster health monitoring tool designed for Fortnox's infrastructure. It performs concurrent health checks across PostgreSQL clusters, classifies their health states, and provides actionable diagnostics for operators. The tool is currently a read-only diagnostic utility with a planned evolution toward alert integration and a plugin-based extensibility model.

---

## 1. Architecture Overview

### 1.1 Pipeline Model

```
Node Discovery → Parallel Scanning → Cluster Aggregation → Analysis → Plugin Processing → Output
      ↓                                                                                    ↓
 Database Portal                                                              Terminal / CSV / Metrics
 (with caching)
```

### 1.2 Core Design Principles

1. **Graceful Degradation**: Partial visibility is better than total failure. One unreachable node or failed cluster scan does not block monitoring of others.

2. **Stateless Execution**: Each scan is independent. No state persistence between runs. Historical tracking and trend analysis are delegated to external systems (Prometheus, logging infrastructure).

3. **Future-Aware Design**: While currently read-only diagnostic, architecture decisions should accommodate future alert integration without major refactoring.

4. **Availability Over Accuracy**: When infrastructure components (portal API, Prometheus) are degraded, prefer serving potentially stale data with warnings over failing entirely.

---

## 2. Node Discovery

### 2.1 Database Portal Integration

- **Source**: REST API at configured endpoint (default: `https://database.fnox.se/api/v1/nodes`)
- **Data**: Node inventory including IP address, cluster_id, node_name, pg_version

### 2.2 Caching Behavior

- **TTL**: 24-hour file cache at `/tmp/nodes_response.json`
- **Stale Cache Policy**: If portal is unreachable and cache is expired:
  - Use stale cache data
  - Display prominent warning in output indicating staleness
  - Include cache age in warning message
- **Watch Mode Refresh**: Background refresh of portal cache during watch mode to detect new/removed clusters without operator intervention

---

## 3. Scanning

### 3.1 Connection Strategy

- **Concurrency**: Async tasks per node via tokio
- **mTLS**: Required for production and specific dev clusters (`dev-pg-app006`, `dev-pg-app010`, `dev-pg-app011`)
- **Certificate Hot-Reload**: Detect certificate file changes and reload without restart during long-running watch sessions

### 3.2 Retry Logic

- **Attempts**: 3 total (initial + 2 retries)
- **Delay**: 500ms between retries
- **Failure Handling**: Mark node as `Unknown` after all attempts exhausted; continue scanning remaining nodes

### 3.3 Adaptive Rate Limiting

- **Initial Behavior**: Start with unlimited concurrency
- **Back-off Trigger**: Increase in connection failure rate
- **Back-off Strategy**: Reduce concurrent connections progressively
- **Recovery**: Gradual ramp-up; double concurrency every successful interval until restored to maximum; reset to reduced state on new failures

### 3.4 Role Detection

- **Method**: `SELECT pg_is_in_recovery()`
- **Primary**: Not in recovery → execute primary health check
- **Replica**: In recovery → execute replica health check

### 3.5 Health Check Queries

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

### 3.6 Inferring Unreachable Node State

When a node is unreachable but the primary is accessible:

- **Query primary's pg_stat_replication** to determine replica state
- **Distinguish failure modes**:
  - "Unreachable but streaming per primary" → likely network issue from db-scan's perspective
  - "Unreachable and not in pg_stat_replication" → actual replica failure
- **Display both observations**: Show db-scan's direct result alongside primary's reported state

---

## 4. Cluster Aggregation

### 4.1 Topology Profiles

Clusters are assigned to topology profiles that define expected characteristics:

- **Profile Definition**: Specified in YAML topology configuration
- **Profile Assignment**: Clusters tagged with profile via database portal metadata
- **Example Profiles**:
  - `standard-3node`: 1 primary + 2 replicas (default Fortnox configuration)
  - `reporting-2node`: 1 primary + 1 replica
  - `legacy-5node`: Extended replication for legacy applications

### 4.2 Unexpected Topology Handling

- **Do not silently skip**: Clusters with node counts not matching their profile
- **Dedicated output section**: List all topology anomalies with:
  - Cluster name
  - Expected node count (from profile)
  - Actual node count
  - List of nodes found

### 4.3 Configuration Validation

- **synchronous_standby_names validation**: Verify configuration matches actual connected replicas
- **Pattern support**: Configurable patterns in topology config to support:
  - Explicit names (db002, db003)
  - Patroni-style dynamic names
  - Hostname-based names
- **Mismatch handling**: Flag as warning requiring investigation

---

## 5. Health Classification

### 5.1 Health States

```rust
enum ClusterHealth {
    Healthy { failover: bool, cluster: AnalyzedCluster },
    Degraded { reasons: Vec<Reason>, cluster: AnalyzedCluster },
    Critical { reasons: Vec<Reason>, cluster: AnalyzedCluster },
    Unknown { cluster: AnalyzedCluster, reachable_nodes: usize },
}
```

### 5.2 Classification Logic

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

### 5.3 Compound Severity Escalation

When multiple degraded conditions exist simultaneously:

- **Two or more Degraded reasons** → Escalate to **Critical**
- **Rationale**: Compound failures represent higher risk than single issues
- **Example**: OneReplicaDown + HighReplicationLag on remaining replica = Critical

### 5.4 Dynamic Lag Threshold

**Data Source**: Prometheus integration (pg_stat_replication metrics history)

**Calculation**:
- Query last 7 days of WAL generation data per cluster
- Calculate 95th percentile of WAL bytes/second
- Threshold = 95th percentile rate × configured lag tolerance seconds (default: 5s)

**Fallback**: When Prometheus unavailable, use static threshold (80MB based on 16MB/s × 5s)

**Caching**: 30-60 second TTL on Prometheus queries to avoid excessive load during watch mode

### 5.5 Lag Diagnostic Heuristics

When lag exceeds threshold, provide breakdown:

- **send_lag**: Network transfer delay (network bottleneck indicator)
- **flush_lag**: Disk write delay (I/O bottleneck indicator)
- **replay_lag**: Transaction application delay (CPU/lock contention indicator)

**Display Logic**:
- Below threshold: Single lag number
- Above threshold: Automatically expand to show component breakdown

---

## 6. Split-Brain Analysis

### 6.1 Detection

Multiple nodes return `pg_is_in_recovery() = false`

### 6.2 Resolution Strategies

1. **HigherTimeline**: Different timeline IDs → higher timeline is true primary
2. **ReplicaFollowing**: Equal timelines → determine which primary replicas are streaming from
3. **Both**: Timeline and replica evidence agree (high confidence)
4. **ReplicaOverridesTimeline**: Replicas follow lower-timeline primary
5. **Indeterminate**: Cannot determine (equal timelines, no replica evidence)

### 6.3 Timeline Relationship Analysis

When replica follows lower timeline on same timeline:
- Higher timeline primary can be safely shut down and marked for rebuild
- Tool should recommend this action

When replica follows lower timeline with higher timeline itself:
- Manual decision required
- Tool should present both options with implications

### 6.4 Recommended Actions with Approval

- **Default (interactive)**: Display recommended action, prompt operator for confirmation
- **Script mode (--generate-script)**: Write remediation commands to script file for review and manual execution
- **Never auto-execute** without explicit operator approval

---

## 7. Plugin System

### 7.1 Architecture

- **Communication**: External processes via stdin/stdout JSON
- **Discovery**: Plugins specified in configuration file
- **Language Agnostic**: Plugins can be written in any language

### 7.2 Data Contract

- **Input**: Full `ClusterHealth` struct as JSON (maximum flexibility)
- **Output**: JSON response containing:
  - Optional health classification override with justification
  - Optional suggestions/annotations
  - Optional recommended actions

### 7.3 Override Capability

Plugins can reclassify health states:
- Must provide justification string
- Original classification preserved alongside override
- Use case: Maintenance windows, known acceptable states

### 7.4 Execution Model

- **Timeout**: None (trust plugins to behave)
- **Failure Handling**: Operator responsibility to fix misbehaving plugins
- **Ordering**: Plugins execute in configuration order; each receives output from previous

---

## 8. Watch Mode

### 8.1 Activation

```bash
db-scan --watch 30s
```

### 8.2 Features

- **Efficient delta detection**: Track state changes between scans
- **Certificate hot-reload**: Automatically reload TLS certificates on file change
- **Background cache refresh**: Portal API cache refreshed periodically to detect infrastructure changes

### 8.3 Transition Visualization

- **Changed clusters highlighted**: Visual distinction for clusters that changed state since last scan
- **Event log**: Chronological log of state transitions alongside current state table
  - Format: `[14:32:15] prod-pg-app007: Healthy → Degraded (OneReplicaDown)`

### 8.4 Metrics Exposition

- **Endpoint**: `/metrics` (Prometheus format) when running in watch mode
- **Metrics Include**:
  - Scan duration per cluster
  - Connection success/failure counts
  - Cache hit rates
  - Cluster health state gauges

---

## 9. Output

### 9.1 Terminal Output

- **Table format**: Cluster name, health state, primary node, replica states, lag
- **Color coding**: Green (Healthy), Yellow (Degraded), Red (Critical), Gray (Unknown)
- **Failover annotation**: Healthy clusters with failover show "Healthy (Failover)" in green
- **Per-cluster timing**: Show scan duration for each cluster

### 9.2 CSV Output

- **Content**: Full details including all health metrics, configuration values, split-brain resolution methods
- **Stateless**: Each run produces independent snapshot; no historical accumulation

### 9.3 Filtering

- **--quiet**: Only output non-Healthy clusters (Degraded/Critical/Unknown)
- **--show-healthy**: Include healthy clusters (default: true)
- **--show-failover**: Highlight failed-over healthy clusters

---

## 10. Configuration

### 10.1 Topology Configuration (YAML)

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

### 10.2 Configuration Loading

- **Loaded once at startup**: Changes require restart
- **Rationale**: Predictable behavior; operators know when config takes effect

---

## 11. PostgreSQL Version Handling

### 11.1 Version Mismatch Detection

- **Flag as Degraded warning**: When nodes in same cluster report different major versions
- **Use case**: During rolling upgrades this is expected; otherwise indicates issues
- **Display**: Include version information in output for mismatched clusters

### 11.2 Query Compatibility

- Health check queries should work across supported PostgreSQL versions
- No version-specific query variants in initial implementation

---

## 12. Error Handling

### 12.1 Connection Failures

- Retry with backoff
- Mark node as Unknown after exhausted retries
- Continue with remaining nodes
- Infer state from primary when possible

### 12.2 Query Failures

- Mark node with appropriate Unknown variant (UnknownPrimary, UnknownReplica)
- Log error details at debug level
- Include error summary in output

### 12.3 External Service Failures

| Service | Failure Behavior |
|---------|------------------|
| Database Portal | Use stale cache with warning |
| Prometheus | Fall back to static thresholds |
| Plugin | Skip plugin output, log warning |

---

## 13. Security Considerations

### 13.1 Credential Handling

- PostgreSQL credentials via environment variables (PGUSER, PGPASSWORD)
- Certificate paths via environment variables (PGSSLKEY, PGSSLCERT, PGSSLROOTCERT)
- No credentials stored in configuration files

### 13.2 Connection Security

- mTLS required for production environments
- Certificate validation enabled
- No fallback to unencrypted connections

### 13.3 Output Sanitization

- Connection strings containing credentials must be redacted in logs and output
- Plugin JSON output should not include raw credentials

---

## 14. Performance Characteristics

| Operation | Expected Performance |
|-----------|---------------------|
| Node discovery | O(1) cached lookup |
| Scanning | O(n) concurrent (all nodes in parallel) |
| Clustering | O(3n) linear pass |
| Analysis | O(1) per cluster |
| Total scan | 5-10 seconds for 100+ clusters typical |

---

## 15. Future Considerations

### 15.1 Alert Integration (Planned)

- Export health states to monitoring systems
- Webhook support for state change notifications
- Integration with PagerDuty/Slack/etc.

### 15.2 Generalization Path

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
  -q, --quiet                 Only show non-Healthy clusters
      --show-healthy          Include healthy clusters in output [default: true]
      --show-failover         Highlight failed-over healthy clusters
      --generate-script       Output remediation as script instead of interactive prompts
      --config <PATH>         Path to topology configuration file
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

## Revision History

| Version | Date       | Author         | Changes |
|---------|------------|----------------|---------|
| 1.0     | 2026-01-20 | Robert Sjöblom | Initial specification based on requirements interview |
