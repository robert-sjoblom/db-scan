# db-scan

A PostgreSQL cluster health monitoring tool that scans, analyzes, and reports on the health status of PostgreSQL clusters with streaming replication.

This is a project that's tightly coupled to our current setup, so it's probably mostly useless to outsiders. That said, it's open source so do with it what thou wilst.

Large parts of this readme (but not the code) was AI-summarized. Tread carefully.

## Features

- **Cluster Health Detection**: Identifies healthy, degraded, critical, and unknown cluster states
- **Split-Brain Detection**: Detects and resolves split-brain scenarios using timeline and replica evidence
- **Replication Monitoring**: Tracks replication lag, replica status, and synchronization state
- **Failover Detection**: Identifies clusters that have experienced failover
- **Archive Failure Detection**: Detects when WAL archiving is enabled but has never succeeded
- **Disk Health Checks** (optional): SSHes into nodes and parses `dmesg` for I/O, filesystem, and block device errors
- **Backup Progress Tracking** (optional): Estimates pg_basebackup progress via Prometheus filesystem metrics
- **Watch Mode**: Continuously rescans unhealthy clusters at a configurable interval
- **Concurrent Scanning**: Parallel health checks across multiple clusters and nodes
- **Multiple Output Formats**: Terminal output (with colors) and CSV export
- **Structured Logging**: Full tracing support with spans and structured fields

## Installation

### Prerequisites

- Rust 1.70+ (for building from source)
- PostgreSQL credentials and SSL certificates
- Network access to PostgreSQL nodes
- A Database Portal that the tool can connect to to get an initial list of nodes to scan

### Database Portal API Requirements

The tool expects a REST API endpoint that returns PostgreSQL node information. The API must:

**Endpoint**: `GET https://database.example.com/api/v1/nodes` (default)

You can configure a custom URL at compile time:

```bash
DATABASE_PORTAL_URL=https://your-api.com/api/v1/nodes cargo build --release
```

**Cache Behavior**:
- Responses are cached in `/tmp/nodes_response.json`
- Cache is valid for 24 hours
- Stale cache triggers automatic re-fetch

**Response Format**:
```json
{
  "items": [
    {
      "id": 1,
      "cluster_id": 33,
      "node_name": "dev-pg-app001-db001.sto1.example.com",
      "pg_version": "15.14",
      "ip_address": "127.1.12.151"
    },
    {
      "id": 2,
      "cluster_id": 33,
      "node_name": "dev-pg-app001-db002.sto2.example.com",
      "pg_version": "15.14",
      "ip_address": "127.2.12.151"
    }
  ],
  "count": 2
}
```

**Required Fields**:
- `items`: Array of node objects
- `count`: Total number of nodes returned
- Each node must have: `id`, `cluster_id`, `node_name`, `pg_version`, `ip_address`

**Node Naming Convention**:
Nodes must follow the naming pattern: `{env}-pg-{app}-{db}.{zone}.{domain}`
- Example: `dev-pg-app001-db001.sto1.example.com`
- The tool uses this pattern to extract environment, cluster name, and database number (GOOD FIRST PR IF YOU EXTRACT THIS INTO CONFIG)

### Build from Source

```bash
# Basic build
cargo build --release

# Build with Prometheus integration for backup progress tracking
PROMETHEUS_URL=https://prometheus.example.com DATABASE_PORTAL_URL=https://database.example.com cargo build --release --features prometheus
```

The binary will be available at `target/release/db-scan`

### Optional Features

#### Prometheus (backup progress tracking)

```bash
PROMETHEUS_URL=https://prometheus.example.com cargo build --release --features prometheus
```

**Requirements:**
- Prometheus server with `node_exporter` metrics
- Metrics: `node_filesystem_size_bytes` and `node_filesystem_avail_bytes`
- Labels: `host` (hostname) and `mountpoint` (e.g., `/var/lib/pgsql`)

**How it works:**
1. Fetches filesystem metrics for all nodes at startup using a single batch query (e.g., `host=~"dev-pg-app001.*"`)
2. For replicas using pg_basebackup, compares the replica's filesystem used bytes against the primary's
3. Estimates progress: `(replica_used_bytes / primary_used_bytes) * 100`
4. Progress stored as percentage * 100 (e.g., 4156 = 41.56%), keyed by replica IP address

**Note:** Filesystem-to-filesystem comparison (not database size) — chosen for performance. Rough estimate; assumes both filesystems primarily contain PostgreSQL data.

#### Disk health checks

Pass `--check-disks` at runtime (no feature flag needed). Requires `--ssh-user` or the `SSH_USER` env var.

```bash
db-scan --check-disks --ssh-user first_last
```

**How it works:**
- SSHes into every node and runs `dmesg -T --since "<window> minutes ago"`
- Parses output for I/O errors, filesystem errors (EXT4/XFS), and block device errors
- Errors are surfaced as **Degraded { DiskIoErrors }** or **Critical { FilesystemErrors }** cluster states
- Window defaults to 60 minutes; override with `--disk-check-window-minutes` or `DISK_CHECK_WINDOW_MINUTES`

## Configuration

### Config File

By default, `db-scan` loads `~/.config/db-scan/config.yml` (respects `$XDG_CONFIG_HOME`). Use `--config <PATH>` to specify a different file, or `--no-config` to skip loading one entirely.

```yaml
postgres:
  user: myuser
  sslkey: /path/to/ssl.key
  sslcert: /path/to/ssl.crt
  sslrootcert: /path/to/ca.crt

defaults:
  user: postgres
  password: secret

ssh:
  user: first_last

display:
  log_level: info
  no_color: false

disk_check:
  window_minutes: 60

scan:
  max_concurrency: 256
```

All fields are optional. CLI flags and environment variables take precedence over the config file. `PGPASSWORD` is never read from the config file.

### Environment Variables

```bash
export PGUSER="your-username"
export PGPASSWORD="your-password"
export PGSSLKEY="/path/to/ssl.key"
export PGSSLCERT="/path/to/ssl.crt"
export PGSSLROOTCERT="/path/to/ca.crt"
export RUST_LOG="info"  # or debug, trace, warn, error
```

### Command-Line Options

```bash
db-scan [OPTIONS]

Options:
  --config <PATH>                          Path to config file [default: $XDG_CONFIG_HOME/db-scan/config.yml]
  --no-config                              Skip loading the config file
  --pguser <PGUSER>                        PostgreSQL username
  --pgpassword <PGPASSWORD>                PostgreSQL password
  --pgsslkey <PGSSLKEY>                    Path to SSL key file
  --pgsslcert <PGSSLCERT>                  Path to SSL certificate file
  --pgsslrootcert <PGSSLROOTCERT>          Path to SSL root certificate file
  -c, --cluster <CLUSTER>                  Filter by cluster name (regex)
  -l, --log-level <LOG_LEVEL>              Log level [default: info]
  --show-healthy                           Show healthy clusters in output
  --show-failover                          Show clusters that have experienced failover
  --csv <PATH>                             Write results to CSV file
  --no-color                               Disable terminal colors
  --watch [<SECONDS>]                      Watch mode: rescan unhealthy clusters repeatedly [default interval: 60s]
  -s, --silence-tracing                    Suppress tracing output (useful with --watch)
  --check-disks                            Enable disk health checks via SSH on nodes
  --ssh-user <SSH_USER>                    SSH user for disk checks (e.g. "first_last") [env: SSH_USER]
  --disk-check-window-minutes <MINUTES>    dmesg recency window for disk checks [default: 60] [env: DISK_CHECK_WINDOW_MINUTES]
  --max-concurrency <N>                    Max nodes scanned in parallel [default: 256] [env: DB_SCAN_MAX_CONCURRENCY]
  --default-user <USER>                    Default PostgreSQL user for non-cert auth [env: DEFAULT_USER]
  --default-pass <PASS>                    Default PostgreSQL password for non-cert auth [env: DEFAULT_PASS]
  -h, --help                               Print help
  -V, --version                            Print version
```

## Usage

### Basic Usage

```bash
# Scan all clusters
db-scan

# Filter by cluster name (regex)
db-scan --cluster 'prod-pg-app'
db-scan --cluster '.*-ts-.*'

# Show healthy clusters
db-scan --show-healthy

# Export to CSV
db-scan --csv results.csv

# Watch mode: rescan unhealthy clusters every 60 seconds
db-scan --watch --silence-tracing

# Watch mode with custom interval
db-scan --watch 30

# Include disk health checks via SSH
db-scan --check-disks --ssh-user first_last

# Increase logging verbosity
db-scan --log-level debug
```

### Output Format

Terminal output prints a stage-timings block followed by a cluster health table:

```
Prometheus       124 ms
Node Discovery     0 ms
Scan             424 ms
Clustering       454 ms
Analysis         454 ms
Output           454 ms
────────────────────────
Total           1.91 s
STATUS   CLUSTER        PRIMARY                    REPLICAS              LAG DISK REASON
HEALTHY  dev-pg-app001  db001@sto1                 db002,db003           -   -    -
CRITICAL prod-pg-app123 db001@sto1⁷ vs db002@sto2⁸ db003@sto3→db001@sto1 -   -    SplitBrain: replica overrides timeline (7 < 8)

⁷ = timeline id
```

Notes on the format:
- `PRIMARY` and replica cells include the node's zone (`@sto1`).
- A chained replica shows its upstream: `db003@sto3→db001@sto1`.
- When split-brain is detected, the primary cell lists candidates side-by-side with superscript timeline markers; footnote keys appear below the table.
- `LAG` shows `-` when not applicable (no replica or no measurement), otherwise a byte count (e.g. `80MB`).
- `DISK` is only populated when `--check-disks` is enabled.

## Health States

### Healthy ✅
- One primary and two replicas online
- Replication lag < 5 seconds (80MB at 16MB/s WAL generation)
- All replicas streaming

### Degraded ⚠️
- **OneReplicaDown**: One replica unavailable
- **HighReplicationLag**: Lag exceeds 5 second threshold
- **RebuildingReplica**: Replica is rebuilding (no WAL receiver)
- **ChainedReplica**: Cascading replication detected
- **DiskIoErrors**: I/O or block device errors detected in dmesg (requires `--check-disks`)

### Critical 🚨
- **NoPrimary**: No primary node found
- **SplitBrain**: Multiple primaries detected
- **WritesBlocked**: Primary with sync_commit=on but no replicas
- **WritesUnprotected**: Primary with sync_commit=off and no replicas
- **ArchiveFailure**: WAL archiving is enabled but has never successfully archived
- **FilesystemErrors**: Filesystem errors (EXT4/XFS) detected in dmesg (requires `--check-disks`)

### Unknown ❓
- **NoNodesReachable**: Cannot connect to any nodes
- **UnexpectedTopology**: Cluster has unexpected node count

## Architecture

### Components

1. **Scanner** (`v2/scan/`): Connects to PostgreSQL nodes and executes health checks
2. **Cluster Builder** (`v2/cluster.rs`): Groups nodes into clusters
3. **Analyzer** (`v2/analyze/`): Evaluates cluster health and detects issues
4. **Writer** (`v2/writer/`): Formats and outputs results (terminal, CSV)

### Data Flow

```
Nodes API → Scanner → Analyzed Nodes → Cluster Builder → Clusters → Analyzer → Health Status → Writer → Output
```

## Split-Brain Resolution

When multiple primaries are detected, the tool uses multiple strategies to determine the true primary:

1. **Timeline Analysis**: Higher timeline ID indicates more recent promotion
2. **Replica Evidence**: Which primary are the replicas following?
3. **Combined Evidence**: Both timeline and replica data agree
4. **Override Case**: Replicas override timeline (isolated failed promotion)

## Development

### Running Tests

```bash
cargo test
```

### Code Structure

```
src/
├── main.rs                    # Entry point
├── config.rs                  # CLI + config file merge
├── database_portal.rs         # Node API client
├── logging.rs                 # Tracing setup
├── pipeline.rs                # Scan → analyze → write orchestration
├── timings.rs                 # Stage timing instrumentation
├── prometheus.rs              # Prometheus client (optional feature)
├── prometheus/
│   └── models.rs
└── v2/
    ├── node.rs                # Node data structure
    ├── cluster.rs             # Cluster builder
    ├── db.rs                  # Database connection
    ├── db/
    │   └── db_error.rs        # Database error taxonomy
    ├── scan.rs                # Scan orchestration
    ├── scan/
    │   ├── health_check_primary.rs
    │   ├── health_check_replica.rs
    │   └── disk_check.rs      # SSH/dmesg disk checks
    ├── analyze.rs             # Health analysis entry point
    ├── analyze/
    │   ├── checks.rs          # Individual health checks
    │   ├── classify.rs        # Verdict classification
    │   └── split_brain.rs     # Split-brain resolution
    ├── writer.rs              # Output dispatch
    └── writer/
        ├── build.rs           # Build view rows from cluster state
        ├── view.rs            # Row/view data types
        ├── terminal.rs        # Terminal renderer
        ├── csv.rs             # CSV renderer
        └── units.rs           # Byte/duration formatting
```

## Logging

The tool uses structured logging with tracing. All logs include relevant context fields:

```rust
tracing::info!(
    node_name = %node.node_name,
    attempt = attempt,
    "successfully connected after retry"
);
```

Spans are used for major operations to provide hierarchical context.

## Support

lol, no