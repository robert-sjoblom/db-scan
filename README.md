# db-scan

A PostgreSQL cluster health monitoring tool that scans, analyzes, and reports on the health status of PostgreSQL clusters with streaming replication.

This is a project that's tightly coupled to the our current setup, so it's probably mostly useless to outsides. That said, it's open source so do with it what thou wilst.

Large parts of this readme (but not the code) was AI-summarized. Tread carefully.

## Features

- **Cluster Health Detection**: Identifies healthy, degraded, critical, and unknown cluster states
- **Split-Brain Detection**: Detects and resolves split-brain scenarios using timeline and replica evidence
- **Replication Monitoring**: Tracks replication lag, replica status, and synchronization state
- **Failover Detection**: Identifies clusters that have experienced failover
- **Backup Progress Tracking** (optional): Estimates pg_basebackup progress via Prometheus filesystem metrics
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
# PostgreSQL credentials
export PGUSER="your-username"
export PGPASSWORD="your-password"
export PGSSLKEY="/path/to/ssl.key"
export PGSSLCERT="/path/to/ssl.crt"
export PGSSLROOTCERT="/path/to/ca.crt"

# Logging
export RUST_LOG="info"  # or debug, trace, warn, error

# Optional: Prometheus URL (compile-time only, see Build from Source)
# PROMETHEUS_URL=https://prometheus.example.com
```bash
cargo build --release --features prometheus
```

**Configure Prometheus URL** (compile-time):
```bash
PROMETHEUS_URL=https://prometheus.example.com cargo build --release --features prometheus
```

**Requirements:**
- Prometheus server with `node_exporter` metrics
- Metrics: `node_filesystem_size_bytes` and `node_filesystem_avail_bytes`
- Labels: `host` (hostname) and `mountpoint` (e.g., `/var/lib/pgsql`)

**How it works:**
1. During primary health check, captures total database size
2. For replicas using pg_basebackup, queries Prometheus for filesystem usage
3. Estimates progress: `(used_bytes / primary_db_size) * 100`
4. Progress stored in analysis results (0-10000, where 4156 = 41.56%)

**Note:** This is a rough estimate assuming filesystem usage is mostly from the backup. May be inaccurate if significant other data exists on the filesystem.

## Configuration

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
  --pguser <PGUSER>                  PostgreSQL username
  --pgpassword <PGPASSWORD>          PostgreSQL password
  --pgsslkey <PGSSLKEY>              Path to SSL key file
  --pgsslcert <PGSSLCERT>            Path to SSL certificate file
  --pgsslrootcert <PGSSLROOTCERT>    Path to SSL root certificate file
  -c, --cluster <CLUSTER>            Filter by cluster name
  -l, --log-level <LOG_LEVEL>        Log level [default: info]
  --show-healthy                     Show healthy clusters in output
  --show-failover                    Show clusters that have experienced failover
  --csv <PATH>                       Write results to CSV file
  --no-color                         Disable terminal colors
  -h, --help                         Print help
  -V, --version                      Print version
```

## Usage

### Basic Usage

```bash
# Scan all clusters
db-scan

# Scan specific cluster
db-scan --cluster dev-pg-app001

# Show healthy clusters
db-scan --show-healthy

# Export to CSV
db-scan --csv results.csv

# Increase logging verbosity
db-scan --log-level debug
```

### Output Format

Terminal output shows cluster health in a table format:

```
STATUS    CLUSTER           PRIMARY    REPLICAS      LAG     REASON
CRITICAL  prod-pg-app007    db001      -             -       NoPrimary
DEGRADED  dev-pg-app001     db001      db002,db003   50MB    HighReplicationLag
HEALTHY   prod-pg-app123    db002      db001,db003   0B      Failover
```

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

### Critical 🚨
- **NoPrimary**: No primary node found
- **SplitBrain**: Multiple primaries detected
- **WritesBlocked**: Primary with sync_commit=on but no replicas
- **WritesUnprotected**: Primary with sync_commit=off and no replicas

### Unknown ❓
- **NoNodesReachable**: Cannot connect to any nodes
- **UnexpectedTopology**: Cluster has unexpected node count

## Architecture

### Components

1. **Scanner** (`v2/scan/`): Connects to PostgreSQL nodes and executes health checks
2. **Cluster Builder** (`v2/cluster.rs`): Groups nodes into clusters
3. **Analyzer** (`v2/analyze/`): Evaluates cluster health and detects issues
4. **Writer** (`v2/writer.rs`): Formats and outputs results

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
├── database_portal.rs         # Node API client
├── logging.rs                 # Tracing setup
└── v2/
    ├── mod.rs
    ├── node.rs                # Node data structure
    ├── db.rs                  # Database connection
    ├── cluster.rs             # Cluster builder
    ├── writer.rs              # Output formatting
    ├── scan/
    │   ├── mod.rs             # Scan orchestration
    │   ├── health_check_primary.rs
    │   └── health_check_replica.rs
    └── analyze/
        └── mod.rs             # Health analysis logic
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