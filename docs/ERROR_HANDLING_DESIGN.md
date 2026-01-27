# Error Handling Design

## Context

This document outlines the error handling strategy for db-scan, based on principles from ["Stop Forwarding Errors, Start Designing Them"](https://fast.github.io/blog/stop-forwarding-errors-start-designing-them/).

**Key Insight:** Errors serve two audiences:
1. **Machines** - Need flat, actionable error kinds to decide what to do
2. **Humans** - Need context-rich chains to understand what went wrong

## Current Problems

### DbError Anti-Pattern

The current `DbError` ([src/v2/db.rs:15-63](src/v2/db.rs#L15-L63)) exhibits classic error forwarding issues:

```rust
pub enum DbError {
    TlsConnector(String),   // Origin: TLS layer
    Postgres(String),        // Origin: postgres driver
    Io(String),              // Origin: I/O
    SerdeJson(String),       // Origin: JSON parser
}
```

**Problems:**
- Categorized by **origin** (where it came from), not **actionability** (what to do about it)
- All underlying error structure lost (converted to `String`)
- No way to ask: "Is this retryable?"
- No context about what operation failed
- `DbError::Postgres` could be auth failure, syntax error, or network timeout - all require different handling

### Missing Context

When scanning fails, we know:
- Connection failed
- Which node failed
- Why it failed (bad credentials vs transient network issue?) - NO
- What operation was executing - NO
- Where in the code it happened - NO

### Unused error-stack Dependency

The crate depends on `error-stack = "0.6.0"` but never uses it. This is the right tool for the job.

## Design Principles

### 1. Flat Error Kinds (For Machines)

Model errors based on **response action**, not origin:

```rust
pub enum DbErrorKind {
    // Transient - retryable
    ConnectionRefused,
    ConnectionTimeout,
    QueryTimeout,

    // Permanent - mark node as Unknown
    AuthenticationFailed,
    TlsHandshakeFailed,
    SslCertificateInvalid,
    InvalidHostname,

    // Query-specific
    QuerySyntaxError,
    InsufficientPrivileges,

    // Serialization
    InvalidResponse,
}
```

**Key method:**
```rust
impl DbErrorKind {
    pub fn is_retryable(&self) -> bool {
        matches!(self,
            Self::ConnectionRefused |
            Self::ConnectionTimeout |
            Self::QueryTimeout
        )
    }
}
```

This eliminates type traversal - callers can directly ask "should I retry?"

### 2. Zero-Cost Context (For Humans)

Use `error-stack` to capture context without allocation in the hot path:

```rust
use error_stack::{Context, Report, ResultExt};

#[derive(Debug)]
pub struct DbError {
    pub kind: DbErrorKind,
}

impl Context for DbError {}

// Automatic location tracking with #[track_caller]
#[track_caller]
pub async fn connect(node: &Node) -> error_stack::Result<(Client, Connection), DbError> {
    // ...
}
```

### 3. Context at Boundaries

Add context where it matters - at **module boundaries** and **operation boundaries**:

```rust
// At connection boundary
db::connect(&node)
    .attach_printable_lazy(|| format!("connecting to node: {}", node.node_name))
    .attach_printable_lazy(|| format!("cluster: {} (id: {})", node.cluster_id, node.id))

// At query boundary
client.query(PRIMARY_HEALTH_QUERY, &[])
    .change_context(DbError { kind: DbErrorKind::QueryTimeout })
    .attach_printable("executing primary health check query")
```

**Result:** Rich error chains without manual propagation:
```
failed to scan node
  └→ connecting to node: prod-pg-app007-db001.sto1.example.com
      └→ cluster: prod-pg-app007 (id: 42)
          └→ TLS handshake failed: certificate verify failed, at db.rs:82
```

## Implementation Plan

### Phase 1: Define Error Taxonomy

**File:** `src/v2/db.rs`

1. Replace `DbError` enum with `DbErrorKind`
2. Map underlying errors to kinds:
   - `tokio_postgres::Error` → match on error code/kind
   - `native_tls::Error` → match on error message patterns
   - `std::io::Error` → match on `ErrorKind`
   - `serde_json::Error` → `InvalidResponse`

3. Implement `is_retryable()` method
4. Create `DbError` struct wrapping `DbErrorKind`
5. Implement `error_stack::Context` trait

**Key Decision:** Mapping postgres errors to kinds requires inspection:
- Connection refused: `ConnectionRefused`
- Authentication failure: `AuthenticationFailed`
- Query timeout: `QueryTimeout`
- Invalid SQL: `QuerySyntaxError`

Reference: [`tokio_postgres::error::DbError`](https://docs.rs/tokio-postgres/latest/tokio_postgres/error/struct.DbError.html) for SQL state codes.

### Phase 2: Update Connection Logic

**File:** `src/v2/db.rs`

1. Change return type: `Result<T, DbError>` → `error_stack::Result<T, DbError>`
2. Add `#[track_caller]` to `connect()` function
3. Convert underlying errors using `.change_context()`:
   ```rust
   cfg.connect(connector)
       .await
       .change_context(DbError { kind: classify_error(&err) })
   ```
4. Remove all `.to_string()` conversions
5. Keep original error in report chain (error-stack does this automatically)

### Phase 3: Add Context in Scan Pipeline

**File:** `src/v2/scan/mod.rs`

1. Update retry logic to use `DbErrorKind::is_retryable()`
2. Add context at scan boundary:
   ```rust
   db::connect(&node)
       .attach_printable_lazy(|| format!("node: {}", node.node_name))
       .attach_printable_lazy(|| format!("cluster_id: {}", node.cluster_id))
   ```
3. Improve error logging - print full error-stack Report
4. Store DbErrorKind in `AnalyzedNode.errors` instead of full DbError

**Rationale:** `AnalyzedNode` is serialized for tests, so store only the actionable kind.

### Phase 4: Update Health Check Queries

**Files:**
- `src/v2/scan/health_check_primary.rs`
- `src/v2/scan/health_check_replica.rs`

1. Add context for query failures:
   ```rust
   client.query(QUERY, &[])
       .change_context(DbError { kind: DbErrorKind::QueryTimeout })
       .attach_printable("executing primary health check")
   ```
2. Add context for deserialization failures:
   ```rust
   serde_json::from_value(row.get(0))
       .change_context(DbError { kind: DbErrorKind::InvalidResponse })
       .attach_printable_lazy(|| format!("row: {:?}", row))
   ```

### Phase 5: Handle Serialization

**Problem:** `DbError` appears in `AnalyzedNode.errors: Vec<DbError>`, which is serialized for test fixtures.

**Current Fixture Usage:**
- Fixtures are JSON files in `tests/fixtures/` loaded via `include_str!`
- They capture real cluster states for regression testing
- Example: `DB001_UNREACHABLE_FAILOVER_WITH_REPLICA.json` has one node with:
  ```json
  "errors": [{ "Postgres": "error connecting to server: connection timed out" }]
  ```

**Solution Options:**

**Option A: Keep JSON fixtures, serialize DbErrorKind**
```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct AnalyzedNode {
    // ...
    pub errors: Vec<DbErrorKind>,  // Just the kind, not the full Report
}

// In JSON fixtures:
"errors": ["ConnectionTimeout"]
// Or with snake_case for readability:
"errors": ["connection_timeout"]
```

**Pros:**
- Simple to serialize/deserialize
- Test fixtures remain readable
- Type-safe assertions in tests

**Cons:**
- Loses original error message in fixtures
- Can't capture "why" classification chose that kind

**Option B: Separate runtime and fixture types**
```rust
// Runtime: Rich error context
pub struct AnalyzedNode {
    pub errors: Vec<error_stack::Report<DbError>>,
}

// Test fixtures: Simple enum
#[cfg(test)]
#[derive(Serialize, Deserialize)]
pub struct AnalyzedNodeFixture {
    pub errors: Vec<DbErrorKind>,
}

impl From<AnalyzedNodeFixture> for AnalyzedNode {
    fn from(fixture: AnalyzedNodeFixture) -> Self {
        // Convert kinds to Reports
    }
}
```

**Pros:**
- Runtime code has full error context
- Test fixtures stay simple
- Clear separation of concerns

**Cons:**
- More code complexity
- Two representations to maintain

**Option C: Rethink fixture strategy - use builders instead of JSON**
```rust
// Don't serialize errors at all in fixtures
// Instead, use builder pattern in tests

#[cfg(test)]
impl AnalyzedNode {
    pub fn builder() -> AnalyzedNodeBuilder { /* ... */ }
}

// In tests:
let node = AnalyzedNode::builder()
    .role(Role::Unknown)
    .error(DbErrorKind::ConnectionTimeout)
    .build();
```

**Pros:**
- Test intent clearer than JSON
- No serialization issues
- More flexible for test variations

**Cons:**
- Loses "snapshot of production" aspect
- More work to migrate existing fixtures
- Can't easily inspect fixture data outside tests

**Recommendation:** Start with **Option A** (serialize `DbErrorKind`), consider **Option C** long-term.

**Rationale:**
- Test fixtures represent "what happened," not "how we learned it"
- The error **kind** (ConnectionTimeout) is what matters for testing analysis logic
- The error **context** (which operation, call stack) matters for production debugging
- These are different concerns that don't need to be coupled
- JSON fixtures are valuable as "golden files" showing real cluster states

**Migration path:**
1. Change `errors: Vec<DbError>` → `errors: Vec<DbErrorKind>`
2. Update existing JSON fixtures to use kind strings (e.g., `"connection_timeout"`)
3. Tests continue to work, now with actionable kinds
4. If fixtures become unwieldy, migrate to builders in Phase 7

### Phase 6: Update Tests

**Files:** `src/v2/scan/mod.rs`, `src/v2/analyze/mod.rs` tests

1. Update test fixtures to use `DbErrorKind` instead of `DbError`
2. Update assertions that check error types
3. Add tests for `is_retryable()` logic
4. Verify retry logic handles different error kinds correctly

## Error Kind Taxonomy

### Retryable Errors (Transient)

| Kind | Cause | Action |
|------|-------|--------|
| `ConnectionRefused` | Network issue, DB restarting | Retry with backoff |
| `ConnectionTimeout` | Network latency, DB overloaded | Retry with backoff |
| `QueryTimeout` | Long-running query, DB busy | Retry once |

### Non-Retryable Errors (Permanent)

| Kind | Cause | Action |
|------|-------|--------|
| `AuthenticationFailed` | Wrong credentials | Mark node Unknown, log warning |
| `TlsHandshakeFailed` | Certificate issue, TLS config | Mark node Unknown, log error |
| `SslCertificateInvalid` | Expired/invalid cert | Mark node Unknown, log error |
| `InvalidHostname` | DNS issue | Mark node Unknown, log error |
| `InsufficientPrivileges` | User lacks permissions | Mark node Unknown, log error |

### Query Errors

| Kind | Cause | Action |
|------|-------|--------|
| `QuerySyntaxError` | Bug in SQL query | Panic (dev error) |
| `InvalidResponse` | Unexpected result format | Mark node Unknown, log error |

## Migration Strategy

### Step 1: Parallel Implementation
- Create new `DbErrorKind` alongside existing `DbError`
- Implement mapping functions
- Write unit tests for error classification

### Step 2: Update Connection Layer
- Change `connect()` signature
- Update all `From<T>` impls to return error-stack Reports
- Run tests to ensure no regressions

### Step 3: Update Callsites
- Update scan pipeline to use new error types
- Update retry logic to use `is_retryable()`
- Add context at boundaries

### Step 4: Remove Old DbError
- Delete old enum variants
- Update serialization in AnalyzedNode
- Update test fixtures

## Error Mapping Examples

### tokio_postgres::Error → DbErrorKind

```rust
fn classify_postgres_error(err: &tokio_postgres::Error) -> DbErrorKind {
    // Check for connection errors
    if err.is_closed() {
        return DbErrorKind::ConnectionRefused;
    }

    // Check for database-level errors (SQL state codes)
    if let Some(db_err) = err.as_db_error() {
        match db_err.code().code() {
            "28000" => DbErrorKind::AuthenticationFailed,
            "28P01" => DbErrorKind::AuthenticationFailed,
            "42501" => DbErrorKind::InsufficientPrivileges,
            "42601" => DbErrorKind::QuerySyntaxError,
            _ => DbErrorKind::QueryTimeout,  // Default for unknown DB errors
        }
    } else {
        // Check error message for patterns
        let msg = err.to_string();
        if msg.contains("timeout") {
            DbErrorKind::QueryTimeout
        } else if msg.contains("connection refused") {
            DbErrorKind::ConnectionRefused
        } else {
            DbErrorKind::ConnectionTimeout  // Generic fallback
        }
    }
}
```

### native_tls::Error → DbErrorKind

```rust
fn classify_tls_error(err: &native_tls::Error) -> DbErrorKind {
    let msg = err.to_string();
    if msg.contains("certificate") {
        DbErrorKind::SslCertificateInvalid
    } else if msg.contains("handshake") {
        DbErrorKind::TlsHandshakeFailed
    } else {
        DbErrorKind::TlsHandshakeFailed  // Default
    }
}
```

### std::io::Error → DbErrorKind

```rust
fn classify_io_error(err: &std::io::Error) -> DbErrorKind {
    match err.kind() {
        std::io::ErrorKind::ConnectionRefused => DbErrorKind::ConnectionRefused,
        std::io::ErrorKind::TimedOut => DbErrorKind::ConnectionTimeout,
        std::io::ErrorKind::NotFound => DbErrorKind::InvalidHostname,
        _ => DbErrorKind::ConnectionTimeout,  // Generic fallback
    }
}
```

## Benefits of This Approach

### For Machines (Automated Handling)

1. **Zero traversal** - `err.kind.is_retryable()` is O(1)
2. **Type safety** - Compiler enforces handling all error kinds
3. **Clear contracts** - Function signatures declare error kinds
4. **Easy to extend** - Add new kinds without breaking existing code

### For Humans (Debugging)

1. **Rich context** - Full operation chain visible in logs
2. **Location tracking** - `#[track_caller]` provides file:line automatically
3. **Lazy formatting** - `attach_printable_lazy` only allocates on error path
4. **Structured output** - error-stack provides both Debug and Display formatting

### For Testing

1. **Deterministic** - Test fixtures serialize only `DbErrorKind`
2. **Inspectable** - Can assert on specific error kinds
3. **Mockable** - Easy to inject specific error kinds in tests

## Anti-Patterns to Avoid

### Don't categorize by origin
```rust
// BAD
enum DbError {
    PostgresError(String),
    TlsError(String),
}
```

### Don't lose error structure
```rust
// BAD
impl From<tokio_postgres::Error> for DbError {
    fn from(err: tokio_postgres::Error) -> Self {
        DbError::Postgres(err.to_string())  // Lost all structure!
    }
}
```

### Don't add context inside error types
```rust
// BAD - context belongs at call site, not in error definition
enum DbError {
    ConnectionFailed { node_name: String, cluster_id: u32 },
}
```

### Don't use anyhow for library code
```rust
// BAD - anyhow doesn't enforce context addition
pub async fn connect(node: &Node) -> anyhow::Result<Client> {
    // Caller has no idea what errors are possible
}
```

### Do use error-stack with flat kinds
```rust
// GOOD
pub async fn connect(node: &Node) -> error_stack::Result<Client, DbError> {
    cfg.connect(connector)
        .await
        .change_context(DbError { kind: classify_error(&err) })
        .attach_printable_lazy(|| format!("node: {}", node.node_name))
}
```

## References

- ["Stop Forwarding Errors, Start Designing Them"](https://fast.github.io/blog/stop-forwarding-errors-start-designing-them/)
- [error-stack documentation](https://docs.rs/error-stack/)
- [PostgreSQL Error Codes](https://www.postgresql.org/docs/current/errcodes-appendix.html)
- [tokio-postgres error handling](https://docs.rs/tokio-postgres/latest/tokio_postgres/error/)

## Open Questions

1. **Should UnknownPrimary/UnknownReplica in Role enum carry DbErrorKind?**
   - Currently they're unit variants
   - Could be `UnknownPrimary(DbErrorKind)` to preserve why health check failed
   - Tradeoff: More memory, but better debuggability

2. **How to handle prometheus fetch errors?**
   - Currently they're just logged
   - Should they be represented in ClusterHealth::Unknown?
   - Probably not - prometheus is enrichment, not critical path

3. **Should analyze() return Result?**
   - Currently it always returns ClusterHealth (never fails)
   - This is probably correct - partial data is still useful
   - Unknown variants capture "couldn't determine" states

4. **Log level for different error kinds?**
   - Retryable: WARN (transient, expected)
   - Auth failures: ERROR (config issue)
   - Query syntax: FATAL (bug)
   - Need to document this policy

## Revision History

| Date       | Author         | Changes |
|------------|----------------|---------|
| 2026-01-27 | Robert Sjöblom | Initial design document created |
