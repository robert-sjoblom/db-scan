  1. reference_sysid isn't a general cluster property. Its definition is "the sysid agreed by ≥2 candidate primaries", which
  only has meaning during split-brain resolution. In a healthy cluster (one primary) it would return None and mean nothing. On
  Cluster, it would look like a general accessor but behave like split-brain-internal logic.
  2. mismatched_sysid_nodes takes pre-filtered &[&AnalyzedNode] slices (primaries vs replicas already separated) and a
  reference parameter. That's a resolver-step shape, not a "give me X from the cluster" shape.

  The project's style guideline ("no abstractions for single-use code", "surgical changes") points the same way: these are
  called only from resolve_split_brain and belong next to it.

  That said, if your real itch is that split_brain.rs is becoming a pile of free functions reading deep into AnalyzedNode/Role,
   the right move isn't pushing helpers onto Cluster — it's deepening the resolver itself. Something like:

  struct SplitBrainResolver<'a> {
      primaries: &'a [&'a AnalyzedNode],
      replicas: &'a [&'a AnalyzedNode],
      scan_start: DateTime<Utc>,
      findings: Vec<SplitBrainFinding>,
      confidence: Confidence,
  }

  impl<'a> SplitBrainResolver<'a> {
      fn reference_sysid(&self) -> Option<&str> { ... }
      fn filter_foreign_sysid_replicas(&mut self) -> Vec<&AnalyzedNode> { ... }
      fn check_synchronous_commit(&mut self) { ... }
      fn build_following_map(&mut self, replicas: &[&AnalyzedNode]) -> HashMap<...> { ... }
      fn resolve(mut self) -> SplitBrainInfo { ... }
  }

---

## TODO: systemd service health check (`--check-services`)

**Status:** designed 2026-08-27, not started. Lands after ADR-002 commits 11-12.

**Why:** a prod host had pgbouncer down after a reboot and db-scan reported the cluster
healthy. Postgres itself was fine, so no existing check could see it.

### Collection

Mirror `--check-disks` exactly: new `--check-services` flag, same `ssh_user`, own module
`src/v2/scan/service_check.rs` modelled on `disk_check.rs` (same 10s connect/command
timeouts, `connect_mux`, `KnownHosts::Accept`). Two commands on one session:

```
systemctl list-unit-files --no-legend --no-pager 'postgresql*' 'pgbouncer*' 'repmgr*' 'pg-cluster-health*'
systemctl is-active <unit names from half one>
```

Half one is the authoritative inventory (token 0 of each line). Half two supplies state:
one word per line, one line per argument, in argument order -- zip positionally. Count
mismatch -> `ServiceCheckOutcome::Failed`; never mis-attribute one unit's state to another.

Match on glob shape, not names: units are version-suffixed (`postgresql-17.service`,
`repmgr-17.service`), so a hardcoded name list breaks on upgrade. An earlier draft used
`repmgrd*`, which matches nothing on these hosts -- a glob that matches nothing monitors
nothing, silently. Five units on the reference host: `pgbouncer`, `pgbouncer_exporter`,
`postgresql-17`, `repmgr-17`, `pg-cluster-health`.

**Down = `inactive` or `failed`.** `activating`/`deactivating` are ignored so a node
mid-restart does not flap. An armed timer reports `active`, so timers need no special rule.

### Gotchas confirmed against a real host (do not "simplify" these away)

- `list-unit-files` carries a PRESET column here: `pgbouncer.service enabled disabled` is
  STATE + PRESET, not two units. Taking only token 0 is immune.
- `list-units --all` returns phantom rows: `postgresql-17.target  not-found inactive dead`,
  prefixed with a bullet. Under an "inactive = down" rule that is a false positive on every
  host in the fleet. Joining on `list-unit-files` excludes it structurally -- no unit file,
  so it never appears there.
- `list-units` prefixes a marker glyph on non-ok rows only (`*` U+25CF, and U+00D7 for
  failed units on v250+), so it is absent from every healthy fixture and appears exactly
  when something is wrong. Sidestepped by not using `list-units` at all.
- `list-units --all` can omit a unit that was never loaded, so a disabled-and-stopped
  pgbouncer could yield zero rows and zero findings -- the exact incident this feature is
  for. That is why `list-unit-files` is the inventory, not merely a source of enablement.
- `systemctl show -p Id -p Type -p ActiveState` returns properties in systemd's own order
  (`Type` before `Id`), not the requested order. Unused in the final design; relevant if a
  future need for `Type` (oneshot detection) brings it back.

### Decisions

- Enablement is not reported -- `enabled` is the default state on every host.
- Only unhealthy units surface. Consequence: no new report column, since it would read `-`
  on every healthy cluster and duplicate the reason text everywhere else. `ClusterView`,
  `csv.rs` and the terminal writer stay untouched.
- Glob list hardcoded in one `const SERVICE_GLOBS`, not config. It already grew by one entry
  during design; if it grows a few more times, moving it to the config file is contained.
- Severity: `Reason::ServiceDown` in the Degraded tier, ranked between `ArchiveLagging` and
  `ReducedRedundancy`. A stopped daemon is a present, actionable fault that should outrank
  lag and archive-lagging, but an unreachable node should still headline.

### Files

- New `src/v2/scan/service_check.rs`: `ServiceCheckOutcome::{Checked(Vec<ServiceState>),
  Failed { reason }}`, `ServiceState { unit, active }`, one pure parser per half.
- `src/v2/scan.rs`: `pub mod service_check`, `AnalyzedNode.service_check`, task spawned
  beside the disk check (~`:61`), `collect_service_check` (~`:212`), patch sites `:114`,
  `:169`, `:205`.
- `service_check: None` beside each existing `disk_check: None`: `v2.rs:408`,
  `health_check_primary.rs:255,270`, `health_check_replica.rs:134,149`, `scan.rs:151`.
- `src/v2/analyze.rs`: `NodeVerdict::ServiceDown { unit, state }`; call `check_services`
  at `:341`.
- `src/v2/analyze/checks.rs`: `check_services`, modelled on `check_disk_errors` (`:270`).
- `src/v2/analyze/classify.rs`: `From<&NodeVerdict>` arm, Degraded arm of
  `From<&Reason> for Tier`, plus the test-side `severity_rank` entry and ladder pair that
  the convention documented at `:104-115` requires.
- `src/v2/writer/build.rs`: `format_reason` arm modelled on `Reason::DiskIoErrors` (`:641`),
  plus `ServiceDown` added to the exhaustive `Reason` matches at `:227` and `:343`.
- `src/config.rs:150` and `src/main.rs:68`: `--check-services` plus the missing-`ssh_user`
  warning, via `eprintln!` (no tracing at config time).

### Tests (TDD, parsers first)

Parser fixtures from real host output: three-line `list-unit-files` with the PRESET column;
`is-active` with `failed` and `inactive` lines; count mismatch -> `Failed`; empty output.
`check_services` verdict tests beside the `check_disk_errors_*` ones. Classify cases for
verdict mapping, Degraded tier, and the severity rung. One writer short-string test.

### Out of scope

- `systemctl list-units --state=failed --type=service` sweep for unknown-unknowns. Safe and
  cheap -- oneshots that succeed read `inactive`, not `failed`, so no mass false positives --
  but left out to keep this tight. Note the general "flag any enabled service that is not
  active" rule does NOT work: `Type=oneshot` units legitimately sit `inactive (dead)`.
- Config-driven unit list. Per-unit or per-role severity (e.g. pgbouncer down on the primary
  as Critical). Any restart or remediation action.

### Assumptions to revisit

`pgbouncer_exporter` is worth flagging when down (dead exporter = blind dashboards). No host
has pgbouncer installed but deliberately stopped. Two SSH connects per node when both checks
are enabled -- folding them into one session means rewriting `disk_check`.

---

## TODO: which DC each machine is in

Clobbered from this file on 2026-06-07; original line was "TODO: we need to see which dc
each machine is in" (recoverable from commit 673b577's parent). Probably needs no SSH: the
reference host is `prod-pg-app048-db001.sto2.fnox.se`, so the site code is in the FQDN. But
inventory's `Node` carries only `node_name` (`prod-pg-app048-db001`, no domain) and
`ip_address` -- so check whether the portal can supply the FQDN, or whether the DC is
derivable from the IP range, before reaching for SSH.