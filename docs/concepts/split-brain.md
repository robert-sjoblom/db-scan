# Split-brain: timelines, forks, and the "true primary" question

This explains the domain model the split-brain resolver depends on. It is **not** a decision record -- see [ADR-002](../adr/002-split-brain-resolution-refinement.md) for what we decided and [`src/v2/analyze/split_brain.rs`](../../src/v2/analyze/split_brain.rs) for the implementation. Read this when the resolver's reasoning stops feeling obvious; which, in this domain, is quickly.

## The setup

Three nodes, `synchronous_standby_names = 'ANY 1 (A, B)'`, `synchronous_commit = on`. Normally: one primary, two replicas. The tool runs **after** a failover, in stable-but-degraded state. The incident class is **slow fencing**: the old primary was supposed to be demoted but the fence didn't take, so a scan sees *two* primaries:

- **db001**: the pre-failover primary, still on timeline **TL=N**, still running because the fence failed ("zombie").
- **db002**: was a replica, got promoted to **TL=N+1**.
- **db003**: the other replica; its state is the interesting variable.

## The quorum-sync invariant (and its one failure mode)

Under `ANY 1 (A, B)` with `synchronous_commit = on`, a primary **does not acknowledge a client commit until a standby has flushed (fsync'd) that WAL.** Consequence: an *isolated* primary -- one with no live standby acking it -- physically cannot commit. This is why write divergence is normally *structurally* prevented: two primaries can't both be acking writes unless they each have a standby flushing for them.

The failure mode this whole subsystem exists for: in a 3-node cluster the two primaries share a single pool of possible standbys (just db003), so the invariant holds **only while db003 acks at most one of them**. The danger is entirely about *which* primary db003 was acking, and when.

## Timelines and forks

When db002 is promoted it starts a new **timeline** (TL=N+1) and records, in its `.history` file, the **switch LSN X** where TL=N ended:

```
TL=N  (db001, never fenced -- keeps writing past X):  …──●──────────●X──────────►  (more TL=N WAL)
                                                                    │ fork at X
TL=N+1 (db002, from promotion onward):                              └───────────►  (TL=N+1 WAL)
```

After X there are **two WAL branches sharing a common prefix up to X**. The same LSN past X means *different* records on TL=N vs TL=N+1. The fork LSN X is an objective fact, written into **db002's** `.history` (the newer timeline records where it split from the older). Reading X from db002's history says nothing about who is "right"; it's just where the fork is.

## "Divergent" is relative, not absolute

A node is never divergent in isolation -- only *relative to a chosen canonical branch*. db003, streaming TL=N from db001 with WAL past X, is:

- **convergent** with db001 (same TL=N stream, byte-for-byte -- it received that WAL from db001), and
- **divergent** from db002 (TL=N+1 lacks db003's TL=N tail past X).

Same node, same on-disk WAL, opposite answers. So "does this replica need a rebuild?" is undefined until you fix the canonical primary. This is the *verdict ≠ safety* distinction; see "Two questions, not one" below.

## The key inference: flushed-past-fork -> acknowledged writes

Because `synchronous_commit = on` makes the primary wait for a standby flush before acking, a replica whose **flushed/applied LSN is past the fork X on the lower timeline** is *proof* that the lower-TL primary **client-acknowledged** writes in `(X, flushed_lsn]`. Those are durable, confirmed-to-the-client transactions -- and the higher-TL primary, having forked at X, does not have them.

(If `synchronous_commit` is weakened, this inference breaks -- which is exactly why weakened `synchronous_commit` is its own hard `Refuse` gate, ADR-002 §2. Past that gate, the inference holds.)

## Two questions, not one

Keep these separate or you will tie yourself in knots:

1. **Which primary is "true"?** Operational definition: the one whose sync quorum is satisfied and that is *actively committing*. This can be the **lower-TL** primary -- if db003 is flushing for db001 while db002 is isolated, db001 is true even though db002 has the higher timeline.
2. **Could acknowledged writes be lost by acting on the verdict?** A separate axis. Even a correct "which primary" answer can hide the fact that acked writes exist on a branch the apparent winner lacks.

`DivergentReplicaWal` is the finding for question 2. It is a **committed-write-divergence safety gate**, *not* a "which replica do I rebuild" pointer.

## Anchoring the divergence check: the inter-primary fork, not the winner

To detect "are there acked writes on the lower branch past the split", compare the replica's applied LSN against the **inter-primary fork X** (read from the higher-TL primary's `.history`). Do **not** anchor the check to whichever node the resolver currently calls `true_primary`:

| Anchor | Question it asks | Answer for db003 (on TL=N, past X) |
|---|---|---|
| inter-primary fork X | "past where the branches split?" | **Yes** -> fires |
| `true_primary`'s lineage | "diverges from the winner?" | once db001 wins, db003 is *on* db001's lineage -> **No** -> never fires |

Anchoring to the winner suppresses the signal precisely when there are acked writes to protect (db003 sits on the winning lower-TL lineage). Anchoring to the fork asks the right question -- "did writes get acked past the split?" -- independent of the verdict. **Using db002's history to read X is not the same as treating db002 as canonical**.

## The 3-node proof: "is db002's fork empty?" is decidable

In the split-brain scope there are exactly two candidate primaries and one replica. db002's quorum can be satisfied **only by db003** (a peer primary isn't its standby; a primary isn't its own). A replica is on one timeline at a time. So:

> If db003 is observably on TL=N (acking db001), then db002 had no acker and -- under `synchronous_commit = on` -- **provably committed nothing on TL=N+1.** Its fork is empty.

This is why, when db003's allegiance is observable, the verdict can be **confident** ("keep db001, fence db002 -- its fork is empty"), not merely conservative. The whole "is this safe?" question reduces, in a 3-node cluster, to "is db003 acking db002?".

## Remediation direction: keep the lower TL

When db003's WAL proves acked writes on TL=N that db002 lacks, the correct action is **keep the lower TL (db001), discard/rebuild the higher TL (db002).** db002 was isolated and committed nothing on its fork (the 3-node proof) -- it is the empty branch. The divergent node to *rebuild* is db002; db003 re-points to db001. The intuitive "promote the higher timeline" is exactly the data-losing move here.

In this cluster, "rebuild" means **tear down and re-basebackup from the true primary, not `pg_rewind`.** `pg_rewind` would rejoin a divergent node by rewinding it to the fork, but the operational policy here is a clean basebackup; the tool's job is only to name the divergent node and the canonical source: it does not attempt an in-place reconciliation. (Whether the basebackup comes from archive or a fresh copy is operator judgment based on archive integrity.)

## Observability is the hinge

The verdict you can give depends on what you can *see* of db003:

- **db003's allegiance observable** (e.g. it's streaming db001): run the 3-node proof -> **confident** lower-TL verdict.
- **db003 unobservable** (timeline-wedged, no `wal_receiver`): you cannot prove db002's fork is empty -> **`Refuse`** (decline to auto-resolve).

This is why the dangerous case is hard: a replica wedged on a timeline divergence may expose *no* `wal_receiver` at all, so the very evidence we need (`received_tli`/`flushed_lsn`) is absent. Data-loss danger and `wal_receiver`-based observability are **anti-correlated** -- the case that most needs detecting is the one streaming stats can't see. Closing that gap means reading the replica's position from the **control file** (`pg_control_checkpoint().timeline_id`, `pg_last_wal_replay_lsn()`), which survives with no receiver. As of this writing we have **no captured run** of the wedged state, so divergence detection is deferred behind capturing that evidence first (ADR-002 §7).

**What "wedged" looks like in the logs.** When db003 is re-pointed at db002 while already past X on TL=N, db002 refuses to stream it -- db003 *is* ahead of where TL=N+1 forked. The standby logs a `FATAL` of the form:

```
new timeline N+1 forked off current database system timeline N before current recovery point X/X
```

possibly followed, on retry via archive, by `requested timeline N+1 does not contain minimum recovery point …`. The receiver never reaches a healthy streaming state, which is why `pg_stat_wal_receiver` ends up empty or only transiently populated: **the divergence is loud in the server log but quiet in the stats views the scanner reads.** (The log signature is taken from a PostgreSQL bug report -- [BUG #8294](https://www.postgresql.org/message-id/E1Ux7WL-0001Hi-De@wrigleys.postgresql.org), whose subject line is this exact message -- not yet from one of our own runs; the precise `pg_stat_wal_receiver` contents in this state are likewise unconfirmed, which is part of what the §7 capture is meant to pin down.)

## Pointers

- Decisions & case matrix: [ADR-002](../adr/002-split-brain-resolution-refinement.md) (esp. the matrix rows C-a…C-g and §7).
- Implementation: [`src/v2/analyze/split_brain.rs`](../../src/v2/analyze/split_brain.rs).
