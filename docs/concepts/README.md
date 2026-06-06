# Concepts

Explanation docs for the *domain model* behind tricky parts of this tool: the things you need to hold in your head to read the code or an ADR and not get them wrong. This domain is hella hard to wrap your head around, and there are many edge-cases to keep track of. So we'll save the concepts here.

**How this differs from neighbouring docs:**

- **ADRs (`docs/adr/`)** record *decisions*: what we chose, the alternatives, the trade-offs, at a point in time. They assume you already understand the domain.
- **Concepts (here)** explain the *domain itself*: the invariants, the failure modes, the mental model. They're durable background, not decisions. An ADR may supersede a decision without changing the concepts underneath it.

When a correct understanding depends on a chain of facts that isn't visible in any single file, we'll add a document here. Rule: we must keep them grounded in a running example. Also good if we can link to the ADR for decisions, and maybe code for the implementation.

## Index

- [split-brain.md](split-brain.md) — timelines, forks, divergence, and why the post-failover "true primary" question is subtler than "highest timeline wins".
