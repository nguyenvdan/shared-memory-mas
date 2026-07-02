# Quorum Invariants (I1–I5)

These are the spec. The Phase 4 checker is written to satisfy these; code is
never allowed to redefine them. All are checked by replaying the append-only
findings log (plus, from Phase 2, the lease event log).

- **I1 — No duplication (coordinated mode):** each `doc_id` has at most one
  committed annotation. `duplicate(a, b) := a.doc_id == b.doc_id`.
- **I2 — No lost updates:** every committed write's `base_version` equalled the
  entry's live version at commit time; for each doc, the number of committed
  writes equals its final `committed_version`, with no gaps.
- **I3 — Lease mutual exclusion (Phase 2):** at most one live (unexpired) lease
  per doc at any instant; every committed annotation's author held a valid
  lease at commit time.
- **I4 — Recovery (Phase 2):** after a lease expires or is released, the doc is
  reclaimable, and a reclaim produces a valid subsequent annotation.
- **I5 — Log integrity:** sequence numbers are strictly monotonic starting at 1
  with no gaps; the log is replayable to reconstruct entry state.

Phase 1 exercises I2 and I5 (single-agent). I1, I3, I4 come online in Phase 2.
