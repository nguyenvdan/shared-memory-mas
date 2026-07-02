# Quorum

A concurrent shared-memory coordination substrate for multi-agent systems (Go).
Lease-based claiming + CAS-versioned writes over a shared in-memory store.

Phase 1 (single-agent slice) is runnable today:

    go run ./cmd/quorum-store &                 # start the store on :8080
    go run ./cmd/quorum-agent -corpus corpus/fixture.jsonl
    curl -s localhost:8080/findings | head      # see the annotations

Phase 2 adds lease-based claiming + CAS-retry and a concurrent swarm:

    go run ./cmd/quorum-store &
    go run ./cmd/quorum-swarm -agents 8 -corpus corpus/fixture.jsonl
    # N agents coordinate; each doc is annotated exactly once (no duplication).

Design and benchmark numbers land in later phases. In-memory only; single node.
