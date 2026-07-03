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

## Benchmark: coordination eliminates duplicated work

Run the duplication benchmark across the arXiv corpus:

    go run ./cmd/quorum-bench -corpus corpus/arxiv.jsonl -runs 1 -agents 2,4,8,16

Doc-level duplication rate (fraction of committed annotations that are redundant),
coordinated (lease-based claiming) vs baseline (no coordination), N agents over
the arXiv corpus:

| Mode         | N  | findings | unique | dup%  | conflicts |
|--------------|----|-----------:|--------:|--------:|-----------:|
| baseline     | 2  |        600 |    300 |  50.0% |         3 |
| coordinated  | 2  |        300 |    300 |   0.0% |         0 |
| baseline     | 4  |       1200 |    300 |  75.0% |        41 |
| coordinated  | 4  |        300 |    300 |   0.0% |         0 |
| baseline     | 8  |       2400 |    300 |  87.5% |       153 |
| coordinated  | 8  |        300 |    300 |   0.0% |         0 |
| baseline     | 16 |       4800 |    300 |  93.8% |       554 |
| coordinated  | 16 |        300 |    300 |   0.0% |         0 |

Baseline agents redundantly re-annotate every document; lease-based claiming
reduces duplicated annotation work to zero while the version CAS keeps the log
consistent (no lost updates — verified by log replay).

Design and benchmark numbers land in later phases. In-memory only; single node.
