
A concurrent shared-memory coordination substrate for multi-agent systems, in Go.
Lease-based claiming + CAS-versioned writes over a shared in-memory store, built
to answer one question **with numbers**: does a coordination layer measurably
eliminate duplicated agent work while keeping shared state consistent under
concurrency?

Short answer, on a 300-document corpus: duplicated annotation work drops from up
to **93.8%** (no coordination) to **0%** (lease-based claiming), with **zero lost
updates** verified by an automated invariant checker — not asserted — including
under injected failures, at **sub-millisecond** per-operation latency and **~83k
ops/sec** at 16 concurrent agents.

## What it is

Multiple agents working over a shared corpus waste effort and corrupt state: they
annotate the same documents twice, overwrite each other's writes, and act on
stale reads. Quorum is a small, benchmarked substrate that fixes exactly that,
using two mechanisms and nothing more:

- **Lease-based claiming with TTL** — an agent claims a document before working
  it; a dead agent's lease expires and the document becomes reclaimable.
- **Optimistic concurrency (version/CAS) on writes** — conflicting writes are
  detected and retried, never silently lost.

Agents talk to the store over a real **HTTP/JSON** boundary (not in-process
function calls), so the concurrency being measured is real. The agents
themselves are deliberately dumb — deterministic keyword extraction, no LLM — so
the numbers are reproducible and isolate the *substrate*, not agent cleverness.

## Quickstart

    go run ./cmd/quorum-store &                              # start the store on :8080
    go run ./cmd/quorum-swarm -agents 8 -corpus corpus/fixture.jsonl
    # 8 agents coordinate; each document is annotated exactly once.
    curl -s localhost:8080/findings | head
    kill %1

Reproduce the three headline results (each is a self-contained command):

    go run ./cmd/quorum-bench  -corpus corpus/arxiv.jsonl -agents 2,4,8,16 -runs 5   # duplication
    go run ./cmd/quorum-check  -corpus corpus/arxiv.jsonl -agents 16                 # consistency
    go run ./cmd/quorum-perf   -agents 2,4,8,16 -runs 5                              # latency/throughput

Run the test suite (race detector on):

    go test -race ./...

## Result 1 — Coordination eliminates duplicated work

    go run ./cmd/quorum-bench -corpus corpus/arxiv.jsonl -agents 2,4,8,16 -runs 5

Doc-level duplication rate (fraction of committed annotations that are redundant),
coordinated (lease-based claiming) vs. baseline (no coordination), N agents over
the arXiv corpus:

| Mode         |  N | findings | unique |  dup% |
|--------------|---:|---------:|-------:|------:|
| baseline     |  2 |      600 |    300 | 50.0% |
| coordinated  |  2 |      300 |    300 |  0.0% |
| baseline     |  4 |    1,200 |    300 | 75.0% |
| coordinated  |  4 |      300 |    300 |  0.0% |
| baseline     |  8 |    2,400 |    300 | 87.5% |
| coordinated  |  8 |      300 |    300 |  0.0% |
| baseline     | 16 |    4,800 |    300 | 93.8% |
| coordinated  | 16 |      300 |    300 |  0.0% |

The comparison is fair: both modes run the *same* agents, the *same* deterministic
annotator, and the *same* store — the only difference is whether coordination
(lease claiming + skipping already-annotated docs) is enabled. The version CAS
stays on in both modes, so duplicated work shows up as extra committed
annotations, not as lost updates. The numbers are deterministic:
`(N−1)/N` of baseline work is redundant.

## Result 2 — Consistency is checker-verified, not asserted

    go run ./cmd/quorum-check -corpus corpus/arxiv.jsonl -agents 16
    # -> ALL INVARIANTS HOLD (I1–I5)

`internal/checker` is a **pure function** over the append-only findings and
lease-event logs — no store, no clock, no HTTP — so it genuinely *replays* the
history rather than re-running the code that produced it. It verifies:

- **I1** no duplicate committed annotations (coordinated mode)
- **I2** no lost updates (every write's base version matched at commit; version
  chains have no gaps)
- **I3** lease mutual exclusion + every committed write was covered by a live
  lease held by its author
- **I4** recovery: a lease reclaimed after expiry is legal
- **I5** log integrity: sequence numbers monotonic from 1, no gaps

Crucially, **failure-injection tests feed the checker adversarial runs**: one
kills an agent mid-claim (a deterministic mock clock advances past the TTL,
another agent reclaims and commits); another forces real write conflicts under
8-way contention. Each test *asserts the injected condition actually occurred* — a
reclaim-after-expiry appears in the lease log; conflicts are non-zero — **before**
running the checker, so an injection can't pass vacuously.

## Result 3 — Latency & throughput

    go run ./cmd/quorum-perf -agents 2,4,8,16 -runs 5

Per-operation latency and aggregate throughput across N agents — race detector
**off** (it distorts timing), HTTP keep-alives **on**, `docs/agent=200`,
`passes=3`, **mean over 5 runs**. Ops/sec is `mean±stddev` (a single run swings
~30%, so a lone figure would overstate reproducibility); the p50/p95/p99
percentiles each aggregate `docs/agent × passes × agents` samples per run and are
stable, shown as the mean across runs. The workload gives each agent its own
private document keys, so there is no cross-agent lease/version contention — this
measures raw *substrate* operation cost (mutex + HTTP + allocation), not
coordination under contention (that is Result 1's job).

|  N |    ops |       ops/sec | claim p50/p95/p99 | write p50/p95/p99 | release p50/p95/p99 |
|---:|-------:|--------------:|-------------------|-------------------|---------------------|
|  2 |  3,600 | 39.8k ± 2.9k | 35 / 65 / 118 µs | 36 / 67 / 122 µs | 31 / 58 / 107 µs |
|  4 |  7,200 | 52.3k ± 2.1k | 49 / 126 / 194 µs | 49 / 130 / 201 µs | 44 / 118 / 185 µs |
|  8 | 14,400 | 70.8k ± 1.8k | 70 / 219 / 391 µs | 72 / 216 / 387 µs | 64 / 192 / 348 µs |
| 16 | 28,800 | 83.1k ± 2.8k | 111 / 414 / 727 µs | 112 / 416 / 736 µs | 104 / 369 / 680 µs |

**The one profiled optimization.** Load-testing didn't just produce numbers — at
N=16 it *failed*. `api.NewClient` built its `*http.Client` with a nil `Transport`,
so every agent fell back to Go's shared `http.DefaultTransport`, whose
`MaxIdleConnsPerHost` is 2. With N agents hammering one host, only 2 connections
pooled and the rest churned (open+close per request): at N=16 this exhausted
ephemeral ports and the run errored outright, and below that throughput was
non-monotonic (N=4 was *slower* than N=2). The fix gives each client its own
transport with `MaxIdleConnsPerHost=256`, so each agent reuses pooled
connections. After it, N=16 completes cleanly and throughput scales monotonically
(≈40k → 52k → 71k → 83k ops/sec, above).

## Architecture

```
agents ── HTTP/JSON ──▶ quorum-store (single process)
                          ├─ claim registry (leases, TTL)      ┐
                          ├─ versioned entry map (CAS)         ├─ one RWMutex
                          └─ append-only logs (findings +      ┘
                             lease events)  ◀── the checker replays these
```

- **The append-only logs are the source of truth.** Entry versions and lease
  state are derived; the logs are what the checker replays offline.
- **One mutex.** Lease state, entries, and log appends are all guarded by a single
  `sync.RWMutex` — no lock-ordering hazard by construction. (The obvious next step
  under contention is to shard it; see below.)
- **Time is injected.** All lease/TTL logic reads a `Clock` interface; tests use a
  mock clock and advance it deterministically instead of sleeping.

More detail: **[docs/TECHNICAL-WRITEUP.md](docs/TECHNICAL-WRITEUP.md)** ·
consolidated numbers + methodology: **[docs/BENCHMARK-REPORT.md](docs/BENCHMARK-REPORT.md)** ·
frozen invariants: **[INVARIANTS.md](INVARIANTS.md)**.

## Limitations (stated, not hidden)

- **In-memory, single node.** No disk durability, no multi-node replication.
  "Distributed" is a v2 word; this is a v1.
- **Benchmarks are localhost.** Store and agents share a machine over the loopback
  interface — real network latency would shift the absolute numbers (the
  *relative* coordination result would not).
- **Agents are deliberately dumb** (deterministic keyword extraction, no LLM), so
  the numbers isolate the substrate and stay reproducible.
- The latency workload has no cross-agent contention by design; contention
  behavior is characterized separately in Result 1.

## What I'd change next

Shard the single store mutex per document (or per hash bucket) to lift the
write-throughput ceiling; add write-ahead persistence for durability; move the
transport to gRPC for streaming and backpressure; and add a porcupine-style
linearizability check alongside the invariant checker. None are needed for the
thesis this project set out to prove — they're where it would go if it grew up.
