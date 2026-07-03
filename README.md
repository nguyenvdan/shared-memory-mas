

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

    go run ./cmd/quorum-bench -corpus corpus/arxiv.jsonl -runs 5 -agents 2,4,8,16

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
consistent (no lost updates — verified by log replay in the test suite).

`findings`, `unique`, and `dup%` are deterministic by construction; the
`conflicts` column is a scheduling-dependent mean across runs (representative,
not exact).

Design and benchmark numbers land in later phases. In-memory only; single node.

## Consistency: checker-verified, not asserted

    go run ./cmd/quorum-check -corpus corpus/arxiv.jsonl -agents 16

Replays the append-only findings + lease-event logs and verifies I1–I5 (no
duplicate committed annotations, no lost updates, lease mutual exclusion,
expiry recovery, log integrity). Failure-injection tests kill an agent
mid-claim and force write conflicts, then run the same checker — the injected
condition is asserted to actually occur before the invariants are checked.

## Latency & throughput

    go run ./cmd/quorum-perf -agents 2,4,8,16 -runs 5

Per-operation latency and aggregate throughput across N agents (race off,
keep-alives on, docs/agent=200, 3 passes), **mean over 5 runs**. Ops/sec is
reported as mean±stddev across those runs — a single run swings ~30%
run-to-run, so a lone figure overstates reproducibility. Percentiles
aggregate docs/agent×passes×agents samples per run and are stable, so
they're shown as the mean across runs. The workload uses per-agent private
doc keys (no cross-agent contention), so this measures substrate op cost,
not contention behavior:

   N       ops          ops/sec | claim p50/p95/p99      | write p50/p95/p99      | release p50/p95/p99
----------------------------------------------------------------------------------------------------------
   2      3600    39764±2903   | 35µs/65µs/118µs        | 36µs/67µs/122µs        | 31µs/58µs/107µs
   4      7200    52306±2147   | 49µs/126µs/194µs       | 49µs/130µs/201µs       | 44µs/118µs/185µs
   8     14400    70763±1830   | 70µs/219µs/391µs       | 72µs/216µs/387µs       | 64µs/192µs/348µs
  16     28800    83144±2815   | 111µs/414µs/727µs      | 112µs/416µs/736µs      | 104µs/369µs/680µs

**One optimization:** `api.NewClient` used to build its `*http.Client` with a
nil `Transport`, so every agent fell back to Go's shared
`http.DefaultTransport`, whose `MaxIdleConnsPerHost` is 2. With N agents
hammering one host, only 2 connections pooled per client and the rest churned
(open+close per request). At N=16 this exhausted ephemeral ports and the run
failed outright; throughput below that was also non-monotonic (N=4 was slower
than both N=2 and N=8). The fix gives each client its own transport (cloned
from the default) with `MaxIdleConnsPerHost` raised to 256, so each agent
reuses its own pooled connections instead of contending over a global
2-connection pool. After the fix, N=16 completes cleanly and throughput scales
monotonically with N (32455 → 48216 → 64294 → 79937 ops/sec).
