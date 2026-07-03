# Quorum — Benchmark Report

Consolidated results and methodology for the three claims Quorum makes:
coordination eliminates duplicated work, consistency holds under concurrency and
injected failure, and per-operation latency stays sub-millisecond. Everything
here is reproducible from a clean checkout with the commands shown.

## Environment

- Language/runtime: Go (standard library only; no third-party dependencies).
- Corpus: ~300 arXiv abstracts (`corpus/arxiv.jsonl`), fetched once and committed
  as a frozen snapshot with a content hash (`corpus/MANIFEST`); benchmarks read
  the committed file, never the live API.
- Concurrency model measured: in-process goroutine agents, each with its own HTTP
  client, over the loopback interface to a single store process. Disclosed as
  loopback HTTP — not a direct-call fast path.
- Machine: single host, in-memory store.

## Methodology and honesty rules

- **Fair baseline.** The coordination-off (baseline) mode runs the *same* agents,
  the *same* deterministic annotator, and the *same* store as coordinated mode —
  only lease claiming and the already-annotated skip are disabled. The version
  CAS stays on in both modes.
- **Deterministic annotation.** Keyword extraction, no LLM, so duplication counts
  are exact and reproducible.
- **Race detector off for latency numbers** (it distorts timing); **on for all
  correctness tests** (`go test -race ./...`).
- **Variance reported.** Latency throughput is the mean ± population stddev over 5
  runs; a single run's ops/sec swings ~30%, so a lone figure would overstate
  reproducibility.
- **Metric frozen before measurement.** "Duplicate" was defined (doc-level: a
  document committed more than once) before any number was seen.

---

## 1. Duplication — coordination vs. baseline

Command:

    go run ./cmd/quorum-bench -corpus corpus/arxiv.jsonl -agents 2,4,8,16 -runs 5

Duplication rate = `(total committed findings − unique documents) / total
findings`, computed by replaying the findings log.

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

**Reading it.** Baseline agents each annotate every one of the 300 documents, so N
agents produce ~300N committed findings of which 300 are unique — `(N−1)/N` is
redundant work. Coordinated agents claim before working and skip already-done
documents, so exactly 300 findings are committed (one per document) at every N:
duplication is 0%. The result is deterministic (structural, not sampled). No
duplicate write is silently dropped — baseline `findings = 300N` exactly confirms
every duplicate committed, so the metric is not undercounting.

---

## 2. Consistency — invariant checker + failure injection

Command:

    go run ./cmd/quorum-check -corpus corpus/arxiv.jsonl -agents 16
    # -> ALL INVARIANTS HOLD (I1–I5)

The checker (`internal/checker`) is a pure function over the append-only findings
and lease-event logs. It verifies:

| Invariant | Statement | Scope |
|-----------|-----------|-------|
| I1 | No duplicate committed annotations | coordinated |
| I2 | No lost updates (version chains replay with no gaps) | always |
| I3 | Lease mutual exclusion; every write covered by its author's live lease | coordinated |
| I4 | A lease reclaimed after expiry is legal | coordinated |
| I5 | Log integrity: monotonic sequence from 1, no gaps | always |

Every coordinated concurrent run in the test suite is checker-verified. Two
failure-injection tests feed the checker adversarial histories and **assert the
injected condition actually occurred before checking**:

- **Dead agent mid-claim.** An agent claims and never releases; a mock clock
  advances past the TTL; another agent reclaims and commits. The test asserts a
  reclaim-after-expiry event exists in the lease log, then the checker confirms
  I1–I5 hold.
- **Forced write conflicts.** Eight agents contend on every document with the
  lease guard disabled. The test asserts the observed conflict count is non-zero,
  then the checker confirms no lost updates (I2) and clean version chains.

The checker was itself adversarially reviewed and two false-pass holes (phantom
coverage after a shorter re-claim; the renew-after-expiry twin) were found and
fixed with regression tests — details in [TECHNICAL-WRITEUP.md](TECHNICAL-WRITEUP.md).

---

## 3. Latency & throughput

Command:

    go run ./cmd/quorum-perf -agents 2,4,8,16 -runs 5

Conditions: race detector off, HTTP keep-alives on, `docs/agent=200`, `passes=3`,
mean over 5 runs. Ops/sec is mean ± stddev; percentiles are the mean of per-run
percentiles (each per-run percentile aggregates `docs/agent × passes × agents`
samples and is stable). Each agent uses private document keys, so there is no
cross-agent contention — this isolates substrate operation cost.

|  N |    ops |       ops/sec | claim p50/p95/p99 | write p50/p95/p99 | release p50/p95/p99 |
|---:|-------:|--------------:|-------------------|-------------------|---------------------|
|  2 |  3,600 | 39.8k ± 2.9k | 35 / 65 / 118 µs | 36 / 67 / 122 µs | 31 / 58 / 107 µs |
|  4 |  7,200 | 52.3k ± 2.1k | 49 / 126 / 194 µs | 49 / 130 / 201 µs | 44 / 118 / 185 µs |
|  8 | 14,400 | 70.8k ± 1.8k | 70 / 219 / 391 µs | 72 / 216 / 387 µs | 64 / 192 / 348 µs |
| 16 | 28,800 | 83.1k ± 2.8k | 111 / 414 / 727 µs | 112 / 416 / 736 µs | 104 / 369 / 680 µs |

**The one profiled optimization.** Load-testing at N=16 initially *failed* with
ephemeral-port exhaustion: `api.NewClient` left `http.Client.Transport` nil, so
all agents shared Go's global `http.DefaultTransport` (`MaxIdleConnsPerHost=2`),
churning connections instead of reusing them. Giving each client its own transport
with `MaxIdleConnsPerHost=256` fixed it — N=16 completes and throughput scales
monotonically (≈40k → 52k → 71k → 83k ops/sec). Profile captured with the CLI's
`-cpuprofile` flag and `go tool pprof`.

Absolute latencies are loopback-HTTP numbers on a single machine; real network
latency would raise them. The relative duplication result in §1 is independent of
this.

---

## Reproducing everything

    git clone <repo> && cd shared-memory-mas
    go test -race ./...                                                   # correctness (all invariants)
    go run ./cmd/quorum-bench -corpus corpus/arxiv.jsonl -agents 2,4,8,16 -runs 5
    go run ./cmd/quorum-check -corpus corpus/arxiv.jsonl -agents 16
    go run ./cmd/quorum-perf  -agents 2,4,8,16 -runs 5

The duplication percentages are deterministic and will match exactly. The latency
throughput will vary with the machine; the *shape* (sub-ms per op, monotonic
scaling) is what reproduces.
