# Quorum — Portfolio Material

Drafts for a resume and an interview. Adapt the voice as you like; every claim
below is backed by the code and [BENCHMARK-REPORT.md](BENCHMARK-REPORT.md).

## Résumé bullets

Pick one or two.

- **Built a concurrent shared-memory coordination substrate for multi-agent
  systems in Go** — lease-based claiming with TTL + CAS-versioned writes over a
  shared store behind a real HTTP/JSON boundary — that cut duplicated agent work
  from up to **93.8%** (no coordination) to **0%** across 2–16 concurrent agents,
  with **zero lost updates verified by an automated invariant checker** (not
  asserted) even under injected agent failures, at **sub-millisecond** p50 latency
  and **~83k ops/sec**.

- **Designed and load-tested a Go coordination layer** proving a shared-memory
  substrate eliminates duplicated multi-agent work; profiling under load surfaced
  and fixed an HTTP connection-pool exhaustion bug that made throughput scale
  monotonically to ~83k ops/sec at 16 agents; consistency verified by a
  log-replaying invariant checker whose own two false-pass bugs I caught and fixed
  with regression tests.

## Two-minute interview story

**The premise (15s).** Multiple agents over a shared corpus waste effort — they
annotate the same documents twice and overwrite each other's writes. I wanted to
prove, with numbers, that a small coordination layer fixes that without becoming a
database. So I built Quorum in Go: lease-based claiming plus optimistic-concurrency
writes over a shared store, with agents talking to it over real HTTP.

**The headline result (20s).** On 300 documents, with no coordination, N agents
each annotate everything — at 16 agents, 94% of the committed work is redundant.
Turn on lease claiming and it drops to exactly zero duplication, at every agent
count, while the version CAS keeps the log consistent. The comparison is fair on
purpose: same agents, same store, same deterministic annotator — the only variable
is whether coordination is on.

**The part I'm proudest of (40s).** I didn't want to just *assert* consistency, so
I wrote an invariant checker that's a pure function over the append-only logs — it
replays history instead of trusting the code that produced it — and I fed it
adversarial runs: kill an agent mid-claim and let its lease expire, force write
conflicts under contention. Each injection test asserts the bad thing actually
happened *before* it checks the invariants, so it can't pass vacuously. And the
lesson that stuck: when I had that checker adversarially reviewed, it had two
false-pass bugs — cases where it would have missed a real violation. A checker with
a false pass is worse than no checker, because it launders bugs into green
checkmarks. I fixed both with regression tests. The interesting bugs were never in
the feature — they were in the seams between changes and in the code that verifies
correctness.

**The systems find (25s).** For performance I measured p50/p95/p99 per operation
under load — and at 16 agents the load test didn't just give me a slow number, it
*failed* with port exhaustion. The client was sharing Go's default HTTP transport,
which pools only two connections per host, so under concurrency it churned
connections instead of reusing them. Gave each client its own transport, and
throughput went from failing to scaling monotonically to ~83k ops/sec. It's the
kind of bug you only find by actually pushing the thing until it breaks.

**The honesty note (20s).** I held myself to strict benchmark rules — race
detector off for timing but on for correctness, variance reported over five runs,
the duplication metric frozen before I saw any number, and the limitations stated
plainly: it's in-memory, single-node, and the benchmarks are localhost. The
relative result — coordination takes duplication to zero — is structural and would
survive a real network; the absolute latencies wouldn't, and I say so.

## Anticipated interview questions

- **"How did you measure that?"** — Everything's reproducible from a clean
  checkout with four commands; duplication is deterministic and exact, latency is
  mean±stddev over five runs. See the benchmark report.
- **"Isn't the baseline a strawman?"** — No: both modes share the exact code path
  minus the two coordination mechanisms, same annotator, same store. Baseline
  duplication is the honest worst case of N agents each told to process the whole
  corpus.
- **"Why one mutex?"** — Correctness-first for a v1, and the profiler showed the
  bottleneck was HTTP pooling, not lock contention, at these scales. Sharding it
  is the first scaling change I'd make.
- **"What would you do differently?"** — Shard the lock, add write-ahead
  persistence, move to gRPC, and add porcupine-style linearizability checking. All
  in [TECHNICAL-WRITEUP.md](TECHNICAL-WRITEUP.md).
