# Quorum — Technical Writeup

A concurrent shared-memory coordination substrate for multi-agent systems, in Go.
This document covers the design decisions, how failure is handled, honest
limitations, and what I would change. For the numbers, see
[BENCHMARK-REPORT.md](BENCHMARK-REPORT.md); for the run commands, the
[README](../README.md).

## The problem, precisely

Give N agents a shared corpus and tell each to annotate it, and without
coordination they each annotate everything — the work is duplicated N times.
They also race on shared state: two agents writing the same entry can lose an
update. Quorum is the thin layer that makes uncoordinated-looking agents stop
duplicating work and stop losing writes, and it proves both claims with
measurements rather than assertions.

The scope is deliberately narrow. It is **not** a database, a RAG system, a
governance/permissions layer, or a distributed system. It is two coordination
mechanisms over a shared store, benchmarked honestly.

## Design decisions (and the tradeoffs)

**Two mechanisms, no more: leases + CAS.** Lease-based claiming (with a TTL)
handles *who works what* and survives dead agents; optimistic
concurrency (a version compared-and-swapped on every write) handles *conflicting
writes*. These are the minimum needed to kill duplication and lost updates.
Everything else (roles, persistence, distribution) was kept out — the
out-of-scope list was treated as a contract, because the fastest way to sink a
small systems project is to let it balloon.

**A real HTTP/JSON boundary, even for in-process agents.** The agents run as
goroutines, but they reach the store only over `net/http` on the loopback
interface — there is no direct-function-call fast path. This matters for
honesty: if goroutine agents called the store directly, the benchmark would be
measuring something I couldn't legitimately call concurrent coordination over a
shared service. The cost is real HTTP latency in the numbers; the benefit is that
the numbers mean what they say.

**The append-only logs are the source of truth.** The store keeps a derived
version map and a live claim registry for speed, but the ground truth is two
append-only logs: committed *findings* (writes) and *lease events*
(claim/renew/release). Everything the consistency story rests on is
reconstructable by replaying those logs offline. This is what makes consistency
*checkable* rather than *asserted* (see Failure handling).

**One mutex, on purpose.** Lease state, the entry version map, and log appends
are all guarded by a single `sync.RWMutex`. A finer-grained scheme (per-doc
locks, lock-free structures) would scale better under write contention, but it
would also introduce lock-ordering hazards and the exotic-bug surface that sinks
concurrent code. For a v1 whose job is to be *correct and measured*, one mutex
with an obvious critical section was the right call — and the profiler agreed the
bottleneck was elsewhere (HTTP connection pooling), not lock contention, at these
scales. Sharding it is the first thing I'd do to raise the throughput ceiling.

**Time is injected, never read directly.** Every piece of lease/TTL logic reads a
`Clock` interface. Production uses a real clock; tests use a mock clock and
*advance it* to cross a TTL boundary. This is the single most valuable testing
decision in the project: lease-expiry and dead-agent-recovery tests are
deterministic and instant, with no `time.Sleep` and no flakiness — the classic
failure mode of time-based concurrency tests.

**Deterministic, dumb agents.** The annotator is ~20 lines of keyword extraction,
no LLM. This keeps the benchmark numbers reproducible (no model nondeterminism)
and isolates the substrate — the result is "even completely dumb agents stop
duplicating work once the substrate is underneath them," which is a stronger
claim than "smart agents coordinate."

## Failure handling — the unfakeable part

The project's headline consistency claim is "zero lost updates, verified by an
invariant checker, not asserted." Making that real drove several decisions.

**A pure checker over the logs.** `internal/checker` takes the two log slices and
a `coordinated` flag and returns a report of invariant violations. It imports no
store, no clock, no HTTP — it *replays* the recorded history. It verifies I1 (no
duplicate committed annotations, coordinated mode), I2 (no lost updates — version
chains replay cleanly), I3 (lease mutual exclusion, and every committed write was
covered by a live lease held by its author — reconstructed from a per-document
lease-interval timeline), I4 (a reclaim after expiry is legal), and I5 (log
integrity: monotonic sequence, no gaps).

**Failure injection, with the condition asserted.** A checker that only ever sees
clean runs proves little. Two injection tests feed it adversarial histories:
one kills an agent mid-claim (the mock clock advances past the TTL; a second
agent reclaims and commits), and one forces real write conflicts under 8-way
contention on the un-coordinated store. Each test first *asserts the injected
condition actually happened* — a reclaim-after-expiry event exists in the lease
log; the observed conflict count is non-zero — and only then runs the checker.
That guard is the point: it prevents an injection from passing vacuously because
nothing actually went wrong.

**The checker was itself adversarially reviewed — and it had bugs.** Two
false-pass holes were found and fixed with regression tests: (1) a same-agent
re-claim to a *shorter* expiry left a stale interval that could falsely "cover" a
later write in the phantom window between the real and stale expiry; the fix caps
a superseded interval at the reclaim timestamp. (2) The exact twin on the renew
path — a renew-after-expiry could resurrect a dead interval — fixed the same way.
Neither is reachable by a correct store, but a verifier whose job is to catch
store bugs must not have false-pass paths, so both were closed. This is the most
important lesson of the phase: *the thing that checks correctness needs its own
adversarial review*, because a checker with a false pass is worse than no checker
— it launders bugs into green checkmarks.

**Runtime conflict handling.** On a version conflict a writer re-reads the current
version and retries under a bounded policy (capped attempts, deterministic
backoff), and the retry count is surfaced in per-agent stats — conflicts are
*handled and reported*, not merely avoided. A subtle bug here (swallowing the
re-read error and retrying with a stale base version) was caught in review and
fixed to fail the operation instead.

## The one profiled optimization

Phase 5 measured per-operation latency and throughput, and load-testing surfaced
a genuine scalability bug rather than a micro-optimization. At N=16 the run
*failed* with ephemeral-port exhaustion. The cause: `api.NewClient` left the
`http.Client.Transport` nil, so every agent shared Go's global
`http.DefaultTransport`, whose `MaxIdleConnsPerHost` is 2. N agents contending
over a 2-connection pool meant connections churned (open+close per request)
instead of being reused; below N=16 this also made throughput non-monotonic (N=4
slower than N=2). Giving each client its own transport with a real idle-connection
pool fixed it: N=16 now completes, and throughput scales monotonically to ~83k
ops/sec. It's a satisfying find because the load test didn't just rank a known
system — it *broke* it and revealed a real defect.

## Honest limitations

- **In-memory, single node.** No durability, no replication. A process restart
  loses all state. This is stated up front as a known v1 boundary.
- **Localhost benchmarks.** The store and agents share a machine over loopback.
  Real network latency would raise the absolute latency numbers; it would not
  change the *relative* duplication result (0% vs. up to 93.8%), which is
  structural.
- **The latency workload has no cross-agent contention** (each agent uses private
  doc keys), so it measures substrate op cost, not contention throughput. That is
  intentional and disclosed; the contention story is Result 1.
- **The duplication baseline models "each agent processes the whole corpus."**
  That is the honest worst case of no coordination (N independent agents each told
  to annotate everything), and it is disclosed — not a strawman, since both modes
  share the exact code path minus the two coordination mechanisms.
- **`conflicts` counts are scheduling-dependent**; they are reported as
  representative, and the headline duplication/latency numbers do not depend on
  them.

## What I'd change with more time

- **Shard the store mutex** per document or hash bucket to lift the
  write-throughput ceiling — the current single mutex is the obvious next
  bottleneck once HTTP pooling is fixed.
- **Write-ahead persistence** so state survives a restart (the append-only log is
  already the natural WAL).
- **gRPC transport** for streaming, backpressure, and lower per-op overhead than
  JSON-over-HTTP.
- **Porcupine-style linearizability checking** alongside the hand-written
  invariant checker, to catch ordering anomalies the per-invariant checks don't
  model.

## Engineering process notes

Every phase closed with an adversarial whole-branch review before merge, and
those reviews repeatedly earned their keep — they caught the arXiv query-encoding
bug (the corpus fetch would have returned zero documents), a lease-guard change
that broke the single-agent path, the two checker false-passes above, the HTTP
connection-pool exhaustion, and a benchmark-honesty gap (single-run throughput
presented as reproducible, fixed by averaging over five runs with stddev). The
consistent theme: the interesting bugs were not in writing the feature but in the
seams between changes and in the code that verifies correctness.
