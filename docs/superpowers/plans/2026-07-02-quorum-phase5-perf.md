# Quorum Phase 5 — Latency/Throughput + Profiling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure the substrate's per-operation latency (p50/p95/p99 for claim/write/release) and throughput (ops/sec) under N = 2/4/8/16 concurrent agents, then profile with `pprof`, apply **one** documented optimization, and report before/after.

**Architecture:** A dedicated perf harness (`internal/perf`) drives N goroutine agents, each operating on its OWN private set of doc keys so no lease/version contention occurs between agents — the only shared contention is the store's single mutex and the HTTP layer, which is exactly what we want to measure. Keep-alives are ON (the Phase-3 duplication harness turned them off; that was for count-correctness, not latency) and the race detector is OFF for reported numbers. Each operation is individually timed; percentiles are computed offline. A `cmd/quorum-perf` CLI runs the sweep and can emit a CPU profile. The optimization task is authored only after the profile identifies the real bottleneck.

**Tech Stack:** Go (stdlib only: `net/http/httptest`, `runtime/pprof`, `sort`, `time`), building on Phases 1–4 (`quorum` module). New package `internal/perf`; new command `cmd/quorum-perf`.

## Global Constraints

- **Language:** Go, standard library only. Module `quorum`; imports `quorum/internal/...`. Go floor `go 1.22`.
- **Reported numbers are taken with the race detector OFF.** Race instrumentation distorts timing. Correctness tests still run under `-race`; the perf CLI and its numbers do not.
- **Keep-alives ON for the perf harness.** Use a plain `httptest.NewServer` (default keep-alives). The client already drains response bodies (Phase 3), so each sequential agent reuses one connection — no port exhaustion, realistic latency.
- **No agent-to-agent logical contention.** Each agent operates on doc keys unique to it (e.g. `a<agent>-d<index>`), so every claim/write/release succeeds first try. This isolates *substrate* cost (mutex + HTTP + allocation) from coordination outcomes (which Phase 3 already covered).
- **Injected clock unchanged.** The perf harness uses `clock.Real{}` (it measures real time); no `time.Now()` is added outside `internal/clock` except through the store — the harness times operations with `time.Since`, which is measurement, not lease logic. (Timing the harness itself is allowed; lease/store time still flows through the injected clock.)
- **The optimization is ONE change, documented before/after.** Not a scattershot tuning pass. It is authored against real `pprof` data (Task 5), not guessed.
- **TDD** for the harness code (Tasks 1–3). One commit per task. Every commit message ends with:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`

---

## File Structure

**Created:**
```
internal/perf/percentiles.go       # Percentiles + OpStats + Summarize
internal/perf/percentiles_test.go
internal/perf/harness.go           # RunPerf(...) PerfResult (per-op timed workload)
internal/perf/harness_test.go
cmd/quorum-perf/main.go            # sweep N=2/4/8/16; -cpuprofile; prints latency table
```
**Modified (Task 5, after profiling):**
```
<the one file the profile points at>  # the single optimization
README.md                             # latency table + the documented optimization
docs/ (optional perf note)
```

**Responsibility boundaries:**
- `internal/perf` is measurement only: percentile math + a timed workload driver. It imports agent-facing pieces (`api`, `store`, `clock`) but holds no coordination logic.
- The optimization (Task 5) lands in whatever production file the profile implicates — not pre-decided here.

---

### Task 1: Percentile math

**Files:**
- Create: `internal/perf/percentiles.go`
- Test: `internal/perf/percentiles_test.go`

**Interfaces:**
- Produces:
  - `func Percentiles(ds []time.Duration) (p50, p95, p99 time.Duration)` — nearest-rank over a sorted copy; returns zeros for empty input; does not mutate the caller's slice.
  - `type OpStats struct { Count int; P50, P95, P99 time.Duration }`
  - `func Summarize(ds []time.Duration) OpStats` — `{Count: len(ds), P50/P95/P99 from Percentiles}`.

- [ ] **Step 1: Write the failing test**

```go
package perf

import (
	"testing"
	"time"
)

func TestPercentilesNearestRank(t *testing.T) {
	// 1..100 ms. p50≈50th, p95≈95th, p99≈99th (nearest-rank, 0-based index p*n/100).
	ds := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		ds[i] = time.Duration(i+1) * time.Millisecond
	}
	p50, p95, p99 := Percentiles(ds)
	if p50 != 51*time.Millisecond { // index 50 -> value 51ms
		t.Fatalf("p50 = %v, want 51ms", p50)
	}
	if p95 != 96*time.Millisecond {
		t.Fatalf("p95 = %v, want 96ms", p95)
	}
	if p99 != 100*time.Millisecond {
		t.Fatalf("p99 = %v, want 100ms", p99)
	}
}

func TestPercentilesEmpty(t *testing.T) {
	p50, p95, p99 := Percentiles(nil)
	if p50 != 0 || p95 != 0 || p99 != 0 {
		t.Fatalf("empty percentiles = %v/%v/%v, want 0", p50, p95, p99)
	}
}

func TestPercentilesDoesNotMutateInput(t *testing.T) {
	ds := []time.Duration{3, 1, 2}
	Percentiles(ds)
	if ds[0] != 3 || ds[1] != 1 || ds[2] != 2 {
		t.Fatalf("input was mutated: %v", ds)
	}
}

func TestSummarize(t *testing.T) {
	ds := []time.Duration{time.Millisecond, 2 * time.Millisecond}
	s := Summarize(ds)
	if s.Count != 2 {
		t.Fatalf("count = %d, want 2", s.Count)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `export PATH="/opt/homebrew/bin:$PATH"; go test ./internal/perf/`
Expected: FAIL — `Percentiles` undefined.

- [ ] **Step 3: Write `internal/perf/percentiles.go`**

```go
// Package perf measures Quorum's per-operation latency and throughput.
package perf

import (
	"sort"
	"time"
)

// OpStats summarizes a set of operation latencies.
type OpStats struct {
	Count         int
	P50, P95, P99 time.Duration
}

// Percentiles returns the p50/p95/p99 latencies via nearest-rank over a sorted
// copy of ds (the input is not mutated). Empty input yields zeros.
func Percentiles(ds []time.Duration) (p50, p95, p99 time.Duration) {
	if len(ds) == 0 {
		return 0, 0, 0
	}
	s := make([]time.Duration, len(ds))
	copy(s, ds)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[rank(len(s), 50)], s[rank(len(s), 95)], s[rank(len(s), 99)]
}

// rank is the 0-based nearest-rank index for percentile p over n samples.
func rank(n, p int) int {
	i := p * n / 100
	if i >= n {
		i = n - 1
	}
	return i
}

// Summarize computes OpStats over ds.
func Summarize(ds []time.Duration) OpStats {
	p50, p95, p99 := Percentiles(ds)
	return OpStats{Count: len(ds), P50: p50, P95: p95, P99: p99}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/perf/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/perf/percentiles.go internal/perf/percentiles_test.go
git commit -m "feat: percentile math for perf measurement (p50/p95/p99)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Timed perf workload harness

**Files:**
- Create: `internal/perf/harness.go`
- Test: `internal/perf/harness_test.go`

**Interfaces:**
- Produces:
  - `type PerfResult struct { Agents int; Claim, Write, Release OpStats; TotalOps int; Wall time.Duration; OpsPerSec float64 }`
  - `func RunPerf(base string, agents, docsPerAgent, passes, k int, ttl time.Duration) (PerfResult, error)` — launches `agents` goroutines, each with its OWN `api.NewClient(base)`. Each agent, for `passes` iterations over `docsPerAgent` private doc keys (`fmt.Sprintf("a%d-d%d", agentIndex, j)`), performs claim → write → release, timing each op with `time.Since`. Agents never share doc keys, so every op succeeds. Per-op durations are collected per goroutine (no shared slice during timing) and merged after `wg.Wait()`. Returns per-op `OpStats`, total ops, wall time, and ops/sec.
- Consumes: `api.Client`, `annotate`, `corpus`/`model` (a payload string is fine — use a fixed annotation to avoid annotator cost dominating; e.g. `"x"`), `perf.Summarize`.

- [ ] **Step 1: Write the failing test**

```go
package perf

import (
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/store"
)

func TestRunPerfProducesStats(t *testing.T) {
	s := store.NewMemStore(clock.Real{})
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()

	const agents, docsPerAgent, passes = 4, 10, 2
	res, err := RunPerf(ts.URL, agents, docsPerAgent, passes, 5, time.Minute)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// Each agent does docsPerAgent*passes cycles of claim+write+release.
	wantCycles := agents * docsPerAgent * passes
	if res.Claim.Count != wantCycles || res.Write.Count != wantCycles || res.Release.Count != wantCycles {
		t.Fatalf("op counts = claim %d write %d release %d, want %d each",
			res.Claim.Count, res.Write.Count, res.Release.Count, wantCycles)
	}
	if res.TotalOps != wantCycles*3 {
		t.Fatalf("total ops = %d, want %d", res.TotalOps, wantCycles*3)
	}
	if res.OpsPerSec <= 0 || res.Write.P50 <= 0 {
		t.Fatalf("expected positive throughput and latency, got ops/sec=%v p50=%v", res.OpsPerSec, res.Write.P50)
	}
	if res.Agents != agents {
		t.Fatalf("agents = %d, want %d", res.Agents, agents)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/perf/ -run TestRunPerf`
Expected: FAIL — `RunPerf` undefined.

- [ ] **Step 3: Write `internal/perf/harness.go`**

```go
package perf

import (
	"fmt"
	"sync"
	"time"

	"quorum/internal/api"
)

type PerfResult struct {
	Agents               int
	Claim, Write, Release OpStats
	TotalOps             int
	Wall                 time.Duration
	OpsPerSec            float64
}

// RunPerf drives `agents` goroutines, each operating on its own private doc keys
// (so no cross-agent lease/version contention — only the store mutex and HTTP
// layer are shared). Each agent performs claim→write→release per doc for
// `passes` iterations, timing every operation. Returns per-op latency stats and
// throughput. Errors from any agent abort with the first error.
func RunPerf(base string, agents, docsPerAgent, passes, k int, ttl time.Duration) (PerfResult, error) {
	type agentSamples struct {
		claim, write, release []time.Duration
		err                   error
	}
	samples := make([]agentSamples, agents)

	var wg sync.WaitGroup
	start := time.Now()
	for a := 0; a < agents; a++ {
		wg.Add(1)
		go func(a int) {
			defer wg.Done()
			c := api.NewClient(base) // per-goroutine client
			var sm agentSamples
			for p := 0; p < passes; p++ {
				for d := 0; d < docsPerAgent; d++ {
					doc := fmt.Sprintf("a%d-d%d", a, d)

					t0 := time.Now()
					if _, err := c.Claim(doc, agentID(a), ttl); err != nil {
						sm.err = err
						samples[a] = sm
						return
					}
					sm.claim = append(sm.claim, time.Since(t0))

					// Read to get the current version, then write (timed).
					e, err := c.Read(doc)
					if err != nil {
						sm.err = err
						samples[a] = sm
						return
					}
					t1 := time.Now()
					if _, err := c.Write(doc, agentID(a), "x", e.Version); err != nil {
						sm.err = err
						samples[a] = sm
						return
					}
					sm.write = append(sm.write, time.Since(t1))

					t2 := time.Now()
					if err := c.Release(doc, agentID(a)); err != nil {
						sm.err = err
						samples[a] = sm
						return
					}
					sm.release = append(sm.release, time.Since(t2))
				}
			}
			samples[a] = sm
		}(a)
	}
	wg.Wait()
	wall := time.Since(start)

	var claim, write, release []time.Duration
	for _, sm := range samples {
		if sm.err != nil {
			return PerfResult{}, sm.err
		}
		claim = append(claim, sm.claim...)
		write = append(write, sm.write...)
		release = append(release, sm.release...)
	}

	total := len(claim) + len(write) + len(release)
	res := PerfResult{
		Agents:   agents,
		Claim:    Summarize(claim),
		Write:    Summarize(write),
		Release:  Summarize(release),
		TotalOps: total,
		Wall:     wall,
	}
	if wall > 0 {
		res.OpsPerSec = float64(total) / wall.Seconds()
	}
	return res, nil
}

func agentID(a int) string { return fmt.Sprintf("agent-%d", a) }
```

> Note: `k` is accepted for signature symmetry with other drivers but the perf payload is a fixed `"x"` so the deterministic annotator's cost does not dominate the store-op measurement. Keep the param (the CLI passes it); do not remove it.

- [ ] **Step 4: Run tests under race (correctness) — then note perf runs are race-OFF**

Run: `go test -race ./internal/perf/`
Expected: PASS (this is the correctness check; the reported numbers in Task 3 come from a race-OFF build).

- [ ] **Step 5: Commit**

```bash
git add internal/perf/harness.go internal/perf/harness_test.go
git commit -m "feat: timed perf workload harness (per-op latency + throughput)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: `quorum-perf` CLI (sweep + optional CPU profile)

**Files:**
- Create: `cmd/quorum-perf/main.go`

**Interfaces:**
- `quorum-perf` flags: `-agents "2,4,8,16"`, `-docs-per-agent` (default 200), `-passes` (default 3), `-k` (default 5), `-ttl` (default 60s), `-cpuprofile ""` (optional path). For each N it stands up a fresh `httptest.NewServer` (keep-alives ON) over a coordinated store, runs `RunPerf`, and prints a row: N, total ops, ops/sec, and claim/write/release p50·p95·p99. When `-cpuprofile` is set, the entire sweep is wrapped in `pprof.StartCPUProfile`/`StopCPUProfile`.

- [ ] **Step 1: Write `cmd/quorum-perf/main.go`**

```go
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/perf"
	"quorum/internal/store"
)

func main() {
	agentsCSV := flag.String("agents", "2,4,8,16", "comma-separated agent counts")
	docsPerAgent := flag.Int("docs-per-agent", 200, "private docs per agent")
	passes := flag.Int("passes", 3, "passes over each agent's docs")
	k := flag.Int("k", 5, "keywords per annotation (unused in perf payload)")
	ttl := flag.Duration("ttl", 60*time.Second, "lease TTL")
	cpuprofile := flag.String("cpuprofile", "", "write a CPU profile to this path")
	flag.Parse()

	counts, err := parseCounts(*agentsCSV)
	if err != nil {
		log.Fatalf("parse -agents: %v", err)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatalf("create cpuprofile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("start cpuprofile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	fmt.Printf("Quorum latency/throughput — docs/agent=%d passes=%d (keep-alives on, race off)\n\n",
		*docsPerAgent, *passes)
	fmt.Printf("%4s %9s %10s | %-22s | %-22s | %-22s\n",
		"N", "ops", "ops/sec", "claim p50/p95/p99", "write p50/p95/p99", "release p50/p95/p99")
	fmt.Println(strings.Repeat("-", 100))

	for _, n := range counts {
		s := store.NewMemStore(clock.Real{})
		ts := httptest.NewServer(api.NewServer(s))
		res, err := perf.RunPerf(ts.URL, n, *docsPerAgent, *passes, *k, *ttl)
		ts.Close()
		if err != nil {
			log.Fatalf("perf (n=%d): %v", n, err)
		}
		fmt.Printf("%4d %9d %10.0f | %-22s | %-22s | %-22s\n",
			res.Agents, res.TotalOps, res.OpsPerSec,
			trip(res.Claim), trip(res.Write), trip(res.Release))
	}
}

func trip(s perf.OpStats) string {
	return fmt.Sprintf("%v/%v/%v", s.P50.Round(time.Microsecond), s.P95.Round(time.Microsecond), s.P99.Round(time.Microsecond))
}

func parseCounts(csv string) ([]int, error) {
	parts := strings.Split(csv, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}
```

- [ ] **Step 2: Build + smoke run (small)**

Run: `export PATH="/opt/homebrew/bin:$PATH"; go build ./...`
Then: `go run ./cmd/quorum-perf -agents 2,4 -docs-per-agent 20 -passes 2`
Expected: a table with two rows (N=2, N=4), positive ops/sec, and non-trivial p50/p95/p99 for each op.

- [ ] **Step 3: Full suite still green**

Run: `go vet ./... && go test -race ./...`
Expected: all PASS, vet clean.

- [ ] **Step 4: Commit**

```bash
git add cmd/quorum-perf/
git commit -m "feat: quorum-perf CLI — latency/throughput sweep + CPU profile

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Capture baseline numbers + profile (controller-driven)

> This task is NOT a subagent implementation task — it is a measurement procedure the controller runs directly. Its output (the baseline table + the identified bottleneck) is the input to Task 5.

- [ ] **Step 1: Capture the baseline latency table (race OFF)**

Run: `go build -o /tmp/quorum-perf ./cmd/quorum-perf && /tmp/quorum-perf -agents 2,4,8,16 -docs-per-agent 200 -passes 3`
Record the full table.

- [ ] **Step 2: Capture a CPU profile at the highest contention**

Run: `/tmp/quorum-perf -agents 16 -docs-per-agent 200 -passes 3 -cpuprofile /tmp/quorum-cpu.prof`
Then: `go tool pprof -top -nodecount=20 /tmp/quorum-cpu.prof`
Identify the single worst bottleneck (e.g. mutex contention, JSON alloc, log snapshot copy, per-request allocation).

- [ ] **Step 3: Write down the finding.** The bottleneck + a one-paragraph hypothesis for the fix. This becomes Task 5's spec.

---

### Task 5: Apply ONE optimization + re-measure (authored after Task 4)

> Authored against the real profile from Task 4. The plan cannot pre-specify the code because the bottleneck is unknown until measured. When Task 4 identifies it, this task is filled in with: the exact change, a before/after latency comparison, and a README update. Guardrails below hold regardless of what the optimization is.

**Guardrails (fixed):**
- It is **one** change addressing the **one** worst bottleneck — not a tuning grab-bag.
- **Correctness is preserved:** `go test -race ./...` stays green and `quorum-check` still reports ALL INVARIANTS HOLD after the change.
- **Before/after is measured the same way** (same `-agents/-docs-per-agent/-passes`, race off, keep-alives on) and both numbers are recorded.
- If the profile shows no single dominant bottleneck (already efficient), that is a legitimate finding: **document "no optimization warranted"** with the flat profile rather than inventing a micro-optimization. Honesty over a manufactured win.

- [ ] **Step 1:** Implement the single optimization the profile pointed at.
- [ ] **Step 2:** `go test -race ./...` green; `go run ./cmd/quorum-check -corpus corpus/fixture.jsonl -agents 8` still prints ALL INVARIANTS HOLD.
- [ ] **Step 3:** Re-run the perf sweep; record the after-numbers next to the before-numbers.
- [ ] **Step 4:** Update `README.md` with the latency table and a short "one optimization" note (what/why/before→after). If no optimization was warranted, document that instead.
- [ ] **Step 5:** Commit.

---

## Self-Review

**Spec coverage (PRD Phase 5):**
- p50/p95/p99 per store operation + ops/sec at N=2/4/8/16 → Tasks 1–3 (percentiles + timed harness + CLI). ✅
- `pprof` to find and fix the one worst bottleneck; document before/after → Tasks 4–5. ✅
- Benchmark with race detector OFF (skews numbers) → Global Constraints + Task 4 uses a non-race build. ✅
- GC pauses dominating p99 → the profile/percentiles will surface this; if p99 is GC-driven, that is reported honestly, not hidden (Task 5 guardrail). ✅
- Carry-forward: keep-alives RE-ENABLED for latency → perf harness uses a plain httptest server (keep-alives on), unlike the Phase-3 duplication harness. ✅
- Carry-forward: numbers with `-race` OFF → enforced. ✅

**Placeholder scan:** Tasks 1–3 are fully specified with complete code. Tasks 4–5 are deliberately procedural/authored-later because the PRD requires the optimization to be based on real profile data — this is the honest structure, not a placeholder. No TBD in shipped code.

**Type consistency:**
- `perf.OpStats`, `perf.Percentiles`, `perf.Summarize` — consistent across Tasks 1/2/3. ✅
- `perf.RunPerf(base string, agents, docsPerAgent, passes, k int, ttl time.Duration) (PerfResult, error)` — same in Task 2 def and Task 3 CLI caller. ✅
- `PerfResult` fields identical between harness (Task 2) and CLI printer (Task 3). ✅

**⚠️ Watch-fors:**
- **Do not report numbers from a `-race` build.** The correctness test (`go test -race ./internal/perf/`) is fine; the CLI numbers must come from a normal build (`go build`/`go run`). Race instrumentation can 5–10× the latencies.
- **Per-agent private doc keys are load-bearing** — if agents shared docs, claim/write would contend and the numbers would measure coordination, not substrate op cost. Keep the `a<agent>-d<index>` keying.
- **Percentile sample size:** at `docs-per-agent=200, passes=3` each op-type has 600×N samples — plenty. Don't shrink the smoke-test numbers into the reported table.
- **The optimization must not regress correctness** — re-run `go test -race ./...` AND `quorum-check` after it.
- Re-run `go vet ./...` before the final commit.

---

## Plan sequence (remaining)

- **Plan 6 — Phase 6 (writeup + packaging):** README (what/why/run-it), consolidated benchmark report (duplication + latency + invariant results), technical writeup (design decisions, failure handling, honest limitations — in-memory, single-node — and what I'd change), resume bullets, 2-min interview story.
