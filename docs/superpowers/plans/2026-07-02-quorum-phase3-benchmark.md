# Quorum Phase 3 — Baseline Mode + Duplication Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce the headline coordination-efficiency number: run the same agents over the same corpus with coordination ON (leases) vs OFF (baseline), measure the doc-level duplication rate at N = 2/4/8/16 agents over ≥5 runs, and show coordinated ≈ 0% vs a meaningful baseline duplication rate.

**Architecture:** Baseline mode disables ONLY the agent-level coordination (lease claiming + consulting prior findings to skip done docs) while keeping the store's version CAS on — so duplicate work manifests as multiple committed annotations of the same doc, not lost updates. The store gains an `Uncoordinated()` construction option that bypasses the lease guard (CAS stays). A baseline agent loop annotates-and-writes every doc it is given with the shared CAS-retry helper. A benchmark harness runs both modes at each N over the arXiv corpus, computes the duplication metric by replaying the findings log, and a CLI prints the comparison table.

**Tech Stack:** Go (stdlib only), building on Phase 1+2 (`quorum` module). Reuses `internal/agent`, `internal/api`, `internal/store`, `internal/retry`, `internal/corpus`. New package `internal/bench`.

## Global Constraints

- **Language:** Go, standard library only. No third-party modules.
- **Module path:** `quorum`; imports `quorum/internal/...`. Go floor `go 1.22` (do not bump the go.mod directive).
- **Deterministic annotation:** benchmark uses the existing deterministic `annotate.Annotate` — no LLM, reproducible counts.
- **Duplication metric is FROZEN (doc-level):** a duplicate is the same `doc_id` committed ≥2 times. Headline number = **duplication rate = (total_findings − unique_docs) / total_findings**. Also report raw: total findings, unique docs, duplicate findings, conflicts. Defined here, not adjusted after seeing numbers.
- **Fair baseline (anti-strawman):** baseline shares the exact code path minus the two coordination mechanisms (no `Claim`, no `Exists`-skip). Store CAS stays ON in both modes so the ONLY difference is claiming. Disclose the workload model (each of N agents processes the shared corpus).
- **Injected clock preserved:** no `time.Now()` outside `internal/clock`. The benchmark uses `clock.Real{}` (it measures real concurrent behavior); lease tests still use the mock clock.
- **`-race` on for all tests.** The benchmark binary itself is NOT run under `-race` for reported numbers (race instrumentation skews behavior), but its correctness tests are.
- **No second store mutex; lock ordering store→log preserved.** The `Uncoordinated` option only gates the lease check inside `Write`; it adds no new lock.
- **TDD:** failing test first, then implement. Commit per task (RED/GREEN may be one commit per task this phase). Every commit message ends with:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`

---

## File Structure

**Modified:**
```
internal/agent/agent.go        # fix swallowed re-Read; extract writeWithRetry helper; RunCoordinated uses it
internal/agent/agent_test.go   # writeWithRetry conflict-refresh test
internal/store/store.go        # NewMemStore variadic Option; Uncoordinated() bypasses lease guard (CAS stays)
internal/store/store_test.go   # uncoordinated-write test
README.md                      # benchmark run + headline note
```

**Created:**
```
internal/agent/baseline.go       # RunBaseline + RunBaselineConcurrent (no claim, no Exists-skip)
internal/agent/baseline_test.go
internal/bench/metric.go         # DuplicationRate over the findings log + Result type
internal/bench/metric_test.go
internal/bench/harness.go        # RunScenario(coordinated, agents, docs, ...) -> Result
internal/bench/harness_test.go   # coordinated dup≈0 vs baseline dup>0
cmd/quorum-bench/main.go         # runs both modes at N=2/4/8/16 over ≥5 runs; prints table
```

**Responsibility boundaries:**
- `internal/bench` owns measurement only — metric computation and scenario orchestration; it imports agent/api/store/corpus but holds no coordination logic itself.
- `baseline.go` is kept separate from `agent.go` so the coordinated and baseline loops are each independently readable; both share the `writeWithRetry` helper (DRY on the CAS-retry core).
- The store `Option` mechanism is additive (variadic) so all existing `NewMemStore(clk)` callers are unaffected.

---

### Task 1: Fix swallowed re-Read + extract the CAS-retry helper

**Files:**
- Modify: `internal/agent/agent.go`
- Test: `internal/agent/agent_test.go`

**Interfaces:**
- Produces (unexported, package agent): `writeWithRetry(c *api.Client, docID, agentID, payload string, base int, p retry.Policy) (conflicts int, err error)` — annotates-writes under bounded CAS retry; on `api.ErrConflict` re-Reads to refresh `base`; **if the re-Read itself errors, it returns that error (no longer silently continues with a stale base)**. Returns retries observed (`attempts-1`).
- `RunCoordinated` is refactored to call `writeWithRetry` instead of its inline retry block (behavior otherwise identical; `st.Conflicts += conflicts`).

- [ ] **Step 1: Write the failing test**

```go
func TestWriteWithRetryRefreshesBaseOnConflict(t *testing.T) {
	s := store.NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()
	c := api.NewClient(ts.URL)

	if _, err := c.Claim("d1", "a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write("d1", "a", "v1", 0); err != nil { // doc now at version 1
		t.Fatal(err)
	}
	// Call the helper with a STALE base (0): first attempt conflicts, the helper
	// re-Reads (version 1), retries, and commits version 2. One conflict observed.
	conflicts, err := writeWithRetry(c, "d1", "a", "v2", 0, retry.Default())
	if err != nil {
		t.Fatalf("writeWithRetry: %v", err)
	}
	if conflicts != 1 {
		t.Fatalf("conflicts = %d, want 1", conflicts)
	}
	if e, _ := c.Read("d1"); e.Version != 2 {
		t.Fatalf("version = %d, want 2", e.Version)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `export PATH="/opt/homebrew/bin:$PATH"; go test ./internal/agent/ -run TestWriteWithRetry`
Expected: FAIL — `writeWithRetry` undefined.

- [ ] **Step 3: Add the helper and refactor `RunCoordinated`**

Add the helper to `internal/agent/agent.go`:

```go
// writeWithRetry writes payload for docID under a bounded CAS retry. On a
// version conflict it re-Reads to refresh the base version and retries. If the
// re-Read itself fails, it stops and returns that error (rather than retrying
// with a stale base). Returns the number of conflicts observed (retries).
func writeWithRetry(c *api.Client, docID, agentID, payload string, base int, p retry.Policy) (int, error) {
	attempts, err := retry.Do(p, func() (bool, error) {
		_, werr := c.Write(docID, agentID, payload, base)
		if errors.Is(werr, api.ErrConflict) {
			re, rerr := c.Read(docID)
			if rerr != nil {
				return false, rerr // cannot refresh base; fail this doc
			}
			base = re.Version
			return true, werr // retryable
		}
		return false, werr // success or fatal
	})
	return attempts - 1, err
}
```

Now replace the inline retry block in `RunCoordinated`. Find the block that currently reads (approximately):

```go
		note := annotate.Annotate(d, k)
		base := e.Version
		attempts, werr := retry.Do(p, func() (bool, error) {
			_, err := c.Write(d.ID, agentID, note, base)
			if errors.Is(err, api.ErrConflict) {
				if re, rerr := c.Read(d.ID); rerr == nil {
					base = re.Version
				}
				return true, err
			}
			return false, err
		})
		st.Conflicts += attempts - 1
		_ = c.Release(d.ID, agentID)
		if werr != nil {
			return st, werr
		}
		st.Annotated++
```

and replace it with:

```go
		note := annotate.Annotate(d, k)
		conflicts, werr := writeWithRetry(c, d.ID, agentID, note, e.Version, p)
		st.Conflicts += conflicts
		_ = c.Release(d.ID, agentID)
		if werr != nil {
			return st, werr
		}
		st.Annotated++
```

Keep the renewal-heartbeat line (`_, _ = c.Renew(d.ID, agentID, ttl)`) immediately before the `writeWithRetry` call, exactly where it is now.

- [ ] **Step 4: Run the agent package under race**

Run: `go test -race ./internal/agent/`
Expected: PASS (new test + all prior coordinated/driver tests).

- [ ] **Step 5: Commit**

```bash
git add internal/agent/agent.go internal/agent/agent_test.go
git commit -m "refactor: extract writeWithRetry; fix swallowed re-Read on conflict

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Store `Uncoordinated()` option (bypass lease guard, keep CAS)

**Files:**
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces:
  - `type Option func(*MemStore)`
  - `func Uncoordinated() Option` — sets the store to skip the lease guard in `Write` (CAS stays on).
  - `NewMemStore(clk clock.Clock, opts ...Option) *MemStore` (variadic; existing `NewMemStore(clk)` calls unaffected).
- Behavior: in an uncoordinated store, `Write` does NOT require a live lease (no `ErrNoLease`); it still enforces version CAS (`ErrVersionConflict` on base mismatch). `Claim`/`Renew`/`Release` remain callable but baseline agents won't use them.

- [ ] **Step 1: Write the failing test (append to `store_test.go`)**

```go
func TestUncoordinatedWriteNeedsNoLeaseButKeepsCAS(t *testing.T) {
	s := NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)), Uncoordinated())

	// No claim taken, yet the write commits (lease guard bypassed).
	f, err := s.Write("d1", "agent-a", "note", 0)
	if err != nil {
		t.Fatalf("uncoordinated write: %v", err)
	}
	if f.CommittedVersion != 1 {
		t.Fatalf("version = %d, want 1", f.CommittedVersion)
	}
	// CAS is still enforced: a stale base still conflicts.
	if _, err := s.Write("d1", "agent-b", "note2", 0); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict (CAS still on)", err)
	}
	// A correct base commits a second annotation for the same doc.
	if _, err := s.Write("d1", "agent-b", "note2", 1); err != nil {
		t.Fatalf("second annotation: %v", err)
	}
}

func TestCoordinatedStoreStillRequiresLease(t *testing.T) {
	s := NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0))) // default: coordinated
	if _, err := s.Write("d1", "agent-a", "note", 0); !errors.Is(err, ErrNoLease) {
		t.Fatalf("err = %v, want ErrNoLease (default store is coordinated)", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/ -run 'TestUncoordinated|TestCoordinatedStoreStill'`
Expected: FAIL — `Uncoordinated`/`Option` undefined (and the variadic constructor).

- [ ] **Step 3: Implement in `internal/store/store.go`**

Add the option type and constructor change. Add a `coordinated bool` field to `MemStore`, defaulting to true.

Add near the top (after the sentinels):

```go
// Option configures a MemStore at construction.
type Option func(*MemStore)

// Uncoordinated disables the lease guard on Write (version CAS stays on). Used
// for the no-coordination baseline benchmark.
func Uncoordinated() Option {
	return func(s *MemStore) { s.coordinated = false }
}
```

Add the field to the struct (alongside `claims`):

```go
	coordinated bool
```

Change `NewMemStore` to be variadic and default `coordinated: true`:

```go
func NewMemStore(clk clock.Clock, opts ...Option) *MemStore {
	s := &MemStore{
		entries:     make(map[string]model.Entry),
		claims:      make(map[string]model.Claim),
		log:         NewLog(),
		clk:         clk,
		coordinated: true,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
```

In `Write`, gate the lease guard on `s.coordinated`:

```go
	if s.coordinated {
		if lc := s.claims[docID]; !leaseLive(lc, s.clk.Now()) || lc.AgentID != agentID {
			return model.Finding{}, ErrNoLease
		}
	}
	// version CAS runs in BOTH modes:
	cur := s.entries[docID]
	if cur.Version != baseVersion {
		return model.Finding{}, ErrVersionConflict
	}
	// ... rest unchanged (append finding, bump entry) ...
```

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/store/`
Expected: PASS (new tests + all prior store tests — the default constructor still yields a coordinated, lease-requiring store).

- [ ] **Step 5: Commit**

```bash
git add internal/store/store.go internal/store/store_test.go
git commit -m "feat: Uncoordinated() store option (bypass lease guard, keep CAS)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Baseline agent loop + concurrent baseline driver

**Files:**
- Create: `internal/agent/baseline.go`
- Test: `internal/agent/baseline_test.go`

**Interfaces:**
- Consumes: `writeWithRetry` (Task 1), `api.Client`, `annotate`, `retry`, `model`.
- Produces:
  - `RunBaseline(c *api.Client, docs []model.Doc, agentID string, k int, p retry.Policy) (Stats, error)` — for each doc: annotate and write with `writeWithRetry` (NO claim, NO `Exists`-skip, NO release). Every doc is (re)annotated; `Annotated++` on each committed write; `Conflicts += conflicts`.
  - `RunBaselineConcurrent(base string, docs []model.Doc, agentIDs []string, k int, p retry.Policy) ([]Stats, error)` — one goroutine per agentID, each with its OWN `api.NewClient(base)`, running `RunBaseline`; waits for all; returns per-agent stats in order; first non-nil error.

- [ ] **Step 1: Write the failing test `internal/agent/baseline_test.go`**

```go
package agent

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/corpus"
	"quorum/internal/replay"
	"quorum/internal/retry"
	"quorum/internal/store"
)

func TestBaselineDuplicatesAcrossAgents(t *testing.T) {
	// Uncoordinated store: no lease guard, CAS on.
	s := store.NewMemStore(clock.Real{}, store.Uncoordinated())
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()
	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	const n = 4
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("agent-%d", i)
	}
	// Generous retry so every duplicate annotation commits under contention.
	p := retry.Policy{MaxAttempts: 128, BaseDelay: 0}

	stats, err := RunBaselineConcurrent(ts.URL, docs, ids, 3, p)
	if err != nil {
		t.Fatalf("baseline run: %v", err)
	}

	c := api.NewClient(ts.URL)
	all, _ := c.Findings("")
	// Each of n agents annotates every doc -> n*len(docs) committed findings.
	if len(all) != n*len(docs) {
		t.Fatalf("findings = %d, want %d (n*docs, duplicated work)", len(all), n*len(docs))
	}
	// Sum of Annotated across agents matches.
	total := 0
	for _, st := range stats {
		total += st.Annotated
	}
	if total != n*len(docs) {
		t.Fatalf("sum annotated = %d, want %d", total, n*len(docs))
	}
	// Despite duplication, the log still replays with no version gaps (CAS held).
	if _, err := replay.Replay(all); err != nil {
		t.Fatalf("replay: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/agent/ -run TestBaselineDuplicates`
Expected: FAIL — `RunBaseline`/`RunBaselineConcurrent` undefined.

- [ ] **Step 3: Write `internal/agent/baseline.go`**

```go
package agent

import (
	"sync"

	"quorum/internal/annotate"
	"quorum/internal/api"
	"quorum/internal/model"
	"quorum/internal/retry"
)

// RunBaseline walks the corpus once with NO coordination: it does not claim
// leases and does not consult prior findings to skip already-annotated docs.
// Every doc is annotated and written (under CAS retry, since concurrent agents
// contend on the same doc versions). This is the no-shared-memory baseline used
// to measure how much duplicated work coordination eliminates.
func RunBaseline(c *api.Client, docs []model.Doc, agentID string, k int, p retry.Policy) (Stats, error) {
	var st Stats
	for _, d := range docs {
		e, err := c.Read(d.ID)
		if err != nil {
			return st, err
		}
		note := annotate.Annotate(d, k)
		conflicts, werr := writeWithRetry(c, d.ID, agentID, note, e.Version, p)
		st.Conflicts += conflicts
		if werr != nil {
			return st, werr
		}
		st.Annotated++
	}
	return st, nil
}

// RunBaselineConcurrent runs one goroutine per agentID (each with its own
// client) all running RunBaseline against the same store. Returns per-agent
// Stats in order; returns the first non-nil agent error after all finish.
func RunBaselineConcurrent(base string, docs []model.Doc, agentIDs []string, k int, p retry.Policy) ([]Stats, error) {
	stats := make([]Stats, len(agentIDs))
	errs := make([]error, len(agentIDs))
	var wg sync.WaitGroup
	for i, id := range agentIDs {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			c := api.NewClient(base) // per-goroutine client, never shared
			st, err := RunBaseline(c, docs, id, k, p)
			stats[i] = st
			errs[i] = err
		}(i, id)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return stats, err
		}
	}
	return stats, nil
}
```

- [ ] **Step 4: Run tests under race**

Run: `go test -race ./internal/agent/`
Expected: PASS. If the baseline test flakes on the finding count, the CAS-retry cap may be too low for the contention — but with `MaxAttempts: 128` and 4 agents it should always commit. If it genuinely flakes, STOP and report (do not just bump the number blindly without understanding why).

- [ ] **Step 5: Commit**

```bash
git add internal/agent/baseline.go internal/agent/baseline_test.go
git commit -m "feat: baseline agent loop + concurrent driver (no coordination)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Duplication metric

**Files:**
- Create: `internal/bench/metric.go`
- Test: `internal/bench/metric_test.go`

**Interfaces:**
- Consumes: `[]model.Finding`.
- Produces:
  - `type Result struct { Mode string; Agents int; TotalFindings, UniqueDocs, DuplicateFindings int; DuplicationRate float64; Conflicts, ClaimsLost int }`
  - `func DuplicationRate(findings []model.Finding) (rate float64, total, unique, dupes int)` — `total = len(findings)`; `unique = distinct doc_ids`; `dupes = total - unique`; `rate = dupes/total` (0 when total==0). This is the frozen doc-level metric.

- [ ] **Step 1: Write the failing test**

```go
package bench

import (
	"testing"

	"quorum/internal/model"
)

func TestDuplicationRate(t *testing.T) {
	// 3 findings, 2 unique docs (d1 annotated twice) -> 1 dupe, rate 1/3.
	fs := []model.Finding{
		{DocID: "d1"}, {DocID: "d2"}, {DocID: "d1"},
	}
	rate, total, unique, dupes := DuplicationRate(fs)
	if total != 3 || unique != 2 || dupes != 1 {
		t.Fatalf("total=%d unique=%d dupes=%d", total, unique, dupes)
	}
	if rate < 0.333 || rate > 0.334 {
		t.Fatalf("rate = %v, want ~0.333", rate)
	}
}

func TestDuplicationRateEmpty(t *testing.T) {
	rate, total, _, _ := DuplicationRate(nil)
	if total != 0 || rate != 0 {
		t.Fatalf("empty: rate=%v total=%d", rate, total)
	}
}

func TestDuplicationRateNoDupes(t *testing.T) {
	fs := []model.Finding{{DocID: "d1"}, {DocID: "d2"}}
	rate, _, unique, dupes := DuplicationRate(fs)
	if unique != 2 || dupes != 0 || rate != 0 {
		t.Fatalf("no-dupes: unique=%d dupes=%d rate=%v", unique, dupes, rate)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/bench/`
Expected: FAIL — `DuplicationRate` undefined.

- [ ] **Step 3: Write `internal/bench/metric.go`**

```go
package bench

import "quorum/internal/model"

// Result is one benchmark scenario's outcome.
type Result struct {
	Mode              string  // "coordinated" or "baseline"
	Agents            int     // N
	TotalFindings     int     // committed annotations in the log
	UniqueDocs        int     // distinct doc_ids annotated
	DuplicateFindings int     // TotalFindings - UniqueDocs
	DuplicationRate   float64 // DuplicateFindings / TotalFindings
	Conflicts         int     // summed CAS retries across agents
	ClaimsLost        int     // summed lost claims across agents
}

// DuplicationRate computes the frozen doc-level duplication metric over the
// findings log: the fraction of committed annotations that are redundant (a
// doc annotated more than once). Returns the rate plus the raw counts.
func DuplicationRate(findings []model.Finding) (rate float64, total, unique, dupes int) {
	total = len(findings)
	seen := make(map[string]struct{}, total)
	for _, f := range findings {
		seen[f.DocID] = struct{}{}
	}
	unique = len(seen)
	dupes = total - unique
	if total == 0 {
		return 0, 0, 0, 0
	}
	return float64(dupes) / float64(total), total, unique, dupes
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/bench/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/bench/metric.go internal/bench/metric_test.go
git commit -m "feat: doc-level duplication metric + Result type

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Benchmark harness (run one scenario, both modes)

**Files:**
- Create: `internal/bench/harness.go`
- Test: `internal/bench/harness_test.go`

**Interfaces:**
- Consumes: `agent.RunConcurrent`, `agent.RunBaselineConcurrent`, `agent.Stats`, `store`, `api`, `clock`, `retry`, `model`, `DuplicationRate`.
- Produces:
  - `func RunScenario(coordinated bool, agents int, docs []model.Doc, k int, ttl time.Duration, p retry.Policy) (Result, error)` — spins up a fresh store (coordinated or `store.Uncoordinated()`) behind an in-process httptest server, builds `agents` agent IDs (`agent-0`..), runs the matching concurrent driver, reads the findings, and returns a populated `Result` (Mode set accordingly; Conflicts/ClaimsLost summed from the per-agent stats).

- [ ] **Step 1: Write the failing test**

```go
package bench

import (
	"testing"
	"time"

	"quorum/internal/corpus"
	"quorum/internal/retry"
)

func TestRunScenarioCoordinatedHasNoDuplication(t *testing.T) {
	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	res, err := RunScenario(true, 4, docs, 3, time.Minute, retry.Default())
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	if res.Mode != "coordinated" || res.Agents != 4 {
		t.Fatalf("result meta = %+v", res)
	}
	// Coordinated: each doc annotated exactly once.
	if res.TotalFindings != len(docs) || res.UniqueDocs != len(docs) {
		t.Fatalf("coordinated findings=%d unique=%d want %d/%d", res.TotalFindings, res.UniqueDocs, len(docs), len(docs))
	}
	if res.DuplicationRate != 0 {
		t.Fatalf("coordinated dup rate = %v, want 0", res.DuplicationRate)
	}
}

func TestRunScenarioBaselineHasDuplication(t *testing.T) {
	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	res, err := RunScenario(false, 4, docs, 3, time.Minute, retry.Policy{MaxAttempts: 128, BaseDelay: 0})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	if res.Mode != "baseline" {
		t.Fatalf("mode = %s", res.Mode)
	}
	// Baseline: 4 agents each annotate every doc -> heavy duplication.
	if res.TotalFindings != 4*len(docs) {
		t.Fatalf("baseline findings = %d, want %d", res.TotalFindings, 4*len(docs))
	}
	if res.DuplicationRate <= 0.7 { // (4-1)/4 = 0.75
		t.Fatalf("baseline dup rate = %v, want ~0.75", res.DuplicationRate)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/bench/ -run TestRunScenario`
Expected: FAIL — `RunScenario` undefined.

- [ ] **Step 3: Write `internal/bench/harness.go`**

```go
package bench

import (
	"fmt"
	"net/http/httptest"
	"time"

	"quorum/internal/agent"
	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/model"
	"quorum/internal/retry"
	"quorum/internal/store"
)

// RunScenario runs one benchmark scenario end-to-end over an in-process HTTP
// server and returns the duplication Result. When coordinated is false the
// store bypasses the lease guard and agents run the no-coordination baseline.
func RunScenario(coordinated bool, agents int, docs []model.Doc, k int, ttl time.Duration, p retry.Policy) (Result, error) {
	var s *store.MemStore
	if coordinated {
		s = store.NewMemStore(clock.Real{})
	} else {
		s = store.NewMemStore(clock.Real{}, store.Uncoordinated())
	}
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()

	ids := make([]string, agents)
	for i := range ids {
		ids[i] = fmt.Sprintf("agent-%d", i)
	}

	var stats []agent.Stats
	var err error
	mode := "coordinated"
	if coordinated {
		stats, err = agent.RunConcurrent(ts.URL, docs, ids, k, ttl, p)
	} else {
		mode = "baseline"
		stats, err = agent.RunBaselineConcurrent(ts.URL, docs, ids, k, p)
	}
	if err != nil {
		return Result{}, err
	}

	all, ferr := api.NewClient(ts.URL).Findings("")
	if ferr != nil {
		return Result{}, ferr
	}
	rate, total, unique, dupes := DuplicationRate(all)

	res := Result{
		Mode:              mode,
		Agents:            agents,
		TotalFindings:     total,
		UniqueDocs:        unique,
		DuplicateFindings: dupes,
		DuplicationRate:   rate,
	}
	for _, st := range stats {
		res.Conflicts += st.Conflicts
		res.ClaimsLost += st.ClaimsLost
	}
	return res, nil
}
```

- [ ] **Step 4: Run tests under race**

Run: `go test -race ./internal/bench/`
Expected: PASS (both scenario tests + metric tests).

- [ ] **Step 5: Commit**

```bash
git add internal/bench/harness.go internal/bench/harness_test.go
git commit -m "feat: benchmark harness (coordinated vs baseline scenario)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: `quorum-bench` CLI (the headline table) + README

**Files:**
- Create: `cmd/quorum-bench/main.go`
- Modify: `README.md`

**Interfaces:**
- `quorum-bench` flags: `-corpus` (default `corpus/arxiv.jsonl`), `-k` (default 5), `-runs` (default 5), `-ttl` (default 60s), `-agents` (default `2,4,8,16` comma list). For each agent count, runs both modes `-runs` times, and prints a table: mode, N, total findings, unique docs, duplication rate (mean over runs), conflicts. Baseline uses a generous retry policy (`MaxAttempts` scaled to contention); coordinated uses `retry.Default()`.

- [ ] **Step 1: Write `cmd/quorum-bench/main.go`** (verbatim — this is the complete file)

```go
package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"quorum/internal/bench"
	"quorum/internal/corpus"
	"quorum/internal/model"
	"quorum/internal/retry"
)

func main() {
	corpusPath := flag.String("corpus", "corpus/arxiv.jsonl", "corpus JSONL path")
	k := flag.Int("k", 5, "keywords per annotation")
	runs := flag.Int("runs", 5, "runs per (mode, N) cell")
	ttl := flag.Duration("ttl", 60*time.Second, "lease TTL")
	agentsCSV := flag.String("agents", "2,4,8,16", "comma-separated agent counts")
	flag.Parse()

	docs, err := corpus.Load(*corpusPath)
	if err != nil {
		log.Fatalf("load corpus: %v", err)
	}
	counts, err := parseCounts(*agentsCSV)
	if err != nil {
		log.Fatalf("parse -agents: %v", err)
	}

	fmt.Printf("Quorum duplication benchmark — corpus=%d docs, runs=%d each\n\n", len(docs), *runs)
	fmt.Printf("%-12s %4s %10s %10s %8s %10s\n", "mode", "N", "findings", "unique", "dup%", "conflicts")
	fmt.Println(strings.Repeat("-", 60))

	for _, n := range counts {
		// baseline: retry cap generous enough that every duplicate commits.
		baseP := retry.Policy{MaxAttempts: 4*n + 32, BaseDelay: 0}
		printRow(runCell(false, n, docs, *k, *ttl, baseP, *runs))
		printRow(runCell(true, n, docs, *k, *ttl, retry.Default(), *runs))
	}
}

// runCell runs one (mode, N) cell `runs` times and returns the last run's raw
// counts with the duplication rate averaged across runs (counts are
// deterministic by construction; averaging guards against any surprise).
func runCell(coordinated bool, n int, docs []model.Doc, k int, ttl time.Duration, p retry.Policy, runs int) bench.Result {
	var agg bench.Result
	var rateSum float64
	for i := 0; i < runs; i++ {
		res, err := bench.RunScenario(coordinated, n, docs, k, ttl, p)
		if err != nil {
			log.Fatalf("scenario (coord=%v n=%d): %v", coordinated, n, err)
		}
		agg = res
		rateSum += res.DuplicationRate
	}
	agg.DuplicationRate = rateSum / float64(runs)
	return agg
}

func printRow(r bench.Result) {
	fmt.Printf("%-12s %4d %10d %10d %7.1f%% %10d\n",
		r.Mode, r.Agents, r.TotalFindings, r.UniqueDocs, r.DuplicationRate*100, r.Conflicts)
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

- [ ] **Step 2: Build and run against the fixture (fast smoke), then the arXiv corpus**

Run: `export PATH="/opt/homebrew/bin:$PATH"; go build ./...`
Expected: builds.

Smoke on the small fixture (fast):
```bash
go run ./cmd/quorum-bench -corpus corpus/fixture.jsonl -runs 2 -agents 2,4
```
Expected: a table where every `baseline` row has `dup%` > 0 and rising with N (≈50% at N=2, ≈75% at N=4), and every `coordinated` row has `dup% = 0.0%` with `unique == findings == 3`.

Full run on arXiv (the headline numbers):
```bash
go run ./cmd/quorum-bench -corpus corpus/arxiv.jsonl -runs 5 -agents 2,4,8,16
```
Expected: coordinated `dup% = 0.0%` at every N; baseline `dup%` rising ≈ (N-1)/N (50/75/87.5/93.75%). Capture this output for the README/report.

- [ ] **Step 3: Update `README.md`**

Add a Phase-3 section with the run command and a short interpretation. Paste the actual arXiv table you captured in Step 2. Example scaffold (replace numbers with the real captured output):

```markdown
## Benchmark: coordination eliminates duplicated work

    go run ./cmd/quorum-bench -corpus corpus/arxiv.jsonl -runs 5 -agents 2,4,8,16

Doc-level duplication rate (fraction of committed annotations that are redundant),
coordinated (lease-based claiming) vs baseline (no coordination), N agents over
the arXiv corpus:

| N  | baseline dup% | coordinated dup% |
|----|---------------|------------------|
| 2  | ~50%          | 0.0%             |
| 4  | ~75%          | 0.0%             |
| 8  | ~88%          | 0.0%             |
| 16 | ~94%          | 0.0%             |

Baseline agents redundantly re-annotate every document; lease-based claiming
reduces duplicated annotation work to zero while the version CAS keeps the log
consistent (no lost updates — verified by log replay).
```

- [ ] **Step 4: Full suite under race**

Run: `go test -race ./...`
Expected: all packages PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/quorum-bench/ README.md
git commit -m "feat: quorum-bench CLI + headline duplication table in README

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage (PRD Phase 3):**
- Baseline mode, same agents, coordination disabled → Tasks 2 (store) + 3 (baseline loop). Fair: same code path minus claim + Exists-skip; CAS held constant. ✅
- Define "duplicate" rigorously (doc-level, frozen) → Task 4 metric; frozen in Global Constraints. ✅
- Harness runs both modes at N = 2/4/8/16, ≥5 runs → Task 6 CLI. ✅
- Headline coordination-efficiency number with the raw counts → Task 6 table (dup rate + findings/unique/conflicts). ✅
- Duplication verified by replaying the log → metric operates on the findings log; baseline test asserts replay still holds. ✅
- Carry-forward #1 (baseline must bypass lease guard) → Task 2. ✅
- Carry-forward #2 (fix swallowed re-Read) → Task 1. ✅
- Carry-forward #3 (real contention corpus) → Task 6 defaults to arXiv. ✅

**Placeholder scan:** no TBD/TODO; Task 6's CLI is a single complete verbatim file (the earlier stub/correction was cleaned up). All code steps contain full code.

**Type consistency:**
- `RunScenario(coordinated bool, agents int, docs []model.Doc, k int, ttl time.Duration, p retry.Policy) (Result, error)` — signature identical between Task 5 def and Task 6 caller. ✅
- `bench.Result` fields identical across metric.go (Task 4), harness.go (Task 5), and main.go printer (Task 6). ✅
- `writeWithRetry(c, docID, agentID, payload, base, p) (int, error)` — same between Task 1 (coordinated) and Task 3 (baseline) consumers. ✅
- `store.Uncoordinated() Option` + variadic `NewMemStore` — Task 2 def; consumed in Task 5 harness. ✅
- `RunBaselineConcurrent(base, docs, agentIDs, k, p) ([]Stats, error)` — Task 3 def; Task 5 caller. ✅

**⚠️ Watch-fors for the implementer:**
- **Baseline retry cap must exceed worst-case per-doc contention.** Under N agents, a doc can see up to N−1 conflicts before an agent commits its (duplicate) version. The harness/CLI use `MaxAttempts` generously above N (`4*n+32`). If a baseline run's finding count is LESS than `N*len(docs)`, some duplicate writes gave up — raise the cap; do NOT silently accept an undercount (it deflates the headline number). Report if raising doesn't fix it.
- **Coordinated `Conflicts` should be ~0** (leases prevent same-doc contention). Baseline `Conflicts` will be large — that is the honest contention cost, report it, don't hide it.
- **Do not run the reported benchmark under `-race`** (instrumentation skews concurrency/timing). Run correctness tests under `-race`; run the CLI numbers without it.
- **The default store stays coordinated.** `NewMemStore(clk)` must remain lease-requiring so all Phase 1+2 callers/tests are unaffected — only `Uncoordinated()` opts out.
- Re-run `go vet ./...` before the final commit (Phase 1 shipped a vet miss once).

---

## Plan sequence (remaining phases)

- **Plan 4 — Phase 4 (invariant checker + failure injection):** full I1–I5 checker over the log (incl. the deferred Seq-order guard) + injected dead-agent / forced-conflict runs; verify each injection actually triggered its condition.
- **Plan 5 — Phase 5 (latency/throughput + profiling):** measurement harness (race OFF) + pprof + one documented optimization (authored against real profile data).
- **Plan 6 — Phase 6 (writeup + packaging).**
