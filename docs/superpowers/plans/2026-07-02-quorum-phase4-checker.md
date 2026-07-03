# Quorum Phase 4 — Invariant Checker + Failure Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Quorum's consistency claims *checker-verified, not asserted*: a standalone checker replays the append-only logs and verifies invariants I1–I5, and failure-injection scenarios (dead agent mid-claim, forced write conflicts) are run through the checker — which must first confirm each injection actually triggered its condition.

**Architecture:** The store gains an append-only **lease-event log** (claim/renew/release, with timestamps) alongside the existing findings log, so lease invariants (I3 mutual exclusion, I4 recovery) become checkable offline instead of only enforced at runtime. A new `internal/checker` package replays both logs and returns a structured invariant report. Failure injection reuses existing pieces — the mock clock for deterministic lease expiry, and the Phase-3 uncoordinated store for real write contention — and every injected run is fed through the checker with an assertion that the injected condition actually occurred.

**Tech Stack:** Go (stdlib only), building on Phases 1–3 (`quorum` module). New package `internal/checker`; new command `cmd/quorum-check`.

## Global Constraints

- **Language:** Go, standard library only. No third-party modules. Module `quorum`; imports `quorum/internal/...`. Go floor `go 1.22`.
- **Invariants are FROZEN (I1–I5 in `INVARIANTS.md`).** The checker is written to satisfy them; it never redefines them. I1 and I3 apply in **coordinated mode only** (baseline runs legitimately duplicate and write without leases). I2 and I5 apply in both modes.
- **The checker replays the log; it does not re-run the store.** It is a pure function over `[]model.Finding` + `[]model.LeaseEvent` + a `coordinated bool`. No HTTP, no store access inside the checker.
- **Injected-failure runs are the whole point.** Every injection test must (a) assert the injected condition actually occurred (a lease actually expired and was reclaimed; a conflict was actually observed) — a vacuous injection that never triggers is a test failure — and (b) run the checker and assert the invariants hold.
- **Time is injected.** Lease-expiry injection uses `clock.NewMock` + `Advance`, never `time.Sleep` to cross a TTL. No `time.Now()` outside `internal/clock`.
- **Lease events are appended under the existing store mutex.** No second mutex; lock ordering unchanged (lease-event append is a leaf write to a store-owned slice, like `entries`).
- **`-race` on for all tests.** TDD: failing test first. One commit per task. Every commit message ends with:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`

---

## File Structure

**Modified:**
```
internal/model/model.go        # add LeaseEvent
internal/store/store.go        # leaseSeq/leaseEvents fields; append events in Claim/Renew/Release; LeaseEvents()
internal/store/store_test.go   # lease-event logging tests
internal/replay/replay.go      # add Seq-order guard (deferred carry-forward)
internal/replay/replay_test.go
internal/agent/driver_test.go  # wire the checker into the coordinated N-agent run
INVARIANTS.md                  # note I1–I5 are now all checker-verified
README.md                      # checker + failure-injection note
```

**Created:**
```
internal/checker/checker.go       # Check(findings, leaseEvents, coordinated) Report over I1–I5
internal/checker/checker_test.go  # crafted good logs + one crafted violation per invariant
internal/checker/inject_test.go   # failure injection: dead-agent recovery + forced conflicts, checker-verified
cmd/quorum-check/main.go          # runs a coordinated scenario, replays logs, prints the invariant report
```

**Responsibility boundaries:**
- `internal/checker` is pure analysis: log slices in, `Report` out. No store, no HTTP, no clock. This is what makes it credible and trivially unit-testable with crafted logs.
- Lease-event logging lives in the store next to finding logging (same mutex, same append-only discipline); the store stays the single source of truth.
- Failure injection lives in `checker/inject_test.go` (it exists to feed the checker), not in production code.

---

### Task 1: Lease-event logging in the store

**Files:**
- Modify: `internal/model/model.go` (add `LeaseEvent`)
- Modify: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces:
  - `model.LeaseEvent{ Seq int64; Kind string; DocID, AgentID string; LeaseExpiry time.Time; Timestamp time.Time }` (JSON snake_case). `Kind` is one of `"claim"`, `"renew"`, `"release"`.
  - `(*MemStore).LeaseEvents() []model.LeaseEvent` — defensive copy, in append order.
- Behavior: `Claim` (on success), `Renew` (on success), `Release` (on success) each append one `LeaseEvent` with a monotonic `Seq` starting at 1, `Timestamp = s.clk.Now()`, and `LeaseExpiry` set for claim/renew (zero for release). Appends happen under the existing `s.mu`.

- [ ] **Step 1: Add `LeaseEvent` to `internal/model/model.go`**

```go
// LeaseEvent is one append-only lease-log record: a claim, renew, or release.
type LeaseEvent struct {
	Seq         int64     `json:"seq"`
	Kind        string    `json:"kind"` // "claim" | "renew" | "release"
	DocID       string    `json:"doc_id"`
	AgentID     string    `json:"agent_id"`
	LeaseExpiry time.Time `json:"lease_expiry"`
	Timestamp   time.Time `json:"timestamp"`
}
```

- [ ] **Step 2: Write the failing test (append to `internal/store/store_test.go`)**

```go
func TestLeaseEventsAreLogged(t *testing.T) {
	clk := clock.NewMock(time.Unix(1_700_000_000, 0))
	s := NewMemStore(clk)

	if _, err := s.Claim("d1", "agent-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Second)
	if _, err := s.Renew("d1", "agent-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Release("d1", "agent-a"); err != nil {
		t.Fatal(err)
	}

	ev := s.LeaseEvents()
	if len(ev) != 3 {
		t.Fatalf("got %d lease events, want 3", len(ev))
	}
	if ev[0].Kind != "claim" || ev[1].Kind != "renew" || ev[2].Kind != "release" {
		t.Fatalf("kinds = %s,%s,%s", ev[0].Kind, ev[1].Kind, ev[2].Kind)
	}
	if ev[0].Seq != 1 || ev[1].Seq != 2 || ev[2].Seq != 3 {
		t.Fatalf("seqs = %d,%d,%d", ev[0].Seq, ev[1].Seq, ev[2].Seq)
	}
	if !ev[0].LeaseExpiry.Equal(time.Unix(1_700_000_000, 0).Add(time.Minute)) {
		t.Fatalf("claim expiry = %v", ev[0].LeaseExpiry)
	}
	if ev[0].AgentID != "agent-a" || ev[0].DocID != "d1" {
		t.Fatalf("claim event = %+v", ev[0])
	}
}

func TestLeaseEventsSnapshotIsDefensiveCopy(t *testing.T) {
	s := NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	s.Claim("d1", "a", time.Minute)
	snap := s.LeaseEvents()
	snap[0].AgentID = "mutated"
	if s.LeaseEvents()[0].AgentID != "a" {
		t.Fatal("LeaseEvents must return a defensive copy")
	}
}

// A refused claim (live lease held by another) must NOT log an event.
func TestRefusedClaimLogsNoEvent(t *testing.T) {
	s := NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	s.Claim("d1", "agent-a", time.Minute)
	_, _ = s.Claim("d1", "agent-b", time.Minute) // refused
	ev := s.LeaseEvents()
	if len(ev) != 1 {
		t.Fatalf("got %d events, want 1 (refused claim must not log)", len(ev))
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `export PATH="/opt/homebrew/bin:$PATH"; go test ./internal/store/ -run TestLease`
Expected: FAIL — `LeaseEvents` undefined.

- [ ] **Step 4: Implement in `internal/store/store.go`**

Add fields to the `MemStore` struct (next to `claims`):

```go
	leaseSeq    int64
	leaseEvents []model.LeaseEvent
```

Add a small unexported helper (called only while `s.mu` is held):

```go
// appendLeaseEvent records a lease-log event. Caller must hold s.mu.
func (s *MemStore) appendLeaseEvent(kind, docID, agentID string, expiry, now time.Time) {
	s.leaseSeq++
	s.leaseEvents = append(s.leaseEvents, model.LeaseEvent{
		Seq:         s.leaseSeq,
		Kind:        kind,
		DocID:       docID,
		AgentID:     agentID,
		LeaseExpiry: expiry,
		Timestamp:   now,
	})
}
```

In `Claim`, immediately after `s.claims[docID] = c` (the success path), add:

```go
	s.appendLeaseEvent("claim", docID, agentID, c.LeaseExpiry, now)
```

In `Renew`, immediately after `s.claims[docID] = cur` (success), add:

```go
	s.appendLeaseEvent("renew", docID, agentID, cur.LeaseExpiry, now)
```

In `Release`, immediately after clearing the claim (success), add (release has no expiry):

```go
	s.appendLeaseEvent("release", docID, agentID, time.Time{}, now)
```

Add the accessor:

```go
// LeaseEvents returns a defensive copy of the append-only lease-event log.
func (s *MemStore) LeaseEvents() []model.LeaseEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.LeaseEvent, len(s.leaseEvents))
	copy(out, s.leaseEvents)
	return out
}
```

> Note: `Claim`/`Renew`/`Release` already compute `now := s.clk.Now()` and hold `s.mu`; reuse that `now`. Do NOT call `s.clk.Now()` a second time.

- [ ] **Step 5: Run tests under race**

Run: `go test -race ./internal/store/`
Expected: PASS (new lease-event tests + all prior store tests).

- [ ] **Step 6: Commit**

```bash
git add internal/model/model.go internal/store/store.go internal/store/store_test.go
git commit -m "feat: append-only lease-event log (claim/renew/release) in the store

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Replay Seq-order guard (deferred carry-forward)

**Files:**
- Modify: `internal/replay/replay.go`
- Test: `internal/replay/replay_test.go`

**Interfaces:**
- `replay.Replay(findings []model.Finding) (map[string]int, error)` — unchanged signature, now also returns an error if the findings are NOT in strictly ascending `Seq` order (before it trusted append order silently).

- [ ] **Step 1: Write the failing test (append to `replay_test.go`)**

```go
func TestReplayRejectsOutOfOrderSeq(t *testing.T) {
	fs := []model.Finding{
		{Seq: 2, DocID: "d1", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 1, DocID: "d2", BaseVersion: 0, CommittedVersion: 1},
	}
	if _, err := Replay(fs); err == nil {
		t.Fatal("expected an out-of-order Seq error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/replay/ -run TestReplayRejectsOutOfOrder`
Expected: FAIL — Replay currently accepts any order.

- [ ] **Step 3: Add the guard in `internal/replay/replay.go`**

At the top of the loop body in `Replay`, track the previous seq and reject a non-increasing one. Replace the loop with:

```go
	versions := map[string]int{}
	var prevSeq int64
	for _, f := range findings {
		if f.Seq <= prevSeq {
			return nil, fmt.Errorf("out-of-order or duplicate seq %d (previous %d)", f.Seq, prevSeq)
		}
		prevSeq = f.Seq
		if f.BaseVersion != versions[f.DocID] {
			return nil, fmt.Errorf("doc %s seq %d: base_version %d != running %d",
				f.DocID, f.Seq, f.BaseVersion, versions[f.DocID])
		}
		if f.CommittedVersion != versions[f.DocID]+1 {
			return nil, fmt.Errorf("doc %s seq %d: committed_version %d != running+1 %d",
				f.DocID, f.Seq, f.CommittedVersion, versions[f.DocID]+1)
		}
		versions[f.DocID] = f.CommittedVersion
	}
	return versions, nil
```

(Keep the existing function signature, doc comment, and the `fmt`/`model` imports.)

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/replay/`
Expected: PASS (new guard test + existing valid/gap tests).

- [ ] **Step 5: Commit**

```bash
git add internal/replay/replay.go internal/replay/replay_test.go
git commit -m "fix: reject out-of-order sequence numbers in replay (I5 completeness)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Checker — findings invariants (I1, I2, I5-findings)

**Files:**
- Create: `internal/checker/checker.go`
- Test: `internal/checker/checker_test.go`

**Interfaces:**
- Produces:
  - `type Violation struct { Invariant, Detail string }`
  - `type Report struct { Violations []Violation }` with `func (r Report) OK() bool { return len(r.Violations) == 0 }`
  - `func Check(findings []model.Finding, lease []model.LeaseEvent, coordinated bool) Report`
- This task implements the findings-based checks; the lease-based checks (I3/I4) are added in Task 4 (the `lease` param is accepted now, used in Task 4).
  - **I5 (findings):** findings `Seq` strictly increasing from 1 with no gaps.
  - **I2:** per doc, `BaseVersion == running` and `CommittedVersion == running+1` (delegate to `replay.Replay`; a replay error is an I2 violation).
  - **I1 (coordinated only):** each `doc_id` appears in at most one committed finding. Skipped when `coordinated == false`.

- [ ] **Step 1: Write the failing tests**

```go
package checker

import (
	"testing"
	"time"

	"quorum/internal/model"
)

func goodCoordinatedFindings() []model.Finding {
	return []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(100, 0)},
		{Seq: 2, DocID: "d2", AgentID: "b", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(101, 0)},
	}
}

func TestCheckPassesOnCleanCoordinatedLog(t *testing.T) {
	r := Check(goodCoordinatedFindings(), nil, true)
	if !r.OK() {
		t.Fatalf("expected OK, got violations: %+v", r.Violations)
	}
}

func TestCheckI1FlagsDuplicateInCoordinatedMode(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", AgentID: "b", BaseVersion: 1, CommittedVersion: 2}, // d1 twice
	}
	r := Check(fs, nil, true)
	if r.OK() || !hasInvariant(r, "I1") {
		t.Fatalf("expected I1 violation, got %+v", r.Violations)
	}
}

func TestCheckI1AllowsDuplicatesInBaselineMode(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", AgentID: "b", BaseVersion: 1, CommittedVersion: 2},
	}
	r := Check(fs, nil, false) // baseline: duplicates are expected, not an I1 violation
	if !r.OK() {
		t.Fatalf("baseline duplicates should not violate I1: %+v", r.Violations)
	}
}

func TestCheckI2FlagsBrokenVersionChain(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", BaseVersion: 0, CommittedVersion: 2}, // base should be 1
	}
	r := Check(fs, nil, false)
	if r.OK() || !hasInvariant(r, "I2") {
		t.Fatalf("expected I2 violation, got %+v", r.Violations)
	}
}

func TestCheckI5FlagsSeqGap(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 3, DocID: "d2", BaseVersion: 0, CommittedVersion: 1}, // gap: 2 missing
	}
	r := Check(fs, nil, false)
	if r.OK() || !hasInvariant(r, "I5") {
		t.Fatalf("expected I5 violation, got %+v", r.Violations)
	}
}

func hasInvariant(r Report, inv string) bool {
	for _, v := range r.Violations {
		if v.Invariant == inv {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/checker/`
Expected: FAIL — `Check` undefined.

- [ ] **Step 3: Write `internal/checker/checker.go`**

```go
// Package checker replays Quorum's append-only logs and verifies invariants
// I1–I5. It is a pure function over the logs (no store, no clock, no HTTP), so
// consistency is checked, not asserted.
package checker

import (
	"fmt"

	"quorum/internal/model"
	"quorum/internal/replay"
)

type Violation struct {
	Invariant string // "I1".."I5"
	Detail    string
}

type Report struct {
	Violations []Violation
}

func (r Report) OK() bool { return len(r.Violations) == 0 }

func (r *Report) flag(inv, format string, args ...any) {
	r.Violations = append(r.Violations, Violation{Invariant: inv, Detail: fmt.Sprintf(format, args...)})
}

// Check verifies I1–I5 over the findings and lease-event logs. I1 and I3 apply
// only in coordinated mode; I2 and I5 apply always. coordinated indicates
// whether the run used lease-based coordination.
func Check(findings []model.Finding, lease []model.LeaseEvent, coordinated bool) Report {
	var r Report
	checkFindingsSeq(&r, findings)     // I5 (findings)
	checkVersionChains(&r, findings)   // I2
	if coordinated {
		checkNoDuplicates(&r, findings) // I1
	}
	checkLeaseInvariants(&r, findings, lease, coordinated) // I3/I4/I5(lease) — Task 4
	return r
}

// checkFindingsSeq verifies I5: strictly increasing Seq from 1, no gaps.
func checkFindingsSeq(r *Report, findings []model.Finding) {
	want := int64(1)
	for _, f := range findings {
		if f.Seq != want {
			r.flag("I5", "findings seq = %d, want %d (gap or out-of-order)", f.Seq, want)
			return
		}
		want++
	}
}

// checkVersionChains verifies I2 via replay (base==running, committed==running+1).
func checkVersionChains(r *Report, findings []model.Finding) {
	if _, err := replay.Replay(findings); err != nil {
		r.flag("I2", "version chain broken: %v", err)
	}
}

// checkNoDuplicates verifies I1: each doc committed at most once (coordinated).
func checkNoDuplicates(r *Report, findings []model.Finding) {
	seen := make(map[string]int64, len(findings))
	for _, f := range findings {
		if prev, ok := seen[f.DocID]; ok {
			r.flag("I1", "doc %s committed more than once (seq %d and %d)", f.DocID, prev, f.Seq)
			return
		}
		seen[f.DocID] = f.Seq
	}
}
```

Add a temporary no-op stub for the lease check so the package compiles this task (Task 4 replaces its body):

```go
// checkLeaseInvariants verifies I3/I4 and lease-log I5. Implemented in Task 4.
func checkLeaseInvariants(r *Report, findings []model.Finding, lease []model.LeaseEvent, coordinated bool) {
	_ = r
	_ = findings
	_ = lease
	_ = coordinated
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/checker/`
Expected: PASS (all findings-invariant tests).

- [ ] **Step 5: Commit**

```bash
git add internal/checker/checker.go internal/checker/checker_test.go
git commit -m "feat: invariant checker — findings invariants (I1/I2/I5)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Checker — lease-timeline invariants (I3, I4, I5-lease)

**Files:**
- Modify: `internal/checker/checker.go` (replace the `checkLeaseInvariants` stub)
- Test: `internal/checker/checker_test.go`

**Interfaces:**
- Fills in `checkLeaseInvariants`. It reconstructs per-doc lease-ownership intervals from the lease events and verifies:
  - **I5 (lease):** lease `Seq` strictly increasing from 1, no gaps.
  - **I3 (mutual exclusion):** no `claim` is granted to a different agent while the doc's current lease is still live (i.e., at `claim.Timestamp`, the previous owner's lease had expired or been released). And every write finding's author held a live lease covering the write's `Timestamp`.
  - **I4 (recovery):** implied by I3's interval reconstruction — a reclaim after expiry starts a new legal interval; this is verified by the same "grant only after prior interval ended" rule. (No separate code path; a violation surfaces as I3.)
- All lease checks apply in coordinated mode only (baseline runs have no lease events; if `!coordinated`, only the lease-Seq check runs and it is trivially satisfied on an empty log).

- [ ] **Step 1: Write the failing tests (append to `checker_test.go`)**

```go
func TestCheckLeasePassesOnCleanCoordinatedRun(t *testing.T) {
	// agent-a claims d1 at t=100 (expiry 160), writes at t=105, releases at t=110.
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
		{Seq: 2, Kind: "release", DocID: "d1", AgentID: "a", Timestamp: time.Unix(110, 0)},
	}
	findings := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(105, 0)},
	}
	r := Check(findings, lease, true)
	if !r.OK() {
		t.Fatalf("expected OK, got %+v", r.Violations)
	}
}

func TestCheckI3FlagsOverlappingLeases(t *testing.T) {
	// agent-b claims d1 at t=120 while agent-a's lease (expiry 160) is still live.
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
		{Seq: 2, Kind: "claim", DocID: "d1", AgentID: "b", LeaseExpiry: time.Unix(180, 0), Timestamp: time.Unix(120, 0)},
	}
	r := Check(nil, lease, true)
	if r.OK() || !hasInvariant(r, "I3") {
		t.Fatalf("expected I3 violation, got %+v", r.Violations)
	}
}

func TestCheckI4RecoveryAfterExpiryIsLegal(t *testing.T) {
	// agent-a claims (expiry 160), dies. agent-b claims at t=170 (after expiry) — legal.
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
		{Seq: 2, Kind: "claim", DocID: "d1", AgentID: "b", LeaseExpiry: time.Unix(230, 0), Timestamp: time.Unix(170, 0)},
	}
	findings := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "b", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(175, 0)},
	}
	r := Check(findings, lease, true)
	if !r.OK() {
		t.Fatalf("post-expiry reclaim should be legal, got %+v", r.Violations)
	}
}

func TestCheckI3FlagsWriteWithoutLease(t *testing.T) {
	// A write at t=200 but the only lease expired at t=160 — no live lease covers it.
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
	}
	findings := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(200, 0)},
	}
	r := Check(findings, lease, true)
	if r.OK() || !hasInvariant(r, "I3") {
		t.Fatalf("expected I3 (write without live lease), got %+v", r.Violations)
	}
}

func TestCheckLeaseI5FlagsSeqGap(t *testing.T) {
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
		{Seq: 3, Kind: "release", DocID: "d1", AgentID: "a", Timestamp: time.Unix(110, 0)}, // gap
	}
	r := Check(nil, lease, true)
	if r.OK() || !hasInvariant(r, "I5") {
		t.Fatalf("expected lease I5 violation, got %+v", r.Violations)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/checker/ -run TestCheckLease`
Expected: FAIL — the stub does nothing, so violations aren't flagged.

- [ ] **Step 3: Replace the `checkLeaseInvariants` stub in `internal/checker/checker.go`**

```go
import (
	"fmt"
	"time"

	"quorum/internal/model"
	"quorum/internal/replay"
)

// leaseInterval is one doc's ownership window [Start, End) held by Agent.
type leaseInterval struct {
	Agent string
	Start time.Time
	End   time.Time // expiry, or release time if released earlier
	Ended bool      // true once released
}

// checkLeaseInvariants verifies lease-log I5, I3 (mutual exclusion + write
// coverage), and I4 (legal reclaim after expiry). Lease events exist only in
// coordinated runs; in baseline mode the lease log is empty and only the
// (trivial) lease-Seq check runs.
func checkLeaseInvariants(r *Report, findings []model.Finding, lease []model.LeaseEvent, coordinated bool) {
	// I5 (lease): strictly increasing Seq from 1, no gaps.
	want := int64(1)
	for _, e := range lease {
		if e.Seq != want {
			r.flag("I5", "lease seq = %d, want %d (gap or out-of-order)", e.Seq, want)
			return
		}
		want++
	}
	if !coordinated {
		return
	}

	// Reconstruct the current lease per doc while replaying events in order.
	// A claim by a different agent while the current lease is still live is an
	// I3 violation; a legal claim (after expiry or release) starts a new
	// interval. Renew extends the current interval's End.
	current := map[string]*leaseInterval{}
	intervals := map[string][]leaseInterval{}

	closeCurrent := func(doc string, end time.Time) {
		if cur := current[doc]; cur != nil {
			cur.End = end
			cur.Ended = true
			intervals[doc] = append(intervals[doc], *cur)
			current[doc] = nil
		}
	}

	for _, e := range lease {
		cur := current[e.DocID]
		switch e.Kind {
		case "claim":
			if cur != nil && !cur.Ended && e.Timestamp.Before(cur.End) && cur.Agent != e.AgentID {
				r.flag("I3", "doc %s: %s claimed at %v while %s held a live lease until %v",
					e.DocID, e.AgentID, e.Timestamp, cur.Agent, cur.End)
			}
			// Close any prior interval (legal reclaim after expiry, or a
			// same-agent re-claim) and open a new one.
			if cur != nil {
				intervals[e.DocID] = append(intervals[e.DocID], *cur)
			}
			current[e.DocID] = &leaseInterval{Agent: e.AgentID, Start: e.Timestamp, End: e.LeaseExpiry}
		case "renew":
			if cur == nil || cur.Agent != e.AgentID {
				r.flag("I3", "doc %s: renew by %s who does not hold the lease", e.DocID, e.AgentID)
				continue
			}
			cur.End = e.LeaseExpiry // extend
		case "release":
			closeCurrent(e.DocID, e.Timestamp)
		}
	}
	// Flush open intervals.
	for doc, cur := range current {
		if cur != nil {
			intervals[doc] = append(intervals[doc], *cur)
		}
	}

	// I3 write coverage: every write finding's author held a live lease covering
	// the write's Timestamp.
	for _, f := range findings {
		if !writeCovered(intervals[f.DocID], f.AgentID, f.Timestamp) {
			r.flag("I3", "doc %s: write by %s at %v not covered by a live lease",
				f.DocID, f.AgentID, f.Timestamp)
		}
	}
}

// writeCovered reports whether some interval held by agent covers ts (Start <= ts < End).
func writeCovered(ivs []leaseInterval, agent string, ts time.Time) bool {
	for _, iv := range ivs {
		if iv.Agent == agent && !ts.Before(iv.Start) && ts.Before(iv.End) {
			return true
		}
	}
	return false
}
```

> NOTE: this replaces the Task-3 stub. Keep the `Check` function body and the findings checks unchanged. Add `"time"` to the imports.

- [ ] **Step 4: Run tests under race**

Run: `go test -race ./internal/checker/`
Expected: PASS (all findings AND lease-timeline tests).

- [ ] **Step 5: Commit**

```bash
git add internal/checker/checker.go internal/checker/checker_test.go
git commit -m "feat: invariant checker — lease-timeline invariants (I3/I4/I5)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Failure injection — checker-verified, condition-confirmed

**Files:**
- Create: `internal/checker/inject_test.go`

**Interfaces:**
- No production code. Two injection scenarios, each asserting the injected condition ACTUALLY occurred, then running `checker.Check` and asserting invariants hold.
- Consumes: `store` (direct, with `clock.NewMock`), `agent.RunBaselineConcurrent`, `api`, `httptest`, `checker`.

- [ ] **Step 1: Write the injection tests**

```go
package checker

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/agent"
	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/corpus"
	"quorum/internal/retry"
	"quorum/internal/store"
)

// Injection 1: an agent claims and dies mid-claim (never writes/releases).
// After the TTL expires, another agent reclaims and commits. We assert the
// injected condition (a reclaim-after-expiry actually happened) AND that the
// checker reports all invariants hold.
func TestInjectDeadAgentRecovery(t *testing.T) {
	clk := clock.NewMock(time.Unix(1_700_000_000, 0))
	s := store.NewMemStore(clk)

	// agent-a claims d1 and "dies" — no write, no release.
	if _, err := s.Claim("d1", "agent-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	// Confirm the injection precondition: before expiry, agent-b is locked out.
	if _, err := s.Claim("d1", "agent-b", time.Minute); err == nil {
		t.Fatal("injection setup wrong: agent-b claimed while agent-a's lease was live")
	}

	// TTL passes; agent-b reclaims and commits.
	clk.Advance(90 * time.Second)
	if _, err := s.Claim("d1", "agent-b", time.Minute); err != nil {
		t.Fatalf("reclaim after expiry failed: %v", err)
	}
	if _, err := s.Write("d1", "agent-b", "recovered", 0); err != nil {
		t.Fatalf("write after reclaim failed: %v", err)
	}

	// Assert the injected condition actually triggered: the lease log shows a
	// reclaim by a different agent AFTER the first lease's expiry.
	ev := s.LeaseEvents()
	if !sawReclaimAfterExpiry(ev) {
		t.Fatal("injection did not trigger: no reclaim-after-expiry in the lease log")
	}

	// The checker must report all invariants hold for this recovered run.
	r := Check(s.Findings(), ev, true)
	if !r.OK() {
		t.Fatalf("checker flagged violations after legal recovery: %+v", r.Violations)
	}
}

func sawReclaimAfterExpiry(ev []model.LeaseEvent) bool {
	type live struct {
		agent  string
		expiry time.Time
	}
	cur := map[string]live{}
	for _, e := range ev {
		if e.Kind == "claim" {
			if c, ok := cur[e.DocID]; ok && c.agent != e.AgentID && !e.Timestamp.Before(c.expiry) {
				return true // a different agent claimed after the prior lease expired
			}
			cur[e.DocID] = live{agent: e.AgentID, expiry: e.LeaseExpiry}
		}
	}
	return false
}

// Injection 2: force real write conflicts via the uncoordinated store (N agents
// contend on every doc). Assert conflicts actually occurred (>0) AND the checker
// reports no lost updates (I2) / clean version chains despite the contention.
func TestInjectForcedConflictsNoLostUpdates(t *testing.T) {
	s := store.NewMemStore(clock.Real{}, store.Uncoordinated())
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()
	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("agent-%d", i)
	}

	stats, err := agent.RunBaselineConcurrent(ts.URL, docs, ids, 3, retry.Policy{MaxAttempts: 128, BaseDelay: 0})
	if err != nil {
		t.Fatal(err)
	}
	// Assert the injected condition actually triggered: conflicts were observed.
	total := 0
	for _, st := range stats {
		total += st.Conflicts
	}
	if total == 0 {
		t.Fatal("injection did not trigger: zero conflicts under 8-way contention")
	}

	// Baseline mode: duplicates are expected (I1 off), but I2 (no lost updates)
	// and I5 must hold — the checker confirms.
	r := Check(s.Findings(), s.LeaseEvents(), false)
	if !r.OK() {
		t.Fatalf("checker flagged violations under forced conflicts: %+v", r.Violations)
	}
}
```

> NOTE: `inject_test.go` is `package checker` (same package) so it can call `Check` and the unexported helpers. Add the `model` import used by `sawReclaimAfterExpiry` (`quorum/internal/model`).

- [ ] **Step 2: Run the injection tests under race**

Run: `export PATH="/opt/homebrew/bin:$PATH"; go test -race ./internal/checker/ -run TestInject`
Expected: PASS. Both injections must trigger their condition (the `t.Fatal` guards prove it) and the checker must report OK.

- [ ] **Step 3: Full package + suite**

Run: `go test -race ./internal/checker/ && go test -race ./...`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/checker/inject_test.go
git commit -m "test: failure injection (dead-agent recovery, forced conflicts) checker-verified

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Wire the checker into driver tests + `quorum-check` CLI

**Files:**
- Modify: `internal/agent/driver_test.go` (assert the checker passes after a coordinated run)
- Create: `cmd/quorum-check/main.go`
- Modify: `INVARIANTS.md`, `README.md`

**Interfaces:**
- The coordinated N-agent driver test additionally pulls `store.Findings()` + `store.LeaseEvents()` and asserts `checker.Check(..., true).OK()`.
- `quorum-check` runs a coordinated scenario in-process, replays the logs through the checker, and prints the per-invariant report (exit non-zero on any violation).

- [ ] **Step 1: Add a checker assertion to the coordinated driver test**

In `internal/agent/driver_test.go`, the existing `TestRunConcurrentNoDuplicateAnnotations` builds a store `s`, runs `RunConcurrent`, and reads findings. After its existing assertions, add (the test already has `s` in scope):

```go
			// The invariant checker must independently confirm the run is clean.
			rep := checker.Check(s.Findings(), s.LeaseEvents(), true)
			if !rep.OK() {
				t.Fatalf("checker flagged violations: %+v", rep.Violations)
			}
```

Add `"quorum/internal/checker"` to the test file's imports.

- [ ] **Step 2: Run the driver test**

Run: `go test -race ./internal/agent/ -run TestRunConcurrent`
Expected: PASS (checker confirms every N=2/4/8 coordinated run).

- [ ] **Step 3: Write `cmd/quorum-check/main.go`**

```go
package main

import (
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	"quorum/internal/agent"
	"quorum/internal/api"
	"quorum/internal/checker"
	"quorum/internal/clock"
	"quorum/internal/corpus"
	"quorum/internal/retry"
	"quorum/internal/store"
)

func main() {
	corpusPath := flag.String("corpus", "corpus/fixture.jsonl", "corpus JSONL path")
	agents := flag.Int("agents", 8, "number of concurrent agents")
	k := flag.Int("k", 5, "keywords per annotation")
	ttl := flag.Duration("ttl", 60*time.Second, "lease TTL")
	flag.Parse()

	docs, err := corpus.Load(*corpusPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load corpus: %v\n", err)
		os.Exit(2)
	}

	s := store.NewMemStore(clock.Real{})
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()

	ids := make([]string, *agents)
	for i := range ids {
		ids[i] = fmt.Sprintf("agent-%d", i)
	}
	if _, err := agent.RunConcurrent(ts.URL, docs, ids, *k, *ttl, retry.Default()); err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		os.Exit(2)
	}

	rep := checker.Check(s.Findings(), s.LeaseEvents(), true)
	fmt.Printf("Quorum invariant check — %d agents, %d docs\n", *agents, len(docs))
	fmt.Printf("findings=%d lease-events=%d\n", len(s.Findings()), len(s.LeaseEvents()))
	if rep.OK() {
		fmt.Println("ALL INVARIANTS HOLD (I1–I5)")
		return
	}
	fmt.Printf("VIOLATIONS (%d):\n", len(rep.Violations))
	for _, v := range rep.Violations {
		fmt.Printf("  [%s] %s\n", v.Invariant, v.Detail)
	}
	os.Exit(1)
}
```

- [ ] **Step 4: Build + run the checker CLI**

Run: `go build ./... && go run ./cmd/quorum-check -corpus corpus/fixture.jsonl -agents 8`
Expected: prints `ALL INVARIANTS HOLD (I1–I5)` and exits 0.

- [ ] **Step 5: Update `INVARIANTS.md` and `README.md`**

In `INVARIANTS.md`, replace the last line (`Phase 1 exercises I2 and I5...`) with:

```markdown
All five invariants are now verified by an automated checker (`internal/checker`)
that replays the findings and lease-event logs — including under injected
failures (dead-agent recovery, forced write conflicts). I1 and I3 apply in
coordinated mode; I2 and I5 apply always.
```

In `README.md`, add a short section:

```markdown
## Consistency: checker-verified, not asserted

    go run ./cmd/quorum-check -corpus corpus/arxiv.jsonl -agents 16

Replays the append-only findings + lease-event logs and verifies I1–I5 (no
duplicate committed annotations, no lost updates, lease mutual exclusion,
expiry recovery, log integrity). Failure-injection tests kill an agent
mid-claim and force write conflicts, then run the same checker — the injected
condition is asserted to actually occur before the invariants are checked.
```

- [ ] **Step 6: Full suite under race + vet**

Run: `go vet ./... && go test -race ./...`
Expected: all PASS, vet clean.

- [ ] **Step 7: Commit**

```bash
git add internal/agent/driver_test.go cmd/quorum-check/ INVARIANTS.md README.md
git commit -m "feat: wire checker into driver tests + quorum-check CLI; docs

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage (PRD Phase 4):**
- Checker replays the log and verifies invariants → Tasks 3–4 (`internal/checker`, pure over both logs). ✅
- Failure injection: kill agents mid-claim, verify lease expiry + reclamation → Task 5 injection 1 (mock clock, checker-verified, condition asserted). ✅
- Force write conflicts, verify no lost updates → Task 5 injection 2 (uncoordinated contention, conflicts>0 asserted, checker confirms I2). ✅
- Consistency claims checker-verified under injected failures (the unfakeable part) → Task 5 + Task 6 (driver tests + CLI). ✅
- Checker verifies lease-expiry recovery correctness → I3/I4 timeline reconstruction (Task 4) + injection 1 (Task 5). ✅
- Carry-forward: `replay.Replay` Seq-order guard → Task 2. ✅
- Carry-forward: injected failures must actually trigger → Task 5 asserts the condition before checking (the checker isn't fed a happy path pretending to be an injection). ✅
- Enabling decision: lease-event logging → Task 1 (consistent with INVARIANTS.md's existing mention of "the lease event log"). ✅

**Placeholder scan:** Task 3 ships a deliberate no-op `checkLeaseInvariants` stub that Task 4 replaces — this is sequenced (Task 3's tests don't exercise lease checks; Task 4 adds them). Not a placeholder in shipped-at-end code: after Task 4 the stub is gone. No TBD/TODO.

**Type consistency:**
- `model.LeaseEvent` fields identical across model/store/checker. ✅
- `checker.Check(findings []model.Finding, lease []model.LeaseEvent, coordinated bool) Report` — same across Tasks 3/4/5/6 and the CLI. ✅
- `Report.OK()`, `Violation{Invariant, Detail}` — consistent. ✅
- `store.LeaseEvents() []model.LeaseEvent` — Task 1 def; consumed in Tasks 5/6 + CLI. ✅
- `replay.Replay` signature unchanged (Task 2 only adds an internal guard). ✅

**⚠️ Watch-fors for the implementer:**
- **Lease-event append reuses the `now` already computed** in Claim/Renew/Release — do not call `s.clk.Now()` twice (it would desync the event timestamp from the lease expiry).
- **I1 and I3 are coordinated-only.** Baseline runs MUST NOT be flagged for duplicates (I1) or missing leases (I3) — those are expected in baseline. The `coordinated` flag gates both. The forced-conflict injection (Task 5.2) runs the checker with `coordinated=false` for exactly this reason.
- **The injection tests must fail loudly if the condition doesn't trigger** — the `t.Fatal("injection did not trigger…")` guards are the point of Phase 4; do not weaken them into `t.Skip` or remove them.
- **Write coverage uses the same clock** as the lease events (both from the store's clock), so timestamps are comparable; `writeCovered` uses `Start <= ts < End`. Do not compare against wall-clock `time.Now()`.
- Re-run `go vet ./...` before the final commit.

---

## Plan sequence (remaining phases)

- **Plan 5 — Phase 5 (latency/throughput + profiling):** measurement harness with keep-alives RE-ENABLED and `-race` OFF; p50/p95/p99 + ops/sec at N=2/4/8/16; `pprof` the one worst bottleneck; document one before/after optimization.
- **Plan 6 — Phase 6 (writeup + packaging):** README, benchmark report, technical writeup (design, failure handling, honest limits, what I'd change), resume bullets, interview story.
