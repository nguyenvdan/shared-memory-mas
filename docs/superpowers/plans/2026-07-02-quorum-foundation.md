# Quorum Foundation (Phase 0 + Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Quorum project skeleton and a working single-agent end-to-end slice: one deterministic agent claims nothing yet, reads/annotates/writes documents to an in-memory versioned store over a real HTTP/JSON boundary, and the append-only findings log is complete and replayable.

**Architecture:** A single in-memory store process exposes an HTTP/JSON API (`/read`, `/write`, `/findings`). An append-only log is the source of truth; a derived version map holds current entries. Agents are dumb deterministic control loops that talk to the store only over HTTP — no direct-call fast path, even in-process. Time is accessed only through an injected `Clock`. This phase is deliberately sequential and unlocked (single agent); concurrency, leases, and CAS-retry arrive in Plan 2 (Phase 2).

**Tech Stack:** Go (pure, stdlib only — `net/http`, `encoding/json`, `testing`, `httptest`). Module path `quorum`. No third-party dependencies.

## Global Constraints

- **Language:** Go, standard library only. No third-party modules in `go.mod` require blocks for this plan.
- **Module path:** `quorum` (local module name; avoids guessing the GitHub remote path). All imports are `quorum/internal/...`.
- **Go version floor:** `go 1.22` in `go.mod`.
- **Network boundary is real:** agents reach the store only via HTTP/JSON. No function-call shortcut anywhere in the agent→store path, including tests that exercise the agent (use `httptest`).
- **Time is injected:** no code outside `internal/clock` calls `time.Now()` directly. Everything that needs the current time takes a `clock.Clock`.
- **Concurrency-safe from the start:** every shared mutable struct (`Log`, `MemStore`, mock clock) is guarded by a mutex now, even though this phase is single-agent — per Dan's "just make everything safe."
- **Deterministic:** the annotator is a pure function; identical input yields byte-identical output. No randomness, no wall-clock in annotation.
- **Invariants are the spec:** `INVARIANTS.md` (I1–I5) is written in Task 1, before store code. The Phase 4 checker will be written to it, not the reverse.
- **TDD:** every code task writes a failing test first, watches it fail, then implements. Commit after each task.
- **Run tests with `-race`** for any test that touches shared state (`go test -race ./...`), even in this single-agent phase, to catch mistakes early.

---

## File Structure

```
go.mod                          # module quorum, go 1.22
INVARIANTS.md                   # I1–I5 — the consistency spec (committed)
README.md                       # minimal: what it is, how to run (fleshed out in Phase 6)
corpus/
  fixture.jsonl                 # tiny hand-written corpus for tests (committed)
  arxiv.jsonl                   # real ~300-doc snapshot (produced by fetch script)
  MANIFEST                      # sha256 of arxiv.jsonl (produced by fetch script)
cmd/
  quorum-store/main.go          # HTTP server entrypoint
  quorum-agent/main.go          # runs one agent once against a store URL (demo/E2E)
scripts/
  fetch_corpus/main.go          # one-time arXiv fetch -> corpus/arxiv.jsonl + MANIFEST
internal/
  model/model.go                # Doc, Entry, Finding types (JSON-tagged)
  clock/clock.go                # Clock interface, Real, Mock
  clock/clock_test.go
  store/log.go                  # append-only Log (monotonic seq)
  store/log_test.go
  store/store.go                # MemStore: versioned entries + write CAS + findings + lookup
  store/store_test.go
  annotate/annotate.go          # deterministic top-K keyword annotator
  annotate/annotate_test.go
  corpus/corpus.go              # JSONL loader
  corpus/corpus_test.go
  api/server.go                 # HTTP handlers over MemStore
  api/server_test.go            # httptest-based
  api/client.go                 # HTTP client used by agents
  api/client_test.go            # httptest-based
  agent/agent.go                # single-agent control loop (RunOnce)
  agent/agent_test.go           # E2E over httptest
  replay/replay.go              # fold findings -> versions; verify no gaps
  replay/replay_test.go
```

**Responsibility boundaries:**
- `model` holds only data types — no logic, no imports beyond `time`.
- `clock` is the only place `time.Now()` is called.
- `store/log.go` owns append-only ordering + sequence numbers; `store/store.go` owns entry versioning and delegates persistence to the log.
- `api` is a thin HTTP shell over `store`; it holds no business logic.
- `agent` contains the control loop and depends only on `api.Client`, `annotate`, `corpus`, `model`.
- `replay` is a pure function over a findings slice (seed of the Phase 4 checker).

---

### Task 0: Toolchain prerequisite

**Files:** none (environment setup).

- [ ] **Step 1: Install Go**

Go is not currently installed; Homebrew is present.

Run: `brew install go`

- [ ] **Step 2: Verify**

Run: `go version`
Expected: prints `go version go1.22` or newer. If older than 1.22, run `brew upgrade go`.

---

### Task 1: Project skeleton + invariants + model types

**Files:**
- Create: `go.mod`
- Create: `INVARIANTS.md`
- Create: `internal/model/model.go`
- Create: `.gitkeep` placeholders not needed — dirs come with files.

**Interfaces:**
- Produces: `model.Doc{ID, Title, Abstract string; Categories []string}`, `model.Entry{DocID string; Version int; Payload string; Exists bool}`, `model.Finding{Seq int64; DocID, AgentID, Payload string; BaseVersion, CommittedVersion int; Timestamp time.Time}`. All JSON-tagged snake_case.

- [ ] **Step 1: Initialize the module**

Run: `go mod init quorum`
Then edit `go.mod` so the version line reads:

```
module quorum

go 1.22
```

- [ ] **Step 2: Write `INVARIANTS.md` (the spec — before any store code)**

```markdown
# Quorum Invariants (I1–I5)

These are the spec. The Phase 4 checker is written to satisfy these; code is
never allowed to redefine them. All are checked by replaying the append-only
findings log (plus, from Phase 2, the lease event log).

- **I1 — No duplication (coordinated mode):** each `doc_id` has at most one
  committed annotation. `duplicate(a, b) := a.doc_id == b.doc_id`.
- **I2 — No lost updates:** every committed write's `base_version` equalled the
  entry's live version at commit time; for each doc, the number of committed
  writes equals its final `committed_version`, with no gaps.
- **I3 — Lease mutual exclusion (Phase 2):** at most one live (unexpired) lease
  per doc at any instant; every committed annotation's author held a valid
  lease at commit time.
- **I4 — Recovery (Phase 2):** after a lease expires or is released, the doc is
  reclaimable, and a reclaim produces a valid subsequent annotation.
- **I5 — Log integrity:** sequence numbers are strictly monotonic starting at 1
  with no gaps; the log is replayable to reconstruct entry state.

Phase 1 exercises I2 and I5 (single-agent). I1, I3, I4 come online in Phase 2.
```

- [ ] **Step 3: Write `internal/model/model.go`**

```go
package model

import "time"

// Doc is one corpus document. Categories is kept for optional future
// topic work but is unused by the doc-level duplication metric.
type Doc struct {
	ID         string   `json:"id"`
	Title      string   `json:"title"`
	Abstract   string   `json:"abstract"`
	Categories []string `json:"categories"`
}

// Entry is the current versioned state of a document in the store.
type Entry struct {
	DocID   string `json:"doc_id"`
	Version int    `json:"version"`
	Payload string `json:"payload"`
	Exists  bool   `json:"exists"`
}

// Finding is one append-only log record: a committed write.
type Finding struct {
	Seq              int64     `json:"seq"`
	DocID            string    `json:"doc_id"`
	AgentID          string    `json:"agent_id"`
	Payload          string    `json:"payload"`
	BaseVersion      int       `json:"base_version"`
	CommittedVersion int       `json:"committed_version"`
	Timestamp        time.Time `json:"timestamp"`
}
```

- [ ] **Step 4: Verify it builds**

Run: `go build ./... && go vet ./...`
Expected: no output, exit 0.

- [ ] **Step 5: Commit**

```bash
git add go.mod INVARIANTS.md internal/model/model.go
git commit -m "feat: project skeleton, invariants spec, and core model types"
```

---

### Task 2: Injected clock

**Files:**
- Create: `internal/clock/clock.go`
- Test: `internal/clock/clock_test.go`

**Interfaces:**
- Produces: `clock.Clock` interface with `Now() time.Time`; `clock.Real{}` (wraps `time.Now`); `clock.NewMock(t time.Time) *clock.Mock` with `Now()` and `Advance(d time.Duration)`. `Mock` is concurrency-safe.

- [ ] **Step 1: Write the failing test**

```go
package clock

import (
	"testing"
	"time"
)

func TestMockNowIsStableUntilAdvanced(t *testing.T) {
	start := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	m := NewMock(start)

	if !m.Now().Equal(start) {
		t.Fatalf("Now() = %v, want %v", m.Now(), start)
	}
	m.Advance(90 * time.Second)
	want := start.Add(90 * time.Second)
	if !m.Now().Equal(want) {
		t.Fatalf("after Advance, Now() = %v, want %v", m.Now(), want)
	}
}

func TestMockAdvanceIsRaceSafe(t *testing.T) {
	m := NewMock(time.Unix(0, 0))
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				m.Advance(time.Millisecond)
				_ = m.Now()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if m.Now().Sub(time.Unix(0, 0)) != 800*time.Millisecond {
		t.Fatalf("total advance = %v, want 800ms", m.Now().Sub(time.Unix(0, 0)))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/clock/`
Expected: FAIL — `NewMock` / `Mock` undefined.

- [ ] **Step 3: Write `internal/clock/clock.go`**

```go
package clock

import (
	"sync"
	"time"
)

// Clock is the only interface allowed to read the current time.
type Clock interface {
	Now() time.Time
}

// Real is the production clock.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

// Mock is a manually-advanced, concurrency-safe clock for tests.
type Mock struct {
	mu sync.Mutex
	t  time.Time
}

func NewMock(t time.Time) *Mock { return &Mock{t: t} }

func (m *Mock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.t
}

func (m *Mock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t = m.t.Add(d)
}
```

- [ ] **Step 4: Run tests (with race detector)**

Run: `go test -race ./internal/clock/`
Expected: PASS, no race warnings.

- [ ] **Step 5: Commit**

```bash
git add internal/clock/
git commit -m "feat: injected Clock (real + race-safe mock)"
```

---

### Task 3: Append-only log

**Files:**
- Create: `internal/store/log.go`
- Test: `internal/store/log_test.go`

**Interfaces:**
- Produces: `store.NewLog() *store.Log`; `(*Log).Append(model.Finding) model.Finding` (stamps and returns `Seq`, starting at 1, strictly monotonic); `(*Log).Snapshot() []model.Finding` (returns a defensive copy).

- [ ] **Step 1: Write the failing test**

```go
package store

import (
	"testing"

	"quorum/internal/model"
)

func TestLogAssignsMonotonicSeqFrom1(t *testing.T) {
	l := NewLog()
	a := l.Append(model.Finding{DocID: "d1"})
	b := l.Append(model.Finding{DocID: "d2"})
	if a.Seq != 1 || b.Seq != 2 {
		t.Fatalf("seqs = %d,%d want 1,2", a.Seq, b.Seq)
	}
}

func TestLogSnapshotIsADefensiveCopy(t *testing.T) {
	l := NewLog()
	l.Append(model.Finding{DocID: "d1"})
	snap := l.Snapshot()
	snap[0].DocID = "mutated"
	if l.Snapshot()[0].DocID != "d1" {
		t.Fatal("Snapshot must not alias internal storage")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/`
Expected: FAIL — `NewLog` undefined.

- [ ] **Step 3: Write `internal/store/log.go`**

```go
package store

import (
	"sync"

	"quorum/internal/model"
)

// Log is the append-only source of truth. Lock ordering: a MemStore may call
// Log.Append while holding its own mutex; Log.mu is a leaf and never calls
// back into the store, so no cycle is possible.
type Log struct {
	mu      sync.Mutex
	records []model.Finding
	seq     int64
}

func NewLog() *Log { return &Log{} }

// Append stamps the next sequence number (starting at 1) and stores the record.
func (l *Log) Append(f model.Finding) model.Finding {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	f.Seq = l.seq
	l.records = append(l.records, f)
	return f
}

// Snapshot returns a defensive copy of the log in append order.
func (l *Log) Snapshot() []model.Finding {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]model.Finding, len(l.records))
	copy(out, l.records)
	return out
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/store/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/log.go internal/store/log_test.go
git commit -m "feat: append-only log with monotonic sequence numbers"
```

---

### Task 4: In-memory versioned store

**Files:**
- Create: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Consumes: `clock.Clock`, `store.Log`, `model`.
- Produces:
  - `store.ErrVersionConflict` (sentinel `error`)
  - `store.NewMemStore(clk clock.Clock) *store.MemStore`
  - `(*MemStore).Read(docID string) model.Entry` (`Exists=false`, `Version=0` when absent)
  - `(*MemStore).Write(docID, agentID, payload string, baseVersion int) (model.Finding, error)` — returns `ErrVersionConflict` if `baseVersion != current version`; otherwise appends a finding and bumps version.
  - `(*MemStore).Findings() []model.Finding`
  - `(*MemStore).Lookup(keyword string) []model.Finding` (case-insensitive substring over payloads)

- [ ] **Step 1: Write the failing test**

```go
package store

import (
	"errors"
	"testing"
	"time"

	"quorum/internal/clock"
)

func newTestStore() *MemStore {
	return NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
}

func TestReadMissingDocReturnsVersion0(t *testing.T) {
	s := newTestStore()
	e := s.Read("nope")
	if e.Exists || e.Version != 0 {
		t.Fatalf("got %+v, want Exists=false Version=0", e)
	}
}

func TestWriteFromVersion0Commits(t *testing.T) {
	s := newTestStore()
	f, err := s.Write("d1", "agent-a", "alpha beta", 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if f.CommittedVersion != 1 || f.Seq != 1 {
		t.Fatalf("got version=%d seq=%d want 1,1", f.CommittedVersion, f.Seq)
	}
	e := s.Read("d1")
	if !e.Exists || e.Version != 1 || e.Payload != "alpha beta" {
		t.Fatalf("read after write = %+v", e)
	}
}

func TestWriteWithStaleBaseVersionConflicts(t *testing.T) {
	s := newTestStore()
	if _, err := s.Write("d1", "a", "first", 0); err != nil {
		t.Fatal(err)
	}
	_, err := s.Write("d1", "b", "second", 0) // base 0 is now stale
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
}

func TestLookupIsCaseInsensitiveSubstring(t *testing.T) {
	s := newTestStore()
	s.Write("d1", "a", "Quorum Coordination", 0)
	s.Write("d2", "a", "unrelated", 0)
	got := s.Lookup("coordination")
	if len(got) != 1 || got[0].DocID != "d1" {
		t.Fatalf("lookup = %+v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/ -run TestWrite`
Expected: FAIL — `NewMemStore` undefined.

- [ ] **Step 3: Write `internal/store/store.go`**

```go
package store

import (
	"errors"
	"strings"
	"sync"

	"quorum/internal/clock"
	"quorum/internal/model"
)

// ErrVersionConflict is returned by Write when the supplied base version does
// not match the entry's current version (optimistic-concurrency miss).
var ErrVersionConflict = errors.New("version conflict")

// MemStore is the in-memory store: a derived version map plus the append-only
// log that is the source of truth. Single mutex now; leases arrive in Phase 2.
type MemStore struct {
	mu      sync.RWMutex
	entries map[string]model.Entry
	log     *Log
	clk     clock.Clock
}

func NewMemStore(clk clock.Clock) *MemStore {
	return &MemStore{
		entries: make(map[string]model.Entry),
		log:     NewLog(),
		clk:     clk,
	}
}

func (s *MemStore) Read(docID string) model.Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.entries[docID]; ok {
		return e
	}
	return model.Entry{DocID: docID, Version: 0, Exists: false}
}

// Write commits an annotation iff baseVersion matches the current version.
func (s *MemStore) Write(docID, agentID, payload string, baseVersion int) (model.Finding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.entries[docID] // zero Entry => Version 0 when absent
	if cur.Version != baseVersion {
		return model.Finding{}, ErrVersionConflict
	}
	next := cur.Version + 1
	f := s.log.Append(model.Finding{
		DocID:            docID,
		AgentID:          agentID,
		Payload:          payload,
		BaseVersion:      baseVersion,
		CommittedVersion: next,
		Timestamp:        s.clk.Now(),
	})
	s.entries[docID] = model.Entry{DocID: docID, Version: next, Payload: payload, Exists: true}
	return f, nil
}

func (s *MemStore) Findings() []model.Finding { return s.log.Snapshot() }

func (s *MemStore) Lookup(keyword string) []model.Finding {
	kw := strings.ToLower(keyword)
	var out []model.Finding
	for _, f := range s.log.Snapshot() {
		if strings.Contains(strings.ToLower(f.Payload), kw) {
			out = append(out, f)
		}
	}
	return out
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/store/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/store.go internal/store/store_test.go
git commit -m "feat: in-memory versioned store with optimistic-write conflict detection"
```

---

### Task 5: Deterministic annotator

**Files:**
- Create: `internal/annotate/annotate.go`
- Test: `internal/annotate/annotate_test.go`

**Interfaces:**
- Consumes: `model.Doc`.
- Produces: `annotate.Annotate(doc model.Doc, k int) string` — the top-`k` keywords of `doc.Abstract` by frequency, tie-broken alphabetically, space-joined; drops stopwords and tokens shorter than 3 chars; deterministic.

- [ ] **Step 1: Write the failing test**

```go
package annotate

import (
	"testing"

	"quorum/internal/model"
)

func TestAnnotateIsDeterministic(t *testing.T) {
	doc := model.Doc{Abstract: "Agents coordinate. Agents coordinate over shared memory. Memory memory."}
	first := Annotate(doc, 3)
	for i := 0; i < 50; i++ {
		if Annotate(doc, 3) != first {
			t.Fatalf("non-deterministic output on run %d: %q vs %q", i, Annotate(doc, 3), first)
		}
	}
}

func TestAnnotateRanksByFrequencyThenAlphabetical(t *testing.T) {
	doc := model.Doc{Abstract: "memory memory memory agents agents coordinate"}
	// memory:3, agents:2, coordinate:1 -> top-3 in that order
	if got := Annotate(doc, 3); got != "memory agents coordinate" {
		t.Fatalf("got %q", got)
	}
}

func TestAnnotateDropsStopwordsAndShortTokens(t *testing.T) {
	doc := model.Doc{Abstract: "the of a in on to be memory"}
	if got := Annotate(doc, 5); got != "memory" {
		t.Fatalf("got %q, want only 'memory'", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/annotate/`
Expected: FAIL — `Annotate` undefined.

- [ ] **Step 3: Write `internal/annotate/annotate.go`**

```go
package annotate

import (
	"sort"
	"strings"

	"quorum/internal/model"
)

var stopwords = map[string]bool{
	"the": true, "and": true, "for": true, "are": true, "was": true,
	"with": true, "that": true, "this": true, "from": true, "our": true,
	"its": true, "into": true, "over": true, "such": true, "these": true,
}

// Annotate returns a deterministic annotation: the top-k keywords of the
// abstract by frequency, tie-broken alphabetically, space-joined.
func Annotate(doc model.Doc, k int) string {
	counts := map[string]int{}
	for _, tok := range tokenize(doc.Abstract) {
		if len(tok) < 3 || stopwords[tok] {
			continue
		}
		counts[tok]++
	}
	type kv struct {
		word  string
		count int
	}
	kvs := make([]kv, 0, len(counts))
	for w, c := range counts {
		kvs = append(kvs, kv{w, c})
	}
	sort.Slice(kvs, func(i, j int) bool {
		if kvs[i].count != kvs[j].count {
			return kvs[i].count > kvs[j].count
		}
		return kvs[i].word < kvs[j].word
	})
	top := make([]string, 0, k)
	for i := 0; i < len(kvs) && i < k; i++ {
		top = append(top, kvs[i].word)
	}
	return strings.Join(top, " ")
}

func tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	})
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/annotate/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/annotate/
git commit -m "feat: deterministic top-K keyword annotator"
```

---

### Task 6: Corpus loader + test fixture

**Files:**
- Create: `internal/corpus/corpus.go`
- Create: `corpus/fixture.jsonl`
- Test: `internal/corpus/corpus_test.go`

**Interfaces:**
- Produces: `corpus.Load(path string) ([]model.Doc, error)` — parses JSONL, one `model.Doc` per non-empty line.

- [ ] **Step 1: Create the fixture `corpus/fixture.jsonl`**

```
{"id":"doc-1","title":"Coordination","abstract":"Agents coordinate over shared memory to avoid duplicated work.","categories":["cs.DC"]}
{"id":"doc-2","title":"Leases","abstract":"Lease based claiming with a time to live handles dead agents cleanly.","categories":["cs.OS"]}
{"id":"doc-3","title":"Consistency","abstract":"Optimistic concurrency with version checks prevents lost updates under load.","categories":["cs.DC"]}
```

- [ ] **Step 2: Write the failing test**

```go
package corpus

import "testing"

func TestLoadFixture(t *testing.T) {
	docs, err := Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("got %d docs, want 3", len(docs))
	}
	if docs[0].ID != "doc-1" || docs[2].Categories[0] != "cs.DC" {
		t.Fatalf("unexpected parse: %+v", docs)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/corpus/`
Expected: FAIL — `Load` undefined.

- [ ] **Step 4: Write `internal/corpus/corpus.go`**

```go
package corpus

import (
	"bufio"
	"encoding/json"
	"os"

	"quorum/internal/model"
)

// Load parses a JSONL corpus file into Docs. Blank lines are skipped.
func Load(path string) ([]model.Doc, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var docs []model.Doc
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20) // 1 MiB lines
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var d model.Doc
		if err := json.Unmarshal(line, &d); err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	return docs, sc.Err()
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/corpus/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/corpus/ corpus/fixture.jsonl
git commit -m "feat: JSONL corpus loader with test fixture"
```

---

### Task 7: HTTP server (the real boundary)

**Files:**
- Create: `internal/api/server.go`
- Test: `internal/api/server_test.go`

**Interfaces:**
- Consumes: `store.MemStore`, `store.ErrVersionConflict`, `model`.
- Produces: `api.NewServer(s *store.MemStore) *api.Server`; `Server` implements `http.Handler`. Routes:
  - `GET /read?doc=ID` → 200, JSON `model.Entry`.
  - `POST /write` body `{"doc_id","agent_id","payload","base_version"}` → 200 JSON `model.Finding`, or 409 on conflict, 400 on bad JSON.
  - `GET /findings` → 200 JSON `[]model.Finding` (all); `GET /findings?q=kw` → filtered by lookup.

- [ ] **Step 1: Write the failing test**

```go
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/clock"
	"quorum/internal/model"
	"quorum/internal/store"
)

func newTestServer() *httptest.Server {
	s := store.NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	return httptest.NewServer(NewServer(s))
}

func TestWriteThenReadOverHTTP(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{
		"doc_id": "d1", "agent_id": "a", "payload": "alpha", "base_version": 0,
	})
	resp, err := http.Post(ts.URL+"/write", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write status = %d", resp.StatusCode)
	}

	r, _ := http.Get(ts.URL + "/read?doc=d1")
	var e model.Entry
	json.NewDecoder(r.Body).Decode(&e)
	if !e.Exists || e.Version != 1 || e.Payload != "alpha" {
		t.Fatalf("read = %+v", e)
	}
}

func TestWriteConflictReturns409(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	first, _ := json.Marshal(map[string]any{"doc_id": "d1", "agent_id": "a", "payload": "x", "base_version": 0})
	http.Post(ts.URL+"/write", "application/json", bytes.NewReader(first))

	stale, _ := json.Marshal(map[string]any{"doc_id": "d1", "agent_id": "b", "payload": "y", "base_version": 0})
	resp, _ := http.Post(ts.URL+"/write", "application/json", bytes.NewReader(stale))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", resp.StatusCode)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/`
Expected: FAIL — `NewServer` undefined.

- [ ] **Step 3: Write `internal/api/server.go`**

```go
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"quorum/internal/model"
	"quorum/internal/store"
)

type Server struct {
	store *store.MemStore
	mux   *http.ServeMux
}

func NewServer(s *store.MemStore) *Server {
	srv := &Server{store: s, mux: http.NewServeMux()}
	srv.mux.HandleFunc("/read", srv.handleRead)
	srv.mux.HandleFunc("/write", srv.handleWrite)
	srv.mux.HandleFunc("/findings", srv.handleFindings)
	return srv
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.store.Read(r.URL.Query().Get("doc")))
}

type writeRequest struct {
	DocID       string `json:"doc_id"`
	AgentID     string `json:"agent_id"`
	Payload     string `json:"payload"`
	BaseVersion int    `json:"base_version"`
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	var req writeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f, err := s.store.Write(req.DocID, req.AgentID, req.Payload, req.BaseVersion)
	switch {
	case errors.Is(err, store.ErrVersionConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "version conflict"})
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusOK, f)
	}
}

func (s *Server) handleFindings(w http.ResponseWriter, r *http.Request) {
	var res []model.Finding
	if q := r.URL.Query().Get("q"); q != "" {
		res = s.store.Lookup(q)
	} else {
		res = s.store.Findings()
	}
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/api/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/api/server.go internal/api/server_test.go
git commit -m "feat: HTTP/JSON server over the store (read/write/findings)"
```

---

### Task 8: HTTP client

**Files:**
- Create: `internal/api/client.go`
- Test: `internal/api/client_test.go`

**Interfaces:**
- Consumes: the server routes from Task 7, `model`.
- Produces:
  - `api.ErrConflict` (sentinel `error`)
  - `api.NewClient(base string) *api.Client`
  - `(*Client).Read(docID string) (model.Entry, error)`
  - `(*Client).Write(docID, agentID, payload string, baseVersion int) (model.Finding, error)` — returns `ErrConflict` on 409.
  - `(*Client).Findings(query string) ([]model.Finding, error)` — empty `query` lists all.

- [ ] **Step 1: Write the failing test**

```go
package api

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/clock"
	"quorum/internal/store"
)

func TestClientRoundTrip(t *testing.T) {
	s := store.NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	ts := httptest.NewServer(NewServer(s))
	defer ts.Close()
	c := NewClient(ts.URL)

	e, err := c.Read("d1")
	if err != nil || e.Exists {
		t.Fatalf("initial read = %+v, %v", e, err)
	}
	f, err := c.Write("d1", "agent-a", "hello", e.Version)
	if err != nil || f.CommittedVersion != 1 {
		t.Fatalf("write = %+v, %v", f, err)
	}
	all, err := c.Findings("")
	if err != nil || len(all) != 1 {
		t.Fatalf("findings = %+v, %v", all, err)
	}
}

func TestClientWriteConflictReturnsErrConflict(t *testing.T) {
	s := store.NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	ts := httptest.NewServer(NewServer(s))
	defer ts.Close()
	c := NewClient(ts.URL)

	c.Write("d1", "a", "x", 0)
	_, err := c.Write("d1", "b", "y", 0)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/ -run TestClient`
Expected: FAIL — `NewClient` undefined.

- [ ] **Step 3: Write `internal/api/client.go`**

```go
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"quorum/internal/model"
)

// ErrConflict is returned by Write when the server responds 409.
var ErrConflict = errors.New("version conflict")

type Client struct {
	base string
	http *http.Client
}

func NewClient(base string) *Client {
	return &Client{base: base, http: &http.Client{}}
}

func (c *Client) Read(docID string) (model.Entry, error) {
	var e model.Entry
	resp, err := c.http.Get(c.base + "/read?doc=" + url.QueryEscape(docID))
	if err != nil {
		return e, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return e, fmt.Errorf("read: status %d", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&e)
	return e, err
}

func (c *Client) Write(docID, agentID, payload string, baseVersion int) (model.Finding, error) {
	var f model.Finding
	body, _ := json.Marshal(writeRequest{
		DocID: docID, AgentID: agentID, Payload: payload, BaseVersion: baseVersion,
	})
	resp, err := c.http.Post(c.base+"/write", "application/json", bytes.NewReader(body))
	if err != nil {
		return f, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		err = json.NewDecoder(resp.Body).Decode(&f)
		return f, err
	case http.StatusConflict:
		return f, ErrConflict
	default:
		return f, fmt.Errorf("write: status %d", resp.StatusCode)
	}
}

func (c *Client) Findings(query string) ([]model.Finding, error) {
	u := c.base + "/findings"
	if query != "" {
		u += "?q=" + url.QueryEscape(query)
	}
	resp, err := c.http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("findings: status %d", resp.StatusCode)
	}
	var out []model.Finding
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/api/`
Expected: PASS (both server and client tests).

- [ ] **Step 5: Commit**

```bash
git add internal/api/client.go internal/api/client_test.go
git commit -m "feat: HTTP client for the store API"
```

---

### Task 9: Log replay (seed of the invariant checker)

**Files:**
- Create: `internal/replay/replay.go`
- Test: `internal/replay/replay_test.go`

**Interfaces:**
- Consumes: `[]model.Finding`.
- Produces: `replay.Replay(findings []model.Finding) (map[string]int, error)` — folds findings in order; errors if any finding's `BaseVersion` != the doc's running version (gap / lost update, I2/I5). Returns final version per doc.

- [ ] **Step 1: Write the failing test**

```go
package replay

import (
	"testing"

	"quorum/internal/model"
)

func TestReplayValidChain(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", BaseVersion: 1, CommittedVersion: 2},
		{Seq: 3, DocID: "d2", BaseVersion: 0, CommittedVersion: 1},
	}
	versions, err := Replay(fs)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if versions["d1"] != 2 || versions["d2"] != 1 {
		t.Fatalf("versions = %+v", versions)
	}
}

func TestReplayDetectsGap(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", BaseVersion: 0, CommittedVersion: 2}, // base should be 1
	}
	if _, err := Replay(fs); err == nil {
		t.Fatal("expected a gap/lost-update error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/replay/`
Expected: FAIL — `Replay` undefined.

- [ ] **Step 3: Write `internal/replay/replay.go`**

```go
package replay

import (
	"fmt"

	"quorum/internal/model"
)

// Replay folds committed findings into per-doc versions, verifying that each
// finding's BaseVersion matches the running version (I2: no lost updates;
// I5: no gaps). Findings must be supplied in append (Seq) order.
func Replay(findings []model.Finding) (map[string]int, error) {
	versions := map[string]int{}
	for _, f := range findings {
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
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/replay/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/replay/
git commit -m "feat: log replay verifying no-gap version chains (I2/I5 seed)"
```

---

### Task 10: Single-agent control loop + end-to-end slice

**Files:**
- Create: `internal/agent/agent.go`
- Test: `internal/agent/agent_test.go`

**Interfaces:**
- Consumes: `api.Client`, `annotate`, `model`.
- Produces: `agent.RunOnce(c *api.Client, docs []model.Doc, agentID string, k int) (annotated int, err error)` — for each doc: read; if it already exists, skip (dedup guard); else annotate and write. Returns count of docs it annotated.

- [ ] **Step 1: Write the failing end-to-end test**

```go
package agent

import (
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/corpus"
	"quorum/internal/replay"
	"quorum/internal/store"
)

func TestSingleAgentAnnotatesWholeCorpusOnce(t *testing.T) {
	s := store.NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()

	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	c := api.NewClient(ts.URL)

	n, err := RunOnce(c, docs, "agent-0", 3)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if n != len(docs) {
		t.Fatalf("annotated %d, want %d", n, len(docs))
	}

	// Every doc has exactly one finding.
	all, _ := c.Findings("")
	if len(all) != len(docs) {
		t.Fatalf("findings = %d, want %d", len(all), len(docs))
	}

	// Second pass annotates nothing (dedup guard via existence check).
	n2, _ := RunOnce(c, docs, "agent-0", 3)
	if n2 != 0 {
		t.Fatalf("second pass annotated %d, want 0", n2)
	}

	// Log is replayable with no gaps.
	if _, err := replay.Replay(all); err != nil {
		t.Fatalf("replay: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/agent/`
Expected: FAIL — `RunOnce` undefined.

- [ ] **Step 3: Write `internal/agent/agent.go`**

```go
package agent

import (
	"quorum/internal/annotate"
	"quorum/internal/api"
	"quorum/internal/model"
)

// RunOnce walks the corpus once. For each doc it reads current state; if the
// doc already has a committed annotation it is skipped (the Phase 1 dedup
// guard), otherwise the agent annotates and writes it. Returns how many docs
// this agent annotated.
func RunOnce(c *api.Client, docs []model.Doc, agentID string, k int) (int, error) {
	annotated := 0
	for _, d := range docs {
		e, err := c.Read(d.ID)
		if err != nil {
			return annotated, err
		}
		if e.Exists {
			continue
		}
		note := annotate.Annotate(d, k)
		if _, err := c.Write(d.ID, agentID, note, e.Version); err != nil {
			return annotated, err
		}
		annotated++
	}
	return annotated, nil
}
```

- [ ] **Step 4: Run the end-to-end test**

Run: `go test -race ./internal/agent/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/agent/
git commit -m "feat: single-agent control loop with end-to-end corpus slice"
```

---

### Task 11: Runnable binaries (store + agent)

**Files:**
- Create: `cmd/quorum-store/main.go`
- Create: `cmd/quorum-agent/main.go`
- Create: `README.md`

**Interfaces:**
- Consumes: `api`, `store`, `clock`, `corpus`, `agent`.
- Produces: two runnable commands. `quorum-store` serves on `-addr` (default `:8080`). `quorum-agent` loads a corpus and runs one agent once against `-store`.

- [ ] **Step 1: Write `cmd/quorum-store/main.go`**

```go
package main

import (
	"flag"
	"log"
	"net/http"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	s := store.NewMemStore(clock.Real{})
	log.Printf("quorum-store listening on %s", *addr)
	if err := http.ListenAndServe(*addr, api.NewServer(s)); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 2: Write `cmd/quorum-agent/main.go`**

```go
package main

import (
	"flag"
	"log"

	"quorum/internal/agent"
	"quorum/internal/api"
	"quorum/internal/corpus"
)

func main() {
	storeURL := flag.String("store", "http://localhost:8080", "store base URL")
	corpusPath := flag.String("corpus", "corpus/fixture.jsonl", "corpus JSONL path")
	agentID := flag.String("id", "agent-0", "agent id")
	k := flag.Int("k", 5, "keywords per annotation")
	flag.Parse()

	docs, err := corpus.Load(*corpusPath)
	if err != nil {
		log.Fatalf("load corpus: %v", err)
	}
	n, err := agent.RunOnce(api.NewClient(*storeURL), docs, *agentID, *k)
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	log.Printf("agent %s annotated %d/%d docs", *agentID, n, len(docs))
}
```

- [ ] **Step 3: Write a minimal `README.md`**

```markdown
# Quorum

A concurrent shared-memory coordination substrate for multi-agent systems (Go).
Lease-based claiming + CAS-versioned writes over a shared in-memory store.

Phase 1 (single-agent slice) is runnable today:

    go run ./cmd/quorum-store &                 # start the store on :8080
    go run ./cmd/quorum-agent -corpus corpus/fixture.jsonl
    curl -s localhost:8080/findings | head      # see the annotations

Design and benchmark numbers land in later phases. In-memory only; single node.
```

- [ ] **Step 4: Verify the full build and manual run**

Run: `go build ./...`
Expected: builds all binaries, exit 0.

Manual smoke test:
```bash
go run ./cmd/quorum-store -addr :8080 &
sleep 1
go run ./cmd/quorum-agent -store http://localhost:8080 -corpus corpus/fixture.jsonl
curl -s localhost:8080/findings
kill %1
```
Expected: agent logs `annotated 3/3 docs`; `curl` returns a JSON array of 3 findings.

- [ ] **Step 5: Full test sweep**

Run: `go test -race ./...`
Expected: all packages PASS, no race warnings.

- [ ] **Step 6: Commit**

```bash
git add cmd/ README.md
git commit -m "feat: runnable store and agent binaries + README"
```

---

### Task 12: arXiv corpus fetch script (Phase 0 corpus artifact)

**Files:**
- Create: `scripts/fetch_corpus/main.go`
- Produces (at runtime): `corpus/arxiv.jsonl`, `corpus/MANIFEST`

**Interfaces:**
- Standalone command. Flags: `-categories` (comma list, default `cs.DC,cs.OS`), `-max` (default 300), `-out` (default `corpus/arxiv.jsonl`). Emits normalized `model.Doc` JSONL + a `MANIFEST` containing the sha256 of the output.

**Note:** This task is not unit-tested (it hits the network non-deterministically); it is verified by running it and inspecting output. The benchmark path always reads the committed snapshot, never the live API.

- [ ] **Step 1: Write `scripts/fetch_corpus/main.go`**

```go
package main

import (
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"quorum/internal/model"
)

// Minimal Atom parsing for the arXiv API response.
type feed struct {
	Entries []struct {
		ID       string `xml:"id"`
		Title    string `xml:"title"`
		Summary  string `xml:"summary"`
		Category []struct {
			Term string `xml:"term,attr"`
		} `xml:"category"`
	} `xml:"entry"`
}

func main() {
	cats := flag.String("categories", "cs.DC,cs.OS", "comma-separated arXiv categories")
	max := flag.Int("max", 300, "number of abstracts to fetch")
	out := flag.String("out", "corpus/arxiv.jsonl", "output JSONL path")
	flag.Parse()

	query := "cat:" + strings.Join(strings.Split(*cats, ","), "+OR+cat:")
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	const page = 100
	hasher := sha256.New()
	w := io.MultiWriter(f, hasher)
	written := 0

	for start := 0; start < *max; start += page {
		n := page
		if start+n > *max {
			n = *max - start
		}
		u := fmt.Sprintf(
			"http://export.arxiv.org/api/query?search_query=%s&start=%d&max_results=%d&sortBy=submittedDate&sortOrder=descending",
			url.QueryEscape(query), start, n,
		)
		resp, err := http.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var fd feed
		if err := xml.Unmarshal(body, &fd); err != nil {
			log.Fatalf("parse: %v", err)
		}
		for _, e := range fd.Entries {
			doc := model.Doc{
				ID:       strings.TrimSpace(e.ID),
				Title:    strings.Join(strings.Fields(e.Title), " "),
				Abstract: strings.Join(strings.Fields(e.Summary), " "),
			}
			for _, c := range e.Category {
				doc.Categories = append(doc.Categories, c.Term)
			}
			line, _ := json.Marshal(doc)
			w.Write(append(line, '\n'))
			written++
		}
		time.Sleep(3 * time.Second) // arXiv rate-limit etiquette
	}

	manifest := fmt.Sprintf("file: %s\ndocs: %d\nsha256: %x\n", *out, written, hasher.Sum(nil))
	if err := os.WriteFile("corpus/MANIFEST", []byte(manifest), 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %d docs to %s", written, *out)
}
```

- [ ] **Step 2: Build it**

Run: `go build ./scripts/fetch_corpus/`
Expected: builds, exit 0.

- [ ] **Step 3: Run it (network) and sanity-check**

Run: `go run ./scripts/fetch_corpus -max 300`
Expected: logs `wrote ~300 docs`; `corpus/arxiv.jsonl` has ~300 lines; `corpus/MANIFEST` shows a sha256. Spot-check one line parses as a Doc with a non-empty abstract.

> ⚠️ If the arXiv API is flaky or returns fewer entries, re-run; the committed snapshot is what matters, not a perfect fetch. Freeze whatever clean snapshot you get.

- [ ] **Step 4: Commit the snapshot + script**

```bash
git add scripts/fetch_corpus/main.go corpus/arxiv.jsonl corpus/MANIFEST
git commit -m "feat: arXiv corpus fetch script + frozen snapshot"
```

---

## Self-Review

**Spec coverage (Phase 0 + Phase 1 of the PRD):**
- Phase 0 data model → Task 1 (`model`). ✅
- Phase 0 API contract → Task 7 routes + Task 8 client. ✅
- Phase 0 invariants written before code → Task 1 `INVARIANTS.md` (before Task 4 store). ✅
- Phase 0 corpus prepared + deterministically chunked → Task 6 fixture + Task 12 arXiv snapshot (chunk rule: abstract = one document/chunk). ✅
- Phase 0 duplication metric frozen (doc-level) → recorded in `INVARIANTS.md` I1. ✅
- Phase 1 store (no locking) + findings log + versioned entries → Tasks 3–4. ✅
- Phase 1 one agent + deterministic annotation → Tasks 5, 10. ✅
- Phase 1 basic keyword lookup → Task 4 `Lookup` + Task 7 `/findings?q=`. ✅
- Phase 1 log complete + replayable → Task 9 + Task 10 assertion. ✅
- Real HTTP boundary from day one → Tasks 7–8, exercised in Task 10 via `httptest`. ✅
- Injected clock → Task 2, used everywhere time is needed. ✅

**Deferred to later plans (correctly out of scope here):** lease-based claiming + TTL + renewal, CAS-retry policy, N concurrent goroutine agents, baseline/coordination-disabled mode, duplication benchmark harness, invariant checker + failure injection, latency/throughput + pprof, writeup. These are Plans 2–6.

**Placeholder scan:** no TBD/TODO; every code step contains full code. ✅
**Type consistency:** `model.Entry`/`model.Finding`/`model.Doc` field names identical across store, api, agent, replay, fetch. `ErrVersionConflict` (store) vs `ErrConflict` (client) are intentionally distinct sentinels on their respective sides of the HTTP boundary. `RunOnce` signature matches between Task 10 def and Task 11 caller. ✅

**⚠️ Known watch-fors for the implementer:**
- The `-race` flag is on `go test` here for hygiene; it belongs on all dev/test runs but must be OFF for the Phase 5 latency benchmarks (separate build). Not relevant to this plan's correctness, noted for continuity.
- `internal/store/store.go` takes `store.mu` then calls `log.Append` which takes `log.mu`. Lock ordering is store→log, and log never calls back into store — documented in `log.go`. Preserve this when Phase 2 adds the claim registry (add its lock in a documented order too).
- Corpus test paths are relative (`../../corpus/fixture.jsonl`) because Go runs tests from the package dir. Keep the fixture at repo-root `corpus/`.

---

## Plan sequence (the remaining phases each get their own just-in-time plan)

- **Plan 2 — Phase 2 (concurrency core):** add claim registry + lease TTL (via `Clock`) + renewal heartbeat behind the existing store interface; CAS-retry policy in the client; N goroutine agents in a driver. Written next.
- **Plan 3 — Phase 3 (baseline + duplication benchmark):** coordination-disabled mode (same code path, mechanisms toggled) + doc-level duplication metric + N=2/4/8/16 harness, ≥5 runs.
- **Plan 4 — Phase 4 (invariant checker + failure injection):** full I1–I5 checker over the log + injected dead-agent/forced-conflict runs; verify each injection actually triggered its condition.
- **Plan 5 — Phase 5 (latency/throughput + profiling):** measurement harness (race OFF) + pprof + the one documented optimization. Steps for the *optimization itself* can only be written after profiling reveals the bottleneck — this plan is authored against real profile data, not guessed.
- **Plan 6 — Phase 6 (writeup + packaging):** README, benchmark report, technical writeup, resume bullets, interview story.
