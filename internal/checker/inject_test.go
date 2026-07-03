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
	"quorum/internal/model"
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
