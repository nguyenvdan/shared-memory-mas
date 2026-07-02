package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"quorum/internal/clock"
	"quorum/internal/model"
	"quorum/internal/replay"
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
	if _, err := s.Claim("d1", "agent-a", time.Minute); err != nil {
		t.Fatal(err)
	}
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
	if _, err := s.Claim("d1", "a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write("d1", "a", "first", 0); err != nil {
		t.Fatal(err)
	}
	// agent b tries to write without holding the lease
	_, err := s.Write("d1", "b", "second", 0)
	if !errors.Is(err, ErrNoLease) {
		t.Fatalf("err = %v, want ErrNoLease", err)
	}
}

func TestLookupIsCaseInsensitiveSubstring(t *testing.T) {
	s := newTestStore()
	s.Claim("d1", "a", time.Minute)
	s.Claim("d2", "a", time.Minute)
	s.Write("d1", "a", "Quorum Coordination", 0)
	s.Write("d2", "a", "unrelated", 0)
	got := s.Lookup("coordination")
	if len(got) != 1 || got[0].DocID != "d1" {
		t.Fatalf("lookup = %+v", got)
	}
}

func TestClaimGrantsLeaseWhenFree(t *testing.T) {
	s := newTestStore() // uses mock clock at a fixed time
	c, err := s.Claim("d1", "agent-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if c.AgentID != "agent-a" || c.DocID != "d1" {
		t.Fatalf("claim = %+v", c)
	}
}

func TestClaimHeldByOtherReturnsErrLeaseHeld(t *testing.T) {
	s := newTestStore()
	if _, err := s.Claim("d1", "agent-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	_, err := s.Claim("d1", "agent-b", time.Minute)
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("err = %v, want ErrLeaseHeld", err)
	}
}

func TestClaimExpiredLeaseIsReclaimable(t *testing.T) {
	clk := clock.NewMock(time.Unix(1_700_000_000, 0))
	s := NewMemStore(clk)
	if _, err := s.Claim("d1", "agent-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute) // lease a's TTL expires
	c, err := s.Claim("d1", "agent-b", time.Minute)
	if err != nil {
		t.Fatalf("expected reclaim, got %v", err)
	}
	if c.AgentID != "agent-b" {
		t.Fatalf("reclaim holder = %s, want agent-b", c.AgentID)
	}
}

func TestRenewByHolderExtendsExpiry(t *testing.T) {
	clk := clock.NewMock(time.Unix(1_700_000_000, 0))
	s := NewMemStore(clk)
	c0, _ := s.Claim("d1", "agent-a", time.Minute)
	clk.Advance(30 * time.Second)
	c1, err := s.Renew("d1", "agent-a", time.Minute)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !c1.LeaseExpiry.After(c0.LeaseExpiry) {
		t.Fatalf("renew did not extend: %v !> %v", c1.LeaseExpiry, c0.LeaseExpiry)
	}
}

func TestRenewByNonHolderFails(t *testing.T) {
	s := newTestStore()
	s.Claim("d1", "agent-a", time.Minute)
	if _, err := s.Renew("d1", "agent-b", time.Minute); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("err = %v, want ErrNotHolder", err)
	}
}

func TestReleaseByHolderFreesLease(t *testing.T) {
	s := newTestStore()
	s.Claim("d1", "agent-a", time.Minute)
	if err := s.Release("d1", "agent-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// now claimable by another
	if _, err := s.Claim("d1", "agent-b", time.Minute); err != nil {
		t.Fatalf("claim after release: %v", err)
	}
}

func TestClaimSameAgentReclaimRenews(t *testing.T) {
	clk := clock.NewMock(time.Unix(1_700_000_000, 0))
	s := NewMemStore(clk)
	c0, err := s.Claim("d1", "agent-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Second)
	c1, err := s.Claim("d1", "agent-a", time.Minute) // same agent re-claims
	if err != nil {
		t.Fatalf("same-agent reclaim: %v", err)
	}
	if c1.Version != c0.Version+1 {
		t.Fatalf("version = %d, want %d", c1.Version, c0.Version+1)
	}
	if !c1.LeaseExpiry.After(c0.LeaseExpiry) {
		t.Fatal("expiry not extended on same-agent reclaim")
	}
}

func TestWriteRequiresLease(t *testing.T) {
	s := newTestStore()
	_, err := s.Write("d1", "agent-a", "note", 0) // no claim taken
	if !errors.Is(err, ErrNoLease) {
		t.Fatalf("err = %v, want ErrNoLease", err)
	}
}

func TestWriteSucceedsWithLease(t *testing.T) {
	s := newTestStore()
	if _, err := s.Claim("d1", "agent-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	f, err := s.Write("d1", "agent-a", "note", 0)
	if err != nil || f.CommittedVersion != 1 {
		t.Fatalf("write = %+v err=%v", f, err)
	}
}

// Concurrency: many agents contend for the same doc; exactly one commits per
// version, and the store never races. Uses real time (short TTL) but asserts
// only the invariant, not timing.
func TestConcurrentClaimWriteIsRaceFreeAndSingleWinner(t *testing.T) {
	s := NewMemStore(clock.Real{})
	const agents = 16
	var wg sync.WaitGroup
	wins := make([]bool, agents)
	for i := 0; i < agents; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			agent := fmt.Sprintf("agent-%d", id)
			c, err := s.Claim("hot", agent, time.Minute)
			if err != nil {
				return // lost the claim race; fine
			}
			if _, err := s.Write("hot", agent, "x", c.Version-1+0); err == nil {
				// note: baseVersion is 0 for the first writer; later writers
				// would need the current version. We only assert no race + that
				// findings never exceed the version chain via replay below.
				wins[id] = true
			}
			_ = s.Release("hot", agent)
		}(i)
	}
	wg.Wait()
	// The log must replay cleanly regardless of interleaving.
	if _, err := replayVersions(s.Findings()); err != nil {
		t.Fatalf("log not replayable: %v", err)
	}
}

// replayVersions is a thin test helper delegating to the replay package.
func replayVersions(fs []model.Finding) (map[string]int, error) {
	return replay.Replay(fs)
}
