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
