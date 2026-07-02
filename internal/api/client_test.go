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

	// Claim a lease first
	_, err = c.Claim("d1", "agent-a", time.Minute)
	if err != nil {
		t.Fatalf("claim = %v", err)
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

	// Claim lease for agent a
	c.Claim("d1", "a", time.Minute)
	c.Write("d1", "a", "x", 0)

	// Agent b tries to write without a lease - should get ErrConflict (maps ErrNoLease)
	_, err := c.Write("d1", "b", "y", 0)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}

func TestClientClaimAndRelease(t *testing.T) {
	s := store.NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	ts := httptest.NewServer(NewServer(s))
	defer ts.Close()
	c := NewClient(ts.URL)
	if _, err := c.Claim("d1", "agent-a", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := c.Claim("d1", "agent-b", time.Minute); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("claim b err = %v, want ErrLeaseHeld", err)
	}
	if err := c.Release("d1", "agent-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestClientRenewByNonHolder(t *testing.T) {
	s := store.NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	ts := httptest.NewServer(NewServer(s))
	defer ts.Close()
	c := NewClient(ts.URL)
	c.Claim("d1", "agent-a", time.Minute)
	if _, err := c.Renew("d1", "agent-b", time.Minute); !errors.Is(err, ErrNotHolder) {
		t.Fatalf("renew err = %v, want ErrNotHolder", err)
	}
}
