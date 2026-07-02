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
	_, err = c.Claim("d1", "agent-a", 60000)
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
	c.Claim("d1", "a", 60000)
	c.Write("d1", "a", "x", 0)

	// Agent b tries to write without a lease - should get ErrConflict (maps ErrNoLease)
	_, err := c.Write("d1", "b", "y", 0)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}
