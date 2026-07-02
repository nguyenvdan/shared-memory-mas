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
