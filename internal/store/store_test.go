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
