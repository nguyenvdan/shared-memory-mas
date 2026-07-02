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
