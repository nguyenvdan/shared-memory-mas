package bench

import (
	"testing"

	"quorum/internal/model"
)

func TestDuplicationRate(t *testing.T) {
	// 3 findings, 2 unique docs (d1 annotated twice) -> 1 dupe, rate 1/3.
	fs := []model.Finding{
		{DocID: "d1"}, {DocID: "d2"}, {DocID: "d1"},
	}
	rate, total, unique, dupes := DuplicationRate(fs)
	if total != 3 || unique != 2 || dupes != 1 {
		t.Fatalf("total=%d unique=%d dupes=%d", total, unique, dupes)
	}
	if rate < 0.333 || rate > 0.334 {
		t.Fatalf("rate = %v, want ~0.333", rate)
	}
}

func TestDuplicationRateEmpty(t *testing.T) {
	rate, total, _, _ := DuplicationRate(nil)
	if total != 0 || rate != 0 {
		t.Fatalf("empty: rate=%v total=%d", rate, total)
	}
}

func TestDuplicationRateNoDupes(t *testing.T) {
	fs := []model.Finding{{DocID: "d1"}, {DocID: "d2"}}
	rate, _, unique, dupes := DuplicationRate(fs)
	if unique != 2 || dupes != 0 || rate != 0 {
		t.Fatalf("no-dupes: unique=%d dupes=%d rate=%v", unique, dupes, rate)
	}
}
