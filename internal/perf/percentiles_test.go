package perf

import (
	"testing"
	"time"
)

func TestPercentilesNearestRank(t *testing.T) {
	// 1..100 ms. p50≈50th, p95≈95th, p99≈99th (nearest-rank, 0-based index p*n/100).
	ds := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		ds[i] = time.Duration(i+1) * time.Millisecond
	}
	p50, p95, p99 := Percentiles(ds)
	if p50 != 51*time.Millisecond { // index 50 -> value 51ms
		t.Fatalf("p50 = %v, want 51ms", p50)
	}
	if p95 != 96*time.Millisecond {
		t.Fatalf("p95 = %v, want 96ms", p95)
	}
	if p99 != 100*time.Millisecond {
		t.Fatalf("p99 = %v, want 100ms", p99)
	}
}

func TestPercentilesEmpty(t *testing.T) {
	p50, p95, p99 := Percentiles(nil)
	if p50 != 0 || p95 != 0 || p99 != 0 {
		t.Fatalf("empty percentiles = %v/%v/%v, want 0", p50, p95, p99)
	}
}

func TestPercentilesDoesNotMutateInput(t *testing.T) {
	ds := []time.Duration{3, 1, 2}
	Percentiles(ds)
	if ds[0] != 3 || ds[1] != 1 || ds[2] != 2 {
		t.Fatalf("input was mutated: %v", ds)
	}
}

func TestSummarize(t *testing.T) {
	ds := []time.Duration{time.Millisecond, 2 * time.Millisecond}
	s := Summarize(ds)
	if s.Count != 2 {
		t.Fatalf("count = %d, want 2", s.Count)
	}
}
