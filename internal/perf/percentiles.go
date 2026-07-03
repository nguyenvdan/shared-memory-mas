// Package perf measures Quorum's per-operation latency and throughput.
package perf

import (
	"sort"
	"time"
)

// OpStats summarizes a set of operation latencies.
type OpStats struct {
	Count         int
	P50, P95, P99 time.Duration
}

// Percentiles returns the p50/p95/p99 latencies via nearest-rank over a sorted
// copy of ds (the input is not mutated). Empty input yields zeros.
func Percentiles(ds []time.Duration) (p50, p95, p99 time.Duration) {
	if len(ds) == 0 {
		return 0, 0, 0
	}
	s := make([]time.Duration, len(ds))
	copy(s, ds)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[rank(len(s), 50)], s[rank(len(s), 95)], s[rank(len(s), 99)]
}

// rank is the 0-based nearest-rank index for percentile p over n samples.
func rank(n, p int) int {
	i := p * n / 100
	if i >= n {
		i = n - 1
	}
	return i
}

// Summarize computes OpStats over ds.
func Summarize(ds []time.Duration) OpStats {
	p50, p95, p99 := Percentiles(ds)
	return OpStats{Count: len(ds), P50: p50, P95: p95, P99: p99}
}
