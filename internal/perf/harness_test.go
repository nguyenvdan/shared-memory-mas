package perf

import (
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/store"
)

func TestRunPerfProducesStats(t *testing.T) {
	s := store.NewMemStore(clock.Real{})
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()

	const agents, docsPerAgent, passes = 4, 10, 2
	res, err := RunPerf(ts.URL, agents, docsPerAgent, passes, 5, time.Minute)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// Each agent does docsPerAgent*passes cycles of claim+write+release.
	wantCycles := agents * docsPerAgent * passes
	if res.Claim.Count != wantCycles || res.Write.Count != wantCycles || res.Release.Count != wantCycles {
		t.Fatalf("op counts = claim %d write %d release %d, want %d each",
			res.Claim.Count, res.Write.Count, res.Release.Count, wantCycles)
	}
	if res.TotalOps != wantCycles*3 {
		t.Fatalf("total ops = %d, want %d", res.TotalOps, wantCycles*3)
	}
	if res.OpsPerSec <= 0 || res.Write.P50 <= 0 {
		t.Fatalf("expected positive throughput and latency, got ops/sec=%v p50=%v", res.OpsPerSec, res.Write.P50)
	}
	if res.Agents != agents {
		t.Fatalf("agents = %d, want %d", res.Agents, agents)
	}
}
