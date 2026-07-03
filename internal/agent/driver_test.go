package agent

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/api"
	"quorum/internal/checker"
	"quorum/internal/clock"
	"quorum/internal/corpus"
	"quorum/internal/replay"
	"quorum/internal/retry"
	"quorum/internal/store"
)

func TestRunConcurrentNoDuplicateAnnotations(t *testing.T) {
	for _, n := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("agents=%d", n), func(t *testing.T) {
			s := store.NewMemStore(clock.Real{})
			ts := httptest.NewServer(api.NewServer(s))
			defer ts.Close()
			docs, err := corpus.Load("../../corpus/fixture.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, n)
			for i := range ids {
				ids[i] = fmt.Sprintf("agent-%d", i)
			}

			stats, err := RunConcurrent(ts.URL, docs, ids, 3, time.Minute, retry.Default())
			if err != nil {
				t.Fatalf("run: %v", err)
			}

			// I1: every doc annotated exactly once across all agents.
			c := api.NewClient(ts.URL)
			all, _ := c.Findings("")
			if len(all) != len(docs) {
				t.Fatalf("findings = %d, want %d (one per doc)", len(all), len(docs))
			}
			// Sum of annotated across agents equals corpus size.
			total := 0
			for _, st := range stats {
				total += st.Annotated
			}
			if total != len(docs) {
				t.Fatalf("sum annotated = %d, want %d", total, len(docs))
			}
			// Log replays with no gaps.
			if _, err := replay.Replay(all); err != nil {
				t.Fatalf("replay: %v", err)
			}

			// The invariant checker must independently confirm the run is clean.
			rep := checker.Check(s.Findings(), s.LeaseEvents(), true)
			if !rep.OK() {
				t.Fatalf("checker flagged violations: %+v", rep.Violations)
			}
		})
	}
}
