package agent

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/corpus"
	"quorum/internal/replay"
	"quorum/internal/retry"
	"quorum/internal/store"
)

func TestBaselineDuplicatesAcrossAgents(t *testing.T) {
	// Uncoordinated store: no lease guard, CAS on.
	s := store.NewMemStore(clock.Real{}, store.Uncoordinated())
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()
	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	const n = 4
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("agent-%d", i)
	}
	// Generous retry so every duplicate annotation commits under contention.
	p := retry.Policy{MaxAttempts: 128, BaseDelay: 0}

	stats, err := RunBaselineConcurrent(ts.URL, docs, ids, 3, p)
	if err != nil {
		t.Fatalf("baseline run: %v", err)
	}

	c := api.NewClient(ts.URL)
	all, _ := c.Findings("")
	// Each of n agents annotates every doc -> n*len(docs) committed findings.
	if len(all) != n*len(docs) {
		t.Fatalf("findings = %d, want %d (n*docs, duplicated work)", len(all), n*len(docs))
	}
	// Sum of Annotated across agents matches.
	total := 0
	for _, st := range stats {
		total += st.Annotated
	}
	if total != n*len(docs) {
		t.Fatalf("sum annotated = %d, want %d", total, n*len(docs))
	}
	// Despite duplication, the log still replays with no version gaps (CAS held).
	if _, err := replay.Replay(all); err != nil {
		t.Fatalf("replay: %v", err)
	}
}
