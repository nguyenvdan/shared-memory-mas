package agent

import (
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/corpus"
	"quorum/internal/replay"
	"quorum/internal/retry"
	"quorum/internal/store"
)

func TestSingleAgentAnnotatesWholeCorpusOnce(t *testing.T) {
	s := store.NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()

	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	c := api.NewClient(ts.URL)

	n, err := RunOnce(c, docs, "agent-0", 3)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if n != len(docs) {
		t.Fatalf("annotated %d, want %d", n, len(docs))
	}

	// Every doc has exactly one finding.
	all, _ := c.Findings("")
	if len(all) != len(docs) {
		t.Fatalf("findings = %d, want %d", len(all), len(docs))
	}

	// Second pass annotates nothing (dedup guard via existence check).
	n2, _ := RunOnce(c, docs, "agent-0", 3)
	if n2 != 0 {
		t.Fatalf("second pass annotated %d, want 0", n2)
	}

	// Log is replayable with no gaps.
	if _, err := replay.Replay(all); err != nil {
		t.Fatalf("replay: %v", err)
	}
}

func TestRunCoordinatedAnnotatesCorpusOnce(t *testing.T) {
	s := store.NewMemStore(clock.Real{})
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()
	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	c := api.NewClient(ts.URL)

	st, err := RunCoordinated(c, docs, "agent-0", 3, time.Minute, retry.Default())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if st.Annotated != len(docs) {
		t.Fatalf("annotated %d, want %d", st.Annotated, len(docs))
	}
	all, _ := c.Findings("")
	if len(all) != len(docs) {
		t.Fatalf("findings %d, want %d", len(all), len(docs))
	}
	// Second pass: everything already annotated -> all skipped, zero new.
	st2, _ := RunCoordinated(c, docs, "agent-0", 3, time.Minute, retry.Default())
	if st2.Annotated != 0 || st2.Skipped != len(docs) {
		t.Fatalf("second pass stats = %+v", st2)
	}
}
