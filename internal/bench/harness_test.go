package bench

import (
	"testing"
	"time"

	"quorum/internal/corpus"
	"quorum/internal/retry"
)

func TestRunScenarioCoordinatedHasNoDuplication(t *testing.T) {
	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	res, err := RunScenario(true, 4, docs, 3, time.Minute, retry.Default())
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	if res.Mode != "coordinated" || res.Agents != 4 {
		t.Fatalf("result meta = %+v", res)
	}
	// Coordinated: each doc annotated exactly once.
	if res.TotalFindings != len(docs) || res.UniqueDocs != len(docs) {
		t.Fatalf("coordinated findings=%d unique=%d want %d/%d", res.TotalFindings, res.UniqueDocs, len(docs), len(docs))
	}
	if res.DuplicationRate != 0 {
		t.Fatalf("coordinated dup rate = %v, want 0", res.DuplicationRate)
	}
}

func TestRunScenarioBaselineHasDuplication(t *testing.T) {
	docs, err := corpus.Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	res, err := RunScenario(false, 4, docs, 3, time.Minute, retry.Policy{MaxAttempts: 128, BaseDelay: 0})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}
	if res.Mode != "baseline" {
		t.Fatalf("mode = %s", res.Mode)
	}
	// Baseline: 4 agents each annotate every doc -> heavy duplication.
	if res.TotalFindings != 4*len(docs) {
		t.Fatalf("baseline findings = %d, want %d", res.TotalFindings, 4*len(docs))
	}
	if res.DuplicationRate <= 0.7 { // (4-1)/4 = 0.75
		t.Fatalf("baseline dup rate = %v, want ~0.75", res.DuplicationRate)
	}
}
