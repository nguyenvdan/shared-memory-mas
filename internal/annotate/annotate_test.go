package annotate

import (
	"testing"

	"quorum/internal/model"
)

func TestAnnotateIsDeterministic(t *testing.T) {
	doc := model.Doc{Abstract: "Agents coordinate. Agents coordinate over shared memory. Memory memory."}
	first := Annotate(doc, 3)
	for i := 0; i < 50; i++ {
		if Annotate(doc, 3) != first {
			t.Fatalf("non-deterministic output on run %d: %q vs %q", i, Annotate(doc, 3), first)
		}
	}
}

func TestAnnotateRanksByFrequencyThenAlphabetical(t *testing.T) {
	doc := model.Doc{Abstract: "memory memory memory agents agents coordinate"}
	// memory:3, agents:2, coordinate:1 -> top-3 in that order
	if got := Annotate(doc, 3); got != "memory agents coordinate" {
		t.Fatalf("got %q", got)
	}
}

func TestAnnotateDropsStopwordsAndShortTokens(t *testing.T) {
	doc := model.Doc{Abstract: "the of a in on to be memory"}
	if got := Annotate(doc, 5); got != "memory" {
		t.Fatalf("got %q, want only 'memory'", got)
	}
}
