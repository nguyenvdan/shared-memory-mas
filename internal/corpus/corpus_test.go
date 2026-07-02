package corpus

import "testing"

func TestLoadFixture(t *testing.T) {
	docs, err := Load("../../corpus/fixture.jsonl")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("got %d docs, want 3", len(docs))
	}
	if docs[0].ID != "doc-1" || docs[2].Categories[0] != "cs.DC" {
		t.Fatalf("unexpected parse: %+v", docs)
	}
}
