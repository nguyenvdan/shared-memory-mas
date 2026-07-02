package replay

import (
	"testing"

	"quorum/internal/model"
)

func TestReplayValidChain(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", BaseVersion: 1, CommittedVersion: 2},
		{Seq: 3, DocID: "d2", BaseVersion: 0, CommittedVersion: 1},
	}
	versions, err := Replay(fs)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if versions["d1"] != 2 || versions["d2"] != 1 {
		t.Fatalf("versions = %+v", versions)
	}
}

func TestReplayDetectsGap(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", BaseVersion: 0, CommittedVersion: 2}, // base should be 1
	}
	if _, err := Replay(fs); err == nil {
		t.Fatal("expected a gap/lost-update error")
	}
}
