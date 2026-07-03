package checker

import (
	"testing"
	"time"

	"quorum/internal/model"
)

func goodCoordinatedFindings() []model.Finding {
	return []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(100, 0)},
		{Seq: 2, DocID: "d2", AgentID: "b", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(101, 0)},
	}
}

func TestCheckPassesOnCleanCoordinatedLog(t *testing.T) {
	r := Check(goodCoordinatedFindings(), nil, true)
	if !r.OK() {
		t.Fatalf("expected OK, got violations: %+v", r.Violations)
	}
}

func TestCheckI1FlagsDuplicateInCoordinatedMode(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", AgentID: "b", BaseVersion: 1, CommittedVersion: 2}, // d1 twice
	}
	r := Check(fs, nil, true)
	if r.OK() || !hasInvariant(r, "I1") {
		t.Fatalf("expected I1 violation, got %+v", r.Violations)
	}
}

func TestCheckI1AllowsDuplicatesInBaselineMode(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", AgentID: "b", BaseVersion: 1, CommittedVersion: 2},
	}
	r := Check(fs, nil, false) // baseline: duplicates are expected, not an I1 violation
	if !r.OK() {
		t.Fatalf("baseline duplicates should not violate I1: %+v", r.Violations)
	}
}

func TestCheckI2FlagsBrokenVersionChain(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 2, DocID: "d1", BaseVersion: 0, CommittedVersion: 2}, // base should be 1
	}
	r := Check(fs, nil, false)
	if r.OK() || !hasInvariant(r, "I2") {
		t.Fatalf("expected I2 violation, got %+v", r.Violations)
	}
}

func TestCheckI5FlagsSeqGap(t *testing.T) {
	fs := []model.Finding{
		{Seq: 1, DocID: "d1", BaseVersion: 0, CommittedVersion: 1},
		{Seq: 3, DocID: "d2", BaseVersion: 0, CommittedVersion: 1}, // gap: 2 missing
	}
	r := Check(fs, nil, false)
	if r.OK() || !hasInvariant(r, "I5") {
		t.Fatalf("expected I5 violation, got %+v", r.Violations)
	}
}

func hasInvariant(r Report, inv string) bool {
	for _, v := range r.Violations {
		if v.Invariant == inv {
			return true
		}
	}
	return false
}
