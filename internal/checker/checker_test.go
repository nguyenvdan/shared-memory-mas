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

// goodCoordinatedLease covers each write in goodCoordinatedFindings() with a
// live lease held by its author, so the fixture is legal once I3's
// write-coverage check (added in Task 4) is in play.
func goodCoordinatedLease() []model.LeaseEvent {
	return []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(90, 0)},
		{Seq: 2, Kind: "claim", DocID: "d2", AgentID: "b", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(91, 0)},
	}
}

func TestCheckPassesOnCleanCoordinatedLog(t *testing.T) {
	r := Check(goodCoordinatedFindings(), goodCoordinatedLease(), true)
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

func TestCheckLeasePassesOnCleanCoordinatedRun(t *testing.T) {
	// agent-a claims d1 at t=100 (expiry 160), writes at t=105, releases at t=110.
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
		{Seq: 2, Kind: "release", DocID: "d1", AgentID: "a", Timestamp: time.Unix(110, 0)},
	}
	findings := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(105, 0)},
	}
	r := Check(findings, lease, true)
	if !r.OK() {
		t.Fatalf("expected OK, got %+v", r.Violations)
	}
}

func TestCheckI3FlagsOverlappingLeases(t *testing.T) {
	// agent-b claims d1 at t=120 while agent-a's lease (expiry 160) is still live.
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
		{Seq: 2, Kind: "claim", DocID: "d1", AgentID: "b", LeaseExpiry: time.Unix(180, 0), Timestamp: time.Unix(120, 0)},
	}
	r := Check(nil, lease, true)
	if r.OK() || !hasInvariant(r, "I3") {
		t.Fatalf("expected I3 violation, got %+v", r.Violations)
	}
}

func TestCheckI4RecoveryAfterExpiryIsLegal(t *testing.T) {
	// agent-a claims (expiry 160), dies. agent-b claims at t=170 (after expiry) — legal.
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
		{Seq: 2, Kind: "claim", DocID: "d1", AgentID: "b", LeaseExpiry: time.Unix(230, 0), Timestamp: time.Unix(170, 0)},
	}
	findings := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "b", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(175, 0)},
	}
	r := Check(findings, lease, true)
	if !r.OK() {
		t.Fatalf("post-expiry reclaim should be legal, got %+v", r.Violations)
	}
}

func TestCheckI3FlagsWriteWithoutLease(t *testing.T) {
	// A write at t=200 but the only lease expired at t=160 — no live lease covers it.
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
	}
	findings := []model.Finding{
		{Seq: 1, DocID: "d1", AgentID: "a", BaseVersion: 0, CommittedVersion: 1, Timestamp: time.Unix(200, 0)},
	}
	r := Check(findings, lease, true)
	if r.OK() || !hasInvariant(r, "I3") {
		t.Fatalf("expected I3 (write without live lease), got %+v", r.Violations)
	}
}

func TestCheckLeaseI5FlagsSeqGap(t *testing.T) {
	lease := []model.LeaseEvent{
		{Seq: 1, Kind: "claim", DocID: "d1", AgentID: "a", LeaseExpiry: time.Unix(160, 0), Timestamp: time.Unix(100, 0)},
		{Seq: 3, Kind: "release", DocID: "d1", AgentID: "a", Timestamp: time.Unix(110, 0)}, // gap
	}
	r := Check(nil, lease, true)
	if r.OK() || !hasInvariant(r, "I5") {
		t.Fatalf("expected lease I5 violation, got %+v", r.Violations)
	}
}
