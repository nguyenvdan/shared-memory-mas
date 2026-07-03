// Package checker replays Quorum's append-only logs and verifies invariants
// I1–I5. It is a pure function over the logs (no store, no clock, no HTTP), so
// consistency is checked, not asserted.
package checker

import (
	"fmt"

	"quorum/internal/model"
	"quorum/internal/replay"
)

type Violation struct {
	Invariant string // "I1".."I5"
	Detail    string
}

type Report struct {
	Violations []Violation
}

func (r Report) OK() bool { return len(r.Violations) == 0 }

func (r *Report) flag(inv, format string, args ...any) {
	r.Violations = append(r.Violations, Violation{Invariant: inv, Detail: fmt.Sprintf(format, args...)})
}

// Check verifies I1–I5 over the findings and lease-event logs. I1 and I3 apply
// only in coordinated mode; I2 and I5 apply always. coordinated indicates
// whether the run used lease-based coordination.
func Check(findings []model.Finding, lease []model.LeaseEvent, coordinated bool) Report {
	var r Report
	checkFindingsSeq(&r, findings)     // I5 (findings)
	checkVersionChains(&r, findings)   // I2
	if coordinated {
		checkNoDuplicates(&r, findings) // I1
	}
	checkLeaseInvariants(&r, findings, lease, coordinated) // I3/I4/I5(lease) — Task 4
	return r
}

// checkFindingsSeq verifies I5: strictly increasing Seq from 1, no gaps.
func checkFindingsSeq(r *Report, findings []model.Finding) {
	want := int64(1)
	for _, f := range findings {
		if f.Seq != want {
			r.flag("I5", "findings seq = %d, want %d (gap or out-of-order)", f.Seq, want)
			return
		}
		want++
	}
}

// checkVersionChains verifies I2 via replay (base==running, committed==running+1).
func checkVersionChains(r *Report, findings []model.Finding) {
	if _, err := replay.Replay(findings); err != nil {
		r.flag("I2", "version chain broken: %v", err)
	}
}

// checkNoDuplicates verifies I1: each doc committed at most once (coordinated).
func checkNoDuplicates(r *Report, findings []model.Finding) {
	seen := make(map[string]int64, len(findings))
	for _, f := range findings {
		if prev, ok := seen[f.DocID]; ok {
			r.flag("I1", "doc %s committed more than once (seq %d and %d)", f.DocID, prev, f.Seq)
			return
		}
		seen[f.DocID] = f.Seq
	}
}

// checkLeaseInvariants verifies I3/I4 and lease-log I5. Implemented in Task 4.
func checkLeaseInvariants(r *Report, findings []model.Finding, lease []model.LeaseEvent, coordinated bool) {
	_ = r
	_ = findings
	_ = lease
	_ = coordinated
}
