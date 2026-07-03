// Package checker replays Quorum's append-only logs and verifies invariants
// I1–I5. It is a pure function over the logs (no store, no clock, no HTTP), so
// consistency is checked, not asserted.
package checker

import (
	"fmt"
	"time"

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
	checkFindingsSeq(&r, findings)   // I5 (findings)
	checkVersionChains(&r, findings) // I2
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

// leaseInterval is one doc's ownership window [Start, End) held by Agent.
type leaseInterval struct {
	Agent string
	Start time.Time
	End   time.Time // expiry, or release time if released earlier
	Ended bool      // true once released
}

// checkLeaseInvariants verifies lease-log I5, I3 (mutual exclusion + write
// coverage), and I4 (legal reclaim after expiry). Lease events exist only in
// coordinated runs; in baseline mode the lease log is empty and only the
// (trivial) lease-Seq check runs.
func checkLeaseInvariants(r *Report, findings []model.Finding, lease []model.LeaseEvent, coordinated bool) {
	// I5 (lease): strictly increasing Seq from 1, no gaps.
	want := int64(1)
	for _, e := range lease {
		if e.Seq != want {
			r.flag("I5", "lease seq = %d, want %d (gap or out-of-order)", e.Seq, want)
			return
		}
		want++
	}
	if !coordinated {
		return
	}

	// Reconstruct the current lease per doc while replaying events in order.
	// A claim by a different agent while the current lease is still live is an
	// I3 violation; a legal claim (after expiry or release) starts a new
	// interval. Renew extends the current interval's End.
	current := map[string]*leaseInterval{}
	intervals := map[string][]leaseInterval{}

	closeCurrent := func(doc string, end time.Time) {
		if cur := current[doc]; cur != nil {
			cur.End = end
			cur.Ended = true
			intervals[doc] = append(intervals[doc], *cur)
			current[doc] = nil
		}
	}

	for _, e := range lease {
		cur := current[e.DocID]
		switch e.Kind {
		case "claim":
			if cur != nil && !cur.Ended && e.Timestamp.Before(cur.End) && cur.Agent != e.AgentID {
				r.flag("I3", "doc %s: %s claimed at %v while %s held a live lease until %v",
					e.DocID, e.AgentID, e.Timestamp, cur.Agent, cur.End)
			}
			// Close any prior interval (legal reclaim after expiry, or a
			// same-agent re-claim) and open a new one.
			if cur != nil {
				// The new claim supersedes the prior lease: cap the prior
				// interval's End at this claim's timestamp so it never retains a
				// stale future End that would falsely "cover" a later write.
				if e.Timestamp.Before(cur.End) {
					cur.End = e.Timestamp
				}
				cur.Ended = true
				intervals[e.DocID] = append(intervals[e.DocID], *cur)
			}
			current[e.DocID] = &leaseInterval{Agent: e.AgentID, Start: e.Timestamp, End: e.LeaseExpiry}
		case "renew":
			if cur == nil || cur.Agent != e.AgentID {
				r.flag("I3", "doc %s: renew by %s who does not hold the lease", e.DocID, e.AgentID)
				continue
			}
			cur.End = e.LeaseExpiry // extend
		case "release":
			closeCurrent(e.DocID, e.Timestamp)
		}
	}
	// Flush open intervals.
	for doc, cur := range current {
		if cur != nil {
			intervals[doc] = append(intervals[doc], *cur)
		}
	}

	// I3 write coverage: every write finding's author held a live lease covering
	// the write's Timestamp.
	for _, f := range findings {
		if !writeCovered(intervals[f.DocID], f.AgentID, f.Timestamp) {
			r.flag("I3", "doc %s: write by %s at %v not covered by a live lease",
				f.DocID, f.AgentID, f.Timestamp)
		}
	}
}

// writeCovered reports whether some interval held by agent covers ts (Start <= ts < End).
func writeCovered(ivs []leaseInterval, agent string, ts time.Time) bool {
	for _, iv := range ivs {
		if iv.Agent == agent && !ts.Before(iv.Start) && ts.Before(iv.End) {
			return true
		}
	}
	return false
}
