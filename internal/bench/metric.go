package bench

import "quorum/internal/model"

// Result is one benchmark scenario's outcome.
type Result struct {
	Mode              string  // "coordinated" or "baseline"
	Agents            int     // N
	TotalFindings     int     // committed annotations in the log
	UniqueDocs        int     // distinct doc_ids annotated
	DuplicateFindings int     // TotalFindings - UniqueDocs
	DuplicationRate   float64 // DuplicateFindings / TotalFindings
	Conflicts         int     // summed CAS retries across agents
	ClaimsLost        int     // summed lost claims across agents
}

// DuplicationRate computes the frozen doc-level duplication metric over the
// findings log: the fraction of committed annotations that are redundant (a
// doc annotated more than once). Returns the rate plus the raw counts.
func DuplicationRate(findings []model.Finding) (rate float64, total, unique, dupes int) {
	total = len(findings)
	seen := make(map[string]struct{}, total)
	for _, f := range findings {
		seen[f.DocID] = struct{}{}
	}
	unique = len(seen)
	dupes = total - unique
	if total == 0 {
		return 0, 0, 0, 0
	}
	return float64(dupes) / float64(total), total, unique, dupes
}
