package agent

import (
	"errors"
	"time"

	"quorum/internal/annotate"
	"quorum/internal/api"
	"quorum/internal/model"
	"quorum/internal/retry"
)

// runOnceTTL is the lease duration a single agent takes per document. Kept
// internal so RunOnce's signature stays stable across the Phase-1 -> Phase-2
// change that made writes require a held lease.
const runOnceTTL = time.Minute

// RunOnce walks the corpus once as a single agent, using lease-based claiming
// (required since Phase 2). For each doc it claims a lease; skips docs already
// annotated or already held by another agent; otherwise annotates and writes
// under the lease; and always releases a lease it acquired. Returns how many
// docs this agent annotated.
func RunOnce(c *api.Client, docs []model.Doc, agentID string, k int) (int, error) {
	annotated := 0
	for _, d := range docs {
		if _, err := c.Claim(d.ID, agentID, runOnceTTL); err != nil {
			if errors.Is(err, api.ErrLeaseHeld) {
				continue
			}
			return annotated, err
		}
		e, err := c.Read(d.ID)
		if err != nil {
			_ = c.Release(d.ID, agentID)
			return annotated, err
		}
		if e.Exists {
			_ = c.Release(d.ID, agentID)
			continue
		}
		note := annotate.Annotate(d, k)
		_, werr := c.Write(d.ID, agentID, note, e.Version)
		_ = c.Release(d.ID, agentID)
		if werr != nil {
			return annotated, werr
		}
		annotated++
	}
	return annotated, nil
}

// Stats are per-run counters for a coordinated agent.
type Stats struct {
	Annotated  int
	Skipped    int
	Conflicts  int
	ClaimsLost int
}

// RunCoordinated walks the corpus once using lease-based claiming and
// CAS-retry writes. It claims each doc; skips docs already annotated or held by
// another agent; annotates+writes under a bounded retry on version conflict;
// and always releases a lease it acquired.
func RunCoordinated(c *api.Client, docs []model.Doc, agentID string, k int, ttl time.Duration, p retry.Policy) (Stats, error) {
	var st Stats
	for _, d := range docs {
		_, err := c.Claim(d.ID, agentID, ttl)
		if errors.Is(err, api.ErrLeaseHeld) {
			st.ClaimsLost++
			continue
		}
		if err != nil {
			return st, err
		}

		// Holding the lease: decide whether work is needed.
		e, err := c.Read(d.ID)
		if err != nil {
			_ = c.Release(d.ID, agentID)
			return st, err
		}
		if e.Exists {
			st.Skipped++
			_ = c.Release(d.ID, agentID)
			continue
		}

		note := annotate.Annotate(d, k)
		base := e.Version

		// Heartbeat: extend the lease before doing the write. If we have
		// already lost the lease (expired), ignore the error and let the write
		// surface the failure so the doc is left for another agent.
		_, _ = c.Renew(d.ID, agentID, ttl)

		attempts, werr := retry.Do(p, func() (bool, error) {
			_, err := c.Write(d.ID, agentID, note, base)
			if errors.Is(err, api.ErrConflict) {
				if re, rerr := c.Read(d.ID); rerr == nil {
					base = re.Version
				}
				return true, err // retryable
			}
			return false, err // success or fatal
		})
		st.Conflicts += attempts - 1
		_ = c.Release(d.ID, agentID)
		if werr != nil {
			return st, werr
		}
		st.Annotated++
	}
	return st, nil
}
