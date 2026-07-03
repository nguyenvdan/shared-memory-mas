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

		// Heartbeat: extend the lease before doing the write. If we have
		// already lost the lease (expired), ignore the error and let the write
		// surface the failure so the doc is left for another agent.
		_, _ = c.Renew(d.ID, agentID, ttl)

		conflicts, werr := writeWithRetry(c, d.ID, agentID, note, e.Version, p)
		st.Conflicts += conflicts
		_ = c.Release(d.ID, agentID)
		if werr != nil {
			return st, werr
		}
		st.Annotated++
	}
	return st, nil
}

// writeWithRetry writes payload for docID under a bounded CAS retry. On a
// version conflict it re-Reads to refresh the base version and retries. If the
// re-Read itself fails, it stops and returns that error (rather than retrying
// with a stale base). Returns the number of conflicts observed (retries).
func writeWithRetry(c *api.Client, docID, agentID, payload string, base int, p retry.Policy) (int, error) {
	attempts, err := retry.Do(p, func() (bool, error) {
		_, werr := c.Write(docID, agentID, payload, base)
		if errors.Is(werr, api.ErrConflict) {
			re, rerr := c.Read(docID)
			if rerr != nil {
				return false, rerr // cannot refresh base; fail this doc
			}
			base = re.Version
			return true, werr // retryable
		}
		return false, werr // success or fatal
	})
	return attempts - 1, err
}
