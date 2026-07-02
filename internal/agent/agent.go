package agent

import (
	"errors"
	"time"

	"quorum/internal/annotate"
	"quorum/internal/api"
	"quorum/internal/model"
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
