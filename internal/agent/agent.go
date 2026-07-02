package agent

import (
	"quorum/internal/annotate"
	"quorum/internal/api"
	"quorum/internal/model"
)

// RunOnce walks the corpus once. For each doc it reads current state; if the
// doc already has a committed annotation it is skipped (the Phase 1 dedup
// guard), otherwise the agent annotates and writes it. Returns how many docs
// this agent annotated.
func RunOnce(c *api.Client, docs []model.Doc, agentID string, k int) (int, error) {
	annotated := 0
	for _, d := range docs {
		e, err := c.Read(d.ID)
		if err != nil {
			return annotated, err
		}
		if e.Exists {
			continue
		}
		note := annotate.Annotate(d, k)
		if _, err := c.Write(d.ID, agentID, note, e.Version); err != nil {
			return annotated, err
		}
		annotated++
	}
	return annotated, nil
}
