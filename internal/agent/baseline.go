package agent

import (
	"sync"

	"quorum/internal/annotate"
	"quorum/internal/api"
	"quorum/internal/model"
	"quorum/internal/retry"
)

// RunBaseline walks the corpus once with NO coordination: it does not claim
// leases and does not consult prior findings to skip already-annotated docs.
// Every doc is annotated and written (under CAS retry, since concurrent agents
// contend on the same doc versions). This is the no-shared-memory baseline used
// to measure how much duplicated work coordination eliminates.
func RunBaseline(c *api.Client, docs []model.Doc, agentID string, k int, p retry.Policy) (Stats, error) {
	var st Stats
	for _, d := range docs {
		e, err := c.Read(d.ID)
		if err != nil {
			return st, err
		}
		note := annotate.Annotate(d, k)
		conflicts, werr := writeWithRetry(c, d.ID, agentID, note, e.Version, p)
		st.Conflicts += conflicts
		if werr != nil {
			return st, werr
		}
		st.Annotated++
	}
	return st, nil
}

// RunBaselineConcurrent runs one goroutine per agentID (each with its own
// client) all running RunBaseline against the same store. Returns per-agent
// Stats in order; returns the first non-nil agent error after all finish.
func RunBaselineConcurrent(base string, docs []model.Doc, agentIDs []string, k int, p retry.Policy) ([]Stats, error) {
	stats := make([]Stats, len(agentIDs))
	errs := make([]error, len(agentIDs))
	var wg sync.WaitGroup
	for i, id := range agentIDs {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			c := api.NewClient(base) // per-goroutine client, never shared
			st, err := RunBaseline(c, docs, id, k, p)
			stats[i] = st
			errs[i] = err
		}(i, id)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return stats, err
		}
	}
	return stats, nil
}
