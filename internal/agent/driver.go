package agent

import (
	"sync"
	"time"

	"quorum/internal/api"
	"quorum/internal/model"
	"quorum/internal/retry"
)

// RunConcurrent runs one goroutine per agentID, each with its own HTTP client,
// all coordinating over the same store at base. Returns per-agent Stats in the
// same order as agentIDs; returns the first non-nil agent error (if any) after
// all goroutines finish.
func RunConcurrent(base string, docs []model.Doc, agentIDs []string, k int, ttl time.Duration, p retry.Policy) ([]Stats, error) {
	stats := make([]Stats, len(agentIDs))
	errs := make([]error, len(agentIDs))
	var wg sync.WaitGroup
	for i, id := range agentIDs {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			c := api.NewClient(base) // per-goroutine client, never shared
			st, err := RunCoordinated(c, docs, id, k, ttl, p)
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
