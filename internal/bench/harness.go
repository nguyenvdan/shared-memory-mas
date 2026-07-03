package bench

import (
	"fmt"
	"net/http/httptest"
	"time"

	"quorum/internal/agent"
	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/model"
	"quorum/internal/retry"
	"quorum/internal/store"
)

// RunScenario runs one benchmark scenario end-to-end over an in-process HTTP
// server and returns the duplication Result. When coordinated is false the
// store bypasses the lease guard and agents run the no-coordination baseline.
func RunScenario(coordinated bool, agents int, docs []model.Doc, k int, ttl time.Duration, p retry.Policy) (Result, error) {
	var s *store.MemStore
	if coordinated {
		s = store.NewMemStore(clock.Real{})
	} else {
		s = store.NewMemStore(clock.Real{}, store.Uncoordinated())
	}
	ts := httptest.NewUnstartedServer(api.NewServer(s))
	ts.Config.SetKeepAlivesEnabled(false)
	ts.Start()
	defer ts.Close()

	ids := make([]string, agents)
	for i := range ids {
		ids[i] = fmt.Sprintf("agent-%d", i)
	}

	var stats []agent.Stats
	var err error
	mode := "coordinated"
	if coordinated {
		stats, err = agent.RunConcurrent(ts.URL, docs, ids, k, ttl, p)
	} else {
		mode = "baseline"
		stats, err = agent.RunBaselineConcurrent(ts.URL, docs, ids, k, p)
	}
	if err != nil {
		return Result{}, err
	}

	all, ferr := api.NewClient(ts.URL).Findings("")
	if ferr != nil {
		return Result{}, ferr
	}
	rate, total, unique, dupes := DuplicationRate(all)

	res := Result{
		Mode:              mode,
		Agents:            agents,
		TotalFindings:     total,
		UniqueDocs:        unique,
		DuplicateFindings: dupes,
		DuplicationRate:   rate,
	}
	for _, st := range stats {
		res.Conflicts += st.Conflicts
		res.ClaimsLost += st.ClaimsLost
	}
	return res, nil
}
