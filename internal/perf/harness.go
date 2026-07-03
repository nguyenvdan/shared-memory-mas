package perf

import (
	"fmt"
	"sync"
	"time"

	"quorum/internal/api"
)

type PerfResult struct {
	Agents               int
	Claim, Write, Release OpStats
	TotalOps             int
	Wall                 time.Duration
	OpsPerSec            float64
}

// RunPerf drives `agents` goroutines, each operating on its own private doc keys
// (so no cross-agent lease/version contention — only the store mutex and HTTP
// layer are shared). Each agent performs claim→write→release per doc for
// `passes` iterations, timing every operation. Returns per-op latency stats and
// throughput. Errors from any agent abort with the first error.
func RunPerf(base string, agents, docsPerAgent, passes, k int, ttl time.Duration) (PerfResult, error) {
	type agentSamples struct {
		claim, write, release []time.Duration
		err                   error
	}
	samples := make([]agentSamples, agents)

	var wg sync.WaitGroup
	start := time.Now()
	for a := 0; a < agents; a++ {
		wg.Add(1)
		go func(a int) {
			defer wg.Done()
			c := api.NewClient(base) // per-goroutine client
			var sm agentSamples
			for p := 0; p < passes; p++ {
				for d := 0; d < docsPerAgent; d++ {
					doc := fmt.Sprintf("a%d-d%d", a, d)

					t0 := time.Now()
					if _, err := c.Claim(doc, agentID(a), ttl); err != nil {
						sm.err = err
						samples[a] = sm
						return
					}
					sm.claim = append(sm.claim, time.Since(t0))

					// Read to get the current version, then write (timed).
					e, err := c.Read(doc)
					if err != nil {
						sm.err = err
						samples[a] = sm
						return
					}
					t1 := time.Now()
					if _, err := c.Write(doc, agentID(a), "x", e.Version); err != nil {
						sm.err = err
						samples[a] = sm
						return
					}
					sm.write = append(sm.write, time.Since(t1))

					t2 := time.Now()
					if err := c.Release(doc, agentID(a)); err != nil {
						sm.err = err
						samples[a] = sm
						return
					}
					sm.release = append(sm.release, time.Since(t2))
				}
			}
			samples[a] = sm
		}(a)
	}
	wg.Wait()
	wall := time.Since(start)

	var claim, write, release []time.Duration
	for _, sm := range samples {
		if sm.err != nil {
			return PerfResult{}, sm.err
		}
		claim = append(claim, sm.claim...)
		write = append(write, sm.write...)
		release = append(release, sm.release...)
	}

	total := len(claim) + len(write) + len(release)
	res := PerfResult{
		Agents:   agents,
		Claim:    Summarize(claim),
		Write:    Summarize(write),
		Release:  Summarize(release),
		TotalOps: total,
		Wall:     wall,
	}
	if wall > 0 {
		res.OpsPerSec = float64(total) / wall.Seconds()
	}
	return res, nil
}

func agentID(a int) string { return fmt.Sprintf("agent-%d", a) }
