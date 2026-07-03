package main

import (
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	"quorum/internal/agent"
	"quorum/internal/api"
	"quorum/internal/checker"
	"quorum/internal/clock"
	"quorum/internal/corpus"
	"quorum/internal/retry"
	"quorum/internal/store"
)

func main() {
	corpusPath := flag.String("corpus", "corpus/fixture.jsonl", "corpus JSONL path")
	agents := flag.Int("agents", 8, "number of concurrent agents")
	k := flag.Int("k", 5, "keywords per annotation")
	ttl := flag.Duration("ttl", 60*time.Second, "lease TTL")
	flag.Parse()

	docs, err := corpus.Load(*corpusPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load corpus: %v\n", err)
		os.Exit(2)
	}

	s := store.NewMemStore(clock.Real{})
	ts := httptest.NewServer(api.NewServer(s))
	defer ts.Close()

	ids := make([]string, *agents)
	for i := range ids {
		ids[i] = fmt.Sprintf("agent-%d", i)
	}
	if _, err := agent.RunConcurrent(ts.URL, docs, ids, *k, *ttl, retry.Default()); err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		os.Exit(2)
	}

	rep := checker.Check(s.Findings(), s.LeaseEvents(), true)
	fmt.Printf("Quorum invariant check — %d agents, %d docs\n", *agents, len(docs))
	fmt.Printf("findings=%d lease-events=%d\n", len(s.Findings()), len(s.LeaseEvents()))
	if rep.OK() {
		fmt.Println("ALL INVARIANTS HOLD (I1–I5)")
		return
	}
	fmt.Printf("VIOLATIONS (%d):\n", len(rep.Violations))
	for _, v := range rep.Violations {
		fmt.Printf("  [%s] %s\n", v.Invariant, v.Detail)
	}
	os.Exit(1)
}
