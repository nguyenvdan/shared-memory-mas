package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"quorum/internal/agent"
	"quorum/internal/corpus"
	"quorum/internal/retry"
)

func main() {
	storeURL := flag.String("store", "http://localhost:8080", "store base URL")
	corpusPath := flag.String("corpus", "corpus/fixture.jsonl", "corpus JSONL path")
	nAgents := flag.Int("agents", 4, "number of concurrent agents")
	k := flag.Int("k", 5, "keywords per annotation")
	ttl := flag.Duration("ttl", 60*time.Second, "lease TTL")
	flag.Parse()

	docs, err := corpus.Load(*corpusPath)
	if err != nil {
		log.Fatalf("load corpus: %v", err)
	}
	ids := make([]string, *nAgents)
	for i := range ids {
		ids[i] = fmt.Sprintf("agent-%d", i)
	}

	stats, err := agent.RunConcurrent(*storeURL, docs, ids, *k, *ttl, retry.Default())
	if err != nil {
		log.Fatalf("swarm run: %v", err)
	}

	var annotated, skipped, conflicts, lost int
	for _, s := range stats {
		annotated += s.Annotated
		skipped += s.Skipped
		conflicts += s.Conflicts
		lost += s.ClaimsLost
	}
	log.Printf("swarm of %d agents over %d docs: annotated=%d skipped=%d conflicts=%d claims_lost=%d",
		*nAgents, len(docs), annotated, skipped, conflicts, lost)
}
