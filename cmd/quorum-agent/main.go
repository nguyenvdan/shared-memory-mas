package main

import (
	"flag"
	"log"

	"quorum/internal/agent"
	"quorum/internal/api"
	"quorum/internal/corpus"
)

func main() {
	storeURL := flag.String("store", "http://localhost:8080", "store base URL")
	corpusPath := flag.String("corpus", "corpus/fixture.jsonl", "corpus JSONL path")
	agentID := flag.String("id", "agent-0", "agent id")
	k := flag.Int("k", 5, "keywords per annotation")
	flag.Parse()

	docs, err := corpus.Load(*corpusPath)
	if err != nil {
		log.Fatalf("load corpus: %v", err)
	}
	n, err := agent.RunOnce(api.NewClient(*storeURL), docs, *agentID, *k)
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	log.Printf("agent %s annotated %d/%d docs", *agentID, n, len(docs))
}
