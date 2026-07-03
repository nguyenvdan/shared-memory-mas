package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"quorum/internal/bench"
	"quorum/internal/corpus"
	"quorum/internal/model"
	"quorum/internal/retry"
)

func main() {
	corpusPath := flag.String("corpus", "corpus/arxiv.jsonl", "corpus JSONL path")
	k := flag.Int("k", 5, "keywords per annotation")
	runs := flag.Int("runs", 5, "runs per (mode, N) cell")
	ttl := flag.Duration("ttl", 60*time.Second, "lease TTL")
	agentsCSV := flag.String("agents", "2,4,8,16", "comma-separated agent counts")
	flag.Parse()

	docs, err := corpus.Load(*corpusPath)
	if err != nil {
		log.Fatalf("load corpus: %v", err)
	}
	counts, err := parseCounts(*agentsCSV)
	if err != nil {
		log.Fatalf("parse -agents: %v", err)
	}

	fmt.Printf("Quorum duplication benchmark — corpus=%d docs, runs=%d each\n\n", len(docs), *runs)
	fmt.Printf("%-12s %4s %10s %10s %8s %10s\n", "mode", "N", "findings", "unique", "dup%", "conflicts")
	fmt.Println(strings.Repeat("-", 60))

	for _, n := range counts {
		// baseline: retry cap generous enough that every duplicate commits.
		baseP := retry.Policy{MaxAttempts: 4*n + 32, BaseDelay: 0}
		printRow(runCell(false, n, docs, *k, *ttl, baseP, *runs))
		runtime.GC()
		debug.FreeOSMemory()
		cleanupTime := 3 * time.Second
		if n >= 8 {
			cleanupTime = 10 * time.Second // heavy contention at N>=8
		}
		time.Sleep(cleanupTime) // allow cleanup between baseline and coordinated
		runtime.GC()
		debug.FreeOSMemory()
		time.Sleep(1 * time.Second)
		printRow(runCell(true, n, docs, *k, *ttl, retry.Default(), *runs))
		runtime.GC()
		debug.FreeOSMemory()
		time.Sleep(cleanupTime) // allow cleanup between agent counts
	}
}

// runCell runs one (mode, N) cell `runs` times and returns the last run's raw
// counts with the duplication rate averaged across runs (counts are
// deterministic by construction; averaging guards against any surprise).
func runCell(coordinated bool, n int, docs []model.Doc, k int, ttl time.Duration, p retry.Policy, runs int) bench.Result {
	var agg bench.Result
	var rateSum float64
	for i := 0; i < runs; i++ {
		res, err := bench.RunScenario(coordinated, n, docs, k, ttl, p)
		if err != nil {
			log.Fatalf("scenario (coord=%v n=%d): %v", coordinated, n, err)
		}
		agg = res
		rateSum += res.DuplicationRate
		if i < runs-1 { // no sleep after last run in cell
			time.Sleep(3 * time.Second) // allow port cleanup between runs
		}
	}
	agg.DuplicationRate = rateSum / float64(runs)
	return agg
}

func printRow(r bench.Result) {
	fmt.Printf("%-12s %4d %10d %10d %7.1f%% %10d\n",
		r.Mode, r.Agents, r.TotalFindings, r.UniqueDocs, r.DuplicationRate*100, r.Conflicts)
}

func parseCounts(csv string) ([]int, error) {
	parts := strings.Split(csv, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}
