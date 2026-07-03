package main

import (
	"flag"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"quorum/internal/api"
	"quorum/internal/clock"
	"quorum/internal/perf"
	"quorum/internal/store"
)

func main() {
	agentsCSV := flag.String("agents", "2,4,8,16", "comma-separated agent counts")
	docsPerAgent := flag.Int("docs-per-agent", 200, "private docs per agent")
	passes := flag.Int("passes", 3, "passes over each agent's docs")
	k := flag.Int("k", 5, "keywords per annotation (unused in perf payload)")
	ttl := flag.Duration("ttl", 60*time.Second, "lease TTL")
	cpuprofile := flag.String("cpuprofile", "", "write a CPU profile to this path")
	flag.Parse()

	counts, err := parseCounts(*agentsCSV)
	if err != nil {
		log.Fatalf("parse -agents: %v", err)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatalf("create cpuprofile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("start cpuprofile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	fmt.Printf("Quorum latency/throughput — docs/agent=%d passes=%d (keep-alives on, race off)\n\n",
		*docsPerAgent, *passes)
	fmt.Printf("%4s %9s %10s | %-22s | %-22s | %-22s\n",
		"N", "ops", "ops/sec", "claim p50/p95/p99", "write p50/p95/p99", "release p50/p95/p99")
	fmt.Println(strings.Repeat("-", 100))

	for _, n := range counts {
		s := store.NewMemStore(clock.Real{})
		ts := httptest.NewServer(api.NewServer(s))
		res, err := perf.RunPerf(ts.URL, n, *docsPerAgent, *passes, *k, *ttl)
		ts.Close()
		if err != nil {
			log.Fatalf("perf (n=%d): %v", n, err)
		}
		fmt.Printf("%4d %9d %10.0f | %-22s | %-22s | %-22s\n",
			res.Agents, res.TotalOps, res.OpsPerSec,
			trip(res.Claim), trip(res.Write), trip(res.Release))
	}
}

func trip(s perf.OpStats) string {
	return fmt.Sprintf("%v/%v/%v", s.P50.Round(time.Microsecond), s.P95.Round(time.Microsecond), s.P99.Round(time.Microsecond))
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
