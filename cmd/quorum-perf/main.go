package main

import (
	"flag"
	"fmt"
	"log"
	"math"
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
	runs := flag.Int("runs", 5, "runs per N cell (for variance)")
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

	fmt.Printf("Quorum latency/throughput — docs/agent=%d passes=%d, mean over %d runs (keep-alives on, race off)\n",
		*docsPerAgent, *passes, *runs)
	fmt.Printf("ops/sec is mean±stddev across runs; no cross-agent contention — measures substrate op cost\n\n")
	fmt.Printf("%4s %9s %16s | %-22s | %-22s | %-22s\n",
		"N", "ops", "ops/sec", "claim p50/p95/p99", "write p50/p95/p99", "release p50/p95/p99")
	fmt.Println(strings.Repeat("-", 106))

	for _, n := range counts {
		var opsPerSec []float64
		var claimP50, claimP95, claimP99 []time.Duration
		var writeP50, writeP95, writeP99 []time.Duration
		var releaseP50, releaseP95, releaseP99 []time.Duration
		var lastTotalOps int

		for r := 0; r < *runs; r++ {
			s := store.NewMemStore(clock.Real{})
			ts := httptest.NewServer(api.NewServer(s))
			res, err := perf.RunPerf(ts.URL, n, *docsPerAgent, *passes, *k, *ttl)
			ts.Close()
			if err != nil {
				log.Fatalf("perf (n=%d, run=%d): %v", n, r, err)
			}
			opsPerSec = append(opsPerSec, res.OpsPerSec)
			lastTotalOps = res.TotalOps

			claimP50 = append(claimP50, res.Claim.P50)
			claimP95 = append(claimP95, res.Claim.P95)
			claimP99 = append(claimP99, res.Claim.P99)
			writeP50 = append(writeP50, res.Write.P50)
			writeP95 = append(writeP95, res.Write.P95)
			writeP99 = append(writeP99, res.Write.P99)
			releaseP50 = append(releaseP50, res.Release.P50)
			releaseP95 = append(releaseP95, res.Release.P95)
			releaseP99 = append(releaseP99, res.Release.P99)
		}

		mean, sd := meanStddev(opsPerSec)
		claim := perf.OpStats{P50: meanDur(claimP50), P95: meanDur(claimP95), P99: meanDur(claimP99)}
		write := perf.OpStats{P50: meanDur(writeP50), P95: meanDur(writeP95), P99: meanDur(writeP99)}
		release := perf.OpStats{P50: meanDur(releaseP50), P95: meanDur(releaseP95), P99: meanDur(releaseP99)}

		fmt.Printf("%4d %9d %8.0f±%-6.0f | %-22s | %-22s | %-22s\n",
			n, lastTotalOps, mean, sd,
			trip(claim), trip(write), trip(release))
	}
}

func trip(s perf.OpStats) string {
	return fmt.Sprintf("%v/%v/%v", s.P50.Round(time.Microsecond), s.P95.Round(time.Microsecond), s.P99.Round(time.Microsecond))
}

func meanStddev(xs []float64) (mean, sd float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean = sum / float64(len(xs))
	var ss float64
	for _, x := range xs {
		d := x - mean
		ss += d * d
	}
	sd = math.Sqrt(ss / float64(len(xs))) // population stddev
	return mean, sd
}

func meanDur(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range ds {
		sum += d
	}
	return sum / time.Duration(len(ds))
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
