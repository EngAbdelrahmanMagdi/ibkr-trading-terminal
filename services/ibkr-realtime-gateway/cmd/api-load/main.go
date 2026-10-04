// Command api-load measures bounded HTTP read workloads. It never submits orders.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

func main() {
	endpoints := flag.String("url", "http://127.0.0.1:8080/api/v1/portfolio", "comma-separated HTTP read endpoints (up to 10)")
	clients := flag.Int("clients", 10, "concurrent clients (1..100)")
	duration := flag.Duration("duration", time.Minute, "measurement duration (up to 10m)")
	flag.Parse()
	urls := strings.Split(*endpoints, ",")
	if len(urls) > 10 {
		fmt.Fprintln(os.Stderr, "too many read endpoints")
		os.Exit(2)
	}
	for i, endpoint := range urls {
		urls[i] = strings.TrimSpace(endpoint)
		parsed, err := url.Parse(urls[i])
		if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			fmt.Fprintln(os.Stderr, "invalid read endpoint")
			os.Exit(2)
		}
	}
	if *clients < 1 || *clients > 100 || *duration <= 0 || *duration > 10*time.Minute {
		fmt.Fprintln(os.Stderr, "invalid workload bounds")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	var mu sync.Mutex
	var latencies []float64
	var failures, requests int
	statuses := make(map[int]int)
	var transportFailures int
	var wg sync.WaitGroup
	started := time.Now()
	for range *clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			endpointIndex := 0
			for ctx.Err() == nil {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, urls[endpointIndex], nil)
				endpointIndex = (endpointIndex + 1) % len(urls)
				if err != nil {
					cancel()
					return
				}
				at := time.Now()
				response, err := client.Do(request)
				failed := err != nil
				if response != nil {
					_, copyErr := io.Copy(io.Discard, io.LimitReader(response.Body, 4<<20))
					_ = response.Body.Close()
					failed = failed || copyErr != nil || response.StatusCode >= 400
				}
				if ctx.Err() != nil {
					return
				}
				elapsed := float64(time.Since(at).Microseconds()) / 1000
				mu.Lock()
				requests++
				if response != nil {
					statuses[response.StatusCode]++
				} else if err != nil {
					transportFailures++
				}
				if failed {
					failures++
				}
				if len(latencies) < 200000 {
					latencies = append(latencies, elapsed)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Float64s(latencies)
	percentile := func(p float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	result := map[string]any{"requests": requests, "failures": failures, "statusCounts": statuses, "transportFailures": transportFailures, "clients": *clients, "seconds": time.Since(started).Seconds(), "requestsPerSecond": float64(requests) / time.Since(started).Seconds(), "samples": len(latencies), "p50Ms": percentile(.5), "p95Ms": percentile(.95), "p99Ms": percentile(.99)}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		os.Exit(1)
	}
	if requests == 0 {
		os.Exit(1)
	}
}
