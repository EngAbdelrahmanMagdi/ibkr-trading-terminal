// Command stream-load is a load and soak client for the market stream. It opens many WebSocket clients that
// subscribe to random symbols (by default the simulator's synthetic SYNnnn instruments), optionally churns
// subscriptions and simulates slow readers, and reports delivery statistics.
//
// With -metrics-url it also samples the gateway's Prometheus endpoint during the run and, after all clients
// have disconnected, checks for leaks: no sessions or upstream subscriptions left, goroutines back to the
// baseline, and heap growth within bounds. The numbers it prints are measurements of one run, not targets.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type options struct {
	url              string
	metricsURL       string
	clients          int
	symbolsPerClient int
	universe         []string
	churnEvery       time.Duration
	duration         time.Duration
	ramp             time.Duration
	slowReaders      int
	slowDelay        time.Duration
	drainWait        time.Duration
	sampleEvery      time.Duration
	goroutineSlack   float64
	heapGrowth       float64
}

func main() {
	var o options
	var synthetic int
	var symbols string
	flag.StringVar(&o.url, "url", "ws://127.0.0.1:8090/ws", "market stream URL")
	flag.StringVar(&o.metricsURL, "metrics-url", "", "gateway /metrics URL; enables sampling and leak checks")
	flag.IntVar(&o.clients, "clients", 50, "concurrent clients")
	flag.IntVar(&o.symbolsPerClient, "symbols-per-client", 20, "symbols each client subscribes to")
	flag.IntVar(&synthetic, "synthetic", 100, "use SYN001..SYNnnn as the symbol universe")
	flag.StringVar(&symbols, "symbols", "", "comma-separated symbol universe (overrides -synthetic)")
	flag.DurationVar(&o.churnEvery, "churn-every", 2*time.Second, "per client: replace one subscription this often (0 disables)")
	flag.DurationVar(&o.duration, "duration", time.Minute, "run duration")
	flag.DurationVar(&o.ramp, "ramp", 5*time.Second, "spread client connections over this period")
	flag.IntVar(&o.slowReaders, "slow-readers", 0, "clients that pause between reads (expected to receive coalesced updates or be evicted)")
	flag.DurationVar(&o.slowDelay, "slow-delay", 200*time.Millisecond, "pause of slow readers between reads")
	flag.DurationVar(&o.drainWait, "drain-wait", 30*time.Second, "after disconnecting, how long to wait for the gateway to release sessions and subscriptions")
	flag.DurationVar(&o.sampleEvery, "sample-every", 5*time.Second, "metrics sampling period")
	flag.Float64Var(&o.goroutineSlack, "goroutine-slack", 10, "allowed goroutines above the baseline after the run")
	flag.Float64Var(&o.heapGrowth, "heap-growth", 1.5, "allowed ratio of late-run to early-run mean heap in use")
	flag.Parse()

	if symbols != "" {
		o.universe = strings.Split(symbols, ",")
	} else {
		for i := 1; i <= synthetic; i++ {
			o.universe = append(o.universe, fmt.Sprintf("SYN%03d", i))
		}
	}
	if err := validate(o); err != nil {
		fmt.Fprintf(os.Stderr, "stream-load: %v\n", err)
		os.Exit(2)
	}
	if err := run(o); err != nil {
		fmt.Fprintf(os.Stderr, "stream-load: FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("stream-load: ok")
}

func validate(o options) error {
	switch {
	case o.clients < 1 || o.clients > 10_000:
		return errors.New("-clients must be between 1 and 10000")
	case len(o.universe) == 0:
		return errors.New("empty symbol universe")
	case o.symbolsPerClient < 1 || o.symbolsPerClient > len(o.universe):
		return fmt.Errorf("-symbols-per-client must be between 1 and %d", len(o.universe))
	case o.slowReaders < 0 || o.slowReaders > o.clients:
		return errors.New("-slow-readers must be between 0 and -clients")
	case o.duration <= 0:
		return errors.New("-duration must be positive")
	}
	return nil
}

// ---------------------------------------------------------------- statistics

// latencyBuckets are upper bounds in milliseconds for quote delivery latency (receive time minus quote time).
var latencyBuckets = []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000}

type stats struct {
	quotes, snapshots, staleMsgs, errorsMsgs atomic.Int64
	gaps, orderViolations, slowReordered     atomic.Int64
	subscribes, unsubscribes                 atomic.Int64
	connectFailures, evicted, otherCloses    atomic.Int64
	latency                                  [13]atomic.Int64 // len(latencyBuckets)+1 (overflow)
}

func (s *stats) observeLatency(ms float64) {
	i, _ := slices.BinarySearch(latencyBuckets, ms)
	s.latency[i].Add(1)
}

func (s *stats) percentile(p float64) string {
	var total int64
	for i := range s.latency {
		total += s.latency[i].Load()
	}
	if total == 0 {
		return "n/a"
	}
	target := int64(float64(total) * p)
	var acc int64
	for i := range s.latency {
		acc += s.latency[i].Load()
		if acc > target {
			if i == len(latencyBuckets) {
				return fmt.Sprintf("> %.0f ms", latencyBuckets[len(latencyBuckets)-1])
			}
			return fmt.Sprintf("<= %.0f ms", latencyBuckets[i])
		}
	}
	return "n/a"
}

// ---------------------------------------------------------------- clients

type message struct {
	Type      string `json:"type"`
	Symbol    string `json:"symbol"`
	Sequence  int64  `json:"sequence"`
	Timestamp string `json:"timestamp"`
}

func run(o options) error {
	st := &stats{}
	var sampler *sampler
	if o.metricsURL != "" {
		sampler = newSampler(o.metricsURL)
		if err := sampler.sample(); err != nil {
			return fmt.Errorf("metrics endpoint: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), o.duration)
	defer cancel()
	samplerDone := make(chan struct{})
	go func() { // owned by run; ends with ctx
		defer close(samplerDone)
		if sampler == nil {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(o.sampleEvery):
				if err := sampler.sample(); err != nil {
					fmt.Fprintf(os.Stderr, "stream-load: metrics sample failed: %v\n", err)
				}
			}
		}
	}()

	started := time.Now()
	var wg sync.WaitGroup
	for i := range o.clients {
		wg.Add(1)
		go func() { // one goroutine per client; ends with ctx
			defer wg.Done()
			if o.ramp > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(o.ramp * time.Duration(i) / time.Duration(o.clients)):
				}
			}
			client(ctx, o, i, i < o.slowReaders, st)
		}()
	}
	wg.Wait()
	<-samplerDone
	elapsed := time.Since(started)

	fmt.Printf("clients: %d (slow readers: %d), symbols per client: %d of %d, churn every: %s, duration: %s\n",
		o.clients, o.slowReaders, o.symbolsPerClient, len(o.universe), o.churnEvery, elapsed.Round(time.Millisecond))
	fmt.Printf("received: %d quotes (%.0f/s), %d snapshots, %d stale, %d errors\n",
		st.quotes.Load(), float64(st.quotes.Load())/elapsed.Seconds(), st.snapshots.Load(), st.staleMsgs.Load(), st.errorsMsgs.Load())
	fmt.Printf("subscription changes: %d subscribes, %d unsubscribes\n", st.subscribes.Load(), st.unsubscribes.Load())
	fmt.Printf("sequence gaps (coalesced or skipped ticks; not meaningful for time-derived IBKR sequences): %d\n", st.gaps.Load())
	fmt.Printf("ordering: %d violations (normal clients), %d out-of-order frames seen by slow readers behind their own churn\n",
		st.orderViolations.Load(), st.slowReordered.Load())
	fmt.Printf("delivery latency (receive - quote time): p50 %s, p95 %s, p99 %s\n", st.percentile(0.50), st.percentile(0.95), st.percentile(0.99))
	fmt.Printf("connections: %d failed, %d evicted (seen by clients before the run ended), %d closed otherwise\n", st.connectFailures.Load(), st.evicted.Load(), st.otherCloses.Load())

	var failures []string
	if n := st.orderViolations.Load(); n > 0 {
		failures = append(failures, fmt.Sprintf("%d sequence order violations", n))
	}
	if n := st.connectFailures.Load(); n > 0 {
		failures = append(failures, fmt.Sprintf("%d connection failures", n))
	}
	if n := st.otherCloses.Load(); n > 0 {
		failures = append(failures, fmt.Sprintf("%d unexpected connection closes", n))
	}
	if n := st.evicted.Load(); n > 0 && o.slowReaders == 0 {
		failures = append(failures, fmt.Sprintf("%d evictions without slow readers", n))
	}
	if st.quotes.Load() == 0 {
		failures = append(failures, "no quotes received")
	}
	if sampler != nil {
		failures = append(failures, sampler.verify(o)...)
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

func client(ctx context.Context, o options, id int, slow bool, st *stats) {
	rng := rand.New(rand.NewPCG(uint64(id), 0x5eed)) //nolint:gosec // reproducible load pattern, not security
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, resp, err := websocket.Dial(dialCtx, o.url, &websocket.DialOptions{HTTPClient: http.DefaultClient})
	cancel()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		if ctx.Err() == nil {
			st.connectFailures.Add(1)
		}
		return
	}
	conn.SetReadLimit(1 << 20)
	defer func() { _ = conn.CloseNow() }()

	var mu sync.Mutex
	lastSeq := map[string]int64{} // subscribed symbol -> last sequence seen (0 until the snapshot)
	subscribed := rng.Perm(len(o.universe))[:o.symbolsPerClient]
	names := make([]string, 0, len(subscribed))
	for _, i := range subscribed {
		names = append(names, o.universe[i])
		lastSeq[o.universe[i]] = 0
	}
	if !write(ctx, conn, "subscribe", names...) {
		return
	}
	st.subscribes.Add(int64(len(names)))

	readerDone := make(chan struct{})
	go func() { // reader; ends when the connection closes
		defer close(readerDone)
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				switch status := websocket.CloseStatus(err); {
				case status == websocket.StatusTryAgainLater, slow && ctx.Err() == nil:
					// Unresponsive peers are dropped without a close frame, so a slow reader may see EOF.
					st.evicted.Add(1)
				case status == websocket.StatusNormalClosure, status == websocket.StatusGoingAway:
				default:
					if ctx.Err() == nil {
						st.otherCloses.Add(1)
					}
				}
				return
			}
			now := time.Now()
			var m message
			if json.Unmarshal(data, &m) != nil {
				continue
			}
			switch m.Type {
			case "snapshot", "quote":
				track(st, &mu, lastSeq, m, now, slow)
			case "stale":
				st.staleMsgs.Add(1)
			case "error":
				st.errorsMsgs.Add(1)
			}
			if slow {
				time.Sleep(o.slowDelay)
			}
		}
	}()

	var churn <-chan time.Time
	if o.churnEvery > 0 {
		ticker := time.NewTicker(o.churnEvery)
		defer ticker.Stop()
		churn = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			_ = conn.Close(websocket.StatusNormalClosure, "load test finished")
			<-readerDone
			return
		case <-readerDone:
			return
		case <-churn:
			mu.Lock()
			current := make([]string, 0, len(lastSeq))
			for s := range lastSeq {
				current = append(current, s)
			}
			slices.Sort(current)
			drop := current[rng.IntN(len(current))]
			add := o.universe[rng.IntN(len(o.universe))]
			if _, dup := lastSeq[add]; dup {
				mu.Unlock()
				continue
			}
			delete(lastSeq, drop)
			lastSeq[add] = 0
			mu.Unlock()
			if !write(ctx, conn, "unsubscribe", drop) || !write(ctx, conn, "subscribe", add) {
				continue
			}
			st.unsubscribes.Add(1)
			st.subscribes.Add(1)
		}
	}
}

// track checks ordering per symbol. Quotes of a symbol unsubscribed moments ago may still arrive and are
// ignored; a snapshot restarts tracking. A slow reader can lag its own subscription changes by minutes, so
// frames from an earlier subscription of a re-added symbol are indistinguishable from reordering; they are
// counted separately and do not fail the run.
func track(st *stats, mu *sync.Mutex, lastSeq map[string]int64, m message, now time.Time, slow bool) {
	violation := &st.orderViolations
	if slow {
		violation = &st.slowReordered
	}
	mu.Lock()
	last, ok := lastSeq[m.Symbol]
	if ok {
		switch {
		case m.Type == "snapshot":
			lastSeq[m.Symbol] = m.Sequence
		case last == 0:
			violation.Add(1) // a quote before its snapshot
		case m.Sequence <= last:
			violation.Add(1)
		default:
			if m.Sequence > last+1 {
				st.gaps.Add(m.Sequence - last - 1)
			}
			lastSeq[m.Symbol] = m.Sequence
		}
	}
	mu.Unlock()
	if m.Type == "snapshot" {
		st.snapshots.Add(1)
		return
	}
	st.quotes.Add(1)
	if at, err := time.Parse(time.RFC3339Nano, m.Timestamp); err == nil {
		st.observeLatency(float64(now.Sub(at).Microseconds()) / 1000)
	}
}

func write(ctx context.Context, conn *websocket.Conn, typ string, symbols ...string) bool {
	data, err := json.Marshal(map[string]any{"type": typ, "symbols": symbols})
	if err != nil {
		return false
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, data) == nil
}

// ---------------------------------------------------------------- gateway metrics sampling

type sample struct {
	at         time.Time
	goroutines float64
	heapInuse  float64
	clients    float64
	symbols    float64
	evictions  float64
}

type sampler struct {
	url     string
	client  *http.Client
	samples []sample
}

func newSampler(url string) *sampler {
	return &sampler{url: url, client: &http.Client{Timeout: 5 * time.Second}}
}

func (s *sampler) scrape() (map[string]float64, error) {
	resp, err := s.client.Get(s.url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	values := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			continue
		}
		name := line[:i]
		if j := strings.IndexByte(name, '{'); j >= 0 {
			name = name[:j] // labelled series are summed per metric name
		}
		values[name] += v
	}
	return values, sc.Err()
}

func (s *sampler) sample() error {
	v, err := s.scrape()
	if err != nil {
		return err
	}
	s.samples = append(s.samples, sample{
		at: time.Now(), goroutines: v["go_goroutines"], heapInuse: v["go_memstats_heap_inuse_bytes"],
		clients: v["realtime_gateway_ws_clients"], symbols: v["realtime_gateway_active_symbols"],
		evictions: v["realtime_gateway_slow_consumer_evictions_total"],
	})
	return nil
}

// verify waits for the gateway to drain and checks for leaks against the pre-run baseline.
func (s *sampler) verify(o options) []string {
	baseline := s.samples[0]
	loaded := s.samples[1:] // samples taken while the clients were running
	var failures []string

	var final sample
	deadline := time.Now().Add(o.drainWait)
	for {
		if err := s.sample(); err != nil {
			return append(failures, "final metrics sample failed: "+err.Error())
		}
		final = s.samples[len(s.samples)-1]
		drained := final.clients == 0 && final.symbols <= baseline.symbols &&
			final.goroutines <= baseline.goroutines+o.goroutineSlack
		if drained || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}

	var peak sample
	for _, x := range s.samples {
		peak.goroutines = max(peak.goroutines, x.goroutines)
		peak.heapInuse = max(peak.heapInuse, x.heapInuse)
		peak.clients = max(peak.clients, x.clients)
		peak.symbols = max(peak.symbols, x.symbols)
	}
	fmt.Printf("gateway samples: %d; baseline goroutines %.0f, heap in use %.1f MiB\n", len(s.samples), baseline.goroutines, baseline.heapInuse/(1<<20))
	fmt.Printf("gateway peak: %.0f clients, %.0f active symbols, %.0f goroutines, heap in use %.1f MiB\n",
		peak.clients, peak.symbols, peak.goroutines, peak.heapInuse/(1<<20))
	fmt.Printf("gateway after drain: %.0f clients, %.0f active symbols, %.0f goroutines, heap in use %.1f MiB, evictions %.0f\n",
		final.clients, final.symbols, final.goroutines, final.heapInuse/(1<<20), final.evictions-baseline.evictions)

	if final.clients != 0 {
		failures = append(failures, fmt.Sprintf("%.0f sessions left after the run", final.clients))
	}
	// Symbols subscribed before the run may expire during it (unsubscribe grace), so only growth is a leak.
	if final.symbols > baseline.symbols {
		failures = append(failures, fmt.Sprintf("active symbols %.0f after the run, baseline %.0f", final.symbols, baseline.symbols))
	}
	if final.goroutines > baseline.goroutines+o.goroutineSlack {
		failures = append(failures, fmt.Sprintf("goroutines %.0f after the run, baseline %.0f", final.goroutines, baseline.goroutines))
	}
	// Heap plateau: compare the mean heap of the last third of the loaded samples with the middle third.
	if n := len(loaded); n >= 6 {
		mid, late := mean(loaded[n/3:2*n/3]), mean(loaded[2*n/3:])
		fmt.Printf("heap plateau: middle third %.1f MiB, last third %.1f MiB\n", mid/(1<<20), late/(1<<20))
		if late > mid*o.heapGrowth {
			failures = append(failures, fmt.Sprintf("heap kept growing: %.1f MiB -> %.1f MiB", mid/(1<<20), late/(1<<20)))
		}
	} else {
		fmt.Println("heap plateau: not evaluated (fewer than 6 samples under load; lengthen -duration)")
	}
	return failures
}

func mean(xs []sample) float64 {
	var sum float64
	for _, x := range xs {
		sum += x.heapInuse
	}
	return sum / float64(len(xs))
}
