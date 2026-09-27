package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/hotcache"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/registry"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/testsource"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/transport/ws"
)

// memStore is an in-memory Redis stand-in that can be made to fail.
type memStore struct {
	mu   sync.Mutex
	data map[string]hotcache.Entry
	fail bool
}

func newMemStore() *memStore { return &memStore{data: map[string]hotcache.Entry{}} }

func (s *memStore) SetMany(_ context.Context, entries []hotcache.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("dial tcp: connection refused")
	}
	for _, e := range entries {
		s.data[e.Key] = hotcache.Entry{Key: e.Key, Value: append([]byte(nil), e.Value...), TTL: e.TTL}
	}
	return nil
}

func (s *memStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return nil, errors.New("dial tcp: connection refused")
	}
	e, ok := s.data[key]
	if !ok {
		return nil, hotcache.ErrMiss
	}
	return e.Value, nil
}

func (s *memStore) Close() error { return nil }

func (s *memStore) entry(key string) (hotcache.Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	return e, ok
}

// readUntil reads messages until match returns true.
func readUntil(t *testing.T, conn *websocket.Conn, what string, match func(serverMessage) bool) serverMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m, err := read(t, conn, time.Until(deadline))
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		if match(m) {
			return m
		}
	}
	t.Fatalf("timed out waiting for %s", what)
	return serverMessage{}
}

func TestManyClientsShareOneUpstreamSubscriptionPerSymbol(t *testing.T) {
	g := startGateway(t, nil)
	const clients = 20
	conns := make([]*websocket.Conn, clients)
	for i := range conns {
		conns[i] = g.mustDial()
		mustRead(t, conns[i])
		send(t, conns[i], subscribeMsg("NVDA", "AAPL"))
	}
	for _, c := range conns {
		readUntil(t, c, "a quote", func(m serverMessage) bool { return m.Type == "quote" })
	}
	if n := g.sim.ActiveSubscriptions(); n != 2 {
		t.Fatalf("upstream subscriptions = %d, want 2 for %d clients", n, clients)
	}
	if got := testutil.ToFloat64(g.metrics.WSClients); got != clients {
		t.Fatalf("ws_clients = %v", got)
	}
	for _, c := range conns {
		_ = c.Close(websocket.StatusNormalClosure, "")
	}
	waitUntil(t, "upstream subscriptions to close after the grace period", func() bool {
		return g.sim.ActiveSubscriptions() == 0 && g.reg.ActiveSymbols() == 0 && g.ws.Clients() == 0
	})
	if got := testutil.ToFloat64(g.metrics.ActiveSymbols); got != 0 {
		t.Fatalf("active_symbols = %v", got)
	}
}

func TestStalledReaderIsEvictedWhileOthersContinue(t *testing.T) {
	g := startGateway(t, func(c *ws.Config) { c.WriteTimeout = 200 * time.Millisecond })
	stalled := g.mustDial() // never reads, so it cannot answer pings
	send(t, stalled, subscribeMsg("NVDA", "AAPL", "META"))
	healthy := g.mustDial()
	mustRead(t, healthy)
	send(t, healthy, subscribeMsg("NVDA", "AAPL", "META"))

	// The healthy client keeps reading (and so answers pings) while the stalled one is evicted.
	readUntil(t, healthy, "stalled client eviction", func(serverMessage) bool {
		return testutil.ToFloat64(g.metrics.Evictions.WithLabelValues(metrics.EvictPongTimeout)) == 1
	})
	readUntil(t, healthy, "evicted session to end", func(serverMessage) bool { return g.ws.Clients() == 1 })
	for range 20 { // the healthy client keeps streaming
		readUntil(t, healthy, "quote after eviction", func(m serverMessage) bool { return m.Type == "quote" })
	}
	if n := g.sim.ActiveSubscriptions(); n != 3 {
		t.Fatalf("upstream subscriptions = %d; the healthy client's symbols must stay subscribed", n)
	}
}

func TestSourceOutageReportsStateAndStaleThenRecovers(t *testing.T) {
	src := testsource.New(clock.Real{}, "NVDA", "AAPL")
	g := startGatewayWith(t, options{source: src})
	src.Emit(marketdata.StateConnecting, "")
	src.Emit(marketdata.StateReady, "")
	waitUntil(t, "READY", g.reg.Ready)

	conn := g.mustDial()
	if m := mustRead(t, conn); m.State != "READY" {
		t.Fatalf("connection: %+v", m)
	}
	send(t, conn, subscribeMsg("NVDA", "AAPL"))
	readUntil(t, conn, "snapshots", func(m serverMessage) bool { return m.Type == "snapshot" && m.Symbol == "AAPL" })
	src.Publish("NVDA")
	readUntil(t, conn, "quote", func(m serverMessage) bool { return m.Type == "quote" && !m.Stale })

	src.Break()
	src.Emit(marketdata.StateReconnecting, "CONNECTION_LOST")
	if m := mustRead(t, conn); m.Type != "connection" || m.State != "RECONNECTING" {
		t.Fatalf("expected the state change first, got %+v", m)
	}
	stale := mustRead(t, conn)
	if stale.Type != "stale" || stale.Reason != "SOURCE_DISCONNECTED" || len(stale.Symbols) != 2 {
		t.Fatalf("stale: %+v", stale)
	}
	resp, body := g.get(t, g.intern.URL, "/health", nil)
	conform(t, "ops/health-detail.schema.json", body)
	if !strings.Contains(string(body), `"connectionState":"RECONNECTING"`) || !strings.Contains(string(body), `"lastErrorCategory":"CONNECTION_LOST"`) {
		t.Fatalf("health during outage: %d %s", resp.StatusCode, body)
	}
	if resp, _ := g.get(t, g.intern.URL, "/readiness", nil); resp.StatusCode != http.StatusOK {
		t.Fatal("readiness stays up during a bounded reconnect cycle")
	}
	// A new client during the outage sees the state and a stale snapshot.
	other := g.mustDial()
	if m := mustRead(t, other); m.State != "RECONNECTING" {
		t.Fatalf("new client state: %+v", m)
	}
	send(t, other, subscribeMsg("NVDA"))
	if m := readUntil(t, other, "snapshot", func(m serverMessage) bool { return m.Type == "snapshot" }); !m.Stale {
		t.Fatal("snapshot during an outage must be stale")
	}

	src.Restore()
	src.Emit(marketdata.StateConnecting, "")
	src.Emit(marketdata.StateReady, "")
	readUntil(t, conn, "READY", func(m serverMessage) bool { return m.Type == "connection" && m.State == "READY" })
	waitUntil(t, "resubscription", func() bool { return src.ActiveSubscriptions() == 2 })
	src.Publish("NVDA")
	readUntil(t, conn, "fresh quote", func(m serverMessage) bool { return m.Type == "quote" && m.Symbol == "NVDA" && !m.Stale })

	src.Emit(marketdata.StateReconnecting, "CONNECTION_LOST")
	src.Emit(marketdata.StateDisconnected, "RECONNECT_EXHAUSTED")
	waitUntil(t, "not ready", func() bool { return !g.reg.Ready() })
	if resp, _ := g.get(t, g.intern.URL, "/readiness", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("readiness must fail once the source gives up (DISCONNECTED)")
	}
	_, body = g.get(t, g.intern.URL, "/health", nil)
	conform(t, "ops/health-detail.schema.json", body)
}

func TestSymbolsWithoutUpdatesAreReportedStale(t *testing.T) {
	src := testsource.New(clock.Real{}, "NVDA")
	g := startGatewayWith(t, options{source: src, registry: func(c *registry.Config) { c.StaleAfter = 150 * time.Millisecond }})
	src.Emit(marketdata.StateConnecting, "")
	src.Emit(marketdata.StateReady, "")
	waitUntil(t, "READY", g.reg.Ready)
	conn := g.mustDial()
	mustRead(t, conn)
	send(t, conn, subscribeMsg("NVDA"))
	m := readUntil(t, conn, "stale", func(m serverMessage) bool { return m.Type == "stale" })
	if m.Reason != "NO_UPDATES" || len(m.Symbols) != 1 || m.Symbols[0] != "NVDA" {
		t.Fatalf("stale: %+v", m)
	}
	src.Publish("NVDA")
	readUntil(t, conn, "fresh quote", func(m serverMessage) bool { return m.Type == "quote" && !m.Stale })
}

func TestHotCacheStoresTheLatestQuoteInContractShape(t *testing.T) {
	store := newMemStore()
	g := startGatewayWith(t, options{store: store})
	conn := g.mustDial()
	mustRead(t, conn)
	send(t, conn, subscribeMsg("NVDA"))
	readUntil(t, conn, "quote", func(m serverMessage) bool { return m.Type == "quote" })
	waitUntil(t, "quote:MOCK:NVDA", func() bool { _, ok := store.entry(hotcache.QuoteKey(marketdata.SourceMock, "NVDA")); return ok })
	e, _ := store.entry(hotcache.QuoteKey(marketdata.SourceMock, "NVDA"))
	conform(t, "stream/quote.schema.json", e.Value)
	if e.TTL != 30*time.Second {
		t.Fatalf("TTL = %s", e.TTL)
	}
	if testutil.ToFloat64(g.metrics.QuoteCacheWrites) == 0 {
		t.Fatal("quote_cache_writes_total")
	}
}

func TestBarsAreCachedAndMarked(t *testing.T) {
	store := newMemStore()
	g := startGatewayWith(t, options{store: store})
	path := "/api/v1/market/bars?symbol=AAPL&interval=1h&range=5d"
	var bodies [2][]byte
	for i := range bodies {
		resp, body := g.get(t, g.public.URL, path, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
		conform(t, "market/bars-response.schema.json", body)
		bodies[i] = body
	}
	var first, second struct {
		Cached bool              `json:"cached"`
		Bars   []json.RawMessage `json:"bars"`
	}
	_ = json.Unmarshal(bodies[0], &first)
	_ = json.Unmarshal(bodies[1], &second)
	if first.Cached || !second.Cached || len(first.Bars) != len(second.Bars) || len(first.Bars) == 0 {
		t.Fatalf("cached flags %v/%v, bars %d/%d", first.Cached, second.Cached, len(first.Bars), len(second.Bars))
	}
	if e, ok := store.entry("bars:MOCK:AAPL:1h:5d"); !ok || e.TTL != time.Minute {
		t.Fatalf("cache entry: %+v", e)
	}
}

// blockingBars delays bar computations until released, to saturate the computation limit.
type blockingBars struct {
	*testsource.Source
	release chan struct{}
	started atomic.Int64
}

func (b *blockingBars) Bars(ctx context.Context, s string, i marketdata.Interval, r marketdata.Range) ([]marketdata.Bar, error) {
	b.started.Add(1)
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.Source.Bars(ctx, s, i, r)
}

func TestSaturatedBarComputationsAreRateLimited(t *testing.T) {
	src := &blockingBars{Source: testsource.New(clock.Real{}, "NVDA"), release: make(chan struct{})}
	g := startGatewayWith(t, options{source: src})
	var wg sync.WaitGroup
	combos := []string{"1m&range=1d", "5m&range=1d", "15m&range=5d", "1h&range=5d"} // 4 = the test's computation limit
	for _, c := range combos {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, body := g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NVDA&interval="+c, nil)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("held request: %d %s", resp.StatusCode, body)
			}
		}()
	}
	waitUntil(t, "computations to start", func() bool { return src.started.Load() == 4 })
	resp, body := g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NVDA&interval=1d&range=1mo", nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429: %s", resp.StatusCode, body)
	}
	conform(t, "common/problem.schema.json", body)
	if !strings.Contains(string(body), `"category":"RATE_LIMITED"`) {
		t.Fatalf("problem: %s", body)
	}
	close(src.release)
	wg.Wait()
}

func TestRedisOutageDoesNotAffectStreamingOrBars(t *testing.T) {
	store := newMemStore()
	store.fail = true
	g := startGatewayWith(t, options{store: store})
	conn := g.mustDial()
	mustRead(t, conn)
	send(t, conn, subscribeMsg("NVDA"))
	for got := 0; got < 10; {
		if mustRead(t, conn).Type == "quote" {
			got++
		}
	}
	resp, body := g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NVDA&interval=1h&range=5d", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"cached":false`) {
		t.Fatalf("bars during a Redis outage: %d %s", resp.StatusCode, body)
	}
	if resp, _ := g.get(t, g.intern.URL, "/readiness", nil); resp.StatusCode != http.StatusOK {
		t.Fatal("readiness must not depend on Redis")
	}
	waitUntil(t, "redis error metric", func() bool {
		return testutil.ToFloat64(g.metrics.RedisErrors.WithLabelValues("quote_write"))+
			testutil.ToFloat64(g.metrics.RedisErrors.WithLabelValues("bars_get")) > 0
	})
}

func TestMetricsEndpoint(t *testing.T) {
	g := startGateway(t, nil)
	conn := g.mustDial()
	mustRead(t, conn)
	send(t, conn, subscribeMsg("NVDA"))
	readUntil(t, conn, "quote", func(m serverMessage) bool { return m.Type == "quote" })
	resp, body := g.get(t, g.intern.URL, "/metrics", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	text := string(body)
	for _, want := range []string{
		`realtime_gateway_connection_state{state="READY"} 1`,
		`realtime_gateway_ws_clients 1`,
		`realtime_gateway_active_symbols 1`,
		`realtime_gateway_quotes_received_total`,
		`realtime_gateway_quotes_forwarded_total`,
		`realtime_gateway_message_processing_seconds_bucket`,
		`realtime_gateway_slow_consumer_evictions_total{reason="write_timeout"} 0`,
		`go_goroutines`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// TestShortSoakWithSubscriptionChurn runs many clients with random subscription churn over synthetic symbols
// and checks that sessions, upstream subscriptions and goroutines all return to zero afterwards.
func TestShortSoakWithSubscriptionChurn(t *testing.T) {
	g := startGatewayWith(t, options{synthetic: 100})
	const clients = 25
	duration := 3 * time.Second
	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(c), 7)) //nolint:gosec // reproducible churn pattern, not security
			conn, _, err := g.dial("")
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer func() { _ = conn.CloseNow() }()
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			go func() { // drain: a well-behaved client keeps reading
				for {
					if _, _, err := conn.Read(ctx); err != nil {
						return
					}
				}
			}()
			for ctx.Err() == nil {
				syms := make([]string, 1+rng.IntN(10))
				for i := range syms {
					syms[i] = fmt.Sprintf("SYN%03d", 1+rng.IntN(100))
				}
				typ := "subscribe"
				if rng.IntN(3) == 0 {
					typ = "unsubscribe"
				}
				data, _ := json.Marshal(map[string]any{"type": typ, "symbols": dedupe(syms)})
				if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
					return
				}
				time.Sleep(time.Duration(5+rng.IntN(20)) * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	waitUntil(t, "sessions and upstream subscriptions to drain", func() bool {
		return g.ws.Clients() == 0 && g.reg.ActiveSymbols() == 0 && g.sim.ActiveSubscriptions() == 0
	})
	if testutil.ToFloat64(g.metrics.QuotesForwarded) == 0 {
		t.Fatal("no quotes forwarded during the soak")
	}
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := in[:0]
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
