// Package tests holds integration and contract-conformance tests: the real gateway on an httptest server,
// a real WebSocket client, and validation of every produced message against contracts/schemas.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/config"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/hotcache"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/httpapi"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/modes"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/registry"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/simulator"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/transport/ws"
)

const (
	schemaBaseID = "https://contracts.trading-terminal.invalid/schemas/"
	testSeed     = 20260927
	testTick     = 20 * time.Millisecond
)

// ---------------------------------------------------------------- contract schemas

type noFetch struct{}

func (noFetch) Load(url string) (any, error) {
	return nil, errors.New("schema retrieval disabled: " + url)
}

var schemaCompiler *jsonschema.Compiler

func compiler(t *testing.T) *jsonschema.Compiler {
	t.Helper()
	if schemaCompiler != nil {
		return schemaCompiler
	}
	dir := os.Getenv("CONTRACTS_DIR")
	if dir == "" {
		dir = "../../../contracts"
	}
	schemasDir := filepath.Join(dir, "schemas")
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.UseLoader(noFetch{})
	err := filepath.WalkDir(schemasDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".schema.json") {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		doc, err := jsonschema.UnmarshalJSON(f)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(schemasDir, path)
		return c.AddResource(schemaBaseID+filepath.ToSlash(rel), doc)
	})
	if err != nil {
		t.Fatalf("load contract schemas from %s: %v", schemasDir, err)
	}
	schemaCompiler = c
	return c
}

// conform fails the test unless data validates against the contract schema at rel (e.g. "stream/quote.schema.json").
func conform(t *testing.T, rel string, data []byte) {
	t.Helper()
	sch, err := compiler(t).Compile(schemaBaseID + rel)
	if err != nil {
		t.Fatalf("compile %s: %v", rel, err)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, data)
	}
	if err := sch.Validate(inst); err != nil {
		t.Fatalf("message does not conform to %s: %v\n%s", rel, err, data)
	}
}

// ---------------------------------------------------------------- gateway under test

type gateway struct {
	t       *testing.T
	ws      *ws.Server
	reg     *registry.Registry
	model   *simulator.Model
	sim     *simulator.SimulatorMarketDataSource // nil when a custom source is used
	metrics *metrics.Gateway
	health  *httpapi.Health
	public  *httptest.Server
	intern  *httptest.Server
	client  *http.Client
}

func defaultConfig() ws.Config {
	return ws.Config{
		AllowedOrigins:         []string{"localhost:3000"},
		MaxInboundMessageBytes: 4096,
		MaxSymbolsPerSubscribe: 50,
		MaxSubscribedSymbols:   100,
		HeartbeatInterval:      100 * time.Millisecond,
		WriteTimeout:           time.Second,
		ControlQueueSize:       256,
		FlushInterval:          5 * time.Millisecond,
		LagThreshold:           2 * time.Second,
		MaxLaggingFlushes:      5,
		MaxConnections:         100,
	}
}

// options customizes the gateway under test.
type options struct {
	ws         func(*ws.Config)
	registry   func(*registry.Config)
	source     marketdata.MarketDataSource // default: the simulator
	synthetic  int                         // synthetic simulator instruments
	store      hotcache.Store              // Redis stand-in; nil disables the hot cache
	quoteEvery time.Duration               // quote-cache write interval (default 50ms)
	mode       *config.Config              // select the source like cmd/realtime-gateway does (modes.Select)
}

// startGateway starts the gateway with an optional WebSocket config change and registers shutdown plus a
// goroutine leak check as test cleanup.
func startGateway(t *testing.T, mutate func(*ws.Config)) *gateway {
	t.Helper()
	return startGatewayWith(t, options{ws: mutate})
}

// startGatewayWith wires the gateway like cmd/realtime-gateway does, on httptest servers.
func startGatewayWith(t *testing.T, o options) *gateway {
	t.Helper()
	baseline := runtime.NumGoroutine()
	cfg := defaultConfig()
	if o.ws != nil {
		o.ws(&cfg)
	}
	rcfg := registry.Config{MaxActiveSymbols: 200, UnsubscribeGrace: 50 * time.Millisecond, StaleAfter: time.Second}
	if o.registry != nil {
		o.registry(&rcfg)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clk := clock.Real{}
	m := metrics.New()
	g := &gateway{t: t, metrics: m, client: &http.Client{Transport: &http.Transport{}, Timeout: 10 * time.Second}}

	source := o.source
	var closeSource func()
	if o.mode != nil {
		sel, err := modes.Select(*o.mode, clk, m, logger, o.store, nil)
		if err != nil {
			t.Fatal(err)
		}
		source = sel.Source
		rcfg.StaleAfter = sel.StaleAfter
		sel.Start()
		closeSource = func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := source.Close(ctx); err != nil {
				t.Errorf("source close: %v", err)
			}
		}
		if sim, ok := source.(*simulator.SimulatorMarketDataSource); ok {
			g.sim = sim
		}
	}
	if source == nil {
		g.model = simulator.NewModel(testSeed, simulator.WithSyntheticSymbols(o.synthetic))
		sim, err := simulator.NewSource(g.model, clk, testTick)
		if err != nil {
			t.Fatal(err)
		}
		g.sim, source = sim, sim
	}

	var (
		guard       *hotcache.Guard
		quoteWriter *hotcache.QuoteWriter
		hooks       registry.Hooks
	)
	hooks.OnState = func(s marketdata.SourceState, at time.Time) { g.ws.BroadcastState(s, at) }
	if o.store != nil {
		every := o.quoteEvery
		if every == 0 {
			every = 50 * time.Millisecond
		}
		guard = hotcache.NewGuard(clk, 200*time.Millisecond, time.Second, m, logger)
		quoteWriter = hotcache.NewQuoteWriter(source.ID(), o.store, guard, clk, every, 30*time.Second, m)
		hooks.OnQuote = func(u *registry.Update) { quoteWriter.Publish(u.Symbol, u.Data) }
	}
	reg, err := registry.New(source, clk, rcfg, m, logger, hooks)
	if err != nil {
		t.Fatal(err)
	}
	g.reg = reg
	g.ws = ws.NewServer(cfg, reg, clk, m, logger)
	reg.Start()
	if quoteWriter != nil {
		go quoteWriter.Run()
	}
	g.health = httpapi.NewHealth(reg, g.ws.Clients, m.Handler())
	// The compute timeout is generous because the race detector slows heavy computations considerably.
	bars := httpapi.NewBarsService(source, o.store, guard, httpapi.BarsConfig{MaxConcurrent: 4, ComputeTimeout: time.Minute, MaxCacheTTL: time.Minute}, m)

	mux := http.NewServeMux()
	mux.Handle("GET /ws", g.ws)
	mux.Handle("GET /api/v1/market/bars", httpapi.NewBarsHandler(source, bars, logger))
	g.public = httptest.NewServer(mux)
	g.intern = httptest.NewServer(g.health.Handler())
	if g.sim != nil && o.mode == nil {
		waitUntil(t, "source READY", func() bool { return reg.Ready() })
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := g.ws.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		reg.Close()
		if closeSource != nil {
			closeSource()
		}
		if quoteWriter != nil {
			if err := quoteWriter.Close(ctx); err != nil {
				t.Errorf("quote writer: %v", err)
			}
		}
		g.public.Close()
		g.intern.Close()
		g.client.CloseIdleConnections()
		assertNoGoroutineLeaks(t, baseline)
	})
	return g
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (g *gateway) wsURL() string { return "ws" + strings.TrimPrefix(g.public.URL, "http") + "/ws" }

// dial opens a WebSocket connection; origin may be empty (non-browser client).
func (g *gateway) dial(origin string) (*websocket.Conn, *http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := &websocket.DialOptions{HTTPClient: g.client}
	if origin != "" {
		opts.HTTPHeader = http.Header{"Origin": []string{origin}}
	}
	conn, resp, err := websocket.Dial(ctx, g.wsURL(), opts)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	return conn, resp, err
}

func (g *gateway) mustDial() *websocket.Conn {
	g.t.Helper()
	conn, _, err := g.dial("")
	if err != nil {
		g.t.Fatalf("dial: %v", err)
	}
	conn.SetReadLimit(1 << 20)
	g.t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

// serverMessage is the union of fields the tests inspect.
type serverMessage struct {
	Type      string   `json:"type"`
	Symbol    string   `json:"symbol"`
	State     string   `json:"state"`
	Source    string   `json:"source"`
	Code      string   `json:"code"`
	Symbols   []string `json:"symbols"`
	Last      *string  `json:"last"`
	Stale     bool     `json:"stale"`
	Reason    string   `json:"reason"`
	DataMode  string   `json:"dataMode"`
	Halted    *bool    `json:"halted"`
	Sequence  int64    `json:"sequence"`
	Timestamp string   `json:"timestamp"`
	Limits    struct {
		MaxSymbolsPerSubscribe int   `json:"maxSymbolsPerSubscribe"`
		MaxSubscribedSymbols   int   `json:"maxSubscribedSymbols"`
		MaxInboundMessageBytes int64 `json:"maxInboundMessageBytes"`
		HeartbeatIntervalMs    int64 `json:"heartbeatIntervalMs"`
	} `json:"limits"`
}

// read returns the next message; every message is validated against the ServerMessage contract.
func read(t *testing.T, conn *websocket.Conn, timeout time.Duration) (serverMessage, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		return serverMessage{}, err
	}
	conform(t, "stream/server-message.schema.json", data)
	var m serverMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return m, nil
}

func mustRead(t *testing.T, conn *websocket.Conn) serverMessage {
	t.Helper()
	m, err := read(t, conn, 5*time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return m
}

func send(t *testing.T, conn *websocket.Conn, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	sendRaw(t, conn, websocket.MessageText, data)
}

func sendRaw(t *testing.T, conn *websocket.Conn, typ websocket.MessageType, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, typ, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// ---------------------------------------------------------------- goroutine leak detection

// assertNoGoroutineLeaks waits for the goroutine count to return to the baseline and checks the runtime's
// goroutine leak profile (Go 1.27). The count check also catches goroutines that are still running (for
// example an uncancelled ticker loop), which the leak profile cannot see.
func assertNoGoroutineLeaks(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			var stacks bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
			t.Fatalf("goroutine leak: %d goroutines, baseline %d\n%s", runtime.NumGoroutine(), baseline, stacks.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := leakedGoroutines(t); n > 0 {
		t.Fatalf("goroutine leak profile reports %d leaked goroutines", n)
	}
}

func leakedGoroutines(t *testing.T) int {
	t.Helper()
	profile := pprof.Lookup("goroutineleak")
	if profile == nil {
		t.Fatal("goroutineleak profile is not available")
	}
	var buf bytes.Buffer
	if err := profile.WriteTo(&buf, 1); err != nil {
		t.Fatalf("goroutineleak profile: %v", err)
	}
	var total int
	if _, err := fmt.Sscanf(buf.String(), "goroutineleak profile: total %d", &total); err != nil {
		t.Fatalf("unexpected goroutineleak profile format: %q", firstLine(buf.String()))
	}
	return total
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
