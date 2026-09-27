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
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/httpapi"
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
	t      *testing.T
	ws     *ws.Server
	model  *simulator.Model
	health *httpapi.Health
	public *httptest.Server
	intern *httptest.Server
	client *http.Client
}

func defaultConfig() ws.Config {
	return ws.Config{
		AllowedOrigins:         []string{"localhost:3000"},
		MaxInboundMessageBytes: 4096,
		MaxSymbolsPerSubscribe: 50,
		MaxSubscribedSymbols:   100,
		HeartbeatInterval:      100 * time.Millisecond,
		WriteTimeout:           time.Second,
		SendQueueSize:          256,
		MaxConnections:         100,
	}
}

// startGateway starts the gateway with an optional config change and registers shutdown plus a goroutine
// leak check as test cleanup.
func startGateway(t *testing.T, mutate func(*ws.Config)) *gateway {
	t.Helper()
	baseline := runtime.NumGoroutine()
	cfg := defaultConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	model := simulator.NewModel(testSeed)
	source, err := simulator.NewSource(model, clock.Real{}, testTick)
	if err != nil {
		t.Fatal(err)
	}
	srv := ws.NewServer(cfg, source, clock.Real{}, logger)
	health := httpapi.NewHealth(source, srv.Clients, clock.Real{})

	mux := http.NewServeMux()
	mux.Handle("GET /ws", srv)
	mux.Handle("GET /api/v1/market/bars", httpapi.NewBarsHandler(source, logger))
	g := &gateway{
		t: t, ws: srv, model: model, health: health,
		public: httptest.NewServer(mux),
		intern: httptest.NewServer(health.Handler()),
		client: &http.Client{Transport: &http.Transport{}, Timeout: 10 * time.Second},
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		g.public.Close()
		g.intern.Close()
		g.client.CloseIdleConnections()
		assertNoGoroutineLeaks(t, baseline)
	})
	return g
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
