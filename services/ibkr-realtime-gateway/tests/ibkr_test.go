package tests

import (
	"crypto/tls"
	"io"
	stdlog "log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/config"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/ibkr/fakecpgw"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

// fakeGateway serves the fake CP Gateway over TLS for 127.0.0.1 and returns it with its base URL and CA file.
func fakeGateway(t *testing.T) (*fakecpgw.Server, string, string) {
	t.Helper()
	pki, err := fakecpgw.NewPKI("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	fake := fakecpgw.New()
	ts := httptest.NewUnstartedServer(fake.Handler())
	ts.Config.ErrorLog = stdlog.New(io.Discard, "", 0)
	ts.TLS = &tls.Config{Certificates: []tls.Certificate{pki.Server}, MinVersion: tls.VersionTLS12}
	ts.StartTLS()
	t.Cleanup(func() {
		fake.DropWebSockets()
		ts.Close()
	})
	return fake, ts.URL + "/v1/api", writeCA(t, pki.CAPEM)
}

func writeCA(t *testing.T, pem []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// closedPort returns an https base URL on which nothing listens.
func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return "https://" + addr + "/v1/api"
}

// modeConfig is a validated-looking configuration for modes.Select with test-friendly IBKR timings.
func modeConfig(mode, baseURL, caFile string) *config.Config {
	return &config.Config{
		MarketDataMode: mode, TrustedEnvironment: true,
		Seed: testSeed, TickInterval: testTick, StaleAfter: time.Second,
		IBKR: config.IBKR{
			BaseURL: baseURL, CAFile: caFile,
			SessionLimit: 10, Allocation: 5, Headroom: 1, LimiterQueue: 16, LimiterTimeout: time.Second,
			PenaltyCooldown: 500 * time.Millisecond, RequestTimeout: 2 * time.Second,
			TickleInterval: 50 * time.Millisecond, PingInterval: 50 * time.Millisecond,
			RenewAfter: 5 * time.Second, RenewJitter: 100 * time.Millisecond,
			ReconnectBase: 10 * time.Millisecond, ReconnectMax: 50 * time.Millisecond, ReconnectTries: 5, ReconnectTotal: 2 * time.Second,
			ProbeInterval: 100 * time.Millisecond, SnapshotWait: 2 * time.Second, WSSendRate: 50,
			ConidCacheTTL: time.Hour, StaleAfter: 2 * time.Second, MarketDataLines: 100, AutoProbeTimeout: 2 * time.Second,
		},
	}
}

// ------------------------------------------------------------------------------------------ IBKR path end to end

func TestIBKRStreamAndResubscriptionAfterReconnect(t *testing.T) {
	fake, base, ca := fakeGateway(t)
	g := startGatewayWith(t, options{mode: modeConfig(config.ModeIBKR, base, ca)})
	waitUntil(t, "IBKR READY", g.reg.Ready)

	conn := g.mustDial()
	if m := mustRead(t, conn); m.Source != "IBKR" || m.State != "READY" {
		t.Fatalf("connection: %+v", m)
	}
	send(t, conn, subscribeMsg("MSFT", "UNSUB"))
	snap := readUntil(t, conn, "MSFT snapshot", func(m serverMessage) bool { return m.Type == "snapshot" && m.Symbol == "MSFT" })
	if snap.Last == nil || *snap.Last != "412.1250" || snap.DataMode != "REALTIME" {
		t.Fatalf("snapshot must keep the received precision and carry the data mode: %+v", snap)
	}
	unavailable := readUntil(t, conn, "UNSUB rejected", func(m serverMessage) bool { return m.Type == "error" })
	if unavailable.Code != "SOURCE_UNAVAILABLE" || len(unavailable.Symbols) != 1 || unavailable.Symbols[0] != "UNSUB" {
		t.Fatalf("a symbol without market-data subscription must be rejected: %+v", unavailable)
	}
	fake.Push(fakecpgw.ConidMSFT, map[string]string{"31": "H412.50"})
	halted := readUntil(t, conn, "halted quote", func(m serverMessage) bool { return m.Type == "quote" && m.Symbol == "MSFT" })
	if halted.Halted == nil || !*halted.Halted || *halted.Last != "412.50" {
		t.Fatalf("halt: %+v", halted)
	}

	// Lose the IBKR websocket: RECONNECTING and stale, then recovery re-sends smd and quotes flow again.
	smdBefore := fake.CountTopics("smd+272093+")
	fake.DropWebSockets()
	readUntil(t, conn, "RECONNECTING", func(m serverMessage) bool { return m.Type == "connection" && m.State == "RECONNECTING" })
	readUntil(t, conn, "stale", func(m serverMessage) bool { return m.Type == "stale" && m.Reason == "SOURCE_DISCONNECTED" })
	readUntil(t, conn, "READY", func(m serverMessage) bool { return m.Type == "connection" && m.State == "READY" })
	waitUntil(t, "resubscription", func() bool { return fake.CountTopics("smd+272093+") > smdBefore })
	fake.Push(fakecpgw.ConidMSFT, map[string]string{"31": "413.00"})
	fresh := readUntil(t, conn, "fresh quote", func(m serverMessage) bool {
		return m.Type == "quote" && m.Symbol == "MSFT" && m.Last != nil && *m.Last == "413.00"
	})
	if fresh.Stale {
		t.Fatal("a fresh quote after recovery must not be stale")
	}
}

func TestIBKRBarsCachedAndRateLimited(t *testing.T) {
	fake, base, ca := fakeGateway(t)
	store := newMemStore()
	g := startGatewayWith(t, options{mode: modeConfig(config.ModeIBKR, base, ca), store: store})
	waitUntil(t, "IBKR READY", g.reg.Ready)

	resp, body := g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NVDA&interval=1h&range=5d", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"source":"IBKR"`) || !strings.Contains(string(body), `"open":"173.40"`) {
		t.Fatalf("bars: %d %s", resp.StatusCode, body)
	}
	conform(t, "market/bars-response.schema.json", body)
	if _, ok := store.entry("bars:IBKR:NVDA:1h:5d"); !ok {
		t.Fatal("bars must be cached under the IBKR namespace")
	}
	if resp, _ := g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NVDA&interval=1m&range=5d", nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("combination IBKR cannot serve: %d, want 400", resp.StatusCode)
	}

	fake.FailNext("/tickle", 429, 1)
	waitUntil(t, "cool-down", func() bool { return g.reg.State().State == marketdata.StateDegraded })
	resp, body = g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NVDA&interval=1d&range=1mo", nil)
	if resp.StatusCode != http.StatusTooManyRequests || !strings.Contains(string(body), `"category":"RATE_LIMITED"`) {
		t.Fatalf("bars during the IBKR cool-down: %d %s", resp.StatusCode, body)
	}
	conform(t, "common/problem.schema.json", body)
}

// ------------------------------------------------------------------------------------------ modes

func TestAutoModeSelectsIBKRWhenASessionIsEstablished(t *testing.T) {
	_, base, ca := fakeGateway(t)
	g := startGatewayWith(t, options{mode: modeConfig(config.ModeAuto, base, ca)})
	waitUntil(t, "READY", g.reg.Ready)
	assertActiveSource(t, g, "IBKR", "AUTO")
}

func TestAutoModeFallsBackToMockVisibly(t *testing.T) {
	g := startGatewayWith(t, options{mode: modeConfig(config.ModeAuto, closedPort(t), writeCA(t, mustPKI(t).CAPEM))})
	waitUntil(t, "READY", g.reg.Ready)
	assertActiveSource(t, g, "MOCK", "AUTO")
	if g.sim == nil {
		t.Fatal("the fallback must be the live deterministic simulator")
	}
}

func TestIBKRModeNeverFallsBackToMock(t *testing.T) {
	g := startGatewayWith(t, options{mode: modeConfig(config.ModeIBKR, closedPort(t), writeCA(t, mustPKI(t).CAPEM))})
	// The source also starts out DISCONNECTED, so wait until a reconnect cycle ran before checking the final state.
	waitUntil(t, "DISCONNECTED after the bounded cycle", func() bool {
		return testutil.ToFloat64(g.metrics.ReconnectTotal) > 0 && g.reg.State().State == marketdata.StateDisconnected
	})
	if g.reg.SourceID() != marketdata.SourceIBKR || g.sim != nil {
		t.Fatal("IBKR mode must keep the IBKR source")
	}
	if g.reg.Ready() {
		t.Fatal("not ready without IBKR")
	}
	resp, body := g.get(t, g.intern.URL, "/health", nil)
	conform(t, "ops/health-detail.schema.json", body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"source":"IBKR"`) || !strings.Contains(string(body), `"status":"DOWN"`) {
		t.Fatalf("health: %s", body)
	}
	if got := testutil.ToFloat64(g.metrics.SourceInfo.WithLabelValues("MOCK", "IBKR")); got != 0 {
		t.Fatal("MOCK must never be used in IBKR mode")
	}
}

func mustPKI(t *testing.T) *fakecpgw.PKI {
	t.Helper()
	pki, err := fakecpgw.NewPKI("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	return pki
}

// assertActiveSource checks that the source is exposed everywhere: connection message, bars, /health, metrics.
func assertActiveSource(t *testing.T, g *gateway, source, mode string) {
	t.Helper()
	conn := g.mustDial()
	if m := mustRead(t, conn); m.Source != source {
		t.Fatalf("connection source = %q, want %q", m.Source, source)
	}
	_, body := g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NVDA&interval=1d&range=1mo", nil)
	if !strings.Contains(string(body), `"source":"`+source+`"`) {
		t.Fatalf("bars source: %s", body)
	}
	_, body = g.get(t, g.intern.URL, "/health", nil)
	conform(t, "ops/health-detail.schema.json", body)
	if !strings.Contains(string(body), `"source":"`+source+`"`) {
		t.Fatalf("health source: %s", body)
	}
	if got := testutil.ToFloat64(g.metrics.SourceInfo.WithLabelValues(source, mode)); got != 1 {
		t.Fatalf("realtime_gateway_source_info{source=%q,mode=%q} = %v", source, mode, got)
	}
}
