package ibkr

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	stdlog "log"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/ibkr/fakecpgw"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/ibkr/pacing"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/reconnect"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// ------------------------------------------------------------------------------------------ helpers

type harness struct {
	t      *testing.T
	fake   *fakecpgw.Server
	src    *Source
	m      *metrics.Gateway
	mu     sync.Mutex
	events []marketdata.StatusEvent
}

// startFake serves the fake gateway over TLS with a certificate for the given hosts.
func startFake(t *testing.T, hosts ...string) (*fakecpgw.Server, string, *fakecpgw.PKI) {
	t.Helper()
	pki, err := fakecpgw.NewPKI(hosts...)
	if err != nil {
		t.Fatal(err)
	}
	fake := fakecpgw.New()
	ts := httptest.NewUnstartedServer(fake.Handler())
	ts.Config.ErrorLog = stdlog.New(io.Discard, "", 0) // expected handshake failures in the TLS tests
	ts.TLS = &tls.Config{Certificates: []tls.Certificate{pki.Server}, MinVersion: tls.VersionTLS12}
	ts.StartTLS()
	t.Cleanup(func() {
		fake.DropWebSockets()
		ts.Close()
	})
	return fake, ts.URL + "/v1/api", pki
}

// testConfig uses short timings. The keepalive stays above /tickle's own limit of 1 request/second: a faster one
// would queue on the limiter inside the session loop (blocking renewals) and could time out twice in a row,
// ending the session.
func testConfig(baseURL string, ca []byte) Config {
	return Config{
		Client:         ClientConfig{BaseURL: baseURL, CAPEM: ca, Timeout: 2 * time.Second, UserAgent: "test"},
		Pacing:         pacing.Config{SessionLimit: 10, Allocation: 5, Headroom: 1, MaxWaiters: 16, AcquireTimeout: time.Second, Cooldown: 300 * time.Millisecond},
		TickleInterval: 1200 * time.Millisecond, PingInterval: 50 * time.Millisecond,
		RenewAfter: 300 * time.Millisecond, RenewJitter: 20 * time.Millisecond,
		Reconnect:     reconnect.Policy{Base: 10 * time.Millisecond, Max: 20 * time.Millisecond, MaxAttempts: 2, MaxTotal: time.Second},
		ProbeInterval: time.Hour, SnapshotWait: time.Second, SnapshotLinger: 50 * time.Millisecond,
		InitWait: 10 * time.Millisecond, AuthWait: time.Second, WSSendRate: 100,
		CacheTTL: time.Hour, NegativeTTL: time.Hour,
	}
}

func newHarness(t *testing.T, fake *fakecpgw.Server, cfg Config) *harness {
	t.Helper()
	h := &harness{t: t, fake: fake, m: metrics.New()}
	src, err := New(cfg, nil, nil, clock.Real{}, h.m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	h.src = src
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case ev := <-src.Status():
				h.mu.Lock()
				h.events = append(h.events, ev)
				h.mu.Unlock()
			case <-src.done:
				return
			}
		}
	}()
	src.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := src.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
		<-done
	})
	return h
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *harness) states() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.events))
	for i, ev := range h.events {
		out[i] = string(ev.State)
		if ev.ErrorCategory != "" {
			out[i] += ":" + ev.ErrorCategory
		}
	}
	return out
}

func (h *harness) waitState(state string) {
	h.t.Helper()
	waitFor(h.t, "state "+state, func() bool {
		s := h.states()
		return len(s) > 0 && s[len(s)-1] == state
	})
}

type sink struct {
	mu          sync.Mutex
	quotes      []marketdata.Quote
	unavailable []error
}

func (s *sink) OfferQuote(q marketdata.Quote) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quotes = append(s.quotes, q)
	return true
}

func (s *sink) SymbolUnavailable(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unavailable = append(s.unavailable, err)
}

func (s *sink) last() (marketdata.Quote, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.quotes) == 0 {
		return marketdata.Quote{}, false
	}
	return s.quotes[len(s.quotes)-1], true
}

func str(d *marketdata.Decimal) string {
	if d == nil {
		return "<nil>"
	}
	return d.String()
}

// ------------------------------------------------------------------------------------------ 1. contracts

func TestContractResolution(t *testing.T) {
	fake, base, pki := startFake(t, "127.0.0.1")
	h := newHarness(t, fake, testConfig(base, pki.CAPEM))

	inst, err := h.src.Instrument("AAPL")
	if err != nil || inst.Currency != "USD" || inst.Name != "APPLE INC" {
		t.Fatalf("AAPL: %+v, %v", inst, err)
	}
	if _, err := h.src.Instrument("AAPL"); err != nil {
		t.Fatal(err)
	}
	if n := fake.Requests("/trsrv/stocks"); n != 1 {
		t.Fatalf("stock lookups = %d, want 1 (the second resolution is cached)", n)
	}
	// Only the US listing is a candidate; the MXN listing of the same company is ignored.
	c, _ := h.src.resolver.resolve(context.Background(), "AAPL")
	if c.Conid != fakecpgw.ConidAAPL {
		t.Fatalf("conid = %d", c.Conid)
	}
	if _, err := h.src.Instrument("AMBIG"); !errors.Is(err, marketdata.ErrUnknownSymbol) {
		t.Fatalf("ambiguous symbol must be rejected, got %v", err)
	}
	if _, err := h.src.Instrument("ZZZZ"); !errors.Is(err, marketdata.ErrUnknownSymbol) {
		t.Fatalf("unknown symbol: %v", err)
	}
	before := fake.Requests("/trsrv/stocks")
	_, _ = h.src.Instrument("ZZZZ")
	if fake.Requests("/trsrv/stocks") != before {
		t.Fatal("an unknown symbol must be remembered, not looked up again")
	}
}

// ------------------------------------------------------------------------------------------ 2. mapping

func frame(t *testing.T, fields map[string]any) map[string]json.RawMessage {
	t.Helper()
	out := map[string]json.RawMessage{}
	for k, v := range fields {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		out[k] = b
	}
	return out
}

func TestFrameMapping(t *testing.T) {
	now := time.Date(2026, 9, 28, 14, 0, 0, 0, time.UTC)
	st := &quoteState{symbol: "MSFT"}

	// No availability yet: nothing to emit.
	if res := st.merge(frame(t, map[string]any{"31": "412.1250"}), now); res.emit {
		t.Fatal("a quote without a known data mode must not be emitted")
	}
	res := st.merge(frame(t, map[string]any{"6509": "RpB", "84": "412.10", "86": "412.1300", "88": "1,200", "85": "800", "7762": "1234567", "_updated": 1790600000000}), now)
	q := res.quote
	switch {
	case !res.emit:
		t.Fatal("expected a quote")
	case str(q.Last) != "412.1250" || str(q.Bid) != "412.10" || str(q.Ask) != "412.1300":
		t.Fatalf("prices must keep their received precision: last %s bid %s ask %s", str(q.Last), str(q.Bid), str(q.Ask))
	case *q.BidSize != 1200 || *q.AskSize != 800 || *q.Volume != 1234567:
		t.Fatalf("sizes/volume: %d %d %d", *q.BidSize, *q.AskSize, *q.Volume)
	case q.DataMode != marketdata.DataRealtime || q.Halted == nil || *q.Halted:
		t.Fatalf("mode %s halted %v", q.DataMode, q.Halted)
	case q.Sequence != 1790600000000*1000 || !q.Time.Equal(time.UnixMilli(1790600000000)):
		t.Fatalf("sequence %d time %s", q.Sequence, q.Time)
	}

	// Partial frame: untouched fields keep their values; the sequence keeps increasing.
	res = st.merge(frame(t, map[string]any{"86": "412.14", "_updated": 1790600000000}), now)
	if str(res.quote.Bid) != "412.10" || str(res.quote.Ask) != "412.14" || res.quote.Sequence != q.Sequence+1 {
		t.Fatalf("partial merge: %+v", res.quote)
	}

	for code, mode := range map[string]marketdata.DataMode{"DpB": marketdata.DataDelayed, "ZpB": marketdata.DataFrozen, "YpB": marketdata.DataFrozenDelayed} {
		if res := st.merge(frame(t, map[string]any{"6509": code}), now); res.quote.DataMode != mode {
			t.Errorf("%s: mode %s", code, res.quote.DataMode)
		}
	}

	res = st.merge(frame(t, map[string]any{"31": "H412.00", "6509": "RpB"}), now)
	if !*res.quote.Halted || str(res.quote.Last) != "412.00" {
		t.Fatalf("halt prefix: halted %v last %s", *res.quote.Halted, str(res.quote.Last))
	}
	res = st.merge(frame(t, map[string]any{"31": "C411.50"}), now)
	if res.quote.Last != nil {
		t.Fatalf("a previous close is not a current trade: last %s", str(res.quote.Last))
	}

	res = st.merge(frame(t, map[string]any{"84": "-1", "86": "1.1234567", "88": "12x"}), now)
	if res.quote.Bid != nil || res.quote.Ask != nil || res.quote.BidSize != nil {
		t.Fatal("malformed or unrepresentable values must become null, never guessed or rounded")
	}
	if res.malformed != 2 || res.unwireable != 1 {
		t.Fatalf("malformed %d unwireable %d", res.malformed, res.unwireable)
	}

	res = st.merge(frame(t, map[string]any{"6509": "NpB"}), now)
	if !errors.Is(res.unavailable, marketdata.ErrSymbolUnavailable) || res.emit {
		t.Fatalf("not subscribed must be an availability error: %v", res.unavailable)
	}

	// The encoded message carries the mode and the halt flag.
	msg := stream.NewQuoteMessage(stream.TypeQuote, q, false)
	if msg.DataMode != "REALTIME" || msg.Halted == nil || *msg.Last != "412.1250" {
		t.Fatalf("encoded: %+v", msg)
	}
}

// ------------------------------------------------------------------------------------------ 3. streams

func TestSubscribeShareRenewAndUnsubscribe(t *testing.T) {
	fake, base, pki := startFake(t, "127.0.0.1")
	h := newHarness(t, fake, testConfig(base, pki.CAPEM))
	h.waitState("READY")
	ctx := context.Background()

	q, err := h.src.Snapshot(ctx, "NVDA")
	if err != nil || str(q.Last) != "182.13" || q.DataMode != marketdata.DataRealtime {
		t.Fatalf("snapshot: %+v, %v", q, err)
	}
	a, b := &sink{}, &sink{}
	subA, err := h.src.Subscribe(ctx, "NVDA", a)
	if err != nil {
		t.Fatal(err)
	}
	subB, _ := h.src.Subscribe(ctx, "NVDA", b)
	smdTopic := "smd+4815747+"
	if n := fake.CountTopics(smdTopic); n != 1 {
		t.Fatalf("smd topics = %d, want 1 shared stream", n)
	}
	fake.Push(fakecpgw.ConidNVDA, map[string]string{"31": "182.20"})
	waitFor(t, "quotes on both sinks", func() bool {
		qa, okA := a.last()
		qb, okB := b.last()
		return okA && okB && str(qa.Last) == "182.20" && str(qb.Last) == "182.20"
	})

	waitFor(t, "renewal before IBKR's stream termination", func() bool { return fake.CountTopics(smdTopic) >= 2 })
	if testutil.ToFloat64(h.m.IBKRSMDRenewals) < 1 {
		t.Fatal("renewal metric")
	}

	subA.Close()
	subB.Close()
	waitFor(t, "umd after the last subscriber", func() bool { return fake.CountTopics("umd+4815747+") >= 1 })

	if _, err := h.src.Snapshot(ctx, "UNSUB"); !errors.Is(err, marketdata.ErrSymbolUnavailable) {
		t.Fatalf("no market-data subscription: %v", err)
	}
}

// ------------------------------------------------------------------------------------------ 5. session states

func TestSessionStates(t *testing.T) {
	t.Run("established session becomes READY", func(t *testing.T) {
		fake, base, pki := startFake(t, "127.0.0.1")
		h := newHarness(t, fake, testConfig(base, pki.CAPEM))
		h.waitState("READY")
		if got := strings.Join(h.states(), ","); got != "CONNECTING,AUTHENTICATING,READY" {
			t.Fatalf("states = %s", got)
		}
	})
	t.Run("unauthenticated brokerage session is initialized without competing", func(t *testing.T) {
		fake, base, pki := startFake(t, "127.0.0.1")
		fake.SetAuth(fakecpgw.Auth{Connected: true})
		h := newHarness(t, fake, testConfig(base, pki.CAPEM))
		h.waitState("READY")
		if inits := fake.Inits(); len(inits) != 1 || inits[0] {
			t.Fatalf("init requests (compete flags) = %v, want exactly one with compete=false", inits)
		}
	})
	t.Run("no gateway login ends DISCONNECTED", func(t *testing.T) {
		fake, base, pki := startFake(t, "127.0.0.1")
		fake.SetAuth(fakecpgw.Auth{})
		h := newHarness(t, fake, testConfig(base, pki.CAPEM))
		h.waitState("DISCONNECTED:LOGIN_REQUIRED")
		if !strings.Contains(strings.Join(h.states(), ","), "RECONNECTING:LOGIN_REQUIRED") {
			t.Fatalf("states = %v", h.states())
		}
		if len(fake.Inits()) != 0 {
			t.Fatal("a missing login cannot be fixed by the adapter")
		}
	})
	t.Run("competing session degrades and recovers", func(t *testing.T) {
		fake, base, pki := startFake(t, "127.0.0.1")
		h := newHarness(t, fake, testConfig(base, pki.CAPEM))
		h.waitState("READY")
		fake.SetAuth(fakecpgw.Auth{Connected: true, Authenticated: true, Established: true, Competing: true})
		h.waitState("DEGRADED:COMPETING_SESSION")
		fake.SetAuth(fakecpgw.Auth{Connected: true, Authenticated: true, Established: true})
		h.waitState("READY")
		if len(fake.Inits()) != 0 {
			t.Fatal("the adapter must never try to take over a competing session")
		}
	})
}

// ------------------------------------------------------------------------------------------ 6. pacing

func TestRateLimitedResponseStartsCooldown(t *testing.T) {
	fake, base, pki := startFake(t, "127.0.0.1")
	h := newHarness(t, fake, testConfig(base, pki.CAPEM))
	h.waitState("READY")
	fake.FailNext("/tickle", 429, 1)
	h.waitState("DEGRADED:RATE_LIMITED")
	if !h.src.limiter.CoolingDown() || testutil.ToFloat64(h.m.IBKRRateLimited) != 1 {
		t.Fatal("a 429 must start the cool-down")
	}
	if _, err := h.src.Bars(context.Background(), "NVDA", marketdata.Interval1h, marketdata.Range5d); !errors.Is(err, marketdata.ErrRateLimited) {
		t.Fatalf("bars during the cool-down: %v", err)
	}
	if fake.Requests("/iserver/marketdata/history") != 0 {
		t.Fatal("IBKR must not be contacted for non-essential requests during the cool-down")
	}
	h.waitState("READY") // the cool-down ends
}

func TestPacingAllocationInvariant(t *testing.T) {
	cfg := pacing.Config{SessionLimit: 10, Allocation: 9.5, Headroom: 1, MaxWaiters: 1, AcquireTimeout: time.Second, Cooldown: time.Second}
	if cfg.Validate() == nil {
		t.Fatal("allocation + headroom above the session limit must be rejected")
	}
	cfg.Allocation = 5
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

// ------------------------------------------------------------------------------------------ 7. history

func TestHistoricalBars(t *testing.T) {
	fake, base, pki := startFake(t, "127.0.0.1")
	h := newHarness(t, fake, testConfig(base, pki.CAPEM))
	bars, err := h.src.Bars(context.Background(), "NVDA", marketdata.Interval1h, marketdata.Range5d)
	if err != nil || len(bars) != 2 {
		t.Fatalf("bars = %d, err = %v", len(bars), err)
	}
	b0, b1 := bars[0], bars[1]
	switch {
	case b0.Open.String() != "173.40" || b0.Close.String() != "174.70" || b0.Volume != 472117:
		t.Fatalf("bar 0 must keep the received precision: %+v", b0)
	case b1.High.String() != "175.25": // exponent notation parsed exactly
		t.Fatalf("bar 1 high = %s", b1.High.String())
	case b1.Volume != 1200: // whole shares
		t.Fatalf("bar 1 volume = %d", b1.Volume)
	case !b0.Time.Equal(time.UnixMilli(1790496000000)):
		t.Fatalf("time = %s", b0.Time)
	}
	if _, err := h.src.Bars(context.Background(), "NVDA", marketdata.Interval1m, marketdata.Range5d); !errors.Is(err, marketdata.ErrUnsupportedRange) {
		t.Fatalf("combination beyond one IBKR request: %v", err)
	}
}

// ------------------------------------------------------------------------------------------ 8. TLS

func TestUntrustedGatewayCertificate(t *testing.T) {
	other, err := fakecpgw.NewPKI("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func() (*fakecpgw.Server, string, []byte){
		"unknown CA": func() (*fakecpgw.Server, string, []byte) {
			fake, base, _ := startFake(t, "127.0.0.1")
			return fake, base, other.CAPEM
		},
		"wrong host name": func() (*fakecpgw.Server, string, []byte) {
			fake, base, pki := startFake(t, "cpgw.example")
			return fake, base, pki.CAPEM
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			fake, base, ca := setup()
			h := newHarness(t, fake, testConfig(base, ca))
			h.waitState("DISCONNECTED:TLS_UNTRUSTED")
			if strings.Contains(strings.Join(h.states(), ","), "RECONNECTING") {
				t.Fatalf("a TLS failure must not start a retry cycle: %v", h.states())
			}
			if fake.Requests("/iserver/auth/status") != 0 {
				t.Fatal("no request may complete over an untrusted connection")
			}
		})
	}
}
