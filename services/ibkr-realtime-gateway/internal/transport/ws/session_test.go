package ws

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/registry"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/testsource"
)

var errClosed = errors.New("fake conn closed")

func TestSameHostOriginCannotBypassCompleteAllowlist(t *testing.T) {
	cfg := testConfig()
	cfg.AllowedOrigins = []string{"http://configured.example"}
	h := newHarness(t, cfg, clock.Real{})
	server := httptest.NewServer(h.srv)
	defer server.Close()
	request, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	request.Header.Set("Origin", server.URL)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d", response.StatusCode)
	}
}

func TestInboundCommandsAreBoundedEvenWhenMalformed(t *testing.T) {
	cfg := testConfig()
	cfg.CommandsPerSecond, cfg.CommandBurst = 1, 2
	h := newHarness(t, cfg, clock.NewFake(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)))
	c := newFakeConn()
	_, done := h.start(c)
	for range 3 {
		c.send(`{"type":"invalid"}`)
	}
	waitFor(t, "policy close", func() bool { return c.code() == websocket.StatusPolicyViolation })
	<-done
}

// fakeConn is a scripted peer. Writes can be delayed or blocked to simulate slow and stuck clients.
type fakeConn struct {
	reads      chan []byte
	closed     chan struct{}
	closeOnce  sync.Once
	writeDelay time.Duration
	block      bool // writes block until their context expires

	mu        sync.Mutex
	frames    [][]byte
	closeCode websocket.StatusCode
	abrupt    bool
}

func newFakeConn() *fakeConn {
	return &fakeConn{reads: make(chan []byte, 64), closed: make(chan struct{})}
}

func (c *fakeConn) Read(context.Context) (websocket.MessageType, []byte, error) {
	select {
	case m := <-c.reads:
		return websocket.MessageText, m, nil
	case <-c.closed:
		return 0, nil, errClosed
	}
}

func (c *fakeConn) Write(ctx context.Context, _ websocket.MessageType, p []byte) error {
	if c.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if c.writeDelay > 0 {
		time.Sleep(c.writeDelay)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frames = append(c.frames, append([]byte(nil), p...))
	return nil
}

func (c *fakeConn) Ping(context.Context) error { return nil }

func (c *fakeConn) Close(code websocket.StatusCode, _ string) error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closeCode = code
		c.mu.Unlock()
		close(c.closed)
	})
	return nil
}

func (c *fakeConn) CloseNow() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.abrupt = true
		c.mu.Unlock()
		close(c.closed)
	})
	return nil
}

func (c *fakeConn) wasAbrupt() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.abrupt
}

func (c *fakeConn) send(msg string) { c.reads <- []byte(msg) }

type wireMsg struct {
	Type     string   `json:"type"`
	State    string   `json:"state"`
	Symbol   string   `json:"symbol"`
	Sequence int64    `json:"sequence"`
	Code     string   `json:"code"`
	Symbols  []string `json:"symbols"`
	Reason   string   `json:"reason"`
	Stale    bool     `json:"stale"`
}

func (c *fakeConn) messages(t *testing.T) []wireMsg {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]wireMsg, 0, len(c.frames))
	for _, f := range c.frames {
		var m wireMsg
		if err := json.Unmarshal(f, &m); err != nil {
			t.Fatalf("bad frame %s: %v", f, err)
		}
		out = append(out, m)
	}
	return out
}

func (c *fakeConn) waitFrames(t *testing.T, n int) []wireMsg {
	t.Helper()
	waitFor(t, "frames", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.frames) >= n
	})
	return c.messages(t)
}

func (c *fakeConn) code() websocket.StatusCode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCode
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitRetryTimer waits until the writer armed its rate-limit timer. The registry's housekeeping timer is
// always pending on the same fake clock, hence two waiters.
func waitRetryTimer(t *testing.T, clk *clock.Fake) {
	t.Helper()
	waitFor(t, "rate-limit timer", func() bool { return clk.Waiters() >= 2 })
}

type harness struct {
	srv *Server
	src *testsource.Source
	reg *registry.Registry
	m   *metrics.Gateway
}

func testConfig() Config {
	return Config{
		MaxInboundMessageBytes: 4096, MaxSymbolsPerSubscribe: 10, MaxSubscribedSymbols: 20,
		HeartbeatInterval: time.Hour, WriteTimeout: time.Second, ControlQueueSize: 64,
		FlushInterval: 50 * time.Millisecond, LagThreshold: time.Hour, MaxLaggingFlushes: 5, MaxConnections: 10,
	}
}

func newHarness(t *testing.T, cfg Config, clk clock.Clock) *harness {
	t.Helper()
	h := &harness{m: metrics.New()}
	h.src = testsource.New(clk, "NVDA", "AAPL", "META", "AMD", "TSLA")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg, err := registry.New(h.src, clk, registry.Config{MaxActiveSymbols: 10, StaleAfter: time.Hour, SweepInterval: time.Hour}, h.m, log,
		registry.Hooks{OnState: func(s marketdata.SourceState, at time.Time) { h.srv.BroadcastState(s, at) }})
	if err != nil {
		t.Fatal(err)
	}
	h.reg = reg
	h.srv = NewServer(cfg, reg, clk, h.m, log)
	reg.Start()
	h.src.Emit(marketdata.StateConnecting, "")
	h.src.Emit(marketdata.StateReady, "")
	waitFor(t, "READY", func() bool { return reg.State().State == marketdata.StateReady })
	t.Cleanup(func() {
		_ = h.srv.Shutdown(context.Background())
		reg.Close()
	})
	return h
}

// start runs a session on c and returns a channel closed when it ends.
func (h *harness) start(c *fakeConn) (*session, chan struct{}) {
	sess := newSession(h.srv, c)
	h.srv.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer h.srv.wg.Done()
		sess.run()
	}()
	return sess, done
}

// subscribe sends a subscription followed by an invalid message and waits for the error reply. The read loop
// handles messages in order, so the reply proves the subscription completed (the session is in the fan-out)
// before the test publishes quotes. It returns the frames received so far (the last one is the error).
func (c *fakeConn) subscribe(t *testing.T, symbols string, frames int) []wireMsg {
	t.Helper()
	c.send(`{"type":"subscribe","symbols":[` + symbols + `]}`)
	c.send(`{"type":"sync"}`)
	msgs := c.waitFrames(t, frames)
	if last := msgs[len(msgs)-1]; last.Type != "error" || last.Code != "INVALID_MESSAGE" {
		t.Fatalf("expected the sync error reply, got %+v", msgs)
	}
	return msgs
}

func TestSnapshotFirstThenCoalescedLatestQuote(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC))
	h := newHarness(t, testConfig(), clk)
	c := newFakeConn()
	h.start(c)
	msgs := c.subscribe(t, `"NVDA"`, 3)
	if msgs[0].Type != "connection" || msgs[0].State != "READY" || msgs[1].Type != "snapshot" || msgs[1].Symbol != "NVDA" {
		t.Fatalf("unexpected opening frames: %+v", msgs)
	}

	h.src.Publish("NVDA") // first quote is sent at once
	msgs = c.waitFrames(t, 4)
	if msgs[3].Type != "quote" || msgs[3].Sequence != 1 {
		t.Fatalf("first quote: %+v", msgs[3])
	}
	for range 5 { // within the flush interval: only the latest survives
		h.src.Publish("NVDA")
	}
	time.Sleep(20 * time.Millisecond)
	if n := len(c.messages(t)); n != 4 {
		t.Fatalf("quotes flushed before the interval elapsed: %d frames: %+v", n, c.messages(t))
	}
	waitRetryTimer(t, clk)
	clk.Advance(50 * time.Millisecond)
	msgs = c.waitFrames(t, 5)
	if msgs[4].Type != "quote" || msgs[4].Sequence != 6 {
		t.Fatalf("coalesced quote: %+v", msgs[4])
	}
	if got := testutil.ToFloat64(h.m.QuotesCoalesced); got != 4 {
		t.Fatalf("quotes_coalesced_total = %v, want 4", got)
	}
	if got := testutil.ToFloat64(h.m.QuotesForwarded); got != 2 {
		t.Fatalf("quotes_forwarded_total = %v, want 2", got)
	}
}

func TestQuotesOfUnsubscribedSymbolsAreDropped(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC))
	h := newHarness(t, testConfig(), clk)
	c := newFakeConn()
	h.start(c)
	c.subscribe(t, `"NVDA"`, 3)
	h.src.Publish("NVDA")
	c.waitFrames(t, 4)
	h.src.Publish("NVDA") // pending behind the rate limit
	waitRetryTimer(t, clk)
	c.send(`{"type":"unsubscribe","symbols":["NVDA"]}`)
	waitFor(t, "unsubscribe", func() bool { return h.src.ActiveSubscriptions() == 0 })
	clk.Advance(time.Second)
	time.Sleep(20 * time.Millisecond)
	if n := len(c.messages(t)); n != 4 {
		t.Fatalf("frames after unsubscribe = %d, want 4", n)
	}
}

func TestResubscribeSendsFreshSnapshot(t *testing.T) {
	h := newHarness(t, testConfig(), clock.Real{})
	c := newFakeConn()
	h.start(c)
	c.subscribe(t, `"NVDA"`, 3)
	h.src.Publish("NVDA")
	c.waitFrames(t, 4)
	c.send(`{"type":"subscribe","symbols":["NVDA"]}`)
	msgs := c.waitFrames(t, 5)
	if msgs[4].Type != "snapshot" || msgs[4].Sequence != 1 {
		t.Fatalf("re-subscription: %+v", msgs[4])
	}
	if h.src.SubscribeCalls() != 1 {
		t.Fatal("re-subscription must not create another upstream subscription")
	}
}

func TestStateChangesAndStaleAreBroadcast(t *testing.T) {
	h := newHarness(t, testConfig(), clock.Real{})
	c := newFakeConn()
	h.start(c)
	c.send(`{"type":"subscribe","symbols":["NVDA","AAPL"]}`)
	c.waitFrames(t, 3)
	h.src.Break()
	h.src.Emit(marketdata.StateReconnecting, "CONNECTION_LOST")
	msgs := c.waitFrames(t, 5)
	var sawStale, sawState bool
	for _, m := range msgs[3:] {
		switch m.Type {
		case "stale":
			sawStale = m.Reason == "SOURCE_DISCONNECTED" && len(m.Symbols) == 2
		case "connection":
			sawState = m.State == "RECONNECTING"
		}
	}
	if !sawStale || !sawState {
		t.Fatalf("expected stale and connection messages, got %+v", msgs[3:])
	}
}

func TestSubscriptionErrorsAreGrouped(t *testing.T) {
	h := newHarness(t, testConfig(), clock.Real{})
	c := newFakeConn()
	h.start(c)
	c.send(`{"type":"subscribe","symbols":["NVDA","ZZZZ","YYYY"]}`)
	msgs := c.waitFrames(t, 3)
	last := msgs[2]
	if last.Type != "error" || last.Code != "UNKNOWN_SYMBOL" || len(last.Symbols) != 2 {
		t.Fatalf("unexpected error frame %+v", last)
	}
}

func TestWriteTimeoutEvictsSlowConsumer(t *testing.T) {
	cfg := testConfig()
	cfg.WriteTimeout = 50 * time.Millisecond
	h := newHarness(t, cfg, clock.Real{})
	c := newFakeConn()
	c.block = true
	sess, done := h.start(c)
	<-done
	if code, _ := sess.closeStatus(); code != websocket.StatusTryAgainLater || !c.wasAbrupt() {
		t.Fatalf("close code = %d, abrupt = %v; want 1013 without a handshake", code, c.wasAbrupt())
	}
	if got := testutil.ToFloat64(h.m.Evictions.WithLabelValues(metrics.EvictWriteTimeout)); got != 1 {
		t.Fatalf("evictions{write_timeout} = %v", got)
	}
}

func TestControlQueueOverflowEvicts(t *testing.T) {
	cfg := testConfig()
	cfg.ControlQueueSize = 4
	cfg.WriteTimeout = 300 * time.Millisecond
	h := newHarness(t, cfg, clock.Real{})
	c := newFakeConn()
	c.block = true // the writer is stuck on the first frame; control frames pile up
	_, done := h.start(c)
	for range 8 {
		c.send(`not json`)
	}
	<-done
	if c.code() != websocket.StatusTryAgainLater {
		t.Fatalf("close code = %d, want 1013", c.code())
	}
	if got := testutil.ToFloat64(h.m.Evictions.WithLabelValues(metrics.EvictControlQueue)); got != 1 {
		t.Fatalf("evictions{control_queue_full} = %v", got)
	}
	if got := testutil.ToFloat64(h.m.Evictions.WithLabelValues(metrics.EvictWriteTimeout)); got != 0 {
		t.Fatalf("an already evicted client must not be counted twice, write_timeout = %v", got)
	}
}

func TestLaggingFlushesEvictSlowConsumer(t *testing.T) {
	cfg := testConfig()
	cfg.FlushInterval = 5 * time.Millisecond
	cfg.LagThreshold = 40 * time.Millisecond
	cfg.MaxLaggingFlushes = 2
	h := newHarness(t, cfg, clock.Real{})
	c := newFakeConn()
	c.writeDelay = 15 * time.Millisecond // 5 quotes per flush take ~75ms
	_, done := h.start(c)
	c.send(`{"type":"subscribe","symbols":["NVDA","AAPL","META","AMD","TSLA"]}`)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
				for _, s := range []string{"NVDA", "AAPL", "META", "AMD", "TSLA"} {
					h.src.Publish(s)
				}
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("slow consumer was not evicted")
	}
	close(stop)
	if c.code() != websocket.StatusTryAgainLater {
		t.Fatalf("close code = %d, want 1013", c.code())
	}
	if got := testutil.ToFloat64(h.m.Evictions.WithLabelValues(metrics.EvictLaggingFlushes)); got != 1 {
		t.Fatalf("evictions{lagging_flushes} = %v", got)
	}
	if h.srv.Clients() != 0 {
		t.Fatal("session still registered")
	}
}

func TestShutdownClosesWithGoingAway(t *testing.T) {
	h := newHarness(t, testConfig(), clock.Real{})
	c := newFakeConn()
	_, done := h.start(c)
	c.waitFrames(t, 1)
	if err := h.srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-done
	if c.code() != websocket.StatusGoingAway {
		t.Fatalf("close code = %d, want 1001", c.code())
	}
}

func TestRateLimitIsPerSymbol(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC))
	h := newHarness(t, testConfig(), clk)
	c := newFakeConn()
	h.start(c)
	c.subscribe(t, `"NVDA","AAPL"`, 4)
	h.src.Publish("NVDA")
	c.waitFrames(t, 5)
	h.src.Publish("NVDA") // rate limited: NVDA was just sent
	h.src.Publish("AAPL") // not delayed by NVDA's limit
	msgs := c.waitFrames(t, 6)
	if msgs[5].Symbol != "AAPL" {
		t.Fatalf("expected AAPL without waiting, got %+v", msgs[5])
	}
	time.Sleep(20 * time.Millisecond)
	if n := len(c.messages(t)); n != 6 {
		t.Fatalf("NVDA sent again within the flush interval: %d frames", n)
	}
	waitRetryTimer(t, clk)
	clk.Advance(50 * time.Millisecond)
	if msgs := c.waitFrames(t, 7); msgs[6].Symbol != "NVDA" || msgs[6].Sequence != 2 {
		t.Fatalf("rate-limited NVDA quote: %+v", msgs[6])
	}
}
