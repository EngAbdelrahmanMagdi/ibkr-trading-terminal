package registry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/reconnect"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/testsource"
)

const sweep = 100 * time.Millisecond

var t0 = time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC)

type staleEvent struct {
	symbols []string
	reason  string
}

// recorder is a Subscriber that records everything it is offered.
type recorder struct {
	mu          sync.Mutex
	quotes      []*Update
	stale       []staleEvent
	unavailable []string
}

func (r *recorder) OfferQuote(u *Update) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.quotes = append(r.quotes, u)
}

func (r *recorder) OfferStale(symbols []string, _ time.Time, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stale = append(r.stale, staleEvent{symbols: symbols, reason: reason})
}

func (r *recorder) OfferUnavailable(symbol string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unavailable = append(r.unavailable, symbol+": "+err.Error())
}

func (r *recorder) unavailableSymbols() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.unavailable...)
}

func (r *recorder) quoteCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.quotes)
}

func (r *recorder) lastQuote() *Update {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.quotes) == 0 {
		return nil
	}
	return r.quotes[len(r.quotes)-1]
}

func (r *recorder) staleEvents() []staleEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]staleEvent(nil), r.stale...)
}

type fixture struct {
	t      *testing.T
	clk    *clock.Fake
	src    *testsource.Source
	reg    *Registry
	m      *metrics.Gateway
	mu     sync.Mutex
	states []marketdata.SourceState
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	f := &fixture{t: t, clk: clock.NewFake(t0), m: metrics.New()}
	f.src = testsource.New(f.clk, "NVDA", "AAPL", "META")
	if cfg.MaxActiveSymbols == 0 {
		cfg.MaxActiveSymbols = 10
	}
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = time.Hour
	}
	cfg.SweepInterval = sweep
	reg, err := New(f.src, f.clk, cfg, f.m, slog.New(slog.NewTextHandler(io.Discard, nil)), Hooks{
		OnState: func(s marketdata.SourceState, _ time.Time) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.states = append(f.states, s)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.reg = reg
	reg.Start()
	t.Cleanup(reg.Close)
	f.emit(marketdata.StateConnecting)
	f.emit(marketdata.StateReady)
	return f
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

// emit injects a status event and waits until the registry applied it.
func (f *fixture) emit(s marketdata.SourceState) {
	f.t.Helper()
	f.mu.Lock()
	before := len(f.states)
	f.mu.Unlock()
	f.src.Emit(s, "")
	waitFor(f.t, "state "+string(s), func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.states) > before
	})
}

// advance moves the fake clock by d in sweep steps, waiting for each housekeeping pass to complete.
func (f *fixture) advance(d time.Duration) {
	f.t.Helper()
	for elapsed := time.Duration(0); elapsed < d; elapsed += sweep {
		waitFor(f.t, "housekeeping timer", func() bool { return f.clk.Waiters() > 0 })
		f.clk.Advance(sweep)
		waitFor(f.t, "housekeeping pass", func() bool { return f.clk.Waiters() > 0 })
	}
}

func (f *fixture) entry(symbol string) *entry {
	f.reg.mu.Lock()
	defer f.reg.mu.Unlock()
	return f.reg.entries[symbol]
}

func (f *fixture) subscribe(symbol string, sub Subscriber) Snapshot {
	f.t.Helper()
	var snap Snapshot
	if err := f.reg.Subscribe(symbol, sub, func(s Snapshot) bool { snap = s; return true }); err != nil {
		f.t.Fatalf("subscribe %s: %v", symbol, err)
	}
	return snap
}

func decode(t *testing.T, data []byte) stream.QuoteMessage {
	t.Helper()
	var m stream.QuoteMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestOneUpstreamSubscriptionIsSharedAndReferenceCounted(t *testing.T) {
	f := newFixture(t, Config{UnsubscribeGrace: time.Second})
	a, b := &recorder{}, &recorder{}
	f.subscribe("NVDA", a)
	f.subscribe("NVDA", b)
	if n := f.src.ActiveSubscriptions(); n != 1 {
		t.Fatalf("upstream subscriptions = %d, want 1", n)
	}
	if f.src.Publish("NVDA") != 1 {
		t.Fatal("publish did not reach the single upstream subscription")
	}
	if a.lastQuote() == nil || a.lastQuote() != b.lastQuote() {
		t.Fatal("both subscribers must receive the same encoded update")
	}

	f.reg.Unsubscribe("NVDA", a)
	f.reg.Unsubscribe("NVDA", b)
	if f.src.ActiveSubscriptions() != 1 || f.reg.ActiveSymbols() != 1 {
		t.Fatal("upstream must survive during the grace period")
	}
	f.advance(time.Second)
	if f.src.ActiveSubscriptions() != 0 || f.reg.ActiveSymbols() != 0 {
		t.Fatalf("upstream not closed after grace: upstream=%d active=%d", f.src.ActiveSubscriptions(), f.reg.ActiveSymbols())
	}
	if got := testutil.ToFloat64(f.m.ActiveSymbols); got != 0 {
		t.Fatalf("active_symbols = %v", got)
	}
}

func TestResubscribingWithinGraceReusesTheUpstream(t *testing.T) {
	f := newFixture(t, Config{UnsubscribeGrace: time.Second})
	a := &recorder{}
	f.subscribe("NVDA", a)
	f.reg.Unsubscribe("NVDA", a)
	f.advance(500 * time.Millisecond)
	f.subscribe("NVDA", a)
	f.advance(time.Second)
	if calls := f.src.SubscribeCalls(); calls != 1 || f.src.ActiveSubscriptions() != 1 {
		t.Fatalf("subscribe calls = %d, active = %d; want 1, 1", calls, f.src.ActiveSubscriptions())
	}
}

func TestZeroGraceClosesImmediately(t *testing.T) {
	f := newFixture(t, Config{})
	a := &recorder{}
	f.subscribe("NVDA", a)
	f.reg.Unsubscribe("NVDA", a)
	if f.src.ActiveSubscriptions() != 0 {
		t.Fatal("upstream must close immediately without a grace period")
	}
}

func TestSnapshotIsHandedOverBeforeJoiningTheFanOut(t *testing.T) {
	f := newFixture(t, Config{})
	f.src.Publish("NVDA") // sequence 1 before anyone subscribes
	a := &recorder{}
	err := f.reg.Subscribe("NVDA", a, func(s Snapshot) bool {
		m := decode(t, s.Data)
		if m.Type != stream.TypeSnapshot || m.Sequence != s.Sequence || m.Stale {
			t.Errorf("unexpected snapshot %+v", m)
		}
		f.src.Publish("NVDA") // not yet part of the fan-out: must not reach the subscriber
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.quoteCount() != 0 {
		t.Fatal("a quote reached the subscriber before its snapshot was handed over")
	}
	f.src.Publish("NVDA")
	if a.quoteCount() != 1 {
		t.Fatalf("quotes = %d, want 1", a.quoteCount())
	}
	if m := decode(t, a.lastQuote().Data); m.Type != stream.TypeQuote || m.Sequence != a.lastQuote().Sequence {
		t.Fatalf("unexpected quote %+v", m)
	}
}

func TestRejectedSnapshotDoesNotSubscribe(t *testing.T) {
	f := newFixture(t, Config{})
	a := &recorder{}
	if err := f.reg.Subscribe("NVDA", a, func(Snapshot) bool { return false }); !errors.Is(err, ErrRejected) {
		t.Fatalf("err = %v", err)
	}
	f.src.Publish("NVDA")
	if a.quoteCount() != 0 {
		t.Fatal("rejected subscriber received quotes")
	}
}

func TestActiveSymbolCapEvictsIdleSymbolsFirst(t *testing.T) {
	f := newFixture(t, Config{MaxActiveSymbols: 2, UnsubscribeGrace: time.Minute})
	a := &recorder{}
	f.subscribe("NVDA", a)
	f.subscribe("AAPL", a)
	if err := f.reg.Subscribe("META", a, func(Snapshot) bool { return true }); !errors.Is(err, ErrActiveSymbolLimit) {
		t.Fatalf("err = %v, want ErrActiveSymbolLimit", err)
	}
	f.reg.Unsubscribe("NVDA", a) // idle, in grace
	f.subscribe("META", a)       // evicts the idle NVDA entry
	if f.reg.ActiveSymbols() != 2 || f.src.ActiveSubscriptions() != 2 {
		t.Fatalf("active = %d, upstream = %d", f.reg.ActiveSymbols(), f.src.ActiveSubscriptions())
	}
}

func TestUnknownSymbol(t *testing.T) {
	f := newFixture(t, Config{})
	if err := f.reg.Subscribe("ZZZZ", &recorder{}, func(Snapshot) bool { return true }); !errors.Is(err, marketdata.ErrUnknownSymbol) {
		t.Fatalf("err = %v", err)
	}
}

func TestDuplicateAndOutOfOrderQuotesAreDropped(t *testing.T) {
	f := newFixture(t, Config{})
	a := &recorder{}
	f.subscribe("NVDA", a)
	f.src.Publish("NVDA")
	f.src.Publish("NVDA")
	e := f.entry("NVDA")
	f.reg.ingest(e, marketdata.Quote{Symbol: "NVDA", Sequence: 1, Time: t0})
	f.reg.ingest(e, marketdata.Quote{Symbol: "NVDA", Sequence: 2, Time: t0})
	if a.quoteCount() != 2 {
		t.Fatalf("quotes = %d, want 2 (duplicates dropped)", a.quoteCount())
	}
}

func TestSymbolsWithoutUpdatesBecomeStaleAndRecover(t *testing.T) {
	f := newFixture(t, Config{StaleAfter: time.Second})
	a := &recorder{}
	f.subscribe("NVDA", a)
	f.advance(900 * time.Millisecond)
	if len(a.staleEvents()) != 0 {
		t.Fatal("stale too early")
	}
	f.advance(200 * time.Millisecond)
	ev := a.staleEvents()
	if len(ev) != 1 || ev[0].reason != stream.StaleNoUpdates || len(ev[0].symbols) != 1 || ev[0].symbols[0] != "NVDA" {
		t.Fatalf("stale events = %+v", ev)
	}
	if got := testutil.ToFloat64(f.m.StaleSymbols); got != 1 {
		t.Fatalf("stale_symbols = %v", got)
	}
	// A snapshot taken while stale says so.
	if snap := f.subscribe("NVDA", &recorder{}); !decode(t, snap.Data).Stale {
		t.Fatal("snapshot of a stale symbol must carry stale=true")
	}
	f.advance(sweep)
	if len(a.staleEvents()) != 1 {
		t.Fatal("stale must be reported once per episode")
	}

	f.src.Publish("NVDA")
	if decode(t, a.lastQuote().Data).Stale {
		t.Fatal("a fresh quote clears the stale flag")
	}
	f.advance(sweep)
	if got := testutil.ToFloat64(f.m.StaleSymbols); got != 0 {
		t.Fatalf("stale_symbols after recovery = %v", got)
	}
}

func TestSourceOutageMarksStaleAndRecoveryResubscribes(t *testing.T) {
	f := newFixture(t, Config{})
	a := &recorder{}
	f.subscribe("NVDA", a)
	f.subscribe("AAPL", a)

	f.src.Break()
	f.emit(marketdata.StateReconnecting)
	ev := a.staleEvents()
	if len(ev) != 1 || ev[0].reason != stream.StaleSourceDisconnected || len(ev[0].symbols) != 2 || ev[0].symbols[0] != "AAPL" {
		t.Fatalf("stale events = %+v", ev)
	}
	if f.reg.Ready() != true {
		t.Fatal("readiness stays true while a reconnect cycle runs")
	}
	if got := testutil.ToFloat64(f.m.ReconnectTotal); got != 1 {
		t.Fatalf("reconnect_total = %v", got)
	}
	if err := f.reg.Subscribe("META", a, func(Snapshot) bool { return true }); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("new symbol during outage: err = %v", err)
	}
	if got := testutil.ToFloat64(f.m.ConnectionState.WithLabelValues("RECONNECTING")); got != 1 {
		t.Fatalf("connection_state{RECONNECTING} = %v", got)
	}

	f.src.Restore()
	f.emit(marketdata.StateConnecting)
	f.emit(marketdata.StateReady)
	if n := f.src.ActiveSubscriptions(); n != 2 {
		t.Fatalf("upstream subscriptions after recovery = %d, want 2", n)
	}
	before := a.quoteCount()
	f.src.Publish("NVDA")
	if a.quoteCount() != before+1 || decode(t, a.lastQuote().Data).Stale {
		t.Fatal("quotes must flow again, fresh, after resubscription")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	want := []marketdata.SourceState{marketdata.StateConnecting, marketdata.StateReady, marketdata.StateReconnecting, marketdata.StateConnecting, marketdata.StateReady}
	if len(f.states) != len(want) {
		t.Fatalf("state hook calls = %v", f.states)
	}
}

func TestDegradedMarksStaleWithoutResubscribing(t *testing.T) {
	f := newFixture(t, Config{})
	a := &recorder{}
	f.subscribe("NVDA", a)
	f.emit(marketdata.StateDegraded)
	if ev := a.staleEvents(); len(ev) != 1 || ev[0].reason != stream.StaleSourceDegraded {
		t.Fatalf("stale events = %+v", ev)
	}
	f.emit(marketdata.StateReady)
	if f.src.SubscribeCalls() != 1 {
		t.Fatal("DEGRADED -> READY keeps the existing upstream subscriptions")
	}
}

func TestInvalidTransitionsAreIgnored(t *testing.T) {
	f := newFixture(t, Config{})
	f.src.Emit(marketdata.StateAuthenticating, "")
	f.emit(marketdata.StateDegraded) // processed after the invalid event
	if got := f.reg.State().State; got != marketdata.StateDegraded {
		t.Fatalf("state = %s", got)
	}
}

// reconnectDriver emulates the flow a real broker source runs: a bounded reconnect cycle
// with backoff, reporting each step as a status event.
func reconnectDriver(f *fixture, p reconnect.Policy) error {
	f.src.Emit(marketdata.StateReconnecting, "CONNECTION_LOST")
	err := reconnect.Run(context.Background(), p, f.clk, fixedRandom{}, func(ctx context.Context) error {
		f.src.Emit(marketdata.StateConnecting, "")
		if err := f.src.Connect(ctx); err != nil {
			f.src.Emit(marketdata.StateReconnecting, "CONNECT_FAILED")
			return err
		}
		f.src.Emit(marketdata.StateAuthenticating, "")
		f.src.Emit(marketdata.StateReady, "")
		return nil
	}, nil)
	if err != nil {
		f.src.Emit(marketdata.StateDisconnected, "RECONNECT_EXHAUSTED")
	}
	return err
}

type fixedRandom struct{}

func (fixedRandom) Int64N(n int64) int64 { return n - 1 }

func runDriver(f *fixture, p reconnect.Policy) error {
	done := make(chan error, 1)
	go func() { done <- reconnectDriver(f, p) }()
	for {
		select {
		case err := <-done:
			return err
		default:
		}
		// Only the driver's backoff timers are advanced here; the housekeeping timer (1 waiter) stays pending.
		if f.clk.Waiters() > 1 {
			f.clk.Advance(p.Base)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReconnectCycleWithFlakySourceResubscribes(t *testing.T) {
	f := newFixture(t, Config{})
	a := &recorder{}
	f.subscribe("NVDA", a)
	f.src.Break()
	f.src.FailConnects(2)
	p := reconnect.Policy{Base: sweep, Max: 4 * sweep, MaxAttempts: 5, MaxTotal: time.Hour}
	if err := runDriver(f, p); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "READY", func() bool { return f.reg.State().State == marketdata.StateReady })
	waitFor(t, "resubscription", func() bool { return f.src.ActiveSubscriptions() == 1 })
	if got := testutil.ToFloat64(f.m.ReconnectTotal); got != 3 {
		t.Fatalf("reconnect_total = %v, want 3 (initial loss + 2 failed attempts)", got)
	}
	f.src.Publish("NVDA")
	if decode(t, a.lastQuote().Data).Stale {
		t.Fatal("fresh quote expected after reconnect")
	}
}

func TestExhaustedReconnectCycleEndsDisconnectedAndNotReady(t *testing.T) {
	f := newFixture(t, Config{})
	f.subscribe("NVDA", &recorder{})
	f.src.Break()
	f.src.FailConnects(100)
	p := reconnect.Policy{Base: sweep, Max: 2 * sweep, MaxAttempts: 3, MaxTotal: time.Hour}
	if err := runDriver(f, p); !errors.Is(err, reconnect.ErrCycleExhausted) {
		t.Fatalf("err = %v", err)
	}
	waitFor(t, "DISCONNECTED", func() bool { return f.reg.State().State == marketdata.StateDisconnected })
	if f.reg.Ready() {
		t.Fatal("readiness must fail once the cycle is exhausted")
	}
	if st := f.reg.State(); st.ErrorCategory != "RECONNECT_EXHAUSTED" {
		t.Fatalf("error category = %q", st.ErrorCategory)
	}
}

func TestConcurrentSubscribeUnsubscribeAndFanOut(t *testing.T) {
	f := newFixture(t, Config{UnsubscribeGrace: time.Second})
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				f.src.Publish("NVDA")
			}
		}
	}()
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := &recorder{}
			for range 200 {
				if err := f.reg.Subscribe("NVDA", r, func(Snapshot) bool { return true }); err != nil {
					t.Error(err)
					return
				}
				f.reg.Unsubscribe("NVDA", r)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	if n := len(*f.entry("NVDA").subs.Load()); n != 0 {
		t.Fatalf("subscribers left = %d", n)
	}
}

func TestSymbolUnavailableEndsTheSubscription(t *testing.T) {
	f := newFixture(t, Config{UnsubscribeGrace: time.Minute})
	a, b := &recorder{}, &recorder{}
	f.subscribe("NVDA", a)
	f.subscribe("NVDA", b)
	f.src.MarkUnavailable("NVDA")
	waitFor(t, "subscribers told", func() bool { return len(a.unavailableSymbols()) == 1 && len(b.unavailableSymbols()) == 1 })
	waitFor(t, "entry removed", func() bool { return f.reg.ActiveSymbols() == 0 && f.src.ActiveSubscriptions() == 0 })
	f.src.Publish("NVDA")
	if a.quoteCount() != 0 {
		t.Fatal("no quotes after the symbol became unavailable")
	}
}
