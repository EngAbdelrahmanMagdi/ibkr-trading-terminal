package simulator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

// collector is a thread-safe sink that records quotes.
type collector struct {
	mu     sync.Mutex
	quotes []marketdata.Quote
	accept bool
}

func (c *collector) OfferQuote(q marketdata.Quote) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.quotes = append(c.quotes, q)
	return c.accept
}

func (c *collector) snapshot() []marketdata.Quote {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]marketdata.Quote(nil), c.quotes...)
}

// waitFor polls cond until it holds or the (real-time) timeout expires.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func newTestSource(t *testing.T, start time.Time, tick time.Duration) (*SimulatorMarketDataSource, *clock.Fake) {
	t.Helper()
	fake := clock.NewFake(start)
	src, err := NewSource(NewModel(testSeed), fake, tick)
	if err != nil {
		t.Fatal(err)
	}
	return src, fake
}

func TestSourceEmitsOneQuotePerTickAfterSnapshot(t *testing.T) {
	start := mustTime(t, "2026-09-27T14:03:11.300Z")
	src, fake := newTestSource(t, start, 250*time.Millisecond)
	if src.ID() != marketdata.SourceMock {
		t.Fatalf("ID = %s, want MOCK", src.ID())
	}

	snap, err := src.Snapshot(context.Background(), "NVDA")
	if err != nil {
		t.Fatal(err)
	}
	if want := mustTime(t, "2026-09-27T14:03:11.250Z"); !snap.Time.Equal(want) {
		t.Fatalf("snapshot time %s, want grid time %s", snap.Time, want)
	}

	sink := &collector{accept: true}
	sub, err := src.Subscribe(context.Background(), "NVDA", sink)
	if err != nil {
		t.Fatal(err)
	}
	if src.ActiveSymbols() != 1 {
		t.Fatalf("ActiveSymbols = %d, want 1", src.ActiveSymbols())
	}
	for i := 1; i <= 5; i++ {
		waitFor(t, "timer registration", func() bool { return fake.Waiters() == 1 })
		fake.Advance(250 * time.Millisecond)
		waitFor(t, "quote delivery", func() bool { return len(sink.snapshot()) == i })
	}
	sub.Close()
	if src.ActiveSymbols() != 0 {
		t.Fatalf("ActiveSymbols after close = %d, want 0", src.ActiveSymbols())
	}

	model := NewModel(testSeed)
	for i, q := range sink.snapshot() {
		wantTime := snap.Time.Add(time.Duration(i+1) * 250 * time.Millisecond)
		if !q.Time.Equal(wantTime) || q.Sequence != snap.Sequence+int64(i+1) {
			t.Fatalf("quote %d: time %s seq %d, want %s seq %d", i, q.Time, q.Sequence, wantTime, snap.Sequence+int64(i+1))
		}
		if want, _ := model.Price("NVDA", q.Time); q.Last != want {
			t.Fatalf("quote %d: last %d differs from the price function %d", i, q.Last, want)
		}
	}
}

func TestSourceSkipsMissedTicksInsteadOfBursting(t *testing.T) {
	start := mustTime(t, "2026-09-27T14:03:11Z")
	src, fake := newTestSource(t, start, time.Second)
	sink := &collector{accept: true}
	sub, err := src.Subscribe(context.Background(), "AAPL", sink)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	waitFor(t, "timer registration", func() bool { return fake.Waiters() == 1 })
	fake.Advance(10 * time.Second) // the goroutine was "asleep" for ten ticks
	waitFor(t, "first quote", func() bool { return len(sink.snapshot()) == 1 })
	waitFor(t, "next timer", func() bool { return fake.Waiters() == 1 })
	fake.Advance(time.Second)
	waitFor(t, "second quote", func() bool { return len(sink.snapshot()) == 2 })

	q := sink.snapshot()
	if want := start.Add(11 * time.Second); !q[1].Time.Equal(want) {
		t.Fatalf("after catching up the next quote is at %s, want %s (no burst of missed ticks)", q[1].Time, want)
	}
}

func TestSubscriptionEndsWhenSinkRefusesOrContextEnds(t *testing.T) {
	start := mustTime(t, "2026-09-27T14:03:11Z")
	src, fake := newTestSource(t, start, time.Second)

	refusing := &collector{accept: false}
	sub, err := src.Subscribe(context.Background(), "AMD", refusing)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "timer registration", func() bool { return fake.Waiters() == 1 })
	fake.Advance(time.Second)
	waitFor(t, "subscription to end", func() bool { return src.ActiveSymbols() == 0 })
	sub.Close() // must not block after the goroutine has exited

	ctx, cancel := context.WithCancel(context.Background())
	sub2, err := src.Subscribe(ctx, "AMD", &collector{accept: true})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitFor(t, "subscription to end on cancel", func() bool { return src.ActiveSymbols() == 0 })
	sub2.Close()
}

func TestSourceRejectsUnknownSymbolsAndInvalidConfig(t *testing.T) {
	src, _ := newTestSource(t, time.Now(), time.Second)
	if _, err := src.Subscribe(context.Background(), "NOPE", &collector{}); !errors.Is(err, marketdata.ErrUnknownSymbol) {
		t.Errorf("Subscribe unknown: %v", err)
	}
	if _, err := src.Snapshot(context.Background(), "NOPE"); !errors.Is(err, marketdata.ErrUnknownSymbol) {
		t.Errorf("Snapshot unknown: %v", err)
	}
	if _, err := NewSource(NewModel(1), clock.Real{}, 5*time.Millisecond); err == nil {
		t.Error("tick below the minimum must be rejected")
	}
	if _, err := NewSource(NewModel(1), clock.Real{}, 1500*time.Microsecond); err == nil {
		t.Error("tick that is not a whole number of milliseconds must be rejected")
	}
}
