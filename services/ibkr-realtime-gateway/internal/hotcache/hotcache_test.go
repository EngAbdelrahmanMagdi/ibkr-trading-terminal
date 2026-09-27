package hotcache

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
)

// MemStore is an in-memory Store that can be told to fail.
type memStore struct {
	mu      sync.Mutex
	data    map[string]Entry
	fail    bool
	setMany int
}

func newMemStore() *memStore { return &memStore{data: map[string]Entry{}} }

func (s *memStore) SetMany(ctx context.Context, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("connection refused")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("redis calls must carry a deadline")
	}
	s.setMany++
	for _, e := range entries {
		s.data[e.Key] = Entry{Key: e.Key, Value: append([]byte(nil), e.Value...), TTL: e.TTL}
	}
	return nil
}

func (s *memStore) Get(context.Context, string) ([]byte, error) { return nil, ErrMiss }
func (s *memStore) Close() error                                { return nil }

func (s *memStore) snapshot() (map[string]Entry, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Entry, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out, s.setMany
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

func newWriter(t *testing.T, store Store) (*QuoteWriter, *Guard, *clock.Fake, *metrics.Gateway) {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC))
	m := metrics.New()
	g := NewGuard(clk, 100*time.Millisecond, 5*time.Second, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w := NewQuoteWriter(store, g, clk, time.Second, 30*time.Second, m)
	go w.Run()
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w, g, clk, m
}

// tick advances the writer's interval and waits until the flush has completed (the next timer is armed).
func tick(t *testing.T, clk *clock.Fake) {
	t.Helper()
	waitFor(t, "writer timer", func() bool { return clk.Waiters() > 0 })
	clk.Advance(time.Second)
	waitFor(t, "writer timer re-armed", func() bool { return clk.Waiters() > 0 })
}

func TestQuoteWritesAreThrottledToTheLatestValuePerSymbol(t *testing.T) {
	store := newMemStore()
	w, _, clk, m := newWriter(t, store)
	for i := range 100 {
		w.Publish("NVDA", []byte{byte('0' + i%10)})
	}
	w.Publish("AAPL", []byte("a"))
	if _, calls := store.snapshot(); calls != 0 {
		t.Fatal("nothing may be written before the interval elapses")
	}
	tick(t, clk)
	data, calls := store.snapshot()
	if calls != 1 || len(data) != 2 {
		t.Fatalf("pipelined writes = %d, keys = %d; want 1 request with 2 keys", calls, len(data))
	}
	if e := data["quote:NVDA"]; string(e.Value) != "9" || e.TTL != 30*time.Second {
		t.Fatalf("quote:NVDA = %+v", e)
	}
	if got := testutil.ToFloat64(m.QuoteCacheWrites); got != 2 {
		t.Fatalf("quote_cache_writes_total = %v", got)
	}
	tick(t, clk)
	if _, calls := store.snapshot(); calls != 1 {
		t.Fatal("an idle interval must not write")
	}
}

func TestFailureStartsACooldown(t *testing.T) {
	store := newMemStore()
	store.fail = true
	w, g, clk, m := newWriter(t, store)
	w.Publish("NVDA", []byte("1"))
	tick(t, clk)
	if g.Available() {
		t.Fatal("a failure must open the cool-down")
	}
	if got := testutil.ToFloat64(m.RedisErrors.WithLabelValues("quote_write")); got != 1 {
		t.Fatalf("redis_errors_total = %v", got)
	}

	store.mu.Lock()
	store.fail = false
	store.mu.Unlock()
	w.Publish("NVDA", []byte("2"))
	tick(t, clk) // still cooling down: Redis is not contacted
	if _, calls := store.snapshot(); calls != 0 {
		t.Fatal("Redis contacted during the cool-down")
	}
	for range 4 {
		tick(t, clk)
	}
	w.Publish("NVDA", []byte("3"))
	tick(t, clk)
	data, _ := store.snapshot()
	if string(data["quote:NVDA"].Value) != "3" {
		t.Fatalf("writes must resume after the cool-down, got %q", data["quote:NVDA"].Value)
	}
}

func TestCloseFlushesTheLatestState(t *testing.T) {
	store := newMemStore()
	w, _, _, _ := newWriter(t, store)
	w.Publish("NVDA", []byte("final"))
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, _ := store.snapshot()
	if string(data["quote:NVDA"].Value) != "final" {
		t.Fatal("Close must flush pending quotes")
	}
}
