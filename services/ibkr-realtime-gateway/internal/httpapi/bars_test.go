package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/hotcache"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/simulator"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/testsource"
)

// slowSource counts and delays bar computations.
type slowSource struct {
	*testsource.Source
	calls   atomic.Int64
	release chan struct{} // computations wait for it when non-nil
}

func (s *slowSource) Bars(ctx context.Context, symbol string, i marketdata.Interval, r marketdata.Range) ([]marketdata.Bar, error) {
	s.calls.Add(1)
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.Source.Bars(ctx, symbol, i, r)
}

type mapStore struct {
	mu   sync.Mutex
	data map[string]hotcache.Entry
	fail bool
}

func (s *mapStore) SetMany(_ context.Context, entries []hotcache.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("i/o timeout")
	}
	for _, e := range entries {
		s.data[e.Key] = e
	}
	return nil
}

func (s *mapStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return nil, errors.New("i/o timeout")
	}
	e, ok := s.data[key]
	if !ok {
		return nil, hotcache.ErrMiss
	}
	return e.Value, nil
}

func (s *mapStore) Close() error { return nil }

var nvda = marketdata.Instrument{Symbol: "NVDA", PriceDecimals: 2}

func newBars(t *testing.T, store hotcache.Store, maxConcurrent int) (*BarsService, *slowSource, *metrics.Gateway) {
	t.Helper()
	clk := clock.Real{}
	src := &slowSource{Source: testsource.New(clk, "NVDA")}
	m := metrics.New()
	var guard *hotcache.Guard
	if store != nil {
		guard = hotcache.NewGuard(clk, 100*time.Millisecond, time.Hour, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	b := NewBarsService(src, store, guard, BarsConfig{MaxConcurrent: maxConcurrent, ComputeTimeout: time.Second, MaxCacheTTL: 5 * time.Minute}, m)
	return b, src, m
}

func TestCacheTTLIsAQuarterIntervalCapped(t *testing.T) {
	b, _, _ := newBars(t, nil, 1)
	cases := map[marketdata.Interval]time.Duration{
		marketdata.Interval1m: 15 * time.Second, marketdata.Interval5m: 75 * time.Second,
		marketdata.Interval15m: 225 * time.Second, marketdata.Interval1h: 5 * time.Minute, marketdata.Interval1d: 5 * time.Minute,
	}
	for i, want := range cases {
		if got := b.CacheTTL(i); got != want {
			t.Errorf("%s: %s, want %s", i, got, want)
		}
	}
}

func TestMissComputesAndStoresThenHitServesFromCache(t *testing.T) {
	store := &mapStore{data: map[string]hotcache.Entry{}}
	b, src, m := newBars(t, store, 2)
	raw, cached, err := b.Get(context.Background(), nvda, marketdata.Interval1h, marketdata.Range5d)
	if err != nil || cached {
		t.Fatalf("first request: cached=%v err=%v", cached, err)
	}
	var bars []BarJSON
	if err := json.Unmarshal(raw, &bars); err != nil || len(bars) != 120 {
		t.Fatalf("bars = %d, err = %v", len(bars), err)
	}
	e, ok := store.data[BarsKey(marketdata.SourceMock, "NVDA", marketdata.Interval1h, marketdata.Range5d)]
	if !ok || e.TTL != 5*time.Minute || string(e.Value) != string(raw) {
		t.Fatalf("cache entry = %+v", e)
	}
	raw2, cached, err := b.Get(context.Background(), nvda, marketdata.Interval1h, marketdata.Range5d)
	if err != nil || !cached || string(raw2) != string(raw) || src.calls.Load() != 1 {
		t.Fatalf("second request: cached=%v err=%v computations=%d", cached, err, src.calls.Load())
	}
	if testutil.ToFloat64(m.CacheHits.WithLabelValues("bars")) != 1 || testutil.ToFloat64(m.CacheMisses.WithLabelValues("bars")) != 1 {
		t.Fatal("hit/miss counters")
	}
}

func TestConcurrentIdenticalRequestsShareOneComputation(t *testing.T) {
	b, src, _ := newBars(t, nil, 1)
	src.release = make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := b.Get(context.Background(), nvda, marketdata.Interval1h, marketdata.Range5d)
			errs <- err
		}()
	}
	for src.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // let the other requests join the flight
	close(src.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("computations = %d, want 1 (single-flight)", n)
	}
}

func TestComputationLimitRejectsWithBusy(t *testing.T) {
	b, src, m := newBars(t, nil, 1)
	src.release = make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, _, err := b.Get(context.Background(), nvda, marketdata.Interval1h, marketdata.Range5d)
		first <- err
	}()
	for src.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	// A different key cannot join the flight and finds the only slot taken.
	if _, _, err := b.Get(context.Background(), nvda, marketdata.Interval1d, marketdata.Range1mo); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	if testutil.ToFloat64(m.BarsRateLimited) != 1 {
		t.Fatal("bars_rate_limited_total")
	}
	close(src.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestRedisFailureFallsBackToComputation(t *testing.T) {
	store := &mapStore{data: map[string]hotcache.Entry{}, fail: true}
	b, src, m := newBars(t, store, 2)
	for range 3 {
		if _, cached, err := b.Get(context.Background(), nvda, marketdata.Interval1h, marketdata.Range5d); err != nil || cached {
			t.Fatalf("cached=%v err=%v", cached, err)
		}
	}
	if src.calls.Load() != 3 {
		t.Fatalf("computations = %d", src.calls.Load())
	}
	// One failure opens the cool-down; later requests skip Redis entirely.
	if got := testutil.ToFloat64(m.RedisErrors.WithLabelValues("bars_get")); got != 1 {
		t.Fatalf("redis_errors_total{bars_get} = %v, want 1", got)
	}
}

func TestComputeTimeoutIsReported(t *testing.T) {
	b, src, _ := newBars(t, nil, 1)
	b.cfg.ComputeTimeout = 20 * time.Millisecond
	src.release = make(chan struct{}) // never released
	if _, _, err := b.Get(context.Background(), nvda, marketdata.Interval1h, marketdata.Range5d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}

// BenchmarkBars compares an uncached simulator computation with a cache hit (in-memory store standing in for
// Redis, so network time is excluded). Baseline measurement only.
func BenchmarkBars(b *testing.B) {
	model := simulator.NewModel(20260927)
	src, err := simulator.NewSource(model, clock.Real{}, time.Second)
	if err != nil {
		b.Fatal(err)
	}
	inst, _ := src.Instrument("NVDA")
	cfg := BarsConfig{MaxConcurrent: 4, ComputeTimeout: time.Minute, MaxCacheTTL: time.Hour}
	b.Run("uncached/1h-1mo", func(b *testing.B) {
		svc := NewBarsService(src, nil, nil, cfg, metrics.New())
		for b.Loop() {
			if _, _, err := svc.Get(context.Background(), inst, marketdata.Interval1h, marketdata.Range1mo); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("cached/1h-1mo", func(b *testing.B) {
		m := metrics.New()
		store := &mapStore{data: map[string]hotcache.Entry{}}
		guard := hotcache.NewGuard(clock.Real{}, time.Second, time.Second, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
		svc := NewBarsService(src, store, guard, cfg, m)
		if _, _, err := svc.Get(context.Background(), inst, marketdata.Interval1h, marketdata.Range1mo); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, cached, err := svc.Get(context.Background(), inst, marketdata.Interval1h, marketdata.Range1mo); err != nil || !cached {
				b.Fatal("expected a cache hit", err)
			}
		}
	})
}
