// Package hotcache writes the gateway's disposable hot state to Redis: the latest quote per symbol and the
// historical-bars cache.
//
// Redis is never critical. Every call has a timeout, a failure opens a short cool-down during which Redis is
// not contacted at all (so an outage cannot slow the hot path), failures are counted and logged at a limited
// rate, and callers always have a path that works without Redis.
package hotcache

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
)

// ErrMiss is returned by Store.Get when the key does not exist.
var ErrMiss = errors.New("hotcache: miss")

// Entry is one key to write with its expiry. Every key the gateway writes carries a TTL.
type Entry struct {
	Key   string
	Value []byte
	TTL   time.Duration
}

// Store is the Redis subset the gateway uses.
type Store interface {
	SetMany(ctx context.Context, entries []Entry) error // pipelined SET key value EX ttl
	Get(ctx context.Context, key string) ([]byte, error)
	Close() error
}

// Guard implements the timeout and cool-down policy shared by all Redis callers.
type Guard struct {
	clock    clock.Clock
	timeout  time.Duration
	cooldown time.Duration
	metrics  *metrics.Gateway
	log      *slog.Logger

	openUntil atomic.Int64 // Unix nanoseconds until which Redis is skipped
	lastLog   atomic.Int64 // Unix nanoseconds of the last failure log
}

// NewGuard creates a guard.
func NewGuard(clk clock.Clock, timeout, cooldown time.Duration, m *metrics.Gateway, log *slog.Logger) *Guard {
	return &Guard{clock: clk, timeout: timeout, cooldown: cooldown, metrics: m, log: log}
}

// Available reports whether Redis may be contacted (no cool-down in effect).
func (g *Guard) Available() bool { return g.clock.Now().UnixNano() >= g.openUntil.Load() }

// Context returns a context bounded by the Redis timeout. It is detached from parent cancellation so that a
// departing HTTP client cannot abort a cache write shared with other requests.
func (g *Guard) Context(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), g.timeout)
}

// Fail records a failed operation and starts the cool-down. Logs are limited to one per cool-down period.
func (g *Guard) Fail(op string, err error) {
	g.metrics.RedisErrors.WithLabelValues(op).Inc()
	now := g.clock.Now().UnixNano()
	g.openUntil.Store(now + g.cooldown.Nanoseconds())
	if last := g.lastLog.Load(); now-last >= g.cooldown.Nanoseconds() && g.lastLog.CompareAndSwap(last, now) {
		g.log.Warn("redis unavailable; continuing without it", "op", op, "error", err.Error(),
			"cooldownMs", g.cooldown.Milliseconds())
	}
}

// QuoteWriter keeps the latest quote per symbol and writes them to Redis at a fixed interval with one
// pipelined request. Publish is called on the quote hot path and only updates an in-memory map, whose size is
// bounded by the number of active symbols.
type QuoteWriter struct {
	source   marketdata.SourceID
	store    Store
	guard    *Guard
	clock    clock.Clock
	interval time.Duration
	ttl      time.Duration
	metrics  *metrics.Gateway

	mu      sync.Mutex
	pending map[string][]byte // symbol -> latest encoded quote message
	spare   map[string][]byte

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	entries  []Entry // writer-only buffer
}

// QuoteKey is the Redis key of a symbol's latest quote. Keys are namespaced by source: consumers that need
// broker data read only quote:IBKR:* and can never mistake simulated prices for broker prices.
func QuoteKey(source marketdata.SourceID, symbol string) string {
	return "quote:" + string(source) + ":" + symbol
}

// NewQuoteWriter creates the writer; Run must be started once.
func NewQuoteWriter(source marketdata.SourceID, store Store, guard *Guard, clk clock.Clock, interval, ttl time.Duration, m *metrics.Gateway) *QuoteWriter {
	return &QuoteWriter{
		source: source, store: store, guard: guard, clock: clk, interval: interval, ttl: ttl, metrics: m,
		pending: map[string][]byte{}, spare: map[string][]byte{},
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

// Publish records the latest encoded quote of a symbol. data must not be modified afterwards.
func (w *QuoteWriter) Publish(symbol string, data []byte) {
	w.mu.Lock()
	w.pending[symbol] = data
	w.mu.Unlock()
}

// Run writes pending quotes every interval until Close. It is the writer's only goroutine.
func (w *QuoteWriter) Run() {
	defer close(w.done)
	for {
		select {
		case <-w.stop:
			w.flush() // final write of the latest state
			return
		case <-w.clock.After(w.interval):
			w.flush()
		}
	}
}

// Close stops Run after a final flush and waits for it, or until ctx expires.
func (w *QuoteWriter) Close(ctx context.Context) error {
	w.stopOnce.Do(func() { close(w.stop) })
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *QuoteWriter) flush() {
	w.mu.Lock()
	batch := w.pending
	w.pending, w.spare = w.spare, batch
	w.mu.Unlock()
	defer clear(batch)
	if len(batch) == 0 || !w.guard.Available() {
		return // during a cool-down the latest values are dropped; the next interval writes fresh ones
	}
	entries := w.entries[:0]
	for symbol, data := range batch {
		entries = append(entries, Entry{Key: QuoteKey(w.source, symbol), Value: data, TTL: w.ttl})
	}
	ctx, cancel := w.guard.Context(context.Background())
	err := w.store.SetMany(ctx, entries)
	cancel()
	clear(entries)
	w.entries = entries[:0]
	if err != nil {
		w.guard.Fail("quote_write", err)
		return
	}
	w.metrics.QuoteCacheWrites.Add(float64(len(batch)))
}
