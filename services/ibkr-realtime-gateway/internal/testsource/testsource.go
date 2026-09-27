// Package testsource provides a controllable MarketDataSource for tests: quotes are delivered synchronously
// when the test publishes them, status events are injected explicitly, and connectivity can be broken and
// restored. It is imported only by tests and never linked into the gateway binary.
package testsource

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

// ErrUnavailable is returned while the source is marked unavailable.
var ErrUnavailable = errors.New("testsource: unavailable")

// Source is safe for concurrent use.
type Source struct {
	clock  clock.Clock
	status chan marketdata.StatusEvent

	mu          sync.Mutex
	instruments map[string]marketdata.Instrument
	seq         map[string]int64
	subs        map[*subscription]struct{}
	available   bool
	subscribes  int
	failConnect int // remaining Connect calls that fail
}

// New creates a source serving the given symbols (two price decimals, USD). It starts available.
func New(clk clock.Clock, symbols ...string) *Source {
	s := &Source{
		clock: clk, status: make(chan marketdata.StatusEvent, 64),
		instruments: map[string]marketdata.Instrument{}, seq: map[string]int64{},
		subs: map[*subscription]struct{}{}, available: true,
	}
	for _, sym := range symbols {
		s.instruments[sym] = marketdata.Instrument{Symbol: sym, Name: sym, Currency: "USD", PriceDecimals: 2}
	}
	return s
}

// ID returns MOCK.
func (s *Source) ID() marketdata.SourceID { return marketdata.SourceMock }

// Status returns the injected status events.
func (s *Source) Status() <-chan marketdata.StatusEvent { return s.status }

// Emit injects a status event. It panics if the (generous) buffer is full, which indicates a test bug.
func (s *Source) Emit(state marketdata.SourceState, category string) {
	select {
	case s.status <- marketdata.StatusEvent{State: state, ErrorCategory: category, At: s.clock.Now()}:
	default:
		panic("testsource: status buffer full")
	}
}

// Instrument returns the metadata of a known symbol.
func (s *Source) Instrument(symbol string) (marketdata.Instrument, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, ok := s.instruments[symbol]
	if !ok {
		return marketdata.Instrument{}, fmt.Errorf("%w: %s", marketdata.ErrUnknownSymbol, symbol)
	}
	return inst, nil
}

func (s *Source) quote(symbol string, seq int64) marketdata.Quote {
	last := marketdata.Price(100_000_000 + seq*10_000)
	return marketdata.Quote{
		Symbol: symbol, Bid: last - 10_000, Ask: last + 10_000, Last: last,
		BidSize: 100, AskSize: 200, Volume: 1000 + seq, Sequence: seq, Time: s.clock.Now(),
	}
}

// Snapshot returns the current quote.
func (s *Source) Snapshot(ctx context.Context, symbol string) (marketdata.Quote, error) {
	if err := ctx.Err(); err != nil {
		return marketdata.Quote{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.available {
		return marketdata.Quote{}, ErrUnavailable
	}
	if _, ok := s.instruments[symbol]; !ok {
		return marketdata.Quote{}, marketdata.ErrUnknownSymbol
	}
	return s.quote(symbol, s.seq[symbol]), nil
}

// Subscribe registers a sink. Quotes are delivered only by Publish.
func (s *Source) Subscribe(_ context.Context, symbol string, sink marketdata.QuoteSink) (marketdata.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.available {
		return nil, ErrUnavailable
	}
	if _, ok := s.instruments[symbol]; !ok {
		return nil, marketdata.ErrUnknownSymbol
	}
	sub := &subscription{src: s, symbol: symbol, sink: sink}
	s.subs[sub] = struct{}{}
	s.subscribes++
	return sub, nil
}

// Bars returns one flat bar per interval over the range.
func (s *Source) Bars(ctx context.Context, symbol string, interval marketdata.Interval, rng marketdata.Range) ([]marketdata.Bar, error) {
	if _, err := s.Instrument(symbol); err != nil {
		return nil, err
	}
	if !marketdata.Supported(interval, rng) {
		return nil, marketdata.ErrUnsupportedRange
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	end := s.clock.Now().Truncate(interval.Duration())
	n := int(rng.Duration() / interval.Duration())
	bars := make([]marketdata.Bar, 0, n)
	for i := n - 1; i >= 0; i-- {
		bars = append(bars, marketdata.Bar{Time: end.Add(-time.Duration(i) * interval.Duration()),
			Open: 100_000_000, High: 101_000_000, Low: 99_000_000, Close: 100_500_000, Volume: 1000})
	}
	return bars, nil
}

// Publish delivers the next quote of symbol to its subscriptions synchronously and returns how many
// subscriptions received it.
func (s *Source) Publish(symbol string) int {
	s.mu.Lock()
	s.seq[symbol]++
	q := s.quote(symbol, s.seq[symbol])
	var sinks []marketdata.QuoteSink
	for sub := range s.subs {
		if sub.symbol == symbol {
			sinks = append(sinks, sub.sink)
		}
	}
	s.mu.Unlock()
	for _, sink := range sinks {
		sink.OfferQuote(q)
	}
	return len(sinks)
}

// Break simulates a lost upstream connection: existing subscriptions go silent (they are dropped) and new
// snapshots and subscriptions fail until Restore.
func (s *Source) Break() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = false
	clear(s.subs)
}

// Restore makes the source available again.
func (s *Source) Restore() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = true
}

// FailConnects makes the next n Connect calls fail.
func (s *Source) FailConnects(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failConnect = n
}

// Connect simulates one connection attempt for reconnect tests. It restores the source on success.
func (s *Source) Connect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failConnect > 0 {
		s.failConnect--
		return ErrUnavailable
	}
	s.available = true
	return nil
}

// ActiveSubscriptions returns the number of live subscriptions.
func (s *Source) ActiveSubscriptions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

// SubscribeCalls returns the number of successful Subscribe calls so far.
func (s *Source) SubscribeCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscribes
}

type subscription struct {
	src    *Source
	symbol string
	sink   marketdata.QuoteSink
}

// Close removes the subscription. After it returns, Publish no longer reaches the sink (a Publish already in
// progress may still complete, which matches the documented Subscription semantics closely enough for tests).
func (s *subscription) Close() {
	s.src.mu.Lock()
	defer s.src.mu.Unlock()
	delete(s.src.subs, s)
}
