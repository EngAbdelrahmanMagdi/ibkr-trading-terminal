package simulator

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

// MinTickInterval is the smallest supported quote interval.
const MinTickInterval = 10 * time.Millisecond

// SimulatorMarketDataSource is the MOCK MarketDataSource: it emits quotes from the deterministic Model on a
// fixed time grid. Quote k of a symbol is taken at time k*tickInterval (Unix epoch aligned) and carries
// sequence k, so sequences increase monotonically and every quote is reproducible from its timestamp.
type SimulatorMarketDataSource struct {
	model *Model
	clock clock.Clock
	tick  time.Duration

	mu     sync.Mutex
	active map[string]int // symbol -> number of active subscriptions
}

var _ marketdata.MarketDataSource = (*SimulatorMarketDataSource)(nil)

// NewSource creates the simulator source. tick must be a whole number of milliseconds >= MinTickInterval.
func NewSource(model *Model, clk clock.Clock, tick time.Duration) (*SimulatorMarketDataSource, error) {
	if model == nil || clk == nil {
		return nil, errors.New("simulator: model and clock are required")
	}
	if tick < MinTickInterval || tick%time.Millisecond != 0 {
		return nil, errors.New("simulator: tick interval must be a whole number of milliseconds >= 10ms")
	}
	return &SimulatorMarketDataSource{model: model, clock: clk, tick: tick, active: map[string]int{}}, nil
}

// ID returns MOCK.
func (s *SimulatorMarketDataSource) ID() marketdata.SourceID { return marketdata.SourceMock }

// Instrument returns instrument metadata.
func (s *SimulatorMarketDataSource) Instrument(symbol string) (marketdata.Instrument, error) {
	return s.model.Instrument(symbol)
}

// tickAt returns the grid time at or before t and its sequence number.
func (s *SimulatorMarketDataSource) tickAt(t time.Time) (time.Time, int64) {
	grid := t.Truncate(s.tick)
	return grid, grid.UnixMilli() / s.tick.Milliseconds()
}

// Snapshot returns the quote of the most recent grid tick.
func (s *SimulatorMarketDataSource) Snapshot(ctx context.Context, symbol string) (marketdata.Quote, error) {
	if err := ctx.Err(); err != nil {
		return marketdata.Quote{}, err
	}
	sm, err := s.model.lookup(symbol)
	if err != nil {
		return marketdata.Quote{}, err
	}
	t, seq := s.tickAt(s.clock.Now())
	return sm.quoteAt(t, seq), nil
}

// Bars returns historical bars up to the current time.
func (s *SimulatorMarketDataSource) Bars(ctx context.Context, symbol string, interval marketdata.Interval, rng marketdata.Range) ([]marketdata.Bar, error) {
	return s.model.Bars(ctx, symbol, interval, rng, s.clock.Now())
}

// ActiveSymbols returns the number of distinct subscribed symbols.
func (s *SimulatorMarketDataSource) ActiveSymbols() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

func (s *SimulatorMarketDataSource) track(symbol string, delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[symbol] += delta
	if s.active[symbol] <= 0 {
		delete(s.active, symbol)
	}
}

// Subscribe starts delivering quotes for symbol. The subscription owns one goroutine, which stops when the
// subscription is closed, ctx is done, or the sink refuses a quote.
func (s *SimulatorMarketDataSource) Subscribe(ctx context.Context, symbol string, sink marketdata.QuoteSink) (marketdata.Subscription, error) {
	sm, err := s.model.lookup(symbol)
	if err != nil {
		return nil, err
	}
	if sink == nil {
		return nil, errors.New("simulator: sink is required")
	}
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscription{cancel: cancel, done: make(chan struct{})}
	s.track(symbol, +1)
	go func() {
		defer close(sub.done)
		defer s.track(symbol, -1)
		s.run(subCtx, sm, sink)
	}()
	return sub, nil
}

// run emits one quote per grid tick after the current time. Missed ticks (for example after a scheduling
// delay) are skipped rather than delivered in a burst.
func (s *SimulatorMarketDataSource) run(ctx context.Context, sm *symbolModel, sink marketdata.QuoteSink) {
	next, _ := s.tickAt(s.clock.Now())
	next = next.Add(s.tick)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.clock.After(next.Sub(s.clock.Now())):
		}
		if !sink.OfferQuote(sm.quoteAt(next, next.UnixMilli()/s.tick.Milliseconds())) {
			return
		}
		next = next.Add(s.tick)
		if now := s.clock.Now(); now.Sub(next) >= s.tick {
			next, _ = s.tickAt(now)
			next = next.Add(s.tick)
		}
	}
}

type subscription struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// Close stops the subscription and waits for its goroutine to exit.
func (s *subscription) Close() {
	s.once.Do(s.cancel)
	<-s.done
}
