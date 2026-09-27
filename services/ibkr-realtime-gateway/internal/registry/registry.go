// Package registry owns the gateway's upstream market-data subscriptions and fans quotes out to clients.
//
//   - One upstream subscription per symbol, however many clients want it. It is reference counted and, when
//     the last subscriber leaves, closed after a grace period so that quick re-subscriptions cause no churn.
//   - The number of active symbols is capped (the broker's market-data lines).
//   - Per symbol it keeps the latest quote (for snapshots), its freshness and the stale flag.
//   - Subscriber sets are copy-on-write, so fan-out on every quote takes no lock.
//   - It follows the source's connection state, marks symbols stale, and resubscribes after recovery.
//
// Lock order: Registry.mu before entry.mu. The quote ingest path never acquires Registry.mu, which is why
// upstream subscriptions may be closed while Registry.mu is held.
package registry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/connstate"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// Errors returned by Subscribe.
var (
	ErrActiveSymbolLimit = errors.New("registry: active symbol limit reached")
	ErrSourceUnavailable = errors.New("registry: market data source unavailable")
	ErrRejected          = errors.New("registry: subscriber rejected the snapshot")
	ErrClosed            = errors.New("registry: closed")
)

// Update is one quote, encoded once as a stream quote message and shared read-only by every subscriber.
type Update struct {
	Symbol     string
	Sequence   int64
	Data       []byte
	ReceivedAt time.Time // when the gateway received the quote from the source
}

// Snapshot is the encoded snapshot message handed to a new subscriber.
type Snapshot struct {
	Symbol   string
	Sequence int64
	Data     []byte
}

// Subscriber receives the fan-out. Implementations must never block.
type Subscriber interface {
	OfferQuote(u *Update)
	OfferStale(symbols []string, since time.Time, reason string)
}

// Config bounds the registry.
type Config struct {
	MaxActiveSymbols int
	UnsubscribeGrace time.Duration // 0 closes the upstream subscription immediately
	StaleAfter       time.Duration // no update for this long marks a symbol stale
	SweepInterval    time.Duration // housekeeping period (grace expiry and staleness); 0 derives it
}

// Hooks are optional callbacks. They are called without registry locks held and must not block.
type Hooks struct {
	OnState func(state marketdata.SourceState, at time.Time) // every connection-state change
	OnQuote func(u *Update)                                  // every accepted quote (hot-cache publishing)
}

// Registry is safe for concurrent use.
type Registry struct {
	source  marketdata.MarketDataSource
	clock   clock.Clock
	cfg     Config
	metrics *metrics.Gateway
	log     *slog.Logger
	hooks   Hooks
	machine *connstate.Machine

	ctx       context.Context // lifetime of upstream subscriptions and the registry goroutines
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	startOnce sync.Once
	everReady atomic.Bool

	mu      sync.Mutex
	entries map[string]*entry
	closed  bool
}

type entry struct {
	symbol   string
	decimals int
	subs     atomic.Pointer[[]Subscriber] // copy-on-write; replaced under Registry.mu

	// Guarded by Registry.mu.
	upstream  marketdata.Subscription
	idleSince time.Time // set while there are no subscribers

	mu         sync.Mutex // guards the fields below
	latest     marketdata.Quote
	hasLatest  bool
	lastUpdate time.Time
	stale      bool
}

// New creates a registry. Call Start to begin following the source's status.
func New(source marketdata.MarketDataSource, clk clock.Clock, cfg Config, m *metrics.Gateway, log *slog.Logger, hooks Hooks) (*Registry, error) {
	if cfg.MaxActiveSymbols < 1 || cfg.StaleAfter <= 0 || cfg.UnsubscribeGrace < 0 {
		return nil, errors.New("registry: invalid configuration")
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = deriveSweep(cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Registry{
		source: source, clock: clk, cfg: cfg, metrics: m, log: log, hooks: hooks,
		machine: connstate.New(clk.Now()),
		ctx:     ctx, cancel: cancel,
		entries: map[string]*entry{},
	}, nil
}

// deriveSweep picks a housekeeping period of a quarter of the shortest deadline, between 10ms and 1s.
func deriveSweep(cfg Config) time.Duration {
	d := cfg.StaleAfter
	if cfg.UnsubscribeGrace > 0 && cfg.UnsubscribeGrace < d {
		d = cfg.UnsubscribeGrace
	}
	return max(10*time.Millisecond, min(d/4, time.Second))
}

// Start launches the status follower and the housekeeping goroutine. It is idempotent.
func (r *Registry) Start() {
	r.startOnce.Do(func() {
		r.wg.Add(2)
		go func() { // owned by the registry; ends on Close
			defer r.wg.Done()
			r.followStatus()
		}()
		go func() { // owned by the registry; ends on Close
			defer r.wg.Done()
			r.housekeeping()
		}()
	})
}

// Close stops the goroutines and closes every upstream subscription.
func (r *Registry) Close() {
	r.cancel()
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for sym, e := range r.entries {
		if e.upstream != nil {
			e.upstream.Close()
		}
		delete(r.entries, sym)
	}
	r.metrics.ActiveSymbols.Set(0)
	r.metrics.StaleSymbols.Set(0)
}

// SourceID returns the source kind.
func (r *Registry) SourceID() marketdata.SourceID { return r.source.ID() }

// State returns the current connection state.
func (r *Registry) State() connstate.Snapshot { return r.machine.Current() }

// Ready reports whether the gateway can serve market data: the source has been READY at least once and has
// not given up reconnecting (DISCONNECTED).
func (r *Registry) Ready() bool {
	return r.everReady.Load() && r.machine.Current().State != marketdata.StateDisconnected
}

// ActiveSymbols returns the number of symbols with an upstream subscription (including those in grace).
func (r *Registry) ActiveSymbols() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// Instrument resolves a symbol through the source.
func (r *Registry) Instrument(symbol string) (marketdata.Instrument, error) {
	return r.source.Instrument(symbol)
}

func (r *Registry) usable() bool {
	s := r.machine.Current().State
	return s == marketdata.StateReady || s == marketdata.StateDegraded
}

// Subscribe adds sub to symbol's fan-out. onSnapshot receives the symbol's snapshot before sub joins the
// fan-out, so the subscriber can queue it ahead of any quote; returning false aborts the subscription.
// Subscribing an existing subscriber again only delivers a fresh snapshot.
func (r *Registry) Subscribe(symbol string, sub Subscriber, onSnapshot func(Snapshot) bool) error {
	inst, err := r.source.Instrument(symbol)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	e := r.entries[symbol]
	if e == nil {
		if e, err = r.openLocked(inst); err != nil {
			return err
		}
	}

	e.mu.Lock()
	snapQuote, has, stale := e.latest, e.hasLatest, e.stale
	e.mu.Unlock()
	if !has {
		return ErrSourceUnavailable
	}
	data, err := stream.Encode(stream.NewQuoteMessage(stream.TypeSnapshot, snapQuote, e.decimals, stale))
	if err != nil {
		return fmt.Errorf("registry: encode snapshot: %w", err)
	}
	if !onSnapshot(Snapshot{Symbol: symbol, Sequence: snapQuote.Sequence, Data: data}) {
		return ErrRejected
	}
	subs := *e.subs.Load()
	if !slices.Contains(subs, sub) {
		next := make([]Subscriber, len(subs), len(subs)+1)
		copy(next, subs)
		next = append(next, sub)
		e.subs.Store(&next)
	}
	e.idleSince = time.Time{}
	return nil
}

// openLocked creates the entry and its upstream subscription. Caller holds r.mu.
func (r *Registry) openLocked(inst marketdata.Instrument) (*entry, error) {
	if len(r.entries) >= r.cfg.MaxActiveSymbols && !r.evictIdleLocked() {
		return nil, ErrActiveSymbolLimit
	}
	if !r.usable() {
		return nil, ErrSourceUnavailable
	}
	snap, err := r.source.Snapshot(r.ctx, inst.Symbol)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSourceUnavailable, err)
	}
	e := &entry{symbol: inst.Symbol, decimals: inst.PriceDecimals, latest: snap, hasLatest: true, lastUpdate: r.clock.Now()}
	empty := []Subscriber{}
	e.subs.Store(&empty)
	up, err := r.source.Subscribe(r.ctx, inst.Symbol, r.sinkFor(e))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSourceUnavailable, err)
	}
	e.upstream = up
	r.entries[inst.Symbol] = e
	r.metrics.ActiveSymbols.Set(float64(len(r.entries)))
	return e, nil
}

// evictIdleLocked closes the longest-idle entry without subscribers. Caller holds r.mu.
func (r *Registry) evictIdleLocked() bool {
	var victim *entry
	for _, e := range r.entries {
		if len(*e.subs.Load()) == 0 && (victim == nil || e.idleSince.Before(victim.idleSince)) {
			victim = e
		}
	}
	if victim == nil {
		return false
	}
	r.removeLocked(victim)
	return true
}

func (r *Registry) removeLocked(e *entry) {
	if e.upstream != nil {
		e.upstream.Close()
		e.upstream = nil
	}
	delete(r.entries, e.symbol)
	r.metrics.ActiveSymbols.Set(float64(len(r.entries)))
}

// Unsubscribe removes sub from symbol's fan-out. The upstream subscription closes after the grace period
// once no subscribers remain.
func (r *Registry) Unsubscribe(symbol string, sub Subscriber) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[symbol]
	if e == nil {
		return
	}
	subs := *e.subs.Load()
	i := slices.Index(subs, sub)
	if i < 0 {
		return
	}
	next := make([]Subscriber, 0, len(subs)-1)
	next = append(next, subs[:i]...)
	next = append(next, subs[i+1:]...)
	e.subs.Store(&next)
	if len(next) == 0 {
		if r.cfg.UnsubscribeGrace == 0 {
			r.removeLocked(e)
			return
		}
		e.idleSince = r.clock.Now()
	}
}

// sinkFor returns the upstream sink of an entry. It never blocks and never takes Registry.mu.
func (r *Registry) sinkFor(e *entry) marketdata.QuoteSink {
	return marketdata.QuoteSinkFunc(func(q marketdata.Quote) bool {
		r.ingest(e, q)
		return true
	})
}

func (r *Registry) ingest(e *entry, q marketdata.Quote) {
	now := r.clock.Now()
	r.metrics.QuotesReceived.Inc()
	e.mu.Lock()
	if e.hasLatest && q.Sequence <= e.latest.Sequence {
		e.mu.Unlock()
		return // duplicate or out of order: sequences stay monotonic for clients
	}
	e.latest, e.hasLatest, e.lastUpdate, e.stale = q, true, now, false
	e.mu.Unlock()

	data, err := stream.Encode(stream.NewQuoteMessage(stream.TypeQuote, q, e.decimals, false))
	if err != nil {
		r.log.Error("quote encoding failed", "symbol", e.symbol, "error", err.Error())
		return
	}
	u := &Update{Symbol: e.symbol, Sequence: q.Sequence, Data: data, ReceivedAt: now}
	for _, s := range *e.subs.Load() {
		s.OfferQuote(u)
	}
	if r.hooks.OnQuote != nil {
		r.hooks.OnQuote(u)
	}
}

// followStatus applies the source's status events until Close.
func (r *Registry) followStatus() {
	status := r.source.Status()
	for {
		select {
		case <-r.ctx.Done():
			return
		case ev, ok := <-status:
			if !ok {
				return
			}
			r.applyStatus(ev)
		}
	}
}

func (r *Registry) applyStatus(ev marketdata.StatusEvent) {
	from, err := r.machine.Transition(ev)
	if err != nil {
		r.log.Warn("ignored invalid source state transition", "from", string(from), "to", string(ev.State))
		return
	}
	if from == ev.State {
		return
	}
	r.log.Info("market data source state changed", "from", string(from), "to", string(ev.State),
		"errorCategory", ev.ErrorCategory)
	r.metrics.SetConnectionState(ev.State)
	switch ev.State {
	case marketdata.StateReady:
		r.everReady.Store(true)
		if from != marketdata.StateDegraded {
			r.resubscribeAll()
		}
	case marketdata.StateReconnecting:
		r.metrics.ReconnectTotal.Inc()
	}
	// Clients learn the new state first, then which symbols it made stale.
	if r.hooks.OnState != nil {
		r.hooks.OnState(ev.State, ev.At)
	}
	if ev.State != marketdata.StateReady {
		reason := stream.StaleSourceDisconnected
		if ev.State == marketdata.StateDegraded {
			reason = stream.StaleSourceDegraded
		}
		r.markAllStale(reason, ev.At)
	}
}

// resubscribeAll re-issues the upstream subscription of every active symbol after the source recovered.
// Symbols stay stale until their first fresh quote.
func (r *Registry) resubscribeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.entries {
		if e.upstream != nil {
			e.upstream.Close()
			e.upstream = nil
		}
		up, err := r.source.Subscribe(r.ctx, e.symbol, r.sinkFor(e))
		if err != nil {
			r.log.Warn("resubscription failed; retried on the next recovery", "symbol", e.symbol, "error", err.Error())
			continue
		}
		e.upstream = up
	}
}

// markAllStale marks every subscribed symbol stale and notifies its subscribers.
func (r *Registry) markAllStale(reason string, at time.Time) {
	r.mu.Lock()
	groups := map[Subscriber][]string{}
	for _, e := range r.entries {
		e.mu.Lock()
		e.stale = true
		e.mu.Unlock()
		for _, s := range *e.subs.Load() {
			groups[s] = append(groups[s], e.symbol)
		}
	}
	r.metrics.StaleSymbols.Set(float64(len(r.entries)))
	r.mu.Unlock()
	deliverStale(groups, at, reason)
}

func deliverStale(groups map[Subscriber][]string, at time.Time, reason string) {
	for s, symbols := range groups {
		slices.Sort(symbols)
		s.OfferStale(symbols, at, reason)
	}
}

// housekeeping expires idle entries and detects symbols without updates.
func (r *Registry) housekeeping() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.clock.After(r.cfg.SweepInterval):
			r.sweep()
		}
	}
}

func (r *Registry) sweep() {
	now := r.clock.Now()
	detect := r.usable()
	groups := map[Subscriber][]string{}
	staleCount := 0

	r.mu.Lock()
	for _, e := range r.entries {
		subs := *e.subs.Load()
		if len(subs) == 0 && now.Sub(e.idleSince) >= r.cfg.UnsubscribeGrace {
			r.removeLocked(e)
			continue
		}
		e.mu.Lock()
		newlyStale := detect && !e.stale && now.Sub(e.lastUpdate) >= r.cfg.StaleAfter
		if newlyStale {
			e.stale = true
		}
		if e.stale {
			staleCount++
		}
		e.mu.Unlock()
		if newlyStale {
			for _, s := range subs {
				groups[s] = append(groups[s], e.symbol)
			}
		}
	}
	r.metrics.StaleSymbols.Set(float64(staleCount))
	r.mu.Unlock()
	deliverStale(groups, now, stream.StaleNoUpdates)
}
