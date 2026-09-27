// Package registry owns the gateway's upstream market-data subscriptions and fans quotes out to clients.
//
//   - One upstream subscription per symbol, however many clients want it. It is reference counted and, when
//     the last subscriber leaves, closed after a grace period so that quick re-subscriptions cause no churn.
//   - The number of active symbols is capped (the broker's market-data lines).
//   - Per symbol it keeps the latest quote (for snapshots), its freshness and the stale flag.
//   - Subscriber sets are copy-on-write, so fan-out on every quote takes no lock.
//   - It follows the source's connection state, marks symbols stale, and resubscribes after recovery.
//
// Locking: Registry.mu guards the entry map and is never held during source calls (which may be network
// requests). A symbol being opened is represented by an entry whose opening channel is still open; concurrent
// subscribers wait for it. Lock order: Registry.mu before entry.mu. The quote ingest path never acquires
// Registry.mu.
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
	ErrSourceUnavailable = marketdata.ErrSourceUnavailable
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
	// OfferUnavailable reports that the symbol's subscription ended because the source cannot deliver usable
	// data for it (err wraps marketdata.ErrSymbolUnavailable or marketdata.ErrRateLimited).
	OfferUnavailable(symbol string, err error)
}

// Config bounds the registry.
type Config struct {
	MaxActiveSymbols int
	UnsubscribeGrace time.Duration // 0 closes the upstream subscription immediately
	StaleAfter       time.Duration // no update for this long marks a symbol stale
	SweepInterval    time.Duration // housekeeping period (grace expiry and staleness); 0 derives it
	OpenTimeout      time.Duration // bound on opening a symbol at the source; 0 means 10s
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
	symbol  string
	subs    atomic.Pointer[[]Subscriber] // copy-on-write; replaced under Registry.mu
	opening chan struct{}                // closed once the upstream is open (or opening failed)
	openErr error                        // set before opening is closed

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
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 10 * time.Second
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
	r.closed = true
	var ups []marketdata.Subscription
	for sym, e := range r.entries {
		if e.upstream != nil {
			ups = append(ups, e.upstream)
			e.upstream = nil
		}
		delete(r.entries, sym)
	}
	r.metrics.ActiveSymbols.Set(0)
	r.metrics.StaleSymbols.Set(0)
	r.mu.Unlock()
	closeAll(ups)
}

func closeAll(ups []marketdata.Subscription) {
	for _, up := range ups {
		up.Close()
	}
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
	if _, err := r.source.Instrument(symbol); err != nil {
		return sourceError(err)
	}
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return ErrClosed
		}
		e := r.entries[symbol]
		if e == nil {
			var evicted []marketdata.Subscription
			if len(r.entries) >= r.cfg.MaxActiveSymbols {
				victim := r.idleVictimLocked()
				if victim == nil {
					r.mu.Unlock()
					return ErrActiveSymbolLimit
				}
				evicted = append(evicted, r.removeLocked(victim)...)
			}
			if !r.usable() {
				r.mu.Unlock()
				closeAll(evicted)
				return ErrSourceUnavailable
			}
			e = newEntry(symbol)
			r.entries[symbol] = e
			r.metrics.ActiveSymbols.Set(float64(len(r.entries)))
			r.mu.Unlock()
			closeAll(evicted)
			return r.open(e, sub, onSnapshot)
		}
		select {
		case <-e.opening:
		default:
			r.mu.Unlock()
			if err := r.awaitOpen(e); err != nil {
				return err
			}
			continue
		}
		if e.openErr != nil { // a failed entry that has not been removed yet
			r.mu.Unlock()
			return e.openErr
		}
		err := r.handOverLocked(e, sub, onSnapshot)
		r.mu.Unlock()
		return err
	}
}

func newEntry(symbol string) *entry {
	e := &entry{symbol: symbol, opening: make(chan struct{})}
	empty := []Subscriber{}
	e.subs.Store(&empty)
	return e
}

func (r *Registry) awaitOpen(e *entry) error {
	select {
	case <-e.opening:
		return e.openErr
	case <-r.ctx.Done():
		return ErrClosed
	}
}

// open takes the snapshot and opens the upstream subscription without holding Registry.mu, then publishes
// the entry and hands the first subscriber its snapshot in one critical section (so housekeeping can never
// expire the new entry before it has a subscriber). The snapshot is recorded before subscribing, so a quote
// delivered by the new subscription can never be overwritten by it.
func (r *Registry) open(e *entry, sub Subscriber, onSnapshot func(Snapshot) bool) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.OpenTimeout)
	defer cancel()
	up, err := r.openUpstream(ctx, e)

	r.mu.Lock()
	switch {
	case err != nil:
		e.openErr = err
	case r.closed || r.entries[e.symbol] != e: // closed or removed while opening
		e.openErr = ErrClosed
	default:
		e.upstream = up
		up = nil
	}
	var handOverErr error
	if e.openErr != nil {
		if r.entries[e.symbol] == e {
			delete(r.entries, e.symbol)
			r.metrics.ActiveSymbols.Set(float64(len(r.entries)))
		}
	} else {
		e.idleSince = r.clock.Now() // grace applies if the first subscriber rejects the snapshot
		handOverErr = r.handOverLocked(e, sub, onSnapshot)
	}
	close(e.opening)
	openErr := e.openErr
	r.mu.Unlock()
	if up != nil {
		up.Close()
	}
	if openErr != nil {
		return openErr
	}
	return handOverErr
}

func (r *Registry) openUpstream(ctx context.Context, e *entry) (marketdata.Subscription, error) {
	snap, err := r.source.Snapshot(ctx, e.symbol)
	if err != nil {
		return nil, sourceError(err)
	}
	e.mu.Lock()
	e.latest, e.hasLatest, e.lastUpdate = snap, true, r.clock.Now()
	e.mu.Unlock()
	up, err := r.source.Subscribe(r.ctx, e.symbol, r.sinkFor(e))
	if err != nil {
		return nil, sourceError(err)
	}
	return up, nil
}

// sourceError keeps the source's classified errors and maps everything else to ErrSourceUnavailable.
func sourceError(err error) error {
	switch {
	case errors.Is(err, marketdata.ErrUnknownSymbol), errors.Is(err, marketdata.ErrSymbolUnavailable),
		errors.Is(err, marketdata.ErrRateLimited), errors.Is(err, marketdata.ErrSourceUnavailable):
		return err
	default:
		return fmt.Errorf("%w: %v", ErrSourceUnavailable, err)
	}
}

// handOverLocked gives the subscriber the current snapshot and adds it to the fan-out. Caller holds r.mu.
func (r *Registry) handOverLocked(e *entry, sub Subscriber, onSnapshot func(Snapshot) bool) error {
	e.mu.Lock()
	snapQuote, has, stale := e.latest, e.hasLatest, e.stale
	e.mu.Unlock()
	if !has {
		return ErrSourceUnavailable
	}
	data, err := stream.Encode(stream.NewQuoteMessage(stream.TypeSnapshot, snapQuote, stale))
	if err != nil {
		return fmt.Errorf("registry: encode snapshot: %w", err)
	}
	if !onSnapshot(Snapshot{Symbol: e.symbol, Sequence: snapQuote.Sequence, Data: data}) {
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

// idleVictimLocked returns the longest-idle open entry without subscribers. Caller holds r.mu.
func (r *Registry) idleVictimLocked() *entry {
	var victim *entry
	for _, e := range r.entries {
		if e.upstream != nil && len(*e.subs.Load()) == 0 && (victim == nil || e.idleSince.Before(victim.idleSince)) {
			victim = e
		}
	}
	return victim
}

// removeLocked deletes an entry and returns its upstream subscription for the caller to close after
// releasing r.mu.
func (r *Registry) removeLocked(e *entry) []marketdata.Subscription {
	var ups []marketdata.Subscription
	if e.upstream != nil {
		ups = append(ups, e.upstream)
		e.upstream = nil
	}
	delete(r.entries, e.symbol)
	r.metrics.ActiveSymbols.Set(float64(len(r.entries)))
	return ups
}

// Unsubscribe removes sub from symbol's fan-out. The upstream subscription closes after the grace period
// once no subscribers remain.
func (r *Registry) Unsubscribe(symbol string, sub Subscriber) {
	r.mu.Lock()
	e := r.entries[symbol]
	if e == nil {
		r.mu.Unlock()
		return
	}
	subs := *e.subs.Load()
	i := slices.Index(subs, sub)
	if i < 0 {
		r.mu.Unlock()
		return
	}
	next := make([]Subscriber, 0, len(subs)-1)
	next = append(next, subs[:i]...)
	next = append(next, subs[i+1:]...)
	e.subs.Store(&next)
	var ups []marketdata.Subscription
	if len(next) == 0 {
		if r.cfg.UnsubscribeGrace == 0 {
			ups = r.removeLocked(e)
		} else {
			e.idleSince = r.clock.Now()
		}
	}
	r.mu.Unlock()
	closeAll(ups)
}

// entrySink is the upstream sink of one entry. It never blocks and never takes Registry.mu.
type entrySink struct {
	r *Registry
	e *entry
}

func (s entrySink) OfferQuote(q marketdata.Quote) bool {
	s.r.ingest(s.e, q)
	return true
}

// SymbolUnavailable ends the symbol: it is removed and every subscriber is told why. The removal runs on a
// registry goroutine because it closes the upstream subscription, which must not happen on the source's own
// delivery path.
func (s entrySink) SymbolUnavailable(err error) {
	s.r.wg.Add(1)
	go func() { // owned by the registry; bounded by the number of active symbols
		defer s.r.wg.Done()
		s.r.drop(s.e, err)
	}()
}

func (r *Registry) sinkFor(e *entry) marketdata.QuoteSink { return entrySink{r: r, e: e} }

func (r *Registry) drop(e *entry, err error) {
	r.mu.Lock()
	var ups []marketdata.Subscription
	if r.entries[e.symbol] == e {
		ups = r.removeLocked(e)
	}
	subs := *e.subs.Load()
	empty := []Subscriber{}
	e.subs.Store(&empty)
	r.mu.Unlock()
	closeAll(ups)
	r.log.Warn("market data unavailable for symbol; subscription ended", "symbol", e.symbol, "reason", err.Error())
	for _, s := range subs {
		s.OfferUnavailable(e.symbol, err)
	}
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

	data, err := stream.Encode(stream.NewQuoteMessage(stream.TypeQuote, q, false))
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

// resubscribeAll re-issues the upstream subscription of every open symbol after the source recovered. The
// source calls happen without Registry.mu. Symbols stay stale until their first fresh quote.
func (r *Registry) resubscribeAll() {
	r.mu.Lock()
	var entries []*entry
	var old []marketdata.Subscription
	for _, e := range r.entries {
		select {
		case <-e.opening:
		default:
			continue // still opening; it subscribes against the recovered source itself
		}
		if e.openErr != nil {
			continue
		}
		if e.upstream != nil {
			old = append(old, e.upstream)
			e.upstream = nil
		}
		entries = append(entries, e)
	}
	r.mu.Unlock()
	closeAll(old)

	for _, e := range entries {
		up, err := r.source.Subscribe(r.ctx, e.symbol, r.sinkFor(e))
		if err != nil {
			r.log.Warn("resubscription failed; retried on the next recovery", "symbol", e.symbol, "error", err.Error())
			continue
		}
		r.mu.Lock()
		if r.closed || r.entries[e.symbol] != e {
			r.mu.Unlock()
			up.Close()
			continue
		}
		e.upstream = up
		r.mu.Unlock()
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
	var expired []marketdata.Subscription

	r.mu.Lock()
	for _, e := range r.entries {
		select {
		case <-e.opening:
		default:
			continue // still opening
		}
		subs := *e.subs.Load()
		if len(subs) == 0 && now.Sub(e.idleSince) >= r.cfg.UnsubscribeGrace {
			expired = append(expired, r.removeLocked(e)...)
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
	closeAll(expired)
	deliverStale(groups, now, stream.StaleNoUpdates)
}
