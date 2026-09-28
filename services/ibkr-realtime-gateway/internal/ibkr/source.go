package ibkr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/connstate"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/hotcache"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/ibkr/pacing"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/reconnect"
)

// Error categories reported with state changes (short, never containing upstream text).
const (
	CategoryLoginRequired = "LOGIN_REQUIRED"
	CategoryCompeting     = "COMPETING_SESSION"
	CategoryRateLimited   = "RATE_LIMITED"
	CategoryTLS           = "TLS_UNTRUSTED"
	CategoryConnection    = "CONNECTION_LOST"
	CategoryUnexpected    = "UNEXPECTED_RESPONSE"
)

var (
	errLoginRequired = errors.New("brokerage session not authenticated")
	errCompeting     = errors.New("competing brokerage session")
	errNoAccounts    = errors.New("brokerage session has no accounts yet")
	errNoToken       = errors.New("no session token")
	errWSDropped     = errors.New("websocket closed")
	errNotConnected  = errors.New("not connected to IBKR")
	errNoData        = errors.New("no market data received yet")
)

// Config configures the IBKR source.
type Config struct {
	Client         ClientConfig
	Pacing         pacing.Config
	TickleInterval time.Duration    // keepalive; IBKR expects about every 60s
	PingInterval   time.Duration    // websocket "tic"; at least once a minute
	RenewAfter     time.Duration    // smd renewal; streams terminate after 10 minutes
	RenewJitter    time.Duration    // renewals are spread by up to this much earlier
	Reconnect      reconnect.Policy // bounded cycle after a lost connection
	ProbeInterval  time.Duration    // auth-status health check after a cycle is exhausted
	SnapshotWait   time.Duration    // wait for the first frame of a new instrument
	SnapshotLinger time.Duration    // a snapshot keeps its stream open this long for the subscription that follows
	InitWait       time.Duration    // documented pause after /iserver/auth/ssodh/init
	AuthWait       time.Duration    // wait for the websocket's sts confirmation
	WSSendRate     float64          // websocket topic messages per second
	CacheTTL       time.Duration    // contract cache lifetime in Redis
	NegativeTTL    time.Duration    // unknown-symbol memory
	Seed           map[string]int64 // optional symbol -> conid
}

// OrderStreamSink receives the broker order stream of the session: the current day's orders when the stream starts
// (for correlation) and every "sor"/"str" message. The source only transports them; it never interprets orders.
type OrderStreamSink interface {
	Seed(orders json.RawMessage)
	Observe(topic string, args json.RawMessage)
}

// Source is the IBKRMarketDataSource.
type Source struct {
	cfg      Config
	clock    clock.Clock
	metrics  *metrics.Gateway
	log      *slog.Logger
	client   *Client
	limiter  *pacing.Limiter
	resolver *resolver
	status   chan marketdata.StatusEvent

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	emitMu   sync.Mutex
	state    marketdata.SourceState
	degraded string // reason while DEGRADED

	rndMu sync.Mutex
	rnd   *rand.Rand

	mu      sync.Mutex
	conn    *wsConn
	gen     uint64 // connection generation; smd topics are tied to one connection
	streams map[int64]*streamEntry

	orders OrderStreamSink // optional; set before Start
}

type streamEntry struct {
	conid       int64
	symbol      string
	sinks       map[*subscription]marketdata.QuoteSink
	waiters     []chan struct{}
	quote       quoteState
	latest      *marketdata.Quote
	unavailable error
	gen         uint64 // connection the smd was sent on
	renewAt     time.Time
	lingerUntil time.Time // a snapshot keeps the stream open briefly for the subscription that follows
}

var _ marketdata.MarketDataSource = (*Source)(nil)

// New creates the source; Start begins the session lifecycle. store/guard may be nil (no Redis cache).
func New(cfg Config, store hotcache.Store, guard *hotcache.Guard, clk clock.Clock, m *metrics.Gateway, log *slog.Logger) (*Source, error) {
	if err := cfg.Reconnect.Validate(); err != nil {
		return nil, err
	}
	limiter, err := pacing.New(cfg.Pacing, Endpoints(), clk, m)
	if err != nil {
		return nil, err
	}
	client, err := NewClient(cfg.Client, limiter, clk, m, log)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Source{
		cfg: cfg, clock: clk, metrics: m, log: log, client: client, limiter: limiter,
		status: make(chan marketdata.StatusEvent, 64),
		ctx:    ctx, cancel: cancel, done: make(chan struct{}),
		state:   marketdata.StateDisconnected,
		rnd:     rand.New(rand.NewPCG(uint64(clk.Now().UnixNano()), 0x1b)), //nolint:gosec // jitter, not security
		streams: map[int64]*streamEntry{},
	}
	s.resolver = newResolver(client, store, guard, resolverConfig{
		CacheTTL: cfg.CacheTTL, NegativeTTL: cfg.NegativeTTL, MaxEntries: 1000, Seed: cfg.Seed,
	}, clk, m, log)
	client.OnRateLimited(s.onRateLimited)
	return s, nil
}

// SetOrderSink enables the broker order stream (call before Start). Without a sink the stream is not requested.
func (s *Source) SetOrderSink(sink OrderStreamSink) { s.orders = sink }

// Start runs the session lifecycle until Close.
func (s *Source) Start() {
	go func() { // owned by the source; ends on Close
		defer close(s.done)
		s.run()
	}()
}

// ID returns IBKR.
func (s *Source) ID() marketdata.SourceID { return marketdata.SourceIBKR }

// Status returns the connection-state events.
func (s *Source) Status() <-chan marketdata.StatusEvent { return s.status }

// SupportedBars returns the combinations IBKR history can serve.
func (s *Source) SupportedBars() marketdata.BarSet { return SupportedBars }

// Close stops the lifecycle, cancels the market-data streams and closes the websocket. It never logs out.
func (s *Source) Close(ctx context.Context) error {
	s.cancel()
	select {
	case <-s.done:
		s.client.http.CloseIdleConnections()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ----------------------------------------------------------------------------------------- state

// emit records a state change and publishes it; transitions the state machine would reject are skipped.
func (s *Source) emit(state marketdata.SourceState, category string) {
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	if state == s.state || !connstate.Valid(s.state, state) {
		return
	}
	s.state = state
	if state != marketdata.StateDegraded {
		s.degraded = ""
	} else {
		s.degraded = category
	}
	select {
	case s.status <- marketdata.StatusEvent{State: state, ErrorCategory: category, At: s.clock.Now()}:
	case <-s.ctx.Done():
	}
}

func (s *Source) current() (marketdata.SourceState, string) {
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	return s.state, s.degraded
}

func (s *Source) onRateLimited() {
	if st, _ := s.current(); st == marketdata.StateReady {
		s.emit(marketdata.StateDegraded, CategoryRateLimited)
	}
}

func category(err error) string {
	switch {
	case errors.Is(err, errCompeting):
		return CategoryCompeting
	case classOf(err) == ClassTLS:
		return CategoryTLS
	case classOf(err) == ClassRateLimited:
		return CategoryRateLimited
	case classOf(err) == ClassAuth, errors.Is(err, errLoginRequired), errors.Is(err, errNoAccounts):
		return CategoryLoginRequired
	case classOf(err) == ClassMalformed:
		return CategoryUnexpected
	default:
		return CategoryConnection
	}
}

// ----------------------------------------------------------------------------------------- lifecycle

func (s *Source) run() {
	defer s.dropConnection()
	s.emit(marketdata.StateConnecting, "")
	err := s.connect(s.ctx)
	for s.ctx.Err() == nil {
		if err == nil {
			s.emit(marketdata.StateReady, "")
			err = s.serve(s.ctx)
			s.dropConnection()
			if s.ctx.Err() != nil {
				return
			}
			s.log.Warn("IBKR connection lost", "category", category(err))
		}
		if classOf(err) != ClassTLS {
			s.emit(marketdata.StateReconnecting, category(err))
			if err = s.reconnectCycle(); err == nil {
				continue
			}
			if s.ctx.Err() != nil {
				return
			}
		}
		s.emit(marketdata.StateDisconnected, category(err))
		if classOf(err) == ClassTLS {
			s.log.Error("IBKR gateway certificate is not trusted; fix the local CA setup (no retries until the probe succeeds)")
		}
		if !s.probeUntilReady() {
			return
		}
		s.emit(marketdata.StateConnecting, "")
		err = s.connect(s.ctx)
	}
}

// reconnectCycle runs the bounded reconnect framework. A TLS failure ends the cycle at once.
func (s *Source) reconnectCycle() error {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	var permanent, last error
	err := reconnect.Run(ctx, s.cfg.Reconnect, s.clock, s.random(), func(ctx context.Context) error {
		s.emit(marketdata.StateConnecting, "")
		err := s.connect(ctx)
		if err != nil {
			last = err
			s.emit(marketdata.StateReconnecting, category(err))
			if classOf(err) == ClassTLS {
				permanent = err
				cancel()
			}
		}
		return err
	}, func(ri reconnect.RetryInfo) {
		s.log.Info("IBKR reconnect attempt failed", "attempt", ri.Attempt, "category", category(ri.Err), "nextDelayMs", ri.Delay.Milliseconds())
	})
	switch {
	case permanent != nil:
		return permanent
	case err != nil && last != nil && s.ctx.Err() == nil:
		return last // the cycle is exhausted; report why the last attempt failed
	}
	return err
}

// probeUntilReady is a fixed-interval, rate-limited health check (not a retry loop): it asks the gateway
// for the session status and returns true once a session is established.
func (s *Source) probeUntilReady() bool {
	for {
		select {
		case <-s.ctx.Done():
			return false
		case <-s.clock.After(s.cfg.ProbeInterval):
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.cfg.Client.Timeout)
		st, hasEstablished, err := s.client.authStatus(ctx)
		cancel()
		if err == nil && st.ready(hasEstablished) {
			return true
		}
	}
}

// connect establishes the brokerage session and the websocket.
func (s *Source) connect(ctx context.Context) error {
	st, hasEstablished, err := s.client.authStatus(ctx)
	if err != nil {
		return err
	}
	switch {
	case !st.Connected:
		return &Error{Class: ClassAuth, Endpoint: epAuthStatus, Err: errLoginRequired}
	case st.Competing:
		return errCompeting // never take over a session the user holds elsewhere
	case !st.ready(hasEstablished):
		// Connected but the brokerage session is not (yet) initialized: initialize it without competing.
		if err := s.client.initBrokerage(ctx); err != nil {
			return err
		}
		if err := s.sleep(ctx, s.cfg.InitWait); err != nil {
			return err
		}
		if st, hasEstablished, err = s.client.authStatus(ctx); err != nil {
			return err
		}
		if st.Competing {
			return errCompeting
		}
		if !st.ready(hasEstablished) {
			return &Error{Class: ClassAuth, Endpoint: epAuthStatus, Err: errLoginRequired}
		}
	}
	s.emit(marketdata.StateAuthenticating, "")
	ok, err := s.client.accountsReady(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return &Error{Class: ClassAuth, Endpoint: epAccounts, Err: errNoAccounts}
	}
	token, _, _, err := s.client.tickle(ctx)
	if err != nil {
		return err
	}
	if token == "" {
		return &Error{Class: ClassAuth, Endpoint: epTickle, Err: errNoToken}
	}
	s.mu.Lock()
	gen := s.gen + 1
	s.mu.Unlock()
	var onOrders orderHandler
	if s.orders != nil {
		onOrders = s.orders.Observe
	}
	conn, err := s.client.dialWS(ctx, token, s.cfg.WSSendRate, func(conid int64, fields map[string]json.RawMessage) {
		s.onFrame(gen, conid, fields)
	}, onOrders)
	if err != nil {
		return err
	}
	select {
	case <-conn.authed:
	case <-conn.authLost:
		conn.shutdown(time.Second)
		return &Error{Class: ClassAuth, Endpoint: "websocket", Err: errLoginRequired}
	case <-conn.done:
		conn.shutdown(time.Second)
		return &Error{Class: ClassTransient, Endpoint: "websocket", Err: errWSDropped}
	case <-s.clock.After(s.cfg.AuthWait):
		// The REST status already says the session is established; sts confirmation is best effort.
	case <-ctx.Done():
		conn.shutdown(time.Second)
		return ctx.Err()
	}
	s.mu.Lock()
	s.conn, s.gen = conn, gen
	s.mu.Unlock()
	s.startOrderStream(ctx, conn)
	return nil
}

// startOrderStream seeds the correlation of the order stream with the current day's orders (IBKR advises reading
// them before subscribing), then subscribes to order updates ("sor") and executions ("str", replaying the day's
// executions after every connect; duplicates are harmless downstream). Failures are logged: missed updates are
// recovered by reconciliation.
func (s *Source) startOrderStream(ctx context.Context, conn *wsConn) {
	if s.orders == nil {
		return
	}
	if orders, err := s.client.liveOrders(ctx); err != nil {
		s.log.Warn("order stream: current orders unavailable; correlation starts from live updates", "error", err.Error())
	} else {
		s.orders.Seed(orders)
	}
	for _, topic := range []string{"sor+{}", `str+{"realtimeUpdatesOnly":false,"days":1}`} {
		if err := conn.send(topic); err != nil {
			s.log.Warn("order stream subscription not sent", "error", err.Error())
		}
	}
}

func (s *Source) sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.clock.After(d):
		return nil
	}
}

// serve keeps the session alive until the connection is lost.
func (s *Source) serve(ctx context.Context) error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	check := min(s.cfg.RenewAfter/8, 10*time.Second)
	tickleC, pingC, checkC := s.clock.After(s.cfg.TickleInterval), s.clock.After(s.cfg.PingInterval), s.clock.After(check)
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-conn.done:
			return &Error{Class: ClassTransient, Endpoint: "websocket", Err: errWSDropped}
		case <-conn.authLost:
			return &Error{Class: ClassAuth, Endpoint: "websocket", Err: errLoginRequired}
		case <-tickleC:
			tickleC = s.clock.After(s.cfg.TickleInterval)
			if err := s.keepalive(ctx, &failures); err != nil {
				return err
			}
		case <-pingC:
			pingC = s.clock.After(s.cfg.PingInterval)
			_ = conn.send("tic")
		case <-checkC:
			checkC = s.clock.After(check)
			s.maintainStreams()
			if st, reason := s.current(); st == marketdata.StateDegraded && reason == CategoryRateLimited && !s.limiter.CoolingDown() {
				s.emit(marketdata.StateReady, "")
			}
		}
	}
}

// keepalive tickles the session and follows its status.
func (s *Source) keepalive(ctx context.Context, failures *int) error {
	tctx, cancel := context.WithTimeout(ctx, s.cfg.Client.Timeout)
	defer cancel()
	_, st, hasEstablished, err := s.client.tickle(tctx)
	switch {
	case err != nil && classOf(err) == ClassRateLimited:
		return nil // DEGRADED via the 429 callback; the next tickle checks again
	case err != nil && classOf(err) == ClassAuth:
		return err
	case err != nil:
		*failures++
		if *failures >= 2 {
			return err
		}
		return nil
	}
	*failures = 0
	switch {
	case st.Competing:
		s.emit(marketdata.StateDegraded, CategoryCompeting)
	case !st.Connected || !st.Authenticated:
		return &Error{Class: ClassAuth, Endpoint: epTickle, Err: errLoginRequired}
	case st.ready(hasEstablished):
		if cur, reason := s.current(); cur == marketdata.StateDegraded && reason == CategoryCompeting {
			s.emit(marketdata.StateReady, "")
		}
	}
	return nil
}

// dropConnection closes the current websocket; its smd topics end with it.
func (s *Source) dropConnection() {
	s.mu.Lock()
	conn := s.conn
	s.conn = nil
	if conn != nil {
		for conid := range s.streams {
			_ = conn.send(umd(conid))
		}
	}
	s.mu.Unlock()
	if conn != nil {
		conn.shutdown(2 * time.Second)
	}
}

// ----------------------------------------------------------------------------------------- streams

func smd(conid int64) string {
	return "smd+" + strconv.FormatInt(conid, 10) + `+{"fields":["` + strings.Join(streamFields, `","`) + `"]}`
}

func umd(conid int64) string { return "umd+" + strconv.FormatInt(conid, 10) + "+{}" }

func (s *Source) random() *rand.Rand {
	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	return rand.New(rand.NewPCG(s.rnd.Uint64(), s.rnd.Uint64())) //nolint:gosec // jitter, not security
}

func (s *Source) renewDeadline(now time.Time) time.Time {
	jitter := time.Duration(0)
	if s.cfg.RenewJitter > 0 {
		s.rndMu.Lock()
		jitter = time.Duration(s.rnd.Int64N(int64(s.cfg.RenewJitter)))
		s.rndMu.Unlock()
	}
	return now.Add(s.cfg.RenewAfter - jitter)
}

// openStreamLocked returns the instrument's stream, sending smd on the current connection if needed.
// Caller holds s.mu.
func (s *Source) openStreamLocked(c contract) (*streamEntry, error) {
	if s.conn == nil {
		return nil, &Error{Class: ClassTransient, Endpoint: "websocket", Err: errNotConnected}
	}
	e := s.streams[c.Conid]
	if e == nil {
		e = &streamEntry{conid: c.Conid, symbol: c.Symbol, sinks: map[*subscription]marketdata.QuoteSink{}, quote: quoteState{symbol: c.Symbol}}
		s.streams[c.Conid] = e
	}
	if e.gen != s.gen {
		if err := s.conn.send(smd(c.Conid)); err != nil {
			return nil, &Error{Class: ClassTransient, Endpoint: "websocket", Err: err}
		}
		now := s.clock.Now()
		e.gen, e.renewAt, e.unavailable = s.gen, s.renewDeadline(now), nil
	}
	return e, nil
}

// maintainStreams renews streams before IBKR terminates them and closes streams nobody uses.
func (s *Source) maintainStreams() {
	now := s.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for conid, e := range s.streams {
		if len(e.sinks) == 0 && now.After(e.lingerUntil) {
			if s.conn != nil && e.gen == s.gen {
				_ = s.conn.send(umd(conid))
			}
			delete(s.streams, conid)
			continue
		}
		if s.conn != nil && e.gen == s.gen && !now.Before(e.renewAt) {
			if s.conn.send(smd(conid)) == nil {
				s.metrics.IBKRSMDRenewals.Inc()
				e.renewAt = s.renewDeadline(now)
			}
		}
	}
}

// onFrame merges one market-data frame and delivers the result. Frames arrive on the connection's single
// reader goroutine, so delivery order is preserved.
func (s *Source) onFrame(gen uint64, conid int64, fields map[string]json.RawMessage) {
	s.mu.Lock()
	e := s.streams[conid]
	if e == nil || gen != s.gen {
		s.mu.Unlock()
		return
	}
	res := e.quote.merge(fields, s.clock.Now())
	if res.malformed > 0 {
		s.metrics.IBKRMalformedFrames.Add(float64(res.malformed))
	}
	if res.unwireable > 0 {
		s.metrics.UnrepresentablePrices.Add(float64(res.unwireable))
	}
	if res.unavailable == nil && !res.emit {
		s.mu.Unlock()
		return
	}
	if res.emit {
		q := res.quote
		e.latest = &q
	}
	e.unavailable = res.unavailable
	sinks := make([]marketdata.QuoteSink, 0, len(e.sinks))
	for _, sink := range e.sinks {
		sinks = append(sinks, sink)
	}
	for _, w := range e.waiters {
		close(w)
	}
	e.waiters = nil
	s.mu.Unlock()
	for _, sink := range sinks {
		if res.unavailable != nil {
			sink.SymbolUnavailable(res.unavailable)
		} else {
			sink.OfferQuote(res.quote)
		}
	}
}

// ----------------------------------------------------------------------------------------- port

// Instrument resolves the symbol to its US stock listing.
func (s *Source) Instrument(symbol string) (marketdata.Instrument, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.Client.Timeout)
	defer cancel()
	c, err := s.resolver.resolve(ctx, symbol)
	if err != nil {
		return marketdata.Instrument{}, err
	}
	// Price precision is taken from the data itself (never assumed); -1 marks "no fixed decimals".
	return marketdata.Instrument{Symbol: c.Symbol, Name: c.Name, Currency: c.Currency, PriceDecimals: -1}, nil
}

// Snapshot opens (or reuses) the instrument's stream and waits briefly for its first usable frame.
func (s *Source) Snapshot(ctx context.Context, symbol string) (marketdata.Quote, error) {
	c, err := s.resolver.resolve(ctx, symbol)
	if err != nil {
		return marketdata.Quote{}, err
	}
	s.mu.Lock()
	e, err := s.openStreamLocked(c)
	if err != nil {
		s.mu.Unlock()
		return marketdata.Quote{}, err
	}
	e.lingerUntil = s.clock.Now().Add(s.cfg.SnapshotWait + s.cfg.SnapshotLinger)
	if q, ok, err := e.result(); ok {
		s.mu.Unlock()
		return q, err
	}
	w := make(chan struct{})
	e.waiters = append(e.waiters, w)
	s.mu.Unlock()

	select {
	case <-w:
	case <-s.clock.After(s.cfg.SnapshotWait):
	case <-ctx.Done():
		return marketdata.Quote{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok, err := e.result(); ok {
		return q, err
	}
	return marketdata.Quote{}, &Error{Class: ClassTransient, Endpoint: "websocket", Err: errNoData}
}

// result returns the latest quote or the unavailability error, if either is known. Caller holds s.mu.
func (e *streamEntry) result() (marketdata.Quote, bool, error) {
	switch {
	case e.unavailable != nil:
		return marketdata.Quote{}, true, e.unavailable
	case e.latest != nil:
		return *e.latest, true, nil
	}
	return marketdata.Quote{}, false, nil
}

// Subscribe attaches sink to the instrument's stream.
func (s *Source) Subscribe(ctx context.Context, symbol string, sink marketdata.QuoteSink) (marketdata.Subscription, error) {
	c, err := s.resolver.resolve(ctx, symbol)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.openStreamLocked(c)
	if err != nil {
		return nil, err
	}
	sub := &subscription{src: s, conid: c.Conid}
	e.sinks[sub] = sink
	return sub, nil
}

type subscription struct {
	src   *Source
	conid int64
	once  sync.Once
}

// Close detaches the sink; the stream is cancelled once nobody uses it.
func (sub *subscription) Close() {
	sub.once.Do(func() {
		s := sub.src
		s.mu.Lock()
		defer s.mu.Unlock()
		e := s.streams[sub.conid]
		if e == nil {
			return
		}
		delete(e.sinks, sub)
		if len(e.sinks) == 0 && s.clock.Now().After(e.lingerUntil) {
			if s.conn != nil && e.gen == s.gen {
				_ = s.conn.send(umd(sub.conid))
			}
			delete(s.streams, sub.conid)
		}
	})
}

// Bars fetches historical bars through the pacing limiter.
func (s *Source) Bars(ctx context.Context, symbol string, interval marketdata.Interval, rng marketdata.Range) ([]marketdata.Bar, error) {
	if !SupportedBars.Supports(interval, rng) {
		return nil, fmt.Errorf("%w: %s/%s", marketdata.ErrUnsupportedRange, interval, rng)
	}
	c, err := s.resolver.resolve(ctx, symbol)
	if err != nil {
		return nil, err
	}
	bars, malformed, err := s.client.history(ctx, c.Conid, interval, rng)
	if malformed > 0 {
		s.metrics.IBKRMalformedFrames.Add(float64(malformed))
	}
	return bars, err
}

// ProbeSession reports whether an established brokerage session is available (used by AUTO mode). It makes
// a single status request.
func ProbeSession(ctx context.Context, cfg Config, clk clock.Clock, m *metrics.Gateway, log *slog.Logger) error {
	limiter, err := pacing.New(cfg.Pacing, Endpoints(), clk, m)
	if err != nil {
		return err
	}
	client, err := NewClient(cfg.Client, limiter, clk, m, log)
	if err != nil {
		return err
	}
	defer client.http.CloseIdleConnections()
	st, hasEstablished, err := client.authStatus(ctx)
	if err != nil {
		return err
	}
	if !st.ready(hasEstablished) {
		return errLoginRequired
	}
	return nil
}
