package ws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/registry"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// conn is the subset of *websocket.Conn a session uses; tests substitute slow or blocked peers.
type conn interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Ping(ctx context.Context) error
	Close(code websocket.StatusCode, reason string) error
	CloseNow() error
}

// frame is a queued control message. A snapshot frame arms its symbol's quote slot once written, so quotes
// of that symbol can never overtake their snapshot.
type frame struct {
	data   []byte
	arm    string // symbol whose slot is armed after this frame is written
	armSeq int64  // snapshot sequence; arming only applies if it is still the slot's latest snapshot
}

// slot holds the latest undelivered quote of one subscribed symbol: newer quotes replace older ones.
type slot struct {
	symbol       string
	armed        bool  // the snapshot has been written; quotes may flow
	minSeq       int64 // quotes at or below the snapshot sequence are dropped
	pending      *registry.Update
	pendingSince time.Time // receive time of the oldest update replaced by pending (lag measurement)
	lastSent     time.Time // when a quote of this symbol was last written (per-symbol rate limit)
	dirty        bool      // listed in session.dirty
	removed      bool
}

// session serves one connection.
//
// Outbound paths: a bounded control queue (connection, snapshot, stale, error), written first and never
// dropped (overflow evicts the client); and one latest-value slot per subscribed symbol, each sent at most once
// per FlushInterval. Memory per session is bounded by ControlQueueSize frames plus one quote per symbol.
//
// Goroutines: the HTTP handler goroutine runs the read loop; the session starts exactly one writer goroutine.
type session struct {
	srv  *Server
	conn conn

	ctx     context.Context // cancelled when the session must end (derived from the server lifetime)
	cancel  context.CancelFunc
	control chan frame    // bounded; only the writer receives
	wake    chan struct{} // capacity 1: an armed slot has a pending quote

	mu    sync.Mutex
	slots map[string]*slot
	dirty []*slot
	spare []*slot // reused backing array for dirty

	lastState marketdata.SourceState // last connection state queued; guarded by Server.mu

	// Writer-only state.
	batch   []*registry.Update
	lagging int

	closeOnce   sync.Once
	closeCode   websocket.StatusCode
	closeReason string
	abrupt      atomic.Bool // the peer is unresponsive: skip the close handshake
}

func newSession(srv *Server, c conn) *session {
	ctx, cancel := context.WithCancel(srv.ctx)
	return &session{
		srv: srv, conn: c, ctx: ctx, cancel: cancel,
		control: make(chan frame, srv.cfg.ControlQueueSize),
		wake:    make(chan struct{}, 1),
		slots:   map[string]*slot{},
	}
}

// run serves the connection until it closes. Order on exit: stop the session, leave the fan-out (no more
// quotes arrive), then wait for the writer, which performs the close handshake.
func (s *session) run() {
	writerDone := make(chan struct{})
	go func() { // owned by the session; ends when the session context is cancelled
		defer close(writerDone)
		s.writeLoop()
	}()

	s.srv.register(s)
	s.readLoop()

	s.fail(websocket.StatusNormalClosure, "")
	s.unsubscribeAll()
	s.srv.unregister(s)
	<-writerDone
}

// fail records the close status (first caller wins) and ends the session.
func (s *session) fail(code websocket.StatusCode, reason string) {
	s.closeOnce.Do(func() { s.closeCode, s.closeReason = code, reason })
	s.cancel()
}

// evict closes a slow client with 1013 (try again later). An unresponsive peer (write or pong timeout) is
// dropped without a close handshake, which could otherwise hold the session for the library's handshake
// timeouts; a peer that still reads receives the close frame.
func (s *session) evict(reason string) {
	if s.ctx.Err() == nil {
		s.srv.metrics.Evictions.WithLabelValues(reason).Inc()
		if reason == metrics.EvictWriteTimeout || reason == metrics.EvictPongTimeout {
			s.abrupt.Store(true)
		}
	}
	s.fail(websocket.StatusTryAgainLater, "slow consumer")
}

// closeStatus returns the recorded close status; without one it is 1001 during server shutdown, else 1000.
func (s *session) closeStatus() (websocket.StatusCode, string) {
	s.closeOnce.Do(func() {
		if s.srv.ctx.Err() != nil {
			s.closeCode, s.closeReason = websocket.StatusGoingAway, "server shutting down"
		} else {
			s.closeCode = websocket.StatusNormalClosure
		}
	})
	return s.closeCode, s.closeReason
}

// readLoop processes client messages until the connection closes. Reads are not bound to the session
// context: cancelling a read would drop the connection without a close handshake. The writer closes the
// connection when the session ends, which unblocks the read.
func (s *session) readLoop() {
	for {
		typ, data, err := s.conn.Read(context.Background())
		if err != nil {
			return // peer closed, message too big (1009 already sent), or closed by the writer
		}
		if s.ctx.Err() != nil {
			continue // session is ending; keep reading until the close handshake completes
		}
		if typ != websocket.MessageText {
			s.sendError(stream.ErrInvalidMessage, "only JSON text messages are supported", nil)
			continue
		}
		msg, err := stream.ParseClientMessage(data)
		if err != nil {
			s.sendError(stream.ErrInvalidMessage, "message does not follow the protocol", nil)
			continue
		}
		switch msg.Type {
		case stream.TypeSubscribe:
			s.subscribe(msg.Symbols)
		case stream.TypeUnsubscribe:
			s.unsubscribe(msg.Symbols)
		}
	}
}

func (s *session) subscribe(symbols []string) {
	cfg := s.srv.cfg
	if len(symbols) > cfg.MaxSymbolsPerSubscribe {
		s.sendError(stream.ErrSubscriptionLimit, fmt.Sprintf("at most %d symbols per subscribe message", cfg.MaxSymbolsPerSubscribe), nil)
		return
	}
	s.mu.Lock()
	added := 0
	for _, sym := range symbols {
		if _, ok := s.slots[sym]; !ok {
			added++
		}
	}
	total := len(s.slots) + added
	s.mu.Unlock()
	if total > cfg.MaxSubscribedSymbols {
		s.sendError(stream.ErrSubscriptionLimit, fmt.Sprintf("at most %d subscribed symbols per connection", cfg.MaxSubscribedSymbols), symbols)
		return
	}

	var unknown, limited, unavailable, failed []string
	for _, sym := range symbols {
		err := s.srv.registry.Subscribe(sym, s, s.prepare)
		switch {
		case err == nil:
		case errors.Is(err, marketdata.ErrUnknownSymbol):
			unknown = append(unknown, sym)
		case errors.Is(err, registry.ErrActiveSymbolLimit):
			limited = append(limited, sym)
		case errors.Is(err, registry.ErrSourceUnavailable):
			unavailable = append(unavailable, sym)
		case errors.Is(err, registry.ErrRejected), errors.Is(err, registry.ErrClosed):
			return // the session or the gateway is ending
		default:
			failed = append(failed, sym)
		}
	}
	if len(unknown) > 0 {
		s.sendError(stream.ErrUnknownSymbol, "unknown symbols", unknown)
	}
	if len(limited) > 0 {
		s.sendError(stream.ErrSubscriptionLimit, "the gateway's active symbol limit is reached", limited)
	}
	if len(unavailable) > 0 {
		s.sendError(stream.ErrSourceUnavailable, "market data is currently unavailable", unavailable)
	}
	if len(failed) > 0 {
		s.sendError(stream.ErrInternal, "subscription failed", failed)
	}
}

// prepare is called by the registry with the symbol's snapshot, before the session joins the fan-out: it
// creates (or re-arms) the slot and queues the snapshot.
func (s *session) prepare(snap registry.Snapshot) bool {
	s.mu.Lock()
	sl := s.slots[snap.Symbol]
	if sl == nil {
		sl = &slot{symbol: snap.Symbol}
		s.slots[snap.Symbol] = sl
	}
	sl.armed, sl.minSeq = false, snap.Sequence
	if sl.pending != nil && sl.pending.Sequence <= snap.Sequence {
		sl.pending = nil // superseded by the snapshot; flush skips the empty dirty slot
	}
	s.mu.Unlock()
	return s.enqueueControl(frame{data: snap.Data, arm: snap.Symbol, armSeq: snap.Sequence})
}

func (s *session) unsubscribe(symbols []string) {
	var removed []string
	s.mu.Lock()
	for _, sym := range symbols {
		if sl := s.slots[sym]; sl != nil {
			sl.removed = true
			delete(s.slots, sym)
			removed = append(removed, sym)
		}
	}
	s.mu.Unlock()
	for _, sym := range removed {
		s.srv.registry.Unsubscribe(sym, s)
	}
}

func (s *session) unsubscribeAll() {
	s.mu.Lock()
	symbols := make([]string, 0, len(s.slots))
	for sym, sl := range s.slots {
		sl.removed = true
		delete(s.slots, sym)
		symbols = append(symbols, sym)
	}
	s.mu.Unlock()
	for _, sym := range symbols {
		s.srv.registry.Unsubscribe(sym, s)
	}
}

// OfferQuote implements registry.Subscriber: the quote replaces any pending quote of its symbol.
func (s *session) OfferQuote(u *registry.Update) {
	s.mu.Lock()
	sl := s.slots[u.Symbol]
	if sl == nil || u.Sequence <= sl.minSeq {
		s.mu.Unlock()
		return
	}
	replaced := sl.pending != nil
	if !replaced {
		sl.pendingSince = u.ReceivedAt
	}
	sl.pending = u
	if !sl.dirty {
		sl.dirty = true
		s.dirty = append(s.dirty, sl)
	}
	armed := sl.armed
	s.mu.Unlock()
	if replaced {
		s.srv.metrics.QuotesCoalesced.Inc()
	}
	if armed {
		s.signal()
	}
}

// OfferStale implements registry.Subscriber.
func (s *session) OfferStale(symbols []string, since time.Time, reason string) {
	s.send(stream.NewStale(symbols, since, reason))
}

func (s *session) signal() {
	select {
	case s.wake <- struct{}{}:
	default: // a wake-up is already pending
	}
}

// enqueueControl queues a control frame without blocking. A full queue evicts the client.
func (s *session) enqueueControl(f frame) bool {
	if s.ctx.Err() != nil {
		return false
	}
	select {
	case s.control <- f:
		s.srv.metrics.ControlQueueDepth.Observe(float64(len(s.control)))
		return true
	default:
		s.evict(metrics.EvictControlQueue)
		return false
	}
}

func (s *session) send(v any) bool {
	data, err := stream.Encode(v)
	if err != nil {
		s.fail(websocket.StatusInternalError, "encoding failed")
		return false
	}
	return s.enqueueControl(frame{data: data})
}

func (s *session) sendError(code, message string, symbols []string) {
	s.send(stream.NewError(code, message, symbols))
}

// writeLoop is the only goroutine writing to the connection. Control frames always go first. Quotes are
// flushed as soon as they arrive, but each symbol is sent to this client at most once per FlushInterval: a
// symbol updating faster is coalesced (latest value wins) without delaying other symbols, and a slow client
// receives fewer updates instead of stalling anyone else. When the session ends it closes the connection.
func (s *session) writeLoop() {
	heartbeat := time.NewTicker(s.srv.cfg.HeartbeatInterval)
	defer heartbeat.Stop()
	var (
		retry   <-chan time.Time // fires when a rate-limited symbol becomes eligible again
		retryAt time.Time
	)
	flush := func() {
		if !s.drainControl() {
			return
		}
		if wait, ok := s.flush(); ok {
			at := s.srv.clock.Now().Add(wait)
			if retry == nil || at.Before(retryAt) {
				retry, retryAt = s.srv.clock.After(wait), at
			}
		}
	}
	for {
		select {
		case <-s.ctx.Done():
			code, reason := s.closeStatus()
			if s.abrupt.Load() {
				_ = s.conn.CloseNow()
			} else {
				_ = s.conn.Close(code, reason) // bounded by the library's close handshake timeouts
			}
			return
		case f := <-s.control:
			if s.writeControl(f) {
				s.drainControl()
			}
		case <-s.wake:
			flush()
		case <-retry:
			retry = nil
			flush()
		case <-heartbeat.C:
			if s.drainControl() && !s.heartbeat() {
				s.evict(metrics.EvictPongTimeout)
			}
		}
	}
}

// drainControl writes every queued control frame. It returns false once the session is ending.
func (s *session) drainControl() bool {
	for {
		select {
		case f := <-s.control:
			if !s.writeControl(f) {
				return false
			}
		default:
			return s.ctx.Err() == nil
		}
	}
}

func (s *session) writeControl(f frame) bool {
	if !s.write(f.data) {
		return false
	}
	if f.arm == "" {
		return true
	}
	s.mu.Lock()
	sl := s.slots[f.arm]
	wake := false
	if sl != nil && sl.minSeq == f.armSeq {
		sl.armed = true
		wake = sl.pending != nil
	}
	s.mu.Unlock()
	if wake {
		s.signal()
	}
	return true
}

// flush writes the pending quote of every armed slot whose symbol was not sent within the last
// FlushInterval. It returns how long until the next rate-limited slot becomes eligible, if any.
func (s *session) flush() (time.Duration, bool) {
	interval := s.srv.cfg.FlushInterval
	now := s.srv.clock.Now()
	var (
		oldest  time.Time
		wait    time.Duration
		waiting bool
	)
	s.mu.Lock()
	keep := s.spare[:0]
	batch := s.batch[:0]
	for _, sl := range s.dirty {
		switch {
		case sl.removed || sl.pending == nil:
			sl.dirty, sl.pending = false, nil
		case !sl.armed:
			keep = append(keep, sl) // waits for its snapshot to be written
		case now.Sub(sl.lastSent) < interval:
			keep = append(keep, sl) // rate limited: newer quotes keep replacing pending meanwhile
			if w := interval - now.Sub(sl.lastSent); !waiting || w < wait {
				wait, waiting = w, true
			}
		default:
			batch = append(batch, sl.pending)
			if oldest.IsZero() || sl.pendingSince.Before(oldest) {
				oldest = sl.pendingSince
			}
			sl.dirty, sl.pending, sl.lastSent = false, nil, now
		}
	}
	s.spare, s.dirty = s.dirty[:0], keep
	s.mu.Unlock()

	sent := len(batch)
	for i, u := range batch {
		if !s.write(u.Data) {
			clear(batch[i:])
			s.batch = batch[:0]
			return 0, false
		}
		s.srv.metrics.QuotesForwarded.Inc()
		s.srv.metrics.ProcessingSeconds.Observe(s.srv.clock.Now().Sub(u.ReceivedAt).Seconds())
	}
	clear(batch)
	s.batch = batch[:0]

	if sent > 0 {
		if s.srv.clock.Now().Sub(oldest) > s.srv.cfg.LagThreshold {
			s.lagging++
			if s.lagging > s.srv.cfg.MaxLaggingFlushes {
				s.evict(metrics.EvictLaggingFlushes)
			}
		} else {
			s.lagging = 0
		}
	}
	return wait, waiting
}

// write sends one frame within WriteTimeout. A timeout evicts the client as a slow consumer.
func (s *session) write(data []byte) bool {
	ctx, cancel := context.WithTimeout(context.Background(), s.srv.cfg.WriteTimeout)
	err := s.conn.Write(ctx, websocket.MessageText, data)
	timedOut := ctx.Err() != nil
	cancel()
	switch {
	case err == nil:
		return true
	case timedOut || errors.Is(err, context.DeadlineExceeded):
		s.evict(metrics.EvictWriteTimeout)
	default:
		s.fail(websocket.StatusGoingAway, "write failed")
	}
	return false
}

// heartbeat sends a heartbeat message and verifies the peer answers a ping within the write timeout.
func (s *session) heartbeat() bool {
	data, err := stream.Encode(stream.NewHeartbeat(s.srv.clock.Now()))
	if err != nil || !s.write(data) {
		return err == nil // a failed write already ended the session
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.srv.cfg.WriteTimeout)
	defer cancel()
	return s.conn.Ping(ctx) == nil
}
