package ws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// outbound is a queued frame. symbol is set for quote frames so that quotes of a symbol that was unsubscribed
// after the frame was queued are dropped instead of sent.
type outbound struct {
	symbol string
	data   []byte
}

// session serves one connection.
//
// Goroutines: the HTTP handler goroutine runs the read loop; the session starts exactly one writer goroutine.
// Subscriptions own their goroutines inside the source and are closed before the session ends.
type session struct {
	srv  *Server
	conn *websocket.Conn

	ctx    context.Context // cancelled when the session must end (derived from the server lifetime)
	cancel context.CancelFunc
	out    chan outbound // bounded send queue (Config.SendQueueSize); only the writer receives from it

	mu   sync.Mutex
	subs map[string]marketdata.Subscription // mutated only by the read loop; read by the writer

	closeOnce   sync.Once
	closeCode   websocket.StatusCode
	closeReason string
}

func newSession(srv *Server, conn *websocket.Conn) *session {
	ctx, cancel := context.WithCancel(srv.ctx)
	return &session{
		srv: srv, conn: conn, ctx: ctx, cancel: cancel,
		out:  make(chan outbound, srv.cfg.SendQueueSize),
		subs: map[string]marketdata.Subscription{},
	}
}

// run serves the connection until it closes. Order on exit: stop the session, close subscriptions (no more
// quotes are produced), then wait for the writer, which performs the close handshake.
func (s *session) run() {
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		s.writeLoop()
	}()

	s.send(stream.NewConnection(stream.StateReady, s.srv.source.ID(), s.srv.clock.Now(), s.srv.cfg.Limits()))
	s.readLoop()

	s.fail(websocket.StatusNormalClosure, "")
	s.unsubscribeAll()
	<-writerDone
}

// fail records the close status (first caller wins) and ends the session.
func (s *session) fail(code websocket.StatusCode, reason string) {
	s.closeOnce.Do(func() { s.closeCode, s.closeReason = code, reason })
	s.cancel()
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
		if _, ok := s.subs[sym]; !ok {
			added++
		}
	}
	total := len(s.subs) + added
	s.mu.Unlock()
	if total > cfg.MaxSubscribedSymbols {
		s.sendError(stream.ErrSubscriptionLimit, fmt.Sprintf("at most %d subscribed symbols per connection", cfg.MaxSubscribedSymbols), symbols)
		return
	}

	var unknown []string
	for _, sym := range symbols {
		inst, err := s.srv.source.Instrument(sym)
		if errors.Is(err, marketdata.ErrUnknownSymbol) {
			unknown = append(unknown, sym)
			continue
		}
		if err != nil {
			s.sendError(stream.ErrInternal, "instrument lookup failed", []string{sym})
			continue
		}
		snap, err := s.srv.source.Snapshot(s.ctx, sym)
		if err != nil {
			s.sendError(stream.ErrInternal, "snapshot unavailable", []string{sym})
			continue
		}
		// The snapshot is queued before the subscription starts, so it always precedes the symbol's quotes.
		if !s.send(stream.NewQuoteMessage(stream.TypeSnapshot, snap, inst.PriceDecimals)) {
			return
		}
		if s.subscribed(sym) {
			continue // re-subscription: a fresh snapshot only
		}
		sub, err := s.srv.source.Subscribe(s.ctx, sym, s.sinkFor(sym, inst.PriceDecimals))
		if err != nil {
			s.sendError(stream.ErrInternal, "subscription failed", []string{sym})
			continue
		}
		s.mu.Lock()
		s.subs[sym] = sub
		s.mu.Unlock()
	}
	if len(unknown) > 0 {
		s.sendError(stream.ErrUnknownSymbol, "unknown symbols", unknown)
	}
}

func (s *session) unsubscribe(symbols []string) {
	for _, sym := range symbols {
		s.mu.Lock()
		sub, ok := s.subs[sym]
		delete(s.subs, sym)
		s.mu.Unlock()
		if ok {
			sub.Close()
		}
	}
}

func (s *session) unsubscribeAll() {
	s.mu.Lock()
	subs := make([]marketdata.Subscription, 0, len(s.subs))
	for sym, sub := range s.subs {
		subs = append(subs, sub)
		delete(s.subs, sym)
	}
	s.mu.Unlock()
	for _, sub := range subs {
		sub.Close()
	}
}

func (s *session) subscribed(symbol string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.subs[symbol]
	return ok
}

// sinkFor returns the sink that turns a symbol's quotes into queued frames.
func (s *session) sinkFor(symbol string, decimals int) marketdata.QuoteSink {
	return marketdata.QuoteSinkFunc(func(q marketdata.Quote) bool {
		data, err := stream.Encode(stream.NewQuoteMessage(stream.TypeQuote, q, decimals))
		if err != nil {
			return false
		}
		return s.enqueue(outbound{symbol: symbol, data: data})
	})
}

// enqueue adds a frame without blocking. A full queue ends the session with 1013 (try again later).
func (s *session) enqueue(m outbound) bool {
	if s.ctx.Err() != nil {
		return false
	}
	select {
	case s.out <- m:
		return true
	default:
		s.fail(websocket.StatusTryAgainLater, "send queue full")
		return false
	}
}

func (s *session) send(v any) bool {
	data, err := stream.Encode(v)
	if err != nil {
		s.fail(websocket.StatusInternalError, "encoding failed")
		return false
	}
	return s.enqueue(outbound{data: data})
}

func (s *session) sendError(code, message string, symbols []string) {
	s.send(stream.NewError(code, message, symbols))
}

// writeLoop is the only goroutine writing to the connection. When the session ends it performs the close
// handshake with the recorded status.
func (s *session) writeLoop() {
	heartbeat := time.NewTicker(s.srv.cfg.HeartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-s.ctx.Done():
			code, reason := s.closeStatus()
			_ = s.conn.Close(code, reason) // bounded by the library's close handshake timeout
			return
		case m := <-s.out:
			if m.symbol != "" && !s.subscribed(m.symbol) {
				continue
			}
			if err := s.write(m.data); err != nil {
				s.fail(websocket.StatusGoingAway, "write failed")
			}
		case <-heartbeat.C:
			if !s.heartbeat() {
				s.fail(websocket.StatusGoingAway, "peer unresponsive")
			}
		}
	}
}

func (s *session) write(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.srv.cfg.WriteTimeout)
	defer cancel()
	return s.conn.Write(ctx, websocket.MessageText, data)
}

// heartbeat sends a heartbeat message and verifies the peer answers a ping within the write timeout.
func (s *session) heartbeat() bool {
	data, err := stream.Encode(stream.NewHeartbeat(s.srv.clock.Now()))
	if err != nil || s.write(data) != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.srv.cfg.WriteTimeout)
	defer cancel()
	return s.conn.Ping(ctx) == nil
}
