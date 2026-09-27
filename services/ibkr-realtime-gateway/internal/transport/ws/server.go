// Package ws is the WebSocket transport of the gateway. It is the only package that depends on the WebSocket
// library; protocol messages live in package stream and market data comes through marketdata.MarketDataSource.
//
// Phase scope: one session per connection with its own subscriptions and one bounded send queue. A shared
// subscription registry, quote coalescing and slow-consumer handling are intentionally not implemented here.
package ws

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// Config holds the transport limits and timeouts.
type Config struct {
	AllowedOrigins         []string      // origin host patterns (path.Match); requests without Origin (non-browser) are allowed
	MaxInboundMessageBytes int64         // larger client messages close the connection with 1009
	MaxSymbolsPerSubscribe int           // per subscribe message
	MaxSubscribedSymbols   int           // per connection
	HeartbeatInterval      time.Duration // heartbeat message and ping period
	WriteTimeout           time.Duration // per frame write timeout; also the ping (pong) timeout
	SendQueueSize          int           // bounded per-connection send queue; when full the connection closes with 1013
	MaxConnections         int           // concurrent connections; further upgrade requests get 503
}

// Limits returns the protocol limits advertised to clients.
func (c Config) Limits() stream.Limits {
	return stream.Limits{
		MaxSymbolsPerSubscribe: c.MaxSymbolsPerSubscribe,
		MaxSubscribedSymbols:   c.MaxSubscribedSymbols,
		MaxInboundMessageBytes: c.MaxInboundMessageBytes,
		HeartbeatIntervalMs:    c.HeartbeatInterval.Milliseconds(),
	}
}

// Server accepts WebSocket connections and runs one session per connection.
type Server struct {
	cfg    Config
	source marketdata.MarketDataSource
	clock  clock.Clock
	log    *slog.Logger

	slots chan struct{} // bounded: one slot per concurrent connection

	ctx    context.Context // server lifetime; cancelled by Shutdown
	cancel context.CancelFunc

	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup // one entry per active handler
	clients atomic.Int64
}

// NewServer creates a WebSocket server.
func NewServer(cfg Config, source marketdata.MarketDataSource, clk clock.Clock, log *slog.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg: cfg, source: source, clock: clk, log: log,
		slots: make(chan struct{}, cfg.MaxConnections),
		ctx:   ctx, cancel: cancel,
	}
}

// Clients returns the number of connected clients.
func (s *Server) Clients() int64 { return s.clients.Load() }

// enter registers a handler unless the server is shutting down.
func (s *Server) enter() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.wg.Add(1)
	return true
}

// ServeHTTP upgrades the request and runs the session until it ends.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.enter() {
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}
	defer s.wg.Done()

	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.cfg.AllowedOrigins})
	if err != nil {
		s.log.Info("websocket upgrade rejected", "error", err.Error(), "origin", r.Header.Get("Origin"))
		return
	}
	conn.SetReadLimit(s.cfg.MaxInboundMessageBytes)

	s.clients.Add(1)
	defer s.clients.Add(-1)
	started := s.clock.Now()
	sess := newSession(s, conn)
	sess.run()
	code, reason := sess.closeStatus()
	s.log.Info("websocket session ended", "durationMs", s.clock.Now().Sub(started).Milliseconds(),
		"closeCode", int(code), "closeReason", reason)
}

// Shutdown stops accepting connections, closes every session with 1001 (going away) and waits for all
// sessions to end or ctx to expire.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	s.cancel()

	done := make(chan struct{})
	go func() { // owned by Shutdown; ends once every session has ended
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("websocket shutdown: %w", ctx.Err())
	}
}
