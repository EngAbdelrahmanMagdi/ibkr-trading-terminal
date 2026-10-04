// Package ws is the WebSocket transport of the gateway. It is the only package that depends on the WebSocket
// library; protocol messages live in package stream and market data comes through the subscription registry.
package ws

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/origin"
	"github.com/coder/websocket"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/registry"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// Config holds the transport limits and timeouts.
type Config struct {
	AllowedOrigins         []string      // exact complete origins; no-Origin internal clients remain supported
	MaxInboundMessageBytes int64         // larger client messages close the connection with 1009
	MaxSymbolsPerSubscribe int           // per subscribe message
	MaxSubscribedSymbols   int           // per connection
	HeartbeatInterval      time.Duration // heartbeat message and ping period
	WriteTimeout           time.Duration // per frame write timeout (exceeding it evicts the client); also the pong timeout
	ControlQueueSize       int           // bounded control queue per connection; overflow evicts the client
	FlushInterval          time.Duration // minimum period between two quotes of the same symbol to one connection
	LagThreshold           time.Duration // a flush delivering data older than this counts as lagging
	MaxLaggingFlushes      int           // more consecutive lagging flushes than this evict the client
	MaxConnections         int           // concurrent connections; further upgrade requests get 503
	CommandsPerSecond      int
	CommandBurst           int
	MaxConnectionsPerPeer  int
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
	cfg      Config
	registry *registry.Registry
	clock    clock.Clock
	metrics  *metrics.Gateway
	log      *slog.Logger

	slots chan struct{} // bounded: one slot per concurrent connection

	ctx    context.Context // server lifetime; cancelled by Shutdown
	cancel context.CancelFunc

	mu       sync.Mutex
	closing  bool
	sessions map[*session]struct{}
	peers    map[string]int
	wg       sync.WaitGroup // one entry per active handler
}

// NewServer creates a WebSocket server.
func NewServer(cfg Config, reg *registry.Registry, clk clock.Clock, m *metrics.Gateway, log *slog.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg: cfg, registry: reg, clock: clk, metrics: m, log: log,
		slots: make(chan struct{}, cfg.MaxConnections),
		ctx:   ctx, cancel: cancel,
		sessions: map[*session]struct{}{},
		peers:    map[string]int{},
	}
}

// Clients returns the number of connected clients.
func (s *Server) Clients() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.sessions))
}

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
	peer, _, peerError := net.SplitHostPort(r.RemoteAddr)
	if peerError != nil {
		peer = r.RemoteAddr
	}
	s.mu.Lock()
	full := s.cfg.MaxConnectionsPerPeer > 0 && s.peers[peer] >= s.cfg.MaxConnectionsPerPeer
	if !full {
		s.peers[peer]++
	}
	s.mu.Unlock()
	if full {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	defer func() {
		s.mu.Lock()
		s.peers[peer]--
		if s.peers[peer] == 0 {
			delete(s.peers, peer)
		}
		s.mu.Unlock()
	}()

	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}

	if !origin.Allowed(r.Header.Get("Origin"), s.cfg.AllowedOrigins) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	// The exact policy above is authoritative; avoid the library's implicit same-host exception.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		s.log.Info("websocket upgrade rejected", "category", "invalid_upgrade")
		return
	}
	conn.SetReadLimit(s.cfg.MaxInboundMessageBytes)

	started := s.clock.Now()
	sess := newSession(s, conn)
	sess.run()
	code, reason := sess.closeStatus()
	s.log.Info("websocket session ended", "durationMs", s.clock.Now().Sub(started).Milliseconds(),
		"closeCode", int(code), "closeReason", reason)
}

// register adds a session and queues its initial connection message atomically with respect to
// BroadcastState, so that a client never misses or reorders a state change.
func (s *Server) register(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sess] = struct{}{}
	s.metrics.WSClients.Set(float64(len(s.sessions)))
	st := s.registry.State()
	if data, err := stream.Encode(stream.NewConnection(st.State, s.registry.SourceID(), s.clock.Now(), s.cfg.Limits())); err == nil {
		sess.lastState = st.State
		sess.enqueueControl(frame{data: data})
	}
}

func (s *Server) unregister(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sess)
	s.metrics.WSClients.Set(float64(len(s.sessions)))
}

// BroadcastState sends a connection message with the new source state to every client. A client that
// registered after the transition but before this broadcast already received the state and is skipped.
func (s *Server) BroadcastState(state marketdata.SourceState, at time.Time) {
	data, err := stream.Encode(stream.NewConnection(state, s.registry.SourceID(), at, s.cfg.Limits()))
	if err != nil {
		s.log.Error("connection message encoding failed", "error", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for sess := range s.sessions {
		if sess.lastState != state {
			sess.lastState = state
			sess.enqueueControl(frame{data: data})
		}
	}
}

// BroadcastOrderUpdate queues an order notification for every connected client and returns how many received it.
// It uses the bounded control queue, so a client that cannot keep up is evicted as for any control message.
func (s *Server) BroadcastOrderUpdate(msg stream.OrderUpdate) int {
	data, err := stream.Encode(msg)
	if err != nil {
		s.log.Error("order-update encoding failed", "error", err.Error())
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delivered := 0
	for sess := range s.sessions {
		if sess.enqueueControl(frame{data: data}) {
			delivered++
		}
	}
	return delivered
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
