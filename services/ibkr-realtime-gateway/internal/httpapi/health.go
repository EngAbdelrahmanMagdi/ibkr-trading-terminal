package httpapi

import (
	"net/http"
	"sync/atomic"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/connstate"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// HealthDetail is the body of GET /health.
type HealthDetail struct {
	Status            string  `json:"status"`
	ConnectionState   string  `json:"connectionState"`
	Since             string  `json:"since"`
	ActiveSymbols     int64   `json:"activeSymbols"`
	WsClients         int64   `json:"wsClients"`
	LastErrorCategory *string `json:"lastErrorCategory"`
}

// StateProvider reports the market-data connection state.
type StateProvider interface {
	State() connstate.Snapshot
	Ready() bool
	ActiveSymbols() int
}

// Health serves liveness, readiness, detailed health and (optionally) metrics on the internal port.
// Readiness depends only on the market-data source; Redis is not a readiness dependency.
type Health struct {
	state        StateProvider
	clients      func() int64
	metrics      http.Handler
	shuttingDown atomic.Bool
}

// NewHealth creates the internal endpoints. metrics may be nil.
func NewHealth(state StateProvider, clients func() int64, metrics http.Handler) *Health {
	return &Health{state: state, clients: clients, metrics: metrics}
}

// SetShuttingDown makes readiness fail so that no new traffic is routed to the instance.
func (h *Health) SetShuttingDown() { h.shuttingDown.Store(true) }

// Handler returns the internal routes.
func (h *Health) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /liveness", func(w http.ResponseWriter, _ *http.Request) { plain(w, http.StatusOK, "ok") })
	ready := func(w http.ResponseWriter, _ *http.Request) {
		switch {
		case h.shuttingDown.Load():
			plain(w, http.StatusServiceUnavailable, "shutting down")
		case !h.state.Ready():
			plain(w, http.StatusServiceUnavailable, "market data source not ready")
		default:
			plain(w, http.StatusOK, "ready")
		}
	}
	mux.HandleFunc("GET /readiness", ready)
	mux.HandleFunc("GET /ready", ready)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		st := h.state.State()
		detail := HealthDetail{
			Status: healthStatus(st.State), ConnectionState: string(st.State), Since: stream.FormatTime(st.Since),
			ActiveSymbols: int64(h.state.ActiveSymbols()), WsClients: h.clients(),
		}
		if st.ErrorCategory != "" {
			category := st.ErrorCategory
			detail.LastErrorCategory = &category
		}
		if h.shuttingDown.Load() {
			detail.Status = "DOWN"
		}
		writeJSON(w, http.StatusOK, "application/json", detail)
	})
	if h.metrics != nil {
		mux.Handle("GET /metrics", h.metrics)
	}
	return mux
}

func healthStatus(s marketdata.SourceState) string {
	switch s {
	case marketdata.StateReady:
		return "UP"
	case marketdata.StateDisconnected:
		return "DOWN"
	default:
		return "DEGRADED"
	}
}

func plain(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}
