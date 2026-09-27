package httpapi

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
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

// Health serves liveness, readiness and detailed health on the internal port.
type Health struct {
	source       marketdata.MarketDataSource
	clients      func() int64
	since        time.Time
	shuttingDown atomic.Bool
}

// NewHealth creates the health endpoints. The simulator source is ready as soon as the process runs.
func NewHealth(source marketdata.MarketDataSource, clients func() int64, clk clock.Clock) *Health {
	return &Health{source: source, clients: clients, since: clk.Now()}
}

// SetShuttingDown makes readiness fail so that no new traffic is routed to the instance.
func (h *Health) SetShuttingDown() { h.shuttingDown.Store(true) }

// Handler returns the health routes.
func (h *Health) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /liveness", func(w http.ResponseWriter, _ *http.Request) { plain(w, http.StatusOK, "ok") })
	ready := func(w http.ResponseWriter, _ *http.Request) {
		if h.shuttingDown.Load() {
			plain(w, http.StatusServiceUnavailable, "shutting down")
			return
		}
		plain(w, http.StatusOK, "ready")
	}
	mux.HandleFunc("GET /readiness", ready)
	mux.HandleFunc("GET /ready", ready)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		detail := HealthDetail{
			Status: "UP", ConnectionState: stream.StateReady, Since: stream.FormatTime(h.since),
			ActiveSymbols: int64(h.source.ActiveSymbols()), WsClients: h.clients(),
		}
		if h.shuttingDown.Load() {
			detail.Status, detail.ConnectionState = "DOWN", "DISCONNECTED"
		}
		writeJSON(w, http.StatusOK, "application/json", detail)
	})
	return mux
}

func plain(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}
