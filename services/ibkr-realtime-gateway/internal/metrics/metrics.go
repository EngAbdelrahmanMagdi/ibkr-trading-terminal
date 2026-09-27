// Package metrics defines the gateway's Prometheus metrics on a dedicated registry.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

// Eviction reasons for slow consumers.
const (
	EvictWriteTimeout   = "write_timeout"      // a frame could not be written within the write timeout
	EvictControlQueue   = "control_queue_full" // the bounded control queue overflowed
	EvictLaggingFlushes = "lagging_flushes"    // too many consecutive flushes delivered data older than the lag threshold
	EvictPongTimeout    = "pong_timeout"       // the peer did not answer a ping in time (stalled or backlogged)
)

var states = []marketdata.SourceState{
	marketdata.StateDisconnected, marketdata.StateConnecting, marketdata.StateAuthenticating,
	marketdata.StateReady, marketdata.StateDegraded, marketdata.StateReconnecting,
}

// Gateway holds every gateway metric. All methods are safe for concurrent use.
type Gateway struct {
	registry *prometheus.Registry

	ConnectionState   *prometheus.GaugeVec
	QuotesReceived    prometheus.Counter
	QuotesForwarded   prometheus.Counter
	QuotesCoalesced   prometheus.Counter
	WSClients         prometheus.Gauge
	ActiveSymbols     prometheus.Gauge
	StaleSymbols      prometheus.Gauge
	ReconnectTotal    prometheus.Counter
	ProcessingSeconds prometheus.Histogram
	Evictions         *prometheus.CounterVec
	ControlQueueDepth prometheus.Histogram
	RedisErrors       *prometheus.CounterVec
	CacheHits         *prometheus.CounterVec
	CacheMisses       *prometheus.CounterVec
	QuoteCacheWrites  prometheus.Counter
	BarsRateLimited   prometheus.Counter
}

// New creates the metrics, registered on a new registry together with the Go runtime and process collectors.
func New() *Gateway {
	const ns = "realtime_gateway"
	g := &Gateway{
		registry: prometheus.NewRegistry(),
		ConnectionState: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "connection_state",
			Help: "Market-data source connection state (1 for the current state)."}, []string{"state"}),
		QuotesReceived: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "quotes_received_total",
			Help: "Normalized quotes received from the market-data source."}),
		QuotesForwarded: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "quotes_forwarded_total",
			Help: "Quote messages written to WebSocket clients."}),
		QuotesCoalesced: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "quotes_coalesced_total",
			Help: "Quotes replaced by a newer quote before being sent to a client."}),
		WSClients: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "ws_clients",
			Help: "Connected WebSocket clients."}),
		ActiveSymbols: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "active_symbols",
			Help: "Symbols with an active upstream subscription."}),
		StaleSymbols: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "stale_symbols",
			Help: "Active symbols currently marked stale."}),
		ReconnectTotal: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "reconnect_total",
			Help: "Transitions of the market-data source into RECONNECTING."}),
		ProcessingSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: "message_processing_seconds",
			Help:    "Time from receiving a normalized quote to writing it to a WebSocket client (includes coalescing delay).",
			Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}}),
		Evictions: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "slow_consumer_evictions_total",
			Help: "WebSocket clients disconnected for being too slow."}, []string{"reason"}),
		ControlQueueDepth: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: "control_queue_depth",
			Help: "Depth of a client's control queue when a message is enqueued.", Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64, 128}}),
		RedisErrors: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "redis_errors_total",
			Help: "Failed Redis operations (Redis is non-critical)."}, []string{"op"}),
		CacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "cache_hits_total",
			Help: "Cache hits."}, []string{"cache"}),
		CacheMisses: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "cache_misses_total",
			Help: "Cache misses."}, []string{"cache"}),
		QuoteCacheWrites: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "quote_cache_writes_total",
			Help: "Latest-quote entries written to Redis."}),
		BarsRateLimited: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "bars_rate_limited_total",
			Help: "Bars requests rejected because the computation limit was reached."}),
	}
	g.registry.MustRegister(
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		g.ConnectionState, g.QuotesReceived, g.QuotesForwarded, g.QuotesCoalesced, g.WSClients, g.ActiveSymbols,
		g.StaleSymbols, g.ReconnectTotal, g.ProcessingSeconds, g.Evictions, g.ControlQueueDepth, g.RedisErrors,
		g.CacheHits, g.CacheMisses, g.QuoteCacheWrites, g.BarsRateLimited,
	)
	for _, reason := range []string{EvictWriteTimeout, EvictControlQueue, EvictLaggingFlushes, EvictPongTimeout} {
		g.Evictions.WithLabelValues(reason)
	}
	g.SetConnectionState(marketdata.StateDisconnected)
	return g
}

// SetConnectionState sets the gauge of the current state to 1 and all others to 0.
func (g *Gateway) SetConnectionState(current marketdata.SourceState) {
	for _, s := range states {
		v := 0.0
		if s == current {
			v = 1
		}
		g.ConnectionState.WithLabelValues(string(s)).Set(v)
	}
}

// Handler serves the metrics in the Prometheus exposition format.
func (g *Gateway) Handler() http.Handler {
	return promhttp.HandlerFor(g.registry, promhttp.HandlerOpts{Registry: g.registry})
}

// Registry exposes the underlying registry (used by tests to read values).
func (g *Gateway) Registry() *prometheus.Registry { return g.registry }
