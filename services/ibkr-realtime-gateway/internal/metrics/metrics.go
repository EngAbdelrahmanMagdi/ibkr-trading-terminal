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

	SourceInfo            *prometheus.GaugeVec
	UnrepresentablePrices prometheus.Counter

	// IBKR adapter.
	IBKRRequests        *prometheus.CounterVec
	IBKRRequestSeconds  *prometheus.HistogramVec
	IBKRLimiterWait     prometheus.Histogram
	IBKRLimiterRejected *prometheus.CounterVec
	IBKRRateLimited     prometheus.Counter
	IBKRWSMessages      *prometheus.CounterVec
	IBKRSMDRenewals     prometheus.Counter
	IBKRMalformedFrames prometheus.Counter
	IBKRContractLookups *prometheus.CounterVec

	// Order notifications from Kafka.
	OrderNotifications prometheus.Counter
	OrderEventsSkipped *prometheus.CounterVec
	KafkaConsumeErrors prometheus.Counter
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
		SourceInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "source_info",
			Help: "Active market-data source (1) and the configured mode, fixed for the process lifetime."}, []string{"source", "mode"}),
		UnrepresentablePrices: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "unrepresentable_prices_total",
			Help: "Prices the contract cannot represent without rounding; sent as null or left out, never rounded."}),
		IBKRRequests: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "ibkr_requests_total",
			Help: "Requests to the IBKR Client Portal Gateway by endpoint class and outcome."}, []string{"endpoint", "code"}),
		IBKRRequestSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "ibkr_request_seconds",
			Help: "Duration of IBKR requests.", Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}}, []string{"endpoint"}),
		IBKRLimiterWait: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: "ibkr_limiter_wait_seconds",
			Help: "Time requests waited for the IBKR pacing limiter.", Buckets: []float64{0, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5}}),
		IBKRLimiterRejected: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "ibkr_limiter_rejected_total",
			Help: "Requests rejected by the IBKR pacing limiter."}, []string{"endpoint", "reason"}),
		IBKRRateLimited: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "ibkr_rate_limited_total",
			Help: "HTTP 429 responses from IBKR (each starts a cool-down)."}),
		IBKRWSMessages: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "ibkr_ws_messages_total",
			Help: "IBKR websocket messages by direction and topic class."}, []string{"direction", "topic"}),
		IBKRSMDRenewals: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "ibkr_smd_renewals_total",
			Help: "Market-data stream renewals sent before IBKR's stream termination."}),
		IBKRMalformedFrames: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "ibkr_malformed_frames_total",
			Help: "IBKR messages or fields that could not be mapped (dropped, never guessed)."}),
		IBKRContractLookups: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "ibkr_contract_lookups_total",
			Help: "Symbol to contract resolutions by result."}, []string{"result"}),
		OrderNotifications: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "order_notifications_total",
			Help: "Order events fanned out to WebSocket clients as order-update notifications."}),
		OrderEventsSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "order_events_skipped_total",
			Help: "Order-topic records not fanned out, by reason (unknown type, malformed, invalid, too large)."}, []string{"reason"}),
		KafkaConsumeErrors: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "kafka_consume_errors_total",
			Help: "Kafka fetch or offset-commit errors of the order-notification consumer."}),
	}
	g.registry.MustRegister(
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		g.ConnectionState, g.QuotesReceived, g.QuotesForwarded, g.QuotesCoalesced, g.WSClients, g.ActiveSymbols,
		g.StaleSymbols, g.ReconnectTotal, g.ProcessingSeconds, g.Evictions, g.ControlQueueDepth, g.RedisErrors,
		g.CacheHits, g.CacheMisses, g.QuoteCacheWrites, g.BarsRateLimited,
		g.SourceInfo, g.UnrepresentablePrices,
		g.IBKRRequests, g.IBKRRequestSeconds, g.IBKRLimiterWait, g.IBKRLimiterRejected, g.IBKRRateLimited,
		g.IBKRWSMessages, g.IBKRSMDRenewals, g.IBKRMalformedFrames, g.IBKRContractLookups,
		g.OrderNotifications, g.OrderEventsSkipped, g.KafkaConsumeErrors,
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
