package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/hotcache"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// BarJSON is one bar on the wire.
type BarJSON struct {
	Time   string `json:"time"`
	Open   string `json:"open"`
	High   string `json:"high"`
	Low    string `json:"low"`
	Close  string `json:"close"`
	Volume int64  `json:"volume"`
}

// BarsResponse is the body of GET /api/v1/market/bars. Bars holds the encoded bar array, which is what the
// cache stores.
type BarsResponse struct {
	Symbol   string          `json:"symbol"`
	Interval string          `json:"interval"`
	Range    string          `json:"range"`
	Source   string          `json:"source"`
	Cached   bool            `json:"cached"`
	Bars     json.RawMessage `json:"bars"`
}

// ErrBusy is returned when the concurrent bar computation limit is reached.
var ErrBusy = errors.New("bars: computation limit reached")

// BarsConfig configures bar serving.
type BarsConfig struct {
	MaxConcurrent  int           // concurrent uncached computations; further requests get RATE_LIMITED
	ComputeTimeout time.Duration // bound on one computation
	MaxCacheTTL    time.Duration // cache TTL is a quarter of the interval, capped at this value
}

// BarsService computes bars and caches them in Redis when a store is configured. Concurrent identical
// requests share one computation (single-flight).
type BarsService struct {
	source  marketdata.MarketDataSource
	store   hotcache.Store // nil disables the cache
	guard   *hotcache.Guard
	cfg     BarsConfig
	metrics *metrics.Gateway
	sem     chan struct{}
	group   singleflight.Group
}

// NewBarsService creates the service. store and guard may be nil (no cache).
func NewBarsService(source marketdata.MarketDataSource, store hotcache.Store, guard *hotcache.Guard, cfg BarsConfig, m *metrics.Gateway) *BarsService {
	return &BarsService{source: source, store: store, guard: guard, cfg: cfg, metrics: m, sem: make(chan struct{}, cfg.MaxConcurrent)}
}

// BarsKey is the Redis key of a cached bar array. Keys are namespaced by source, so simulated bars can never
// be served as broker data, or the other way round.
func BarsKey(source marketdata.SourceID, symbol string, interval marketdata.Interval, rng marketdata.Range) string {
	return "bars:" + string(source) + ":" + symbol + ":" + string(interval) + ":" + string(rng)
}

// CacheTTL returns the cache lifetime for an interval: a quarter of the interval, capped. The newest (open)
// bar of a cached response is therefore at most one TTL old.
func (b *BarsService) CacheTTL(interval marketdata.Interval) time.Duration {
	return min(interval.Duration()/4, b.cfg.MaxCacheTTL)
}

func (b *BarsService) cacheUsable() bool { return b.store != nil && b.guard.Available() }

// Get returns the encoded bar array and whether it came from the cache.
func (b *BarsService) Get(ctx context.Context, inst marketdata.Instrument, interval marketdata.Interval, rng marketdata.Range) (json.RawMessage, bool, error) {
	key := BarsKey(b.source.ID(), inst.Symbol, interval, rng)
	if b.cacheUsable() {
		rctx, cancel := b.guard.Context(ctx)
		v, err := b.store.Get(rctx, key)
		cancel()
		switch {
		case err == nil:
			b.metrics.CacheHits.WithLabelValues("bars").Inc()
			return v, true, nil
		case errors.Is(err, hotcache.ErrMiss):
			b.metrics.CacheMisses.WithLabelValues("bars").Inc()
		default:
			b.guard.Fail("bars_get", err)
		}
	}
	v, err, _ := b.group.Do(key, func() (any, error) { return b.compute(ctx, inst, interval, rng, key) })
	if err != nil {
		return nil, false, err
	}
	return v.(json.RawMessage), false, nil
}

// compute runs one bounded computation and stores the result. It is detached from the first caller's
// cancellation because other callers may share it; ComputeTimeout bounds it instead.
func (b *BarsService) compute(ctx context.Context, inst marketdata.Instrument, interval marketdata.Interval, rng marketdata.Range, key string) (json.RawMessage, error) {
	select {
	case b.sem <- struct{}{}:
		defer func() { <-b.sem }()
	default:
		b.metrics.BarsRateLimited.Inc()
		return nil, ErrBusy
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), b.cfg.ComputeTimeout)
	defer cancel()
	bars, err := b.source.Bars(cctx, inst.Symbol, interval, rng)
	if err != nil {
		return nil, err
	}
	out := make([]BarJSON, 0, len(bars))
	for _, bar := range bars {
		if !stream.FitsWire(bar.Open) || !stream.FitsWire(bar.High) || !stream.FitsWire(bar.Low) || !stream.FitsWire(bar.Close) {
			// Never rounded: a bar the contract cannot represent is left out rather than altered.
			b.metrics.UnrepresentablePrices.Inc()
			continue
		}
		out = append(out, BarJSON{
			Time: stream.FormatTime(bar.Time), Open: bar.Open.String(), High: bar.High.String(),
			Low: bar.Low.String(), Close: bar.Close.String(), Volume: bar.Volume,
		})
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if b.cacheUsable() {
		rctx, cancel := b.guard.Context(ctx)
		if err := b.store.SetMany(rctx, []hotcache.Entry{{Key: key, Value: raw, TTL: b.CacheTTL(interval)}}); err != nil {
			b.guard.Fail("bars_set", err)
		}
		cancel()
	}
	return raw, nil
}

// BarsHandler serves historical bars.
type BarsHandler struct {
	source marketdata.MarketDataSource
	bars   *BarsService
	log    *slog.Logger
}

// NewBarsHandler creates the bars handler.
func NewBarsHandler(source marketdata.MarketDataSource, bars *BarsService, log *slog.Logger) *BarsHandler {
	return &BarsHandler{source: source, bars: bars, log: log}
}

// ServeHTTP handles GET /api/v1/market/bars?symbol=&interval=&range=.
func (h *BarsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cid := correlationID(r)
	w.Header().Set(CorrelationHeader, cid)
	q := r.URL.Query()
	symbol := q.Get("symbol")
	interval := marketdata.Interval(q.Get("interval"))
	rng := marketdata.Range(q.Get("range"))

	switch {
	case !stream.ValidSymbol(symbol):
		writeProblem(w, http.StatusBadRequest, CategoryValidation, "validation", "Validation failed", "symbol is missing or invalid", cid)
		return
	case interval.Duration() == 0:
		writeProblem(w, http.StatusBadRequest, CategoryValidation, "validation", "Validation failed", "interval must be one of 1m, 5m, 15m, 1h, 1d", cid)
		return
	case rng.Duration() == 0:
		writeProblem(w, http.StatusBadRequest, CategoryValidation, "validation", "Validation failed", "range must be one of 1d, 5d, 1mo, 3mo, 1y", cid)
		return
	case !h.source.SupportedBars().Supports(interval, rng):
		writeProblem(w, http.StatusBadRequest, CategoryValidation, "validation", "Validation failed",
			"unsupported interval/range combination; supported: "+h.source.SupportedBars().String(), cid)
		return
	}

	inst, err := h.source.Instrument(symbol)
	if errors.Is(err, marketdata.ErrUnknownSymbol) {
		writeProblem(w, http.StatusNotFound, CategoryInstrumentNotFound, "instrument-not-found", "Instrument not found", "unknown symbol", cid)
		return
	}
	var raw json.RawMessage
	cached := false
	if err == nil {
		raw, cached, err = h.bars.Get(r.Context(), inst, interval, rng)
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrBusy), errors.Is(err, marketdata.ErrRateLimited):
		writeProblem(w, http.StatusTooManyRequests, CategoryRateLimited, "rate-limited", "Too many requests",
			"too many historical-bar computations in progress; retry shortly", cid)
		return
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, marketdata.ErrSourceUnavailable):
		h.log.Warn("bars computation timed out", "correlationId", cid, "symbol", symbol, "interval", string(interval), "range", string(rng))
		writeProblem(w, http.StatusServiceUnavailable, CategoryServiceUnavailable, "service-unavailable", "Service unavailable", "historical bars are temporarily unavailable", cid)
		return
	default:
		h.log.Error("bars request failed", "correlationId", cid, "symbol", symbol, "error", err.Error())
		writeProblem(w, http.StatusInternalServerError, CategoryInternal, "internal", "Internal error", "", cid)
		return
	}

	writeJSON(w, http.StatusOK, "application/json", BarsResponse{
		Symbol: symbol, Interval: string(interval), Range: string(rng),
		Source: string(h.source.ID()), Cached: cached, Bars: raw,
	})
}
