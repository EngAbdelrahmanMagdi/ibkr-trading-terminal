package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
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

// BarsResponse is the body of GET /api/v1/market/bars.
type BarsResponse struct {
	Symbol   string    `json:"symbol"`
	Interval string    `json:"interval"`
	Range    string    `json:"range"`
	Source   string    `json:"source"`
	Cached   bool      `json:"cached"`
	Bars     []BarJSON `json:"bars"`
}

// BarsHandler serves historical bars.
type BarsHandler struct {
	source marketdata.MarketDataSource
	log    *slog.Logger
}

// NewBarsHandler creates the bars handler.
func NewBarsHandler(source marketdata.MarketDataSource, log *slog.Logger) *BarsHandler {
	return &BarsHandler{source: source, log: log}
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
	case !marketdata.Supported(interval, rng):
		writeProblem(w, http.StatusBadRequest, CategoryValidation, "validation", "Validation failed",
			"unsupported interval/range combination; supported: "+marketdata.SupportedCombinations(), cid)
		return
	}

	inst, err := h.source.Instrument(symbol)
	if errors.Is(err, marketdata.ErrUnknownSymbol) {
		writeProblem(w, http.StatusNotFound, CategoryInstrumentNotFound, "instrument-not-found", "Instrument not found", "unknown symbol", cid)
		return
	}
	var bars []marketdata.Bar
	if err == nil {
		bars, err = h.source.Bars(r.Context(), symbol, interval, rng)
	}
	if err != nil {
		h.log.Error("bars request failed", "correlationId", cid, "symbol", symbol, "error", err.Error())
		writeProblem(w, http.StatusInternalServerError, CategoryInternal, "internal", "Internal error", "", cid)
		return
	}

	resp := BarsResponse{
		Symbol: symbol, Interval: string(interval), Range: string(rng),
		Source: string(h.source.ID()), Cached: false, Bars: make([]BarJSON, 0, len(bars)),
	}
	d := inst.PriceDecimals
	for _, b := range bars {
		resp.Bars = append(resp.Bars, BarJSON{
			Time: stream.FormatTime(b.Time), Open: b.Open.Format(d), High: b.High.Format(d),
			Low: b.Low.Format(d), Close: b.Close.Format(d), Volume: b.Volume,
		})
	}
	writeJSON(w, http.StatusOK, "application/json", resp)
}
