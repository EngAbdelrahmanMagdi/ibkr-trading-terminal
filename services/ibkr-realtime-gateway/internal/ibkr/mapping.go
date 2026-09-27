package ibkr

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// IBKR market-data field tags (Market Data Fields reference, checked 2026-09-27). This table is the only
// place they appear; they never leave the adapter.
const (
	fieldLast         = "31"   // may be prefixed: C = previous close, H = trading halted
	fieldBid          = "84"   //
	fieldAskSize      = "85"   // may contain thousands separators
	fieldAsk          = "86"   //
	fieldBidSize      = "88"   // may contain thousands separators
	fieldLastSize     = "7059" //
	fieldVolume       = "7762" // exact daily volume (field 87 is formatted with K/M suffixes)
	fieldAvailability = "6509" // R realtime, D delayed, Z frozen, Y frozen delayed, N not subscribed, ...
)

// streamFields is requested on every smd subscription.
var streamFields = []string{fieldLast, fieldBid, fieldAskSize, fieldAsk, fieldBidSize, fieldLastSize, fieldVolume, fieldAvailability}

// dataModes maps the first character of field 6509. Anything else (N not subscribed, O API acknowledgement
// incomplete, i incomplete, unknown) means the data is not usable.
var dataModes = map[byte]marketdata.DataMode{
	'R': marketdata.DataRealtime,
	'D': marketdata.DataDelayed,
	'Z': marketdata.DataFrozen,
	'Y': marketdata.DataFrozenDelayed,
}

// quoteState accumulates the partial frames of one instrument into a complete normalized quote.
type quoteState struct {
	symbol   string
	last     *marketdata.Decimal
	bid      *marketdata.Decimal
	ask      *marketdata.Decimal
	bidSize  *int64
	askSize  *int64
	volume   *int64
	mode     marketdata.DataMode
	halted   *bool
	updated  int64 // epoch milliseconds of the latest frame
	sequence int64
}

// mapResult reports what a merged frame produced.
type mapResult struct {
	quote       marketdata.Quote
	emit        bool  // a usable quote is available
	unavailable error // the source reported no usable data for the instrument
	malformed   int   // fields that could not be mapped (dropped, never guessed)
	unwireable  int   // prices the contract cannot represent (sent as null, never rounded)
}

// merge applies one smd frame. Missing fields keep their previous value; a field never becomes zero by
// omission.
func (st *quoteState) merge(frame map[string]json.RawMessage, now time.Time) mapResult {
	var res mapResult
	if raw, ok := frame[fieldAvailability]; ok {
		code := textValue(raw)
		mode, known := marketdata.DataMode(""), false
		if code != "" {
			mode, known = dataModes[code[0]]
		}
		if !known {
			res.unavailable = fmt.Errorf("%w: market data availability %q", marketdata.ErrSymbolUnavailable, firstChar(code))
			return res
		}
		st.mode = mode
	}
	if raw, ok := frame[fieldLast]; ok {
		v := textValue(raw)
		switch {
		case strings.HasPrefix(v, "C"): // previous close: not a current trade
			st.last = nil
		case strings.HasPrefix(v, "H"):
			halted := true
			st.halted = &halted
			st.last = st.price(v[1:], &res)
		default:
			halted := false
			st.halted = &halted
			st.last = st.price(v, &res)
		}
	}
	if raw, ok := frame[fieldBid]; ok {
		st.bid = st.price(textValue(raw), &res)
	}
	if raw, ok := frame[fieldAsk]; ok {
		st.ask = st.price(textValue(raw), &res)
	}
	if raw, ok := frame[fieldBidSize]; ok {
		st.bidSize = count(textValue(raw), &res)
	}
	if raw, ok := frame[fieldAskSize]; ok {
		st.askSize = count(textValue(raw), &res)
	}
	if raw, ok := frame[fieldVolume]; ok {
		st.volume = count(textValue(raw), &res)
	}
	updated := now.UnixMilli()
	if raw, ok := frame["_updated"]; ok {
		if ms, err := strconv.ParseInt(textValue(raw), 10, 64); err == nil && ms > 0 {
			updated = ms
		}
	}
	st.updated = updated
	if st.mode == "" || (st.last == nil && st.bid == nil && st.ask == nil) {
		return res // availability or any price not known yet
	}
	// Monotonic per instrument, across reconnects and restarts, and below 2^53 for the next centuries.
	st.sequence = max(st.sequence+1, updated*1000)
	res.quote = marketdata.Quote{
		Symbol: st.symbol, Bid: st.bid, Ask: st.ask, Last: st.last,
		BidSize: st.bidSize, AskSize: st.askSize, Volume: st.volume,
		DataMode: st.mode, Halted: st.halted,
		Sequence: st.sequence, Time: time.UnixMilli(updated).UTC(),
	}
	res.emit = true
	return res
}

// price parses an exact decimal. Empty means "not available"; text that is not a non-negative decimal is
// dropped as malformed; a value the contract cannot carry is dropped as unwireable. Nothing is rounded.
func (st *quoteState) price(v string, res *mapResult) *marketdata.Decimal {
	if v == "" {
		return nil
	}
	d, err := marketdata.ParseDecimal(v)
	if err != nil {
		res.malformed++
		return nil
	}
	if !stream.FitsWire(d) {
		res.unwireable++
		return nil
	}
	return &d
}

func count(v string, res *mapResult) *int64 {
	if v == "" {
		return nil
	}
	n, err := strconv.ParseInt(strings.ReplaceAll(v, ",", ""), 10, 64)
	if err != nil || n < 0 || n > marketdata.MaxSafeInteger {
		res.malformed++
		return nil
	}
	return &n
}

// textValue returns a JSON string's content or a JSON number's text.
func textValue(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(string(raw))
}

func firstChar(s string) string {
	if s == "" {
		return ""
	}
	return s[:1]
}
