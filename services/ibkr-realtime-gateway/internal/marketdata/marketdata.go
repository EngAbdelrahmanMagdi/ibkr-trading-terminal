// Package marketdata defines broker-neutral market-data types and the MarketDataSource port.
// Nothing in this package knows about transports (WebSocket, HTTP) or broker-specific payloads.
package marketdata

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SourceID identifies the kind of market-data source on the wire.
type SourceID string

const (
	// SourceMock is the deterministic simulator.
	SourceMock SourceID = "MOCK"
	// SourceIBKR is the Interactive Brokers market-data source.
	SourceIBKR SourceID = "IBKR"
)

// MaxSafeInteger is the largest integer every consumer (including JavaScript) represents exactly.
const MaxSafeInteger int64 = 1<<53 - 1

// Price is a non-negative amount in micro-units (1e-6), used by the simulator for arithmetic on its tick grid.
// Quotes and bars carry Decimal, which keeps any precision a source delivers. Prices never pass through
// floating point.
type Price int64

// MicrosPerUnit is the number of micro-units in one currency unit.
const MicrosPerUnit = 1_000_000

// Format renders the price with exactly decimals fractional digits (0-6), truncating finer digits.
// Prices produced by a source lie on the instrument's tick grid, so no information is lost.
func (p Price) Format(decimals int) string {
	if decimals < 0 {
		decimals = 0
	}
	if decimals > 6 {
		decimals = 6
	}
	whole := int64(p) / MicrosPerUnit
	frac := int64(p) % MicrosPerUnit
	if decimals == 0 {
		return strconv.FormatInt(whole, 10)
	}
	fracDigits := fmt.Sprintf("%06d", frac)[:decimals]
	return strconv.FormatInt(whole, 10) + "." + fracDigits
}

// Instrument is resolved instrument metadata relevant to market data.
type Instrument struct {
	Symbol        string
	Name          string
	Currency      string
	PriceDecimals int
}

// DataMode is how the source delivers market data. It is independent of staleness.
type DataMode string

// Data modes.
const (
	DataRealtime      DataMode = "REALTIME"
	DataDelayed       DataMode = "DELAYED"
	DataFrozen        DataMode = "FROZEN"
	DataFrozenDelayed DataMode = "FROZEN_DELAYED"
)

// Quote is a normalized top-of-book quote. Optional values are nil when the source has not delivered them
// (never zero). Volume is the cumulative volume of the current trading day. Halted is nil when the source
// does not report halts.
type Quote struct {
	Symbol   string
	Bid      *Decimal
	Ask      *Decimal
	Last     *Decimal
	BidSize  *int64
	AskSize  *int64
	Volume   *int64
	DataMode DataMode
	Halted   *bool
	Sequence int64 // monotonically increasing per symbol
	Time     time.Time
}

// Bar is an OHLCV bar starting at Time (UTC). Prices keep the precision delivered by the source.
type Bar struct {
	Time   time.Time
	Open   Decimal
	High   Decimal
	Low    Decimal
	Close  Decimal
	Volume int64
}

// Interval is a bar interval.
type Interval string

// Supported bar intervals.
const (
	Interval1m  Interval = "1m"
	Interval5m  Interval = "5m"
	Interval15m Interval = "15m"
	Interval1h  Interval = "1h"
	Interval1d  Interval = "1d"
)

// Duration returns the length of the interval.
func (i Interval) Duration() time.Duration {
	switch i {
	case Interval1m:
		return time.Minute
	case Interval5m:
		return 5 * time.Minute
	case Interval15m:
		return 15 * time.Minute
	case Interval1h:
		return time.Hour
	case Interval1d:
		return 24 * time.Hour
	default:
		return 0
	}
}

// Range is a history range ending now.
type Range string

// Supported history ranges.
const (
	Range1d  Range = "1d"
	Range5d  Range = "5d"
	Range1mo Range = "1mo"
	Range3mo Range = "3mo"
	Range1y  Range = "1y"
)

// Duration returns the length of the range (months are 30 days, years 365 days).
func (r Range) Duration() time.Duration {
	day := 24 * time.Hour
	switch r {
	case Range1d:
		return day
	case Range5d:
		return 5 * day
	case Range1mo:
		return 30 * day
	case Range3mo:
		return 90 * day
	case Range1y:
		return 365 * day
	default:
		return 0
	}
}

// BarSet is the set of interval/range combinations a source serves. Other combinations are rejected.
type BarSet map[Interval][]Range

// Supports reports whether the combination is served.
func (s BarSet) Supports(i Interval, r Range) bool {
	for _, candidate := range s[i] {
		if candidate == r {
			return true
		}
	}
	return false
}

// String lists the combinations in a stable, human-readable order.
func (s BarSet) String() string {
	var parts []string
	for _, i := range []Interval{Interval1m, Interval5m, Interval15m, Interval1h, Interval1d} {
		ranges := s[i]
		if len(ranges) == 0 {
			continue
		}
		names := make([]string, len(ranges))
		for k, r := range ranges {
			names[k] = string(r)
		}
		parts = append(parts, string(i)+": "+strings.Join(names, ","))
	}
	return strings.Join(parts, "; ")
}

// Errors returned by market-data sources.
var (
	ErrUnknownSymbol    = errors.New("unknown symbol")
	ErrUnsupportedRange = errors.New("unsupported interval/range combination")
	// ErrSymbolUnavailable: the source has no usable market data for the symbol (for example, no market-data
	// subscription). It is never streamed as if it were live.
	ErrSymbolUnavailable = errors.New("market data unavailable for symbol")
	// ErrRateLimited: the request was rejected by the source's pacing limiter or by the broker (429).
	ErrRateLimited = errors.New("market data source rate limited")
	// ErrSourceUnavailable: the source cannot serve the request right now (not connected, not logged in,
	// network failure). Retrying later may succeed.
	ErrSourceUnavailable = errors.New("market data source unavailable")
)
