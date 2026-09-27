// Package marketdata defines broker-neutral market-data types and the MarketDataSource port.
// Nothing in this package knows about transports (WebSocket, HTTP) or broker-specific payloads.
package marketdata

import (
	"errors"
	"fmt"
	"strconv"
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

// Price is a non-negative decimal amount in micro-units (1e-6). Prices never pass through floating point.
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

// Quote is a normalized top-of-book quote. Volume is the cumulative volume of the current UTC day.
type Quote struct {
	Symbol   string
	Bid      Price
	Ask      Price
	Last     Price
	BidSize  int64
	AskSize  int64
	Volume   int64
	Sequence int64 // monotonically increasing per symbol
	Time     time.Time
}

// Bar is an OHLCV bar starting at Time (UTC).
type Bar struct {
	Time   time.Time
	Open   Price
	High   Price
	Low    Price
	Close  Price
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

// supportedRanges lists the interval/range combinations served. Other combinations are rejected.
var supportedRanges = map[Interval][]Range{
	Interval1m:  {Range1d, Range5d},
	Interval5m:  {Range1d, Range5d, Range1mo},
	Interval15m: {Range5d, Range1mo},
	Interval1h:  {Range5d, Range1mo},
	Interval1d:  {Range1mo, Range3mo, Range1y},
}

// Supported reports whether the interval/range combination is served.
func Supported(i Interval, r Range) bool {
	for _, candidate := range supportedRanges[i] {
		if candidate == r {
			return true
		}
	}
	return false
}

// SupportedCombinations returns a human-readable list of the served combinations.
func SupportedCombinations() string {
	return "1m: 1d,5d; 5m: 1d,5d,1mo; 15m: 5d,1mo; 1h: 5d,1mo; 1d: 1mo,3mo,1y"
}

// Errors returned by market-data sources.
var (
	ErrUnknownSymbol    = errors.New("unknown symbol")
	ErrUnsupportedRange = errors.New("unsupported interval/range combination")
)
