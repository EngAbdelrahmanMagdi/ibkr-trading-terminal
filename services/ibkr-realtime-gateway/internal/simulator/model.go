// Package simulator implements a deterministic, stateless market simulator and the SimulatorMarketDataSource.
//
// Every value is a pure function of (seed, symbol, time): identical inputs always produce identical prices,
// historical bars and live quotes agree at the same timestamps, and restarting reproduces the same history.
package simulator

import (
	"fmt"
	"sort"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

// octave is one layer of the price noise: a lattice spacing and an amplitude in parts per million of the
// base price (at 100% volatility).
type octave struct {
	scaleMs int64
	ampPPM  int64
}

// octaves span seconds to weeks. The amplitudes sum to 14.76% of the base price at 100% volatility.
var octaves = []octave{
	{scaleMs: 10_000, ampPPM: 400},
	{scaleMs: 60_000, ampPPM: 1_200},
	{scaleMs: 900_000, ampPPM: 4_000},
	{scaleMs: 14_400_000, ampPPM: 12_000},
	{scaleMs: 259_200_000, ampPPM: 40_000},
	{scaleMs: 2_592_000_000, ampPPM: 90_000},
}

const (
	maxDriftPPM     = 500_000 // prices never move more than 50% away from the base price
	secondsPerDay   = 86_400
	volumeNoiseMs   = 60_000 // volume bursts vary on a one-minute lattice
	volumeBurstMult = 10     // burst amplitude relative to the average rate; keeps cumulative volume increasing
)

// Salts separate the independent noise streams of one symbol.
const (
	saltOctave uint64 = 0xA000
	saltSpread uint64 = 0xB000
	saltSize   uint64 = 0xC000
	saltVolume uint64 = 0xD000
)

// symbolModel holds the precomputed keys of one instrument.
type symbolModel struct {
	spec       instrumentSpec
	octaveKeys []uint64
	spreadKey  uint64
	sizeKey    uint64
	volumeKey  uint64
}

// Model is the deterministic price model for a set of instruments.
type Model struct {
	symbols map[string]*symbolModel
}

// Option configures a Model.
type Option func(*modelOptions)

type modelOptions struct {
	synthetic int
}

// MaxSyntheticSymbols is the largest number of synthetic instruments (SYN001 to SYN999).
const MaxSyntheticSymbols = 999

// WithSyntheticSymbols adds n deterministic synthetic instruments (SYN001, SYN002, ...), used only for load
// testing. n is clamped to [0, MaxSyntheticSymbols].
func WithSyntheticSymbols(n int) Option {
	return func(o *modelOptions) { o.synthetic = max(0, min(n, MaxSyntheticSymbols)) }
}

// NewModel builds the model for the embedded instruments (plus any synthetic ones) with the given seed.
func NewModel(seed uint64, opts ...Option) *Model {
	var o modelOptions
	for _, opt := range opts {
		opt(&o)
	}
	m := &Model{symbols: map[string]*symbolModel{}}
	for _, spec := range append(defaultInstruments(), syntheticInstruments(seed, o.synthetic)...) {
		key := hash2(seed, fnv64a(spec.Symbol))
		sm := &symbolModel{
			spec:      spec,
			spreadKey: hash2(key, saltSpread),
			sizeKey:   hash2(key, saltSize),
			volumeKey: hash2(key, saltVolume),
		}
		for k := range octaves {
			sm.octaveKeys = append(sm.octaveKeys, hash2(key, saltOctave+uint64(k)))
		}
		m.symbols[spec.Symbol] = sm
	}
	return m
}

// Symbols returns the simulated symbols in sorted order.
func (m *Model) Symbols() []string {
	out := make([]string, 0, len(m.symbols))
	for s := range m.symbols {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func (m *Model) lookup(symbol string) (*symbolModel, error) {
	sm, ok := m.symbols[symbol]
	if !ok {
		return nil, fmt.Errorf("%w: %s", marketdata.ErrUnknownSymbol, symbol)
	}
	return sm, nil
}

// Instrument returns the metadata of a simulated symbol.
func (m *Model) Instrument(symbol string) (marketdata.Instrument, error) {
	sm, err := m.lookup(symbol)
	if err != nil {
		return marketdata.Instrument{}, err
	}
	return sm.spec.Instrument, nil
}

// Price returns the deterministic price of symbol at t, on the instrument's tick grid.
func (m *Model) Price(symbol string, t time.Time) (marketdata.Price, error) {
	sm, err := m.lookup(symbol)
	if err != nil {
		return 0, err
	}
	return sm.price(t.UnixMilli()), nil
}

// price computes the price at tMs using fixed-point integer arithmetic only.
func (sm *symbolModel) price(tMs int64) marketdata.Price {
	var offsetPPM int64
	for k, o := range octaves {
		amp := o.ampPPM * sm.spec.volatilityPct / 100
		offsetPPM += amp * valueNoise(sm.octaveKeys[k], o.scaleMs, tMs) / one
	}
	offsetPPM = max(-maxDriftPPM, min(maxDriftPPM, offsetPPM))
	base := int64(sm.spec.base)
	raw := base + base*offsetPPM/1_000_000
	tick := int64(sm.spec.tick)
	ticks := (raw + tick/2) / tick
	if ticks < 1 {
		ticks = 1
	}
	return marketdata.Price(ticks * tick)
}

// cumulativeVolume returns the traded volume of the UTC day up to and including the given Unix second.
//
// C(s) = r*(s+1) + a*(v(s) - v(-1)) with v a one-minute smooth noise and a = 10r. The noise slope is at
// most 3/60 per second, so each second adds at least r/2 shares: the volume strictly increases within a
// day and restarts at the next UTC midnight.
func (sm *symbolModel) cumulativeVolume(unixSec int64) int64 {
	day := floorDiv(unixSec, secondsPerDay)
	s := unixSec - day*secondsPerDay
	dayKey := hash2(sm.volumeKey, uint64(day))
	r := sm.spec.volumePerSecond
	a := volumeBurstMult * r
	v := valueNoise(dayKey, volumeNoiseMs, s*1000)
	v0 := valueNoise(dayKey, volumeNoiseMs, -1000)
	return r*(s+1) + floorDiv(a*(v-v0), one)
}

// quoteAt builds the quote of the tick at t with the given sequence number.
// quote is a simulated quote on the instrument's tick grid (micro-unit prices).
type quote struct {
	Symbol   string
	Bid      marketdata.Price
	Ask      marketdata.Price
	Last     marketdata.Price
	BidSize  int64
	AskSize  int64
	Volume   int64
	Sequence int64
	Time     time.Time
}

// Bar is a simulated OHLCV bar on the instrument's tick grid (micro-unit prices).
type Bar struct {
	Time   time.Time
	Open   marketdata.Price
	High   marketdata.Price
	Low    marketdata.Price
	Close  marketdata.Price
	Volume int64
}

func (sm *symbolModel) quoteAt(t time.Time, seq int64) quote {
	ms := t.UnixMilli()
	sec := floorDiv(ms, 1000)
	last := sm.price(ms)
	tick := sm.spec.tick
	spreadTicks := 1 + int64(hash2(sm.spreadKey, uint64(sec))%uint64(sm.spec.maxSpreadTicks))
	bid := last - marketdata.Price(spreadTicks/2)*tick
	if bid < tick {
		bid = tick
	}
	ask := bid + marketdata.Price(spreadTicks)*tick
	return quote{
		Symbol:   sm.spec.Symbol,
		Bid:      bid,
		Ask:      ask,
		Last:     last,
		BidSize:  (1 + int64(hash2(sm.sizeKey, uint64(2*sec))%50)) * 100,
		AskSize:  (1 + int64(hash2(sm.sizeKey, uint64(2*sec+1))%50)) * 100,
		Volume:   sm.cumulativeVolume(sec),
		Sequence: seq,
		Time:     t.UTC(),
	}
}
