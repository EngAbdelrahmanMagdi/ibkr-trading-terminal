package simulator

import (
	"context"
	"fmt"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

// Bars are derived from the same price function as live quotes:
//   - 1m bars sample every second of the minute (the default live tick grid), so a completed 1m bar equals the
//     aggregate of the live 1-second quotes of that minute;
//   - 5m, 15m and 1h bars are exact aggregates of their 1m bars;
//   - 1d bars sample every minute of the day (plus the latest second) to keep long ranges cheap, so their
//     high/low may miss intra-minute extremes; open, close and volume are exact.
// The last bar of a range is the current bar, truncated at now.

// Bars returns the bars of symbol for the interval/range ending at now.
func (m *Model) Bars(ctx context.Context, symbol string, interval marketdata.Interval, rng marketdata.Range, now time.Time) ([]marketdata.Bar, error) {
	if !marketdata.Supported(interval, rng) {
		return nil, fmt.Errorf("%w: %s/%s", marketdata.ErrUnsupportedRange, interval, rng)
	}
	sm, err := m.lookup(symbol)
	if err != nil {
		return nil, err
	}
	nowSec := now.UTC().Truncate(time.Second)
	d := interval.Duration()
	last := nowSec.Truncate(d) // Unix time is UTC-aligned, so this aligns to UTC boundaries
	count := int(rng.Duration() / d)
	first := last.Add(-time.Duration(count-1) * d)

	bars := make([]marketdata.Bar, 0, count)
	for start := first; !start.After(last); start = start.Add(d) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		bars = append(bars, sm.bar(interval, start, nowSec))
	}
	return bars, nil
}

func (sm *symbolModel) bar(interval marketdata.Interval, start, nowSec time.Time) marketdata.Bar {
	switch interval {
	case marketdata.Interval1m:
		return sm.minuteBar(start, nowSec)
	case marketdata.Interval1d:
		return sm.dayBar(start, nowSec)
	default:
		return sm.aggregateMinutes(start, interval.Duration(), nowSec)
	}
}

// minuteBar samples each second of [start, start+1m), truncated at nowSec.
func (sm *symbolModel) minuteBar(start, nowSec time.Time) marketdata.Bar {
	lastSec := start.Add(59 * time.Second)
	if lastSec.After(nowSec) {
		lastSec = nowSec
	}
	open := sm.price(start.UnixMilli())
	b := marketdata.Bar{Time: start, Open: open, High: open, Low: open, Close: open}
	for t := start.Add(time.Second); !t.After(lastSec); t = t.Add(time.Second) {
		p := sm.price(t.UnixMilli())
		b.High = max(b.High, p)
		b.Low = min(b.Low, p)
		b.Close = p
	}
	b.Volume = sm.volumeBetween(start, lastSec)
	return b
}

// aggregateMinutes combines the 1m bars of [start, start+d) that have started by nowSec.
func (sm *symbolModel) aggregateMinutes(start time.Time, d time.Duration, nowSec time.Time) marketdata.Bar {
	var out marketdata.Bar
	for m := start; m.Before(start.Add(d)) && !m.After(nowSec); m = m.Add(time.Minute) {
		b := sm.minuteBar(m, nowSec)
		if m.Equal(start) {
			out = b
			continue
		}
		out.High = max(out.High, b.High)
		out.Low = min(out.Low, b.Low)
		out.Close = b.Close
		out.Volume += b.Volume
	}
	return out
}

// dayBar samples each minute of the UTC day plus the latest second, truncated at nowSec.
func (sm *symbolModel) dayBar(start, nowSec time.Time) marketdata.Bar {
	lastSec := start.Add(24*time.Hour - time.Second)
	if lastSec.After(nowSec) {
		lastSec = nowSec
	}
	open := sm.price(start.UnixMilli())
	b := marketdata.Bar{Time: start, Open: open, High: open, Low: open}
	for t := start.Add(time.Minute); !t.After(lastSec); t = t.Add(time.Minute) {
		p := sm.price(t.UnixMilli())
		b.High = max(b.High, p)
		b.Low = min(b.Low, p)
	}
	b.Close = sm.price(lastSec.UnixMilli())
	b.High = max(b.High, b.Close)
	b.Low = min(b.Low, b.Close)
	b.Volume = sm.volumeBetween(start, lastSec)
	return b
}

// volumeBetween returns the volume traded from the first to the last second (inclusive) of a range that
// lies within one UTC day.
func (sm *symbolModel) volumeBetween(first, last time.Time) int64 {
	before := int64(0)
	if first.Unix()%secondsPerDay != 0 {
		before = sm.cumulativeVolume(first.Unix() - 1)
	}
	return sm.cumulativeVolume(last.Unix()) - before
}
