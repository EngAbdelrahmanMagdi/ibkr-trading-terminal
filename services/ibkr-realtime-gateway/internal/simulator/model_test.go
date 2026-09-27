package simulator

import (
	"context"
	"testing"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

const testSeed = 20260927

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

// Golden values pin the price model: any change to the model (or a platform-dependent result) fails here,
// because it would silently rewrite every client's history.
func TestPriceGoldenValues(t *testing.T) {
	m := NewModel(testSeed)
	golden := []struct {
		symbol string
		at     string
		micros int64
	}{
		{"NVDA", "2026-09-27T14:03:11Z", 182130000},
		{"SPY", "2026-09-27T14:03:11Z", 609480000},
		{"IONQ", "2026-09-27T14:03:11Z", 42300000},
		{"NVDA", "2026-01-01T00:00:00Z", 191180000},
		{"SPY", "2026-01-01T00:00:00Z", 663580000},
		{"IONQ", "2026-01-01T00:00:00Z", 39840000},
		{"NVDA", "2027-06-15T09:30:00.250Z", 182780000},
		{"SPY", "2027-06-15T09:30:00.250Z", 661870000},
		{"IONQ", "2027-06-15T09:30:00.250Z", 37930000},
	}
	for _, g := range golden {
		p, err := m.Price(g.symbol, mustTime(t, g.at))
		if err != nil {
			t.Fatal(err)
		}
		if int64(p) != g.micros {
			t.Errorf("%s @ %s = %d, want %d", g.symbol, g.at, p, g.micros)
		}
	}
	if v := m.symbols["NVDA"].cumulativeVolume(mustTime(t, "2026-09-27T14:03:11Z").Unix()); v != 101168049 {
		t.Errorf("NVDA cumulative volume = %d, want 101168049", v)
	}
}

func TestDeterministicAcrossInstancesAndDistinctAcrossSeedsAndSymbols(t *testing.T) {
	a, b, other := NewModel(testSeed), NewModel(testSeed), NewModel(testSeed+1)
	start := mustTime(t, "2026-09-27T00:00:00Z")
	sameSeedDiffers, symbolsDiffer := 0, 0
	for i := 0; i < 2000; i++ {
		at := start.Add(time.Duration(i) * 97 * time.Second)
		pa, _ := a.Price("NVDA", at)
		pb, _ := b.Price("NVDA", at)
		if pa != pb {
			t.Fatalf("same seed produced different prices at %s: %d vs %d", at, pa, pb)
		}
		po, _ := other.Price("NVDA", at)
		if po != pa {
			sameSeedDiffers++
		}
		amd, _ := a.Price("AMD", at)
		// Compare relative paths: AMD and NVDA have different bases, so compare offsets from base.
		if int64(amd)*180 != int64(pa)*160 {
			symbolsDiffer++
		}
	}
	if sameSeedDiffers < 1900 {
		t.Errorf("a different seed should change almost every price; only %d of 2000 differed", sameSeedDiffers)
	}
	if symbolsDiffer < 1900 {
		t.Errorf("symbols should follow distinct paths; only %d of 2000 differed", symbolsDiffer)
	}
}

func TestPricesAndQuotesAreValid(t *testing.T) {
	m := NewModel(testSeed)
	start := mustTime(t, "2026-01-01T00:00:00Z")
	for _, symbol := range m.Symbols() {
		sm := m.symbols[symbol]
		spec := sm.spec
		lo := int64(spec.base) * (1_000_000 - maxDriftPPM) / 1_000_000
		hi := int64(spec.base) * (1_000_000 + maxDriftPPM) / 1_000_000
		for i := 0; i < 20_000; i++ { // ~400 days, every 1753 s
			at := start.Add(time.Duration(i) * 1753 * time.Second)
			q := sm.quoteAt(at, int64(i))
			switch {
			case q.Last <= 0 || q.Bid <= 0:
				t.Fatalf("%s: non-positive price at %s: %+v", symbol, at, q)
			case int64(q.Last)%int64(spec.tick) != 0 || int64(q.Bid)%int64(spec.tick) != 0 || int64(q.Ask)%int64(spec.tick) != 0:
				t.Fatalf("%s: price off the tick grid at %s: %+v", symbol, at, q)
			case int64(q.Last) < lo || int64(q.Last) > hi:
				t.Fatalf("%s: price %d outside drift bounds [%d, %d]", symbol, q.Last, lo, hi)
			case q.Bid >= q.Ask || q.Last < q.Bid || q.Last > q.Ask:
				t.Fatalf("%s: inconsistent book at %s: %+v", symbol, at, q)
			case q.BidSize < 100 || q.AskSize < 100 || q.BidSize%100 != 0 || q.AskSize%100 != 0:
				t.Fatalf("%s: invalid sizes at %s: %+v", symbol, at, q)
			case q.Volume <= 0 || q.Volume > marketdata.MaxSafeInteger:
				t.Fatalf("%s: invalid volume at %s: %d", symbol, at, q.Volume)
			}
		}
	}
}

func TestVolumeIncreasesWithinDayAndResetsAtMidnight(t *testing.T) {
	m := NewModel(testSeed)
	day := mustTime(t, "2026-09-27T00:00:00Z").Unix()
	for _, symbol := range m.Symbols() {
		sm := m.symbols[symbol]
		prev := int64(0)
		for s := int64(0); s < secondsPerDay; s++ {
			v := sm.cumulativeVolume(day + s)
			if v <= prev {
				t.Fatalf("%s: volume not increasing at second %d: %d after %d", symbol, s, v, prev)
			}
			prev = v
		}
		next := sm.cumulativeVolume(day + secondsPerDay)
		if next >= prev || next <= 0 {
			t.Fatalf("%s: volume must restart at midnight: %d after %d", symbol, next, prev)
		}
	}
}

func TestBarsInvariantsForEverySupportedCombination(t *testing.T) {
	m := NewModel(testSeed)
	now := mustTime(t, "2026-09-27T14:03:11.500Z")
	combos := map[marketdata.Interval][]marketdata.Range{
		marketdata.Interval1m:  {marketdata.Range1d, marketdata.Range5d},
		marketdata.Interval5m:  {marketdata.Range1d, marketdata.Range5d, marketdata.Range1mo},
		marketdata.Interval15m: {marketdata.Range5d, marketdata.Range1mo},
		marketdata.Interval1h:  {marketdata.Range5d, marketdata.Range1mo},
		marketdata.Interval1d:  {marketdata.Range1mo, marketdata.Range3mo, marketdata.Range1y},
	}
	for interval, ranges := range combos {
		for _, rng := range ranges {
			bars, err := m.Bars(context.Background(), "NVDA", interval, rng, now)
			if err != nil {
				t.Fatalf("%s/%s: %v", interval, rng, err)
			}
			d := interval.Duration()
			if want := int(rng.Duration() / d); len(bars) != want {
				t.Fatalf("%s/%s: %d bars, want %d", interval, rng, len(bars), want)
			}
			for i, b := range bars {
				if b.Time.Unix()%int64(d/time.Second) != 0 {
					t.Fatalf("%s/%s: bar %d not aligned to a UTC boundary: %s", interval, rng, i, b.Time)
				}
				if i > 0 && !b.Time.Equal(bars[i-1].Time.Add(d)) {
					t.Fatalf("%s/%s: bars not contiguous at %d", interval, rng, i)
				}
				if b.Low > b.Open || b.Low > b.Close || b.High < b.Open || b.High < b.Close || b.Low <= 0 {
					t.Fatalf("%s/%s: invalid OHLC at %s: %+v", interval, rng, b.Time, b)
				}
				if b.Volume <= 0 {
					t.Fatalf("%s/%s: non-positive volume at %s", interval, rng, b.Time)
				}
			}
			last := bars[len(bars)-1]
			if last.Time.After(now) || !now.Before(last.Time.Add(d)) {
				t.Fatalf("%s/%s: last bar %s does not contain now %s", interval, rng, last.Time, now)
			}
		}
	}
}

func TestUnsupportedCombinationAndUnknownSymbolAreRejected(t *testing.T) {
	m := NewModel(testSeed)
	now := mustTime(t, "2026-09-27T14:03:11Z")
	if _, err := m.Bars(context.Background(), "NVDA", marketdata.Interval1m, marketdata.Range1y, now); err == nil {
		t.Error("1m over 1y must be rejected")
	}
	if _, err := m.Bars(context.Background(), "NOPE", marketdata.Interval1m, marketdata.Range1d, now); err == nil {
		t.Error("unknown symbol must be rejected")
	}
}

func TestCoarseBarsAggregateMinuteBarsExactly(t *testing.T) {
	m := NewModel(testSeed)
	now := mustTime(t, "2026-09-27T14:03:11Z")
	minutes, err := m.Bars(context.Background(), "TSLA", marketdata.Interval1m, marketdata.Range1d, now)
	if err != nil {
		t.Fatal(err)
	}
	byTime := map[int64]Bar{}
	for _, b := range minutes {
		byTime[b.Time.Unix()] = b
	}
	for _, interval := range []marketdata.Interval{marketdata.Interval5m, marketdata.Interval15m, marketdata.Interval1h} {
		coarse, err := m.Bars(context.Background(), "TSLA", interval, marketdata.Range5d, now)
		if err != nil {
			t.Fatal(err)
		}
		checked := 0
		for _, c := range coarse {
			first, ok := byTime[c.Time.Unix()]
			if !ok {
				continue // older than the 1d window of minute bars
			}
			agg := first
			for tm := c.Time.Add(time.Minute); tm.Before(c.Time.Add(interval.Duration())) && !tm.After(now); tm = tm.Add(time.Minute) {
				b := byTime[tm.Unix()]
				agg.High, agg.Low, agg.Close, agg.Volume = max(agg.High, b.High), min(agg.Low, b.Low), b.Close, agg.Volume+b.Volume
			}
			if agg != c {
				t.Fatalf("%s bar at %s differs from its minute aggregate:\n got  %+v\n want %+v", interval, c.Time, c, agg)
			}
			checked++
		}
		if checked == 0 {
			t.Fatalf("%s: no bars checked", interval)
		}
	}
}

func TestMinuteBarEqualsLiveOneSecondQuotes(t *testing.T) {
	m := NewModel(testSeed)
	sm := m.symbols["META"]
	start := mustTime(t, "2026-09-27T13:59:00Z")
	now := mustTime(t, "2026-09-27T14:03:11Z")
	bars, err := m.Bars(context.Background(), "META", marketdata.Interval1m, marketdata.Range1d, now)
	if err != nil {
		t.Fatal(err)
	}
	var bar Bar
	for _, b := range bars {
		if b.Time.Equal(start) {
			bar = b
		}
	}
	// Aggregate the live 1-second quotes of that minute independently.
	var quotes []quote
	for s := 0; s < 60; s++ {
		quotes = append(quotes, sm.quoteAt(start.Add(time.Duration(s)*time.Second), int64(s)))
	}
	want := Bar{Time: start, Open: quotes[0].Last, High: quotes[0].Last, Low: quotes[0].Last, Close: quotes[59].Last}
	for _, q := range quotes {
		want.High, want.Low = max(want.High, q.Last), min(want.Low, q.Last)
	}
	before := sm.quoteAt(start.Add(-time.Second), 0).Volume
	want.Volume = quotes[59].Volume - before
	if bar != want {
		t.Fatalf("minute bar differs from live quotes:\n got  %+v\n want %+v", bar, want)
	}
}

func TestOpenBarsAreTruncatedAtNow(t *testing.T) {
	m := NewModel(testSeed)
	sm := m.symbols["NVDA"]
	now := mustTime(t, "2026-09-27T14:03:30.750Z")
	for _, interval := range []marketdata.Interval{marketdata.Interval1m, marketdata.Interval1h, marketdata.Interval1d} {
		rng := marketdata.Range5d
		if interval == marketdata.Interval1d {
			rng = marketdata.Range1mo
		}
		bars, err := m.Bars(context.Background(), "NVDA", interval, rng, now)
		if err != nil {
			t.Fatal(err)
		}
		last := bars[len(bars)-1]
		latest := sm.quoteAt(now.Truncate(time.Second), 0)
		if last.Close != latest.Last {
			t.Errorf("%s: open bar close %d, want latest price %d", interval, last.Close, latest.Last)
		}
		dayStart := now.Truncate(24 * time.Hour)
		if interval == marketdata.Interval1d && last.Volume != latest.Volume {
			t.Errorf("1d: open bar volume %d, want day volume %d", last.Volume, latest.Volume)
		}
		if interval == marketdata.Interval1d && !last.Time.Equal(dayStart) {
			t.Errorf("1d: open bar starts %s, want %s", last.Time, dayStart)
		}
	}
}

func TestCompletedDayBar(t *testing.T) {
	m := NewModel(testSeed)
	sm := m.symbols["SPY"]
	now := mustTime(t, "2026-09-27T14:03:11Z")
	bars, err := m.Bars(context.Background(), "SPY", marketdata.Interval1d, marketdata.Range1mo, now)
	if err != nil {
		t.Fatal(err)
	}
	day := bars[len(bars)-2] // yesterday, complete
	lastSecond := day.Time.Add(24*time.Hour - time.Second)
	if want := sm.price(day.Time.UnixMilli()); day.Open != want {
		t.Errorf("open %d, want price at midnight %d", day.Open, want)
	}
	if want := sm.price(lastSecond.UnixMilli()); day.Close != want {
		t.Errorf("close %d, want price at 23:59:59 %d", day.Close, want)
	}
	if want := sm.cumulativeVolume(lastSecond.Unix()); day.Volume != want {
		t.Errorf("volume %d, want full-day volume %d", day.Volume, want)
	}
}

func TestPriceFormatting(t *testing.T) {
	cases := map[marketdata.Price]string{182130000: "182.13", 10_000: "0.01", 700_000_000: "700.00", 5: "0.00"}
	for p, want := range cases {
		if got := p.Format(2); got != want {
			t.Errorf("Format(%d) = %q, want %q", p, got, want)
		}
	}
	if got := marketdata.Price(1_234_567).Format(6); got != "1.234567" {
		t.Errorf("Format(6) = %q", got)
	}
}

// Baseline measurements only; no optimization targets.
func BenchmarkPrice(b *testing.B) {
	sm := NewModel(testSeed).symbols["NVDA"]
	start := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC).UnixMilli()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		_ = sm.price(start + int64(i)*250)
	}
}

func BenchmarkBarsOneMinuteFiveDays(b *testing.B) {
	m := NewModel(testSeed)
	now := time.Date(2026, 9, 27, 14, 3, 11, 0, time.UTC)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := m.Bars(context.Background(), "NVDA", marketdata.Interval1m, marketdata.Range5d, now); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBarsOneDayOneYear(b *testing.B) {
	m := NewModel(testSeed)
	now := time.Date(2026, 9, 27, 14, 3, 11, 0, time.UTC)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := m.Bars(context.Background(), "NVDA", marketdata.Interval1d, marketdata.Range1y, now); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSyntheticInstrumentsAreDeterministicAndOptIn(t *testing.T) {
	if n := len(NewModel(testSeed).Symbols()); n != 8 {
		t.Fatalf("default instruments = %d, want 8", n)
	}
	a, b := NewModel(testSeed, WithSyntheticSymbols(100)), NewModel(testSeed, WithSyntheticSymbols(100))
	if n := len(a.Symbols()); n != 108 {
		t.Fatalf("instruments with 100 synthetic = %d", n)
	}
	for _, sym := range []string{"SYN001", "SYN050", "SYN100"} {
		inst, err := a.Instrument(sym)
		if err != nil || inst.Currency != "USD" || inst.PriceDecimals != 2 {
			t.Fatalf("%s: %+v %v", sym, inst, err)
		}
		at := time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC)
		pa, _ := a.Price(sym, at)
		pb, _ := b.Price(sym, at)
		if pa != pb || pa <= 0 {
			t.Fatalf("%s: prices %d vs %d", sym, pa, pb)
		}
	}
	if _, err := a.Instrument("SYN101"); err == nil {
		t.Fatal("SYN101 must not exist")
	}
	if n := len(NewModel(testSeed, WithSyntheticSymbols(5000)).Symbols()); n != 8+MaxSyntheticSymbols {
		t.Fatalf("synthetic count must be clamped, got %d instruments", n)
	}
}
