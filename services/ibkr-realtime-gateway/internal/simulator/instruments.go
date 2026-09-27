package simulator

import "github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"

// instrumentSpec describes a simulated instrument. Values are illustrative for the demo, not market data.
type instrumentSpec struct {
	marketdata.Instrument
	base            marketdata.Price // reference price the simulation oscillates around
	tick            marketdata.Price // minimum price increment
	volatilityPct   int64            // scales the noise amplitudes; 100 = default
	maxSpreadTicks  int64            // spread varies between 1 and maxSpreadTicks ticks
	volumePerSecond int64            // average traded shares per second
}

func usd(symbol, name string, base marketdata.Price, volatilityPct, maxSpreadTicks, volumePerSecond int64) instrumentSpec {
	return instrumentSpec{
		Instrument:      marketdata.Instrument{Symbol: symbol, Name: name, Currency: "USD", PriceDecimals: 2},
		base:            base,
		tick:            10_000, // 0.01
		volatilityPct:   volatilityPct,
		maxSpreadTicks:  maxSpreadTicks,
		volumePerSecond: volumePerSecond,
	}
}

// defaultInstruments is the embedded instrument fixture of the simulator.
func defaultInstruments() []instrumentSpec {
	return []instrumentSpec{
		usd("NVDA", "NVIDIA Corporation", 180_000_000, 150, 3, 2000),
		usd("AAPL", "Apple Inc.", 190_000_000, 100, 2, 700),
		usd("META", "Meta Platforms, Inc.", 700_000_000, 120, 5, 180),
		usd("AMD", "Advanced Micro Devices, Inc.", 160_000_000, 140, 3, 600),
		usd("IONQ", "IonQ, Inc.", 40_000_000, 200, 2, 250),
		usd("MSFT", "Microsoft Corporation", 500_000_000, 90, 4, 250),
		usd("TSLA", "Tesla, Inc.", 330_000_000, 180, 4, 1100),
		usd("SPY", "SPDR S&P 500 ETF Trust", 650_000_000, 60, 2, 800),
	}
}
