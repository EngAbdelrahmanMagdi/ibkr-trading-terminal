package marketdata

import (
	"context"
	"time"
)

// QuoteSink receives quotes from a subscription. OfferQuote must never block: it returns false when the
// quote cannot be accepted, which ends the subscription.
type QuoteSink interface {
	OfferQuote(Quote) bool
}

// QuoteSinkFunc adapts a function to QuoteSink.
type QuoteSinkFunc func(Quote) bool

// OfferQuote calls f.
func (f QuoteSinkFunc) OfferQuote(q Quote) bool { return f(q) }

// Subscription is an active quote subscription.
type Subscription interface {
	// Close stops the subscription and waits until no more quotes will be delivered.
	Close()
}

// SourceState is the connection state of a market-data source.
type SourceState string

// Source connection states (see the connection state machine in package connstate).
const (
	StateDisconnected   SourceState = "DISCONNECTED"
	StateConnecting     SourceState = "CONNECTING"
	StateAuthenticating SourceState = "AUTHENTICATING"
	StateReady          SourceState = "READY"
	StateDegraded       SourceState = "DEGRADED"
	StateReconnecting   SourceState = "RECONNECTING"
)

// StatusEvent reports a change of the source connection state. ErrorCategory is a short, non-sensitive
// category (never a raw upstream error or credential) and is empty for healthy states.
type StatusEvent struct {
	State         SourceState
	ErrorCategory string
	At            time.Time
}

// MarketDataSource is the port through which the gateway obtains market data.
// Implementations: SimulatorMarketDataSource (MOCK) now, IBKRMarketDataSource later.
type MarketDataSource interface {
	// ID identifies the source kind on the wire.
	ID() SourceID
	// Instrument returns instrument metadata or ErrUnknownSymbol.
	Instrument(symbol string) (Instrument, error)
	// Snapshot returns the current quote for a symbol.
	Snapshot(ctx context.Context, symbol string) (Quote, error)
	// Subscribe delivers quotes for symbol to sink until the subscription is closed, ctx is done, or the sink
	// refuses a quote. Quotes delivered after a Snapshot always have a greater Sequence than the snapshot.
	Subscribe(ctx context.Context, symbol string, sink QuoteSink) (Subscription, error)
	// Bars returns historical bars in ascending time order; the last bar may be the current, still open bar.
	Bars(ctx context.Context, symbol string, interval Interval, rng Range) ([]Bar, error)
	// Status returns the source's status events. The channel is owned and bounded by the source; the first
	// event reports the current state. Consumers must keep reading it while the source is in use.
	Status() <-chan StatusEvent
}
