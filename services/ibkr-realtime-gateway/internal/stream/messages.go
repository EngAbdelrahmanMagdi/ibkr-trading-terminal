// Package stream defines the browser WebSocket protocol messages (contracts/asyncapi/market-stream.yaml) and
// their JSON encoding. It depends only on marketdata; it knows nothing about the WebSocket library.
package stream

import (
	"encoding/json"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

// Message types.
const (
	TypeSubscribe   = "subscribe"
	TypeUnsubscribe = "unsubscribe"
	TypeConnection  = "connection"
	TypeSnapshot    = "snapshot"
	TypeQuote       = "quote"
	TypeStale       = "stale"
	TypeError       = "error"
	TypeHeartbeat   = "heartbeat"
)

// Error codes.
const (
	ErrInvalidMessage    = "INVALID_MESSAGE"
	ErrUnknownSymbol     = "UNKNOWN_SYMBOL"
	ErrSubscriptionLimit = "SUBSCRIPTION_LIMIT"
	ErrSourceUnavailable = "SOURCE_UNAVAILABLE"
	ErrInternal          = "INTERNAL"
)

// Stale reasons.
const (
	StaleNoUpdates          = "NO_UPDATES"
	StaleSourceDisconnected = "SOURCE_DISCONNECTED"
	StaleSourceDegraded     = "SOURCE_DEGRADED"
)

// FormatTime renders a UTC timestamp with millisecond precision and a trailing Z.
func FormatTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// Limits are the effective per-connection protocol limits advertised to clients.
type Limits struct {
	MaxSymbolsPerSubscribe int   `json:"maxSymbolsPerSubscribe"`
	MaxSubscribedSymbols   int   `json:"maxSubscribedSymbols"`
	MaxInboundMessageBytes int64 `json:"maxInboundMessageBytes"`
	HeartbeatIntervalMs    int64 `json:"heartbeatIntervalMs"`
}

// Connection is sent on connect and on every state change.
type Connection struct {
	Type      string `json:"type"`
	State     string `json:"state"`
	Source    string `json:"source"`
	Timestamp string `json:"timestamp"`
	Limits    Limits `json:"limits"`
}

// QuoteMessage is a snapshot or quote. Nullable fields are pointers so that "not available" is null, never 0.
type QuoteMessage struct {
	Type      string  `json:"type"`
	Symbol    string  `json:"symbol"`
	Bid       *string `json:"bid"`
	Ask       *string `json:"ask"`
	Last      *string `json:"last"`
	BidSize   *int64  `json:"bidSize"`
	AskSize   *int64  `json:"askSize"`
	Volume    *int64  `json:"volume"`
	Sequence  int64   `json:"sequence"`
	Timestamp string  `json:"timestamp"`
	Stale     bool    `json:"stale"`
}

// Stale reports symbols without fresh data. Clients must not display them as live.
type Stale struct {
	Type    string   `json:"type"`
	Symbols []string `json:"symbols"`
	Since   string   `json:"since"`
	Reason  string   `json:"reason"`
}

// ErrorMessage reports a recoverable protocol or subscription error.
type ErrorMessage struct {
	Type    string   `json:"type"`
	Code    string   `json:"code"`
	Message string   `json:"message"`
	Symbols []string `json:"symbols,omitempty"`
}

// Heartbeat is a liveness signal.
type Heartbeat struct {
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
}

// NewConnection builds a connection message.
func NewConnection(state marketdata.SourceState, source marketdata.SourceID, now time.Time, limits Limits) Connection {
	return Connection{Type: TypeConnection, State: string(state), Source: string(source), Timestamp: FormatTime(now), Limits: limits}
}

// NewQuoteMessage converts a normalized quote into a snapshot or quote message.
func NewQuoteMessage(msgType string, q marketdata.Quote, priceDecimals int, stale bool) QuoteMessage {
	bid, ask, last := q.Bid.Format(priceDecimals), q.Ask.Format(priceDecimals), q.Last.Format(priceDecimals)
	bidSize, askSize, volume := q.BidSize, q.AskSize, min(q.Volume, marketdata.MaxSafeInteger)
	return QuoteMessage{
		Type: msgType, Symbol: q.Symbol,
		Bid: &bid, Ask: &ask, Last: &last,
		BidSize: &bidSize, AskSize: &askSize, Volume: &volume,
		Sequence: q.Sequence, Timestamp: FormatTime(q.Time), Stale: stale,
	}
}

// NewStale builds a stale message.
func NewStale(symbols []string, since time.Time, reason string) Stale {
	return Stale{Type: TypeStale, Symbols: symbols, Since: FormatTime(since), Reason: reason}
}

// NewError builds an error message.
func NewError(code, message string, symbols []string) ErrorMessage {
	return ErrorMessage{Type: TypeError, Code: code, Message: message, Symbols: symbols}
}

// NewHeartbeat builds a heartbeat message.
func NewHeartbeat(now time.Time) Heartbeat {
	return Heartbeat{Type: TypeHeartbeat, Timestamp: FormatTime(now)}
}

// Encode marshals a message. All message types are plain structs, so encoding cannot fail at run time;
// an error here indicates a programming bug and is returned to the caller.
func Encode(v any) ([]byte, error) { return json.Marshal(v) }
