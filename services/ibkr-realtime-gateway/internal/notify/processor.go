// Package notify fans out order domain events from Kafka to WebSocket clients as lightweight order-update
// notifications. A notification is only a hint to refetch order state from the trading API: the gateway never
// interprets, aggregates or stores order state. Delivery is at least once, so a duplicate notification is harmless and
// no deduplication store is kept.
package notify

import (
	"encoding/json"
	"log/slog"
	"regexp"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// maxEventBytes bounds the size of an order event the gateway parses.
const maxEventBytes = 64 << 10

// Skip reasons (metric label values).
const (
	SkipUnknownType = "unknown_type"
	SkipMalformed   = "malformed"
	SkipInvalid     = "invalid"
	SkipTooLarge    = "too_large"
)

// Order event types that are fanned out. Other types on the topic, including future ones, are skipped.
var orderEventTypes = map[string]bool{
	"ORDER_CREATED": true, "ORDER_SUBMISSION_PENDING": true, "ORDER_CONFIRMATION_REQUIRED": true,
	"ORDER_SUBMITTED": true, "ORDER_PARTIALLY_FILLED": true, "ORDER_FILLED": true, "ORDER_CANCEL_REQUESTED": true,
	"ORDER_CANCELLED": true, "ORDER_REJECTED": true, "ORDER_FAILED": true, "ORDER_STATUS_UNKNOWN": true,
}

var orderStatuses = map[string]bool{
	"CREATED": true, "SUBMISSION_PENDING": true, "PENDING_CONFIRMATION": true, "SUBMITTED": true,
	"PARTIALLY_FILLED": true, "FILLED": true, "CANCEL_PENDING": true, "CANCELLED": true, "REJECTED": true,
	"FAILED": true, "UNKNOWN": true,
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Broadcaster delivers a notification to every connected client and reports how many received it.
type Broadcaster interface {
	BroadcastOrderUpdate(stream.OrderUpdate) int
}

// orderEvent is the part of the order event envelope the notification needs; unknown fields are ignored.
type orderEvent struct {
	EventType  string `json:"eventType"`
	OrderID    string `json:"orderId"`
	OccurredAt string `json:"occurredAt"`
	Payload    struct {
		Status string `json:"status"`
	} `json:"payload"`
}

// Processor maps order events to notifications. It is safe for concurrent use.
type Processor struct {
	out     Broadcaster
	metrics *metrics.Gateway
	log     *slog.Logger
}

// NewProcessor returns a processor that broadcasts through out.
func NewProcessor(out Broadcaster, m *metrics.Gateway, log *slog.Logger) *Processor {
	return &Processor{out: out, metrics: m, log: log}
}

// Process handles one record value of the order-events topic. It returns the skip reason, or "" when the event
// was fanned out. Records that are not usable are counted and skipped; they never stop the consumer.
func (p *Processor) Process(value []byte) string {
	msg, reason := decode(value)
	if reason != "" {
		p.metrics.OrderEventsSkipped.WithLabelValues(reason).Inc()
		p.log.Debug("order event skipped", "reason", reason)
		return reason
	}
	p.out.BroadcastOrderUpdate(msg)
	p.metrics.OrderNotifications.Inc()
	return ""
}

func decode(value []byte) (stream.OrderUpdate, string) {
	if len(value) > maxEventBytes {
		return stream.OrderUpdate{}, SkipTooLarge
	}
	var ev orderEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return stream.OrderUpdate{}, SkipMalformed
	}
	if !orderEventTypes[ev.EventType] {
		return stream.OrderUpdate{}, SkipUnknownType
	}
	at, err := time.Parse(time.RFC3339Nano, ev.OccurredAt)
	if err != nil || !uuidPattern.MatchString(ev.OrderID) || !orderStatuses[ev.Payload.Status] {
		return stream.OrderUpdate{}, SkipInvalid
	}
	return stream.OrderUpdate{
		Type:       stream.TypeOrderUpdate,
		OrderID:    ev.OrderID,
		EventType:  ev.EventType,
		Status:     ev.Payload.Status,
		OccurredAt: stream.FormatTime(at),
	}, ""
}
