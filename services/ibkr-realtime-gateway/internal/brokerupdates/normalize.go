// Package brokerupdates turns the IBKR order stream into broker-neutral observations on the broker.order-updates.v1
// topic (contracts/schemas/events/broker-order-update.schema.json). It only normalizes: it never decides what an
// observation means for an order, which is the trading core's job.
//
// IBKR sources (docs checked 2026-09-28, https://www.interactivebrokers.com/docs/web-api/):
//   - websocket topic "sor" (live order updates): acct, orderId, order_ref (the cOID sent with the order), status,
//     filledQuantity, remainingQuantity, lastExecutionTime_r;
//   - websocket topic "str" (trades): execution_id, order_ref, size, price, trade_time_r, account. It carries no
//     order ID, so executions are correlated with their order through order_ref;
//   - GET /iserver/account/orders (the current day's orders), read when the stream starts to seed the correlation.
package brokerupdates

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
)

// Event types.
const (
	TypeStatus    = "BROKER_ORDER_STATUS_OBSERVED"
	TypeExecution = "BROKER_EXECUTION_OBSERVED"
)

// Drop reasons (metric label values).
const (
	DropInvalid      = "invalid"
	DropUncorrelated = "uncorrelated"
	DropExpired      = "expired"
	DropPendingFull  = "pending_full"
	DropBufferFull   = "buffer_full"
)

// Limits of the correlation state.
const (
	maxCorrelations = 10_000
	maxPending      = 1_000
	pendingTTL      = 10 * time.Second
)

var (
	identifier = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
	accountID  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

// Event is one observation ready to publish: the Kafka key (accountId:brokerOrderId) and the envelope.
type Event struct {
	Key   string
	Type  string
	Value []byte
}

// Sink receives normalized events (the publisher). It must not block.
type Sink interface {
	Enqueue(Event)
}

type envelope struct {
	EventID       string  `json:"eventId"`
	EventType     string  `json:"eventType"`
	EventVersion  int     `json:"eventVersion"`
	OccurredAt    string  `json:"occurredAt"`
	Source        string  `json:"source"`
	CorrelationID string  `json:"correlationId"`
	AccountID     string  `json:"accountId"`
	Payload       payload `json:"payload"`
}

type payload struct {
	BrokerOrderID     string     `json:"brokerOrderId"`
	ClientOrderRef    *string    `json:"clientOrderRef"`
	ObservedStatus    string     `json:"observedStatus"`
	BrokerStatusRaw   string     `json:"brokerStatusRaw"`
	FilledQuantity    string     `json:"filledQuantity"`
	RemainingQuantity string     `json:"remainingQuantity"`
	AveragePrice      *string    `json:"averagePrice"`
	Execution         *execution `json:"execution"`
	SourceTimestamp   string     `json:"sourceTimestamp"`
}

type execution struct {
	BrokerExecutionID string `json:"brokerExecutionId"`
	Quantity          string `json:"quantity"`
	Price             string `json:"price"`
	ExecutedAt        string `json:"executedAt"`
}

// orderState is the latest known order row for one order_ref, used to correlate and describe executions.
type orderState struct {
	account   string
	orderID   string
	status    string
	raw       string
	filled    string
	remaining string
}

type pendingExecution struct {
	ref  string
	exec execution
	at   time.Time
}

// ibkrOrder is the part of a "sor" message or a live-orders row that is used.
type ibkrOrder struct {
	Account           string      `json:"acct"`
	OrderID           json.Number `json:"orderId"`
	OrderRef          string      `json:"order_ref"`
	Status            string      `json:"status"`
	FilledQuantity    json.Number `json:"filledQuantity"`
	RemainingQuantity json.Number `json:"remainingQuantity"`
	LastExecutionR    json.Number `json:"lastExecutionTime_r"`
}

// ibkrTrade is the part of an "str" message that is used.
type ibkrTrade struct {
	ExecutionID string      `json:"execution_id"`
	OrderRef    string      `json:"order_ref"`
	Size        json.Number `json:"size"`
	Price       json.Number `json:"price"`
	TradeTimeR  json.Number `json:"trade_time_r"`
	Account     string      `json:"account"`
}

// Normalizer converts order-stream messages into observations. It implements ibkr.OrderStreamSink and is safe for
// concurrent use. Its memory is bounded: at most maxCorrelations orders and maxPending waiting executions.
type Normalizer struct {
	out     Sink
	clock   clock.Clock
	metrics *metrics.Gateway
	log     *slog.Logger

	mu      sync.Mutex
	orders  map[string]*orderState // by order_ref
	order   []string               // insertion order of orders, for eviction
	pending []pendingExecution
}

// NewNormalizer returns a normalizer that sends events to out.
func NewNormalizer(out Sink, clk clock.Clock, m *metrics.Gateway, log *slog.Logger) *Normalizer {
	return &Normalizer{out: out, clock: clk, metrics: m, log: log, orders: map[string]*orderState{}}
}

// Seed records the current day's orders (GET /iserver/account/orders "orders") for correlation. Nothing is published.
func (n *Normalizer) Seed(orders json.RawMessage) {
	var rows []ibkrOrder
	if len(orders) == 0 || string(orders) == "null" {
		return
	}
	if err := decode(orders, &rows); err != nil {
		n.drop(DropInvalid, 1)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, r := range rows {
		if st, ok := n.state(r, n.orders[r.OrderRef]); ok {
			n.remember(r.OrderRef, st)
		}
	}
}

// Observe handles one "sor" or "str" message (its args).
func (n *Normalizer) Observe(topic string, args json.RawMessage) {
	switch topic {
	case "sor":
		var rows []ibkrOrder
		if err := decode(args, &rows); err != nil {
			n.drop(DropInvalid, 1)
			return
		}
		for _, r := range rows {
			n.observeOrder(r)
		}
	case "str":
		var rows []ibkrTrade
		if err := decode(args, &rows); err != nil {
			n.drop(DropInvalid, 1)
			return
		}
		for _, r := range rows {
			n.observeTrade(r)
		}
	}
	n.Expire()
}

func (n *Normalizer) observeOrder(r ibkrOrder) {
	n.mu.Lock()
	st, ok := n.state(r, n.orders[r.OrderRef])
	var ready []pendingExecution
	if ok && r.OrderRef != "" {
		n.remember(r.OrderRef, st)
		ready = n.takePending(r.OrderRef)
	}
	n.mu.Unlock()
	if !ok {
		n.drop(DropInvalid, 1)
		return
	}
	ts := n.clock.Now()
	if ms, err := r.LastExecutionR.Int64(); err == nil && ms > 0 {
		ts = time.UnixMilli(ms)
	}
	n.emit(st, optionalRef(r.OrderRef), nil, ts)
	for _, p := range ready {
		ref, exec := p.ref, p.exec
		n.emit(st, &ref, &exec, ts)
	}
}

func (n *Normalizer) observeTrade(r ibkrTrade) {
	exec, ok := toExecution(r)
	if !ok || !accountID.MatchString(r.Account) {
		n.drop(DropInvalid, 1)
		return
	}
	if r.OrderRef == "" || !identifier.MatchString(r.OrderRef) {
		// Without an order reference an execution cannot be tied to an order (for example a manual trade).
		n.drop(DropUncorrelated, 1)
		return
	}
	n.mu.Lock()
	var st orderState
	known, full := false, false
	if s, ok := n.orders[r.OrderRef]; ok {
		st, known = *s, true
	} else if len(n.pending) >= maxPending {
		full = true
	} else {
		n.pending = append(n.pending, pendingExecution{ref: r.OrderRef, exec: exec, at: n.clock.Now()})
	}
	n.mu.Unlock()
	switch {
	case known:
		ref := r.OrderRef
		n.emit(st, &ref, &exec, n.clock.Now())
	case full:
		n.drop(DropPendingFull, 1)
	}
}

// Expire drops executions that waited too long for their order. Observe calls it; it can also run periodically.
func (n *Normalizer) Expire() {
	now := n.clock.Now()
	n.mu.Lock()
	kept := n.pending[:0]
	expired := 0
	for _, p := range n.pending {
		if now.Sub(p.at) > pendingTTL {
			expired++
			continue
		}
		kept = append(kept, p)
	}
	n.pending = kept
	n.mu.Unlock()
	if expired > 0 {
		n.drop(DropExpired, expired)
		n.log.Warn("order stream: executions without a known order dropped; reconciliation recovers them", "count", expired)
	}
}

// state validates and normalizes an order row (n.mu held). An update may carry only the fields that changed:
// missing values are taken from the last known row of the same order, never guessed.
func (n *Normalizer) state(r ibkrOrder, prev *orderState) (orderState, bool) {
	if prev != nil && r.OrderID != "" && prev.orderID != r.OrderID.String() {
		prev = nil
	}
	if prev != nil {
		if r.OrderID == "" {
			r.OrderID = json.Number(prev.orderID)
		}
		if r.Account == "" {
			r.Account = prev.account
		}
		if r.Status == "" {
			r.Status = prev.raw
		}
		if r.FilledQuantity == "" {
			r.FilledQuantity = json.Number(prev.filled)
		}
		if r.RemainingQuantity == "" {
			r.RemainingQuantity = json.Number(prev.remaining)
		}
	}
	orderID := r.OrderID.String()
	if !identifier.MatchString(orderID) || !accountID.MatchString(r.Account) || r.Status == "" || len(r.Status) > 64 {
		return orderState{}, false
	}
	filled, ok1 := quantity(r.FilledQuantity)
	remaining, ok2 := quantity(r.RemainingQuantity)
	if !ok1 || !ok2 {
		return orderState{}, false
	}
	return orderState{account: r.Account, orderID: orderID, status: observedStatus(r.Status, filled), raw: r.Status,
		filled: filled, remaining: remaining}, true
}

func (n *Normalizer) remember(ref string, st orderState) {
	if ref == "" || !identifier.MatchString(ref) {
		return
	}
	if _, exists := n.orders[ref]; !exists {
		n.order = append(n.order, ref)
		for len(n.order) > maxCorrelations {
			delete(n.orders, n.order[0])
			n.order = n.order[1:]
		}
	}
	s := st
	n.orders[ref] = &s
}

func (n *Normalizer) takePending(ref string) []pendingExecution {
	var ready []pendingExecution
	kept := n.pending[:0]
	for _, p := range n.pending {
		if p.ref == ref {
			ready = append(ready, p)
		} else {
			kept = append(kept, p)
		}
	}
	n.pending = kept
	return ready
}

func (n *Normalizer) emit(st orderState, ref *string, exec *execution, sourceTime time.Time) {
	eventType := TypeStatus
	if exec != nil {
		eventType = TypeExecution
	}
	ev := envelope{
		EventID: newUUID(), EventType: eventType, EventVersion: 1, OccurredAt: stream.FormatTime(n.clock.Now()),
		Source: "realtime-gateway", CorrelationID: newUUID(), AccountID: st.account,
		Payload: payload{
			BrokerOrderID: st.orderID, ClientOrderRef: ref, ObservedStatus: st.status, BrokerStatusRaw: st.raw,
			FilledQuantity: st.filled, RemainingQuantity: st.remaining, Execution: exec,
			SourceTimestamp: stream.FormatTime(sourceTime),
		},
	}
	data, err := json.Marshal(ev)
	if err != nil {
		n.drop(DropInvalid, 1)
		return
	}
	n.out.Enqueue(Event{Key: st.account + ":" + st.orderID, Type: eventType, Value: data})
}

func (n *Normalizer) drop(reason string, count int) {
	n.metrics.BrokerUpdatesDropped.WithLabelValues(reason).Add(float64(count))
}

// observedStatus maps an IBKR order status to the neutral status of the contract.
func observedStatus(raw, filled string) string {
	switch strings.ToLower(raw) {
	case "submitted", "presubmitted", "pendingsubmit", "pendingcancel", "precancelled":
		if filled != "0" {
			return "PARTIALLY_FILLED"
		}
		return "WORKING"
	case "filled":
		return "FILLED"
	case "cancelled":
		return "CANCELLED"
	case "inactive":
		return "INACTIVE"
	default:
		return "OTHER"
	}
}

func toExecution(r ibkrTrade) (execution, bool) {
	if !identifier.MatchString(r.ExecutionID) {
		return execution{}, false
	}
	qty, ok := quantity(r.Size)
	if !ok || qty == "0" {
		return execution{}, false
	}
	price, ok := decimal(r.Price, 6, 13)
	if !ok || price == "0" {
		return execution{}, false
	}
	ms, err := r.TradeTimeR.Int64()
	if err != nil || ms <= 0 {
		return execution{}, false
	}
	return execution{BrokerExecutionID: r.ExecutionID, Quantity: qty, Price: price,
		ExecutedAt: stream.FormatTime(time.UnixMilli(ms))}, true
}

// quantity returns the exact decimal text of a quantity (at most 4 significant decimals).
func quantity(n json.Number) (string, bool) { return decimal(n, 4, 15) }

// decimal parses a JSON number or numeric string exactly and drops only trailing fractional zeros (the value is
// unchanged). More significant decimals than maxScale are rejected, never rounded.
func decimal(n json.Number, maxScale, maxWhole int) (string, bool) {
	text := strings.TrimSpace(n.String())
	if text == "" {
		return "", false
	}
	d, err := marketdata.ParseDecimalNumber(text)
	if err != nil {
		return "", false
	}
	whole, frac, _ := strings.Cut(d.String(), ".")
	frac = strings.TrimRight(frac, "0")
	if len(frac) > maxScale || len(whole) > maxWhole {
		return "", false
	}
	if frac == "" {
		return whole, true
	}
	return whole + "." + frac, true
}

func optionalRef(ref string) *string {
	if ref == "" || !identifier.MatchString(ref) {
		return nil
	}
	return &ref
}

// decode reads numbers as json.Number (exact); numbers sent as strings are accepted too.
func decode(data json.RawMessage, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	return dec.Decode(v)
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error on supported platforms
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// RunExpiry expires waiting executions every interval until ctx is cancelled (run in a goroutine owned by the caller).
func (n *Normalizer) RunExpiry(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.Expire()
		}
	}
}

// Enabled reports whether the broker order stream runs: only when the market-data source selected for this process is
// IBKR (configured as IBKR, or AUTO resolved to IBKR; both require a trusted environment) and Kafka is configured.
func Enabled(source marketdata.SourceID, kafkaBrokers []string) bool {
	return source == marketdata.SourceIBKR && len(kafkaBrokers) > 0
}
