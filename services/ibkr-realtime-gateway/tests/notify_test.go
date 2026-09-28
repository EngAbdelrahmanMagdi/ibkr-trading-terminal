package tests

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/notify"
)

// An order event exactly as Trading Core publishes it on trading.order-events.v1.
const orderFilledEvent = `{
  "eventId": "0c9d8e7f-6a5b-4c3d-8e1f-a2b3c4d5e6f7", "eventType": "ORDER_FILLED", "eventVersion": 1,
  "occurredAt": "2026-09-27T14:03:11.123456Z", "source": "trading-core",
  "correlationId": "3f1c2a9e-4b7d-4c8e-9f0a-1b2c3d4e5f60", "accountId": "DU000000",
  "orderId": "5b0f6a2e-1c3d-4e5f-8a9b-0c1d2e3f4a5b", "symbol": "NVDA",
  "payload": {"clientOrderId": "TC-5B0F6A2E1C3D4E5F8A9B0C1D2E3F4A5B", "brokerOrderId": "987654", "status": "FILLED",
    "previousStatus": "SUBMITTED", "intent": "BUY", "brokerSide": "BUY", "orderType": "MARKET", "quantity": "10",
    "filledQuantity": "10", "limitPrice": null, "averageFillPrice": "184.21", "timeInForce": "DAY", "reason": null}}`

const executionRecordedEvent = `{
  "eventId": "1d9d8e7f-6a5b-4c3d-8e1f-a2b3c4d5e6f7", "eventType": "EXECUTION_RECORDED", "eventVersion": 1,
  "occurredAt": "2026-09-27T14:03:11.100Z", "source": "trading-core",
  "correlationId": "3f1c2a9e-4b7d-4c8e-9f0a-1b2c3d4e5f60", "accountId": "DU000000",
  "orderId": "5b0f6a2e-1c3d-4e5f-8a9b-0c1d2e3f4a5b", "symbol": "NVDA",
  "payload": {"executionId": "7a0f6a2e-1c3d-4e5f-8a9b-0c1d2e3f4a5b", "brokerExecutionId": "0000e0d5.01.01",
    "side": "BUY", "quantity": "10", "price": "184.21", "commission": "1.00", "currency": "USD",
    "executedAt": "2026-09-27T14:03:11.100Z"}}`

func TestOrderEventsAreFannedOutAsOrderUpdateNotifications(t *testing.T) {
	conform(t, "events/trading-order-event.schema.json", []byte(orderFilledEvent))
	conform(t, "events/trading-execution-event.schema.json", []byte(executionRecordedEvent))

	g := startGateway(t, nil)
	first, second := g.mustDial(), g.mustDial()
	mustRead(t, first)  // connection
	mustRead(t, second) // connection
	proc := notify.NewProcessor(g.ws, g.metrics, slog.New(slog.NewTextHandler(io.Discard, nil)))

	skipped := map[string]string{
		executionRecordedEvent:         notify.SkipUnknownType, // executions are not fanned out
		`{"eventType": "ORDER_FILLED"`: notify.SkipMalformed,
		strings.Replace(orderFilledEvent, `"status": "FILLED"`, `"status": "SETTLED"`, 1):    notify.SkipInvalid,
		`{"eventType": "ORDER_ARCHIVED", "orderId": "5b0f6a2e-1c3d-4e5f-8a9b-0c1d2e3f4a5b"}`: notify.SkipUnknownType,
	}
	for event, want := range skipped {
		if got := proc.Process([]byte(event)); got != want {
			t.Fatalf("Process(%s...) = %q, want skipped as %q", firstLine(event), got, want)
		}
	}
	if got := proc.Process([]byte(orderFilledEvent)); got != "" {
		t.Fatalf("order event skipped: %s", got)
	}

	for _, conn := range []*websocket.Conn{first, second} {
		m := mustRead(t, conn) // validated against the server-message contract by read
		if m.Type != "order-update" || m.OrderID != "5b0f6a2e-1c3d-4e5f-8a9b-0c1d2e3f4a5b" ||
			m.EventType != "ORDER_FILLED" || m.Status != "FILLED" || m.Occurred != "2026-09-27T14:03:11.123Z" {
			t.Fatalf("notification = %+v", m)
		}
	}
	if got := testutil.ToFloat64(g.metrics.OrderNotifications); got != 1 {
		t.Fatalf("order_notifications_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(g.metrics.OrderEventsSkipped.WithLabelValues(notify.SkipUnknownType)); got != 2 {
		t.Fatalf("skipped unknown_type = %v, want 2", got)
	}
}
