package tests

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/brokerupdates"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/config"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/ibkr"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/modes"
)

// recordingSink collects normalized events.
type recordingSink struct {
	mu     sync.Mutex
	events []brokerupdates.Event
}

func (r *recordingSink) Enqueue(e brokerupdates.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recordingSink) snapshot() []brokerupdates.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]brokerupdates.Event(nil), r.events...)
}

type observed struct {
	EventType string `json:"eventType"`
	AccountID string `json:"accountId"`
	Payload   struct {
		BrokerOrderID     string  `json:"brokerOrderId"`
		ClientOrderRef    *string `json:"clientOrderRef"`
		ObservedStatus    string  `json:"observedStatus"`
		BrokerStatusRaw   string  `json:"brokerStatusRaw"`
		FilledQuantity    string  `json:"filledQuantity"`
		RemainingQuantity string  `json:"remainingQuantity"`
		Execution         *struct {
			BrokerExecutionID string `json:"brokerExecutionId"`
			Quantity          string `json:"quantity"`
			Price             string `json:"price"`
		} `json:"execution"`
	} `json:"payload"`
}

func TestBrokerOrderStreamIsNormalizedAndCorrelatedForAnIBKRSourceSelectedThroughAuto(t *testing.T) {
	fake, base, ca := fakeGateway(t)
	fake.SetLiveOrders(`[{"acct":"DU000001","orderId":1001,"order_ref":"TC-A","status":"Submitted",
		"filledQuantity":0.0,"remainingQuantity":10.0}]`)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	clk := clock.Real{}

	fallback, err := modes.Select(*modeConfig(config.ModeAuto, closedPort(t), ca), clk, metrics.New(), logger, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if brokerupdates.Enabled(fallback.Source.ID(), []string{"kafka:29092"}) {
		t.Fatal("AUTO resolved to MOCK must not run the broker order stream")
	}

	sel, err := modes.Select(*modeConfig(config.ModeAuto, base, ca), clk, m, logger, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !brokerupdates.Enabled(sel.Source.ID(), []string{"kafka:29092"}) {
		t.Fatalf("AUTO resolved to %s must run the broker order stream", sel.Source.ID())
	}
	sink := &recordingSink{}
	sel.Source.(*ibkr.Source).SetOrderSink(brokerupdates.NewNormalizer(sink, clk, m, logger))
	sel.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sel.Source.Close(ctx)
	})
	waitUntil(t, "order stream subscriptions", func() bool {
		return fake.CountTopics("sor+") > 0 && fake.CountTopics("str+") > 0 && fake.Requests("/iserver/account/orders") > 0
	})

	// An execution of a seeded order is published at once; one of an order not seen yet waits for its "sor".
	fake.PushRaw(`{"topic":"str","args":[{"execution_id":"0000e0d5.01.01","order_ref":"TC-A","size":4.0,"price":"185.25",
		"trade_time_r":1790582400000,"account":"DU000001","side":"B"}]}`)
	fake.PushRaw(`{"topic":"str","args":[{"execution_id":"0000e0d5.02.01","order_ref":"TC-B","size":"5","price":"12.5000",
		"trade_time_r":1790582401000,"account":"DU000001"}]}`)
	fake.PushRaw(`{"topic":"str","args":[{"execution_id":"0000e0d5.03.01","size":1,"price":"1","trade_time_r":1790582401000,
		"account":"DU000001"}]}`) // manual trade without order_ref: uncorrelated
	fake.PushRaw(`{"topic":"sor","args":[{"acct":"DU000001","orderId":1002,"order_ref":"TC-B","status":"Filled",
		"filledQuantity":5,"remainingQuantity":0}]}`)
	fake.PushRaw(`{"topic":"sor","args":[{"orderId":1002,"order_ref":"TC-B","status":"Cancelled"}]}`) // partial update

	waitUntil(t, "four observations", func() bool { return len(sink.snapshot()) >= 4 })
	events := sink.snapshot()
	var decoded []observed
	for _, e := range events {
		conform(t, "events/broker-order-update.schema.json", e.Value)
		var o observed
		if err := json.Unmarshal(e.Value, &o); err != nil {
			t.Fatal(err)
		}
		if e.Key != o.AccountID+":"+o.Payload.BrokerOrderID {
			t.Fatalf("key %q does not match accountId:brokerOrderId of %s", e.Key, e.Value)
		}
		decoded = append(decoded, o)
	}
	first := decoded[0]
	if first.EventType != brokerupdates.TypeExecution || first.Payload.BrokerOrderID != "1001" ||
		first.Payload.Execution.Quantity != "4" || first.Payload.Execution.Price != "185.25" || first.Payload.ObservedStatus != "WORKING" {
		t.Fatalf("seeded execution: %+v", first)
	}
	if decoded[1].EventType != brokerupdates.TypeStatus || decoded[1].Payload.ObservedStatus != "FILLED" ||
		decoded[2].EventType != brokerupdates.TypeExecution || decoded[2].Payload.BrokerOrderID != "1002" ||
		decoded[2].Payload.Execution.Price != "12.5" {
		t.Fatalf("a waiting execution must follow its order's first update, enriched with the order ID: %+v", decoded[1:3])
	}
	last := decoded[3]
	if last.Payload.ObservedStatus != "CANCELLED" || last.Payload.FilledQuantity != "5" || last.AccountID != "DU000001" {
		t.Fatalf("a partial update keeps the last known values: %+v", last)
	}
	if testutil.ToFloat64(m.BrokerUpdatesDropped.WithLabelValues(brokerupdates.DropUncorrelated)) != 1 {
		t.Fatal("the execution without order_ref must be dropped and counted")
	}
}
