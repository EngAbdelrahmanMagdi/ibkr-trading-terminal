package brokerupdates

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type countingSink struct{ n int }

func (c *countingSink) Enqueue(Event) { c.n++ }

func TestAnExecutionWithoutItsOrderExpiresAndIsCounted(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC))
	m := metrics.New()
	sink := &countingSink{}
	n := NewNormalizer(sink, clk, m, discard)

	n.Observe("str", json.RawMessage(`[{"execution_id":"E1","order_ref":"TC-X","size":1,"price":"10","trade_time_r":1,"account":"DU1"}]`))
	clk.Advance(pendingTTL + time.Second)
	n.Expire()
	n.Observe("sor", json.RawMessage(`[{"acct":"DU1","orderId":7,"order_ref":"TC-X","status":"Filled","filledQuantity":1,"remainingQuantity":0}]`))

	if sink.n != 1 {
		t.Fatalf("only the status update may be published after the execution expired, got %d events", sink.n)
	}
	if got := testutil.ToFloat64(m.BrokerUpdatesDropped.WithLabelValues(DropExpired)); got != 1 {
		t.Fatalf("expired = %v", got)
	}
}

func TestQuantitiesAndPricesAreExactAndNeverRounded(t *testing.T) {
	for in, want := range map[string]string{"10.0": "10", "0.5000": "0.5", "1.725e2": "172.5"} {
		if got, ok := decimal(json.Number(in), 4, 15); !ok || got != want {
			t.Fatalf("decimal(%s) = %q %v, want %q", in, got, ok, want)
		}
	}
	if _, ok := decimal(json.Number("1.23456"), 4, 15); ok {
		t.Fatal("more decimals than allowed must be rejected, not rounded")
	}
}

type flakyProducer struct {
	fail      atomic.Bool
	published atomic.Int32
}

func (f *flakyProducer) Produce(context.Context, Event) error {
	if f.fail.Load() {
		return errors.New("kafka unavailable")
	}
	f.published.Add(1)
	return nil
}

func TestPublisherDropsTheOldestWhenItsBufferIsFullAndDrainsAfterRecovery(t *testing.T) {
	m := metrics.New()
	producer := &flakyProducer{}
	producer.fail.Store(true)
	p := NewPublisher(producer, 3, time.Second, m, discard)
	for i := 0; i < 5; i++ {
		p.Enqueue(Event{Key: "DU1:1", Type: TypeStatus, Value: []byte(`{"correlationId":"x"}`)})
	}
	if p.Queued() != 3 || testutil.ToFloat64(m.BrokerUpdatesDropped.WithLabelValues(DropBufferFull)) != 2 {
		t.Fatalf("queued %d, dropped %v", p.Queued(), testutil.ToFloat64(m.BrokerUpdatesDropped.WithLabelValues(DropBufferFull)))
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { // owned by the test
		defer close(done)
		p.Run(ctx)
	}()
	producer.fail.Store(false)
	deadline := time.Now().Add(5 * time.Second)
	for p.Queued() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	if producer.published.Load() != 3 || p.Queued() != 0 {
		t.Fatalf("published %d, queued %d", producer.published.Load(), p.Queued())
	}
}
