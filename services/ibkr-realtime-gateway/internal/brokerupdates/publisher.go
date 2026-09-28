package brokerupdates

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
)

// Producer sends one record and waits for the broker's acknowledgement (bounded by ctx).
type Producer interface {
	Produce(ctx context.Context, e Event) error
}

// Publisher buffers observations in a bounded queue and publishes them in order with one goroutine. When Kafka is
// unavailable it keeps retrying the oldest event with a capped backoff while new events queue up; once the queue is
// full the oldest queued events are dropped and counted. Nothing is lost silently: reconciliation against the broker
// restores anything that was dropped.
type Publisher struct {
	producer Producer
	capacity int
	timeout  time.Duration
	metrics  *metrics.Gateway
	log      *slog.Logger

	mu     sync.Mutex
	queue  []queued
	seq    uint64
	notify chan struct{} // capacity 1: wakes the sender
}

type queued struct {
	seq   uint64
	event Event
}

// NewPublisher creates a publisher with a queue of capacity events; timeout bounds each send.
func NewPublisher(p Producer, capacity int, timeout time.Duration, m *metrics.Gateway, log *slog.Logger) *Publisher {
	return &Publisher{producer: p, capacity: capacity, timeout: timeout, metrics: m, log: log,
		notify: make(chan struct{}, 1)}
}

// Enqueue adds an event without blocking, dropping the oldest queued event when the queue is full.
func (p *Publisher) Enqueue(e Event) {
	p.mu.Lock()
	if len(p.queue) >= p.capacity {
		p.queue = p.queue[1:]
		p.metrics.BrokerUpdatesDropped.WithLabelValues(DropBufferFull).Inc()
	}
	p.seq++
	p.queue = append(p.queue, queued{seq: p.seq, event: e})
	p.mu.Unlock()
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// Queued returns the number of events waiting to be published.
func (p *Publisher) Queued() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queue)
}

// Run publishes queued events until ctx is cancelled. It is meant to run in its own goroutine, owned by the caller.
func (p *Publisher) Run(ctx context.Context) {
	backoff := 250 * time.Millisecond
	for {
		q, ok := p.peek()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-p.notify:
				continue
			}
		}
		sendCtx, cancel := context.WithTimeout(ctx, p.timeout)
		err := p.producer.Produce(sendCtx, q.event)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			p.pop(q.seq)
			p.metrics.BrokerUpdatesPublished.Inc()
			backoff = 250 * time.Millisecond
			continue
		}
		p.metrics.KafkaProduceErrors.Inc()
		p.log.Warn("broker update not published; retrying", "error", err.Error(), "retryIn", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

func (p *Publisher) peek() (queued, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.queue) == 0 {
		return queued{}, false
	}
	return p.queue[0], true
}

// pop removes the event with sequence seq if it is still at the head (it may have been dropped while being sent).
func (p *Publisher) pop(seq uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.queue) > 0 && p.queue[0].seq == seq {
		p.queue = p.queue[1:]
	}
}

// KafkaProducer publishes to one topic with franz-go: idempotent writes and acknowledgement by all in-sync
// replicas (both franz-go defaults), topics are never auto-created.
type KafkaProducer struct {
	client *kgo.Client
	topic  string
}

// NewKafkaProducer creates the client; it connects lazily.
func NewKafkaProducer(brokers []string, topic string) (*KafkaProducer, error) {
	if len(brokers) == 0 || topic == "" {
		return nil, errors.New("brokerupdates: brokers and topic are required")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("realtime-gateway-broker-updates"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(15*time.Second),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.MaxBufferedRecords(1000),
		kgo.DialTimeout(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{client: client, topic: topic}, nil
}

// Produce sends one event with its key and the standard headers.
func (k *KafkaProducer) Produce(ctx context.Context, e Event) error {
	rec := &kgo.Record{Topic: k.topic, Key: []byte(e.Key), Value: e.Value, Headers: []kgo.RecordHeader{
		{Key: "eventType", Value: []byte(e.Type)},
		{Key: "eventVersion", Value: []byte("1")},
		{Key: "correlationId", Value: correlationID(e.Value)},
	}}
	return k.client.ProduceSync(ctx, rec).FirstErr()
}

// Close flushes nothing further and closes the client.
func (k *KafkaProducer) Close() { k.client.Close() }

func correlationID(value []byte) []byte {
	var env struct {
		CorrelationID string `json:"correlationId"`
	}
	if err := decode(value, &env); err != nil {
		return nil
	}
	return []byte(env.CorrelationID)
}
