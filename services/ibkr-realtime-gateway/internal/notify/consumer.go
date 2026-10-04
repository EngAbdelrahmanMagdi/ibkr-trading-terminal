package notify

import (
	"context"
	"errors"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/kafkatls"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
)

// Config configures the order-notification consumer.
type Config struct {
	Brokers        []string
	Topic          string
	Group          string
	MaxPollRecords int
}

// Consumer reads the order-events topic in a consumer group and hands each record to the Processor. Offsets are
// committed only after the polled records were handled (at-least-once). A new group starts at the end of the
// topic: past notifications are useless to clients, which load current state over REST. Memory is bounded by the
// poll size and the fetch limits. Kafka problems never affect quotes or readiness; they are counted and retried
// with a capped backoff.
type Consumer struct {
	client  *kgo.Client
	proc    *Processor
	cfg     Config
	metrics *metrics.Gateway
	log     *slog.Logger
}

const (
	fetchMaxBytes          = 4 << 20
	fetchMaxPartitionBytes = 1 << 20
	errorBackoffMin        = 500 * time.Millisecond
	errorBackoffMax        = 30 * time.Second
)

// NewConsumer creates the Kafka client. It does not connect until Run polls.
func NewConsumer(cfg Config, proc *Processor, m *metrics.Gateway, log *slog.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.Group == "" || cfg.MaxPollRecords < 1 {
		return nil, errors.New("notify: brokers, topic, group and a positive poll size are required")
	}
	tlsOption, err := kafkatls.Option()
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(
		tlsOption,
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("realtime-gateway"),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxPartitionBytes(fetchMaxPartitionBytes),
		kgo.DialTimeout(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client, proc: proc, cfg: cfg, metrics: m, log: log}, nil
}

// Run polls until ctx is cancelled. It is meant to run in its own goroutine, owned by the caller.
func (c *Consumer) Run(ctx context.Context) {
	backoff := errorBackoffMin
	for {
		fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		failed := false
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			failed = true
			c.metrics.KafkaConsumeErrors.Inc()
			c.log.Warn("order events fetch failed", "topic", topic, "partition", partition, "error", err.Error())
		})
		records := 0
		fetches.EachRecord(func(r *kgo.Record) {
			records++
			c.proc.Process(r.Value)
		})
		if records > 0 {
			if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
				failed = true
				c.metrics.KafkaConsumeErrors.Inc()
				c.log.Warn("order events offset commit failed", "error", err.Error())
			}
		}
		if !failed {
			backoff = errorBackoffMin
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, errorBackoffMax)
	}
}

// Close leaves the consumer group and closes the client. Call it after Run has returned.
func (c *Consumer) Close() {
	c.client.Close()
}
