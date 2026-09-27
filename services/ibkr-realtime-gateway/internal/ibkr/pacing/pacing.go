// Package pacing implements the gateway's central IBKR request limiter.
//
// The Realtime Gateway and Trading Core share one IBKR session budget. Each service enforces its own
// configured allocation locally: a token bucket for the allocation, per-endpoint sub-limits from the IBKR
// documentation, a bounded wait queue with an acquire timeout, and a cool-down after any HTTP 429. There are
// no ad-hoc sleeps anywhere else.
package pacing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
)

// Rejection reasons.
var (
	ErrCoolingDown  = errors.New("pacing: cooling down after HTTP 429")
	ErrQueueFull    = errors.New("pacing: wait queue full")
	ErrWaitTimedOut = errors.New("pacing: acquire timed out")
)

// Config is the gateway's share of the IBKR session budget.
type Config struct {
	SessionLimit   float64       // documented requests/second for the whole session
	Allocation     float64       // requests/second this service may use
	Headroom       float64       // requests/second reserved for nobody
	MaxWaiters     int           // bounded wait queue
	AcquireTimeout time.Duration // longest wait for a permit
	Cooldown       time.Duration // pause of non-essential requests after a 429
}

// Validate enforces the shared-budget invariant for this service: allocation + headroom <= session limit,
// headroom > 0.
func (c Config) Validate() error {
	switch {
	case c.SessionLimit <= 0 || c.Allocation <= 0:
		return errors.New("pacing: session limit and allocation must be positive")
	case c.Headroom <= 0:
		return errors.New("pacing: headroom must be positive")
	case c.Allocation+c.Headroom > c.SessionLimit:
		return fmt.Errorf("pacing: allocation %.2f + headroom %.2f exceeds the session limit %.2f", c.Allocation, c.Headroom, c.SessionLimit)
	case c.MaxWaiters < 1 || c.AcquireTimeout <= 0 || c.Cooldown <= 0:
		return errors.New("pacing: wait queue, acquire timeout and cool-down must be positive")
	}
	return nil
}

// Endpoint is a documented per-endpoint limit. Zero values mean "no own limit" (the global one applies).
type Endpoint struct {
	PerSecond float64
	PerMinute int
}

// Limiter is safe for concurrent use.
type Limiter struct {
	cfg       Config
	clock     clock.Clock
	metrics   *metrics.Gateway
	endpoints map[string]Endpoint

	mu            sync.Mutex
	tokens        float64
	refilled      time.Time
	perSecond     map[string]*bucket
	perMinute     map[string][]time.Time // bounded ring of recent request times per endpoint
	waiters       int
	cooldownUntil time.Time
}

type bucket struct {
	tokens   float64
	refilled time.Time
}

// New creates a limiter. endpoints maps an endpoint class to its documented limits.
func New(cfg Config, endpoints map[string]Endpoint, clk clock.Clock, m *metrics.Gateway) (*Limiter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	now := clk.Now()
	return &Limiter{
		cfg: cfg, clock: clk, metrics: m, endpoints: endpoints,
		tokens: burst(cfg.Allocation), refilled: now,
		perSecond: map[string]*bucket{}, perMinute: map[string][]time.Time{},
	}, nil
}

func burst(rate float64) float64 { return max(1, rate) }

// Acquire waits for a permit for one request of the endpoint class. Essential requests (session keepalive)
// are allowed during a cool-down, because losing the session would be worse; they are still paced.
func (l *Limiter) Acquire(ctx context.Context, endpoint string, essential bool) error {
	start := l.clock.Now()
	queued := false
	defer func() {
		if queued {
			l.mu.Lock()
			l.waiters--
			l.mu.Unlock()
		}
		l.metrics.IBKRLimiterWait.Observe(l.clock.Now().Sub(start).Seconds())
	}()
	for {
		l.mu.Lock()
		now := l.clock.Now()
		if !essential && now.Before(l.cooldownUntil) {
			l.mu.Unlock()
			l.reject(endpoint, "cooldown")
			return ErrCoolingDown
		}
		wait := l.tryLocked(endpoint, now)
		if wait == 0 {
			l.mu.Unlock()
			return nil
		}
		if now.Add(wait).Sub(start) > l.cfg.AcquireTimeout {
			l.mu.Unlock()
			l.reject(endpoint, "timeout")
			return ErrWaitTimedOut
		}
		if !queued {
			if l.waiters >= l.cfg.MaxWaiters {
				l.mu.Unlock()
				l.reject(endpoint, "queue_full")
				return ErrQueueFull
			}
			l.waiters++
			queued = true
		}
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.clock.After(wait):
		}
	}
}

func (l *Limiter) reject(endpoint, reason string) {
	l.metrics.IBKRLimiterRejected.WithLabelValues(endpoint, reason).Inc()
}

// tryLocked consumes a permit if every applicable limit allows it, otherwise returns how long to wait.
func (l *Limiter) tryLocked(endpoint string, now time.Time) time.Duration {
	// Global allocation.
	l.tokens = min(burst(l.cfg.Allocation), l.tokens+now.Sub(l.refilled).Seconds()*l.cfg.Allocation)
	l.refilled = now
	wait := time.Duration(0)
	if l.tokens < 1 {
		wait = time.Duration((1 - l.tokens) / l.cfg.Allocation * float64(time.Second))
	}
	lim := l.endpoints[endpoint]
	var b *bucket
	if lim.PerSecond > 0 {
		b = l.perSecond[endpoint]
		if b == nil {
			b = &bucket{tokens: burst(lim.PerSecond), refilled: now}
			l.perSecond[endpoint] = b
		}
		b.tokens = min(burst(lim.PerSecond), b.tokens+now.Sub(b.refilled).Seconds()*lim.PerSecond)
		b.refilled = now
		if b.tokens < 1 {
			wait = max(wait, time.Duration((1-b.tokens)/lim.PerSecond*float64(time.Second)))
		}
	}
	var recent []time.Time
	if lim.PerMinute > 0 {
		recent = l.perMinute[endpoint]
		for len(recent) > 0 && now.Sub(recent[0]) >= time.Minute {
			recent = recent[1:]
		}
		l.perMinute[endpoint] = recent
		if len(recent) >= lim.PerMinute {
			wait = max(wait, time.Minute-now.Sub(recent[0]))
		}
	}
	if wait > 0 {
		return max(wait, time.Millisecond)
	}
	l.tokens--
	if b != nil {
		b.tokens--
	}
	if lim.PerMinute > 0 {
		l.perMinute[endpoint] = append(recent, now)
	}
	return 0
}

// Report429 starts (or extends) the cool-down after IBKR answered 429 Too Many Requests.
func (l *Limiter) Report429() {
	l.metrics.IBKRRateLimited.Inc()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cooldownUntil = l.clock.Now().Add(l.cfg.Cooldown)
}

// CoolingDown reports whether a cool-down is in effect.
func (l *Limiter) CoolingDown() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.clock.Now().Before(l.cooldownUntil)
}
