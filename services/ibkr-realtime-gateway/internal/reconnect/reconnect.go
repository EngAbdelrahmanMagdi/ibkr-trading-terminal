// Package reconnect implements bounded reconnection with full-jitter exponential backoff.
//
// A cycle makes at most Policy.MaxAttempts attempts and stops once Policy.MaxTotal has elapsed; it then
// returns ErrCycleExhausted. Callers start a new cycle only on an explicit external trigger (for example a
// health probe reporting that the upstream is available again), never in an unbounded loop.
package reconnect

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
)

// ErrCycleExhausted is returned when a cycle ends without a successful attempt.
var ErrCycleExhausted = errors.New("reconnect: attempts exhausted")

// Policy bounds a reconnect cycle.
type Policy struct {
	Base        time.Duration // backoff before the second attempt (upper bound of the jitter range)
	Max         time.Duration // upper bound of any single backoff
	MaxAttempts int           // attempts per cycle
	MaxTotal    time.Duration // wall time per cycle
}

// Validate checks that the policy is bounded.
func (p Policy) Validate() error {
	switch {
	case p.Base <= 0 || p.Max < p.Base:
		return errors.New("reconnect: require 0 < Base <= Max")
	case p.MaxAttempts < 1:
		return errors.New("reconnect: MaxAttempts must be at least 1")
	case p.MaxTotal <= 0:
		return errors.New("reconnect: MaxTotal must be positive")
	}
	return nil
}

// Random supplies jitter. *math/rand/v2.Rand satisfies it; tests use a deterministic implementation.
type Random interface {
	Int64N(n int64) int64
}

// Delay returns the backoff after the given failed attempt (0-based): a uniform random duration in
// [0, min(Max, Base*2^attempt)] ("full jitter").
func (p Policy) Delay(attempt int, r Random) time.Duration {
	ceiling := p.Max
	if attempt < 62 {
		if d := p.Base << uint(attempt); d > 0 && d < p.Max {
			ceiling = d
		}
	}
	return time.Duration(r.Int64N(int64(ceiling) + 1))
}

// RetryInfo describes a failed attempt.
type RetryInfo struct {
	Attempt int           // 1-based attempt number that failed
	Err     error         // the attempt's error
	Delay   time.Duration // backoff before the next attempt (0 when no attempt follows)
}

// Run calls attempt until it succeeds, ctx is done, or the cycle bound is reached. onRetry (optional) is
// called after every failed attempt.
func Run(ctx context.Context, p Policy, clk clock.Clock, r Random, attempt func(context.Context) error, onRetry func(RetryInfo)) error {
	if err := p.Validate(); err != nil {
		return err
	}
	start := clk.Now()
	var lastErr error
	for n := 0; n < p.MaxAttempts; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = attempt(ctx)
		if lastErr == nil {
			return nil
		}
		delay := p.Delay(n, r)
		last := n == p.MaxAttempts-1 || clk.Now().Add(delay).Sub(start) > p.MaxTotal
		if last {
			delay = 0
		}
		if onRetry != nil {
			onRetry(RetryInfo{Attempt: n + 1, Err: lastErr, Delay: delay})
		}
		if last {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clk.After(delay):
		}
	}
	return fmt.Errorf("%w: %v", ErrCycleExhausted, lastErr)
}
