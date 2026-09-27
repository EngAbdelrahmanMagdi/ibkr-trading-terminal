package reconnect

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
)

// maxRandom always returns the upper bound, so delays equal the backoff ceiling.
type maxRandom struct{}

func (maxRandom) Int64N(n int64) int64 { return n - 1 }

// seqRandom returns fixed fractions of the range to exercise jitter deterministically.
type seqRandom struct{ i int }

func (r *seqRandom) Int64N(n int64) int64 {
	fractions := []int64{0, 25, 50, 75, 99}
	v := n * fractions[r.i%len(fractions)] / 100
	r.i++
	return v
}

var errDown = errors.New("down")

func policy() Policy {
	return Policy{Base: 100 * time.Millisecond, Max: 2 * time.Second, MaxAttempts: 6, MaxTotal: time.Minute}
}

func TestDelayIsBoundedExponentialFullJitter(t *testing.T) {
	p := policy()
	want := []time.Duration{100, 200, 400, 800, 1600, 2000, 2000}
	for attempt, ms := range want {
		ceiling := ms * time.Millisecond
		if got := p.Delay(attempt, maxRandom{}); got != ceiling {
			t.Errorf("attempt %d: max delay = %s, want %s", attempt, got, ceiling)
		}
		r := &seqRandom{}
		for range 5 {
			if d := p.Delay(attempt, r); d < 0 || d > ceiling {
				t.Errorf("attempt %d: delay %s outside [0, %s]", attempt, d, ceiling)
			}
		}
	}
	if got := p.Delay(1000, maxRandom{}); got != p.Max {
		t.Errorf("huge attempt: %s, want %s (no overflow)", got, p.Max)
	}
}

func TestPolicyValidation(t *testing.T) {
	for name, p := range map[string]Policy{
		"zero base":     {Base: 0, Max: time.Second, MaxAttempts: 1, MaxTotal: time.Second},
		"max < base":    {Base: time.Second, Max: time.Millisecond, MaxAttempts: 1, MaxTotal: time.Second},
		"no attempts":   {Base: time.Millisecond, Max: time.Second, MaxAttempts: 0, MaxTotal: time.Second},
		"no total time": {Base: time.Millisecond, Max: time.Second, MaxAttempts: 1},
	} {
		if p.Validate() == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// run executes Run in a goroutine and advances the fake clock whenever the loop waits on a timer.
func run(t *testing.T, p Policy, clk *clock.Fake, attempt func(context.Context) error) ([]RetryInfo, error) {
	t.Helper()
	var retries []RetryInfo
	done := make(chan error, 1)
	go func() {
		done <- Run(context.Background(), p, clk, maxRandom{}, attempt, func(ri RetryInfo) { retries = append(retries, ri) })
	}()
	for {
		select {
		case err := <-done:
			return retries, err
		default:
		}
		if clk.Waiters() > 0 {
			clk.Advance(p.Base) // every backoff ceiling is a multiple of Base, so timers fire exactly on time
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}

func TestRunSucceedsAfterTransientFailures(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	calls := 0
	retries, err := run(t, policy(), clk, func(context.Context) error {
		calls++
		if calls < 3 {
			return errDown
		}
		return nil
	})
	if err != nil || calls != 3 || len(retries) != 2 {
		t.Fatalf("err=%v calls=%d retries=%d", err, calls, len(retries))
	}
	if retries[0].Attempt != 1 || retries[0].Delay != 100*time.Millisecond || retries[1].Delay != 200*time.Millisecond {
		t.Fatalf("retries = %+v", retries)
	}
}

func TestRunStopsAfterMaxAttempts(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	calls := 0
	retries, err := run(t, policy(), clk, func(context.Context) error { calls++; return errDown })
	if !errors.Is(err, ErrCycleExhausted) || calls != 6 || len(retries) != 6 {
		t.Fatalf("err=%v calls=%d retries=%d", err, calls, len(retries))
	}
	if last := retries[len(retries)-1]; last.Delay != 0 {
		t.Fatalf("no backoff after the final attempt, got %s", last.Delay)
	}
}

func TestRunStopsAtMaxTotalTime(t *testing.T) {
	p := policy()
	p.MaxAttempts = 100
	p.MaxTotal = 3 * time.Second // 0.1+0.2+0.4+0.8+1.6 = 3.1s exceeds it before the 6th attempt
	clk := clock.NewFake(time.Unix(0, 0))
	calls := 0
	_, err := run(t, p, clk, func(context.Context) error { calls++; return errDown })
	if !errors.Is(err, ErrCycleExhausted) || calls != 5 {
		t.Fatalf("err=%v calls=%d, want exhaustion after 5 attempts", err, calls)
	}
}

func TestRunHonoursCancellation(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, policy(), clk, maxRandom{}, func(context.Context) error { return errDown }, nil)
	}()
	for clk.Waiters() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
