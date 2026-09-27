// Package clock abstracts time so that time-dependent logic can be tested deterministically.
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock provides the current time and timers. Implementations must return UTC times.
type Clock interface {
	Now() time.Time
	// After returns a channel that receives once d has elapsed.
	After(d time.Duration) <-chan time.Time
}

// Real is the system clock.
type Real struct{}

// Now returns the current UTC time.
func (Real) Now() time.Time { return time.Now().UTC() }

// After wraps time.After.
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Fake is a manually advanced clock for tests.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

// NewFake returns a fake clock set to start (converted to UTC).
func NewFake(start time.Time) *Fake { return &Fake{now: start.UTC()} }

// Now returns the fake current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After returns a channel that fires when the fake time reaches now+d.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1) // buffered: firing never blocks the clock
	at := f.now.Add(d)
	if d <= 0 {
		ch <- f.now
		return ch
	}
	f.waiters = append(f.waiters, waiter{at: at, ch: ch})
	return ch
}

// Waiters returns the number of pending timers (useful to synchronize tests with goroutines).
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

// Advance moves the fake time forward and fires every timer that is due, in time order.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	now := f.now
	sort.Slice(f.waiters, func(i, j int) bool { return f.waiters[i].at.Before(f.waiters[j].at) })
	var due []waiter
	kept := f.waiters[:0]
	for _, w := range f.waiters {
		if !w.at.After(now) {
			due = append(due, w)
		} else {
			kept = append(kept, w)
		}
	}
	f.waiters = kept
	f.mu.Unlock()
	for _, w := range due {
		w.ch <- w.at
	}
}
