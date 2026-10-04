// Package admission implements bounded, local request policy independently of Redis.
package admission

import (
	"sync"
	"time"
)

type bucket struct {
	tokens float64
	at     time.Time
}
type peer struct {
	touched time.Time
	buckets map[string]bucket
}
type Limiter struct {
	mu       sync.Mutex
	peers    map[string]*peer
	capacity int
	ttl      time.Duration
}

func New(capacity int, ttl time.Duration) *Limiter {
	return &Limiter{peers: map[string]*peer{}, capacity: capacity, ttl: ttl}
}

// Admit distinguishes intentional request throttling from identity-capacity exhaustion.
func (l *Limiter) Admit(address, policy string, rate float64, burst int, now time.Time) (allowed, capacity bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.peers[address]
	if p == nil {
		if len(l.peers) >= l.capacity {
			for key, value := range l.peers {
				if now.Sub(value.touched) >= l.ttl {
					delete(l.peers, key)
				}
			}
		}
		if len(l.peers) >= l.capacity {
			return false, true
		}
		p = &peer{buckets: map[string]bucket{}}
		l.peers[address] = p
	}
	p.touched = now
	b, exists := p.buckets[policy]
	if !exists {
		b = bucket{tokens: float64(burst), at: now}
	}
	b.tokens = min(float64(burst), b.tokens+max(0, now.Sub(b.at).Seconds())*rate)
	b.at = now
	allowed = b.tokens >= 1
	if allowed {
		b.tokens--
	}
	p.buckets[policy] = b
	return allowed, false
}
