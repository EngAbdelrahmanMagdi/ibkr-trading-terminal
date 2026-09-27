// Package connstate implements the explicit connection state machine of the market-data source:
//
//	DISCONNECTED -> CONNECTING -> AUTHENTICATING -> READY <-> DEGRADED -> RECONNECTING -> CONNECTING ...
//
// Sources without an authentication step may go from CONNECTING directly to READY. Invalid transitions are
// rejected so that a misbehaving source cannot put the gateway into an impossible state.
package connstate

import (
	"fmt"
	"sync"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

var allowed = map[marketdata.SourceState][]marketdata.SourceState{
	marketdata.StateDisconnected:   {marketdata.StateConnecting},
	marketdata.StateConnecting:     {marketdata.StateAuthenticating, marketdata.StateReady, marketdata.StateReconnecting, marketdata.StateDisconnected},
	marketdata.StateAuthenticating: {marketdata.StateReady, marketdata.StateReconnecting, marketdata.StateDisconnected},
	marketdata.StateReady:          {marketdata.StateDegraded, marketdata.StateReconnecting, marketdata.StateDisconnected},
	marketdata.StateDegraded:       {marketdata.StateReady, marketdata.StateReconnecting, marketdata.StateDisconnected},
	marketdata.StateReconnecting:   {marketdata.StateConnecting, marketdata.StateDisconnected},
}

// Valid reports whether from -> to is an allowed transition.
func Valid(from, to marketdata.SourceState) bool {
	for _, s := range allowed[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Snapshot is the current state with its metadata.
type Snapshot struct {
	State         marketdata.SourceState
	Since         time.Time
	ErrorCategory string
}

// Machine tracks the current state. It is safe for concurrent use.
type Machine struct {
	mu      sync.Mutex
	current Snapshot
}

// New returns a machine in DISCONNECTED.
func New(now time.Time) *Machine {
	return &Machine{current: Snapshot{State: marketdata.StateDisconnected, Since: now}}
}

// Transition moves to the state of ev. It returns the previous state and an error (leaving the state
// unchanged) when the transition is not allowed. Repeating the current state is a no-op.
func (m *Machine) Transition(ev marketdata.StatusEvent) (marketdata.SourceState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	from := m.current.State
	if from == ev.State {
		m.current.ErrorCategory = ev.ErrorCategory
		return from, nil
	}
	if !Valid(from, ev.State) {
		return from, fmt.Errorf("connstate: invalid transition %s -> %s", from, ev.State)
	}
	m.current = Snapshot{State: ev.State, Since: ev.At, ErrorCategory: ev.ErrorCategory}
	return from, nil
}

// Current returns the current state.
func (m *Machine) Current() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}
