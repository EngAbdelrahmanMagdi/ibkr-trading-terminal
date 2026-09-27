package connstate

import (
	"testing"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

var t0 = time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC)

func ev(s marketdata.SourceState, at time.Time) marketdata.StatusEvent {
	return marketdata.StatusEvent{State: s, At: at}
}

func TestLifecycleTransitions(t *testing.T) {
	m := New(t0)
	path := []marketdata.SourceState{
		marketdata.StateConnecting, marketdata.StateAuthenticating, marketdata.StateReady,
		marketdata.StateDegraded, marketdata.StateReady, marketdata.StateReconnecting,
		marketdata.StateConnecting, marketdata.StateReady, marketdata.StateReconnecting, marketdata.StateDisconnected,
		marketdata.StateConnecting,
	}
	for i, s := range path {
		at := t0.Add(time.Duration(i+1) * time.Second)
		if _, err := m.Transition(ev(s, at)); err != nil {
			t.Fatalf("step %d -> %s: %v", i, s, err)
		}
		if got := m.Current(); got.State != s || !got.Since.Equal(at) {
			t.Fatalf("step %d: current = %+v", i, got)
		}
	}
}

func TestInvalidTransitionsAreRejectedAndStateIsKept(t *testing.T) {
	invalid := [][2]marketdata.SourceState{
		{marketdata.StateDisconnected, marketdata.StateReady},
		{marketdata.StateDisconnected, marketdata.StateAuthenticating},
		{marketdata.StateReady, marketdata.StateConnecting},
		{marketdata.StateReady, marketdata.StateAuthenticating},
		{marketdata.StateDegraded, marketdata.StateConnecting},
		{marketdata.StateReconnecting, marketdata.StateReady},
		{marketdata.StateAuthenticating, marketdata.StateDegraded},
		{marketdata.StateConnecting, marketdata.StateDegraded},
	}
	for _, c := range invalid {
		if Valid(c[0], c[1]) {
			t.Errorf("%s -> %s must be invalid", c[0], c[1])
		}
	}

	m := New(t0)
	from, err := m.Transition(ev(marketdata.StateReady, t0.Add(time.Second)))
	if err == nil || from != marketdata.StateDisconnected {
		t.Fatalf("expected rejection from DISCONNECTED, got from=%s err=%v", from, err)
	}
	if got := m.Current(); got.State != marketdata.StateDisconnected || !got.Since.Equal(t0) {
		t.Fatalf("state changed after invalid transition: %+v", got)
	}
}

func TestRepeatedStateUpdatesOnlyTheErrorCategory(t *testing.T) {
	m := New(t0)
	if _, err := m.Transition(ev(marketdata.StateConnecting, t0.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	from, err := m.Transition(marketdata.StatusEvent{State: marketdata.StateConnecting, ErrorCategory: "TIMEOUT", At: t0.Add(5 * time.Second)})
	if err != nil || from != marketdata.StateConnecting {
		t.Fatalf("from=%s err=%v", from, err)
	}
	got := m.Current()
	if !got.Since.Equal(t0.Add(time.Second)) || got.ErrorCategory != "TIMEOUT" {
		t.Fatalf("current = %+v", got)
	}
}
