package tests

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/transport/ws"
)

func subscribeMsg(symbols ...string) map[string]any {
	return map[string]any{"type": "subscribe", "symbols": symbols}
}

func TestConnectionMessageAdvertisesLimitsAndMockSource(t *testing.T) {
	g := startGateway(t, nil)
	conn := g.mustDial()
	m := mustRead(t, conn)
	cfg := defaultConfig()
	if m.Type != "connection" || m.State != "READY" || m.Source != "MOCK" {
		t.Fatalf("first message = %+v, want a READY connection message from MOCK", m)
	}
	if m.Limits.MaxSymbolsPerSubscribe != cfg.MaxSymbolsPerSubscribe || m.Limits.MaxSubscribedSymbols != cfg.MaxSubscribedSymbols ||
		m.Limits.MaxInboundMessageBytes != cfg.MaxInboundMessageBytes || m.Limits.HeartbeatIntervalMs != cfg.HeartbeatInterval.Milliseconds() {
		t.Fatalf("advertised limits %+v do not match the configuration", m.Limits)
	}
}

func TestSnapshotPrecedesOrderedDeterministicQuotes(t *testing.T) {
	g := startGateway(t, nil)
	conn := g.mustDial()
	mustRead(t, conn) // connection
	symbols := []string{"NVDA", "AAPL", "META"}
	send(t, conn, subscribeMsg(symbols...))

	type state struct {
		snapshot bool
		lastSeq  int64
		quotes   int
	}
	states := map[string]*state{"NVDA": {}, "AAPL": {}, "META": {}}
	heartbeats := 0
	complete := func() bool {
		for _, s := range states {
			if s.quotes < 5 {
				return false
			}
		}
		return heartbeats > 0
	}
	for !complete() {
		m := mustRead(t, conn)
		switch m.Type {
		case "heartbeat":
			heartbeats++
		case "snapshot", "quote":
			s := states[m.Symbol]
			if s == nil {
				t.Fatalf("unexpected symbol %q", m.Symbol)
			}
			if m.Type == "quote" && !s.snapshot {
				t.Fatalf("%s: quote before snapshot", m.Symbol)
			}
			if m.Sequence <= s.lastSeq {
				t.Fatalf("%s: sequence %d not greater than %d", m.Symbol, m.Sequence, s.lastSeq)
			}
			s.lastSeq = m.Sequence
			if m.Type == "snapshot" {
				s.snapshot = true
			} else {
				s.quotes++
			}
			// Every quote is reproducible from its timestamp.
			at, err := time.Parse(time.RFC3339Nano, m.Timestamp)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := g.model.Price(m.Symbol, at)
			if m.Last == nil || *m.Last != want.Format(2) {
				t.Fatalf("%s @ %s: last %v, want %s", m.Symbol, m.Timestamp, m.Last, want.Format(2))
			}
		default:
			t.Fatalf("unexpected message %+v", m)
		}
	}
}

func TestUnsubscribeStopsQuotes(t *testing.T) {
	g := startGateway(t, nil)
	conn := g.mustDial()
	mustRead(t, conn)
	send(t, conn, subscribeMsg("NVDA", "AMD"))
	for got := 0; got < 4; {
		if mustRead(t, conn).Type == "quote" {
			got++
		}
	}
	send(t, conn, map[string]any{"type": "unsubscribe", "symbols": []string{"NVDA"}})
	// Quotes queued before the unsubscribe was processed may still arrive; after a grace period none may.
	graceEnd := time.Now().Add(10 * testTick)
	amdAfterGrace := 0
	for time.Now().Before(graceEnd.Add(20 * testTick)) {
		m := mustRead(t, conn)
		if m.Type != "quote" || time.Now().Before(graceEnd) {
			continue
		}
		if m.Symbol == "NVDA" {
			t.Fatalf("received an NVDA quote after unsubscribing: %+v", m)
		}
		amdAfterGrace++
	}
	if amdAfterGrace == 0 {
		t.Fatal("AMD quotes must continue after unsubscribing NVDA")
	}
}

func TestInvalidMessagesProduceErrorsAndKeepTheConnection(t *testing.T) {
	g := startGateway(t, nil)
	conn := g.mustDial()
	mustRead(t, conn)

	expectError := func(code string) serverMessage {
		t.Helper()
		for {
			m := mustRead(t, conn)
			if m.Type == "error" {
				if m.Code != code {
					t.Fatalf("error code %s, want %s", m.Code, code)
				}
				return m
			}
		}
	}
	sendRaw(t, conn, websocket.MessageText, []byte("not json"))
	expectError("INVALID_MESSAGE")
	send(t, conn, map[string]any{"type": "subscribe", "symbols": []string{}})
	expectError("INVALID_MESSAGE")
	sendRaw(t, conn, websocket.MessageBinary, []byte{0x01, 0x02})
	expectError("INVALID_MESSAGE")
	send(t, conn, subscribeMsg("NVDA", "NOPE"))
	unknown := expectError("UNKNOWN_SYMBOL")
	if len(unknown.Symbols) != 1 || unknown.Symbols[0] != "NOPE" {
		t.Fatalf("unknown symbols = %v, want [NOPE]", unknown.Symbols)
	}
	// The connection is still usable and the valid symbol was subscribed.
	for {
		if m := mustRead(t, conn); m.Type == "quote" && m.Symbol == "NVDA" {
			break
		}
	}
}

func TestOversizedMessageClosesWithMessageTooBig(t *testing.T) {
	g := startGateway(t, func(c *ws.Config) { c.MaxInboundMessageBytes = 256 })
	conn := g.mustDial()
	mustRead(t, conn)
	sendRaw(t, conn, websocket.MessageText, []byte(`{"type":"subscribe","symbols":["`+strings.Repeat("A", 400)+`"]}`))
	for {
		_, err := read(t, conn, 5*time.Second)
		if err != nil {
			if status := websocket.CloseStatus(err); status != websocket.StatusMessageTooBig {
				t.Fatalf("close status %d, want %d (err: %v)", status, websocket.StatusMessageTooBig, err)
			}
			return
		}
	}
}

func TestOriginAllowlist(t *testing.T) {
	g := startGateway(t, nil)
	if _, resp, err := g.dial("http://evil.example"); err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("disallowed origin: err=%v resp=%v, want 403", err, resp)
	}
	conn, _, err := g.dial("http://localhost:3000")
	if err != nil {
		t.Fatalf("allowed origin rejected: %v", err)
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
}

func TestSubscriptionLimitsAreEnforced(t *testing.T) {
	g := startGateway(t, func(c *ws.Config) { c.MaxSymbolsPerSubscribe = 2; c.MaxSubscribedSymbols = 3 })
	conn := g.mustDial()
	mustRead(t, conn)
	nextError := func() serverMessage {
		t.Helper()
		for {
			if m := mustRead(t, conn); m.Type == "error" {
				return m
			}
		}
	}
	send(t, conn, subscribeMsg("NVDA", "AAPL", "META"))
	if m := nextError(); m.Code != "SUBSCRIPTION_LIMIT" {
		t.Fatalf("per-message limit: got %+v", m)
	}
	send(t, conn, subscribeMsg("NVDA", "AAPL"))
	send(t, conn, subscribeMsg("META", "AMD"))
	if m := nextError(); m.Code != "SUBSCRIPTION_LIMIT" {
		t.Fatalf("per-connection limit: got %+v", m)
	}
}

func TestMaxConnectionsIsEnforced(t *testing.T) {
	g := startGateway(t, func(c *ws.Config) { c.MaxConnections = 1 })
	g.mustDial()
	if _, resp, err := g.dial(""); err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second connection: err=%v resp=%v, want 503", err, resp)
	}
}

func TestShutdownClosesClientsWithGoingAway(t *testing.T) {
	g := startGateway(t, nil)
	conn := g.mustDial()
	mustRead(t, conn)
	send(t, conn, subscribeMsg("NVDA"))
	mustRead(t, conn)

	shutdownErr := make(chan error, 1)
	go func() { // owned by the test; ends when Shutdown returns
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr <- g.ws.Shutdown(ctx)
	}()
	for {
		_, err := read(t, conn, 5*time.Second)
		if err != nil {
			if status := websocket.CloseStatus(err); status != websocket.StatusGoingAway {
				t.Fatalf("close status %d, want %d (err: %v)", status, websocket.StatusGoingAway, err)
			}
			break
		}
	}
	if err := <-shutdownErr; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, resp, err := g.dial(""); err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("new connection during shutdown: err=%v resp=%v, want 503", err, resp)
	}
}

func TestConnectionChurnLeavesNoGoroutines(t *testing.T) {
	g := startGateway(t, nil) // cleanup asserts that every goroutine is gone
	for i := 0; i < 40; i++ {
		conn, _, err := g.dial("")
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		mustRead(t, conn)
		send(t, conn, subscribeMsg("NVDA", "TSLA", "SPY"))
		for got := 0; got < 3; {
			if m := mustRead(t, conn); m.Type == "snapshot" {
				got++
			}
		}
		if i%2 == 0 {
			_ = conn.Close(websocket.StatusNormalClosure, "churn")
		} else {
			_ = conn.CloseNow() // abrupt disconnect
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for g.ws.Clients() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions still active after all clients disconnected", g.ws.Clients())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReadAfterServerCloseIsAnError(t *testing.T) {
	// Guards the helpers: a closed connection must surface as an error, never as a silent empty message.
	g := startGateway(t, nil)
	conn := g.mustDial()
	mustRead(t, conn)
	_ = conn.Close(websocket.StatusNormalClosure, "")
	if _, err := read(t, conn, time.Second); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected a close error, got %v", err)
	}
}
