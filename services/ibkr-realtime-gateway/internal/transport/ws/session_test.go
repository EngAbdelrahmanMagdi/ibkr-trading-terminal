package ws

import (
	"context"
	"testing"

	"github.com/coder/websocket"
)

// The send-queue policy is verified without a network: a full queue ends the session with 1013.
// (Filling a real socket's kernel buffers would make an integration test slow and flaky.)
func TestFullSendQueueEndsSessionWithTryAgainLater(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	srv := &Server{cfg: Config{SendQueueSize: 2}, ctx: srvCtx}
	ctx, cancel := context.WithCancel(srvCtx)
	s := &session{srv: srv, ctx: ctx, cancel: cancel, out: make(chan outbound, 2)}

	if !s.enqueue(outbound{data: []byte("1")}) || !s.enqueue(outbound{data: []byte("2")}) {
		t.Fatal("enqueue into a queue with free capacity must succeed")
	}
	if s.enqueue(outbound{data: []byte("3")}) {
		t.Fatal("enqueue into a full queue must fail")
	}
	if ctx.Err() == nil {
		t.Fatal("a full queue must end the session")
	}
	if code, _ := s.closeStatus(); code != websocket.StatusTryAgainLater {
		t.Fatalf("close code %d, want %d", code, websocket.StatusTryAgainLater)
	}
	if s.enqueue(outbound{data: []byte("4")}) {
		t.Fatal("enqueue after the session ended must fail")
	}
}

func TestCloseStatusDefaultsToGoingAwayDuringServerShutdown(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	srv := &Server{ctx: srvCtx}
	ctx, cancel := context.WithCancel(srvCtx)
	defer cancel()
	s := &session{srv: srv, ctx: ctx, cancel: cancel}
	srvCancel()
	if code, _ := s.closeStatus(); code != websocket.StatusGoingAway {
		t.Fatalf("close code %d, want %d", code, websocket.StatusGoingAway)
	}
}
