package ws

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/registry"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/stream"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/testsource"
)

// benchServer builds a server and registry over the controllable source, without running writers.
func benchServer(b *testing.B) (*Server, *testsource.Source) {
	b.Helper()
	clk := clock.Real{}
	src := testsource.New(clk, "NVDA")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	reg, err := registry.New(src, clk, registry.Config{MaxActiveSymbols: 10, StaleAfter: time.Hour, SweepInterval: time.Hour}, m, log, registry.Hooks{})
	if err != nil {
		b.Fatal(err)
	}
	reg.Start()
	src.Emit(marketdata.StateConnecting, "")
	src.Emit(marketdata.StateReady, "")
	for reg.State().State != marketdata.StateReady {
		time.Sleep(time.Millisecond)
	}
	cfg := testConfig()
	srv := NewServer(cfg, reg, clk, m, log)
	b.Cleanup(reg.Close)
	return srv, src
}

// BenchmarkFanOut measures one upstream quote reaching N subscribed sessions: normalization, encoding once,
// and a non-blocking offer into each session's latest-value slot.
func BenchmarkFanOut(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			srv, src := benchServer(b)
			for range n {
				sess := newSession(srv, newFakeConn())
				if err := srv.registry.Subscribe("NVDA", sess, sess.prepare); err != nil {
					b.Fatal(err)
				}
				sess.mu.Lock()
				sess.slots["NVDA"].armed = true
				sess.mu.Unlock()
			}
			b.ReportAllocs()
			for b.Loop() {
				src.Publish("NVDA")
			}
		})
	}
}

// BenchmarkOfferCoalescing measures replacing a pending quote in a slot (the slow-consumer path).
func BenchmarkOfferCoalescing(b *testing.B) {
	srv, _ := benchServer(b)
	sess := newSession(srv, newFakeConn())
	sess.slots["NVDA"] = &slot{symbol: "NVDA", armed: true}
	u := &registry.Update{Symbol: "NVDA", Data: []byte(`{}`), ReceivedAt: time.Now()}
	b.ReportAllocs()
	var seq int64
	for b.Loop() {
		seq++
		u.Sequence = seq
		sess.OfferQuote(u)
	}
}

// BenchmarkEncodeQuote measures encoding one quote message (done once per quote, shared by all clients).
func BenchmarkEncodeQuote(b *testing.B) {
	q := marketdata.Quote{Symbol: "NVDA", Bid: 182_120_000, Ask: 182_140_000, Last: 182_130_000,
		BidSize: 300, AskSize: 500, Volume: 101_168_049, Sequence: 1_790_000_000, Time: time.Now()}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := stream.Encode(stream.NewQuoteMessage(stream.TypeQuote, q, 2, false)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFrameWriteLoopback measures writing one quote-sized text frame over a real WebSocket connection
// on the loopback interface, with a client draining it. This is the per-frame cost the flush interval bounds.
func BenchmarkFrameWriteLoopback(b *testing.B) {
	accepted := make(chan *websocket.Conn, 1)
	done := make(chan struct{})
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		accepted <- c
		<-done
	}))
	defer hs.Close()
	ctx := context.Background()
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(hs.URL, "http"), nil)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		for {
			if _, _, err := client.Read(ctx); err != nil {
				return
			}
		}
	}()
	server := <-accepted
	frame := []byte(`{"type":"quote","symbol":"NVDA","bid":"182.12","ask":"182.14","last":"182.13","bidSize":300,"askSize":500,"volume":101168049,"sequence":1790000000,"timestamp":"2026-09-27T14:03:11.000Z","stale":false}`)
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for b.Loop() {
		wctx, cancel := context.WithTimeout(ctx, time.Second)
		if err := server.Write(wctx, websocket.MessageText, frame); err != nil {
			cancel()
			b.Fatal(err)
		}
		cancel()
	}
	b.StopTimer()
	close(done)
	_ = client.CloseNow()
	_ = server.CloseNow()
}
