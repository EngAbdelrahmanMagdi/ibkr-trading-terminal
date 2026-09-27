package ibkr

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
)

// errQueueFull means the websocket's outbound queue is full; the topic is dropped and re-sent by renewal.
var errQueueFull = errors.New("ibkr websocket: outbound queue full")

// frameHandler receives the market-data frames of one connection.
type frameHandler func(conid int64, fields map[string]json.RawMessage)

// wsConn is one websocket connection to the CP Gateway (wss://…/v1/api/ws, cookie api=<session>).
// Goroutines: one reader and one writer, both ending when the connection closes.
type wsConn struct {
	c       *websocket.Conn
	clock   clock.Clock
	metrics *metrics.Gateway
	out     chan string // bounded outbound topic queue
	gap     time.Duration

	done      chan struct{} // closed when the reader ends
	authLost  chan struct{} // closed when an sts message reports the session unauthenticated
	authed    chan struct{} // closed on the first sts message reporting authenticated
	closeOnce sync.Once
	outOnce   sync.Once
	lostOnce  sync.Once
	authOnce  sync.Once
	writerEnd chan struct{}
}

// dialWS opens the websocket with the session token as the api cookie. The token is never logged.
func (c *Client) dialWS(ctx context.Context, token string, sendRate float64, onFrame frameHandler) (*wsConn, error) {
	u := *c.base
	u.Scheme = "wss"
	u = *u.JoinPath("/ws")
	transport := &http.Transport{TLSClientConfig: c.tls.Clone(), ForceAttemptHTTP2: false}
	conn, resp, err := websocket.Dial(ctx, u.String(), &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		HTTPHeader: http.Header{"Cookie": {"api=" + token}, "User-Agent": {c.userAgent}},
	})
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		class := ClassTransient
		switch {
		case isTLSError(err):
			class = ClassTLS
		case resp != nil && resp.StatusCode == http.StatusUnauthorized:
			class = ClassAuth
		}
		return nil, &Error{Class: class, Endpoint: "websocket", Err: sanitize(err)}
	}
	conn.SetReadLimit(1 << 20)
	w := &wsConn{
		c: conn, clock: c.clock, metrics: c.metrics,
		out:  make(chan string, 256),
		gap:  time.Duration(float64(time.Second) / sendRate),
		done: make(chan struct{}), authLost: make(chan struct{}), authed: make(chan struct{}),
		writerEnd: make(chan struct{}),
	}
	go w.readLoop(onFrame) // owned by the connection; ends when it closes
	go w.writeLoop()       // owned by the connection; ends when out is closed or the connection fails
	return w, nil
}

// send queues a topic message without blocking.
func (w *wsConn) send(msg string) error {
	select {
	case <-w.done:
		return errors.New("ibkr websocket: closed")
	default:
	}
	select {
	case w.out <- msg:
		return nil
	default:
		return errQueueFull
	}
}

// writeLoop writes queued topics, spaced by the configured send rate.
func (w *wsConn) writeLoop() {
	defer close(w.writerEnd)
	for msg := range w.out {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := w.c.Write(ctx, websocket.MessageText, []byte(msg))
		cancel()
		if err != nil {
			w.close()
			for range w.out { // drain until closed
			}
			return
		}
		w.metrics.IBKRWSMessages.WithLabelValues("out", topicClass(msg)).Inc()
		select {
		case <-w.done:
		case <-w.clock.After(w.gap):
		}
	}
}

func (w *wsConn) readLoop(onFrame frameHandler) {
	defer close(w.done)
	for {
		_, data, err := w.c.Read(context.Background())
		if err != nil {
			return
		}
		var msg struct {
			Topic string          `json:"topic"`
			Args  json.RawMessage `json:"args"`
		}
		if json.Unmarshal(data, &msg) != nil {
			w.metrics.IBKRMalformedFrames.Inc()
			continue
		}
		w.metrics.IBKRWSMessages.WithLabelValues("in", topicClass(msg.Topic)).Inc()
		switch {
		case msg.Topic == "sts":
			var args struct {
				Authenticated *bool `json:"authenticated"`
			}
			if json.Unmarshal(msg.Args, &args) != nil || args.Authenticated == nil {
				continue
			}
			if *args.Authenticated {
				w.authOnce.Do(func() { close(w.authed) })
			} else {
				w.lostOnce.Do(func() { close(w.authLost) })
			}
		case strings.HasPrefix(msg.Topic, "smd+"):
			conid, err := strconv.ParseInt(strings.SplitN(strings.TrimPrefix(msg.Topic, "smd+"), "@", 2)[0], 10, 64)
			if err != nil {
				w.metrics.IBKRMalformedFrames.Inc()
				continue
			}
			var fields map[string]json.RawMessage
			if json.Unmarshal(data, &fields) != nil {
				w.metrics.IBKRMalformedFrames.Inc()
				continue
			}
			onFrame(conid, fields)
		}
	}
}

// close ends the connection immediately.
func (w *wsConn) close() {
	w.closeOnce.Do(func() { _ = w.c.CloseNow() })
}

// shutdown flushes queued topics (bounded) and closes the connection. It must be called for every
// connection: it also releases the writer after a failed write.
func (w *wsConn) shutdown(timeout time.Duration) {
	w.outOnce.Do(func() { close(w.out) })
	select {
	case <-w.writerEnd:
	case <-w.clock.After(timeout):
	}
	w.closeOnce.Do(func() { _ = w.c.Close(websocket.StatusNormalClosure, "") })
	w.close()
	<-w.done
}

func topicClass(topic string) string {
	switch {
	case strings.HasPrefix(topic, "smd"):
		return "smd"
	case strings.HasPrefix(topic, "umd"):
		return "umd"
	case topic == "sts", topic == "tic", topic == "system":
		return topic
	default:
		return "other"
	}
}
