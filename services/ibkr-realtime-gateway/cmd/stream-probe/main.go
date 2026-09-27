// Command stream-probe is a basic WebSocket client for the market stream. It subscribes to symbols, checks
// the protocol (connection first, snapshot before quotes, increasing sequences, well-formed prices) and exits
// with status 0 once every symbol has received the requested number of quotes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/coder/websocket"
)

var decimalPattern = regexp.MustCompile(`^[0-9]{1,13}(\.[0-9]{1,6})?$`)

type message struct {
	Type     string          `json:"type"`
	Symbol   string          `json:"symbol"`
	State    string          `json:"state"`
	Source   string          `json:"source"`
	Code     string          `json:"code"`
	Message  string          `json:"message"`
	Bid      *string         `json:"bid"`
	Ask      *string         `json:"ask"`
	Last     *string         `json:"last"`
	Sequence int64           `json:"sequence"`
	Limits   json.RawMessage `json:"limits"`
}

type symbolState struct {
	snapshot bool
	lastSeq  int64
	quotes   int
}

func main() {
	url := flag.String("url", "ws://127.0.0.1:8090/ws", "market stream URL")
	symbols := flag.String("symbols", "NVDA,AAPL,META", "comma-separated symbols")
	quotes := flag.Int("quotes", 3, "quotes to receive per symbol")
	timeout := flag.Duration("timeout", 30*time.Second, "overall timeout")
	origin := flag.String("origin", "", "optional Origin header")
	flag.Parse()

	if err := probe(*url, strings.Split(*symbols, ","), *quotes, *timeout, *origin); err != nil {
		fmt.Fprintf(os.Stderr, "stream-probe: FAILED: %v\n", err)
		os.Exit(1)
	}
}

func probe(url string, symbols []string, want int, timeout time.Duration, origin string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	opts := &websocket.DialOptions{}
	if origin != "" {
		opts.HTTPHeader = http.Header{"Origin": []string{origin}}
	}
	conn, resp, err := websocket.Dial(ctx, url, opts)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.CloseNow() }() // no-op after a clean close
	conn.SetReadLimit(1 << 20)

	first, err := read(ctx, conn)
	if err != nil {
		return err
	}
	if first.Type != "connection" || first.State == "" || len(first.Limits) == 0 {
		return fmt.Errorf("first message must be a connection message with limits, got %q", first.Type)
	}
	fmt.Printf("connected: state=%s source=%s limits=%s\n", first.State, first.Source, first.Limits)

	sub, _ := json.Marshal(map[string]any{"type": "subscribe", "symbols": symbols})
	if err := conn.Write(ctx, websocket.MessageText, sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	state := map[string]*symbolState{}
	for _, s := range symbols {
		state[s] = &symbolState{}
	}
	heartbeats := 0
	for !done(state, want) {
		m, err := read(ctx, conn)
		if err != nil {
			return err
		}
		switch m.Type {
		case "snapshot", "quote":
			st, ok := state[m.Symbol]
			if !ok {
				return fmt.Errorf("unexpected symbol %q", m.Symbol)
			}
			if err := checkPrices(m); err != nil {
				return err
			}
			if m.Sequence <= st.lastSeq {
				return fmt.Errorf("%s: sequence %d is not greater than %d", m.Symbol, m.Sequence, st.lastSeq)
			}
			st.lastSeq = m.Sequence
			if m.Type == "snapshot" {
				st.snapshot = true
			} else {
				if !st.snapshot {
					return fmt.Errorf("%s: quote received before snapshot", m.Symbol)
				}
				st.quotes++
			}
		case "heartbeat":
			heartbeats++
		case "error":
			return fmt.Errorf("server error %s: %s", m.Code, m.Message)
		}
	}
	for _, s := range symbols {
		fmt.Printf("%s: snapshot=ok quotes=%d lastSequence=%d\n", s, state[s].quotes, state[s].lastSeq)
	}
	fmt.Printf("stream-probe: OK (%d symbols, %d quotes each, %d heartbeats)\n", len(symbols), want, heartbeats)
	return conn.Close(websocket.StatusNormalClosure, "probe complete")
}

func read(ctx context.Context, conn *websocket.Conn) (message, error) {
	_, data, err := conn.Read(ctx)
	if err != nil {
		if status := websocket.CloseStatus(err); status != -1 {
			return message{}, fmt.Errorf("connection closed with status %d", status)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return message{}, errors.New("timed out waiting for messages")
		}
		return message{}, fmt.Errorf("read: %w", err)
	}
	var m message
	if err := json.Unmarshal(data, &m); err != nil {
		return message{}, fmt.Errorf("invalid JSON from server: %w", err)
	}
	return m, nil
}

func checkPrices(m message) error {
	values := map[string]*string{"bid": m.Bid, "ask": m.Ask, "last": m.Last}
	parsed := map[string]*big.Rat{}
	for name, v := range values {
		if v == nil {
			continue
		}
		if !decimalPattern.MatchString(*v) {
			return fmt.Errorf("%s: %s %q is not a decimal string", m.Symbol, name, *v)
		}
		parsed[name], _ = new(big.Rat).SetString(*v)
	}
	// A locked market (bid == ask) is legitimate in real data; a crossed one is reported.
	if parsed["bid"] != nil && parsed["ask"] != nil && parsed["bid"].Cmp(parsed["ask"]) > 0 {
		return fmt.Errorf("%s: bid %s is above ask %s", m.Symbol, *m.Bid, *m.Ask)
	}
	return nil
}

func done(state map[string]*symbolState, want int) bool {
	for _, st := range state {
		if st.quotes < want {
			return false
		}
	}
	return true
}
