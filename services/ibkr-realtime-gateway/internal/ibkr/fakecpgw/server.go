package fakecpgw

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Auth is the fake brokerage session status.
type Auth struct {
	Connected     bool
	Authenticated bool
	Established   bool
	Competing     bool
}

// Server is the fake gateway. All methods are safe for concurrent use.
type Server struct {
	mu           sync.Mutex
	auth         Auth
	token        string
	accounts     bool
	stocks       map[string]json.RawMessage
	secdef       map[int64]json.RawMessage
	history      json.RawMessage
	fields       map[int64]map[string]string // current field values per conid
	failures     map[string][]int            // path -> queued status codes
	requests     map[string]int
	inits        []bool // compete flags received by ssodh/init
	topics       []string
	conns        map[*conn]struct{}
	autoStop     chan struct{}
	autoInterval time.Duration
	liveOrders   json.RawMessage // GET /iserver/account/orders "orders"
}

type conn struct {
	c    *websocket.Conn
	mu   sync.Mutex
	subs map[int64]bool
}

// Known conids of the default data set.
const (
	ConidAAPL  = 265598
	ConidNVDA  = 4815747
	ConidMSFT  = 272093
	ConidUNSUB = 999001 // real listing without a market-data subscription (availability N)
)

// New returns a fake gateway with an established session and a small data set: AAPL, NVDA, MSFT, META,
// AMD, IONQ, TSLA and SPY resolve to one US listing each (identifiers are illustrative); AMBIG has two US
// listings; UNSUB has no market-data subscription.
func New() *Server {
	s := &Server{
		auth:     Auth{Connected: true, Authenticated: true, Established: true},
		token:    randomToken(),
		accounts: true,
		stocks:   map[string]json.RawMessage{},
		secdef:   map[int64]json.RawMessage{},
		fields:   map[int64]map[string]string{},
		failures: map[string][]int{},
		requests: map[string]int{},
		conns:    map[*conn]struct{}{},
	}
	s.addStock("AAPL", "APPLE INC", ConidAAPL, "NASDAQ", "189.60")
	s.addStock("NVDA", "NVIDIA CORP", ConidNVDA, "NASDAQ", "182.13")
	s.addStock("MSFT", "MICROSOFT CORP", ConidMSFT, "NASDAQ", "412.1250")
	s.addStock("META", "META PLATFORMS INC", 107113386, "NASDAQ", "702.40")
	s.addStock("AMD", "ADVANCED MICRO DEVICES", 4391, "NASDAQ", "160.05")
	s.addStock("IONQ", "IONQ INC", 520040000, "NYSE", "40.15")
	s.addStock("TSLA", "TESLA INC", 76792991, "NASDAQ", "330.60")
	s.addStock("SPY", "SPDR S&P 500 ETF TRUST", 756733, "ARCA", "650.12")
	s.addStock("UNSUB", "UNSUBSCRIBED CORP", ConidUNSUB, "NYSE", "10.00")
	s.fields[ConidUNSUB]["6509"] = "NpB"
	s.stocks["AMBIG"] = json.RawMessage(`[{"name":"AMBIG ONE","assetClass":"STK","contracts":[{"conid":999101,"exchange":"NYSE","isUS":true}]},` +
		`{"name":"AMBIG TWO","assetClass":"STK","contracts":[{"conid":999102,"exchange":"NASDAQ","isUS":true}]}]`)
	for _, id := range []int64{999101, 999102} {
		s.secdef[id] = json.RawMessage(fmt.Sprintf(`{"conid":%d,"currency":"USD","name":"AMBIG","assetClass":"STK","listingExchange":"NYSE","ticker":"AMBIG","isUS":true}`, id))
	}
	s.history = json.RawMessage(`{"symbol":"X","volumeFactor":1,"data":[` +
		`{"o":173.40,"h":175.10,"l":171.70,"c":174.70,"v":472117,"t":1790496000000},` +
		`{"o":174.7,"h":1.7525e2,"l":174.05,"c":175.0,"v":1200.9,"t":1790499600000}]}`)
	return s
}

func (s *Server) addStock(symbol, name string, conid int64, exchange, last string) {
	s.stocks[symbol] = json.RawMessage(fmt.Sprintf(`[{"name":%q,"assetClass":"STK","contracts":[{"conid":%d,"exchange":%q,"isUS":true},{"conid":%d,"exchange":"MEXI","isUS":false}]}]`,
		name, conid, exchange, conid+1_000_000))
	s.secdef[conid] = json.RawMessage(fmt.Sprintf(`{"conid":%d,"currency":"USD","name":%q,"assetClass":"STK","listingExchange":%q,"ticker":%q,"isUS":true}`,
		conid, name, exchange, symbol))
	s.secdef[conid+1_000_000] = json.RawMessage(fmt.Sprintf(`{"conid":%d,"currency":"MXN","name":%q,"assetClass":"STK","listingExchange":"MEXI","ticker":%q,"isUS":false}`,
		conid+1_000_000, name, symbol))
	s.fields[conid] = map[string]string{"31": last, "84": offset(last, -1), "86": offset(last, 1), "88": "1,200", "85": "800", "7762": "1234567", "6509": "RpB"}
}

// offset moves a decimal price by delta units of its last digit, keeping its scale (fake data only).
func offset(price string, delta int64) string {
	whole, frac, _ := strings.Cut(price, ".")
	n, _ := strconv.ParseInt(whole+frac, 10, 64)
	digits := strconv.FormatInt(n+delta, 10)
	if frac == "" {
		return digits
	}
	for len(digits) <= len(frac) {
		digits = "0" + digits
	}
	return digits[:len(digits)-len(frac)] + "." + digits[len(digits)-len(frac):]
}

func randomToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ------------------------------------------------------------------------------------------ controls

// SetAuth replaces the session status. A session that is no longer authenticated is announced on open
// websockets with an sts message.
func (s *Server) SetAuth(a Auth) {
	s.mu.Lock()
	s.auth = a
	conns := s.connList()
	s.mu.Unlock()
	if !a.Authenticated {
		for _, c := range conns {
			c.write(`{"topic":"sts","args":{"authenticated":false}}`)
		}
	}
}

// FailNext makes the next n requests to path (for example "/tickle") answer with status.
func (s *Server) FailNext(path string, status, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for range n {
		s.failures[path] = append(s.failures[path], status)
	}
}

// DropWebSockets closes every open websocket abruptly.
func (s *Server) DropWebSockets() {
	s.mu.Lock()
	conns := s.connList()
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.c.CloseNow()
	}
}

// Push updates fields of an instrument and sends one frame with them to its subscribers.
func (s *Server) Push(conid int64, fields map[string]string) {
	s.mu.Lock()
	for k, v := range fields {
		if s.fields[conid] == nil {
			s.fields[conid] = map[string]string{}
		}
		s.fields[conid][k] = v
	}
	conns := s.connList()
	s.mu.Unlock()
	frame := frameJSON(conid, fields)
	for _, c := range conns {
		if c.subscribed(conid) {
			c.write(frame)
		}
	}
}

// SetLiveOrders sets the "orders" array returned by GET /iserver/account/orders.
func (s *Server) SetLiveOrders(orders string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.liveOrders = json.RawMessage(orders)
}

// PushRaw sends one raw websocket message (for example an order-stream "sor" or "str" message) to every client.
func (s *Server) PushRaw(msg string) {
	s.mu.Lock()
	conns := s.connList()
	s.mu.Unlock()
	for _, c := range conns {
		c.write(msg)
	}
}

// Requests returns how many requests path has received.
func (s *Server) Requests(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[path]
}

// Inits returns the compete flag of every ssodh/init request.
func (s *Server) Inits() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.inits...)
}

// Topics returns every websocket message received, in order.
func (s *Server) Topics() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.topics...)
}

// CountTopics returns how many received messages start with prefix.
func (s *Server) CountTopics(prefix string) int {
	n := 0
	for _, t := range s.Topics() {
		if strings.HasPrefix(t, prefix) {
			n++
		}
	}
	return n
}

// StartAuto pushes a changing, deterministic quote for every subscribed instrument at the interval, so the
// fake end-to-end path streams continuously. Stop ends it.
func (s *Server) StartAuto(interval time.Duration) {
	s.mu.Lock()
	if s.autoStop != nil {
		s.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	s.autoStop, s.autoInterval = stop, interval
	s.mu.Unlock()
	go func() { // owned by the fake; ends on Stop
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		n := 0
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n++
				for _, conid := range s.subscribedConids() {
					cents := 10_000 + int64(n%200) + conid%1000
					s.Push(conid, map[string]string{
						"31": fmt.Sprintf("%d.%02d", cents/100, cents%100), "84": fmt.Sprintf("%d.%02d", (cents-1)/100, (cents-1)%100),
						"86": fmt.Sprintf("%d.%02d", (cents+1)/100, (cents+1)%100), "7762": strconv.Itoa(1_000_000 + n*100),
					})
				}
			}
		}
	}()
}

// Stop ends StartAuto.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.autoStop != nil {
		close(s.autoStop)
		s.autoStop = nil
	}
}

func (s *Server) subscribedConids() []int64 {
	s.mu.Lock()
	conns := s.connList()
	s.mu.Unlock()
	seen := map[int64]bool{}
	var out []int64
	for _, c := range conns {
		c.mu.Lock()
		for id := range c.subs {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
		c.mu.Unlock()
	}
	return out
}

func (s *Server) connList() []*conn {
	out := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		out = append(out, c)
	}
	return out
}

// ------------------------------------------------------------------------------------------ HTTP

// Handler serves the fake API under /v1/api.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/api/iserver/auth/status", s.handleStatus)
	mux.HandleFunc("POST /v1/api/iserver/auth/ssodh/init", s.handleInit)
	mux.HandleFunc("GET /v1/api/iserver/accounts", s.handleAccounts)
	mux.HandleFunc("POST /v1/api/tickle", s.handleTickle)
	mux.HandleFunc("GET /v1/api/trsrv/stocks", s.handleStocks)
	mux.HandleFunc("GET /v1/api/trsrv/secdef", s.handleSecdef)
	mux.HandleFunc("GET /v1/api/iserver/marketdata/history", s.handleHistory)
	mux.HandleFunc("GET /v1/api/iserver/account/orders", s.handleLiveOrders)
	mux.HandleFunc("GET /v1/api/ws", s.handleWS)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1/api")
		s.mu.Lock()
		s.requests[path]++
		var status int
		if q := s.failures[path]; len(q) > 0 {
			status, s.failures[path] = q[0], q[1:]
		}
		s.mu.Unlock()
		if status != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"injected failure"}`))
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) statusBody() map[string]any {
	a := s.auth
	return map[string]any{"authenticated": a.Authenticated, "established": a.Established, "competing": a.Competing,
		"connected": a.Connected, "message": "", "fail": ""}
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, s.statusBody())
}

func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Publish bool `json:"publish"`
		Compete bool `json:"compete"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inits = append(s.inits, body.Compete)
	if s.auth.Connected && !s.auth.Competing {
		s.auth.Authenticated, s.auth.Established = true, true
	}
	writeJSON(w, s.statusBody())
}

func (s *Server) handleAccounts(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.auth.Authenticated {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	accounts := []string{}
	if s.accounts {
		accounts = []string{"DU0000000"}
	}
	writeJSON(w, map[string]any{"accounts": accounts})
}

func (s *Server) handleTickle(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, map[string]any{"session": s.token, "ssoExpires": 460000, "iserver": map[string]any{"authStatus": s.statusBody()}})
}

func (s *Server) handleStocks(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]json.RawMessage{}
	for _, sym := range strings.Split(r.URL.Query().Get("symbols"), ",") {
		if raw, ok := s.stocks[sym]; ok {
			out[sym] = raw
		}
	}
	writeJSON(w, out)
}

func (s *Server) handleSecdef(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []json.RawMessage{}
	for _, id := range strings.Split(r.URL.Query().Get("conids"), ",") {
		conid, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if raw, ok := s.secdef[conid]; ok {
			list = append(list, raw)
		}
	}
	writeJSON(w, map[string]any{"secdef": list})
}

func (s *Server) handleLiveOrders(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	orders := s.liveOrders
	s.mu.Unlock()
	if orders == nil {
		orders = json.RawMessage("[]")
	}
	writeJSON(w, map[string]any{"orders": orders, "snapshot": true})
}

func (s *Server) handleHistory(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(s.history)
}

// ------------------------------------------------------------------------------------------ websocket

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	token := s.token
	s.mu.Unlock()
	cookie, err := r.Cookie("api")
	if err != nil || cookie.Value != token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	cn := &conn{c: c, subs: map[int64]bool{}}
	s.mu.Lock()
	s.conns[cn] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, cn)
		s.mu.Unlock()
		_ = c.CloseNow()
	}()
	cn.write(`{"topic":"sts","args":{"authenticated":true}}`)
	for {
		_, data, err := c.Read(context.Background())
		if err != nil {
			return
		}
		msg := string(data)
		s.mu.Lock()
		s.topics = append(s.topics, msg)
		s.mu.Unlock()
		switch {
		case strings.HasPrefix(msg, "smd+"):
			conid, ok := topicConid(msg)
			if !ok {
				continue
			}
			cn.mu.Lock()
			cn.subs[conid] = true
			cn.mu.Unlock()
			s.mu.Lock()
			fields := map[string]string{}
			for k, v := range s.fields[conid] {
				fields[k] = v
			}
			s.mu.Unlock()
			if len(fields) > 0 {
				cn.write(frameJSON(conid, fields))
			}
		case strings.HasPrefix(msg, "umd+"):
			if conid, ok := topicConid(msg); ok {
				cn.mu.Lock()
				delete(cn.subs, conid)
				cn.mu.Unlock()
			}
		}
	}
}

func topicConid(msg string) (int64, bool) {
	parts := strings.SplitN(msg, "+", 3)
	if len(parts) < 2 {
		return 0, false
	}
	conid, err := strconv.ParseInt(parts[1], 10, 64)
	return conid, err == nil
}

func (c *conn) subscribed(conid int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subs[conid]
}

func (c *conn) write(msg string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.c.Write(ctx, websocket.MessageText, []byte(msg))
}

func frameJSON(conid int64, fields map[string]string) string {
	m := map[string]any{"conid": conid, "conidEx": strconv.FormatInt(conid, 10), "_updated": time.Now().UnixMilli(),
		"topic": "smd+" + strconv.FormatInt(conid, 10), "server_id": "q0"}
	for k, v := range fields {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	return string(b)
}
