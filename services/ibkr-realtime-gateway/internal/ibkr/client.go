// Package ibkr is the IBKR Client Portal Gateway market-data adapter (IBKRMarketDataSource). It is the only
// package that knows IBKR endpoints, field tags and payload shapes; everything it returns is broker-neutral.
//
// IBKR behavior implemented here was taken from the official Web API documentation, checked on 2026-09-27:
// https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-doc/ (and the endpoint reference under
// https://www.interactivebrokers.com/docs/web-api/), plus the Web API changelog.
package ibkr

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/ibkr/pacing"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
)

// Endpoint classes used for pacing and metrics.
const (
	epAuthStatus = "auth_status"
	epInit       = "init"
	epAccounts   = "accounts"
	epTickle     = "tickle"
	epStocks     = "contracts"
	epSecdef     = "contracts"
	epHistory    = "history"
	epLiveOrders = "live_orders"
)

// Endpoints returns the documented per-endpoint limits (docs, 2026-09-27; history per changelog 2026-09-09).
func Endpoints() map[string]pacing.Endpoint {
	return map[string]pacing.Endpoint{
		epTickle:     {PerSecond: 1},
		epHistory:    {PerSecond: 10, PerMinute: 50},
		epLiveOrders: {PerSecond: 0.2}, // one request per 5 s
	}
}

// Class is the recovery class of an IBKR error.
type Class int

// Error classes.
const (
	ClassTransient   Class = iota // network, timeouts, 5xx: bounded backoff
	ClassRateLimited              // 429 or local pacing rejection: cool-down
	ClassAuth                     // 401 or no brokerage session: login required
	ClassBadRequest               // 400: not retried
	ClassTLS                      // certificate not trusted: no retries, operator action
	ClassMalformed                // unexpected payload
)

// Error is a classified IBKR failure. Its message never contains response bodies or credentials.
type Error struct {
	Class    Class
	Endpoint string
	Status   int
	Err      error
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("ibkr %s: HTTP %d", e.Endpoint, e.Status)
	}
	return fmt.Sprintf("ibkr %s: %v", e.Endpoint, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Is maps classes onto the broker-neutral errors the rest of the gateway understands.
func (e *Error) Is(target error) bool {
	switch target {
	case marketdata.ErrRateLimited:
		return e.Class == ClassRateLimited
	case marketdata.ErrSourceUnavailable:
		return e.Class != ClassRateLimited && e.Class != ClassBadRequest
	}
	return false
}

func classOf(err error) Class {
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	return ClassTransient
}

// ClientConfig configures the connection to the local CP Gateway.
type ClientConfig struct {
	BaseURL   string        // e.g. https://host.docker.internal:5000/v1/api
	CAPEM     []byte        // the only trusted CA (the per-machine local CA)
	Timeout   time.Duration // per request
	UserAgent string
}

// NewTLSConfig trusts exactly the given CA (no system roots) with hostname verification on. Certificate
// verification is never disabled.
func NewTLSConfig(caPEM []byte) (*tls.Config, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("ibkr: CA file contains no PEM certificate")
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}

// Client calls the CP Gateway REST API through the pacing limiter.
type Client struct {
	base      *url.URL
	http      *http.Client
	tls       *tls.Config
	userAgent string
	limiter   *pacing.Limiter
	metrics   *metrics.Gateway
	clock     clock.Clock
	log       *slog.Logger
	on429     func()
}

// maxBody bounds every response body.
const maxBody = 4 << 20

// NewClient creates the client.
func NewClient(cfg ClientConfig, limiter *pacing.Limiter, clk clock.Clock, m *metrics.Gateway, log *slog.Logger) (*Client, error) {
	base, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil || base.Scheme != "https" || base.Host == "" {
		return nil, errors.New("ibkr: base URL must be an https URL")
	}
	tlsCfg, err := NewTLSConfig(cfg.CAPEM)
	if err != nil {
		return nil, err
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		TLSClientConfig:       tlsCfg.Clone(),
		DialContext:           (&net.Dialer{Timeout: cfg.Timeout}).DialContext,
		TLSHandshakeTimeout:   cfg.Timeout,
		ResponseHeaderTimeout: cfg.Timeout,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     false,
	}
	return &Client{
		base: base, tls: tlsCfg, userAgent: cfg.UserAgent, limiter: limiter, metrics: m, clock: clk, log: log,
		http: &http.Client{
			Transport: transport, Timeout: cfg.Timeout, Jar: jar,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// OnRateLimited registers a callback for HTTP 429 responses.
func (c *Client) OnRateLimited(f func()) { c.on429 = f }

// request performs one paced call and decodes the JSON response into out (if not nil).
func (c *Client) request(ctx context.Context, method, path string, query url.Values, body any, endpoint string, essential bool, out any) error {
	if err := c.limiter.Acquire(ctx, endpoint, essential); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return &Error{Class: ClassTransient, Endpoint: endpoint, Err: err}
		}
		return &Error{Class: ClassRateLimited, Endpoint: endpoint, Err: err}
	}
	u := c.base.JoinPath(path)
	u.RawQuery = query.Encode()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return &Error{Class: ClassBadRequest, Endpoint: endpoint, Err: err}
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil {
		return &Error{Class: ClassBadRequest, Endpoint: endpoint, Err: err}
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	start := c.clock.Now()
	resp, err := c.http.Do(req)
	c.metrics.IBKRRequestSeconds.WithLabelValues(endpoint).Observe(c.clock.Now().Sub(start).Seconds())
	if err != nil {
		class := ClassTransient
		if isTLSError(err) {
			class = ClassTLS
		}
		c.metrics.IBKRRequests.WithLabelValues(endpoint, "error").Inc()
		return &Error{Class: class, Endpoint: endpoint, Err: sanitize(err)}
	}
	defer func() { _ = resp.Body.Close() }()
	c.metrics.IBKRRequests.WithLabelValues(endpoint, strconv.Itoa(resp.StatusCode)).Inc()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return &Error{Class: ClassTransient, Endpoint: endpoint, Err: sanitize(err)}
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		c.limiter.Report429()
		if c.on429 != nil {
			c.on429()
		}
		return &Error{Class: ClassRateLimited, Endpoint: endpoint, Status: resp.StatusCode}
	case resp.StatusCode == http.StatusUnauthorized:
		return &Error{Class: ClassAuth, Endpoint: endpoint, Status: resp.StatusCode}
	case resp.StatusCode == http.StatusBadRequest:
		return &Error{Class: ClassBadRequest, Endpoint: endpoint, Status: resp.StatusCode}
	case resp.StatusCode < 200 || resp.StatusCode > 299:
		return &Error{Class: ClassTransient, Endpoint: endpoint, Status: resp.StatusCode}
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber() // numbers stay exact text; never float64
	if err := dec.Decode(out); err != nil {
		c.metrics.IBKRMalformedFrames.Inc()
		return &Error{Class: ClassMalformed, Endpoint: endpoint, Err: errors.New("unexpected response shape")}
	}
	return nil
}

func isTLSError(err error) bool {
	var unknown x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var verify *tls.CertificateVerificationError
	var record tls.RecordHeaderError
	return errors.As(err, &unknown) || errors.As(err, &hostname) || errors.As(err, &invalid) ||
		errors.As(err, &verify) || errors.As(err, &record)
}

// sanitize drops URLs (which could carry query values) from transport errors.
func sanitize(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
}

// authStatus is the brokerage session status (POST /iserver/auth/status, and inside /tickle).
type authStatus struct {
	Authenticated bool   `json:"authenticated"`
	Established   bool   `json:"established"`
	Competing     bool   `json:"competing"`
	Connected     bool   `json:"connected"`
	Message       string `json:"message"`
	Fail          string `json:"fail"`
}

// ready reports whether the brokerage session can serve market data. established is authoritative when
// present; older gateways omit it, in which case authenticated is used.
func (a authStatus) ready(hasEstablished bool) bool {
	ok := a.Connected && a.Authenticated && !a.Competing
	if hasEstablished {
		ok = ok && a.Established
	}
	return ok
}

func (c *Client) authStatus(ctx context.Context) (authStatus, bool, error) {
	var raw map[string]json.RawMessage
	if err := c.request(ctx, http.MethodPost, "/iserver/auth/status", nil, map[string]any{}, epAuthStatus, true, &raw); err != nil {
		return authStatus{}, false, err
	}
	return decodeAuthStatus(raw)
}

func decodeAuthStatus(raw map[string]json.RawMessage) (authStatus, bool, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return authStatus{}, false, err
	}
	var st authStatus
	if err := json.Unmarshal(b, &st); err != nil {
		return authStatus{}, false, &Error{Class: ClassMalformed, Endpoint: epAuthStatus, Err: err}
	}
	_, hasEstablished := raw["established"]
	return st, hasEstablished, nil
}

// initBrokerage asks the gateway to (re)initialize the brokerage session. compete is always false: the
// adapter never disconnects a session the user holds elsewhere.
func (c *Client) initBrokerage(ctx context.Context) error {
	return c.request(ctx, http.MethodPost, "/iserver/auth/ssodh/init", nil,
		map[string]bool{"publish": true, "compete": false}, epInit, true, nil)
}

// accountsReady checks GET /iserver/accounts returns a non-empty account list (required before market
// data). Account identifiers are never logged or stored.
func (c *Client) accountsReady(ctx context.Context) (bool, error) {
	var resp struct {
		Accounts []json.RawMessage `json:"accounts"`
	}
	if err := c.request(ctx, http.MethodGet, "/iserver/accounts", nil, nil, epAccounts, true, &resp); err != nil {
		return false, err
	}
	return len(resp.Accounts) > 0, nil
}

// liveOrders returns the current day's orders (GET /iserver/account/orders), used to correlate the order stream.
func (c *Client) liveOrders(ctx context.Context) (json.RawMessage, error) {
	var resp struct {
		Orders json.RawMessage `json:"orders"`
	}
	if err := c.request(ctx, http.MethodGet, "/iserver/account/orders", nil, nil, epLiveOrders, false, &resp); err != nil {
		return nil, err
	}
	return resp.Orders, nil
}

// tickleResponse carries the session token for the websocket; the token is a secret.
type tickleResponse struct {
	Session string `json:"session"`
	Iserver struct {
		AuthStatus map[string]json.RawMessage `json:"authStatus"`
	} `json:"iserver"`
}

func (c *Client) tickle(ctx context.Context) (token string, st authStatus, hasEstablished bool, err error) {
	var resp tickleResponse
	if err := c.request(ctx, http.MethodPost, "/tickle", nil, map[string]any{}, epTickle, true, &resp); err != nil {
		return "", authStatus{}, false, err
	}
	st, hasEstablished, err = decodeAuthStatus(resp.Iserver.AuthStatus)
	return resp.Session, st, hasEstablished, err
}
