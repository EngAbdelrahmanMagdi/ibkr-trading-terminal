package ibkr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/hotcache"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
)

// contract is a resolved US stock listing.
type contract struct {
	Conid    int64  `json:"conid"`
	Symbol   string `json:"symbol"`
	Name     string `json:"name"`
	Currency string `json:"currency"`
	Exchange string `json:"exchange"`
}

// errAmbiguous is returned when several US listings remain after verification; the adapter never picks one.
var errAmbiguous = errors.New("ambiguous contract")

// resolverConfig bounds the resolver.
type resolverConfig struct {
	CacheTTL    time.Duration    // Redis entry lifetime
	NegativeTTL time.Duration    // how long an unknown symbol is remembered
	MaxEntries  int              // memory cache bound
	Seed        map[string]int64 // optional operator-provided symbol -> conid
}

// resolver maps symbols to contracts: memory cache, then Redis, then GET /trsrv/stocks confirmed with
// GET /trsrv/secdef. It is the gateway's read-only contract lookup for market data;
// Trading Core owns the persisted instruments table.
type resolver struct {
	client  *Client
	store   hotcache.Store // nil: no Redis layer
	guard   *hotcache.Guard
	cfg     resolverConfig
	clock   clock.Clock
	metrics *metrics.Gateway
	log     *slog.Logger
	group   singleflight.Group

	mu       sync.Mutex
	mem      map[string]contract
	order    []string // insertion order for eviction
	negative map[string]time.Time
}

func newResolver(client *Client, store hotcache.Store, guard *hotcache.Guard, cfg resolverConfig, clk clock.Clock, m *metrics.Gateway, log *slog.Logger) *resolver {
	return &resolver{
		client: client, store: store, guard: guard, cfg: cfg, clock: clk, metrics: m, log: log,
		mem: map[string]contract{}, negative: map[string]time.Time{},
	}
}

func instrumentKey(symbol string) string { return "ibkr:instrument:" + symbol }

// resolve returns the symbol's contract. Unknown and ambiguous symbols wrap marketdata.ErrUnknownSymbol.
func (r *resolver) resolve(ctx context.Context, symbol string) (contract, error) {
	if c, ok := r.fromMemory(symbol); ok {
		r.metrics.IBKRContractLookups.WithLabelValues("memory").Inc()
		return c, nil
	}
	if r.isNegative(symbol) {
		return contract{}, fmt.Errorf("%w: %s", marketdata.ErrUnknownSymbol, symbol)
	}
	v, err, _ := r.group.Do(symbol, func() (any, error) { return r.lookup(ctx, symbol) })
	if err != nil {
		return contract{}, err
	}
	return v.(contract), nil
}

func (r *resolver) lookup(ctx context.Context, symbol string) (contract, error) {
	if c, ok := r.fromRedis(ctx, symbol); ok {
		r.metrics.IBKRContractLookups.WithLabelValues("redis").Inc()
		r.remember(c)
		return c, nil
	}
	var candidates []int64
	if conid, ok := r.cfg.Seed[symbol]; ok {
		candidates = []int64{conid}
	} else {
		var err error
		if candidates, err = r.usCandidates(ctx, symbol); err != nil {
			if errors.Is(err, marketdata.ErrUnknownSymbol) {
				r.metrics.IBKRContractLookups.WithLabelValues("unknown").Inc()
				r.markNegative(symbol)
			}
			return contract{}, err
		}
	}
	c, err := r.verify(ctx, symbol, candidates)
	switch {
	case errors.Is(err, errAmbiguous):
		r.metrics.IBKRContractLookups.WithLabelValues("ambiguous").Inc()
		r.log.Warn("contract lookup ambiguous; symbol rejected", "symbol", symbol, "candidates", len(candidates))
		r.markNegative(symbol)
		return contract{}, fmt.Errorf("%w: %s (ambiguous)", marketdata.ErrUnknownSymbol, symbol)
	case errors.Is(err, marketdata.ErrUnknownSymbol):
		r.metrics.IBKRContractLookups.WithLabelValues("unknown").Inc()
		r.markNegative(symbol)
		return contract{}, err
	case err != nil:
		r.metrics.IBKRContractLookups.WithLabelValues("error").Inc()
		return contract{}, err
	}
	r.metrics.IBKRContractLookups.WithLabelValues("resolved").Inc()
	r.remember(c)
	r.toRedis(ctx, c)
	return c, nil
}

// stocksEntry is one company in the GET /trsrv/stocks response.
type stocksEntry struct {
	Name       string `json:"name"`
	AssetClass string `json:"assetClass"`
	Contracts  []struct {
		Conid    json.Number `json:"conid"`
		Exchange string      `json:"exchange"`
		IsUS     bool        `json:"isUS"`
	} `json:"contracts"`
}

// usCandidates lists every US stock listing of the symbol.
func (r *resolver) usCandidates(ctx context.Context, symbol string) ([]int64, error) {
	var resp map[string][]stocksEntry
	if err := r.client.request(ctx, http.MethodGet, "/trsrv/stocks", url.Values{"symbols": {symbol}}, nil, epStocks, false, &resp); err != nil {
		return nil, err
	}
	seen := map[int64]bool{}
	var out []int64
	for _, e := range resp[symbol] {
		if e.AssetClass != "STK" {
			continue
		}
		for _, c := range e.Contracts {
			conid, err := c.Conid.Int64()
			if err != nil || !c.IsUS || seen[conid] {
				continue
			}
			seen[conid] = true
			out = append(out, conid)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: %s", marketdata.ErrUnknownSymbol, symbol)
	}
	return out, nil
}

// secdefResponse is GET /trsrv/secdef (path verified against the official reference, 2026-09-28).
type secdefResponse struct {
	Secdef []struct {
		Conid           json.Number `json:"conid"`
		Currency        string      `json:"currency"`
		Name            string      `json:"name"`
		AssetClass      string      `json:"assetClass"`
		ListingExchange string      `json:"listingExchange"`
		Ticker          string      `json:"ticker"`
		IsUS            bool        `json:"isUS"`
	} `json:"secdef"`
}

// verify confirms currency, asset class and listing for the candidates; exactly one must remain.
func (r *resolver) verify(ctx context.Context, symbol string, candidates []int64) (contract, error) {
	ids := make([]string, 0, len(candidates))
	for _, c := range candidates {
		ids = append(ids, strconv.FormatInt(c, 10))
	}
	var resp secdefResponse
	if err := r.client.request(ctx, http.MethodGet, "/trsrv/secdef", url.Values{"conids": {strings.Join(ids, ",")}}, nil, epSecdef, false, &resp); err != nil {
		return contract{}, err
	}
	var matches []contract
	for _, s := range resp.Secdef {
		conid, err := s.Conid.Int64()
		if err != nil || s.AssetClass != "STK" || s.Currency != "USD" || !s.IsUS {
			continue
		}
		if s.Ticker != "" && !strings.EqualFold(s.Ticker, symbol) {
			continue
		}
		matches = append(matches, contract{Conid: conid, Symbol: symbol, Name: s.Name, Currency: s.Currency, Exchange: s.ListingExchange})
	}
	switch len(matches) {
	case 0:
		return contract{}, fmt.Errorf("%w: %s", marketdata.ErrUnknownSymbol, symbol)
	case 1:
		return matches[0], nil
	default:
		return contract{}, errAmbiguous
	}
}

func (r *resolver) fromMemory(symbol string) (contract, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.mem[symbol]
	return c, ok
}

func (r *resolver) remember(c contract) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.mem[c.Symbol]; !ok {
		r.order = append(r.order, c.Symbol)
		for len(r.order) > r.cfg.MaxEntries {
			delete(r.mem, r.order[0])
			r.order = r.order[1:]
		}
	}
	r.mem[c.Symbol] = c
}

func (r *resolver) isNegative(symbol string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	until, ok := r.negative[symbol]
	if ok && r.clock.Now().After(until) {
		delete(r.negative, symbol)
		return false
	}
	return ok
}

func (r *resolver) markNegative(symbol string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.negative) >= r.cfg.MaxEntries { // bounded: drop everything rather than grow
		clear(r.negative)
	}
	r.negative[symbol] = r.clock.Now().Add(r.cfg.NegativeTTL)
}

func (r *resolver) fromRedis(ctx context.Context, symbol string) (contract, bool) {
	if r.store == nil || r.guard == nil || !r.guard.Available() {
		return contract{}, false
	}
	rctx, cancel := r.guard.Context(ctx)
	defer cancel()
	v, err := r.store.Get(rctx, instrumentKey(symbol))
	if err != nil {
		if !errors.Is(err, hotcache.ErrMiss) {
			r.guard.Fail("instrument_get", err)
		}
		return contract{}, false
	}
	var c contract
	if json.Unmarshal(v, &c) != nil || c.Conid <= 0 || c.Symbol != symbol {
		return contract{}, false
	}
	return c, true
}

func (r *resolver) toRedis(ctx context.Context, c contract) {
	if r.store == nil || r.guard == nil || !r.guard.Available() {
		return
	}
	data, err := json.Marshal(c)
	if err != nil {
		return
	}
	rctx, cancel := r.guard.Context(ctx)
	defer cancel()
	if err := r.store.SetMany(rctx, []hotcache.Entry{{Key: instrumentKey(c.Symbol), Value: data, TTL: r.cfg.CacheTTL}}); err != nil {
		r.guard.Fail("instrument_set", err)
	}
}
