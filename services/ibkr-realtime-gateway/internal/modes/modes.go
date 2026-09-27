// Package modes selects the market-data source for the configured mode (MOCK, IBKR or AUTO), once, at
// startup, for the whole process lifetime.
package modes

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/config"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/hotcache"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/ibkr"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/ibkr/pacing"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/reconnect"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/simulator"
)

// Selected is the market-data source chosen once at startup, for the whole process lifetime.
type Selected struct {
	Source     marketdata.MarketDataSource
	Start      func()
	StaleAfter time.Duration // per-source NO_UPDATES threshold
}

// Select applies the market-data mode:
//   - MOCK: the deterministic simulator;
//   - IBKR: the IBKR adapter, never falling back (an outage means RECONNECTING/DISCONNECTED and stale data);
//   - AUTO (local development only): IBKR when configured and a session is established at startup,
//     otherwise MOCK. The choice is logged loudly, exposed in metrics, /health, connection messages and bars,
//     and never changes while the process runs.
//
// The active source is recorded in realtime_gateway_source_info.
func Select(cfg config.Config, clk clock.Clock, m *metrics.Gateway, log *slog.Logger, store hotcache.Store, guard *hotcache.Guard) (Selected, error) {
	sel, err := choose(cfg, clk, m, log, store, guard)
	if err != nil {
		return Selected{}, err
	}
	m.SourceInfo.WithLabelValues(string(sel.Source.ID()), cfg.MarketDataMode).Set(1)
	log.Info("market data source selected for this process", "source", string(sel.Source.ID()), "mode", cfg.MarketDataMode)
	return sel, nil
}

func choose(cfg config.Config, clk clock.Clock, m *metrics.Gateway, log *slog.Logger, store hotcache.Store, guard *hotcache.Guard) (Selected, error) {
	mock := func() (Selected, error) {
		sim, err := simulator.NewSource(simulator.NewModel(cfg.Seed, simulator.WithSyntheticSymbols(cfg.SyntheticSymbols)), clk, cfg.TickInterval)
		if err != nil {
			return Selected{}, err
		}
		return Selected{Source: sim, Start: func() {}, StaleAfter: cfg.StaleAfter}, nil
	}
	switch cfg.MarketDataMode {
	case config.ModeMock:
		return mock()
	case config.ModeIBKR:
		return ibkrSource(cfg, clk, m, log, store, guard)
	}
	// AUTO
	if !cfg.IBKR.Configured() {
		log.Warn("AUTO mode: IBKR is not configured; using the MOCK simulator for this process")
		return mock()
	}
	icfg, err := IBKRConfig(cfg)
	if err != nil {
		return Selected{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.IBKR.AutoProbeTimeout)
	defer cancel()
	if err := ibkr.ProbeSession(ctx, icfg, clk, m, log); err != nil {
		log.Warn("AUTO mode: no established IBKR session at startup; using the MOCK simulator for this process (restart after logging in to use IBKR)",
			"reason", err.Error())
		return mock()
	}
	return ibkrSource(cfg, clk, m, log, store, guard)
}

func ibkrSource(cfg config.Config, clk clock.Clock, m *metrics.Gateway, log *slog.Logger, store hotcache.Store, guard *hotcache.Guard) (Selected, error) {
	icfg, err := IBKRConfig(cfg)
	if err != nil {
		return Selected{}, err
	}
	src, err := ibkr.New(icfg, store, guard, clk, m, log)
	if err != nil {
		return Selected{}, err
	}
	return Selected{Source: src, Start: src.Start, StaleAfter: cfg.IBKR.StaleAfter}, nil
}

// IBKRConfig builds the adapter configuration from the validated gateway configuration.
func IBKRConfig(cfg config.Config) (ibkr.Config, error) {
	ca, err := os.ReadFile(cfg.IBKR.CAFile)
	if err != nil {
		return ibkr.Config{}, fmt.Errorf("IBKR CA file: %w", err)
	}
	i := cfg.IBKR
	return ibkr.Config{
		Client: ibkr.ClientConfig{BaseURL: i.BaseURL, CAPEM: ca, Timeout: i.RequestTimeout, UserAgent: "trading-terminal-realtime-gateway"},
		Pacing: pacing.Config{
			SessionLimit: i.SessionLimit, Allocation: i.Allocation, Headroom: i.Headroom,
			MaxWaiters: i.LimiterQueue, AcquireTimeout: i.LimiterTimeout, Cooldown: i.PenaltyCooldown,
		},
		TickleInterval: i.TickleInterval, PingInterval: i.PingInterval,
		RenewAfter: i.RenewAfter, RenewJitter: i.RenewJitter,
		Reconnect:     reconnect.Policy{Base: i.ReconnectBase, Max: i.ReconnectMax, MaxAttempts: i.ReconnectTries, MaxTotal: i.ReconnectTotal},
		ProbeInterval: i.ProbeInterval, SnapshotWait: i.SnapshotWait,
		InitWait: 2 * time.Second, AuthWait: 5 * time.Second, SnapshotLinger: 10 * time.Second,
		WSSendRate: i.WSSendRate, CacheTTL: i.ConidCacheTTL, NegativeTTL: 10 * time.Minute, Seed: i.ConidSeed,
	}, nil
}
