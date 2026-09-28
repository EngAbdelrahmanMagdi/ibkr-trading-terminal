// Command realtime-gateway serves market data over WebSocket and HTTP.
//
//	realtime-gateway serve        run the gateway (default)
//	realtime-gateway healthcheck  probe the local readiness endpoint (exit code 0 when ready)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/config"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/hotcache"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/httpapi"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/metrics"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/modes"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/notify"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/registry"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/transport/ws"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	cmd := "serve"
	if len(args) > 0 {
		cmd = args[0]
	}
	switch cmd {
	case "serve":
		return serve()
	case "healthcheck":
		return healthcheck()
	default:
		fmt.Fprintf(os.Stderr, "usage: realtime-gateway [serve|healthcheck]\n")
		return 2
	}
}

func serve() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "realtime-gateway")
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		logger.Error("invalid configuration", "error", err.Error())
		return 2
	}
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).With("service", "realtime-gateway")

	clk := clock.Real{}
	gm := metrics.New()

	// Redis hot state (optional, never critical).
	var (
		store       hotcache.Store
		guard       *hotcache.Guard
		quoteWriter *hotcache.QuoteWriter
	)
	if cfg.RedisAddr != "" {
		password, err := readSecret(cfg.RedisPasswordFile)
		if err != nil {
			logger.Error("redis password file unreadable", "error", err.Error())
			return 2
		}
		redisStore := hotcache.NewRedisStore(hotcache.RedisConfig{
			Addr: cfg.RedisAddr, Username: cfg.RedisUsername, Password: password, Timeout: cfg.RedisTimeout,
		})
		store = redisStore
		guard = hotcache.NewGuard(clk, cfg.RedisTimeout, cfg.RedisCooldown, gm, logger)
	}

	sel, err := modes.Select(cfg, clk, gm, logger, store, guard)
	if err != nil {
		logger.Error("market data source setup failed", "error", err.Error())
		return 2
	}
	source := sel.Source
	if store != nil {
		quoteWriter = hotcache.NewQuoteWriter(source.ID(), store, guard, clk, cfg.QuoteCacheInterval, cfg.QuoteCacheTTL, gm)
	}

	var wsServer *ws.Server
	hooks := registry.Hooks{
		OnState: func(state marketdata.SourceState, at time.Time) { wsServer.BroadcastState(state, at) },
	}
	if quoteWriter != nil {
		hooks.OnQuote = func(u *registry.Update) { quoteWriter.Publish(u.Symbol, u.Data) }
	}
	reg, err := registry.New(source, clk, registry.Config{
		MaxActiveSymbols: cfg.MaxActiveSymbols,
		UnsubscribeGrace: cfg.UnsubscribeGrace,
		StaleAfter:       sel.StaleAfter,
	}, gm, logger, hooks)
	if err != nil {
		logger.Error("subscription registry setup failed", "error", err.Error())
		return 2
	}
	wsServer = ws.NewServer(ws.Config{
		AllowedOrigins:         cfg.AllowedOrigins,
		MaxInboundMessageBytes: cfg.MaxInboundMessageBytes,
		MaxSymbolsPerSubscribe: cfg.MaxSymbolsPerSubscribe,
		MaxSubscribedSymbols:   cfg.MaxSubscribedSymbols,
		HeartbeatInterval:      cfg.HeartbeatInterval,
		WriteTimeout:           cfg.WriteTimeout,
		ControlQueueSize:       cfg.ControlQueueSize,
		FlushInterval:          cfg.FlushInterval,
		LagThreshold:           cfg.SlowConsumerLag,
		MaxLaggingFlushes:      cfg.MaxLaggingFlushes,
		MaxConnections:         cfg.MaxConnections,
	}, reg, clk, gm, logger)
	reg.Start()
	sel.Start()
	if quoteWriter != nil {
		go quoteWriter.Run() // owned by serve(); stopped by quoteWriter.Close during shutdown
	}
	// Order notifications from Kafka (optional, never critical: quotes and readiness do not depend on Kafka).
	notifyCtx, stopNotify := context.WithCancel(context.Background())
	defer stopNotify()
	var notifier *notify.Consumer
	notifierDone := make(chan struct{})
	if len(cfg.KafkaBrokers) > 0 {
		notifier, err = notify.NewConsumer(notify.Config{
			Brokers: cfg.KafkaBrokers, Topic: cfg.OrderEventsTopic, Group: cfg.OrderEventsGroup,
			MaxPollRecords: cfg.KafkaMaxPollRecords,
		}, notify.NewProcessor(wsServer, gm, logger), gm, logger)
		if err != nil {
			logger.Error("order notification consumer setup failed", "error", err.Error())
			return 2
		}
		go func() { // owned by serve(); returns once notifyCtx is cancelled
			defer close(notifierDone)
			notifier.Run(notifyCtx)
		}()
	} else {
		close(notifierDone)
	}
	health := httpapi.NewHealth(reg, wsServer.Clients, gm.Handler())
	bars := httpapi.NewBarsService(source, store, guard, httpapi.BarsConfig{
		MaxConcurrent: cfg.MaxBarComputations, ComputeTimeout: cfg.BarsTimeout, MaxCacheTTL: cfg.BarsCacheMaxTTL,
	}, gm)

	mux := http.NewServeMux()
	mux.Handle("GET /ws", wsServer)
	mux.Handle("GET /api/v1/market/bars", http.TimeoutHandler(httpapi.NewBarsHandler(source, bars, logger), cfg.BarsTimeout+2*time.Second, "request timed out"))

	errorLog := slog.NewLogLogger(logger.Handler(), slog.LevelWarn)
	public := &http.Server{
		Addr: cfg.HTTPAddr, Handler: mux, ErrorLog: errorLog,
		ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	internal := &http.Server{
		Addr: cfg.HealthAddr, Handler: health.Handler(), ErrorLog: errorLog,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErrs := make(chan error, 2) // bounded: at most one error per server
	var servers sync.WaitGroup
	for _, srv := range []*http.Server{public, internal} {
		servers.Add(1)
		go func() { // owned by serve(); returns once the server is shut down
			defer servers.Done()
			if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				serverErrs <- fmt.Errorf("%s: %w", srv.Addr, err)
			}
		}()
	}
	logger.Info("realtime gateway started", "source", string(source.ID()), "mode", cfg.MarketDataMode,
		"httpAddr", cfg.HTTPAddr, "healthAddr", cfg.HealthAddr, "redis", cfg.RedisAddr != "",
		"orderNotifications", len(cfg.KafkaBrokers) > 0)

	exitCode := 0
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-serverErrs:
		logger.Error("server failed", "error", err.Error())
		exitCode = 1
	}

	// Graceful shutdown, all within the deadline: fail readiness, close WebSocket sessions (1001), close
	// upstream subscriptions and the stale monitor, stop the market-data source (never logging out of a
	// broker session), flush and stop the hot-cache writer, close Redis, then stop the HTTP servers.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	health.SetShuttingDown()
	stopNotify()
	select {
	case <-notifierDone:
	case <-shutdownCtx.Done():
		logger.Error("order notification consumer did not stop in time")
		exitCode = 1
	}
	if notifier != nil {
		notifier.Close()
	}
	if err := wsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("websocket shutdown incomplete", "error", err.Error())
		exitCode = 1
	}
	reg.Close()
	if err := source.Close(shutdownCtx); err != nil {
		logger.Error("market data source shutdown incomplete", "error", err.Error())
		exitCode = 1
	}
	if quoteWriter != nil {
		if err := quoteWriter.Close(shutdownCtx); err != nil {
			logger.Error("hot-cache writer shutdown incomplete", "error", err.Error())
			exitCode = 1
		}
	}
	if store != nil {
		if err := store.Close(); err != nil {
			logger.Warn("redis client close failed", "error", err.Error())
		}
	}
	for _, srv := range []*http.Server{public, internal} {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown incomplete", "addr", srv.Addr, "error", err.Error())
			exitCode = 1
		}
	}
	servers.Wait()
	logger.Info("realtime gateway stopped", "exitCode", exitCode)
	return exitCode
}

// healthcheck probes the readiness endpoint on the loopback interface. It exists because the runtime image
// has no shell or HTTP client for container health checks.
func healthcheck() int {
	addr := os.Getenv("GATEWAY_HEALTH_ADDR")
	if addr == "" {
		addr = config.DefaultHealthAddr
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: invalid GATEWAY_HEALTH_ADDR %q\n", addr)
		return 1
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/readiness")
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: readiness returned %d\n", resp.StatusCode)
		return 1
	}
	return 0
}

// readSecret reads a secret file, ignoring surrounding whitespace (including a trailing CR/LF).
func readSecret(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	secret := strings.TrimSpace(string(b))
	if secret == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return secret, nil
}
