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
	"sync"
	"syscall"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/clock"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/config"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/httpapi"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/simulator"
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
	source, err := simulator.NewSource(simulator.NewModel(cfg.Seed), clk, cfg.TickInterval)
	if err != nil {
		logger.Error("market data source setup failed", "error", err.Error())
		return 2
	}
	wsServer := ws.NewServer(ws.Config{
		AllowedOrigins:         cfg.AllowedOrigins,
		MaxInboundMessageBytes: cfg.MaxInboundMessageBytes,
		MaxSymbolsPerSubscribe: cfg.MaxSymbolsPerSubscribe,
		MaxSubscribedSymbols:   cfg.MaxSubscribedSymbols,
		HeartbeatInterval:      cfg.HeartbeatInterval,
		WriteTimeout:           cfg.WriteTimeout,
		SendQueueSize:          cfg.SendQueueSize,
		MaxConnections:         cfg.MaxConnections,
	}, source, clk, logger)
	health := httpapi.NewHealth(source, wsServer.Clients, clk)

	mux := http.NewServeMux()
	mux.Handle("GET /ws", wsServer)
	mux.Handle("GET /api/v1/market/bars", http.TimeoutHandler(httpapi.NewBarsHandler(source, logger), 10*time.Second, "request timed out"))

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
	logger.Info("realtime gateway started", "source", string(source.ID()), "httpAddr", cfg.HTTPAddr,
		"healthAddr", cfg.HealthAddr, "tickInterval", cfg.TickInterval.String())

	exitCode := 0
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-serverErrs:
		logger.Error("server failed", "error", err.Error())
		exitCode = 1
	}

	// Graceful shutdown: fail readiness, close WebSocket sessions (1001), stop HTTP servers, all within the deadline.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	health.SetShuttingDown()
	if err := wsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("websocket shutdown incomplete", "error", err.Error())
		exitCode = 1
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
