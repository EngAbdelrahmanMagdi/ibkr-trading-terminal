// Package config loads and validates the gateway configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// Config is the validated gateway configuration.
type Config struct {
	HTTPAddr               string
	HealthAddr             string
	AllowedOrigins         []string
	Seed                   uint64
	TickInterval           time.Duration
	MaxSymbolsPerSubscribe int
	MaxSubscribedSymbols   int
	MaxInboundMessageBytes int64
	HeartbeatInterval      time.Duration
	WriteTimeout           time.Duration
	SendQueueSize          int
	MaxConnections         int
	ShutdownTimeout        time.Duration
	LogLevel               slog.Level
}

// Defaults are documented in the service README and the WebSocket protocol description.
const (
	DefaultHTTPAddr               = ":8090"
	DefaultHealthAddr             = ":8091"
	DefaultAllowedOrigins         = "localhost:3000,127.0.0.1:3000"
	DefaultSeed                   = "20260927"
	DefaultTickInterval           = "1s"
	DefaultMaxSymbolsPerSubscribe = "50"
	DefaultMaxSubscribedSymbols   = "100"
	DefaultMaxInboundMessageBytes = "4096"
	DefaultHeartbeatInterval      = "15s"
	DefaultWriteTimeout           = "5s"
	DefaultSendQueueSize          = "256"
	DefaultMaxConnections         = "1000"
	DefaultShutdownTimeout        = "10s"
	DefaultLogLevel               = "info"
)

// Load reads the configuration using getenv (os.Getenv in production) and validates every value.
func Load(getenv func(string) string) (Config, error) {
	get := func(key, def string) string {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
		return def
	}
	var errs []error
	durationVar := func(key, def string, lo, hi time.Duration) time.Duration {
		d, err := time.ParseDuration(get(key, def))
		if err != nil || d < lo || d > hi {
			errs = append(errs, fmt.Errorf("%s must be a duration between %s and %s", key, lo, hi))
		}
		return d
	}
	intVar := func(key, def string, lo, hi int64) int64 {
		n, err := strconv.ParseInt(get(key, def), 10, 64)
		if err != nil || n < lo || n > hi {
			errs = append(errs, fmt.Errorf("%s must be an integer between %d and %d", key, lo, hi))
		}
		return n
	}

	c := Config{
		HTTPAddr:               get("GATEWAY_HTTP_ADDR", DefaultHTTPAddr),
		HealthAddr:             get("GATEWAY_HEALTH_ADDR", DefaultHealthAddr),
		TickInterval:           durationVar("GATEWAY_TICK_INTERVAL", DefaultTickInterval, 10*time.Millisecond, time.Minute),
		MaxSymbolsPerSubscribe: int(intVar("GATEWAY_MAX_SYMBOLS_PER_SUBSCRIBE", DefaultMaxSymbolsPerSubscribe, 1, 1000)),
		MaxSubscribedSymbols:   int(intVar("GATEWAY_MAX_SUBSCRIBED_SYMBOLS", DefaultMaxSubscribedSymbols, 1, 10_000)),
		MaxInboundMessageBytes: intVar("GATEWAY_MAX_INBOUND_MESSAGE_BYTES", DefaultMaxInboundMessageBytes, 256, 1<<20),
		HeartbeatInterval:      durationVar("GATEWAY_HEARTBEAT_INTERVAL", DefaultHeartbeatInterval, time.Second, 55*time.Second),
		WriteTimeout:           durationVar("GATEWAY_WRITE_TIMEOUT", DefaultWriteTimeout, 100*time.Millisecond, 30*time.Second),
		SendQueueSize:          int(intVar("GATEWAY_SEND_QUEUE_SIZE", DefaultSendQueueSize, 16, 65_536)),
		MaxConnections:         int(intVar("GATEWAY_MAX_CONNECTIONS", DefaultMaxConnections, 1, 100_000)),
		ShutdownTimeout:        durationVar("GATEWAY_SHUTDOWN_TIMEOUT", DefaultShutdownTimeout, time.Second, 2*time.Minute),
	}

	seed, err := strconv.ParseUint(get("GATEWAY_SEED", DefaultSeed), 10, 64)
	if err != nil {
		errs = append(errs, errors.New("GATEWAY_SEED must be an unsigned 64-bit integer"))
	}
	c.Seed = seed
	if c.TickInterval%time.Millisecond != 0 {
		errs = append(errs, errors.New("GATEWAY_TICK_INTERVAL must be a whole number of milliseconds"))
	}
	if c.MaxSymbolsPerSubscribe > c.MaxSubscribedSymbols {
		errs = append(errs, errors.New("GATEWAY_MAX_SYMBOLS_PER_SUBSCRIBE must not exceed GATEWAY_MAX_SUBSCRIBED_SYMBOLS"))
	}
	if c.WriteTimeout >= c.HeartbeatInterval {
		errs = append(errs, errors.New("GATEWAY_WRITE_TIMEOUT must be shorter than GATEWAY_HEARTBEAT_INTERVAL"))
	}
	for _, o := range strings.Split(get("GATEWAY_ALLOWED_ORIGINS", DefaultAllowedOrigins), ",") {
		o = strings.TrimSpace(o)
		switch {
		case o == "":
		case strings.Contains(o, "*"):
			errs = append(errs, fmt.Errorf("GATEWAY_ALLOWED_ORIGINS must list explicit origins, not wildcards: %q", o))
		default:
			c.AllowedOrigins = append(c.AllowedOrigins, o)
		}
	}
	if len(c.AllowedOrigins) == 0 {
		errs = append(errs, errors.New("GATEWAY_ALLOWED_ORIGINS must list at least one origin"))
	}
	if err := c.LogLevel.UnmarshalText([]byte(get("GATEWAY_LOG_LEVEL", DefaultLogLevel))); err != nil {
		errs = append(errs, errors.New("GATEWAY_LOG_LEVEL must be one of debug, info, warn, error"))
	}
	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return c, nil
}
