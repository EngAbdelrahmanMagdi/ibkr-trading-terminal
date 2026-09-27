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
	ControlQueueSize       int
	FlushInterval          time.Duration
	SlowConsumerLag        time.Duration
	MaxLaggingFlushes      int
	MaxConnections         int
	MaxActiveSymbols       int
	UnsubscribeGrace       time.Duration
	StaleAfter             time.Duration
	SyntheticSymbols       int
	MaxBarComputations     int
	BarsTimeout            time.Duration
	BarsCacheMaxTTL        time.Duration
	RedisAddr              string // empty disables Redis (hot state and bars cache)
	RedisUsername          string
	RedisPasswordFile      string
	RedisTimeout           time.Duration
	RedisCooldown          time.Duration
	QuoteCacheInterval     time.Duration
	QuoteCacheTTL          time.Duration
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
	DefaultControlQueueSize       = "256"
	DefaultFlushInterval          = "50ms"
	DefaultSlowConsumerLag        = "2s"
	DefaultMaxLaggingFlushes      = "5"
	DefaultMaxConnections         = "1000"
	DefaultMaxActiveSymbols       = "100"
	DefaultUnsubscribeGrace       = "10s"
	DefaultStaleAfter             = "5s"
	DefaultSyntheticSymbols       = "0"
	DefaultMaxBarComputations     = "2"
	DefaultBarsTimeout            = "8s"
	DefaultBarsCacheMaxTTL        = "5m"
	DefaultRedisUsername          = "app"
	DefaultRedisTimeout           = "100ms"
	DefaultRedisCooldown          = "5s"
	DefaultQuoteCacheInterval     = "1s"
	DefaultQuoteCacheTTL          = "30s"
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
		ControlQueueSize:       int(intVar("GATEWAY_CONTROL_QUEUE_SIZE", DefaultControlQueueSize, 16, 65_536)),
		FlushInterval:          durationVar("GATEWAY_FLUSH_INTERVAL", DefaultFlushInterval, 5*time.Millisecond, time.Second),
		SlowConsumerLag:        durationVar("GATEWAY_SLOW_CONSUMER_LAG", DefaultSlowConsumerLag, 100*time.Millisecond, time.Minute),
		MaxLaggingFlushes:      int(intVar("GATEWAY_SLOW_CONSUMER_MAX_LAGGING_FLUSHES", DefaultMaxLaggingFlushes, 1, 1000)),
		MaxConnections:         int(intVar("GATEWAY_MAX_CONNECTIONS", DefaultMaxConnections, 1, 100_000)),
		MaxActiveSymbols:       int(intVar("GATEWAY_MAX_ACTIVE_SYMBOLS", DefaultMaxActiveSymbols, 1, 10_000)),
		UnsubscribeGrace:       durationVar("GATEWAY_UNSUBSCRIBE_GRACE", DefaultUnsubscribeGrace, 0, 10*time.Minute),
		StaleAfter:             durationVar("GATEWAY_STALE_AFTER", DefaultStaleAfter, 20*time.Millisecond, 10*time.Minute),
		SyntheticSymbols:       int(intVar("GATEWAY_SIM_SYNTHETIC_SYMBOLS", DefaultSyntheticSymbols, 0, 999)),
		MaxBarComputations:     int(intVar("GATEWAY_MAX_BAR_COMPUTATIONS", DefaultMaxBarComputations, 1, 64)),
		BarsTimeout:            durationVar("GATEWAY_BARS_TIMEOUT", DefaultBarsTimeout, time.Second, time.Minute),
		BarsCacheMaxTTL:        durationVar("GATEWAY_BARS_CACHE_MAX_TTL", DefaultBarsCacheMaxTTL, time.Second, time.Hour),
		RedisAddr:              get("GATEWAY_REDIS_ADDR", ""),
		RedisUsername:          get("GATEWAY_REDIS_USERNAME", DefaultRedisUsername),
		RedisPasswordFile:      get("GATEWAY_REDIS_PASSWORD_FILE", ""),
		RedisTimeout:           durationVar("GATEWAY_REDIS_TIMEOUT", DefaultRedisTimeout, 10*time.Millisecond, 5*time.Second),
		RedisCooldown:          durationVar("GATEWAY_REDIS_COOLDOWN", DefaultRedisCooldown, 100*time.Millisecond, 5*time.Minute),
		QuoteCacheInterval:     durationVar("GATEWAY_QUOTE_CACHE_INTERVAL", DefaultQuoteCacheInterval, 100*time.Millisecond, time.Minute),
		QuoteCacheTTL:          durationVar("GATEWAY_QUOTE_CACHE_TTL", DefaultQuoteCacheTTL, time.Second, time.Hour),
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
	if c.SlowConsumerLag <= c.FlushInterval {
		errs = append(errs, errors.New("GATEWAY_SLOW_CONSUMER_LAG must be longer than GATEWAY_FLUSH_INTERVAL"))
	}
	if c.StaleAfter < 2*c.TickInterval {
		errs = append(errs, errors.New("GATEWAY_STALE_AFTER must be at least twice GATEWAY_TICK_INTERVAL"))
	}
	if c.QuoteCacheTTL <= c.QuoteCacheInterval {
		errs = append(errs, errors.New("GATEWAY_QUOTE_CACHE_TTL must be longer than GATEWAY_QUOTE_CACHE_INTERVAL"))
	}
	if c.RedisAddr != "" && c.RedisPasswordFile == "" {
		errs = append(errs, errors.New("GATEWAY_REDIS_PASSWORD_FILE is required when GATEWAY_REDIS_ADDR is set"))
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
