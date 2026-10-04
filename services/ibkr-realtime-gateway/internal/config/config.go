// Package config loads and validates the gateway configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/origin"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config is the validated gateway configuration.
type Config struct {
	HTTPAddr               string
	HealthAddr             string
	AllowedOrigins         []string
	BarsRequestsPerMinute  int
	UpgradesPerMinute      int
	CommandsPerSecond      int
	CommandBurst           int
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

	// Order notifications (empty KafkaBrokers disables them).
	KafkaBrokers        []string
	OrderEventsTopic    string
	OrderEventsGroup    string
	KafkaMaxPollRecords int
	BrokerUpdatesTopic  string
	BrokerUpdatesBuffer int

	MarketDataMode     string // MOCK, IBKR or AUTO
	TrustedEnvironment bool
	IBKR               IBKR
}

// Market-data modes.
const (
	ModeMock = "MOCK"
	ModeIBKR = "IBKR"
	ModeAuto = "AUTO"
)

// IBKR configures the IBKR market-data source (used in IBKR mode, and in AUTO mode when configured).
type IBKR struct {
	BaseURL          string
	CAFile           string
	SessionLimit     float64
	Allocation       float64
	Headroom         float64
	LimiterQueue     int
	LimiterTimeout   time.Duration
	PenaltyCooldown  time.Duration
	RequestTimeout   time.Duration
	TickleInterval   time.Duration
	PingInterval     time.Duration
	RenewAfter       time.Duration
	RenewJitter      time.Duration
	ReconnectBase    time.Duration
	ReconnectMax     time.Duration
	ReconnectTries   int
	ReconnectTotal   time.Duration
	ProbeInterval    time.Duration
	SnapshotWait     time.Duration
	WSSendRate       float64
	ConidCacheTTL    time.Duration
	ConidSeed        map[string]int64
	StaleAfter       time.Duration
	MarketDataLines  int
	AutoProbeTimeout time.Duration
}

// Configured reports whether the settings needed to reach the CP Gateway are present.
func (i IBKR) Configured() bool { return i.BaseURL != "" && i.CAFile != "" }

// Defaults are documented in the service README and the WebSocket protocol description.
const (
	DefaultHTTPAddr               = ":8090"
	DefaultHealthAddr             = ":8091"
	DefaultAllowedOrigins         = "http://localhost:3000,http://127.0.0.1:3000"
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
	DefaultOrderEventsTopic       = "trading.order-events.v1"
	DefaultOrderEventsGroup       = "realtime-gateway-order-notifications"
	DefaultKafkaMaxPollRecords    = "500"
	DefaultBrokerUpdatesTopic     = "broker.order-updates.v1"
	DefaultBrokerUpdatesBuffer    = "10000"
	DefaultQuoteCacheInterval     = "1s"
	DefaultQuoteCacheTTL          = "30s"
	DefaultShutdownTimeout        = "10s"
	DefaultLogLevel               = "info"
	DefaultMarketDataMode         = ModeMock
	DefaultIBKRBaseURL            = "https://host.docker.internal:5000/v1/api"
)

// parseSeed parses "SYMBOL:CONID,SYMBOL:CONID".
func parseSeed(v string) (map[string]int64, error) {
	seed := map[string]int64{}
	for _, pair := range strings.Split(v, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		sym, id, ok := strings.Cut(pair, ":")
		conid, err := strconv.ParseInt(strings.TrimSpace(id), 10, 64)
		if !ok || err != nil || conid <= 0 || strings.TrimSpace(sym) == "" {
			return nil, errors.New("GATEWAY_IBKR_CONID_SEED must be a comma-separated list of SYMBOL:CONID")
		}
		seed[strings.ToUpper(strings.TrimSpace(sym))] = conid
	}
	return seed, nil
}

// brokerModeErrors enforces the trusted-environment rules for modes that may use IBKR.
func (c Config) brokerModeErrors() []error {
	var errs []error
	if !c.TrustedEnvironment {
		errs = append(errs, errors.New("GATEWAY_TRUSTED_ENVIRONMENT=true is required for IBKR and AUTO modes; public deployments use MOCK"))
	}
	for _, o := range c.AllowedOrigins {
		if !privateOrigin(o) {
			errs = append(errs, fmt.Errorf("GATEWAY_ALLOWED_ORIGINS must list only loopback or private origins in IBKR and AUTO modes: %q", o))
		}
	}
	if c.MaxActiveSymbols > c.IBKR.MarketDataLines {
		errs = append(errs, errors.New("GATEWAY_MAX_ACTIVE_SYMBOLS must not exceed GATEWAY_IBKR_MARKET_DATA_LINES"))
	}
	if c.MarketDataMode == ModeIBKR {
		if c.IBKR.CAFile == "" {
			errs = append(errs, errors.New("GATEWAY_IBKR_CA_FILE is required in IBKR mode"))
		}
		if !strings.HasPrefix(c.IBKR.BaseURL, "https://") {
			errs = append(errs, errors.New("GATEWAY_IBKR_BASE_URL must be an https URL"))
		}
	}
	return errs
}

// privateOrigin reports whether an origin host pattern is loopback or on a private network.
func privateOrigin(origin string) bool {
	host := origin
	if parsed, err := url.Parse(origin); err == nil && parsed.Host != "" {
		host = parsed.Hostname()
	}
	if h, _, err := net.SplitHostPort(origin); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}

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
	c.KafkaBrokers = splitList(get("GATEWAY_KAFKA_BROKERS", ""))
	c.OrderEventsTopic = get("GATEWAY_ORDER_EVENTS_TOPIC", DefaultOrderEventsTopic)
	c.OrderEventsGroup = get("GATEWAY_ORDER_EVENTS_GROUP", DefaultOrderEventsGroup)
	c.KafkaMaxPollRecords = int(intVar("GATEWAY_KAFKA_MAX_POLL_RECORDS", DefaultKafkaMaxPollRecords, 1, 10_000))
	c.BrokerUpdatesTopic = get("GATEWAY_BROKER_UPDATES_TOPIC", DefaultBrokerUpdatesTopic)
	c.BrokerUpdatesBuffer = int(intVar("GATEWAY_BROKER_UPDATES_BUFFER", DefaultBrokerUpdatesBuffer, 100, 100_000))

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
	c.BarsRequestsPerMinute = int(intVar("GATEWAY_BARS_REQUESTS_PER_MINUTE", "120", 1, 1000000))
	c.UpgradesPerMinute = int(intVar("GATEWAY_UPGRADES_PER_MINUTE", "30", 1, 1000000))
	c.CommandsPerSecond = int(intVar("GATEWAY_COMMANDS_PER_SECOND", "10", 1, 10000))
	c.CommandBurst = int(intVar("GATEWAY_COMMAND_BURST", "20", 1, 10000))
	for _, o := range strings.Split(get("GATEWAY_ALLOWED_ORIGINS", DefaultAllowedOrigins), ",") {
		o = strings.TrimSpace(o)
		switch {
		case o == "":
		case strings.Contains(o, "*"):
			errs = append(errs, fmt.Errorf("GATEWAY_ALLOWED_ORIGINS must list explicit origins, not wildcards: %q", o))
		default:
			normalized, err := origin.Normalize(o)
			if err != nil {
				errs = append(errs, errors.New("GATEWAY_ALLOWED_ORIGINS requires exact HTTP origins"))
			} else {
				c.AllowedOrigins = append(c.AllowedOrigins, normalized)
			}
		}
	}
	if len(c.AllowedOrigins) == 0 {
		errs = append(errs, errors.New("GATEWAY_ALLOWED_ORIGINS must list at least one origin"))
	}
	if err := c.LogLevel.UnmarshalText([]byte(get("GATEWAY_LOG_LEVEL", DefaultLogLevel))); err != nil {
		errs = append(errs, errors.New("GATEWAY_LOG_LEVEL must be one of debug, info, warn, error"))
	}

	floatVar := func(key, def string, lo, hi float64) float64 {
		f, err := strconv.ParseFloat(get(key, def), 64)
		if err != nil || f < lo || f > hi {
			errs = append(errs, fmt.Errorf("%s must be a number between %g and %g", key, lo, hi))
		}
		return f
	}
	c.MarketDataMode = strings.ToUpper(get("GATEWAY_MARKET_DATA_MODE", DefaultMarketDataMode))
	switch c.MarketDataMode {
	case ModeMock, ModeIBKR, ModeAuto:
	default:
		errs = append(errs, errors.New("GATEWAY_MARKET_DATA_MODE must be MOCK, IBKR or AUTO"))
	}
	switch get("GATEWAY_TRUSTED_ENVIRONMENT", "false") {
	case "true":
		c.TrustedEnvironment = true
	case "false":
	default:
		errs = append(errs, errors.New("GATEWAY_TRUSTED_ENVIRONMENT must be true or false"))
	}
	c.IBKR = IBKR{
		BaseURL:          get("GATEWAY_IBKR_BASE_URL", DefaultIBKRBaseURL),
		CAFile:           get("GATEWAY_IBKR_CA_FILE", ""),
		SessionLimit:     floatVar("GATEWAY_IBKR_SESSION_LIMIT", "10", 1, 100),
		Allocation:       floatVar("GATEWAY_IBKR_ALLOCATION", "5", 0.1, 100),
		Headroom:         floatVar("GATEWAY_IBKR_HEADROOM", "1", 0.1, 100),
		LimiterQueue:     int(intVar("GATEWAY_IBKR_LIMITER_QUEUE", "64", 1, 10_000)),
		LimiterTimeout:   durationVar("GATEWAY_IBKR_LIMITER_TIMEOUT", "5s", 100*time.Millisecond, time.Minute),
		PenaltyCooldown:  durationVar("GATEWAY_IBKR_PENALTY_COOLDOWN", "15m", time.Second, time.Hour),
		RequestTimeout:   durationVar("GATEWAY_IBKR_REQUEST_TIMEOUT", "10s", time.Second, time.Minute),
		TickleInterval:   durationVar("GATEWAY_IBKR_TICKLE_INTERVAL", "60s", 5*time.Second, 4*time.Minute),
		PingInterval:     durationVar("GATEWAY_IBKR_PING_INTERVAL", "30s", time.Second, 59*time.Second),
		RenewAfter:       durationVar("GATEWAY_IBKR_SMD_RENEW_AFTER", "8m", 10*time.Second, 570*time.Second),
		RenewJitter:      durationVar("GATEWAY_IBKR_SMD_RENEW_JITTER", "60s", 0, 5*time.Minute),
		ReconnectBase:    durationVar("GATEWAY_IBKR_RECONNECT_BASE", "1s", 10*time.Millisecond, time.Minute),
		ReconnectMax:     durationVar("GATEWAY_IBKR_RECONNECT_MAX", "30s", 10*time.Millisecond, 10*time.Minute),
		ReconnectTries:   int(intVar("GATEWAY_IBKR_RECONNECT_ATTEMPTS", "10", 1, 1000)),
		ReconnectTotal:   durationVar("GATEWAY_IBKR_RECONNECT_MAX_TOTAL", "5m", time.Second, time.Hour),
		ProbeInterval:    durationVar("GATEWAY_IBKR_PROBE_INTERVAL", "30s", time.Second, 10*time.Minute),
		SnapshotWait:     durationVar("GATEWAY_IBKR_SNAPSHOT_WAIT", "2s", 100*time.Millisecond, 30*time.Second),
		WSSendRate:       floatVar("GATEWAY_IBKR_WS_SEND_RATE", "5", 0.1, 100),
		ConidCacheTTL:    durationVar("GATEWAY_IBKR_CONID_CACHE_TTL", "168h", time.Minute, 30*24*time.Hour),
		StaleAfter:       durationVar("GATEWAY_IBKR_STALE_AFTER", "120s", time.Second, time.Hour),
		MarketDataLines:  int(intVar("GATEWAY_IBKR_MARKET_DATA_LINES", "100", 1, 10_000)),
		AutoProbeTimeout: durationVar("GATEWAY_AUTO_PROBE_TIMEOUT", "10s", time.Second, time.Minute),
	}
	conidSeed, err := parseSeed(get("GATEWAY_IBKR_CONID_SEED", ""))
	if err != nil {
		errs = append(errs, err)
	}
	c.IBKR.ConidSeed = conidSeed
	if c.IBKR.Allocation+c.IBKR.Headroom > c.IBKR.SessionLimit {
		errs = append(errs, errors.New("GATEWAY_IBKR_ALLOCATION + GATEWAY_IBKR_HEADROOM must not exceed GATEWAY_IBKR_SESSION_LIMIT"))
	}
	if c.IBKR.RenewJitter >= c.IBKR.RenewAfter {
		errs = append(errs, errors.New("GATEWAY_IBKR_SMD_RENEW_JITTER must be shorter than GATEWAY_IBKR_SMD_RENEW_AFTER"))
	}
	if c.IBKR.ReconnectMax < c.IBKR.ReconnectBase {
		errs = append(errs, errors.New("GATEWAY_IBKR_RECONNECT_MAX must not be shorter than GATEWAY_IBKR_RECONNECT_BASE"))
	}
	if c.MarketDataMode != ModeMock {
		errs = append(errs, c.brokerModeErrors()...)
	}
	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return c, nil
}

// splitList splits a comma-separated list, dropping blank entries.
func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
