package config

import (
	"strings"
	"testing"
	"time"
)

func env(values map[string]string) func(string) string {
	return func(k string) string { return values[k] }
}

func TestDefaultsAreValid(t *testing.T) {
	c, err := Load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.TickInterval != time.Second || c.MaxSymbolsPerSubscribe != 50 || c.MaxSubscribedSymbols != 100 ||
		c.MaxInboundMessageBytes != 4096 || c.HeartbeatInterval != 15*time.Second || len(c.AllowedOrigins) != 2 ||
		c.RedisAddr != "" || c.MaxActiveSymbols != 100 || c.SyntheticSymbols != 0 || c.StaleAfter != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestInvalidValuesAreRejected(t *testing.T) {
	cases := map[string]map[string]string{
		"wildcard origin":         {"GATEWAY_ALLOWED_ORIGINS": "*"},
		"empty origins":           {"GATEWAY_ALLOWED_ORIGINS": " , "},
		"tick too small":          {"GATEWAY_TICK_INTERVAL": "1ms"},
		"tick not milliseconds":   {"GATEWAY_TICK_INTERVAL": "1500us"},
		"heartbeat too long":      {"GATEWAY_HEARTBEAT_INTERVAL": "90s"},
		"write >= heartbeat":      {"GATEWAY_WRITE_TIMEOUT": "20s", "GATEWAY_HEARTBEAT_INTERVAL": "15s"},
		"per-subscribe > total":   {"GATEWAY_MAX_SYMBOLS_PER_SUBSCRIBE": "200", "GATEWAY_MAX_SUBSCRIBED_SYMBOLS": "100"},
		"message limit too small": {"GATEWAY_MAX_INBOUND_MESSAGE_BYTES": "10"},
		"queue too small":         {"GATEWAY_CONTROL_QUEUE_SIZE": "1"},
		"lag <= flush interval":   {"GATEWAY_SLOW_CONSUMER_LAG": "100ms", "GATEWAY_FLUSH_INTERVAL": "200ms"},
		"stale < 2 ticks":         {"GATEWAY_STALE_AFTER": "1s", "GATEWAY_TICK_INTERVAL": "1s"},
		"quote ttl <= interval":   {"GATEWAY_QUOTE_CACHE_TTL": "1s", "GATEWAY_QUOTE_CACHE_INTERVAL": "1s"},
		"redis without password":  {"GATEWAY_REDIS_ADDR": "redis:6379"},
		"negative grace":          {"GATEWAY_UNSUBSCRIBE_GRACE": "-1s"},
		"too many synthetic":      {"GATEWAY_SIM_SYNTHETIC_SYMBOLS": "1000"},
		"zero bar computations":   {"GATEWAY_MAX_BAR_COMPUTATIONS": "0"},
		"not a number":            {"GATEWAY_MAX_CONNECTIONS": "many"},
		"bad seed":                {"GATEWAY_SEED": "-1"},
		"bad log level":           {"GATEWAY_LOG_LEVEL": "loud"},
	}
	for name, values := range cases {
		if _, err := Load(env(values)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestAllErrorsAreReportedTogether(t *testing.T) {
	_, err := Load(env(map[string]string{"GATEWAY_TICK_INTERVAL": "x", "GATEWAY_CONTROL_QUEUE_SIZE": "x"}))
	if err == nil || !strings.Contains(err.Error(), "GATEWAY_TICK_INTERVAL") || !strings.Contains(err.Error(), "GATEWAY_CONTROL_QUEUE_SIZE") {
		t.Fatalf("expected both errors, got %v", err)
	}
}
