package tests

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func (g *gateway) get(t *testing.T, base, path string, header http.Header) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := g.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func TestBarsConformToContractForEverySupportedCombination(t *testing.T) {
	g := startGateway(t, nil)
	// Uncached one-month bars are CPU-heavy and can take several seconds under the race detector; allow up to
	// the bars compute timeout of the gateway under test.
	g.client.Timeout = 60 * time.Second
	combos := map[string][]string{"1m": {"1d", "5d"}, "5m": {"1d", "5d", "1mo"}, "15m": {"5d", "1mo"}, "1h": {"5d", "1mo"}, "1d": {"1mo", "3mo", "1y"}}
	for interval, ranges := range combos {
		for _, rng := range ranges {
			resp, body := g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NVDA&interval="+interval+"&range="+rng, nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s/%s: status %d: %s", interval, rng, resp.StatusCode, body)
			}
			conform(t, "market/bars-response.schema.json", body)
			var out struct {
				Source string            `json:"source"`
				Bars   []json.RawMessage `json:"bars"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatal(err)
			}
			if out.Source != "MOCK" || len(out.Bars) == 0 {
				t.Fatalf("%s/%s: source %q with %d bars", interval, rng, out.Source, len(out.Bars))
			}
		}
	}
}

func TestBarsErrorsUseTheProblemContract(t *testing.T) {
	g := startGateway(t, nil)
	cases := []struct {
		query    string
		status   int
		category string
	}{
		{"symbol=NVDA&interval=1m&range=1y", http.StatusBadRequest, "VALIDATION"},
		{"symbol=NVDA&interval=2m&range=1d", http.StatusBadRequest, "VALIDATION"},
		{"symbol=nvda&interval=1m&range=1d", http.StatusBadRequest, "VALIDATION"},
		{"interval=1m&range=1d", http.StatusBadRequest, "VALIDATION"},
		{"symbol=NOPE&interval=1m&range=1d", http.StatusNotFound, "INSTRUMENT_NOT_FOUND"},
	}
	for _, c := range cases {
		resp, body := g.get(t, g.public.URL, "/api/v1/market/bars?"+c.query, nil)
		if resp.StatusCode != c.status || resp.Header.Get("Content-Type") != "application/problem+json" {
			t.Fatalf("%s: status %d (%s), want %d problem", c.query, resp.StatusCode, resp.Header.Get("Content-Type"), c.status)
		}
		conform(t, "common/problem.schema.json", body)
		if !strings.Contains(string(body), `"category":"`+c.category+`"`) {
			t.Fatalf("%s: body %s, want category %s", c.query, body, c.category)
		}
	}
}

func TestCorrelationIdIsEchoedOrGenerated(t *testing.T) {
	g := startGateway(t, nil)
	const id = "3f1c2a9e-4b7d-4c8e-9f0a-1b2c3d4e5f60"
	resp, body := g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NOPE&interval=1m&range=1d", http.Header{"X-Correlation-Id": {id}})
	if resp.Header.Get("X-Correlation-Id") != id || !strings.Contains(string(body), id) {
		t.Fatalf("valid correlation id not echoed: header %q body %s", resp.Header.Get("X-Correlation-Id"), body)
	}
	resp, _ = g.get(t, g.public.URL, "/api/v1/market/bars?symbol=NVDA&interval=1m&range=1d", http.Header{"X-Correlation-Id": {"not-a-uuid"}})
	if got := resp.Header.Get("X-Correlation-Id"); got == "not-a-uuid" || len(got) != 36 {
		t.Fatalf("invalid correlation id must be replaced with a generated UUID, got %q", got)
	}
}

func TestHealthEndpoints(t *testing.T) {
	g := startGateway(t, nil)
	for _, path := range []string{"/liveness", "/readiness", "/ready"} {
		if resp, body := g.get(t, g.intern.URL, path, nil); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d: %s", path, resp.StatusCode, body)
		}
	}
	resp, body := g.get(t, g.intern.URL, "/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health: status %d", resp.StatusCode)
	}
	conform(t, "ops/health-detail.schema.json", body)

	g.health.SetShuttingDown()
	if resp, _ := g.get(t, g.intern.URL, "/readiness", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readiness during shutdown: %d, want 503", resp.StatusCode)
	}
	if resp, _ := g.get(t, g.intern.URL, "/liveness", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("liveness during shutdown: %d, want 200", resp.StatusCode)
	}
	_, body = g.get(t, g.intern.URL, "/health", nil)
	conform(t, "ops/health-detail.schema.json", body)
	if !strings.Contains(string(body), `"status":"DOWN"`) {
		t.Fatalf("health during shutdown: %s", body)
	}
}
