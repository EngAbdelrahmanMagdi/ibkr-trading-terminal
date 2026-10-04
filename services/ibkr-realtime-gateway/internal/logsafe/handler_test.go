package logsafe

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestStructuredBoundaryRedactsCredentials(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: ReplaceAttr}))
	logger.Error("Authorization: Bearer sentinel-header", "Cookie", "sentinel-cookie", "error", errors.New("sentinel-exception"), "url", "?token=sentinel-token&symbol=AAPL")
	if strings.Contains(output.String(), "sentinel") {
		t.Fatal("credential retained in structured log")
	}
	if !strings.Contains(output.String(), "[REDACTED]") {
		t.Fatal("missing redaction")
	}
}
