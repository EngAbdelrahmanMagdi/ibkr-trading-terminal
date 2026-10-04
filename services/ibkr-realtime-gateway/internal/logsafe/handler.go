// Package logsafe provides redaction at the structured log boundary.
package logsafe

import (
	"fmt"
	"log/slog"
	"regexp"
	"strings"
)

var header = regexp.MustCompile(`(?i)(authorization|cookie|set-cookie)["']?\s*[:=]\s*[^\r\n]+`)
var value = regexp.MustCompile(`(?i)(api[-_]?key|password|token|session(?:id)?)["']?\s*[:=]\s*["']?[^\s"',;&}\]]+`)

func redact(text string) string {
	return value.ReplaceAllString(header.ReplaceAllString(text, "$1=[REDACTED]"), "$1=[REDACTED]")
}

func ReplaceAttr(_ []string, attr slog.Attr) slog.Attr {
	key := strings.ToLower(strings.NewReplacer("-", "", "_", "").Replace(attr.Key))
	switch key {
	case "authorization", "cookie", "setcookie", "password", "token", "apikey", "session", "sessionid", "ibkrsession", "accountid":
		attr.Value = slog.StringValue("[REDACTED]")
		return attr
	}
	if attr.Value.Kind() == slog.KindString {
		attr.Value = slog.StringValue(redact(attr.Value.String()))
	}
	if err, ok := attr.Value.Any().(error); ok {
		attr.Value = slog.StringValue(fmt.Sprintf("%T", err))
	}
	return attr
}
