// Package origin defines the shared exact REST/WebSocket browser-origin policy.
package origin

import (
	"errors"
	"net/url"
	"strings"
)

func Normalize(value string) (string, error) {
	if !strings.Contains(value, "://") {
		value = "http://" + value
	}
	u, err := url.Parse(value)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") || strings.ContainsAny(value, "*\r\n") || value != u.Scheme+"://"+u.Host {
		return "", errors.New("explicit HTTP origin required")
	}
	return value, nil
}

func Allowed(value string, configured []string) bool {
	if value == "" {
		return true
	}
	if !strings.Contains(value, "://") {
		return false
	}
	parsed, err := Normalize(value)
	if err != nil {
		return false
	}
	for _, candidate := range configured {
		allowed, err := Normalize(candidate)
		if err == nil && parsed == allowed {
			return true
		}
	}
	return false
}
