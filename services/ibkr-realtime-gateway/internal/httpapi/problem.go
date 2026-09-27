// Package httpapi serves the gateway's HTTP endpoints: historical bars and health.
package httpapi

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// Problem is the error body (RFC 9457 compatible) shared by all services.
type Problem struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Category      string `json:"category"`
	Detail        string `json:"detail,omitempty"`
	CorrelationID string `json:"correlationId"`
}

// Error categories used by this service.
const (
	CategoryValidation         = "VALIDATION"
	CategoryInstrumentNotFound = "INSTRUMENT_NOT_FOUND"
	CategoryInternal           = "INTERNAL"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// CorrelationHeader carries the correlation id of a request.
const CorrelationHeader = "X-Correlation-Id"

// correlationID returns the client-supplied correlation id if it is a UUID, otherwise a new random UUID.
func correlationID(r *http.Request) string {
	if id := r.Header.Get(CorrelationHeader); uuidPattern.MatchString(id) {
		return strings.ToLower(id)
	}
	return newUUID()
}

// newUUID returns a random (version 4) UUID.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error on supported platforms
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func writeJSON(w http.ResponseWriter, status int, contentType string, v any) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the status line is already sent; a failed write means the client left
}

func writeProblem(w http.ResponseWriter, status int, category, slug, title, detail, cid string) {
	writeJSON(w, status, "application/problem+json", Problem{
		Type: "urn:problem-type:trading:" + slug, Title: title, Status: status,
		Category: category, Detail: detail, CorrelationID: cid,
	})
}
