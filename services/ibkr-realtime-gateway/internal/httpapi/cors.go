package httpapi

import (
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/origin"
	"net/http"
	"strings"
)

// BarsCORS uses the same complete-origin policy as the WebSocket.
// Legacy bare local host:port configuration permits HTTP only.
func BarsCORS(next http.Handler, allowedOrigins []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		origin := r.Header.Get("Origin")
		if origin != "" {
			if !originpolicy(origin, allowedOrigins) {
				http.Error(w, "origin not allowed", http.StatusForbidden)
				return
			}
		}

		switch r.Method {
		case http.MethodGet:
			if origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Expose-Headers", "Retry-After")
			}
			next.ServeHTTP(w, r)
		case http.MethodOptions:
			if origin == "" || r.Header.Get("Access-Control-Request-Method") != http.MethodGet {
				http.Error(w, "preflight not allowed", http.StatusForbidden)
				return
			}
			for _, requested := range strings.Split(r.Header.Get("Access-Control-Request-Headers"), ",") {
				requested = strings.TrimSpace(requested)
				if requested != "" && !strings.EqualFold(requested, CorrelationHeader) {
					http.Error(w, "preflight not allowed", http.StatusForbidden)
					return
				}
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", http.MethodGet)
			w.Header().Set("Access-Control-Allow-Headers", CorrelationHeader)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.Header().Set("Allow", "GET, OPTIONS")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func originpolicy(value string, configured []string) bool { return origin.Allowed(value, configured) }
