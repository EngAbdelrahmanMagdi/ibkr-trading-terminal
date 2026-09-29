package httpapi

import (
	"net/http"
	"net/url"
	"strings"
)

// BarsCORS permits browser bars requests from the same explicit origin hosts used by the WebSocket.
// The configured values are host:port entries; no-Origin internal requests remain available.
func BarsCORS(next http.Handler, allowedOrigins []string) http.Handler {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, host := range allowedOrigins {
		allowed[host] = struct{}{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		origin := r.Header.Get("Origin")
		if origin != "" {
			u, err := url.Parse(origin)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") ||
				u.User != nil || u.Host == "" || origin != u.Scheme+"://"+u.Host {
				http.Error(w, "origin not allowed", http.StatusForbidden)
				return
			}
			if _, ok := allowed[u.Host]; !ok {
				http.Error(w, "origin not allowed", http.StatusForbidden)
				return
			}
		}

		switch r.Method {
		case http.MethodGet:
			if origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
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
