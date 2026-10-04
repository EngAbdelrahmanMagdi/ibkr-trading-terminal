package httpapi

import (
	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/admission"
	"net"
	"net/http"
	"time"
)

func Admission(next http.Handler, barsPerMinute, upgradesPerMinute int) http.Handler {
	limiter := admission.New(4096, 10*time.Minute)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		if r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		policy, rate, burst := "bars", float64(barsPerMinute)/60, 20
		if r.URL.Path == "/ws" {
			policy, rate, burst = "upgrade", float64(upgradesPerMinute)/60, 10
		}
		address, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			address = r.RemoteAddr
		}
		ok, full := limiter.Admit(address, policy, rate, burst, time.Now())
		if !ok {
			cid := correlationID(r)
			w.Header().Set(CorrelationHeader, cid)
			if full {
				writeProblem(w, 503, CategoryServiceUnavailable, "service-unavailable", "Service unavailable", "request admission is temporarily unavailable", cid)
			} else {
				w.Header().Set("Retry-After", "2")
				writeProblem(w, 429, CategoryRateLimited, "rate-limited", "Too many requests", "request rate policy exceeded", cid)
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}
