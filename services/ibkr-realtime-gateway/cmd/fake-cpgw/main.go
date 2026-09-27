// Command fake-cpgw runs a FAKE IBKR Client Portal Gateway for the end-to-end test path (make up-ibkr-fake).
// It serves scripted data only, needs no IBKR account, and must never be pointed at by a real deployment.
//
// At startup it creates a throwaway CA and a server certificate for -hosts, writes the CA certificate to
// -ca-out (a volume shared with the gateway, which trusts only that CA) and serves HTTPS on -addr.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/ibkr/fakecpgw"
)

func main() {
	addr := flag.String("addr", ":5000", "listen address")
	hosts := flag.String("hosts", "fake-cpgw,localhost", "comma-separated certificate host names")
	caOut := flag.String("ca-out", "/run/fake-cpgw/ca.pem", "where to write the CA certificate")
	tick := flag.Duration("tick", 500*time.Millisecond, "quote update interval for subscribed instruments")
	check := flag.Bool("check", false, "health check: verify the running fake answers over TLS, then exit")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "fake-cpgw")
	if *check {
		os.Exit(healthcheck(*caOut, *addr))
	}

	pki, err := fakecpgw.NewPKI(strings.Split(*hosts, ",")...)
	if err != nil {
		log.Error("certificate generation failed", "error", err.Error())
		os.Exit(1)
	}
	if err := os.MkdirAll(filepath.Dir(*caOut), 0o755); err != nil {
		log.Error("CA output directory", "error", err.Error())
		os.Exit(1)
	}
	if err := os.WriteFile(*caOut, pki.CAPEM, 0o644); err != nil { //nolint:gosec // a public CA certificate
		log.Error("CA output", "error", err.Error())
		os.Exit(1)
	}
	fake := fakecpgw.New()
	fake.StartAuto(*tick)
	srv := &http.Server{
		Addr: *addr, Handler: fake.Handler(), ReadHeaderTimeout: 5 * time.Second,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{pki.Server}, MinVersion: tls.VersionTLS12},
	}
	log.Info("FAKE CP Gateway serving scripted test data", "addr", *addr, "hosts", *hosts)
	if err := srv.ListenAndServeTLS("", ""); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// healthcheck calls the fake's status endpoint on localhost, trusting only the CA it wrote.
func healthcheck(caFile, addr string) int {
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return 1
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return 1
	}
	port := addr[strings.LastIndex(addr, ":")+1:]
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	resp, err := client.Post("https://localhost:"+port+"/v1/api/iserver/auth/status", "application/json", strings.NewReader("{}"))
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
