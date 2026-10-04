// Package kafkatls supplies one service identity to all gateway Kafka clients.
package kafkatls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func Option() (kgo.Opt, error) {
	ca, cert, key := os.Getenv("KAFKA_TLS_CA_FILE"), os.Getenv("KAFKA_TLS_CERT_FILE"), os.Getenv("KAFKA_TLS_KEY_FILE")
	if ca == "" && cert == "" && key == "" {
		return kgo.DialTimeout(5 * time.Second), nil
	}
	if ca == "" || cert == "" || key == "" {
		return nil, errors.New("kafka TLS identity is incomplete")
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, errors.New("kafka TLS trust is unreadable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("kafka TLS trust is invalid")
	}
	identity, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, errors.New("kafka TLS identity is invalid")
	}
	return kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{identity}}), nil
}
