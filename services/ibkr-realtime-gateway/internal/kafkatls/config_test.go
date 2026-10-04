package kafkatls

import "testing"

func TestPartialIdentityFailsClosed(t *testing.T) {
	for _, name := range []string{"KAFKA_TLS_CA_FILE", "KAFKA_TLS_CERT_FILE", "KAFKA_TLS_KEY_FILE"} {
		t.Setenv(name, "")
	}
	if _, err := Option(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFKA_TLS_CA_FILE", "missing")
	if _, err := Option(); err == nil {
		t.Fatal("partial identity accepted")
	}
	t.Setenv("KAFKA_TLS_CERT_FILE", "missing")
	t.Setenv("KAFKA_TLS_KEY_FILE", "missing")
	if _, err := Option(); err == nil {
		t.Fatal("unreadable trust accepted")
	}
}
