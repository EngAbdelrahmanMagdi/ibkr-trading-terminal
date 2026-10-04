package origin

import "testing"

func TestExactOrigins(t *testing.T) {
	allowed := []string{"localhost:3000", "https://terminal.example"}
	for _, value := range []string{"", "http://localhost:3000", "https://terminal.example"} {
		if !Allowed(value, allowed) {
			t.Fatalf("allowed origin rejected: %s", value)
		}
	}
	for _, value := range []string{"null", "https://localhost:3000", "http://localhost:8090", "https://terminal.example/", "https://user@terminal.example", "https://terminal.example.evil"} {
		if Allowed(value, allowed) {
			t.Fatalf("unconfigured origin accepted: %s", value)
		}
	}
}
