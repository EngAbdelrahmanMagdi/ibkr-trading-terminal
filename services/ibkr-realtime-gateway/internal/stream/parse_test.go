package stream

import (
	"errors"
	"testing"
)

func TestParseClientMessageAcceptsValidMessages(t *testing.T) {
	for _, in := range []string{
		`{"type":"subscribe","symbols":["NVDA","AAPL","BRK.B"]}`,
		`{"type":"unsubscribe","symbols":["META"]}`,
	} {
		if _, err := ParseClientMessage([]byte(in)); err != nil {
			t.Errorf("%s: %v", in, err)
		}
	}
}

func TestParseClientMessageRejectsInvalidMessages(t *testing.T) {
	cases := map[string]string{
		"not json":           `nope`,
		"unknown type":       `{"type":"heartbeat","symbols":["NVDA"]}`,
		"missing symbols":    `{"type":"subscribe"}`,
		"empty symbols":      `{"type":"subscribe","symbols":[]}`,
		"lowercase symbol":   `{"type":"subscribe","symbols":["nvda"]}`,
		"too long symbol":    `{"type":"subscribe","symbols":["ABCDEFGHIJKLM"]}`,
		"duplicate symbols":  `{"type":"subscribe","symbols":["NVDA","NVDA"]}`,
		"unknown field":      `{"type":"subscribe","symbols":["NVDA"],"extra":1}`,
		"trailing data":      `{"type":"subscribe","symbols":["NVDA"]} {}`,
		"symbols not string": `{"type":"subscribe","symbols":[1]}`,
	}
	for name, in := range cases {
		if _, err := ParseClientMessage([]byte(in)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: expected ErrInvalid, got %v", name, err)
		}
	}
}
