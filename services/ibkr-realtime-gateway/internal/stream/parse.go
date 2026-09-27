package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

// symbolPattern matches the Symbol primitive of the contracts.
var symbolPattern = regexp.MustCompile(`^[A-Z][A-Z0-9.-]{0,11}$`)

// ValidSymbol reports whether s is a syntactically valid symbol.
func ValidSymbol(s string) bool { return symbolPattern.MatchString(s) }

// ClientMessage is a parsed subscribe or unsubscribe message.
type ClientMessage struct {
	Type    string   `json:"type"`
	Symbols []string `json:"symbols"`
}

// ErrInvalid is returned for messages that do not follow the client message schemas.
var ErrInvalid = errors.New("invalid client message")

// ParseClientMessage strictly parses a client message: known type, no unknown fields, at least one valid
// and unique symbol, and no trailing data. Limits (message size, symbol counts) are enforced by the caller.
func ParseClientMessage(data []byte) (ClientMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var msg ClientMessage
	if err := dec.Decode(&msg); err != nil {
		return ClientMessage{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if dec.More() {
		return ClientMessage{}, fmt.Errorf("%w: trailing data", ErrInvalid)
	}
	if msg.Type != TypeSubscribe && msg.Type != TypeUnsubscribe {
		return ClientMessage{}, fmt.Errorf("%w: unsupported type %q", ErrInvalid, msg.Type)
	}
	if len(msg.Symbols) == 0 {
		return ClientMessage{}, fmt.Errorf("%w: symbols must not be empty", ErrInvalid)
	}
	seen := make(map[string]struct{}, len(msg.Symbols))
	for _, s := range msg.Symbols {
		if !ValidSymbol(s) {
			return ClientMessage{}, fmt.Errorf("%w: invalid symbol %q", ErrInvalid, s)
		}
		if _, dup := seen[s]; dup {
			return ClientMessage{}, fmt.Errorf("%w: duplicate symbol %q", ErrInvalid, s)
		}
		seen[s] = struct{}{}
	}
	return msg, nil
}
