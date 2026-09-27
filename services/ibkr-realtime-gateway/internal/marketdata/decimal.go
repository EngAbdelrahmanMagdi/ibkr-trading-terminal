package marketdata

import (
	"errors"
	"strconv"
	"strings"
)

// ErrInvalidDecimal is returned for text that is not a non-negative decimal number.
var ErrInvalidDecimal = errors.New("invalid decimal")

// Decimal is an exact, non-negative decimal number that keeps the scale it was received with: "182.10" stays
// "182.10". It has no fixed scale and no scale limit, never passes through floating point and is never rounded.
// The zero value is not a valid number; use pointers for optional values.
type Decimal struct {
	whole string // decimal digits without leading zeros ("0" for values below one)
	frac  string // fractional digits exactly as received; empty for integers
}

// ParseDecimal parses plain decimal text such as "182.1" or "0.0001". Signs, exponents, separators and
// surrounding whitespace are rejected.
func ParseDecimal(s string) (Decimal, error) {
	whole, frac, hasPoint := strings.Cut(s, ".")
	if !digitsOnly(whole) || (hasPoint && !digitsOnly(frac)) {
		return Decimal{}, ErrInvalidDecimal
	}
	return Decimal{whole: trimLeadingZeros(whole), frac: frac}, nil
}

// ParseDecimalNumber parses the text of a JSON number (json.Number) exactly, including exponent notation such
// as "1.725e2". Negative numbers are rejected.
func ParseDecimalNumber(s string) (Decimal, error) {
	mantissa, exp := s, 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil || e > 1_000 || e < -1_000 {
			return Decimal{}, ErrInvalidDecimal
		}
		mantissa, exp = s[:i], e
	}
	d, err := ParseDecimal(mantissa)
	if err != nil || exp == 0 {
		return d, err
	}
	digits := d.whole + d.frac
	point := len(d.whole) + exp // position of the decimal point within digits
	switch {
	case point <= 0:
		digits = strings.Repeat("0", -point+1) + digits
		point = 1
	case point > len(digits):
		digits += strings.Repeat("0", point-len(digits))
	}
	return Decimal{whole: trimLeadingZeros(digits[:point]), frac: digits[point:]}, nil
}

// DecimalFromPrice converts a micro-unit price into a Decimal with exactly decimals fractional digits (0-6).
// The simulator uses it for prices on its tick grid, where no information is lost.
func DecimalFromPrice(p Price, decimals int) Decimal {
	d, _ := ParseDecimal(p.Format(decimals)) // Format always produces valid decimal text
	return d
}

// String returns the decimal exactly as received (canonical whole part, fractional digits untouched).
func (d Decimal) String() string {
	if d.frac == "" {
		return d.whole
	}
	return d.whole + "." + d.frac
}

// Valid reports whether d holds a parsed number (the zero value does not).
func (d Decimal) Valid() bool { return d.whole != "" }

// Scale returns the number of fractional digits.
func (d Decimal) Scale() int { return len(d.frac) }

// IntegerDigits returns the number of digits before the decimal point.
func (d Decimal) IntegerDigits() int { return len(d.whole) }

func digitsOnly(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func trimLeadingZeros(s string) string {
	t := strings.TrimLeft(s, "0")
	if t == "" {
		return "0"
	}
	return t
}
