package ibkr

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/EngAbdelrahmanMagdi/ibkr-trading-terminal/services/ibkr-realtime-gateway/internal/marketdata"
)

// SupportedBars is the conservative set IBKR can serve in one request each: at most 1,000 data points per
// request and the documented period/bar step sizes, regular trading hours only, no request chunking.
var SupportedBars = marketdata.BarSet{
	marketdata.Interval1m:  {marketdata.Range1d},
	marketdata.Interval5m:  {marketdata.Range1d, marketdata.Range5d},
	marketdata.Interval15m: {marketdata.Range5d},
	marketdata.Interval1h:  {marketdata.Range5d, marketdata.Range1mo},
	marketdata.Interval1d:  {marketdata.Range1mo, marketdata.Range3mo, marketdata.Range1y},
}

// History request parameters: bar widths (units S, min, h, d, w, m) and periods (units min, h, d, w, m, y;
// "m" is months).
var (
	barParam = map[marketdata.Interval]string{
		marketdata.Interval1m: "1min", marketdata.Interval5m: "5min", marketdata.Interval15m: "15min",
		marketdata.Interval1h: "1h", marketdata.Interval1d: "1d",
	}
	periodParam = map[marketdata.Range]string{
		marketdata.Range1d: "1d", marketdata.Range5d: "5d", marketdata.Range1mo: "1m",
		marketdata.Range3mo: "3m", marketdata.Range1y: "1y",
	}
)

// historyResponse is GET /iserver/marketdata/history. Numbers stay json.Number (exact text).
type historyResponse struct {
	VolumeFactor json.Number `json:"volumeFactor"`
	Data         []struct {
		O json.Number `json:"o"`
		H json.Number `json:"h"`
		L json.Number `json:"l"`
		C json.Number `json:"c"`
		V json.Number `json:"v"`
		T json.Number `json:"t"`
	} `json:"data"`
}

// history fetches and maps bars. Bars whose prices are malformed are left out, never guessed.
func (c *Client) history(ctx context.Context, conid int64, interval marketdata.Interval, rng marketdata.Range) ([]marketdata.Bar, int, error) {
	q := url.Values{
		"conid":      {strconv.FormatInt(conid, 10)},
		"period":     {periodParam[rng]},
		"bar":        {barParam[interval]},
		"outsideRth": {"false"},
	}
	var resp historyResponse
	if err := c.request(ctx, http.MethodGet, "/iserver/marketdata/history", q, nil, epHistory, false, &resp); err != nil {
		return nil, 0, err
	}
	factor := int64(1)
	if f, err := resp.VolumeFactor.Int64(); err == nil && f > 1 {
		factor = f
	}
	bars := make([]marketdata.Bar, 0, len(resp.Data))
	malformed := 0
	for _, d := range resp.Data {
		o, e1 := marketdata.ParseDecimalNumber(d.O.String())
		h, e2 := marketdata.ParseDecimalNumber(d.H.String())
		l, e3 := marketdata.ParseDecimalNumber(d.L.String())
		cl, e4 := marketdata.ParseDecimalNumber(d.C.String())
		ms, e5 := d.T.Int64()
		vol, ok := scaledVolume(d.V.String(), factor)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || !ok {
			malformed++
			continue
		}
		bars = append(bars, marketdata.Bar{Time: time.UnixMilli(ms).UTC(), Open: o, High: h, Low: l, Close: cl, Volume: vol})
	}
	return bars, malformed, nil
}

// scaledVolume multiplies the reported volume by the volume factor exactly and keeps the whole shares
// (volume is an integer count in the contract). The factor semantics are modeled from the documentation
// and remain to be confirmed against a Paper session.
func scaledVolume(text string, factor int64) (int64, bool) {
	if text == "" {
		return 0, true
	}
	d, err := marketdata.ParseDecimalNumber(text)
	if err != nil {
		return 0, false
	}
	unscaled, ok := new(big.Int).SetString(d.String()[:d.IntegerDigits()]+fracDigits(d), 10)
	if !ok {
		return 0, false
	}
	unscaled.Mul(unscaled, big.NewInt(factor))
	unscaled.Quo(unscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.Scale())), nil))
	if !unscaled.IsInt64() || unscaled.Int64() > marketdata.MaxSafeInteger {
		return 0, false
	}
	return unscaled.Int64(), true
}

func fracDigits(d marketdata.Decimal) string {
	s := d.String()
	if d.Scale() == 0 {
		return ""
	}
	return s[len(s)-d.Scale():]
}
