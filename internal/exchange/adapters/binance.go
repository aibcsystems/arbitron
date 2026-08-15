// ============================================================
//  internal/exchange/adapters/binance.go
//  Arbitron v4 — Binance USD-M Futures Adapter
//
//  Implements exchange.ExchangeAdapter for Binance USDM Futures
//  IOC orders via the signed REST API (not the WebSocket feed —
//  that's handled separately by feed_manager.go/websocket.go).
//
//  Auth scheme: HMAC-SHA256 over the query string, key = API
//  secret, sent as the `signature` param. API key goes in the
//  X-MBX-APIKEY header. This matches Binance's documented Futures
//  REST auth (https://binance-docs.github.io/apidocs/futures/en/
//  #signed-trade-user_data-and-margin-endpoints-security).
//
//  ⚠ NOT LIVE-TESTED: written against Binance's public API docs,
//  not verified against a live or testnet endpoint (no network
//  access in the environment this was written in). Before routing
//  real capital, run this against https://testnet.binancefuture.com
//  first — swap baseURL in NewBinanceAdapter to confirm auth,
//  symbol precision, and error-code handling all behave as expected.
// ============================================================

package adapters

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"arbitron/internal/exchange"
)

const binanceBaseURL = "https://fapi.binance.com"

type BinanceAdapter struct {
	apiKey    string
	apiSecret string
	client    *http.Client
	baseURL   string
}

func NewBinanceAdapter(apiKey, apiSecret string) *BinanceAdapter {
	baseURL := binanceBaseURL
	// Testnet override — set BINANCE_BASE_URL=https://testnet.binancefuture.com
	// for paper-trading validation before pointing at production.
	if override := os.Getenv("BINANCE_BASE_URL"); override != "" {
		baseURL = override
	}
	return &BinanceAdapter{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		client:    &http.Client{Timeout: 5 * time.Second},
		baseURL:   baseURL,
	}
}

func (b *BinanceAdapter) Name() string { return "BINANCE_USDM" }

// sign computes the HMAC-SHA256 signature Binance requires on every
// signed endpoint, over the exact query string being sent.
func (b *BinanceAdapter) sign(query string) string {
	mac := hmac.New(sha256.New, []byte(b.apiSecret))
	mac.Write([]byte(query))
	return hex.EncodeToString(mac.Sum(nil))
}

func (b *BinanceAdapter) SubmitIOC(ctx context.Context, order exchange.Order) exchange.FillResult {
	start := time.Now()

	side := "BUY"
	if order.Side == exchange.Sell {
		side = "SELL"
	}

	// Reverse-hedge orders (engine.go's reverseHedge) carry
	// Type: OrderTypeMarket — they must guarantee a fill to flatten
	// unhedged exposure, not sit at a price. Before this branch
	// existed, every order — including these — went out as
	// type=LIMIT with price=order.LimitPrice, whose zero value for
	// a reverse order meant "buy at $0" (never fills, guaranteed
	// KILL_SWITCH) or "sell at $0" (fills, but only by accident of
	// the floor being zero, not because market intent was ever
	// communicated to Binance).
	//
	// Binance USDM futures: a true MARKET order takes no `price`
	// and no `timeInForce` at all — sending either is at best
	// ignored, at worst rejected, so they're omitted entirely below
	// rather than sent as empty/zero values.
	params := url.Values{}
	params.Set("symbol", order.Symbol)
	params.Set("side", side)
	params.Set("quantity", strconv.FormatFloat(order.Quantity, 'f', -1, 64))
	params.Set("newClientOrderId", order.ID)
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", "5000")

	if order.Type == exchange.OrderTypeMarket {
		params.Set("type", "MARKET")
	} else {
		params.Set("type", "LIMIT")
		params.Set("timeInForce", "IOC")
		params.Set("price", strconv.FormatFloat(order.LimitPrice, 'f', -1, 64))
	}

	query := params.Encode()
	query += "&signature=" + b.sign(query)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		b.baseURL+"/fapi/v1/order?"+query, nil)
	if err != nil {
		return failResult(order.ID, "BINANCE_USDM", start, fmt.Errorf("build request: %w", err))
	}
	req.Header.Set("X-MBX-APIKEY", b.apiKey)

	resp, err := b.client.Do(req)
	if err != nil {
		return failResult(order.ID, "BINANCE_USDM", start, fmt.Errorf("http do: %w", err))
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return failResult(order.ID, "BINANCE_USDM", start,
			fmt.Errorf("binance error %d: %s", resp.StatusCode, string(body)))
	}

	var out struct {
		OrderID       int64  `json:"orderId"`
		Status        string `json:"status"`
		ExecutedQty   string `json:"executedQty"`
		AvgPrice      string `json:"avgPrice"`
		ClientOrderID string `json:"clientOrderId"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return failResult(order.ID, "BINANCE_USDM", start, fmt.Errorf("decode response: %w", err))
	}

	filledQty, _ := strconv.ParseFloat(out.ExecutedQty, 64)
	avgPrice, _ := strconv.ParseFloat(out.AvgPrice, 64)

	// IOC semantics: FILLED = fully filled, PARTIALLY_FILLED can still
	// occur on IOC (the unfilled remainder is cancelled by the exchange
	// automatically) — either counts as "filled" if executedQty > 0.
	filled := filledQty > 0 && (out.Status == "FILLED" || out.Status == "PARTIALLY_FILLED")

	return exchange.FillResult{
		OrderID:   order.ID,
		Filled:    filled,
		FilledQty: filledQty,
		AvgPrice:  avgPrice,
		Exchange:  "BINANCE_USDM",
		Latency:   time.Since(start),
	}
}

func (b *BinanceAdapter) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/fapi/v1/ping", nil)
	if err != nil {
		return err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("binance ping returned %d", resp.StatusCode)
	}
	return nil
}

func failResult(orderID, exch string, start time.Time, err error) exchange.FillResult {
	return exchange.FillResult{
		OrderID:  orderID,
		Filled:   false,
		Exchange: exch,
		Latency:  time.Since(start),
		Error:    err,
	}
}
