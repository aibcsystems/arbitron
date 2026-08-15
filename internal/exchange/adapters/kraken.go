// ============================================================
//  internal/exchange/adapters/kraken.go
//  Arbitron v4 — Kraken Futures (Perpetual) Adapter
//
//  Implements exchange.ExchangeAdapter for Kraken Futures IOC
//  orders via the signed REST API.
//
//  Auth scheme (per Kraken Futures API docs):
//    1. digest   = SHA256(postData + nonce + endpointPath)
//    2. secret   = base64-decode(apiSecret)
//    3. Authent  = base64( HMAC-SHA512(secret, digest) )
//  endpointPath is the path WITHOUT the "/derivatives" prefix,
//  e.g. "/api/v3/sendorder". Sent as headers: APIKey, Nonce, Authent.
//
//  ⚠ NOT LIVE-TESTED: written against Kraken's published Futures
//  API signing spec, not verified against a live or demo endpoint
//  (no network access in the environment this was written in).
//  Before routing real capital, run this against
//  https://demo-futures.kraken.com first — Kraken's signature
//  scheme is easy to get subtly wrong (path format, nonce
//  monotonicity), so confirm auth succeeds on demo before mainnet.
// ============================================================

package adapters

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"arbitron/internal/exchange"
)

const krakenBaseURL = "https://futures.kraken.com"

type KrakenAdapter struct {
	apiKey    string
	apiSecret string // base64-encoded, as issued by Kraken
	client    *http.Client
	baseURL   string
}

func NewKrakenAdapter(apiKey, apiSecret string) *KrakenAdapter {
	return &KrakenAdapter{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		client:    &http.Client{Timeout: 5 * time.Second},
		baseURL:   krakenBaseURL,
	}
}

func (k *KrakenAdapter) Name() string { return "KRAKEN_PERP" }

// sign implements Kraken Futures' documented Authent computation.
func (k *KrakenAdapter) sign(endpointPath, postData, nonce string) (string, error) {
	secretDecoded, err := base64.StdEncoding.DecodeString(k.apiSecret)
	if err != nil {
		return "", fmt.Errorf("decode api secret: %w", err)
	}

	sha := sha256.New()
	sha.Write([]byte(postData + nonce + endpointPath))
	digest := sha.Sum(nil)

	mac := hmac.New(sha512.New, secretDecoded)
	mac.Write(digest)

	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (k *KrakenAdapter) SubmitIOC(ctx context.Context, order exchange.Order) exchange.FillResult {
	start := time.Now()

	side := "buy"
	if order.Side == exchange.Sell {
		side = "sell"
	}

	form := url.Values{}
	form.Set("orderType", "ioc")
	form.Set("symbol", order.Symbol)
	form.Set("side", side)
	form.Set("size", strconv.FormatFloat(order.Quantity, 'f', -1, 64))
	form.Set("limitPrice", strconv.FormatFloat(order.LimitPrice, 'f', -1, 64))
	form.Set("cliOrdId", order.ID)
	postData := form.Encode()

	const endpointPath = "/api/v3/sendorder"
	nonce := strconv.FormatInt(time.Now().UnixMilli(), 10)

	signature, err := k.sign(endpointPath, postData, nonce)
	if err != nil {
		return failResult(order.ID, "KRAKEN_PERP", start, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		k.baseURL+"/derivatives"+endpointPath, strings.NewReader(postData))
	if err != nil {
		return failResult(order.ID, "KRAKEN_PERP", start, fmt.Errorf("build request: %w", err))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("APIKey", k.apiKey)
	req.Header.Set("Nonce", nonce)
	req.Header.Set("Authent", signature)

	resp, err := k.client.Do(req)
	if err != nil {
		return failResult(order.ID, "KRAKEN_PERP", start, fmt.Errorf("http do: %w", err))
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return failResult(order.ID, "KRAKEN_PERP", start,
			fmt.Errorf("kraken error %d: %s", resp.StatusCode, string(body)))
	}

	var out struct {
		Result     string `json:"result"`
		SendStatus struct {
			OrderID     string `json:"order_id"`
			Status      string `json:"status"`
			OrderEvents []struct {
				Type   string  `json:"type"`
				Amount float64 `json:"amount"`
				Price  float64 `json:"price"`
			} `json:"orderEvents"`
		} `json:"sendStatus"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return failResult(order.ID, "KRAKEN_PERP", start, fmt.Errorf("decode response: %w", err))
	}
	if out.Result != "success" {
		return failResult(order.ID, "KRAKEN_PERP", start,
			fmt.Errorf("kraken rejected order: %s", out.Error))
	}

	// Sum executed fills across orderEvents to get true avg fill price —
	// IOC orders can partially fill across multiple price levels.
	var totalQty, totalNotional float64
	for _, ev := range out.SendStatus.OrderEvents {
		if ev.Type == "EXECUTION" {
			totalQty += ev.Amount
			totalNotional += ev.Amount * ev.Price
		}
	}

	filled := totalQty > 0
	avgPrice := 0.0
	if filled {
		avgPrice = totalNotional / totalQty
	}

	return exchange.FillResult{
		OrderID:   order.ID,
		Filled:    filled,
		FilledQty: totalQty,
		AvgPrice:  avgPrice,
		Exchange:  "KRAKEN_PERP",
		Latency:   time.Since(start),
	}
}

func (k *KrakenAdapter) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		k.baseURL+"/derivatives/api/v3/instruments", nil)
	if err != nil {
		return err
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kraken instruments check returned %d", resp.StatusCode)
	}
	return nil
}
