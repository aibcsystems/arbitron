// ============================================================
//  internal/exchange/adapters/alpaca.go
//  Arbitron v4 — Alpaca Crypto Spot Adapter
//
//  Implements exchange.ExchangeAdapter for Alpaca's crypto spot
//  IOC orders via the signed REST API.
//
//  Auth scheme: simple API-key headers (no HMAC signing), per
//  Alpaca's documented trading API auth
//  (https://docs.alpaca.markets/docs/trading-api).
//
//  Alpaca's order-submit endpoint returns immediately with the
//  order in a non-terminal state (e.g. "accepted" or "pending_new")
//  — the actual fill happens asynchronously. This adapter polls
//  GET /v2/orders/{id} until a terminal status or the caller's
//  context deadline, whichever comes first. This mirrors how
//  submitIOC's timeout budget in engine.go already caps latency —
//  the poll simply respects the same ctx.
//
//  ⚠ NOT LIVE-TESTED: written against Alpaca's published REST docs,
//  not verified against a live or paper endpoint (no network access
//  in the environment this was written in). Before routing real
//  capital, point baseURL at https://paper-api.alpaca.markets first.
// ============================================================

package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"arbitron/internal/exchange"
)

const alpacaBaseURL = "https://api.alpaca.markets"

type AlpacaAdapter struct {
	apiKey    string
	apiSecret string
	client    *http.Client
	baseURL   string
}

func NewAlpacaAdapter(apiKey, apiSecret string) *AlpacaAdapter {
	baseURL := alpacaBaseURL
	// Paper-trading override — set ALPACA_BASE_URL=https://paper-api.alpaca.markets
	// for validation before pointing at the live-money endpoint.
	if override := os.Getenv("ALPACA_BASE_URL"); override != "" {
		baseURL = override
	}
	return &AlpacaAdapter{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		client:    &http.Client{Timeout: 5 * time.Second},
		baseURL:   baseURL,
	}
}

func (a *AlpacaAdapter) Name() string { return "ALPACA_SPOT" }

func (a *AlpacaAdapter) authHeaders(req *http.Request) {
	req.Header.Set("APCA-API-KEY-ID", a.apiKey)
	req.Header.Set("APCA-API-SECRET-KEY", a.apiSecret)
	req.Header.Set("Content-Type", "application/json")
}

type alpacaOrderResp struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	FilledQty     string `json:"filled_qty"`
	FilledAvgPrice string `json:"filled_avg_price"`
}

func (a *AlpacaAdapter) SubmitIOC(ctx context.Context, order exchange.Order) exchange.FillResult {
	start := time.Now()

	side := "buy"
	if order.Side == exchange.Sell {
		side = "sell"
	}

	// Reverse-hedge orders (engine.go's reverseHedge) are MARKET —
	// no limit_price at all, since the whole point is guaranteed
	// flat, not price-protected. Everything else stays IOC+limit.
	// Before this branch existed, a MARKET order's unset LimitPrice
	// (zero value) got sent as "limit_price": "0.00" and Alpaca
	// rejected it outright — every reversal failed for this reason,
	// not because of a real timeout.
	orderType := "limit"
	timeInForce := "ioc"
	if order.Type == exchange.OrderTypeMarket {
		orderType = "market"
	}

	payload := map[string]string{
		"symbol":          order.Symbol,
		"qty":             strconv.FormatFloat(order.Quantity, 'f', 8, 64),
		"side":            side,
		"type":            orderType,
		"time_in_force":   timeInForce,
		"client_order_id": order.ID,
	}
	if orderType == "limit" {
		payload["limit_price"] = strconv.FormatFloat(order.LimitPrice, 'f', 2, 64)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return failResult(order.ID, "ALPACA_SPOT", start, fmt.Errorf("encode payload: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v2/orders", bytes.NewReader(body))
	if err != nil {
		return failResult(order.ID, "ALPACA_SPOT", start, fmt.Errorf("build request: %w", err))
	}
	a.authHeaders(req)

	resp, err := a.client.Do(req)
	if err != nil {
		return failResult(order.ID, "ALPACA_SPOT", start, fmt.Errorf("http do: %w", err))
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return failResult(order.ID, "ALPACA_SPOT", start,
			fmt.Errorf("alpaca error %d: %s", resp.StatusCode, string(respBody)))
	}

	var placed alpacaOrderResp
	if err := json.Unmarshal(respBody, &placed); err != nil {
		return failResult(order.ID, "ALPACA_SPOT", start, fmt.Errorf("decode response: %w", err))
	}

	final, err := a.pollUntilTerminal(ctx, placed.ID)
	if err != nil {
		return failResult(order.ID, "ALPACA_SPOT", start, err)
	}

	filledQty, _ := strconv.ParseFloat(final.FilledQty, 64)
	avgPrice, _ := strconv.ParseFloat(final.FilledAvgPrice, 64)
	filled := filledQty > 0 && (final.Status == "filled" || final.Status == "partially_filled")

	result := exchange.FillResult{
		OrderID:   order.ID,
		Filled:    filled,
		FilledQty: filledQty,
		AvgPrice:  avgPrice,
		Exchange:  "ALPACA_SPOT",
		Latency:   time.Since(start),
	}
	// Previously silent when Filled=false — the caller only ever saw
	// "err: <nil>" with no way to tell "IOC didn't cross the market"
	// (status=canceled, working as intended) apart from "something's
	// actually wrong" (status=rejected, a real problem). Surface the
	// real terminal status so that distinction is visible in logs.
	if !filled {
		result.Error = fmt.Errorf("order not filled: final status=%s", final.Status)
	}
	return result
}

// pollUntilTerminal polls GET /v2/orders/{id} until the order reaches
// a terminal status (filled, partially_filled+expired i.e. "expired"
// for the IOC-cancelled remainder, canceled, or rejected), or ctx
// is done — whichever comes first. Poll interval is short (25ms)
// since IOC orders resolve near-instantly; this is not a long-poll.
func (a *AlpacaAdapter) pollUntilTerminal(ctx context.Context, orderID string) (*alpacaOrderResp, error) {
	terminal := map[string]bool{
		"filled": true, "partially_filled": true, "canceled": true,
		"expired": true, "rejected": true, "done_for_day": true,
	}

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("poll cancelled: %w", ctx.Err())
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, http.MethodGet,
				a.baseURL+"/v2/orders/"+orderID, nil)
			if err != nil {
				return nil, err
			}
			a.authHeaders(req)

			resp, err := a.client.Do(req)
			if err != nil {
				return nil, err
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var out alpacaOrderResp
			if err := json.Unmarshal(body, &out); err != nil {
				return nil, fmt.Errorf("decode poll response: %w", err)
			}
			if terminal[out.Status] {
				return &out, nil
			}
			// else: still resolving, loop again until ctx deadline
		}
	}
}

func (a *AlpacaAdapter) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL+"/v2/clock", nil)
	if err != nil {
		return err
	}
	a.authHeaders(req)
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("alpaca clock check returned %d", resp.StatusCode)
	}
	return nil
}
