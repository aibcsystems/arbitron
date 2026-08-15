// ============================================================
//  cmd/smoketest/main.go
//  Arbitron v4 — Adapter Smoke Test
//
//  Fires ONE IOC order through a real adapter, isolated from the
//  detector/risk/redis pipeline. Purpose: validate that an
//  adapter's auth + request/response parsing actually works
//  against a real (test) endpoint before trusting it in the full
//  system.
//
//  Usage (Binance testnet):
//    export BINANCE_BASE_URL=https://testnet.binancefuture.com
//    export BINANCE_API_KEY=your_testnet_key
//    export BINANCE_API_SECRET=your_testnet_secret
//    go run ./cmd/smoketest -exchange=binance -symbol=BTCUSDT -side=BUY -qty=0.001 -price=50000
//
//  Usage (Alpaca paper):
//    export ALPACA_BASE_URL=https://paper-api.alpaca.markets
//    export ALPACA_API_KEY=your_paper_key
//    export ALPACA_API_SECRET=your_paper_secret
//    go run ./cmd/smoketest -exchange=alpaca -symbol=BTC/USD -side=BUY -qty=0.001 -price=50000
//
//  ⚠ Only run this against testnet/paper endpoints. Nothing in this
//  file stops it from hitting production if you point the base URL
//  there — that's intentional (it's a smoke test, not a safety
//  rail), so double-check your env vars before running.
// ============================================================

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"arbitron/internal/exchange"
	"arbitron/internal/exchange/adapters"
)

func main() {
	exchangeName := flag.String("exchange", "binance", "binance | alpaca")
	symbol := flag.String("symbol", "BTCUSDT", "order symbol")
	side := flag.String("side", "BUY", "BUY or SELL")
	qty := flag.Float64("qty", 0.001, "order quantity")
	price := flag.Float64("price", 50000, "limit price")
	flag.Parse()

	orderSide := exchange.Buy
	if *side == "SELL" {
		orderSide = exchange.Sell
	}

	order := exchange.Order{
		ID:         fmt.Sprintf("smoketest-%d", time.Now().UnixMilli()),
		Symbol:     *symbol,
		Side:       orderSide,
		Quantity:   *qty,
		LimitPrice: *price,
	}

	var adapter exchange.ExchangeAdapter
	switch *exchangeName {
	case "binance":
		adapter = adapters.NewBinanceAdapter(
			os.Getenv("BINANCE_API_KEY"),
			os.Getenv("BINANCE_API_SECRET"),
		)
	case "alpaca":
		adapter = adapters.NewAlpacaAdapter(
			os.Getenv("ALPACA_API_KEY"),
			os.Getenv("ALPACA_API_SECRET"),
		)
	default:
		fmt.Printf("unknown exchange %q — use 'binance' or 'alpaca'\n", *exchangeName)
		os.Exit(1)
	}

	fmt.Printf("→ Submitting IOC order via %s: %+v\n", adapter.Name(), order)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("→ Running health check...")
	if err := adapter.HealthCheck(ctx); err != nil {
		fmt.Printf("✗ Health check failed: %v\n", err)
		fmt.Println("  (Check your API keys and base URL env vars before continuing.)")
		os.Exit(1)
	}
	fmt.Println("✓ Health check passed")

	result := adapter.SubmitIOC(ctx, order)

	fmt.Println()
	fmt.Println("=== RESULT ===")
	fmt.Printf("OrderID:    %s\n", result.OrderID)
	fmt.Printf("Filled:     %v\n", result.Filled)
	fmt.Printf("FilledQty:  %f\n", result.FilledQty)
	fmt.Printf("AvgPrice:   %f\n", result.AvgPrice)
	fmt.Printf("Exchange:   %s\n", result.Exchange)
	fmt.Printf("Latency:    %v\n", result.Latency)
	if result.Error != nil {
		fmt.Printf("Error:      %v\n", result.Error)
		os.Exit(1)
	}
}
