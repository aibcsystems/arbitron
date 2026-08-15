// ============================================================
//  cmd/pipelinetest/main.go
//  Arbitron v4 — Full Pipeline Closed-Loop Test
//
//  WHAT THIS PROVES: that Detector → Risk Engine → Execution
//  Engine → real ExchangeAdapter is wired correctly end-to-end,
//  with a real order actually reaching Alpaca. Nothing before
//  this point has tested that chain as one system — only pieces
//  of it (unit tests, one manual CLI order via cmd/smoketest).
//
//  WHAT THIS IS NOT: real arbitrage. True spatial arbitrage needs
//  two independently-priced live venues. We have exactly one
//  authenticated adapter (Alpaca). So this harness feeds:
//    - "ALPACA_SPOT"     — real live BTC/USD quotes, polled from
//                          Alpaca's market data API
//    - "SYNTHETIC_VENUE" — the SAME real quote, with an artificial
//                          price offset applied on a timer, purely
//                          to give the detector a second "venue" to
//                          compare against and occasionally cross
//                          the minimum spread threshold
//  SYNTHETIC_VENUE orders never leave this process — they're
//  filled instantly in-memory by a mock adapter, clearly logged as
//  such. Only the Alpaca leg of any detected opportunity touches a
//  real API. This is a controlled test of the plumbing, not a
//  trading strategy.
//
//  ⚠ Requires no Redis, no Postgres — this harness passes nil for
//  the StreamPipeline's client/copier since PublishTick/
//  SubscribeTicks are pure in-memory pub/sub and never touch
//  either dependency. Do not reuse this pattern for
//  PublishTelemetry/RunConsumer, which do need a real client.
//
//  Usage:
//    export ALPACA_BASE_URL=https://paper-api.alpaca.markets
//    export ALPACA_API_KEY=...
//    export ALPACA_API_SECRET=...
//    export RISK_LEG_TIMEOUT_MS=5000   # see note below on why
//    go run ./cmd/pipelinetest
//
//  ⚠ FINDING SURFACED BY THIS HARNESS, not hidden: config.go's
//  default RiskParams.LegTimeoutMs is 45ms — calibrated for
//  institutional low-latency venues with sub-100ms fill
//  confirmation. Alpaca's REST + poll-until-terminal pattern
//  (see alpaca.go's pollUntilTerminal) took ~1.1s in the earlier
//  smoke test. At the 45ms default, every real Alpaca order in
//  the full pipeline would hit engine.go's leg-timeout context
//  and be recorded as a timeout, never as a real fill. This isn't
//  a bug in either file individually — it's a mismatch between an
//  HFT-calibrated risk budget and a REST-polling venue. Either
//  Alpaca needs a per-adapter timeout override in engine.go
//  (reasonable P1 follow-up), or Alpaca simply isn't a fit for
//  this architecture's tight-timing assumptions long-term. Setting
//  RISK_LEG_TIMEOUT_MS=5000 here works around it for this test —
//  it does not fix the underlying architectural question.
// ============================================================

package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"arbitron/config"
	"arbitron/internal/exchange"
	"arbitron/internal/exchange/adapters"
	"arbitron/internal/execution"
	redispkg "arbitron/internal/redis"
	"arbitron/internal/risk"
)

// Synthetic venue is provided by adapters.SyntheticAdapter. It is strictly
// in-memory and never sends orders to a network venue.

// ── Alpaca live quote polling ─────────────────────────────

type alpacaQuoteResp struct {
	Quotes map[string]struct {
		BidPrice float64 `json:"bp"`
		AskPrice float64 `json:"ap"`
	} `json:"quotes"`
}

func fetchAlpacaQuote(ctx context.Context, symbol string) (bid, ask float64, err error) {
	// Alpaca's crypto market data is publicly accessible — no API key
	// needed (unlike the trading endpoints, which do require auth).
	// Sending unnecessary auth headers here was causing a raw 401
	// from the edge before the request even reached Alpaca's app layer.
	url := "https://data.alpaca.markets/v1beta3/crypto/us/latest/quotes?symbols=" + symbol
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, 0, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("alpaca quote error %d: %s", resp.StatusCode, string(body))
	}

	var out alpacaQuoteResp
	if err := json.Unmarshal(body, &out); err != nil {
		return 0, 0, fmt.Errorf("decode quote: %w", err)
	}
	q, ok := out.Quotes[symbol]
	if !ok {
		return 0, 0, fmt.Errorf("no quote for %s in response", symbol)
	}
	return q.BidPrice, q.AskPrice, nil
}

func publishTick(pipeline *redispkg.StreamPipeline, exch, symbol string, bid, ask float64) {
	raw, _ := json.Marshal(redispkg.PriceTick{
		Symbol: symbol,
		Bid:    bid,
		Ask:    ask,
	})
	pipeline.PublishTick(exch, raw)
}

func main() {
	symbol := "BTC/USD"

	apiKey := os.Getenv("ALPACA_API_KEY")
	apiSecret := os.Getenv("ALPACA_API_SECRET")
	if apiKey == "" || apiSecret == "" {
		log.Fatal("ALPACA_API_KEY and ALPACA_API_SECRET must be set")
	}

	cfg := config.Load()

	// Pure in-memory pipeline — nil client/copier are safe here
	// because PublishTick/SubscribeTicks never touch either.
	pipeline := redispkg.NewStreamPipeline(nil, cfg, nil)

	riskEngine := risk.NewEngine(cfg.RiskParams)

	alpacaAdapter := adapters.NewAlpacaAdapter(apiKey, apiSecret)
	syntheticAdapter := adapters.NewSyntheticAdapter(adapters.SyntheticFillFull, 2*time.Millisecond, 0.5)
	registry := exchange.NewAdapterRegistry(alpacaAdapter, syntheticAdapter)

	execEngine := execution.NewEngine(cfg, riskEngine, pipeline, registry)
	detector := execution.NewDetector(cfg, execEngine)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go execEngine.Start(ctx)
	go detector.Start(ctx, pipeline)

	log.Println("[PipelineTest] Wiring live — polling real Alpaca quotes for", symbol)
	log.Println("[PipelineTest] SYNTHETIC_VENUE offset cycles between 0bps (quiet) and +50bps (should trigger detection) every 10s")
	log.Println("[PipelineTest] Press Ctrl+C to stop")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	cycleStart := time.Now()
	for {
		select {
		case <-ctx.Done():
			log.Println("[PipelineTest] Shutting down.")
			return
		case <-ticker.C:
			qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			bid, ask, err := fetchAlpacaQuote(qctx, symbol)
			cancel()
			if err != nil {
				log.Printf("[PipelineTest] quote fetch error: %v", err)
				continue
			}

			publishTick(pipeline, "ALPACA_SPOT", symbol, bid, ask)

			// Alternate the synthetic offset every 10s: 0bps for a quiet
			// window (proves the detector correctly stays silent when
			// there's no real spread), then +50bps for a window that
			// should reliably cross the 15bps default threshold and
			// trigger a real detection → risk check → dual-leg execution.
			elapsed := time.Since(cycleStart)
			offsetBps := 0.0
			if int(elapsed.Seconds())%20 >= 10 {
				offsetBps = 50.0
			}
			mid := (bid + ask) / 2
			offset := mid * offsetBps / 10000.0
			synBid := bid + offset
			synAsk := ask + offset
			// keep spread realistic instead of collapsing to zero
			if synAsk-synBid < math.Abs(ask-bid) {
				synAsk = synBid + (ask - bid)
			}

			publishTick(pipeline, "SYNTHETIC_VENUE", symbol, synBid, synAsk)

			log.Printf("[PipelineTest] tick  ALPACA bid=%.2f ask=%.2f  |  SYNTHETIC(+%.0fbps) bid=%.2f ask=%.2f",
				bid, ask, offsetBps, synBid, synAsk)
		}
	}
}
