// ============================================================
//  internal/execution/engine.go — P0 PATCH
//  Arbitron v4 — Execution Engine with Real Adapter Registry
//
//  Change from stub version:
//    - submitToExchange(ctx, order) stub → registry.SubmitIOC()
//    - Fee schedule reads from cfg.Fees (config-driven)
//    - Engine constructor accepts AdapterRegistry
//
//  ★ THREE BUGS FIXED IN THIS FILE, ALL CONFIRMED BY ACTUALLY
//  BUILDING THE CODE (not inferred from reading):
//
//  1. executeTwoLeg() called `e.risk.ValidateOpportunity()` with
//     no arguments. risk.Engine.ValidateOpportunity needs the two
//     legs. Fixed to pass them — see #2 for why they need converting.
//
//  2. risk.Engine.ValidateOpportunity takes risk.Order, not
//     execution.Order — because internal/risk can't import
//     internal/execution (that's a direct import cycle; confirmed
//     by actually building the two packages together, see
//     internal/risk/engine.go's header comment for the exact
//     compiler error). So opp.Leg1 / opp.Leg2 are converted to
//     risk.Order here before the call, exactly the same way this
//     file already converts an Order to exchange.Order in
//     submitIOC() below.
//
//  3. NewEngine's pipeline parameter was `p *redis.StreamPipeline`
//     — a concrete type. engine_test.go's buildEngine() passes a
//     *mockPipeline (a distinct local test type) into this exact
//     parameter. Go does not allow that for concrete types — only
//     for interfaces (confirmed with a minimal reproduction: passing
//     a *Mock where a *Real is expected fails with "cannot use m
//     (variable of type *Mock) as *Real value in argument to
//     NewThing"). Fixed by introducing a small Pipeline interface
//     that both *redis.StreamPipeline and the test's *mockPipeline
//     already satisfy — zero changes needed to either of those.
// ============================================================

package execution

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"arbitron/config"
	"arbitron/internal/exchange"
	"arbitron/internal/risk"
)

// ── Types unchanged from prior iteration ─────────────────

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type OrderType string

const (
	OrderIOC    OrderType = "IOC"
	OrderMarket OrderType = "MARKET"
)

type Order struct {
	ID          string
	Exchange    string
	Symbol      string
	Side        Side
	Type        OrderType
	Quantity    float64
	LimitPrice  float64
	SubmittedAt time.Time
}

type FillResult struct {
	OrderID   string
	Filled    bool
	FilledQty float64
	AvgPrice  float64
	Exchange  string
	Latency   time.Duration
	Error     error
}

type ArbOpportunity struct {
	ID         string
	Leg1       Order
	Leg2       Order
	SpreadBps  float64
	DetectedAt time.Time
}

type ExecutionEvent struct {
	OpportunityID string
	State         string
	Leg1Result    FillResult
	Leg2Result    FillResult
	Outcome       string
	PnL           float64
	TotalLatency  time.Duration
	Timestamp     time.Time
}

// ── Pipeline interface — fix #3 ───────────────────────────
// *redis.StreamPipeline already has this exact method, so it
// satisfies this interface with no changes. engine_test.go's
// *mockPipeline does too — that's what makes the test buildable.
type Pipeline interface {
	PublishExecution(event interface{}) error
}

// ── Engine ────────────────────────────────────────────────

type Engine struct {
	cfg      *config.Config
	risk     *risk.Engine
	pipeline Pipeline                  // was *redis.StreamPipeline — see fix #3 above
	registry *exchange.AdapterRegistry // Real adapters — stub replaced
	oppCh    chan ArbOpportunity
	mu       sync.Mutex
	dailyPnL float64

	// inFlight guards against a second opportunity on the same symbol
	// launching while a prior one is still resolving. Necessary once
	// any venue in play has real-world latency measured in hundreds
	// of ms to seconds (e.g. Alpaca's REST+poll fill confirmation) —
	// without this, two opportunities on BTC/USD 1s apart both start
	// executing concurrently, and a late-arriving reverse-hedge result
	// from the first can get misread as a failure on the second,
	// tripping the kill switch on a false positive (observed in
	// pipelinetest: two overlapping BTC/USD opps → spurious KILL_SWITCH).
	inFlight   map[string]bool
	inFlightMu sync.Mutex
}

// NewEngine accepts any Pipeline (fix #3) and the real AdapterRegistry.
// Wire: execution.NewEngine(cfg, riskEngine, pipeline, registry)
func NewEngine(
	cfg *config.Config,
	r *risk.Engine,
	p Pipeline,
	registry *exchange.AdapterRegistry,
) *Engine {
	return &Engine{
		cfg:      cfg,
		risk:     r,
		pipeline: p,
		registry: registry,
		oppCh:    make(chan ArbOpportunity, 256),
		inFlight: make(map[string]bool),
	}
}

func (e *Engine) Start(ctx context.Context) {
	log.Println("[Execution] Engine started — waiting for arb opportunities...")
	for {
		select {
		case <-ctx.Done():
			log.Println("[Execution] Engine shutting down.")
			return
		case opp := <-e.oppCh:
			go e.executeTwoLeg(ctx, opp)
		}
	}
}

func (e *Engine) Submit(opp ArbOpportunity) {
	select {
	case e.oppCh <- opp:
	default:
		log.Printf("[Execution] ⚠ Opportunity channel full — dropping %s", opp.ID)
	}
}

// ── toRiskOrder — fix #2: convert execution.Order → risk.Order ───
// Same pattern already used below for exchange.Order in submitIOC.
func toRiskOrder(o Order) risk.Order {
	return risk.Order{
		ID:         o.ID,
		Exchange:   o.Exchange,
		Symbol:     o.Symbol,
		Side:       risk.Side(o.Side),
		Type:       risk.OrderType(o.Type),
		Quantity:   o.Quantity,
		LimitPrice: o.LimitPrice,
	}
}

// ── Core two-leg execution loop ───────────────────────────

func (e *Engine) executeTwoLeg(ctx context.Context, opp ArbOpportunity) {
	// In-flight guard: skip if this symbol already has an opportunity
	// resolving. Prevents concurrent executions on the same symbol from
	// racing each other's reverse-hedge / kill-switch logic. Released
	// via defer once this opportunity fully resolves (clean, reversed,
	// or kill-switched) — not on a fixed timer, since venue latency
	// varies too widely for a fixed cooldown to be safe (see struct
	// comment on inFlight).
	e.inFlightMu.Lock()
	if e.inFlight[opp.Leg1.Symbol] {
		e.inFlightMu.Unlock()
		log.Printf("[Execution] ⏭ Skipping %s — %s already has an opportunity in flight",
			opp.ID, opp.Leg1.Symbol)
		return
	}
	e.inFlight[opp.Leg1.Symbol] = true
	e.inFlightMu.Unlock()

	defer func() {
		e.inFlightMu.Lock()
		delete(e.inFlight, opp.Leg1.Symbol)
		e.inFlightMu.Unlock()
	}()

	start := time.Now()
	event := ExecutionEvent{
		OpportunityID: opp.ID,
		State:         "CREATED",
		Timestamp:     start,
	}

	// Fix #1 + #2: pass both legs, converted to risk.Order.
	if err := e.risk.ValidateOpportunity(toRiskOrder(opp.Leg1), toRiskOrder(opp.Leg2)); err != nil {
		log.Printf("[Execution] Risk check failed for %s: %v", opp.ID, err)
		event.Outcome = "RISK_REJECTED"
		e.emitEvent(event)
		return
	}

	log.Printf("[Execution] ⚡ Executing opp %s | Spread: %.2fbps | %s→%s",
		opp.ID, opp.SpreadBps, opp.Leg1.Exchange, opp.Leg2.Exchange)

	// Per-venue timeout — a global 45ms budget times out real Alpaca
	// fills (~1.3s observed round-trip) while being appropriate for
	// WS-native venues. See config.RiskParams.LegTimeout.
	event.State = "LEG1_PENDING"
	leg1Timeout := e.cfg.RiskParams.LegTimeout(opp.Leg1.Exchange)
	leg1Result := e.submitIOC(ctx, opp.Leg1, leg1Timeout)
	event.Leg1Result = leg1Result

	if !leg1Result.Filled || leg1Result.FilledQty <= 0 {
		log.Printf("[Execution] Leg 1 miss on %s (latency: %v, err: %v)",
			opp.ID, leg1Result.Latency, leg1Result.Error)
		event.Outcome = "MISSED"
		e.emitEvent(event)
		return
	}

	event.State = "LEG1_FILLED"
	log.Printf("[Execution] ✅ Leg 1 filled: %s %.4f @ %.4f (latency: %v)",
		leg1Result.Exchange, leg1Result.FilledQty, leg1Result.AvgPrice, leg1Result.Latency)

	elapsed := time.Since(start)
	// Total budget derived from both legs' real per-venue timeouts,
	// not the flat MaxLatencyMS default. A fixed 45ms total budget is
	// structurally impossible to complete once either leg is on a
	// REST+poll venue like Alpaca (Leg 1 alone can take 600ms-1.6s) —
	// every trade would fill Leg 1 then immediately blow the total
	// budget and reverse, regardless of how the reverse itself performs.
	// +200ms buffer covers risk-check + dispatch overhead between legs.
	totalBudget := e.cfg.RiskParams.LegTimeout(opp.Leg1.Exchange) +
		e.cfg.RiskParams.LegTimeout(opp.Leg2.Exchange) +
		200*time.Millisecond
	remainingBudget := totalBudget - elapsed

	if remainingBudget <= 0 {
		log.Printf("[Execution] ⚠ Budget exhausted after Leg 1 (%v) — reversing", elapsed)
		e.reverseHedge(ctx, opp, leg1Result, leg1Result.FilledQty, &event)
		e.emitEvent(event)
		return
	}

	// Never submit more on leg 2 than leg 1 actually filled. A partial
	// leg-1 fill creates a smaller exposure, and the hedge must match that
	// exact quantity rather than the original requested quantity.
	leg2Order := opp.Leg2
	leg2Order.Quantity = leg1Result.FilledQty
	event.State = "LEG2_PENDING"
	leg2Result := e.submitIOC(ctx, leg2Order, remainingBudget)
	event.Leg2Result = leg2Result

	if leg2Result.Filled && leg2Result.FilledQty >= leg2Order.Quantity-1e-12 {
		pnl := e.calculatePnL(leg1Result, leg2Result)
		event.State = "COMPLETED"
		event.Outcome = "CLEAN"
		event.PnL = pnl
		event.TotalLatency = time.Since(start)

		e.mu.Lock()
		e.dailyPnL += pnl
		e.mu.Unlock()

		e.risk.RecordCleanExecution(pnl)

		log.Printf("[Execution] ✅✅ Clean arb: %s | Net PnL (fee-adjusted): $%.4f | %v",
			opp.ID, pnl, event.TotalLatency)
	} else {
		// Any leg-2 partial fill reduces, but does not eliminate, the
		// original leg-1 exposure. Reverse only the residual quantity.
		residual := leg1Result.FilledQty - leg2Result.FilledQty
		if residual < 0 {
			residual = 0
		}
		log.Printf("[Execution] ❌ Leg 2 incomplete on %s — residual exposure %.8f. Reversing...", opp.ID, residual)
		e.reverseHedge(ctx, opp, leg1Result, residual, &event)
	}

	e.emitEvent(event)
}

// ── reverseHedge ──────────────────────────────────────────

func (e *Engine) reverseHedge(
	ctx context.Context,
	opp ArbOpportunity,
	leg1Result FillResult,
	quantity float64,
	event *ExecutionEvent,
) {
	reverseSide := SideSell
	if opp.Leg1.Side == SideSell {
		reverseSide = SideBuy
	}

	reverseOrder := Order{
		ID:       fmt.Sprintf("%s-REVERSE", opp.ID),
		Exchange: opp.Leg1.Exchange,
		Symbol:   opp.Leg1.Symbol,
		Side:     reverseSide,
		Type:     OrderMarket,
		Quantity: quantity,
	}

	log.Printf("[Execution] 🔄 Reverse hedge: %s %s %.4f on %s",
		reverseOrder.Side, reverseOrder.Symbol, reverseOrder.Quantity, reverseOrder.Exchange)

	// Was hardcoded to 200ms — nowhere near enough for a REST+poll
	// venue like Alpaca (observed ~1.3s for real fill confirmation).
	// A hedge-flattening order is the LAST thing that should time out
	// prematurely, since a real fill just arriving late still looks
	// identical to "reverse failed" and trips the kill switch either
	// way. Use the same per-venue timeout as Leg 1 — the reverse
	// order is placed on opp.Leg1.Exchange, so look up that venue.
	reverseTimeout := e.cfg.RiskParams.LegTimeout(reverseOrder.Exchange)
	reverseResult := e.submitIOC(ctx, reverseOrder, reverseTimeout)

	if reverseResult.Filled && reverseResult.FilledQty >= quantity-1e-12 {
		event.State = "REVERSED"
		pnl := e.calculateReverseSlippage(leg1Result, reverseResult)
		event.Outcome = "REVERSED"
		event.PnL = pnl

		e.mu.Lock()
		e.dailyPnL += pnl
		e.mu.Unlock()

		e.risk.RecordReversal(pnl)
		log.Printf("[Execution] ✅ Reverse filled — exposure closed. Slippage: $%.4f", pnl)
	} else {
		event.State = "UNHEDGED_EXPOSURE"
		event.Outcome = "KILL_SWITCH"
		remaining := quantity - reverseResult.FilledQty
		if remaining < 0 {
			remaining = 0
		}
		log.Printf("[Execution] 🚨 KILL SWITCH — Reverse incomplete on %s. Remaining exposure: %.8f. Manual intervention required.", opp.ID, remaining)
		e.risk.TripKillSwitch(fmt.Sprintf("UNHEDGED_EXPOSURE: opp=%s leg1_fill=%.4f",
			opp.ID, leg1Result.FilledQty))
	}
}

// ── submitIOC — wired to real AdapterRegistry ─────────────

func (e *Engine) submitIOC(ctx context.Context, order Order, timeout time.Duration) FillResult {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resultCh := make(chan FillResult, 1)
	submitted := time.Now()

	go func() {
		exOrder := exchange.Order{
			ID:         order.ID,
			Symbol:     order.Symbol,
			Side:       exchange.Side(order.Side),
			Type:       exchange.OrderType(order.Type),
			Quantity:   order.Quantity,
			LimitPrice: order.LimitPrice,
		}
		exResult := e.registry.SubmitIOC(timeoutCtx, order.Exchange, exOrder)

		result := FillResult{
			OrderID:   exResult.OrderID,
			Filled:    exResult.Filled,
			FilledQty: exResult.FilledQty,
			AvgPrice:  exResult.AvgPrice,
			Exchange:  exResult.Exchange,
			Latency:   time.Since(submitted),
			Error:     exResult.Error,
		}

		select {
		case resultCh <- result:
		case <-ctx.Done():
		}
	}()

	select {
	case result := <-resultCh:
		return result
	case <-timeoutCtx.Done():
		return FillResult{
			OrderID:  order.ID,
			Filled:   false,
			Exchange: order.Exchange,
			Latency:  timeout,
			Error:    fmt.Errorf("IOC timeout after %v: %w", timeout, timeoutCtx.Err()),
		}
	case <-ctx.Done():
		return FillResult{
			OrderID:  order.ID,
			Filled:   false,
			Exchange: order.Exchange,
			Error:    fmt.Errorf("context cancelled: %w", ctx.Err()),
		}
	}
}

func (e *Engine) emitEvent(event ExecutionEvent) {
	if err := e.pipeline.PublishExecution(event); err != nil {
		log.Printf("[Execution] ⚠ Failed to emit execution event: %v", err)
	}
}

// ── Fee schedule — config-driven (P2 audit fix applied) ──

func (e *Engine) getTakerFeeRate(exch string) float64 {
	fees := e.cfg.Fees
	switch exch {
	case "BINANCE_USDM":
		return fees.BinanceUSDM
	case "KRAKEN_PERP":
		return fees.KrakenPerp
	case "ALPACA_SPOT":
		return fees.AlpacaSpot
	case "FIX_DIRECT":
		return fees.FIXDirect
	default:
		return fees.Default
	}
}

func (e *Engine) calculatePnL(leg1, leg2 FillResult) float64 {
	sellGross := leg2.AvgPrice * leg2.FilledQty
	buyGross := leg1.AvgPrice * leg1.FilledQty
	sellFee := sellGross * e.getTakerFeeRate(leg2.Exchange)
	buyFee := buyGross * e.getTakerFeeRate(leg1.Exchange)
	return (sellGross - sellFee) - (buyGross + buyFee)
}

func (e *Engine) calculateReverseSlippage(leg1, reverse FillResult) float64 {
	// Cost basis must be scaled to the quantity actually being reversed,
	// not leg1's total filled quantity. When leg2 partially fills, only
	// the residual (leg1.FilledQty - leg2.FilledQty) gets reversed here —
	// pricing the full leg1 position against a partial liquidation
	// produces a phantom PnL swing (e.g. costing 0.01 BTC against a
	// 0.006 BTC sale). reverse.FilledQty is the reliable quantity anchor
	// since submitIOC/reverseHedge already guarantee it's <= the residual
	// requested.
	costGross := leg1.AvgPrice * reverse.FilledQty
	costFee := costGross * e.getTakerFeeRate(leg1.Exchange)
	liqGross := reverse.AvgPrice * reverse.FilledQty
	liqFee := liqGross * e.getTakerFeeRate(reverse.Exchange)
	return (liqGross - liqFee) - (costGross + costFee)
}
