// ============================================================
//  internal/execution/engine.go — P0 PATCH
//  Arbitron v4 — Execution Engine with Real Adapter Registry
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

// Resolution records whether an order result is directly confirmed by the
// submit path or had to be resolved from authoritative exchange state.
// UNKNOWN is fail-closed: callers must never infer that the order did not fill.
type Resolution string

const (
	ResolutionDirect     Resolution = "DIRECT"
	ResolutionReconciled Resolution = "RECONCILED"
	ResolutionUnknown   Resolution = "UNKNOWN"
)

type FillResult struct {
	OrderID    string
	Filled     bool
	FilledQty  float64
	AvgPrice   float64
	Exchange   string
	Latency    time.Duration
	Resolution Resolution
	Error      error
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

type Pipeline interface {
	PublishExecution(event interface{}) error
}

type Engine struct {
	cfg      *config.Config
	risk     *risk.Engine
	pipeline Pipeline
	registry *exchange.AdapterRegistry
	oppCh    chan ArbOpportunity
	mu       sync.Mutex
	dailyPnL float64

	inFlight   map[string]bool
	inFlightMu sync.Mutex
}

// Reconciliation is deliberately bounded. If a venue cannot authoritatively
// resolve an ambiguous order inside this window, Arbitron stops rather than
// guessing. The unresolved order may have filled after the local timeout.
const reconciliationTimeout = 2 * time.Second

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

func (e *Engine) executeTwoLeg(ctx context.Context, opp ArbOpportunity) {
	e.inFlightMu.Lock()
	if e.inFlight[opp.Leg1.Symbol] {
		e.inFlightMu.Unlock()
		log.Printf("[Execution] ⏭ Skipping %s — %s already has an opportunity in flight", opp.ID, opp.Leg1.Symbol)
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
	event := ExecutionEvent{OpportunityID: opp.ID, State: "CREATED", Timestamp: start}

	if err := e.risk.ValidateOpportunity(toRiskOrder(opp.Leg1), toRiskOrder(opp.Leg2)); err != nil {
		log.Printf("[Execution] Risk check failed for %s: %v", opp.ID, err)
		event.Outcome = "RISK_REJECTED"
		e.emitEvent(event)
		return
	}

	log.Printf("[Execution] ⚡ Executing opp %s | Spread: %.2fbps | %s→%s", opp.ID, opp.SpreadBps, opp.Leg1.Exchange, opp.Leg2.Exchange)

	event.State = "LEG1_PENDING"
	leg1Timeout := e.cfg.RiskParams.LegTimeout(opp.Leg1.Exchange)
	leg1Result := e.submitIOC(ctx, opp.Leg1, leg1Timeout)
	event.Leg1Result = leg1Result

	if leg1Result.Resolution == ResolutionUnknown {
		e.abortUnknownOrder(&event, opp, "leg1", leg1Result)
		e.emitEvent(event)
		return
	}
	if !leg1Result.Filled || leg1Result.FilledQty <= 0 {
		log.Printf("[Execution] Leg 1 miss on %s (latency: %v, err: %v)", opp.ID, leg1Result.Latency, leg1Result.Error)
		event.Outcome = "MISSED"
		e.emitEvent(event)
		return
	}

	event.State = "LEG1_FILLED"
	log.Printf("[Execution] ✅ Leg 1 filled: %s %.4f @ %.4f (latency: %v, resolution: %s)", leg1Result.Exchange, leg1Result.FilledQty, leg1Result.AvgPrice, leg1Result.Latency, leg1Result.Resolution)

	elapsed := time.Since(start)
	totalBudget := e.cfg.RiskParams.LegTimeout(opp.Leg1.Exchange) + e.cfg.RiskParams.LegTimeout(opp.Leg2.Exchange) + 200*time.Millisecond
	remainingBudget := totalBudget - elapsed

	if remainingBudget <= 0 {
		log.Printf("[Execution] ⚠ Budget exhausted after Leg 1 (%v) — reversing", elapsed)
		e.reverseHedge(ctx, opp, leg1Result, leg1Result.FilledQty, &event)
		e.emitEvent(event)
		return
	}

	leg2Order := opp.Leg2
	leg2Order.Quantity = leg1Result.FilledQty
	event.State = "LEG2_PENDING"
	leg2Result := e.submitIOC(ctx, leg2Order, remainingBudget)
	event.Leg2Result = leg2Result

	if leg2Result.Resolution == ResolutionUnknown {
		// Do NOT reverse an unresolved leg-2 order. It may have filled after
		// the local timeout; blindly reversing leg 1 could create a new,
		// over-hedged position. Stop the strategy and require reconciliation.
		e.abortUnknownOrder(&event, opp, "leg2", leg2Result)
		e.emitEvent(event)
		return
	}

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

		log.Printf("[Execution] ✅✅ Clean arb: %s | Net PnL (fee-adjusted): $%.4f | %v", opp.ID, pnl, event.TotalLatency)
	} else {
		residual := leg1Result.FilledQty - leg2Result.FilledQty
		if residual < 0 {
			residual = 0
		}
		if residual <= 1e-12 {
			// Defensive guard against a venue reporting a slightly-short fill.
			// No residual exposure remains, so do not manufacture a reverse.
			event.State = "COMPLETED"
			event.Outcome = "CLEAN"
			event.PnL = e.calculatePnL(leg1Result, leg2Result)
			event.TotalLatency = time.Since(start)
		} else {
			log.Printf("[Execution] ❌ Leg 2 incomplete on %s — residual exposure %.8f. Reversing...", opp.ID, residual)
			e.reverseHedge(ctx, opp, leg1Result, residual, &event)
		}
	}

	e.emitEvent(event)
}

// abortUnknownOrder is the fail-closed terminal path for an order whose
// exchange-side state could not be established. No automatic hedge is sent.
func (e *Engine) abortUnknownOrder(event *ExecutionEvent, opp ArbOpportunity, leg string, result FillResult) {
	event.State = "UNKNOWN_ORDER_STATE"
	event.Outcome = "KILL_SWITCH"
	log.Printf("[Execution] 🚨 UNKNOWN ORDER STATE — opp=%s leg=%s order=%s exchange=%s. No automatic reversal; manual reconciliation required. err=%v", opp.ID, leg, result.OrderID, result.Exchange, result.Error)
	e.risk.TripKillSwitch(fmt.Sprintf("UNKNOWN_ORDER_STATE: opp=%s leg=%s order=%s exchange=%s", opp.ID, leg, result.OrderID, result.Exchange))
}

func (e *Engine) reverseHedge(ctx context.Context, opp ArbOpportunity, leg1Result FillResult, quantity float64, event *ExecutionEvent) {
	if quantity <= 1e-12 {
		event.State = "COMPLETED"
		event.Outcome = "CLEAN"
		return
	}

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

	log.Printf("[Execution] 🔄 Reverse hedge: %s %s %.4f on %s", reverseOrder.Side, reverseOrder.Symbol, reverseOrder.Quantity, reverseOrder.Exchange)
	reverseTimeout := e.cfg.RiskParams.LegTimeout(reverseOrder.Exchange)
	reverseResult := e.submitIOC(ctx, reverseOrder, reverseTimeout)

	if reverseResult.Resolution == ResolutionUnknown {
		event.State = "UNKNOWN_ORDER_STATE"
		event.Outcome = "KILL_SWITCH"
		log.Printf("[Execution] 🚨 KILL SWITCH — Reverse order state unknown on %s. Manual intervention required.", opp.ID)
		e.risk.TripKillSwitch(fmt.Sprintf("UNKNOWN_ORDER_STATE: opp=%s leg=reverse order=%s exchange=%s", opp.ID, reverseResult.OrderID, reverseResult.Exchange))
		return
	}

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
		e.risk.TripKillSwitch(fmt.Sprintf("UNHEDGED_EXPOSURE: opp=%s leg1_fill=%.4f", opp.ID, leg1Result.FilledQty))
	}
}

// submitIOC never treats a local timeout as proof of non-fill. It first asks
// the venue for authoritative state. Only a terminal exchange response can
// produce a normal missed/partial/filled decision. Unresolved state fails closed.
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
			OrderID:    exResult.OrderID,
			Filled:     exResult.Filled,
			FilledQty:  exResult.FilledQty,
			AvgPrice:   exResult.AvgPrice,
			Exchange:   exResult.Exchange,
			Latency:    time.Since(submitted),
			Resolution: ResolutionDirect,
			Error:      exResult.Error,
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
		if ctx.Err() != nil {
			return FillResult{OrderID: order.ID, Exchange: order.Exchange, Latency: time.Since(submitted), Resolution: ResolutionUnknown, Error: fmt.Errorf("context cancelled before order state resolved: %w", ctx.Err())}
		}
		return e.reconcileTimedOutOrder(ctx, order, submitted, timeout)
	case <-ctx.Done():
		return FillResult{OrderID: order.ID, Exchange: order.Exchange, Latency: time.Since(submitted), Resolution: ResolutionUnknown, Error: fmt.Errorf("context cancelled: %w", ctx.Err())}
	}
}

func (e *Engine) reconcileTimedOutOrder(parent context.Context, order Order, submitted time.Time, submitTimeout time.Duration) FillResult {
	reconcileCtx, cancel := context.WithTimeout(parent, reconciliationTimeout)
	defer cancel()

	status := e.registry.ReconcileOrder(reconcileCtx, order.Exchange, order.ID, order.Symbol)
	latency := time.Since(submitted)

	switch status.State {
	case exchange.OrderStateFilled:
		return FillResult{OrderID: order.ID, Filled: true, FilledQty: status.FilledQty, AvgPrice: status.AvgPrice, Exchange: order.Exchange, Latency: latency, Resolution: ResolutionReconciled, Error: status.Error}
	case exchange.OrderStatePartiallyFilled:
		return FillResult{OrderID: order.ID, Filled: status.FilledQty > 0, FilledQty: status.FilledQty, AvgPrice: status.AvgPrice, Exchange: order.Exchange, Latency: latency, Resolution: ResolutionReconciled, Error: status.Error}
	case exchange.OrderStateCancelled, exchange.OrderStateRejected, exchange.OrderStateNotFound:
		return FillResult{OrderID: order.ID, Filled: false, FilledQty: status.FilledQty, AvgPrice: status.AvgPrice, Exchange: order.Exchange, Latency: latency, Resolution: ResolutionReconciled, Error: status.Error}
	default:
		err := status.Error
		if err == nil {
			err = fmt.Errorf("exchange order state unresolved after %v reconciliation window (submit timeout %v)", reconciliationTimeout, submitTimeout)
		}
		return FillResult{OrderID: order.ID, Filled: status.FilledQty > 0, FilledQty: status.FilledQty, AvgPrice: status.AvgPrice, Exchange: order.Exchange, Latency: latency, Resolution: ResolutionUnknown, Error: err}
	}
}

func (e *Engine) emitEvent(event ExecutionEvent) {
	if err := e.pipeline.PublishExecution(event); err != nil {
		log.Printf("[Execution] ⚠ Failed to emit execution event: %v", err)
	}
}

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
	costGross := leg1.AvgPrice * reverse.FilledQty
	costFee := costGross * e.getTakerFeeRate(leg1.Exchange)
	liqGross := reverse.AvgPrice * reverse.FilledQty
	liqFee := liqGross * e.getTakerFeeRate(reverse.Exchange)
	return (liqGross - liqFee) - (costGross + costFee)
}
