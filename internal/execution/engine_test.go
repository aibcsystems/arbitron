// ============================================================
//  internal/execution/engine_test.go
//  Arbitron v4 — Execution Engine Integration Tests
// ============================================================

package execution_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"arbitron/config"
	"arbitron/internal/exchange"
	"arbitron/internal/execution"
	"arbitron/internal/risk"
)

type mockAdapter struct {
	name           string
	shouldFill     bool
	fillPrice      float64
	fillDelay      time.Duration
	reconcileState exchange.OrderState
	reconcileQty   float64
	reconcilePrice float64
	orders         []exchange.Order
	mu             sync.Mutex
}

func (m *mockAdapter) Name() string { return m.name }

func (m *mockAdapter) SubmitIOC(ctx context.Context, order exchange.Order) exchange.FillResult {
	m.mu.Lock()
	m.orders = append(m.orders, order)
	m.mu.Unlock()
	select {
	case <-time.After(m.fillDelay):
	case <-ctx.Done():
		return exchange.FillResult{OrderID: order.ID, Filled: false, Exchange: m.name, Error: ctx.Err()}
	}
	if !m.shouldFill {
		return exchange.FillResult{OrderID: order.ID, Filled: false, Exchange: m.name}
	}
	return exchange.FillResult{OrderID: order.ID, Filled: true, FilledQty: order.Quantity, AvgPrice: m.fillPrice, Exchange: m.name}
}

func (m *mockAdapter) ReconcileOrder(_ context.Context, orderID, symbol string) exchange.OrderStatus {
	state := m.reconcileState
	if state == "" {
		if m.shouldFill { state = exchange.OrderStateFilled } else { state = exchange.OrderStateCancelled }
	}
	qty := m.reconcileQty
	if qty == 0 && state == exchange.OrderStateFilled { qty = 0.01 }
	price := m.reconcilePrice
	if price == 0 { price = m.fillPrice }
	return exchange.OrderStatus{OrderID: orderID, Exchange: m.name, Symbol: symbol, State: state, FilledQty: qty, AvgPrice: price}
}

func (m *mockAdapter) HealthCheck(_ context.Context) error { return nil }

type mockPipeline struct { mu sync.Mutex; events []string; full []execution.ExecutionEvent }
func (mp *mockPipeline) PublishExecution(event interface{}) error {
	if e, ok := event.(execution.ExecutionEvent); ok { mp.mu.Lock(); mp.events = append(mp.events, e.Outcome); mp.full = append(mp.full, e); mp.mu.Unlock() }
	return nil
}
func (mp *mockPipeline) lastOutcome() string { mp.mu.Lock(); defer mp.mu.Unlock(); if len(mp.events) == 0 { return "" }; return mp.events[len(mp.events)-1] }
func (mp *mockPipeline) lastPnL() float64 { mp.mu.Lock(); defer mp.mu.Unlock(); if len(mp.full) == 0 { return 0 }; return mp.full[len(mp.full)-1].PnL }

func testCfg() *config.Config {
	return &config.Config{RiskParams: config.RiskParams{MaxLatencyMS: 45, LegTimeoutMs: 45, DailyLossLimit: 50000, MaxDrawdown: 0.10}, Fees: config.FeeSchedule{BinanceUSDM: 0.0005, KrakenPerp: 0.00075, Default: 0.0010}}
}

func testOpportunity(leg1Exchange, leg2Exchange string) execution.ArbOpportunity {
	return execution.ArbOpportunity{
		ID: "TEST-OPP-001",
		Leg1: execution.Order{ID: "TEST-OPP-001-L1", Exchange: leg1Exchange, Symbol: "BTCUSDT", Side: execution.SideBuy, Type: execution.OrderIOC, Quantity: 0.01, LimitPrice: 42100.0},
		Leg2: execution.Order{ID: "TEST-OPP-001-L2", Exchange: leg2Exchange, Symbol: "BTCUSDT", Side: execution.SideSell, Type: execution.OrderIOC, Quantity: 0.01, LimitPrice: 42120.0},
		SpreadBps: 18.5, DetectedAt: time.Now(),
	}
}

func buildEngine(t *testing.T, pipeline *mockPipeline, adapters ...exchange.ExchangeAdapter) (*execution.Engine, *risk.Engine) {
	t.Helper(); cfg := testCfg(); riskEngine := risk.NewEngine(cfg.RiskParams); registry := exchange.NewAdapterRegistry(adapters...)
	return execution.NewEngine(cfg, riskEngine, pipeline, registry), riskEngine
}

func waitForOutcome(t *testing.T, pipe *mockPipeline, timeout time.Duration) string {
	t.Helper(); deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) { if o := pipe.lastOutcome(); o != "" { return o }; time.Sleep(5 * time.Millisecond) }
	t.Fatal("timed out waiting for execution outcome"); return ""
}

func TestCleanArb(t *testing.T) {
	pipe := &mockPipeline{}
	eng, _ := buildEngine(t, pipe, &mockAdapter{name: "BINANCE_USDM", shouldFill: true, fillPrice: 42100.0, fillDelay: 5 * time.Millisecond}, &mockAdapter{name: "KRAKEN_PERP", shouldFill: true, fillPrice: 42120.0, fillDelay: 5 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel(); go eng.Start(ctx)
	eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, time.Second); outcome != "CLEAN" { t.Errorf("expected CLEAN, got %s", outcome) }
}

func TestLeg1Miss(t *testing.T) {
	pipe := &mockPipeline{}
	eng, _ := buildEngine(t, pipe, &mockAdapter{name: "BINANCE_USDM", shouldFill: false}, &mockAdapter{name: "KRAKEN_PERP", shouldFill: true, fillPrice: 42120.0})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel(); go eng.Start(ctx)
	eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, time.Second); outcome != "MISSED" { t.Errorf("expected MISSED, got %s", outcome) }
}

func TestLeg2MissReverseSuccess(t *testing.T) {
	pipe := &mockPipeline{}
	binance := &mockAdapter{name: "BINANCE_USDM", shouldFill: true, fillPrice: 42100.0, fillDelay: 5 * time.Millisecond}; kraken := &mockAdapter{name: "KRAKEN_PERP", shouldFill: false}
	eng, _ := buildEngine(t, pipe, binance, kraken); ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel(); go eng.Start(ctx)
	eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, time.Second); outcome != "REVERSED" { t.Errorf("expected REVERSED, got %s", outcome) }
}

func TestKillSwitchTrip(t *testing.T) {
	pipe := &mockPipeline{}; countingAdapter := &countAdapter{name: "BINANCE_USDM", responses: []exchange.FillResult{{Filled: true, FilledQty: 0.01, AvgPrice: 42100.0}, {Filled: false}}}; kraken := &mockAdapter{name: "KRAKEN_PERP", shouldFill: false}
	eng, _ := buildEngine(t, pipe, countingAdapter, kraken); ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second); defer cancel(); go eng.Start(ctx)
	eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, 2*time.Second); outcome != "KILL_SWITCH" { t.Errorf("expected KILL_SWITCH, got %s", outcome) }
}

func TestRiskRejected(t *testing.T) {
	pipe := &mockPipeline{}; eng, riskEngine := buildEngine(t, pipe, &mockAdapter{name: "BINANCE_USDM", shouldFill: true, fillPrice: 42100.0}, &mockAdapter{name: "KRAKEN_PERP", shouldFill: true, fillPrice: 42120.0}); riskEngine.TripKillSwitch("TEST_PRE_TRIP")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel(); go eng.Start(ctx); eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, time.Second); outcome != "RISK_REJECTED" { t.Errorf("expected RISK_REJECTED, got %s", outcome) }
}

func TestIOCTimeoutReconcilesCancelled(t *testing.T) {
	pipe := &mockPipeline{}
	eng, _ := buildEngine(t, pipe, &mockAdapter{name: "BINANCE_USDM", shouldFill: true, fillPrice: 42100.0, fillDelay: 100 * time.Millisecond, reconcileState: exchange.OrderStateCancelled}, &mockAdapter{name: "KRAKEN_PERP", shouldFill: true, fillPrice: 42120.0})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second); defer cancel(); go eng.Start(ctx); eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, 3*time.Second); outcome != "MISSED" { t.Errorf("expected MISSED after authoritative cancellation, got %s", outcome) }
}

func TestTimeoutReconcilesFilled(t *testing.T) {
	pipe := &mockPipeline{}
	eng, _ := buildEngine(t, pipe, &mockAdapter{name: "BINANCE_USDM", shouldFill: true, fillPrice: 42100.0, fillDelay: 100 * time.Millisecond, reconcileState: exchange.OrderStateFilled, reconcileQty: 0.01, reconcilePrice: 42100.0}, &mockAdapter{name: "KRAKEN_PERP", shouldFill: true, fillPrice: 42120.0})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second); defer cancel(); go eng.Start(ctx); eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, 3*time.Second); outcome != "CLEAN" { t.Errorf("expected CLEAN from reconciled fill, got %s", outcome) }
}

func TestTimeoutReconcilesPartialAndHedgesResidual(t *testing.T) {
	pipe := &mockPipeline{}; leg1 := &mockAdapter{name: "BINANCE_USDM", shouldFill: true, fillPrice: 42100.0, fillDelay: 100 * time.Millisecond, reconcileState: exchange.OrderStatePartiallyFilled, reconcileQty: 0.006, reconcilePrice: 42100.0}; leg2 := &mockAdapter{name: "KRAKEN_PERP", shouldFill: true, fillPrice: 42120.0}
	eng, _ := buildEngine(t, pipe, leg1, leg2); ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second); defer cancel(); go eng.Start(ctx); eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, 3*time.Second); outcome != "CLEAN" { t.Errorf("expected CLEAN after reconciled partial and matching hedge, got %s", outcome) }
}

func TestUnknownOrderStateKillsAndDoesNotBlindlyReverse(t *testing.T) {
	pipe := &mockPipeline{}; leg1 := &mockAdapter{name: "BINANCE_USDM", shouldFill: true, fillPrice: 42100.0, fillDelay: 5 * time.Millisecond}; leg2 := &noReconcileAdapter{name: "KRAKEN_PERP", shouldFill: true, fillPrice: 42120.0, fillDelay: 100 * time.Millisecond}
	eng, _ := buildEngine(t, pipe, leg1, leg2); ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second); defer cancel(); go eng.Start(ctx); eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, 3*time.Second); outcome != "KILL_SWITCH" { t.Errorf("expected KILL_SWITCH for unresolved leg 2, got %s", outcome) }
	leg1.mu.Lock(); if len(leg1.orders) != 1 { t.Fatalf("expected no blind reverse after unresolved leg 2, got %d Binance orders", len(leg1.orders)) }; leg1.mu.Unlock()
}

type recordingAdapter struct { name string; fillQty float64; fillPrice float64; lastOrder exchange.Order; mu sync.Mutex }
func (a *recordingAdapter) Name() string { return a.name }
func (a *recordingAdapter) SubmitIOC(_ context.Context, order exchange.Order) exchange.FillResult { a.mu.Lock(); a.lastOrder = order; a.mu.Unlock(); qty := a.fillQty; if qty <= 0 { qty = order.Quantity }; return exchange.FillResult{OrderID: order.ID, Filled: true, FilledQty: qty, AvgPrice: a.fillPrice, Exchange: a.name} }
func (a *recordingAdapter) HealthCheck(_ context.Context) error { return nil }
func (a *recordingAdapter) LastOrder() exchange.Order { a.mu.Lock(); defer a.mu.Unlock(); return a.lastOrder }

func TestPartialLeg1UsesFilledQuantityForLeg2(t *testing.T) {
	pipe := &mockPipeline{}; leg1 := &recordingAdapter{name: "BINANCE_USDM", fillQty: 0.004, fillPrice: 42100}; leg2 := &recordingAdapter{name: "KRAKEN_PERP", fillPrice: 42120}; eng, _ := buildEngine(t, pipe, leg1, leg2)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel(); go eng.Start(ctx); eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, time.Second); outcome != "CLEAN" { t.Fatalf("expected CLEAN, got %s", outcome) }; if got := leg2.LastOrder().Quantity; got != 0.004 { t.Fatalf("expected leg 2 quantity 0.004, got %.8f", got) }
}

func TestPartialLeg2ReversesOnlyResidual(t *testing.T) {
	pipe := &mockPipeline{}; binance := &countAdapter{name: "BINANCE_USDM", responses: []exchange.FillResult{{Filled: true, FilledQty: 0.01, AvgPrice: 42100}, {Filled: true, FilledQty: 0.006, AvgPrice: 42090}}}; partialLeg2 := &countAdapter{name: "KRAKEN_PERP", responses: []exchange.FillResult{{Filled: true, FilledQty: 0.004, AvgPrice: 42120}}}; eng, _ := buildEngine(t, pipe, binance, partialLeg2)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel(); go eng.Start(ctx); eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	if outcome := waitForOutcome(t, pipe, time.Second); outcome != "REVERSED" { t.Fatalf("expected REVERSED, got %s", outcome) }; binance.mu.Lock(); if len(binance.orders) != 2 { t.Fatalf("expected 2 Binance orders, got %d", len(binance.orders)) }; if got := binance.orders[1].Quantity; got != 0.006 { t.Fatalf("expected reverse quantity 0.006, got %.8f", got) }; binance.mu.Unlock(); pnl := pipe.lastPnL(); if pnl < -5 || pnl > 5 { t.Fatalf("reverse slippage out of plausible range: got $%.4f", pnl) }
}

type countAdapter struct { name string; responses []exchange.FillResult; mu sync.Mutex; callIdx int; orders []exchange.Order }
func (c *countAdapter) Name() string { return c.name }
func (c *countAdapter) SubmitIOC(_ context.Context, order exchange.Order) exchange.FillResult { c.mu.Lock(); defer c.mu.Unlock(); c.orders = append(c.orders, order); if c.callIdx >= len(c.responses) { return exchange.FillResult{OrderID: order.ID, Filled: false, Exchange: c.name} }; result := c.responses[c.callIdx]; result.OrderID = order.ID; result.Exchange = c.name; c.callIdx++; return result }
func (c *countAdapter) HealthCheck(_ context.Context) error { return nil }

type noReconcileAdapter struct { name string; shouldFill bool; fillPrice float64; fillDelay time.Duration; orders []exchange.Order; mu sync.Mutex }
func (a *noReconcileAdapter) Name() string { return a.name }
func (a *noReconcileAdapter) SubmitIOC(ctx context.Context, order exchange.Order) exchange.FillResult { a.mu.Lock(); a.orders = append(a.orders, order); a.mu.Unlock(); select { case <-time.After(a.fillDelay): case <-ctx.Done(): return exchange.FillResult{OrderID: order.ID, Exchange: a.name, Error: ctx.Err()} }; if !a.shouldFill { return exchange.FillResult{OrderID: order.ID, Exchange: a.name} }; return exchange.FillResult{OrderID: order.ID, Filled: true, FilledQty: order.Quantity, AvgPrice: a.fillPrice, Exchange: a.name} }
func (a *noReconcileAdapter) HealthCheck(_ context.Context) error { return nil }
