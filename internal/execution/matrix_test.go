// ============================================================
//  internal/execution/matrix_test.go
//  Arbitron v4 — Closed-loop execution test matrix, remaining cells
//
//  engine_test.go already covers: clean, leg1 miss, leg1 partial,
//  leg2 miss, leg2 partial, successful reversal, failed reversal
//  (as "kill switch"), and risk rejection (pre-tripped).
//
//  This file adds the three cells that weren't covered:
//    - partial reversal:      reverse fill < residual, but > 0 —
//                              distinct code path from a total
//                              reversal failure (0 fill), same
//                              KILL_SWITCH outcome but worth its
//                              own test since it's the branch most
//                              likely to have an off-by-something
//                              in the "remaining" calculation.
//    - subsequent rejection:  after a kill switch trips on one
//                              opportunity, does a SECOND, otherwise
//                              clean opportunity actually get
//                              rejected by the risk gate — not just
//                              "the risk engine's internal flag is
//                              set", but the full path through
//                              executeTwoLeg() again.
//    - recovery:               after an operator calls
//                              ResetKillSwitch, does the engine
//                              actually resume clean execution, or
//                              does some other piece of state stay
//                              stuck.
//
//  Together with engine_test.go, this closes every cell in the
//  execution-engine test matrix: the engine is now verified not
//  just to work on the happy path, but to be closed under failure.
// ============================================================

package execution_test

import (
	"context"
	"testing"
	"time"

	"arbitron/internal/exchange"
)

// waitForNthEvent blocks until at least n events have been published,
// or fails the test on timeout. Needed for multi-opportunity tests
// where waitForOutcome's "last event" isn't precise enough to
// distinguish "the first opportunity's event" from "the second
// opportunity's event hasn't landed yet".
func waitForNthEvent(t *testing.T, pipe *mockPipeline, n int, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pipe.mu.Lock()
		have := len(pipe.events)
		pipe.mu.Unlock()
		if have >= n {
			pipe.mu.Lock()
			outcome := pipe.events[n-1]
			pipe.mu.Unlock()
			return outcome
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for event #%d (matrix cell)", n)
	return ""
}

// ── Cell: partial reversal ─────────────────────────────────
//
// Leg 2 misses entirely (residual = full leg1 fill = 0.01), and the
// reverse hedge fills only 0.006 of the required 0.01 — a genuine
// partial reversal, not a total failure. Must still KILL_SWITCH
// (engine.go: reverseResult.FilledQty >= quantity-1e-12 is false),
// and must NOT call RecordReversal (that only happens on the success
// branch) — so dailyPnL should be untouched by this failed attempt,
// distinguishing it from a successful reversal's slippage booking.
func TestPartialReversalTripsKillSwitch(t *testing.T) {
	pipe := &mockPipeline{}

	binance := &countAdapter{name: "BINANCE_USDM", responses: []exchange.FillResult{
		{Filled: true, FilledQty: 0.01, AvgPrice: 42100.0},   // Leg 1: full fill
		{Filled: true, FilledQty: 0.006, AvgPrice: 42050.0}, // Reverse: PARTIAL, short of the 0.01 needed
	}}
	kraken := &mockAdapter{name: "KRAKEN_PERP", shouldFill: false} // Leg 2 misses entirely

	eng, riskEngine := buildEngine(t, pipe, binance, kraken)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go eng.Start(ctx)

	eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	outcome := waitForOutcome(t, pipe, 2*time.Second)

	if outcome != "KILL_SWITCH" {
		t.Fatalf("expected KILL_SWITCH on partial reversal, got %s", outcome)
	}

	snap := riskEngine.Snapshot()
	if snap.KillSwitchState != "TRIPPED" {
		t.Fatalf("expected risk engine kill switch TRIPPED, got %s", snap.KillSwitchState)
	}
	if snap.DailyPnL != 0 {
		t.Fatalf("expected dailyPnL untouched by a failed/partial reversal (RecordReversal must not fire), got %.4f", snap.DailyPnL)
	}

	binance.mu.Lock()
	defer binance.mu.Unlock()
	if len(binance.orders) != 2 {
		t.Fatalf("expected exactly 2 orders (leg1 + reverse), got %d", len(binance.orders))
	}
	if got := binance.orders[1].Quantity; got != 0.01 {
		t.Fatalf("expected reverse order to request the full 0.01 residual, got %.8f", got)
	}
}

// ── Cell: subsequent risk rejection ──────────────────────────
//
// Not "the risk engine's flag is set" (that's TestRiskRejected,
// which pre-trips it manually) — this drives a REAL kill switch trip
// through the full executeTwoLeg() path via one opportunity, then
// submits a second, otherwise-clean opportunity through the SAME
// engine/risk instance and confirms it's actually rejected.
func TestSubsequentOpportunityRejectedAfterKillSwitch(t *testing.T) {
	pipe := &mockPipeline{}

	binance := &countAdapter{name: "BINANCE_USDM", responses: []exchange.FillResult{
		{Filled: true, FilledQty: 0.01, AvgPrice: 42100.0}, // opp #1 leg1: fills
		{Filled: false},                                    // opp #1 reverse: fails -> KILL_SWITCH
	}}
	kraken := &mockAdapter{name: "KRAKEN_PERP", shouldFill: false} // opp #1 leg2: misses

	eng, riskEngine := buildEngine(t, pipe, binance, kraken)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go eng.Start(ctx)

	// Opportunity #1: trips the kill switch for real.
	eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	first := waitForNthEvent(t, pipe, 1, 2*time.Second)
	if first != "KILL_SWITCH" {
		t.Fatalf("setup failed: expected opp #1 to KILL_SWITCH, got %s", first)
	}
	if riskEngine.Snapshot().KillSwitchState != "TRIPPED" {
		t.Fatal("setup failed: risk engine not tripped after opp #1")
	}

	// Opportunity #2: adapters would happily fill both legs cleanly —
	// the only thing that should stop it is the now-tripped risk gate.
	opp2 := testOpportunity("BINANCE_USDM", "KRAKEN_PERP")
	opp2.ID = "TEST-OPP-002"
	eng.Submit(opp2)

	second := waitForNthEvent(t, pipe, 2, 2*time.Second)
	if second != "RISK_REJECTED" {
		t.Fatalf("expected opp #2 to be RISK_REJECTED after opp #1's kill switch, got %s", second)
	}
}

// ── Cell: recovery ────────────────────────────────────────────
//
// After a real kill-switch trip (same setup as above), an operator
// calls ResetKillSwitch. A subsequent opportunity should now process
// as a normal CLEAN execution — proving the reset actually clears
// engine-visible state, not just the risk engine's internal flag in
// isolation.
func TestRecoveryAfterOperatorReset(t *testing.T) {
	pipe := &mockPipeline{}

	binance := &countAdapter{name: "BINANCE_USDM", responses: []exchange.FillResult{
		{Filled: true, FilledQty: 0.01, AvgPrice: 42100.0}, // opp #1 leg1: fills
		{Filled: false},                                    // opp #1 reverse: fails -> KILL_SWITCH
		{Filled: true, FilledQty: 0.01, AvgPrice: 42100.0}, // opp #2 leg1 (post-reset): fills
	}}
	kraken := &countAdapter{name: "KRAKEN_PERP", responses: []exchange.FillResult{
		{Filled: false},                                   // opp #1 leg2: misses
		{Filled: true, FilledQty: 0.01, AvgPrice: 42120.0}, // opp #2 leg2 (post-reset): fills
	}}

	eng, riskEngine := buildEngine(t, pipe, binance, kraken)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go eng.Start(ctx)

	// Opportunity #1: trip the kill switch for real.
	eng.Submit(testOpportunity("BINANCE_USDM", "KRAKEN_PERP"))
	first := waitForNthEvent(t, pipe, 1, 2*time.Second)
	if first != "KILL_SWITCH" {
		t.Fatalf("setup failed: expected opp #1 to KILL_SWITCH, got %s", first)
	}

	// Operator recovery.
	if err := riskEngine.ResetKillSwitch("operator-jane"); err != nil {
		t.Fatalf("ResetKillSwitch failed: %v", err)
	}
	if riskEngine.Snapshot().KillSwitchState != "SAFE" {
		t.Fatal("expected risk engine SAFE immediately after reset")
	}

	// Opportunity #2: should now execute cleanly, not get rejected.
	opp2 := testOpportunity("BINANCE_USDM", "KRAKEN_PERP")
	opp2.ID = "TEST-OPP-002"
	eng.Submit(opp2)

	second := waitForNthEvent(t, pipe, 2, 2*time.Second)
	if second != "CLEAN" {
		t.Fatalf("expected opp #2 to execute CLEAN after operator reset, got %s", second)
	}
}
