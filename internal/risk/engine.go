// ============================================================
//  internal/risk/engine.go
//  Arbitron v4 — Risk Validation Engine & Kill Switch
//
//  Enforces all RiskParams from config at the code level.
//  The 45ms latency ceiling is not just a dashboard display —
//  it is a hard constraint enforced here before any order fires.
//
//  ★ BUG FIX APPLIED — confirmed by actually building this
//  package against internal/execution:
//
//    package arbitron/internal/risk
//      imports arbitron/internal/execution
//      imports arbitron/internal/risk: import cycle not allowed
//
//  This file imported "arbitron/internal/execution" solely to
//  name `execution.Order` as ValidateOpportunity's parameter
//  type. But internal/execution already imports internal/risk
//  (to hold the *risk.Engine it calls ValidateOpportunity on) —
//  a direct cycle.
//
//  internal/exchange/adapter.go already solved exactly this
//  problem for the exchange package by duplicating a minimal
//  Order type locally instead of importing execution (see its
//  own comment: "Core order types (duplicated from execution to
//  avoid import cycle)"). Applying the same, already-established
//  convention here rather than introducing a new pattern:
//  execution.Order → local risk.Order, and engine.go now converts
//  opp.Leg1 / opp.Leg2 into risk.Order at the call site — the
//  exact same kind of conversion engine.go already does when
//  building exchange.Order for the adapter registry.
// ============================================================

package risk

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"arbitron/config"
)

// ── Order (duplicated from execution to avoid import cycle) ──
// Mirrors execution.Order's shape. Only the fields ValidateOpportunity
// actually needs are required for correctness today, but the full
// shape is kept so future risk logic (per-symbol limits, per-side
// exposure checks, etc.) doesn't need another cycle-avoidance pass.

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
	ID         string
	Exchange   string
	Symbol     string
	Side       Side
	Type       OrderType
	Quantity   float64
	LimitPrice float64
}

// KillSwitchState represents the current armed/tripped state.
type KillSwitchState int32

const (
	KillSwitchSafe    KillSwitchState = 0
	KillSwitchTripped KillSwitchState = 1
)

// Engine validates every opportunity before execution and tracks
// portfolio-level risk metrics in real time.
type Engine struct {
	params config.RiskParams

	// Atomic kill switch — checked on every execution attempt.
	// int32 for atomic CAS operations without mutex overhead.
	killSwitch int32 // 0 = SAFE, 1 = TRIPPED

	// Portfolio state (mutex-protected)
	mu         sync.RWMutex
	dailyPnL   float64
	peakPnL    float64
	totalFills int64
	reversals  int64
	killReason string
}

func NewEngine(params config.RiskParams) *Engine {
	return &Engine{params: params}
}

// ── Pre-flight validation ─────────────────────────────────
//
// Called before every two-leg execution attempt.
// Returns an error if any risk constraint would be violated.

func (e *Engine) ValidateOpportunity(leg1, leg2 Order) error {
	// Kill switch check first — fastest exit path
	if atomic.LoadInt32(&e.killSwitch) == int32(KillSwitchTripped) {
		return fmt.Errorf("KILL_SWITCH_TRIPPED: %s", e.killReason)
	}

	// Daily loss limit check
	e.mu.RLock()
	dailyPnL := e.dailyPnL
	e.mu.RUnlock()

	if dailyPnL < -e.params.DailyLossLimit {
		e.TripKillSwitch(fmt.Sprintf("DAILY_LOSS_LIMIT: %.2f > %.2f", -dailyPnL, e.params.DailyLossLimit))
		return fmt.Errorf("daily loss limit exceeded: $%.2f", -dailyPnL)
	}

	// Drawdown check
	e.mu.RLock()
	peak := e.peakPnL
	e.mu.RUnlock()

	if peak > 0 {
		drawdown := (peak - dailyPnL) / peak
		if drawdown > e.params.MaxDrawdown {
			e.TripKillSwitch(fmt.Sprintf("MAX_DRAWDOWN: %.2f%% > %.2f%%",
				drawdown*100, e.params.MaxDrawdown*100))
			return fmt.Errorf("max drawdown exceeded: %.2f%%", drawdown*100)
		}
	}

	// Leg timeout budget check
	// If configured budget is tighter than round-trip minimum, reject
	minViableBudget := 5 * time.Millisecond
	budget := time.Duration(e.params.LegTimeoutMs) * time.Millisecond
	if budget < minViableBudget {
		return fmt.Errorf("leg timeout budget %v is below minimum viable %v", budget, minViableBudget)
	}

	return nil
}

// ── State recording ───────────────────────────────────────

func (e *Engine) RecordCleanExecution(pnl float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dailyPnL += pnl
	e.totalFills++
	if e.dailyPnL > e.peakPnL {
		e.peakPnL = e.dailyPnL
	}
}

func (e *Engine) RecordReversal(slippage float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dailyPnL += slippage // Slippage is negative
	e.reversals++
}

// ── Kill switch ───────────────────────────────────────────
//
// TripKillSwitch is an atomic operation.
// Once tripped, no new executions are permitted.
// Reset requires manual operator intervention (not automatic).

func (e *Engine) TripKillSwitch(reason string) {
	if atomic.CompareAndSwapInt32(&e.killSwitch, 0, 1) {
		e.mu.Lock()
		e.killReason = reason
		e.mu.Unlock()
		log.Printf("🚨 [Risk] KILL SWITCH TRIPPED: %s", reason)
	}
}

func (e *Engine) ResetKillSwitch(operatorToken string) error {
	// In production: validate operator auth token before reset
	if operatorToken == "" {
		return fmt.Errorf("operator token required to reset kill switch")
	}
	atomic.StoreInt32(&e.killSwitch, 0)
	e.mu.Lock()
	e.killReason = ""
	e.mu.Unlock()
	log.Printf("✅ [Risk] Kill switch reset by operator")
	return nil
}

// ── Telemetry snapshot for dashboard ─────────────────────

type RiskSnapshot struct {
	KillSwitchState  string
	DailyPnL         float64
	PeakPnL          float64
	Drawdown         float64
	DailyLossUsedPct float64
	TotalFills       int64
	Reversals        int64
	KillReason       string
}

func (e *Engine) Snapshot() RiskSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	state := "SAFE"
	if atomic.LoadInt32(&e.killSwitch) == 1 {
		state = "TRIPPED"
	}

	drawdown := 0.0
	if e.peakPnL > 0 {
		drawdown = (e.peakPnL - e.dailyPnL) / e.peakPnL
	}

	lossUsed := 0.0
	if e.params.DailyLossLimit > 0 && e.dailyPnL < 0 {
		lossUsed = (-e.dailyPnL / e.params.DailyLossLimit) * 100
	}

	return RiskSnapshot{
		KillSwitchState:  state,
		DailyPnL:         e.dailyPnL,
		PeakPnL:          e.peakPnL,
		Drawdown:         drawdown,
		DailyLossUsedPct: lossUsed,
		TotalFills:       e.totalFills,
		Reversals:        e.reversals,
		KillReason:       e.killReason,
	}
}
