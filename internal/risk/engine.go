package risk

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"arbitron/config"
)

type Side string
const ( SideBuy Side = "BUY"; SideSell Side = "SELL" )
type OrderType string
const ( OrderIOC OrderType = "IOC"; OrderMarket OrderType = "MARKET" )
type Order struct { ID string; Exchange string; Symbol string; Side Side; Type OrderType; Quantity float64; LimitPrice float64 }

type KillSwitchState int32
const ( KillSwitchSafe KillSwitchState = 0; KillSwitchTripped KillSwitchState = 1 )

type Engine struct {
	params config.RiskParams
	killSwitch int32
	mu sync.RWMutex
	dailyPnL float64
	peakPnL float64
	totalFills int64
	reversals int64
	killReason string
	sessionDate string
	sessionStartedAt time.Time
}

func NewEngine(params config.RiskParams) *Engine {
	e := &Engine{params: params}
	e.sessionDate = time.Now().UTC().Format("2006-01-02")
	e.sessionStartedAt = time.Now().UTC()
	return e
}

// ensureSessionLocked establishes a UTC trading-day boundary. It never
// clears a tripped kill switch: a new day resets daily accounting, but
// manual safety intervention remains mandatory after a kill.
func (e *Engine) ensureSessionLocked(now time.Time) {
	day := now.UTC().Format("2006-01-02")
	if e.sessionDate == "" { e.sessionDate = day; e.sessionStartedAt = now.UTC(); return }
	if day != e.sessionDate {
		e.sessionDate = day
		e.sessionStartedAt = now.UTC()
		e.dailyPnL = 0
		e.peakPnL = 0
		e.totalFills = 0
		e.reversals = 0
		log.Printf("[Risk] New UTC trading session: %s", day)
	}
}

func (e *Engine) ValidateOpportunity(leg1, leg2 Order) error {
	if atomic.LoadInt32(&e.killSwitch) == int32(KillSwitchTripped) {
		e.mu.RLock(); reason := e.killReason; e.mu.RUnlock()
		return fmt.Errorf("KILL_SWITCH_TRIPPED: %s", reason)
	}
	e.mu.Lock()
	e.ensureSessionLocked(time.Now().UTC())
	dailyPnL := e.dailyPnL
	peak := e.peakPnL
	e.mu.Unlock()
	if dailyPnL < -e.params.DailyLossLimit {
		e.TripKillSwitch(fmt.Sprintf("DAILY_LOSS_LIMIT: %.2f > %.2f", -dailyPnL, e.params.DailyLossLimit))
		return fmt.Errorf("daily loss limit exceeded: $%.2f", -dailyPnL)
	}
	if peak > 0 {
		drawdown := (peak - dailyPnL) / peak
		if drawdown > e.params.MaxDrawdown {
			e.TripKillSwitch(fmt.Sprintf("MAX_DRAWDOWN: %.2f%% > %.2f%%", drawdown*100, e.params.MaxDrawdown*100))
			return fmt.Errorf("max drawdown exceeded: %.2f%%", drawdown*100)
		}
	}
	budget := time.Duration(e.params.LegTimeoutMs) * time.Millisecond
	if budget < 5*time.Millisecond { return fmt.Errorf("leg timeout budget %v is below minimum viable %v", budget, 5*time.Millisecond) }
	return nil
}

func (e *Engine) RecordCleanExecution(pnl float64) {
	e.mu.Lock(); defer e.mu.Unlock()
	e.ensureSessionLocked(time.Now().UTC())
	e.dailyPnL += pnl; e.totalFills++
	if e.dailyPnL > e.peakPnL { e.peakPnL = e.dailyPnL }
}
func (e *Engine) RecordReversal(slippage float64) {
	e.mu.Lock(); defer e.mu.Unlock()
	e.ensureSessionLocked(time.Now().UTC())
	e.dailyPnL += slippage; e.reversals++
}
func (e *Engine) TripKillSwitch(reason string) {
	if atomic.CompareAndSwapInt32(&e.killSwitch, 0, 1) {
		e.mu.Lock(); e.killReason = reason; e.mu.Unlock()
		log.Printf("[Risk] KILL SWITCH TRIPPED: %s", reason)
	}
}

// ResetKillSwitch requires the SHA-256 digest configured in
// RISK_OPERATOR_TOKEN_SHA256. The raw operator token is never logged or stored.
func (e *Engine) ResetKillSwitch(operatorToken string) error {
	if operatorToken == "" { return fmt.Errorf("operator token required to reset kill switch") }
	configured := e.params.OperatorTokenHash
	if len(configured) != sha256.Size*2 { return fmt.Errorf("kill switch reset unavailable: operator credential is not configured") }
	provided := sha256.Sum256([]byte(operatorToken))
	configuredBytes, err := hex.DecodeString(configured)
	if err != nil || len(configuredBytes) != sha256.Size { return fmt.Errorf("kill switch reset unavailable: invalid operator credential configuration") }
	if subtle.ConstantTimeCompare(provided[:], configuredBytes) != 1 { return fmt.Errorf("invalid operator credential") }
	if !atomic.CompareAndSwapInt32(&e.killSwitch, int32(KillSwitchTripped), int32(KillSwitchSafe)) { return fmt.Errorf("kill switch is not tripped") }
	e.mu.Lock(); e.killReason = ""; e.mu.Unlock()
	log.Printf("[Risk] Kill switch reset by authenticated operator")
	return nil
}

type RiskSnapshot struct {
	KillSwitchState string
	DailyPnL float64
	PeakPnL float64
	Drawdown float64
	DailyLossUsedPct float64
	TotalFills int64
	Reversals int64
	KillReason string
	SessionDate string
	SessionStartedAt time.Time
}
func (e *Engine) Snapshot() RiskSnapshot {
	e.mu.Lock(); defer e.mu.Unlock()
	e.ensureSessionLocked(time.Now().UTC())
	state := "SAFE"; if atomic.LoadInt32(&e.killSwitch) == 1 { state = "TRIPPED" }
	drawdown := 0.0; if e.peakPnL > 0 { drawdown = (e.peakPnL-e.dailyPnL)/e.peakPnL }
	lossUsed := 0.0; if e.params.DailyLossLimit > 0 && e.dailyPnL < 0 { lossUsed = (-e.dailyPnL/e.params.DailyLossLimit)*100 }
	return RiskSnapshot{KillSwitchState:state, DailyPnL:e.dailyPnL, PeakPnL:e.peakPnL, Drawdown:drawdown, DailyLossUsedPct:lossUsed, TotalFills:e.totalFills, Reversals:e.reversals, KillReason:e.killReason, SessionDate:e.sessionDate, SessionStartedAt:e.sessionStartedAt}
}
