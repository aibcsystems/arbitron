package risk

import (
	"context"
	"fmt"
	"time"
)

// StateStore provides durable risk-state persistence. Implementations must
// make Save atomic from the engine's perspective: a successful Save means the
// snapshot is safe to use as the recovery point after a process restart.
type StateStore interface {
	Load(ctx context.Context) (RiskState, error)
	Save(ctx context.Context, state RiskState) error
}

type RiskState struct {
	KillSwitchState  KillSwitchState
	DailyPnL         float64
	PeakPnL          float64
	TotalFills       int64
	Reversals        int64
	KillReason       string
	SessionDate      string
	SessionStartedAt time.Time
}

// Restore applies durable state before execution is permitted. A persisted
// kill switch is intentionally authoritative and cannot be cleared by restart.
func (e *Engine) Restore(ctx context.Context, store StateStore) error {
	if store == nil {
		return fmt.Errorf("risk state store is nil")
	}
	state, err := store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load risk state: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sessionDate = state.SessionDate
	e.sessionStartedAt = state.SessionStartedAt
	e.dailyPnL = state.DailyPnL
	e.peakPnL = state.PeakPnL
	e.totalFills = state.TotalFills
	e.reversals = state.Reversals
	e.killReason = state.KillReason
	e.killSwitch = int32(state.KillSwitchState)
	return nil
}

func (e *Engine) Persist(ctx context.Context, store StateStore) error {
	if store == nil {
		return fmt.Errorf("risk state store is nil")
	}
	e.mu.RLock()
	state := RiskState{
		KillSwitchState: KillSwitchState(e.killSwitch),
		DailyPnL: e.dailyPnL, PeakPnL: e.peakPnL,
		TotalFills: e.totalFills, Reversals: e.reversals,
		KillReason: e.killReason, SessionDate: e.sessionDate,
		SessionStartedAt: e.sessionStartedAt,
	}
	e.mu.RUnlock()
	if err := store.Save(ctx, state); err != nil {
		return fmt.Errorf("save risk state: %w", err)
	}
	return nil
}
