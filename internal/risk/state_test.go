package risk

import (
	"context"
	"testing"
	"time"

	"arbitron/config"
)

type memoryStateStore struct{ state RiskState; loaded bool }
func (m *memoryStateStore) Load(context.Context) (RiskState, error) { if !m.loaded { return RiskState{SessionDate: "2026-09-06", SessionStartedAt: time.Date(2026,9,6,0,0,0,0,time.UTC)}, nil }; return m.state, nil }
func (m *memoryStateStore) Save(_ context.Context, state RiskState) error { m.state = state; m.loaded = true; return nil }

func TestRiskStatePersistsKillSwitchAndAccounting(t *testing.T) {
	e := NewEngine(config.RiskParams{DailyLossLimit: 100, MaxDrawdown: .2})
	store := &memoryStateStore{}
	e.RecordCleanExecution(25)
	e.RecordReversal(-3)
	e.TripKillSwitch("test safety event")
	if err := e.Persist(context.Background(), store); err != nil { t.Fatal(err) }

	restarted := NewEngine(config.RiskParams{DailyLossLimit: 100, MaxDrawdown: .2})
	if err := restarted.Restore(context.Background(), store); err != nil { t.Fatal(err) }
	got := restarted.Snapshot()
	if got.KillSwitchState != "TRIPPED" || got.KillReason != "test safety event" { t.Fatalf("kill switch state not restored: %+v", got) }
	if got.DailyPnL != 22 || got.TotalFills != 1 || got.Reversals != 1 { t.Fatalf("accounting not restored: %+v", got) }
}
