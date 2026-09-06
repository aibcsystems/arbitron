package risk

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"arbitron/config"
)

func operatorHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

func TestResetKillSwitchRequiresAuthenticatedOperator(t *testing.T) {
	e := NewEngine(config.RiskParams{LegTimeoutMs: 45, OperatorTokenHash: operatorHash("correct-token")})
	e.TripKillSwitch("test")
	if err := e.ResetKillSwitch("wrong-token"); err == nil { t.Fatal("expected invalid operator credential to be rejected") }
	if e.Snapshot().KillSwitchState != "TRIPPED" { t.Fatal("kill switch must remain tripped after failed reset") }
	if err := e.ResetKillSwitch("correct-token"); err != nil { t.Fatalf("valid operator credential rejected: %v", err) }
	if e.Snapshot().KillSwitchState != "SAFE" { t.Fatal("kill switch should be safe after authenticated reset") }
}

func TestResetKillSwitchRequiresConfiguredCredential(t *testing.T) {
	e := NewEngine(config.RiskParams{LegTimeoutMs: 45})
	e.TripKillSwitch("test")
	if err := e.ResetKillSwitch("anything"); err == nil { t.Fatal("expected reset to fail when credential is not configured") }
	if e.Snapshot().KillSwitchState != "TRIPPED" { t.Fatal("kill switch must remain tripped") }
}

func TestUTCSessionRollsDailyAccountingWithoutResettingKillSwitch(t *testing.T) {
	e := NewEngine(config.RiskParams{LegTimeoutMs: 45, DailyLossLimit: 1000})
	e.RecordCleanExecution(100)
	before := e.Snapshot()
	if before.DailyPnL != 100 { t.Fatalf("unexpected pnl before rollover: %v", before.DailyPnL) }

	e.mu.Lock()
	e.sessionDate = time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	e.mu.Unlock()

	after := e.Snapshot()
	if after.DailyPnL != 0 || after.TotalFills != 0 { t.Fatalf("expected fresh daily accounting, got pnl=%v fills=%v", after.DailyPnL, after.TotalFills) }
	if after.SessionDate != time.Now().UTC().Format("2006-01-02") { t.Fatalf("unexpected session date: %s", after.SessionDate) }

	e.TripKillSwitch("manual safety test")
	e.mu.Lock()
	e.sessionDate = time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	e.mu.Unlock()
	if e.Snapshot().KillSwitchState != "TRIPPED" { t.Fatal("session rollover must not silently clear kill switch") }
}
