package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// ValidateSafety checks configuration invariants that can otherwise cause
// unsafe execution behavior. It is intentionally side-effect free so callers
// can validate before opening network/database connections.
func ValidateSafety(c *Config) error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}
	r := c.RiskParams
	if r.DailyLossLimit <= 0 {
		return fmt.Errorf("RISK_DAILY_LOSS_LIMIT must be > 0")
	}
	if r.MaxDrawdown <= 0 || r.MaxDrawdown >= 1 {
		return fmt.Errorf("RISK_MAX_DRAWDOWN must be > 0 and < 1")
	}
	if r.LegTimeoutMs < 5 {
		return fmt.Errorf("RISK_LEG_TIMEOUT_MS must be >= 5ms")
	}
	for venue, ms := range r.LegTimeoutMsByVenue {
		if ms < 5 {
			return fmt.Errorf("RISK_LEG_TIMEOUT_MS_%s must be >= 5ms", venue)
		}
	}
	if r.MaxOrdersPerSec <= 0 {
		return fmt.Errorf("RISK_MAX_ORDERS_SEC must be > 0")
	}
	if r.MinPositionUSD <= 0 || r.MaxPositionUSD < r.MinPositionUSD {
		return fmt.Errorf("position limits invalid: min=%.2f max=%.2f", r.MinPositionUSD, r.MaxPositionUSD)
	}
	if r.BaseRiskPct <= 0 || r.BaseRiskPct > 1 {
		return fmt.Errorf("RISK_BASE_RISK_PCT must be > 0 and <= 1")
	}
	fees := map[string]float64{
		"FEE_BINANCE_USDM": c.Fees.BinanceUSDM,
		"FEE_KRAKEN_PERP": c.Fees.KrakenPerp,
		"FEE_ALPACA_SPOT": c.Fees.AlpacaSpot,
		"FEE_FIX_DIRECT": c.Fees.FIXDirect,
		"FEE_DEFAULT": c.Fees.Default,
	}
	for name, value := range fees {
		if value < 0 || value >= 1 {
			return fmt.Errorf("%s must be >= 0 and < 1", name)
		}
	}
	if c.StreamFlushInterval <= 0 || c.StreamMaxBatchSize <= 0 {
		return fmt.Errorf("stream flush interval and batch size must be > 0")
	}
	if r.OperatorTokenHash != "" {
		if len(r.OperatorTokenHash) != sha256.Size*2 {
			return fmt.Errorf("RISK_OPERATOR_TOKEN_SHA256 must be a 64-character SHA-256 hex digest")
		}
		if _, err := hex.DecodeString(r.OperatorTokenHash); err != nil {
			return fmt.Errorf("RISK_OPERATOR_TOKEN_SHA256 must contain only hexadecimal characters")
		}
	}
	return nil
}
