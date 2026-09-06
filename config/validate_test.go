package config

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func validTestConfig() *Config {
	return &Config{
		RiskParams: RiskParams{
			DailyLossLimit: 1000, MaxDrawdown: 0.1, LegTimeoutMs: 50,
			LegTimeoutMsByVenue: map[string]int64{"ALPACA_SPOT": 2000}, MaxOrdersPerSec: 10,
			MaxPositionUSD: 1000, MinPositionUSD: 50, BaseRiskPct: 0.02,
		},
		Fees: FeeSchedule{BinanceUSDM: 0.0005, KrakenPerp: 0.00075, AlpacaSpot: 0.0015, FIXDirect: 0.0004, Default: 0.001},
		StreamFlushInterval: 500 * time.Millisecond, StreamMaxBatchSize: 100,
	}
}

func TestValidateSafetyAcceptsValidConfig(t *testing.T) {
	if err := ValidateSafety(validTestConfig()); err != nil { t.Fatalf("valid config rejected: %v", err) }
}

func TestValidateSafetyRejectsUnsafeRiskValues(t *testing.T) {
	cases := []struct{name string; mutate func(*Config)}{
		{"daily loss", func(c *Config) { c.RiskParams.DailyLossLimit = 0 }},
		{"drawdown", func(c *Config) { c.RiskParams.MaxDrawdown = 1 }},
		{"timeout", func(c *Config) { c.RiskParams.LegTimeoutMs = 4 }},
		{"venue timeout", func(c *Config) { c.RiskParams.LegTimeoutMsByVenue["ALPACA_SPOT"] = 0 }},
		{"rate limit", func(c *Config) { c.RiskParams.MaxOrdersPerSec = 0 }},
		{"position bounds", func(c *Config) { c.RiskParams.MaxPositionUSD = 10 }},
		{"risk pct", func(c *Config) { c.RiskParams.BaseRiskPct = 1.1 }},
		{"negative fee", func(c *Config) { c.Fees.Default = -0.01 }},
		{"stream interval", func(c *Config) { c.StreamFlushInterval = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { c := validTestConfig(); tc.mutate(c); if err := ValidateSafety(c); err == nil { t.Fatalf("expected validation failure") } })
	}
}

func TestValidateSafetyRejectsMalformedOperatorCredential(t *testing.T) {
	c := validTestConfig()
	c.RiskParams.OperatorTokenHash = strings.Repeat("z", sha256.Size*2)
	if err := ValidateSafety(c); err == nil { t.Fatal("expected malformed hash to be rejected") }

	c.RiskParams.OperatorTokenHash = hex.EncodeToString(make([]byte, sha256.Size))
	if err := ValidateSafety(c); err != nil { t.Fatalf("valid hash rejected: %v", err) }
}
