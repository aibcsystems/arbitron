// ============================================================
//  config/config.go
//  Arbitron v4 — Production Config
//
//  Extended to carry per-exchange API credentials and FIX
//  session parameters needed by the real adapter implementations.
//
//  ★ NOTE: two config.go variants existed in this repo — a plain
//  one (single APIKey/APISecret, no Fees field) and this extended
//  one. main.go references cfg.Exchange.BinanceAPIKey,
//  .KrakenAPISecret, .FIXSenderCompID, .FIXUsername, cfg.Fees,
//  etc. — only this version has those. The plain version cannot
//  compile against main.go or engine.go. This is the one to keep.
// ============================================================

package config

import (
	"os"
	"strconv"
	"time"
)

type RiskParams struct {
	MaxLatencyMS    int64
	DailyLossLimit  float64
	MaxDrawdown     float64
	// LegTimeoutMs is the DEFAULT leg timeout, used for any venue not
	// listed in LegTimeoutMsByVenue. Calibrated for WS-speed exchanges
	// (Binance, Kraken) where fills confirm near-instantly over the
	// open socket.
	LegTimeoutMs    int64
	// LegTimeoutMsByVenue overrides LegTimeoutMs per exchange, keyed by
	// the adapter's Name() string (e.g. "ALPACA_SPOT"). Needed because
	// REST+poll venues like Alpaca confirm fills on a fundamentally
	// different timescale (~1-2s observed) than WS-native venues — a
	// single global timeout either times out Alpaca's real fills
	// constantly (too tight) or lets a stuck WS venue block far longer
	// than necessary (too loose). Falls back to LegTimeoutMs if the
	// venue has no entry here.
	LegTimeoutMsByVenue map[string]int64
	MaxOrdersPerSec int
	KillSwitchArmed bool

	// ── Position sizing (P1 fix — replaces detector.go's hardcoded 0.01) ──
	// MaxPositionUSD caps the notional size of any single opportunity,
	// regardless of spread quality. Hard ceiling — never exceeded.
	MaxPositionUSD float64
	// MinPositionUSD floors the notional size — below this, exchange
	// minimum-order-size rules would likely reject the order anyway,
	// so opportunities this small are better skipped than attempted.
	MinPositionUSD float64
	// BaseRiskPct is the fraction of DailyLossLimit that a single
	// max-confidence opportunity may size against. E.g. 0.02 = a single
	// trade's notional risk exposure caps at 2% of the daily loss budget.
	BaseRiskPct float64
}

// FeeSchedule holds taker rates per venue (bps as decimals).
// Loaded from env so they update without a recompile when
// exchanges revise their fee tiers.
type FeeSchedule struct {
	BinanceUSDM float64 // Default: 0.0005 (5 bps)
	KrakenPerp  float64 // Default: 0.00075 (7.5 bps)
	AlpacaSpot  float64 // Default: 0.0015 (15 bps)
	FIXDirect   float64 // Default: 0.0004 (4 bps institutional)
	Default     float64 // Defensive default for unknown venues
}

type ExchangeConfig struct {
	// Binance USDM Futures
	BinanceWSURL     string
	BinanceAPIKey    string
	BinanceAPISecret string

	// Kraken Perpetuals
	KrakenWSURL     string
	KrakenAPIKey    string
	KrakenAPISecret string

	// Alpaca Spot Crypto
	AlpacaWSURL     string
	AlpacaAPIKey    string
	AlpacaAPISecret string

	// FIX Direct Gateway
	FIXGatewayURL   string
	FIXSenderCompID string
	FIXTargetCompID string
	FIXUsername     string
	FIXPassword     string
}

type Config struct {
	// Infrastructure
	RedisAddr   string
	PostgresURL string
	GatewayAddr string

	// Exchange connectivity + credentials
	Exchange ExchangeConfig

	// Risk parameters (enforced at code level, not just dashboard)
	RiskParams RiskParams

	// Fee schedule (config-driven, not hardcoded)
	Fees FeeSchedule

	// Telemetry pipeline
	StreamFlushInterval time.Duration
	StreamMaxBatchSize  int
	TelemetryStreamKey  string
	ExecutionStreamKey  string
}

func Load() *Config {
	return &Config{
		RedisAddr: getEnv("REDIS_ADDR", "localhost:6379"),
		PostgresURL: getEnv("POSTGRES_URL",
			"postgres://arbitron:arbitron@localhost:5432/arbitron?sslmode=disable"),
		GatewayAddr: getEnv("GATEWAY_ADDR", ":8080"),

		Exchange: ExchangeConfig{
			// Binance
			BinanceWSURL:     getEnv("BINANCE_WS_URL", "wss://fstream.binance.com/ws"),
			BinanceAPIKey:    getEnv("BINANCE_API_KEY", ""),
			BinanceAPISecret: getEnv("BINANCE_API_SECRET", ""),

			// Kraken
			KrakenWSURL:     getEnv("KRAKEN_WS_URL", "wss://futures.kraken.com/ws/v1"),
			KrakenAPIKey:    getEnv("KRAKEN_API_KEY", ""),
			KrakenAPISecret: getEnv("KRAKEN_API_SECRET", ""),

			// Alpaca
			AlpacaWSURL:     getEnv("ALPACA_WS_URL", "wss://stream.data.alpaca.markets/v1beta3/crypto/us"),
			AlpacaAPIKey:    getEnv("ALPACA_API_KEY", ""),
			AlpacaAPISecret: getEnv("ALPACA_API_SECRET", ""),

			// FIX
			FIXGatewayURL:   getEnv("FIX_GATEWAY_URL", "tcp://fix-gateway:9878"),
			FIXSenderCompID: getEnv("FIX_SENDER_COMP_ID", "ARBITRON"),
			FIXTargetCompID: getEnv("FIX_TARGET_COMP_ID", "BROKER"),
			FIXUsername:     getEnv("FIX_USERNAME", ""),
			FIXPassword:     getEnv("FIX_PASSWORD", ""),
		},

		RiskParams: RiskParams{
			MaxLatencyMS:   getEnvInt64("RISK_MAX_LATENCY_MS", 45),
			DailyLossLimit: getEnvFloat("RISK_DAILY_LOSS_LIMIT", 50000.0),
			MaxDrawdown:    getEnvFloat("RISK_MAX_DRAWDOWN", 0.10),
			LegTimeoutMs:   getEnvInt64("RISK_LEG_TIMEOUT_MS", 45),
			// Per-venue overrides. Alpaca defaults to 2000ms — comfortably
			// above the ~1.3s round-trip observed in smoketest — while
			// WS-native venues stay on the tight 45ms default above.
			// Override any of these independently via env if real-world
			// latency data says otherwise.
			LegTimeoutMsByVenue: map[string]int64{
				"ALPACA_SPOT": getEnvInt64("RISK_LEG_TIMEOUT_MS_ALPACA", 2000),
			},
			MaxOrdersPerSec: getEnvInt("RISK_MAX_ORDERS_SEC", 50),
			KillSwitchArmed: true,

			MaxPositionUSD: getEnvFloat("RISK_MAX_POSITION_USD", 5000.0),
			MinPositionUSD: getEnvFloat("RISK_MIN_POSITION_USD", 50.0),
			BaseRiskPct:    getEnvFloat("RISK_BASE_RISK_PCT", 0.02),
		},

		// Fee schedule — config-driven per audit recommendation
		Fees: FeeSchedule{
			BinanceUSDM: getEnvFloat("FEE_BINANCE_USDM", 0.0005),
			KrakenPerp:  getEnvFloat("FEE_KRAKEN_PERP", 0.00075),
			AlpacaSpot:  getEnvFloat("FEE_ALPACA_SPOT", 0.0015),
			FIXDirect:   getEnvFloat("FEE_FIX_DIRECT", 0.0004),
			Default:     getEnvFloat("FEE_DEFAULT", 0.0010),
		},

		StreamFlushInterval: getEnvDuration("STREAM_FLUSH_INTERVAL", 500*time.Millisecond),
		StreamMaxBatchSize:  getEnvInt("STREAM_MAX_BATCH_SIZE", 500),
		TelemetryStreamKey:  getEnv("TELEMETRY_STREAM_KEY", "arbitron:telemetry"),
		ExecutionStreamKey:  getEnv("EXECUTION_STREAM_KEY", "arbitron:execution"),
	}
}

// LegTimeout returns the configured leg timeout for a given venue name
// (an adapter's Name(), e.g. "ALPACA_SPOT"), falling back to the global
// LegTimeoutMs default if the venue has no override entry.
func (r RiskParams) LegTimeout(venue string) time.Duration {
	if r.LegTimeoutMsByVenue != nil {
		if ms, ok := r.LegTimeoutMsByVenue[venue]; ok {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return time.Duration(r.LegTimeoutMs) * time.Millisecond
}

// ── env helpers ───────────────────────────────────────────

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
