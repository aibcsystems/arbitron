// ============================================================
//  config/config.go
//  Arbitron v4 — Production Config
// ============================================================

package config

import (
	"os"
	"strconv"
	"time"
)

type RiskParams struct {
	MaxLatencyMS   int64
	DailyLossLimit float64
	MaxDrawdown    float64
	LegTimeoutMs   int64
	LegTimeoutMsByVenue map[string]int64
	MaxOrdersPerSec int
	KillSwitchArmed bool

	// OperatorTokenHash is the SHA-256 hex digest of the operator token.
	// The raw token is never stored in configuration or logs.
	OperatorTokenHash string

	MaxPositionUSD float64
	MinPositionUSD float64
	BaseRiskPct    float64
}

type FeeSchedule struct {
	BinanceUSDM float64
	KrakenPerp  float64
	AlpacaSpot  float64
	FIXDirect   float64
	Default     float64
}

type ExchangeConfig struct {
	BinanceWSURL     string
	BinanceAPIKey    string
	BinanceAPISecret string
	KrakenWSURL      string
	KrakenAPIKey     string
	KrakenAPISecret  string
	AlpacaWSURL      string
	AlpacaAPIKey     string
	AlpacaAPISecret  string
	FIXGatewayURL    string
	FIXSenderCompID  string
	FIXTargetCompID  string
	FIXUsername      string
	FIXPassword      string
}

type Config struct {
	RedisAddr   string
	PostgresURL string
	GatewayAddr string
	Exchange    ExchangeConfig
	RiskParams  RiskParams
	Fees        FeeSchedule
	StreamFlushInterval time.Duration
	StreamMaxBatchSize  int
	TelemetryStreamKey  string
	ExecutionStreamKey  string
}

func Load() *Config {
	return &Config{
		RedisAddr: getEnv("REDIS_ADDR", "localhost:6379"),
		PostgresURL: getEnv("POSTGRES_URL", "postgres://arbitron:arbitron@localhost:5432/arbitron?sslmode=disable"),
		GatewayAddr: getEnv("GATEWAY_ADDR", ":8080"),
		Exchange: ExchangeConfig{
			BinanceWSURL: getEnv("BINANCE_WS_URL", "wss://fstream.binance.com/ws"),
			BinanceAPIKey: getEnv("BINANCE_API_KEY", ""),
			BinanceAPISecret: getEnv("BINANCE_API_SECRET", ""),
			KrakenWSURL: getEnv("KRAKEN_WS_URL", "wss://futures.kraken.com/ws/v1"),
			KrakenAPIKey: getEnv("KRAKEN_API_KEY", ""),
			KrakenAPISecret: getEnv("KRAKEN_API_SECRET", ""),
			AlpacaWSURL: getEnv("ALPACA_WS_URL", "wss://stream.data.alpaca.markets/v1beta3/crypto/us"),
			AlpacaAPIKey: getEnv("ALPACA_API_KEY", ""),
			AlpacaAPISecret: getEnv("ALPACA_API_SECRET", ""),
			FIXGatewayURL: getEnv("FIX_GATEWAY_URL", "tcp://fix-gateway:9878"),
			FIXSenderCompID: getEnv("FIX_SENDER_COMP_ID", "ARBITRON"),
			FIXTargetCompID: getEnv("FIX_TARGET_COMP_ID", "BROKER"),
			FIXUsername: getEnv("FIX_USERNAME", ""),
			FIXPassword: getEnv("FIX_PASSWORD", ""),
		},
		RiskParams: RiskParams{
			MaxLatencyMS: getEnvInt64("RISK_MAX_LATENCY_MS", 45),
			DailyLossLimit: getEnvFloat("RISK_DAILY_LOSS_LIMIT", 50000.0),
			MaxDrawdown: getEnvFloat("RISK_MAX_DRAWDOWN", 0.10),
			LegTimeoutMs: getEnvInt64("RISK_LEG_TIMEOUT_MS", 45),
			LegTimeoutMsByVenue: map[string]int64{"ALPACA_SPOT": getEnvInt64("RISK_LEG_TIMEOUT_MS_ALPACA", 2000)},
			MaxOrdersPerSec: getEnvInt("RISK_MAX_ORDERS_SEC", 50),
			KillSwitchArmed: true,
			OperatorTokenHash: getEnv("RISK_OPERATOR_TOKEN_SHA256", ""),
			MaxPositionUSD: getEnvFloat("RISK_MAX_POSITION_USD", 5000.0),
			MinPositionUSD: getEnvFloat("RISK_MIN_POSITION_USD", 50.0),
			BaseRiskPct: getEnvFloat("RISK_BASE_RISK_PCT", 0.02),
		},
		Fees: FeeSchedule{
			BinanceUSDM: getEnvFloat("FEE_BINANCE_USDM", 0.0005),
			KrakenPerp: getEnvFloat("FEE_KRAKEN_PERP", 0.00075),
			AlpacaSpot: getEnvFloat("FEE_ALPACA_SPOT", 0.0015),
			FIXDirect: getEnvFloat("FEE_FIX_DIRECT", 0.0004),
			Default: getEnvFloat("FEE_DEFAULT", 0.0010),
		},
		StreamFlushInterval: getEnvDuration("STREAM_FLUSH_INTERVAL", 500*time.Millisecond),
		StreamMaxBatchSize: getEnvInt("STREAM_MAX_BATCH_SIZE", 500),
		TelemetryStreamKey: getEnv("TELEMETRY_STREAM_KEY", "arbitron:telemetry"),
		ExecutionStreamKey: getEnv("EXECUTION_STREAM_KEY", "arbitron:execution"),
	}
}

func (r RiskParams) LegTimeout(venue string) time.Duration {
	if r.LegTimeoutMsByVenue != nil {
		if ms, ok := r.LegTimeoutMsByVenue[venue]; ok { return time.Duration(ms) * time.Millisecond }
	}
	return time.Duration(r.LegTimeoutMs) * time.Millisecond
}

func getEnv(key, fallback string) string { if v := os.Getenv(key); v != "" { return v }; return fallback }
func getEnvInt64(key string, fallback int64) int64 { if v := os.Getenv(key); v != "" { if n, err := strconv.ParseInt(v, 10, 64); err == nil { return n } }; return fallback }
func getEnvFloat(key string, fallback float64) float64 { if v := os.Getenv(key); v != "" { if f, err := strconv.ParseFloat(v, 64); err == nil { return f } }; return fallback }
func getEnvInt(key string, fallback int) int { if v := os.Getenv(key); v != "" { if n, err := strconv.Atoi(v); err == nil { return n } }; return fallback }
func getEnvDuration(key string, fallback time.Duration) time.Duration { if v := os.Getenv(key); v != "" { if d, err := time.ParseDuration(v); err == nil { return d } }; return fallback }
