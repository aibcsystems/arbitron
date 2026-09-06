package execution_test

import (
	"context"
	"testing"
	"time"

	"arbitron/config"
	"arbitron/internal/exchange"
	"arbitron/internal/execution"
	"arbitron/internal/risk"
)

type shutdownPipeline struct{}
func (shutdownPipeline) PublishExecution(interface{}) error { return nil }

type shutdownAdapter struct{}
func (shutdownAdapter) Name() string { return "BINANCE_USDM" }
func (shutdownAdapter) SubmitIOC(context.Context, exchange.Order) exchange.FillResult { return exchange.FillResult{Filled:false, Exchange:"BINANCE_USDM"} }
func (shutdownAdapter) HealthCheck(context.Context) error { return nil }

func TestShutdownStopsIntakeBeforeContextCancellation(t *testing.T) {
	cfg := &config.Config{RiskParams: config.RiskParams{LegTimeoutMs: 50, DailyLossLimit: 1000, MaxDrawdown: 0.10}}
	riskEngine := risk.NewEngine(cfg.RiskParams)
	registry := exchange.NewAdapterRegistry(shutdownAdapter{})
	eng := execution.NewEngine(cfg, riskEngine, shutdownPipeline{}, registry)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	go func() { close(started); eng.Start(ctx) }()
	<-started

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := eng.Shutdown(shutdownCtx); err != nil { t.Fatalf("shutdown failed: %v", err) }

	// Must be safe and must not enqueue work after shutdown.
	eng.Submit(execution.ArbOpportunity{ID:"AFTER-SHUTDOWN", Leg1:execution.Order{ID:"L1",Exchange:"BINANCE_USDM",Symbol:"BTCUSDT",Side:execution.SideBuy,Type:execution.OrderIOC,Quantity:0.01}})
	cancel()
}
