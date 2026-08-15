// ============================================================
//  cmd/arbitron/main.go
//  Arbitron v4 — Production Entry Point
// ============================================================

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"arbitron/config"
	"arbitron/internal/exchange"
	"arbitron/internal/exchange/adapters"
	"arbitron/internal/execution"
	"arbitron/internal/persistence"
	"arbitron/internal/redis"
	"arbitron/internal/risk"
	"arbitron/internal/telemetry"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Println("⚡ ARBITRON v4 — Initializing production engine...")

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	pgPool, err := persistence.NewPool(ctx, cfg.PostgresURL)
	if err != nil {
		log.Fatalf("❌ PostgreSQL connection failed: %v", err)
	}
	defer pgPool.Close()
	copier := persistence.NewCopier(pgPool)
	log.Println("✅ PostgreSQL pool ready")

	redisClient, err := redis.NewClient(cfg.RedisAddr)
	if err != nil {
		log.Fatalf("❌ Redis connection failed: %v", err)
	}
	log.Println("✅ Redis client ready")

	pipeline := redis.NewStreamPipeline(redisClient, cfg, copier)
	go pipeline.RunConsumer(ctx)
	log.Println("✅ Redis stream pipeline started")

	binanceAdapter := adapters.NewBinanceAdapter(
		cfg.Exchange.BinanceAPIKey,
		cfg.Exchange.BinanceAPISecret,
	)
	krakenAdapter := adapters.NewKrakenAdapter(
		cfg.Exchange.KrakenAPIKey,
		cfg.Exchange.KrakenAPISecret,
	)
	alpacaAdapter := adapters.NewAlpacaAdapter(
		cfg.Exchange.AlpacaAPIKey,
		cfg.Exchange.AlpacaAPISecret,
	)
	fixAdapter := adapters.NewFIXAdapter(
		cfg.Exchange.FIXGatewayURL,
		cfg.Exchange.FIXSenderCompID,
		cfg.Exchange.FIXTargetCompID,
		cfg.Exchange.FIXUsername,
		cfg.Exchange.FIXPassword,
	)

	registry := exchange.NewAdapterRegistry(
		binanceAdapter,
		krakenAdapter,
		alpacaAdapter,
		fixAdapter,
	)
	log.Println("✅ Exchange adapter registry built (4 venues)")

	fixConnCtx, fixConnCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := fixAdapter.Connect(fixConnCtx); err != nil {
		fixConnCancel()
		log.Fatalf("❌ FIX gateway connection failed: %v", err)
	}
	fixConnCancel()
	log.Println("✅ FIX session established")

	healthCtx, healthCancel := context.WithTimeout(ctx, 5*time.Second)
	healthResults := registry.HealthCheckAll(healthCtx)
	healthCancel()

	allHealthy := true
	for venue, err := range healthResults {
		if err != nil {
			log.Printf("⚠ Health check FAILED for %s: %v", venue, err)
			allHealthy = false
		} else {
			log.Printf("✅ Health check OK: %s", venue)
		}
	}
	if !allHealthy {
		log.Fatal("❌ One or more exchange adapters failed health check — aborting startup")
	}

	riskEngine := risk.NewEngine(cfg.RiskParams)
	log.Println("✅ Risk engine armed (kill switch SAFE)")

	feedManager := exchange.NewFeedManager(cfg, pipeline)
	go feedManager.Start(ctx)
	log.Println("✅ Exchange feed manager started (gorilla/websocket)")

	execEngine := execution.NewEngine(cfg, riskEngine, pipeline, registry)
	go execEngine.Start(ctx)
	log.Println("✅ Execution engine started")

	// ── GAP FILLED: the detector was never started ────────────
	// detector.go exists, compiles, and is fully implemented, but
	// nothing in the original main.go constructed or started it.
	// Without this, PublishTick() has no subscriber and no
	// ArbOpportunity is ever generated — the whole system would
	// run with a live execution engine that never receives work.
	detector := execution.NewDetector(cfg, execEngine)
	go detector.Start(ctx, pipeline)
	log.Println("✅ Arbitrage detector started")

	gateway := telemetry.NewGateway(cfg.GatewayAddr, pipeline, riskEngine, registry)
	go func() {
		if err := gateway.ListenAndServe(ctx); err != nil {
			log.Printf("Gateway error: %v", err)
		}
	}()
	log.Printf("✅ Telemetry gateway listening on %s", cfg.GatewayAddr)

	log.Println("🟢 ARBITRON v4 FULLY OPERATIONAL — all subsystems online")
	log.Printf("   Dashboard  : %s/events (SSE) | %s/ws (WebSocket)", cfg.GatewayAddr, cfg.GatewayAddr)
	log.Printf("   Kill switch: POST %s/kill | Reset: POST %s/reset", cfg.GatewayAddr, cfg.GatewayAddr)
	log.Printf("   Risk params: MaxLatency=%dms | DailyLoss=$%.0f | Drawdown=%.0f%%",
		cfg.RiskParams.MaxLatencyMS,
		cfg.RiskParams.DailyLossLimit,
		cfg.RiskParams.MaxDrawdown*100,
	)

	select {
	case sig := <-sigCh:
		log.Printf("⚠ Signal %s received — shutting down gracefully...", sig)
	case <-ctx.Done():
		log.Println("⚠ Context cancelled — shutting down.")
	}

	cancel()

	shutdownTimeout := 3 * time.Second
	log.Printf("⏳ Waiting %v for in-flight operations to complete...", shutdownTimeout)
	time.Sleep(shutdownTimeout)

	log.Println("🔴 ARBITRON v4 shutdown complete.")
}
