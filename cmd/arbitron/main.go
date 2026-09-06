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
	if err := config.ValidateSafety(cfg); err != nil { log.Fatalf("❌ Unsafe configuration — refusing startup: %v", err) }
	log.Println("✅ Safety-critical configuration validated")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	pgPool, err := persistence.NewPool(ctx, cfg.PostgresURL)
	if err != nil { log.Fatalf("❌ PostgreSQL connection failed: %v", err) }
	copier := persistence.NewCopier(pgPool)
	log.Println("✅ PostgreSQL pool ready")

	redisClient, err := redis.NewClient(cfg.RedisAddr)
	if err != nil { pgPool.Close(); log.Fatalf("❌ Redis connection failed: %v", err) }
	log.Println("✅ Redis client ready")

	pipeline := redis.NewStreamPipeline(redisClient, cfg, copier)
	go pipeline.RunConsumer(ctx)
	log.Println("✅ Redis stream pipeline started")

	binanceAdapter := adapters.NewBinanceAdapter(cfg.Exchange.BinanceAPIKey, cfg.Exchange.BinanceAPISecret)
	krakenAdapter := adapters.NewKrakenAdapter(cfg.Exchange.KrakenAPIKey, cfg.Exchange.KrakenAPISecret)
	alpacaAdapter := adapters.NewAlpacaAdapter(cfg.Exchange.AlpacaAPIKey, cfg.Exchange.AlpacaAPISecret)
	fixAdapter := adapters.NewFIXAdapter(cfg.Exchange.FIXGatewayURL, cfg.Exchange.FIXSenderCompID, cfg.Exchange.FIXTargetCompID, cfg.Exchange.FIXUsername, cfg.Exchange.FIXPassword)
	registry := exchange.NewAdapterRegistry(binanceAdapter, krakenAdapter, alpacaAdapter, fixAdapter)
	log.Println("✅ Exchange adapter registry built (4 venues)")

	fixConnCtx, fixConnCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := fixAdapter.Connect(fixConnCtx); err != nil { fixConnCancel(); pgPool.Close(); log.Fatalf("❌ FIX gateway connection failed: %v", err) }
	fixConnCancel()
	log.Println("✅ FIX session established")

	healthCtx, healthCancel := context.WithTimeout(ctx, 5*time.Second)
	healthResults := registry.HealthCheckAll(healthCtx)
	healthCancel()
	allHealthy := true
	for venue, healthErr := range healthResults {
		if healthErr != nil { log.Printf("⚠ Health check FAILED for %s: %v", venue, healthErr); allHealthy = false } else { log.Printf("✅ Health check OK: %s", venue) }
	}
	if !allHealthy { pgPool.Close(); log.Fatal("❌ One or more exchange adapters failed health check — aborting startup") }

	riskEngine := risk.NewEngine(cfg.RiskParams)
	log.Println("✅ Risk engine armed (kill switch SAFE)")
	feedManager := exchange.NewFeedManager(cfg, pipeline)
	go feedManager.Start(ctx)
	log.Println("✅ Exchange feed manager started (gorilla/websocket)")

	execEngine := execution.NewEngine(cfg, riskEngine, pipeline, registry)
	go execEngine.Start(ctx)
	log.Println("✅ Execution engine started")

	detectorCtx, detectorCancel := context.WithCancel(ctx)
	defer detectorCancel()
	detector := execution.NewDetector(cfg, execEngine, feedManager)
	go detector.Start(detectorCtx, pipeline)
	log.Println("✅ Arbitrage detector started with venue-health gate")

	gateway := telemetry.NewGateway(cfg.GatewayAddr, pipeline, riskEngine, registry)
	go func() { if err := gateway.ListenAndServe(ctx); err != nil { log.Printf("Gateway error: %v", err) } }()
	log.Printf("✅ Telemetry gateway listening on %s", cfg.GatewayAddr)

	// Process/dependency health is not the same as trading readiness. The
	// detector remains fail-closed until each participating venue reports a
	// live connection and fresh market data.
	feedHealth := feedManager.GetHealth()
	freshFeeds := 0
	for _, h := range feedHealth {
		if h.State == exchange.CircuitOpen && !h.LastMessage.IsZero() && time.Since(h.LastMessage) <= 200*time.Millisecond { freshFeeds++ }
	}
	if freshFeeds == len(feedHealth) {
		log.Println("🟢 ARBITRON v4 READY — dependencies healthy and all feeds fresh")
	} else {
		log.Printf("🟡 ARBITRON v4 STARTED — trading NOT READY (%d/%d feeds fresh); detector remains fail-closed", freshFeeds, len(feedHealth))
	}
	log.Printf("   Dashboard  : %s/events (SSE) | %s/ws (WebSocket)", cfg.GatewayAddr, cfg.GatewayAddr)
	log.Printf("   Kill switch: POST %s/kill | Reset: POST %s/reset", cfg.GatewayAddr, cfg.GatewayAddr)
	log.Printf("   Risk params: MaxLatency=%dms | DailyLoss=$%.0f | Drawdown=%.0f%%", cfg.RiskParams.MaxLatencyMS, cfg.RiskParams.DailyLossLimit, cfg.RiskParams.MaxDrawdown*100)

	select {
	case sig := <-sigCh:
		log.Printf("⚠ Signal %s received — entering ordered graceful shutdown...", sig)
	case <-ctx.Done():
		log.Println("⚠ Context cancelled — shutting down.")
	}

	// Shutdown order is safety-critical:
	// 1. stop generating new opportunities;
	// 2. stop execution intake and drain already-dispatched executions;
	// 3. only then cancel feeds, pipeline and gateway dependencies.
	detectorCancel()
	shutdownTimeout := 3 * time.Second
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	if err := execEngine.Shutdown(shutdownCtx); err != nil {
		log.Printf("🚨 %v — forcing dependency cancellation; any unresolved order remains fail-closed", err)
	}
	shutdownCancel()
	cancel()

	log.Println("🔴 ARBITRON v4 shutdown complete.")
}
