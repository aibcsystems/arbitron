// ============================================================
//  internal/telemetry/gateway.go
//  Arbitron v4 — SSE / WebSocket Telemetry Gateway
//
//  ★ BUG FIX APPLIED: main.go calls
//      telemetry.NewGateway(cfg.GatewayAddr, pipeline, riskEngine, registry)
//    — four arguments, including the exchange AdapterRegistry.
//    This file's NewGateway only took three (addr, pipeline, risk)
//    and had nowhere to put a registry — a straight compile
//    mismatch against the real call site. Added the registry
//    field/param and wired it into /health so that endpoint
//    reports real per-adapter status instead of just kill-switch
//    state, which is clearly what passing the registry in was for.
// ============================================================

package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"arbitron/internal/exchange"
	"arbitron/internal/redis"
	"arbitron/internal/risk"
)

type Gateway struct {
	addr     string
	pipeline *redis.StreamPipeline
	risk     *risk.Engine
	registry *exchange.AdapterRegistry
}

func NewGateway(addr string, p *redis.StreamPipeline, r *risk.Engine, registry *exchange.AdapterRegistry) *Gateway {
	return &Gateway{addr: addr, pipeline: p, risk: r, registry: registry}
}

func (g *Gateway) ListenAndServe(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", g.sseHandler)
	mux.HandleFunc("/opportunities", g.opportunitiesHandler)
	mux.HandleFunc("/ws", g.wsHandler)
	mux.HandleFunc("/health", g.healthHandler)
	mux.HandleFunc("/risk", g.riskSnapshotHandler)
	mux.HandleFunc("/kill", g.killHandler)
	mux.HandleFunc("/reset", g.resetHandler)

	srv := &http.Server{
		Addr:         g.addr,
		Handler:      corsMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // Disabled for SSE long-lived connections
		IdleTimeout:  3600 * time.Second,
	}

	log.Printf("[Gateway] Listening on %s", g.addr)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	case err := <-errCh:
		return err
	}
}

// ── SSE Handler (/events) ─────────────────────────────────

func (g *Gateway) sseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	ch := g.pipeline.Subscribe()
	defer g.pipeline.Unsubscribe(ch)

	log.Printf("[Gateway] SSE client connected: %s", r.RemoteAddr)

	fmt.Fprintf(w, "event: connected\ndata: {\"status\":\"connected\"}\n\n")
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			log.Printf("[Gateway] SSE client disconnected: %s", r.RemoteAddr)
			return

		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()

		case frame, ok := <-ch:
			if !ok {
				return
			}

			snapshot := g.risk.Snapshot()
			enriched := map[string]interface{}{
				"ts":          frame.Timestamp,
				"latency":     map[string]float64{"p50": frame.P50, "p95": frame.P95, "p99": frame.P99},
				"stress":      frame.Stress,
				"pnl":         frame.PnL,
				"orders_sec":  frame.OrdersSec,
				"fill_rate":   frame.FillRate,
				"clock_drift": frame.ClockDrift,
				"risk": map[string]interface{}{
					"kill_switch":    snapshot.KillSwitchState,
					"daily_pnl":      snapshot.DailyPnL,
					"drawdown":       snapshot.Drawdown,
					"loss_limit_pct": snapshot.DailyLossUsedPct,
					"reversals":      snapshot.Reversals,
				},
			}

			data, err := json.Marshal(enriched)
			if err != nil {
				continue
			}

			fmt.Fprintf(w, "event: telemetry\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// ── WebSocket Handler (/ws) ───────────────────────────────

func (g *Gateway) wsHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Gateway] WS client connected: %s (serving via SSE protocol)", r.RemoteAddr)
	g.sseHandler(w, r)
}

// ── Opportunity/execution feed (/opportunities) ──────────
// Surfaces live ExecutionEvent records (opportunity ID, outcome,
// PnL, per-leg fill results) as they happen. Previously these were
// only written to the Redis execution stream via XAdd — nothing
// pushed them to a live client. Added so dashboards can show real
// executions instead of static/demo data.
//
// ⚠ KNOWN GAP: ExecutionEvent (internal/execution/engine.go) does
// not carry the traded symbol/pair — FillResult has Exchange but no
// Symbol field, even though Order.Symbol exists upstream. Until
// that's threaded through, consumers of this feed get OpportunityID/
// Outcome/PnL/latency but no human-readable pair label (e.g.
// "BTC/USDT"). Flagged rather than patched here since engine.go
// changes need a real build/test environment to verify safely.

func (g *Gateway) opportunitiesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	ch := g.pipeline.SubscribeExecutions()
	defer g.pipeline.UnsubscribeExecutions(ch)

	log.Printf("[Gateway] Opportunity feed client connected: %s", r.RemoteAddr)

	fmt.Fprintf(w, "event: connected\ndata: {\"status\":\"connected\"}\n\n")
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			log.Printf("[Gateway] Opportunity feed client disconnected: %s", r.RemoteAddr)
			return

		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()

		case data, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: execution\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// ── Risk snapshot ─────────────────────────────────────────

func (g *Gateway) riskSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	snapshot := g.risk.Snapshot()
	json.NewEncoder(w).Encode(snapshot)
}

// ── Kill switch endpoints ─────────────────────────────────

func (g *Gateway) killHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	g.risk.TripKillSwitch("MANUAL_OPERATOR_TRIP")
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"status":"KILL_SWITCH_TRIPPED"}`)
}

func (g *Gateway) resetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	token := r.Header.Get("X-Operator-Token")
	if err := g.risk.ResetKillSwitch(token); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"status":"KILL_SWITCH_RESET"}`)
}

// ── Health probe ──────────────────────────────────────────
// Now reports real per-adapter health via the registry, not just
// kill-switch state — this is what passing registry in was for.

func (g *Gateway) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	snapshot := g.risk.Snapshot()

	healthCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	adapterHealth := g.registry.HealthCheckAll(healthCtx)
	adapters := make(map[string]string, len(adapterHealth))
	allHealthy := true
	for venue, err := range adapterHealth {
		if err != nil {
			adapters[venue] = err.Error()
			allHealthy = false
		} else {
			adapters[venue] = "ok"
		}
	}

	status := "ok"
	if !allHealthy {
		status = "degraded"
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      status,
		"kill_switch": snapshot.KillSwitchState,
		"service":     "arbitron-gateway",
		"adapters":    adapters,
	})
}

// ── CORS middleware ───────────────────────────────────────

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Operator-Token")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
