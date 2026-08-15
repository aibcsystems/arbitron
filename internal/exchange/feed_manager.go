// ============================================================
//  internal/exchange/feed_manager.go
//  Arbitron v4 — Exchange WebSocket Feed Manager
//
//  Manages WebSocket connections to all four venues:
//    BINANCE_USDM, KRAKEN_PERP, ALPACA_SPOT, FIX_DIRECT
//
//  Features:
//    - Per-exchange goroutines with independent reconnect loops
//    - Full-jitter exponential backoff (AWS-style)
//    - Circuit breaker per exchange (OPEN/CLOSED/HALF_OPEN)
//    - Feed health telemetry published to Redis Streams
//
//  ★ BUG FIX APPLIED: this file used to define its own stub
//  `dialWebSocket()` + `stubConn` right alongside websocket.go's
//  real gorilla/websocket implementation of the exact same
//  function name in the same package (`exchange`). That's a
//  duplicate declaration — Go refuses to compile it
//  ("dialWebSocket redeclared in this block"). On top of that,
//  the stub's `stubConn.ReadMessage` called `fmt.Sprintf` but
//  this file never imported "fmt" — a second, independent
//  compile error layered on the first.
//
//  Fix: removed the stub `dialWebSocket` and `stubConn` entirely.
//  websocket.go already supplies the real `dialWebSocket`
//  against the same `wsConn` interface (kept below, unchanged —
//  both files need it, and it only needs to be declared once).
// ============================================================

package exchange

import (
	"context"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"arbitron/config"
	"arbitron/internal/redis"
)

// ── Circuit Breaker States ────────────────────────────────

type CircuitState string

const (
	CircuitOpen     CircuitState = "OPEN"
	CircuitClosed   CircuitState = "CLOSED"
	CircuitHalfOpen CircuitState = "HALF_OPEN"
)

// ── Feed health (exported for telemetry dashboard) ────────

type FeedHealth struct {
	Exchange    string
	State       CircuitState
	Latency     time.Duration
	LastMessage time.Time
	Reconnects  int
	StaleCount  int
}

// ── FeedManager coordinates all exchange connections ──────

type FeedManager struct {
	cfg      *config.Config
	pipeline *redis.StreamPipeline
	health   map[string]*FeedHealth
	mu       sync.RWMutex
}

func NewFeedManager(cfg *config.Config, pipeline *redis.StreamPipeline) *FeedManager {
	return &FeedManager{
		cfg:      cfg,
		pipeline: pipeline,
		health: map[string]*FeedHealth{
			"BINANCE_USDM": {Exchange: "BINANCE_USDM", State: CircuitClosed},
			"KRAKEN_PERP":  {Exchange: "KRAKEN_PERP", State: CircuitClosed},
			"ALPACA_SPOT":  {Exchange: "ALPACA_SPOT", State: CircuitClosed},
			"FIX_DIRECT":   {Exchange: "FIX_DIRECT", State: CircuitClosed},
		},
	}
}

// Start launches all feed goroutines. Blocks until ctx is cancelled.
func (fm *FeedManager) Start(ctx context.Context) {
	venues := []struct {
		name string
		url  string
	}{
		{"BINANCE_USDM", fm.cfg.Exchange.BinanceWSURL},
		{"KRAKEN_PERP", fm.cfg.Exchange.KrakenWSURL},
		{"ALPACA_SPOT", fm.cfg.Exchange.AlpacaWSURL},
		{"FIX_DIRECT", fm.cfg.Exchange.FIXGatewayURL},
	}

	var wg sync.WaitGroup
	for _, v := range venues {
		wg.Add(1)
		go func(name, url string) {
			defer wg.Done()
			fm.runFeed(ctx, name, url)
		}(v.name, v.url)
	}

	wg.Wait()
	log.Println("[FeedManager] All feed goroutines stopped.")
}

// ── runFeed: per-exchange reconnect loop ──────────────────
//
// Outer loop: reconnects indefinitely with full-jitter backoff.
// Inner loop: reads messages from an active connection.
// Circuit breaker transitions:
//   CLOSED → OPEN on successful connect + first message
//   OPEN → CLOSED on disconnect / error
//   CLOSED → HALF_OPEN during reconnect window (0 < attempt < 5)

func (fm *FeedManager) runFeed(ctx context.Context, name, wsURL string) {
	log.Printf("[%s] Feed manager starting...", name)
	attempt := 0

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Feed shutting down.", name)
			fm.setState(name, CircuitClosed)
			return
		default:
		}

		// Transition to HALF_OPEN during reconnect window
		if attempt > 0 {
			fm.setState(name, CircuitHalfOpen)
			delay := fullJitterBackoff(attempt, 100*time.Millisecond, 30*time.Second)
			log.Printf("[%s] Reconnect attempt %d — waiting %v (full-jitter backoff)", name, attempt, delay.Round(time.Millisecond))

			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}

		// Attempt connection — dialWebSocket is supplied by websocket.go
		conn, err := dialWebSocket(ctx, wsURL)
		if err != nil {
			log.Printf("[%s] ❌ Connect failed (attempt %d): %v", name, attempt, err)
			attempt++
			fm.incReconnects(name)
			continue
		}

		// Connected — open circuit
		fm.setState(name, CircuitOpen)
		log.Printf("[%s] ✅ Connected (attempt %d)", name, attempt)
		attempt = 0 // Reset backoff on successful connect

		// Inner read loop
		disconnected := fm.readLoop(ctx, name, conn)
		if disconnected {
			log.Printf("[%s] ⚠ Connection dropped — will reconnect.", name)
			fm.setState(name, CircuitClosed)
			attempt++
		}
	}
}

// readLoop processes incoming messages from an active WebSocket connection.
// Returns true if the connection was lost (triggering outer reconnect loop).

func (fm *FeedManager) readLoop(ctx context.Context, name string, conn wsConn) bool {
	staleTicker := time.NewTicker(5 * time.Second) // Detect stale feeds
	defer staleTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			conn.Close()
			return false // Clean shutdown — don't reconnect

		case <-staleTicker.C:
			fm.mu.RLock()
			h := fm.health[name]
			fm.mu.RUnlock()

			if time.Since(h.LastMessage) > 5*time.Second {
				log.Printf("[%s] ⚠ Stale feed detected — no message in 5s. Reconnecting.", name)
				fm.incStale(name)
				conn.Close()
				return true
			}

		default:
			// Read next message (non-blocking with deadline)
			msg, err := conn.ReadMessage(500 * time.Millisecond)
			if err != nil {
				log.Printf("[%s] Read error: %v", name, err)
				conn.Close()
				return true
			}

			// Record message timestamp and publish tick to pipeline
			fm.mu.Lock()
			if h, ok := fm.health[name]; ok {
				h.LastMessage = time.Now()
			}
			fm.mu.Unlock()

			fm.pipeline.PublishTick(name, msg)
		}
	}
}

// ── Full-Jitter Exponential Backoff ──────────────────────
//
// AWS-recommended algorithm for preventing thundering herd
// reconnect storms when multiple connections drop simultaneously.
//
// Formula: sleep = random_between(0, min(cap, base * 2^attempt))
//
// At attempt=10, base=100ms, cap=30s:
//   max_sleep = min(30s, 0.1s * 1024) = 30s
//   actual_sleep = random(0, 30s)
//
// This desynchronizes all exchange reconnect attempts across
// the cluster, preventing coordinated gateway hammering.

func fullJitterBackoff(attempt int, base, cap time.Duration) time.Duration {
	// Compute exponential ceiling: base * 2^attempt
	exp := float64(base) * math.Pow(2, float64(attempt))
	ceiling := float64(cap)
	if exp < ceiling {
		ceiling = exp
	}
	// Full jitter: uniform random in [0, ceiling]
	return time.Duration(rand.Float64() * ceiling)
}

// ── Circuit breaker state helpers ────────────────────────

func (fm *FeedManager) setState(name string, state CircuitState) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if h, ok := fm.health[name]; ok {
		h.State = state
	}
}

func (fm *FeedManager) incReconnects(name string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if h, ok := fm.health[name]; ok {
		h.Reconnects++
	}
}

func (fm *FeedManager) incStale(name string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if h, ok := fm.health[name]; ok {
		h.StaleCount++
	}
}

// GetHealth returns a snapshot of all feed health states.
// Used by the telemetry gateway to populate dashboard circuit breakers.
func (fm *FeedManager) GetHealth() map[string]FeedHealth {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	snapshot := make(map[string]FeedHealth, len(fm.health))
	for k, v := range fm.health {
		snapshot[k] = *v
	}
	return snapshot
}

// ── WebSocket interface ───────────────────────────────────
// Implemented by websocket.go's gorillConn in production.
// Declared here once; both files in this package use it.

type wsConn interface {
	ReadMessage(timeout time.Duration) ([]byte, error)
	Close() error
}
