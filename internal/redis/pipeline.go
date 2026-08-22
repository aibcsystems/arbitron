// ============================================================
//  internal/redis/pipeline.go — P0 PATCH
//  Arbitron v4 — Redis Streams Pipeline with Real pgx Copier
//
//  Change from stub version:
//    - batchInsert stub → copier.Insert() (pgx CopyFrom)
//    - Constructor accepts *persistence.Copier
//    - PublishTick added for feed manager → arb detector path
// ============================================================

package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"arbitron/config"
	"arbitron/internal/persistence"
)

// ── TelemetryFrame ────────────────────────────────────────

type TelemetryFrame struct {
	Timestamp  int64   `json:"ts"`
	P50        float64 `json:"p50"`
	P95        float64 `json:"p95"`
	P99        float64 `json:"p99"`
	Stress     float64 `json:"stress"`
	PnL        float64 `json:"pnl"`
	OrdersSec  float64 `json:"orders_sec"`
	FillRate   float64 `json:"fill_rate"`
	ClockDrift string  `json:"clock_drift"`
}

// ── PriceTick from exchange feed ──────────────────────────

type PriceTick struct {
	Exchange  string  `json:"exchange"`
	Symbol    string  `json:"symbol"`
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	Timestamp int64   `json:"ts"`
}

// ── StreamPipeline ────────────────────────────────────────

type StreamPipeline struct {
	client *goredis.Client
	cfg    *config.Config
	copier *persistence.Copier // Real pgx CopyFrom — stub replaced

	// SSE/WS fan-out subscribers
	telemetrySubs []chan TelemetryFrame
	subMu         sync.RWMutex

	// Execution/opportunity event fan-out — added so the gateway can
	// surface live execution events (previously only written to the
	// Redis stream via XAdd, never pushed to any live subscriber).
	executionSubs []chan []byte
	execMu        sync.RWMutex

	// Price tick fan-out for arb detector
	tickSubs []chan PriceTick
	tickMu   sync.RWMutex
}

// NewStreamPipeline accepts a *persistence.Copier.
// Wire: redis.NewStreamPipeline(redisClient, cfg, copier)
func NewStreamPipeline(
	client *goredis.Client,
	cfg *config.Config,
	copier *persistence.Copier,
) *StreamPipeline {
	return &StreamPipeline{
		client: client,
		cfg:    cfg,
		copier: copier,
	}
}

// ── PublishTelemetry ──────────────────────────────────────

func (p *StreamPipeline) PublishTelemetry(frame TelemetryFrame) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal telemetry: %w", err)
	}

	// Telemetry is observability, not core trading logic — a missing or
	// down Redis client must never crash the caller. Fan-out to live
	// subscribers still happens below even without a backing stream.
	if p.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		if err := p.client.XAdd(ctx, &goredis.XAddArgs{
			Stream: p.cfg.TelemetryStreamKey,
			MaxLen: 10000,
			Approx: true,
			Values: map[string]interface{}{
				"data": string(data),
				"ts":   frame.Timestamp,
			},
		}).Err(); err != nil {
			return fmt.Errorf("XADD telemetry: %w", err)
		}
	}

	// Fan-out to live SSE/WS subscribers
	p.subMu.RLock()
	defer p.subMu.RUnlock()
	for _, ch := range p.telemetrySubs {
		select {
		case ch <- frame:
		default:
		}
	}
	return nil
}

// ── PublishExecution ──────────────────────────────────────

func (p *StreamPipeline) PublishExecution(event interface{}) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal execution event: %w", err)
	}

	// Fan-out to live SSE/WS subscribers happens regardless of Redis
	// availability — the dashboard should stay live even if the
	// Redis audit-trail write below fails or there's no client
	// (test harnesses). Non-blocking: a slow/stalled subscriber
	// drops frames rather than backpressuring the execution engine.
	p.execMu.RLock()
	for _, ch := range p.executionSubs {
		select {
		case ch <- data:
		default:
		}
	}
	p.execMu.RUnlock()

	// Same guard as PublishTelemetry: a nil/unavailable Redis client
	// must not crash the execution engine mid-trade. Losing the
	// audit-trail write is a real problem to alert on — but it must
	// never be a panic, and it must not suppress the live fan-out above.
	if p.client == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	return p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: p.cfg.ExecutionStreamKey,
		MaxLen: 50000,
		Approx: true,
		Values: map[string]interface{}{
			"data": string(data),
			"ts":   time.Now().UnixMilli(),
		},
	}).Err()
}

// SubscribeExecutions registers a new live subscriber for execution
// events (raw JSON, already marshaled). Caller must call
// UnsubscribeExecutions when done (e.g. on client disconnect).
func (p *StreamPipeline) SubscribeExecutions() chan []byte {
	ch := make(chan []byte, 64)
	p.execMu.Lock()
	defer p.execMu.Unlock()
	p.executionSubs = append(p.executionSubs, ch)
	return ch
}

func (p *StreamPipeline) UnsubscribeExecutions(ch chan []byte) {
	p.execMu.Lock()
	defer p.execMu.Unlock()
	for i, sub := range p.executionSubs {
		if sub == ch {
			p.executionSubs = append(p.executionSubs[:i], p.executionSubs[i+1:]...)
			close(ch)
			break
		}
	}
}

// ── PublishTick ───────────────────────────────────────────
// Called by feed_manager.go on every incoming price message.
// Fan-out to arb detector subscribers (non-blocking).

func (p *StreamPipeline) PublishTick(exchange string, raw []byte) {
	var tick PriceTick
	if err := json.Unmarshal(raw, &tick); err != nil {
		return // Malformed tick — discard silently
	}
	tick.Exchange = exchange
	if tick.Timestamp == 0 {
		tick.Timestamp = time.Now().UnixMilli()
	}

	p.tickMu.RLock()
	defer p.tickMu.RUnlock()
	for _, ch := range p.tickSubs {
		select {
		case ch <- tick:
		default:
		}
	}
}

// ── Subscribe / Unsubscribe — telemetry SSE/WS ───────────

func (p *StreamPipeline) Subscribe() chan TelemetryFrame {
	ch := make(chan TelemetryFrame, 64)
	p.subMu.Lock()
	p.telemetrySubs = append(p.telemetrySubs, ch)
	p.subMu.Unlock()
	return ch
}

func (p *StreamPipeline) Unsubscribe(ch chan TelemetryFrame) {
	p.subMu.Lock()
	defer p.subMu.Unlock()
	for i, sub := range p.telemetrySubs {
		if sub == ch {
			p.telemetrySubs = append(p.telemetrySubs[:i], p.telemetrySubs[i+1:]...)
			close(ch)
			return
		}
	}
}

// ── Subscribe / Unsubscribe — price ticks (arb detector) ─

func (p *StreamPipeline) SubscribeTicks() chan PriceTick {
	ch := make(chan PriceTick, 512) // Larger buffer — ticks arrive at high frequency
	p.tickMu.Lock()
	p.tickSubs = append(p.tickSubs, ch)
	p.tickMu.Unlock()
	return ch
}

func (p *StreamPipeline) UnsubscribeTicks(ch chan PriceTick) {
	p.tickMu.Lock()
	defer p.tickMu.Unlock()
	for i, sub := range p.tickSubs {
		if sub == ch {
			p.tickSubs = append(p.tickSubs[:i], p.tickSubs[i+1:]...)
			close(ch)
			return
		}
	}
}

// ── RunConsumer ───────────────────────────────────────────

func (p *StreamPipeline) RunConsumer(ctx context.Context) {
	log.Println("[Pipeline] Consumer started — flushing every", p.cfg.StreamFlushInterval)

	ticker := time.NewTicker(p.cfg.StreamFlushInterval)
	defer ticker.Stop()

	cursors := map[string]string{
		p.cfg.TelemetryStreamKey: "0",
		p.cfg.ExecutionStreamKey: "0",
	}

	// Dead-letter counters — alert after 10 consecutive failures per stream
	failCount := map[string]int{}

	for {
		select {
		case <-ctx.Done():
			log.Println("[Pipeline] Consumer shutting down.")
			return
		case <-ticker.C:
			p.flushStreams(ctx, cursors, failCount)
		}
	}
}

// ── flushStreams ──────────────────────────────────────────

func (p *StreamPipeline) flushStreams(
	ctx context.Context,
	cursors map[string]string,
	failCount map[string]int,
) {
	for streamKey, cursor := range cursors {
		readCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)

		streams, err := p.client.XRead(readCtx, &goredis.XReadArgs{
			Streams: []string{streamKey, cursor},
			Count:   int64(p.cfg.StreamMaxBatchSize),
			Block:   0,
		}).Result()
		cancel()

		if err != nil && err != goredis.Nil {
			log.Printf("[Pipeline] XREAD error on %s: %v", streamKey, err)
			continue
		}

		for _, stream := range streams {
			if len(stream.Messages) == 0 {
				continue
			}

			rows := make([]string, 0, len(stream.Messages))
			var lastID string

			for _, msg := range stream.Messages {
				if data, ok := msg.Values["data"].(string); ok {
					rows = append(rows, data)
				}
				lastID = msg.ID
			}

			if len(rows) == 0 {
				continue
			}

			// ── STUB REPLACED ─────────────────────────────
			// Old: p.batchInsert(ctx, streamKey, rows) → log stub
			// New: p.copier.Insert() → pgx CopyFrom binary protocol
			if err := p.copier.Insert(ctx, streamKey, rows); err != nil {
				// Do NOT advance cursor — re-read next tick (at-least-once)
				failCount[streamKey]++
				log.Printf("[Pipeline] ⚠ Insert failed for %s (attempt %d): %v",
					streamKey, failCount[streamKey], err)

				// Dead-letter alert threshold
				if failCount[streamKey] >= 10 {
					log.Printf("[Pipeline] 🚨 %s failing consistently (%d times) — check PostgreSQL",
						streamKey, failCount[streamKey])
					// In production: emit to risk engine alert channel or PagerDuty
				}
			} else {
				// Success — advance cursor and reset fail counter
				cursors[streamKey] = lastID
				failCount[streamKey] = 0
				log.Printf("[Pipeline] ✅ Flushed %d events from %s", len(rows), streamKey)
			}
		}
	}
}
