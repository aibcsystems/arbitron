// ============================================================
//  internal/persistence/copier.go
//  Arbitron v4 — pgx COPY Protocol Batch Inserter
// ============================================================

package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool struct {
	pool *pgxpool.Pool
}

func NewPool(ctx context.Context, postgresURL string) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(postgresURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres URL: %w", err)
	}

	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgxpool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	log.Println("[Persistence] ✅ PostgreSQL pool connected")
	return &Pool{pool: pool}, nil
}

func (p *Pool) Close() { p.pool.Close() }

type Copier struct {
	pool *Pool
}

func NewCopier(pool *Pool) *Copier {
	return &Copier{pool: pool}
}

func (c *Copier) Insert(ctx context.Context, streamKey string, rows []string) error {
	switch streamKey {
	case "arbitron:telemetry":
		return c.insertTelemetry(ctx, rows)
	case "arbitron:execution":
		return c.insertExecutions(ctx, rows)
	default:
		return fmt.Errorf("unknown stream key: %s", streamKey)
	}
}

type telemetryRow struct {
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

func (c *Copier) insertTelemetry(ctx context.Context, rows []string) error {
	parsed := make([]telemetryRow, 0, len(rows))
	for _, raw := range rows {
		var r telemetryRow
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			log.Printf("[Persistence] ⚠ Skip malformed telemetry row: %v", err)
			continue
		}
		parsed = append(parsed, r)
	}

	if len(parsed) == 0 {
		return nil
	}

	conn, err := c.pool.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{"arbitron_telemetry"},
		[]string{"ts", "p50_ms", "p95_ms", "p99_ms", "stress_pct",
			"pnl", "orders_sec", "fill_rate", "clock_drift", "created_at"},
		pgx.CopyFromSlice(len(parsed), func(i int) ([]interface{}, error) {
			r := parsed[i]
			return []interface{}{
				r.Timestamp, r.P50, r.P95, r.P99, r.Stress,
				r.PnL, r.OrdersSec, r.FillRate, r.ClockDrift,
				time.UnixMilli(r.Timestamp).UTC(),
			}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("COPY telemetry: %w", err)
	}

	log.Printf("[Persistence] ✅ COPY %d telemetry rows", len(parsed))
	return nil
}

type executionRow struct {
	OpportunityID string `json:"opportunity_id"`
	Outcome       string `json:"outcome"`
	Leg1Result    struct {
		Exchange  string  `json:"exchange"`
		Filled    bool    `json:"filled"`
		FilledQty float64 `json:"filled_qty"`
		AvgPrice  float64 `json:"avg_price"`
		LatencyMs float64 `json:"latency_ms"`
	} `json:"leg1_result"`
	Leg2Result struct {
		Exchange  string  `json:"exchange"`
		Filled    bool    `json:"filled"`
		FilledQty float64 `json:"filled_qty"`
		AvgPrice  float64 `json:"avg_price"`
		LatencyMs float64 `json:"latency_ms"`
	} `json:"leg2_result"`
	PnL          float64 `json:"pnl"`
	TotalLatency float64 `json:"total_latency_ms"`
	Timestamp    int64   `json:"ts"`
}

func (c *Copier) insertExecutions(ctx context.Context, rows []string) error {
	parsed := make([]executionRow, 0, len(rows))
	for _, raw := range rows {
		var r executionRow
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			log.Printf("[Persistence] ⚠ Skip malformed execution row: %v", err)
			continue
		}
		parsed = append(parsed, r)
	}

	if len(parsed) == 0 {
		return nil
	}

	conn, err := c.pool.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{"arbitron_executions"},
		[]string{
			"opportunity_id", "outcome",
			"leg1_exchange", "leg1_filled", "leg1_qty", "leg1_avg_price", "leg1_latency_ms",
			"leg2_exchange", "leg2_filled", "leg2_qty", "leg2_avg_price", "leg2_latency_ms",
			"pnl", "total_latency_ms", "created_at",
		},
		pgx.CopyFromSlice(len(parsed), func(i int) ([]interface{}, error) {
			r := parsed[i]
			return []interface{}{
				r.OpportunityID, r.Outcome,
				r.Leg1Result.Exchange, r.Leg1Result.Filled, r.Leg1Result.FilledQty, r.Leg1Result.AvgPrice, r.Leg1Result.LatencyMs,
				r.Leg2Result.Exchange, r.Leg2Result.Filled, r.Leg2Result.FilledQty, r.Leg2Result.AvgPrice, r.Leg2Result.LatencyMs,
				r.PnL, r.TotalLatency,
				time.UnixMilli(r.Timestamp).UTC(),
			}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("COPY executions: %w", err)
	}

	log.Printf("[Persistence] ✅ COPY %d execution rows", len(parsed))
	return nil
}

func (c *Copier) Ping(ctx context.Context) error {
	return c.pool.pool.Ping(ctx)
}

type PoolStats struct {
	TotalConns    int32
	IdleConns     int32
	AcquiredConns int32
}

func (c *Copier) Stats() PoolStats {
	s := c.pool.pool.Stat()
	return PoolStats{
		TotalConns:    s.TotalConns(),
		IdleConns:     s.IdleConns(),
		AcquiredConns: s.AcquiredConns(),
	}
}
