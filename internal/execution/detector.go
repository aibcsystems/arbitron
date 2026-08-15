// ============================================================
//  internal/execution/detector.go
//  Arbitron v4 — Spatial Arbitrage Opportunity Detector
// ============================================================

package execution

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"arbitron/config"
	"arbitron/internal/redis"
)

const (
	defaultMinSpreadBps = 15.0
	oppCooldown         = 50 * time.Millisecond
	maxQuoteAgeMs       = 200
)

type Quote struct {
	Exchange  string
	Symbol    string
	Bid       float64
	Ask       float64
	UpdatedAt time.Time
}

type Detector struct {
	cfg    *config.Config
	engine *Engine

	quotes   map[string]Quote
	quotesMu sync.RWMutex

	lastOpp   map[string]time.Time
	lastOppMu sync.Mutex

	minSpreadBps float64
}

func NewDetector(cfg *config.Config, engine *Engine) *Detector {
	return &Detector{
		cfg:          cfg,
		engine:       engine,
		quotes:       make(map[string]Quote),
		lastOpp:      make(map[string]time.Time),
		minSpreadBps: defaultMinSpreadBps,
	}
}

func (d *Detector) Start(ctx context.Context, pipeline *redis.StreamPipeline) {
	tickCh := pipeline.SubscribeTicks()
	defer pipeline.UnsubscribeTicks(tickCh)

	log.Println("[Detector] Arbitrage detector running...")

	for {
		select {
		case <-ctx.Done():
			log.Println("[Detector] Shutting down.")
			return
		case tick, ok := <-tickCh:
			if !ok {
				return
			}
			d.onTick(tick)
		}
	}
}

func (d *Detector) onTick(tick redis.PriceTick) {
	if tick.Bid <= 0 || tick.Ask <= 0 {
		return
	}

	key := tick.Exchange + ":" + tick.Symbol

	d.quotesMu.Lock()
	d.quotes[key] = Quote{
		Exchange:  tick.Exchange,
		Symbol:    tick.Symbol,
		Bid:       tick.Bid,
		Ask:       tick.Ask,
		UpdatedAt: time.Now(),
	}
	d.quotesMu.Unlock()

	d.scanSymbol(tick.Symbol)
}

func (d *Detector) scanSymbol(symbol string) {
	d.lastOppMu.Lock()
	if last, ok := d.lastOpp[symbol]; ok && time.Since(last) < oppCooldown {
		d.lastOppMu.Unlock()
		return
	}
	d.lastOppMu.Unlock()

	d.quotesMu.RLock()
	defer d.quotesMu.RUnlock()

	var quotes []Quote
	for _, q := range d.quotes {
		if q.Symbol != symbol {
			continue
		}
		if time.Since(q.UpdatedAt) > time.Duration(maxQuoteAgeMs)*time.Millisecond {
			continue
		}
		quotes = append(quotes, q)
	}

	if len(quotes) < 2 {
		return
	}

	var bestBuy, bestSell Quote
	bestBuy.Ask = 1e18
	bestSell.Bid = 0

	for _, q := range quotes {
		if q.Ask < bestBuy.Ask {
			bestBuy = q
		}
		if q.Bid > bestSell.Bid {
			bestSell = q
		}
	}

	if bestBuy.Exchange == bestSell.Exchange {
		return
	}

	mid := (bestBuy.Ask + bestSell.Bid) / 2.0
	if mid <= 0 {
		return
	}
	spreadBps := ((bestSell.Bid - bestBuy.Ask) / mid) * 10000.0

	if spreadBps < d.minSpreadBps {
		return
	}

	oppID := fmt.Sprintf("OPP-%s-%d", symbol, time.Now().UnixMicro())

	qty := d.calcQuantity(bestBuy, spreadBps)
	if qty <= 0 {
		// Sized below MinPositionUSD — not worth the round-trip fees
		// and likely to be rejected by exchange minimum-order rules.
		return
	}

	opp := ArbOpportunity{
		ID: oppID,
		Leg1: Order{
			ID:         oppID + "-L1",
			Exchange:   bestBuy.Exchange,
			Symbol:     symbol,
			Side:       SideBuy,
			Type:       OrderIOC,
			Quantity:   qty,
			LimitPrice: bestBuy.Ask * 1.0001,
		},
		Leg2: Order{
			ID:         oppID + "-L2",
			Exchange:   bestSell.Exchange,
			Symbol:     symbol,
			Side:       SideSell,
			Type:       OrderIOC,
			Quantity:   qty,
			LimitPrice: bestSell.Bid * 0.9999,
		},
		SpreadBps:  spreadBps,
		DetectedAt: time.Now(),
	}

	d.lastOppMu.Lock()
	d.lastOpp[symbol] = time.Now()
	d.lastOppMu.Unlock()

	log.Printf("[Detector] 🎯 Opportunity: %s | %.2fbps | Buy %s Ask %.4f → Sell %s Bid %.4f",
		symbol, spreadBps,
		bestBuy.Exchange, bestBuy.Ask,
		bestSell.Exchange, bestSell.Bid,
	)

	d.engine.Submit(opp)
}

// ── calcQuantity — risk-driven position sizing ────────────
//
// P1 FIX: previously hardcoded to return 0.01 regardless of spread
// or price — every opportunity sized identically, with no relation
// to risk budget, spread quality, or notional value. That's a real
// gap ahead of live capital: a 15bps spread and a 200bps spread got
// the same size, and a $100 asset and a $100,000 asset got the same
// unit quantity (wildly different notional exposure).
//
// Model:
//   1. Base notional = BaseRiskPct × DailyLossLimit — the USD size a
//      merely-adequate opportunity (spread == minSpreadBps) is allowed.
//   2. Confidence scaling: wider spreads (more edge, more margin for
//      slippage) scale the notional up, capped at 2x base. This is a
//      confidence multiplier, not a leverage mechanism — it never
//      exceeds MaxPositionUSD regardless of how wide the spread is.
//   3. Hard clamp to [MinPositionUSD, MaxPositionUSD]. Below the floor,
//      caller skips the opportunity entirely (fees would eat the edge
//      and most exchanges reject sub-minimum orders anyway).
//   4. Convert USD notional → base-asset quantity using the reference
//      price (the buy-side ask, since that's the leg that sets cost).
//
// This still isn't full portfolio-aware sizing (no correlation across
// concurrent open positions, no per-symbol exposure caps) — that's a
// reasonable P2 improvement once real fill data exists to calibrate
// against. This fixes the immediate gap: size is no longer constant
// and no longer decoupled from risk budget or spread quality.
func (d *Detector) calcQuantity(q Quote, spreadBps float64) float64 {
	if q.Ask <= 0 {
		return 0
	}

	rp := d.cfg.RiskParams

	baseNotional := rp.BaseRiskPct * rp.DailyLossLimit
	if baseNotional <= 0 {
		baseNotional = rp.MinPositionUSD
	}

	// Confidence multiplier: linear scale from 1x at minSpreadBps up to
	// 2x at 3×minSpreadBps or wider. Never exceeds 2x — width alone
	// should not justify unbounded sizing.
	confidence := 1.0
	if d.minSpreadBps > 0 {
		confidence = 1.0 + (spreadBps-d.minSpreadBps)/(2*d.minSpreadBps)
		if confidence > 2.0 {
			confidence = 2.0
		}
		if confidence < 1.0 {
			confidence = 1.0
		}
	}

	notional := baseNotional * confidence
	if notional > rp.MaxPositionUSD {
		notional = rp.MaxPositionUSD
	}
	if notional < rp.MinPositionUSD {
		return 0 // caller treats <= 0 as "skip this opportunity"
	}

	return notional / q.Ask
}
