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
	"arbitron/internal/exchange"
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

// VenueHealthProvider exposes the execution gate's authoritative venue state.
// Keeping this as a narrow interface prevents the detector from depending on
// FeedManager internals and makes the gate deterministic in tests.
type VenueHealthProvider interface {
	GetHealth() map[string]exchange.FeedHealth
}

type Detector struct {
	cfg    *config.Config
	engine *Engine
	health VenueHealthProvider

	quotes   map[string]Quote
	quotesMu sync.RWMutex

	lastOpp   map[string]time.Time
	lastOppMu sync.Mutex

	minSpreadBps float64
}

// NewDetector accepts an optional venue health provider. Without one the
// detector retains legacy behavior for isolated callers/tests; production
// wiring supplies FeedManager so stale/disconnected venues cannot generate
// executable opportunities.
func NewDetector(cfg *config.Config, engine *Engine, health ...VenueHealthProvider) *Detector {
	var provider VenueHealthProvider
	if len(health) > 0 {
		provider = health[0]
	}
	return &Detector{
		cfg:          cfg,
		engine:       engine,
		health:       provider,
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

func (d *Detector) venueHealthy(exchangeName string) bool {
	if d.health == nil {
		return true
	}
	health, ok := d.health.GetHealth()[exchangeName]
	if !ok {
		return false
	}
	// FeedManager marks OPEN only after a live connection is established.
	// CLOSED is deliberately not executable: it represents a disconnected
	// or shutdown feed, while HALF_OPEN is a reconnect probe state.
	return health.State == exchange.CircuitOpen &&
		health.LastMessage.IsZero() == false &&
		time.Since(health.LastMessage) <= time.Duration(maxQuoteAgeMs)*time.Millisecond
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
		if !d.venueHealthy(q.Exchange) {
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
		return
	}

	now := time.Now()
	op := ArbOpportunity{
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
		SpreadBps:   spreadBps,
		DetectedAt:  now,
	}

	d.lastOppMu.Lock()
	d.lastOpp[symbol] = now
	d.lastOppMu.Unlock()

	log.Printf("[Detector] 🎯 Opportunity: %s | %.2fbps | Buy %s Ask %.4f → Sell %s Bid %.4f",
		symbol, spreadBps,
		bestBuy.Exchange, bestBuy.Ask,
		bestSell.Exchange, bestSell.Bid,
	)

	d.engine.Submit(op)
}

// ── calcQuantity — risk-driven position sizing ────────────
func (d *Detector) calcQuantity(q Quote, spreadBps float64) float64 {
	if q.Ask <= 0 {
		return 0
	}

	rp := d.cfg.RiskParams
	baseNotional := rp.BaseRiskPct * rp.DailyLossLimit
	if baseNotional <= 0 {
		baseNotional = rp.MinPositionUSD
	}

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
		return 0
	}

	return notional / q.Ask
}
