package adapters

import (
	"context"
	"fmt"
	"log"
	"time"

	"arbitron/internal/exchange"
)

// SyntheticFillMode controls deterministic fault injection for the in-memory
// synthetic venue. It never touches a network.
type SyntheticFillMode string

const (
	SyntheticFillFull    SyntheticFillMode = "FULL"
	SyntheticFillPartial SyntheticFillMode = "PARTIAL"
	SyntheticFillNone    SyntheticFillMode = "NONE"
)

// SyntheticAdapter is a deterministic, in-memory ExchangeAdapter used only
// by integration tests and the closed-loop pipeline harness.
type SyntheticAdapter struct {
	FillDelay    time.Duration
	Mode         SyntheticFillMode
	PartialRatio float64
}

func NewSyntheticAdapter(mode SyntheticFillMode, fillDelay time.Duration, partialRatio float64) *SyntheticAdapter {
	if mode == "" {
		mode = SyntheticFillFull
	}
	if partialRatio <= 0 || partialRatio >= 1 {
		partialRatio = 0.5
	}
	return &SyntheticAdapter{FillDelay: fillDelay, Mode: mode, PartialRatio: partialRatio}
}

func (s *SyntheticAdapter) Name() string { return "SYNTHETIC_VENUE" }

func (s *SyntheticAdapter) SubmitIOC(ctx context.Context, order exchange.Order) exchange.FillResult {
	start := time.Now()
	delay := s.FillDelay
	if delay <= 0 {
		delay = 2 * time.Millisecond
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return exchange.FillResult{OrderID: order.ID, Exchange: s.Name(), Latency: time.Since(start), Error: ctx.Err()}
	}

	switch s.Mode {
	case SyntheticFillNone:
		return exchange.FillResult{
			OrderID: order.ID, Exchange: s.Name(), Latency: time.Since(start),
			Error: fmt.Errorf("synthetic IOC not filled"),
		}
	case SyntheticFillPartial:
		qty := order.Quantity * s.PartialRatio
		if qty <= 0 {
			return exchange.FillResult{OrderID: order.ID, Exchange: s.Name(), Latency: time.Since(start), Error: fmt.Errorf("synthetic partial fill quantity is zero")}
		}
		log.Printf("[SYNTHETIC_VENUE] (test fixture, not a real venue) PARTIAL %s %s %.6f/%.6f @ %.2f", order.Side, order.Symbol, qty, order.Quantity, order.LimitPrice)
		return exchange.FillResult{OrderID: order.ID, Filled: true, FilledQty: qty, AvgPrice: order.LimitPrice, Exchange: s.Name(), Latency: time.Since(start)}
	case SyntheticFillFull:
		fallthrough
	default:
		log.Printf("[SYNTHETIC_VENUE] (test fixture, not a real venue) FULL %s %s %.6f @ %.2f", order.Side, order.Symbol, order.Quantity, order.LimitPrice)
		return exchange.FillResult{OrderID: order.ID, Filled: true, FilledQty: order.Quantity, AvgPrice: order.LimitPrice, Exchange: s.Name(), Latency: time.Since(start)}
	}
}

func (s *SyntheticAdapter) HealthCheck(context.Context) error { return nil }
