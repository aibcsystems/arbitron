package adapters

import (
	"context"
	"testing"
	"time"

	"arbitron/internal/exchange"
)

func syntheticOrder() exchange.Order {
	return exchange.Order{ID: "SYN-1", Symbol: "BTC/USD", Side: exchange.Sell, Type: exchange.OrderTypeIOC, Quantity: 0.01, LimitPrice: 63000}
}

func TestSyntheticFullFill(t *testing.T) {
	a := NewSyntheticAdapter(SyntheticFillFull, time.Millisecond, 0)
	r := a.SubmitIOC(context.Background(), syntheticOrder())
	if !r.Filled || r.FilledQty != 0.01 {
		t.Fatalf("expected full fill, got %+v", r)
	}
}

func TestSyntheticPartialFill(t *testing.T) {
	a := NewSyntheticAdapter(SyntheticFillPartial, time.Millisecond, 0.4)
	r := a.SubmitIOC(context.Background(), syntheticOrder())
	if !r.Filled || r.FilledQty != 0.004 {
		t.Fatalf("expected 0.004 partial fill, got %+v", r)
	}
}

func TestSyntheticNoFill(t *testing.T) {
	a := NewSyntheticAdapter(SyntheticFillNone, time.Millisecond, 0)
	r := a.SubmitIOC(context.Background(), syntheticOrder())
	if r.Filled || r.FilledQty != 0 || r.Error == nil {
		t.Fatalf("expected no fill with error, got %+v", r)
	}
}

func TestSyntheticHonorsContext(t *testing.T) {
	a := NewSyntheticAdapter(SyntheticFillFull, 100*time.Millisecond, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	r := a.SubmitIOC(ctx, syntheticOrder())
	if r.Error == nil {
		t.Fatal("expected context cancellation")
	}
}
