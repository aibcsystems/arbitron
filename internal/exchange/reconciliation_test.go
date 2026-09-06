package exchange

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type reconciliationTestAdapter struct {
	status OrderStatus
}

func (a *reconciliationTestAdapter) SubmitIOC(context.Context, Order) FillResult {
	return FillResult{OrderID: "test-order", Exchange: a.Name()}
}

func (a *reconciliationTestAdapter) Name() string { return "TEST" }

func (a *reconciliationTestAdapter) HealthCheck(context.Context) error { return nil }

func (a *reconciliationTestAdapter) ReconcileOrder(context.Context, string, string) OrderStatus {
	return a.status
}

func TestAdapterRegistryReconcileOrder(t *testing.T) {
	want := OrderStatus{
		OrderID:   "o-1",
		Exchange:  "TEST",
		Symbol:    "BTC/USD",
		State:     OrderStatePartiallyFilled,
		FilledQty: 0.25,
		AvgPrice:  50000,
		UpdatedAt: time.Now(),
	}

	registry := NewAdapterRegistry(&reconciliationTestAdapter{status: want})
	got := registry.ReconcileOrder(context.Background(), "TEST", "o-1", "BTC/USD")

	if got.State != want.State || got.FilledQty != want.FilledQty || got.AvgPrice != want.AvgPrice {
		t.Fatalf("unexpected reconciliation result: got %+v want %+v", got, want)
	}
}

func TestAdapterRegistryReconcileOrderFailsClosedWithoutCapability(t *testing.T) {
	adapter := &reconciliationNoCapabilityAdapter{}
	registry := NewAdapterRegistry(adapter)

	got := registry.ReconcileOrder(context.Background(), adapter.Name(), "o-1", "BTC/USD")
	if got.State != OrderStateUnknown {
		t.Fatalf("expected UNKNOWN, got %s", got.State)
	}
	if got.Error == nil {
		t.Fatal("expected reconciliation capability error")
	}
}

type reconciliationNoCapabilityAdapter struct{}

func (*reconciliationNoCapabilityAdapter) SubmitIOC(context.Context, Order) FillResult {
	return FillResult{}
}

func (*reconciliationNoCapabilityAdapter) Name() string { return "NO_RECON" }

func (*reconciliationNoCapabilityAdapter) HealthCheck(context.Context) error {
	return fmt.Errorf("not implemented")
}
