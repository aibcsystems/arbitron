// ============================================================
//  internal/exchange/adapter.go
//  Arbitron v4 — Exchange Adapter Interface
// ============================================================

package exchange

import (
	"context"
	"fmt"
	"time"
)

type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

type OrderType string

const (
	OrderTypeIOC    OrderType = "IOC"
	OrderTypeMarket OrderType = "MARKET"
)

type Order struct {
	ID         string
	Symbol     string
	Side       Side
	Type       OrderType
	Quantity   float64
	LimitPrice float64
}

type FillResult struct {
	OrderID   string
	Filled    bool
	FilledQty float64
	AvgPrice  float64
	Exchange  string
	Latency   time.Duration
	Error     error
}

// OrderState is the authoritative exchange-side state returned by
// reconciliation. UNKNOWN is intentionally distinct from NOT_FOUND:
// a local timeout never proves that an order was rejected or absent.
type OrderState string

const (
	OrderStateUnknown          OrderState = "UNKNOWN"
	OrderStatePending          OrderState = "PENDING"
	OrderStatePartiallyFilled  OrderState = "PARTIALLY_FILLED"
	OrderStateFilled           OrderState = "FILLED"
	OrderStateCancelled        OrderState = "CANCELLED"
	OrderStateRejected         OrderState = "REJECTED"
	OrderStateNotFound         OrderState = "NOT_FOUND"
)

type OrderStatus struct {
	OrderID   string
	Exchange  string
	Symbol    string
	State     OrderState
	FilledQty float64
	AvgPrice  float64
	UpdatedAt time.Time
	Error     error
}

// ExchangeAdapter is the minimum venue contract. SubmitIOC must honour
// context cancellation and abort in-flight network I/O when ctx is done.
type ExchangeAdapter interface {
	SubmitIOC(ctx context.Context, order Order) FillResult
	Name() string
	HealthCheck(ctx context.Context) error
}

// OrderReconciler is deliberately optional so existing adapters can be
// migrated incrementally. Execution must treat an adapter without this
// capability as unsafe for ambiguous order states rather than guessing.
type OrderReconciler interface {
	ReconcileOrder(ctx context.Context, orderID, symbol string) OrderStatus
}

type AdapterRegistry struct {
	adapters map[string]ExchangeAdapter
}

func NewAdapterRegistry(adapters ...ExchangeAdapter) *AdapterRegistry {
	r := &AdapterRegistry{adapters: make(map[string]ExchangeAdapter, len(adapters))}
	for _, a := range adapters {
		r.adapters[a.Name()] = a
	}
	return r
}

func (r *AdapterRegistry) Get(exchange string) (ExchangeAdapter, error) {
	a, ok := r.adapters[exchange]
	if !ok {
		return nil, fmt.Errorf("no adapter registered for exchange: %s", exchange)
	}
	return a, nil
}

func (r *AdapterRegistry) SubmitIOC(ctx context.Context, exchange string, order Order) FillResult {
	a, err := r.Get(exchange)
	if err != nil {
		return FillResult{OrderID: order.ID, Exchange: exchange, Error: err}
	}
	return a.SubmitIOC(ctx, order)
}

// ReconcileOrder returns the venue's authoritative order state when the
// adapter supports it. Unsupported reconciliation is fail-closed.
func (r *AdapterRegistry) ReconcileOrder(ctx context.Context, exchange, orderID, symbol string) OrderStatus {
	a, err := r.Get(exchange)
	if err != nil {
		return OrderStatus{OrderID: orderID, Exchange: exchange, Symbol: symbol, State: OrderStateUnknown, Error: err}
	}
	reconciler, ok := a.(OrderReconciler)
	if !ok {
		return OrderStatus{OrderID: orderID, Exchange: exchange, Symbol: symbol, State: OrderStateUnknown, Error: fmt.Errorf("adapter %s does not support order reconciliation", exchange)}
	}
	return reconciler.ReconcileOrder(ctx, orderID, symbol)
}

func (r *AdapterRegistry) HealthCheckAll(ctx context.Context) map[string]error {
	results := make(map[string]error, len(r.adapters))
	for name, adapter := range r.adapters {
		results[name] = adapter.HealthCheck(ctx)
	}
	return results
}
