// ============================================================
//  internal/exchange/adapter.go
//  Arbitron v4 — Exchange Adapter Interface
//
//  Every venue (Binance, Kraken, Alpaca, FIX) implements this
//  interface. The execution engine calls SubmitIOC() without
//  knowing which exchange it's talking to.
//
//  AdapterRegistry maps exchange names → adapter instances so
//  submitToExchange() in engine.go becomes a one-liner lookup.
// ============================================================

package exchange

import (
	"context"
	"fmt"
	"time"
)

// ── Core order types (duplicated from execution to avoid import cycle) ──

type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

// Order is the canonical order struct passed to every adapter.
// OrderType distinguishes limit vs market orders at the adapter
// level. Added because reverseHedge (engine.go) builds a MARKET
// order to flatten unhedged exposure — but before this field
// existed, exchange.Order had no way to carry that intent, so the
// order silently became a $0.00 limit order (LimitPrice's zero
// value) and got rejected by the venue. String values match
// execution.OrderType exactly ("IOC", "MARKET") so the cast at the
// call site (engine.go's submitIOC) needs no translation table.
type OrderType string

const (
	OrderTypeIOC    OrderType = "IOC"
	OrderTypeMarket OrderType = "MARKET"
)

type Order struct {
	ID         string
	Symbol     string
	Side       Side
	Type       OrderType // zero value "" is treated as IOC/limit by all adapters
	Quantity   float64
	LimitPrice float64 // IOC: max acceptable buy / min acceptable sell. Ignored for MARKET.
}

// FillResult is returned synchronously by every adapter.
type FillResult struct {
	OrderID   string
	Filled    bool
	FilledQty float64
	AvgPrice  float64
	Exchange  string
	Latency   time.Duration
	Error     error
}

// ── Adapter interface ─────────────────────────────────────

// ExchangeAdapter is the contract every venue must satisfy.
// SubmitIOC must honour ctx cancellation — when ctx is Done,
// any in-flight network I/O must be aborted immediately.
type ExchangeAdapter interface {
	// SubmitIOC places an Immediate-Or-Cancel order.
	// Returns a FillResult regardless of outcome — never panics.
	SubmitIOC(ctx context.Context, order Order) FillResult

	// Name returns the canonical venue identifier used in logs
	// and fee schedule lookups.
	Name() string

	// HealthCheck returns nil if the adapter's connection is alive.
	HealthCheck(ctx context.Context) error
}

// ── Registry ─────────────────────────────────────────────

// AdapterRegistry holds all live exchange adapters keyed by
// canonical name. Thread-safe after initial construction.
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

// Get returns the adapter for a given exchange name.
func (r *AdapterRegistry) Get(exchange string) (ExchangeAdapter, error) {
	a, ok := r.adapters[exchange]
	if !ok {
		return nil, fmt.Errorf("no adapter registered for exchange: %s", exchange)
	}
	return a, nil
}

// SubmitIOC is the drop-in replacement for the submitToExchange stub.
// Wire this into execution/engine.go replacing the stub call.
func (r *AdapterRegistry) SubmitIOC(ctx context.Context, exchange string, order Order) FillResult {
	a, err := r.Get(exchange)
	if err != nil {
		return FillResult{
			OrderID:  order.ID,
			Exchange: exchange,
			Error:    err,
		}
	}
	return a.SubmitIOC(ctx, order)
}

// HealthCheckAll runs HealthCheck on every registered adapter.
// Called at startup and by the telemetry gateway's /health route.
func (r *AdapterRegistry) HealthCheckAll(ctx context.Context) map[string]error {
	results := make(map[string]error, len(r.adapters))
	for name, adapter := range r.adapters {
		results[name] = adapter.HealthCheck(ctx)
	}
	return results
}
