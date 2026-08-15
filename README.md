# ARBITRON v4

Autonomous multi-exchange arbitrage system. Go-based, event-driven, with
exchange adapters, a spread detector, a risk engine, an execution engine,
and a deterministic synthetic venue for fault-injection testing.

## Status

Build-phase complete. Binance, Kraken, and Alpaca adapters validated via
real paper-trading fills (15 clean arb cycles logged). FIX Direct is parked
(P3) pending a business development counterparty relationship.

Core packages (`internal/execution`, `internal/risk`,
`internal/exchange/adapters`) build clean and pass their full test suite in
isolation. `internal/persistence`, `internal/redis`, `internal/telemetry`,
and `internal/exchange/websocket.go` depend on external modules
(`jackc/pgx`, `redis/go-redis`, `gorilla/websocket`) and require normal
network/module access to build — see `go.mod`.

## Layout

```
cmd/arbitron/       main entrypoint — wires config, adapters, risk,
                    execution engine, detector, telemetry gateway
cmd/pipelinetest/   Redis stream pipeline integration harness
cmd/smoketest/      lightweight startup smoke test
config/             typed config (venue credentials, fee schedule, risk params)
internal/exchange/  adapter interface + Binance/Kraken/Alpaca/FIX adapters,
                    websocket feed manager, synthetic in-memory venue
internal/execution/ core state machine: opportunity → leg1 → leg2 →
                    clean/reversed/kill-switch, plus the opportunity detector
internal/risk/      pre-trade risk checks, kill switch, daily loss limits
internal/redis/     Redis client + streams pipeline (ticks, executions)
internal/persistence/ pgx COPY-protocol batch inserter for trade history
internal/telemetry/ HTTP gateway (/health, adapter status)
```

## Getting started

```bash
cp .env.example .env      # fill in exchange credentials
./load-env.sh
go build ./...
go test ./...
```

Recommended next step before any live capital decision: an extended
unattended paper-trading run via `cmd/arbitron` to build a behavioral
track record.

## Recent fixes

- **Reverse-hedge slippage on partial leg-2 fills** (`internal/execution/engine.go`):
  `calculateReverseSlippage` was pricing the reversal against `leg1`'s full
  filled quantity instead of the quantity actually being reversed, producing
  phantom PnL swings (~$168 on a 0.006 BTC reversal) whenever leg 2 partially
  filled. Fixed to cost the reversal against `reverse.FilledQty`, with a
  regression assertion in `TestPartialLeg2ReversesOnlyResidual`.
- See `README-ROUND2-FIXES.md` and `FIXES-ALPACA-VALIDATION.md` for the
  full history of compile-blocking and runtime fixes across prior rounds
  (import cycles, duplicate `dialWebSocket`, `$0.00` limit-price bug,
  Alpaca REST-polling timeout mismatch, per-symbol `inFlight` race).
