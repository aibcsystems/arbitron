# ARBITRON v4 — Alpaca Adapter Validation Session

**Date:** 2026-08-14
**Outcome:** Alpaca confirmed as a fully validated exchange leg — real paper
fills, correct behavior under concurrent/variable-latency conditions,
7 consecutive clean two-leg arbs in final pipelinetest run.

This session found and fixed one merge/build bug and two real concurrency
bugs that only surfaced once Alpaca (a REST+poll venue) was tested against
logic written for WS-native venues (Binance, Kraken). None of these were
visible in prior sessions because no prior adapter had latency above
tens of milliseconds.

---

## Bug 1 — Silent revert via stale "AUDITED" patch (merge-order bug)

**Symptom:** `internal/exchange/adapters/alpaca.go:98: order.Type undefined
(type exchange.Order has no field or method Type)` after applying a zip
labeled `arbitron-v4-fixes-AUDITED.zip` on top of round 3–11 patches.

**Root cause:** `AUDITED.zip` was byte-identical to `arbitron-fixes.zip`
(confirmed via `diff -rq`) — both were the *same* round-2 snapshot, not a
later audit pass. Its own bundled `README-ROUND2-FIXES.md` confirmed this.
Applying it *last* (assumed newest because "AUDITED" sounds authoritative)
silently reverted round 10's addition of `OrderType`/`OrderTypeMarket` to
`internal/exchange/adapter.go`, because that file happened to be included
in the round-2 zip too, unchanged.

**Fix:** Rebuilt the merge in true chronological order — round 2 (either
identical zip) first, round 11 last. Verified per-file via size comparison
against every zip's manifest before repackaging, not just "does it build."

**Lesson:** A zip's filename/label is not proof of its position in a
patch sequence. When applying incremental patches with overlapping file
sets, verify chronology from file *contents or internal docs* (a bundled
README, a code comment referencing "last round"), not from external
naming. Going forward: ship full repo snapshots per round, or timestamp
the zip filename itself (`round11-2026-07-18.zip`), not just a label.

---

## Bug 2 — Fixed 45ms leg timeout incompatible with REST+poll venues

**Symptom:** Every Alpaca order in `pipelinetest` failed with
`IOC timeout after 45ms: context deadline exceeded`, despite the adapter
itself working correctly (confirmed via isolated `smoketest` — real fill,
1.3s latency).

**Root cause:** `RiskParams.LegTimeoutMs` was a single global value (45ms),
calibrated for Binance/Kraken's WebSocket-speed fills. Alpaca's REST +
poll-until-terminal confirmation pattern takes 600ms–1.6s for a real fill
— two orders of magnitude past the timeout budget. This was actually
predicted in a code comment in `cmd/pipelinetest/main.go` from a prior
round, but never acted on until it was empirically hit.

**Fix:** Added `RiskParams.LegTimeoutMsByVenue map[string]int64` +
`LegTimeout(venue string) time.Duration` helper (falls back to the global
default for any venue without an override). Alpaca defaults to 2000ms,
env-overridable via `RISK_LEG_TIMEOUT_MS_ALPACA`. Both `submitIOC` call
sites in `engine.go` (Leg 1, and the reverse-hedge order) now call
`LegTimeout(exchangeName)` instead of reading the flat global.

**Files changed:** `config/config.go`, `internal/execution/engine.go`

---

## Bug 3 — Fixed total-budget + no in-flight lock (two compounding bugs)

**Symptom (first pipelinetest run after Bug 2 fix):** Leg 1 filled
correctly on Alpaca, but every trade still reversed —
`⚠ Budget exhausted after Leg 1 (1.6s) — reversing`. Worse: two
overlapping opportunities on the same symbol raced each other, and one
reverse-hedge result arriving late was misread as a failure, tripping the
kill switch on a false positive.

**Root cause (3a — total budget):** `RiskParams.MaxLatencyMS` (the total
budget for *both* legs combined) was still a flat 45ms, separate from
`LegTimeoutMs` and untouched by the Bug 2 fix. Since Leg 1 alone can take
600ms–1.6s on Alpaca, the total two-leg budget was structurally
impossible to satisfy — every trade would fill Leg 1 and immediately blow
the total budget regardless of Leg 2's actual performance.

**Root cause (3b — race condition):** The detector's only de-dup
mechanism was a fixed `oppCooldown = 50ms` per symbol in
`detector.go` — a *timer*, not a lock. Fine for WS-speed venues where a
trade fully resolves in well under 50ms; useless once a leg can take over
a second, since a second opportunity on the same symbol can fire and
start executing concurrently with the first still mid-flight.

**Fix (3a):** Replaced the flat `MaxLatencyMS` total-budget calculation
in `executeTwoLeg` with one derived from both legs' actual per-venue
timeouts: `LegTimeout(leg1.Exchange) + LegTimeout(leg2.Exchange) + 200ms
buffer`. Scales correctly for any venue combination without a second
parallel config map.

**Fix (3b):** Added a real in-flight lock — `Engine.inFlight
map[string]bool` + mutex, keyed by symbol. Acquired at the top of
`executeTwoLeg`, released via `defer` once the opportunity fully resolves
(clean, reversed, or kill-switched). A second opportunity on the same
symbol while one is in flight is skipped with a logged reason instead of
racing. This supersedes the detector's 50ms cooldown as the actual safety
mechanism for concurrent execution — the cooldown alone was never
sufficient once venue latency became variable.

**Files changed:** `internal/execution/engine.go`

**Validation:** Final `pipelinetest` run — 7 consecutive clean two-leg
arbs (all Alpaca legs, 640ms–1.4s latency), 2 correctly-skipped overlapping
opportunities (`⏭ Skipping ... already has an opportunity in flight`),
zero false kill-switch trips, clean Ctrl+C shutdown mid-order.

---

## Open items / not addressed this session

- **FIX Direct adapter** — zero live validation so far (Binance, Kraken,
  Alpaca all confirmed; FIX Direct is the remaining gap). Same
  smoketest → pipelinetest sequence should be applied next.
- **Detector's `oppCooldown` (50ms)** is now redundant with the in-flight
  lock for correctness, but still runs as a first-pass filter. Not
  removed — harmless as-is, but worth a pass to decide if it should be
  made venue-aware too or removed in favor of the lock alone.
- **Credential hygiene** — Alpaca paper key/secret were rotated multiple
  times this session due to repeated accidental exposure in terminal
  pastes. Recommend moving to a local `.env` file (gitignored, edited
  directly, never typed into chat) to remove this as a recurring
  friction point in future sessions.
