#!/usr/bin/env bash
# Loads .env into the current shell session.
#
# Usage:
#   source ./load-env.sh
#
# (Must be *sourced*, not executed directly — `./load-env.sh` alone
# would only export vars into a subshell that exits immediately,
# leaving your actual terminal session unchanged.)

if [ ! -f "$(dirname "${BASH_SOURCE[0]}")/.env" ]; then
    echo "No .env file found. Copy .env.example to .env and fill in real values first:"
    echo "  cp .env.example .env"
    return 1 2>/dev/null || exit 1
fi

set -a
source "$(dirname "${BASH_SOURCE[0]}")/.env"
set +a

echo "Environment loaded from .env"
echo "  ALPACA_API_KEY:   $([ -n "$ALPACA_API_KEY" ] && echo "set (${#ALPACA_API_KEY} chars)" || echo "not set")"
echo "  BINANCE_API_KEY:  $([ -n "$BINANCE_API_KEY" ] && echo "set (${#BINANCE_API_KEY} chars)" || echo "not set")"
echo "  KRAKEN_API_KEY:   $([ -n "$KRAKEN_API_KEY" ] && echo "set (${#KRAKEN_API_KEY} chars)" || echo "not set")"
echo "  FIX_USERNAME:     $([ -n "$FIX_USERNAME" ] && echo "set" || echo "not set (parked — no counterparty yet)")"
