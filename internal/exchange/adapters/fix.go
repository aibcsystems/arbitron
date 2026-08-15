// ============================================================
//  internal/exchange/adapters/fix.go
//  Arbitron v4 — FIX Direct Gateway Adapter
//
//  Implements exchange.ExchangeAdapter plus a Connect(ctx) method
//  (called separately by main.go before health checks, since a FIX
//  session must be established before any order traffic is valid).
//
//  This is a minimal, from-scratch FIX 4.4 tag=value engine over a
//  raw TCP socket — not a wrapper around QuickFIX or any FIX
//  library, since none is in go.mod. It implements:
//    - Logon (35=A) with username/password auth (553/554)
//    - Heartbeat (35=0) sent on a fixed interval, and reply to
//      TestRequest (35=1) with a Heartbeat, per the FIX spec
//    - NewOrderSingle (35=D) for IOC limit orders, or Market orders
//      (OrdType=1, no Price tag) when order.Type is OrderTypeMarket
//      — used by execution.reverseHedge to force-flatten an
//      unhedged position.
//    - ExecutionReport (35=8) parsing, matched back to the
//      originating order via ClOrdID (tag 11)
//
//  ⚠ SIMPLIFICATIONS, flagged explicitly rather than hidden:
//    - No resend/gap-fill handling (35=2 ResendRequest, 35=4
//      SequenceReset) — a real venue integration needs this for
//      session recovery after a disconnect mid-sequence.
//    - Treats the FIRST ExecutionReport matching a ClOrdID as
//      final. Real venues can send multiple reports per order
//      (New → PartiallyFilled → Filled/Cancelled); a production
//      version needs an OrdStatus state machine per order, not a
//      single channel send.
//    - No FIX session-level sequence number persistence across
//      restarts (starts at 1 every Connect) — most venues require
//      matching or explicitly resetting sequence numbers on Logon
//      (tag 141 ResetSeqNumFlag), which isn't set here.
//  This is a working FIX session for a single connect-trade-run
//  lifecycle, not a hardened multi-day production session manager.
//  ⚠ NOT LIVE-TESTED against any real venue's FIX gateway — every
//  venue has dialect quirks (custom tags, required fields) on top
//  of the FIX spec; confirm against your counterparty's FIX spec
//  document and a certification/UAT environment before going live.
// ============================================================

package adapters

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"arbitron/internal/exchange"
)

type FIXAdapter struct {
	gatewayURL   string
	senderCompID string
	targetCompID string
	username     string
	password     string

	conn   net.Conn
	reader *bufio.Reader
	connMu sync.Mutex

	seqNum   int
	seqMu    sync.Mutex

	pending   map[string]chan exchange.FillResult
	pendingMu sync.Mutex

	connected      bool
	lastHeartbeat  time.Time
	stateMu        sync.RWMutex
}

func NewFIXAdapter(gatewayURL, senderCompID, targetCompID, username, password string) *FIXAdapter {
	return &FIXAdapter{
		gatewayURL:   gatewayURL,
		senderCompID: senderCompID,
		targetCompID: targetCompID,
		username:     username,
		password:     password,
		pending:      make(map[string]chan exchange.FillResult),
	}
}

func (f *FIXAdapter) Name() string { return "FIX_DIRECT" }

// Connect dials the FIX gateway, sends Logon, and starts the
// background read + heartbeat loops. Must be called (and succeed)
// before SubmitIOC — main.go does this explicitly at startup.
func (f *FIXAdapter) Connect(ctx context.Context) error {
	addr := strings.TrimPrefix(f.gatewayURL, "tcp://")

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("FIX dial %s: %w", addr, err)
	}

	f.connMu.Lock()
	f.conn = conn
	f.reader = bufio.NewReader(conn)
	f.connMu.Unlock()

	f.seqMu.Lock()
	f.seqNum = 1
	f.seqMu.Unlock()

	logon := f.buildMessage("A", map[int]string{
		98:  "0",  // EncryptMethod = none
		108: "30", // HeartBtInt = 30s
		553: f.username,
		554: f.password,
	})
	if err := f.writeMessage(logon); err != nil {
		conn.Close()
		return fmt.Errorf("send Logon: %w", err)
	}

	f.stateMu.Lock()
	f.connected = true
	f.lastHeartbeat = time.Now() // grace period until first real heartbeat
	f.stateMu.Unlock()

	go f.readLoop()
	go f.heartbeatLoop(30 * time.Second)

	return nil
}

// buildMessage assembles a full FIX 4.4 message: standard header
// (BeginString, BodyLength), the given body fields plus the
// mandatory MsgType/Sender/Target/SeqNum/SendingTime header fields,
// and the trailing checksum — per FIX tag=value framing rules.
func (f *FIXAdapter) buildMessage(msgType string, fields map[int]string) []byte {
	f.seqMu.Lock()
	seq := f.seqNum
	f.seqNum++
	f.seqMu.Unlock()

	sendingTime := time.Now().UTC().Format("20060102-15:04:05.000")

	var body strings.Builder
	fmt.Fprintf(&body, "35=%s\x01", msgType)
	fmt.Fprintf(&body, "49=%s\x01", f.senderCompID)
	fmt.Fprintf(&body, "56=%s\x01", f.targetCompID)
	fmt.Fprintf(&body, "34=%d\x01", seq)
	fmt.Fprintf(&body, "52=%s\x01", sendingTime)
	for tag, val := range fields {
		fmt.Fprintf(&body, "%d=%s\x01", tag, val)
	}

	bodyStr := body.String()
	header := fmt.Sprintf("8=FIX.4.4\x019=%d\x01", len(bodyStr))
	full := header + bodyStr

	checksum := 0
	for i := 0; i < len(full); i++ {
		checksum += int(full[i])
	}
	full += fmt.Sprintf("10=%03d\x01", checksum%256)

	return []byte(full)
}

func (f *FIXAdapter) writeMessage(msg []byte) error {
	f.connMu.Lock()
	defer f.connMu.Unlock()
	if f.conn == nil {
		return fmt.Errorf("not connected")
	}
	_, err := f.conn.Write(msg)
	return err
}

// SubmitIOC sends a NewOrderSingle (35=D) with TimeInForce=IOC (3)
// and blocks on a per-order channel until the matching
// ExecutionReport arrives or ctx is cancelled/times out.
func (f *FIXAdapter) SubmitIOC(ctx context.Context, order exchange.Order) exchange.FillResult {
	start := time.Now()

	f.stateMu.RLock()
	connected := f.connected
	f.stateMu.RUnlock()
	if !connected {
		return failResult(order.ID, "FIX_DIRECT", start, fmt.Errorf("FIX session not connected"))
	}

	side := "1" // Buy
	if order.Side == exchange.Sell {
		side = "2"
	}

	ch := make(chan exchange.FillResult, 1)
	f.pendingMu.Lock()
	f.pending[order.ID] = ch
	f.pendingMu.Unlock()
	defer func() {
		f.pendingMu.Lock()
		delete(f.pending, order.ID)
		f.pendingMu.Unlock()
	}()

	// Reverse-hedge orders (engine.go's reverseHedge) carry
	// Type: OrderTypeMarket. Before this branch existed, every
	// NewOrderSingle hardcoded OrdType=2 (Limit) with tag 44
	// (Price) set to order.LimitPrice — for a reverse order that's
	// the zero value, meaning "buy at $0" (never fills) or "sell at
	// $0" (fills by accident). FIX OrdType=1 (Market) per spec
	// carries no Price tag at all — sending tag 44 alongside
	// OrdType=1 is itself a spec violation many venues will reject,
	// so it's omitted entirely for market orders rather than sent
	// as zero.
	fields := map[int]string{
		11: order.ID,
		55: order.Symbol,
		54: side,
		38: strconv.FormatFloat(order.Quantity, 'f', -1, 64),
		59: "3", // TimeInForce = IOC
		60: time.Now().UTC().Format("20060102-15:04:05.000"),
	}
	if order.Type == exchange.OrderTypeMarket {
		fields[40] = "1" // OrdType = Market
	} else {
		fields[40] = "2" // OrdType = Limit
		fields[44] = strconv.FormatFloat(order.LimitPrice, 'f', -1, 64)
	}

	msg := f.buildMessage("D", fields)

	if err := f.writeMessage(msg); err != nil {
		return failResult(order.ID, "FIX_DIRECT", start, fmt.Errorf("send NewOrderSingle: %w", err))
	}

	select {
	case res := <-ch:
		res.Latency = time.Since(start)
		res.Exchange = "FIX_DIRECT"
		return res
	case <-ctx.Done():
		return failResult(order.ID, "FIX_DIRECT", start, fmt.Errorf("IOC timeout: %w", ctx.Err()))
	}
}

// readLoop continuously parses inbound FIX messages and dispatches
// them: ExecutionReports resolve pending SubmitIOC calls by ClOrdID;
// TestRequests get an immediate Heartbeat reply (required by the
// FIX spec to avoid the counterparty disconnecting us); Heartbeats
// update lastHeartbeat for HealthCheck.
func (f *FIXAdapter) readLoop() {
	for {
		fields, err := f.readMessage()
		if err != nil {
			f.stateMu.Lock()
			f.connected = false
			f.stateMu.Unlock()
			return
		}

		switch fields[35] {
		case "8": // ExecutionReport
			f.handleExecutionReport(fields)
		case "1": // TestRequest — must reply with Heartbeat carrying TestReqID (112)
			reply := f.buildMessage("0", map[int]string{112: fields[112]})
			_ = f.writeMessage(reply)
			f.stateMu.Lock()
			f.lastHeartbeat = time.Now()
			f.stateMu.Unlock()
		case "0": // Heartbeat
			f.stateMu.Lock()
			f.lastHeartbeat = time.Now()
			f.stateMu.Unlock()
		case "5": // Logout
			f.stateMu.Lock()
			f.connected = false
			f.stateMu.Unlock()
			return
		}
	}
}

func (f *FIXAdapter) handleExecutionReport(fields map[int]string) {
	clOrdID := fields[11]
	if clOrdID == "" {
		return
	}

	f.pendingMu.Lock()
	ch, ok := f.pending[clOrdID]
	f.pendingMu.Unlock()
	if !ok {
		return // no one waiting (already timed out, or unsolicited report)
	}

	cumQty, _ := strconv.ParseFloat(fields[14], 64)  // CumQty
	avgPx, _ := strconv.ParseFloat(fields[6], 64)     // AvgPx
	ordStatus := fields[39]                            // OrdStatus

	// 1 = PartiallyFilled, 2 = Filled → treat as filled.
	// 4 = Cancelled, 8 = Rejected → treat as not filled.
	filled := (ordStatus == "1" || ordStatus == "2") && cumQty > 0

	select {
	case ch <- exchange.FillResult{
		OrderID:   clOrdID,
		Filled:    filled,
		FilledQty: cumQty,
		AvgPrice:  avgPx,
	}:
	default:
		// receiver already gone (context timed out) — drop safely
	}
}

// heartbeatLoop sends a Heartbeat (35=0) on the negotiated interval,
// as required to keep the FIX session alive.
func (f *FIXAdapter) heartbeatLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		f.stateMu.RLock()
		connected := f.connected
		f.stateMu.RUnlock()
		if !connected {
			return
		}
		hb := f.buildMessage("0", map[int]string{})
		_ = f.writeMessage(hb)
	}
}

// readMessage reads one full FIX message off the wire and parses it
// into a tag→value map. Framing uses the checksum field (10=) as the
// message terminator, which is unambiguous since SOH (\x01) never
// appears inside a well-formed tag=value pair.
func (f *FIXAdapter) readMessage() (map[int]string, error) {
	fields := make(map[int]string)

	for {
		raw, err := f.reader.ReadString('\x01')
		if err != nil {
			return nil, err
		}
		raw = strings.TrimSuffix(raw, "\x01")

		eq := strings.IndexByte(raw, '=')
		if eq < 0 {
			continue // malformed field, skip
		}
		tag, err := strconv.Atoi(raw[:eq])
		if err != nil {
			continue
		}
		fields[tag] = raw[eq+1:]

		if tag == 10 { // checksum field marks end of message
			return fields, nil
		}
	}
}

// HealthCheck reports the session healthy if connected and a
// heartbeat (or the post-Logon grace period) has been seen within
// 2x the heartbeat interval — i.e. we haven't silently lost the
// counterparty without a clean Logout.
func (f *FIXAdapter) HealthCheck(ctx context.Context) error {
	f.stateMu.RLock()
	connected := f.connected
	last := f.lastHeartbeat
	f.stateMu.RUnlock()

	if !connected {
		return fmt.Errorf("FIX session not connected")
	}
	if time.Since(last) > 60*time.Second {
		return fmt.Errorf("FIX session stale: no heartbeat in %v", time.Since(last))
	}
	return nil
}
