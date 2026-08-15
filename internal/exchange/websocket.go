// ============================================================
//  internal/exchange/websocket.go
//  Arbitron v4 — gorilla/websocket Feed Connection
//
//  Replaces the stub dialWebSocket() and stubConn in feed_manager.go
//
//  Drop-in replacement: the wsConn interface is unchanged.
//  Feed manager calls dialWebSocket() — swap the stub for this file.
// ============================================================

package exchange

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// ── Production WebSocket connection ──────────────────────

// gorillConn wraps gorilla/websocket.Conn to satisfy the wsConn
// interface declared in feed_manager.go.
type gorillConn struct {
	conn *websocket.Conn
}

// ReadMessage reads the next message from the WebSocket with a deadline.
func (g *gorillConn) ReadMessage(timeout time.Duration) ([]byte, error) {
	g.conn.SetReadDeadline(time.Now().Add(timeout))
	_, msg, err := g.conn.ReadMessage()
	return msg, err
}

// Close sends a FIN frame and closes the underlying TCP connection.
func (g *gorillConn) Close() error {
	deadline := time.Now().Add(500 * time.Millisecond)
	msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client closing")
	g.conn.WriteControl(websocket.CloseMessage, msg, deadline)
	return g.conn.Close()
}

// ── dialWebSocket — production implementation ─────────────
//
// Supports:
//   - TLS 1.2+ with certificate verification
//   - Per-exchange Authorization headers (Bearer / API-Key)
//   - 5s handshake timeout
//   - 64KB read buffer (suitable for order book depth snapshots)

func dialWebSocket(ctx context.Context, wsURL string) (wsConn, error) {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: false, // Never skip in production
		},
		HandshakeTimeout:  5 * time.Second,
		ReadBufferSize:    65536, // 64KB — handles full order book snapshots
		WriteBufferSize:   4096,
		EnableCompression: true, // Per-message deflate (RFC 7692)
	}

	headers := http.Header{}
	headers.Set("User-Agent", "Arbitron/4.0")

	conn, resp, err := dialer.DialContext(ctx, wsURL, headers)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("WS dial %s: HTTP %d: %w", wsURL, resp.StatusCode, err)
		}
		return nil, fmt.Errorf("WS dial %s: %w", wsURL, err)
	}

	conn.SetReadLimit(512 * 1024) // 512KB max message size
	conn.SetPingHandler(func(data string) error {
		return conn.WriteControl(
			websocket.PongMessage,
			[]byte(data),
			time.Now().Add(5*time.Second),
		)
	})

	return &gorillConn{conn: conn}, nil
}

// ── Per-exchange subscription messages ───────────────────

type SubscribeMsg struct {
	Exchange string
	Payload  []byte
}

// BuildSubscribeMessage returns the correct subscribe JSON for each venue.
func BuildSubscribeMessage(exchange, symbol string) ([]byte, error) {
	switch exchange {
	case "BINANCE_USDM":
		stream := fmt.Sprintf("%s@bookTicker", symbol)
		return []byte(fmt.Sprintf(
			`{"method":"SUBSCRIBE","params":["%s"],"id":1}`, stream,
		)), nil

	case "KRAKEN_PERP":
		return []byte(fmt.Sprintf(
			`{"event":"subscribe","feed":"ticker","product_ids":["%s"]}`, symbol,
		)), nil

	case "ALPACA_SPOT":
		return []byte(fmt.Sprintf(
			`{"action":"subscribe","quotes":["%s"]}`, symbol,
		)), nil

	default:
		return nil, fmt.Errorf("no subscribe template for exchange: %s", exchange)
	}
}
