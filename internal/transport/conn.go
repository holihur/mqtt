package transport

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/parser"
)

type Conn struct {
	raw      net.Conn
	reader   *bufio.Reader
	writerMu sync.Mutex
	parser   *parser.Reader
	version  byte
	clientID string
	closed   bool
	mu       sync.Mutex
	onClose  func()

	// writeTimeout bounds each WritePacket so a stalled peer cannot pin the
	// calling goroutine forever (writes are synchronous in deliver paths)
	writeTimeout time.Duration
}

func NewConn(raw net.Conn, maxPacketSize int) *Conn {
	return &Conn{
		raw:          raw,
		reader:       bufio.NewReader(raw),
		parser:       parser.NewReader(raw, maxPacketSize),
		version:      0,
		writeTimeout: 10 * time.Second,
	}
}

func (c *Conn) SetVersion(v byte)     { c.version = v }
func (c *Conn) Version() byte         { return c.version }
func (c *Conn) SetClientID(id string) { c.clientID = id }
func (c *Conn) ClientID() string      { return c.clientID }
func (c *Conn) RemoteAddr() string    { return c.raw.RemoteAddr().String() }
func (c *Conn) Raw() net.Conn         { return c.raw }

func (c *Conn) ReadPacket() (*codec.Packet, error) {
	frame, err := c.parser.ReadFrame()
	if err != nil {
		return nil, err
	}
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		hexStr := fmt.Sprintf("%x", frame)
		if len(hexStr) > 512 {
			hexStr = hexStr[:512] + "..."
		}
		slog.Debug("packet recv raw", "client", c.clientID, "hex", hexStr, "len", len(frame))
	}
	// Use version-aware decode once the client version is known (after CONNECT).
	// This eliminates the v3/v5 ambiguity that the generic Decode path has to
	// guess at: a v3 SUBACK whose first reason code is 0x00, or a v3 PUBLISH
	// whose payload happens to look like a properties block, are parsed
	// correctly instead of being misread as v5.
	if c.version != 0 {
		p, err := codec.DecodeWithVersion(frame, c.version)
		if err == nil && slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			slog.Debug("packet recv decoded", "client", c.clientID, "type", p.Type, "topic", p.Topic, "packetID", p.PacketID)
		}
		return p, err
	}
	// generic decode (only used for the initial CONNECT frame)
	p, err := codec.Decode(frame)
	if err == nil && slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("packet recv decoded", "client", c.clientID, "type", p.Type, "topic", p.Topic, "packetID", p.PacketID)
	}
	return p, err
}

func (c *Conn) WritePacket(p *codec.Packet) error {
	data, err := codec.Encode(p)
	if err != nil {
		return err
	}
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		hexStr := fmt.Sprintf("%x", data)
		if len(hexStr) > 512 {
			hexStr = hexStr[:512] + "..."
		}
		slog.Debug("packet send", "client", c.clientID, "type", p.Type, "hex", hexStr, "len", len(data))
	}
	return c.writeRaw(data)
}

// WriteRaw writes an already-encoded frame to the wire.  Used by the broker
// fan-out fast path: the same QoS0 PUBLISH frame is encoded once and shared
// with every matching subscriber, avoiding one encode + allocation per
// subscriber.  Callers must not mutate data while this is in flight (the write
// is synchronous, so reusing the buffer after the call returns is safe).
func (c *Conn) WriteRaw(data []byte) error {
	return c.writeRaw(data)
}

func (c *Conn) writeRaw(data []byte) error {
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	if c.writeTimeout > 0 {
		_ = c.raw.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	_, err := c.raw.Write(data)
	return err
}

func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	fn := c.onClose
	c.mu.Unlock()
	if fn != nil {
		fn()
	}
	return c.raw.Close()
}

func (c *Conn) SetOnClose(fn func()) {
	c.mu.Lock()
	c.onClose = fn
	c.mu.Unlock()
}
func (c *Conn) SetDeadline(t time.Time) error { return c.raw.SetDeadline(t) }
