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
}

func NewConn(raw net.Conn, maxPacketSize int) *Conn {
	return &Conn{
		raw:     raw,
		reader:  bufio.NewReader(raw),
		parser:  parser.NewReader(raw, maxPacketSize),
		version: 0,
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
	// Use version-aware decode if we know version
	if c.version == codec.ProtocolV5 {
		p, err := codec.DecodeWithVersion(frame, c.version)
		if err == nil && slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			slog.Debug("packet recv decoded", "client", c.clientID, "type", p.Type, "topic", p.Topic, "packetID", p.PacketID)
		}
		return p, err
	}
	// generic decode, but try to infer version from frame if it's CONNECT
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
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	_, err = c.raw.Write(data)
	return err
}

func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	if c.onClose != nil {
		c.onClose()
	}
	return c.raw.Close()
}

func (c *Conn) SetOnClose(fn func())          { c.onClose = fn }
func (c *Conn) SetDeadline(t time.Time) error { return c.raw.SetDeadline(t) }
