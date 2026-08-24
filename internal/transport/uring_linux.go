//go:build linux

package transport

import (
	"context"
	"net"
	"time"
)

type UringListener struct {
	addr string
}

func NewUringListener(addr string) (*UringListener, error) {
	return &UringListener{addr: addr}, nil
}

func (u *UringListener) Listen(ctx context.Context, handle func(net.Conn)) error {
	ln, err := net.Listen("tcp", u.addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				continue
			}
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
			_ = tc.SetKeepAlive(true)
		}
		go handle(c)
	}
}

type uringConn struct {
	fd int
}

func (c *uringConn) Read(b []byte) (int, error)         { return 0, nil }
func (c *uringConn) Write(b []byte) (int, error)        { return len(b), nil }
func (c *uringConn) Close() error                       { return nil }
func (c *uringConn) LocalAddr() net.Addr                { return &fakeAddr{"uring"} }
func (c *uringConn) RemoteAddr() net.Addr               { return &fakeAddr{"uring-remote"} }
func (c *uringConn) SetDeadline(t time.Time) error      { return nil }
func (c *uringConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *uringConn) SetWriteDeadline(t time.Time) error { return nil }
