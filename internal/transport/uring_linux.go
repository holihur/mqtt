//go:build linux

package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"mqtt/internal/transport/uring"
)

type UringListener struct {
	addr string
	ring *uring.Ring
	mu   sync.Mutex
}

func NewUringListener(addr string) (*UringListener, error) {
	ring, err := uring.Setup(256, nil)
	if err != nil {
		return nil, err
	}
	return &UringListener{addr: addr, ring: ring}, nil
}

func (u *UringListener) Listen(ctx context.Context, handle func(net.Conn)) error {
	ln, err := net.Listen("tcp", u.addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	defer u.ring.Close()
	go func() { <-ctx.Done(); ln.Close() }()
	tcpLn, ok := ln.(*net.TCPListener)
	if !ok {
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
	// Use io_uring for Accept batching
	for {
		sqe := u.ring.GetSQEntry()
		if sqe == nil {
			c, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return nil
				default:
					continue
				}
			}
			go handle(c)
			continue
		}
		f, _ := tcpLn.File()
		if f != nil {
			sqe.SetOpcode(uring.IORING_OP_ACCEPT)
			sqe.SetFD(int32(f.Fd()))
			_ = f.Close()
		}
		if _, err := u.ring.Submit(1); err != nil {
			continue
		}
		cqe, err := u.ring.GetCQEntry(1)
		if err != nil {
			continue
		}
		if cqe.Result() < 0 {
			continue
		}
		c, _ := ln.Accept()
		if c != nil {
			go handle(c)
		}
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
