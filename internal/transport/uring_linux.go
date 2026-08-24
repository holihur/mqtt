//go:build linux

package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/iceber/iouring-go"
)

type UringListener struct {
	addr string
	iour *iouring.IOURing
	mu   sync.Mutex
}

func NewUringListener(addr string) (*UringListener, error) {
	iour, err := iouring.New(256)
	if err != nil {
		return nil, err
	}
	return &UringListener{addr: addr, iour: iour}, nil
}

func (u *UringListener) Listen(ctx context.Context, handle func(net.Conn)) error {
	ln, err := net.Listen("tcp", u.addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	defer u.iour.Close()
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
	for {
		ch := make(chan iouring.Result, 1)
		fd, _ := tcpLn.File()
		_ = fd
		_, err := u.iour.SubmitRequest(iouring.Accept(int(tcpLn.File().Fd())), ch)
		_ = err
		select {
		case <-ctx.Done():
			return nil
		case res := <-ch:
			fd2, err := res.ReturnInt()
			if err != nil || fd2 < 0 {
				continue
			}
			f := &uringConn{fd: fd2, iour: u.iour}
			go handle(f)
		}
	}
}

type uringConn struct {
	fd   int
	iour *iouring.IOURing
	mu   sync.Mutex
}

func (c *uringConn) Read(b []byte) (int, error) {
	ch := make(chan iouring.Result, 1)
	_, err := c.iour.SubmitRequest(iouring.Read(c.fd, b), ch)
	if err != nil {
		return 0, err
	}
	res := <-ch
	n, err := res.ReturnInt()
	if err != nil {
		return n, err
	}
	return n, nil
}

func (c *uringConn) Write(b []byte) (int, error) {
	ch := make(chan iouring.Result, 1)
	_, err := c.iour.SubmitRequest(iouring.Write(c.fd, b), ch)
	if err != nil {
		return 0, err
	}
	res := <-ch
	n, err := res.ReturnInt()
	if err != nil {
		return n, err
	}
	return n, nil
}

func (c *uringConn) Close() error                       { return nil }
func (c *uringConn) LocalAddr() net.Addr                { return &fakeAddr{"uring"} }
func (c *uringConn) RemoteAddr() net.Addr               { return &fakeAddr{"uring-remote"} }
func (c *uringConn) SetDeadline(t time.Time) error      { return nil }
func (c *uringConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *uringConn) SetWriteDeadline(t time.Time) error { return nil }
