//go:build !linux

package transport

import (
	"context"
	"net"
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
		go handle(c)
	}
}

func (u *UringListener) Close() error { return nil }
