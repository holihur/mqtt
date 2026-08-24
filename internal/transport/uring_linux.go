//go:build linux

package transport

import (
	"context"
	"net"
	"sync"

	"golang.org/x/sys/unix"
)

type UringListener struct {
	fd  int
	ring *unix.IORing
	mu   sync.Mutex
	addr string
}

func NewUringListener(addr string) (*UringListener, error) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return nil, err
	}
	// placeholder: real io_uring setup would use IORING_SETUP_SQPOLL etc.
	// We keep net.Listener fallback for now and expose uring fd for future SQE batching.
	return &UringListener{fd: fd, addr: addr}, nil
}

func (u *UringListener) Listen(ctx context.Context, handle func(net.Conn)) error {
	// Fallback to net for now; uring Accept batching wired in next iteration via IORING_OP_ACCEPT
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

func (u *UringListener) Close() error {
	if u.ring != nil {
		// u.ring.Close()
	}
	return unix.Close(u.fd)
}
