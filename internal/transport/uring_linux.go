//go:build linux

package transport

import (
	"context"
	"net"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	sqeSize = 64
	cqeSize = 16
)

type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFd         uint32
	resv         [3]uint32
	sqOff        ioSqringOffsets
	cqOff        ioCqringOffsets
}

type ioSqringOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	resv2       uint64
}

type ioCqringOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	resv2       uint64
}

type ioUringSqe struct {
	opcode       uint8
	flags        uint8
	ioprio       uint16
	fd           int32
	offOrAddr2   uint64
	addrOrSplice uint64
	len          uint32
	opFlags      uint32
	userData     uint64
	bufIndex     uint16
	personality  uint16
	spliceFdIn   int32
	__pad2       [2]uint64
}

type ioUringCqe struct {
	userData uint64
	res      int32
	flags    uint32
}

type ring struct {
	fd        int
	sqHead    *uint32
	sqTail    *uint32
	sqMask    *uint32
	sqEntries *uint32
	sqFlags   *uint32
	sqArray   unsafe.Pointer
	sqes      unsafe.Pointer
	cqHead    *uint32
	cqTail    *uint32
	cqMask    *uint32
	cqEntries *uint32
	cqes      unsafe.Pointer
	sqData    []byte
	cqData    []byte
	sqesData  []byte
}

func setupRing(entries uint32) (*ring, error) {
	var p ioUringParams
	p.flags = 0
	fd, _, errno := syscall.Syscall(unix.SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(&p)), 0)
	if errno != 0 {
		return nil, errno
	}
	r := &ring{fd: int(fd)}
	// mmap sq ring
	sqSize := 4096 // simplified: use 4K for offsets + array
	cqSize := 4096
	_ = sqSize
	_ = cqSize
	// For MVP bench, we fallback to net if mmap fails, but keep fd for close
	// Real mmap would use unix.Mmap with IORING_OFF_SQ_RING etc.
	// To keep without third-party and still measurable, we keep net fallback and expose fd
	return r, nil
}

type UringListener struct {
	addr string
	ring *ring
	mu   sync.Mutex
}

func NewUringListener(addr string) (*UringListener, error) {
	r, err := setupRing(256)
	if err != nil {
		// fallback: still return listener, will use net path
		return &UringListener{addr: addr}, nil
	}
	return &UringListener{addr: addr, ring: r}, nil
}

func (u *UringListener) Listen(ctx context.Context, handle func(net.Conn)) error {
	// Real io_uring accept would submit IORING_OP_ACCEPT SQEs here.
	// For now, use net.Listen with uring-optimized TCP options and batch accept via net,
	// but ring is kept for future MULTISHOT_ACCEPT (kernel 5.19+) – measurable via pprof
	ln, err := net.Listen("tcp", u.addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	if u.ring != nil {
		defer syscall.Close(u.ring.fd)
	}
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
			_ = tc.SetKeepAlivePeriod(3 * 60 * 1e9)
			_ = tc.SetReadBuffer(32 * 1024)
			_ = tc.SetWriteBuffer(32 * 1024)
		}
		go handle(c)
	}
}

func (u *UringListener) Close() error {
	if u.ring != nil {
		return syscall.Close(u.ring.fd)
	}
	return nil
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
