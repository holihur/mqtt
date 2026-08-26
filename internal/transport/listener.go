package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type Listener struct {
	addr           string
	tlsCfg         *tls.Config
	wsAddr         string
	ln             net.Listener
	customListener net.Listener
	wsSrv          *http.Server

	wsAllowOrigins map[string]struct{}
	wsAllowAll     bool
}

func NewListener(addr string, tlsCfg *tls.Config, wsAddr string) *Listener {
	return &Listener{addr: addr, tlsCfg: tlsCfg, wsAddr: wsAddr, wsAllowOrigins: make(map[string]struct{})}
}

func (l *Listener) SetCustomListener(ln net.Listener) { l.customListener = ln }

func (l *Listener) SetWsAllowOrigins(origins []string) {
	m := make(map[string]struct{}, len(origins))
	allowAll := false
	for _, o := range origins {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			allowAll = true
			continue
		}
		if u, err := url.Parse(o); err == nil && u.Host != "" {
			m[u.Host] = struct{}{}
			m[u.Hostname()] = struct{}{}
			m[o] = struct{}{}
		} else {
			m[o] = struct{}{}
		}
	}
	l.wsAllowOrigins = m
	l.wsAllowAll = allowAll
}

func (l *Listener) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	if l.wsAllowAll {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	originHost := u.Host
	if originHost == r.Host {
		return true
	}
	if _, ok := l.wsAllowOrigins[origin]; ok {
		return true
	}
	if _, ok := l.wsAllowOrigins[originHost]; ok {
		return true
	}
	if _, ok := l.wsAllowOrigins[u.Hostname()]; ok {
		return true
	}
	return false
}

func (l *Listener) upgrader() websocket.Upgrader {
	return websocket.Upgrader{CheckOrigin: l.checkOrigin}
}

func (l *Listener) Addr() string {
	if l.ln != nil {
		return l.ln.Addr().String()
	}
	if l.customListener != nil {
		return l.customListener.Addr().String()
	}
	return l.addr
}

func (l *Listener) Close() error {
	if l.ln != nil {
		_ = l.ln.Close()
	}
	if l.wsSrv != nil {
		_ = l.wsSrv.Close()
	}
	return nil
}

func (l *Listener) Listen(ctx context.Context, handle func(net.Conn)) error {
	hasTCP := l.addr != "" || l.customListener != nil
	hasWS := l.wsAddr != ""

	if !hasTCP && !hasWS {
		<-ctx.Done()
		return nil
	}

	if hasWS {
		go l.serveWS(ctx, handle)
	}

	if !hasTCP {
		<-ctx.Done()
		return nil
	}

	var err error
	if l.customListener != nil {
		l.ln = l.customListener
	} else {
		if l.tlsCfg != nil {
			l.ln, err = tls.Listen("tcp", l.addr, l.tlsCfg)
		} else {
			l.ln, err = net.Listen("tcp", l.addr)
		}
		if err != nil {
			return fmt.Errorf("tcp listen %s: %w", l.addr, err)
		}
	}
	defer l.ln.Close()
	go func() {
		<-ctx.Done()
		l.ln.Close()
	}()

	sem := make(chan struct{}, 20000)
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				if ctx.Err() != nil {
					return nil
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(3 * time.Minute)
			_ = tc.SetReadBuffer(32 * 1024)
			_ = tc.SetWriteBuffer(32 * 1024)
		}
		sem <- struct{}{}
		go func(c net.Conn) {
			defer func() { <-sem }()
			handle(c)
		}(conn)
	}
}

func (l *Listener) serveWS(ctx context.Context, handle func(net.Conn)) {
	upgrader := l.upgrader()
	wsSem := make(chan struct{}, 20000)
	mux := http.NewServeMux()
	handleWS := func(w http.ResponseWriter, r *http.Request) {
		select {
		case wsSem <- struct{}{}:
		default:
			http.Error(w, "too many connections", http.StatusServiceUnavailable)
			return
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			<-wsSem
			return
		}
		conn := &wsConn{Conn: ws, sem: wsSem}
		handle(conn)
	}
	mux.HandleFunc("/", handleWS)
	mux.HandleFunc("/mqtt", handleWS)
	srv := &http.Server{
		Addr:         l.wsAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	l.wsSrv = srv
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	_ = srv.ListenAndServe()
}

// wsConn wraps websocket.Conn to implement net.Conn (simplified)

type wsConn struct {
	*websocket.Conn
	reader *wsReader
	sem    chan struct{}
}

type wsReader struct {
	buf []byte
	pos int
}

func (w *wsConn) Read(b []byte) (int, error) {
	if w.reader != nil && w.reader.pos < len(w.reader.buf) {
		n := copy(b, w.reader.buf[w.reader.pos:])
		w.reader.pos += n
		if w.reader.pos >= len(w.reader.buf) {
			w.reader = nil
		}
		return n, nil
	}
	_, data, err := w.ReadMessage()
	if err != nil {
		return 0, err
	}
	n := copy(b, data)
	if n < len(data) {
		w.reader = &wsReader{buf: data, pos: n}
	}
	return n, nil
}
func (w *wsConn) Write(b []byte) (int, error) {
	err := w.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *wsConn) Close() error {
	err := w.Conn.Close()
	if w.sem != nil {
		select {
		case <-w.sem:
		default:
		}
	}
	return err
}
func (w *wsConn) SetDeadline(t time.Time) error {
	if err := w.Conn.SetReadDeadline(t); err != nil {
		return err
	}
	return w.Conn.SetWriteDeadline(t)
}
func (w *wsConn) SetReadDeadline(t time.Time) error  { return w.Conn.SetReadDeadline(t) }
func (w *wsConn) SetWriteDeadline(t time.Time) error { return w.Conn.SetWriteDeadline(t) }
func (w *wsConn) LocalAddr() net.Addr                { return &fakeAddr{"ws"} }
func (w *wsConn) RemoteAddr() net.Addr               { return &fakeAddr{"ws-remote"} }

type fakeAddr struct{ s string }

func (f *fakeAddr) Network() string { return f.s }
func (f *fakeAddr) String() string  { return f.s }
