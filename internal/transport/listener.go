package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
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
}

func NewListener(addr string, tlsCfg *tls.Config, wsAddr string) *Listener {
	return &Listener{addr: addr, tlsCfg: tlsCfg, wsAddr: wsAddr}
}

func (l *Listener) SetCustomListener(ln net.Listener) { l.customListener = ln }

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

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "" },
}

func (l *Listener) serveWS(ctx context.Context, handle func(net.Conn)) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn := &wsConn{Conn: ws}
		handle(conn)
	})
	mux.HandleFunc("/mqtt", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn := &wsConn{Conn: ws}
		handle(conn)
	})
	srv := &http.Server{Addr: l.wsAddr, Handler: mux}
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
