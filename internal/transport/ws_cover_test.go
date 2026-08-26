package transport

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSetWsAllowOrigins_Edge(t *testing.T) {
	l := NewListener(":0", nil, ":0")
	l.SetWsAllowOrigins([]string{" ", "", "http://example.com", "http://example.com:8080", "invalid url with spaces", "*", " https://trim.me "})
	if !l.wsAllowAll {
		t.Fatalf("allowAll should be true when * present")
	}
	if _, ok := l.wsAllowOrigins["example.com"]; !ok {
		t.Fatalf("should contain hostname")
	}
	l2 := NewListener(":0", nil, ":0")
	l2.SetWsAllowOrigins([]string{"http://allowed.com"})
	if l2.wsAllowAll {
		t.Fatalf("should not allow all")
	}
	if _, ok := l2.wsAllowOrigins["http://allowed.com"]; !ok {
		t.Fatalf("should contain full origin")
	}
}

func TestUpgraderCheckOrigin(t *testing.T) {
	l := NewListener(":0", nil, ":0")
	l.SetWsAllowOrigins([]string{"http://allowed.com"})
	ug := l.upgrader()
	req, _ := http.NewRequest("GET", "http://example.com/mqtt", nil)
	req.Host = "example.com"
	req.Header.Set("Origin", "http://allowed.com")
	if !ug.CheckOrigin(req) {
		t.Fatalf("upgrader should delegate to checkOrigin")
	}
	req.Header.Set("Origin", "http://evil.com")
	if ug.CheckOrigin(req) {
		t.Fatalf("evil should be denied")
	}
}

func TestCheckOrigin_InvalidURL(t *testing.T) {
	l := NewListener(":0", nil, ":0")
	req, _ := http.NewRequest("GET", "http://example.com/mqtt", nil)
	req.Host = "example.com"
	req.Header.Set("Origin", "http://[invalid")
	if l.checkOrigin(req) {
		t.Fatalf("invalid origin URL should be denied")
	}
}

func TestCheckOrigin_HostnameMatch(t *testing.T) {
	l := NewListener(":0", nil, ":0")
	l.SetWsAllowOrigins([]string{"http://example.com"})
	req, _ := http.NewRequest("GET", "http://other.com/mqtt", nil)
	req.Host = "other.com"
	req.Header.Set("Origin", "http://example.com:8080")
	if !l.checkOrigin(req) {
		t.Fatalf("hostname match should allow")
	}
}

func TestWsConnAddr(t *testing.T) {
	wc := &wsConn{}
	if wc.LocalAddr().Network() != "ws" {
		t.Fatalf("LocalAddr")
	}
	if wc.RemoteAddr().String() != "ws-remote" {
		t.Fatalf("RemoteAddr")
	}
}

func TestWsConnReadWriteClose(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		wsConn := &wsConn{Conn: ws, sem: make(chan struct{}, 1)}
		wsConn.sem <- struct{}{}
		// test Write
		if _, err := wsConn.Write([]byte("hello ws")); err != nil {
			t.Errorf("ws Write: %v", err)
			return
		}
		// test Read via client echo
		buf := make([]byte, 64)
		n, err := wsConn.Read(buf)
		if err != nil {
			t.Errorf("ws Read: %v", err)
			return
		}
		if string(buf[:n]) != "client msg" {
			t.Errorf("read mismatch %q", string(buf[:n]))
		}
		// test buffered read: send large payload that requires split
		// client will send two messages, server reads with small buffer to trigger wsReader path
		n2, err := wsConn.Read(buf[:2])
		if err != nil {
			t.Errorf("ws Read2: %v", err)
			return
		}
		if n2 != 2 {
			t.Errorf("expected 2")
		}
		// second read should drain buffered remainder
		n3, err := wsConn.Read(buf)
		if err != nil {
			t.Errorf("ws Read3: %v", err)
			return
		}
		if string(buf[:n3]) != "llo" {
			t.Errorf("buffered remainder %q", string(buf[:n3]))
		}
		// test deadlines
		_ = wsConn.SetDeadline(time.Now().Add(time.Second))
		_ = wsConn.SetReadDeadline(time.Now().Add(time.Second))
		_ = wsConn.SetWriteDeadline(time.Now().Add(time.Second))
		_ = wsConn.Close()
	}))
	defer server.Close()

	url := "ws" + server.URL[4:] + "/"
	dialer := websocket.Dialer{}
	wsClient, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer wsClient.Close()
	// read server hello
	_, data, err := wsClient.ReadMessage()
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(data) != "hello ws" {
		t.Fatalf("client got %q", string(data))
	}
	if err := wsClient.WriteMessage(websocket.BinaryMessage, []byte("client msg")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	if err := wsClient.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatalf("client write2: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
}

func TestWsConnWriteError(t *testing.T) {
	// wsConn with closed conn should error on Write
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, _ := upgrader.Upgrade(w, r, nil)
		wsConn := &wsConn{Conn: ws}
		ws.Close()
		if _, err := wsConn.Write([]byte("x")); err == nil {
			t.Errorf("expected write error on closed conn")
		}
	}))
	defer server.Close()
	url := "ws" + server.URL[4:] + "/"
	dialer := websocket.Dialer{}
	ws, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ws.Close()
	time.Sleep(100 * time.Millisecond)
}

func TestListenerServeWS_Integration(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	wsAddr := ln.Addr().String()
	_ = ln.Close()
	l := NewListener("", nil, wsAddr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handled := make(chan struct{}, 1)
	go func() {
		_ = l.Listen(ctx, func(c net.Conn) {
			handled <- struct{}{}
			c.Close()
		})
	}()
	time.Sleep(300 * time.Millisecond)
	url := "ws://" + wsAddr + "/mqtt"
	dialer := websocket.Dialer{}
	ws, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	ws.Close()
	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatalf("ws handler not called")
	}
	cancel()
	time.Sleep(100 * time.Millisecond)
	_ = l.Close()
}

func TestListenerServeWS_OriginDenied(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	wsAddr := ln.Addr().String()
	ln.Close()
	l := NewListener("", nil, wsAddr)
	l.SetWsAllowOrigins([]string{"http://allowed.com"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = l.Listen(ctx, func(c net.Conn) {
			c.Close()
		})
	}()
	time.Sleep(300 * time.Millisecond)
	url := "ws://" + wsAddr + "/"
	dialer := websocket.Dialer{}
	header := http.Header{}
	header.Set("Origin", "http://evil.com")
	_, _, err = dialer.Dial(url, header)
	if err == nil {
		t.Fatalf("evil origin should be denied")
	}
	cancel()
}

func TestListenerServeWS_SemLimit(t *testing.T) {
	l := NewListener("", nil, "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.serveWS(ctx, func(c net.Conn) { c.Close() })
	time.Sleep(100 * time.Millisecond)
	_ = l.Close()
	cancel()
}

func TestListenerListen_NoTCP_WSOnly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	wsAddr := ln.Addr().String()
	ln.Close()
	l := NewListener("", nil, wsAddr)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Listen(ctx, func(c net.Conn) { c.Close() }) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ws only listen: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout")
	}
}

func TestWsConnCloseSemRelease(t *testing.T) {
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	// create a dummy websocket Conn via httptest
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, _ := upgrader.Upgrade(w, r, nil)
		wc := &wsConn{Conn: ws, sem: sem}
		// sem already full, Close should release
		_ = wc.Close()
		if len(sem) != 0 {
			t.Errorf("sem should be released")
		}
	}))
	defer server.Close()
	url := "ws" + server.URL[4:] + "/"
	dialer := websocket.Dialer{}
	ws, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ws.Close()
	time.Sleep(100 * time.Millisecond)
}
