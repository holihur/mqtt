package hook

import (
	"errors"
	"testing"

	"mqtt/internal/codec"
)

// ---------------------------------------------------------------------------
// BaseHook
// ---------------------------------------------------------------------------

func TestBaseHookDefaults(t *testing.T) {
	var b BaseHook
	if b.ID() != "base" {
		t.Fatalf("ID = %q, want %q", b.ID(), "base")
	}
	if err := b.OnAuth("c", "u", []byte("p")); err != nil {
		t.Fatalf("OnAuth: %v", err)
	}
	if err := b.OnConnect("c"); err != nil {
		t.Fatalf("OnConnect: %v", err)
	}
	if err := b.OnPublish("c", "t", []byte("p"), 0, false); err != nil {
		t.Fatalf("OnPublish: %v", err)
	}
	if err := b.OnSubscribe("c", "f", 0); err != nil {
		t.Fatalf("OnSubscribe: %v", err)
	}
	if err := b.OnUnsubscribe("c", "f"); err != nil {
		t.Fatalf("OnUnsubscribe: %v", err)
	}
	b.OnDisconnect("c", true)
	b.OnPacket("in", "c", &codec.Packet{}, "hex")
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

func TestNewManager(t *testing.T) {
	m := NewManager()
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
	if m.Len() != 0 {
		t.Fatalf("Len = %d, want 0", m.Len())
	}
}

func TestManagerRegister(t *testing.T) {
	m := NewManager()

	// nil hook is ignored
	m.Register(nil)
	if m.Len() != 0 {
		t.Fatalf("Len after nil register = %d, want 0", m.Len())
	}

	m.Register(BaseHook{})
	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}

	// same ID replaces
	m.Register(BaseHook{})
	if m.Len() != 1 {
		t.Fatalf("Len after re-register = %d, want 1", m.Len())
	}

	// different ID appends
	m.Register(&AuthHook{})
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
}

func TestManagerHooks(t *testing.T) {
	m := NewManager()
	m.Register(BaseHook{})
	snap := m.Hooks()
	if len(snap) != 1 {
		t.Fatalf("Hooks len = %d, want 1", len(snap))
	}
	_ = append(snap, &AuthHook{})
	if m.Len() != 1 {
		t.Fatalf("Len mutated = %d, want 1", m.Len())
	}
}

// ---------------------------------------------------------------------------
// Exec* methods
// ---------------------------------------------------------------------------

type denyHook struct{ BaseHook }

func (denyHook) ID() string                                         { return "deny" }
func (denyHook) OnAuth(string, string, []byte) error                { return ErrDenied }
func (denyHook) OnConnect(string) error                             { return ErrDenied }
func (denyHook) OnPublish(string, string, []byte, byte, bool) error { return ErrDenied }
func (denyHook) OnSubscribe(string, string, byte) error             { return ErrDenied }
func (denyHook) OnUnsubscribe(string, string) error                 { return ErrDenied }

func TestExecAuth(t *testing.T) {
	tests := []struct {
		name    string
		hooks   []Hook
		wantErr error
	}{
		{"no hooks", nil, nil},
		{"allow", []Hook{BaseHook{}}, nil},
		{"deny", []Hook{&denyHook{}}, ErrDenied},
		{"first wins", []Hook{BaseHook{}, &denyHook{}}, ErrDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManager()
			for _, h := range tt.hooks {
				m.Register(h)
			}
			err := m.ExecAuth("c", "u", []byte("p"))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ExecAuth err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestExecConnect(t *testing.T) {
	m := NewManager()
	m.Register(&denyHook{})
	if err := m.ExecConnect("c"); !errors.Is(err, ErrDenied) {
		t.Fatalf("ExecConnect = %v, want ErrDenied", err)
	}
}

func TestExecPublish(t *testing.T) {
	m := NewManager()
	m.Register(&denyHook{})
	if err := m.ExecPublish("c", "t", nil, 0, false); !errors.Is(err, ErrDenied) {
		t.Fatalf("ExecPublish = %v, want ErrDenied", err)
	}
}

func TestExecSubscribe(t *testing.T) {
	m := NewManager()
	m.Register(&denyHook{})
	if err := m.ExecSubscribe("c", "f", 0); !errors.Is(err, ErrDenied) {
		t.Fatalf("ExecSubscribe = %v, want ErrDenied", err)
	}
}

func TestExecUnsubscribe(t *testing.T) {
	m := NewManager()
	m.Register(&denyHook{})
	if err := m.ExecUnsubscribe("c", "f"); !errors.Is(err, ErrDenied) {
		t.Fatalf("ExecUnsubscribe = %v, want ErrDenied", err)
	}
}

func TestExecDisconnect(t *testing.T) {
	m := NewManager()
	m.Register(BaseHook{})
	m.ExecDisconnect("c", true) // should not panic
}

func TestExecPacket(t *testing.T) {
	m := NewManager()
	m.Register(BaseHook{})
	m.ExecPacket("in", "c", &codec.Packet{Type: 3}, "0x30")
}

// nil manager tests (defensive nil checks)
func TestNilManager(t *testing.T) {
	var m *Manager
	if err := m.ExecAuth("c", "u", nil); err != nil {
		t.Fatalf("nil ExecAuth: %v", err)
	}
	if err := m.ExecConnect("c"); err != nil {
		t.Fatalf("nil ExecConnect: %v", err)
	}
	if err := m.ExecPublish("c", "t", nil, 0, false); err != nil {
		t.Fatalf("nil ExecPublish: %v", err)
	}
	if err := m.ExecSubscribe("c", "f", 0); err != nil {
		t.Fatalf("nil ExecSubscribe: %v", err)
	}
	if err := m.ExecUnsubscribe("c", "f"); err != nil {
		t.Fatalf("nil ExecUnsubscribe: %v", err)
	}
	m.ExecDisconnect("c", false)
	m.ExecPacket("in", "c", nil, "")
}

// ---------------------------------------------------------------------------
// AuthAdapter
// ---------------------------------------------------------------------------

type mockAuth struct {
	allow bool
}

func (m *mockAuth) Authenticate(_, _ string, _ []byte) bool { return m.allow }
func (m *mockAuth) Authorize(_, _ string, _ bool) bool      { return m.allow }

func TestNewAuthAdapter(t *testing.T) {
	if a := NewAuthAdapter(nil); a != nil {
		t.Fatalf("NewAuthAdapter(nil) = %v, want nil", a)
	}
	a := NewAuthAdapter(&mockAuth{allow: true})
	if a == nil {
		t.Fatal("NewAuthAdapter returned nil")
	}
	if a.ID() != "auth" {
		t.Fatalf("ID = %q, want %q", a.ID(), "auth")
	}
}

func TestAuthAdapterOnAuth(t *testing.T) {
	a := NewAuthAdapter(&mockAuth{allow: true})
	if err := a.OnAuth("c", "u", nil); err != nil {
		t.Fatalf("allow OnAuth: %v", err)
	}

	a2 := NewAuthAdapter(&mockAuth{allow: false})
	if err := a2.OnAuth("c", "u", nil); !errors.Is(err, ErrDenied) {
		t.Fatalf("deny OnAuth = %v, want ErrDenied", err)
	}
}

func TestAuthAdapterOnPublish(t *testing.T) {
	a := NewAuthAdapter(&mockAuth{allow: false})
	if err := a.OnPublish("c", "t", nil, 0, false); !errors.Is(err, ErrDenied) {
		t.Fatalf("deny OnPublish = %v, want ErrDenied", err)
	}
}

func TestAuthAdapterOnSubscribe(t *testing.T) {
	a := NewAuthAdapter(&mockAuth{allow: false})
	if err := a.OnSubscribe("c", "f", 0); !errors.Is(err, ErrDenied) {
		t.Fatalf("deny OnSubscribe = %v, want ErrDenied", err)
	}
}

func TestAuthAdapterPassThrough(t *testing.T) {
	a := NewAuthAdapter(&mockAuth{allow: true})
	if err := a.OnConnect("c"); err != nil {
		t.Fatalf("OnConnect: %v", err)
	}
	if err := a.OnUnsubscribe("c", "f"); err != nil {
		t.Fatalf("OnUnsubscribe: %v", err)
	}
	a.OnDisconnect("c", true)
	a.OnPacket("in", "c", nil, "")
}

// nil inner
func TestAuthAdapterNilInner(t *testing.T) {
	a := &AuthAdapter{Inner: nil, id: "test"}
	if err := a.OnAuth("c", "u", nil); err != nil {
		t.Fatalf("nil inner OnAuth: %v", err)
	}
	if err := a.OnPublish("c", "t", nil, 0, false); err != nil {
		t.Fatalf("nil inner OnPublish: %v", err)
	}
	if err := a.OnSubscribe("c", "f", 0); err != nil {
		t.Fatalf("nil inner OnSubscribe: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Example hooks
// ---------------------------------------------------------------------------

func TestAuthHook(t *testing.T) {
	h := AuthHook{}
	if h.ID() != "auth-example" {
		t.Fatalf("ID = %q", h.ID())
	}
	if err := h.OnAuth("c", "ok", nil); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if err := h.OnAuth("c", "blocked", nil); !errors.Is(err, ErrDenied) {
		t.Fatalf("blocked: %v", err)
	}
	if err := h.OnAuth("c", "u", []byte("wrong")); !errors.Is(err, ErrDenied) {
		t.Fatalf("wrong pw: %v", err)
	}
}

func TestTenantIsolationHook(t *testing.T) {
	h := TenantIsolationHook{}
	if h.ID() != "tenant-isolation" {
		t.Fatalf("ID = %q", h.ID())
	}

	tests := []struct {
		name    string
		client  string
		topic   string
		wantErr bool
	}{
		{"no dash", "device1", "any/topic", false},
		{"sys topic", "t1-dev", "$SYS/info", false},
		{"correct tenant", "t1-dev", "tenant/t1/data", false},
		{"wrong tenant", "t1-dev", "tenant/t2/data", true},
		{"internal ok", "internal-dev", "internal/config", false},
		{"internal bad", "internal-dev", "tenant/t1/data", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := h.OnPublish(tt.client, tt.topic, nil, 0, false)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTenantIsolationHookSubscribe(t *testing.T) {
	h := TenantIsolationHook{}

	tests := []struct {
		name    string
		client  string
		filter  string
		wantErr bool
	}{
		{"no dash", "device1", "any/filter", false},
		{"sys", "t1-dev", "$SYS/#", false},
		{"correct", "t1-dev", "tenant/t1/#", false},
		{"wrong", "t1-dev", "tenant/t2/#", true},
		{"shared sub", "t1-dev", "$share/g/tenant/t1/#", false},
		{"shared sub wrong", "t1-dev", "$share/g/tenant/t2/#", true},
		{"internal ok", "internal-dev", "internal/config", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := h.OnSubscribe(tt.client, tt.filter, 0)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEncTopicValidationHook(t *testing.T) {
	h := EncTopicValidationHook{}
	if h.ID() != "enc-validation" {
		t.Fatalf("ID = %q", h.ID())
	}

	tests := []struct {
		name    string
		topic   string
		payload []byte
		wantErr bool
	}{
		{"non enc", "tenant/t1/data", []byte("x"), false},
		{"enc empty", "tenant/t1/enc", nil, true},
		{"enc short", "tenant/t1/enc", []byte("short"), true},
		{"enc ok", "tenant/t1/enc", make([]byte, 16), false},
		{"enc nested ok", "tenant/t1/enc/sub", make([]byte, 32), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := h.OnPublish("c", tt.topic, tt.payload, 0, false)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTopicTagHook(t *testing.T) {
	h := TopicTagHook{}
	if h.ID() != "topic-tag" {
		t.Fatalf("ID = %q", h.ID())
	}
	if err := h.OnConnect("c"); err != nil {
		t.Fatalf("OnConnect: %v", err)
	}
	h.OnDisconnect("c", true)
	if err := h.OnUnsubscribe("c", "f"); err != nil {
		t.Fatalf("OnUnsubscribe: %v", err)
	}

	topics := []string{"internal/config", "tenant/t1/enc/data", "tenant/t1/plain", "other/topic"}
	for _, tp := range topics {
		if err := h.OnPublish("c", tp, []byte("x"), 1, false); err != nil {
			t.Fatalf("OnPublish %s: %v", tp, err)
		}
	}
	if err := h.OnSubscribe("c", "f", 1); err != nil {
		t.Fatalf("OnSubscribe: %v", err)
	}
}

func TestHexDumpHook(t *testing.T) {
	h := HexDumpHook{}
	if h.ID() != "hex-dump" {
		t.Fatalf("ID = %q", h.ID())
	}
	h.OnPacket("in", "c", &codec.Packet{Type: 3}, "300a...")
	h.OnPacket("in", "c", nil, "") // nil pkt guard
}

// ---------------------------------------------------------------------------
// Match
// ---------------------------------------------------------------------------

func TestMatch(t *testing.T) {
	tests := []struct {
		filter, topic string
		want          bool
	}{
		{"sport/tennis", "sport/tennis", true},
		{"sport/tennis", "sport/football", false},
		{"sport/#", "sport/tennis/player1", true},
		{"sport/+", "sport/tennis", true},
		{"sport/+", "sport/tennis/player1", false},
		{"+/+", "sport/tennis", true},
		{"#", "anything/at/all", true},
	}
	for _, tt := range tests {
		t.Run(tt.filter+"_"+tt.topic, func(t *testing.T) {
			if got := Match(tt.filter, tt.topic); got != tt.want {
				t.Fatalf("Match(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
			}
		})
	}
}
