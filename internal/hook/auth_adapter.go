package hook

import (
	"mqtt/internal/auth"
	"mqtt/internal/codec"
)

type AuthAdapter struct {
	Inner auth.Authenticator
	id    string
}

func NewAuthAdapter(a auth.Authenticator) *AuthAdapter {
	if a == nil {
		return nil
	}
	return &AuthAdapter{Inner: a, id: "auth"}
}

func (x *AuthAdapter) ID() string { return x.id }

func (x *AuthAdapter) OnAuth(clientID, username string, password []byte) error {
	if x.Inner == nil {
		return nil
	}
	if !x.Inner.Authenticate(clientID, username, password) {
		return ErrDenied
	}
	return nil
}

func (x *AuthAdapter) OnPublish(clientID, topic string, _ []byte, _ byte, _ bool) error {
	if x.Inner == nil {
		return nil
	}
	if !x.Inner.Authorize(clientID, topic, true) {
		return ErrDenied
	}
	return nil
}

func (x *AuthAdapter) OnSubscribe(clientID, filter string, _ byte) error {
	if x.Inner == nil {
		return nil
	}
	if !x.Inner.Authorize(clientID, filter, false) {
		return ErrDenied
	}
	return nil
}

func (x *AuthAdapter) OnConnect(string) error              { return nil }
func (x *AuthAdapter) OnUnsubscribe(string, string) error { return nil }
func (x *AuthAdapter) OnDisconnect(string, bool)           {}
func (x *AuthAdapter) OnPacket(string, string, *codec.Packet, string) {
}
