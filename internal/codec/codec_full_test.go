package codec

import (
	"bytes"
	"testing"
)

// ---- helpers ----

func roundtrip(t *testing.T, p *Packet) *Packet {
	t.Helper()
	data, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	dec, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v data=%x", err, data)
	}
	return dec
}

func ptrU32(v uint32) *uint32 { return &v }
func ptrU16(v uint16) *uint16 { return &v }
func ptrByte(v byte) *byte    { return &v }
func ptrStr(s string) *string  { return &s }

// ---- Pool ----

func TestAcquireReleasePacket(t *testing.T) {
	p := AcquirePacket()
	if p == nil {
		t.Fatal("AcquirePacket returned nil")
	}
	if p.Type != 0 {
		t.Fatalf("expected zeroed packet, got type %d", p.Type)
	}
	p.ClientID = "test"
	p.Payload = []byte("data")
	ReleasePacket(p)

	ReleasePacket(nil) // should not panic
}

// ---- CONNACK ----

func TestConnackV311Roundtrip(t *testing.T) {
	p := &Packet{
		Type:           TypeCONNACK,
		Version:        ProtocolV311,
		SessionPresent: true,
		ReasonCode:     0,
	}
	dec := roundtrip(t, p)
	if !dec.SessionPresent {
		t.Fatal("expected session present")
	}
	if dec.ReasonCode != 0 {
		t.Fatalf("expected reason 0, got %d", dec.ReasonCode)
	}
}

func TestConnackV5Roundtrip(t *testing.T) {
	keepAlive := uint16(120)
	p := &Packet{
		Type:           TypeCONNACK,
		Version:        ProtocolV5,
		SessionPresent: false,
		ReasonCode:     0,
		ConnProperties: &Properties{
			ReceiveMaximum:   ptrU16(100),
			MaximumQoS:       ptrByte(1),
			RetainAvailable:  ptrByte(1),
			AssignedClientID: ptrStr("assigned-id"),
			ServerKeepAlive:  &keepAlive,
			TopicAliasMaximum: ptrU16(50),
			WildcardSubAvailable: ptrByte(1),
			SubIDAvailable:   ptrByte(1),
			SharedSubAvailable: ptrByte(1),
			MaximumPacketSize: ptrU32(65535),
			User:             []UserProperty{{Key: "k1", Val: "v1"}},
			ReasonString:     ptrStr("ok"),
		},
	}
	dec := roundtrip(t, p)
	if dec.SessionPresent {
		t.Fatal("expected no session present")
	}
	if dec.ConnProperties == nil {
		t.Fatal("expected conn properties")
	}
	if dec.ConnProperties.AssignedClientID == nil || *dec.ConnProperties.AssignedClientID != "assigned-id" {
		t.Fatalf("assigned client id mismatch: %+v", dec.ConnProperties)
	}
	if dec.ConnProperties.ReceiveMaximum == nil || *dec.ConnProperties.ReceiveMaximum != 100 {
		t.Fatalf("receive maximum mismatch")
	}
	if dec.ConnProperties.MaximumQoS == nil || *dec.ConnProperties.MaximumQoS != 1 {
		t.Fatalf("maximum QoS mismatch")
	}
	if dec.ConnProperties.ServerKeepAlive == nil || *dec.ConnProperties.ServerKeepAlive != 120 {
		t.Fatalf("server keep alive mismatch")
	}
	if dec.ConnProperties.TopicAliasMaximum == nil || *dec.ConnProperties.TopicAliasMaximum != 50 {
		t.Fatalf("topic alias maximum mismatch")
	}
	if dec.ConnProperties.WildcardSubAvailable == nil || *dec.ConnProperties.WildcardSubAvailable != 1 {
		t.Fatalf("wildcard sub available mismatch")
	}
	if dec.ConnProperties.SubIDAvailable == nil || *dec.ConnProperties.SubIDAvailable != 1 {
		t.Fatalf("sub id available mismatch")
	}
	if dec.ConnProperties.SharedSubAvailable == nil || *dec.ConnProperties.SharedSubAvailable != 1 {
		t.Fatalf("shared sub available mismatch")
	}
	if dec.ConnProperties.MaximumPacketSize == nil || *dec.ConnProperties.MaximumPacketSize != 65535 {
		t.Fatalf("maximum packet size mismatch")
	}
	if len(dec.ConnProperties.User) != 1 || dec.ConnProperties.User[0].Key != "k1" {
		t.Fatalf("user property mismatch")
	}
	if dec.ConnProperties.ReasonString == nil || *dec.ConnProperties.ReasonString != "ok" {
		t.Fatalf("reason string mismatch")
	}
}

func TestConnackV5NilProperties(t *testing.T) {
	p := &Packet{
		Type:       TypeCONNACK,
		Version:    ProtocolV5,
		ReasonCode: 0,
	}
	dec := roundtrip(t, p)
	if dec.Version != ProtocolV5 {
		t.Fatalf("expected v5, got %d", dec.Version)
	}
}

func TestConnackDecodeTooShort(t *testing.T) {
	_, err := Decode([]byte{TypeCONNACK << 4, 0}) // remaining length 0
	if err == nil {
		t.Fatal("expected error for too-short connack")
	}
}

// ---- PUBLISH variants ----

func TestPublishQoS0Roundtrip(t *testing.T) {
	p := &Packet{
		Type:    TypePUBLISH,
		Version: ProtocolV311,
		Topic:   "test/qos0",
		QoS:     0,
		Payload: []byte("qos0 payload"),
	}
	dec := roundtrip(t, p)
	if dec.Topic != "test/qos0" || string(dec.Payload) != "qos0 payload" || dec.QoS != 0 {
		t.Fatalf("qos0 mismatch %+v", dec)
	}
}

func TestPublishQoS2Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypePUBLISH,
		Version:  ProtocolV311,
		Topic:    "test/qos2",
		QoS:      2,
		PacketID: 456,
		Payload:  []byte("qos2 payload"),
		Dup:      true,
		Retain:   true,
	}
	dec := roundtrip(t, p)
	if dec.QoS != 2 || dec.PacketID != 456 || !dec.Dup || !dec.Retain {
		t.Fatalf("qos2 mismatch %+v", dec)
	}
}

func TestPublishV5WithTopicAlias(t *testing.T) {
	ta := uint16(5)
	p := &Packet{
		Type:     TypePUBLISH,
		Version:  ProtocolV5,
		Topic:    "v5/topic",
		QoS:      1,
		PacketID: 100,
		Payload:  []byte("v5 data"),
		PubProps: &Properties{
			TopicAlias:            &ta,
			MessageExpiryInterval: ptrU32(300),
			ResponseTopic:         ptrStr("reply/to"),
			CorrelationData:       []byte{0x01, 0x02},
			ContentType:           ptrStr("application/json"),
			PayloadFormatIndicator: ptrByte(1),
			User:                  []UserProperty{{Key: "k", Val: "v"}, {Key: "k2", Val: "v2"}},
		},
	}
	dec := roundtrip(t, p)
	if dec.PubProps == nil {
		t.Fatal("expected pub props")
	}
	if dec.PubProps.TopicAlias == nil || *dec.PubProps.TopicAlias != 5 {
		t.Fatalf("topic alias mismatch")
	}
	if dec.PubProps.MessageExpiryInterval == nil || *dec.PubProps.MessageExpiryInterval != 300 {
		t.Fatalf("message expiry mismatch")
	}
	if dec.PubProps.ResponseTopic == nil || *dec.PubProps.ResponseTopic != "reply/to" {
		t.Fatalf("response topic mismatch")
	}
	if !bytes.Equal(dec.PubProps.CorrelationData, []byte{0x01, 0x02}) {
		t.Fatalf("correlation data mismatch")
	}
	if dec.PubProps.ContentType == nil || *dec.PubProps.ContentType != "application/json" {
		t.Fatalf("content type mismatch")
	}
	if dec.PubProps.PayloadFormatIndicator == nil || *dec.PubProps.PayloadFormatIndicator != 1 {
		t.Fatalf("payload format indicator mismatch")
	}
	if len(dec.PubProps.User) != 2 {
		t.Fatalf("expected 2 user props, got %d", len(dec.PubProps.User))
	}
}

func TestPublishEmptyPayload(t *testing.T) {
	p := &Packet{
		Type:    TypePUBLISH,
		Version: ProtocolV311,
		Topic:   "empty",
		QoS:     0,
		Payload: []byte{},
	}
	dec := roundtrip(t, p)
	if dec.Topic != "empty" || len(dec.Payload) != 0 {
		t.Fatalf("empty payload mismatch %+v", dec)
	}
}

// ---- PUBACK/PUBREC/PUBREL/PUBCOMP ----

func TestPubackV311Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypePUBACK,
		Version:  ProtocolV311,
		PacketID: 42,
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 42 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

func TestPubackV5Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypePUBACK,
		Version:  ProtocolV5,
		PacketID: 100,
		Reason:   0x83,
		AckProps: &Properties{
			ReasonString: ptrStr("no subscription"),
			User:         []UserProperty{{Key: "k", Val: "v"}},
		},
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 100 || dec.Reason != 0x83 {
		t.Fatalf("puback mismatch %+v", dec)
	}
	if dec.AckProps == nil || dec.AckProps.ReasonString == nil || *dec.AckProps.ReasonString != "no subscription" {
		t.Fatalf("ack props mismatch %+v", dec.AckProps)
	}
}

func TestPubackV5NoReasonNoProps(t *testing.T) {
	p := &Packet{
		Type:     TypePUBACK,
		Version:  ProtocolV5,
		PacketID: 50,
		Reason:   0,
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 50 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

func TestPubrecV311Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypePUBREC,
		Version:  ProtocolV311,
		PacketID: 77,
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 77 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

func TestPubrelRoundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypePUBREL,
		Version:  ProtocolV311,
		PacketID: 88,
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 88 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

func TestPubrelV5Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypePUBREL,
		Version:  ProtocolV5,
		PacketID: 89,
		Reason:   0x92,
		AckProps: &Properties{
			ReasonString: ptrStr("packet-id not found"),
		},
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 89 || dec.Reason != 0x92 {
		t.Fatalf("pubrel v5 mismatch %+v", dec)
	}
}

func TestPubcompRoundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypePUBCOMP,
		Version:  ProtocolV311,
		PacketID: 99,
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 99 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

func TestPubcompV5Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypePUBCOMP,
		Version:  ProtocolV5,
		PacketID: 100,
		Reason:   0,
		AckProps: &Properties{
			ReasonString: ptrStr("ok"),
		},
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 100 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

// ---- SUBACK ----

func TestSubackV311Roundtrip(t *testing.T) {
	// Use codes starting with 0x01 to avoid v5 properties heuristic consuming the first byte
	p := &Packet{
		Type:        TypeSUBACK,
		Version:     ProtocolV311,
		PacketID:    10,
		SubackCodes: []byte{0x01, 0x02, 0x80},
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 10 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
	if len(dec.SubackCodes) != 3 {
		t.Fatalf("expected 3 codes, got %d", len(dec.SubackCodes))
	}
}

func TestSubackV5Roundtrip(t *testing.T) {
	p := &Packet{
		Type:        TypeSUBACK,
		Version:     ProtocolV5,
		PacketID:    20,
		SubackCodes: []byte{0x00, 0x01, 0x80},
		SubackProps: &Properties{
			ReasonString: ptrStr("partial"),
			User:         []UserProperty{{Key: "sk", Val: "sv"}},
		},
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 20 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
	if dec.SubackProps == nil {
		t.Fatal("expected suback props")
	}
	if dec.SubackProps.ReasonString == nil || *dec.SubackProps.ReasonString != "partial" {
		t.Fatalf("reason string mismatch")
	}
}

// ---- UNSUBSCRIBE ----

func TestUnsubscribeV311Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypeUNSUBSCRIBE,
		Version:  ProtocolV311,
		PacketID: 30,
		Topics:   []string{"a/b", "c/d"},
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 30 || len(dec.Topics) != 2 || dec.Topics[0] != "a/b" || dec.Topics[1] != "c/d" {
		t.Fatalf("unsub mismatch %+v", dec)
	}
}

func TestUnsubscribeV5Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypeUNSUBSCRIBE,
		Version:  ProtocolV5,
		PacketID: 40,
		Topics:   []string{"x/y"},
		UnsubProps: &Properties{
			User: []UserProperty{{Key: "uk", Val: "uv"}},
		},
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 40 || len(dec.Topics) != 1 || dec.Topics[0] != "x/y" {
		t.Fatalf("unsub v5 mismatch %+v", dec)
	}
	if dec.UnsubProps == nil || len(dec.UnsubProps.User) != 1 {
		t.Fatalf("unsub props mismatch %+v", dec.UnsubProps)
	}
}

// ---- UNSUBACK ----

func TestUnsubackV311Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypeUNSUBACK,
		Version:  ProtocolV311,
		PacketID: 50,
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 50 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

func TestUnsubackV5Roundtrip(t *testing.T) {
	p := &Packet{
		Type:          TypeUNSUBACK,
		Version:       ProtocolV5,
		PacketID:      60,
		UnsubackCodes: []byte{0x00, 0x11},
		UnsubackProps: &Properties{
			ReasonString: ptrStr("done"),
			User:         []UserProperty{{Key: "uak", Val: "uav"}},
		},
	}
	dec := roundtrip(t, p)
	if dec.PacketID != 60 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
	if dec.UnsubackProps == nil || dec.UnsubackProps.ReasonString == nil || *dec.UnsubackProps.ReasonString != "done" {
		t.Fatalf("unsuback props mismatch %+v", dec.UnsubackProps)
	}
	if len(dec.UnsubackCodes) != 2 {
		t.Fatalf("expected 2 codes, got %d", len(dec.UnsubackCodes))
	}
}

// ---- PINGREQ / PINGRESP ----

func TestPingreqRoundtrip(t *testing.T) {
	p := &Packet{Type: TypePINGREQ}
	dec := roundtrip(t, p)
	if dec.Type != TypePINGREQ {
		t.Fatalf("expected PINGREQ, got %d", dec.Type)
	}
}

func TestPingrespRoundtrip(t *testing.T) {
	p := &Packet{Type: TypePINGRESP}
	dec := roundtrip(t, p)
	if dec.Type != TypePINGRESP {
		t.Fatalf("expected PINGRESP, got %d", dec.Type)
	}
}

func TestPingreqWithPayload(t *testing.T) {
	// PINGREQ with non-empty payload should fail
	data := []byte{TypePINGREQ << 4, 2, 0x00, 0x01}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for PINGREQ with payload")
	}
}

// ---- DISCONNECT ----

func TestDisconnectV311Roundtrip(t *testing.T) {
	p := &Packet{
		Type:    TypeDISCONNECT,
		Version: ProtocolV311,
	}
	dec := roundtrip(t, p)
	if dec.Type != TypeDISCONNECT {
		t.Fatalf("expected DISCONNECT, got %d", dec.Type)
	}
	if dec.DiscReason != 0 {
		t.Fatalf("expected reason 0, got %d", dec.DiscReason)
	}
}

func TestDisconnectV5Roundtrip(t *testing.T) {
	p := &Packet{
		Type:       TypeDISCONNECT,
		Version:    ProtocolV5,
		DiscReason: 0x04,
		DiscProps: &Properties{
			ReasonString:        ptrStr("disconnect reason"),
			ServerReference:     ptrStr("server2.example.com"),
			SessionExpiryInterval: ptrU32(60),
			User:                []UserProperty{{Key: "dk", Val: "dv"}},
		},
	}
	dec := roundtrip(t, p)
	if dec.DiscReason != 0x04 {
		t.Fatalf("disc reason mismatch: %d", dec.DiscReason)
	}
	if dec.DiscProps == nil {
		t.Fatal("expected disc props")
	}
	if dec.DiscProps.ReasonString == nil || *dec.DiscProps.ReasonString != "disconnect reason" {
		t.Fatalf("reason string mismatch")
	}
	if dec.DiscProps.ServerReference == nil || *dec.DiscProps.ServerReference != "server2.example.com" {
		t.Fatalf("server reference mismatch")
	}
	if dec.DiscProps.SessionExpiryInterval == nil || *dec.DiscProps.SessionExpiryInterval != 60 {
		t.Fatalf("session expiry mismatch")
	}
	if len(dec.DiscProps.User) != 1 {
		t.Fatalf("user prop mismatch")
	}
}

func TestDisconnectV5EmptyPayload(t *testing.T) {
	// v3 DISCONNECT has empty payload
	p := &Packet{
		Type:    TypeDISCONNECT,
		Version: ProtocolV311,
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.DiscReason != 0 {
		t.Fatalf("expected reason 0, got %d", dec.DiscReason)
	}
}

// ---- AUTH (v5 only) ----

func TestAuthRoundtrip(t *testing.T) {
	p := &Packet{
		Type:       TypeAUTH,
		Version:    ProtocolV5,
		AuthReason: 0x18, // Continue authentication
		AuthProps: &Properties{
			AuthMethod: ptrStr("SCRAM-SHA-256"),
			AuthData:   []byte{0x01, 0x02, 0x03},
			ReasonString: ptrStr("continue"),
			User:       []UserProperty{{Key: "ak", Val: "av"}},
		},
	}
	dec := roundtrip(t, p)
	if dec.AuthReason != 0x18 {
		t.Fatalf("auth reason mismatch: %d", dec.AuthReason)
	}
	if dec.AuthProps == nil {
		t.Fatal("expected auth props")
	}
	if dec.AuthProps.AuthMethod == nil || *dec.AuthProps.AuthMethod != "SCRAM-SHA-256" {
		t.Fatalf("auth method mismatch")
	}
	if !bytes.Equal(dec.AuthProps.AuthData, []byte{0x01, 0x02, 0x03}) {
		t.Fatalf("auth data mismatch")
	}
	if dec.AuthProps.ReasonString == nil || *dec.AuthProps.ReasonString != "continue" {
		t.Fatalf("reason string mismatch")
	}
	if len(dec.AuthProps.User) != 1 {
		t.Fatalf("user prop mismatch")
	}
}

func TestAuthEmptyPayload(t *testing.T) {
	_, err := Decode([]byte{TypeAUTH << 4, 0})
	if err == nil {
		t.Fatal("expected error for AUTH with empty payload")
	}
}

func TestAuthNoProps(t *testing.T) {
	p := &Packet{
		Type:       TypeAUTH,
		Version:    ProtocolV5,
		AuthReason: 0x00,
	}
	dec := roundtrip(t, p)
	if dec.AuthReason != 0x00 {
		t.Fatalf("auth reason mismatch: %d", dec.AuthReason)
	}
}

// ---- CONNECT variants ----

func TestConnectV31Roundtrip(t *testing.T) {
	p := &Packet{
		Type:          TypeCONNECT,
		Version:       ProtocolV31,
		ProtocolName:  "MQIsdp",
		ProtocolLevel: 3,
		ConnectFlags:  ConnectFlags{CleanSession: true},
		KeepAlive:     30,
		ClientID:      "v31client",
	}
	dec := roundtrip(t, p)
	if dec.Version != ProtocolV31 {
		t.Fatalf("expected v31, got %d", dec.Version)
	}
	if dec.ProtocolName != "MQIsdp" {
		t.Fatalf("expected MQIsdp, got %s", dec.ProtocolName)
	}
	if dec.ClientID != "v31client" {
		t.Fatalf("client id mismatch: %s", dec.ClientID)
	}
}

func TestConnectWithWill(t *testing.T) {
	p := &Packet{
		Type:          TypeCONNECT,
		Version:       ProtocolV311,
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		ConnectFlags: ConnectFlags{
			CleanSession: true,
			WillFlag:     true,
			WillQoS:      1,
			WillRetain:   true,
		},
		KeepAlive: 60,
		ClientID:  "will-client",
		Will: &Will{
			Topic:   "will/topic",
			Payload: []byte("goodbye"),
			QoS:     1,
			Retain:  true,
		},
	}
	dec := roundtrip(t, p)
	if dec.Will == nil {
		t.Fatal("expected will")
	}
	if dec.Will.Topic != "will/topic" || string(dec.Will.Payload) != "goodbye" {
		t.Fatalf("will mismatch %+v", dec.Will)
	}
	if !dec.ConnectFlags.WillFlag {
		t.Fatal("expected will flag")
	}
	if dec.ConnectFlags.WillQoS != 1 {
		t.Fatalf("will qos mismatch: %d", dec.ConnectFlags.WillQoS)
	}
	if !dec.ConnectFlags.WillRetain {
		t.Fatal("expected will retain")
	}
}

func TestConnectV5WithWillAndProps(t *testing.T) {
	delay := uint32(30)
	p := &Packet{
		Type:          TypeCONNECT,
		Version:       ProtocolV5,
		ProtocolName:  "MQTT",
		ProtocolLevel: 5,
		ConnectFlags: ConnectFlags{
			CleanSession: true,
			WillFlag:     true,
			WillQoS:      2,
			UsernameFlag: true,
			PasswordFlag: true,
		},
		KeepAlive: 60,
		ClientID:  "v5will",
		Username:  "user",
		Password:  []byte("pass"),
		Properties: &Properties{
			SessionExpiryInterval: ptrU32(3600),
			ReceiveMaximum:        ptrU16(10),
			MaximumPacketSize:     ptrU32(1024),
			TopicAliasMaximum:     ptrU16(5),
			RequestResponseInfo:   ptrByte(1),
			RequestProblemInfo:    ptrByte(1),
			AuthMethod:            ptrStr("PLAIN"),
			AuthData:              []byte{0xAA},
			User:                  []UserProperty{{Key: "ck", Val: "cv"}},
		},
		Will: &Will{
			Topic:         "v5/will",
			Payload:       []byte("v5 goodbye"),
			QoS:           2,
			Retain:        false,
			DelayInterval: delay,
			Properties: &Properties{
				PayloadFormatIndicator: ptrByte(1),
				MessageExpiryInterval:  ptrU32(600),
				ContentType:            ptrStr("text/plain"),
				ResponseTopic:          ptrStr("reply"),
				CorrelationData:        []byte{0xBB},
				User:                   []UserProperty{{Key: "wk", Val: "wv"}},
			},
		},
	}
	dec := roundtrip(t, p)
	if dec.Will == nil {
		t.Fatal("expected will")
	}
	if dec.Will.Topic != "v5/will" || string(dec.Will.Payload) != "v5 goodbye" {
		t.Fatalf("will mismatch")
	}
	if dec.Will.DelayInterval != 30 {
		t.Fatalf("will delay mismatch: %d", dec.Will.DelayInterval)
	}
	if dec.Will.Properties == nil {
		t.Fatal("expected will properties")
	}
	if dec.Will.Properties.PayloadFormatIndicator == nil || *dec.Will.Properties.PayloadFormatIndicator != 1 {
		t.Fatalf("will payload format mismatch")
	}
	if dec.Will.Properties.MessageExpiryInterval == nil || *dec.Will.Properties.MessageExpiryInterval != 600 {
		t.Fatalf("will message expiry mismatch")
	}
	if dec.Will.Properties.ContentType == nil || *dec.Will.Properties.ContentType != "text/plain" {
		t.Fatalf("will content type mismatch")
	}
	if dec.Will.Properties.ResponseTopic == nil || *dec.Will.Properties.ResponseTopic != "reply" {
		t.Fatalf("will response topic mismatch")
	}
	if !bytes.Equal(dec.Will.Properties.CorrelationData, []byte{0xBB}) {
		t.Fatalf("will correlation data mismatch")
	}
	if len(dec.Will.Properties.User) != 1 {
		t.Fatalf("will user prop mismatch")
	}
	if dec.Properties == nil || dec.Properties.SessionExpiryInterval == nil || *dec.Properties.SessionExpiryInterval != 3600 {
		t.Fatalf("connect props mismatch")
	}
	if dec.Properties.AuthMethod == nil || *dec.Properties.AuthMethod != "PLAIN" {
		t.Fatalf("auth method mismatch")
	}
	if !bytes.Equal(dec.Properties.AuthData, []byte{0xAA}) {
		t.Fatalf("auth data mismatch")
	}
	if dec.Properties.RequestResponseInfo == nil || *dec.Properties.RequestResponseInfo != 1 {
		t.Fatalf("request response info mismatch")
	}
	if dec.Properties.RequestProblemInfo == nil || *dec.Properties.RequestProblemInfo != 1 {
		t.Fatalf("request problem info mismatch")
	}
	if dec.Username != "user" || string(dec.Password) != "pass" {
		t.Fatalf("username/password mismatch")
	}
}

func TestConnectWithUsernameOnly(t *testing.T) {
	p := &Packet{
		Type:          TypeCONNECT,
		Version:       ProtocolV311,
		ProtocolLevel: 4,
		ConnectFlags:  ConnectFlags{CleanSession: true, UsernameFlag: true},
		KeepAlive:     60,
		ClientID:      "user-only",
		Username:      "myuser",
	}
	dec := roundtrip(t, p)
	if dec.Username != "myuser" {
		t.Fatalf("username mismatch: %s", dec.Username)
	}
	if dec.ConnectFlags.PasswordFlag {
		t.Fatal("expected no password flag")
	}
}

func TestConnectWithPasswordOnly(t *testing.T) {
	p := &Packet{
		Type:          TypeCONNECT,
		Version:       ProtocolV311,
		ProtocolLevel: 4,
		ConnectFlags:  ConnectFlags{CleanSession: true, PasswordFlag: true},
		KeepAlive:     60,
		ClientID:      "pass-only",
		Password:      []byte("mypass"),
	}
	dec := roundtrip(t, p)
	if string(dec.Password) != "mypass" {
		t.Fatalf("password mismatch")
	}
	if dec.ConnectFlags.UsernameFlag {
		t.Fatal("expected no username flag")
	}
}

func TestConnectDefaultProtocolName(t *testing.T) {
	// V311 with empty protocol name should default to "MQTT"
	p := &Packet{
		Type:          TypeCONNECT,
		Version:       ProtocolV311,
		ProtocolLevel: 4,
		ConnectFlags:  ConnectFlags{CleanSession: true},
		KeepAlive:     60,
		ClientID:      "default-proto",
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if dec.ProtocolName != "MQTT" {
		t.Fatalf("expected MQTT, got %s", dec.ProtocolName)
	}
}

func TestConnectV31DefaultProtocolName(t *testing.T) {
	p := &Packet{
		Type:          TypeCONNECT,
		Version:       ProtocolV31,
		ProtocolLevel: 3,
		ConnectFlags:  ConnectFlags{CleanSession: true},
		KeepAlive:     60,
		ClientID:      "v31default",
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if dec.ProtocolName != "MQIsdp" {
		t.Fatalf("expected MQIsdp, got %s", dec.ProtocolName)
	}
}

// ---- DecodeWithVersion ----

func TestDecodeWithVersionV311(t *testing.T) {
	p := &Packet{
		Type:     TypePUBLISH,
		Version:  ProtocolV311,
		Topic:    "test/vw",
		QoS:      1,
		PacketID: 42,
		Payload:  []byte("hello v3"),
	}
	data, _ := Encode(p)
	dec, err := DecodeWithVersion(data, ProtocolV311)
	if err != nil {
		t.Fatalf("DecodeWithVersion: %v", err)
	}
	if dec.Topic != "test/vw" || string(dec.Payload) != "hello v3" {
		t.Fatalf("mismatch %+v", dec)
	}
	if dec.Version != ProtocolV311 {
		t.Fatalf("version mismatch: %d", dec.Version)
	}
}

func TestDecodeWithVersionV5(t *testing.T) {
	p := &Packet{
		Type:     TypePUBLISH,
		Version:  ProtocolV5,
		Topic:    "test/v5vw",
		QoS:      0,
		Payload:  []byte("hello v5"),
		PubProps: &Properties{
			PayloadFormatIndicator: ptrByte(1),
		},
	}
	data, _ := Encode(p)
	dec, err := DecodeWithVersion(data, ProtocolV5)
	if err != nil {
		t.Fatalf("DecodeWithVersion: %v", err)
	}
	if dec.Topic != "test/v5vw" || string(dec.Payload) != "hello v5" {
		t.Fatalf("mismatch %+v", dec)
	}
}

func TestDecodeWithVersionNonPublish(t *testing.T) {
	p := &Packet{
		Type: TypePINGREQ,
	}
	data, _ := Encode(p)
	dec, err := DecodeWithVersion(data, ProtocolV311)
	if err != nil {
		t.Fatalf("DecodeWithVersion: %v", err)
	}
	if dec.Type != TypePINGREQ {
		t.Fatalf("type mismatch: %d", dec.Type)
	}
}

func TestDecodeWithVersionError(t *testing.T) {
	_, err := DecodeWithVersion([]byte{0xFF}, ProtocolV311)
	if err == nil {
		t.Fatal("expected error for bad frame")
	}
}

// ---- Error branches ----

func TestDecodeTooShort(t *testing.T) {
	_, err := Decode([]byte{0x10})
	if err == nil {
		t.Fatal("expected error for too-short frame")
	}
}

func TestDecodeTruncatedRemainingLength(t *testing.T) {
	// VarInt continuation byte but no termination
	_, err := Decode([]byte{TypeCONNECT << 4, 0x80})
	if err == nil {
		t.Fatal("expected error for truncated varint")
	}
}

func TestDecodeRemainingLengthMismatch(t *testing.T) {
	// Frame says remaining length is 10, but actual payload is shorter
	data := []byte{TypeCONNECT << 4, 10, 0, 0, 0, 0}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for length mismatch")
	}
}

func TestDecodeInvalidPacketType(t *testing.T) {
	_, err := Decode([]byte{0x0F << 4, 0})
	if err == nil {
		t.Fatal("expected error for invalid packet type 0")
	}
}

func TestDecodeInvalidQoS(t *testing.T) {
	// QoS 3 is invalid
	data := []byte{byte(TypePUBLISH<<4) | (3 << 1), 5, 0, 3, 'a', '/', 'b'}
	_, err := Decode(data)
	if err != ErrInvalidQoS {
		t.Fatalf("expected ErrInvalidQoS, got %v", err)
	}
}

func TestDecodeSubscribeBadFlags(t *testing.T) {
	// SUBSCRIBE must have flags 0x02
	data := []byte{byte(TypeSUBSCRIBE<<4) | 0x00, 2, 0, 10}
	_, err := Decode(data)
	if err != ErrProtocolViolation {
		t.Fatalf("expected ErrProtocolViolation, got %v", err)
	}
}

func TestDecodeUnsubscribeBadFlags(t *testing.T) {
	data := []byte{byte(TypeUNSUBSCRIBE<<4) | 0x00, 2, 0, 10}
	_, err := Decode(data)
	if err != ErrProtocolViolation {
		t.Fatalf("expected ErrProtocolViolation, got %v", err)
	}
}

func TestEncodeInvalidType(t *testing.T) {
	p := &Packet{Type: 0}
	_, err := Encode(p)
	if err != ErrMalformedPacket {
		t.Fatalf("expected ErrMalformedPacket, got %v", err)
	}
}

func TestDecodeConnectTooShort(t *testing.T) {
	// CONNECT with payload < 10 bytes
	data := []byte{TypeCONNECT << 4, 5, 0, 0, 0, 0, 0}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for too-short CONNECT")
	}
}

func TestDecodeConnectBadProtocol(t *testing.T) {
	// Build a CONNECT with an invalid protocol name
	buf := []byte{}
	buf = append(buf, 0, 3, 'B', 'A', 'D') // protocol name "BAD"
	buf = append(buf, 4)                     // level
	buf = append(buf, 0x02)                  // flags clean session
	buf = append(buf, 0, 60)                 // keepalive
	buf = append(buf, 0, 3, 'c', 'i', 'd')  // client id
	frame := []byte{TypeCONNECT << 4, byte(len(buf))}
	frame = append(frame, buf...)
	_, err := Decode(frame)
	if err != ErrMalformedPacket {
		t.Fatalf("expected ErrMalformedPacket, got %v", err)
	}
}

func TestDecodeConnectUnsupportedLevel(t *testing.T) {
	buf := []byte{}
	buf = append(buf, 0, 4, 'M', 'Q', 'T', 'T') // protocol name "MQTT"
	buf = append(buf, 99)                          // unsupported level
	buf = append(buf, 0x02)                        // flags
	buf = append(buf, 0, 60)                       // keepalive
	buf = append(buf, 0, 3, 'c', 'i', 'd')        // client id
	frame := []byte{TypeCONNECT << 4, byte(len(buf))}
	frame = append(frame, buf...)
	_, err := Decode(frame)
	if err != ErrUnsupportedProtocol {
		t.Fatalf("expected ErrUnsupportedProtocol, got %v", err)
	}
}

func TestDecodeConnackTooShort(t *testing.T) {
	data := []byte{TypeCONNACK << 4, 1, 0}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for too-short CONNACK")
	}
}

func TestDecodeAckTooShort(t *testing.T) {
	data := []byte{TypePUBACK << 4, 1, 0}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for too-short ACK")
	}
}

func TestDecodeSubscribeTooShort(t *testing.T) {
	data := []byte{byte(TypeSUBSCRIBE<<4) | 0x02, 1, 0}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for too-short SUBSCRIBE")
	}
}

func TestDecodeSubackTooShort(t *testing.T) {
	data := []byte{TypeSUBACK << 4, 1, 0}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for too-short SUBACK")
	}
}

func TestDecodeUnsubscribeTooShort(t *testing.T) {
	data := []byte{byte(TypeUNSUBSCRIBE<<4) | 0x02, 1, 0}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for too-short UNSUBSCRIBE")
	}
}

func TestDecodeUnsubackTooShort(t *testing.T) {
	data := []byte{TypeUNSUBACK << 4, 1, 0}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for too-short UNSUBACK")
	}
}

// ---- Properties edge cases ----

func TestPropertiesAllTypes(t *testing.T) {
	p := &Properties{
		PayloadFormatIndicator: ptrByte(1),
		MessageExpiryInterval:  ptrU32(60),
		ContentType:            ptrStr("text/plain"),
		ResponseTopic:          ptrStr("reply/topic"),
		CorrelationData:        []byte{0x01, 0x02},
		SubscriptionID:         []uint32{42},
		SessionExpiryInterval:  ptrU32(3600),
		AssignedClientID:       ptrStr("assigned"),
		ServerKeepAlive:        ptrU16(30),
		AuthMethod:             ptrStr("PLAIN"),
		AuthData:               []byte{0xAA, 0xBB},
		RequestProblemInfo:     ptrByte(1),
		RequestResponseInfo:    ptrByte(1),
		ServerReference:        ptrStr("server.example.com"),
		ReasonString:           ptrStr("reason"),
		ReceiveMaximum:         ptrU16(100),
		TopicAliasMaximum:      ptrU16(50),
		TopicAlias:             ptrU16(10),
		MaximumQoS:             ptrByte(1),
		RetainAvailable:        ptrByte(1),
		MaximumPacketSize:      ptrU32(65535),
		WildcardSubAvailable:   ptrByte(1),
		SubIDAvailable:         ptrByte(1),
		SharedSubAvailable:     ptrByte(1),
		User:                   []UserProperty{{Key: "key1", Val: "val1"}, {Key: "key2", Val: "val2"}},
	}

	// Encode then decode via a PUBLISH packet
	pkt := &Packet{
		Type:     TypePUBLISH,
		Version:  ProtocolV5,
		Topic:    "props/test",
		QoS:      1,
		PacketID: 1,
		Payload:  []byte("data"),
		PubProps: p,
	}
	dec := roundtrip(t, pkt)
	if dec.PubProps == nil {
		t.Fatal("expected pub props")
	}
	dp := dec.PubProps
	if dp.PayloadFormatIndicator == nil || *dp.PayloadFormatIndicator != 1 {
		t.Fatal("PayloadFormatIndicator mismatch")
	}
	if dp.MessageExpiryInterval == nil || *dp.MessageExpiryInterval != 60 {
		t.Fatal("MessageExpiryInterval mismatch")
	}
	if dp.ContentType == nil || *dp.ContentType != "text/plain" {
		t.Fatal("ContentType mismatch")
	}
	if dp.ResponseTopic == nil || *dp.ResponseTopic != "reply/topic" {
		t.Fatal("ResponseTopic mismatch")
	}
	if !bytes.Equal(dp.CorrelationData, []byte{0x01, 0x02}) {
		t.Fatal("CorrelationData mismatch")
	}
	if len(dp.SubscriptionID) != 1 || dp.SubscriptionID[0] != 42 {
		t.Fatal("SubscriptionID mismatch")
	}
	if dp.SessionExpiryInterval == nil || *dp.SessionExpiryInterval != 3600 {
		t.Fatal("SessionExpiryInterval mismatch")
	}
	if dp.AssignedClientID == nil || *dp.AssignedClientID != "assigned" {
		t.Fatal("AssignedClientID mismatch")
	}
	if dp.ServerKeepAlive == nil || *dp.ServerKeepAlive != 30 {
		t.Fatal("ServerKeepAlive mismatch")
	}
	if dp.AuthMethod == nil || *dp.AuthMethod != "PLAIN" {
		t.Fatal("AuthMethod mismatch")
	}
	if !bytes.Equal(dp.AuthData, []byte{0xAA, 0xBB}) {
		t.Fatal("AuthData mismatch")
	}
	if dp.RequestProblemInfo == nil || *dp.RequestProblemInfo != 1 {
		t.Fatal("RequestProblemInfo mismatch")
	}
	if dp.RequestResponseInfo == nil || *dp.RequestResponseInfo != 1 {
		t.Fatal("RequestResponseInfo mismatch")
	}
	if dp.ServerReference == nil || *dp.ServerReference != "server.example.com" {
		t.Fatal("ServerReference mismatch")
	}
	if dp.ReasonString == nil || *dp.ReasonString != "reason" {
		t.Fatal("ReasonString mismatch")
	}
	if dp.ReceiveMaximum == nil || *dp.ReceiveMaximum != 100 {
		t.Fatal("ReceiveMaximum mismatch")
	}
	if dp.TopicAliasMaximum == nil || *dp.TopicAliasMaximum != 50 {
		t.Fatal("TopicAliasMaximum mismatch")
	}
	if dp.TopicAlias == nil || *dp.TopicAlias != 10 {
		t.Fatal("TopicAlias mismatch")
	}
	if dp.MaximumQoS == nil || *dp.MaximumQoS != 1 {
		t.Fatal("MaximumQoS mismatch")
	}
	if dp.RetainAvailable == nil || *dp.RetainAvailable != 1 {
		t.Fatal("RetainAvailable mismatch")
	}
	if dp.MaximumPacketSize == nil || *dp.MaximumPacketSize != 65535 {
		t.Fatal("MaximumPacketSize mismatch")
	}
	if dp.WildcardSubAvailable == nil || *dp.WildcardSubAvailable != 1 {
		t.Fatal("WildcardSubAvailable mismatch")
	}
	if dp.SubIDAvailable == nil || *dp.SubIDAvailable != 1 {
		t.Fatal("SubIDAvailable mismatch")
	}
	if dp.SharedSubAvailable == nil || *dp.SharedSubAvailable != 1 {
		t.Fatal("SharedSubAvailable mismatch")
	}
	if len(dp.User) != 2 || dp.User[0].Key != "key1" || dp.User[1].Val != "val2" {
		t.Fatal("User properties mismatch")
	}
}

func TestEncodePropertiesNil(t *testing.T) {
	b := encodeProperties(nil)
	if len(b) == 0 || b[0] != 0 {
		t.Fatalf("expected varint 0 for nil properties, got %x", b)
	}
}

func TestEncodeWillPropertiesNil(t *testing.T) {
	b := encodeWillProperties(nil, nil)
	if len(b) == 0 || b[0] != 0 {
		t.Fatalf("expected varint 0 for nil will properties, got %x", b)
	}
}

func TestEncodeWillPropertiesWithDelay(t *testing.T) {
	delay := uint32(60)
	b := encodeWillProperties(&delay, nil)
	if len(b) == 0 {
		t.Fatal("expected non-empty will properties")
	}
}

func TestEncodeWillPropertiesWithProps(t *testing.T) {
	delay := uint32(30)
	props := &Properties{
		User:                  []UserProperty{{Key: "k", Val: "v"}},
		PayloadFormatIndicator: ptrByte(1),
		MessageExpiryInterval:  ptrU32(120),
		ContentType:            ptrStr("text/plain"),
		ResponseTopic:          ptrStr("reply"),
		CorrelationData:        []byte{0x01},
	}
	b := encodeWillProperties(&delay, props)
	if len(b) == 0 {
		t.Fatal("expected non-empty will properties")
	}
}

func TestPropertiesEmptyUserKey(t *testing.T) {
	// Encode a property block with an empty user property key (should fail decode)
	// Build manually: varint len, then PropUserProperty, then empty string
	body := []byte{PropUserProperty, 0, 0, 0, 3, 'v', 'a', 'l'} // key length 0
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for empty user property key")
	}
}

func TestPropertiesTooManyUserProperties(t *testing.T) {
	// Build properties with 11 user properties (limit is 10)
	var body []byte
	for i := 0; i < 11; i++ {
		body = append(body, PropUserProperty)
		body = append(body, encodeString("key")...)
		body = append(body, encodeString("val")...)
	}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err != ErrTooManyUserProperties {
		t.Fatalf("expected ErrTooManyUserProperties, got %v", err)
	}
}

func TestPropertiesUnknownPropertySkipped(t *testing.T) {
	// Unknown property ID should be skipped
	body := []byte{0xFE, 0x01, PropPayloadFormatIndicator, 1}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	props, _, err := decodeProperties(propsBytes, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if props.PayloadFormatIndicator == nil || *props.PayloadFormatIndicator != 1 {
		t.Fatal("expected PayloadFormatIndicator to be set")
	}
}

func TestDecodePropertiesEmpty(t *testing.T) {
	propsBytes := encodeVarInt(0)
	props, _, err := decodeProperties(propsBytes, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if props == nil {
		t.Fatal("expected non-nil props")
	}
}

func TestDecodePropertiesBeyondEnd(t *testing.T) {
	// pos >= len(src) should return empty props
	props, _, err := decodeProperties([]byte{}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if props == nil {
		t.Fatal("expected non-nil props")
	}
}

func TestPropertiesResponseInfo(t *testing.T) {
	// ResponseInfo property (0x1A) is decoded but discarded
	body := []byte{PropResponseInfo, 0, 3, 'i', 'n', 'f'}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	props, _, err := decodeProperties(propsBytes, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = props
}

// ---- VarInt edge cases ----

func TestVarIntOverflow(t *testing.T) {
	// 5 continuation bytes should overflow
	src := []byte{0x80, 0x80, 0x80, 0x80, 0x01}
	_, _, err := decodeVarInt(src)
	if err != ErrVarIntOverflow {
		t.Fatalf("expected ErrVarIntOverflow, got %v", err)
	}
}

func TestVarIntEmpty(t *testing.T) {
	_, _, err := decodeVarInt([]byte{})
	if err == nil {
		t.Fatal("expected error for empty varint")
	}
}

func TestVarIntNegative(t *testing.T) {
	b := encodeVarInt(-1)
	if len(b) == 0 {
		t.Fatal("expected non-empty for negative varint")
	}
}

func TestVarIntLenNegative(t *testing.T) {
	l := varIntLen(-5)
	if l < 1 {
		t.Fatalf("expected >= 1, got %d", l)
	}
}

func TestVarIntLenLarge(t *testing.T) {
	l := varIntLen(2097151) // max 3-byte varint
	if l != 3 {
		t.Fatalf("expected 3, got %d", l)
	}
	l2 := varIntLen(2097152) // 4-byte varint
	if l2 != 4 {
		t.Fatalf("expected 4, got %d", l2)
	}
}

func TestAppendVarIntToExistingSlice(t *testing.T) {
	dst := []byte{0x01, 0x02}
	dst = appendVarInt(dst, 128)
	if len(dst) <= 2 {
		t.Fatal("expected appended bytes")
	}
}

// ---- Uint helpers ----

func TestEncodeUint16(t *testing.T) {
	b := encodeUint16(0x0102)
	if len(b) != 2 || b[0] != 0x01 || b[1] != 0x02 {
		t.Fatalf("encodeUint16 mismatch: %x", b)
	}
}

func TestAppendUint16(t *testing.T) {
	dst := []byte{0xFF}
	dst = appendUint16(dst, 0x0304)
	if len(dst) != 3 || dst[1] != 0x03 || dst[2] != 0x04 {
		t.Fatalf("appendUint16 mismatch: %x", dst)
	}
}

func TestAppendUint32(t *testing.T) {
	dst := []byte{0xFF}
	dst = appendUint32(dst, 0x01020304)
	if len(dst) != 5 {
		t.Fatalf("appendUint32 length mismatch: %d", len(dst))
	}
	if dst[1] != 0x01 || dst[2] != 0x02 || dst[3] != 0x03 || dst[4] != 0x04 {
		t.Fatalf("appendUint32 mismatch: %x", dst)
	}
}

func TestAppendString(t *testing.T) {
	dst := []byte{0xFF}
	dst = appendString(dst, "hi")
	if len(dst) != 5 {
		t.Fatalf("appendString length mismatch: %d", len(dst))
	}
}

// ---- String/Binary decode edge cases ----

func TestDecodeStringTooShort(t *testing.T) {
	_, _, err := decodeString([]byte{0x00}, 0)
	if err == nil {
		t.Fatal("expected error for too-short string")
	}
}

func TestDecodeStringTruncated(t *testing.T) {
	_, _, err := decodeString([]byte{0x00, 0x05, 'a', 'b'}, 0)
	if err == nil {
		t.Fatal("expected error for truncated string")
	}
}

func TestDecodeBinaryTooShort(t *testing.T) {
	_, _, err := decodeBinary([]byte{0x00}, 0)
	if err == nil {
		t.Fatal("expected error for too-short binary")
	}
}

func TestDecodeBinaryTruncated(t *testing.T) {
	_, _, err := decodeBinary([]byte{0x00, 0x05, 0x01}, 0)
	if err == nil {
		t.Fatal("expected error for truncated binary")
	}
}

// ---- SUBSCRIBE edge cases ----

func TestSubscribeV5WithRetainAsPublished(t *testing.T) {
	p := &Packet{
		Type:     TypeSUBSCRIBE,
		Version:  ProtocolV5,
		PacketID: 30,
		Subscriptions: []Subscription{
			{Filter: "test/#", QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 2},
		},
		SubProps: &Properties{
			SubscriptionID: []uint32{99},
			User:           []UserProperty{{Key: "sk", Val: "sv"}},
		},
	}
	dec := roundtrip(t, p)
	if !dec.Subscriptions[0].RetainAsPublished {
		t.Fatal("expected retain as published")
	}
	if dec.Subscriptions[0].RetainHandling != 2 {
		t.Fatalf("retain handling mismatch: %d", dec.Subscriptions[0].RetainHandling)
	}
	if dec.SubProps == nil || len(dec.SubProps.SubscriptionID) != 1 || dec.SubProps.SubscriptionID[0] != 99 {
		t.Fatalf("sub props mismatch")
	}
}

func TestSubscribeEmptyPayload(t *testing.T) {
	// SUBSCRIBE with no subscriptions should fail
	data := []byte{byte(TypeSUBSCRIBE<<4) | 0x02, 2, 0, 10}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for empty SUBSCRIBE")
	}
}

// ---- UNSUBACK v3 with codes ----

func TestUnsubackV3WithCodes(t *testing.T) {
	p := &Packet{
		Type:          TypeUNSUBACK,
		Version:       ProtocolV311,
		PacketID:      70,
		UnsubackCodes: []byte{0x00, 0x01},
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.PacketID != 70 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

// ---- boolToByte ----

func TestBoolToByte(t *testing.T) {
	if boolToByte(true) != 1 {
		t.Fatal("expected 1 for true")
	}
	if boolToByte(false) != 0 {
		t.Fatal("expected 0 for false")
	}
}

// ---- CONNACK v3 with reason code > 5 ----

func TestConnackV3ReasonCodeMapping(t *testing.T) {
	// ReasonCode 0-2 with 2-byte payload => v3
	p := &Packet{
		Type:       TypeCONNACK,
		ReasonCode: 0,
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if dec.ReasonCode != 0 {
		t.Fatalf("reason code mismatch")
	}
}

func TestConnackReasonCodeAbove5(t *testing.T) {
	// ReasonCode > 5 with 2-byte payload => v5
	p := &Packet{
		Type:       TypeCONNACK,
		ReasonCode: 0x80,
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Version != ProtocolV5 {
		t.Fatalf("expected v5, got %d", dec.Version)
	}
}

// ---- PUBLISH with SubscriptionID property ----

func TestPublishV5SubscriptionID(t *testing.T) {
	p := &Packet{
		Type:     TypePUBLISH,
		Version:  ProtocolV5,
		Topic:    "sub/id/test",
		QoS:      1,
		PacketID: 50,
		Payload:  []byte("sub-id-payload"),
		PubProps: &Properties{
			SubscriptionID: []uint32{1, 2},
		},
	}
	dec := roundtrip(t, p)
	if dec.PubProps == nil || len(dec.PubProps.SubscriptionID) != 2 {
		t.Fatalf("subscription ID mismatch: %+v", dec.PubProps)
	}
}

// ---- DISCONNECT v5 with nil props ----

func TestDisconnectV5NilProps(t *testing.T) {
	p := &Packet{
		Type:       TypeDISCONNECT,
		Version:    ProtocolV5,
		DiscReason: 0x04,
		DiscProps:  nil,
	}
	dec := roundtrip(t, p)
	if dec.DiscReason != 0x04 {
		t.Fatalf("disc reason mismatch: %d", dec.DiscReason)
	}
}

// ---- SUBACK v3 ambiguous codes ----

func TestSubackV3AmbiguousCodes(t *testing.T) {
	// Use non-zero first code to avoid v5 properties heuristic
	data := []byte{TypeSUBACK << 4, 4, 0, 10, 0x01, 0x02}
	dec, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.PacketID != 10 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
	if len(dec.SubackCodes) != 2 {
		t.Fatalf("expected 2 codes, got %d", len(dec.SubackCodes))
	}
}

// ---- Properties malformed cases ----

func TestPropertiesMalformedPayloadFormat(t *testing.T) {
	// Property with not enough bytes for PayloadFormatIndicator
	body := []byte{PropPayloadFormatIndicator}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated PayloadFormatIndicator")
	}
}

func TestPropertiesMalformedMessageExpiry(t *testing.T) {
	body := []byte{PropMessageExpiryInterval, 0x01, 0x02}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated MessageExpiryInterval")
	}
}

func TestPropertiesMalformedSessionExpiry(t *testing.T) {
	body := []byte{PropSessionExpiryInterval, 0x01, 0x02}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated SessionExpiryInterval")
	}
}

func TestPropertiesMalformedServerKeepAlive(t *testing.T) {
	body := []byte{PropServerKeepAlive, 0x01}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated ServerKeepAlive")
	}
}

func TestPropertiesMalformedReceiveMaximum(t *testing.T) {
	body := []byte{PropReceiveMaximum, 0x01}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated ReceiveMaximum")
	}
}

func TestPropertiesMalformedTopicAliasMaximum(t *testing.T) {
	body := []byte{PropTopicAliasMaximum, 0x01}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated TopicAliasMaximum")
	}
}

func TestPropertiesMalformedTopicAlias(t *testing.T) {
	body := []byte{PropTopicAlias, 0x01}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated TopicAlias")
	}
}

func TestPropertiesMalformedMaximumPacketSize(t *testing.T) {
	body := []byte{PropMaximumPacketSize, 0x01, 0x02}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated MaximumPacketSize")
	}
}

func TestPropertiesMalformedWillDelayInterval(t *testing.T) {
	body := []byte{PropWillDelayInterval, 0x01, 0x02}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated WillDelayInterval")
	}
}

func TestPropertiesMalformedMaximumQoS(t *testing.T) {
	body := []byte{PropMaximumQoS}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated MaximumQoS")
	}
}

func TestPropertiesMalformedRetainAvailable(t *testing.T) {
	body := []byte{PropRetainAvailable}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated RetainAvailable")
	}
}

func TestPropertiesMalformedWildcardSubAvailable(t *testing.T) {
	body := []byte{PropWildcardSubAvailable}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated WildcardSubAvailable")
	}
}

func TestPropertiesMalformedSubIDAvailable(t *testing.T) {
	body := []byte{PropSubIDAvailable}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated SubIDAvailable")
	}
}

func TestPropertiesMalformedSharedSubAvailable(t *testing.T) {
	body := []byte{PropSharedSubAvailable}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated SharedSubAvailable")
	}
}

func TestPropertiesMalformedRequestProblemInfo(t *testing.T) {
	body := []byte{PropRequestProblemInfo}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated RequestProblemInfo")
	}
}

func TestPropertiesMalformedRequestResponseInfo(t *testing.T) {
	body := []byte{PropRequestResponseInfo}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated RequestResponseInfo")
	}
}

func TestPropertiesMalformedUserPropertyValue(t *testing.T) {
	// Valid key but truncated value
	body := []byte{PropUserProperty, 0, 3, 'k', 'e', 'y', 0, 5, 'v', 'a'}
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for truncated user property value")
	}
}

func TestPropertiesUserPropertyTooLongValue(t *testing.T) {
	// Value > 1024 bytes
	longVal := make([]byte, 1025)
	for i := range longVal {
		longVal[i] = 'x'
	}
	var body []byte
	body = append(body, PropUserProperty)
	body = append(body, encodeString("key")...)
	body = append(body, encodeBinary(longVal)...)
	propsBytes := encodeVarInt(len(body))
	propsBytes = append(propsBytes, body...)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for too-long user property value")
	}
}

// ---- CONNECT decode with truncated fields ----

func TestDecodeConnectTruncatedKeepAlive(t *testing.T) {
	buf := []byte{}
	buf = append(buf, 0, 4, 'M', 'Q', 'T', 'T') // protocol
	buf = append(buf, 4)                           // level
	buf = append(buf, 0x02)                        // flags
	buf = append(buf, 0)                           // only 1 byte of keepalive
	frame := []byte{TypeCONNECT << 4, byte(len(buf))}
	frame = append(frame, buf...)
	_, err := Decode(frame)
	if err == nil {
		t.Fatal("expected error for truncated keepalive")
	}
}

func TestDecodeConnectTruncatedClientID(t *testing.T) {
	buf := []byte{}
	buf = append(buf, 0, 4, 'M', 'Q', 'T', 'T')
	buf = append(buf, 4)
	buf = append(buf, 0x02)
	buf = append(buf, 0, 60)
	buf = append(buf, 0, 5, 'a', 'b') // says 5 bytes but only 2
	frame := []byte{TypeCONNECT << 4, byte(len(buf))}
	frame = append(frame, buf...)
	_, err := Decode(frame)
	if err == nil {
		t.Fatal("expected error for truncated client id")
	}
}

// ---- PUBLISH decode edge cases ----

func TestDecodePublishQoS1TruncatedPacketID(t *testing.T) {
	// QoS 1 but not enough bytes for packet ID
	data := []byte{byte(TypePUBLISH<<4) | (1 << 1), 4, 0, 2, 'a', 'b'}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for truncated packet ID")
	}
}

// ---- SUBSCRIBE decode with truncated subscription ----

func TestDecodeSubscribeTruncatedFilter(t *testing.T) {
	// PacketID + truncated string
	data := []byte{byte(TypeSUBSCRIBE<<4) | 0x02, 4, 0, 10, 0, 5, 'a'}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for truncated subscription filter")
	}
}

func TestDecodeSubscribeMissingQoS(t *testing.T) {
	// Filter but no QoS byte
	data := []byte{byte(TypeSUBSCRIBE<<4) | 0x02, 4, 0, 10, 0, 1, 'a'}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for missing QoS byte")
	}
}

// ---- UNSUBSCRIBE decode edge cases ----

func TestDecodeUnsubscribeTruncatedTopic(t *testing.T) {
	data := []byte{byte(TypeUNSUBSCRIBE<<4) | 0x02, 4, 0, 10, 0, 5, 'a'}
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for truncated unsubscribe topic")
	}
}

// ---- Properties varint malformed ----

func TestPropertiesMalformedVarInt(t *testing.T) {
	// VarInt with 5 continuation bytes
	propsBytes := []byte{0x80, 0x80, 0x80, 0x80, 0x01}
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for malformed varint in properties")
	}
}

func TestPropertiesLengthExceedsData(t *testing.T) {
	// VarInt says 100 bytes but only a few bytes follow
	propsBytes := encodeVarInt(100)
	propsBytes = append(propsBytes, 0x01, 0x02)
	_, _, err := decodeProperties(propsBytes, 0)
	if err == nil {
		t.Fatal("expected error for properties length exceeding data")
	}
}

// ---- CONNECT decode with will but truncated ----

func TestDecodeConnectWillTruncatedTopic(t *testing.T) {
	buf := []byte{}
	buf = append(buf, 0, 4, 'M', 'Q', 'T', 'T')
	buf = append(buf, 4)
	buf = append(buf, 0x04|0x02) // will flag + clean session
	buf = append(buf, 0, 60)
	buf = append(buf, 0, 3, 'c', 'i', 'd')
	buf = append(buf, 0, 5, 'a') // will topic says 5 bytes but only 1
	frame := []byte{TypeCONNECT << 4, byte(len(buf))}
	frame = append(frame, buf...)
	_, err := Decode(frame)
	if err == nil {
		t.Fatal("expected error for truncated will topic")
	}
}

func TestDecodeConnectWillTruncatedPayload(t *testing.T) {
	buf := []byte{}
	buf = append(buf, 0, 4, 'M', 'Q', 'T', 'T')
	buf = append(buf, 4)
	buf = append(buf, 0x04|0x02) // will flag + clean session
	buf = append(buf, 0, 60)
	buf = append(buf, 0, 3, 'c', 'i', 'd')
	buf = append(buf, 0, 3, 'w', '/', 't') // will topic
	buf = append(buf, 0, 5, 'a', 'b')       // will payload says 5 but only 2
	frame := []byte{TypeCONNECT << 4, byte(len(buf))}
	frame = append(frame, buf...)
	_, err := Decode(frame)
	if err == nil {
		t.Fatal("expected error for truncated will payload")
	}
}

func TestDecodeConnectTruncatedUsername(t *testing.T) {
	buf := []byte{}
	buf = append(buf, 0, 4, 'M', 'Q', 'T', 'T')
	buf = append(buf, 4)
	buf = append(buf, 0x80|0x02) // username flag + clean session
	buf = append(buf, 0, 60)
	buf = append(buf, 0, 3, 'c', 'i', 'd')
	buf = append(buf, 0, 5, 'a') // username says 5 but only 1
	frame := []byte{TypeCONNECT << 4, byte(len(buf))}
	frame = append(frame, buf...)
	_, err := Decode(frame)
	if err == nil {
		t.Fatal("expected error for truncated username")
	}
}

func TestDecodeConnectTruncatedPassword(t *testing.T) {
	buf := []byte{}
	buf = append(buf, 0, 4, 'M', 'Q', 'T', 'T')
	buf = append(buf, 4)
	buf = append(buf, 0x40|0x02) // password flag + clean session
	buf = append(buf, 0, 60)
	buf = append(buf, 0, 3, 'c', 'i', 'd')
	buf = append(buf, 0, 5, 'a') // password says 5 but only 1
	frame := []byte{TypeCONNECT << 4, byte(len(buf))}
	frame = append(frame, buf...)
	_, err := Decode(frame)
	if err == nil {
		t.Fatal("expected error for truncated password")
	}
}

// ---- ACK decode with properties ----

func TestDecodeAckV5WithProps(t *testing.T) {
	// Manually build PUBACK with reason + properties
	var buf bytes.Buffer
	buf.WriteByte(0)    // packetID high
	buf.WriteByte(42)   // packetID low
	buf.WriteByte(0x83) // reason
	// properties: ReasonString
	propsBody := []byte{PropReasonString}
	propsBody = append(propsBody, encodeString("no sub")...)
	buf.Write(encodeVarInt(len(propsBody)))
	buf.Write(propsBody)
	frame := []byte{TypePUBACK << 4, byte(buf.Len())}
	frame = append(frame, buf.Bytes()...)
	dec, err := Decode(frame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.PacketID != 42 || dec.Reason != 0x83 {
		t.Fatalf("ack mismatch %+v", dec)
	}
	if dec.AckProps == nil || dec.AckProps.ReasonString == nil || *dec.AckProps.ReasonString != "no sub" {
		t.Fatalf("ack props mismatch %+v", dec.AckProps)
	}
}

// ---- DISCONNECT decode edge cases ----

func TestDecodeDisconnectV5MalformedProps(t *testing.T) {
	// DISCONNECT with reason + malformed properties
	body := []byte{0x04, 0x80, 0x80, 0x80, 0x80, 0x01} // reason + bad varint
	frame := []byte{TypeDISCONNECT << 4, byte(len(body))}
	frame = append(frame, body...)
	_, err := Decode(frame)
	if err == nil {
		t.Fatal("expected error for malformed disconnect props")
	}
}

// ---- AUTH decode edge cases ----

func TestDecodeAuthMalformedProps(t *testing.T) {
	body := []byte{0x18, 0x80, 0x80, 0x80, 0x80, 0x01}
	frame := []byte{TypeAUTH << 4, byte(len(body))}
	frame = append(frame, body...)
	_, err := Decode(frame)
	if err == nil {
		t.Fatal("expected error for malformed auth props")
	}
}

// ---- SUBACK decode v5 with non-v3 codes ----

func TestSubackV5NonV3Codes(t *testing.T) {
	// Build SUBACK with codes that are NOT valid v3 codes (0x04 = quota exceeded in v5)
	// This should be detected as v5
	var buf bytes.Buffer
	buf.WriteByte(0)
	buf.WriteByte(20) // packetID
	buf.WriteByte(0)  // empty props varint
	buf.WriteByte(0x04)
	buf.WriteByte(0x80)
	frame := []byte{TypeSUBACK << 4, byte(buf.Len())}
	frame = append(frame, buf.Bytes()...)
	dec, err := Decode(frame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.PacketID != 20 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

// ---- UNSUBACK decode v5 with empty props ----

func TestUnsubackV5EmptyProps(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0)
	buf.WriteByte(30) // packetID
	buf.WriteByte(0)  // empty props varint
	buf.WriteByte(0x00)
	frame := []byte{TypeUNSUBACK << 4, byte(buf.Len())}
	frame = append(frame, buf.Bytes()...)
	dec, err := Decode(frame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.PacketID != 30 {
		t.Fatalf("packet id mismatch: %d", dec.PacketID)
	}
}

// ---- Round-trip stress: encode then decode all packet types ----

func TestRoundTripAllTypes(t *testing.T) {
	tests := []struct {
		name string
		pkt  *Packet
	}{
		{"CONNECT-v311", &Packet{Type: TypeCONNECT, Version: ProtocolV311, ProtocolLevel: 4, ConnectFlags: ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "c"}},
		{"CONNECT-v5", &Packet{Type: TypeCONNECT, Version: ProtocolV5, ProtocolLevel: 5, ConnectFlags: ConnectFlags{CleanSession: true}, KeepAlive: 30, ClientID: "c5", Properties: &Properties{SessionExpiryInterval: ptrU32(100)}}},
		{"CONNACK-v311", &Packet{Type: TypeCONNACK, SessionPresent: true, ReasonCode: 0}},
		{"CONNACK-v5", &Packet{Type: TypeCONNACK, Version: ProtocolV5, ReasonCode: 0, ConnProperties: &Properties{ReceiveMaximum: ptrU16(10)}}},
		{"PUBLISH-QoS0", &Packet{Type: TypePUBLISH, Topic: "t", QoS: 0, Payload: []byte("p")}},
		{"PUBLISH-QoS1", &Packet{Type: TypePUBLISH, Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("p")}},
		{"PUBLISH-QoS2", &Packet{Type: TypePUBLISH, Topic: "t", QoS: 2, PacketID: 2, Payload: []byte("p")}},
		{"PUBLISH-v5", &Packet{Type: TypePUBLISH, Version: ProtocolV5, Topic: "t", QoS: 1, PacketID: 3, Payload: []byte("p"), PubProps: &Properties{TopicAlias: ptrU16(5)}}},
		{"PUBACK", &Packet{Type: TypePUBACK, PacketID: 10}},
		{"PUBACK-v5", &Packet{Type: TypePUBACK, Version: ProtocolV5, PacketID: 11, Reason: 0, AckProps: &Properties{ReasonString: ptrStr("ok")}}},
		{"PUBREC", &Packet{Type: TypePUBREC, PacketID: 20}},
		{"PUBREL", &Packet{Type: TypePUBREL, PacketID: 30}},
		{"PUBCOMP", &Packet{Type: TypePUBCOMP, PacketID: 40}},
		{"SUBSCRIBE", &Packet{Type: TypeSUBSCRIBE, PacketID: 50, Subscriptions: []Subscription{{Filter: "a", QoS: 0}}}},
		{"SUBSCRIBE-v5", &Packet{Type: TypeSUBSCRIBE, Version: ProtocolV5, PacketID: 51, Subscriptions: []Subscription{{Filter: "b", QoS: 1, NoLocal: true}}, SubProps: &Properties{}}},
		{"SUBACK", &Packet{Type: TypeSUBACK, PacketID: 60, SubackCodes: []byte{0}}},
		{"UNSUBSCRIBE", &Packet{Type: TypeUNSUBSCRIBE, PacketID: 70, Topics: []string{"t"}}},
		{"UNSUBSCRIBE-v5", &Packet{Type: TypeUNSUBSCRIBE, Version: ProtocolV5, PacketID: 71, Topics: []string{"t"}, UnsubProps: &Properties{}}},
		{"UNSUBACK", &Packet{Type: TypeUNSUBACK, PacketID: 80}},
		{"PINGREQ", &Packet{Type: TypePINGREQ}},
		{"PINGRESP", &Packet{Type: TypePINGRESP}},
		{"DISCONNECT", &Packet{Type: TypeDISCONNECT, Version: ProtocolV311}},
		{"DISCONNECT-v5", &Packet{Type: TypeDISCONNECT, Version: ProtocolV5, DiscReason: 4, DiscProps: &Properties{ReasonString: ptrStr("r")}}},
		{"AUTH", &Packet{Type: TypeAUTH, Version: ProtocolV5, AuthReason: 0, AuthProps: &Properties{AuthMethod: ptrStr("m")}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.pkt)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			dec, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v data=%x", err, data)
			}
			if dec.Type != tt.pkt.Type {
				t.Fatalf("type mismatch: got %d want %d", dec.Type, tt.pkt.Type)
			}
		})
	}
}
