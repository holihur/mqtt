package codec

import "testing"

func TestConnectV311Roundtrip(t *testing.T) {
	p := &Packet{
		Type:          TypeCONNECT,
		Version:       ProtocolV311,
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		ConnectFlags:  ConnectFlags{CleanSession: true},
		KeepAlive:     60,
		ClientID:      "test123",
	}
	data, err := Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v data=%x", err, data)
	}
	if dec.ClientID != "test123" || dec.KeepAlive != 60 || dec.Version != ProtocolV311 {
		t.Fatalf("mismatch %+v", dec)
	}
}

func TestConnectV5Roundtrip(t *testing.T) {
	exp := uint32(3600)
	p := &Packet{
		Type:          TypeCONNECT,
		Version:       ProtocolV5,
		ProtocolName:  "MQTT",
		ProtocolLevel: 5,
		ConnectFlags:  ConnectFlags{CleanSession: true, UsernameFlag: true, PasswordFlag: true},
		KeepAlive:     30,
		ClientID:      "v5client",
		Username:      "user",
		Password:      []byte("pass"),
		Properties:    &Properties{SessionExpiryInterval: &exp},
	}
	data, err := Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := Decode(data)
	if err != nil {
		t.Fatalf("decode v5: %v", err)
	}
	if dec.ClientID != "v5client" || dec.Username != "user" {
		t.Fatalf("mismatch v5 %+v", dec)
	}
	if dec.Properties == nil || dec.Properties.SessionExpiryInterval == nil || *dec.Properties.SessionExpiryInterval != 3600 {
		t.Fatalf("props mismatch %+v", dec.Properties)
	}
}

func TestPublishQoS1Roundtrip(t *testing.T) {
	p := &Packet{
		Type:     TypePUBLISH,
		Version:  ProtocolV311,
		Topic:    "a/b",
		QoS:      1,
		PacketID: 123,
		Payload:  []byte("hello"),
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Topic != "a/b" || string(dec.Payload) != "hello" || dec.PacketID != 123 || dec.QoS != 1 {
		t.Fatalf("publish mismatch %+v", dec)
	}
}

func TestPublishV5WithProps(t *testing.T) {
	pi := byte(1)
	p := &Packet{
		Type:     TypePUBLISH,
		Version:  ProtocolV5,
		Topic:    "test/topic",
		QoS:      0,
		Payload:  []byte("v5 payload"),
		PubProps: &Properties{PayloadFormatIndicator: &pi, User: []UserProperty{{Key: "k", Val: "v"}}},
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if dec.PubProps == nil || dec.PubProps.PayloadFormatIndicator == nil || *dec.PubProps.PayloadFormatIndicator != 1 {
		t.Fatalf("v5 props mismatch %+v", dec.PubProps)
	}
}

func TestSubscribeRoundtrip(t *testing.T) {
	p := &Packet{
		Type:          TypeSUBSCRIBE,
		Version:       ProtocolV311,
		PacketID:      10,
		Subscriptions: []Subscription{{Filter: "a/b", QoS: 1}, {Filter: "c/#", QoS: 0}},
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(dec.Subscriptions) != 2 || dec.Subscriptions[0].Filter != "a/b" {
		t.Fatalf("sub mismatch %+v", dec)
	}
}

func TestSubscribeV5WithOptions(t *testing.T) {
	p := &Packet{
		Type:          TypeSUBSCRIBE,
		Version:       ProtocolV5,
		PacketID:      20,
		Subscriptions: []Subscription{{Filter: "a/+", QoS: 1, NoLocal: true, RetainHandling: 1}},
		SubProps:      &Properties{},
	}
	data, _ := Encode(p)
	dec, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if !dec.Subscriptions[0].NoLocal || dec.Subscriptions[0].RetainHandling != 1 {
		t.Fatalf("v5 sub opts mismatch %+v", dec.Subscriptions[0])
	}
}
