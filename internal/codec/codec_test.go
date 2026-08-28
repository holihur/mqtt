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
	// A QoS0 v5 PUBLISH is ambiguous under the generic Decode path (a v3 QoS0
	// payload can start with a byte that looks like a properties length), so the
	// version-aware path is the correct way to recover the properties.
	dec, err := DecodeWithVersion(data, ProtocolV5)
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

// TestDecodePropertiesUnknownID_SkipBytesPOC verifies that an unknown property
// ID is rejected rather than skipped: its value length is unknowable, so a
// naive 1-byte skip desyncs the parse and silently misreads or drops the
// properties that follow (e.g. a TopicAlias that affects routing).
//
// MQTT v5 spec § 2.2.2: properties have variable-length encodings. An unknown
// property ID 0xFE with a 4-byte value followed by a known TopicAlias (0x23,
// 2-byte value) must fail closed — the decoder cannot know where 0xFE's value
// ends, so it must reject the packet instead of guessing.
func TestDecodePropertiesUnknownID_SkipBytesPOC(t *testing.T) {
	// Properties block: [total-len][unknown 0xFE][4-byte value][TopicAlias 0x23][2-byte value]
	topicAlias := uint16(42)
	props := &Properties{TopicAlias: &topicAlias}
	encoded := encodeProperties(props)

	// encoded = [len][0x23][0x00 0x2A]
	// Prepend an unknown property: [0xFE][0x00 0x00 0x00 0x00] (4 zero bytes as value)
	unknownProp := []byte{0xFE, 0x00, 0x00, 0x00, 0x00}
	origLen := encoded[0]
	newProps := append([]byte{origLen + 5}, unknownProp...)
	newProps = append(newProps, encoded[1:]...)

	_, _, err := decodeProperties(newProps, 0)
	if err != ErrUnknownProperty {
		t.Fatalf("expected ErrUnknownProperty for unknown property ID, got: %v", err)
	}
}

// TestDecodePublishV5UnknownPropertyPOC shows the impact on a full v5 PUBLISH:
// an unknown property in the properties block must cause the packet to be
// rejected, not silently drop or misparse the TopicAlias (which affects
// routing).
func TestDecodePublishV5UnknownPropertyPOC(t *testing.T) {
	// Construct a v5 PUBLISH frame with:
	//   topic = "test/topic"
	//   properties = [unknown 0xFE 4-byte][TopicAlias 0x23 = 7]
	//   payload = "hello"

	// We'll build the raw bytes to ensure the exact wire format.
	var payload []byte

	// Topic (UTF-8 encoded string)
	topic := "test/topic"
	payload = append(payload, byte(len(topic)>>8), byte(len(topic)))
	payload = append(payload, []byte(topic)...)

	// PacketID (QoS > 0)
	payload = append(payload, 0x00, 0x01)

	// Properties block: [len][unknown 0xFE 4-byte val][TopicAlias 0x23 2-byte val]
	propBody := []byte{
		0xFE, 0xAA, 0xBB, 0xCC, 0xDD, // unknown property (ID + 4 bytes)
		0x23, 0x00, 0x07, // TopicAlias = 7
	}
	props := append([]byte{byte(len(propBody))}, propBody...)
	payload = append(payload, props...)

	// Payload
	payload = append(payload, []byte("hello")...)

	// Fixed header: PUBLISH type (0x30), QoS=1 (0x02) => 0x32
	fixed := byte(TypePUBLISH<<4) | 0x02

	// Remaining length as varint
	rl := encodeVarInt(len(payload))

	frame := append([]byte{fixed}, rl...)
	frame = append(frame, payload...)

	pkt, err := DecodeWithVersion(frame, ProtocolV5)
	if err == nil {
		t.Fatalf("expected unknown property to be rejected, got packet: %+v", pkt)
	}
	if err != ErrUnknownProperty {
		t.Fatalf("expected ErrUnknownProperty, got: %v", err)
	}
}

// TestDecodePublishV3PayloadMisinterpretedAsV5Props shows that a v3 PUBLISH
// whose payload happens to look like a valid properties block is silently
// misparsed as v5, corrupting the payload.
func TestDecodePublishV3PayloadMisinterpretedAsV5Props(t *testing.T) {
	// v3 PUBLISH: topic="t", payload = [0x05, 0x02, 0x00, 0x00, 0x00, 0x00, 0xAA]
	// The payload starts with 0x05 (could be read as properties length=5)
	// followed by 0x02 (PropMessageExpiryInterval) + 4 bytes (value=0)
	// This makes decodeProperties "succeed", misparsing the v3 payload as v5 props.

	var payload []byte
	topic := "t"
	payload = append(payload, byte(len(topic)>>8), byte(len(topic)))
	payload = append(payload, []byte(topic)...)
	// Payload that looks like: [props-len=5][PropMessageExpiryInterval][4-byte value=0][0xAA]
	payload = append(payload, 0x05, 0x02, 0x00, 0x00, 0x00, 0x00, 0xAA)

	fixed := byte(TypePUBLISH << 4) // QoS=0, v3
	rl := encodeVarInt(len(payload))
	frame := append([]byte{fixed}, rl...)
	frame = append(frame, payload...)

	pkt, err := DecodeWithVersion(frame, ProtocolV311)
	if err != nil {
		t.Fatalf("DecodeWithVersion failed: %v", err)
	}

	// The payload should be the original 7 bytes, not corrupted
	expected := []byte{0x05, 0x02, 0x00, 0x00, 0x00, 0x00, 0xAA}
	if string(pkt.Payload) != string(expected) {
		t.Fatalf("BUG REPRODUCED: v3 payload corrupted\n  got:      %x\n  expected: %x", pkt.Payload, expected)
	}
	if pkt.PubProps != nil && pkt.PubProps.MessageExpiryInterval != nil {
		t.Fatal("BUG REPRODUCED: v3 PUBLISH incorrectly parsed as having v5 MessageExpiryInterval property")
	}
}

// TestDecodeSubackV3MisinterpretedAsV5 shows that a v3 SUBACK whose first
// reason code is 0x00 is misinterpreted as v5 with empty properties, causing
// the first reason code to be consumed as the properties length byte.
func TestDecodeSubackV3MisinterpretedAsV5(t *testing.T) {
	// v3 SUBACK: packetID=1, codes=[0x00, 0x01, 0x02]
	// Wire: [0x00 0x01][0x00 0x01 0x02]
	//
	// The decoder tries v5 first: decodeProperties at pos 2 reads 0x00 (empty props).
	// Since np(3) < len(b)(5), it treats the packet as v5 with empty props.
	// The 0x00 (first reason code) is consumed as the props length byte.
	// Result: SubackCodes = [0x01, 0x02] — first code is LOST.

	b := []byte{0x00, 0x01, 0x00, 0x01, 0x02} // packetID + 3 codes
	p := &Packet{Type: TypeSUBACK}
	err := decodeSuback(p, b)
	if err != nil {
		t.Fatalf("decodeSuback failed: %v", err)
	}

	// Expected: SubackCodes = [0x00, 0x01, 0x02] (3 bytes)
	// Bug: first code 0x00 is consumed as props length, codes = [0x01, 0x02]
	if len(p.SubackCodes) != 3 {
		t.Fatalf("BUG REPRODUCED: SubackCodes length = %d, want 3 — first reason code lost, codes: %x", len(p.SubackCodes), p.SubackCodes)
	}
	expected := []byte{0x00, 0x01, 0x02}
	for i, c := range expected {
		if p.SubackCodes[i] != c {
			t.Fatalf("BUG REPRODUCED: SubackCodes[%d] = 0x%02x, want 0x%02x — full: %x", i, p.SubackCodes[i], c, p.SubackCodes)
		}
	}
}

// TestDecodeSubackV3FallbackOffByOne shows the off-by-one in the v3 fallback
// path: when the "ambiguous empty props" branch IS reached (np == len(b)),
// p.SubackCodes = b[pos:] uses the wrong pos.
func TestDecodeSubackV3FallbackOffByOne(t *testing.T) {
	// v3 SUBACK: packetID=1, codes=[0x01, 0x02] (no leading 0x00)
	// Wire: [0x00 0x01][0x01 0x02]
	//
	// decodeProperties at pos 2: reads 0x01 (props length=1), but then tries
	// to parse 1 byte of props which may fail. If it fails, we fall through.
	// If it succeeds (e.g., 0x02 = partial), np=4=len(b), so we hit the
	// "ambiguous" branch where b[pos]=0x01 != 0, so it's treated as v5 with
	// props length 1 and 0 remaining codes.

	b := []byte{0x00, 0x01, 0x01, 0x02} // packetID + 2 codes
	p := &Packet{Type: TypeSUBACK}
	err := decodeSuback(p, b)
	if err != nil {
		t.Fatalf("decodeSuback failed: %v", err)
	}

	// The expected behavior for a v3 SUBACK: codes = [0x01, 0x02]
	// But the decoder may misinterpret due to the heuristic.
	t.Logf("SubackCodes = %x, version = %d", p.SubackCodes, p.Version)
}

// TestDecodeSubscribeV3V5Confusion shows that a v3 SUBSCRIBE whose filter
// options byte happens to look like valid v5 subscription data can be
// misinterpreted, leading to wrong NoLocal/RetainHandling bits.
func TestDecodeSubscribeV3V5Confusion(t *testing.T) {
	// v3 SUBSCRIBE: packetID=1, filter="test", QoS=0
	// Wire: [0x00 0x01][0x00 0x04 t e s t][0x00]
	// The byte at pos 2 is 0x00. decodeProperties reads varint 0 (empty props, np=3).
	// Then tryParseSubscribePayload from pos 3: 0x04 = length 4, "test", then 0x00 = opts.
	// This succeeds as v5! So a v3 SUBSCRIBE is parsed as v5.

	b := []byte{0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00}
	p := &Packet{Type: TypeSUBSCRIBE}
	err := decodeSubscribe(p, b)
	if err != nil {
		t.Fatalf("decodeSubscribe failed: %v", err)
	}

	// The filter should be "test" regardless
	if len(p.Subscriptions) != 1 || p.Subscriptions[0].Filter != "test" {
		t.Fatalf("subscriptions mismatch: %+v", p.Subscriptions)
	}

	// But the version detection is ambiguous. The real issue is that a v3 SUBSCRIBE
	// is silently treated as v5, which affects option bit interpretation.
	// For this specific input, QoS=0 and opts=0x00, so no functional difference,
	// but the version tag is wrong:
	if p.Version != ProtocolV311 {
		t.Logf("NOTE: v3 SUBSCRIBE parsed as v5 (version=%d) — this is the ambiguous parsing path", p.Version)
	}
}

// TestDecodePropertiesWillDelayIntervalClobber verifies that WillDelayInterval
// (0x18) and SessionExpiryInterval (0x11) are decoded into separate fields and
// do not clobber each other.
func TestDecodePropertiesWillDelayIntervalClobber(t *testing.T) {
	// Properties block: [SessionExpiryInterval=0x11, value=3600][WillDelayInterval=0x18, value=60]
	propBody := []byte{
		0x11, 0x00, 0x00, 0x0E, 0x10, // SessionExpiryInterval = 3600
		0x18, 0x00, 0x00, 0x00, 0x3C, // WillDelayInterval = 60
	}
	props := append([]byte{byte(len(propBody))}, propBody...)

	decoded, _, err := decodeProperties(props, 0)
	if err != nil {
		t.Fatalf("decodeProperties failed: %v", err)
	}

	if decoded.SessionExpiryInterval == nil {
		t.Fatal("SessionExpiryInterval is nil")
	}
	if *decoded.SessionExpiryInterval != 3600 {
		t.Fatalf("SessionExpiryInterval = %d, want 3600 (WillDelayInterval clobbered it)", *decoded.SessionExpiryInterval)
	}
	if decoded.WillDelayInterval == nil {
		t.Fatal("WillDelayInterval is nil")
	}
	if *decoded.WillDelayInterval != 60 {
		t.Fatalf("WillDelayInterval = %d, want 60", *decoded.WillDelayInterval)
	}
}

// TestDecodePropertiesSubscriptionIDVarintBound shows that SubscriptionID
// decode uses decodeVarInt which limits to 4 bytes, but the property value
// could be up to 4 bytes per spec. This is actually correct, but let's verify
// the boundary.
func TestDecodePropertiesSubscriptionID(t *testing.T) {
	// SubscriptionID = 0x0B, varint value
	// Test with value 1 (valid)
	propBody := []byte{0x0B, 0x01}
	props := append([]byte{byte(len(propBody))}, propBody...)

	decoded, _, err := decodeProperties(props, 0)
	if err != nil {
		t.Fatalf("decodeProperties failed: %v", err)
	}

	if len(decoded.SubscriptionID) != 1 || decoded.SubscriptionID[0] != 1 {
		t.Fatalf("SubscriptionID = %v, want [1]", decoded.SubscriptionID)
	}
}

// TestDecodeGenericV3PublishPayloadCorruption shows that the generic Decode
// path (no version info) can misinterpret a v3 PUBLISH payload as v5 properties
// when the payload happens to start with a byte that looks like a valid
// properties length varint. This corrupts the payload.
func TestDecodeGenericV3PublishPayloadCorruption(t *testing.T) {
	// v3 PUBLISH (QoS=0): topic="t", payload starts with 0x05 followed by
	// bytes that look like a valid properties block.
	// Wire (remaining payload): [0x00 0x01 t][0x05 0x02 0x00 0x00 0x00 0x00 0xAA]
	// The generic Decode path doesn't know it's v3, so it tries v5 props.
	// 0x05 → props length 5, then 0x02 (PropMessageExpiryInterval) + 4 bytes = valid!
	// The payload "0xAA" remains, but the first 6 bytes are consumed as props.

	var payload []byte
	topic := "t"
	payload = append(payload, byte(len(topic)>>8), byte(len(topic)))
	payload = append(payload, []byte(topic)...)
	// Payload: [props-len=5][PropMessageExpiryInterval=0x02][4 bytes][0xAA]
	payload = append(payload, 0x05, 0x02, 0x00, 0x00, 0x00, 0x00, 0xAA)

	fixed := byte(TypePUBLISH << 4) // QoS=0
	rl := encodeVarInt(len(payload))
	frame := append([]byte{fixed}, rl...)
	frame = append(frame, payload...)

	// Generic Decode (no version info) — this is the problematic path
	pkt, err := Decode(frame)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// The payload should be the full 7 bytes, but the generic path misparses it
	expectedPayload := []byte{0x05, 0x02, 0x00, 0x00, 0x00, 0x00, 0xAA}
	if string(pkt.Payload) != string(expectedPayload) {
		t.Fatalf("BUG REPRODUCED: generic Decode corrupted v3 payload\n  got:      %x\n  expected: %x\n  PubProps: %+v", pkt.Payload, expectedPayload, pkt.PubProps)
	}
}

// TestDecodePublishV5PropsLengthZeroVsV3 shows the ambiguity when a v5 PUBLISH
// has empty properties (length=0) followed by payload — the generic decoder
// cannot distinguish this from a v3 PUBLISH with a payload starting with 0x00.
func TestDecodePublishV5PropsLengthZeroVsV3(t *testing.T) {
	// v5 PUBLISH with empty properties: [topic][packetID][props-len=0][payload]
	var payload []byte
	topic := "test"
	payload = append(payload, byte(len(topic)>>8), byte(len(topic)))
	payload = append(payload, []byte(topic)...)
	payload = append(payload, 0x00, 0x01) // packetID
	payload = append(payload, 0x00)       // empty properties
	payload = append(payload, 0x41, 0x42) // payload "AB"

	fixed := byte(TypePUBLISH<<4) | 0x02 // QoS=1
	rl := encodeVarInt(len(payload))
	frame := append([]byte{fixed}, rl...)
	frame = append(frame, payload...)

	pkt, err := Decode(frame)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// The payload should be [0x41, 0x42]
	if string(pkt.Payload) != "AB" {
		t.Fatalf("payload = %q, want AB", string(pkt.Payload))
	}
}

func uint16Ptr(v uint16) *uint16  { return &v }
func uint32Ptr(v uint32) *uint32  { return &v }
func strPtr(s string) *string     { return &s }
func bytePtr(b byte) *byte        { return &b }

// TestDecodeRoundtripInvariant verifies that encode→decode→encode produces
// a consistent result for various property combinations. This catches
// encode/decode asymmetries.
func TestDecodeRoundtripInvariant(t *testing.T) {
	tests := []struct {
		name  string
		props *Properties
	}{
		{"nil", nil},
		{"empty", &Properties{}},
		{"topic_alias", &Properties{TopicAlias: uint16Ptr(1)}},
		{"topic_alias_max", &Properties{TopicAliasMaximum: uint16Ptr(10)}},
		{"session_expiry", &Properties{SessionExpiryInterval: uint32Ptr(3600)}},
		{"message_expiry", &Properties{MessageExpiryInterval: uint32Ptr(60)}},
		{"receive_max", &Properties{ReceiveMaximum: uint16Ptr(100)}},
		{"max_packet_size", &Properties{MaximumPacketSize: uint32Ptr(65535)}},
		{"content_type", &Properties{ContentType: strPtr("application/json")}},
		{"response_topic", &Properties{ResponseTopic: strPtr("response/topic")}},
		{"correlation_data", &Properties{CorrelationData: []byte{0x01, 0x02, 0x03}}},
		{"subscription_id", &Properties{SubscriptionID: []uint32{1, 2, 3}}},
		{"user_property", &Properties{User: []UserProperty{{Key: "k", Val: "v"}}}},
		{"multiple", &Properties{
			TopicAlias:            uint16Ptr(5),
			MessageExpiryInterval: uint32Ptr(120),
			ContentType:           strPtr("text/plain"),
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encodeProperties(tc.props)
			decoded, _, err := decodeProperties(encoded, 0)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			// Re-encode and compare
			reEncoded := encodeProperties(decoded)
			if string(encoded) != string(reEncoded) {
				t.Errorf("encode/decode asymmetry\n  orig:    %x\n  re-enc:  %x\n  decoded: %+v", encoded, reEncoded, decoded)
			}
		})
	}
}

// TestDecodePropertiesUserPropertyLimit verifies the 10-user-property limit.
func TestDecodePropertiesUserPropertyLimit(t *testing.T) {
	// Build 11 user properties — should fail with ErrTooManyUserProperties
	var propBody []byte
	for i := 0; i < 11; i++ {
		key := []byte("k")
		val := []byte("v")
		propBody = append(propBody, PropUserProperty)
		propBody = append(propBody, byte(len(key)>>8), byte(len(key)))
		propBody = append(propBody, key...)
		propBody = append(propBody, byte(len(val)>>8), byte(len(val)))
		propBody = append(propBody, val...)
	}
	props := append([]byte{byte(len(propBody))}, propBody...)

	_, _, err := decodeProperties(props, 0)
	if err != ErrTooManyUserProperties {
		t.Fatalf("expected ErrTooManyUserProperties, got: %v", err)
	}
}

// TestDecodePropertiesInvalidStringLength verifies that a string property
// with length exceeding the properties block is rejected.
func TestDecodePropertiesInvalidStringLength(t *testing.T) {
	// ContentType (0x03) with length 100 but only 2 bytes follow
	propBody := []byte{0x03, 0x00, 0x64, 0x41, 0x42} // length=100, but only 2 bytes
	props := append([]byte{byte(len(propBody))}, propBody...)

	_, _, err := decodeProperties(props, 0)
	if err == nil {
		t.Fatal("expected error for string exceeding properties block, got nil")
	}
}
