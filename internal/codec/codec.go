package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
)

var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func getBuf() *bytes.Buffer {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func putBuf(b *bytes.Buffer) {
	if b.Cap() < 64*1024 {
		bufPool.Put(b)
	}
}

var packetPool = sync.Pool{New: func() any { return &Packet{} }}

func AcquirePacket() *Packet {
	p := packetPool.Get().(*Packet)
	*p = Packet{}
	return p
}

func ReleasePacket(p *Packet) {
	if p == nil {
		return
	}
	// clear slices to allow GC
	p.Payload = nil
	p.Subscriptions = nil
	p.Topics = nil
	p.SubackCodes = nil
	p.UnsubackCodes = nil
	p.Password = nil
	p.Will = nil
	p.Properties = nil
	p.ConnProperties = nil
	p.PubProps = nil
	p.AckProps = nil
	p.SubProps = nil
	p.SubackProps = nil
	p.UnsubProps = nil
	p.UnsubackProps = nil
	p.DiscProps = nil
	p.AuthProps = nil
	packetPool.Put(p)
}

// Encode encodes packet to wire bytes (fixed header + remaining length + payload)
func Encode(p *Packet) ([]byte, error) {
	var vhAndPayload []byte
	var flags byte
	switch p.Type {
	case TypeCONNECT:
		flags = 0
		vhAndPayload = encodeConnect(p)
	case TypeCONNACK:
		flags = 0
		vhAndPayload = encodeConnack(p)
	case TypePUBLISH:
		dup := byte(0)
		if p.Dup {
			dup = 1
		}
		flags = dup<<3 | p.QoS<<1 | boolToByte(p.Retain)
		vhAndPayload = encodePublish(p)
	case TypePUBACK, TypePUBREC, TypePUBCOMP:
		flags = 0
		vhAndPayload = encodeAck(p)
	case TypePUBREL:
		flags = 0x02
		vhAndPayload = encodeAck(p)
	case TypeSUBSCRIBE:
		flags = 0x02
		vhAndPayload = encodeSubscribe(p)
	case TypeSUBACK:
		flags = 0
		vhAndPayload = encodeSuback(p)
	case TypeUNSUBSCRIBE:
		flags = 0x02
		vhAndPayload = encodeUnsubscribe(p)
	case TypeUNSUBACK:
		flags = 0
		vhAndPayload = encodeUnsuback(p)
	case TypePINGREQ, TypePINGRESP:
		flags = 0
		vhAndPayload = []byte{}
	case TypeDISCONNECT:
		flags = 0
		vhAndPayload = encodeDisconnect(p)
	case TypeAUTH:
		flags = 0
		vhAndPayload = encodeAuth(p)
	default:
		return nil, ErrMalformedPacket
	}
	fixed := byte(p.Type<<4) | (flags & 0x0F)
	out := make([]byte, 0, 1+varIntLen(len(vhAndPayload))+len(vhAndPayload))
	out = append(out, fixed)
	out = appendVarInt(out, len(vhAndPayload))
	out = append(out, vhAndPayload...)
	return out, nil
}

// Decode decodes one complete frame (including fixed header) to Packet.
// Protocol version is inferred heuristically for version-sensitive packet
// types; the broker's transport should use DecodeWithVersion once the client
// version is known (after CONNECT).
func Decode(frame []byte) (*Packet, error) {
	return decodeVersioned(frame, 0)
}

// DecodeWithVersion is the version-aware decode used by the broker transport
// after the client's protocol version is known. It eliminates the v3/v5
// ambiguity that the generic Decode path has to guess at.
func DecodeWithVersion(frame []byte, version byte) (*Packet, error) {
	return decodeVersioned(frame, version)
}

func decodeVersioned(frame []byte, version byte) (*Packet, error) {
	if len(frame) < 2 {
		return nil, ErrMalformedPacket
	}
	fixed := frame[0]
	ptype := fixed >> 4
	flags := fixed & 0x0F
	rl, n, err := decodeVarInt(frame[1:])
	if err != nil {
		return nil, err
	}
	if 1+n+rl != len(frame) {
		return nil, ErrMalformedPacket
	}
	payload := frame[1+n:]
	p := &Packet{Type: ptype, Fixed: fixed, Version: version}
	switch ptype {
	case TypeCONNECT:
		if err := decodeConnect(p, payload); err != nil {
			return nil, err
		}
	case TypeCONNACK:
		if err := decodeConnack(p, payload); err != nil {
			return nil, err
		}
	case TypePUBLISH:
		p.QoS = (flags >> 1) & 0x03
		p.Dup = (flags>>3)&0x01 == 1
		p.Retain = flags&0x01 == 1
		if p.QoS > 2 {
			return nil, ErrInvalidQoS
		}
		if err := decodePublish(p, payload); err != nil {
			return nil, err
		}
	case TypePUBACK, TypePUBREC, TypePUBREL, TypePUBCOMP:
		if err := decodeAck(p, payload); err != nil {
			return nil, err
		}
	case TypeSUBSCRIBE:
		if flags != 0x02 {
			return nil, ErrProtocolViolation
		}
		if err := decodeSubscribe(p, payload); err != nil {
			return nil, err
		}
	case TypeSUBACK:
		if err := decodeSuback(p, payload); err != nil {
			return nil, err
		}
	case TypeUNSUBSCRIBE:
		if flags != 0x02 {
			return nil, ErrProtocolViolation
		}
		if err := decodeUnsubscribe(p, payload); err != nil {
			return nil, err
		}
	case TypeUNSUBACK:
		if err := decodeUnsuback(p, payload); err != nil {
			return nil, err
		}
	case TypePINGREQ, TypePINGRESP:
		if len(payload) != 0 {
			return nil, ErrMalformedPacket
		}
	case TypeDISCONNECT:
		if err := decodeDisconnect(p, payload); err != nil {
			return nil, err
		}
	case TypeAUTH:
		if err := decodeAuth(p, payload); err != nil {
			return nil, err
		}
	default:
		return nil, ErrMalformedPacket
	}
	return p, nil
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// ---- CONNECT ----

func encodeConnect(p *Packet) []byte {
	buf := getBuf()
	defer putBuf(buf)
	proto := p.ProtocolName
	if proto == "" {
		if p.Version == ProtocolV31 {
			proto = "MQIsdp"
		} else {
			proto = "MQTT"
		}
	}
	buf.Write(encodeString(proto))
	buf.WriteByte(p.ProtocolLevel)
	// flags
	var flags byte
	if p.ConnectFlags.CleanSession {
		flags |= 0x02
	}
	if p.ConnectFlags.WillFlag {
		flags |= 0x04
		flags |= (p.ConnectFlags.WillQoS & 0x03) << 3
		if p.ConnectFlags.WillRetain {
			flags |= 0x20
		}
	}
	if p.ConnectFlags.PasswordFlag {
		flags |= 0x40
	}
	if p.ConnectFlags.UsernameFlag {
		flags |= 0x80
	}
	buf.WriteByte(flags)
	_ = binary.Write(buf, binary.BigEndian, p.KeepAlive)
	// v5 properties
	if p.Version == ProtocolV5 {
		buf.Write(encodeProperties(p.Properties))
	}
	buf.Write(encodeString(p.ClientID))
	// will
	if p.ConnectFlags.WillFlag && p.Will != nil {
		if p.Version == ProtocolV5 {
			buf.Write(encodeWillProperties(&p.Will.DelayInterval, p.Will.Properties))
			buf.Write(encodeString(p.Will.Topic))
			buf.Write(encodeBinary(p.Will.Payload))
		} else {
			buf.Write(encodeString(p.Will.Topic))
			buf.Write(encodeBinary(p.Will.Payload))
		}
	}
	if p.ConnectFlags.UsernameFlag {
		buf.Write(encodeString(p.Username))
	}
	if p.ConnectFlags.PasswordFlag {
		buf.Write(encodeBinary(p.Password))
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}

func decodeConnect(p *Packet, b []byte) error {
	if len(b) < 10 {
		return ErrMalformedPacket
	}
	proto, pos, err := decodeString(b, 0)
	if err != nil {
		return err
	}
	if proto != "MQTT" && proto != "MQIsdp" {
		return ErrMalformedPacket
	}
	p.ProtocolName = proto
	if pos >= len(b) {
		return ErrMalformedPacket
	}
	level := b[pos]
	pos++
	p.ProtocolLevel = level
	switch level {
	case 3:
		p.Version = ProtocolV31
	case 4:
		p.Version = ProtocolV311
	case 5:
		p.Version = ProtocolV5
	default:
		return ErrUnsupportedProtocol
	}
	if pos >= len(b) {
		return ErrMalformedPacket
	}
	flags := b[pos]
	pos++
	p.ConnectFlags.CleanSession = flags&0x02 != 0
	p.ConnectFlags.WillFlag = flags&0x04 != 0
	p.ConnectFlags.WillQoS = (flags >> 3) & 0x03
	p.ConnectFlags.WillRetain = flags&0x20 != 0
	p.ConnectFlags.PasswordFlag = flags&0x40 != 0
	p.ConnectFlags.UsernameFlag = flags&0x80 != 0
	if pos+2 > len(b) {
		return ErrMalformedPacket
	}
	p.KeepAlive = decodeUint16(b[pos:])
	pos += 2
	// v5 properties
	if p.Version == ProtocolV5 {
		props, np, err := decodeProperties(b, pos)
		if err != nil {
			return err
		}
		p.Properties = props
		pos = np
	}
	// ClientID
	cid, np, err := decodeString(b, pos)
	if err != nil {
		return err
	}
	p.ClientID = cid
	pos = np
	// Will
	if p.ConnectFlags.WillFlag {
		w := &Will{QoS: p.ConnectFlags.WillQoS, Retain: p.ConnectFlags.WillRetain}
		if p.Version == ProtocolV5 {
			wprops, np2, err := decodeProperties(b, pos)
			if err != nil {
				return err
			}
			// Extract WillDelayInterval (0x18) into the dedicated field;
			// SessionExpiryInterval (0x11) is not a legal will property.
			if wprops.WillDelayInterval != nil {
				w.DelayInterval = *wprops.WillDelayInterval
				wprops.WillDelayInterval = nil
			}
			w.Properties = wprops
			pos = np2
		}
		topic, np2, err := decodeString(b, pos)
		if err != nil {
			return err
		}
		w.Topic = topic
		pos = np2
		pay, np3, err := decodeBinary(b, pos)
		if err != nil {
			return err
		}
		w.Payload = pay
		pos = np3
		p.Will = w
	}
	if p.ConnectFlags.UsernameFlag {
		u, np, err := decodeString(b, pos)
		if err != nil {
			return err
		}
		p.Username = u
		pos = np
	}
	if p.ConnectFlags.PasswordFlag {
		pw, _, err := decodeBinary(b, pos)
		if err != nil {
			return err
		}
		p.Password = pw
	}
	return nil
}

// ---- CONNACK ----

func encodeConnack(p *Packet) []byte {
	buf := getBuf()
	defer putBuf(buf)
	buf.WriteByte(boolToByte(p.SessionPresent))
	buf.WriteByte(p.ReasonCode)
	if p.Version == ProtocolV5 {
		if p.ConnProperties == nil {
			buf.Write(encodeVarInt(0))
		} else {
			buf.Write(encodeProperties(p.ConnProperties))
		}
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}
func decodeConnack(p *Packet, b []byte) error {
	if len(b) < 2 {
		return ErrMalformedPacket
	}
	p.SessionPresent = b[0]&0x01 == 1
	p.ReasonCode = b[1]
	// Heuristic: if remaining bytes >2, treat as v5 properties. Also check if byte 1 is 0..5 and length 2 => v3
	if len(b) > 2 {
		p.Version = ProtocolV5
		props, _, err := decodeProperties(b, 2)
		if err != nil {
			return err
		}
		p.ConnProperties = props
	} else {
		// v3: ReasonCode 0-5, map to version 4
		if p.ReasonCode <= 5 {
			p.Version = ProtocolV311
		} else {
			p.Version = ProtocolV5
		}
	}
	return nil
}

// ---- PUBLISH ----

func encodePublish(p *Packet) []byte {
	buf := getBuf()
	defer putBuf(buf)
	buf.Write(encodeString(p.Topic))
	if p.QoS > 0 {
		_ = binary.Write(buf, binary.BigEndian, p.PacketID)
	}
	if p.Version == ProtocolV5 {
		buf.Write(encodeProperties(p.PubProps))
	}
	buf.Write(p.Payload)
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}
func decodePublish(p *Packet, b []byte) error {
	topic, pos, err := decodeString(b, 0)
	if err != nil {
		return err
	}
	p.Topic = topic
	if p.QoS > 0 {
		if pos+2 > len(b) {
			return ErrMalformedPacket
		}
		p.PacketID = decodeUint16(b[pos:])
		pos += 2
	}
	// v5 properties detection depends on the known protocol version:
	//   - v5: properties are mandatory (may be zero-length) and precede the payload
	//   - v3/v3.1.1: no properties; the remainder is entirely payload
	//   - generic (version unknown): only attempt properties when there is a QoS
	//     level > 0. A v3 QoS0 PUBLISH payload frequently starts with a byte that
	//     decodes as a plausible properties length (e.g. 0x05), so guessing there
	//     risks silently corrupting the payload; the version-aware path is the
	//     correct way to decode a QoS0 v5 PUBLISH. For QoS>0 we attempt a v5
	//     parse but fall back to treating the whole remainder as payload when it
	//     is not a well-formed properties block (v3 shape).
	if pos < len(b) {
		saved := pos
		if p.Version == ProtocolV5 || (p.Version == 0 && p.QoS > 0) {
			props, np, err := decodeProperties(b, pos)
			if err != nil {
				if err == ErrMalformedPacket && p.Version == 0 {
					// Not a valid v5 properties block: v3 PUBLISH, the rest is payload.
					p.Payload = b[saved:]
					return nil
				}
				return err
			}
			p.PubProps = props
			pos = np
		}
		p.Payload = b[pos:]
	} else {
		p.Payload = []byte{}
	}
	return nil
}

// ---- ACK (PUBACK etc) ----

func encodeAck(p *Packet) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, p.PacketID)
	if p.Version == ProtocolV5 {
		// v5 PUBACK may be 2,3,4 bytes depending on reason/properties
		// If reason is 0 and no props, we can send 2 bytes (just packetID) per spec, but we encode full if needed
		if p.Reason != 0 || p.AckProps != nil {
			buf.WriteByte(p.Reason)
			if p.AckProps == nil {
				buf.Write(encodeVarInt(0))
			} else {
				buf.Write(encodeProperties(p.AckProps))
			}
		} else if p.AckProps != nil {
			buf.Write(encodeProperties(p.AckProps))
		}
	}
	return buf.Bytes()
}
func decodeAck(p *Packet, b []byte) error {
	if len(b) < 2 {
		return ErrMalformedPacket
	}
	p.PacketID = decodeUint16(b)
	if len(b) == 2 {
		p.Reason = 0
		return nil
	}
	if len(b) >= 3 {
		p.Reason = b[2]
		if len(b) > 3 {
			props, _, err := decodeProperties(b, 3)
			if err != nil {
				return err
			}
			p.AckProps = props
			p.Version = ProtocolV5
		} else {
			p.Version = ProtocolV5
		}
	}
	return nil
}

// ---- SUBSCRIBE ----

func encodeSubscribe(p *Packet) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, p.PacketID)
	if p.Version == ProtocolV5 {
		buf.Write(encodeProperties(p.SubProps))
	}
	for _, s := range p.Subscriptions {
		buf.Write(encodeString(s.Filter))
		opts := s.QoS & 0x03
		if p.Version == ProtocolV5 {
			if s.NoLocal {
				opts |= 1 << 2
			}
			if s.RetainAsPublished {
				opts |= 1 << 3
			}
			opts |= (s.RetainHandling & 0x03) << 4
		}
		buf.WriteByte(opts)
	}
	return buf.Bytes()
}
func decodeSubscribe(p *Packet, b []byte) error {
	if len(b) < 2 {
		return ErrMalformedPacket
	}
	p.PacketID = decodeUint16(b)
	if p.Version == ProtocolV5 {
		props, np, err := decodeProperties(b, 2)
		if err != nil {
			return err
		}
		subs, ok := tryParseSubscribePayload(b, np, true)
		if !ok {
			return ErrMalformedPacket
		}
		p.SubProps = props
		p.Subscriptions = subs
		return nil
	}
	// v3 (or generic): no properties. Generic keeps the existing behavior of
	// attempting a v5 parse first and falling back to v3.
	start := 2
	if p.Version == 0 {
		if props, np, err := decodeProperties(b, 2); err == nil {
			if subs, ok := tryParseSubscribePayload(b, np, true); ok {
				p.SubProps = props
				p.Subscriptions = subs
				return nil
			}
		}
	}
	subs, ok := tryParseSubscribePayload(b, start, false)
	if !ok {
		return ErrMalformedPacket
	}
	p.Subscriptions = subs
	p.Version = ProtocolV311
	return nil
}

func tryParseSubscribePayload(b []byte, pos int, isV5 bool) ([]Subscription, bool) {
	var subs []Subscription
	for pos < len(b) {
		filter, np, err := decodeString(b, pos)
		if err != nil {
			return nil, false
		}
		pos = np
		if pos >= len(b) {
			return nil, false
		}
		opts := b[pos]
		pos++
		sub := Subscription{Filter: filter, QoS: opts & 0x03}
		if isV5 {
			sub.NoLocal = opts&0x04 != 0
			sub.RetainAsPublished = opts&0x08 != 0
			sub.RetainHandling = (opts >> 4) & 0x03
		}
		subs = append(subs, sub)
	}
	if pos != len(b) {
		return nil, false
	}
	if len(subs) == 0 {
		return nil, false
	}
	return subs, true
}

// ---- SUBACK ----

func encodeSuback(p *Packet) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, p.PacketID)
	if p.Version == ProtocolV5 {
		buf.Write(encodeProperties(p.SubackProps))
	}
	buf.Write(p.SubackCodes)
	return buf.Bytes()
}
func decodeSuback(p *Packet, b []byte) error {
	if len(b) < 2 {
		return ErrMalformedPacket
	}
	p.PacketID = decodeUint16(b)
	pos := 2
	// v5 SUBACK layout: packetID, properties length varint, properties, then
	// one reason code per requested subscription.
	//
	// v3 SUBACK layout: packetID, then one reason code per subscription. A v3
	// packet's FIRST reason code is often 0x00 (granted at QoS0), which reads
	// as a zero-length v5 properties varint. The version-aware path resolves
	// this exactly: v3 never has properties. The generic path only treats the
	// leading byte as a properties length when it is clearly non-empty (>0) or
	// when the resulting parse leaves nothing behind (a plausible v5 SUBACK
	// with empty properties and a single 0x00 code).
	if p.Version == ProtocolV5 {
		props, np, err := decodeProperties(b, pos)
		if err != nil {
			return err
		}
		p.SubackProps = props
		p.SubackCodes = b[np:]
		return nil
	}
	if p.Version == 0 && len(b) > pos {
		props, np, err := decodeProperties(b, pos)
		if err == nil && np <= len(b) {
			// Non-empty properties block: unambiguously v5.
			if props != nil && (len(props.User) > 0 || props.ReasonString != nil || np > pos+1) {
				p.SubackProps = props
				p.SubackCodes = b[np:]
				p.Version = ProtocolV5
				return nil
			}
			// props length == 0 (np == pos+1): ambiguous between
			//   v3 SUBACK whose first reason code is 0x00, and
			//   v5 SUBACK with empty properties.
			// v3 reason codes are only 0x00/0x01/0x02/0x80. If every remaining
			// byte is a valid v3 code, treat it as v3 and do NOT consume the
			// leading 0x00 — it is the first reason code, and dropping it would
			// silently change the client's view of which subscriptions succeeded.
			remaining := b[np:]
			isV3 := true
			for _, c := range remaining {
				if c != 0x00 && c != 0x01 && c != 0x02 && c != 0x80 {
					isV3 = false
					break
				}
			}
			if isV3 {
				if len(remaining) == 0 {
					// Single 0x00 reason code: v3.
					p.SubackCodes = b[pos:]
					p.Version = ProtocolV311
					return nil
				}
				// All-valid-v3 codes after a would-be empty props block: the
				// leading 0x00 is a code, not a properties length.
				p.SubackCodes = b[pos:]
				p.Version = ProtocolV311
				return nil
			}
			// Remaining bytes include v5-only reason codes (0x87/0x97/...): v5.
			p.SubackProps = props
			p.SubackCodes = b[np:]
			p.Version = ProtocolV5
			return nil
		}
	}
	// v3: all remaining bytes are reason codes.
	p.SubackCodes = b[pos:]
	p.Version = ProtocolV311
	return nil
}

// ---- UNSUBSCRIBE ----

func encodeUnsubscribe(p *Packet) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, p.PacketID)
	if p.Version == ProtocolV5 {
		buf.Write(encodeProperties(p.UnsubProps))
	}
	for _, t := range p.Topics {
		buf.Write(encodeString(t))
	}
	return buf.Bytes()
}
func decodeUnsubscribe(p *Packet, b []byte) error {
	if len(b) < 2 {
		return ErrMalformedPacket
	}
	p.PacketID = decodeUint16(b)
	if p.Version == ProtocolV5 {
		props, np, err := decodeProperties(b, 2)
		if err != nil {
			return err
		}
		topics, ok := tryParseUnsubPayload(b, np)
		if !ok {
			return ErrMalformedPacket
		}
		p.UnsubProps = props
		p.Topics = topics
		return nil
	}
	// v3 (or generic): no properties. Generic keeps the existing behavior of
	// attempting a v5 parse first and falling back to v3.
	start := 2
	if p.Version == 0 {
		if props, np, err := decodeProperties(b, 2); err == nil {
			if topics, ok := tryParseUnsubPayload(b, np); ok {
				p.UnsubProps = props
				p.Topics = topics
				return nil
			}
		}
	}
	topics, ok := tryParseUnsubPayload(b, start)
	if !ok {
		return ErrMalformedPacket
	}
	p.Topics = topics
	p.Version = ProtocolV311
	return nil
}

func tryParseUnsubPayload(b []byte, pos int) ([]string, bool) {
	var out []string
	for pos < len(b) {
		t, np, err := decodeString(b, pos)
		if err != nil {
			return nil, false
		}
		pos = np
		out = append(out, t)
	}
	if pos != len(b) || len(out) == 0 {
		return nil, false
	}
	return out, true
}

func encodeUnsuback(p *Packet) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, p.PacketID)
	if p.Version == ProtocolV5 {
		buf.Write(encodeProperties(p.UnsubackProps))
		for _, c := range p.UnsubackCodes {
			buf.WriteByte(c)
		}
	} else {
		// v3: no codes? Actually v3 UNSUBACK has no reason codes, just packetID. But we still allow empty.
		if len(p.UnsubackCodes) > 0 {
			buf.Write(p.UnsubackCodes)
		}
	}
	return buf.Bytes()
}
func decodeUnsuback(p *Packet, b []byte) error {
	if len(b) < 2 {
		return ErrMalformedPacket
	}
	p.PacketID = decodeUint16(b)
	if len(b) > 2 {
		if p.Version == ProtocolV5 {
			props, np, err := decodeProperties(b, 2)
			if err != nil {
				return err
			}
			p.UnsubackProps = props
			p.UnsubackCodes = b[np:]
			return nil
		}
		props, np, err := decodeProperties(b, 2)
		if err == nil && np <= len(b) {
			p.UnsubackProps = props
			p.Version = ProtocolV5
			p.UnsubackCodes = b[np:]
		} else {
			p.UnsubackCodes = b[2:]
		}
	}
	return nil
}

// ---- DISCONNECT ----

func encodeDisconnect(p *Packet) []byte {
	if p.Version != ProtocolV5 {
		return []byte{}
	}
	var buf bytes.Buffer
	buf.WriteByte(p.DiscReason)
	buf.Write(encodeProperties(p.DiscProps))
	return buf.Bytes()
}
func decodeDisconnect(p *Packet, b []byte) error {
	if len(b) == 0 {
		p.DiscReason = 0
		return nil
	}
	if len(b) >= 1 {
		p.DiscReason = b[0]
		p.Version = ProtocolV5
	}
	if len(b) > 1 {
		props, _, err := decodeProperties(b, 1)
		if err != nil {
			return err
		}
		p.DiscProps = props
	}
	return nil
}

// ---- AUTH ----

func encodeAuth(p *Packet) []byte {
	var buf bytes.Buffer
	buf.WriteByte(p.AuthReason)
	buf.Write(encodeProperties(p.AuthProps))
	return buf.Bytes()
}
func decodeAuth(p *Packet, b []byte) error {
	if len(b) == 0 {
		return ErrMalformedPacket
	}
	p.AuthReason = b[0]
	p.Version = ProtocolV5
	if len(b) > 1 {
		props, _, err := decodeProperties(b, 1)
		if err != nil {
			return err
		}
		p.AuthProps = props
	}
	return nil
}

// Ensure errors import not unused
var _ = errors.New
