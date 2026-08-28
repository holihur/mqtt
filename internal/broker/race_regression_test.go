package broker

import (
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
)

// Regression for concurrent map read/write on Session.Subscriptions:
// the subscriber's readLoop writes sess.Subscriptions (SUBSCRIBE/UNSUBSCRIBE)
// while publisher goroutines read it in deliverLocal (shared-sub QoS lookup).
// Run with -race: must be data-race free.
func TestRaceSharedSubPublishConcurrent(t *testing.T) {
	b := New(Config{NodeID: "race-shared", TCPAddr: "127.0.0.1:12211", AllowAnonymous: true}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	readPacket := func(conn net.Conn) *codec.Packet {
		buf := make([]byte, 4096)
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, _ := conn.Read(buf)
		if n == 0 {
			return nil
		}
		p, _ := codec.Decode(buf[:n])
		return p
	}

	sub, _ := net.Dial("tcp", "127.0.0.1:12211")
	c := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "race-sub"}
	d, _ := codec.Encode(c)
	sub.Write(d)
	readPacket(sub)

	pub, _ := net.Dial("tcp", "127.0.0.1:12211")
	c2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "race-pub"}
	d, _ = codec.Encode(c2)
	pub.Write(d)
	readPacket(pub)

	done := make(chan struct{})
	// subscriber churns shared subscriptions (writes sess.Subscriptions)
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			s := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: uint16(i%60000 + 2),
				Subscriptions: []codec.Subscription{{Filter: "$share/g/race/room", QoS: 1}}}
			d, _ := codec.Encode(s)
			sub.Write(d)
			readPacket(sub)
			u := &codec.Packet{Type: codec.TypeUNSUBSCRIBE, Version: codec.ProtocolV311, PacketID: uint16(i%60000 + 2), Topics: []string{"$share/g/race/room"}}
			d, _ = codec.Encode(u)
			sub.Write(d)
			readPacket(sub)
		}
	}()
	// publisher floods QoS1 messages (publisher goroutine reads sess.Subscriptions)
	for i := 0; i < 500; i++ {
		p := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, PacketID: uint16(i%60000 + 1), Topic: "race/room", QoS: 1, Payload: []byte("x")}
		d, _ := codec.Encode(p)
		pub.Write(d)
		// drain PUBACK to keep publisher flowing
		readPacket(pub)
	}
	<-done
	sub.Close()
	pub.Close()
}
