package broker

import (
	"net"
	"sync/atomic"
	"time"

	"log/slog"
	"mqtt/internal/codec"
	"mqtt/internal/session"
	"mqtt/internal/transport"
	"strings"

	"github.com/google/uuid"
)

func (b *Broker) handleRawConn(raw net.Conn) {
	conn := transport.NewConn(raw, b.cfg.MaxPacketSize)
	// first packet must be CONNECT with timeout 10s
	_ = raw.SetReadDeadline(time.Now().Add(10 * time.Second))
	pkt, err := conn.ReadPacket()
	if err != nil {
		slog.Debug("read CONNECT failed", "addr", raw.RemoteAddr().String(), "err", err)
		_ = raw.Close()
		return
	}
	b.debugPacket("recv", pkt.ClientID, pkt)
	slog.Info("client connect attempt", "client", pkt.ClientID, "addr", raw.RemoteAddr().String(), "version", pkt.Version, "keepAlive", pkt.KeepAlive, "clean", pkt.ConnectFlags.CleanSession)
	if pkt.Type != codec.TypeCONNECT {
		slog.Warn("first packet not CONNECT", "addr", raw.RemoteAddr().String())
		_ = raw.Close()
		return
	}
	if err := b.hooks.ExecAuth(pkt.ClientID, pkt.Username, pkt.Password); err != nil {
		slog.Info("auth denied", "client", pkt.ClientID, "addr", raw.RemoteAddr().String(), "username", pkt.Username, "err", err)
		mqttAuthFailed.Inc()
		mqttPacketDropped.WithLabelValues("auth").Inc()
		reason := byte(0x04)
		if pkt.Version == codec.ProtocolV5 {
			reason = 0x86
		}
		resp := &codec.Packet{Type: codec.TypeCONNACK, Version: pkt.Version, ReasonCode: reason}
		_ = b.sendPacket(conn, resp)
		b.debugPacket("send", pkt.ClientID, resp)
		_ = conn.Close()
		return
	}
	clientID := pkt.ClientID
	if clientID == "" {
		if !pkt.ConnectFlags.CleanSession && pkt.Version != codec.ProtocolV5 {
			reason := byte(0x02)
			resp := &codec.Packet{Type: codec.TypeCONNACK, Version: pkt.Version, ReasonCode: reason}
			_ = b.sendPacket(conn, resp)
			_ = conn.Close()
			return
		}
		clientID = "auto-" + uuid.NewString()[:8]
	}
	if len(clientID) > 64 {
		mqttPacketDropped.WithLabelValues("clientid_too_long").Inc()
		reason := byte(0x02)
		if pkt.Version == codec.ProtocolV5 {
			reason = 0x85
		}
		resp := &codec.Packet{Type: codec.TypeCONNACK, Version: pkt.Version, ReasonCode: reason}
		_ = b.sendPacket(conn, resp)
		_ = conn.Close()
		return
	}
	if pkt.Version != codec.ProtocolV5 && len(clientID) > 23 {
		slog.Warn("clientID exceeds 23 chars for v3", "client", clientID, "len", len(clientID))
	}
	conn.SetClientID(clientID)
	conn.SetVersion(pkt.Version)
	b.mu.RLock()
	if len(b.conns) >= b.cfg.MaxConnections {
		b.mu.RUnlock()
		mqttPacketDropped.WithLabelValues("max_connections").Inc()
		reason := byte(0x03)
		if pkt.Version == codec.ProtocolV5 {
			reason = 0x97
		}
		resp := &codec.Packet{Type: codec.TypeCONNACK, Version: pkt.Version, ReasonCode: reason}
		_ = b.sendPacket(conn, resp)
		b.debugPacket("send", clientID, resp)
		slog.Info("reject max connections", "client", clientID, "current", len(b.conns), "max", b.cfg.MaxConnections)
		_ = conn.Close()
		return
	}
	b.mu.RUnlock()

	// Session handling
	sess, sessionExisted, err := b.getOrCreateSession(pkt)
	if err != nil {
		slog.Error("session error", "err", err)
		_ = conn.Close()
		return
	}
	if sessionExisted {
		sess.Mu.Lock()
		oldUsername := sess.Username
		sess.Mu.Unlock()
		if oldUsername != "" && pkt.Username != oldUsername {
			b.mu.RLock()
			_, hasOldConn := b.conns[clientID]
			b.mu.RUnlock()
			if hasOldConn {
				mqttPacketDropped.WithLabelValues("session_hijack").Inc()
				slog.Warn("session hijack attempt", "client", clientID, "oldUser", oldUsername, "newUser", pkt.Username, "addr", raw.RemoteAddr().String())
				reason := byte(0x04)
				if pkt.Version == codec.ProtocolV5 {
					reason = 0x86
				}
				resp := &codec.Packet{Type: codec.TypeCONNACK, Version: pkt.Version, ReasonCode: reason}
				_ = b.sendPacket(conn, resp)
				_ = conn.Close()
				return
			}
		}
	}
	sess.Mu.Lock()
	sess.ClientID = clientID
	sess.Version = pkt.Version
	sess.KeepAlive = pkt.KeepAlive
	sess.Connected = true
	sess.NodeID = b.nodeID
	sess.Username = pkt.Username
	if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil {
		if pkt.Properties.ReceiveMaximum != nil {
			sess.ReceiveMaximum = *pkt.Properties.ReceiveMaximum
		}
		if pkt.Properties.MaximumPacketSize != nil {
			sess.MaximumPacketSize = *pkt.Properties.MaximumPacketSize
		}
		if pkt.Properties.TopicAliasMaximum != nil {
			v := *pkt.Properties.TopicAliasMaximum
			if v > 100 {
				v = 100
			}
			sess.TopicAliasMaximum = v
		}
	}
	clean := pkt.ConnectFlags.CleanSession
	expiry := uint32(0)
	if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil && pkt.Properties.SessionExpiryInterval != nil {
		expiry = *pkt.Properties.SessionExpiryInterval
	} else if clean {
		expiry = 0
	} else {
		expiry = 0xFFFFFFFF
	}
	sess.CleanStart = clean
	sess.ExpiryInterval = expiry
	sess.Mu.Unlock()
	atomic.StoreInt64(&b.stats.ClientsConnected, int64(len(b.conns))+1)
	atomic.AddInt64(&b.stats.ClientsTotal, 1)

	if err := b.hooks.ExecConnect(clientID); err != nil {
		mqttPacketDropped.WithLabelValues("hook_connect").Inc()
		reason := byte(0x87)
		if pkt.Version != codec.ProtocolV5 {
			reason = 0x04
		}
		resp := &codec.Packet{Type: codec.TypeCONNACK, Version: pkt.Version, ReasonCode: reason}
		_ = b.sendPacket(conn, resp)
		_ = conn.Close()
		return
	}
	// Kick existing connection with same clientID
	b.mu.Lock()
	if old, ok := b.conns[clientID]; ok {
		_ = old.Close()
	}
	b.conns[clientID] = conn
	b.sessions[clientID] = sess
	b.mu.Unlock()
	if err := b.store.SaveSession(bgCtx(), sess); err != nil {
		slog.Warn("store SaveSession failed", "err", err)
	}

	// Will with validation and delay cap
	if pkt.Will != nil {
		if pkt.Will.Topic == "" || strings.HasPrefix(pkt.Will.Topic, "$SYS/") {
			// invalid will topic
		} else if len(pkt.Will.Payload) > b.cfg.MaxPacketSize || len(pkt.Will.Topic)+len(pkt.Will.Payload) > b.cfg.MaxPacketSize {
			slog.Warn("will payload too large", "client", clientID, "size", len(pkt.Will.Payload))
			mqttPacketDropped.WithLabelValues("will_too_large").Inc()
		} else {
			delay := pkt.Will.DelayInterval
			if delay > 86400 {
				delay = 86400
			}
			sess.Will = &session.Will{
				Topic:         pkt.Will.Topic,
				Payload:       pkt.Will.Payload,
				QoS:           pkt.Will.QoS,
				Retain:        pkt.Will.Retain,
				DelayInterval: delay,
			}
		}
	}

	// CONNACK - SessionPresent per MQTT spec: true if session existed and CleanSession/CleanStart is false
	sessionPresent := sessionExisted && !pkt.ConnectFlags.CleanSession
	if clientID != pkt.ClientID {
		sessionPresent = false
	}
	connack := &codec.Packet{
		Type:           codec.TypeCONNACK,
		Version:        pkt.Version,
		SessionPresent: sessionPresent,
		ReasonCode:     0,
	}
	// For v5, include props like AssignedClientID if we generated one
	if pkt.Version == codec.ProtocolV5 {
		props := &codec.Properties{}
		if pkt.ClientID == "" {
			props.AssignedClientID = &clientID
		}
		rm := uint16(65535)
		props.ReceiveMaximum = &rm
		ssa := byte(1)
		props.SharedSubAvailable = &ssa
		mps := uint32(b.cfg.MaxPacketSize)
		props.MaximumPacketSize = &mps
		ta := uint16(100)
		props.TopicAliasMaximum = &ta
		connack.ConnProperties = props
	}
	if err := b.sendPacket(conn, connack); err != nil {
		_ = conn.Close()
		return
	}
	b.debugPacket("send", clientID, connack)
	slog.Info("client connected", "client", clientID, "addr", raw.RemoteAddr().String(), "sessionPresent", sessionPresent, "version", pkt.Version, "clean", pkt.ConnectFlags.CleanSession)
	for filter, qos := range sess.Subscriptions {
		b.trie.Add(filter, clientID, qos, false)
	}

	// Replay retained for existing subs? Not needed until SUBSCRIBE

	// Replay offline queue (filter expired)
	offline, err := b.store.DequeueOffline(bgCtx(), clientID)
	if err != nil {
		slog.Warn("dequeue offline failed", "client", clientID, "err", err)
	} else if len(offline) > 0 {
		for _, m := range offline {
			if m.IsExpired() {
				mqttPacketDropped.WithLabelValues("message_expiry").Inc()
				continue
			}
			pub := &codec.Packet{
				Type:    codec.TypePUBLISH,
				Version: pkt.Version,
				Topic:   m.Topic,
				QoS:     m.QoS,
				Payload: m.Payload,
				Retain:  m.Retain,
			}
			if m.QoS > 0 {
				pub.PacketID = sess.NextPacketID()
				sess.AddInflight(&session.InflightEntry{PacketID: pub.PacketID, QoS: m.QoS, Topic: m.Topic, Payload: m.Payload})
			}
			_ = b.sendPacket(conn, pub)
		}
	}

	// Main loop
	_ = raw.SetReadDeadline(time.Time{}) // clear
	conn.SetOnClose(func() {
		b.onClientDisconnect(clientID, sess, false)
	})
	go b.readLoop(conn, sess)
}

func (b *Broker) getOrCreateSession(pkt *codec.Packet) (*session.Session, bool, error) {
	clientID := pkt.ClientID
	if clientID == "" {
		return session.NewSession(clientID, pkt.Version, pkt.ConnectFlags.CleanSession, 0), false, nil
	}
	b.mu.RLock()
	if s, ok := b.sessions[clientID]; ok {
		b.mu.RUnlock()
		existed := true
		if pkt.ConnectFlags.CleanSession {
			s.Mu.Lock()
			s.Subscriptions = make(map[string]byte)
			s.Inflight = make(map[uint16]*session.InflightEntry)
			s.Mu.Unlock()
			if err := b.store.ClearOffline(bgCtx(), clientID); err != nil {
				slog.Warn("store ClearOffline failed", "err", err)
			}
		}
		return s, existed, nil
	}
	b.mu.RUnlock()
	s, err := b.store.GetSession(bgCtx(), clientID)
	if err != nil {
		return nil, false, err
	}
	if s != nil {
		existed := true
		if pkt.ConnectFlags.CleanSession {
			s.Mu.Lock()
			s.Subscriptions = make(map[string]byte)
			s.Inflight = make(map[uint16]*session.InflightEntry)
			s.Mu.Unlock()
			if err := b.store.ClearOffline(bgCtx(), clientID); err != nil {
				slog.Warn("store ClearOffline failed", "err", err)
			}
		}
		return s, existed, nil
	}
	// new session
	expiry := uint32(0)
	if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil && pkt.Properties.SessionExpiryInterval != nil {
		expiry = *pkt.Properties.SessionExpiryInterval
	}
	s = session.NewSession(clientID, pkt.Version, pkt.ConnectFlags.CleanSession, expiry)
	return s, false, nil
}

func (b *Broker) readLoop(conn *transport.Conn, sess *session.Session) {
	defer func() { _ = conn.Close() }()
	for {
		if sess.KeepAlive > 0 {
			_ = conn.Raw().SetReadDeadline(time.Now().Add(time.Duration(float64(sess.KeepAlive)*1.5) * time.Second))
		} else {
			_ = conn.Raw().SetReadDeadline(time.Time{})
		}
		pkt, err := conn.ReadPacket()
		if err != nil {
			b.onClientDisconnect(conn.ClientID(), sess, false)
			return
		}
		b.debugPacket("recv", conn.ClientID(), pkt)
		switch pkt.Type {
		case codec.TypePUBLISH:
			slog.Debug("publish recv", "client", conn.ClientID(), "topic", pkt.Topic, "qos", pkt.QoS, "retain", pkt.Retain, "payloadLen", len(pkt.Payload))
			b.handlePublish(conn, sess, pkt)
		case codec.TypeSUBSCRIBE:
			slog.Debug("subscribe recv", "client", conn.ClientID(), "packetID", pkt.PacketID, "filters", pkt.Subscriptions)
			b.handleSubscribe(conn, sess, pkt)
		case codec.TypeUNSUBSCRIBE:
			slog.Debug("unsubscribe recv", "client", conn.ClientID(), "packetID", pkt.PacketID, "topics", pkt.Topics)
			b.handleUnsubscribe(conn, sess, pkt)
		case codec.TypePUBACK:
			sess.RemoveInflight(pkt.PacketID)
			_ = b.store.DeletePendingRetry(bgCtx(), sess.ClientID, pkt.PacketID)
		case codec.TypePUBREC:
			if _, ok := sess.GetInflight(pkt.PacketID); ok {
				rel := &codec.Packet{Type: codec.TypePUBREL, Version: conn.Version(), PacketID: pkt.PacketID}
				_ = b.sendPacket(conn, rel)
			}
		case codec.TypePUBREL:
			if e, ok := sess.GetInflight(pkt.PacketID); ok {
				b.routeMessage(e.Topic, e.Payload, 2, false, nil, sess.ClientID)
				sess.RemoveInflight(pkt.PacketID)
			} else {
				sess.RemoveInflight(pkt.PacketID)
			}
			_ = b.store.DeletePendingRetry(bgCtx(), sess.ClientID, pkt.PacketID)
			comp := &codec.Packet{Type: codec.TypePUBCOMP, Version: conn.Version(), PacketID: pkt.PacketID}
			_ = b.sendPacket(conn, comp)
		case codec.TypePUBCOMP:
			sess.RemoveInflight(pkt.PacketID)
			_ = b.store.DeletePendingRetry(bgCtx(), sess.ClientID, pkt.PacketID)
		case codec.TypePINGREQ:
			resp := &codec.Packet{Type: codec.TypePINGRESP, Version: conn.Version()}
			_ = b.sendPacket(conn, resp)
		case codec.TypeDISCONNECT:
			b.onClientDisconnect(conn.ClientID(), sess, true)
			return
		default:
			slog.Debug("unhandled packet", "type", pkt.Type, "client", conn.ClientID())
		}
	}
}

//nolint:unused
func (b *Broker) keepAliveMonitor(conn *transport.Conn, sess *session.Session) {
	interval := time.Duration(float64(sess.KeepAlive)*1.5) * time.Second
	if interval == 0 {
		return
	}
	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()
	for {
		// we rely on read deadline? Simpler: check last activity via conn read timeout
		// For now, just sleep and check if conn still exists
		time.Sleep(interval)
		b.mu.RLock()
		_, ok := b.conns[sess.ClientID]
		b.mu.RUnlock()
		if !ok {
			return
		}
		// If no packet received within 1.5*keepalive, close
		// We set read deadline dynamically: transport's parser blocks, so we set deadline on raw conn
		// This monitor just closes if still idle; actual deadline is set below
		// Set read deadline to trigger next ReadPacket timeout
		_ = conn.Raw().SetReadDeadline(time.Now().Add(2 * time.Second))
	}
}
