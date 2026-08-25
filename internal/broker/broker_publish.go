package broker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"log/slog"
	"mqtt/internal/codec"
	"mqtt/internal/persistence"
	"mqtt/internal/session"
	"mqtt/internal/topic"
	"mqtt/internal/transport"
	"strings"
)

func packetHex(p *codec.Packet) string {
	if p == nil {
		return ""
	}
	data, err := codec.Encode(p)
	if err != nil || len(data) == 0 {
		return ""
	}
	hexStr := fmt.Sprintf("%x", data)
	if len(hexStr) > 512 {
		hexStr = hexStr[:512] + "..."
	}
	return hexStr
}

func (b *Broker) debugPacket(dir, clientID string, pkt *codec.Packet) {
	hex := packetHex(pkt)
	b.hooks.ExecPacket(dir, clientID, pkt, hex)
	if b.hooks.Len() == 0 && slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("packet "+dir, "client", clientID, "type", pkt.Type, "version", pkt.Version, "hex", hex)
	}
}

func (b *Broker) sendPacket(conn *transport.Conn, pkt *codec.Packet) error {
	err := conn.WritePacket(pkt)
	b.debugPacket("send", conn.ClientID(), pkt)
	return err
}

func (b *Broker) allowPublish(clientID string) bool {
	b.limitMu.Lock()
	lim, ok := b.limiters[clientID]
	if !ok {
		lim = &clientLimiter{window: time.Now()}
		b.limiters[clientID] = lim
	}
	b.limitMu.Unlock()
	lim.mu.Lock()
	defer lim.mu.Unlock()
	now := time.Now()
	if now.Sub(lim.window) >= time.Second {
		lim.window = now
		lim.publishCount = 0
	}
	lim.publishCount++
	return lim.publishCount <= b.cfg.MaxPublishPerSec
}

func (b *Broker) allowSubscribe(clientID string) bool {
	b.limitMu.Lock()
	lim, ok := b.limiters[clientID]
	if !ok {
		lim = &clientLimiter{window: time.Now()}
		b.limiters[clientID] = lim
	}
	b.limitMu.Unlock()
	lim.mu.Lock()
	defer lim.mu.Unlock()
	now := time.Now()
	if now.Sub(lim.window) >= time.Second {
		lim.window = now
		lim.subscribeCount = 0
	}
	lim.subscribeCount++
	return lim.subscribeCount <= b.cfg.MaxSubscribePerSec
}

func (b *Broker) handlePublish(conn *transport.Conn, sess *session.Session, pkt *codec.Packet) {
	if strings.HasPrefix(pkt.Topic, "$SYS/") {
		if pkt.QoS == 1 {
			ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x87}
			_ = b.sendPacket(conn, ack)
		}
		return
	}
	if !b.allowPublish(sess.ClientID) {
		mqttPacketDropped.WithLabelValues("publish_rate").Inc()
		if pkt.QoS == 1 {
			ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x97}
			_ = b.sendPacket(conn, ack)
			b.debugPacket("send", sess.ClientID, ack)
		}
		return
	}
	if err := b.hooks.ExecPublish(sess.ClientID, pkt.Topic, pkt.Payload, pkt.QoS, pkt.Retain); err != nil {
		mqttPacketDropped.WithLabelValues("hook").Inc()
		if pkt.QoS == 1 {
			ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x87}
			_ = b.sendPacket(conn, ack)
			b.debugPacket("send", sess.ClientID, ack)
		}
		return
	}
	// topic alias handling (v5) with limit
	topicName := pkt.Topic
	if sess.Version == codec.ProtocolV5 && pkt.PubProps != nil && pkt.PubProps.TopicAlias != nil {
		alias := *pkt.PubProps.TopicAlias
		if alias == 0 || alias > sess.TopicAliasMaximum {
			if pkt.QoS == 1 {
				ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x94}
				_ = b.sendPacket(conn, ack)
			}
			_ = conn.Close()
			return
		}
		if len(sess.AliasToTopic) >= int(sess.TopicAliasMaximum) && sess.AliasToTopic[alias] == "" {
			if pkt.QoS == 1 {
				ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x94}
				_ = b.sendPacket(conn, ack)
			}
			return
		}
		if topicName != "" {
			sess.AliasToTopic[alias] = topicName
			sess.TopicToAlias[topicName] = alias
		} else {
			if t, ok := sess.AliasToTopic[alias]; ok {
				topicName = t
				pkt.Topic = t
			} else {
				// invalid alias
				if pkt.QoS == 1 {
					ack := &codec.Packet{Type: codec.TypePUBACK, Version: codec.ProtocolV5, PacketID: pkt.PacketID, Reason: 0x94}
					_ = b.sendPacket(conn, ack)
				}
				return
			}
		}
	}
	if topicName == "" {
		return
	}
	if len(topicName) > 4096 || len(pkt.Payload) > 1<<20 {
		return
	}
	if sess.MaximumPacketSize > 0 && len(topicName)+len(pkt.Payload)+10 > int(sess.MaximumPacketSize) {
		_ = conn.Close()
		return
	}
	if !topic.IsValidTopic(topicName) {
		return
	}
	// QoS2 inbound: need to send PUBREC and store, route after PUBREL
	if pkt.QoS == 2 {
		if _, exists := sess.GetInflight(pkt.PacketID); exists {
			rec := &codec.Packet{Type: codec.TypePUBREC, Version: conn.Version(), PacketID: pkt.PacketID}
			_ = b.sendPacket(conn, rec)
			return
		}
		sess.AddInflight(&session.InflightEntry{PacketID: pkt.PacketID, QoS: 2, Topic: topicName, Payload: pkt.Payload, State: "qos2-publish"})
		rec := &codec.Packet{Type: codec.TypePUBREC, Version: conn.Version(), PacketID: pkt.PacketID}
		_ = b.sendPacket(conn, rec)
		return
	}

	// Retain handling
	if pkt.Retain {
		if len(pkt.Payload) == 0 {
			if err := b.store.DeleteRetained(bgCtx(), topicName); err != nil {
				slog.Warn("store DeleteRetained failed", "err", err)
			}
		} else {
			if err := b.store.SaveRetained(bgCtx(), topicName, &persistence.Message{Topic: topicName, Payload: pkt.Payload, QoS: pkt.QoS, Retain: true}); err != nil {
				slog.Warn("store SaveRetained failed", "err", err)
			}
		}
	}

	// ACK for QoS1
	if pkt.QoS == 1 {
		ack := &codec.Packet{Type: codec.TypePUBACK, Version: conn.Version(), PacketID: pkt.PacketID}
		if sess.Version == codec.ProtocolV5 {
			ack.Reason = 0
		}
		_ = b.sendPacket(conn, ack)
	}

	// Route locally + cluster
	b.routeMessage(topicName, pkt.Payload, pkt.QoS, pkt.Retain, pkt.PubProps, sess.ClientID)

	// For QoS2 inbound, actual routing should happen after PUBREL; we already did. To be spec compliant, we would defer until PUBREL. Simplified as above.
}

func (b *Broker) routeMessage(topicName string, payload []byte, qos byte, retain bool, props *codec.Properties, from string) {
	atomic.AddInt64(&b.stats.MessagesReceived, 1)
	mqttMessagesReceived.Inc()
	if b.cfg.MaxPacketSize > 0 && len(payload)+len(topicName) > b.cfg.MaxPacketSize {
		return
	}
	if b.cluster != nil && b.hasRemoteSubscribers(topicName) {
		go func() {
			if err := b.cluster.Publish(bgCtx(), topicName, payload, qos, retain); err != nil {
				slog.Warn("cluster publish failed", "err", err)
			}
		}()
	}
	b.deliverLocal(topicName, payload, qos, props, from)
}

func (b *Broker) deliverLocal(topicName string, payload []byte, qos byte, props *codec.Properties, from string) {
	type shChoice struct {
		group  string
		filter string
		client string
	}
	var choices []shChoice
	b.sharedMu.Lock()
	for group, filters := range b.sharedSubs {
		for filter, clients := range filters {
			if len(clients) == 0 {
				continue
			}
			if !topic.MatchFilter(topicName, filter) {
				continue
			}
			idx := b.sharedIdx[group] % len(clients)
			b.sharedIdx[group] = (idx + 1) % len(clients)
			chosen := clients[idx]
			if chosen == from {
				continue
			}
			choices = append(choices, shChoice{group: group, filter: filter, client: chosen})
		}
	}
	b.sharedMu.Unlock()
	for _, ch := range choices {
		b.mu.RLock()
		conn, ok1 := b.conns[ch.client]
		sess, ok2 := b.sessions[ch.client]
		b.mu.RUnlock()
		if !ok1 || !ok2 {
			if sess != nil && sess.ExpiryInterval != 0 {
				if err := b.store.EnqueueOffline(bgCtx(), ch.client, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos}); err != nil {
					slog.Warn("store EnqueueOffline failed", "err", err)
				}
			}
			continue
		}
		q := qos
		if storedQoS, ok := sess.Subscriptions["$share/"+ch.group+"/"+ch.filter]; ok && storedQoS < q {
			q = storedQoS
		}
		pub := &codec.Packet{Type: codec.TypePUBLISH, Version: conn.Version(), Topic: topicName, QoS: q, Payload: payload}
		if q > 0 {
			if !sess.CanSend() {
				if err := b.store.EnqueueOffline(bgCtx(), ch.client, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos}); err != nil {
					slog.Warn("store EnqueueOffline failed", "err", err)
				}
				continue
			}
			pub.PacketID = sess.NextPacketID()
			if pub.PacketID == 0 {
				continue
			}
			sess.AddInflight(&session.InflightEntry{PacketID: pub.PacketID, QoS: q, Topic: topicName, Payload: payload})
			b.scheduleRetry(ch.client, pub.PacketID)
		}
		if sess.Version == codec.ProtocolV5 && props != nil && len(props.SubscriptionID) > 0 {
			pub.PubProps = &codec.Properties{SubscriptionID: props.SubscriptionID}
		}
		mqttMessagesSent.Inc()
		mqttInflight.Set(float64(sess.InflightCount()))
		_ = b.sendPacket(conn, pub)
	}
	subs := b.trie.Match(topicName)
	for _, sub := range subs {
		if sub.ClientID == from && sub.NoLocal {
			continue
		}
		b.mu.RLock()
		conn, ok := b.conns[sub.ClientID]
		sess, sok := b.sessions[sub.ClientID]
		b.mu.RUnlock()
		if !ok || !sok {
			// offline: enqueue if session expiry >0
			if sess != nil && sess.ExpiryInterval != 0 {
				if err := b.store.EnqueueOffline(bgCtx(), sub.ClientID, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos}); err != nil {
					slog.Warn("store EnqueueOffline failed", "err", err)
				}
			}
			continue
		}
		// deliver with min QoS (publish QoS and sub QoS)
		deliverQoS := qos
		if sub.QoS < deliverQoS {
			deliverQoS = sub.QoS
		}
		pub := &codec.Packet{
			Type:    codec.TypePUBLISH,
			Version: conn.Version(),
			Topic:   topicName,
			QoS:     deliverQoS,
			Payload: payload,
			Retain:  false,
		}
		if deliverQoS > 0 {
			if !sess.CanSend() {
				if err := b.store.EnqueueOffline(bgCtx(), sub.ClientID, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos}); err != nil {
					slog.Warn("store EnqueueOffline failed", "err", err)
				}
				continue
			}
			pub.PacketID = sess.NextPacketID()
			if pub.PacketID == 0 {
				continue
			}
			e := &session.InflightEntry{PacketID: pub.PacketID, QoS: deliverQoS, Topic: topicName, Payload: payload}
			sess.AddInflight(e)
			b.scheduleRetry(sess.ClientID, pub.PacketID)
		}
		// v5 subscription ID
		if sess.Version == codec.ProtocolV5 && props != nil && len(props.SubscriptionID) > 0 {
			pub.PubProps = &codec.Properties{SubscriptionID: props.SubscriptionID}
		}
		mqttMessagesSent.Inc()
		mqttInflight.Set(float64(len(sess.Inflight) + 1))
		if err := b.sendPacket(conn, pub); err != nil {
			slog.Warn("deliver failed", "client", sub.ClientID, "err", err)
		}
	}
}

func (b *Broker) scheduleRetry(clientID string, packetID uint16) {
	time.AfterFunc(20*time.Second, func() {
		b.mu.RLock()
		sess, ok1 := b.sessions[clientID]
		conn, ok2 := b.conns[clientID]
		b.mu.RUnlock()
		if !ok1 || !ok2 {
			return
		}
		if e, ok := sess.GetInflight(packetID); ok {
			e.Dup = true
			pub := &codec.Packet{Type: codec.TypePUBLISH, Version: conn.Version(), Topic: e.Topic, QoS: e.QoS, Payload: e.Payload, PacketID: packetID, Dup: true}
			_ = b.sendPacket(conn, pub)
			b.scheduleRetry(clientID, packetID)
		}
	})
}
