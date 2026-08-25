package broker

import (
	"log/slog"
	"mqtt/internal/codec"
	"mqtt/internal/session"
	"mqtt/internal/topic"
	"mqtt/internal/transport"
)

func (b *Broker) handleSubscribe(conn *transport.Conn, sess *session.Session, pkt *codec.Packet) {
	if !b.allowSubscribe(sess.ClientID) {
		mqttPacketDropped.WithLabelValues("subscribe_rate").Inc()
		codes := make([]byte, len(pkt.Subscriptions))
		for i := range codes {
			if sess.Version == codec.ProtocolV5 {
				codes[i] = 0x97
			} else {
				codes[i] = 0x80
			}
		}
		ack := &codec.Packet{Type: codec.TypeSUBACK, Version: conn.Version(), PacketID: pkt.PacketID, SubackCodes: codes}
		_ = b.sendPacket(conn, ack)
		b.debugPacket("send", sess.ClientID, ack)
		return
	}
	var codes []byte
	for _, sub := range pkt.Subscriptions {
		if !topic.IsValidFilter(sub.Filter) {
			codes = append(codes, 0x80) // failure
			continue
		}
		if err := b.hooks.ExecSubscribe(sess.ClientID, sub.Filter, sub.QoS); err != nil {
			mqttPacketDropped.WithLabelValues("hook").Inc()
			if sess.Version == codec.ProtocolV5 {
				codes = append(codes, 0x87)
			} else {
				codes = append(codes, 0x80)
			}
			continue
		}
		if isShared, group, realFilter := isSharedFilter(sub.Filter); isShared {
			if !topic.IsValidFilter(realFilter) {
				codes = append(codes, 0x80)
				continue
			}
			b.sharedMu.Lock()
			if b.sharedSubs[group] == nil {
				b.sharedSubs[group] = make(map[string][]string)
			}
			// avoid duplicate
			found := false
			for _, cid := range b.sharedSubs[group][realFilter] {
				if cid == sess.ClientID {
					found = true
					break
				}
			}
			if !found {
				b.sharedSubs[group][realFilter] = append(b.sharedSubs[group][realFilter], sess.ClientID)
			}
			b.sharedMu.Unlock()
			sess.Subscriptions[sub.Filter] = sub.QoS
			if err := b.store.SaveSession(bgCtx(), sess); err != nil {
				slog.Warn("store SaveSession failed", "err", err)
			}
			codes = append(codes, sub.QoS)
			if b.cluster != nil {
				_ = b.cluster.PublishMeta(bgCtx(), "sub", sub.Filter)
			}
		} else {
			b.trie.Add(sub.Filter, sess.ClientID, sub.QoS, sub.NoLocal)
			sess.Subscriptions[sub.Filter] = sub.QoS
			if err := b.store.SaveSession(bgCtx(), sess); err != nil {
				slog.Warn("store SaveSession failed", "err", err)
			}
			codes = append(codes, sub.QoS)
			if b.cluster != nil {
				_ = b.cluster.PublishMeta(bgCtx(), "sub", sub.Filter)
			}
		}

		// deliver retained messages matching this filter
		retained, err := b.store.ListRetained(bgCtx())
		if err != nil {
			slog.Warn("list retained failed", "err", err)
		}
		for _, m := range retained {
			if matchFilter(m.Topic, sub.Filter) {
				if !b.auth.Authorize(sess.ClientID, m.Topic, false) {
					continue
				}
				pub := &codec.Packet{
					Type:    codec.TypePUBLISH,
					Version: conn.Version(),
					Topic:   m.Topic,
					QoS:     m.QoS,
					Payload: m.Payload,
					Retain:  true,
				}
				if sub.QoS > 0 && m.QoS > 0 {
					// retain deliver QoS = min(sub QoS, retained QoS)
					if m.QoS < sub.QoS {
						pub.QoS = m.QoS
					} else {
						pub.QoS = sub.QoS
					}
					if pub.QoS > 0 {
						pub.PacketID = sess.NextPacketID()
					}
				} else {
					pub.QoS = 0
				}
				_ = b.sendPacket(conn, pub)
			}
		}
	}
	ack := &codec.Packet{Type: codec.TypeSUBACK, Version: conn.Version(), PacketID: pkt.PacketID, SubackCodes: codes}
	if sess.Version == codec.ProtocolV5 {
		ack.SubackProps = &codec.Properties{}
	}
	_ = b.sendPacket(conn, ack)
}

func (b *Broker) handleUnsubscribe(conn *transport.Conn, sess *session.Session, pkt *codec.Packet) {
	for _, t := range pkt.Topics {
		if err := b.hooks.ExecUnsubscribe(sess.ClientID, t); err != nil {
			mqttPacketDropped.WithLabelValues("hook").Inc()
			continue
		}
		if isShared, group, realFilter := isSharedFilter(t); isShared {
			b.sharedMu.Lock()
			if m, ok := b.sharedSubs[group]; ok {
				list := m[realFilter]
				newList := list[:0]
				for _, cid := range list {
					if cid != sess.ClientID {
						newList = append(newList, cid)
					}
				}
				if len(newList) == 0 {
					delete(m, realFilter)
					if len(m) == 0 {
						delete(b.sharedSubs, group)
					} else {
						m[realFilter] = newList
					}
				} else {
					m[realFilter] = newList
				}
			}
			b.sharedMu.Unlock()
		} else {
			b.trie.Remove(t, sess.ClientID)
		}
		delete(sess.Subscriptions, t)
		if b.cluster != nil {
			_ = b.cluster.PublishMeta(bgCtx(), "unsub", t)
		}
	}
	if err := b.store.SaveSession(bgCtx(), sess); err != nil {
		slog.Warn("store SaveSession failed", "err", err)
	}
	ack := &codec.Packet{Type: codec.TypeUNSUBACK, Version: conn.Version(), PacketID: pkt.PacketID}
	if sess.Version == codec.ProtocolV5 {
		ack.UnsubackProps = &codec.Properties{}
		ack.UnsubackCodes = make([]byte, len(pkt.Topics)) // 0 = success
	}
	_ = b.sendPacket(conn, ack)
}

func isSharedFilter(filter string) (bool, string, string) {
	if len(filter) > 7 && filter[:7] == "$share/" {
		rest := filter[7:]
		slash := -1
		for i, c := range rest {
			if c == '/' {
				slash = i
				break
			}
		}
		if slash < 0 {
			return false, "", ""
		}
		group := rest[:slash]
		realFilter := rest[slash+1:]
		if group == "" || realFilter == "" {
			return false, "", ""
		}
		return true, group, realFilter
	}
	return false, "", ""
}

func matchFilter(t, filter string) bool {
	return topic.MatchFilter(t, filter)
}
