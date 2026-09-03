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
	// hex 编码成本高（= 再次完整 Encode + 格式化），只在确有消费方时计算：
	// 存在需要 hex 的 hook，或 slog debug 开启。
	debugOn := slog.Default().Enabled(context.Background(), slog.LevelDebug)
	if !debugOn && !b.hooks.PacketHexNeeded() {
		return
	}
	hex := packetHex(pkt)
	b.hooks.ExecPacket(dir, clientID, pkt, hex)
	if b.hooks.Len() == 0 && debugOn {
		slog.Debug("packet "+dir, "client", clientID, "type", pkt.Type, "version", pkt.Version, "hex", hex)
	}
}

func (b *Broker) sendPacket(conn *transport.Conn, pkt *codec.Packet) error {
	err := conn.WritePacket(pkt)
	b.debugPacket("send", conn.ClientID(), pkt)
	return err
}

func (b *Broker) allowPublish(clientID string) bool {
	now := time.Now()
	b.limitMu.Lock()
	lim, ok := b.limiters[clientID]
	if !ok {
		lim = &clientLimiter{window: now, lastSeen: now}
		b.limiters[clientID] = lim
	}
	b.limitMu.Unlock()
	lim.mu.Lock()
	defer lim.mu.Unlock()
	if now.Sub(lim.window) >= time.Second {
		lim.window = now
		lim.publishCount = 0
	}
	lim.publishCount++
	lim.lastSeen = now
	return lim.publishCount <= b.cfg.MaxPublishPerSec
}

func (b *Broker) allowSubscribe(clientID string) bool {
	now := time.Now()
	b.limitMu.Lock()
	lim, ok := b.limiters[clientID]
	if !ok {
		lim = &clientLimiter{window: now, lastSeen: now}
		b.limiters[clientID] = lim
	}
	b.limitMu.Unlock()
	lim.mu.Lock()
	defer lim.mu.Unlock()
	if now.Sub(lim.window) >= time.Second {
		lim.window = now
		lim.subscribeCount = 0
	}
	lim.subscribeCount++
	lim.lastSeen = now
	return lim.subscribeCount <= b.cfg.MaxSubscribePerSec
}

func (b *Broker) checkRetainQuota(topic string, payload []byte) (bool, string) {
	stats, err := b.store.GetRetainedStats(bgCtx())
	if err != nil {
		// fail closed: if we cannot read the quota we cannot enforce it,
		// and allowing the write lets disk fill up while the backend errors
		slog.Warn("GetRetainedStats failed, denying retain write", "err", err)
		return true, "stats_unavailable"
	}
	newSize := int64(len(topic) + len(payload) + 10)
	existingSize := int64(0)
	exists := false
	if ts, ok := stats.TopicStats[topic]; ok {
		exists = true
		existingSize = ts.Size
	}
	totalCountAfter := stats.TotalMessages
	if !exists {
		totalCountAfter++
	}
	totalSizeAfter := stats.TotalSize - existingSize + newSize
	if totalCountAfter > b.cfg.MaxRetainedMessages {
		return true, "global_count"
	}
	if totalSizeAfter > b.cfg.MaxRetainedSize {
		return true, "global_size"
	}
	if 1 > b.cfg.MaxRetainPerTopic {
		return true, "per_topic_count"
	}
	if newSize > b.cfg.MaxRetainSizePerTopic {
		return true, "per_topic_size"
	}
	_ = exists
	return false, ""
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

	var msgExpiry uint32
	var msgCreatedAt int64
	if pkt.PubProps != nil && pkt.PubProps.MessageExpiryInterval != nil {
		msgExpiry = *pkt.PubProps.MessageExpiryInterval
		msgCreatedAt = time.Now().UnixMilli()
		if msgExpiry == 0 {
			mqttPacketDropped.WithLabelValues("message_expiry").Inc()
		}
	}
	// Retain handling with quota and expiry
	if pkt.Retain {
		if len(pkt.Payload) == 0 {
			if err := b.store.DeleteRetained(bgCtx(), topicName); err != nil {
				slog.Warn("store DeleteRetained failed", "err", err)
			}
		} else if msgExpiry == 0 && pkt.PubProps != nil && pkt.PubProps.MessageExpiryInterval != nil {
			// expired for retain, skip store
		} else {
			if exceeded, reason := b.checkRetainQuota(topicName, pkt.Payload); exceeded {
				slog.Warn("retain quota exceeded", "reason", reason, "topic", topicName, "client", sess.ClientID, "payloadSize", len(pkt.Payload))
				mqttRetainQuotaExceeded.WithLabelValues(reason).Inc()
				mqttPacketDropped.WithLabelValues("retain_quota").Inc()
				if pkt.QoS == 1 {
					ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x97}
					_ = b.sendPacket(conn, ack)
					b.debugPacket("send", sess.ClientID, ack)
				}
				return
			}
			msg := &persistence.Message{Topic: topicName, Payload: pkt.Payload, QoS: pkt.QoS, Retain: true, CreatedAt: msgCreatedAt, ExpiryInterval: msgExpiry}
			if err := b.store.SaveRetained(bgCtx(), topicName, msg); err != nil {
				slog.Warn("store SaveRetained failed", "err", err)
			} else if msgExpiry > 0 {
				topicCopy := topicName
				time.AfterFunc(time.Duration(msgExpiry)*time.Second, func() {
					_ = b.store.DeleteRetained(context.Background(), topicCopy)
				})
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
	b.deliverLocal(topicName, payload, qos, retain, props, from)
}

// forwardPublishProps 构造要转发给 v5 订阅者的发布属性集 (MQTT5 §3.3.2.3)：
// PayloadFormat/ContentType/ResponseTopic/CorrelationData/UserProperty 原样转发；
// MessageExpiryInterval 透传 (服务端可在转发时缩减，此处不缩减)；
// TopicAlias/SubscriptionID 不转发 (SubscriptionID 由服务端逐订阅附加)。
func forwardPublishProps(in *codec.Properties) *codec.Properties {
	if in == nil {
		return nil
	}
	out := &codec.Properties{}
	any := false
	if in.PayloadFormatIndicator != nil {
		out.PayloadFormatIndicator = in.PayloadFormatIndicator
		any = true
	}
	if in.MessageExpiryInterval != nil {
		out.MessageExpiryInterval = in.MessageExpiryInterval
		any = true
	}
	if in.ContentType != nil {
		out.ContentType = in.ContentType
		any = true
	}
	if in.ResponseTopic != nil {
		out.ResponseTopic = in.ResponseTopic
		any = true
	}
	if len(in.CorrelationData) > 0 {
		out.CorrelationData = append([]byte(nil), in.CorrelationData...)
		any = true
	}
	if len(in.User) > 0 {
		out.User = append([]codec.UserProperty(nil), in.User...)
		any = true
	}
	if !any {
		return nil
	}
	return out
}

// pubPropsForSub 返回单个订阅者要携带的属性：fwd (无逐订阅字段时直接共享,
// 只读) 或 fwd+SubscriptionID 的拷贝；v3 返回 nil。
func pubPropsForSub(fwd *codec.Properties, version byte, subIDs []uint32) *codec.Properties {
	if version != codec.ProtocolV5 {
		return nil
	}
	if fwd == nil {
		if len(subIDs) == 0 {
			return nil
		}
		return &codec.Properties{SubscriptionID: append([]uint32(nil), subIDs...)}
	}
	if len(subIDs) == 0 {
		return fwd
	}
	cp := *fwd
	cp.SubscriptionID = append([]uint32(nil), subIDs...)
	return &cp
}

func (b *Broker) deliverLocal(topicName string, payload []byte, qos byte, retain bool, props *codec.Properties, from string) {
	if props != nil && props.MessageExpiryInterval != nil && *props.MessageExpiryInterval == 0 {
		mqttPacketDropped.WithLabelValues("message_expiry").Inc()
	}
	fwd := forwardPublishProps(props)
	var subIDs []uint32
	if props != nil {
		subIDs = props.SubscriptionID
	}
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
				if props != nil && props.MessageExpiryInterval != nil && *props.MessageExpiryInterval == 0 {
				} else {
					var expiry uint32
					var created int64
					if props != nil && props.MessageExpiryInterval != nil {
						expiry = *props.MessageExpiryInterval
						created = time.Now().UnixMilli()
					}
					if err := b.store.EnqueueOffline(bgCtx(), ch.client, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos, CreatedAt: created, ExpiryInterval: expiry}); err != nil {
						slog.Warn("store EnqueueOffline failed", "err", err)
					}
				}
			}
			continue
		}
		q := qos
		if storedQoS, ok := sess.GetSubscription("$share/" + ch.group + "/" + ch.filter); ok && storedQoS < q {
			q = storedQoS
		}
		pub := &codec.Packet{Type: codec.TypePUBLISH, Version: conn.Version(), Topic: topicName, QoS: q, Payload: payload}
		if conn.Version() == codec.ProtocolV5 && retain {
			if _, rap := sess.SubOptsFor("$share/" + ch.group + "/" + ch.filter); rap {
				pub.Retain = true
			}
		}
		if q > 0 {
			if !sess.CanSend() {
				if props != nil && props.MessageExpiryInterval != nil && *props.MessageExpiryInterval == 0 {
				} else {
					var expiry2 uint32
					var created2 int64
					if props != nil && props.MessageExpiryInterval != nil {
						expiry2 = *props.MessageExpiryInterval
						created2 = time.Now().UnixMilli()
					}
					if err := b.store.EnqueueOffline(bgCtx(), ch.client, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos, CreatedAt: created2, ExpiryInterval: expiry2}); err != nil {
						slog.Warn("store EnqueueOffline failed", "err", err)
					}
				}
				continue
			}
			pub.PacketID = sess.NextPacketID()
			if pub.PacketID == 0 {
				continue
			}
			sess.AddInflight(&session.InflightEntry{PacketID: pub.PacketID, QoS: q, Topic: topicName, Payload: payload})
			b.scheduleRetry(ch.client, pub.PacketID, 0)
		}
		pub.PubProps = pubPropsForSub(fwd, conn.Version(), subIDs)
		mqttMessagesSent.Inc()
		mqttInflight.Set(float64(sess.InflightCount()))
		_ = b.sendPacket(conn, pub)
	}
	subs := b.trie.Match(topicName)
	// 广播快速路径: 无 hook 消费包 hex、无逐订阅者 v5 属性、且 retain 不参与时，
	// QoS0 投递帧对所有匹配订阅者逐字节相同 —— 按线族 (v3.x / v5) 各 Encode
	// 一次后共享写入，避免每个订阅者重复 Encode + 分配。
	shareFrame := !retain && !b.hooks.PacketHexNeeded() && len(subIDs) == 0
	var sharedV3, sharedV5 []byte
	sharedFrame := func(isV5 bool, subClientID string) ([]byte, bool) {
		if isV5 {
			if sharedV5 == nil {
				data, err := codec.Encode(&codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: topicName, QoS: 0, Payload: payload, PubProps: fwd})
				if err != nil {
					slog.Warn("deliver encode failed", "client", subClientID, "err", err)
					return nil, false
				}
				sharedV5 = data
			}
			return sharedV5, true
		}
		if sharedV3 == nil {
			data, err := codec.Encode(&codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: topicName, QoS: 0, Payload: payload})
			if err != nil {
				slog.Warn("deliver encode failed", "client", subClientID, "err", err)
				return nil, false
			}
			sharedV3 = data
		}
		return sharedV3, true
	}
	for _, sub := range subs {
		if sub.ClientID == from && sub.NoLocal {
			continue
		}
		b.mu.RLock()
		conn, ok := b.conns[sub.ClientID]
		sess, sok := b.sessions[sub.ClientID]
		b.mu.RUnlock()
		if !ok || !sok {
			if sess != nil && sess.ExpiryInterval != 0 {
				if props != nil && props.MessageExpiryInterval != nil && *props.MessageExpiryInterval == 0 {
				} else {
					var expiry3 uint32
					var created3 int64
					if props != nil && props.MessageExpiryInterval != nil {
						expiry3 = *props.MessageExpiryInterval
						created3 = time.Now().UnixMilli()
					}
					if err := b.store.EnqueueOffline(bgCtx(), sub.ClientID, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos, CreatedAt: created3, ExpiryInterval: expiry3}); err != nil {
						slog.Warn("store EnqueueOffline failed", "err", err)
					}
				}
			}
			continue
		}
		// deliver with min QoS (publish QoS and sub QoS)
		deliverQoS := qos
		if sub.QoS < deliverQoS {
			deliverQoS = sub.QoS
		}
		// v5 RAP: 该订阅请求了 Retain As Published，转发时保留 Retain 标志
		rapFlag := false
		if retain && conn.Version() == codec.ProtocolV5 {
			if _, rap := sess.SubOptsFor(sub.Filter); rap {
				rapFlag = true
			}
		}
		// QoS0 且可共享: 单次 Encode + WriteRaw 到每个订阅者
		if deliverQoS == 0 && shareFrame && !rapFlag {
			isV5 := conn.Version() == codec.ProtocolV5
			buf, okEncode := sharedFrame(isV5, sub.ClientID)
			if !okEncode {
				continue
			}
			mqttMessagesSent.Inc()
			if err := conn.WriteRaw(buf); err != nil {
				slog.Warn("deliver failed", "client", sub.ClientID, "err", err)
			}
			continue
		}
		pub := &codec.Packet{
			Type:    codec.TypePUBLISH,
			Version: conn.Version(),
			Topic:   topicName,
			QoS:     deliverQoS,
			Payload: payload,
			Retain:  rapFlag,
		}
		if deliverQoS > 0 {
			if !sess.CanSend() {
				if props != nil && props.MessageExpiryInterval != nil && *props.MessageExpiryInterval == 0 {
				} else {
					var expiry4 uint32
					var created4 int64
					if props != nil && props.MessageExpiryInterval != nil {
						expiry4 = *props.MessageExpiryInterval
						created4 = time.Now().UnixMilli()
					}
					if err := b.store.EnqueueOffline(bgCtx(), sub.ClientID, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos, CreatedAt: created4, ExpiryInterval: expiry4}); err != nil {
						slog.Warn("store EnqueueOffline failed", "err", err)
					}
				}
				continue
			}
			pub.PacketID = sess.NextPacketID()
			if pub.PacketID == 0 {
				continue
			}
			e := &session.InflightEntry{PacketID: pub.PacketID, QoS: deliverQoS, Topic: topicName, Payload: payload}
			sess.AddInflight(e)
			b.scheduleRetry(sess.ClientID, pub.PacketID, 0)
		}
		// v5 转发属性 + SubscriptionID
		pub.PubProps = pubPropsForSub(fwd, conn.Version(), subIDs)
		mqttMessagesSent.Inc()
		mqttInflight.Set(float64(sess.InflightCount() + 1))
		if err := b.sendPacket(conn, pub); err != nil {
			slog.Warn("deliver failed", "client", sub.ClientID, "err", err)
		}
	}
}

// ---------------------------------------------------------------------------
// QoS 重试调度
//
// 持久化语义 (broker 崩溃后至少一次重投) 不变：投递时 SavePendingRetry、
// ACK 时 DeletePendingRetry。唯一变化是触发方式：不再为每条消息创建
// time.AfterFunc goroutine，而是由单一 retryLoop ticker 扫描在内存到期队列，
// 快速 ACK 场景下不会再有 20s 后空转的定时器/多余 store 删除。
// ---------------------------------------------------------------------------

const (
	retryDelay       = 20 * time.Second // 每次重试间隔
	retryMaxAttempts = 5                // 超过则丢弃 inflight
	retryScanTick    = 200 * time.Millisecond
)

// retryEntry 是在内存的到期重试项。
type retryEntry struct {
	clientID string
	packetID uint16
	retries  int
	nextAt   int64 // unix millis
}

// armRetry 将到期项加入调度队列 (若已存在则更新)。
func (b *Broker) armRetry(clientID string, packetID uint16, retries int, nextAt int64) {
	b.retryMu.Lock()
	m := b.retryQueue[clientID]
	if m == nil {
		m = make(map[uint16]*retryEntry)
		b.retryQueue[clientID] = m
	}
	m[packetID] = &retryEntry{clientID: clientID, packetID: packetID, retries: retries, nextAt: nextAt}
	b.retryMu.Unlock()
}

// disarmRetry 从内存调度队列移除到期项 (不触碰 store)。
func (b *Broker) disarmRetry(clientID string, packetID uint16) {
	b.retryMu.Lock()
	if m := b.retryQueue[clientID]; m != nil {
		delete(m, packetID)
		if len(m) == 0 {
			delete(b.retryQueue, clientID)
		}
	}
	b.retryMu.Unlock()
}

// cancelRetry 取消在内存调度并删除持久化记录 (ACK / inflight 丢失路径)。
func (b *Broker) cancelRetry(clientID string, packetID uint16) {
	b.disarmRetry(clientID, packetID)
	_ = b.store.DeletePendingRetry(bgCtx(), clientID, packetID)
}

// retryLoop 是 broker 级重试 ticker，周期性触发到期的重试。
func (b *Broker) retryLoop(ctx context.Context) {
	ticker := time.NewTicker(retryScanTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.processDueRetries()
		}
	}
}

func (b *Broker) processDueRetries() {
	now := time.Now().UnixMilli()
	b.retryMu.Lock()
	var due []*retryEntry
	for _, m := range b.retryQueue {
		for _, e := range m {
			if e.nextAt <= now {
				due = append(due, e)
			}
		}
	}
	b.retryMu.Unlock()
	for _, e := range due {
		b.fireRetry(e.clientID, e.packetID, e.retries)
	}
}

// fireRetry 处理一条到期重试：客户端在线且有对应 inflight 则 Dup 重投并
// 安排下一次；否则清理。
func (b *Broker) fireRetry(clientID string, packetID uint16, retries int) {
	b.mu.RLock()
	conn, ok1 := b.conns[clientID]
	sess, ok2 := b.sessions[clientID]
	b.mu.RUnlock()
	// 客户端离线: 暂停重试 (与旧 AfterFunc 行为一致，持久化记录保留待 ACK 清理)
	if !ok1 || !ok2 {
		b.disarmRetry(clientID, packetID)
		return
	}
	if retries >= retryMaxAttempts {
		sess.RemoveInflight(packetID)
		mqttPacketDropped.WithLabelValues("retry_exceeded").Inc()
		slog.Warn("retry exceeded, dropping inflight", "client", clientID, "packetID", packetID)
		b.cancelRetry(clientID, packetID)
		return
	}
	if e, ok := sess.GetInflight(packetID); ok {
		e.Dup = true
		pub := &codec.Packet{Type: codec.TypePUBLISH, Version: conn.Version(), Topic: e.Topic, QoS: e.QoS, Payload: e.Payload, PacketID: packetID, Dup: true}
		_ = b.sendPacket(conn, pub)
		b.scheduleRetry(clientID, packetID, retries+1)
	} else {
		b.cancelRetry(clientID, packetID)
	}
}

// scheduleRetry 投递 QoS>0 消息后调用：立即持久化 PendingRetry (崩溃恢复用)，
// 并把到期重试加入内存调度队列 (不再 per-message time.AfterFunc)。
func (b *Broker) scheduleRetry(clientID string, packetID uint16, retries int) {
	if retries >= retryMaxAttempts {
		b.mu.RLock()
		sess, ok := b.sessions[clientID]
		b.mu.RUnlock()
		if ok {
			sess.RemoveInflight(packetID)
			mqttPacketDropped.WithLabelValues("retry_exceeded").Inc()
			slog.Warn("retry exceeded, dropping inflight", "client", clientID, "packetID", packetID)
		}
		_ = b.store.DeletePendingRetry(bgCtx(), clientID, packetID)
		return
	}
	b.mu.RLock()
	sess, ok := b.sessions[clientID]
	b.mu.RUnlock()
	var topic string
	var payload []byte
	var qos byte
	if ok {
		if e, exists := sess.GetInflight(packetID); exists {
			topic = e.Topic
			payload = e.Payload
			qos = e.QoS
		}
	}
	nextAt := time.Now().UnixMilli() + retryDelay.Milliseconds()
	pr := &persistence.PendingRetry{
		ClientID:    clientID,
		PacketID:    packetID,
		Topic:       topic,
		Payload:     payload,
		QoS:         qos,
		NextRetryAt: nextAt,
		Retries:     retries,
		CreatedAt:   time.Now().UnixMilli(),
	}
	if err := b.store.SavePendingRetry(bgCtx(), pr); err != nil {
		slog.Warn("store SavePendingRetry failed", "client", clientID, "packetID", packetID, "err", err)
	}
	b.armRetry(clientID, packetID, retries, nextAt)
}
