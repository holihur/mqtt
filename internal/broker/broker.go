package broker

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"mqtt/internal/auth"
	"mqtt/internal/cluster"
	"mqtt/internal/codec"
	"mqtt/internal/persistence"
	"mqtt/internal/session"
	"mqtt/internal/topic"
	"mqtt/internal/transport"
	"net/http"
	_ "net/http/pprof"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	NodeID         string
	TCPAddr        string
	WSAddr         string
	RedisAddr      string
	PprofAddr      string
	MaxPacketSize  int
	AllowAnonymous bool
}

type BrokerStats struct {
	StartedAt        time.Time
	MessagesReceived int64
	MessagesSent     int64
	ClientsConnected int64
	ClientsTotal     int64
}

type Broker struct {
	cfg      Config
	store    persistence.Store
	trie     *topic.Trie
	auth     auth.Authenticator
	nodeID   string
	cluster  *cluster.Cluster
	redisCli redis.UniversalClient

	mu       sync.RWMutex
	conns    map[string]*transport.Conn // clientID -> conn
	sessions map[string]*session.Session

	statsMu  sync.Mutex
	stats    BrokerStats
	listener *transport.Listener
}

func New(cfg Config, store persistence.Store, authenticator auth.Authenticator) *Broker {
	if cfg.NodeID == "" {
		cfg.NodeID = uuid.NewString()[:8]
	}
	if authenticator == nil {
		authenticator = &auth.AllowAll{}
	}
	if cfg.MaxPacketSize == 0 {
		cfg.MaxPacketSize = 1 << 20 // 1MB
	}
	b := &Broker{
		cfg:      cfg,
		store:    store,
		trie:     topic.NewTrie(),
		auth:     authenticator,
		nodeID:   cfg.NodeID,
		conns:    make(map[string]*transport.Conn),
		sessions: make(map[string]*session.Session),
	}
	// setup redis cluster if addr provided
	if cfg.RedisAddr != "" {
		cli := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{cfg.RedisAddr}})
		// test ping but not fatal
		if err := cli.Ping(context.Background()).Err(); err != nil {
			log.Printf("redis ping failed: %v (cluster disabled)", err)
		} else {
			b.redisCli = cli
			b.cluster = cluster.New(cli, cfg.NodeID, "mqtt", b.onClusterMessage)
		}
	}
	// If store is nil, use memory
	if b.store == nil {
		b.store = persistence.NewMemoryStore()
	}
	return b
}

func (b *Broker) Start(ctx context.Context) error {
	b.stats.StartedAt = time.Now()
	if b.cfg.PprofAddr != "" {
		go func() { _ = http.ListenAndServe(b.cfg.PprofAddr, nil) }()
		log.Printf("pprof listening %s", b.cfg.PprofAddr)
	}
	if b.cluster != nil {
		if err := b.cluster.Start(ctx); err != nil {
			log.Printf("cluster start failed: %v", err)
		} else {
			log.Printf("cluster started node=%s", b.nodeID)
		}
	}
	go b.sysTicker(ctx)
	b.listener = transport.NewListener(b.cfg.TCPAddr, nil, b.cfg.WSAddr)
	log.Printf("broker node=%s listening tcp=%s ws=%s redis=%s", b.nodeID, b.cfg.TCPAddr, b.cfg.WSAddr, b.cfg.RedisAddr)
	return b.listener.Listen(ctx, b.handleRawConn)
}
func (b *Broker) sysTicker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.publishSys()
		}
	}
}
func (b *Broker) publishSys() {
	b.statsMu.Lock()
	u := time.Since(b.stats.StartedAt).Seconds()
	n := int64(len(b.conns))
	b.statsMu.Unlock()
	b.routeMessage("$SYS/broker/uptime", []byte(fmt.Sprintf("%.0f", u)), 0, true, nil, "sys")
	b.routeMessage("$SYS/broker/clients/connected", []byte(fmt.Sprintf("%d", n)), 0, true, nil, "sys")
	_ = u
}

func (b *Broker) handleRawConn(raw net.Conn) {
	conn := transport.NewConn(raw, b.cfg.MaxPacketSize)
	// first packet must be CONNECT with timeout 10s
	_ = raw.SetReadDeadline(time.Now().Add(10 * time.Second))
	pkt, err := conn.ReadPacket()
	if err != nil {
		log.Printf("read CONNECT failed %s: %v", raw.RemoteAddr(), err)
		_ = raw.Close()
		return
	}
	if pkt.Type != codec.TypeCONNECT {
		log.Printf("first packet not CONNECT from %s", raw.RemoteAddr())
		_ = raw.Close()
		return
	}
	// Authenticate
	if !b.auth.Authenticate(pkt.ClientID, pkt.Username, pkt.Password) {
		reason := byte(0x04) // bad username/password for v3
		if pkt.Version == codec.ProtocolV5 {
			reason = 0x86 // Bad User Name or Password
		}
		resp := &codec.Packet{Type: codec.TypeCONNACK, Version: pkt.Version, ReasonCode: reason}
		_ = conn.WritePacket(resp)
		_ = conn.Close()
		return
	}
	clientID := pkt.ClientID
	if clientID == "" {
		// v3: if clean session true, broker may assign id. For simplicity, generate
		clientID = "auto-" + uuid.NewString()[:8]
	}
	conn.SetClientID(clientID)
	conn.SetVersion(pkt.Version)

	// Session handling
	sess, err := b.getOrCreateSession(pkt)
	if err != nil {
		log.Printf("session error: %v", err)
		_ = conn.Close()
		return
	}
	sess.ClientID = clientID
	sess.Version = pkt.Version
	sess.KeepAlive = pkt.KeepAlive
	sess.Connected = true
	sess.NodeID = b.nodeID
	if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil {
		if pkt.Properties.ReceiveMaximum != nil {
			sess.ReceiveMaximum = *pkt.Properties.ReceiveMaximum
		}
		if pkt.Properties.MaximumPacketSize != nil {
			sess.MaximumPacketSize = *pkt.Properties.MaximumPacketSize
		}
		if pkt.Properties.TopicAliasMaximum != nil {
			sess.TopicAliasMaximum = *pkt.Properties.TopicAliasMaximum
		}
	}
	b.statsMu.Lock()
	b.stats.ClientsConnected = int64(len(b.conns)) + 1
	b.stats.ClientsTotal++
	b.statsMu.Unlock()

	// Clean start handling per version
	clean := pkt.ConnectFlags.CleanSession
	expiry := uint32(0)
	if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil && pkt.Properties.SessionExpiryInterval != nil {
		expiry = *pkt.Properties.SessionExpiryInterval
	} else if clean {
		expiry = 0
	} else {
		expiry = 0xFFFFFFFF // v3 clean false => never expire
	}
	sess.CleanStart = clean
	sess.ExpiryInterval = expiry

	// Kick existing connection with same clientID
	b.mu.Lock()
	if old, ok := b.conns[clientID]; ok {
		_ = old.Close()
	}
	b.conns[clientID] = conn
	b.sessions[clientID] = sess
	b.mu.Unlock()
	_ = b.store.SaveSession(context.Background(), sess)

	// Will
	if pkt.Will != nil {
		sess.Will = &session.Will{
			Topic:         pkt.Will.Topic,
			Payload:       pkt.Will.Payload,
			QoS:           pkt.Will.QoS,
			Retain:        pkt.Will.Retain,
			DelayInterval: pkt.Will.DelayInterval,
		}
	}

	// CONNACK
	connack := &codec.Packet{
		Type:           codec.TypeCONNACK,
		Version:        pkt.Version,
		SessionPresent: false, // TODO: check if session existed
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
		mps := uint32(b.cfg.MaxPacketSize)
		props.MaximumPacketSize = &mps
		ta := uint16(100)
		props.TopicAliasMaximum = &ta
		connack.ConnProperties = props
	}
	if err := conn.WritePacket(connack); err != nil {
		_ = conn.Close()
		return
	}

	// Subscribe persistence: restore trie entries from session.Subscriptions
	for filter, qos := range sess.Subscriptions {
		b.trie.Add(filter, clientID, qos, false)
	}

	// Replay retained for existing subs? Not needed until SUBSCRIBE

	// Replay offline queue
	if offline, _ := b.store.DequeueOffline(context.Background(), clientID); len(offline) > 0 {
		for _, m := range offline {
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
			_ = conn.WritePacket(pub)
		}
	}

	// Main loop
	_ = raw.SetReadDeadline(time.Time{}) // clear
	conn.SetOnClose(func() {
		b.onClientDisconnect(clientID, sess, false)
	})
	go b.readLoop(conn, sess)
	// keepalive monitor
	if pkt.KeepAlive > 0 {
		go b.keepAliveMonitor(conn, sess)
	}
}

func (b *Broker) getOrCreateSession(pkt *codec.Packet) (*session.Session, error) {
	clientID := pkt.ClientID
	if clientID == "" {
		return session.NewSession(clientID, pkt.Version, pkt.ConnectFlags.CleanSession, 0), nil
	}
	// try memory first
	b.mu.RLock()
	if s, ok := b.sessions[clientID]; ok {
		b.mu.RUnlock()
		return s, nil
	}
	b.mu.RUnlock()
	// try store
	s, err := b.store.GetSession(context.Background(), clientID)
	if err != nil {
		return nil, err
	}
	if s != nil {
		// clean start => clear old session
		if pkt.ConnectFlags.CleanSession {
			if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil && pkt.Properties.SessionExpiryInterval != nil && *pkt.Properties.SessionExpiryInterval == 0 {
				// clean start true and expiry 0 => delete
			}
			// For v3 clean true or v5 clean true, we should clear subscriptions
			s.Subscriptions = make(map[string]byte)
			s.Inflight = make(map[uint16]*session.InflightEntry)
			_ = b.store.ClearOffline(context.Background(), clientID)
		}
		return s, nil
	}
	// new session
	expiry := uint32(0)
	if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil && pkt.Properties.SessionExpiryInterval != nil {
		expiry = *pkt.Properties.SessionExpiryInterval
	}
	s = session.NewSession(clientID, pkt.Version, pkt.ConnectFlags.CleanSession, expiry)
	return s, nil
}

func (b *Broker) readLoop(conn *transport.Conn, sess *session.Session) {
	defer func() { _ = conn.Close() }()
	for {
		pkt, err := conn.ReadPacket()
		if err != nil {
			// trigger will if not clean disconnect
			b.onClientDisconnect(conn.ClientID(), sess, false)
			return
		}
		switch pkt.Type {
		case codec.TypePUBLISH:
			b.handlePublish(conn, sess, pkt)
		case codec.TypeSUBSCRIBE:
			b.handleSubscribe(conn, sess, pkt)
		case codec.TypeUNSUBSCRIBE:
			b.handleUnsubscribe(conn, sess, pkt)
		case codec.TypePUBACK:
			sess.RemoveInflight(pkt.PacketID)
		case codec.TypePUBREC:
			// QoS2: send PUBREL
			if _, ok := sess.GetInflight(pkt.PacketID); ok {
				rel := &codec.Packet{Type: codec.TypePUBREL, Version: conn.Version(), PacketID: pkt.PacketID}
				_ = conn.WritePacket(rel)
			} else {
				// publish QoS2 inbound: receive PUBLISH then send PUBREC, wait PUBREL
				// This path is for inbound QoS2 publish? handled in handlePublish
				// For outbound QoS2, ack is PUBREC -> PUBREL -> PUBCOMP
			}
		case codec.TypePUBREL:
			// inbound QoS2 complete
			sess.RemoveInflight(pkt.PacketID)
			comp := &codec.Packet{Type: codec.TypePUBCOMP, Version: conn.Version(), PacketID: pkt.PacketID}
			_ = conn.WritePacket(comp)
		case codec.TypePUBCOMP:
			sess.RemoveInflight(pkt.PacketID)
		case codec.TypePINGREQ:
			resp := &codec.Packet{Type: codec.TypePINGRESP, Version: conn.Version()}
			_ = conn.WritePacket(resp)
		case codec.TypeDISCONNECT:
			b.onClientDisconnect(conn.ClientID(), sess, true)
			return
		default:
			log.Printf("unhandled packet type %d from %s", pkt.Type, conn.ClientID())
		}
	}
}

func (b *Broker) handlePublish(conn *transport.Conn, sess *session.Session, pkt *codec.Packet) {
	// ACL check
	if !b.auth.Authorize(sess.ClientID, pkt.Topic, true) {
		if sess.Version == codec.ProtocolV5 {
			ack := &codec.Packet{Type: codec.TypePUBACK, Version: codec.ProtocolV5, PacketID: pkt.PacketID, Reason: 0x87} // Not authorized
			if pkt.QoS == 1 {
				_ = conn.WritePacket(ack)
			}
		}
		return
	}
	// topic alias handling (v5)
	topicName := pkt.Topic
	if sess.Version == codec.ProtocolV5 && pkt.PubProps != nil && pkt.PubProps.TopicAlias != nil {
		alias := *pkt.PubProps.TopicAlias
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
					_ = conn.WritePacket(ack)
				}
				return
			}
		}
	}
	if topicName == "" {
		return
	}
	// QoS2 inbound: need to send PUBREC and store
	if pkt.QoS == 2 {
		// dedup by packetID
		if _, exists := sess.GetInflight(pkt.PacketID); exists {
			// duplicate
			rec := &codec.Packet{Type: codec.TypePUBREC, Version: conn.Version(), PacketID: pkt.PacketID}
			_ = conn.WritePacket(rec)
			return
		}
		sess.AddInflight(&session.InflightEntry{PacketID: pkt.PacketID, QoS: 2, Topic: topicName, Payload: pkt.Payload, State: "qos2-publish"})
		rec := &codec.Packet{Type: codec.TypePUBREC, Version: conn.Version(), PacketID: pkt.PacketID}
		_ = conn.WritePacket(rec)
		// wait for PUBREL before routing (spec: should route after PUBREL)
		// For simplicity, route now (at-most-once side effect is same if we dedup)
	}

	// Retain handling
	if pkt.Retain {
		if len(pkt.Payload) == 0 {
			_ = b.store.DeleteRetained(context.Background(), topicName)
		} else {
			_ = b.store.SaveRetained(context.Background(), topicName, &persistence.Message{Topic: topicName, Payload: pkt.Payload, QoS: pkt.QoS, Retain: true})
		}
	}

	// ACK for QoS1
	if pkt.QoS == 1 {
		ack := &codec.Packet{Type: codec.TypePUBACK, Version: conn.Version(), PacketID: pkt.PacketID}
		if sess.Version == codec.ProtocolV5 {
			ack.Reason = 0
		}
		_ = conn.WritePacket(ack)
	}

	// Route locally + cluster
	b.routeMessage(topicName, pkt.Payload, pkt.QoS, pkt.Retain, pkt.PubProps, sess.ClientID)

	// For QoS2 inbound, actual routing should happen after PUBREL; we already did. To be spec compliant, we would defer until PUBREL. Simplified as above.
}

func (b *Broker) routeMessage(topicName string, payload []byte, qos byte, retain bool, props *codec.Properties, from string) {
	b.statsMu.Lock()
	b.stats.MessagesReceived++
	b.statsMu.Unlock()
	if b.cfg.MaxPacketSize > 0 && len(payload)+len(topicName) > b.cfg.MaxPacketSize {
		return
	}
	if b.cluster != nil {
		_ = b.cluster.Publish(context.Background(), topicName, payload, qos, retain)
	}
	b.deliverLocal(topicName, payload, qos, props, from)
}

func (b *Broker) deliverLocal(topicName string, payload []byte, qos byte, props *codec.Properties, from string) {
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
				_ = b.store.EnqueueOffline(context.Background(), sub.ClientID, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos})
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
				_ = b.store.EnqueueOffline(context.Background(), sub.ClientID, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos})
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
		if err := conn.WritePacket(pub); err != nil {
			log.Printf("deliver to %s failed: %v", sub.ClientID, err)
		}
	}
}

func (b *Broker) handleSubscribe(conn *transport.Conn, sess *session.Session, pkt *codec.Packet) {
	var codes []byte
	for _, sub := range pkt.Subscriptions {
		if !topic.IsValidFilter(sub.Filter) {
			codes = append(codes, 0x80) // failure
			continue
		}
		if !b.auth.Authorize(sess.ClientID, sub.Filter, false) {
			if sess.Version == codec.ProtocolV5 {
				codes = append(codes, 0x87) // Not authorized
			} else {
				codes = append(codes, 0x80)
			}
			continue
		}
		// add to trie and session
		b.trie.Add(sub.Filter, sess.ClientID, sub.QoS, sub.NoLocal)
		sess.Subscriptions[sub.Filter] = sub.QoS
		_ = b.store.SaveSession(context.Background(), sess)
		codes = append(codes, sub.QoS)

		// deliver retained messages matching this filter
		retained, _ := b.store.ListRetained(context.Background())
		for _, m := range retained {
			// check if retained topic matches filter (use trie match trick: see if filter matches topic)
			// simple: create temp trie with one filter and match
			if matchFilter(m.Topic, sub.Filter) {
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
				_ = conn.WritePacket(pub)
			}
		}
	}
	ack := &codec.Packet{Type: codec.TypeSUBACK, Version: conn.Version(), PacketID: pkt.PacketID, SubackCodes: codes}
	if sess.Version == codec.ProtocolV5 {
		ack.SubackProps = &codec.Properties{}
	}
	_ = conn.WritePacket(ack)
}

func (b *Broker) handleUnsubscribe(conn *transport.Conn, sess *session.Session, pkt *codec.Packet) {
	for _, t := range pkt.Topics {
		b.trie.Remove(t, sess.ClientID)
		delete(sess.Subscriptions, t)
	}
	_ = b.store.SaveSession(context.Background(), sess)
	ack := &codec.Packet{Type: codec.TypeUNSUBACK, Version: conn.Version(), PacketID: pkt.PacketID}
	if sess.Version == codec.ProtocolV5 {
		ack.UnsubackProps = &codec.Properties{}
		ack.UnsubackCodes = make([]byte, len(pkt.Topics)) // 0 = success
	}
	_ = conn.WritePacket(ack)
}

func (b *Broker) onClientDisconnect(clientID string, sess *session.Session, clean bool) {
	b.mu.Lock()
	delete(b.conns, clientID)
	// clean up trie entries for this client? Keep if session expiry >0
	if sess != nil && sess.ExpiryInterval == 0 {
		for f := range sess.Subscriptions {
			b.trie.Remove(f, clientID)
		}
		if !clean {
			// will handling
			b.handleWill(sess)
		}
		// delete session if clean
		b.mu.Unlock()
		_ = b.store.DeleteSession(context.Background(), clientID)
		return
	}
	b.mu.Unlock()
	if !clean && sess != nil && sess.Will != nil {
		b.handleWill(sess)
	}
	// else keep session for expiry interval (store persists)
	if sess != nil {
		sess.Connected = false
		_ = b.store.SaveSession(context.Background(), sess)
		if sess.ExpiryInterval == 0 && clean {
			for f := range sess.Subscriptions {
				b.trie.Remove(f, clientID)
			}
		}
	}
}

func (b *Broker) handleWill(sess *session.Session) {
	if sess.Will == nil {
		return
	}
	w := sess.Will
	sess.Will = nil
	// delay
	if w.DelayInterval > 0 {
		time.AfterFunc(time.Duration(w.DelayInterval)*time.Second, func() {
			b.routeMessage(w.Topic, w.Payload, w.QoS, w.Retain, nil, sess.ClientID)
		})
		return
	}
	b.routeMessage(w.Topic, w.Payload, w.QoS, w.Retain, nil, sess.ClientID)
}

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
			_ = conn.WritePacket(pub)
			b.scheduleRetry(clientID, packetID)
		}
	})
}

func (b *Broker) onClusterMessage(msg *cluster.ClusterMessage) {
	b.deliverLocal(msg.Topic, msg.Payload, msg.QoS, nil, msg.From)
}

func matchFilter(t, filter string) bool {
	// reuse trie for single match
	tr := topic.NewTrie()
	tr.Add(filter, "test", 0, false)
	return len(tr.Match(t)) > 0
}

var _ = fmt.Sprintf
