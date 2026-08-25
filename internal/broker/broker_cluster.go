package broker

import (
	"log/slog"
	"mqtt/internal/cluster"
	"mqtt/internal/topic"
)

func (b *Broker) hasRemoteSubscribers(topic string) bool {
	b.remoteMu.RLock()
	defer b.remoteMu.RUnlock()
	if len(b.remoteTries) == 0 {
		return true
	}
	for _, trie := range b.remoteTries {
		if len(trie.Match(topic)) > 0 {
			return true
		}
	}
	return false
}

func (b *Broker) addRemoteSub(nodeID, filter string) {
	b.remoteMu.Lock()
	defer b.remoteMu.Unlock()
	trie, ok := b.remoteTries[nodeID]
	if !ok {
		trie = topic.NewTrie()
		b.remoteTries[nodeID] = trie
	}
	trie.Add(filter, nodeID, 0, false)
}

func (b *Broker) removeRemoteSub(nodeID, filter string) {
	b.remoteMu.Lock()
	defer b.remoteMu.Unlock()
	if trie, ok := b.remoteTries[nodeID]; ok {
		trie.Remove(filter, nodeID)
	}
}

func (b *Broker) onClusterMeta(meta *cluster.ClusterMeta) {
	switch meta.Action {
	case "sub":
		b.addRemoteSub(meta.From, meta.Filter)
		slog.Debug("remote sub", "node", meta.From, "filter", meta.Filter)
	case "unsub":
		b.removeRemoteSub(meta.From, meta.Filter)
		slog.Debug("remote unsub", "node", meta.From, "filter", meta.Filter)
	}
}

func (b *Broker) onClusterMessage(msg *cluster.ClusterMessage) {
	if msg.Topic == "" || msg.Topic[0] == '$' {
		return
	}
	b.deliverLocal(msg.Topic, msg.Payload, msg.QoS, nil, msg.From)
}
