package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cluster provides inter-broker routing via Redis PubSub.

type Cluster struct {
	nodeID string
	cli    redis.UniversalClient
	prefix string
	onMsg  func(msg *ClusterMessage)
	cancel context.CancelFunc
}

type ClusterMessage struct {
	From    string `json:"from"`
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	QoS     byte   `json:"qos"`
	Retain  bool   `json:"retain"`
}

func New(cli redis.UniversalClient, nodeID, prefix string, onMsg func(*ClusterMessage)) *Cluster {
	if prefix == "" {
		prefix = "mqtt"
	}
	return &Cluster{nodeID: nodeID, cli: cli, prefix: prefix, onMsg: onMsg}
}

func (c *Cluster) Start(ctx context.Context) error {
	ctx2, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	// node heartbeat
	go c.heartbeatLoop(ctx2)

	channel := c.prefix + ":cluster"
	pubsub := c.cli.Subscribe(ctx2, channel)
	if _, err := pubsub.Receive(ctx2); err != nil {
		return err
	}
	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx2.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				var cm ClusterMessage
				if err := json.Unmarshal([]byte(m.Payload), &cm); err != nil {
					continue
				}
				if cm.From == c.nodeID {
					continue
				}
				if c.onMsg != nil {
					c.onMsg(&cm)
				}
			}
		}
	}()
	return nil
}

func (c *Cluster) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	// remove node key
	_ = c.cli.Del(context.Background(), c.prefix+":nodes:"+c.nodeID).Err()
}

func (c *Cluster) heartbeatLoop(ctx context.Context) {
	key := c.prefix + ":nodes:" + c.nodeID
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	// initial register
	_ = c.cli.Set(ctx, key, time.Now().Unix(), 15*time.Second).Err()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.cli.Set(ctx, key, time.Now().Unix(), 15*time.Second).Err()
		}
	}
}

func (c *Cluster) Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	cm := ClusterMessage{From: c.nodeID, Topic: topic, Payload: payload, QoS: qos, Retain: retain}
	data, err := json.Marshal(cm)
	if err != nil {
		return err
	}
	return c.cli.Publish(ctx, c.prefix+":cluster", data).Err()
}

func (c *Cluster) Nodes(ctx context.Context) ([]string, error) {
	var out []string
	iter := c.cli.Scan(ctx, 0, c.prefix+":nodes:*", 0).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		// k = prefix:nodes:nodeID
		// extract nodeID
		p := c.prefix + ":nodes:"
		if len(k) > len(p) {
			out = append(out, k[len(p):])
		}
	}
	return out, nil
}

var _ = fmt.Sprintf
