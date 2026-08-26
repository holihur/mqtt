package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(func() { mr.Close() })
	cli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	return mr, cli
}

func TestClusterStartStopPublishNodes(t *testing.T) {
	_, cli := newTestRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan *ClusterMessage, 2)
	c := New(cli, "nodeA", "test-prefix", func(m *ClusterMessage) {
		received <- m
	})
	c.SetOnMeta(func(m *ClusterMeta) {})
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := c.Publish(ctx, "topic/test", []byte("payload"), 1, false); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := c.PublishMeta(ctx, "sub", "home/#"); err != nil {
		t.Fatalf("PublishMeta: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	// Nodes should contain nodeA after heartbeat
	nodes, err := c.Nodes(ctx)
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	found := false
	for _, n := range nodes {
		if n == "nodeA" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Nodes should contain nodeA, got %v", nodes)
	}
	// second node should receive publish (from different nodeID)
	c2 := New(cli, "nodeB", "test-prefix", func(m *ClusterMessage) {
		received <- m
	})
	_ = c2.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	if err := c.Publish(ctx, "topic/test2", []byte("hello"), 0, true); err != nil {
		t.Fatalf("Publish2: %v", err)
	}
	select {
	case m := <-received:
		if m.Topic != "topic/test2" {
			t.Fatalf("got %q", m.Topic)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for cluster message")
	}
	c.Stop()
	c2.Stop()
}

func TestClusterHeartbeatAndStop(t *testing.T) {
	_, cli := newTestRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	c := New(cli, "nodeX", "mqtt", nil)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	c.Stop()
	cancel()
	time.Sleep(50 * time.Millisecond)
	// Stop without Start should not panic
	c2 := &Cluster{nodeID: "n", prefix: "mqtt", cli: cli}
	c2.cancel = func() {}
	c2.Stop()
}

func TestClusterPublishMetaIsolation(t *testing.T) {
	_, cli := newTestRedis(t)
	ctx := context.Background()
	c := New(cli, "node1", "mqtt", nil)
	metaReceived := false
	c.SetOnMeta(func(m *ClusterMeta) { metaReceived = true })
	_ = c.Start(ctx)
	defer c.Stop()
	time.Sleep(100 * time.Millisecond)
	// publish from same node should be ignored
	if err := c.PublishMeta(ctx, "sub", "home/#"); err != nil {
		t.Fatalf("PublishMeta: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if metaReceived {
		t.Fatalf("same node meta should be ignored")
	}
}
