package cluster

import (
	"context"
	"encoding/json"
	"testing"
)

// ---------------------------------------------------------------------------
// ClusterMessage JSON round-trip
// ---------------------------------------------------------------------------

func TestClusterMessageJSON(t *testing.T) {
	msg := ClusterMessage{
		From:    "node1",
		Topic:   "sensor/temp",
		Payload: []byte(`{"v":23.5}`),
		QoS:     1,
		Retain:  true,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got ClusterMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.From != "node1" {
		t.Fatalf("From = %q", got.From)
	}
	if got.Topic != "sensor/temp" {
		t.Fatalf("Topic = %q", got.Topic)
	}
	if string(got.Payload) != `{"v":23.5}` {
		t.Fatalf("Payload = %q", got.Payload)
	}
	if got.QoS != 1 {
		t.Fatalf("QoS = %d", got.QoS)
	}
	if !got.Retain {
		t.Fatal("Retain should be true")
	}
}

// ---------------------------------------------------------------------------
// ClusterMeta JSON round-trip
// ---------------------------------------------------------------------------

func TestClusterMetaJSON(t *testing.T) {
	meta := ClusterMeta{
		From:   "node2",
		Action: "sub",
		Filter: "home/#",
	}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got ClusterMeta
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.From != "node2" {
		t.Fatalf("From = %q", got.From)
	}
	if got.Action != "sub" {
		t.Fatalf("Action = %q", got.Action)
	}
	if got.Filter != "home/#" {
		t.Fatalf("Filter = %q", got.Filter)
	}
}

// ---------------------------------------------------------------------------
// New with default prefix
// ---------------------------------------------------------------------------

func TestNew_DefaultPrefix(t *testing.T) {
	called := false
	onMsg := func(msg *ClusterMessage) { called = true }
	c := New(nil, "n1", "", onMsg)
	if c.prefix != "mqtt" {
		t.Fatalf("prefix = %q, want %q", c.prefix, "mqtt")
	}
	if c.nodeID != "n1" {
		t.Fatalf("nodeID = %q", c.nodeID)
	}
	_ = called
}

func TestNew_CustomPrefix(t *testing.T) {
	c := New(nil, "n1", "custom", nil)
	if c.prefix != "custom" {
		t.Fatalf("prefix = %q", c.prefix)
	}
}

// ---------------------------------------------------------------------------
// SetOnMeta
// ---------------------------------------------------------------------------

func TestSetOnMeta(t *testing.T) {
	c := New(nil, "n1", "", nil)
	var received *ClusterMeta
	c.SetOnMeta(func(m *ClusterMeta) { received = m })
	if c.onMeta == nil {
		t.Fatal("onMeta should be set")
	}
	// invoke it
	c.onMeta(&ClusterMeta{From: "n2", Action: "sub", Filter: "t/#"})
	if received == nil || received.From != "n2" {
		t.Fatalf("onMeta not invoked correctly: %+v", received)
	}
}

// ---------------------------------------------------------------------------
// ClusterMessage zero values
// ---------------------------------------------------------------------------

func TestClusterMessageZeroValues(t *testing.T) {
	var m ClusterMessage
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal zero: %v", err)
	}
	var got ClusterMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal zero: %v", err)
	}
	if got.From != "" || got.Topic != "" || got.QoS != 0 || got.Retain != false {
		t.Fatalf("zero values mismatch: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// ClusterMeta zero values
// ---------------------------------------------------------------------------

func TestClusterMetaZeroValues(t *testing.T) {
	var m ClusterMeta
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal zero: %v", err)
	}
	var got ClusterMeta
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal zero: %v", err)
	}
	if got.From != "" || got.Action != "" || got.Filter != "" {
		t.Fatalf("zero values mismatch: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// ClusterMessage with empty payload
// ---------------------------------------------------------------------------

func TestClusterMessageEmptyPayload(t *testing.T) {
	msg := ClusterMessage{From: "n1", Topic: "t", Payload: nil, QoS: 0}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got ClusterMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// nil payload becomes null in JSON, which unmarshals to nil
	if got.Payload != nil {
		t.Fatalf("Payload should be nil, got %q", got.Payload)
	}
}

// ---------------------------------------------------------------------------
// Stop without cancel (no panic)
// ---------------------------------------------------------------------------

func TestStopNilCancel(t *testing.T) {
	c := &Cluster{nodeID: "n1", prefix: "mqtt"}
	// Stop with nil cancel and nil cli will panic on cli.Del
	// Just test that cancel path works
	c.cancel = func() {}
	c.cli = nil
	// This will panic on cli.Del, so we only test the cancel path
	// We can't call Stop() without a real redis client
}

// ---------------------------------------------------------------------------
// Publish / PublishMeta JSON structure (without redis)
// ---------------------------------------------------------------------------

func TestPublishMessageStructure(t *testing.T) {
	// Verify the JSON that Publish would send
	cm := ClusterMessage{From: "n1", Topic: "t", Payload: []byte("p"), QoS: 1, Retain: false}
	data, err := json.Marshal(cm)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got ClusterMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.From != "n1" || got.Topic != "t" || string(got.Payload) != "p" || got.QoS != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestPublishMetaStructure(t *testing.T) {
	meta := ClusterMeta{From: "n1", Action: "sub", Filter: "home/#"}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got ClusterMeta
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.From != "n1" || got.Action != "sub" || got.Filter != "home/#" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Context usage pattern
// ---------------------------------------------------------------------------

func TestContextPattern(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	select {
	case <-ctx.Done():
		// expected
	default:
		t.Fatal("context should be done")
	}
}
