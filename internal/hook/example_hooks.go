package hook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"mqtt/internal/codec"
)

// ---------------------------------------------------------------------------
// Example 1: TenantIsolationHook
// Enforces tenant/{tenantID}/# isolation based on clientID prefix.
// Assumes clientID format: {tenantID}-{random}  e.g. "t42-a3f9c1e2"
// If clientID doesn't contain "-", isolation is skipped (internal devices).
// ---------------------------------------------------------------------------

type AuthHook struct{ BaseHook }

func (AuthHook) ID() string { return "auth-example" }

func (AuthHook) OnAuth(clientID, username string, password []byte) error {
	if username == "blocked" {
		return fmt.Errorf("%w: user %s blocked", ErrDenied, username)
	}
	if len(password) > 0 && string(password) == "wrong" {
		return fmt.Errorf("%w: bad password for %s", ErrDenied, clientID)
	}
	slog.Debug("hook auth passed", "client", clientID, "username", username)
	return nil
}

type TenantIsolationHook struct{ BaseHook }

func (TenantIsolationHook) ID() string { return "tenant-isolation" }

func tenantOf(clientID string) string {
	if idx := strings.Index(clientID, "-"); idx > 0 {
		return clientID[:idx]
	}
	return ""
}

func (TenantIsolationHook) OnPublish(clientID, topic string, _ []byte, _ byte, _ bool) error {
	tenant := tenantOf(clientID)
	if tenant == "" {
		return nil
	}
	if strings.HasPrefix(topic, "$SYS/") {
		return nil
	}
	if tenant == "internal" && strings.HasPrefix(topic, "internal/") {
		return nil
	}
	if strings.HasPrefix(topic, "tenant/"+tenant+"/") {
		return nil
	}
	return fmt.Errorf("%w: tenant %s cannot publish to %s", ErrDenied, tenant, topic)
}

func (TenantIsolationHook) OnSubscribe(clientID, filter string, _ byte) error {
	tenant := tenantOf(clientID)
	if tenant == "" {
		return nil
	}
	if strings.HasPrefix(filter, "$SYS/") {
		return nil
	}
	if tenant == "internal" && strings.HasPrefix(filter, "internal/") {
		return nil
	}
	if strings.HasPrefix(filter, "$share/") {
		parts := strings.SplitN(filter, "/", 3)
		if len(parts) == 3 {
			filter = parts[2]
		}
	}
	if strings.HasPrefix(filter, "tenant/"+tenant+"/") {
		return nil
	}
	return fmt.Errorf("%w: tenant %s cannot subscribe to %s", ErrDenied, tenant, filter)
}

// ---------------------------------------------------------------------------
// Example 2: EncTopicValidationHook
// Validates that topics under tenant/{id}/enc/# carry non-empty payload with minimal length.
// This is a placeholder for real AES-GCM length/ciphertext checks without needing keys.
// ---------------------------------------------------------------------------

type EncTopicValidationHook struct{ BaseHook }

func (EncTopicValidationHook) ID() string { return "enc-validation" }

func isEncTopic(topic string) bool {
	// matches tenant/+/enc/#  or tenant/+/enc
	return Match("tenant/+/enc/#", topic) || Match("tenant/+/enc", topic)
}

func (EncTopicValidationHook) OnPublish(clientID, topic string, payload []byte, _ byte, _ bool) error {
	if !isEncTopic(topic) {
		return nil
	}
	if len(payload) == 0 {
		return fmt.Errorf("%w: enc topic %s requires non-empty payload", ErrDenied, topic)
	}
	if len(payload) < 16 {
		return fmt.Errorf("%w: enc topic %s payload too short (need >=16 for AES-GCM tag)", ErrDenied, topic)
	}
	slog.Debug("enc topic validated", "client", clientID, "topic", topic, "len", len(payload))
	return nil
}

// ---------------------------------------------------------------------------
// Example 3: TopicTagHook (observability / routing tag)
// Tags topics for downstream routing without blocking. Logs and could emit metrics.
// Demonstrates a non-blocking hook that never denies.
// ---------------------------------------------------------------------------

type TopicTagHook struct{ BaseHook }

func (TopicTagHook) ID() string { return "topic-tag" }

func (TopicTagHook) OnConnect(clientID string) error {
	slog.Info("hook: client connected", "client", clientID)
	return nil
}

func (TopicTagHook) OnDisconnect(clientID string, clean bool) {
	slog.Info("hook: client disconnected", "client", clientID, "clean", clean)
}

func (TopicTagHook) OnUnsubscribe(clientID, filter string) error {
	slog.Debug("hook: unsubscribe", "client", clientID, "filter", filter)
	return nil
}

func (TopicTagHook) OnPublish(clientID, topic string, payload []byte, qos byte, retain bool) error {
	var tag string
	switch {
	case strings.HasPrefix(topic, "internal/"):
		tag = "internal"
	case isEncTopic(topic):
		tag = "enc"
	case strings.HasPrefix(topic, "tenant/"):
		tag = "tenant-plain"
	default:
		tag = "other"
	}
	slog.Debug("topic tagged", "client", clientID, "topic", topic, "tag", tag, "qos", qos, "retain", retain, "len", len(payload))
	return nil
}

func (TopicTagHook) OnSubscribe(clientID, filter string, qos byte) error {
	slog.Debug("subscribe tagged", "client", clientID, "filter", filter, "qos", qos)
	return nil
}

// ---------------------------------------------------------------------------
// Example 4: HexDumpHook - replaces direct log hex dump
// Extracts hex handling from broker log into hook. Hook decides when/where to log.
// ---------------------------------------------------------------------------

type HexDumpHook struct{ BaseHook }

func (HexDumpHook) ID() string { return "hex-dump" }

func (HexDumpHook) PacketHexNeeded() bool { return true }

func (HexDumpHook) OnPacket(dir, clientID string, pkt *codec.Packet, hex string) {
	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	if pkt == nil {
		return
	}
	slog.Debug("packet "+dir, "client", clientID, "type", pkt.Type, "version", pkt.Version, "hex", hex)
}
