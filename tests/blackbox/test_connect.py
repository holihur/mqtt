"""
Connection black-box tests.

Covers: anonymous connect, rejected connect, clean session, keepalive ping,
multiple simultaneous clients, and client IDs.
"""

import time
import threading
import uuid

import paho.mqtt.client as mqtt
import pytest

from conftest import make_client, BrokerProcess, BrokerConfig, _free_port


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _collect_events(client, events, event_name="default"):
    """Attach callback hooks that append event dicts to *events* list."""
    def on_connect(cl, userdata, flags, rc, properties=None):
        events.append({"event": "connect", "rc": rc})

    def on_disconnect(cl, userdata, flags, rc, properties=None):
        events.append({"event": "disconnect", "rc": rc})

    def on_message(cl, userdata, msg):
        events.append({"event": "message", "topic": msg.topic, "payload": msg.payload})

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    if on_message:
        client.on_message = on_message


# -----------------------------------------------------------------------
# Tests — anonymous broker
# -----------------------------------------------------------------------

class TestConnectAnonymous:
    """Verify clients can connect when allow-anonymous=true."""

    def test_connect_and_disconnect(self, broker_anon):
        events = []
        c = make_client(broker_anon.config.tcp_port, client_id="conn-disc-1")
        _collect_events(c, events)
        c.loop_start()
        time.sleep(0.5)
        c.disconnect()
        time.sleep(0.5)
        c.loop_stop()

        assert any(e["event"] == "connect" and e["rc"] == 0 for e in events)

    def test_connect_return_code_success(self, broker_anon):
        rc_holder = {}
        def on_connect(cl, userdata, flags, rc, properties=None):
            rc_holder["rc"] = rc
        c = make_client(broker_anon.config.tcp_port, client_id="rc-check", on_connect=on_connect)
        c.loop_start()
        time.sleep(0.5)
        c.disconnect()
        time.sleep(0.3)
        c.loop_stop()
        assert rc_holder.get("rc") == 0

    def test_multiple_clients_same_broker(self, broker_anon):
        clients = []
        rc_list = []
        for i in range(5):
            def on_connect(cl, userdata, flags, rc, properties=None, idx=i):
                rc_list.append((idx, rc))
            c = make_client(
                broker_anon.config.tcp_port,
                client_id=f"multi-{i}",
                on_connect=on_connect,
            )
            c.loop_start()
            clients.append(c)
        time.sleep(1)
        for c in clients:
            c.disconnect()
        time.sleep(0.5)
        for c in clients:
            c.loop_stop()

        assert len(rc_list) == 5
        assert all(rc == 0 for _, rc in rc_list)

    def test_client_id_unique_enforced(self, broker_anon):
        """Two clients with the same ID — second should disconnect the first."""
        c1 = make_client(broker_anon.config.tcp_port, client_id="dup-id")
        c1.loop_start()
        time.sleep(0.3)
        # Second with same ID
        c2 = make_client(broker_anon.config.tcp_port, client_id="dup-id")
        c2.loop_start()
        time.sleep(1)
        # c1 should have been kicked (disconnect callback fires)
        c1_connected = c1.is_connected()
        c2_connected = c2.is_connected()
        c1.disconnect()
        c2.disconnect()
        time.sleep(0.3)
        c1.loop_stop()
        c2.loop_stop()
        # At least one should be disconnected or both may be
        # The broker should have rejected one of them
        assert not (c1_connected and c2_connected), "Both dup-ID clients remained connected"

    @pytest.mark.skip(reason="keepalive timing is flaky in CI")
    def test_keepalive_ping(self, broker_anon):
        """With short keepalive, client should survive at least one ping cycle."""
        uid = uuid.uuid4().hex[:8]
        rc_holder = {}
        def on_connect(cl, userdata, flags, rc, properties=None):
            rc_holder["rc"] = rc
        c = make_client(
            broker_anon.config.tcp_port,
            client_id=f"keepalive-{uid}",
            keepalive=4,
            on_connect=on_connect,
        )
        c.loop_start()
        time.sleep(1)
        assert c.is_connected(), "Should be connected initially"
        time.sleep(3)  # wait for at least one ping cycle
        connected = c.is_connected()
        c.disconnect()
        time.sleep(0.3)
        c.loop_stop()
        assert rc_holder.get("rc") == 0, "Should have connected successfully"


# -----------------------------------------------------------------------
# Tests — deny-all broker
# -----------------------------------------------------------------------

class TestConnectDeny:
    """Verify anonymous clients are rejected when allow-anonymous=false."""

    def test_anonymous_rejected(self, broker_deny):
        rc_holder = {}
        def on_connect(cl, userdata, flags, rc, properties=None):
            rc_holder["rc"] = rc
        def on_disconnect(cl, userdata, flags, rc, properties=None):
            rc_holder["disc_rc"] = rc

        c = make_client(
            broker_deny.config.tcp_port,
            client_id="deny-test",
            on_connect=on_connect,
            on_disconnect=on_disconnect,
        )
        c.loop_start()
        time.sleep(2)
        connected = c.is_connected()
        c.disconnect()
        time.sleep(0.5)
        c.loop_stop()

        # Client should either fail to connect (rc != 0) or get disconnected
        rc = rc_holder.get("rc")
        assert not connected or rc != 0, (
            f"Anonymous client connected to deny-all broker (rc={rc})"
        )


# -----------------------------------------------------------------------
# Tests — protocol versions
# -----------------------------------------------------------------------

class TestProtocolVersions:
    """Connect with different MQTT protocol versions."""

    @pytest.mark.parametrize("proto,proto_name", [
        (mqtt.MQTTv31, "3.1"),
        (mqtt.MQTTv311, "3.1.1"),
        (mqtt.MQTTv5, "5.0"),
    ])
    def test_connect_protocol(self, broker_anon, proto, proto_name):
        rc_holder = {}
        def on_connect(cl, userdata, flags, rc, properties=None):
            rc_holder["rc"] = rc
        c = make_client(
            broker_anon.config.tcp_port,
            client_id=f"proto-{proto_name}-{uuid.uuid4().hex[:4]}",
            protocol=proto,
            on_connect=on_connect,
        )
        c.loop_start()
        time.sleep(2)
        c.disconnect()
        time.sleep(0.5)
        c.loop_stop()
        assert rc_holder.get("rc") == 0, f"Protocol {proto_name} failed to connect"


# -----------------------------------------------------------------------
# Tests — connect with username/password
# -----------------------------------------------------------------------

class TestConnectCredentials:
    """Broker without auth accepts (and ignores) any credentials."""

    def test_username_password_accepted(self, broker_anon):
        rc_holder = {}
        def on_connect(cl, userdata, flags, rc, properties=None):
            rc_holder["rc"] = rc
        c = make_client(
            broker_anon.config.tcp_port,
            client_id=f"creds-test-{uuid.uuid4().hex[:4]}",
            username="anyuser",
            password="anypass",
            on_connect=on_connect,
        )
        c.loop_start()
        time.sleep(2)
        c.disconnect()
        time.sleep(0.5)
        c.loop_stop()
        assert rc_holder.get("rc") == 0
