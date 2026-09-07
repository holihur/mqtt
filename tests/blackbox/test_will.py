"""
Last Will and Testament black-box tests.

Covers: will delivery on ungraceful disconnect, will not on graceful
disconnect, will QoS, will retain.
"""

import socket
import time
import uuid
import threading

import paho.mqtt.client as mqtt
import pytest

from conftest import make_client, BrokerProcess


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class MessageCollector:
    def __init__(self):
        self._msgs = []
        self._lock = threading.Lock()

    def on_message(self, cl, userdata, msg):
        with self._lock:
            self._msgs.append((msg.topic, msg.payload, msg.retain))

    def wait(self, count=1, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if len(self._msgs) >= count:
                    return list(self._msgs)
            time.sleep(0.05)
        with self._lock:
            return list(self._msgs)


def connect_will_client(tcp_port, will_topic, will_payload, will_qos=0,
                        will_retain=False, client_id=""):
    """Connect a paho client with LWT and return it."""
    if not client_id:
        client_id = f"will-{uuid.uuid4().hex[:8]}"
    c = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
    )
    c.will_set(will_topic, will_payload, qos=will_qos, retain=will_retain)
    c.connect("127.0.0.1", tcp_port, keepalive=30)
    c.loop_start()
    return c


def force_ungraceful_disconnect(client):
    """Close the raw TCP socket WITHOUT sending an MQTT DISCONNECT packet."""
    if client.is_connected() and hasattr(client, "_sock") and client._sock:
        try:
            client._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            client._sock.close()
        except OSError:
            pass


class TestWill:

    def test_will_delivered_on_ungraceful_disconnect(self, broker_anon):
        """Client connects with will; when killed (no DISCONNECT sent),
        the will message should be delivered to subscribers."""
        uid = uuid.uuid4().hex[:8]
        will_topic = f"will/{uid}"
        will_payload = "goodbye-ungraceful"

        # Subscribe first
        collector = MessageCollector()
        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"will-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(will_topic, qos=1)
        sub.loop_start()
        time.sleep(0.3)

        # Connect with will, then kill without DISCONNECT
        will_client = connect_will_client(
            broker_anon.config.tcp_port,
            will_topic, will_payload,
            will_qos=1,
            client_id=f"will-client-{uid}",
        )
        time.sleep(0.5)
        assert will_client.is_connected(), "Will client should be connected"

        # Simulate ungraceful disconnect (close socket without MQTT DISCONNECT)
        force_ungraceful_disconnect(will_client)
        time.sleep(2)

        msgs = collector.wait(count=1, timeout=5)
        sub.disconnect()
        sub.loop_stop()

        will_msgs = [m for m in msgs if m[0] == will_topic]
        assert len(will_msgs) >= 1
        assert will_msgs[0][1] == will_payload.encode()

    def test_will_not_delivered_on_graceful_disconnect(self, broker_anon):
        """Client connects with will but disconnects gracefully (sends
        DISCONNECT); will should NOT be published."""
        uid = uuid.uuid4().hex[:8]
        will_topic = f"will-graceful/{uid}"
        will_payload = "should-not-appear"

        collector = MessageCollector()
        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"wg-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(will_topic, qos=1)
        sub.loop_start()
        time.sleep(0.3)

        will_client = connect_will_client(
            broker_anon.config.tcp_port,
            will_topic, will_payload,
            will_qos=1,
            client_id=f"wg-client-{uid}",
        )
        time.sleep(0.5)
        assert will_client.is_connected(), "Will client should be connected"

        # Graceful disconnect
        will_client.disconnect()
        will_client.loop_stop()
        time.sleep(1.5)

        msgs = collector.wait(count=1, timeout=2)
        sub.disconnect()
        sub.loop_stop()

        will_msgs = [m for m in msgs if m[0] == will_topic]
        assert len(will_msgs) == 0

    def test_will_qos1(self, broker_anon):
        uid = uuid.uuid4().hex[:8]
        will_topic = f"will-q1/{uid}"

        collector = MessageCollector()
        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"wq1-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(will_topic, qos=1)
        sub.loop_start()
        time.sleep(0.3)

        wc = connect_will_client(
            broker_anon.config.tcp_port,
            will_topic, "q1-will",
            will_qos=1,
            client_id=f"wq1-{uid}",
        )
        time.sleep(0.5)
        assert wc.is_connected()
        force_ungraceful_disconnect(wc)
        time.sleep(2)

        msgs = collector.wait(count=1, timeout=5)
        sub.disconnect()
        sub.loop_stop()

        will_msgs = [m for m in msgs if m[0] == will_topic]
        assert len(will_msgs) >= 1
        assert will_msgs[0][1] == b"q1-will"

    def test_will_qos2(self, broker_anon):
        uid = uuid.uuid4().hex[:8]
        will_topic = f"will-q2/{uid}"

        collector = MessageCollector()
        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"wq2-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(will_topic, qos=2)
        sub.loop_start()
        time.sleep(0.3)

        wc = connect_will_client(
            broker_anon.config.tcp_port,
            will_topic, "q2-will",
            will_qos=2,
            client_id=f"wq2-{uid}",
        )
        time.sleep(0.5)
        assert wc.is_connected()
        force_ungraceful_disconnect(wc)
        time.sleep(2)

        msgs = collector.wait(count=1, timeout=5)
        sub.disconnect()
        sub.loop_stop()

        will_msgs = [m for m in msgs if m[0] == will_topic]
        assert len(will_msgs) >= 1
        assert will_msgs[0][1] == b"q2-will"

    def test_will_delivered_to_connected_subscriber(self, broker_anon):
        """A will with retain=true should still be delivered to currently
        connected subscribers on ungraceful disconnect."""
        uid = uuid.uuid4().hex[:8]
        will_topic = f"will-ret/{uid}"

        # Subscribe while the will client is alive
        collector = MessageCollector()
        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"wret-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(will_topic, qos=1)
        sub.loop_start()
        time.sleep(0.3)

        wc = connect_will_client(
            broker_anon.config.tcp_port,
            will_topic, "retained-will",
            will_qos=1, will_retain=True,
            client_id=f"wret-{uid}",
        )
        time.sleep(0.5)
        assert wc.is_connected()
        force_ungraceful_disconnect(wc)
        time.sleep(2)

        msgs = collector.wait(count=1, timeout=5)
        sub.disconnect()
        sub.loop_stop()

        # The will should be delivered; per MQTT spec the live delivery
        # carries Retain=0 (Retain=1 is only for new-subscriber retained
        # delivery), matching mosquitto's behavior.
        will_msgs = [m for m in msgs if m[0] == will_topic]
        assert len(will_msgs) >= 1
        assert will_msgs[0][1] == b"retained-will"

        # A new subscriber should receive the will as a retained message
        collector2 = MessageCollector()
        sub2 = make_client(
            broker_anon.config.tcp_port,
            client_id=f"wret-sub2-{uid}",
            on_message=collector2.on_message,
        )
        sub2.subscribe(will_topic, qos=1)
        sub2.loop_start()
        msgs2 = collector2.wait(count=1, timeout=5)
        sub2.disconnect()
        sub2.loop_stop()

        retained = [m for m in msgs2 if m[0] == will_topic]
        assert len(retained) >= 1
        assert retained[0][1] == b"retained-will"
        assert retained[0][2] is True
