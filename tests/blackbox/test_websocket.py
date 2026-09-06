"""
WebSocket transport black-box tests.

Covers: WebSocket connect, pub/sub over WS, cross-transport delivery (WS→TCP).
"""

import time
import uuid

import paho.mqtt.client as mqtt
import pytest

import threading
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
            self._msgs.append((msg.topic, msg.payload))

    def wait(self, count=1, timeout=3.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if len(self._msgs) >= count:
                    return list(self._msgs)
            time.sleep(0.05)
        with self._lock:
            return list(self._msgs)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWebSocket:

    def test_ws_connect(self, broker_ws):
        """Client should be able to connect over WebSocket."""
        rc_holder = {}
        def on_connect(cl, userdata, flags, rc, properties=None):
            rc_holder["rc"] = rc

        ws_port = broker_ws.config.ws_port
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"ws-connect-{uuid.uuid4().hex[:8]}",
            protocol=mqtt.MQTTv311,
            transport="websockets",
        )
        client.on_connect = on_connect
        client.ws_set_options(path="/mqtt")
        client.connect("127.0.0.1", ws_port, keepalive=30)
        client.loop_start()
        time.sleep(1)
        client.disconnect()
        time.sleep(0.3)
        client.loop_stop()

        assert rc_holder.get("rc") == 0

    def test_ws_pubsub(self, broker_ws):
        """Pub/sub should work over WebSocket."""
        uid = uuid.uuid4().hex[:8]
        topic = f"ws/{uid}"
        collector = MessageCollector()

        ws_port = broker_ws.config.ws_port

        sub = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"ws-sub-{uid}",
            protocol=mqtt.MQTTv311,
            transport="websockets",
        )
        sub.on_message = collector.on_message
        sub.ws_set_options(path="/mqtt")
        sub.connect("127.0.0.1", ws_port, keepalive=30)
        sub.subscribe(topic, qos=1)
        sub.loop_start()
        time.sleep(0.5)

        pub = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"ws-pub-{uid}",
            protocol=mqtt.MQTTv311,
            transport="websockets",
        )
        pub.ws_set_options(path="/mqtt")
        pub.connect("127.0.0.1", ws_port, keepalive=30)
        pub.loop_start()
        time.sleep(0.3)
        pub.publish(topic, "ws-hello", qos=1)
        time.sleep(0.5)

        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"ws-hello"

    def test_ws_to_tcp_delivery(self, broker_ws):
        """A message published over WS should be received by a TCP subscriber."""
        uid = uuid.uuid4().hex[:8]
        topic = f"ws2tcp/{uid}"
        collector = MessageCollector()

        ws_port = broker_ws.config.ws_port
        tcp_port = broker_ws.config.tcp_port

        # TCP subscriber
        sub = make_client(
            tcp_port,
            client_id=f"tcp-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=1)
        sub.loop_start()
        time.sleep(0.3)

        # WS publisher
        pub = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"ws-pub2-{uid}",
            protocol=mqtt.MQTTv311,
            transport="websockets",
        )
        pub.ws_set_options(path="/mqtt")
        pub.connect("127.0.0.1", ws_port, keepalive=30)
        pub.loop_start()
        time.sleep(0.3)
        pub.publish(topic, "cross-transport", qos=1)
        time.sleep(0.5)

        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"cross-transport"

    def test_tcp_to_ws_delivery(self, broker_ws):
        """A message published over TCP should be received by a WS subscriber."""
        uid = uuid.uuid4().hex[:8]
        topic = f"tcp2ws/{uid}"
        collector = MessageCollector()

        ws_port = broker_ws.config.ws_port
        tcp_port = broker_ws.config.tcp_port

        # WS subscriber
        sub = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"ws-sub2-{uid}",
            protocol=mqtt.MQTTv311,
            transport="websockets",
        )
        sub.on_message = collector.on_message
        sub.ws_set_options(path="/mqtt")
        sub.connect("127.0.0.1", ws_port, keepalive=30)
        sub.subscribe(topic, qos=1)
        sub.loop_start()
        time.sleep(0.5)

        # TCP publisher
        pub = make_client(tcp_port, client_id=f"tcp-pub-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "tcp-2-ws", qos=1)
        time.sleep(0.5)

        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"tcp-2-ws"
