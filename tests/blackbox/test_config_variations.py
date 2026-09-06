"""
Cross-configuration parametrized integration tests.

Each test is executed against multiple broker configurations (anon, wal, …)
via the broker_param fixture.
"""

import time
import uuid
import threading

import paho.mqtt.client as mqtt
import pytest

from conftest import make_client


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
# Tests — run against every broker_param config
# ---------------------------------------------------------------------------

class TestCrossConfig:

    def test_connect_and_ping(self, broker_param):
        """Basic connect must succeed for every configuration."""
        rc_holder = {}
        def on_connect(cl, userdata, flags, rc, properties=None):
            rc_holder["rc"] = rc

        c = make_client(
            broker_param.config.tcp_port,
            client_id=f"cc-ping-{uuid.uuid4().hex[:8]}",
            on_connect=on_connect,
        )
        c.loop_start()
        time.sleep(0.5)
        assert c.is_connected()
        c.disconnect()
        time.sleep(0.3)
        c.loop_stop()
        assert rc_holder.get("rc") == 0

    def test_pubsub_qos0(self, broker_param):
        uid = uuid.uuid4().hex[:8]
        topic = f"cc/q0/{uid}"
        collector = MessageCollector()

        sub = make_client(
            broker_param.config.tcp_port,
            client_id=f"cc-q0s-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=0)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_param.config.tcp_port, client_id=f"cc-q0p-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "q0-cross", qos=0)
        time.sleep(0.5)

        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"q0-cross"

    def test_pubsub_qos1(self, broker_param):
        uid = uuid.uuid4().hex[:8]
        topic = f"cc/q1/{uid}"
        collector = MessageCollector()

        sub = make_client(
            broker_param.config.tcp_port,
            client_id=f"cc-q1s-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=1)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_param.config.tcp_port, client_id=f"cc-q1p-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "q1-cross", qos=1)
        time.sleep(0.5)

        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"q1-cross"

    def test_pubsub_qos2(self, broker_param):
        uid = uuid.uuid4().hex[:8]
        topic = f"cc/q2/{uid}"
        collector = MessageCollector()

        sub = make_client(
            broker_param.config.tcp_port,
            client_id=f"cc-q2s-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=2)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_param.config.tcp_port, client_id=f"cc-q2p-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "q2-cross", qos=2)
        time.sleep(1)

        msgs = collector.wait(count=1, timeout=3)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"q2-cross"

    def test_wildcard_single(self, broker_param):
        uid = uuid.uuid4().hex[:8]
        collector = MessageCollector()

        sub = make_client(
            broker_param.config.tcp_port,
            client_id=f"cc-wc1-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(f"cc/wc/+/{uid}", qos=0)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_param.config.tcp_port, client_id=f"cc-wc1p-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(f"cc/wc/hello/{uid}", "wildcard-single", qos=0)
        time.sleep(0.5)

        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"wildcard-single"

    def test_wildcard_multi(self, broker_param):
        uid = uuid.uuid4().hex[:8]
        collector = MessageCollector()

        sub = make_client(
            broker_param.config.tcp_port,
            client_id=f"cc-wch-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(f"cc/wch/{uid}/#", qos=0)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_param.config.tcp_port, client_id=f"cc-wchp-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(f"cc/wch/{uid}/a/b", "wildcard-hash", qos=0)
        time.sleep(0.5)

        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"wildcard-hash"

    def test_retain_delivery(self, broker_param):
        uid = uuid.uuid4().hex[:8]
        topic = f"cc/ret/{uid}"

        # Publish retained
        pub = make_client(broker_param.config.tcp_port, client_id=f"cc-ret-p-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "retained-cross", qos=1, retain=True)
        time.sleep(0.5)
        pub.disconnect()
        pub.loop_stop()
        time.sleep(0.3)

        # New subscriber should get retained
        collector = MessageCollector()
        sub = make_client(
            broker_param.config.tcp_port,
            client_id=f"cc-ret-s-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=1)
        sub.loop_start()
        time.sleep(1)

        msgs = collector.wait(count=1, timeout=3)
        sub.disconnect()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"retained-cross"

    def test_will_delivery(self, broker_param):
        import socket as _socket
        uid = uuid.uuid4().hex[:8]
        will_topic = f"cc/will/{uid}"

        # Subscribe first
        collector = MessageCollector()
        sub = make_client(
            broker_param.config.tcp_port,
            client_id=f"cc-will-s-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(will_topic, qos=1)
        sub.loop_start()
        time.sleep(0.3)

        # Client with will
        wc = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"cc-will-c-{uid}",
            protocol=mqtt.MQTTv311,
        )
        wc.will_set(will_topic, "will-cross", qos=1)
        wc.connect("127.0.0.1", broker_param.config.tcp_port, keepalive=30)
        wc.loop_start()
        time.sleep(0.5)
        # Ungraceful disconnect
        if hasattr(wc, "_sock") and wc._sock:
            try:
                wc._sock.close()
            except OSError:
                pass
        time.sleep(2)

        msgs = collector.wait(count=1, timeout=5)
        sub.disconnect()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"will-cross"

    def test_large_payload(self, broker_param):
        uid = uuid.uuid4().hex[:8]
        topic = f"cc/large/{uid}"
        payload = "Z" * 32768
        collector = MessageCollector()

        sub = make_client(
            broker_param.config.tcp_port,
            client_id=f"cc-lg-s-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=0)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_param.config.tcp_port, client_id=f"cc-lg-p-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, payload, qos=0)
        time.sleep(0.5)

        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == payload.encode()
