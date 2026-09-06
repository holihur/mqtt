"""
Retained messages black-box tests.

Covers: retain publish, retain delivery to new subscriber,
retain deletion, retain over QoS levels.
"""

import time
import uuid

import paho.mqtt.client as mqtt
import pytest

from conftest import make_client, BrokerProcess


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class MessageCollector:
    def __init__(self):
        self._msgs = []
        self._lock = __import__("threading").Lock()

    def on_message(self, cl, userdata, msg):
        with self._lock:
            self._msgs.append((msg.topic, msg.payload, msg.retain))

    def wait(self, count=1, timeout=3.0):
        import time as _t
        deadline = _t.monotonic() + timeout
        while _t.monotonic() < deadline:
            with self._lock:
                if len(self._msgs) >= count:
                    return list(self._msgs)
            _t.sleep(0.05)
        with self._lock:
            return list(self._msgs)


class TestRetain:

    def test_retain_message_received_by_new_subscriber(self, broker_anon):
        """Publish with retain=True; a new subscriber should get it."""
        uid = uuid.uuid4().hex[:8]
        topic = f"retain/{uid}"

        # Publisher
        pub = make_client(broker_anon.config.tcp_port, client_id=f"rpub-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "retained-hello", qos=1, retain=True)
        time.sleep(0.5)
        pub.disconnect()
        pub.loop_stop()
        time.sleep(0.3)

        # New subscriber — should receive the retained message
        collector = MessageCollector()
        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"rsub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=1)
        sub.loop_start()
        time.sleep(1)

        msgs = collector.wait(count=1, timeout=2)
        sub.disconnect()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][0] == topic
        assert msgs[0][1] == b"retained-hello"
        assert msgs[0][2] is True  # retain flag

    def test_retain_cleared_by_empty_payload(self, broker_anon):
        """Publish retain=True then publish retain=True with empty payload
        to clear; new subscriber should get nothing."""
        uid = uuid.uuid4().hex[:8]
        topic = f"retain-clear/{uid}"

        pub = make_client(broker_anon.config.tcp_port, client_id=f"rc-pub-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "will-be-cleared", qos=1, retain=True)
        time.sleep(0.5)
        # Clear
        pub.publish(topic, "", qos=1, retain=True)
        time.sleep(0.5)
        pub.disconnect()
        pub.loop_stop()
        time.sleep(0.3)

        collector = MessageCollector()
        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"rc-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=1)
        sub.loop_start()
        time.sleep(1)

        msgs = collector.wait(count=1, timeout=2)
        sub.disconnect()
        sub.loop_stop()

        # Should NOT receive the retained message
        retained = [m for m in msgs if m[2]]
        assert len(retained) == 0

    def test_retain_qos0(self, broker_anon):
        uid = uuid.uuid4().hex[:8]
        topic = f"retain-q0/{uid}"

        pub = make_client(broker_anon.config.tcp_port, client_id=f"rq0-pub-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "q0-retained", qos=0, retain=True)
        time.sleep(0.5)
        pub.disconnect()
        pub.loop_stop()
        time.sleep(0.3)

        collector = MessageCollector()
        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"rq0-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=0)
        sub.loop_start()
        time.sleep(1)

        msgs = collector.wait(count=1, timeout=2)
        sub.disconnect()
        sub.loop_stop()

        assert len(msgs) >= 1
        assert msgs[0][1] == b"q0-retained"

    def test_non_retain_not_stored(self, broker_anon):
        """A non-retained message should NOT be delivered to a later subscriber."""
        uid = uuid.uuid4().hex[:8]
        topic = f"no-retain/{uid}"

        pub = make_client(broker_anon.config.tcp_port, client_id=f"nr-pub-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "ephemeral", qos=1, retain=False)
        time.sleep(0.5)
        pub.disconnect()
        pub.loop_stop()
        time.sleep(0.3)

        collector = MessageCollector()
        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"nr-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=1)
        sub.loop_start()
        time.sleep(1)

        msgs = collector.wait(count=1, timeout=2)
        sub.disconnect()
        sub.loop_stop()

        assert len(msgs) == 0
