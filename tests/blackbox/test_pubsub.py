"""
Publish / Subscribe black-box tests.

Covers: basic pub/sub, QoS 0/1/2, wildcards (+, #), multi-subscriber,
topic isolation, large payloads.
"""

import time
import threading
import uuid

import paho.mqtt.client as mqtt
import pytest

from conftest import make_client, BrokerProcess


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

class MessageCollector:
    """Thread-safe collector for received messages."""

    def __init__(self):
        self._msgs = []
        self._lock = threading.Lock()
        self._event = threading.Event()

    def on_message(self, cl, userdata, msg):
        with self._lock:
            self._msgs.append((msg.topic, msg.payload))
        self._event.set()

    def wait(self, count=1, timeout=3.0) -> list:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if len(self._msgs) >= count:
                    return list(self._msgs)
            time.sleep(0.05)
        with self._lock:
            return list(self._msgs)

    def clear(self):
        with self._lock:
            self._msgs.clear()
        self._event.clear()


def _pub_sub_roundtrip(broker, topic, payload, qos=0, client_id_prefix="ps"):
    """Publish one message and return collected messages on the same topic."""
    uid = uuid.uuid4().hex[:8]
    collector = MessageCollector()

    sub = make_client(
        broker.config.tcp_port,
        client_id=f"{client_id_prefix}-sub-{uid}",
        on_message=collector.on_message,
    )
    sub.subscribe(topic, qos=qos)
    sub.loop_start()
    time.sleep(0.3)

    pub = make_client(broker.config.tcp_port, client_id=f"{client_id_prefix}-pub-{uid}")
    pub.loop_start()
    time.sleep(0.2)
    pub.publish(topic, payload, qos=qos)
    time.sleep(0.5)

    msgs = collector.wait(count=1, timeout=2.0)
    pub.disconnect()
    sub.disconnect()
    time.sleep(0.3)
    pub.loop_stop()
    sub.loop_stop()
    return msgs


# -----------------------------------------------------------------------
# Tests — basic pub/sub
# -----------------------------------------------------------------------

class TestPubSubBasic:

    def test_publish_subscribe_qos0(self, broker_anon):
        msgs = _pub_sub_roundtrip(broker_anon, "test/q0", "hello-q0", qos=0)
        assert len(msgs) >= 1
        assert msgs[0][0] == "test/q0"
        assert msgs[0][1] == b"hello-q0"

    def test_publish_subscribe_qos1(self, broker_anon):
        msgs = _pub_sub_roundtrip(broker_anon, "test/q1", "hello-q1", qos=1)
        assert len(msgs) >= 1
        assert msgs[0][1] == b"hello-q1"

    def test_publish_subscribe_qos2(self, broker_anon):
        msgs = _pub_sub_roundtrip(broker_anon, "test/q2", "hello-q2", qos=2)
        assert len(msgs) >= 1
        assert msgs[0][1] == b"hello-q2"

    def test_multiple_messages(self, broker_anon):
        uid = uuid.uuid4().hex[:8]
        topic = f"test/multi/{uid}"
        collector = MessageCollector()

        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"multi-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(topic, qos=1)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_anon.config.tcp_port, client_id=f"multi-pub-{uid}")
        pub.loop_start()
        time.sleep(0.2)

        for i in range(10):
            pub.publish(topic, f"msg-{i}", qos=1)
        time.sleep(1)

        msgs = collector.wait(count=10, timeout=3)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        payloads = sorted([m[1] for m in msgs])
        assert len(payloads) == 10
        for i in range(10):
            assert f"msg-{i}".encode() in payloads


# -----------------------------------------------------------------------
# Tests — wildcards
# -----------------------------------------------------------------------

class TestWildcards:

    def test_single_level_wildcard(self, broker_anon):
        uid = uuid.uuid4().hex[:8]
        collector = MessageCollector()

        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"wc1-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(f"test/+/{uid}", qos=0)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_anon.config.tcp_port, client_id=f"wc1-pub-{uid}")
        pub.loop_start()
        time.sleep(0.2)

        # Should match
        pub.publish(f"test/a/{uid}", "matched", qos=0)
        # Should NOT match (different prefix depth)
        pub.publish(f"test/a/b/{uid}", "not-matched", qos=0)

        time.sleep(0.5)
        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) == 1
        assert msgs[0][1] == b"matched"

    def test_multi_level_wildcard(self, broker_anon):
        uid = uuid.uuid4().hex[:8]
        collector = MessageCollector()

        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"wc-hash-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(f"test/{uid}/#", qos=0)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_anon.config.tcp_port, client_id=f"wc-hash-pub-{uid}")
        pub.loop_start()
        time.sleep(0.2)

        pub.publish(f"test/{uid}/a", "hash-a", qos=0)
        pub.publish(f"test/{uid}/a/b/c", "hash-deep", qos=0)
        pub.publish(f"test/{uid}", "hash-root", qos=0)

        time.sleep(0.5)
        msgs = collector.wait(count=3, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        topics = [m[0] for m in msgs]
        assert f"test/{uid}/a" in topics
        assert f"test/{uid}/a/b/c" in topics
        assert f"test/{uid}" in topics

    def test_exact_topic_not_matching_wildcard(self, broker_anon):
        uid = uuid.uuid4().hex[:8]
        collector = MessageCollector()

        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"exact-sub-{uid}",
            on_message=collector.on_message,
        )
        sub.subscribe(f"exact/{uid}", qos=0)
        sub.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_anon.config.tcp_port, client_id=f"exact-pub-{uid}")
        pub.loop_start()
        time.sleep(0.2)

        pub.publish(f"exact/{uid}", "yes", qos=0)
        pub.publish(f"exact/{uid}/child", "no", qos=0)
        pub.publish(f"exact/{uid}extra", "no2", qos=0)

        time.sleep(0.5)
        msgs = collector.wait(count=1, timeout=2)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) == 1
        assert msgs[0][1] == b"yes"


# -----------------------------------------------------------------------
# Tests — no-local / multi-subscriber
# -----------------------------------------------------------------------

class TestMultiSubscriber:

    def test_two_subscribers_receive(self, broker_anon):
        uid = uuid.uuid4().hex[:8]
        topic = f"test/multi-sub/{uid}"
        c1_msgs = MessageCollector()
        c2_msgs = MessageCollector()

        s1 = make_client(
            broker_anon.config.tcp_port,
            client_id=f"s1-{uid}",
            on_message=c1_msgs.on_message,
        )
        s1.subscribe(topic, qos=0)
        s1.loop_start()

        s2 = make_client(
            broker_anon.config.tcp_port,
            client_id=f"s2-{uid}",
            on_message=c2_msgs.on_message,
        )
        s2.subscribe(topic, qos=0)
        s2.loop_start()
        time.sleep(0.3)

        pub = make_client(broker_anon.config.tcp_port, client_id=f"mpub-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "broadcast", qos=0)
        time.sleep(0.5)

        m1 = c1_msgs.wait(count=1, timeout=2)
        m2 = c2_msgs.wait(count=1, timeout=2)
        pub.disconnect()
        s1.disconnect()
        s2.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        s1.loop_stop()
        s2.loop_stop()

        assert len(m1) >= 1
        assert len(m2) >= 1
        assert m1[0][1] == b"broadcast"
        assert m2[0][1] == b"broadcast"

    def test_unsubscribed_client_receives_nothing(self, broker_anon):
        uid = uuid.uuid4().hex[:8]
        topic = f"test/unsub/{uid}"
        collector = MessageCollector()

        sub = make_client(
            broker_anon.config.tcp_port,
            client_id=f"unsub-{uid}",
            on_message=collector.on_message,
        )
        sub.loop_start()
        time.sleep(0.2)
        # Subscribe then immediately unsubscribe
        sub.subscribe(topic, qos=0)
        time.sleep(0.2)
        sub.unsubscribe(topic)
        time.sleep(0.3)

        pub = make_client(broker_anon.config.tcp_port, client_id=f"unsub-pub-{uid}")
        pub.loop_start()
        time.sleep(0.2)
        pub.publish(topic, "gone", qos=0)
        time.sleep(0.5)

        msgs = collector.wait(count=1, timeout=1)
        pub.disconnect()
        sub.disconnect()
        time.sleep(0.3)
        pub.loop_stop()
        sub.loop_stop()

        assert len(msgs) == 0


# -----------------------------------------------------------------------
# Tests — large payloads
# -----------------------------------------------------------------------

class TestLargePayload:

    def test_1kb_payload(self, broker_anon):
        payload = "X" * 1024
        msgs = _pub_sub_roundtrip(broker_anon, "test/large-1k", payload, qos=1)
        assert len(msgs) >= 1
        assert msgs[0][1] == payload.encode()

    def test_64kb_payload(self, broker_anon):
        payload = "Y" * 65536
        msgs = _pub_sub_roundtrip(broker_anon, "test/large-64k", payload, qos=0)
        assert len(msgs) >= 1
        assert len(msgs[0][1]) == 65536
