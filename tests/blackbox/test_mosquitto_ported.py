"""
Protocol conformance tests ported from eclipse/mosquitto test/broker.

Source tests (raw-socket protocol level, broker-agnostic):
  - 01-bad-initial-packets.py
  - 01-connect-zero-length-id.py
  - 02-subhier-crash.py
  - 03-pattern-matching.py
  - 03-publish-bad-flags.py
  - 04-retain-clear-multiple.py
  - 04-retain-qos0-repeated.py
  - 05-clean-session-qos1.py

These use raw sockets (not paho) to reach wire-level behavior:
malformed packets, exact CONNACK reason codes, retain flag on delivery.
"""

import struct
import sys
import os

import pytest

sys.path.insert(0, os.path.dirname(__file__))
import mosq_packets as mp
from conftest import BrokerConfig, BrokerProcess, _free_port

BROKER = None  # lazily-started shared broker for stateless tests


def start_broker(**kwargs):
    cfg = BrokerConfig(tcp_port=_free_port(), allow_anonymous=True, **kwargs)
    return BrokerProcess(cfg).start()


@pytest.fixture(scope="module")
def broker():
    """Shared broker for tests that do not need a fresh instance."""
    bp = start_broker()
    yield bp
    bp.stop()


@pytest.fixture()
def broker_fresh():
    """Fresh broker per test (for session-state tests)."""
    bp = start_broker()
    yield bp
    bp.stop()


def raw_connect(port, client_id="mosq-port-test", proto_ver=4, **kwargs):
    c = mp.RawClient(port)
    c.connect_and_expect_connack(
        mp.gen_connect(client_id, proto_ver=proto_ver, **kwargs), expect_rc=0
    )
    return c


# ---------------------------------------------------------------------------
# Ported: 01-bad-initial-packets.py
# ---------------------------------------------------------------------------

BAD_INITIAL_PACKETS = [
    b"\x20\x02\x00\x00",  # CONNACK
    b"\x30\x0a\x00\x04testhello",  # PUBLISH
    b"\x40\x02\x00\x01",  # PUBACK
    b"\x50\x02\x00\x01",  # PUBREC
    b"\x62\x02\x00\x01",  # PUBREL
    b"\x70\x02\x00\x01",  # PUBCOMP
    b"\x82\x08\x00\x01\x00\x04test\x00",  # SUBSCRIBE
    b"\x90\x03\x00\x01\x00",  # SUBACK
    b"\xA2\x07\x00\x01\x00\x04test",  # UNSUBSCRIBE
    b"\xB0\x02\x00\x01",  # UNSUBACK
    b"\xC0\x00",  # PINGREQ
    b"\xD0\x00",  # PINGRESP
    b"\xE0\x00",  # DISCONNECT
    b"\xF0\x00",  # AUTH
]


@pytest.mark.parametrize("payload", BAD_INITIAL_PACKETS, ids=lambda p: f"0x{p[0]:02x}")
class TestBadInitialPackets:
    """A non-CONNECT packet sent before CONNECT must cause a disconnect."""

    def test_bad_initial_packet(self, broker, payload):
        c = mp.RawClient(broker.config.tcp_port, timeout=2)
        c.send(payload)
        mp.expect_closed(c.sock, timeout=3)
        c.close()


# ---------------------------------------------------------------------------
# Ported: 01-connect-zero-length-id.py (v3.1.1 / v5 basics)
# ---------------------------------------------------------------------------


class TestConnectZeroLengthId:

    def test_v311_empty_id_clean_session_accepted(self, broker):
        c = mp.RawClient(broker.config.tcp_port)
        c.connect_and_expect_connack(
            mp.gen_connect("", clean_session=True, proto_ver=4), expect_rc=0
        )
        c.close()

    def test_v311_empty_id_persistent_rejected(self, broker):
        """mosquitto rejects empty client id with clean_session=False (rc=2)."""
        c = mp.RawClient(broker.config.tcp_port)
        c.connect_and_expect_connack(
            mp.gen_connect("", clean_session=False, proto_ver=4), expect_rc=2
        )
        c.close()

    def test_v5_empty_id_accepted(self, broker):
        c = mp.RawClient(broker.config.tcp_port)
        c.connect_and_expect_connack(
            mp.gen_connect("", clean_session=True, proto_ver=5), expect_rc=0
        )
        c.close()


# ---------------------------------------------------------------------------
# Ported: 02-subhier-crash.py
# ---------------------------------------------------------------------------


class TestSubhierCrash:
    """Overlapping subscribe/unsubscribe on adjacent hierarchy levels
    must not crash the broker (mosquitto issue #505)."""

    def _one_pass(self, broker):
        c = raw_connect(broker.config.tcp_port, "subhier-crash")
        c.send(mp.gen_subscribe(1, [("topic/a", 0)]))
        mp.expect_packet(c.sock, "suback 1", mp.SUBACK)
        c.send(mp.gen_subscribe(2, [("topic/b", 0)]))
        mp.expect_packet(c.sock, "suback 2", mp.SUBACK)
        c.send(mp.gen_unsubscribe(3, "topic/a"))
        mp.expect_packet(c.sock, "unsuback", mp.UNSUBACK)
        c.send(mp.gen_disconnect())
        c.close()

    def test_broker_survives(self, broker):
        self._one_pass(broker)
        self._one_pass(broker)  # repeat: broker still there


# ---------------------------------------------------------------------------
# Ported: 03-pattern-matching.py
# ---------------------------------------------------------------------------

PATTERN_CASES = [
    ("#", "test/topic"),
    ("#", "/test/topic"),
    ("foo/#", "foo/bar/baz"),
    ("foo/+/baz", "foo/bar/baz"),
    ("foo/+/baz/#", "foo/bar/baz"),
    ("foo/+/baz/#", "foo/bar/baz/bar"),
    ("foo/foo/baz/#", "foo/foo/baz/bar"),
    ("foo/#", "foo"),
    ("foo/#", "foo/"),
    ("/#", "/foo"),
    ("test/topic/", "test/topic/"),
    ("test/topic/+", "test/topic/"),
    ("+/+/+/+/+/+/+/+/+/+/test", "one/two/three/four/five/six/seven/eight/nine/ten/test"),
    ("#", "test////a//topic"),
    ("#", "/test////a//topic"),
    ("foo/#", "foo//bar///baz"),
    ("foo/+/baz", "foo//baz"),
    ("foo/+/baz//", "foo//baz//"),
    ("foo/+/baz/#", "foo//baz"),
    ("foo/+/baz/#", "foo//baz/bar"),
    ("foo//baz/#", "foo//baz/bar"),
    ("/#", "////foo///bar"),
]


class TestPatternMatching:

    @pytest.mark.parametrize("sub_topic,pub_topic", PATTERN_CASES)
    def test_pattern(self, broker_fresh, sub_topic, pub_topic):
        port = broker_fresh.config.tcp_port
        c = raw_connect(port, "pattern-sub-test")
        # NOTE: this broker sends retained PUBLISH before SUBACK (mosquitto
        # sends SUBACK first), so we match packets out of order.
        c.send(mp.gen_subscribe(312, [(sub_topic, 0)]))
        mp.read_until(c.sock, [mp.is_packet_type(mp.SUBACK)])

        # publish retained from the same client (not shared subscriptions)
        c.send(mp.gen_publish(pub_topic, qos=0, retain=True, payload="message"))
        pkt = mp.expect_packet(c.sock, "publish", mp.CMD_PUBLISH)
        self._assert_publish(pkt, pub_topic, b"message", retain=False)

        # unsub + resub → retained copy comes back with retain flag set
        c.send(mp.gen_unsubscribe(234, sub_topic))
        mp.expect_packet(c.sock, "unsuback", mp.UNSUBACK)
        c.send(mp.gen_subscribe(313, [(sub_topic, 0)]))
        pkts = mp.read_until(c.sock, [mp.is_packet_type(mp.SUBACK),
                                      mp.is_packet_type(mp.CMD_PUBLISH)])
        pkt = [p for p in pkts if (p[0] & 0xF0) == 0x30][0]
        self._assert_publish(pkt, pub_topic, b"message", retain=True)

        # clear retain
        c.send(mp.gen_publish(pub_topic, qos=0, retain=True, payload=b""))
        c.close()

    @staticmethod
    def _assert_publish(pkt, topic, payload, retain):
        _, flags, body = pkt
        assert (flags & 0x01) == (0x01 if retain else 0), (
            f"retain flag wrong on delivery: {flags:04b}"
        )
        tlen = struct.unpack("!H", body[:2])[0]
        got_topic = body[2:2 + tlen].decode()
        assert got_topic == topic, f"delivered topic {got_topic!r} != {topic!r}"
        got_payload = body[2 + tlen:]
        assert got_payload == payload


# ---------------------------------------------------------------------------
# Ported: 03-publish-bad-flags.py (v3.1.1: qos=3 must disconnect)
# ---------------------------------------------------------------------------


class TestPublishBadFlags:

    def test_publish_qos3_disconnects(self, broker):
        """PUBLISH with QoS 3 (0x06 flag bits) is a protocol violation."""
        c = raw_connect(broker.config.tcp_port, "bad-flags")
        c.send(mp.gen_publish("test/topic", qos=3, mid=1, payload="a"))
        mp.expect_closed(c.sock, timeout=3)
        c.close()

    def test_publish_dup_qos0_disconnects(self, broker):
        """DUP=1 with QoS 0 is a protocol violation (MQTT-3.3.1-2)."""
        c = raw_connect(broker.config.tcp_port, "bad-flags-dup")
        c.send(mp.gen_publish("test/topic", qos=0, dup=True, mid=1, payload="a"))
        mp.expect_closed(c.sock, timeout=3)
        c.close()


# ---------------------------------------------------------------------------
# Ported: 04-retain-clear-multiple.py
# ---------------------------------------------------------------------------


class TestRetainClearMultiple:

    def _send_retain(self, port, topic, payload):
        c = raw_connect(port, "retain-clear-test")
        if payload is None:
            c.send(mp.gen_publish(topic, qos=1, mid=1, payload=b"", retain=True))
        else:
            c.send(mp.gen_publish(topic, qos=1, mid=1, payload=payload, retain=True))
        mp.expect_packet(c.sock, f"puback {topic}", mp.PUBACK)
        c.close()

    def _collect_retained(self, port, sub_pattern):
        c = raw_connect(port, "retain-collector")
        c.send(mp.gen_subscribe(1, [(sub_pattern, 0)]))
        got = []
        seen_suback = False
        while True:
            try:
                pkt = mp.read_packet(c.sock, timeout=1.0)
            except TimeoutError:
                break
            if pkt is None:
                break
            first, flags, body = pkt
            if (first & 0xF0) == 0x90:
                seen_suback = True
                continue
            if (first & 0xF0) == 0x30:
                tlen = struct.unpack("!H", body[:2])[0]
                got.append((body[2:2 + tlen].decode(), body[2 + tlen:], bool(flags & 0x01)))
            elif (first & 0xF0) == 0xE0:
                break
        assert seen_suback, "no SUBACK received"
        c.close()
        return got

    def test_clear_multiple_levels(self, broker_fresh):
        port = broker_fresh.config.tcp_port
        self._send_retain(port, "1/2/3/4/5/6/7", "retained message")
        self._send_retain(port, "1/2/3/4", "retained message")
        self._send_retain(port, "1", "retained message")

        got = self._collect_retained(port, "#")
        topics = {t for t, _, _ in got}
        assert topics == {"1/2/3/4/5/6/7", "1/2/3/4", "1"}
        # all delivered with retain flag set
        assert all(r for _, _, r in got)

        self._send_retain(port, "1/2/3/4", None)
        got = self._collect_retained(port, "#")
        assert {t for t, _, _ in got} == {"1/2/3/4/5/6/7", "1"}

        self._send_retain(port, "1/2/3/4/5/6/7", None)
        got = self._collect_retained(port, "#")
        assert {t for t, _, _ in got} == {"1"}

        self._send_retain(port, "1", None)
        got = self._collect_retained(port, "#")
        assert got == []


# ---------------------------------------------------------------------------
# Ported: 04-retain-qos0-repeated.py
# ---------------------------------------------------------------------------


class TestRetainQos0Repeated:

    @pytest.mark.parametrize("proto_ver", [4, 5])
    def test_retain_survives_unsub_resub(self, broker, proto_ver):
        port = broker.config.tcp_port
        c = mp.RawClient(port)
        c.connect_and_expect_connack(
            mp.gen_connect("retain-qos0-rep-test", proto_ver=proto_ver), expect_rc=0
        )
        c.send(mp.gen_publish("retain/qos0/repeated", qos=0,
                              payload="retained message", retain=True,
                              proto_ver=proto_ver))
        c.send(mp.gen_subscribe(16, [("retain/qos0/repeated", 0)], proto_ver=proto_ver))
        pkts = mp.read_until(c.sock, [mp.is_packet_type(mp.SUBACK),
                                      mp.is_packet_type(mp.CMD_PUBLISH)])
        pkt = [p for p in pkts if (p[0] & 0xF0) == 0x30][0]
        _, flags, body = pkt
        assert flags & 0x01, "retained message must have retain flag set on delivery"

        c.send(mp.gen_unsubscribe(13, "retain/qos0/repeated", proto_ver=proto_ver))
        mp.expect_packet(c.sock, "unsuback", mp.UNSUBACK)
        c.send(mp.gen_subscribe(16, [("retain/qos0/repeated", 0)], proto_ver=proto_ver))
        pkts = mp.read_until(c.sock, [mp.is_packet_type(mp.SUBACK),
                                      mp.is_packet_type(mp.CMD_PUBLISH)])
        pkt = [p for p in pkts if (p[0] & 0xF0) == 0x30][0]
        _, flags, body = pkt
        assert flags & 0x01
        c.close()


# ---------------------------------------------------------------------------
# Ported: 05-clean-session-qos1.py
# ---------------------------------------------------------------------------


class TestCleanSessionQos1:

    @pytest.mark.parametrize("proto_ver", [4, 5])
    def test_persistent_session_receives_qos1_offline_message(self, broker_fresh, proto_ver):
        port = broker_fresh.config.tcp_port
        cid = f"05-clean-session-{proto_ver}"

        # persistent session, subscribe qos1, disconnect
        c = mp.RawClient(port)
        body = c.connect_and_expect_connack(
            mp.gen_connect(cid, clean_session=False, proto_ver=proto_ver),
            expect_rc=0,
        )
        if proto_ver == 4:
            assert body[0] == 0, f"first connect must be session-present=0, got {body[0]}"
        c.send(mp.gen_subscribe(109, [("qos1/05-clean_session/test", 1)], proto_ver=proto_ver))
        mp.expect_packet(c.sock, "suback", mp.SUBACK)
        c.send(mp.gen_disconnect())
        c.close()

        # helper publishes qos1 message while client is offline
        h = raw_connect(port, "05-clean-qos1-test-helper")
        h.send(mp.gen_publish("qos1/05-clean_session/test", qos=1,
                              mid=128, payload="clean-session-message",
                              proto_ver=proto_ver))
        mp.expect_packet(h.sock, "puback", mp.PUBACK)
        h.close()

        # reconnect (persistent): expect session-present=1 and the queued publish
        c = mp.RawClient(port)
        body = c.connect_and_expect_connack(
            mp.gen_connect(cid, clean_session=False, proto_ver=proto_ver),
            expect_rc=0,
        )
        if proto_ver == 4:
            assert body[0] == 1, f"session-present must be 1, got {body[0]}"
        pkt = mp.expect_packet(c.sock, "publish", mp.CMD_PUBLISH)
        _, flags, bodyp = pkt
        qos = (flags >> 1) & 0x03
        assert qos == 1
        tlen = struct.unpack("!H", bodyp[:2])[0]
        assert bodyp[2:2 + tlen].decode() == "qos1/05-clean_session/test"
        mid = struct.unpack("!H", bodyp[2 + tlen:4 + tlen])[0]
        c.send(mp.gen_puback(mid, proto_ver=proto_ver))
        c.close()

        # clean the session
        c = mp.RawClient(port)
        c.connect_and_expect_connack(
            mp.gen_connect(cid, clean_session=True, proto_ver=proto_ver), expect_rc=0
        )
        c.close()
