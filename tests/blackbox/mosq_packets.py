"""
Raw MQTT packet builders + socket helpers, ported from mosquitto's
test/mqtt_packets.py and test/mosq_test.py (simplified).

Used to exercise wire-level protocol behavior that paho-mqtt cannot reach
(malformed packets, exact CONNACK codes, retain flag on delivery, etc.).
"""
from __future__ import annotations

import socket
import struct
import time

# Packed type constants (mosquitto test suite uses these names)
CONNACK = 0x20
CONNECT = 0x10
DISCONNECT = 0xE0
PUBACK = 0x40
PUBREC = 0x50
PUBREL = 0x62
PUBCOMP = 0x70
SUBSCRIBE = 0x82
SUBACK = 0x90
UNSUBSCRIBE = 0xA2
UNSUBACK = 0xB0
PINGREQ = 0xC0
PINGRESP = 0xD0
CMD_PUBLISH = 0x30


def gen_varint(n: int) -> bytes:
    b = b""
    while True:
        byte = n % 128
        n //= 128
        if n > 0:
            byte |= 0x80
        b += struct.pack("!B", byte)
        if n == 0:
            return b


def _pack_bytes(data: bytes) -> bytes:
    return struct.pack("!H", len(data)) + data


def gen_string(s: str) -> bytes:
    return _pack_bytes(s.encode("utf8"))


def gen_connect(
    client_id: str,
    clean_session: bool = True,
    keepalive: int = 60,
    username=None,
    password=None,
    will_topic=None,
    will_qos: int = 0,
    will_retain: bool = False,
    will_payload: bytes = b"",
    proto_ver: int = 4,
) -> bytes:
    connect_flags = 0
    if clean_session:
        connect_flags |= 0x02
    if will_topic is not None:
        connect_flags |= 0x04 | (will_qos << 3)
        if will_retain:
            connect_flags |= 0x20
    if username is not None:
        connect_flags |= 0x80
    if password is not None:
        connect_flags |= 0x40

    # variable header (protocol name/level first, per spec)
    if proto_ver == 5:
        vh = gen_string("MQTT") + struct.pack("!B", 5)
    elif proto_ver == 3:
        vh = gen_string("MQIsdp") + struct.pack("!B", 3)
    else:
        vh = gen_string("MQTT") + struct.pack("!B", 4)
    vh += struct.pack("!B", connect_flags) + struct.pack("!H", keepalive)
    if proto_ver == 5:
        vh += b"\x00"  # empty property block

    payload = b""
    if will_topic is not None:
        payload += gen_string(will_topic) + _pack_bytes(will_payload)
    if username is not None:
        payload += gen_string(username)
    if password is not None:
        payload += _pack_bytes(password.encode() if isinstance(password, str) else password)
    full_vh = vh + gen_string(client_id) + payload

    return struct.pack("!B", CONNECT) + gen_varint(len(full_vh)) + full_vh


def gen_publish(topic, qos, payload=None, retain=False, dup=False, mid=0, proto_ver=4, properties=b""):
    if payload is None:
        payload = b"message"
    if isinstance(payload, str):
        payload = payload.encode("utf8")
    cmd = CMD_PUBLISH | (qos << 1)
    if dup:
        cmd |= 0x08
    if retain:
        cmd |= 0x01
    vh = gen_string(topic)
    if qos > 0:
        vh += struct.pack("!H", mid)
    if proto_ver == 5:
        vh += properties if properties else b"\x00"
    return struct.pack("!B", cmd) + gen_varint(len(vh) + len(payload)) + vh + payload


def gen_puback(mid: int, proto_ver: int = 4, reason_code: int = -1) -> bytes:
    if proto_ver == 5 and reason_code >= 0:
        return struct.pack("!BHBB", PUBACK, 3, mid, reason_code) + b"\x00"
    return struct.pack("!BHB", PUBACK, 2, mid)


def gen_pubrec(mid: int, proto_ver: int = 4, reason_code: int = -1) -> bytes:
    if proto_ver == 5 and reason_code >= 0:
        return struct.pack("!BHBB", PUBREC, 3, mid, reason_code) + b"\x00"
    return struct.pack("!BHB", PUBREC, 2, mid)


def gen_pubrel(mid: int, dup=False, proto_ver: int = 4, reason_code: int = -1) -> bytes:
    cmd = 0x60 | 0x02
    if dup:
        cmd |= 0x08
    if proto_ver == 5 and reason_code >= 0:
        return struct.pack("!BHBB", cmd, 3, mid, reason_code) + b"\x00"
    return struct.pack("!BHB", cmd, 2, mid)


def gen_pubcomp(mid: int, proto_ver: int = 4, reason_code: int = -1) -> bytes:
    if proto_ver == 5 and reason_code >= 0:
        return struct.pack("!BHBB", PUBCOMP, 3, mid, reason_code) + b"\x00"
    return struct.pack("!BHB", PUBCOMP, 2, mid)


def gen_subscribe(mids, topics, proto_ver: int = 4, cmd: int = SUBSCRIBE) -> bytes:
    if isinstance(mids, int):
        mids = [mids]
    if isinstance(topics, tuple):
        topics = [topics]
    payload = b""
    for (topic, qos) in topics:
        payload += gen_string(topic) + struct.pack("!B", qos)
    mid = mids[0]
    if proto_ver == 5:
        payload = b"\x00" + payload
    rl = 2 + len(payload)
    return struct.pack("!BBH", cmd, rl, mid) + payload


def gen_unsubscribe(mid: int, topics, proto_ver: int = 4) -> bytes:
    if isinstance(topics, str):
        topics = [topics]
    payload = b""
    for t in topics:
        payload += gen_string(t)
    if proto_ver == 5:
        payload = b"\x00" + payload
    rl = 2 + len(payload)
    return struct.pack("!BBH", UNSUBSCRIBE, rl, mid) + payload


def gen_disconnect(reason_code: int = 0, proto_ver: int = 4) -> bytes:
    if proto_ver == 5:
        return struct.pack("!BBB", DISCONNECT, 2, reason_code) + b"\x00"
    return struct.pack("!BB", DISCONNECT, 0)


def gen_pingreq() -> bytes:
    return struct.pack("!BB", PINGREQ, 0)


# ---------------------------------------------------------------------------
# Wire reading helpers
# ---------------------------------------------------------------------------

def read_packet(sock: socket.socket, timeout: float = 5.0):
    """Read a single MQTT packet from *sock*.

    Returns (fixed_header_byte, flags, remaining_bytes).
    Raises TimeoutError if no data arrives within *timeout*.
    Returns None if the peer closed the connection cleanly.
    """
    sock.settimeout(timeout)
    try:
        first = sock.recv(1)
    except socket.timeout:
        raise TimeoutError("no packet within %ss" % timeout)
    if first == b"":
        return None
    first = first[0]
    # remaining length varint
    rl = 0
    mult = 1
    while True:
        chunk = sock.recv(1)
        if chunk == b"":
            return None
        byte = chunk[0]
        rl += (byte & 0x7F) * mult
        mult *= 128
        if not (byte & 0x80):
            break
    body = b""
    while len(body) < rl:
        chunk = sock.recv(rl - len(body))
        if chunk == b"":
            return None
        body += chunk
    return first, first & 0x0F, body


def expect_packet(sock, name, first_byte, timeout: float = 5.0):
    """Read one packet and assert its type."""
    pkt = read_packet(sock, timeout)
    assert pkt is not None, f"{name}: connection closed, expected packet type 0x{first_byte:02x}"
    got = pkt[0]
    assert (got & 0xF0) == (first_byte & 0xF0), (
        f"{name}: expected packet 0x{first_byte:02x}, got 0x{got:02x} body={pkt[2][:64]!r}"
    )
    return pkt


def read_until(sock, want, timeout: float = 5.0):
    """Read packets until every predicate in *want* has been satisfied.

    *want* is a list of callables taking (first_byte, flags, body) -> bool.
    Returns list of matched packets in arrival order. Tolerates retained
    PUBLISH arriving before SUBACK (ordering deviation of this broker).
    """
    pending = list(want)
    got = []
    deadline = time.monotonic() + timeout
    while pending:
        pkt = read_packet(sock, timeout=max(0.1, deadline - time.monotonic()))
        assert pkt is not None, "connection closed while waiting for packets"
        for w in list(pending):
            if w(*pkt):
                pending.remove(w)
                got.append(pkt)
                break
        else:
            raise AssertionError(f"unexpected packet 0x{pkt[0]:02x} body={pkt[2][:64]!r}")
        if time.monotonic() > deadline:
            break
    return got


def is_packet_type(ptype):
    return lambda first, flags, body: (first & 0xF0) == (ptype & 0xF0)


def expect_closed(sock, timeout: float = 5.0):
    """Assert that the broker closes the connection (optionally after a DISCONNECT)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            pkt = read_packet(sock, timeout=1.0)
        except (TimeoutError, socket.timeout):
            raise AssertionError("connection stayed open (recv timeout), expected close")
        except OSError:
            return  # reset = closed
        if pkt is None:
            return  # clean close
        # A DISCONNECT packet before close is allowed (MQTT5)
        if pkt[0] == DISCONNECT:
            # drain until close
            continue
        raise AssertionError(f"expected close, got packet 0x{pkt[0]:02x}")
    raise AssertionError("connection not closed within timeout")


class RawClient:
    """Minimal raw-socket MQTT client (mosquitto mosq_test.do_client_connect style)."""

    def __init__(self, port: int, host: str = "127.0.0.1", timeout: float = 5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)

    def send(self, data: bytes):
        self.sock.sendall(data)

    def connect_and_expect_connack(
        self,
        connect_packet: bytes,
        expect_rc=None,
        timeout: float = 5.0,
    ):
        """Send CONNECT, read CONNACK. If expect_rc is not None, assert reason code.

        Returns the CONNACK remaining body bytes.
        If expect_rc is None, only checks that a CONNACK arrived (or closes).
        """
        self.send(connect_packet)
        pkt = read_packet(self.sock, timeout)
        assert pkt is not None, "connection closed before CONNACK"
        assert pkt[0] == CONNACK, f"expected CONNACK, got 0x{pkt[0]:02x} body={pkt[2][:32]!r}"
        body = pkt[2]
        rc = body[1] if len(body) >= 2 else -1
        if expect_rc is not None:
            assert rc == expect_rc, f"CONNACK rc={rc}, expected {expect_rc}"
        return body

    def read(self, timeout: float = 5.0):
        return read_packet(self.sock, timeout)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass
