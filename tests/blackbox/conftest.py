"""
Black-box test fixtures for MQTT Broker.

Starts/stops the broker binary with various configurations and provides
pre-configured paho-mqtt clients for each scenario.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import paho.mqtt.client as mqtt
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BROKER_BIN = os.path.join(
    os.path.dirname(__file__), "..", "..", "bin", "broker"
)

def _free_port() -> int:
    """Return a free TCP port on 127.0.0.1."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_port(port: int, timeout: float = 5.0) -> bool:
    """Block until *port* is accepting connections or *timeout* expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def _wait_http(port: int, path: str = "/healthz", timeout: float = 5.0) -> bool:
    """Wait until the HTTP admin/health endpoint responds 200."""
    import urllib.request
    import urllib.error

    deadline = time.monotonic() + timeout
    url = f"http://127.0.0.1:{port}{path}"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.15)
    return False


# ---------------------------------------------------------------------------
# BrokerProcess wrapper
# ---------------------------------------------------------------------------

@dataclass
class BrokerConfig:
    """Declarative broker configuration for a single test run."""
    name: str = "default"
    tcp_port: int = 0
    ws_port: int = 0
    admin_port: int = 0
    allow_anonymous: bool = True
    log_level: str = "warn"
    wal_enabled: bool = False
    node_id: str = ""
    extra_args: list = field(default_factory=list)
    pprof_port: int = 0

    def build_args(self) -> list:
        args = [BROKER_BIN]
        if self.tcp_port:
            args += ["-tcp", f"127.0.0.1:{self.tcp_port}"]
        if self.ws_port:
            args += ["-ws", f"127.0.0.1:{self.ws_port}"]
        if self.admin_port:
            args += ["-admin-api", f"127.0.0.1:{self.admin_port}"]
        if self.pprof_port:
            args += ["-pprof", f"127.0.0.1:{self.pprof_port}"]
        args += ["-allow-anonymous", str(self.allow_anonymous).lower()]
        args += ["-log-level", self.log_level]
        args += ["-redis", ""]
        args += ["-wal", str(self.wal_enabled).lower()]
        args += ["-wal-dir", "-"]
        if self.node_id:
            args += ["-node", self.node_id]
        args.extend(self.extra_args)
        return args


class BrokerProcess:
    """Manages a broker subprocess lifecycle."""

    def __init__(self, config: BrokerConfig):
        self.config = config
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> BrokerProcess:
        args = self.config.build_args()
        self.proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if not _wait_port(self.config.tcp_port, timeout=8):
            stderr = self.proc.stderr.read().decode() if self.proc.stderr else ""
            self.stop()
            raise RuntimeError(
                f"Broker did not start on port {self.config.tcp_port}.\n"
                f"Args: {' '.join(args)}\nstderr: {stderr[:2000]}"
            )
        return self

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=3)
        self.proc = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.stop()


# ---------------------------------------------------------------------------
# Helpers to build paho-mqtt clients
# ---------------------------------------------------------------------------

def make_client(
    tcp_port: int,
    client_id: str = "",
    clean_session: bool = True,
    keepalive: int = 30,
    username: str = None,
    password: str = None,
    protocol: int = mqtt.MQTTv311,
    on_message=None,
    on_connect=None,
    on_disconnect=None,
) -> mqtt.Client:
    """Create a paho-mqtt Client connected to the given TCP port."""
    if not client_id:
        client_id = f"test-{os.getpid()}-{_free_port()}"
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=protocol,
    )
    if username:
        client.username_pw_set(username, password)
    if on_connect:
        client.on_connect = on_connect
    if on_disconnect:
        client.on_disconnect = on_disconnect
    if on_message:
        client.on_message = on_message
    client.connect("127.0.0.1", tcp_port, keepalive=keepalive)
    return client


def make_will_client(
    tcp_port: int,
    will_topic: str,
    will_payload: str,
    will_qos: int = 0,
    will_retain: bool = False,
    client_id: str = "",
    protocol: int = mqtt.MQTTv311,
) -> mqtt.Client:
    """Create a client with a Last Will and Testament."""
    if not client_id:
        client_id = f"will-{os.getpid()}-{_free_port()}"
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=protocol,
    )
    client.will_set(will_topic, will_payload, qos=will_qos, retain=will_retain)
    return client


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# ---- 1) Anonymous broker (default) ----

@pytest.fixture(scope="class")
def broker_anon():
    """Start a broker with allow-anonymous=true, memory-only, no WS."""
    port = _free_port()
    cfg = BrokerConfig(
        name="anon",
        tcp_port=port,
        allow_anonymous=True,
    )
    with BrokerProcess(cfg) as bp:
        yield bp


# ---- 2) Anonymous broker with WebSocket ----

@pytest.fixture(scope="module")
def broker_ws():
    """Start a broker with TCP + WebSocket enabled."""
    tcp_port = _free_port()
    ws_port = _free_port()
    cfg = BrokerConfig(
        name="ws",
        tcp_port=tcp_port,
        ws_port=ws_port,
        allow_anonymous=True,
    )
    with BrokerProcess(cfg) as bp:
        yield bp


# ---- 3) Anonymous + admin API ----

@pytest.fixture(scope="module")
def broker_admin():
    """Start a broker with admin API enabled."""
    tcp_port = _free_port()
    admin_port = _free_port()
    cfg = BrokerConfig(
        name="admin",
        tcp_port=tcp_port,
        admin_port=admin_port,
        allow_anonymous=True,
    )
    with BrokerProcess(cfg) as bp:
        yield bp


# ---- 4) Anonymous + pprof ----

@pytest.fixture(scope="module")
def broker_pprof():
    """Start a broker with pprof enabled."""
    tcp_port = _free_port()
    pprof_port = _free_port()
    cfg = BrokerConfig(
        name="pprof",
        tcp_port=tcp_port,
        pprof_port=pprof_port,
        allow_anonymous=True,
    )
    with BrokerProcess(cfg) as bp:
        yield bp


# ---- 5) Anonymous with WAL enabled ----

@pytest.fixture(scope="module")
def broker_wal(tmp_path_factory):
    """Start a broker with WAL (pebble) enabled."""
    wal_dir = str(tmp_path_factory.mktemp("wal"))
    port = _free_port()
    cfg = BrokerConfig(
        name="wal",
        tcp_port=port,
        wal_enabled=True,
        extra_args=["-wal-dir", wal_dir],
    )
    with BrokerProcess(cfg) as bp:
        yield bp


# ---- 6) Non-anonymous (DenyAll) broker ----

@pytest.fixture(scope="module")
def broker_deny():
    """Broker with allow-anonymous=false — anonymous clients must be rejected."""
    port = _free_port()
    cfg = BrokerConfig(
        name="deny",
        tcp_port=port,
        allow_anonymous=False,
    )
    with BrokerProcess(cfg) as bp:
        yield bp


# ---- Parametrized config fixture ----

BROKER_CONFIGS = [
    BrokerConfig(name="anon", tcp_port=0, allow_anonymous=True),
    BrokerConfig(name="wal", tcp_port=0, allow_anonymous=True, wal_enabled=True),
]


@pytest.fixture(scope="module", params=BROKER_CONFIGS, ids=lambda c: c.name)
def broker_param(request):
    """Parametrized broker fixture — runs each test against multiple configs."""
    cfg = request.param
    cfg.tcp_port = _free_port()
    with BrokerProcess(cfg) as bp:
        yield bp


# ---------------------------------------------------------------------------
# Re-export helper so tests can do: from conftest import make_client
# ---------------------------------------------------------------------------
