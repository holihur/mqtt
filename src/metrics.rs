//! Minimal Prometheus metrics registry (text exposition format).
//! Mirrors the metric names used by the Go broker (`internal/broker/broker.go`):
//! `mqtt_messages_received_total`, `mqtt_messages_sent_total`,
//! `mqtt_clients_connected`, `mqtt_inflight_messages`, `mqtt_auth_failed_total`,
//! `mqtt_packet_dropped_total{reason}`, `mqtt_redis_latency_seconds`,
//! `mqtt_retain_quota_exceeded_total{reason}`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

#[derive(Default)]
pub struct Counter {
    v: AtomicI64,
}

impl Counter {
    pub fn inc(&self) {
        self.v.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: i64) {
        self.v.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> i64 {
        self.v.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct Gauge {
    v: AtomicI64,
}

impl Gauge {
    pub fn set(&self, v: i64) {
        self.v.store(v, Ordering::Relaxed);
    }
    pub fn get(&self) -> i64 {
        self.v.load(Ordering::Relaxed)
    }
}

pub struct CounterVec {
    name: &'static str,
    help: &'static str,
    label: &'static str,
    children: Mutex<HashMap<String, std::sync::Arc<Counter>>>,
}

impl CounterVec {
    fn new(name: &'static str, help: &'static str, label: &'static str) -> Self {
        Self { name, help, label, children: Mutex::new(HashMap::new()) }
    }

    pub fn with_label(&self, value: &str) -> std::sync::Arc<Counter> {
        let mut g = self.children.lock().unwrap();
        g.entry(value.to_string()).or_insert_with(|| std::sync::Arc::new(Counter::default())).clone()
    }

    pub fn inc(&self, label_value: &str) {
        self.with_label(label_value).inc();
    }

    fn render(&self, out: &mut String) {
        out.push_str(&format!("# HELP {} {}\n", self.name, self.help));
        out.push_str(&format!("# TYPE {} counter\n", self.name));
        let g = self.children.lock().unwrap();
        let mut keys: Vec<&String> = g.keys().collect();
        keys.sort();
        for k in keys {
            let c = g.get(k).unwrap();
            out.push_str(&format!("{}{{{}=\"{}\"}} {}\n", self.name, self.label, escape_label(k), c.get()));
        }
    }
}

const DEF_BUCKETS: [f64; 11] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

pub struct Histogram {
    name: &'static str,
    help: &'static str,
    counts: [AtomicU64; 11],
    sum: AtomicI64, // store as micro-units? store nanos as i64
    total: AtomicU64,
}

impl Histogram {
    fn new(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            counts: Default::default(),
            sum: AtomicI64::new(0),
            total: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, v: f64) {
        for (i, b) in DEF_BUCKETS.iter().enumerate() {
            if v <= *b {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.sum.fetch_add((v * 1e9) as i64, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String) {
        out.push_str(&format!("# HELP {} {}\n", self.name, self.help));
        out.push_str(&format!("# TYPE {} histogram\n", self.name));
        let mut cumulative = 0u64;
        for (i, b) in DEF_BUCKETS.iter().enumerate() {
            cumulative += self.counts[i].load(Ordering::Relaxed);
            out.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", self.name, b, cumulative));
        }
        out.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", self.name, self.total.load(Ordering::Relaxed)));
        let sum_nanos = self.sum.load(Ordering::Relaxed);
        out.push_str(&format!("{}_sum {}\n", self.name, sum_nanos as f64 / 1e9));
        out.push_str(&format!("{}_count {}\n", self.name, self.total.load(Ordering::Relaxed)));
    }
}

pub struct Registry {
    pub messages_received: Counter,
    pub messages_sent: Counter,
    pub clients_connected: Gauge,
    pub inflight: Gauge,
    pub auth_failed: Counter,
    pub packet_dropped: CounterVec,
    pub redis_latency: Histogram,
    pub retain_quota_exceeded: CounterVec,
}

fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

impl Registry {
    fn new() -> Self {
        Self {
            messages_received: Counter::default(),
            messages_sent: Counter::default(),
            clients_connected: Gauge::default(),
            inflight: Gauge::default(),
            auth_failed: Counter::default(),
            packet_dropped: CounterVec::new(
                "mqtt_packet_dropped_total",
                "Total dropped packets",
                "reason",
            ),
            redis_latency: Histogram::new("mqtt_redis_latency_seconds", "Redis operation latency"),
            retain_quota_exceeded: CounterVec::new(
                "mqtt_retain_quota_exceeded_total",
                "Total retain quota exceeded",
                "reason",
            ),
        }
    }

    pub fn global() -> &'static Registry {
        static REG: OnceLock<Registry> = OnceLock::new();
        REG.get_or_init(Registry::new)
    }

    /// Render Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP mqtt_messages_received_total Total MQTT messages received\n");
        out.push_str("# TYPE mqtt_messages_received_total counter\n");
        out.push_str(&format!("mqtt_messages_received_total {}\n", self.messages_received.get()));
        out.push_str("# HELP mqtt_messages_sent_total Total MQTT messages sent\n");
        out.push_str("# TYPE mqtt_messages_sent_total counter\n");
        out.push_str(&format!("mqtt_messages_sent_total {}\n", self.messages_sent.get()));
        out.push_str("# HELP mqtt_clients_connected Current connected clients\n");
        out.push_str("# TYPE mqtt_clients_connected gauge\n");
        out.push_str(&format!("mqtt_clients_connected {}\n", self.clients_connected.get()));
        out.push_str("# HELP mqtt_inflight_messages Current inflight messages\n");
        out.push_str("# TYPE mqtt_inflight_messages gauge\n");
        out.push_str(&format!("mqtt_inflight_messages {}\n", self.inflight.get()));
        out.push_str("# HELP mqtt_auth_failed_total Total auth failures\n");
        out.push_str("# TYPE mqtt_auth_failed_total counter\n");
        out.push_str(&format!("mqtt_auth_failed_total {}\n", self.auth_failed.get()));
        self.packet_dropped.render(&mut out);
        self.redis_latency.render(&mut out);
        self.retain_quota_exceeded.render(&mut out);
        out
    }
}

/// Convenience helpers matching the Go global metric variables.
pub fn metrics() -> &'static Registry {
    Registry::global()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_format() {
        let r = Registry::new();
        r.messages_received.inc();
        r.packet_dropped.inc("auth");
        let s = r.render();
        assert!(s.contains("mqtt_messages_received_total 1"));
        assert!(s.contains("mqtt_packet_dropped_total{reason=\"auth\"} 1"));
        assert!(s.contains("# TYPE mqtt_redis_latency_seconds histogram"));
    }
}
