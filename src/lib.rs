//! MQTT Broker — Rust port of the Go broker (mqtt-broker).
//!
//! Module layout mirrors the Go `internal/` packages:
//! `codec`, `parser`, `topic`, `session`, `persistence`, `auth`,
//! `cluster`, `transport`, `broker` (core + admin API), `webui`.

pub mod auth;
pub mod broker;
pub mod cluster;
pub mod codec;
pub mod metrics;
pub mod parser;
pub mod persistence;
pub mod session;
pub mod topic;
pub mod transport;
pub mod webui;
