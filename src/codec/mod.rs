//! Packet codec for MQTT 3.1 / 3.1.1 / 5.0.

mod packet;
mod properties;
mod varint;

pub use packet::*;
pub use properties::{encode_properties, encode_will_properties, decode_properties, Properties, UserProperty};
pub use varint::*;
