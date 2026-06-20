//! Feishu WS v2 protobuf frame protocol types.
//!
//! Matches the Python SDK's `pbbp2.proto` definition.
//!
//! The Feishu WebSocket v2 protocol wraps JSON event payloads in protobuf
//! `Frame` messages.  Each frame contains headers (key-value pairs), a
//! payload (the actual JSON event), and routing metadata (`service`, `method`).
//!
//! # Frame types
//!
//! | method  | Constant      | Description                  |
//! |---------|---------------|------------------------------|
//! | 0       | FRAME_CONTROL | Ping / Pong heartbeat        |
//! | 1       | FRAME_DATA    | Event / card notification    |
//!
//! # Multi-part messages
//!
//! Large messages are split across multiple frames using the `sum` and `seq`
//! headers.  The receiver must reassemble fragments in order.

/// A key-value header attached to a protobuf [`Frame`].
#[derive(Clone, PartialEq, prost::Message)]
pub struct Header {
    #[prost(string, required, tag = "1")]
    pub key: String,
    #[prost(string, required, tag = "2")]
    pub value: String,
}

/// Feishu WS v2 protobuf frame.
///
/// This is the binary envelope that wraps every WebSocket message in the
/// Feishu long-connection protocol.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Frame {
    #[prost(uint64, required, tag = "1")]
    pub seq_id: u64,

    #[prost(uint64, required, tag = "2")]
    pub log_id: u64,

    /// Service ID extracted from the WS URL query parameter.
    #[prost(int32, required, tag = "3")]
    pub service: i32,

    /// Frame method: 0 = CONTROL, 1 = DATA.
    #[prost(int32, required, tag = "4")]
    pub method: i32,

    /// Key-value headers (e.g. "type", "message_id", "sum", "seq").
    #[prost(message, repeated, tag = "5")]
    pub headers: Vec<Header>,

    /// Payload encoding (e.g. "json").
    #[prost(string, optional, tag = "6")]
    pub payload_encoding: Option<String>,

    /// Payload content type (e.g. "im.message.receive_v1").
    #[prost(string, optional, tag = "7")]
    pub payload_type: Option<String>,

    /// The actual event payload as UTF-8 JSON bytes.
    #[prost(bytes = "vec", optional, tag = "8")]
    pub payload: Option<Vec<u8>>,

    /// Alternative log ID (string form).
    #[prost(string, optional, tag = "9")]
    pub log_id_new: Option<String>,
}

impl Frame {
    /// Look up a header value by key.
    ///
    /// Returns `None` when the key is not present in the frame headers.
    pub fn get_header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|h| h.key == key)
            .map(|h| h.value.as_str())
    }
}

// ---------------------------------------------------------------------------
// Constants matching the Python SDK `pbbp2.proto`
// ---------------------------------------------------------------------------

/// Control frame (ping / pong).
pub const FRAME_CONTROL: i32 = 0;
/// Data frame (event / card).
pub const FRAME_DATA: i32 = 1;

// Well-known message type values for the `type` header.
pub const MSG_PING: &str = "ping";
pub const MSG_PONG: &str = "pong";
pub const MSG_EVENT: &str = "event";
pub const MSG_CARD: &str = "card";

// Standard header keys.
pub const HEADER_TYPE: &str = "type";
pub const HEADER_MESSAGE_ID: &str = "message_id";
pub const HEADER_TRACE_ID: &str = "trace_id";
pub const HEADER_SUM: &str = "sum";
pub const HEADER_SEQ: &str = "seq";
pub const HEADER_BIZ_RT: &str = "biz_rt";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as ProstMessage;

    // -- Round-trip encode / decode -------------------------------------------

    #[test]
    fn test_frame_encode_decode_roundtrip() {
        let frame = Frame {
            seq_id: 42,
            log_id: 987654321,
            service: 5,
            method: FRAME_DATA,
            headers: vec![
                Header {
                    key: HEADER_TYPE.to_string(),
                    value: MSG_EVENT.to_string(),
                },
                Header {
                    key: HEADER_MESSAGE_ID.to_string(),
                    value: "msg_abc123".to_string(),
                },
                Header {
                    key: HEADER_TRACE_ID.to_string(),
                    value: "trace_xyz".to_string(),
                },
            ],
            payload_encoding: Some("json".to_string()),
            payload_type: Some("im.message.receive_v1".to_string()),
            payload: Some(b"{\"event\":\"test\"}".to_vec()),
            log_id_new: Some("new_log_001".to_string()),
        };

        let encoded = frame.encode_to_vec();
        assert!(!encoded.is_empty(), "encoded buffer should not be empty");

        let decoded = Frame::decode(encoded.as_ref()).expect("decode should succeed");
        assert_eq!(decoded.seq_id, 42);
        assert_eq!(decoded.log_id, 987654321);
        assert_eq!(decoded.service, 5);
        assert_eq!(decoded.method, FRAME_DATA);
        assert_eq!(decoded.headers.len(), 3);
        assert_eq!(decoded.payload_encoding.as_deref(), Some("json"));
        assert_eq!(
            decoded.payload_type.as_deref(),
            Some("im.message.receive_v1")
        );
        assert_eq!(
            decoded.payload.as_deref(),
            Some(b"{\"event\":\"test\"}".as_ref())
        );
        assert_eq!(decoded.log_id_new.as_deref(), Some("new_log_001"));
    }

    // -- Header lookup ---------------------------------------------------------

    #[test]
    fn test_frame_get_header() {
        let frame = Frame {
            seq_id: 1,
            log_id: 1,
            service: 1,
            method: FRAME_DATA,
            headers: vec![
                Header {
                    key: HEADER_TYPE.to_string(),
                    value: MSG_EVENT.to_string(),
                },
                Header {
                    key: HEADER_SUM.to_string(),
                    value: "1".to_string(),
                },
                Header {
                    key: HEADER_SEQ.to_string(),
                    value: "0".to_string(),
                },
            ],
            payload_encoding: None,
            payload_type: None,
            payload: Some(b"{}".to_vec()),
            log_id_new: None,
        };

        assert_eq!(frame.get_header(HEADER_TYPE), Some(MSG_EVENT));
        assert_eq!(frame.get_header(HEADER_SUM), Some("1"));
        assert_eq!(frame.get_header(HEADER_SEQ), Some("0"));
        assert_eq!(frame.get_header("nonexistent"), None);
    }

    // -- Control frame round-trip ----------------------------------------------

    #[test]
    fn test_control_frame_roundtrip() {
        let frame = Frame {
            seq_id: 0,
            log_id: 0,
            service: 3,
            method: FRAME_CONTROL,
            headers: vec![Header {
                key: HEADER_TYPE.to_string(),
                value: MSG_PING.to_string(),
            }],
            payload_encoding: None,
            payload_type: None,
            payload: None,
            log_id_new: None,
        };

        let encoded = frame.encode_to_vec();
        let decoded = Frame::decode(encoded.as_ref()).expect("decode control frame");
        assert_eq!(decoded.method, FRAME_CONTROL);
        assert_eq!(decoded.get_header(HEADER_TYPE), Some(MSG_PING));
        assert!(decoded.payload.is_none());
    }

    // -- Empty frame -----------------------------------------------------------

    #[test]
    fn test_empty_frame_defaults() {
        let frame = Frame {
            seq_id: 0,
            log_id: 0,
            service: 0,
            method: FRAME_CONTROL,
            headers: vec![],
            payload_encoding: None,
            payload_type: None,
            payload: None,
            log_id_new: None,
        };

        let encoded = frame.encode_to_vec();
        let decoded = Frame::decode(encoded.as_ref()).expect("decode empty frame");
        assert_eq!(decoded.seq_id, 0);
        assert_eq!(decoded.headers.len(), 0);
        assert!(decoded.payload.is_none());
    }
}
