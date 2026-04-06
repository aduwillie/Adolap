//! Binary wire protocol shared by the Adolap CLI and server.
//!
//! It defines framed request and response message types plus the codec used to
//! serialize them over TCP.

pub mod message;
pub mod codec;
pub mod framing;

pub use message::{
    ClientMessage,
    ColumnDefinition,
    ColumnType,
    MetaResult,
    QueryResult,
    QuerySummary,
    ResultRow,
    ResultSet,
    ScalarValue,
    ServerMessage,
};
pub use codec::{
    PROTOCOL_VERSION,
    encode_client_message,
    decode_client_message,
    encode_server_message,
    decode_server_message,
};

#[cfg(test)]
mod tests {
    use super::{decode_client_message, decode_server_message, encode_client_message, encode_server_message, ClientMessage, ServerMessage};

    #[test]
    fn reexports_support_ping_pong_round_trip() {
        let client = decode_client_message(&encode_client_message(&ClientMessage::Ping)).unwrap();
        match client {
            ClientMessage::Ping => {}
            other => panic!("unexpected client message: {:?}", other),
        }

        let server = decode_server_message(&encode_server_message(&ServerMessage::Pong)).unwrap();
        match server {
            ServerMessage::Pong => {}
            other => panic!("unexpected server message: {:?}", other),
        }
    }
}
