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
