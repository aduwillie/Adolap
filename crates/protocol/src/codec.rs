use crate::message::{
    ClientMessage, ColumnDefinition, ColumnType, MetaResult, ProtocolResult, QueryResult, ResultRow, ResultSet,
    ScalarValue, ServerMessage,
};
use core::error::AdolapError;
use std::io::{Cursor, Read};

pub const PROTOCOL_VERSION: u16 = 1;

// Message type tags
const MSG_CLIENT_QUERY_TEXT: u8 = 1;
const MSG_CLIENT_PING: u8 = 2;
const MSG_CLIENT_CREATE_DATABASE: u8 = 3;
const MSG_CLIENT_CREATE_TABLE: u8 = 4;
const MSG_CLIENT_INSERT_ROWS: u8 = 5;
const MSG_CLIENT_INGEST_INTO: u8 = 6;
const MSG_CLIENT_META_COMMAND: u8 = 7;

const MSG_SERVER_QUERY_RESULT: u8 = 1;
const MSG_SERVER_ERROR: u8 = 2;
const MSG_SERVER_PONG: u8 = 3;
const MSG_SERVER_OK: u8 = 4;
const MSG_SERVER_META_RESULT: u8 = 5;

/// Encode a `ClientMessage` into a binary frame.
///
/// Frame layout:
///   [u16 version][u8 msg_type][payload...]
pub fn encode_client_message(msg: &ClientMessage) -> Vec<u8> {
    let mut out = Vec::new();

    // version
    out.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());

    match msg {
        ClientMessage::QueryText(text) => {
            out.push(MSG_CLIENT_QUERY_TEXT);
            write_string(&mut out, text);
        }
        ClientMessage::CreateDatabase { name } => {
            out.push(MSG_CLIENT_CREATE_DATABASE);
            write_string(&mut out, name);
        }
        ClientMessage::CreateTable { table, columns } => {
            out.push(MSG_CLIENT_CREATE_TABLE);
            write_string(&mut out, table);
            write_u32(&mut out, columns.len() as u32);
            for column in columns {
                write_column_definition(&mut out, column);
            }
        }
        ClientMessage::InsertRows { table, rows } => {
            out.push(MSG_CLIENT_INSERT_ROWS);
            write_optional_string(&mut out, table.as_deref());
            write_u32(&mut out, rows.len() as u32);
            for row in rows {
                write_u32(&mut out, row.len() as u32);
                for value in row {
                    write_scalar_value(&mut out, value);
                }
            }
        }
        ClientMessage::IngestInto { table, file_path } => {
            out.push(MSG_CLIENT_INGEST_INTO);
            write_string(&mut out, table);
            write_string(&mut out, file_path);
        }
        ClientMessage::MetaCommand(command) => {
            out.push(MSG_CLIENT_META_COMMAND);
            write_string(&mut out, command);
        }
        ClientMessage::Ping => {
            out.push(MSG_CLIENT_PING);
        }
    }

    out
}

/// Decode a `ClientMessage` from a binary frame.
pub fn decode_client_message(buf: &[u8]) -> ProtocolResult<ClientMessage> {
    let mut cur = Cursor::new(buf);

    let version = read_u16(&mut cur)?;
    if version != PROTOCOL_VERSION {
        return Err(AdolapError::ExecutionError(format!(
            "Unsupported protocol version: {}",
            version
        )));
    }

    let msg_type = read_u8(&mut cur)?;

    match msg_type {
        MSG_CLIENT_QUERY_TEXT => {
            let text = read_string(&mut cur)?;
            Ok(ClientMessage::QueryText(text))
        }
        MSG_CLIENT_CREATE_DATABASE => {
            let name = read_string(&mut cur)?;
            Ok(ClientMessage::CreateDatabase { name })
        }
        MSG_CLIENT_CREATE_TABLE => {
            let table = read_string(&mut cur)?;
            let column_count = read_u32(&mut cur)? as usize;
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                columns.push(read_column_definition(&mut cur)?);
            }
            Ok(ClientMessage::CreateTable { table, columns })
        }
        MSG_CLIENT_INSERT_ROWS => {
            let table = read_optional_string(&mut cur)?;
            let row_count = read_u32(&mut cur)? as usize;
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let value_count = read_u32(&mut cur)? as usize;
                let mut row = Vec::with_capacity(value_count);
                for _ in 0..value_count {
                    row.push(read_scalar_value(&mut cur)?);
                }
                rows.push(row);
            }
            Ok(ClientMessage::InsertRows { table, rows })
        }
        MSG_CLIENT_INGEST_INTO => {
            let table = read_string(&mut cur)?;
            let file_path = read_string(&mut cur)?;
            Ok(ClientMessage::IngestInto { table, file_path })
        }
        MSG_CLIENT_META_COMMAND => {
            let command = read_string(&mut cur)?;
            Ok(ClientMessage::MetaCommand(command))
        }
        MSG_CLIENT_PING => Ok(ClientMessage::Ping),
        other => Err(AdolapError::ExecutionError(format!(
            "Unknown client message type: {}",
            other
        ))),
    }
}

/// Encode a `ServerMessage` into a binary frame.
///
/// Frame layout:
///   [u16 version][u8 msg_type][payload...]
pub fn encode_server_message(msg: &ServerMessage) -> Vec<u8> {
    let mut out = Vec::new();

    out.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());

    match msg {
        ServerMessage::QueryResult(query_result) => {
            out.push(MSG_SERVER_QUERY_RESULT);
            write_query_result(&mut out, query_result);
        }
        ServerMessage::Ok(message) => {
            out.push(MSG_SERVER_OK);
            write_string(&mut out, message);
        }
        ServerMessage::MetaResult(meta_result) => {
            out.push(MSG_SERVER_META_RESULT);
            write_meta_result(&mut out, meta_result);
        }
        ServerMessage::Error(err) => {
            out.push(MSG_SERVER_ERROR);
            write_string(&mut out, err);
        }
        ServerMessage::Pong => {
            out.push(MSG_SERVER_PONG);
        }
    }

    out
}

/// Decode a `ServerMessage` from a binary frame.
pub fn decode_server_message(buf: &[u8]) -> ProtocolResult<ServerMessage> {
    let mut cur = Cursor::new(buf);

    let version = read_u16(&mut cur)?;
    if version != PROTOCOL_VERSION {
        return Err(AdolapError::ExecutionError(format!(
            "Unsupported protocol version: {}",
            version
        )));
    }

    let msg_type = read_u8(&mut cur)?;

    match msg_type {
        MSG_SERVER_QUERY_RESULT => {
            let query_result = read_query_result(&mut cur)?;
            Ok(ServerMessage::QueryResult(query_result))
        }
        MSG_SERVER_OK => {
            let message = read_string(&mut cur)?;
            Ok(ServerMessage::Ok(message))
        }
        MSG_SERVER_META_RESULT => {
            let meta_result = read_meta_result(&mut cur)?;
            Ok(ServerMessage::MetaResult(meta_result))
        }
        MSG_SERVER_ERROR => {
            let err = read_string(&mut cur)?;
            Ok(ServerMessage::Error(err))
        }
        MSG_SERVER_PONG => Ok(ServerMessage::Pong),
        other => Err(AdolapError::ExecutionError(format!(
            "Unknown server message type: {}",
            other
        ))),
    }
}

// -----------------------------
// ResultSet encoding/decoding
// -----------------------------

fn write_result_set(out: &mut Vec<u8>, rs: &ResultSet) {
    // columns
    write_u32(out, rs.columns.len() as u32);
    for col in &rs.columns {
        write_string(out, col);
    }

    // rows
    write_u32(out, rs.rows.len() as u32);
    for row in &rs.rows {
        write_u32(out, row.values.len() as u32);
        for v in &row.values {
            write_string(out, v);
        }
    }
}

fn write_query_result(out: &mut Vec<u8>, query_result: &QueryResult) {
    write_result_set(out, &query_result.result_set);
    write_string(out, &query_result.logical_plan);
    write_string(out, &query_result.physical_plan);
}

fn write_meta_result(out: &mut Vec<u8>, meta_result: &MetaResult) {
    write_string(out, &meta_result.title);
    write_string(out, &meta_result.content);
}

fn read_result_set(cur: &mut Cursor<&[u8]>) -> ProtocolResult<ResultSet> {
    let col_count = read_u32(cur)? as usize;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        columns.push(read_string(cur)?);
    }

    let row_count = read_u32(cur)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let val_count = read_u32(cur)? as usize;
        let mut values = Vec::with_capacity(val_count);
        for _ in 0..val_count {
            values.push(read_string(cur)?);
        }
        rows.push(ResultRow::new(values));
    }

    Ok(ResultSet::new(columns, rows))
}

fn read_query_result(cur: &mut Cursor<&[u8]>) -> ProtocolResult<QueryResult> {
    let result_set = read_result_set(cur)?;
    let logical_plan = read_string(cur)?;
    let physical_plan = read_string(cur)?;
    Ok(QueryResult::new(result_set, logical_plan, physical_plan))
}

fn read_meta_result(cur: &mut Cursor<&[u8]>) -> ProtocolResult<MetaResult> {
    Ok(MetaResult::new(read_string(cur)?, read_string(cur)?))
}

// -----------------------------
// Primitive helpers
// -----------------------------

fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn write_optional_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            write_bool(out, true);
            write_string(out, value);
        }
        None => write_bool(out, false),
    }
}

fn write_column_definition(out: &mut Vec<u8>, column: &ColumnDefinition) {
    write_string(out, &column.name);
    write_column_type(out, &column.column_type);
    write_bool(out, column.nullable);
}

fn write_column_type(out: &mut Vec<u8>, column_type: &ColumnType) {
    let tag = match column_type {
        ColumnType::Utf8 => 1,
        ColumnType::I32 => 2,
        ColumnType::U32 => 3,
        ColumnType::Bool => 4,
    };
    out.push(tag);
}

fn write_scalar_value(out: &mut Vec<u8>, value: &ScalarValue) {
    match value {
        ScalarValue::Null => out.push(0),
        ScalarValue::Utf8(value) => {
            out.push(1);
            write_string(out, value);
        }
        ScalarValue::I32(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_be_bytes());
        }
        ScalarValue::U32(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_be_bytes());
        }
        ScalarValue::Bool(value) => {
            out.push(4);
            write_bool(out, *value);
        }
    }
}

fn read_u8(cur: &mut Cursor<&[u8]>) -> ProtocolResult<u8> {
    let mut buf = [0u8; 1];
    cur.read_exact(&mut buf)
        .map_err(|e| AdolapError::ExecutionError(format!("read_u8: {}", e)))?;
    Ok(buf[0])
}

fn read_bool(cur: &mut Cursor<&[u8]>) -> ProtocolResult<bool> {
    match read_u8(cur)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(AdolapError::ExecutionError(format!(
            "Invalid bool tag: {}",
            other
        ))),
    }
}

fn read_u16(cur: &mut Cursor<&[u8]>) -> ProtocolResult<u16> {
    let mut buf = [0u8; 2];
    cur.read_exact(&mut buf)
        .map_err(|e| AdolapError::ExecutionError(format!("read_u16: {}", e)))?;
    Ok(u16::from_be_bytes(buf))
}

fn read_u32(cur: &mut Cursor<&[u8]>) -> ProtocolResult<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)
        .map_err(|e| AdolapError::ExecutionError(format!("read_u32: {}", e)))?;
    Ok(u32::from_be_bytes(buf))
}

fn read_string(cur: &mut Cursor<&[u8]>) -> ProtocolResult<String> {
    let len = read_u32(cur)? as usize;
    let mut buf = vec![0u8; len];
    cur.read_exact(&mut buf)
        .map_err(|e| AdolapError::ExecutionError(format!("read_string: {}", e)))?;
    String::from_utf8(buf)
        .map_err(|e| AdolapError::ExecutionError(format!("Invalid UTF-8 string: {}", e)))
}

fn read_optional_string(cur: &mut Cursor<&[u8]>) -> ProtocolResult<Option<String>> {
    if read_bool(cur)? {
        Ok(Some(read_string(cur)?))
    } else {
        Ok(None)
    }
}

fn read_column_definition(cur: &mut Cursor<&[u8]>) -> ProtocolResult<ColumnDefinition> {
    Ok(ColumnDefinition {
        name: read_string(cur)?,
        column_type: read_column_type(cur)?,
        nullable: read_bool(cur)?,
    })
}

fn read_column_type(cur: &mut Cursor<&[u8]>) -> ProtocolResult<ColumnType> {
    match read_u8(cur)? {
        1 => Ok(ColumnType::Utf8),
        2 => Ok(ColumnType::I32),
        3 => Ok(ColumnType::U32),
        4 => Ok(ColumnType::Bool),
        other => Err(AdolapError::ExecutionError(format!(
            "Unknown column type tag: {}",
            other
        ))),
    }
}

fn read_scalar_value(cur: &mut Cursor<&[u8]>) -> ProtocolResult<ScalarValue> {
    match read_u8(cur)? {
        0 => Ok(ScalarValue::Null),
        1 => Ok(ScalarValue::Utf8(read_string(cur)?)),
        2 => {
            let mut buf = [0u8; 4];
            cur.read_exact(&mut buf)
                .map_err(|e| AdolapError::ExecutionError(format!("read_i32: {}", e)))?;
            Ok(ScalarValue::I32(i32::from_be_bytes(buf)))
        }
        3 => Ok(ScalarValue::U32(read_u32(cur)?)),
        4 => Ok(ScalarValue::Bool(read_bool(cur)?)),
        other => Err(AdolapError::ExecutionError(format!(
            "Unknown scalar value tag: {}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_client_message, decode_server_message, encode_client_message, encode_server_message};
    use crate::message::{ClientMessage, ColumnDefinition, ColumnType, MetaResult, QueryResult, ResultRow, ResultSet, ScalarValue, ServerMessage};

    #[test]
    fn round_trips_create_table() {
        let message = ClientMessage::CreateTable {
            table: "default.events".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "country".to_string(),
                    column_type: ColumnType::Utf8,
                    nullable: false,
                },
                ColumnDefinition {
                    name: "revenue".to_string(),
                    column_type: ColumnType::I32,
                    nullable: false,
                },
            ],
        };

        let decoded = decode_client_message(&encode_client_message(&message)).unwrap();
        match decoded {
            ClientMessage::CreateTable { table, columns } => {
                assert_eq!(table, "default.events");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[1].column_type, ColumnType::I32);
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }

    #[test]
    fn round_trips_insert_rows() {
        let message = ClientMessage::InsertRows {
            table: Some("events".to_string()),
            rows: vec![vec![
                ScalarValue::Utf8("US".to_string()),
                ScalarValue::Utf8("mobile".to_string()),
                ScalarValue::I32(25),
            ]],
        };

        let decoded = decode_client_message(&encode_client_message(&message)).unwrap();
        match decoded {
            ClientMessage::InsertRows { table, rows } => {
                assert_eq!(table.as_deref(), Some("events"));
                assert_eq!(rows.len(), 1);
                assert!(matches!(rows[0][2], ScalarValue::I32(25)));
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }

    #[test]
    fn round_trips_meta_command() {
        let message = ClientMessage::MetaCommand("tables analytics".to_string());

        let decoded = decode_client_message(&encode_client_message(&message)).unwrap();
        match decoded {
            ClientMessage::MetaCommand(command) => assert_eq!(command, "tables analytics"),
            other => panic!("unexpected message: {:?}", other),
        }
    }

    #[test]
    fn round_trips_server_ok() {
        let message = ServerMessage::Ok("table created".to_string());

        let decoded = decode_server_message(&encode_server_message(&message)).unwrap();
        match decoded {
            ServerMessage::Ok(value) => assert_eq!(value, "table created"),
            other => panic!("unexpected message: {:?}", other),
        }
    }

    #[test]
    fn round_trips_query_result_with_plans() {
        let message = ServerMessage::QueryResult(QueryResult::new(
            ResultSet::new(
                vec!["country".to_string()],
                vec![ResultRow::new(vec!["US".to_string()])],
            ),
            "LogicalPlan::Project".to_string(),
            "PhysicalPlan::Project".to_string(),
        ));

        let decoded = decode_server_message(&encode_server_message(&message)).unwrap();
        match decoded {
            ServerMessage::QueryResult(query_result) => {
                assert_eq!(query_result.result_set.columns, vec!["country"]);
                assert_eq!(query_result.logical_plan, "LogicalPlan::Project");
                assert_eq!(query_result.physical_plan, "PhysicalPlan::Project");
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }

    #[test]
    fn round_trips_meta_result() {
        let message = ServerMessage::MetaResult(MetaResult::new(
            "Tables".to_string(),
            "analytics.events".to_string(),
        ));

        let decoded = decode_server_message(&encode_server_message(&message)).unwrap();
        match decoded {
            ServerMessage::MetaResult(meta_result) => {
                assert_eq!(meta_result.title, "Tables");
                assert_eq!(meta_result.content, "analytics.events");
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }
}
