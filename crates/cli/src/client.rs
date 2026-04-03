use adolap_core::error::AdolapError;
use protocol::framing::{read_frame, write_frame};
use protocol::{decode_server_message, encode_client_message, ClientMessage, ServerMessage};
use std::env;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tracing::{debug, info};

const DEFAULT_ADDR: &str = "127.0.0.1:5999";

pub struct Client {
    stream: TcpStream,
}

pub struct ClientResponse {
    pub message: ServerMessage,
    pub elapsed: Duration,
    pub response_bytes: usize,
}

impl Client {
    pub async fn connect() -> Result<(Self, String), AdolapError> {
        let addr = resolve_addr();
        info!(%addr, "connecting client");
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|error| AdolapError::CliError(format!("Failed to connect to {}: {}", addr, error)))?;
        Ok((Self { stream }, addr))
    }

    pub async fn send_query(&mut self, query: &str) -> Result<ClientResponse, AdolapError> {
        self.send_message(ClientMessage::QueryText(query.to_string())).await
    }

    pub async fn send_meta_command(&mut self, command: &str) -> Result<ClientResponse, AdolapError> {
        self.send_message(ClientMessage::MetaCommand(command.to_string())).await
    }

    async fn send_message(&mut self, message: ClientMessage) -> Result<ClientResponse, AdolapError> {
        let request = encode_client_message(&message);
        debug!(request_bytes = request.len(), message_kind = client_message_kind(&message), "sending client message");
        let started = Instant::now();
        write_frame(&mut self.stream, &request).await?;
        let response = read_frame(&mut self.stream).await?;
        debug!(response_bytes = response.len(), elapsed_ms = started.elapsed().as_secs_f64() * 1000.0, "received client response");
        Ok(ClientResponse {
            message: decode_server_message(&response)?,
            elapsed: started.elapsed(),
            response_bytes: response.len(),
        })
    }
}

fn client_message_kind(message: &ClientMessage) -> &'static str {
    match message {
        ClientMessage::QueryText(_) => "query_text",
        ClientMessage::CreateDatabase { .. } => "create_database",
        ClientMessage::CreateTable { .. } => "create_table",
        ClientMessage::InsertRows { .. } => "insert_rows",
        ClientMessage::IngestInto { .. } => "ingest_into",
        ClientMessage::MetaCommand(_) => "meta_command",
        ClientMessage::Ping => "ping",
    }
}

fn resolve_addr() -> String {
    env::args()
        .nth(1)
        .or_else(|| env::var("ADOLAP_ADDR").ok())
        .unwrap_or_else(|| DEFAULT_ADDR.to_string())
}