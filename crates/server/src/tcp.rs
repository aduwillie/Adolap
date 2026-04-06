use tokio::net::{TcpListener, TcpStream};
use tokio::task;
use adolap_core::error::AdolapError;
use std::path::PathBuf;
use storage::background_compaction::BackgroundCompactionScheduler;
use tracing::{debug, error, info, warn};

use protocol::framing::{read_frame, write_frame};
use protocol::{decode_client_message, encode_server_message};

use crate::handler::handle_message;

pub async fn start_server(addr: &str) -> Result<(), AdolapError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| AdolapError::ExecutionError(format!("Bind error: {}", e)))?;

    let _background_compaction = BackgroundCompactionScheduler::new(PathBuf::from("data")).spawn();

    info!(%addr, "server listening");
    println!("Adolap server listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        info!(%peer, "client connected");
        println!("Client connected: {}", peer);

        task::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                error!(error = %e, "connection handler failed");
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream) -> Result<(), AdolapError> {
    loop {
        let frame = match read_frame(&mut socket).await {
            Ok(f) => {
                debug!(frame_bytes = f.len(), "received protocol frame");
                f
            }
            Err(e) => {
                warn!(error = %e, "client disconnected while reading frame");
                eprintln!("Client disconnected: {}", e);
                return Ok(());
            }
        };

        let msg = decode_client_message(&frame)?;
        let response = handle_message(msg).await?;

        let bytes = encode_server_message(&response);
        debug!(frame_bytes = bytes.len(), "sending protocol frame");
        write_frame(&mut socket, &bytes).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::handle_connection;
    use protocol::{decode_server_message, encode_client_message, ClientMessage, ServerMessage};
    use protocol::framing::{read_frame, write_frame};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::runtime::Runtime;

    #[test]
    fn handle_connection_round_trips_ping() {
        Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let server_task = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handle_connection(socket).await.unwrap();
            });

            let mut client = TcpStream::connect(addr).await.unwrap();
            let request = encode_client_message(&ClientMessage::Ping);
            write_frame(&mut client, &request).await.unwrap();

            let response = read_frame(&mut client).await.unwrap();
            match decode_server_message(&response).unwrap() {
                ServerMessage::Pong => {}
                other => panic!("unexpected response: {:?}", other),
            }

            drop(client);
            server_task.await.unwrap();
        });
    }
}
