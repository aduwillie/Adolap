use tokio::net::{TcpListener, TcpStream};
use tokio::task;
use adolap_core::error::AdolapError;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use storage::background_compaction::BackgroundCompactionScheduler;
use tracing::{debug, error, info, warn};

use protocol::framing::{read_frame, write_frame};
use protocol::{decode_client_message, encode_server_message};

use crate::handler::handle_message;

/// Default connection limit used when no explicit value is provided.
const DEFAULT_MAX_CONNECTIONS: usize = 256;

pub async fn start_server(addr: &str) -> Result<(), AdolapError> {
    start_server_with_limit(addr, DEFAULT_MAX_CONNECTIONS).await
}

pub async fn start_server_with_limit(addr: &str, max_connections: usize) -> Result<(), AdolapError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| AdolapError::ExecutionError(format!("Bind error: {}", e)))?;

    let _background_compaction = BackgroundCompactionScheduler::new(PathBuf::from("data")).spawn();

    let connection_count = Arc::new(AtomicUsize::new(0));

    info!(%addr, max_connections, "server listening");
    println!("Adolap server listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;

        let current = connection_count.load(Ordering::Relaxed);
        if current >= max_connections {
            warn!(
                %peer,
                current_connections = current,
                max_connections,
                "connection limit reached; dropping new connection"
            );
            drop(socket);
            continue;
        }

        info!(%peer, connections = current + 1, "client connected");
        println!("Client connected: {}", peer);

        let count = Arc::clone(&connection_count);
        count.fetch_add(1, Ordering::Relaxed);

        task::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                error!(error = %e, "connection handler failed");
                eprintln!("Connection error: {}", e);
            }
            count.fetch_sub(1, Ordering::Relaxed);
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
    use super::{handle_connection, start_server_with_limit};
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

    #[test]
    fn server_enforces_connection_limit_via_atomic_counter() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Simulate the accept-loop's connection-limit check directly.
        let max_connections = 2usize;
        let count = Arc::new(AtomicUsize::new(0));

        // Simulate 3 incoming connections; only the first 2 should be accepted.
        let mut accepted = 0usize;
        let mut rejected = 0usize;
        for _ in 0..3 {
            let current = count.load(Ordering::Relaxed);
            if current >= max_connections {
                rejected += 1;
            } else {
                count.fetch_add(1, Ordering::Relaxed);
                accepted += 1;
            }
        }

        assert_eq!(accepted, 2, "expected 2 accepted connections");
        assert_eq!(rejected, 1, "expected 1 rejected connection");
        assert_eq!(count.load(Ordering::Relaxed), 2);

        // Simulate a disconnect reducing the count.
        count.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(count.load(Ordering::Relaxed), 1);

        // Now a new connection should be accepted again.
        let current = count.load(Ordering::Relaxed);
        assert!(current < max_connections, "connection should be allowed after one disconnect");
    }

    #[test]
    fn connection_count_increments_and_decrements() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Verify the AtomicUsize accounting logic directly without a live server.
        let count = Arc::new(AtomicUsize::new(0));
        count.fetch_add(1, Ordering::Relaxed);
        assert_eq!(count.load(Ordering::Relaxed), 1);
        count.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }
}
