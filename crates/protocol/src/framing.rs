// protocol/src/framing.rs

/*!
    TCP Frame Encoding/Decoding
    ---------------------------

    The protocol uses a simple, length‑prefixed binary framing format:

        [u32 length][frame bytes...]

    - `length` is the number of bytes in the frame (not including the length field)
    - The frame bytes are passed to `decode_client_message` or `decode_server_message`
    - This module does NOT interpret the frame contents; it only moves bytes.

    This keeps the server networking layer clean and the protocol crate reusable.
*/

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use core::error::AdolapError;

/// Read a single frame from a TCP stream.
///
/// Frame format:
///     [u32 length][frame bytes...]
pub async fn read_frame<R>(stream: &mut R) -> Result<Vec<u8>, AdolapError>
where
    R: AsyncReadExt + Unpin,
{
    // Read the 4‑byte length prefix
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| AdolapError::ExecutionError(format!("Failed to read frame length: {}", e)))?;

    let frame_len = u32::from_be_bytes(len_buf) as usize;

    // Read the frame payload
    let mut frame = vec![0u8; frame_len];
    stream
        .read_exact(&mut frame)
        .await
        .map_err(|e| AdolapError::ExecutionError(format!("Failed to read frame payload: {}", e)))?;

    Ok(frame)
}

/// Write a single frame to a TCP stream.
///
/// Frame format:
///     [u32 length][frame bytes...]
pub async fn write_frame<W>(stream: &mut W, frame: &[u8]) -> Result<(), AdolapError>
where
    W: AsyncWriteExt + Unpin,
{
    let len = frame.len() as u32;

    // Write length prefix
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| AdolapError::ExecutionError(format!("Failed to write frame length: {}", e)))?;

    // Write frame payload
    stream
        .write_all(frame)
        .await
        .map_err(|e| AdolapError::ExecutionError(format!("Failed to write frame payload: {}", e)))?;

    Ok(())
}
