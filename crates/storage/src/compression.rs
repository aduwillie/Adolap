use core::error::AdolapError;
use crate::config::CompressionType;

pub fn compress_buffer(input: &[u8], compression: CompressionType) -> Result<Vec<u8>, AdolapError> {
    match compression {
        CompressionType::None => Ok(input.to_vec()),

        CompressionType::Lz4 => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            std::io::Write::write_all(&mut encoder, input)
                .map_err(|e| AdolapError::StorageError(format!("LZ4 compression failed: {}", e)))?;

            encoder
                .finish()
                .map_err(|e| AdolapError::StorageError(format!("LZ4 compression finalize failed: {}", e)))
        }

        CompressionType::Zstd => {
            let mut encoder = zstd::stream::Encoder::new(Vec::new(), 0)
                .map_err(|e| AdolapError::StorageError(format!("Zstd compression failed: {}", e)))?;

            std::io::Write::write_all(&mut encoder, input)
                .map_err(|e| AdolapError::StorageError(format!("Zstd compression failed: {}", e)))?;

            encoder
                .finish()
                .map_err(|e| AdolapError::StorageError(format!("Zstd compression finalize failed: {}", e)))
        }
    }
}

pub fn decompress_buffer(
    compressed: &[u8],
    compression: CompressionType,
) -> Result<Vec<u8>, AdolapError> {
    match compression {
        CompressionType::None => Ok(compressed.to_vec()),

        CompressionType::Lz4 => {
            let mut decoder = lz4_flex::frame::FrameDecoder::new(compressed);
            let mut output = Vec::new();

            std::io::Read::read_to_end(&mut decoder, &mut output)
                .map_err(|e| AdolapError::StorageError(format!("LZ4 decompression failed: {}", e)))?;

            Ok(output)
        }

        CompressionType::Zstd => {
            zstd::decode_all(compressed)
                .map_err(|e| AdolapError::StorageError(format!("ZSTD decompression failed: {}", e)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{compress_buffer, decompress_buffer};
    use crate::config::CompressionType;
    use core::error::AdolapError;

    #[test]
    fn round_trips_supported_compression_types() {
        let payload = b"alpha alpha alpha alpha alpha alpha alpha alpha";

        for compression in [CompressionType::None, CompressionType::Lz4, CompressionType::Zstd] {
            let compressed = compress_buffer(payload, compression).unwrap();
            let decompressed = decompress_buffer(&compressed, compression).unwrap();
            assert_eq!(decompressed, payload);
        }
    }

    #[test]
    fn rejects_invalid_compressed_payload() {
        let error = decompress_buffer(b"not-a-frame", CompressionType::Lz4).unwrap_err();
        match error {
            AdolapError::StorageError(message) => {
                assert!(message.contains("LZ4 decompression failed"));
            }
            other => panic!("expected storage error, got {:?}", other),
        }
    }
}
