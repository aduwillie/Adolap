use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdolapError {
  #[error("Core error: {0}")]
  IO(#[from] std::io::Error),

  #[error("Storage error: {0}")]
  StorageError(String),

  #[error("Execution error: {0}")]
  ExecutionError(String),

  #[error("Protocol error: {0}")]
  ProtocolError(String),

  #[error("Server error: {0}")]
  ServerError(String),

  #[error("CLI error: {0}")]
  CliError(String),

  #[error("Serialization error: {0}")]
  Serialization(String),

  #[error("Unknown error: {0}")]
  UnknownError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adolap_error() {
      let io_error = AdolapError::IO(std::io::Error::new(std::io::ErrorKind::Other, "test"));
      match &io_error {
        AdolapError::IO(inner) => {
          assert_eq!(inner.kind(), std::io::ErrorKind::Other);
          assert_eq!(inner.to_string(), "test");
        }
        other => panic!("expected IO error, got {:?}", other),
      }
      assert_eq!(format!("{}", io_error), "Core error: test");

      let storage_error = AdolapError::StorageError("storage failed".to_string());
      assert_eq!(format!("{}", storage_error), "Storage error: storage failed");

      let execution_error = AdolapError::ExecutionError("execution failed".to_string());
      assert_eq!(format!("{}", execution_error), "Execution error: execution failed");

      let protocol_error = AdolapError::ProtocolError("protocol failed".to_string());
      assert_eq!(format!("{}", protocol_error), "Protocol error: protocol failed");

      let server_error = AdolapError::ServerError("server failed".to_string());
      assert_eq!(format!("{}", server_error), "Server error: server failed");

      let cli_error = AdolapError::CliError("cli failed".to_string());
      assert_eq!(format!("{}", cli_error), "CLI error: cli failed");

      let serialization_error = AdolapError::Serialization("serialization failed".to_string());
      assert_eq!(format!("{}", serialization_error), "Serialization error: serialization failed");

      let unknown_error = AdolapError::UnknownError("unknown failed".to_string());
      assert_eq!(format!("{}", unknown_error), "Unknown error: unknown failed");
    }
}