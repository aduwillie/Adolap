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
