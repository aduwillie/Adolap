use std::path::Path;

use crate::error::AdolapError;

// Utility function to ensure a directory exists, creating it if necessary.
// If the directory already exists, it will not return an error. 
// If there is an I/O error other than "already exists", it will return an AdolapError::IO.
pub async fn ensure_dir(path: &Path) -> Result<(), AdolapError> {
  match tokio::fs::create_dir_all(path).await {
    Ok(()) => Ok(()),
    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
    Err(e) => Err(AdolapError::IO(e)),
  }
}

#[cfg(test)]
mod tests {
  use super::ensure_dir;
  use tokio::runtime::Runtime;
  use std::{time::{SystemTime, UNIX_EPOCH}};

  fn unique_test_dir() -> std::path::PathBuf {
    let unique = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("Time went backwards")
      .as_nanos();

    std::env::temp_dir().join(format!("adolap-core-util-{unique}"))
  }

  #[test]
  fn ensure_dir_creates_missing_directory() {
    Runtime::new().unwrap().block_on(async {
      let dir = unique_test_dir();

      ensure_dir(&dir).await.unwrap();

      let metadata = tokio::fs::metadata(&dir).await.unwrap();
      assert!(metadata.is_dir());

      tokio::fs::remove_dir_all(&dir).await.unwrap();
    });
  }

  #[test]
  fn ensure_dir_is_idempotent_for_existing_directory() {
    Runtime::new().unwrap().block_on(async {
      let dir = unique_test_dir();

      ensure_dir(&dir).await.unwrap();
      ensure_dir(&dir).await.unwrap();

      let metadata = tokio::fs::metadata(&dir).await.unwrap();
      assert!(metadata.is_dir());

      tokio::fs::remove_dir_all(&dir).await.unwrap();
    });
  }
}
