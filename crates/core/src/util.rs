pub async fn ensure_dir(path: &str) -> std::io::Result<()> {
  tokio::fs::create_dir_all(path).await
}

#[cfg(test)]
mod tests {
  use super::ensure_dir;
  use std::path::PathBuf;
  use std::time::{SystemTime, UNIX_EPOCH};

  fn unique_test_dir() -> PathBuf {
    let unique = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("Time went backwards")
      .as_nanos();

    std::env::temp_dir().join(format!("adolap-core-util-{unique}"))
  }

  #[tokio::test]
  async fn ensure_dir_creates_missing_directory() {
    let dir = unique_test_dir();
    let dir_text = dir.to_string_lossy().to_string();

    ensure_dir(&dir_text).await.unwrap();

    let metadata = tokio::fs::metadata(&dir).await.unwrap();
    assert!(metadata.is_dir());

    tokio::fs::remove_dir_all(&dir).await.unwrap();
  }

  #[tokio::test]
  async fn ensure_dir_is_idempotent_for_existing_directory() {
    let dir = unique_test_dir();
    let dir_text = dir.to_string_lossy().to_string();

    ensure_dir(&dir_text).await.unwrap();
    ensure_dir(&dir_text).await.unwrap();

    let metadata = tokio::fs::metadata(&dir).await.unwrap();
    assert!(metadata.is_dir());

    tokio::fs::remove_dir_all(&dir).await.unwrap();
  }
}
