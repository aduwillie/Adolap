use crate::client::Client;
use crate::ctrlc_handler::{handle_interrupt, InterruptAction};
use crate::meta::{handle_meta_command, MetaCommandResult, ReplSettings};
use crate::printer::print_response;
use adolap_core::error::AdolapError;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::path::PathBuf;
use tracing::{debug, info};

const PRIMARY_PROMPT: &str = "adolap> ";
const CONTINUATION_PROMPT: &str = ".....> ";

pub async fn run() -> Result<(), AdolapError> {
    let (mut client, addr) = Client::connect().await?;
    info!(%addr, "connected to server");

    let mut editor = DefaultEditor::new()
        .map_err(|error| AdolapError::CliError(format!("Failed to initialize line editor: {}", error)))?;
    let history_path = history_path();
    if let Some(path) = history_path.as_ref() {
        let _ = editor.load_history(path);
    }

    println!("Connected to Adolap at {}", addr);

    let mut query_buffer = String::new();
    let mut settings = ReplSettings::default();

    loop {
        let prompt = if query_buffer.is_empty() {
            PRIMARY_PROMPT
        } else {
            CONTINUATION_PROMPT
        };

        match editor.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                if query_buffer.is_empty() && trimmed.starts_with('\\') {
                    match handle_meta_command(trimmed, &mut client, &mut settings).await? {
                        MetaCommandResult::Continue(Some(response)) => print_response(response, &settings),
                        MetaCommandResult::Continue(None) => {}
                        MetaCommandResult::Exit => {
                            println!("Exiting.");
                            break;
                        }
                    }
                    continue;
                }

                if query_buffer.is_empty() && trimmed.is_empty() {
                    continue;
                }

                append_line(&mut query_buffer, &line);
                if !query_is_complete(&query_buffer) {
                    continue;
                }

                let query = finalize_query(&query_buffer);
                query_buffer.clear();

                if query.is_empty() {
                    continue;
                }

                debug!(query_lines = query.lines().count(), query_len = query.len(), "sending query from repl");

                let _ = editor.add_history_entry(query.as_str());

                let response = client.send_query(&query).await?;
                print_response(response, &settings);
            }
            Err(ReadlineError::Interrupted) => {
                debug!(buffer_len = query_buffer.len(), "received interrupt in repl");
                if matches!(handle_interrupt(&mut query_buffer), InterruptAction::Exit) {
                    info!("exiting repl after interrupt");
                    break;
                }
            }
            Err(ReadlineError::Eof) => {
                info!("exiting repl after eof");
                println!("Exiting.");
                break;
            }
            Err(error) => {
                return Err(AdolapError::CliError(format!(
                    "Failed to read input: {}",
                    error
                )));
            }
        }
    }

    if let Some(path) = history_path.as_ref() {
        let _ = editor.save_history(path);
    }

    Ok(())
}

fn history_path() -> Option<PathBuf> {
    let home = env::var_os("USERPROFILE").or_else(|| env::var_os("HOME"))?;
    Some(PathBuf::from(home).join(".adolap_history"))
}

fn append_line(buffer: &mut String, line: &str) {
    if !buffer.is_empty() {
        buffer.push('\n');
    }
    buffer.push_str(line);
}

fn query_is_complete(buffer: &str) -> bool {
    buffer.trim_end().ends_with(';')
}

fn finalize_query(buffer: &str) -> String {
    buffer.trim().trim_end_matches(';').trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::{finalize_query, query_is_complete};

    #[test]
    fn detects_terminated_multiline_query() {
        assert!(query_is_complete("FROM analytics.events\nSELECT country;"));
        assert!(!query_is_complete("FROM analytics.events\nSELECT country"));
    }

    #[test]
    fn strips_trailing_semicolons_from_query() {
        assert_eq!(
            finalize_query("\nFROM analytics.events\nSELECT country;;  "),
            "FROM analytics.events\nSELECT country"
        );
    }
}