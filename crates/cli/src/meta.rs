use crate::client::{Client, ClientResponse};
use adolap_core::error::AdolapError;
use std::fmt::Write as _;
use sysinfo::{ProcessesToUpdate, System};
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Unicode,
    Ascii,
    Csv,
    Tsv,
}

impl OutputFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            OutputFormat::Unicode => "unicode",
            OutputFormat::Ascii => "ascii",
            OutputFormat::Csv => "csv",
            OutputFormat::Tsv => "tsv",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplSettings {
    pub show_timing: bool,
    pub show_plan: bool,
    pub show_profile: bool,
    pub output_format: OutputFormat,
}

impl Default for ReplSettings {
    fn default() -> Self {
        Self {
            show_timing: false,
            show_plan: true,
            show_profile: false,
            output_format: OutputFormat::Unicode,
        }
    }
}

pub enum MetaCommandResult {
    Continue(Option<ClientResponse>),
    Exit,
}

pub async fn handle_meta_command(
    line: &str,
    client: &mut Client,
    settings: &mut ReplSettings,
) -> Result<MetaCommandResult, AdolapError> {
    let body = line
        .trim()
        .strip_prefix('\\')
        .ok_or_else(|| AdolapError::CliError("Meta commands must start with '\\'".into()))?
        .trim();

    if body.is_empty() {
        println!("{}", help_text());
        return Ok(MetaCommandResult::Continue(None));
    }

    let mut parts = body.split_whitespace();
    let command = parts.next().unwrap_or_default().to_ascii_lowercase();
    let args = parts.collect::<Vec<_>>();
    debug!(meta_command = %command, arg_count = args.len(), "handling cli meta command");

    match command.as_str() {
        "q" | "quit" => Ok(MetaCommandResult::Exit),
        "?" | "help" | "commands" => {
            println!("{}", help_text());
            Ok(MetaCommandResult::Continue(None))
        }
        "version" => {
            println!("Adolap CLI {}", env!("CARGO_PKG_VERSION"));
            Ok(MetaCommandResult::Continue(None))
        }
        "about" => {
            println!("{}", about_text());
            Ok(MetaCommandResult::Continue(None))
        }
        "timing" => {
            let updated = update_toggle("timing", &args, &mut settings.show_timing)?;
            info!(show_timing = settings.show_timing, "updated timing setting");
            println!("{}", updated);
            Ok(MetaCommandResult::Continue(None))
        }
        "plan" => {
            let updated = update_toggle("plan", &args, &mut settings.show_plan)?;
            info!(show_plan = settings.show_plan, "updated plan setting");
            println!("{}", updated);
            Ok(MetaCommandResult::Continue(None))
        }
        "profile" => {
            let updated = update_toggle("profile", &args, &mut settings.show_profile)?;
            info!(show_profile = settings.show_profile, "updated profile setting");
            println!("{}", updated);
            Ok(MetaCommandResult::Continue(None))
        }
        "format" => {
            let updated = update_format(&args, settings)?;
            info!(format = settings.output_format.as_str(), "updated output format");
            println!("{}", updated);
            Ok(MetaCommandResult::Continue(None))
        }
        "memory" => {
            println!("{}", memory_usage_text()?);
            Ok(MetaCommandResult::Continue(None))
        }
        "l" | "databases" => Ok(MetaCommandResult::Continue(Some(
            client.send_meta_command("databases").await?,
        ))),
        "dt" | "tables" => {
            let command = if args.is_empty() {
                "tables".to_string()
            } else {
                format!("tables {}", args.join(" "))
            };
            Ok(MetaCommandResult::Continue(Some(client.send_meta_command(&command).await?)))
        }
        "d" | "schema" | "segments" | "storage" | "stats" => {
            let target = args.join(" ");
            if target.is_empty() {
                return Err(AdolapError::CliError(format!("\\{} requires a table reference", command)));
            }
            let remote = match command.as_str() {
                "d" | "schema" => format!("schema {}", target),
                "segments" => format!("segments {}", target),
                "storage" => format!("storage {}", target),
                "stats" => format!("stats {}", target),
                _ => unreachable!(),
            };
            Ok(MetaCommandResult::Continue(Some(client.send_meta_command(&remote).await?)))
        }
        "optimize" | "vacuum" => {
            let target = args.join(" ");
            if target.is_empty() {
                return Err(AdolapError::CliError(format!("\\{} requires a table reference", command)));
            }
            Ok(MetaCommandResult::Continue(Some(
                client.send_meta_command(&format!("{} {}", command, target)).await?,
            )))
        }
        "explain" => {
            let query = body["explain".len()..].trim();
            if query.is_empty() {
                return Err(AdolapError::CliError("\\explain requires an inline query".into()));
            }
            Ok(MetaCommandResult::Continue(Some(
                client.send_meta_command(&format!("explain {}", query)).await?,
            )))
        }
        other => Err(AdolapError::CliError(format!(
            "Unknown meta command: \\{} (use \\help)",
            other
        ))),
    }
}

fn update_toggle(name: &str, args: &[&str], current: &mut bool) -> Result<String, AdolapError> {
    if let Some(value) = args.first() {
        *current = parse_on_off(value)?;
    }
    Ok(format!("{} {}", name, if *current { "on" } else { "off" }))
}

fn update_format(args: &[&str], settings: &mut ReplSettings) -> Result<String, AdolapError> {
    if let Some(value) = args.first() {
        settings.output_format = match value.to_ascii_lowercase().as_str() {
            "unicode" => OutputFormat::Unicode,
            "ascii" => OutputFormat::Ascii,
            "csv" => OutputFormat::Csv,
            "tsv" => OutputFormat::Tsv,
            other => return Err(AdolapError::CliError(format!("Unknown output format: {}", other))),
        };
    }
    Ok(format!("format {}", settings.output_format.as_str()))
}

fn parse_on_off(value: &str) -> Result<bool, AdolapError> {
    match value.to_ascii_lowercase().as_str() {
        "on" => Ok(true),
        "off" => Ok(false),
        other => Err(AdolapError::CliError(format!("Expected 'on' or 'off', got '{}'", other))),
    }
}

fn memory_usage_text() -> Result<String, AdolapError> {
    let mut system = System::new_all();
    let pid = sysinfo::get_current_pid()
        .map_err(|error| AdolapError::CliError(format!("Failed to inspect process id: {}", error)))?;
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    let process = system
        .process(pid)
        .ok_or_else(|| AdolapError::CliError("Current process not found in sysinfo".into()))?;

    Ok(format!(
        "CLI memory usage\n  rss_bytes: {}\n  virtual_bytes: {}",
        process.memory(),
        process.virtual_memory()
    ))
}

fn about_text() -> String {
    format!(
        "Adolap CLI {}\nA lightweight interactive shell for Adolap over TCP with multiline queries, meta commands, and planner visibility.",
        env!("CARGO_PKG_VERSION")
    )
}

fn help_text() -> String {
    let mut help = String::new();
    let commands = [
        "\\q, \\quit                       Quit the session",
        "\\l, \\databases                  List databases",
        "\\dt, \\tables [database]         List tables, optionally filtered by database",
        "\\d, \\schema <db.table>          Show table schema",
        "\\segments <db.table>             Show segment metadata for a table",
        "\\storage <db.table>              Show table storage configuration",
        "\\stats <db.table>                Show table and column stats",
        "\\optimize <db.table>             Compact segments and row groups for a table",
        "\\vacuum <db.table>               Remove stale compaction leftovers for a table",
        "\\explain <query>                 Build logical and physical plans without executing",
        "\\plan [on|off]                   Toggle planner log display for query results",
        "\\profile [on|off]                Toggle response-size and duration profiling footer",
        "\\timing [on|off]                 Toggle query timing footer",
        "\\format [unicode|ascii|csv|tsv]  Set query result output format",
        "\\memory                          Show CLI memory usage",
        "\\version                         Show CLI version",
        "\\about                           Show CLI about text",
        "\\help, \\?, \\commands           List all meta commands",
    ];

    let _ = writeln!(&mut help, "Adolap CLI meta commands");
    for command in commands {
        let _ = writeln!(&mut help, "{}", command);
    }
    help.trim_end().to_string()
}

#[cfg(test)]
mod tests {
    use super::{about_text, help_text, OutputFormat, ReplSettings};

    #[test]
    fn help_lists_meta_commands() {
        let help = help_text();
        assert!(help.contains("\\tables"));
        assert!(help.contains("\\stats"));
        assert!(help.contains("\\format"));
    }

    #[test]
    fn about_mentions_version() {
        assert!(about_text().contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn default_settings_use_unicode_and_plan_output() {
        let settings = ReplSettings::default();
        assert_eq!(settings.output_format, OutputFormat::Unicode);
        assert!(settings.show_plan);
    }
}