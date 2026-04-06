use crate::client::ClientResponse;
use crate::meta::{OutputFormat, ReplSettings};
use protocol::{MetaResult, QueryResult, ResultSet, ServerMessage};
use std::time::Duration;
use unicode_width::UnicodeWidthStr;

pub fn print_response(response: ClientResponse, settings: &ReplSettings) {
    let ClientResponse {
        message,
        elapsed,
        response_bytes,
    } = response;

    let is_query_result = matches!(message, ServerMessage::QueryResult(_));

    match message {
        ServerMessage::QueryResult(query_result) => {
            print_query_result(&query_result, elapsed, response_bytes, settings)
        }
        ServerMessage::MetaResult(meta_result) => print_meta_result(&meta_result),
        ServerMessage::Ok(message) => println!("{}", message),
        ServerMessage::Error(message) => eprintln!("Error: {}", message),
        ServerMessage::Pong => println!("PONG"),
    }

    if settings.show_profile && !is_query_result {
        println!(
            "profile: elapsed={:.3} ms, response_bytes={}",
            elapsed.as_secs_f64() * 1000.0,
            response_bytes
        );
    } else if settings.show_timing && !is_query_result {
        println!("time: {:.3} ms", elapsed.as_secs_f64() * 1000.0);
    }
}

pub fn print_query_result(
    query_result: &QueryResult,
    elapsed: Duration,
    response_bytes: usize,
    settings: &ReplSettings,
) {
    println!("{}", render_query_result(query_result, elapsed, response_bytes, settings));
}

pub fn render_query_result(
    query_result: &QueryResult,
    elapsed: Duration,
    response_bytes: usize,
    settings: &ReplSettings,
) -> String {
    let mut sections = Vec::new();
    sections.push(render_section(
        "Query Summary",
        &render_query_summary(query_result, elapsed, response_bytes, settings),
    ));
    if settings.show_plan {
        sections.push(render_section("Logical Plan", &query_result.logical_plan));
        sections.push(render_section("Physical Plan", &query_result.physical_plan));
    }
    sections.push(render_section(
        "Query Results",
        &render_result_set(&query_result.result_set, settings.output_format),
    ));
    sections.join("\n\n")
}

pub fn render_result_set(result_set: &ResultSet, format: OutputFormat) -> String {
    match format {
        OutputFormat::Unicode => render_box_table(result_set, BoxStyle::unicode()),
        OutputFormat::Ascii => render_box_table(result_set, BoxStyle::ascii()),
        OutputFormat::Csv => render_delimited(result_set, ","),
        OutputFormat::Tsv => render_delimited(result_set, "\t"),
    }
}

fn print_meta_result(meta_result: &MetaResult) {
    println!("{}", render_meta_result(meta_result));
}

fn render_meta_result(meta_result: &MetaResult) -> String {
    format!("{}\n{}", meta_result.title, meta_result.content)
}

fn render_box_table(result_set: &ResultSet, style: BoxStyle) -> String {
    if result_set.columns.is_empty() {
        return format!("({} rows)", result_set.rows.len());
    }

    let widths = column_widths(result_set);
    let top = render_border(
        &widths,
        style.top_left,
        style.top_mid,
        style.top_right,
        style.horizontal,
    );
    let middle = render_border(
        &widths,
        style.mid_left,
        style.mid_mid,
        style.mid_right,
        style.horizontal,
    );
    let bottom = render_border(
        &widths,
        style.bottom_left,
        style.bottom_mid,
        style.bottom_right,
        style.horizontal,
    );
    let header = render_row(&result_set.columns, &widths, style.vertical);
    let mut lines = Vec::new();

    lines.push(top);
    lines.push(header);
    lines.push(middle);

    for row in &result_set.rows {
        lines.push(render_row(&row.values, &widths, style.vertical));
    }

    lines.push(bottom);
    lines.push(format!("({} {})", result_set.rows.len(), row_label(result_set.rows.len())));
    lines.join("\n")
}

fn render_delimited(result_set: &ResultSet, delimiter: &str) -> String {
    let mut lines = Vec::new();
    lines.push(result_set.columns.join(delimiter));
    for row in &result_set.rows {
        lines.push(row.values.join(delimiter));
    }
    lines.push(format!("({} {})", result_set.rows.len(), row_label(result_set.rows.len())));
    lines.join("\n")
}

fn render_section(title: &str, content: &str) -> String {
    format!("=== {} ===\n{}", title, content)
}

fn render_query_summary(
    query_result: &QueryResult,
    elapsed: Duration,
    response_bytes: usize,
    settings: &ReplSettings,
) -> String {
    let mut lines = vec![
        format!("rows: {}", query_result.summary.row_count),
        format!("columns: {}", query_result.summary.column_count),
        format!("batches: {}", query_result.summary.batch_count),
        format!("server_execution_ms: {:.3}", query_result.summary.execution_time_ms),
    ];

    if settings.show_timing || settings.show_profile {
        lines.push(format!("client_round_trip_ms: {:.3}", elapsed.as_secs_f64() * 1000.0));
    }

    if settings.show_profile {
        lines.push(format!("response_bytes: {}", response_bytes));
    }

    lines.join("\n")
}

fn column_widths(result_set: &ResultSet) -> Vec<usize> {
    let mut widths = result_set
        .columns
        .iter()
        .map(|column| display_width(column))
        .collect::<Vec<_>>();

    for row in &result_set.rows {
        for (index, value) in row.values.iter().enumerate() {
            if let Some(width) = widths.get_mut(index) {
                *width = (*width).max(display_width(value));
            }
        }
    }

    widths
}

fn render_border(
    widths: &[usize],
    left: &str,
    middle: &str,
    right: &str,
    horizontal: &str,
) -> String {
    let mut border = String::new();
    border.push_str(left);
    for (index, width) in widths.iter().enumerate() {
        border.push_str(&horizontal.repeat(*width + 2));
        border.push_str(if index + 1 == widths.len() { right } else { middle });
    }
    border
}

fn render_row(values: &[String], widths: &[usize], vertical: &str) -> String {
    let mut row = String::from(vertical);
    for (index, width) in widths.iter().enumerate() {
        let value = values.get(index).map(String::as_str).unwrap_or("");
        let padding = width.saturating_sub(display_width(value));
        row.push(' ');
        row.push_str(value);
        row.push_str(&" ".repeat(padding + 1));
        row.push_str(vertical);
    }
    row
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

fn row_label(count: usize) -> &'static str {
    if count == 1 {
        "row"
    } else {
        "rows"
    }
}

#[derive(Clone, Copy)]
struct BoxStyle {
    top_left: &'static str,
    top_mid: &'static str,
    top_right: &'static str,
    mid_left: &'static str,
    mid_mid: &'static str,
    mid_right: &'static str,
    bottom_left: &'static str,
    bottom_mid: &'static str,
    bottom_right: &'static str,
    horizontal: &'static str,
    vertical: &'static str,
}

impl BoxStyle {
    fn unicode() -> Self {
        Self {
            top_left: "┌",
            top_mid: "┬",
            top_right: "┐",
            mid_left: "├",
            mid_mid: "┼",
            mid_right: "┤",
            bottom_left: "└",
            bottom_mid: "┴",
            bottom_right: "┘",
            horizontal: "─",
            vertical: "│",
        }
    }

    fn ascii() -> Self {
        Self {
            top_left: "+",
            top_mid: "+",
            top_right: "+",
            mid_left: "+",
            mid_mid: "+",
            mid_right: "+",
            bottom_left: "+",
            bottom_mid: "+",
            bottom_right: "+",
            horizontal: "-",
            vertical: "|",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{render_query_result, render_result_set};
    use crate::meta::{OutputFormat, ReplSettings};
    use protocol::{QueryResult, QuerySummary, ResultRow, ResultSet};
    use std::time::Duration;

    #[test]
    fn renders_unicode_table() {
        let result = render_result_set(
            &ResultSet {
                columns: vec!["country".into(), "count".into()],
                rows: vec![
                    ResultRow {
                        values: vec!["US".into(), "10".into()],
                    },
                    ResultRow {
                        values: vec!["CA".into(), "2".into()],
                    },
                ],
            },
            OutputFormat::Unicode,
        );

        assert!(result.contains("│ country │ count │"));
        assert!(result.contains("┌"));
        assert!(result.contains("(2 rows)"));
    }

    #[test]
    fn renders_ascii_table() {
        let result = render_result_set(
            &ResultSet {
                columns: vec!["country".into()],
                rows: vec![ResultRow {
                    values: vec!["US".into()],
                }],
            },
            OutputFormat::Ascii,
        );

        assert!(result.contains("| country |"));
    }

    #[test]
    fn renders_csv_table() {
        let result = render_result_set(
            &ResultSet {
                columns: vec!["country".into(), "count".into()],
                rows: vec![ResultRow {
                    values: vec!["US".into(), "10".into()],
                }],
            },
            OutputFormat::Csv,
        );

        assert!(result.contains("country,count"));
    }

    #[test]
    fn renders_query_plans_with_result() {
        let output = render_query_result(
            &QueryResult {
                result_set: ResultSet {
                    columns: vec!["country".into()],
                    rows: vec![ResultRow {
                        values: vec!["US".into()],
                    }],
                },
                logical_plan: "Scan(events) -> Project(country)".into(),
                physical_plan: "Scan(table=default.events) -> Project(country)".into(),
                summary: QuerySummary::new(1, 1, 1, 1.5),
            },
            Duration::from_millis(2),
            128,
            &ReplSettings::default(),
        );

        assert!(output.contains("=== Query Summary ==="));
        assert!(output.contains("=== Logical Plan ==="));
        assert!(output.contains("=== Physical Plan ==="));
        assert!(output.contains("=== Query Results ==="));
        assert!(output.contains("Scan(events) -> Project(country)"));
        assert!(output.contains("server_execution_ms: 1.500"));
    }

    #[test]
    fn hides_plans_when_disabled() {
        let mut settings = ReplSettings::default();
        settings.show_plan = false;

        let output = render_query_result(
            &QueryResult {
                result_set: ResultSet {
                    columns: vec!["country".into()],
                    rows: vec![ResultRow {
                        values: vec!["US".into()],
                    }],
                },
                logical_plan: "Scan(events) -> Project(country)".into(),
                physical_plan: "Scan(table=default.events) -> Project(country)".into(),
                summary: QuerySummary::new(1, 1, 1, 1.5),
            },
            Duration::from_millis(2),
            128,
            &settings,
        );

        assert!(!output.contains("Logical Plan"));
    }

    #[test]
    fn profile_summary_includes_client_metrics() {
        let mut settings = ReplSettings::default();
        settings.show_profile = true;

        let output = render_query_result(
            &QueryResult {
                result_set: ResultSet {
                    columns: vec!["country".into()],
                    rows: vec![ResultRow {
                        values: vec!["US".into()],
                    }],
                },
                logical_plan: "Scan(events)".into(),
                physical_plan: "Scan(table=default.events)".into(),
                summary: QuerySummary::new(1, 1, 1, 1.25),
            },
            Duration::from_millis(5),
            512,
            &settings,
        );

        assert!(output.contains("client_round_trip_ms: 5.000"));
        assert!(output.contains("response_bytes: 512"));
    }
}