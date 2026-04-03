use crate::client::ClientResponse;
use crate::meta::{OutputFormat, ReplSettings};
use protocol::{MetaResult, QueryResult, ResultSet, ServerMessage};
use unicode_width::UnicodeWidthStr;

pub fn print_response(response: ClientResponse, settings: &ReplSettings) {
    match response.message {
        ServerMessage::QueryResult(query_result) => print_query_result(&query_result, settings),
        ServerMessage::MetaResult(meta_result) => print_meta_result(&meta_result),
        ServerMessage::Ok(message) => println!("{}", message),
        ServerMessage::Error(message) => eprintln!("Error: {}", message),
        ServerMessage::Pong => println!("PONG"),
    }

    if settings.show_profile {
        println!(
            "profile: elapsed={:.3} ms, response_bytes={}",
            response.elapsed.as_secs_f64() * 1000.0,
            response.response_bytes
        );
    } else if settings.show_timing {
        println!("time: {:.3} ms", response.elapsed.as_secs_f64() * 1000.0);
    }
}

pub fn print_query_result(query_result: &QueryResult, settings: &ReplSettings) {
    println!("{}", render_query_result(query_result, settings));
}

pub fn render_query_result(query_result: &QueryResult, settings: &ReplSettings) -> String {
    let mut sections = Vec::new();
    if settings.show_plan {
        sections.push(render_plan("Logical Plan", &query_result.logical_plan));
        sections.push(render_plan("Physical Plan", &query_result.physical_plan));
    }
    sections.push(render_result_set(&query_result.result_set, settings.output_format));
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

fn render_plan(title: &str, content: &str) -> String {
    format!("{}\n{}", title, content)
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
    use protocol::{QueryResult, ResultRow, ResultSet};

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
                logical_plan: "LogicalPlan::Project".into(),
                physical_plan: "PhysicalPlan::Project".into(),
            },
            &ReplSettings::default(),
        );

        assert!(output.contains("Logical Plan"));
        assert!(output.contains("Physical Plan"));
        assert!(output.contains("LogicalPlan::Project"));
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
                logical_plan: "LogicalPlan::Project".into(),
                physical_plan: "PhysicalPlan::Project".into(),
            },
            &settings,
        );

        assert!(!output.contains("Logical Plan"));
    }
}