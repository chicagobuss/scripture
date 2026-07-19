//! OpenMetrics / Prometheus text scrape + parse.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// One parsed OpenMetrics / Prometheus sample.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenMetricsSample {
    /// Metric name.
    pub name: String,
    /// Label set (sorted keys).
    pub labels: BTreeMap<String, String>,
    /// Numeric value.
    pub value: f64,
    /// Optional timestamp millis if present on the line.
    pub timestamp_ms: Option<i64>,
    /// `# TYPE` hint when known (`counter`, `gauge`, `histogram`, `summary`, `untyped`).
    pub metric_type: String,
}

/// Result of parsing an exposition body.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseResult {
    /// Successfully parsed samples.
    pub samples: Vec<OpenMetricsSample>,
    /// Malformed sample lines skipped (counted, not fatal).
    pub unparseable_lines: u64,
}

/// OpenMetrics parse failures (reserved for whole-body fatals; sample lines are counted).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// Truncated or malformed body that must fail closed.
    #[error("malformed openmetrics: {0}")]
    Malformed(String),
}

/// Scrape transport failures.
#[derive(Debug, thiserror::Error)]
pub enum ScrapeError {
    /// Connect / write / read IO.
    #[error("scrape io: {0}")]
    Io(#[from] std::io::Error),
    /// Request timed out.
    #[error("scrape timeout")]
    Timeout,
    /// Body exceeded hard cap.
    #[error("response exceeded max_response_bytes ({0})")]
    Oversized(usize),
    /// HTTP status not 2xx.
    #[error("http status {0}")]
    HttpStatus(u16),
    /// URL could not be parsed for a minimal HTTP GET.
    #[error("bad url: {0}")]
    BadUrl(String),
    /// Response body was not valid UTF-8.
    #[error("response body is not utf-8")]
    NotUtf8,
}

/// Parses Prometheus / OpenMetrics text exposition format.
///
/// Exemplars are ignored. Individual malformed sample lines are counted in
/// [`ParseResult::unparseable_lines`] and skipped — a single odd series must
/// not blank an otherwise healthy scrape. `# HELP` / `# TYPE` / `# EOF` /
/// blank lines are metadata only.
pub fn parse_openmetrics(body: &str) -> Result<ParseResult, ParseError> {
    let mut samples = Vec::new();
    let mut unparseable_lines = 0_u64;
    let mut types: BTreeMap<String, String> = BTreeMap::new();

    for raw in body.lines() {
        let line = raw.trim();
        if line.is_empty() || line == "# EOF" {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut parts = rest.split_whitespace();
            let Some(name) = parts.next() else {
                unparseable_lines += 1;
                continue;
            };
            let Some(ty) = parts.next() else {
                unparseable_lines += 1;
                continue;
            };
            types.insert(name.to_owned(), ty.to_owned());
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        match parse_sample_line(line) {
            Ok(sample) => {
                let metric_type = types
                    .get(&sample.name)
                    .cloned()
                    .unwrap_or_else(|| "untyped".to_owned());
                samples.push(OpenMetricsSample {
                    metric_type,
                    ..sample
                });
            }
            Err(_) => {
                unparseable_lines += 1;
            }
        }
    }
    Ok(ParseResult {
        samples,
        unparseable_lines,
    })
}

fn parse_sample_line(line: &str) -> Result<OpenMetricsSample, String> {
    let line = strip_exemplar_outside_quotes(line);
    let (name_and_labels, value_and_ts) = split_metric_and_value(line)?;
    let (name, labels) = parse_name_labels(name_and_labels)?;
    let (value, timestamp_ms) = parse_value_timestamp(value_and_ts)?;
    Ok(OpenMetricsSample {
        name,
        labels,
        value,
        timestamp_ms,
        metric_type: String::new(),
    })
}

/// Drops an exemplar suffix (` # {…}`) only when the ` # ` is outside quotes.
fn strip_exemplar_outside_quotes(line: &str) -> &str {
    let mut in_quotes = false;
    let mut escaped = false;
    let bytes = line.as_bytes();
    let mut i = 0;
    while i + 2 < bytes.len() {
        let ch = bytes[i] as char;
        if escaped {
            escaped = false;
            i += 1;
            continue;
        }
        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            ' ' if !in_quotes && bytes[i + 1] == b'#' && bytes[i + 2] == b' ' => {
                return &line[..i];
            }
            _ => {}
        }
        i += 1;
    }
    line
}

fn split_metric_and_value(line: &str) -> Result<(&str, &str), String> {
    // Quote-aware scan: find the closing `}` of the label set (if any), then
    // take the remainder as value (+ optional timestamp).
    let mut in_quotes = false;
    let mut escaped = false;
    let mut brace_depth = 0_i32;
    let mut labels_end = None;
    for (idx, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            '{' if !in_quotes => brace_depth += 1,
            '}' if !in_quotes => {
                brace_depth -= 1;
                if brace_depth == 0 {
                    labels_end = Some(idx + ch.len_utf8());
                    break;
                }
            }
            _ => {}
        }
    }
    if brace_depth != 0 {
        return Err("unbalanced label braces".into());
    }

    if let Some(end) = labels_end {
        let name_and_labels = line[..end].trim();
        let value_and_ts = line[end..].trim_start();
        if value_and_ts.is_empty() {
            return Err("missing value".into());
        }
        return Ok((name_and_labels, value_and_ts));
    }

    // No labels: split on first whitespace.
    let mut in_quotes = false;
    let mut escaped = false;
    for (idx, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            ' ' | '\t' if !in_quotes => {
                return Ok((line[..idx].trim(), line[idx + 1..].trim()));
            }
            _ => {}
        }
    }
    Err("missing value".into())
}

fn parse_name_labels(raw: &str) -> Result<(String, BTreeMap<String, String>), String> {
    if let Some(brace) = raw.find('{') {
        let name = raw[..brace].to_owned();
        if !raw.ends_with('}') {
            return Err("unclosed labels".into());
        }
        let inner = &raw[brace + 1..raw.len() - 1];
        let labels = parse_labels(inner)?;
        Ok((name, labels))
    } else {
        Ok((raw.to_owned(), BTreeMap::new()))
    }
}

fn parse_labels(inner: &str) -> Result<BTreeMap<String, String>, String> {
    let mut labels = BTreeMap::new();
    if inner.trim().is_empty() {
        return Ok(labels);
    }
    let mut rest = inner;
    while !rest.is_empty() {
        let eq = rest
            .find('=')
            .ok_or_else(|| "label missing '='".to_owned())?;
        let key = rest[..eq].trim();
        rest = rest[eq + 1..].trim_start();
        if !rest.starts_with('"') {
            return Err("label value must be quoted".into());
        }
        rest = &rest[1..];
        let mut value = String::new();
        let mut chars = rest.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '\\' => match chars.next() {
                    Some('n') => value.push('\n'),
                    Some('\\') => value.push('\\'),
                    Some('"') => value.push('"'),
                    Some(other) => {
                        value.push('\\');
                        value.push(other);
                    }
                    None => return Err("trailing escape".into()),
                },
                '"' => break,
                other => value.push(other),
            }
        }
        let consumed = find_closing_quote(rest)?;
        rest = rest[consumed + 1..].trim_start();
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        }
        labels.insert(key.to_owned(), value);
    }
    Ok(labels)
}

fn find_closing_quote(rest: &str) -> Result<usize, String> {
    let mut escaped = false;
    for (idx, ch) in rest.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '"' => return Ok(idx),
            _ => {}
        }
    }
    Err("unclosed label quote".into())
}

fn parse_value_timestamp(raw: &str) -> Result<(f64, Option<i64>), String> {
    let mut parts = raw.split_whitespace();
    let value_raw = parts.next().ok_or_else(|| "missing value".to_owned())?;
    let value = match value_raw {
        "+Inf" | "Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        "NaN" => f64::NAN,
        other => other
            .parse::<f64>()
            .map_err(|_| format!("bad value {other}"))?,
    };
    let timestamp_ms = match parts.next() {
        Some(ts) => Some(parse_timestamp_to_millis(ts)?),
        None => None,
    };
    if parts.next().is_some() {
        return Err("unexpected trailing tokens".into());
    }
    Ok((value, timestamp_ms))
}

/// Prometheus uses integer milliseconds; OpenMetrics allows float seconds.
fn parse_timestamp_to_millis(raw: &str) -> Result<i64, String> {
    if raw.contains('.') {
        let secs: f64 = raw.parse().map_err(|_| format!("bad timestamp {raw}"))?;
        Ok((secs * 1000.0).round() as i64)
    } else {
        raw.parse::<i64>()
            .map_err(|_| format!("bad timestamp {raw}"))
    }
}

/// Minimal HTTP/1.0 GET against `url`, enforcing `max_response_bytes` and timeout.
///
/// HTTP/1.0 forbids chunked transfer encoding on responses, which keeps the
/// hand-rolled client honest against Go exporters (node-exporter, etc.).
pub async fn scrape_url(
    url: &str,
    max_response_bytes: usize,
    request_timeout: Duration,
) -> Result<String, ScrapeError> {
    let (host, port, path) = parse_http_url(url)?;
    let addr = format!("{host}:{port}");
    let connect = timeout(request_timeout, TcpStream::connect(&addr))
        .await
        .map_err(|_| ScrapeError::Timeout)?
        .map_err(ScrapeError::Io)?;
    let mut stream = connect;
    let request = format!(
        "GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\nAccept: text/plain\r\n\r\n"
    );
    timeout(request_timeout, stream.write_all(request.as_bytes()))
        .await
        .map_err(|_| ScrapeError::Timeout)?
        .map_err(ScrapeError::Io)?;

    let mut buf = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = timeout(request_timeout, stream.read(&mut chunk))
            .await
            .map_err(|_| ScrapeError::Timeout)?
            .map_err(ScrapeError::Io)?;
        if read == 0 {
            break;
        }
        if buf.len() + read > max_response_bytes {
            return Err(ScrapeError::Oversized(max_response_bytes));
        }
        buf.extend_from_slice(&chunk[..read]);
    }

    let text = String::from_utf8(buf).map_err(|_| ScrapeError::NotUtf8)?;
    let (status, body) = split_http_response(&text)?;
    if !(200..300).contains(&status) {
        return Err(ScrapeError::HttpStatus(status));
    }
    Ok(body.to_owned())
}

fn parse_http_url(url: &str) -> Result<(String, u16, String), ScrapeError> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| ScrapeError::BadUrl(url.to_owned()))?;
    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return Err(ScrapeError::BadUrl(url.to_owned()));
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => {
            let port: u16 = port
                .parse()
                .map_err(|_| ScrapeError::BadUrl(url.to_owned()))?;
            (host.to_owned(), port)
        }
        None => (authority.to_owned(), 80),
    };
    Ok((host, port, path.to_owned()))
}

fn split_http_response(text: &str) -> Result<(u16, &str), ScrapeError> {
    let (header, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))
        .ok_or_else(|| ScrapeError::Io(std::io::Error::other("missing http header terminator")))?;
    let status_line = header.lines().next().unwrap_or_default();
    let mut parts = status_line.split_whitespace();
    let _http = parts.next();
    let status: u16 = parts
        .next()
        .ok_or_else(|| ScrapeError::Io(std::io::Error::other("missing status")))?
        .parse()
        .map_err(|_| ScrapeError::Io(std::io::Error::other("bad status")))?;
    Ok((status, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_counter_gauge_and_ignores_exemplar() {
        let body = r#"
# HELP node_cpu_seconds_total Seconds
# TYPE node_cpu_seconds_total counter
node_cpu_seconds_total{cpu="0",mode="idle"} 12.5 # {span_id="abc"} 1.0
# TYPE node_memory_MemAvailable_bytes gauge
node_memory_MemAvailable_bytes 1024
"#;
        let result = parse_openmetrics(body).expect("parse");
        assert_eq!(result.samples.len(), 2);
        assert_eq!(result.unparseable_lines, 0);
        assert_eq!(result.samples[0].name, "node_cpu_seconds_total");
        assert_eq!(
            result.samples[0].labels.get("cpu").map(String::as_str),
            Some("0")
        );
        assert!((result.samples[0].value - 12.5).abs() < f64::EPSILON);
        assert_eq!(result.samples[0].metric_type, "counter");
        assert_eq!(result.samples[1].name, "node_memory_MemAvailable_bytes");
        assert_eq!(result.samples[1].metric_type, "gauge");
    }

    #[test]
    fn quoted_brace_and_hash_in_labels_are_legal() {
        let body = r#"weird{path="a}b",note="x # y"} 1
# TYPE node_cpu_seconds_total counter
node_cpu_seconds_total{cpu="0",mode="idle"} 2
"#;
        let result = parse_openmetrics(body).expect("parse");
        assert_eq!(result.samples.len(), 2);
        assert_eq!(
            result.samples[0].labels.get("path").map(String::as_str),
            Some("a}b")
        );
        assert_eq!(
            result.samples[0].labels.get("note").map(String::as_str),
            Some("x # y")
        );
    }

    #[test]
    fn float_timestamp_seconds_accepted() {
        let body = r#"node_memory_MemAvailable_bytes 1024 1710000000.5
"#;
        let result = parse_openmetrics(body).expect("parse");
        assert_eq!(result.samples.len(), 1);
        assert_eq!(result.samples[0].timestamp_ms, Some(1_710_000_000_500));
    }

    #[test]
    fn malformed_line_is_counted_not_fatal() {
        let body = "not_a_valid_sample\nnode_memory_MemAvailable_bytes 1\n";
        let result = parse_openmetrics(body).expect("parse");
        assert_eq!(result.unparseable_lines, 1);
        assert_eq!(result.samples.len(), 1);
    }
}
