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

/// OpenMetrics parse failures.
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
/// Exemplars are ignored. Malformed sample lines fail the whole scrape closed
/// (no half-parsed garbage). `# HELP` / `# TYPE` / `# EOF` / blank lines are
/// metadata only.
pub fn parse_openmetrics(body: &str) -> Result<Vec<OpenMetricsSample>, ParseError> {
    let mut samples = Vec::new();
    let mut types: BTreeMap<String, String> = BTreeMap::new();

    for (line_no, raw) in body.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line == "# EOF" {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut parts = rest.split_whitespace();
            let name = parts
                .next()
                .ok_or_else(|| ParseError::Malformed(format!("type line {line_no}")))?;
            let ty = parts
                .next()
                .ok_or_else(|| ParseError::Malformed(format!("type line {line_no}")))?;
            types.insert(name.to_owned(), ty.to_owned());
            continue;
        }
        if line.starts_with('#') {
            // HELP / UNIT / comments — ignore.
            continue;
        }
        let sample = parse_sample_line(line)
            .map_err(|message| ParseError::Malformed(format!("line {}: {message}", line_no + 1)))?;
        let metric_type = types
            .get(&sample.name)
            .cloned()
            .unwrap_or_else(|| "untyped".to_owned());
        samples.push(OpenMetricsSample {
            metric_type,
            ..sample
        });
    }
    Ok(samples)
}

fn parse_sample_line(line: &str) -> Result<OpenMetricsSample, String> {
    // Drop exemplars: `... # {…} value`
    let line = match line.find(" # ") {
        Some(idx) => &line[..idx],
        None => line,
    };

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

fn split_metric_and_value(line: &str) -> Result<(&str, &str), String> {
    // Value starts at the last whitespace-separated token group. Labels may
    // contain spaces inside quotes, so walk carefully.
    let mut in_quotes = false;
    let mut last_space = None;
    for (idx, ch) in line.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ' ' | '\t' if !in_quotes => last_space = Some(idx),
            _ => {}
        }
    }
    // May have "value" or "value timestamp". Find the first space after labels.
    let brace_end = line.find('}').map(|idx| idx + 1).unwrap_or(0);
    let search_from = if line.contains('{') { brace_end } else { 0 };
    let rest = line
        .get(search_from..)
        .ok_or_else(|| "truncated sample".to_owned())?;
    let trimmed = rest.trim_start();
    if trimmed.is_empty() {
        // Name-only before value — use last_space path for no-label metrics.
        let space = last_space.ok_or_else(|| "missing value".to_owned())?;
        return Ok((line[..space].trim(), line[space + 1..].trim()));
    }
    if line.contains('{') {
        let name_and_labels = line[..search_from].trim();
        Ok((name_and_labels, trimmed))
    } else {
        let space = last_space.ok_or_else(|| "missing value".to_owned())?;
        Ok((line[..space].trim(), line[space + 1..].trim()))
    }
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
        // Advance rest past consumed value.
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
        Some(ts) => Some(
            ts.parse::<i64>()
                .map_err(|_| format!("bad timestamp {ts}"))?,
        ),
        None => None,
    };
    if parts.next().is_some() {
        return Err("unexpected trailing tokens".into());
    }
    Ok((value, timestamp_ms))
}

/// Minimal HTTP/1.1 GET against `url`, enforcing `max_response_bytes` and timeout.
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
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nAccept: text/plain\r\n\r\n"
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
        let samples = parse_openmetrics(body).expect("parse");
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].name, "node_cpu_seconds_total");
        assert_eq!(samples[0].labels.get("cpu").map(String::as_str), Some("0"));
        assert!((samples[0].value - 12.5).abs() < f64::EPSILON);
        assert_eq!(samples[0].metric_type, "counter");
        assert_eq!(samples[1].name, "node_memory_MemAvailable_bytes");
        assert_eq!(samples[1].metric_type, "gauge");
    }

    #[test]
    fn malformed_line_fails_closed() {
        let body = "not_a_valid_sample\n";
        assert!(parse_openmetrics(body).is_err());
    }
}
