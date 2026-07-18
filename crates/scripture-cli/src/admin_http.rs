//! Strict admin HTTP promote request parsing (privileged surface).

use serde::Deserialize;

/// Maximum accepted admin request size (headers + body).
pub const MAX_ADMIN_REQUEST_BYTES: usize = 4096;

/// Parsed promote request after authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromoteRequest {
    /// Candidate writer term (must be >= 1).
    pub candidate_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminParseError {
    /// Truncated or empty request.
    Incomplete,
    /// Method/path not POST /v1/promote.
    NotFound,
    /// Missing or wrong bearer token.
    Unauthorized,
    /// Malformed headers/body/JSON.
    BadRequest(&'static str),
}

impl std::fmt::Display for AdminParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete request"),
            Self::NotFound => write!(f, "not found"),
            Self::Unauthorized => write!(f, "unauthorized"),
            Self::BadRequest(message) => write!(f, "{message}"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PromoteBody {
    candidate_term: u64,
}

/// Authenticate and strictly decode a promote request from raw HTTP bytes.
///
/// Requires a complete header block ending in `\r\n\r\n`, exact Content-Length
/// body bytes, case-insensitive `Authorization: Bearer`, and exact JSON object
/// decode with no trailing bytes.
pub fn parse_promote_request(
    raw: &[u8],
    expected_token: &str,
) -> Result<PromoteRequest, AdminParseError> {
    if raw.is_empty() {
        return Err(AdminParseError::Incomplete);
    }
    if raw.len() > MAX_ADMIN_REQUEST_BYTES {
        return Err(AdminParseError::BadRequest("request too large"));
    }
    let text = std::str::from_utf8(raw).map_err(|_| AdminParseError::BadRequest("not utf-8"))?;
    let (header_block, body) = split_headers_body(text)?;
    let mut lines = header_block.lines();
    let request_line = lines.next().ok_or(AdminParseError::Incomplete)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if method != "POST" || path != "/v1/promote" {
        return Err(AdminParseError::NotFound);
    }

    let mut content_length: Option<usize> = None;
    let mut bearer: Option<&str> = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(AdminParseError::BadRequest("malformed header"));
        };
        let name = name.trim();
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| AdminParseError::BadRequest("bad content-length"))?;
            content_length = Some(parsed);
        } else if name.eq_ignore_ascii_case("authorization") {
            let Some(token) = value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
            else {
                return Err(AdminParseError::Unauthorized);
            };
            bearer = Some(token.trim());
        }
    }

    let Some(token) = bearer else {
        return Err(AdminParseError::Unauthorized);
    };
    if !tokens_equal(token.as_bytes(), expected_token.as_bytes()) {
        return Err(AdminParseError::Unauthorized);
    }

    let Some(length) = content_length else {
        return Err(AdminParseError::BadRequest("content-length required"));
    };
    if body.len() < length {
        return Err(AdminParseError::Incomplete);
    }
    if body.len() > length {
        // Reject trailing bytes after the declared body.
        return Err(AdminParseError::BadRequest("trailing bytes after body"));
    }
    let body = &body[..length];
    let parsed: PromoteBody = serde_json::from_str(body)
        .map_err(|_| AdminParseError::BadRequest("body must be exact JSON object"))?;
    // serde_json::from_str already rejects trailing junk for Value; for structs
    // it also requires full consumption of the input.
    if parsed.candidate_term == 0 {
        return Err(AdminParseError::BadRequest("candidate_term must be >= 1"));
    }
    Ok(PromoteRequest {
        candidate_term: parsed.candidate_term,
    })
}

fn split_headers_body(text: &str) -> Result<(&str, &str), AdminParseError> {
    if let Some(idx) = text.find("\r\n\r\n") {
        return Ok((&text[..idx], &text[idx + 4..]));
    }
    Err(AdminParseError::Incomplete)
}

fn tokens_equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left, right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(auth: &str, body: &str) -> Vec<u8> {
        format!(
            "POST /v1/promote HTTP/1.1\r\nAuthorization: {auth}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[test]
    fn accepts_exact_authorized_json() {
        let raw = request("Bearer secret-token", r#"{"candidate_term":2}"#);
        let parsed = parse_promote_request(&raw, "secret-token").expect("ok");
        assert_eq!(parsed.candidate_term, 2);
    }

    #[test]
    fn authorization_header_is_case_insensitive() {
        let body = r#"{"candidate_term":3}"#;
        let raw = format!(
            "POST /v1/promote HTTP/1.1\r\naUtHoRiZaTiOn: Bearer secret-token\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        parse_promote_request(raw.as_bytes(), "secret-token").expect("ok");
    }

    #[test]
    fn rejects_missing_bearer() {
        let body = r#"{"candidate_term":2}"#;
        let raw = format!(
            "POST /v1/promote HTTP/1.1\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert_eq!(
            parse_promote_request(raw.as_bytes(), "secret-token"),
            Err(AdminParseError::Unauthorized)
        );
    }

    #[test]
    fn rejects_wrong_bearer() {
        let raw = request("Bearer other", r#"{"candidate_term":2}"#);
        assert_eq!(
            parse_promote_request(&raw, "secret-token"),
            Err(AdminParseError::Unauthorized)
        );
    }

    #[test]
    fn rejects_trailing_json_garbage() {
        let raw = request("Bearer secret-token", r#"{"candidate_term":2}garbage"#);
        assert!(matches!(
            parse_promote_request(&raw, "secret-token"),
            Err(AdminParseError::BadRequest(_))
        ));
    }

    #[test]
    fn rejects_prefix_json_without_full_object_end() {
        // Content-Length stops before a valid object end → incomplete/invalid JSON.
        let body = r#"{"candidate_term":2"#;
        let raw = request("Bearer secret-token", body);
        assert!(matches!(
            parse_promote_request(&raw, "secret-token"),
            Err(AdminParseError::BadRequest(_))
        ));
    }

    #[test]
    fn rejects_zero_term() {
        let raw = request("Bearer secret-token", r#"{"candidate_term":0}"#);
        assert_eq!(
            parse_promote_request(&raw, "secret-token"),
            Err(AdminParseError::BadRequest("candidate_term must be >= 1"))
        );
    }

    #[test]
    fn rejects_missing_content_length() {
        let raw = b"POST /v1/promote HTTP/1.1\r\nAuthorization: Bearer secret-token\r\n\r\n{\"candidate_term\":2}";
        assert!(matches!(
            parse_promote_request(raw, "secret-token"),
            Err(AdminParseError::BadRequest(_))
        ));
    }

    #[test]
    fn rejects_wrong_path() {
        let body = r#"{"candidate_term":2}"#;
        let raw = format!(
            "POST /v1/other HTTP/1.1\r\nAuthorization: Bearer secret-token\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert_eq!(
            parse_promote_request(raw.as_bytes(), "secret-token"),
            Err(AdminParseError::NotFound)
        );
    }

    #[test]
    fn rejects_incomplete_body() {
        let raw = b"POST /v1/promote HTTP/1.1\r\nAuthorization: Bearer secret-token\r\nContent-Length: 40\r\n\r\n{\"candidate_term\":2}";
        assert_eq!(
            parse_promote_request(raw, "secret-token"),
            Err(AdminParseError::Incomplete)
        );
    }
}
