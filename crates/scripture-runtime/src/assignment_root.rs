//! Deterministic durable-store roots for multi-assignment Scribe.

use scripture::{JournalId, VerseId};

/// Exclusive object-store root for one Canon/Verse under a deployment prefix.
///
/// Format: `{deployment_prefix.trim}/cv/{hex(canon_bytes)}/{hex(verse_bytes)}`
///
/// Roots are derived from Canon/Verse identity only — renaming an operator
/// assignment id does not change the durable root.
#[must_use]
pub fn assignment_durable_root(
    deployment_prefix: &str,
    journal_id: JournalId,
    verse_id: VerseId,
) -> String {
    let prefix = deployment_prefix.trim().trim_end_matches('/');
    format!(
        "{}/cv/{}/{}",
        prefix,
        hex16(&journal_id.as_bytes()),
        hex16(&verse_id.as_bytes()),
    )
}

fn hex16(bytes: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(32);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_canon_verse_derived_not_assignment_id() {
        let journal = JournalId::from_bytes(*b"telemetry-jrnl!!");
        let verse = VerseId::from_bytes(*b"telemetry-host-a");
        let root = assignment_durable_root("scripture/drills/run-1", journal, verse);
        assert_eq!(
            root,
            format!(
                "scripture/drills/run-1/cv/{}/{}",
                hex16(b"telemetry-jrnl!!"),
                hex16(b"telemetry-host-a")
            )
        );
        assert!(!root.contains("assignments/"));
        assert!(!root.contains("telemetry-host-a")); // ASCII id is not a path segment
    }

    #[test]
    fn trims_deployment_prefix() {
        let journal = JournalId::from_bytes(*b"aaaaaaaaaaaaaaaa");
        let verse = VerseId::from_bytes(*b"bbbbbbbbbbbbbbbb");
        let a = assignment_durable_root("  prefix/x/  ", journal, verse);
        let b = assignment_durable_root("prefix/x", journal, verse);
        assert_eq!(a, b);
    }
}
