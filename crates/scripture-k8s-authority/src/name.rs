//! Deterministic DNS-1123 object names derived from [`AuthorityKey`].

use scripture::serving_authority::AuthorityKey;

/// Domain separation context for Serving Authority object-name digests.
pub const NAME_DOMAIN: &str = "scripture.dev/serving-authority/object-name/v1";

/// Fixed authority-key bytes hashed for the object name: journal ∥ verse.
#[must_use]
pub fn authority_key_fixed_bytes(key: &AuthorityKey) -> [u8; 32] {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(&key.journal_id.as_bytes());
    bytes[16..].copy_from_slice(&key.verse_id.as_bytes());
    bytes
}

/// Derives a DNS-1123 subdomain name uniquely naming one AuthorityKey object.
///
/// Format: `sa-` + lowercase hex of the first 20 digest bytes (43 characters).
#[must_use]
pub fn authority_object_name(key: &AuthorityKey) -> String {
    let mut hasher = blake3::Hasher::new_derive_key(NAME_DOMAIN);
    hasher.update(&authority_key_fixed_bytes(key));
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(40);
    for byte in digest.as_bytes().iter().take(20) {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    format!("sa-{hex}")
}

/// Returns true when `name` is a valid Kubernetes DNS subdomain (RFC 1123).
#[must_use]
pub fn is_dns1123_subdomain(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_lowercase() && !bytes[0].is_ascii_digit() {
        return false;
    }
    if !bytes[bytes.len() - 1].is_ascii_lowercase() && !bytes[bytes.len() - 1].is_ascii_digit() {
        return false;
    }
    bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

#[cfg(test)]
mod tests {
    use scripture::canon::VerseId;
    use scripture::model::JournalId;
    use scripture::serving_authority::AuthorityKey;

    use super::*;

    fn key(journal: &[u8; 16], verse: &[u8; 16]) -> AuthorityKey {
        AuthorityKey {
            journal_id: JournalId::from_bytes(*journal),
            verse_id: VerseId::from_bytes(*verse),
        }
    }

    #[test]
    fn name_changes_with_journal_or_verse_and_is_dns_safe() {
        let a = key(b"journal-aaaa-id!", b"verse-aaaa-id!!!");
        let b = key(b"journal-bbbb-id!", b"verse-aaaa-id!!!");
        let c = key(b"journal-aaaa-id!", b"verse-bbbb-id!!!");
        let na = authority_object_name(&a);
        let nb = authority_object_name(&b);
        let nc = authority_object_name(&c);
        assert_ne!(na, nb);
        assert_ne!(na, nc);
        assert_ne!(nb, nc);
        assert!(is_dns1123_subdomain(&na));
        assert!(na.starts_with("sa-"));
        assert_eq!(na.len(), 43);
        assert_eq!(authority_object_name(&a), na);
    }
}
