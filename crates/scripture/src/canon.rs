//! Scripture-owned schema for the opaque Holylog application fence.
//!
//! A [`CanonFence`] is intentionally a compact, deterministic record. Holylog
//! stores it atomically with a VirtualLog membership transition but never
//! interprets it. This module does not grant ownership by itself: a service
//! must still obtain a fresh linearizable register observation and respect the
//! Holylog seal fence before it accepts an append.

use std::fmt;

use holylog::virtual_log::{ApplicationFence, VirtualLog, VirtualLogError, VirtualLogState};

use crate::model::JournalId;

const MAGIC: &[u8; 4] = b"SCNF";
const FORMAT_VERSION: u8 = 1;
const UNOWNED: u8 = 0;
const OWNED: u8 = 1;
const MAX_ENDPOINT_BYTES: usize = 1024;
const FIXED_PREFIX_BYTES: usize = 4 + 1 + 8 + 16 + 16 + 1;
const OWNED_FIXED_BYTES: usize = 16 + 2;

/// Stable identity of a physical ordered Scripture append lane.
///
/// A Line is replaceable physical realization of a logical Scripture. It is
/// deliberately distinct from [`JournalId`], which names the logical journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LineId([u8; 16]);

impl LineId {
    /// Constructs a Line identity from its durable 128-bit representation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the durable 128-bit representation.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Display for LineId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Stable identity of a Scripture process allowed to own a Line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerId([u8; 16]);

impl OwnerId {
    /// Constructs an owner identity from its durable 128-bit representation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the durable 128-bit representation.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Display for OwnerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Reachable endpoint advertised for one fenced owner.
///
/// Scripture deliberately does not impose a transport scheme here. A caller
/// may use a DNS name, service-discovery target, or protocol URL; discovery is
/// still advisory and this string is not a fencing grant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerEndpoint(String);

impl OwnerEndpoint {
    /// Validates and stores one compact, non-empty endpoint advertisement.
    pub fn new(value: impl Into<String>) -> Result<Self, CanonFenceError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CanonFenceError::EmptyEndpoint);
        }
        if value.len() > MAX_ENDPOINT_BYTES {
            return Err(CanonFenceError::EndpointTooLong {
                actual: value.len(),
                maximum: MAX_ENDPOINT_BYTES,
            });
        }
        if value.chars().any(char::is_control) {
            return Err(CanonFenceError::ControlCharacterInEndpoint);
        }
        Ok(Self(value))
    }

    /// Returns the advertised endpoint text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Owner state encoded inside a Canon fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonOwner {
    /// No node may accept new appends while the Line is recovering or draining.
    Unowned,
    /// The named process is the owner designated by this Canon revision.
    Owned {
        /// Stable process identity.
        owner_id: OwnerId,
        /// Advisory endpoint clients may try after validating this revision.
        endpoint: OwnerEndpoint,
    },
}

/// Scripture's application-owned owner fence, atomically coupled to a
/// [`VirtualLogState`] membership revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonFence {
    /// Revision that must equal the enclosing VirtualLog register revision.
    pub revision: u64,
    /// Logical Scripture journal.
    pub journal_id: JournalId,
    /// Physical ordered Line being fenced.
    pub line_id: LineId,
    /// Owner allowed to serve the Line, or explicit recovery/drain state.
    pub owner: CanonOwner,
}

impl CanonFence {
    /// Creates a Canon fence. The caller must later bind it to the exact
    /// enclosing VirtualLog revision through [`Self::from_virtual_log_state`].
    #[must_use]
    pub const fn new(
        revision: u64,
        journal_id: JournalId,
        line_id: LineId,
        owner: CanonOwner,
    ) -> Self {
        Self {
            revision,
            journal_id,
            line_id,
            owner,
        }
    }

    /// Returns canonical bytes suitable for Holylog's opaque application fence.
    #[must_use]
    pub fn encode(&self) -> ApplicationFence {
        let endpoint_len = match &self.owner {
            CanonOwner::Unowned => 0,
            CanonOwner::Owned { endpoint, .. } => endpoint.as_str().len(),
        };
        let mut encoded = Vec::with_capacity(
            FIXED_PREFIX_BYTES
                + if endpoint_len == 0 {
                    0
                } else {
                    OWNED_FIXED_BYTES + endpoint_len
                },
        );
        encoded.extend_from_slice(MAGIC);
        encoded.push(FORMAT_VERSION);
        encoded.extend_from_slice(&self.revision.to_be_bytes());
        encoded.extend_from_slice(&self.journal_id.as_bytes());
        encoded.extend_from_slice(&self.line_id.as_bytes());
        match &self.owner {
            CanonOwner::Unowned => encoded.push(UNOWNED),
            CanonOwner::Owned { owner_id, endpoint } => {
                encoded.push(OWNED);
                encoded.extend_from_slice(&owner_id.as_bytes());
                // OwnerEndpoint enforces a maximum far below u16::MAX.
                let length = endpoint.as_str().len() as u16;
                encoded.extend_from_slice(&length.to_be_bytes());
                encoded.extend_from_slice(endpoint.as_str().as_bytes());
            }
        }
        ApplicationFence::new(encoded)
    }

    /// Decodes and fully validates canonical Scripture Canon-fence bytes.
    pub fn decode(fence: &ApplicationFence) -> Result<Self, CanonFenceError> {
        let bytes = fence.as_bytes();
        let mut cursor = Cursor::new(bytes);
        if cursor.take(4)? != MAGIC {
            return Err(CanonFenceError::BadMagic);
        }
        let version = cursor.byte()?;
        if version != FORMAT_VERSION {
            return Err(CanonFenceError::UnsupportedVersion { version });
        }
        let revision = u64::from_be_bytes(cursor.array()?);
        let journal_id = JournalId::from_bytes(cursor.array()?);
        let line_id = LineId::from_bytes(cursor.array()?);
        let owner = match cursor.byte()? {
            UNOWNED => CanonOwner::Unowned,
            OWNED => {
                let owner_id = OwnerId::from_bytes(cursor.array()?);
                let endpoint_len = usize::from(u16::from_be_bytes(cursor.array()?));
                let endpoint = std::str::from_utf8(cursor.take(endpoint_len)?)
                    .map_err(|_| CanonFenceError::EndpointNotUtf8)?;
                CanonOwner::Owned {
                    owner_id,
                    endpoint: OwnerEndpoint::new(endpoint)?,
                }
            }
            tag => return Err(CanonFenceError::UnknownOwnerTag { tag }),
        };
        if !cursor.is_at_end() {
            return Err(CanonFenceError::TrailingBytes);
        }
        Ok(Self {
            revision,
            journal_id,
            line_id,
            owner,
        })
    }

    /// Decodes this fence and verifies it is bound to exactly `state`'s
    /// register revision.
    pub fn from_virtual_log_state(state: &VirtualLogState) -> Result<Self, CanonFenceError> {
        let fence = Self::decode(&state.application_fence)?;
        if fence.revision != state.revision {
            return Err(CanonFenceError::RevisionMismatch {
                fence_revision: fence.revision,
                state_revision: state.revision,
            });
        }
        Ok(fence)
    }
}

/// Canon-fence encoding or binding failure.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum CanonFenceError {
    /// The bytes ended before a required field was available.
    #[error("truncated Canon fence")]
    Truncated,
    /// The canonical format marker did not match.
    #[error("invalid Canon fence magic")]
    BadMagic,
    /// The document format version is unsupported.
    #[error("unsupported Canon fence version {version}")]
    UnsupportedVersion {
        /// Version byte observed.
        version: u8,
    },
    /// Owner-state tag is unknown.
    #[error("unknown Canon owner tag {tag}")]
    UnknownOwnerTag {
        /// Tag byte observed.
        tag: u8,
    },
    /// Endpoint bytes were not valid UTF-8.
    #[error("Canon owner endpoint is not UTF-8")]
    EndpointNotUtf8,
    /// Endpoint was empty.
    #[error("Canon owner endpoint is empty")]
    EmptyEndpoint,
    /// Endpoint exceeded the compact fence budget.
    #[error("Canon owner endpoint is too long: {actual} > {maximum}")]
    EndpointTooLong {
        /// Observed byte length.
        actual: usize,
        /// Maximum byte length.
        maximum: usize,
    },
    /// Endpoint contained an ASCII or Unicode control character.
    #[error("Canon owner endpoint contains a control character")]
    ControlCharacterInEndpoint,
    /// Extra bytes followed an otherwise valid canonical document.
    #[error("trailing bytes after Canon fence")]
    TrailingBytes,
    /// Canon and enclosing VirtualLog revisions did not agree.
    #[error("Canon revision {fence_revision} does not match VirtualLog revision {state_revision}")]
    RevisionMismatch {
        /// Revision carried inside the Canon fence.
        fence_revision: u64,
        /// Revision of the enclosing VirtualLog state.
        state_revision: u64,
    },
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], CanonFenceError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(CanonFenceError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CanonFenceError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, CanonFenceError> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CanonFenceError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CanonFenceError::Truncated)
    }

    fn is_at_end(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

/// Fresh Canon authority observation used to start one owner attempt.
///
/// This is not a forever lease. Callers must treat a later register advance or
/// seal fence as invalidating the attempt and re-inspect before serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonAuthoritySnapshot {
    /// Linearizable VirtualLog membership observation.
    pub state: VirtualLogState,
    /// Fence bound to [`VirtualLogState::revision`].
    pub fence: CanonFence,
}

impl CanonAuthoritySnapshot {
    /// Canon / VirtualLog revision used for this observation.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.fence.revision
    }
}

/// Why a fresh Canon observation refused to authorize an owner.
#[derive(Debug, thiserror::Error)]
pub enum CanonAuthorityError {
    /// Holylog register or VirtualLog failed.
    #[error(transparent)]
    VirtualLog(#[from] VirtualLogError),
    /// Opaque fence bytes failed to decode or bind.
    #[error(transparent)]
    Fence(#[from] CanonFenceError),
    /// The Canon fence names a different Scripture journal.
    #[error("Canon journal {actual} does not match expected {expected}")]
    JournalMismatch {
        /// Expected journal.
        expected: JournalId,
        /// Journal named by the fence.
        actual: JournalId,
    },
    /// The Canon fence names a different physical Line.
    #[error("Canon Line {actual} does not match expected {expected}")]
    LineMismatch {
        /// Expected Line.
        expected: LineId,
        /// Line named by the fence.
        actual: LineId,
    },
    /// The Line is explicitly unowned / recovering.
    #[error("Canon revision {revision} leaves Line {line_id} unowned")]
    Unowned {
        /// Observed revision.
        revision: u64,
        /// Line that is unowned.
        line_id: LineId,
    },
    /// The fence names a different owner identity.
    #[error("Canon owner {actual} does not match expected {expected} at revision {revision}")]
    NotOwner {
        /// Observed revision.
        revision: u64,
        /// Expected owner.
        expected: OwnerId,
        /// Owner named by the fence.
        actual: OwnerId,
    },
}

/// Reads a fresh VirtualLog register state and validates Canon identity/owner.
///
/// Always uses [`VirtualLog::state`] (linearizable). Cached membership is never
/// treated as startup authority.
pub async fn observe_canon_authority(
    virtual_log: &VirtualLog,
    expected_journal_id: JournalId,
    expected_line_id: LineId,
    expected_owner_id: OwnerId,
) -> Result<CanonAuthoritySnapshot, CanonAuthorityError> {
    let state = virtual_log.state().await?;
    let fence = CanonFence::from_virtual_log_state(&state)?;
    if fence.journal_id != expected_journal_id {
        return Err(CanonAuthorityError::JournalMismatch {
            expected: expected_journal_id,
            actual: fence.journal_id,
        });
    }
    if fence.line_id != expected_line_id {
        return Err(CanonAuthorityError::LineMismatch {
            expected: expected_line_id,
            actual: fence.line_id,
        });
    }
    match &fence.owner {
        CanonOwner::Unowned => Err(CanonAuthorityError::Unowned {
            revision: fence.revision,
            line_id: fence.line_id,
        }),
        CanonOwner::Owned { owner_id, .. } if *owner_id != expected_owner_id => {
            Err(CanonAuthorityError::NotOwner {
                revision: fence.revision,
                expected: expected_owner_id,
                actual: *owner_id,
            })
        }
        CanonOwner::Owned { .. } => Ok(CanonAuthoritySnapshot { state, fence }),
    }
}
