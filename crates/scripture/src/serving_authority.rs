//! Scripture Serving Authority types and canonical deterministic binary codec.
//!
//! Provides the P2P coordination and fencing models for client-facing writer
//! permission. This module contains no external database, cloud SDK, or
//! container scheduler types.

use std::fmt;

use holylog::virtual_log::{ApplicationFence, LogletId, VirtualLogState};

use crate::canon::{OwnerId, VerseId};
use crate::model::JournalId;

const MAGIC: &[u8; 4] = b"SCAR";
const CURRENT_FORMAT_VERSION: u32 = 3;

/// Bounded, validated error taxonomy for Serving Authority actions.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ServingAuthorityError {
    /// Writer term must be non-zero.
    #[error("invalid writer term: must be non-zero")]
    InvalidWriterTerm,
    /// Bounded string limit exceeded.
    #[error("string is too long: {actual} > {maximum}")]
    StringTooLong {
        /// Observed length.
        actual: usize,
        /// Maximum allowed length.
        maximum: usize,
    },
    /// Text contained control characters.
    #[error("text contains control characters")]
    ControlCharacterInText,
    /// Invalid binary magic bytes.
    #[error("invalid Serving Authority record magic")]
    BadMagic,
    /// Unsupported format version.
    #[error(
        "unsupported Serving Authority record format version: expected {expected}, got {actual}"
    )]
    UnsupportedVersion {
        /// Expected version.
        expected: u32,
        /// Actual version.
        actual: u32,
    },
    /// Unknown enum tag observed during decoding.
    #[error("unknown Serving Authority enum tag: {tag}")]
    UnknownTag {
        /// State tag byte.
        tag: u8,
    },
    /// Loglet ID resolution failure.
    #[error("invalid Loglet ID: {message}")]
    InvalidLogletId {
        /// Parsing details.
        message: String,
    },
    /// The input ended before all required fields were parsed.
    #[error("truncated Serving Authority payload")]
    Truncated,
    /// Extra trailing bytes followed an otherwise complete payload.
    #[error("trailing bytes after Serving Authority payload")]
    TrailingBytes,
    /// The decoded state is malformed or violates invariants.
    #[error("malformed Serving Authority state: {message}")]
    MalformedState {
        /// Invariant violation details.
        message: String,
    },
}

/// Bounded, validated advisory route text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RouteHint(String);

impl RouteHint {
    /// Validates and constructs a compact, non-control-character RouteHint.
    pub fn new(value: impl Into<String>) -> Result<Self, ServingAuthorityError> {
        let value = value.into();
        if value.len() > 1024 {
            return Err(ServingAuthorityError::StringTooLong {
                actual: value.len(),
                maximum: 1024,
            });
        }
        if value.chars().any(char::is_control) {
            return Err(ServingAuthorityError::ControlCharacterInText);
        }
        Ok(Self(value))
    }

    /// Returns the raw route hint string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RouteHint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Non-zero monotonic writer authority term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriterTerm(u64);

impl WriterTerm {
    /// Validates and constructs a non-zero WriterTerm.
    pub fn new(value: u64) -> Result<Self, ServingAuthorityError> {
        if value == 0 {
            return Err(ServingAuthorityError::InvalidWriterTerm);
        }
        Ok(Self(value))
    }

    /// Returns the underlying u64 value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for WriterTerm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Typed successor publication: membership + Serving grant only.
///
/// Structurally impossible to construct with a Transitioning fence. Coordinators
/// must pass this into foundation publish / final root CAS — never raw fence
/// bytes with an arbitrary authority state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServingPublication {
    key: AuthorityKey,
    authority: WriterAuthority,
    route_hint: RouteHint,
}

impl ServingPublication {
    /// Validates and constructs a Serving-only publication.
    pub fn new(
        key: AuthorityKey,
        authority: WriterAuthority,
        route_hint: RouteHint,
    ) -> Result<Self, ServingAuthorityError> {
        if authority.owner_id.as_bytes() == [0; 16] {
            return Err(ServingAuthorityError::MalformedState {
                message: "Serving publication requires a non-zero owner id".into(),
            });
        }
        Ok(Self {
            key,
            authority,
            route_hint,
        })
    }

    /// Authority key for this domain.
    #[must_use]
    pub const fn key(&self) -> AuthorityKey {
        self.key
    }

    /// Serving credentials being published.
    #[must_use]
    pub fn authority(&self) -> &WriterAuthority {
        &self.authority
    }

    /// Advisory route.
    #[must_use]
    pub fn route_hint(&self) -> &RouteHint {
        &self.route_hint
    }

    /// Materializes the Serving authority record for fence encoding.
    #[must_use]
    pub fn into_record(self) -> ServingAuthorityRecord {
        ServingAuthorityRecord::new(
            self.key,
            AuthorityState::Serving {
                authority: self.authority,
                route_hint: self.route_hint,
            },
        )
    }

    /// Encodes as Holylog application-fence bytes (Serving only).
    pub fn encode_application_fence(&self) -> Result<ApplicationFence, ServingAuthorityError> {
        ServingAuthorityRecord::new(
            self.key,
            AuthorityState::Serving {
                authority: self.authority.clone(),
                route_hint: self.route_hint.clone(),
            },
        )
        .encode_application_fence()
    }
}

/// Durable, cryptographic reference to a specific VirtualLog generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalGenerationRef {
    /// Authoritative VirtualLog revision.
    pub virtual_log_revision: u64,
    /// Active generation's Loglet ID.
    pub active_loglet_id: LogletId,
    /// Active generation start boundary.
    pub active_start: u64,
    /// BLAKE3 digest of the full enclosing CanonFence payload.
    pub canon_fence_digest: [u8; 32],
}

impl JournalGenerationRef {
    /// Binds the active generation in `state` without digesting application-fence
    /// bytes. Membership binding is `(revision, active_loglet_id, active_start)`;
    /// `canon_fence_digest` is BLAKE3 over those fields only so one-record Serving
    /// fences can embed this ref without a circular self-digest.
    pub fn from_virtual_log_state(state: &VirtualLogState) -> Result<Self, ServingAuthorityError> {
        let active_desc = state
            .active()
            .ok_or_else(|| ServingAuthorityError::MalformedState {
                message: "VirtualLogState has no active generation descriptor".to_string(),
            })?;

        Ok(Self::from_active_generation(
            state.revision,
            active_desc.loglet_id.clone(),
            active_desc.start,
        ))
    }

    /// Constructs a generation binding and its membership digest.
    #[must_use]
    pub fn from_active_generation(
        virtual_log_revision: u64,
        active_loglet_id: LogletId,
        active_start: u64,
    ) -> Self {
        let mut digest_input = Vec::with_capacity(8 + 2 + active_loglet_id.as_str().len() + 8);
        digest_input.extend_from_slice(&virtual_log_revision.to_be_bytes());
        let loglet_len = u16::try_from(active_loglet_id.as_str().len()).unwrap_or(u16::MAX);
        digest_input.extend_from_slice(&loglet_len.to_be_bytes());
        digest_input.extend_from_slice(active_loglet_id.as_str().as_bytes());
        digest_input.extend_from_slice(&active_start.to_be_bytes());
        let canon_fence_digest: [u8; 32] = blake3::hash(&digest_input).into();
        Self {
            virtual_log_revision,
            active_loglet_id,
            active_start,
            canon_fence_digest,
        }
    }
}

/// Opaque, collision-resistant token tracking one recovery or handoff transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransitionId([u8; 16]);

impl TransitionId {
    /// Constructs a TransitionId from its raw 128-bit byte layout.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the raw 128-bit byte representation.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Display for TransitionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Directed transition intent inside the Serving Authority record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionKind {
    /// Planned handoff from an active writer.
    PlannedHandoff,
    /// Recovery promotion following a crash or seal.
    RecoveryPromotion,
}

/// Precondition for a Journal Foundation transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoundationPrecondition {
    /// No Journal Foundation exists yet. A valid generation at revision zero is not Empty.
    Empty,
    /// Expected active generation.
    Expected(JournalGenerationRef),
}

/// Durable operation block capturing the entire planned transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionIntent {
    /// Unique ID for this transition.
    pub transition_id: TransitionId,
    /// Kind of transition.
    pub kind: TransitionKind,
    /// Precondition of the foundation being replaced.
    pub precondition: FoundationPrecondition,
    /// Candidate owner being promoted.
    pub candidate_owner_id: OwnerId,
    /// Requested target monotonic term.
    pub next_writer_term: WriterTerm,
}

/// Core Serving Authority details of an active owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterAuthority {
    /// Lawful owner identifier.
    pub owner_id: OwnerId,
    /// Monotonic writer authority term.
    pub writer_term: WriterTerm,
    /// Durable reference to the current VirtualLog generation.
    pub generation_ref: JournalGenerationRef,
}

/// High-level Serving Authority state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityState {
    /// The Verse is vacant; no owner has permission to serve.
    Unassigned,
    /// Transition is active, halting client serving permissions.
    Transitioning {
        /// Complete planned operation.
        intent: TransitionIntent,
    },
    /// The designated owner is serving clients under an active term and generation.
    Serving {
        /// Active serving credentials.
        authority: WriterAuthority,
        /// Bounded, advisory routing target.
        route_hint: RouteHint,
    },
    /// Explicitly halted state requiring operator/reconciliation tool action.
    ReconciliationRequired {
        /// The complete planned operation that was in-progress when failure occurred.
        intent: TransitionIntent,
        /// Optionally discovered Foundation successor, if published.
        observed_generation: Option<JournalGenerationRef>,
    },
}

/// Stable identity of a physical Scripture/Verse authority domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AuthorityKey {
    /// Logical Scripture journal.
    pub journal_id: JournalId,
    /// Physical Verse append lane.
    pub verse_id: VerseId,
}

/// Complete, versioned document tracking one domain's Serving Authority state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServingAuthorityRecord {
    /// Format version of the payload, ensuring forwards compatibility.
    pub format_version: u32,
    /// The target Verse and journal identity.
    pub key: AuthorityKey,
    /// The current state machine state.
    pub state: AuthorityState,
}

impl ServingAuthorityRecord {
    /// Constructs a ServingAuthorityRecord.
    #[must_use]
    pub const fn new(key: AuthorityKey, state: AuthorityState) -> Self {
        Self {
            format_version: CURRENT_FORMAT_VERSION,
            key,
            state,
        }
    }

    /// Evaluates whether the local owner holds effective client-facing writer authority.
    ///
    /// One-record rule: `self` must be the authority decoded from
    /// `witnessed_state.application_fence`, in `Serving`, with exact owner/term/
    /// generation binding, and the process must hold an unsealed writable.
    pub fn is_effective_writer(
        &self,
        witnessed_state: &VirtualLogState,
        local_owner_id: OwnerId,
        is_writable: bool,
        is_sealed: bool,
    ) -> bool {
        if is_sealed || !is_writable {
            return false;
        }

        let Ok(from_root) = Self::decode_application_fence(&witnessed_state.application_fence)
        else {
            return false;
        };
        if &from_root != self {
            return false;
        }

        let AuthorityState::Serving { authority, .. } = &self.state else {
            return false;
        };

        if authority.owner_id != local_owner_id {
            return false;
        }

        let Ok(gen_ref) = JournalGenerationRef::from_virtual_log_state(witnessed_state) else {
            return false;
        };

        if authority.generation_ref != gen_ref {
            return false;
        }

        true
    }

    /// Encodes this record as Holylog opaque application-fence bytes.
    pub fn encode_application_fence(&self) -> Result<ApplicationFence, ServingAuthorityError> {
        Ok(ApplicationFence::new(self.encode()?))
    }

    /// Decodes a Serving Authority record from Holylog application-fence bytes.
    pub fn decode_application_fence(
        fence: &ApplicationFence,
    ) -> Result<Self, ServingAuthorityError> {
        Self::decode(fence.as_bytes())
    }

    /// Encodes this record into its canonical deterministic binary payload.
    pub fn encode(&self) -> Result<Vec<u8>, ServingAuthorityError> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&self.format_version.to_be_bytes());
        encoded.extend_from_slice(&self.key.journal_id.as_bytes());
        encoded.extend_from_slice(&self.key.verse_id.as_bytes());

        match &self.state {
            AuthorityState::Unassigned => {
                encoded.push(0);
            }
            AuthorityState::Transitioning { intent } => {
                encoded.push(1);
                encode_intent(&mut encoded, intent)?;
            }
            AuthorityState::Serving {
                authority,
                route_hint,
            } => {
                encoded.push(2);
                encoded.extend_from_slice(&authority.owner_id.as_bytes());
                encoded.extend_from_slice(&authority.writer_term.get().to_be_bytes());
                encode_generation_ref(&mut encoded, &authority.generation_ref)?;
                let route_len = u16::try_from(route_hint.as_str().len()).map_err(|_| {
                    ServingAuthorityError::StringTooLong {
                        actual: route_hint.as_str().len(),
                        maximum: u16::MAX as usize,
                    }
                })?;
                encoded.extend_from_slice(&route_len.to_be_bytes());
                encoded.extend_from_slice(route_hint.as_str().as_bytes());
            }
            AuthorityState::ReconciliationRequired {
                intent,
                observed_generation,
            } => {
                encoded.push(3);
                encode_intent(&mut encoded, intent)?;
                if let Some(obs) = observed_generation {
                    encoded.push(1);
                    encode_generation_ref(&mut encoded, obs)?;
                } else {
                    encoded.push(0);
                }
            }
        }
        Ok(encoded)
    }

    /// Decodes and validates a canonical ServingAuthorityRecord.
    pub fn decode(bytes: &[u8]) -> Result<Self, ServingAuthorityError> {
        let mut cursor = Cursor::new(bytes);
        if cursor.take(4)? != MAGIC {
            return Err(ServingAuthorityError::BadMagic);
        }
        let version = u32::from_be_bytes(cursor.array()?);
        if version != CURRENT_FORMAT_VERSION {
            return Err(ServingAuthorityError::UnsupportedVersion {
                expected: CURRENT_FORMAT_VERSION,
                actual: version,
            });
        }
        let journal_id = JournalId::from_bytes(cursor.array()?);
        let verse_id = VerseId::from_bytes(cursor.array()?);
        let key = AuthorityKey {
            journal_id,
            verse_id,
        };

        let state = match cursor.byte()? {
            0 => AuthorityState::Unassigned,
            1 => {
                let intent = decode_intent(&mut cursor)?;
                AuthorityState::Transitioning { intent }
            }
            2 => {
                let owner_id = OwnerId::from_bytes(cursor.array()?);
                let term_val = u64::from_be_bytes(cursor.array()?);
                let writer_term = WriterTerm::new(term_val)?;

                let generation_ref = decode_generation_ref(&mut cursor)?;

                let authority = WriterAuthority {
                    owner_id,
                    writer_term,
                    generation_ref,
                };

                let route_len = usize::from(u16::from_be_bytes(cursor.array()?));
                let route_bytes = cursor.take(route_len)?;
                let route_str = std::str::from_utf8(route_bytes).map_err(|e| {
                    ServingAuthorityError::MalformedState {
                        message: format!("RouteHint not UTF-8: {e}"),
                    }
                })?;
                let route_hint = RouteHint::new(route_str)?;

                AuthorityState::Serving {
                    authority,
                    route_hint,
                }
            }
            3 => {
                let intent = decode_intent(&mut cursor)?;
                let observed_generation = match cursor.byte()? {
                    0 => None,
                    1 => Some(decode_generation_ref(&mut cursor)?),
                    tag => return Err(ServingAuthorityError::UnknownTag { tag }),
                };

                AuthorityState::ReconciliationRequired {
                    intent,
                    observed_generation,
                }
            }
            tag => return Err(ServingAuthorityError::UnknownTag { tag }),
        };

        if !cursor.is_at_end() {
            return Err(ServingAuthorityError::TrailingBytes);
        }

        Ok(ServingAuthorityRecord {
            format_version: version,
            key,
            state,
        })
    }
}

fn encode_intent(
    encoded: &mut Vec<u8>,
    intent: &TransitionIntent,
) -> Result<(), ServingAuthorityError> {
    encoded.extend_from_slice(&intent.transition_id.as_bytes());
    match intent.kind {
        TransitionKind::PlannedHandoff => encoded.push(0),
        TransitionKind::RecoveryPromotion => encoded.push(1),
    }
    encode_precondition(encoded, &intent.precondition)?;
    encoded.extend_from_slice(&intent.candidate_owner_id.as_bytes());
    encoded.extend_from_slice(&intent.next_writer_term.get().to_be_bytes());
    Ok(())
}

fn decode_intent(cursor: &mut Cursor) -> Result<TransitionIntent, ServingAuthorityError> {
    let transition_id = TransitionId::from_bytes(cursor.array()?);
    let kind = match cursor.byte()? {
        0 => TransitionKind::PlannedHandoff,
        1 => TransitionKind::RecoveryPromotion,
        tag => return Err(ServingAuthorityError::UnknownTag { tag }),
    };
    let precondition = decode_precondition(cursor)?;
    let candidate_owner_id = OwnerId::from_bytes(cursor.array()?);
    let term_val = u64::from_be_bytes(cursor.array()?);
    let next_writer_term = WriterTerm::new(term_val)?;
    Ok(TransitionIntent {
        transition_id,
        kind,
        precondition,
        candidate_owner_id,
        next_writer_term,
    })
}

fn encode_precondition(
    encoded: &mut Vec<u8>,
    pre: &FoundationPrecondition,
) -> Result<(), ServingAuthorityError> {
    match pre {
        FoundationPrecondition::Empty => encoded.push(0),
        FoundationPrecondition::Expected(gen_ref) => {
            encoded.push(1);
            encode_generation_ref(encoded, gen_ref)?;
        }
    }
    Ok(())
}

fn decode_precondition(
    cursor: &mut Cursor,
) -> Result<FoundationPrecondition, ServingAuthorityError> {
    match cursor.byte()? {
        0 => Ok(FoundationPrecondition::Empty),
        1 => Ok(FoundationPrecondition::Expected(decode_generation_ref(
            cursor,
        )?)),
        tag => Err(ServingAuthorityError::UnknownTag { tag }),
    }
}

fn encode_generation_ref(
    encoded: &mut Vec<u8>,
    gen_ref: &JournalGenerationRef,
) -> Result<(), ServingAuthorityError> {
    encoded.extend_from_slice(&gen_ref.virtual_log_revision.to_be_bytes());
    let loglet_len = u16::try_from(gen_ref.active_loglet_id.as_str().len()).map_err(|_| {
        ServingAuthorityError::StringTooLong {
            actual: gen_ref.active_loglet_id.as_str().len(),
            maximum: u16::MAX as usize,
        }
    })?;
    encoded.extend_from_slice(&loglet_len.to_be_bytes());
    encoded.extend_from_slice(gen_ref.active_loglet_id.as_str().as_bytes());
    encoded.extend_from_slice(&gen_ref.active_start.to_be_bytes());
    encoded.extend_from_slice(&gen_ref.canon_fence_digest);
    Ok(())
}

fn decode_generation_ref(
    cursor: &mut Cursor,
) -> Result<JournalGenerationRef, ServingAuthorityError> {
    let virtual_log_revision = u64::from_be_bytes(cursor.array()?);
    let loglet_len = usize::from(u16::from_be_bytes(cursor.array()?));
    let loglet_bytes = cursor.take(loglet_len)?;
    let loglet_str =
        std::str::from_utf8(loglet_bytes).map_err(|e| ServingAuthorityError::MalformedState {
            message: format!("Loglet ID not UTF-8: {e}"),
        })?;
    let active_loglet_id =
        LogletId::new(loglet_str).map_err(|e| ServingAuthorityError::InvalidLogletId {
            message: e.to_string(),
        })?;
    let active_start = u64::from_be_bytes(cursor.array()?);
    let canon_fence_digest = cursor.array()?;

    Ok(JournalGenerationRef {
        virtual_log_revision,
        active_loglet_id,
        active_start,
        canon_fence_digest,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], ServingAuthorityError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(ServingAuthorityError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ServingAuthorityError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, ServingAuthorityError> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ServingAuthorityError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ServingAuthorityError::Truncated)
    }

    fn is_at_end(&self) -> bool {
        self.offset == self.bytes.len()
    }
}
