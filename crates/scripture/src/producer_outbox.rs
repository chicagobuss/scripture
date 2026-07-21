//! Durable client-side outbox for experimental Producer Wire v1.
//!
//! This is deliberately distinct from [`crate::spool`]. A Scribe spool may
//! support a `spooled` receipt; this outbox grants no receipt at all. It keeps
//! the producer's exact Wire submission safe across a client crash so a later
//! process can replay the same `(producer_id, epoch, sequence, records)` claim.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::{
    ProducerId, ProducerWireError, ProducerWireFrame, decode_producer_wire_frame,
    encode_producer_wire_frame,
};

const META_NAME: &str = "producer-wire.meta";
const WAL_NAME: &str = "producer-wire.wal";
const LOCK_NAME: &str = "producer-wire.owner";
const META_MAGIC: &[u8; 8] = b"SPWOUT01";
const RECORD_SUBMIT: u8 = 1;
const RECORD_ACK: u8 = 2;

/// Maximum logical target-label bytes persisted in one outbox identity file.
pub const MAX_OUTBOX_TARGET_BYTES: usize = 1024;

/// Stable identity and logical destination of one producer outbox.
///
/// The logical target is deliberately not a host:port. Routes may change during
/// HA handoff, while a Canon/Verse assignment must not. Until Wire carries a
/// canonical assignment identifier itself, the caller supplies this durable
/// label and must use the same label when reopening the outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerOutboxIdentity {
    /// Stable producer identity used in Wire Hello frames.
    pub producer_id: ProducerId,
    /// Nonzero incarnation fencing prior producer processes.
    pub producer_epoch: u32,
    /// Stable caller-supplied target label, not a mutable network route.
    pub target: String,
}

/// Exact pending Wire frame recovered from the durable outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingWireSubmission {
    /// Per-epoch sequence carried by the frame.
    pub sequence: u64,
    /// Complete length-framed `ProducerWireFrame::Submit` bytes.
    pub encoded_submit: Vec<u8>,
}

/// Errors from durable producer-outbox operations.
#[derive(Debug, thiserror::Error)]
pub enum ProducerOutboxError {
    /// Operating-system failure while opening, reading, writing, or syncing.
    #[error("producer outbox I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Another live local process owns this outbox directory.
    #[error("producer outbox is already owned by another live process")]
    Locked,
    /// An existing outbox belongs to a different producer/epoch/target.
    #[error("producer outbox identity does not match its durable identity file")]
    IdentityMismatch,
    /// The requested identity is malformed before touching durable state.
    #[error("invalid producer outbox identity: {0}")]
    InvalidIdentity(&'static str),
    /// The requested capacity cannot hold even the durable protocol framing.
    #[error("producer outbox max_bytes must be nonzero")]
    InvalidCapacity,
    /// A submitted frame is not a valid Wire Submit frame.
    #[error("invalid producer-wire submit frame: {0}")]
    Wire(#[from] ProducerWireError),
    /// A submit frame's sequence differs from the one encoded in its body.
    #[error("producer outbox frame is not a Submit frame")]
    NotSubmit,
    /// A newly staged sequence is not the next contiguous sequence.
    #[error("producer outbox expected sequence {expected}, got {actual}")]
    OutOfSequence {
        /// Next sequence not already in the durable transcript.
        expected: u64,
        /// Sequence presented in the new submit frame.
        actual: u64,
    },
    /// The same sequence was staged with different exact Wire bytes.
    #[error("producer outbox identity conflict at sequence {sequence}")]
    IdentityConflict {
        /// The reused sequence.
        sequence: u64,
    },
    /// No room was reserved before attempting to persist this submission.
    #[error(
        "producer outbox capacity exceeded: used {used_bytes}, attempted {attempted_bytes}, max {max_bytes}"
    )]
    CapacityExceeded {
        /// Current WAL bytes.
        used_bytes: usize,
        /// Bytes required by the next durable record.
        attempted_bytes: usize,
        /// Configured hard maximum.
        max_bytes: usize,
    },
    /// An ACK was offered for a different persisted epoch.
    #[error("producer outbox ACK epoch mismatch: expected {expected}, got {actual}")]
    EpochMismatch {
        /// Durable outbox epoch.
        expected: u32,
        /// ACK epoch supplied by the peer.
        actual: u32,
    },
    /// An ACK refers to no staged submission.
    #[error("producer outbox ACK refers to unknown sequence {sequence}")]
    UnknownSequence {
        /// Sequence with no durable Submit frame.
        sequence: u64,
    },
    /// The durable sequence transcript cannot advance further.
    #[error("producer outbox sequence is exhausted")]
    SequenceExhausted,
    /// A complete durable record is malformed or has a mismatching checksum.
    #[error("corrupt producer outbox WAL: {0}")]
    Corrupt(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EntryState {
    Pending(Vec<u8>),
    Acknowledged(Vec<u8>),
}

/// A crash-safe, single-process durable queue of exact Wire Submit frames.
///
/// `stage_submit` is the local durability boundary. Call it before attempting
/// the TCP write. `mark_committed` is the only operation that retires a pending
/// submission, and it must follow a matching Wire ACK.
pub struct ProducerOutbox {
    identity: ProducerOutboxIdentity,
    root: PathBuf,
    _owner_lock: OwnerLock,
    wal: File,
    max_bytes: usize,
    used_bytes: usize,
    entries: BTreeMap<u64, EntryState>,
}

impl ProducerOutbox {
    /// Opens a durable outbox, creating it with `identity` when absent.
    ///
    /// The caller must choose one local directory per stable producer and
    /// logical Canon/Verse target. Reopening with any different identity fails
    /// closed; network route changes are intentionally not part of identity.
    pub fn open(
        root: impl AsRef<Path>,
        identity: ProducerOutboxIdentity,
        max_bytes: usize,
    ) -> Result<Self, ProducerOutboxError> {
        validate_identity(&identity)?;
        if max_bytes == 0 {
            return Err(ProducerOutboxError::InvalidCapacity);
        }
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        let owner_lock = OwnerLock::acquire(&root)?;
        ensure_identity(&root, &identity)?;

        let wal_path = root.join(WAL_NAME);
        let mut wal = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(wal_path)?;
        let mut bytes = Vec::new();
        wal.seek(SeekFrom::Start(0))?;
        wal.read_to_end(&mut bytes)?;
        let (entries, valid_bytes) = scan_wal(&bytes)?;
        if valid_bytes != bytes.len() {
            // A torn trailing record never crossed this outbox's fsync boundary.
            // Remove it before appending so a later valid record is not hidden
            // behind an unrecoverable partial suffix.
            wal.set_len(valid_bytes as u64)?;
            wal.sync_all()?;
        }
        wal.seek(SeekFrom::End(0))?;

        Ok(Self {
            identity,
            root,
            _owner_lock: owner_lock,
            wal,
            max_bytes,
            used_bytes: valid_bytes,
            entries,
        })
    }

    /// Durable producer identity and logical target.
    #[must_use]
    pub fn identity(&self) -> &ProducerOutboxIdentity {
        &self.identity
    }

    /// Directory holding the identity and WAL files.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Next unused contiguous sequence in this persisted epoch.
    pub fn next_sequence(&self) -> Result<u64, ProducerOutboxError> {
        self.entries
            .last_key_value()
            .map_or(Ok(0), |(sequence, _)| {
                sequence
                    .checked_add(1)
                    .ok_or(ProducerOutboxError::SequenceExhausted)
            })
    }

    /// Encodes the exact Hello frame corresponding to the durable identity.
    pub fn hello_frame(&self) -> Result<Vec<u8>, ProducerOutboxError> {
        Ok(encode_producer_wire_frame(&ProducerWireFrame::Hello {
            producer_id: self.identity.producer_id,
            producer_epoch: self.identity.producer_epoch,
        })?)
    }

    /// Fsyncs one exact Wire Submit frame before the caller sends it.
    ///
    /// Restaging byte-identical known work is idempotent. Any byte change under
    /// the same sequence is rejected locally, before it can violate the Scribe
    /// deduplication contract.
    pub fn stage_submit(
        &mut self,
        encoded_submit: &[u8],
    ) -> Result<PendingWireSubmission, ProducerOutboxError> {
        let sequence = submit_sequence(encoded_submit)?;
        if let Some(entry) = self.entries.get(&sequence) {
            let original = match entry {
                EntryState::Pending(bytes) | EntryState::Acknowledged(bytes) => bytes,
            };
            if original == encoded_submit {
                return Ok(PendingWireSubmission {
                    sequence,
                    encoded_submit: original.clone(),
                });
            }
            return Err(ProducerOutboxError::IdentityConflict { sequence });
        }
        let expected = self.next_sequence()?;
        if sequence != expected {
            return Err(ProducerOutboxError::OutOfSequence {
                expected,
                actual: sequence,
            });
        }
        let record = encode_submit_record(sequence, encoded_submit)?;
        self.reserve(record.len())?;
        self.wal.write_all(&record)?;
        self.wal.flush()?;
        self.wal.sync_all()?;
        self.used_bytes += record.len();
        self.entries
            .insert(sequence, EntryState::Pending(encoded_submit.to_vec()));
        Ok(PendingWireSubmission {
            sequence,
            encoded_submit: encoded_submit.to_vec(),
        })
    }

    /// Records a matching committed ACK and retires the pending submission.
    ///
    /// The original Submit bytes remain in the append-only transcript so a
    /// restarted producer cannot reuse the same sequence with different bytes.
    pub fn mark_committed(
        &mut self,
        producer_epoch: u32,
        sequence: u64,
    ) -> Result<(), ProducerOutboxError> {
        if producer_epoch != self.identity.producer_epoch {
            return Err(ProducerOutboxError::EpochMismatch {
                expected: self.identity.producer_epoch,
                actual: producer_epoch,
            });
        }
        let state = self
            .entries
            .get(&sequence)
            .ok_or(ProducerOutboxError::UnknownSequence { sequence })?;
        if matches!(state, EntryState::Acknowledged(_)) {
            return Ok(());
        }
        let record = encode_ack_record(sequence);
        self.reserve(record.len())?;
        self.wal.write_all(&record)?;
        self.wal.flush()?;
        self.wal.sync_all()?;
        self.used_bytes += record.len();
        let original = match self.entries.remove(&sequence) {
            Some(EntryState::Pending(bytes)) => bytes,
            Some(EntryState::Acknowledged(bytes)) => {
                self.entries
                    .insert(sequence, EntryState::Acknowledged(bytes));
                return Ok(());
            }
            None => return Err(ProducerOutboxError::UnknownSequence { sequence }),
        };
        self.entries
            .insert(sequence, EntryState::Acknowledged(original));
        Ok(())
    }

    /// Returns unfinished submissions in durable sequence order.
    #[must_use]
    pub fn pending_submissions(&self) -> Vec<PendingWireSubmission> {
        self.entries
            .iter()
            .filter_map(|(sequence, state)| match state {
                EntryState::Pending(encoded_submit) => Some(PendingWireSubmission {
                    sequence: *sequence,
                    encoded_submit: encoded_submit.clone(),
                }),
                EntryState::Acknowledged(_) => None,
            })
            .collect()
    }

    fn reserve(&self, attempted_bytes: usize) -> Result<(), ProducerOutboxError> {
        let total = self.used_bytes.checked_add(attempted_bytes).ok_or(
            ProducerOutboxError::CapacityExceeded {
                used_bytes: self.used_bytes,
                attempted_bytes,
                max_bytes: self.max_bytes,
            },
        )?;
        if total > self.max_bytes {
            return Err(ProducerOutboxError::CapacityExceeded {
                used_bytes: self.used_bytes,
                attempted_bytes,
                max_bytes: self.max_bytes,
            });
        }
        Ok(())
    }
}

struct OwnerLock {
    path: PathBuf,
    pid: u32,
}

impl OwnerLock {
    fn acquire(root: &Path) -> Result<Self, ProducerOutboxError> {
        let path = root.join(LOCK_NAME);
        let pid = std::process::id();
        if path.exists() {
            let existing = std::fs::read_to_string(&path).unwrap_or_default();
            if let Ok(other) = existing.trim().parse::<u32>()
                && other != pid
                && pid_appears_alive(other)
            {
                return Err(ProducerOutboxError::Locked);
            }
            std::fs::remove_file(&path)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    ProducerOutboxError::Locked
                } else {
                    ProducerOutboxError::Io(error)
                }
            })?;
        write!(file, "{pid}")?;
        file.sync_all()?;
        Ok(Self { path, pid })
    }
}

impl Drop for OwnerLock {
    fn drop(&mut self) {
        if let Ok(contents) = std::fs::read_to_string(&self.path)
            && contents.trim().parse::<u32>().ok() == Some(self.pid)
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn pid_appears_alive(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        Path::new(&format!("/proc/{pid}")).exists()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        true
    }
}

fn validate_identity(identity: &ProducerOutboxIdentity) -> Result<(), ProducerOutboxError> {
    if identity.producer_epoch == 0 {
        return Err(ProducerOutboxError::InvalidIdentity("producer_epoch"));
    }
    if identity.target.is_empty() || identity.target.len() > MAX_OUTBOX_TARGET_BYTES {
        return Err(ProducerOutboxError::InvalidIdentity("target"));
    }
    Ok(())
}

fn ensure_identity(
    root: &Path,
    identity: &ProducerOutboxIdentity,
) -> Result<(), ProducerOutboxError> {
    let path = root.join(META_NAME);
    let expected = encode_identity(identity)?;
    match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => {
            file.write_all(&expected)?;
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let actual = std::fs::read(path)?;
            if actual == expected {
                Ok(())
            } else {
                Err(ProducerOutboxError::IdentityMismatch)
            }
        }
        Err(error) => Err(ProducerOutboxError::Io(error)),
    }
}

fn encode_identity(identity: &ProducerOutboxIdentity) -> Result<Vec<u8>, ProducerOutboxError> {
    validate_identity(identity)?;
    let target = identity.target.as_bytes();
    let target_len =
        u16::try_from(target.len()).map_err(|_| ProducerOutboxError::InvalidIdentity("target"))?;
    let mut bytes = Vec::with_capacity(META_MAGIC.len() + 16 + 4 + 2 + target.len());
    bytes.extend_from_slice(META_MAGIC);
    bytes.extend_from_slice(&identity.producer_id.as_bytes());
    bytes.extend_from_slice(&identity.producer_epoch.to_be_bytes());
    bytes.extend_from_slice(&target_len.to_be_bytes());
    bytes.extend_from_slice(target);
    Ok(bytes)
}

fn submit_sequence(encoded_submit: &[u8]) -> Result<u64, ProducerOutboxError> {
    match decode_producer_wire_frame(encoded_submit)? {
        ProducerWireFrame::Submit { sequence, .. } => Ok(sequence),
        _ => Err(ProducerOutboxError::NotSubmit),
    }
}

fn encode_submit_record(sequence: u64, submit: &[u8]) -> Result<Vec<u8>, ProducerOutboxError> {
    let submit_len = u32::try_from(submit.len())
        .map_err(|_| ProducerOutboxError::Corrupt("submit frame exceeds u32"))?;
    let mut body = Vec::with_capacity(1 + 8 + 4 + submit.len());
    body.push(RECORD_SUBMIT);
    body.extend_from_slice(&sequence.to_be_bytes());
    body.extend_from_slice(&submit_len.to_be_bytes());
    body.extend_from_slice(submit);
    Ok(frame_record(body))
}

fn encode_ack_record(sequence: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(1 + 8);
    body.push(RECORD_ACK);
    body.extend_from_slice(&sequence.to_be_bytes());
    frame_record(body)
}

fn frame_record(body: Vec<u8>) -> Vec<u8> {
    let body_len = u32::try_from(body.len()).expect("outbox frame body is bounded by submit cap");
    let checksum = crc32c::crc32c(&body);
    let mut record = Vec::with_capacity(4 + body.len() + 4);
    record.extend_from_slice(&body_len.to_be_bytes());
    record.extend_from_slice(&body);
    record.extend_from_slice(&checksum.to_be_bytes());
    record
}

fn scan_wal(bytes: &[u8]) -> Result<(BTreeMap<u64, EntryState>, usize), ProducerOutboxError> {
    let mut entries = BTreeMap::new();
    let mut cursor = 0_usize;
    while cursor < bytes.len() {
        let record_start = cursor;
        let Some(prefix_end) = cursor.checked_add(4) else {
            return Err(ProducerOutboxError::Corrupt("length overflow"));
        };
        if prefix_end > bytes.len() {
            return Ok((entries, record_start));
        }
        let body_len =
            u32::from_be_bytes(bytes[cursor..prefix_end].try_into().expect("slice len")) as usize;
        cursor = prefix_end;
        let Some(body_end) = cursor.checked_add(body_len) else {
            return Err(ProducerOutboxError::Corrupt("body length overflow"));
        };
        let Some(record_end) = body_end.checked_add(4) else {
            return Err(ProducerOutboxError::Corrupt("checksum length overflow"));
        };
        if record_end > bytes.len() {
            return Ok((entries, record_start));
        }
        let body = &bytes[cursor..body_end];
        let checksum =
            u32::from_be_bytes(bytes[body_end..record_end].try_into().expect("slice len"));
        if crc32c::crc32c(body) != checksum {
            return Err(ProducerOutboxError::Corrupt("record checksum"));
        }
        apply_record(&mut entries, body)?;
        cursor = record_end;
    }
    Ok((entries, cursor))
}

fn apply_record(
    entries: &mut BTreeMap<u64, EntryState>,
    body: &[u8],
) -> Result<(), ProducerOutboxError> {
    let Some((&kind, remainder)) = body.split_first() else {
        return Err(ProducerOutboxError::Corrupt("empty record"));
    };
    match kind {
        RECORD_SUBMIT => {
            if remainder.len() < 12 {
                return Err(ProducerOutboxError::Corrupt("truncated submit record"));
            }
            let sequence = u64::from_be_bytes(remainder[..8].try_into().expect("slice len"));
            let submit_len =
                u32::from_be_bytes(remainder[8..12].try_into().expect("slice len")) as usize;
            let submit = &remainder[12..];
            if submit.len() != submit_len || submit_sequence(submit)? != sequence {
                return Err(ProducerOutboxError::Corrupt("invalid submit record"));
            }
            if let Some(existing) = entries.get(&sequence) {
                let original = match existing {
                    EntryState::Pending(bytes) | EntryState::Acknowledged(bytes) => bytes,
                };
                if original != submit {
                    return Err(ProducerOutboxError::IdentityConflict { sequence });
                }
                return Ok(());
            }
            let expected = entries.last_key_value().map_or(Ok(0), |(previous, _)| {
                previous
                    .checked_add(1)
                    .ok_or(ProducerOutboxError::SequenceExhausted)
            })?;
            if sequence != expected {
                return Err(ProducerOutboxError::OutOfSequence {
                    expected,
                    actual: sequence,
                });
            }
            entries.insert(sequence, EntryState::Pending(submit.to_vec()));
            Ok(())
        }
        RECORD_ACK => {
            if remainder.len() != 8 {
                return Err(ProducerOutboxError::Corrupt("invalid ACK record"));
            }
            let sequence = u64::from_be_bytes(remainder.try_into().expect("slice len"));
            let current = entries
                .get(&sequence)
                .ok_or(ProducerOutboxError::UnknownSequence { sequence })?;
            if matches!(current, EntryState::Acknowledged(_)) {
                return Ok(());
            }
            let original = match entries.remove(&sequence) {
                Some(EntryState::Pending(bytes)) => bytes,
                Some(EntryState::Acknowledged(bytes)) => bytes,
                None => return Err(ProducerOutboxError::UnknownSequence { sequence }),
            };
            entries.insert(sequence, EntryState::Acknowledged(original));
            Ok(())
        }
        _ => Err(ProducerOutboxError::Corrupt("unknown record kind")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;

    use super::*;

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

    fn root(name: &str) -> PathBuf {
        let nonce = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "scripture-producer-outbox-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn identity() -> ProducerOutboxIdentity {
        ProducerOutboxIdentity {
            producer_id: ProducerId::from_bytes(*b"producer-outbox1"),
            producer_epoch: 7,
            target: "canon/telemetry/verse/host-metrics".into(),
        }
    }

    fn submit(sequence: u64, value: &[u8]) -> Vec<u8> {
        encode_producer_wire_frame(&ProducerWireFrame::Submit {
            sequence,
            records: vec![Bytes::copy_from_slice(value)],
        })
        .expect("valid test submit")
    }

    #[test]
    fn fsynced_submit_replays_exact_bytes_after_restart() {
        let root = root("replay");
        let expected = submit(0, b"first");
        {
            let mut outbox = ProducerOutbox::open(&root, identity(), 1024 * 1024).expect("open");
            outbox.stage_submit(&expected).expect("stage");
        }
        let outbox = ProducerOutbox::open(&root, identity(), 1024 * 1024).expect("reopen");
        assert_eq!(
            outbox.pending_submissions(),
            vec![PendingWireSubmission {
                sequence: 0,
                encoded_submit: expected
            }]
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn changed_retry_is_rejected_before_network_send() {
        let root = root("identity-conflict");
        let mut outbox = ProducerOutbox::open(&root, identity(), 1024 * 1024).expect("open");
        outbox.stage_submit(&submit(0, b"first")).expect("stage");
        assert!(matches!(
            outbox.stage_submit(&submit(0, b"changed")),
            Err(ProducerOutboxError::IdentityConflict { sequence: 0 })
        ));
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn ack_is_durable_and_sequence_never_reused_after_restart() {
        let root = root("ack");
        {
            let mut outbox = ProducerOutbox::open(&root, identity(), 1024 * 1024).expect("open");
            outbox.stage_submit(&submit(0, b"first")).expect("stage");
            outbox.mark_committed(7, 0).expect("ack");
            assert!(outbox.pending_submissions().is_empty());
        }
        let mut outbox = ProducerOutbox::open(&root, identity(), 1024 * 1024).expect("reopen");
        assert_eq!(outbox.next_sequence().expect("next"), 1);
        assert!(matches!(
            outbox.stage_submit(&submit(0, b"different")),
            Err(ProducerOutboxError::IdentityConflict { sequence: 0 })
        ));
        outbox
            .stage_submit(&submit(1, b"second"))
            .expect("next stage");
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn outbox_target_is_durable_but_route_is_not() {
        let root = root("target");
        let outbox = ProducerOutbox::open(&root, identity(), 1024 * 1024).expect("open");
        assert!(outbox.hello_frame().is_ok());
        drop(outbox);
        let mut changed = identity();
        changed.target = "canon/other/verse/host-metrics".into();
        assert!(matches!(
            ProducerOutbox::open(&root, changed, 1024 * 1024),
            Err(ProducerOutboxError::IdentityMismatch)
        ));
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn torn_unsynced_tail_is_discarded_but_complete_corruption_is_not() {
        let root = root("tail");
        let first = submit(0, b"first");
        {
            let mut outbox = ProducerOutbox::open(&root, identity(), 1024 * 1024).expect("open");
            outbox.stage_submit(&first).expect("stage");
        }
        let wal = root.join(WAL_NAME);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&wal)
            .expect("append torn tail");
        file.write_all(&[0, 0]).expect("tear");
        drop(file);
        let outbox = ProducerOutbox::open(&root, identity(), 1024 * 1024).expect("discard tail");
        assert_eq!(outbox.pending_submissions().len(), 1);
        drop(outbox);
        let mut bytes = std::fs::read(&wal).expect("read WAL");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&wal, bytes).expect("corrupt WAL");
        assert!(matches!(
            ProducerOutbox::open(&root, identity(), 1024 * 1024),
            Err(ProducerOutboxError::Corrupt("record checksum"))
        ));
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}
