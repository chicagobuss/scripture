//! Durable local progress evidence for a submission identity (decision 0013).

use scripture::chunk::ProducerId;
use scripture::model::JournalId;

/// Identity shared by a submission WAL frame and its progress frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProgressIdentity {
    /// Journal the submission targeted.
    pub journal_id: JournalId,
    /// Producer identity.
    pub producer_id: ProducerId,
    /// Producer epoch.
    pub producer_epoch: u32,
    /// Producer sequence.
    pub sequence: u64,
}
