//! File-backed spool storage with exclusive process ownership.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::frame::SpoolFrame;
use super::storage::{
    ScanTail, SpoolError, SpoolStorage, SpoolStorageFaults, ValidFrame, encoded_frame_bytes,
    scan_bytes,
};

const LOCK_NAME: &str = "OWNER.lock";
const SEGMENT_NAME: &str = "segment-000001.wal";

/// Held exclusive ownership of a spool directory (PID lock file).
struct OwnerLock {
    path: PathBuf,
    pid: u32,
}

impl OwnerLock {
    fn acquire(root: &Path) -> Result<Self, SpoolError> {
        let path = root.join(LOCK_NAME);
        let pid = std::process::id();
        if path.exists() {
            let existing = std::fs::read_to_string(&path).unwrap_or_default();
            if let Ok(other) = existing.trim().parse::<u32>()
                && other != pid
                && pid_appears_alive(other)
            {
                return Err(SpoolError::Locked);
            }
            // Stale lock from a dead process: reclaim.
            let _ = std::fs::remove_file(&path);
        }
        // Exclusive create — if we race another live owner, fail closed.
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    SpoolError::Locked
                } else {
                    SpoolError::Io(error)
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
        // Fail closed: treat unknown platforms as live so we do not steal.
        let _ = pid;
        true
    }
}

/// Durable file spool: one active segment, exclusive owner lock.
pub struct FileSpoolStorage {
    root: PathBuf,
    _lock: OwnerLock,
    segment: File,
    used_bytes: usize,
    frame_count: usize,
    faults: SpoolStorageFaults,
}

impl FileSpoolStorage {
    /// Opens `root`, creating it if needed, and takes exclusive ownership.
    ///
    /// Live concurrent owners fail closed. A lock left by a dead process is
    /// reclaimed so crash restart can classify `RecoveryRequired`.
    ///
    /// Ownership uses a PID lock file: best-effort on the local host only.
    /// PID reuse can reclaim a live peer incorrectly; this is not a portable
    /// cross-host lock and must not be used as shared-filesystem coordination.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, SpoolError> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        let lock = OwnerLock::acquire(&root)?;

        let segment_path = root.join(SEGMENT_NAME);
        let mut segment = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&segment_path)?;
        let used_bytes = segment.seek(SeekFrom::End(0))? as usize;
        let mut bytes = Vec::new();
        segment.seek(SeekFrom::Start(0))?;
        segment.read_to_end(&mut bytes)?;
        segment.seek(SeekFrom::End(0))?;
        let (frames, _) = scan_bytes(&bytes);

        Ok(Self {
            root,
            _lock: lock,
            segment,
            used_bytes,
            frame_count: frames.len(),
            faults: SpoolStorageFaults::default(),
        })
    }

    /// Spool directory path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Read-only scan without taking the owner lock (inspection / lab diagnostics).
    pub fn inspect(root: impl AsRef<Path>) -> Result<(Vec<ValidFrame>, ScanTail), SpoolError> {
        let path = root.as_ref().join(SEGMENT_NAME);
        if !path.exists() {
            return Ok((Vec::new(), ScanTail::CleanEof));
        }
        let mut file = File::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(scan_bytes(&bytes))
    }
}

impl SpoolStorage for FileSpoolStorage {
    fn append_frame(&mut self, frame: &SpoolFrame) -> Result<(), SpoolError> {
        if self.faults.fail_next_append {
            self.faults.fail_next_append = false;
            return Err(SpoolError::CapacityExceeded);
        }
        let encoded = encoded_frame_bytes(frame)?;
        let full_len = encoded.len();
        let to_write = if let Some(tear) = self.faults.tear_after_bytes.take() {
            &encoded[..tear.min(full_len)]
        } else {
            &encoded
        };
        self.segment.write_all(to_write)?;
        self.used_bytes = self.used_bytes.saturating_add(to_write.len());
        if to_write.len() == full_len {
            self.frame_count = self.frame_count.saturating_add(1);
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), SpoolError> {
        if self.faults.fail_next_sync {
            self.faults.fail_next_sync = false;
            return Err(SpoolError::Io(std::io::Error::other(
                "injected sync failure",
            )));
        }
        self.segment.flush()?;
        self.segment.sync_all()?;
        Ok(())
    }

    fn scan_valid_frames(&self) -> Result<(Vec<ValidFrame>, ScanTail), SpoolError> {
        FileSpoolStorage::inspect(&self.root)
    }

    fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    fn frame_count(&self) -> usize {
        self.frame_count
    }

    fn set_faults(&mut self, faults: SpoolStorageFaults) {
        self.faults = faults;
    }
}
