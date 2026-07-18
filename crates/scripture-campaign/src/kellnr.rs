//! Kellnr / release-attestation classification for campaign runs.

use std::path::Path;

use serde::Serialize;

/// How a campaign image/binary was built relative to Kellnr.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseClassification {
    /// Ordinary developer/source tree build — cannot close release rows.
    DevelopmentSource,
    /// Image/binary resolved locked packages from a selected Kellnr RC manifest.
    KellnrRc,
}

impl ReleaseClassification {
    /// Stable artifact label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DevelopmentSource => "development-source",
            Self::KellnrRc => "kellnr-rc",
        }
    }
}

/// Redacted release attestation recorded in run.json / preflight.
#[derive(Debug, Clone, Serialize)]
pub struct ReleaseAttestation {
    /// Classification for this run.
    pub classification: ReleaseClassification,
    /// Whether an operator-local Kellnr config was present.
    pub kellnr_config_present: bool,
    /// Whether a selected RC manifest path was present.
    pub rc_manifest_present: bool,
    /// Advisory notes (never secrets).
    pub notes: Vec<String>,
}

impl ReleaseAttestation {
    /// Classifies the local tree. Never reads credential values.
    #[must_use]
    pub fn detect(repo_root: &Path) -> Self {
        let kellnr_env = repo_root.join("config/local/kellnr/registry.env");
        let rc_manifest = repo_root.join("config/local/kellnr/rc-manifest.json");
        let kellnr_config_present = kellnr_env.is_file();
        let rc_manifest_present = rc_manifest.is_file();
        let mut notes = Vec::new();

        if !kellnr_config_present {
            notes.push(
                "config/local/kellnr/registry.env absent; runs are development-source only".into(),
            );
        }
        if !rc_manifest_present {
            notes.push(
                "config/local/kellnr/rc-manifest.json absent; cannot claim kellnr-rc attestation"
                    .into(),
            );
        }

        // A future release-class execute path must verify image package identities
        // against the RC manifest. Presence alone is not enough to claim kellnr-rc
        // until that verification lands; stay honest.
        let classification = ReleaseClassification::DevelopmentSource;
        if kellnr_config_present && rc_manifest_present {
            notes.push(
                "Kellnr config + RC manifest present, but locked image package verification is not yet implemented; classifying as development-source".into(),
            );
        }

        Self {
            classification,
            kellnr_config_present,
            rc_manifest_present,
            notes,
        }
    }

    /// Whether this classification may close family 22.
    #[must_use]
    pub fn can_close_release_row(&self) -> bool {
        matches!(self.classification, ReleaseClassification::KellnrRc)
    }
}
