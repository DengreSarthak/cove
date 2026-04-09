use std::str::FromStr as _;

use cove_cspp::backup_data::{EncryptedWalletBackup, WalletEntry};
use cove_cspp::wallet_crypto;
use cove_device::cloud_storage::{CloudStorage, CloudStorageError};
use cove_util::ResultExt as _;
use tracing::{info, warn};
use zeroize::Zeroizing;

use super::super::{CloudBackupError, LocalWalletMode, LocalWalletSecret};
use super::payload::{convert_cloud_secret, descriptor_pair_from_cloud};
use super::{DownloadedWalletBackup, RemoteWalletBackupSummary, decode_cloud_labels_jsonl};
use crate::backup::import::{LabelRestoreBehavior, LabelRestoreWarning, restore_wallet_labels};
use crate::wallet::fingerprint::Fingerprint;
use crate::wallet::metadata::WalletMetadata;

pub(crate) enum WalletBackupLookup<T> {
    Found(T),
    NotFound,
    UnsupportedVersion(u32),
}

type ExistingFingerprints = Vec<(Fingerprint, cove_types::network::Network, LocalWalletMode)>;

pub(crate) struct WalletBackupReader {
    cloud: CloudStorage,
    namespace: String,
    critical_key: Zeroizing<[u8; 32]>,
}

impl WalletBackupReader {
    pub(crate) fn new(
        cloud: CloudStorage,
        namespace: String,
        critical_key: Zeroizing<[u8; 32]>,
    ) -> Self {
        Self { cloud, namespace, critical_key }
    }

    pub(crate) fn download(
        &self,
        record_id: &str,
    ) -> Result<DownloadedWalletBackup, CloudBackupError> {
        match self.lookup(record_id)? {
            WalletBackupLookup::Found(wallet) => Ok(wallet),
            WalletBackupLookup::NotFound => Err(CloudBackupError::Cloud(format!(
                "download {record_id}: not found in cloud backup"
            ))),
            WalletBackupLookup::UnsupportedVersion(version) => Err(CloudBackupError::Internal(
                format!("download {record_id}: unsupported wallet backup version {version}"),
            )),
        }
    }

    pub(crate) fn summary(
        &self,
        record_id: &str,
    ) -> Result<WalletBackupLookup<RemoteWalletBackupSummary>, CloudBackupError> {
        Ok(match self.lookup_entry(record_id)? {
            WalletBackupLookup::Found(entry) => {
                WalletBackupLookup::Found(RemoteWalletBackupSummary {
                    revision_hash: entry.content_revision_hash.clone(),
                    label_count: entry.labels_count,
                    updated_at: entry.updated_at,
                })
            }
            WalletBackupLookup::NotFound => WalletBackupLookup::NotFound,
            WalletBackupLookup::UnsupportedVersion(version) => {
                WalletBackupLookup::UnsupportedVersion(version)
            }
        })
    }

    pub(crate) fn lookup(
        &self,
        record_id: &str,
    ) -> Result<WalletBackupLookup<DownloadedWalletBackup>, CloudBackupError> {
        Ok(match self.lookup_entry(record_id)? {
            WalletBackupLookup::Found(entry) => {
                let metadata = serde_json::from_value(entry.metadata.clone())
                    .map_err_prefix("parse wallet metadata", CloudBackupError::Internal)?;
                WalletBackupLookup::Found(DownloadedWalletBackup { metadata, entry })
            }
            WalletBackupLookup::NotFound => WalletBackupLookup::NotFound,
            WalletBackupLookup::UnsupportedVersion(version) => {
                WalletBackupLookup::UnsupportedVersion(version)
            }
        })
    }

    pub(crate) fn lookup_entry(
        &self,
        record_id: &str,
    ) -> Result<WalletBackupLookup<WalletEntry>, CloudBackupError> {
        Ok(match self.download_encrypted(record_id)? {
            WalletBackupLookup::Found(encrypted) => WalletBackupLookup::Found(
                self.decrypt_entry(&encrypted)
                    .map_err_prefix("decrypt wallet", CloudBackupError::Crypto)?,
            ),
            WalletBackupLookup::NotFound => WalletBackupLookup::NotFound,
            WalletBackupLookup::UnsupportedVersion(version) => {
                WalletBackupLookup::UnsupportedVersion(version)
            }
        })
    }

    pub(crate) fn download_encrypted(
        &self,
        record_id: &str,
    ) -> Result<WalletBackupLookup<EncryptedWalletBackup>, CloudBackupError> {
        let wallet_json = match self
            .cloud
            .download_wallet_backup(self.namespace.clone(), record_id.to_string())
        {
            Ok(wallet_json) => wallet_json,
            Err(CloudStorageError::NotFound(_)) => return Ok(WalletBackupLookup::NotFound),
            Err(error) => {
                return Err(CloudBackupError::Cloud(format!("download {record_id}: {error}")));
            }
        };

        let encrypted: EncryptedWalletBackup = serde_json::from_slice(&wallet_json)
            .map_err_prefix("deserialize wallet", CloudBackupError::Internal)?;

        if encrypted.version != 1 {
            let version = encrypted.version;
            warn!(
                "Skipping wallet backup {record_id}: unsupported wallet backup version {version}"
            );
            return Ok(WalletBackupLookup::UnsupportedVersion(version));
        }

        Ok(WalletBackupLookup::Found(encrypted))
    }

    pub(crate) fn decrypt_entry(
        &self,
        encrypted: &EncryptedWalletBackup,
    ) -> Result<WalletEntry, cove_cspp::CsppError> {
        wallet_crypto::decrypt_wallet_backup(encrypted, &self.critical_key)
    }
}

pub(crate) struct WalletRestoreSession(ExistingFingerprints);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct WalletRestoreOutcome {
    pub(crate) labels_warning: Option<LabelRestoreWarning>,
}

impl WalletRestoreSession {
    pub(crate) fn new(existing_fingerprints: ExistingFingerprints) -> Self {
        Self(existing_fingerprints)
    }

    pub(crate) fn restore_record(
        &mut self,
        reader: &WalletBackupReader,
        record_id: &str,
    ) -> Result<WalletRestoreOutcome, CloudBackupError> {
        let wallet = reader.download(record_id)?;
        self.restore_downloaded(&wallet)
    }

    pub(crate) fn restore_downloaded(
        &mut self,
        wallet: &DownloadedWalletBackup,
    ) -> Result<WalletRestoreOutcome, CloudBackupError> {
        if should_skip_duplicate_wallet(&wallet.metadata, &self.0) {
            return Ok(WalletRestoreOutcome::default());
        }

        let outcome = restore_downloaded_wallet(&wallet.metadata, &wallet.entry)?;
        remember_restored_wallet_fingerprint(&wallet.metadata, &mut self.0);

        Ok(outcome)
    }
}

fn should_skip_duplicate_wallet(
    metadata: &WalletMetadata,
    existing_fingerprints: &ExistingFingerprints,
) -> bool {
    if crate::backup::import::is_wallet_duplicate(metadata, existing_fingerprints)
        .inspect_err(|error| {
            warn!("is_wallet_duplicate check failed for {}: {error}", metadata.name)
        })
        .unwrap_or(false)
    {
        info!("Skipping duplicate wallet {}", metadata.name);
        true
    } else {
        false
    }
}

fn restore_downloaded_wallet(
    metadata: &WalletMetadata,
    entry: &WalletEntry,
) -> Result<WalletRestoreOutcome, CloudBackupError> {
    let backup_model = crate::backup::model::WalletBackup {
        metadata: entry.metadata.clone(),
        secret: convert_cloud_secret(&entry.secret),
        descriptors: descriptor_pair_from_cloud(&entry.descriptors),
        xpub: entry.xpub.clone(),
        labels_jsonl: decode_cloud_labels_jsonl(entry)?,
    };

    match &backup_model.secret {
        LocalWalletSecret::Mnemonic(words) => {
            let mnemonic = bip39::Mnemonic::from_str(words)
                .map_err_prefix("invalid mnemonic", CloudBackupError::Internal)?;

            crate::backup::import::restore_cloud_mnemonic_wallet(metadata, mnemonic).map_err(
                |(error, _)| {
                    CloudBackupError::Internal(format!("restore mnemonic wallet: {error}"))
                },
            )?;
        }
        _ => {
            crate::backup::import::restore_cloud_descriptor_wallet(metadata, &backup_model)
                .map_err(|(error, _)| {
                    CloudBackupError::Internal(format!("restore descriptor wallet: {error}"))
                })?;
        }
    }

    let labels_outcome = restore_wallet_labels(
        &metadata.id,
        &metadata.name,
        backup_model.labels_jsonl.as_deref(),
        LabelRestoreBehavior::PreserveCloudBackupClean,
    );
    if let Some(warning) = &labels_outcome.warning {
        warn!("Failed to restore labels for wallet {}: {}", metadata.name, warning.error);
    }

    Ok(WalletRestoreOutcome { labels_warning: labels_outcome.warning })
}

fn remember_restored_wallet_fingerprint(
    metadata: &WalletMetadata,
    existing_fingerprints: &mut ExistingFingerprints,
) {
    if let Some(fingerprint) = &metadata.master_fingerprint {
        existing_fingerprints.push((**fingerprint, metadata.network, metadata.wallet_mode));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cove_cspp::backup_data::WalletSecret;
    use cove_device::cloud_storage::{CloudStorageAccess, CloudStorageError};

    fn test_wallet_entry(metadata: &WalletMetadata) -> WalletEntry {
        WalletEntry {
            wallet_id: metadata.id.to_string(),
            secret: WalletSecret::WatchOnly,
            metadata: serde_json::to_value(metadata).unwrap(),
            descriptors: None,
            xpub: None,
            wallet_mode: cove_cspp::backup_data::WalletMode::Main,
            labels_zstd_jsonl: None,
            labels_count: 0,
            labels_hash: None,
            labels_uncompressed_size: None,
            content_revision_hash: "test-revision".into(),
            updated_at: 42,
        }
    }

    #[derive(Debug)]
    struct NoopCloudStorage;

    impl CloudStorageAccess for NoopCloudStorage {
        fn upload_master_key_backup(
            &self,
            _namespace: String,
            _data: Vec<u8>,
        ) -> Result<(), CloudStorageError> {
            Err(CloudStorageError::NotAvailable("unused in test".into()))
        }

        fn upload_wallet_backup(
            &self,
            _namespace: String,
            _record_id: String,
            _data: Vec<u8>,
        ) -> Result<(), CloudStorageError> {
            Err(CloudStorageError::NotAvailable("unused in test".into()))
        }

        fn download_master_key_backup(
            &self,
            _namespace: String,
        ) -> Result<Vec<u8>, CloudStorageError> {
            Err(CloudStorageError::NotAvailable("unused in test".into()))
        }

        fn download_wallet_backup(
            &self,
            _namespace: String,
            _record_id: String,
        ) -> Result<Vec<u8>, CloudStorageError> {
            Err(CloudStorageError::NotAvailable("unused in test".into()))
        }

        fn delete_wallet_backup(
            &self,
            _namespace: String,
            _record_id: String,
        ) -> Result<(), CloudStorageError> {
            Err(CloudStorageError::NotAvailable("unused in test".into()))
        }

        fn list_namespaces(&self) -> Result<Vec<String>, CloudStorageError> {
            Ok(Vec::new())
        }

        fn list_wallet_files(&self, _namespace: String) -> Result<Vec<String>, CloudStorageError> {
            Ok(Vec::new())
        }

        fn is_backup_uploaded(
            &self,
            _namespace: String,
            _record_id: String,
        ) -> Result<bool, CloudStorageError> {
            Ok(false)
        }
    }

    fn test_cloud_storage() -> CloudStorage {
        CloudStorage::new(Box::new(NoopCloudStorage))
    }

    #[test]
    fn decrypt_entry_round_trips_encrypted_wallet_entry() {
        let metadata = WalletMetadata::preview_new();
        let entry = test_wallet_entry(&metadata);
        let critical_key = [7; 32];
        let encrypted = wallet_crypto::encrypt_wallet_entry(&entry, &critical_key).unwrap();
        let reader = WalletBackupReader::new(
            test_cloud_storage(),
            "test-namespace".into(),
            Zeroizing::new(critical_key),
        );

        let decrypted = reader.decrypt_entry(&encrypted).unwrap();

        assert_eq!(decrypted.wallet_id, entry.wallet_id);
        assert_eq!(decrypted.content_revision_hash, entry.content_revision_hash);
    }

    #[test]
    fn restore_session_skips_duplicate_wallet() {
        let metadata = WalletMetadata::preview_new();
        let wallet = DownloadedWalletBackup {
            metadata: metadata.clone(),
            entry: test_wallet_entry(&metadata),
        };
        let existing_fingerprints = vec![(
            *metadata.master_fingerprint.as_ref().unwrap().as_ref(),
            metadata.network,
            metadata.wallet_mode,
        )];
        let mut session = WalletRestoreSession::new(existing_fingerprints);

        session.restore_downloaded(&wallet).unwrap();

        assert_eq!(session.0.len(), 1);
    }

    #[test]
    fn remember_restored_wallet_fingerprint_tracks_restored_wallet() {
        let metadata = WalletMetadata::preview_new();
        let mut existing_fingerprints = Vec::new();

        remember_restored_wallet_fingerprint(&metadata, &mut existing_fingerprints);

        assert_eq!(existing_fingerprints.len(), 1);
        assert_eq!(existing_fingerprints[0].0, *metadata.master_fingerprint.unwrap().as_ref());
    }
}
