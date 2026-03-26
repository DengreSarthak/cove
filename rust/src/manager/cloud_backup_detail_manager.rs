use std::sync::Arc;

use flume::Receiver;
use tracing::error;

use super::cloud_backup_manager::{
    CLOUD_BACKUP_MANAGER, CloudBackupDetail, CloudBackupWalletItem, DeepVerificationFailure,
    DeepVerificationReport, DeepVerificationResult, RustCloudBackupManager,
};
use super::deferred_sender::{MessageSender, SingleOrMany};

#[derive(Debug, Clone, uniffi::Enum)]
pub enum RecoveryAction {
    RecreateManifest,
    ReinitializeBackup,
    RepairPasskey,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum VerificationState {
    Idle,
    Verifying,
    Verified(DeepVerificationReport),
    PasskeyConfirmed,
    Failed(DeepVerificationFailure),
    Cancelled,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum SyncState {
    Idle,
    Syncing,
    Failed(String),
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum RecoveryState {
    Idle,
    Recovering(RecoveryAction),
    Failed { action: RecoveryAction, error: String },
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum CloudOnlyState {
    NotFetched,
    Loading,
    Loaded { wallets: Vec<CloudBackupWalletItem> },
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum CloudOnlyOperation {
    Idle,
    Operating { record_id: String },
    Failed { error: String },
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum CloudBackupDetailAction {
    StartVerification,
    StartVerificationDiscoverable,
    RecreateManifest,
    ReinitializeBackup,
    RepairPasskey,
    SyncUnsynced,
    FetchCloudOnly,
    RestoreCloudWallet { record_id: String },
    DeleteCloudWallet { record_id: String },
    RefreshDetail,
}

type Message = CloudBackupDetailReconcileMessage;

#[derive(Debug, Clone, uniffi::Enum)]
pub enum CloudBackupDetailReconcileMessage {
    DetailUpdated(CloudBackupDetail),
    VerificationChanged(VerificationState),
    SyncChanged(SyncState),
    RecoveryChanged(RecoveryState),
    CloudOnlyChanged(CloudOnlyState),
    CloudOnlyWalletRemoved(String),
    CloudOnlyOperationChanged(CloudOnlyOperation),
}

#[uniffi::export(callback_interface)]
pub trait CloudBackupDetailManagerReconciler: Send + Sync + std::fmt::Debug + 'static {
    fn reconcile(&self, message: CloudBackupDetailReconcileMessage);
    fn reconcile_many(&self, messages: Vec<CloudBackupDetailReconcileMessage>);
}

#[derive(Clone, Debug, uniffi::Object)]
pub struct RustCloudBackupDetailManager {
    reconciler: MessageSender<Message>,
    reconcile_receiver: Arc<Receiver<SingleOrMany<Message>>>,
}

#[uniffi::export]
impl RustCloudBackupDetailManager {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        let (sender, receiver) = flume::bounded(1000);
        Arc::new(Self {
            reconciler: MessageSender::new(sender),
            reconcile_receiver: Arc::new(receiver),
        })
    }

    pub fn listen_for_updates(&self, reconciler: Box<dyn CloudBackupDetailManagerReconciler>) {
        let reconcile_receiver = self.reconcile_receiver.clone();

        std::thread::spawn(move || {
            while let Ok(field) = reconcile_receiver.recv() {
                match field {
                    SingleOrMany::Single(message) => reconciler.reconcile(message),
                    SingleOrMany::Many(messages) => reconciler.reconcile_many(messages),
                }
            }
        });
    }

    pub fn dispatch(&self, action: CloudBackupDetailAction) {
        let this = self.clone();
        cove_tokio::task::spawn_blocking(move || match action {
            CloudBackupDetailAction::StartVerification => this.handle_start_verification(false),
            CloudBackupDetailAction::StartVerificationDiscoverable => {
                this.handle_start_verification(true)
            }
            CloudBackupDetailAction::RecreateManifest => {
                this.handle_recovery(RecoveryAction::RecreateManifest)
            }
            CloudBackupDetailAction::ReinitializeBackup => {
                this.handle_recovery(RecoveryAction::ReinitializeBackup)
            }
            CloudBackupDetailAction::RepairPasskey => {
                this.handle_recovery(RecoveryAction::RepairPasskey)
            }
            CloudBackupDetailAction::SyncUnsynced => this.handle_sync(),
            CloudBackupDetailAction::FetchCloudOnly => this.handle_fetch_cloud_only(),
            CloudBackupDetailAction::RestoreCloudWallet { record_id } => {
                this.handle_restore_cloud_wallet(&record_id)
            }
            CloudBackupDetailAction::DeleteCloudWallet { record_id } => {
                this.handle_delete_cloud_wallet(&record_id)
            }
            CloudBackupDetailAction::RefreshDetail => this.handle_refresh_detail(),
        });
    }
}

impl RustCloudBackupDetailManager {
    fn send(&self, message: Message) {
        self.reconciler.send(message);
    }

    fn backup_manager(&self) -> &RustCloudBackupManager {
        &CLOUD_BACKUP_MANAGER
    }

    fn handle_start_verification(&self, force_discoverable: bool) {
        self.send(Message::VerificationChanged(VerificationState::Verifying));
        let mgr = self.backup_manager();

        let result = mgr.deep_verify_cloud_backup(force_discoverable);

        match result {
            DeepVerificationResult::Verified(report) => {
                if let Some(detail) = &report.detail {
                    self.send(Message::DetailUpdated(detail.clone()));
                }
                self.send(Message::VerificationChanged(VerificationState::Verified(report)));
            }
            DeepVerificationResult::PasskeyConfirmed(detail) => {
                if let Some(detail) = detail {
                    self.send(Message::DetailUpdated(detail));
                }
                self.send(Message::VerificationChanged(VerificationState::PasskeyConfirmed));
            }
            DeepVerificationResult::PasskeyMissing(detail) => {
                if let Some(detail) = detail {
                    self.send(Message::DetailUpdated(detail));
                }
                // auto-start passkey repair without requiring a separate user action
                self.handle_recovery(RecoveryAction::RepairPasskey);
            }
            DeepVerificationResult::UserCancelled(detail) => {
                if let Some(detail) = detail {
                    self.send(Message::DetailUpdated(detail));
                }
                self.send(Message::VerificationChanged(VerificationState::Cancelled));
            }
            DeepVerificationResult::NotEnabled => {}
            DeepVerificationResult::Failed(failure) => {
                if let Some(detail) = failure.detail.clone() {
                    self.send(Message::DetailUpdated(detail));
                }
                self.send(Message::VerificationChanged(VerificationState::Failed(failure)));
            }
        }
    }

    fn handle_recovery(&self, action: RecoveryAction) {
        self.send(Message::RecoveryChanged(RecoveryState::Recovering(action.clone())));
        let mgr = self.backup_manager();

        let result = match &action {
            RecoveryAction::RecreateManifest => mgr.do_reupload_all_wallets(),
            RecoveryAction::ReinitializeBackup => mgr.do_enable_cloud_backup(),
            RecoveryAction::RepairPasskey => mgr.do_repair_passkey_wrapper(),
        };

        match result {
            Ok(()) => {
                self.send(Message::RecoveryChanged(RecoveryState::Idle));
                self.handle_start_verification(false);
            }
            Err(e) => {
                self.send(Message::RecoveryChanged(RecoveryState::Failed {
                    action,
                    error: e.to_string(),
                }));
            }
        }
    }

    fn handle_sync(&self) {
        self.send(Message::SyncChanged(SyncState::Syncing));
        let mgr = self.backup_manager();

        match mgr.do_sync_unsynced_wallets() {
            Ok(()) => {
                self.handle_refresh_detail();
                self.send(Message::SyncChanged(SyncState::Idle));
            }
            Err(e) => {
                self.send(Message::SyncChanged(SyncState::Failed(e.to_string())));
            }
        }
    }

    fn handle_fetch_cloud_only(&self) {
        self.send(Message::CloudOnlyChanged(CloudOnlyState::Loading));
        let mgr = self.backup_manager();

        match mgr.do_fetch_cloud_only_wallets() {
            Ok(items) => {
                self.send(Message::CloudOnlyChanged(CloudOnlyState::Loaded { wallets: items }));
            }
            Err(e) => {
                error!("Failed to fetch cloud-only wallets: {e}");
                self.send(Message::CloudOnlyChanged(CloudOnlyState::Loaded {
                    wallets: Vec::new(),
                }));
            }
        }
    }

    fn handle_restore_cloud_wallet(&self, record_id: &str) {
        self.send(Message::CloudOnlyOperationChanged(CloudOnlyOperation::Operating {
            record_id: record_id.to_string(),
        }));
        let mgr = self.backup_manager();

        match mgr.do_restore_cloud_wallet(record_id) {
            Ok(()) => {
                self.send(Message::CloudOnlyOperationChanged(CloudOnlyOperation::Idle));
                self.send(Message::CloudOnlyWalletRemoved(record_id.to_string()));
                self.handle_refresh_detail();
            }
            Err(e) => {
                self.send(Message::CloudOnlyOperationChanged(CloudOnlyOperation::Failed {
                    error: e.to_string(),
                }));
            }
        }
    }

    fn handle_delete_cloud_wallet(&self, record_id: &str) {
        self.send(Message::CloudOnlyOperationChanged(CloudOnlyOperation::Operating {
            record_id: record_id.to_string(),
        }));
        let mgr = self.backup_manager();

        match mgr.do_delete_cloud_wallet(record_id) {
            Ok(()) => {
                self.send(Message::CloudOnlyOperationChanged(CloudOnlyOperation::Idle));
                self.send(Message::CloudOnlyWalletRemoved(record_id.to_string()));
                self.handle_refresh_detail();
            }
            Err(e) => {
                self.send(Message::CloudOnlyOperationChanged(CloudOnlyOperation::Failed {
                    error: e.to_string(),
                }));
            }
        }
    }

    fn handle_refresh_detail(&self) {
        let mgr = self.backup_manager();

        if let Some(result) = mgr.refresh_cloud_backup_detail() {
            match result {
                super::cloud_backup_manager::CloudBackupDetailResult::Success(detail) => {
                    self.send(Message::DetailUpdated(detail));
                }
                super::cloud_backup_manager::CloudBackupDetailResult::AccessError(e) => {
                    error!("Failed to refresh detail: {e}");
                }
            }
        }
    }
}
