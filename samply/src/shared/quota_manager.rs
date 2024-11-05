use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use bytesize::ByteSize;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::file_inventory::{FileInfo, FileInventory};

pub struct QuotaManager {
    inner: Arc<Mutex<QuotaManagerInner>>,
    /// Sent from QuotaManager::finish
    stop_signal_sender: tokio::sync::oneshot::Sender<()>,
    /// Sent whenever a file is added. A `Notify` has the semantics
    /// we want if the sender side notifies more frequently than the
    /// receiver side can check; multiple notifications are coalesced
    /// into one.
    eviction_signal_sender: Arc<tokio::sync::Notify>,
    /// The join handle for the tokio task that receives the signals
    /// and deletes the files. Stored here so that finish() can block on it.
    join_handle: JoinHandle<()>,
}

pub struct QuotaManagerNotifier {
    inner: Arc<Mutex<QuotaManagerInner>>,
    eviction_signal_sender: Arc<tokio::sync::Notify>,
}

struct QuotaManagerInner {
    inventory: FileInventory,
    max_size_bytes: Option<u64>,
    max_age_seconds: Option<u64>,
}

impl QuotaManager {
    pub fn new(
        root_path: &Path,
        db_path: &Path,
        max_size_bytes: Option<u64>,
        max_age_seconds: Option<u64>,
    ) -> Self {
        let root_path = root_path.to_path_buf();
        let root_path_clone = root_path.clone();
        let inventory = FileInventory::new(&root_path, db_path, move || {
            Self::list_existing_files_sync(&root_path_clone)
        })
        .unwrap();

        let inner = QuotaManagerInner {
            inventory,
            max_size_bytes,
            max_age_seconds,
        };
        let inner = Arc::new(Mutex::new(inner));

        let (stop_signal_sender, mut stop_signal_receiver) = tokio::sync::oneshot::channel();

        let eviction_signal_sender = Arc::new(Notify::new());
        let eviction_signal_receiver = eviction_signal_sender.clone();

        let inner_clone = Arc::clone(&inner);
        let join_handle = tokio::spawn(async move {
            let inner = inner_clone;
            loop {
                tokio::select! {
                    _ = &mut stop_signal_receiver => {
                        return;
                    }
                    _ = eviction_signal_receiver.notified() => {
                        Self::perform_eviction_if_needed(&inner).await;
                    }
                }
            }
        });

        Self {
            inner,
            stop_signal_sender,
            eviction_signal_sender,
            join_handle,
        }
    }

    pub fn notifier(&self) -> QuotaManagerNotifier {
        QuotaManagerNotifier {
            inner: Arc::clone(&self.inner),
            eviction_signal_sender: Arc::clone(&self.eviction_signal_sender),
        }
    }

    pub async fn finish(self) {
        let _ = self.stop_signal_sender.send(());
        self.join_handle.await.unwrap()
    }

    fn list_existing_files_sync(dir: &Path) -> Vec<FileInfo> {
        let mut files = Vec::new();
        let mut dirs_to_visit = VecDeque::new();
        dirs_to_visit.push_back(dir.to_path_buf());

        while let Some(current_dir) = dirs_to_visit.pop_front() {
            let entries = match fs::read_dir(&current_dir) {
                Ok(entries) => entries,
                Err(e) => {
                    log::error!("Failed to read directory {:?}: {}", current_dir, e);
                    continue;
                }
            };
            for entry in entries {
                let path = match entry {
                    Ok(entry) => entry.path(),
                    Err(e) => {
                        log::error!("Failed to read directory entry in {:?}: {}", current_dir, e);
                        continue;
                    }
                };

                if path.is_dir() {
                    dirs_to_visit.push_back(path);
                    continue;
                }
                if !path.is_file() {
                    continue;
                }

                let metadata = match fs::metadata(&path) {
                    Ok(metadata) => metadata,
                    Err(e) => {
                        log::error!("Failed to query file size for {:?}: {}", path, e);
                        continue;
                    }
                };
                files.push(FileInfo {
                    path,
                    size_in_bytes: metadata.len(),
                    creation_time: metadata.created().ok().unwrap_or_else(SystemTime::now),
                    last_access_time: metadata.accessed().ok().unwrap_or_else(SystemTime::now),
                });
            }
        }
        log::info!("Found {} existing files in {:?}", files.len(), dir);
        files
    }

    async fn perform_eviction_if_needed(inner: &Mutex<QuotaManagerInner>) {
        let total_size_before = inner.lock().unwrap().inventory.total_size_in_bytes();
        log::info!("Current total size: {}", ByteSize(total_size_before));

        let files_to_delete_for_enforcing_max_size = {
            let inner = inner.lock().unwrap();
            match inner.max_size_bytes {
                Some(max_size_bytes) => inner
                    .inventory
                    .get_files_to_delete_to_enforce_max_size(max_size_bytes),
                None => vec![],
            }
        };

        if !files_to_delete_for_enforcing_max_size.is_empty() {
            Self::delete_files(files_to_delete_for_enforcing_max_size, inner).await;
            let total_size = inner.lock().unwrap().inventory.total_size_in_bytes();
            log::info!("Current total size: {}", ByteSize(total_size));
        }

        let files_to_delete_for_enforcing_max_age = {
            let inner = inner.lock().unwrap();
            match inner.max_age_seconds {
                Some(max_age_seconds) => inner
                    .inventory
                    .get_files_to_delete_to_enforce_max_age(max_age_seconds),
                None => vec![],
            }
        };

        if !files_to_delete_for_enforcing_max_age.is_empty() {
            Self::delete_files(files_to_delete_for_enforcing_max_age, inner).await;
            let total_size = inner.lock().unwrap().inventory.total_size_in_bytes();
            log::info!("Current total size: {}", ByteSize(total_size));
        }
    }

    async fn delete_files(files: Vec<PathBuf>, inner: &Mutex<QuotaManagerInner>) {
        for file_path in files {
            log::info!("Deleting file {:?}", file_path);
            match tokio::fs::remove_file(&file_path).await {
                Ok(()) => {
                    inner.lock().unwrap().inventory.on_file_deleted(&file_path);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    inner
                        .lock()
                        .unwrap()
                        .inventory
                        .on_file_found_to_be_absent(&file_path);
                }
                Err(e) => {
                    log::error!("Error when deleting {:?}: {}", file_path, e);
                }
            }
            // TODO: delete containing directory if empty
        }
    }
}

impl QuotaManagerNotifier {
    pub fn on_file_created(&self, path: &Path, size_in_bytes: u64, creation_time: SystemTime) {
        self.inner
            .lock()
            .unwrap()
            .inventory
            .on_file_created(path, size_in_bytes, creation_time);
        self.trigger_eviction_if_needed();
    }

    pub fn on_file_accessed(&self, path: &Path, access_time: SystemTime) {
        self.inner
            .lock()
            .unwrap()
            .inventory
            .on_file_accessed(path, access_time);
    }

    pub fn trigger_eviction_if_needed(&self) {
        self.eviction_signal_sender.notify_one();
    }
}
