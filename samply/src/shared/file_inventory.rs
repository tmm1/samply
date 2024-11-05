use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OpenFlags, Transaction};

pub struct FileInventory {
    root_path: PathBuf,
    db_connection: rusqlite::Connection,
}

pub struct FileInfo {
    pub path: PathBuf,
    pub size_in_bytes: u64,
    pub creation_time: SystemTime,
    pub last_access_time: SystemTime,
}

impl FileInventory {
    pub fn new<F>(
        root_path: &Path,
        db_path: &Path,
        list_existing_files_fn: F,
    ) -> rusqlite_migration::Result<Self>
    where
        F: Fn() -> Vec<FileInfo> + Send + Sync + 'static,
    {
        let root_path = root_path
            .canonicalize()
            .unwrap_or_else(|_| root_path.to_path_buf());
        let db_connection = Self::init_db_at(&root_path, db_path, list_existing_files_fn)?;

        Ok(Self {
            root_path,
            db_connection,
        })
    }

    fn init_db_at<F>(
        root_path: &Path,
        db_path: &Path,
        list_existing_files_fn: F,
    ) -> rusqlite_migration::Result<rusqlite::Connection>
    where
        F: Fn() -> Vec<FileInfo> + Send + Sync + 'static,
    {
        use rusqlite_migration::{Migrations, M};

        let list_existing_files_fn = Arc::new(list_existing_files_fn);
        let root_path = root_path.to_path_buf();

        let migrations = Migrations::new(vec![
            M::up_with_hook(
                r#"
                    CREATE TABLE "files"
                    (
                        [Path] TEXT NOT NULL,
                        [Size] INT NOT NULL,
                        [CreationTime] INT NOT NULL,
                        [LastAccessTime] INT NOT NULL,
                        PRIMARY KEY ([Path])
                    );
                    CREATE INDEX idx_files_LastAccessTime ON "files" ([LastAccessTime]);
                "#,
                move |transaction: &Transaction| {
                    let existing_files = list_existing_files_fn();
                    Self::insert_existing_files(&root_path, transaction, existing_files);
                    Ok(())
                },
            ),
            // Future migrations can be added here.
        ]);

        let mut conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        migrations.to_latest(&mut conn)?;

        Ok(conn)
    }

    fn insert_existing_files(
        root_path: &Path,
        transaction: &Transaction,
        existing_files: Vec<FileInfo>,
    ) {
        let mut stmt = transaction
            .prepare(
                r#"
                    INSERT INTO files (Path, Size, CreationTime, LastAccessTime)
                    VALUES (?1, ?2, ?3, ?4)
                    ON CONFLICT(Path) DO UPDATE SET
                        Size=?2,
                        CreationTime=?3,
                        LastAccessTime=?4;
                "#,
            )
            .unwrap();
        for file_info in existing_files {
            let FileInfo {
                path,
                size_in_bytes,
                creation_time,
                last_access_time,
            } = file_info;
            let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
            let Ok(relative_path) = path.strip_prefix(root_path) else {
                continue;
            };

            stmt.execute(params![
                relative_path.to_string_lossy(),
                size_in_bytes,
                creation_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                last_access_time
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64
            ])
            .unwrap();
        }
    }

    fn relative_path_under_managed_directory(&self, path: &Path) -> Option<PathBuf> {
        let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let relative_path = path.strip_prefix(&self.root_path).ok()?;
        Some(relative_path.to_path_buf())
    }

    fn to_absolute_path(&self, relative_path: &Path) -> PathBuf {
        let abs_path = self.root_path.join(relative_path).canonicalize().unwrap();
        assert!(abs_path.starts_with(&self.root_path));
        abs_path
    }

    pub fn on_file_created(&mut self, path: &Path, size_in_bytes: u64, creation_time: SystemTime) {
        let Some(relative_path) = self.relative_path_under_managed_directory(path) else {
            return;
        };

        let creation_time = creation_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let last_access_time = creation_time;

        let mut stmt = self
            .db_connection
            .prepare_cached(
                r#"
                    INSERT INTO files (Path, Size, CreationTime, LastAccessTime)
                    VALUES (?1, ?2, ?3, ?4)
                    ON CONFLICT(Path) DO UPDATE SET
                        Size=?2,
                        CreationTime=?3,
                        LastAccessTime=?4;
                "#,
            )
            .unwrap();
        stmt.execute(params![
            relative_path.to_string_lossy(),
            size_in_bytes as i64,
            creation_time,
            last_access_time
        ])
        .unwrap();
    }

    pub fn on_file_accessed(&mut self, path: &Path, access_time: SystemTime) {
        let Some(relative_path) = self.relative_path_under_managed_directory(path) else {
            return;
        };

        let access_time = access_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        let mut stmt = self
            .db_connection
            .prepare_cached("UPDATE files SET LastAccessTime = ?1 WHERE Path = ?2")
            .unwrap();
        stmt.execute(params![access_time, relative_path.to_string_lossy()])
            .unwrap();
    }

    pub fn on_file_deleted(&mut self, path: &Path) {
        let Some(relative_path) = self.relative_path_under_managed_directory(path) else {
            return;
        };

        let mut stmt = self
            .db_connection
            .prepare_cached("DELETE FROM files WHERE Path = ?1")
            .unwrap();
        stmt.execute(params![relative_path.to_string_lossy()])
            .unwrap();
    }

    pub fn on_file_found_to_be_absent(&mut self, path: &Path) {
        self.on_file_deleted(path);
    }

    pub fn total_size_in_bytes(&self) -> u64 {
        let total_size: i64 = self
            .db_connection
            .query_row("SELECT SUM(Size) FROM files", [], |row| row.get(0))
            .unwrap_or(0);
        total_size as u64
    }

    pub fn get_files_to_delete_to_enforce_max_size(&self, max_size_bytes: u64) -> Vec<PathBuf> {
        let total_size = self.total_size_in_bytes();
        let Some(mut excess_bytes) = total_size.checked_sub(max_size_bytes) else {
            // Nothing needs to be deleted.
            return vec![];
        };

        let mut stmt = self
            .db_connection
            .prepare_cached("SELECT Path, Size FROM files ORDER BY LastAccessTime ASC")
            .unwrap();

        let files = stmt
            .query_map([], |row| {
                let relative_path: String = row.get(0)?;
                let size: i64 = row.get(1)?;
                let path = self.to_absolute_path(Path::new(&relative_path));
                Ok((path, size))
            })
            .unwrap()
            .filter_map(Result::ok);

        let mut files_to_delete = vec![];

        for (path, size) in files {
            let size = u64::try_from(size).unwrap();

            files_to_delete.push(path);
            excess_bytes = excess_bytes.saturating_sub(size);
            if excess_bytes == 0 {
                break;
            }
        }

        files_to_delete
    }

    pub fn get_files_to_delete_to_enforce_max_age(&self, max_age_seconds: u64) -> Vec<PathBuf> {
        let max_age = max_age_seconds as i64;

        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - max_age;

        let mut stmt = self
            .db_connection
            .prepare_cached("SELECT Path FROM files WHERE LastAccessTime < ?1")
            .unwrap();

        let files_to_delete = stmt
            .query_map([cutoff_time], |row| {
                let relative_path: String = row.get(0)?;
                let path = self.to_absolute_path(Path::new(&relative_path));
                Ok(path)
            })
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        files_to_delete
    }
}
