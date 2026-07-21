use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(test)]
use std::sync::{
    atomic::{AtomicBool, Ordering as AtomicOrdering},
    Arc,
};

use herdr_flow_core::Sha256Digest;

static TEMPORARY_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
const DEFAULT_MAX_ARTIFACT_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct ArtifactStore {
    root: PathBuf,
    objects: PathBuf,
    max_artifact_size: u64,
    #[cfg(test)]
    corrupt_temporary_before_verification: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredArtifact {
    pub sha256: Sha256Digest,
    pub size: u64,
}

#[derive(Debug)]
pub enum ArtifactStoreError {
    Io(io::Error),
    NotRegularFile,
    InsecurePermissions,
    MultipleLinks,
    DigestMismatch,
    SizeOverflow,
    ArtifactTooLarge { size: u64, maximum: u64 },
    AllocationFailed,
    TemporaryNameExhausted,
}

impl ArtifactStore {
    /// Opens the artifact store for the coordinator that owns the run lease.
    /// The parent run-state directory must already exist and be durable. Stale
    /// temporary files from a previous coordinator process are removed before
    /// new writes are accepted.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ArtifactStoreError> {
        Self::open_with_max_size(root, DEFAULT_MAX_ARTIFACT_SIZE)
    }

    pub fn open_with_max_size(
        root: impl AsRef<Path>,
        max_artifact_size: u64,
    ) -> Result<Self, ArtifactStoreError> {
        let root = root.as_ref().to_path_buf();
        create_private_directory(&root)?;
        let objects = root.join("sha256");
        create_private_directory(&objects)?;
        cleanup_stale_temporaries(&objects)?;
        sync_directory(&objects)?;
        sync_directory(&root)?;
        Ok(Self {
            root,
            objects,
            max_artifact_size,
            #[cfg(test)]
            corrupt_temporary_before_verification: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Durably stores immutable bytes under their SHA-256 digest.
    ///
    /// The final link is created without replacement. Concurrent writers of the
    /// same bytes converge on one object; an existing corrupt, writable,
    /// multiply-linked, or non-regular object fails closed.
    pub fn put(&self, bytes: &[u8]) -> Result<StoredArtifact, ArtifactStoreError> {
        let size = u64::try_from(bytes.len()).map_err(|_| ArtifactStoreError::SizeOverflow)?;
        self.validate_size(size)?;
        let digest = Sha256Digest::of_bytes(bytes);
        let final_path = self.path_for(digest);
        if final_path.try_exists().map_err(ArtifactStoreError::Io)? {
            // This fsync also makes a concurrently published link durable before
            // this caller is allowed to report success.
            sync_directory(&self.objects)?;
            return self.verify_path(&final_path, digest);
        }

        let temporary_path = self.create_verified_temporary(bytes, digest)?;
        match fs::hard_link(&temporary_path, &final_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                remove_temporary(&temporary_path)?;
                return Err(ArtifactStoreError::Io(error));
            }
        }

        // Removing the temporary link before one directory fsync makes the final
        // link publication and temporary cleanup durable in the same barrier.
        remove_temporary(&temporary_path)?;
        sync_directory(&self.objects)?;
        self.verify_path(&final_path, digest)
    }

    pub fn read_verified(&self, digest: Sha256Digest) -> Result<Vec<u8>, ArtifactStoreError> {
        let path = self.path_for(digest);
        let mut file = open_regular_nofollow(&path, false)?;
        validate_immutable_inode(&file.metadata().map_err(ArtifactStoreError::Io)?)?;
        read_and_verify(&mut file, digest, None, self.max_artifact_size)
    }

    fn create_verified_temporary(
        &self,
        bytes: &[u8],
        expected_digest: Sha256Digest,
    ) -> Result<PathBuf, ArtifactStoreError> {
        for _ in 0..128 {
            let sequence = TEMPORARY_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = self
                .objects
                .join(format!(".tmp-{}-{sequence}", std::process::id()));
            let mut file = match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(ArtifactStoreError::Io(error)),
            };
            let result = (|| -> Result<(), ArtifactStoreError> {
                file.write_all(bytes).map_err(ArtifactStoreError::Io)?;
                file.set_permissions(fs::Permissions::from_mode(0o400))
                    .map_err(ArtifactStoreError::Io)?;
                file.sync_all().map_err(ArtifactStoreError::Io)?;

                #[cfg(test)]
                if self
                    .corrupt_temporary_before_verification
                    .load(AtomicOrdering::SeqCst)
                {
                    file.seek(SeekFrom::Start(0))
                        .map_err(ArtifactStoreError::Io)?;
                    file.write_all(b"corrupt").map_err(ArtifactStoreError::Io)?;
                    file.sync_all().map_err(ArtifactStoreError::Io)?;
                }

                validate_immutable_inode(&file.metadata().map_err(ArtifactStoreError::Io)?)?;
                read_and_verify(
                    &mut file,
                    expected_digest,
                    Some(bytes.len() as u64),
                    self.max_artifact_size,
                )?;
                Ok(())
            })();
            if let Err(error) = result {
                drop(file);
                remove_temporary(&path)?;
                return Err(error);
            }
            return Ok(path);
        }
        Err(ArtifactStoreError::TemporaryNameExhausted)
    }

    fn verify_path(
        &self,
        path: &Path,
        expected_digest: Sha256Digest,
    ) -> Result<StoredArtifact, ArtifactStoreError> {
        let mut file = open_regular_nofollow(path, false)?;
        let mut metadata = file.metadata().map_err(ArtifactStoreError::Io)?;

        // If another writer published this inode before removing its private
        // temporary link, remove only matching .tmp-* links inside the private
        // object directory. Any remaining link proves an outside alias.
        if metadata.nlink() > 1 {
            cleanup_temporary_links_for_inode(&self.objects, &metadata)?;
            sync_directory(&self.objects)?;
            metadata = file.metadata().map_err(ArtifactStoreError::Io)?;
        }
        validate_immutable_inode(&metadata)?;
        let bytes = read_and_verify(
            &mut file,
            expected_digest,
            Some(metadata.len()),
            self.max_artifact_size,
        )?;
        let size = u64::try_from(bytes.len()).map_err(|_| ArtifactStoreError::SizeOverflow)?;
        Ok(StoredArtifact {
            sha256: expected_digest,
            size,
        })
    }

    fn validate_size(&self, size: u64) -> Result<(), ArtifactStoreError> {
        if size > self.max_artifact_size {
            return Err(ArtifactStoreError::ArtifactTooLarge {
                size,
                maximum: self.max_artifact_size,
            });
        }
        Ok(())
    }

    fn path_for(&self, digest: Sha256Digest) -> PathBuf {
        let digest = digest.to_prefixed_string();
        self.objects.join(&digest["sha256:".len()..])
    }

    #[cfg(test)]
    fn inject_temporary_corruption(&self, enabled: bool) {
        self.corrupt_temporary_before_verification
            .store(enabled, AtomicOrdering::SeqCst);
    }
}

fn create_private_directory(path: &Path) -> Result<(), ArtifactStoreError> {
    create_private_directory_with_hook(path, || Ok(()))
}

fn create_private_directory_with_hook<F>(
    path: &Path,
    before_parent_sync: F,
) -> Result<(), ArtifactStoreError>
where
    F: FnOnce() -> Result<(), ArtifactStoreError>,
{
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(ArtifactStoreError::Io(error)),
    }
    let metadata = fs::symlink_metadata(path).map_err(ArtifactStoreError::Io)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(ArtifactStoreError::NotRegularFile);
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(ArtifactStoreError::Io)?;
    sync_directory(path)?;
    before_parent_sync()?;
    let parent = path.parent().ok_or_else(|| {
        ArtifactStoreError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "artifact directory must have an existing parent",
        ))
    })?;
    // Always sync the parent: a prior failed creator may have left an entry
    // whose durability has not yet been proven.
    sync_directory(parent)?;
    Ok(())
}

fn open_regular_nofollow(path: &Path, write: bool) -> Result<File, ArtifactStoreError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(write)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path).map_err(ArtifactStoreError::Io)?;
    if !file
        .metadata()
        .map_err(ArtifactStoreError::Io)?
        .file_type()
        .is_file()
    {
        return Err(ArtifactStoreError::NotRegularFile);
    }
    Ok(file)
}

fn validate_immutable_inode(metadata: &fs::Metadata) -> Result<(), ArtifactStoreError> {
    if !metadata.file_type().is_file() {
        return Err(ArtifactStoreError::NotRegularFile);
    }
    if metadata.permissions().mode() & 0o777 != 0o400 {
        return Err(ArtifactStoreError::InsecurePermissions);
    }
    if metadata.nlink() != 1 {
        return Err(ArtifactStoreError::MultipleLinks);
    }
    Ok(())
}

fn read_and_verify(
    file: &mut File,
    expected_digest: Sha256Digest,
    expected_size: Option<u64>,
    maximum_size: u64,
) -> Result<Vec<u8>, ArtifactStoreError> {
    file.seek(SeekFrom::Start(0))
        .map_err(ArtifactStoreError::Io)?;
    let metadata = file.metadata().map_err(ArtifactStoreError::Io)?;
    if metadata.len() > maximum_size {
        return Err(ArtifactStoreError::ArtifactTooLarge {
            size: metadata.len(),
            maximum: maximum_size,
        });
    }
    if let Some(expected) = expected_size {
        if expected > maximum_size {
            return Err(ArtifactStoreError::ArtifactTooLarge {
                size: expected,
                maximum: maximum_size,
            });
        }
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| ArtifactStoreError::SizeOverflow)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| ArtifactStoreError::AllocationFailed)?;
    file.take(maximum_size.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(ArtifactStoreError::Io)?;
    let size = u64::try_from(bytes.len()).map_err(|_| ArtifactStoreError::SizeOverflow)?;
    if size > maximum_size {
        return Err(ArtifactStoreError::ArtifactTooLarge {
            size,
            maximum: maximum_size,
        });
    }
    if expected_size.is_some_and(|expected| expected != size)
        || Sha256Digest::of_bytes(&bytes) != expected_digest
    {
        return Err(ArtifactStoreError::DigestMismatch);
    }
    Ok(bytes)
}

fn cleanup_stale_temporaries(objects: &Path) -> Result<(), ArtifactStoreError> {
    for entry in fs::read_dir(objects).map_err(ArtifactStoreError::Io)? {
        let entry = entry.map_err(ArtifactStoreError::Io)?;
        if !entry.file_name().to_string_lossy().starts_with(".tmp-") {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path()).map_err(ArtifactStoreError::Io)?;
        if metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            remove_temporary(&entry.path())?;
        }
    }
    Ok(())
}

fn cleanup_temporary_links_for_inode(
    objects: &Path,
    target: &fs::Metadata,
) -> Result<(), ArtifactStoreError> {
    for entry in fs::read_dir(objects).map_err(ArtifactStoreError::Io)? {
        let entry = entry.map_err(ArtifactStoreError::Io)?;
        if !entry.file_name().to_string_lossy().starts_with(".tmp-") {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path()).map_err(ArtifactStoreError::Io)?;
        if metadata.file_type().is_file()
            && metadata.dev() == target.dev()
            && metadata.ino() == target.ino()
        {
            remove_temporary(&entry.path())?;
        }
    }
    Ok(())
}

fn remove_temporary(path: &Path) -> Result<(), ArtifactStoreError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ArtifactStoreError::Io(error)),
    }
}

fn sync_directory(path: &Path) -> Result<(), ArtifactStoreError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(ArtifactStoreError::Io)
}

impl fmt::Display for ArtifactStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::NotRegularFile => formatter.write_str("artifact path is not a regular file"),
            Self::InsecurePermissions => {
                formatter.write_str("artifact permissions are not immutable and private")
            }
            Self::MultipleLinks => formatter.write_str("artifact inode has an outside hard link"),
            Self::DigestMismatch => formatter.write_str("artifact bytes do not match their digest"),
            Self::SizeOverflow => formatter.write_str("artifact size exceeds this platform"),
            Self::ArtifactTooLarge { size, maximum } => {
                write!(
                    formatter,
                    "artifact size {size} exceeds configured maximum {maximum}"
                )
            }
            Self::AllocationFailed => {
                formatter.write_str("memory allocation for artifact verification failed")
            }
            Self::TemporaryNameExhausted => {
                formatter.write_str("could not allocate a temporary artifact name")
            }
        }
    }
}

impl From<io::Error> for ArtifactStoreError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, os::unix::fs::symlink, process::Command, sync::Arc, thread};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn stores_reads_and_reuses_verified_content() {
        let directory = tempdir().unwrap();
        let store = ArtifactStore::open(directory.path().join("artifacts")).unwrap();

        let first = store.put(b"immutable artifact").unwrap();
        let second = store.put(b"immutable artifact").unwrap();

        assert_eq!(first, second);
        assert_eq!(
            store.read_verified(first.sha256).unwrap(),
            b"immutable artifact"
        );
        assert_eq!(first.size, 18);
        assert_eq!(
            fs::metadata(store.root()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(store.path_for(first.sha256))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
    }

    #[test]
    fn concurrent_writers_converge_without_replacement() {
        let directory = tempdir().unwrap();
        let store = Arc::new(ArtifactStore::open(directory.path().join("artifacts")).unwrap());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = Arc::clone(&store);
                thread::spawn(move || store.put(b"same bytes").unwrap())
            })
            .collect();
        let artifacts: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();

        assert!(artifacts.iter().all(|artifact| *artifact == artifacts[0]));
        assert_eq!(
            store.read_verified(artifacts[0].sha256).unwrap(),
            b"same bytes"
        );
        assert!(fs::read_dir(&store.objects).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".tmp-")));
    }

    #[test]
    fn temporary_bytes_are_verified_before_publication() {
        let directory = tempdir().unwrap();
        let store = ArtifactStore::open(directory.path().join("artifacts")).unwrap();
        let digest = Sha256Digest::of_bytes(b"expected bytes");
        store.inject_temporary_corruption(true);

        assert!(store.put(b"expected bytes").is_err());
        assert!(!store.path_for(digest).exists());
    }

    #[test]
    fn retry_after_failed_creation_still_reaches_parent_sync_barrier() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("artifacts");
        assert!(create_private_directory_with_hook(&root, || {
            Err(ArtifactStoreError::Io(io::Error::other(
                "injected before parent sync",
            )))
        })
        .is_err());
        assert!(root.is_dir());

        let reached_parent_barrier = Cell::new(false);
        create_private_directory_with_hook(&root, || {
            reached_parent_barrier.set(true);
            Ok(())
        })
        .unwrap();
        assert!(reached_parent_barrier.get());
    }

    #[test]
    fn stale_temporary_files_are_removed_on_open() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("artifacts");
        let store = ArtifactStore::open(&root).unwrap();
        let stale = store.objects.join(".tmp-stale");
        fs::write(&stale, b"stale").unwrap();
        drop(store);

        ArtifactStore::open(&root).unwrap();
        assert!(!stale.exists());
    }

    #[test]
    fn existing_corrupt_object_fails_closed() {
        let directory = tempdir().unwrap();
        let store = ArtifactStore::open(directory.path().join("artifacts")).unwrap();
        let digest = Sha256Digest::of_bytes(b"expected");
        let path = store.path_for(digest);
        fs::write(&path, b"different").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();

        assert!(matches!(
            store.put(b"expected"),
            Err(ArtifactStoreError::DigestMismatch)
        ));
    }

    #[test]
    fn writable_and_outside_hardlinked_objects_fail_closed() {
        let directory = tempdir().unwrap();
        let store = ArtifactStore::open(directory.path().join("artifacts")).unwrap();
        let artifact = store.put(b"artifact").unwrap();
        let path = store.path_for(artifact.sha256);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(matches!(
            store.put(b"artifact"),
            Err(ArtifactStoreError::InsecurePermissions)
        ));

        fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
        fs::hard_link(&path, directory.path().join("outside-link")).unwrap();
        assert!(matches!(
            store.put(b"artifact"),
            Err(ArtifactStoreError::MultipleLinks)
        ));
    }

    #[test]
    fn fifo_at_digest_path_fails_without_blocking() {
        let directory = tempdir().unwrap();
        let store = ArtifactStore::open(directory.path().join("artifacts")).unwrap();
        let digest = Sha256Digest::of_bytes(b"artifact");
        let path = store.path_for(digest);
        assert!(Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success());

        assert!(matches!(
            store.put(b"artifact"),
            Err(ArtifactStoreError::NotRegularFile)
        ));
    }

    #[test]
    fn oversized_sparse_object_is_rejected_before_allocation() {
        let directory = tempdir().unwrap();
        let store =
            ArtifactStore::open_with_max_size(directory.path().join("artifacts"), 8).unwrap();
        let digest = Sha256Digest::of_bytes(b"artifact");
        let path = store.path_for(digest);
        let file = File::create(&path).unwrap();
        file.set_len(9).unwrap();
        file.set_permissions(fs::Permissions::from_mode(0o400))
            .unwrap();

        assert!(matches!(
            store.put(b"artifact"),
            Err(ArtifactStoreError::ArtifactTooLarge {
                size: 9,
                maximum: 8
            })
        ));
    }

    #[test]
    fn symlink_at_digest_path_is_never_followed() {
        let directory = tempdir().unwrap();
        let store = ArtifactStore::open(directory.path().join("artifacts")).unwrap();
        let outside = directory.path().join("outside");
        fs::write(&outside, b"outside").unwrap();
        let digest = Sha256Digest::of_bytes(b"artifact");
        symlink(&outside, store.path_for(digest)).unwrap();

        assert!(store.put(b"artifact").is_err());
        assert_eq!(fs::read(outside).unwrap(), b"outside");
    }
}
