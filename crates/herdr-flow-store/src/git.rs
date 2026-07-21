use std::{
    fmt,
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Output},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use herdr_flow_core::{ArtifactId, GitObjectFormat, GitObjectId, RunId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct GitRepository {
    root: PathBuf,
    common_dir: PathBuf,
    common_dir_identity: (u64, u64),
    object_format: GitObjectFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotRefOutcome {
    Created,
}

#[derive(Debug)]
pub enum GitRepositoryError {
    Io(io::Error),
    GitFailed { status: ExitStatus, stderr: String },
    InvalidOutput,
    UnsupportedObjectFormat,
    UnsupportedRefFormat,
    ObjectFormatMismatch,
    NotCommit,
    SnapshotRefConflict,
    TargetAlreadyExists,
    TargetInsideUserRepository,
    WorktreeObjectMismatch,
    WorktreeNotOwned,
    WorktreeRepositoryMismatch,
    RepositoryIdentityChanged,
}

impl GitRepository {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, GitRepositoryError> {
        let output = run_git_at(path.as_ref(), &["rev-parse", "--show-toplevel"])?;
        let root = PathBuf::from(trim_output(&output)?);
        let root = fs::canonicalize(root).map_err(GitRepositoryError::Io)?;
        let common_dir = repository_common_dir(&root)?;
        let common_dir_identity = directory_identity(&common_dir)?;
        let format_output = run_git_at(&root, &["rev-parse", "--show-object-format"])?;
        let object_format = match trim_output(&format_output)? {
            "sha1" => GitObjectFormat::Sha1,
            "sha256" => GitObjectFormat::Sha256,
            _ => return Err(GitRepositoryError::UnsupportedObjectFormat),
        };
        if repository_ref_format(&root)? != "files" {
            return Err(GitRepositoryError::UnsupportedRefFormat);
        }
        Ok(Self {
            root,
            common_dir,
            common_dir_identity,
            object_format,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn object_format(&self) -> GitObjectFormat {
        self.object_format
    }

    pub fn verify_commit(&self, object: &GitObjectId) -> Result<(), GitRepositoryError> {
        self.verify_repository_identity()?;
        self.require_format(object)?;
        let output = run_git_at(&self.root, &["cat-file", "-t", object.hex()])?;
        if trim_output(&output)? != "commit" {
            return Err(GitRepositoryError::NotCommit);
        }
        Ok(())
    }

    pub fn is_ancestor(
        &self,
        baseline: &GitObjectId,
        candidate: &GitObjectId,
    ) -> Result<bool, GitRepositoryError> {
        self.verify_commit(baseline)?;
        self.verify_commit(candidate)?;
        let output = git_command(&self.root)
            .args([
                "merge-base",
                "--is-ancestor",
                baseline.hex(),
                candidate.hex(),
            ])
            .output()
            .map_err(GitRepositoryError::Io)?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(git_failure(output)),
        }
    }

    pub fn create_snapshot_ref(
        &self,
        run_id: &RunId,
        artifact_id: &ArtifactId,
        object: &GitObjectId,
    ) -> Result<SnapshotRefOutcome, GitRepositoryError> {
        self.verify_commit(object)?;
        let reference = snapshot_ref(run_id, artifact_id);
        self.create_direct_ref_transaction(&reference, object)?;
        Ok(SnapshotRefOutcome::Created)
    }

    pub fn snapshot_ref_matches(
        &self,
        run_id: &RunId,
        artifact_id: &ArtifactId,
        object: &GitObjectId,
    ) -> Result<bool, GitRepositoryError> {
        self.verify_commit(object)?;
        let reference = snapshot_ref(run_id, artifact_id);
        self.reconcile_direct_ref(&reference, object)
    }

    pub fn materialize_detached(
        &self,
        target: impl AsRef<Path>,
        object: &GitObjectId,
    ) -> Result<(), GitRepositoryError> {
        self.verify_commit(object)?;
        let target = target.as_ref();
        if target.try_exists().map_err(GitRepositoryError::Io)? {
            return Err(GitRepositoryError::TargetAlreadyExists);
        }
        let parent = target.parent().ok_or_else(|| {
            GitRepositoryError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worktree target requires a parent",
            ))
        })?;
        let parent = fs::canonicalize(parent).map_err(GitRepositoryError::Io)?;
        let target = parent.join(target.file_name().ok_or_else(|| {
            GitRepositoryError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worktree target requires a file name",
            ))
        })?);
        if target.starts_with(&self.root) {
            return Err(GitRepositoryError::TargetInsideUserRepository);
        }
        let target_text = target.to_str().ok_or(GitRepositoryError::InvalidOutput)?;
        run_git_at(
            &self.root,
            &["worktree", "add", "--detach", target_text, object.hex()],
        )?;
        let head = self.read_worktree_head(&target)?;
        if head != *object || !head_is_detached(&target)? {
            let _ = run_git_at(&self.root, &["worktree", "remove", "--force", target_text]);
            return Err(GitRepositoryError::WorktreeObjectMismatch);
        }
        let target = fs::canonicalize(&target).map_err(GitRepositoryError::Io)?;
        let target_common_dir = repository_common_dir(&target)?;
        if target_common_dir != self.common_dir {
            let _ = run_git_at(&self.root, &["worktree", "remove", "--force", target_text]);
            return Err(GitRepositoryError::WorktreeRepositoryMismatch);
        }
        let marker = worktree_ownership_marker(&target)?;
        let ownership = WorktreeOwnership {
            version: 1,
            repository_common_dir: self.common_dir.clone(),
            expected_object: object.clone(),
        };
        let marker_bytes =
            serde_json::to_vec(&ownership).map_err(|_| GitRepositoryError::InvalidOutput)?;
        if let Err(error) = fs::write(&marker, marker_bytes) {
            let _ = run_git_at(&self.root, &["worktree", "remove", "--force", target_text]);
            return Err(GitRepositoryError::Io(error));
        }
        Ok(())
    }

    pub fn worktree_matches_clean_object(
        &self,
        worktree: impl AsRef<Path>,
        expected_object: &GitObjectId,
    ) -> Result<bool, GitRepositoryError> {
        self.verify_commit(expected_object)?;
        let worktree = fs::canonicalize(worktree).map_err(GitRepositoryError::Io)?;
        self.verify_worktree_ownership(&worktree, expected_object)?;
        if self.read_worktree_head(&worktree)? != *expected_object || !head_is_detached(&worktree)?
        {
            return Ok(false);
        }
        let output = run_git_at(
            &worktree,
            &["status", "--porcelain=v1", "--untracked-files=all"],
        )?;
        Ok(output.stdout.is_empty())
    }

    pub fn remove_worktree(&self, worktree: impl AsRef<Path>) -> Result<(), GitRepositoryError> {
        self.verify_repository_identity()?;
        let worktree = fs::canonicalize(worktree).map_err(GitRepositoryError::Io)?;
        let ownership = read_worktree_ownership(&worktree)?;
        if ownership.repository_common_dir != self.common_dir {
            return Err(GitRepositoryError::WorktreeRepositoryMismatch);
        }
        self.verify_worktree_ownership(&worktree, &ownership.expected_object)?;
        let path = worktree.to_str().ok_or(GitRepositoryError::InvalidOutput)?;
        run_git_at(&self.root, &["worktree", "remove", "--force", path])?;
        Ok(())
    }

    fn verify_worktree_ownership(
        &self,
        worktree: &Path,
        expected_object: &GitObjectId,
    ) -> Result<(), GitRepositoryError> {
        let ownership = read_worktree_ownership(worktree)?;
        if ownership.version != 1
            || ownership.repository_common_dir != self.common_dir
            || ownership.expected_object != *expected_object
            || repository_common_dir(worktree)? != self.common_dir
        {
            return Err(GitRepositoryError::WorktreeRepositoryMismatch);
        }
        Ok(())
    }

    fn verify_repository_identity(&self) -> Result<(), GitRepositoryError> {
        if repository_common_dir(&self.root)? != self.common_dir
            || directory_identity(&self.common_dir)? != self.common_dir_identity
        {
            return Err(GitRepositoryError::RepositoryIdentityChanged);
        }
        self.verify_ref_backend()
    }

    fn verify_ref_backend(&self) -> Result<(), GitRepositoryError> {
        if repository_ref_format(&self.root)? != "files" {
            return Err(GitRepositoryError::UnsupportedRefFormat);
        }
        Ok(())
    }

    fn create_direct_ref_transaction(
        &self,
        reference: &str,
        object: &GitObjectId,
    ) -> Result<(), GitRepositoryError> {
        let paths = ref_paths(&self.common_dir, reference)?;
        create_durable_directories(&self.common_dir, &paths.ref_parent)?;
        create_durable_directories(&self.common_dir, &paths.guard_parent)?;
        let _guard = open_locked_guard(&paths.guard)?;
        self.verify_ref_backend()?;
        self.recover_owned_ref(reference, object, &paths)?;
        if loose_ref_exists(&paths.reference)? || self.read_ref(reference)?.is_some() {
            return Err(GitRepositoryError::SnapshotRefConflict);
        }

        let (mut attempt, attempt_path) = create_unique_attempt(&paths.guard)?;
        attempt
            .write_all(format!("{}\n", object.hex()).as_bytes())
            .and_then(|()| attempt.sync_all())
            .map_err(GitRepositoryError::Io)?;
        let metadata = attempt.metadata().map_err(GitRepositoryError::Io)?;
        let recorded = RefAttempt {
            object: object.clone(),
            device: metadata.dev(),
            inode: metadata.ino(),
            attempt_path: attempt_path.clone(),
        };
        write_ref_attempt_atomic(&paths.state, &recorded)?;
        sync_directory(&paths.guard_parent)?;

        match fs::hard_link(&attempt_path, &paths.lock) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(GitRepositoryError::SnapshotRefConflict);
            }
            Err(error) => return Err(GitRepositoryError::Io(error)),
        }
        sync_directory(&paths.ref_parent)?;
        match fs::hard_link(&paths.lock, &paths.reference) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(GitRepositoryError::SnapshotRefConflict);
            }
            Err(error) => return Err(GitRepositoryError::Io(error)),
        }
        sync_directory(&paths.ref_parent)?;
        self.verify_ref_backend()?;
        fs::remove_file(&paths.lock).map_err(GitRepositoryError::Io)?;
        fs::remove_file(&attempt_path).map_err(GitRepositoryError::Io)?;
        sync_directory(&paths.ref_parent)?;
        sync_directory(&paths.guard_parent)?;
        clear_ref_attempt(&paths.state)?;
        sync_directory(&paths.guard_parent)
    }

    fn reconcile_direct_ref(
        &self,
        reference: &str,
        object: &GitObjectId,
    ) -> Result<bool, GitRepositoryError> {
        let paths = ref_paths(&self.common_dir, reference)?;
        create_durable_directories(&self.common_dir, &paths.ref_parent)?;
        create_durable_directories(&self.common_dir, &paths.guard_parent)?;
        let _guard = open_locked_guard(&paths.guard)?;
        self.verify_ref_backend()?;
        self.recover_owned_ref(reference, object, &paths)?;
        sync_directory(&paths.ref_parent)?;
        self.verify_ref_backend()?;
        Ok(self.read_direct_ref(reference)?.as_ref() == Some(object))
    }

    fn recover_owned_ref(
        &self,
        reference: &str,
        object: &GitObjectId,
        paths: &RefPaths,
    ) -> Result<(), GitRepositoryError> {
        let Some(recorded) = read_ref_attempt(&paths.state)? else {
            return Ok(());
        };
        normalize_state_link(&paths.state)?;
        if recorded.object != *object
            || recorded.attempt_path.parent() != Some(paths.guard_parent.as_path())
            || !recorded
                .attempt_path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().contains(".attempt-"))
        {
            return Err(GitRepositoryError::SnapshotRefConflict);
        }
        for owned_path in [&recorded.attempt_path, &paths.lock] {
            if loose_ref_exists(owned_path)? {
                let metadata = fs::symlink_metadata(owned_path).map_err(GitRepositoryError::Io)?;
                if !metadata.file_type().is_file()
                    || metadata.dev() != recorded.device
                    || metadata.ino() != recorded.inode
                {
                    return Err(GitRepositoryError::SnapshotRefConflict);
                }
            }
        }
        if loose_ref_exists(&paths.lock)? {
            fs::remove_file(&paths.lock).map_err(GitRepositoryError::Io)?;
            sync_directory(&paths.ref_parent)?;
        }
        if loose_ref_exists(&recorded.attempt_path)? {
            fs::remove_file(&recorded.attempt_path).map_err(GitRepositoryError::Io)?;
            sync_directory(&paths.guard_parent)?;
        }
        if loose_ref_exists(&paths.reference)?
            && self.read_direct_ref(reference)?.as_ref() != Some(object)
        {
            return Err(GitRepositoryError::SnapshotRefConflict);
        }
        clear_ref_attempt(&paths.state)?;
        sync_directory(&paths.guard_parent)
    }

    fn read_direct_ref(&self, reference: &str) -> Result<Option<GitObjectId>, GitRepositoryError> {
        let output = run_git_at(
            &self.root,
            &[
                "for-each-ref",
                "--format=%(refname)%00%(objectname)%00%(symref)",
                reference,
            ],
        )?;
        for line in trim_output(&output)?.lines() {
            let mut fields = line.split('\0');
            let name = fields.next().ok_or(GitRepositoryError::InvalidOutput)?;
            let object = fields.next().ok_or(GitRepositoryError::InvalidOutput)?;
            let symbolic = fields.next().ok_or(GitRepositoryError::InvalidOutput)?;
            if name == reference {
                if !symbolic.is_empty() {
                    return Ok(None);
                }
                return GitObjectId::from_hex(self.object_format, object)
                    .map(Some)
                    .map_err(|_| GitRepositoryError::InvalidOutput);
            }
        }
        Ok(None)
    }

    fn read_ref(&self, reference: &str) -> Result<Option<GitObjectId>, GitRepositoryError> {
        let output = git_command(&self.root)
            .args(["rev-parse", "--verify", "--quiet", reference])
            .output()
            .map_err(GitRepositoryError::Io)?;
        match output.status.code() {
            Some(0) => GitObjectId::from_hex(self.object_format, trim_output(&output)?)
                .map(Some)
                .map_err(|_| GitRepositoryError::InvalidOutput),
            Some(1) => Ok(None),
            _ => Err(git_failure(output)),
        }
    }

    fn read_worktree_head(&self, worktree: &Path) -> Result<GitObjectId, GitRepositoryError> {
        let output = run_git_at(worktree, &["rev-parse", "--verify", "HEAD"])?;
        GitObjectId::from_hex(self.object_format, trim_output(&output)?)
            .map_err(|_| GitRepositoryError::InvalidOutput)
    }

    fn require_format(&self, object: &GitObjectId) -> Result<(), GitRepositoryError> {
        if object.format() != self.object_format {
            return Err(GitRepositoryError::ObjectFormatMismatch);
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
struct WorktreeOwnership {
    version: u32,
    repository_common_dir: PathBuf,
    expected_object: GitObjectId,
}

#[derive(Deserialize, Serialize)]
struct RefAttempt {
    object: GitObjectId,
    device: u64,
    inode: u64,
    attempt_path: PathBuf,
}

struct RefPaths {
    reference: PathBuf,
    ref_parent: PathBuf,
    lock: PathBuf,
    guard: PathBuf,
    guard_parent: PathBuf,
    state: PathBuf,
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn ref_paths(common_dir: &Path, reference: &str) -> Result<RefPaths, GitRepositoryError> {
    let reference_path = common_dir.join(reference);
    let ref_parent = reference_path
        .parent()
        .ok_or(GitRepositoryError::InvalidOutput)?
        .to_path_buf();
    let guard_relative = reference
        .strip_prefix("refs/herdr-flow/snapshots/")
        .ok_or(GitRepositoryError::InvalidOutput)?;
    let guard = append_suffix(
        &common_dir
            .join("herdr-flow/ref-guards")
            .join(guard_relative),
        ".guard",
    );
    let guard_parent = guard
        .parent()
        .ok_or(GitRepositoryError::InvalidOutput)?
        .to_path_buf();
    Ok(RefPaths {
        lock: append_suffix(&reference_path, ".lock"),
        state: append_suffix(&guard, ".state"),
        reference: reference_path,
        ref_parent,
        guard,
        guard_parent,
    })
}

fn open_locked_guard(path: &Path) -> Result<fs::File, GitRepositoryError> {
    let guard = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(GitRepositoryError::Io)?;
    let metadata = guard.metadata().map_err(GitRepositoryError::Io)?;
    if !metadata.file_type().is_file() || metadata.nlink() != 1 {
        return Err(GitRepositoryError::SnapshotRefConflict);
    }
    guard.lock_exclusive().map_err(GitRepositoryError::Io)?;
    Ok(guard)
}

fn unique_path(base: &Path, label: &str) -> Result<PathBuf, GitRepositoryError> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GitRepositoryError::InvalidOutput)?
        .as_nanos();
    Ok(append_suffix(
        base,
        &format!(
            ".{label}-{}-{now}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ),
    ))
}

fn create_unique_attempt(base: &Path) -> Result<(fs::File, PathBuf), GitRepositoryError> {
    for _ in 0..16 {
        let path = unique_path(base, "attempt")?;
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
        {
            Ok(file) => {
                let metadata = file.metadata().map_err(GitRepositoryError::Io)?;
                if metadata.file_type().is_file() && metadata.nlink() == 1 {
                    return Ok((file, path));
                }
                return Err(GitRepositoryError::SnapshotRefConflict);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(GitRepositoryError::Io(error)),
        }
    }
    Err(GitRepositoryError::SnapshotRefConflict)
}

fn read_ref_attempt(path: &Path) -> Result<Option<RefAttempt>, GitRepositoryError> {
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(GitRepositoryError::Io(error)),
    };
    let metadata = file.metadata().map_err(GitRepositoryError::Io)?;
    if !metadata.file_type().is_file() || !(1..=2).contains(&metadata.nlink()) {
        return Err(GitRepositoryError::SnapshotRefConflict);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(GitRepositoryError::Io)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| GitRepositoryError::InvalidOutput)
}

fn normalize_state_link(state_path: &Path) -> Result<(), GitRepositoryError> {
    let metadata = fs::symlink_metadata(state_path).map_err(GitRepositoryError::Io)?;
    if metadata.nlink() == 1 {
        return Ok(());
    }
    let parent = state_path
        .parent()
        .ok_or(GitRepositoryError::InvalidOutput)?;
    let state_name = state_path
        .file_name()
        .ok_or(GitRepositoryError::InvalidOutput)?
        .to_string_lossy();
    let expected_prefix = format!("{state_name}.attempt-");
    let mut removed = false;
    for entry in fs::read_dir(parent).map_err(GitRepositoryError::Io)? {
        let entry = entry.map_err(GitRepositoryError::Io)?;
        let path = entry.path();
        if path == state_path
            || !entry
                .file_name()
                .to_string_lossy()
                .starts_with(&expected_prefix)
        {
            continue;
        }
        let candidate = fs::symlink_metadata(&path).map_err(GitRepositoryError::Io)?;
        if candidate.dev() == metadata.dev() && candidate.ino() == metadata.ino() {
            fs::remove_file(path).map_err(GitRepositoryError::Io)?;
            removed = true;
        }
    }
    if !removed
        || fs::symlink_metadata(state_path)
            .map_err(GitRepositoryError::Io)?
            .nlink()
            != 1
    {
        return Err(GitRepositoryError::SnapshotRefConflict);
    }
    sync_directory(parent)
}

fn write_ref_attempt_atomic(
    state_path: &Path,
    attempt: &RefAttempt,
) -> Result<(), GitRepositoryError> {
    if loose_ref_exists(state_path)? {
        return Err(GitRepositoryError::SnapshotRefConflict);
    }
    let bytes = serde_json::to_vec(attempt).map_err(|_| GitRepositoryError::InvalidOutput)?;
    let (mut temporary, temporary_path) = create_unique_attempt(state_path)?;
    temporary
        .write_all(&bytes)
        .and_then(|()| temporary.sync_all())
        .map_err(GitRepositoryError::Io)?;
    match fs::hard_link(&temporary_path, state_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(GitRepositoryError::SnapshotRefConflict);
        }
        Err(error) => return Err(GitRepositoryError::Io(error)),
    }
    fs::remove_file(&temporary_path).map_err(GitRepositoryError::Io)?;
    sync_directory(
        state_path
            .parent()
            .ok_or(GitRepositoryError::InvalidOutput)?,
    )
}

fn clear_ref_attempt(state_path: &Path) -> Result<(), GitRepositoryError> {
    match fs::remove_file(state_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(GitRepositoryError::Io(error)),
    }
}

fn create_durable_directories(root: &Path, target: &Path) -> Result<(), GitRepositoryError> {
    let relative = target
        .strip_prefix(root)
        .map_err(|_| GitRepositoryError::InvalidOutput)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let parent = current.clone();
        current.push(component);
        match fs::create_dir(&current) {
            Ok(()) => {
                sync_directory(&current)?;
                sync_directory(&parent)?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&current).map_err(GitRepositoryError::Io)?;
                if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
                    return Err(GitRepositoryError::SnapshotRefConflict);
                }
                sync_directory(&current)?;
                sync_directory(&parent)?;
            }
            Err(error) => return Err(GitRepositoryError::Io(error)),
        }
    }
    Ok(())
}

fn loose_ref_exists(path: &Path) -> Result<bool, GitRepositoryError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(GitRepositoryError::Io(error)),
    }
}

fn sync_directory(path: &Path) -> Result<(), GitRepositoryError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(GitRepositoryError::Io)
}

fn directory_identity(path: &Path) -> Result<(u64, u64), GitRepositoryError> {
    let metadata = fs::metadata(path).map_err(GitRepositoryError::Io)?;
    Ok((metadata.dev(), metadata.ino()))
}

fn repository_ref_format(worktree: &Path) -> Result<String, GitRepositoryError> {
    let output = run_git_at(worktree, &["rev-parse", "--show-ref-format"])?;
    Ok(trim_output(&output)?.to_owned())
}

fn repository_common_dir(worktree: &Path) -> Result<PathBuf, GitRepositoryError> {
    let output = run_git_at(
        worktree,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?;
    fs::canonicalize(trim_output(&output)?).map_err(GitRepositoryError::Io)
}

fn head_is_detached(worktree: &Path) -> Result<bool, GitRepositoryError> {
    let output = git_command(worktree)
        .args(["symbolic-ref", "--quiet", "HEAD"])
        .output()
        .map_err(GitRepositoryError::Io)?;
    match output.status.code() {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        _ => Err(git_failure(output)),
    }
}

fn worktree_ownership_marker(worktree: &Path) -> Result<PathBuf, GitRepositoryError> {
    let output = run_git_at(worktree, &["rev-parse", "--absolute-git-dir"])?;
    Ok(PathBuf::from(trim_output(&output)?).join("herdr-flow-owned"))
}

fn read_worktree_ownership(worktree: &Path) -> Result<WorktreeOwnership, GitRepositoryError> {
    let marker = worktree_ownership_marker(worktree)?;
    let bytes = match fs::read(marker) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(GitRepositoryError::WorktreeNotOwned);
        }
        Err(error) => return Err(GitRepositoryError::Io(error)),
    };
    serde_json::from_slice(&bytes).map_err(|_| GitRepositoryError::WorktreeNotOwned)
}

fn snapshot_ref(run_id: &RunId, artifact_id: &ArtifactId) -> String {
    format!(
        "refs/herdr-flow/snapshots/{}/{}",
        run_id.as_str(),
        artifact_id.as_str()
    )
}

fn git_command(root: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .current_dir(root)
        .env_clear()
        .env("LC_ALL", "C")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .arg("-c")
        .arg("core.hooksPath=/dev/null");
    command
}

fn run_git_at(root: &Path, arguments: &[&str]) -> Result<Output, GitRepositoryError> {
    let output = git_command(root)
        .args(arguments)
        .output()
        .map_err(GitRepositoryError::Io)?;
    if !output.status.success() {
        return Err(git_failure(output));
    }
    Ok(output)
}

fn trim_output(output: &Output) -> Result<&str, GitRepositoryError> {
    core::str::from_utf8(&output.stdout)
        .map(str::trim)
        .map_err(|_| GitRepositoryError::InvalidOutput)
}

fn git_failure(output: Output) -> GitRepositoryError {
    GitRepositoryError::GitFailed {
        status: output.status,
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

impl fmt::Display for GitRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::GitFailed { status, stderr } => {
                write!(formatter, "Git failed with {status}: {stderr}")
            }
            Self::InvalidOutput => formatter.write_str("Git returned invalid output"),
            Self::UnsupportedObjectFormat => formatter.write_str("unsupported Git object format"),
            Self::UnsupportedRefFormat => formatter.write_str("unsupported Git ref format"),
            Self::ObjectFormatMismatch => formatter.write_str("Git object format mismatch"),
            Self::NotCommit => formatter.write_str("Git object is not a commit"),
            Self::SnapshotRefConflict => {
                formatter.write_str("snapshot ref points to another object")
            }
            Self::TargetAlreadyExists => formatter.write_str("worktree target already exists"),
            Self::TargetInsideUserRepository => {
                formatter.write_str("worktree target is inside the user repository")
            }
            Self::WorktreeObjectMismatch => {
                formatter.write_str("detached worktree has the wrong HEAD")
            }
            Self::WorktreeNotOwned => {
                formatter.write_str("refusing to remove a worktree not owned by Herdr Flow")
            }
            Self::WorktreeRepositoryMismatch => {
                formatter.write_str("worktree belongs to another Git repository")
            }
            Self::RepositoryIdentityChanged => {
                formatter.write_str("Git repository identity changed after opening")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, process::Command};

    use tempfile::tempdir;

    use super::*;

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    fn git(root: &Path, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .current_dir(root)
            .args(arguments)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn repository() -> (tempfile::TempDir, GitRepository, GitObjectId, GitObjectId) {
        let directory = tempdir().unwrap();
        git(directory.path(), &["init", "-q"]);
        git(directory.path(), &["config", "user.name", "Herdr Test"]);
        git(
            directory.path(),
            &["config", "user.email", "herdr@example.invalid"],
        );
        fs::write(directory.path().join("file.txt"), "one").unwrap();
        git(directory.path(), &["add", "file.txt"]);
        git(directory.path(), &["commit", "-qm", "one"]);
        let store = GitRepository::open(directory.path()).unwrap();
        let first = GitObjectId::from_hex(
            store.object_format(),
            &git(directory.path(), &["rev-parse", "HEAD"]),
        )
        .unwrap();
        fs::write(directory.path().join("file.txt"), "two").unwrap();
        git(directory.path(), &["commit", "-qam", "two"]);
        let second = GitObjectId::from_hex(
            store.object_format(),
            &git(directory.path(), &["rev-parse", "HEAD"]),
        )
        .unwrap();
        (directory, store, first, second)
    }

    #[test]
    fn validates_commits_ancestry_and_no_clobber_snapshot_refs() {
        let (directory, store, first, second) = repository();
        store.verify_commit(&first).unwrap();
        assert!(store.is_ancestor(&first, &second).unwrap());
        assert!(!store.is_ancestor(&second, &first).unwrap());
        let run_id = RunId::parse(format!("flow_{ULID}")).unwrap();
        let artifact_id = ArtifactId::parse(format!("art_{ULID}")).unwrap();
        assert_eq!(
            store
                .create_snapshot_ref(&run_id, &artifact_id, &first)
                .unwrap(),
            SnapshotRefOutcome::Created
        );
        assert!(store
            .snapshot_ref_matches(&run_id, &artifact_id, &first)
            .unwrap());
        assert!(!store
            .snapshot_ref_matches(&run_id, &artifact_id, &second)
            .unwrap());
        assert!(matches!(
            store.create_snapshot_ref(&run_id, &artifact_id, &first),
            Err(GitRepositoryError::SnapshotRefConflict)
        ));
        assert!(matches!(
            store.create_snapshot_ref(&run_id, &artifact_id, &second),
            Err(GitRepositoryError::SnapshotRefConflict)
        ));
        let other_artifact = ArtifactId::parse("art_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap();
        let symbolic_snapshot = snapshot_ref(&run_id, &other_artifact);
        let current_branch = git(directory.path(), &["symbolic-ref", "HEAD"]);
        git(
            directory.path(),
            &["symbolic-ref", &symbolic_snapshot, &current_branch],
        );
        assert!(matches!(
            store.create_snapshot_ref(&run_id, &other_artifact, &second),
            Err(GitRepositoryError::SnapshotRefConflict)
        ));
        let dangling_artifact = ArtifactId::parse("art_01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap();
        let dangling_snapshot = snapshot_ref(&run_id, &dangling_artifact);
        let dangling_target = "refs/heads/herdr-dangling-target";
        git(
            directory.path(),
            &["symbolic-ref", &dangling_snapshot, dangling_target],
        );
        let dangling_result = store.create_snapshot_ref(&run_id, &dangling_artifact, &second);
        assert!(
            matches!(
                dangling_result,
                Err(GitRepositoryError::SnapshotRefConflict)
            ),
            "{dangling_result:?}"
        );
        let target_status = Command::new("git")
            .current_dir(directory.path())
            .args(["show-ref", "--verify", "--quiet", dangling_target])
            .status()
            .unwrap();
        assert!(!target_status.success());

        let recovered_artifact = ArtifactId::parse("art_01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap();
        let recovered_reference = snapshot_ref(&run_id, &recovered_artifact);
        let recovered_paths = ref_paths(&store.common_dir, &recovered_reference).unwrap();
        create_durable_directories(&store.common_dir, &recovered_paths.ref_parent).unwrap();
        create_durable_directories(&store.common_dir, &recovered_paths.guard_parent).unwrap();
        let recovered_attempt = append_suffix(&recovered_paths.guard, ".attempt-crash-test");
        fs::write(&recovered_attempt, format!("{}\n", second.hex())).unwrap();
        fs::hard_link(&recovered_attempt, &recovered_paths.lock).unwrap();
        let metadata = fs::metadata(&recovered_attempt).unwrap();
        write_ref_attempt_atomic(
            &recovered_paths.state,
            &RefAttempt {
                object: second.clone(),
                device: metadata.dev(),
                inode: metadata.ino(),
                attempt_path: recovered_attempt.clone(),
            },
        )
        .unwrap();
        let interrupted_state_temp = append_suffix(&recovered_paths.state, ".attempt-crash-state");
        fs::hard_link(&recovered_paths.state, &interrupted_state_temp).unwrap();
        let unrecorded_foreign = append_suffix(&recovered_paths.guard, ".attempt-foreign");
        fs::write(&unrecorded_foreign, "foreign").unwrap();
        git(directory.path(), &["pack-refs", "--all", "--prune"]);
        assert_eq!(
            store
                .create_snapshot_ref(&run_id, &recovered_artifact, &second)
                .unwrap(),
            SnapshotRefOutcome::Created
        );
        assert!(!recovered_paths.lock.exists());
        assert!(!recovered_attempt.exists());
        assert!(!recovered_paths.state.exists());
        assert!(!interrupted_state_temp.exists());
        assert_eq!(fs::read_to_string(unrecorded_foreign).unwrap(), "foreign");
        assert!(recovered_paths.guard.exists());
        assert_eq!(fs::metadata(&recovered_paths.guard).unwrap().len(), 0);
        assert!(store
            .snapshot_ref_matches(&run_id, &recovered_artifact, &second)
            .unwrap());

        fs::write(directory.path().join("unrelated.txt"), "dirty").unwrap();
        assert!(!store
            .worktree_matches_clean_object(directory.path(), &second)
            .unwrap_or(false));
    }

    #[test]
    fn materializes_exact_detached_object_without_touching_user_worktree() {
        let (directory, store, first, second) = repository();
        let worktrees = tempdir().unwrap();
        let target = worktrees.path().join("candidate");
        store.materialize_detached(&target, &first).unwrap();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), first.hex());
        assert!(store
            .worktree_matches_clean_object(&target, &first)
            .unwrap());
        fs::write(target.join("file.txt"), "changed").unwrap();
        assert!(!store
            .worktree_matches_clean_object(&target, &first)
            .unwrap());
        git(&target, &["reset", "--hard", second.hex()]);
        assert!(!store
            .worktree_matches_clean_object(&target, &first)
            .unwrap());
        git(
            &target,
            &["switch", "-c", "herdr-test-attached", first.hex()],
        );
        assert!(!store
            .worktree_matches_clean_object(&target, &first)
            .unwrap());
        store.remove_worktree(&target).unwrap();
        assert!(!target.exists());

        let foreign = worktrees.path().join("foreign");
        git(
            directory.path(),
            &[
                "worktree",
                "add",
                "--detach",
                foreign.to_str().unwrap(),
                second.hex(),
            ],
        );
        assert!(matches!(
            store.remove_worktree(&foreign),
            Err(GitRepositoryError::WorktreeNotOwned)
        ));
        git(
            directory.path(),
            &["worktree", "remove", "--force", foreign.to_str().unwrap()],
        );
        assert_eq!(git(directory.path(), &["rev-parse", "HEAD"]), second.hex());
    }

    #[test]
    fn rejects_repository_identity_replacement_after_open() {
        let (directory, store, first, _) = repository();
        fs::rename(
            directory.path().join(".git"),
            directory.path().join(".git-original"),
        )
        .unwrap();
        git(directory.path(), &["init", "-q"]);

        assert!(matches!(
            store.verify_commit(&first),
            Err(GitRepositoryError::RepositoryIdentityChanged)
        ));
    }

    #[test]
    fn rejects_ref_backend_migration_after_open() {
        let (directory, store, first, _) = repository();
        let status = Command::new("git")
            .current_dir(directory.path())
            .args(["refs", "migrate", "--ref-format=reftable"])
            .status()
            .unwrap();
        if !status.success() {
            return;
        }
        assert!(matches!(
            store.verify_commit(&first),
            Err(GitRepositoryError::UnsupportedRefFormat)
        ));
        let run_id = RunId::parse(format!("flow_{ULID}")).unwrap();
        let artifact_id = ArtifactId::parse(format!("art_{ULID}")).unwrap();
        assert!(matches!(
            store.create_snapshot_ref(&run_id, &artifact_id, &first),
            Err(GitRepositoryError::UnsupportedRefFormat)
        ));
    }

    #[test]
    fn rejects_non_commit_objects_and_targets_inside_user_repository() {
        let (directory, store, first, _) = repository();
        let blob = git(directory.path(), &["hash-object", "file.txt"]);
        let blob = GitObjectId::from_hex(store.object_format(), &blob).unwrap();
        assert!(matches!(
            store.verify_commit(&blob),
            Err(GitRepositoryError::NotCommit)
        ));
        git(
            directory.path(),
            &[
                "update-ref",
                &format!("refs/replace/{}", blob.hex()),
                first.hex(),
            ],
        );
        assert!(matches!(
            store.verify_commit(&blob),
            Err(GitRepositoryError::NotCommit)
        ));
        assert!(matches!(
            store.is_ancestor(&blob, &first),
            Err(GitRepositoryError::NotCommit)
        ));
        let outside = tempdir().unwrap();
        assert!(matches!(
            store.materialize_detached(outside.path().join("blob"), &blob),
            Err(GitRepositoryError::NotCommit)
        ));
        assert!(matches!(
            store.materialize_detached(directory.path().join("nested"), &first),
            Err(GitRepositoryError::TargetInsideUserRepository)
        ));
    }
}
