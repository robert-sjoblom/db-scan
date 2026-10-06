use std::fs;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use anyhow::Context as _;

const REPO_OWNER: &str = "robert-sjoblom";
const REPO_NAME: &str = "db-scan";
const BIN_NAME: &str = "db-scan";

const WORLD_RUNNABLE: u32 = 0o555;

/// Download and install the latest GitHub release, replacing the running binary.
pub(crate) fn update() -> anyhow::Result<()> {
    let exe = installed_path().context("locating the running binary")?;
    let status = self_update::backends::github::Update::configure()
        .repo_owner(REPO_OWNER)
        .repo_name(REPO_NAME)
        .bin_name(BIN_NAME)
        .bin_path_in_archive("{{ bin }}-v{{ version }}-{{ target }}/{{ bin }}")
        .show_download_progress(true)
        .current_version(self_update::cargo_crate_version!())
        .build()
        .context("configuring self-update")?
        .update();

    let status = match status {
        Ok(s) => s,
        Err(self_update::errors::Error::Io(e))
            if e.kind() == std::io::ErrorKind::PermissionDenied =>
        {
            anyhow::bail!(
                "permission denied replacing the binary ({e}). Try `sudo db-scan self-update`, or reinstall db-scan to a user-writable location (e.g. ~/.local/bin)."
            );
        }
        Err(e) => return Err(anyhow::Error::new(e).context("running self-update")),
    };

    if status.updated() {
        ensure_runnable(&exe)
            .with_context(|| format!("setting permissions on {}", exe.display()))?;
        println!("Updated db-scan to {}", status.version());
    } else {
        println!("Already up to date ({})", status.version());
    }
    Ok(())
}

/// Resolved before the update, because the kernel reports the replaced executable as deleted
/// afterwards.
fn installed_path() -> std::io::Result<PathBuf> {
    fs::canonicalize(std::env::current_exe()?)
}

/// Replacing the binary under sudo hands ownership to root while keeping the old mode, which
/// locks out a user who owned a private copy.
fn ensure_runnable(path: &Path) -> std::io::Result<()> {
    let meta = fs::metadata(path)?;
    let current = meta.mode() & 0o7777;
    let wanted = runnable_mode(meta.uid(), current);
    if wanted != current {
        fs::set_permissions(path, fs::Permissions::from_mode(wanted))?;
    }
    Ok(())
}

fn runnable_mode(owner_uid: u32, mode: u32) -> u32 {
    if owner_uid == 0 {
        mode | WORLD_RUNNABLE
    } else {
        mode
    }
}

/// Best-effort check: if a newer release exists on GitHub, print a one-line nag
/// to stderr. Network/API failures are silently ignored.
pub(crate) fn nag_if_outdated() {
    let current = self_update::cargo_crate_version!();
    let latest = self_update::backends::github::Update::configure()
        .repo_owner(REPO_OWNER)
        .repo_name(REPO_NAME)
        .bin_name(BIN_NAME)
        .current_version(current)
        .build()
        .and_then(|u| u.get_latest_release());

    let Ok(release) = latest else { return };

    if self_update::version::bump_is_greater(current, &release.version).unwrap_or(false) {
        eprintln!(
            "note: db-scan {} is available (current: {}). Run `db-scan self-update` to upgrade.",
            release.version, current
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::root_owner_only(0, 0o700, 0o755)]
    #[case::root_owner_and_group(0, 0o750, 0o755)]
    #[case::root_already_runnable(0, 0o755, 0o755)]
    #[case::root_keeps_setuid_bit(0, 0o4700, 0o4755)]
    #[case::user_owned_unchanged(1000, 0o700, 0o700)]
    fn runnable_mode_widens_root_owned_binaries(
        #[case] owner_uid: u32,
        #[case] mode: u32,
        #[case] expected: u32,
    ) {
        assert_eq!(
            runnable_mode(owner_uid, mode),
            expected,
            "uid {owner_uid} mode {mode:o}"
        );
    }

    #[test]
    fn ensure_runnable_leaves_user_owned_binary_untouched() {
        let file = tempfile::NamedTempFile::new().unwrap();
        fs::set_permissions(file.path(), fs::Permissions::from_mode(0o700)).unwrap();

        ensure_runnable(file.path()).unwrap();

        let mode = fs::metadata(file.path()).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700, "mode after ensure_runnable");
    }
}
