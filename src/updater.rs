use anyhow::Context as _;

const REPO_OWNER: &str = "robert-sjoblom";
const REPO_NAME: &str = "db-scan";
const BIN_NAME: &str = "db-scan";

/// Download and install the latest GitHub release, replacing the running binary.
pub(crate) fn update() -> anyhow::Result<()> {
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
        println!("Updated db-scan to {}", status.version());
    } else {
        println!("Already up to date ({})", status.version());
    }
    Ok(())
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
