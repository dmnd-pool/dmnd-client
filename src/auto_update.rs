use minisign_verify::{PublicKey, Signature};
use self_update::{backends, cargo_crate_version, update::ReleaseAsset, TempDir};
use tracing::{debug, error, info};

const REPO_OWNER: &str = "dmnd-pool";
const REPO_NAME: &str = "dmnd-client";
const BIN_NAME: &str = "dmnd-client";
const RELEASE_PUBLIC_KEY: &str = include_str!("../release-signing.pub");

pub fn check_update_proxy() {
    info!("Checking for latest released version...");
    // Determine the OS and map to the asset name
    let os = std::env::consts::OS;
    let target_bin = match os {
        "linux" => "dmnd-client-linux",
        "macos" => "dmnd-client-macos",
        "windows" => "dmnd-client-windows.exe",
        _ => {
            error!("Warning: Unsupported OS '{}', skipping update", os);
            unreachable!()
        }
    };

    debug!("OS: {}", target_bin);
    debug!("DMND Client version: {}", cargo_crate_version!());
    let original_path = std::env::current_exe().expect("Failed to get current executable path");
    let tmp_dir = TempDir::new_in(::std::env::current_dir().expect("Failed to get current dir"))
        .expect("Failed to create tmp dir");

    let updater = match backends::github::Update::configure()
        .repo_owner(REPO_OWNER)
        .repo_name(REPO_NAME)
        .bin_name(BIN_NAME)
        .current_version(cargo_crate_version!())
        .target(target_bin)
        .show_output(false)
        .no_confirm(true)
        .build()
    {
        Ok(updater) => updater,
        Err(e) => {
            error!("Failed to configure update: {}", e);
            return;
        }
    };

    let latest_release = match updater.get_latest_release() {
        Ok(release) => release,
        Err(e) => {
            error!("Failed to check the latest release: {}", e);
            return;
        }
    };
    if !self_update::version::bump_is_greater(cargo_crate_version!(), &latest_release.version)
        .unwrap_or(false)
    {
        info!("Package is up to date");
        return;
    }

    info!(
        "New version available: v{} (current version: v{})",
        latest_release.version,
        cargo_crate_version!()
    );
    let binary_asset = match latest_release
        .assets
        .iter()
        .find(|asset| asset.name == target_bin)
    {
        Some(asset) => asset,
        None => {
            error!(
                "Latest release v{} is missing {}",
                latest_release.version, target_bin
            );
            return;
        }
    };
    let binary_path = tmp_dir.path().join(target_bin);
    let mut binary_file =
        std::fs::File::create(&binary_path).expect("Failed to create update file");
    let mut download = self_update::Download::from_url(&binary_asset.download_url);
    download.set_header(
        reqwest::header::ACCEPT,
        reqwest::header::HeaderValue::from_static("application/octet-stream"),
    );
    info!("Updating to latest release");
    download
        .download_to(&mut binary_file)
        .expect("Failed to download update");
    drop(binary_file);

    if let Err(e) = verify_signature(
        &latest_release.assets,
        target_bin,
        &latest_release.version,
        &binary_path,
        tmp_dir.path(),
    ) {
        error!("Signature verification failed: {e}; skipping update");
        return;
    }
    if let Err(e) = self_update::self_replace::self_replace(&binary_path) {
        error!("Failed to replace {}: {e}", original_path.display());
        return;
    }

    let _ = std::fs::remove_dir_all(tmp_dir); // clean up tmp dir
    let args = std::env::args().skip(1).collect::<Vec<_>>();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::process::CommandExt;
        // On Unix-like systems, replace the current process with the new binary
        if let Err(e) =
            std::fs::set_permissions(&original_path, std::fs::Permissions::from_mode(0o755))
        {
            error!(
                "Failed to set executable permissions on {}: {}",
                original_path.display(),
                e
            );
            return;
        }

        info!(
            "Proxy updated to version {}. Restarting Proxy",
            latest_release.version
        );
        let err = std::process::Command::new(&original_path)
            .args(&args)
            .exec();
        // If exec fails, log the error and exit
        error!("Failed to exec new binary: {:?}", err);
        std::process::exit(1);
    }
    #[cfg(not(unix))]
    {
        // On Windows, spawn the new process and exit the current one
        info!(
            "Proxy updated to version {}. Restarting Proxy",
            latest_release.version
        );
        std::process::Command::new(&original_path)
            .args(&args)
            .spawn()
            .expect("Failed to start proxy");
        std::process::exit(0);
    }
}

fn verify_signature(
    assets: &[ReleaseAsset],
    target_bin: &str,
    release_version: &str,
    binary: &std::path::Path,
    temp_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let signature_name = format!("{target_bin}.minisig");
    let signature_asset = assets
        .iter()
        .find(|asset| asset.name == signature_name)
        .ok_or_else(|| format!("Release is missing {signature_name}"))?;
    let signature_path = temp_dir.join(signature_name);
    let mut signature_file = std::fs::File::create(&signature_path)?;
    let mut download = self_update::Download::from_url(&signature_asset.download_url);
    download.set_header(
        reqwest::header::ACCEPT,
        reqwest::header::HeaderValue::from_static("application/octet-stream"),
    );
    download.download_to(&mut signature_file)?;

    info!("Verifying release signature");
    let public_key = PublicKey::decode(RELEASE_PUBLIC_KEY)?;
    let signature = Signature::from_file(&signature_path)?;
    public_key.verify(&std::fs::read(binary)?, &signature, false)?;
    verify_signature_metadata(signature.trusted_comment(), release_version, target_bin)?;
    info!("Release signature verified");
    Ok(())
}

fn verify_signature_metadata(
    trusted_comment: &str,
    release_version: &str,
    target_bin: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = format!("{BIN_NAME} {release_version} {target_bin}");
    if trusted_comment != expected {
        return Err(format!(
            "Release signature metadata mismatch: expected {expected:?}, got {trusted_comment:?}"
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_public_key_is_valid() {
        assert!(PublicKey::decode(RELEASE_PUBLIC_KEY).is_ok());
    }

    #[test]
    fn release_signature_metadata_is_bound_to_version_and_target() {
        assert!(verify_signature_metadata(
            "dmnd-client 0.3.29 dmnd-client-linux",
            "0.3.29",
            "dmnd-client-linux"
        )
        .is_ok());
        assert!(verify_signature_metadata(
            "dmnd-client 0.3.28 dmnd-client-linux",
            "0.3.29",
            "dmnd-client-linux"
        )
        .is_err());
        assert!(verify_signature_metadata(
            "dmnd-client 0.3.29 dmnd-client-windows.exe",
            "0.3.29",
            "dmnd-client-linux"
        )
        .is_err());
    }
}
