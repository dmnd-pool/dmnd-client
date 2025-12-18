use self_update::{backends, cargo_crate_version, update::UpdateStatus, TempDir};
use tracing::{debug, error, info};

const REPO_OWNER: &str = "dmnd-pool";
const REPO_NAME: &str = "dmnd-client";
const BIN_NAME: &str = "dmnd-client";

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
        .bin_install_path(tmp_dir.path())
        .build()
    {
        Ok(updater) => updater,
        Err(e) => {
            error!("Failed to configure update: {}", e);
            return;
        }
    };

    match updater.update_extended() {
        Ok(status) => match status {
            UpdateStatus::UpToDate => {
                info!("Package is up to date");
            }
            UpdateStatus::Updated(release) => {
                info!(
                    "Proxy updated to version {}. Restarting Proxy",
                    release.version
                );
                for asset in release.assets {
                    if asset.name == target_bin {
                        let bin_name = std::path::PathBuf::from(target_bin);
                        let new_exe = tmp_dir.path().join(&bin_name);
                        let mut file =
                            std::fs::File::create(&new_exe).expect("Failed to create file");
                        let mut download = self_update::Download::from_url(&asset.download_url);
                        download.set_header(
                            reqwest::header::ACCEPT,
                            reqwest::header::HeaderValue::from_static("application/octet-stream"), // to triggers a redirect to the actual binary.
                        );
                        download
                            .download_to(&mut file)
                            .expect("Failed to download file");
                    }
                }
                let bin_name = std::path::PathBuf::from(target_bin);
                let new_exe = tmp_dir.path().join(&bin_name);
                if let Err(e) = std::fs::rename(&new_exe, &original_path) {
                    error!(
                        "Failed to move new binary to {}: {}",
                        original_path.display(),
                        e
                    );
                    return;
                }

                let _ = std::fs::remove_dir_all(tmp_dir); // clean up tmp dir
                                                          // Get original cli rgs
                let args = std::env::args().skip(1).collect::<Vec<_>>();

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    use std::os::unix::process::CommandExt;
                    // On Unix-like systems, replace the current process with the new binary
                    if let Err(e) = std::fs::set_permissions(
                        &original_path,
                        std::fs::Permissions::from_mode(0o755),
                    ) {
                        error!(
                            "Failed to set executable permissions on {}: {}",
                            original_path.display(),
                            e
                        );
                        return;
                    }

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
                    std::process::Command::new(&original_path)
                        .args(&args)
                        .spawn()
                        .expect("Failed to start proxy");
                    std::process::exit(0);
                }
            }
        },
        Err(e) => {
            error!("Failed to update proxy: {}", e);
        }
    }
}
