#![allow(dead_code, unused_crate_dependencies)]
mod support;

use dmnd_client::sv2_connection;
use std::{fs, path::Path, time::Duration};
use support::{
    artifacts::Artifacts,
    processes::{controlled_env, free_address, ManagedProcess},
    TestResult,
};
use tokio::time::Instant;

#[tokio::test]
async fn invalid_arguments_exit_with_diagnostics() -> TestResult {
    for (index, args, diagnostic) in [
        (0, vec!["--unknown-argument"], "unexpected argument"),
        (1, vec!["--token"], "a value is required"),
        (2, vec!["-d", "invalid"], "Invalid number"),
        (
            3,
            vec!["--downstream-min-difficulty=-1"],
            "finite non-negative",
        ),
        (
            4,
            vec!["--downstream-min-difficulty=NaN"],
            "finite non-negative",
        ),
        (5, vec!["--max-active-downstreams", "abc"], "invalid value"),
    ] {
        let mut artifacts = Artifacts::new(&format!("cli-invalid-{index}"))?;
        let args = args.into_iter().map(String::from).collect::<Vec<_>>();
        let mut process = ManagedProcess::spawn(
            Path::new(env!("CARGO_BIN_EXE_dmnd-client")),
            &args,
            &controlled_env(),
            &artifacts,
            "proxy",
        )?;
        let status = process
            .wait(Instant::now() + Duration::from_secs(10))
            .await?;
        let log = fs::read_to_string(&process.log)?;
        process.shutdown().await?;
        assert!(!status.success(), "{args:?} unexpectedly succeeded");
        assert!(
            log.contains(diagnostic),
            "{args:?}: expected {diagnostic:?}; got {log}"
        );
        artifacts.finish(&Ok(()), &[])?;
    }
    Ok(())
}

#[tokio::test]
async fn cli_file_environment_precedence() -> TestResult {
    for (name, file, cli, expected) in [
        (
            "env",
            None,
            None,
            "Using downstream minimum difficulty: 0.125",
        ),
        (
            "toml",
            Some(0.25),
            None,
            "Using downstream minimum difficulty: 0.25",
        ),
        (
            "cli",
            Some(0.25),
            Some("0.5"),
            "Using downstream minimum difficulty: 0.5",
        ),
    ] {
        let mut artifacts = Artifacts::new(&format!("cli-precedence-{name}"))?;
        let config = artifacts.path.join("selected.toml");
        fs::write(
            &config,
            file.map(|value| format!("downstream_min_difficulty = {value}\n"))
                .unwrap_or_default(),
        )?;
        let pool = free_address()?;
        let mut args = vec![
            "--local".into(),
            "--pool-address".into(),
            pool.to_string(),
            "--token".into(),
            "e2e-token".into(),
            "--listening-addr".into(),
            free_address()?.to_string(),
            "--api-server-port".into(),
            free_address()?.port().to_string(),
        ];
        let mut env = controlled_env();
        env.insert("DOWNSTREAM_MIN_DIFFICULTY".into(), "0.125".into());
        env.insert(
            "DMND_CLIENT_CONFIG_FILE".into(),
            config.to_string_lossy().into(),
        );
        if let Some(value) = cli {
            args.extend(["--downstream-min-difficulty".into(), value.into()]);
        }
        let mut process = ManagedProcess::spawn(
            Path::new(env!("CARGO_BIN_EXE_dmnd-client")),
            &args,
            &env,
            &artifacts,
            "proxy",
        )?;
        let result = process
            .wait_for_log(expected, Instant::now() + Duration::from_secs(10))
            .await
            .map(|_| ());
        process.shutdown().await?;
        artifacts.finish(&result, &[])?;
        result?;
    }
    Ok(())
}

#[tokio::test]
async fn explicit_config_path_overrides_environment_path() -> TestResult {
    let mut artifacts = Artifacts::new("cli-config-path")?;
    let file = artifacts.path.join("explicit.toml");
    fs::write(&file, "downstream_min_difficulty = 0.75\n")?;
    let mut env = controlled_env();
    env.insert(
        "DMND_CLIENT_CONFIG_FILE".into(),
        artifacts.path.join("missing.toml").to_string_lossy().into(),
    );
    let args = vec![
        "--config".into(),
        file.to_string_lossy().into(),
        "--local".into(),
        "--token".into(),
        "e2e-token".into(),
        "--pool-address".into(),
        free_address()?.to_string(),
        "--listening-addr".into(),
        free_address()?.to_string(),
        "--api-server-port".into(),
        free_address()?.port().to_string(),
    ];
    let mut process = ManagedProcess::spawn(
        Path::new(env!("CARGO_BIN_EXE_dmnd-client")),
        &args,
        &env,
        &artifacts,
        "proxy",
    )?;
    let result = process
        .wait_for_log(
            "Using downstream minimum difficulty: 0.75",
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .map(|_| ());
    process.shutdown().await?;
    artifacts.finish(&result, &[])?;
    result
}
