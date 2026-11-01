#![allow(dead_code, unused_crate_dependencies)]
mod support;

use dmnd_client::sv2_connection;
use support::{
    error,
    pool::{
        control::{ChannelKey, EventMatcher, PoolCommand, PoolEvent},
        AckMode, PoolConfig,
    },
    processes::{ConfigurationSource, TestRig},
    sv1_client::{SolvedSubmission, Sv1Client},
    TestResult,
};
use tokio::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum Scenario {
    NonJd,
    Jd,
    DemandAck,
    Target,
    Tip,
    Disconnect,
    Failover,
    MultipleMiners,
    InvalidToken,
    RejectSubmit,
    RejectDeclaration,
    Pause,
    InvalidSv1,
    Duplicate,
    MalformedSv1,
    Environment,
    Toml,
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

async fn run(name: &str, scenario: Scenario) -> TestResult {
    // Serialize expensive external scenarios while preserving concurrent peers/miners within each scenario.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _serial = SERIAL.lock().await;
    let mut rig = TestRig::new(name)?;
    let result = exercise(&mut rig, scenario).await;
    rig.finish(result).await
}

async fn exercise(rig: &mut TestRig, scenario: Scenario) -> TestResult {
    rig.configuration_source = match scenario {
        Scenario::Environment => ConfigurationSource::Environment,
        Scenario::Toml => ConfigurationSource::Toml,
        _ => ConfigurationSource::Cli,
    };
    let jd = matches!(scenario, Scenario::Jd | Scenario::RejectDeclaration);
    if matches!(scenario, Scenario::Tip) {
        rig.network_hashpower = 0.00000001;
    }
    let mut config = PoolConfig::default();
    if matches!(scenario, Scenario::RejectDeclaration) {
        // Keep this longer scenario below the proxy's 70 upstream shares/minute limit.
        config.target =
            bitcoin::Target::from_compact(bitcoin::CompactTarget::from_consensus(0x1e00ffff))
                .to_le_bytes();
    }
    if matches!(scenario, Scenario::DemandAck) {
        config.ack_mode = AckMode::DemandShareOk;
    }
    let pools = if matches!(scenario, Scenario::Failover) {
        vec![config.clone(), config]
    } else {
        vec![config]
    };
    let miners = if matches!(scenario, Scenario::InvalidToken) {
        0
    } else if matches!(scenario, Scenario::MultipleMiners) {
        2
    } else {
        1
    };
    let token = if matches!(scenario, Scenario::InvalidToken) {
        "invalid-token"
    } else {
        "e2e-token"
    };
    rig.start(
        jd,
        pools,
        miners,
        token,
        match scenario {
            Scenario::RejectDeclaration => 1.0,
            _ => 0.1,
        },
    )
    .await?;
    let handle = rig.pools[0].handle();
    if matches!(scenario, Scenario::InvalidToken) {
        handle
            .wait_for(EventMatcher::SetupRejected, deadline())
            .await?;
        let snapshot = handle.command(PoolCommand::Snapshot).await?;
        if snapshot.accepted != 0 || !snapshot.channels.is_empty() {
            return Err(error("invalid token opened a mining channel"));
        }
        return Ok(());
    }
    if jd {
        handle
            .wait_for(EventMatcher::DeclarationRegistered, deadline())
            .await?;
        handle
            .wait_for(EventMatcher::CustomJobRegistered, deadline())
            .await?;
    }
    let first = if matches!(scenario, Scenario::Failover) {
        let other = rig.pools[1].handle();
        tokio::select! {
            event = handle.wait_for(EventMatcher::ValidatedShare,deadline()) => (0,event?),
            event = other.wait_for(EventMatcher::ValidatedShare,deadline()) => (1,event?),
        }
    } else {
        (
            0,
            handle
                .wait_for(EventMatcher::ValidatedShare, deadline())
                .await?,
        )
    };
    let PoolEvent::ShareAccepted(share) = &first.1.event else {
        unreachable!()
    };
    let channel = ChannelKey {
        peer: share.key.peer,
        channel: share.key.channel,
    };
    match scenario {
        Scenario::NonJd
        | Scenario::Jd
        | Scenario::DemandAck
        | Scenario::Environment
        | Scenario::Toml => {
            // A second validated submission and a real miner acknowledgement exercise both response paths.
            handle
                .wait_after(first.1.sequence, EventMatcher::ValidatedShare, deadline())
                .await?;
        }
        Scenario::Target => {
            let target =
                bitcoin::Target::from_compact(bitcoin::CompactTarget::from_consensus(0x1e07ffff))
                    .to_le_bytes();
            let changed = handle
                .command(PoolCommand::SetTarget { channel, target })
                .await?;
            let after = changed.events.last().expect("target event").sequence;
            let event = handle
                .wait_after(after, EventMatcher::ValidatedShare, deadline())
                .await?;
            if !matches!(event.event,PoolEvent::ShareAccepted(s) if s.target == target) {
                return Err(error("share used target preceding SetTarget"));
            }
        }
        Scenario::Tip => {
            let mut client =
                Sv1Client::connect(rig.proxy.as_ref().expect("proxy").sv1, deadline()).await?;
            let subscription = client.authorize("stale-worker", deadline()).await?;
            let notify = client.notify(deadline()).await?;
            let solved = SolvedSubmission::solve(
                &subscription,
                &notify,
                PoolConfig::default().target,
                deadline(),
            )
            .await?;
            let before = handle
                .command(PoolCommand::Snapshot)
                .await?
                .events
                .last()
                .expect("events")
                .sequence;
            let block = rig
                .tp
                .as_ref()
                .expect("TP")
                .mine_tip(&share.header, deadline())
                .await?;
            rig.artifacts.json("tip-block.json", &block)?;
            let tip = handle
                .wait_after(before, EventMatcher::TipChanged, deadline())
                .await?;
            let fresh_notify = client.notify(deadline()).await?;
            if fresh_notify["params"][0] == notify["params"][0] {
                return Err(error("proxy did not publish fresh work after the new tip"));
            }
            let log_offset =
                std::fs::metadata(&rig.proxy.as_ref().expect("proxy").process.log)?.len() as usize;
            let stale = solved
                .submit(&mut client, "stale-worker", deadline())
                .await?;
            let boundary = if stale["result"] == true {
                // SV1 can acknowledge before the bridge rejects a retained downstream job.
                // Require the actual rejection at the bridge or pool, after this submission.
                let diagnostic = format!(
                    "Share rejected: job_id {} not in retained job cache",
                    share.key.job
                );
                tokio::select! {
                    rejected = handle.wait_after(tip.sequence, EventMatcher::RejectedShare, deadline()) => {
                        if !matches!(rejected?.event, PoolEvent::ShareRejected {reason,..} if reason == "stale-share" || reason == "invalid-job-id") {
                            return Err(error("previous-tip submission was not rejected as stale work"));
                        }
                        "pool"
                    }
                    result = rig.proxy.as_mut().expect("proxy").process.wait_for_log_after(&diagnostic, log_offset, deadline()) => {
                        result?;
                        "proxy-bridge"
                    }
                }
            } else {
                "proxy-sv1"
            };
            rig.artifacts.json(
                "stale-sv1.json",
                &serde_json::json!({"submission":solved,"response":stale,"boundary":boundary,"fresh_notify":fresh_notify}),
            )?;
            let event = handle
                .wait_after(tip.sequence, EventMatcher::ValidatedShare, deadline())
                .await?;
            let PoolEvent::ShareAccepted(new) = event.event else {
                unreachable!()
            };
            if new.header[4..36] == share.header[4..36] {
                return Err(error("fresh share retained old chain tip"));
            }
            let PoolEvent::TipChanged { prev_hash } = tip.event else {
                unreachable!()
            };
            use bitcoin::hashes::Hash;
            if prev_hash != block.block_hash().to_byte_array() {
                return Err(error(
                    "TP announced a different tip than the accepted block",
                ));
            }
            if new.header[4..36] != prev_hash {
                return Err(error(
                    "fresh work does not reference the TP's announced tip",
                ));
            }
        }
        Scenario::Disconnect => {
            let disconnected = handle
                .command(PoolCommand::DisconnectPeer(channel.peer))
                .await?;
            let after = disconnected
                .events
                .last()
                .expect("disconnect event")
                .sequence;
            let event = handle
                .wait_after(after, EventMatcher::ValidatedShare, deadline())
                .await?;
            if !matches!(event.event,PoolEvent::ShareAccepted(s) if s.key.peer != channel.peer) {
                return Err(error("mining did not resume on a new peer"));
            }
        }
        Scenario::Failover => {
            let active = first.0;
            let remaining = rig.pools[1 - active].handle();
            let before = remaining
                .command(PoolCommand::Snapshot)
                .await?
                .events
                .last()
                .map_or(0, |event| event.sequence);
            let failed = rig.pools.remove(active);
            rig.retired_pools
                .push(failed.handle().command(PoolCommand::Snapshot).await?);
            failed.shutdown().await?;
            remaining
                .wait_after(before, EventMatcher::ValidatedShare, deadline())
                .await?;
        }
        Scenario::MultipleMiners => {
            for miner in &mut rig.miners {
                miner.process.wait_for_log("accepted", deadline()).await?;
            }
            let limit = deadline();
            loop {
                let snapshot = handle.command(PoolCommand::Snapshot).await?;
                let extranonces = snapshot
                    .events
                    .iter()
                    .filter_map(|e| match &e.event {
                        PoolEvent::ShareAccepted(s) => Some(s.extranonce[..28].to_vec()),
                        _ => None,
                    })
                    .collect::<std::collections::HashSet<_>>();
                if extranonces.len() >= 2 {
                    break;
                }
                let after = snapshot.events.last().expect("events").sequence;
                handle
                    .wait_after(after, EventMatcher::ValidatedShare, limit)
                    .await?;
            }
        }
        Scenario::RejectSubmit => {
            let snapshot = handle.command(PoolCommand::RejectNextSubmit).await?;
            let after = snapshot.events.last().expect("share event").sequence;
            let event = handle
                .wait_after(after, EventMatcher::ValidatedShare, deadline())
                .await?;
            let snapshot = handle.command(PoolCommand::Snapshot).await?;
            if !snapshot.events.iter().any(|r| r.sequence > after && r.sequence < event.sequence && matches!(&r.event,PoolEvent::ShareRejected {reason,..} if reason == "forced-rejection")) {
                return Err(error("RejectNextSubmit did not reject a submission before mining resumed"));
            }
        }
        Scenario::RejectDeclaration => {
            handle.command(PoolCommand::RejectNextDeclaration).await?;
            // TP refreshes trigger another declaration; the proxy may reconnect after its rejection.
            let before = handle
                .command(PoolCommand::Snapshot)
                .await?
                .events
                .last()
                .expect("event")
                .sequence;
            handle
                .wait_after(before, EventMatcher::DeclarationRegistered, deadline())
                .await?;
            handle
                .wait_after(before, EventMatcher::ValidatedShare, deadline())
                .await?;
            if !handle.command(PoolCommand::Snapshot).await?.events.iter().any(|r| matches!(&r.event,PoolEvent::DeclarationRejected {reason,..} if reason == "forced-rejection")) { return Err(error("declaration rejection was not observed")); }
        }
        Scenario::Pause => {
            let paused = handle.command(PoolCommand::PauseResponses).await?;
            let after = paused.events.last().expect("events").sequence;
            let during = handle
                .wait_after(after, EventMatcher::ValidatedShare, deadline())
                .await?;
            handle.command(PoolCommand::ResumeResponses).await?;
            handle
                .wait_after(during.sequence, EventMatcher::ValidatedShare, deadline())
                .await?;
        }
        Scenario::InvalidSv1 => {
            let mut client =
                Sv1Client::connect(rig.proxy.as_ref().expect("proxy").sv1, deadline()).await?;
            let _ = client.authorize("scripted-worker", deadline()).await?;
            let notify = client.notify(deadline()).await?;
            let response = client
                .submit(
                    "scripted-worker",
                    "4294967295",
                    "00",
                    "00000000",
                    "00000000",
                    deadline(),
                )
                .await?;
            if response["result"] == true {
                return Err(error("unknown SV1 job was accepted"));
            }
            rig.artifacts.json(
                "sv1-negative.json",
                &serde_json::json!({"notify":notify,"response":response,"boundary":"proxy"}),
            )?;
            handle
                .wait_after(first.1.sequence, EventMatcher::ValidatedShare, deadline())
                .await?;
        }
        Scenario::Duplicate => {
            // Keep the scripted submissions out of competition with the miner's upstream quota.
            rig.miners[0]
                .process
                .wait_for_log("accepted", deadline())
                .await?;
            rig.miners[0].process.shutdown().await?;
            // Allow the debug-build proof-of-work search a fixed budget, including both submits.
            let limit = Instant::now() + Duration::from_secs(180);
            let mut client =
                Sv1Client::connect(rig.proxy.as_ref().expect("proxy").sv1, limit).await?;
            let subscription = client.authorize("duplicate-worker", limit).await?;
            let notify = client.notify(limit).await?;
            let solved = SolvedSubmission::solve(
                &subscription,
                &notify,
                PoolConfig::default().target,
                limit,
            )
            .await?;
            let before = handle
                .command(PoolCommand::Snapshot)
                .await?
                .events
                .last()
                .expect("events")
                .sequence;
            let accepted = solved
                .submit(&mut client, "duplicate-worker", limit)
                .await?;
            if accepted["result"] != true {
                return Err(error(format!("valid scripted share rejected: {accepted}")));
            }
            let nonce = u32::from_str_radix(&solved.nonce, 16)?.to_le_bytes();
            let mut after = before;
            loop {
                if Instant::now() >= limit {
                    return Err(error("timed out waiting for the scripted pool share"));
                }
                let event = handle
                    .wait_after(after, EventMatcher::ValidatedShare, limit)
                    .await?;
                after = event.sequence;
                if matches!(&event.event,PoolEvent::ShareAccepted(s) if s.header[76..80] == nonce) {
                    break;
                }
            }
            let duplicate = solved
                .submit(&mut client, "duplicate-worker", limit)
                .await?;
            let rejected = handle
                .wait_after(after, EventMatcher::RejectedShare, limit)
                .await?;
            if !matches!(&rejected.event,PoolEvent::ShareRejected {reason,..} if reason == "duplicate-share")
            {
                return Err(error("duplicate was not rejected at the pool"));
            }
            rig.artifacts.json("duplicate-sv1.json",&serde_json::json!({"submission":solved,"first":accepted,"duplicate":duplicate,"rejection":rejected}))?;
            return Ok(());
        }
        Scenario::MalformedSv1 => {
            let mut client =
                Sv1Client::connect(rig.proxy.as_ref().expect("proxy").sv1, deadline()).await?;
            client.authorize("malformed-worker", deadline()).await?;
            let response = client
                .request(
                    "mining.submit",
                    serde_json::json!(["malformed-worker"]),
                    deadline(),
                )
                .await;
            let observation = match response {
                Ok(response) => {
                    if response["result"] == true {
                        return Err(error("malformed submit was accepted"));
                    }
                    response
                }
                Err(e) if e.to_string().contains("disconnected") => {
                    serde_json::json!({"disconnect":e.to_string()})
                }
                Err(e) => return Err(e),
            };
            rig.artifacts.json("malformed-sv1.json", &observation)?;
            handle
                .wait_after(first.1.sequence, EventMatcher::ValidatedShare, deadline())
                .await?;
        }
        Scenario::InvalidToken => unreachable!(),
    }
    for miner in &mut rig.miners {
        miner.process.ensure_running()?;
    }
    rig.miners[0]
        .process
        .wait_for_log("accepted", deadline())
        .await?;
    Ok(())
}

macro_rules! scenario {
    ($name:ident,$scenario:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() -> TestResult {
            run(stringify!($name), Scenario::$scenario).await
        }
    };
}
scenario!(non_jd_mining, NonJd);
scenario!(jd_mining, Jd);
scenario!(demand_share_ok, DemandAck);
scenario!(target_change, Target);
scenario!(new_chain_tip, Tip);
scenario!(upstream_disconnect, Disconnect);
scenario!(two_pool_endpoints, Failover);
scenario!(multiple_miners, MultipleMiners);
scenario!(invalid_token, InvalidToken);
scenario!(reject_next_submit, RejectSubmit);
scenario!(reject_next_declaration, RejectDeclaration);
scenario!(pause_and_resume_responses, Pause);
scenario!(unknown_sv1_job, InvalidSv1);
scenario!(duplicate_sv1_share, Duplicate);
scenario!(malformed_sv1_submit, MalformedSv1);
scenario!(environment_configuration_mining, Environment);
scenario!(toml_configuration_mining, Toml);
