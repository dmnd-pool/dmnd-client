use crate::config::Configuration;
use crate::proxy_state::{DownstreamType, ProxyState};
use crate::translator::downstream::SUBSCRIBE_TIMEOUT_SECS;
use crate::translator::error::Error;

use super::{downstream::Downstream, task_manager::TaskManager};
use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use sv1_api::json_rpc;
use sv1_api::server_to_client;
use tokio::sync::broadcast;
use tokio::task;
use tracing::{debug, error, warn};

fn current_or_initial_job(downstream: &mut Downstream) -> server_to_client::Notify<'static> {
    if let Some(job) = downstream.recent_jobs.clone_last() {
        return job;
    }

    let mut first_job = downstream.first_job.clone();
    downstream
        .recent_jobs
        .add_job(&mut first_job, downstream.version_rolling_mask.clone());
    first_job
}

pub async fn start_notify(
    task_manager: Arc<Mutex<TaskManager>>,
    downstream: Arc<Mutex<Downstream>>,
    mut rx_sv1_notify: broadcast::Receiver<server_to_client::Notify<'static>>,
    host: String,
    connection_id: u32,
) -> Result<(), Error<'static>> {
    let handle = {
        let task_manager = task_manager.clone();
        let (upstream_difficulty_config, stats_sender, latest_diff) =
            downstream.safe_lock(|d| {
                (
                    d.upstream_difficulty_config.clone(),
                    d.stats_sender.clone(),
                    d.difficulty_mgmt.current_difficulties.back().copied(),
                )
            })?;
        upstream_difficulty_config
            .safe_lock(|c| c.channel_nominal_hashrate += Configuration::downstream_hashrate())?;
        stats_sender.setup_stats(connection_id);
        let task_manager_clone = task_manager.clone();
        task::spawn(async move {
            let timeout_timer = std::time::Instant::now();
            let mut authorized_in_time = true;
            // Initialization loop. As soon as the miner authorizes, seed it with the latest known
            // job snapshot instead of waiting for a future notify or the timeout fallback.
            loop {
                let job = downstream
                    .safe_lock(|d| {
                        if d.authorized_names.is_empty() {
                            None
                        } else {
                            Some(current_or_initial_job(d))
                        }
                    })
                    .unwrap();

                if let Some(job) = job {
                    if let Err(e) = Downstream::init_difficulty_management(&downstream).await {
                        error!("Failed to initailize difficulty managemant {e}")
                    } else {
                        let message: json_rpc::Message = job.into();
                        Downstream::send_message_downstream(downstream.clone(), message).await;
                    }
                    break;
                } else {
                    warn!("Downstream {}: waiting for auth", connection_id);
                }

                // timeout connection if miner does not send the authorize message after sending a subscribe
                if timeout_timer.elapsed().as_secs() > SUBSCRIBE_TIMEOUT_SECS {
                    warn!(
                        "Downstream: miner.subscribe/miner.authorize TIMEOUT for {} {}",
                        &host, connection_id
                    );
                    authorized_in_time = false;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            if let Err(e) = start_update(task_manager_clone.clone(), downstream.clone(), connection_id).await {
                warn!("Translator impossible to start update task: {e}");
            } else if authorized_in_time {
                loop {
                    let mut sv1_mining_notify_msg = match rx_sv1_notify.recv().await {
                        Ok(msg) => msg,
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(
                                "Downstream {}: notify receiver lagged by {} messages",
                                connection_id, skipped
                            );
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    };

                    if downstream
                        .safe_lock(|d| {
                            d.first_job = sv1_mining_notify_msg.clone();
                            let mask = d.version_rolling_mask.clone();
                            d.recent_jobs.add_job(&mut sv1_mining_notify_msg, mask);
                            debug!(
                                "Downstream {}: Added job_id {} to recent_notifies. Current jobs: {:?}",
                                connection_id,
                                sv1_mining_notify_msg.job_id,
                                d.recent_jobs.current_jobs()
                            );
                        })
                        .is_err()
                    {
                        error!("Translator Downstream Mutex Poisoned");
                        ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                        break;
                    }

                    debug!(
                        "Sending Job {:?} to miner. Difficulty: {:?}",
                        &sv1_mining_notify_msg, latest_diff
                    );
                    let message: json_rpc::Message = sv1_mining_notify_msg.into();
                    Downstream::send_message_downstream(downstream.clone(), message).await;
                }
            }
            warn!(
                "Downstream: Shutting down sv1 downstream job notifier for {}",
                &host
            );
            if let Some(kill_signal) = task_manager_clone.safe_lock(|tm| tm.send_kill_signal.clone()).ok() {
                let _ = kill_signal.send(connection_id).await;
            }
        })
    };
    TaskManager::add_notify(task_manager, handle.into(), connection_id)
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)
}

async fn start_update(
    task_manager: Arc<Mutex<TaskManager>>,
    downstream: Arc<Mutex<Downstream>>,
    connection_id: u32,
) -> Result<(), Error<'static>> {
    let task_manager_clone = task_manager.clone();
    let handle = task::spawn(async move {
        // Prevent difficulty adjustments until after delay elapses
        tokio::time::sleep(std::time::Duration::from_secs(crate::Configuration::delay())).await;
        loop {
            let share_count = crate::translator::utils::get_share_count(connection_id);
            let sleep_duration = if share_count >= *crate::SHARE_PER_MIN * 3.0
                || share_count <= *crate::SHARE_PER_MIN / 3.0
            {
                // TODO: this should only apply when after the first share has been received
                std::time::Duration::from_millis(crate::Configuration::adjustment_interval())
            } else {
                std::time::Duration::from_millis(crate::Configuration::adjustment_interval())
            };

            tokio::time::sleep(sleep_duration).await;

            // if hashrate has changed, update difficulty management, and send new
            // mining.set_difficulty
            if let Err(e) = Downstream::try_update_difficulty_settings(&downstream).await {
                error!("{e}");
                break;
            };
        }
        if let Some(kill_signal) = task_manager_clone.safe_lock(|tm| tm.send_kill_signal.clone()).ok() {
            let _ = kill_signal.send(connection_id).await;
        }
    });
    TaskManager::add_update(task_manager, handle.into(), connection_id)
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)
}

#[cfg(test)]
mod tests {
    use super::{current_or_initial_job, start_notify};
    use crate::{
        api::stats::StatsSender,
        translator::{
            downstream::{
                downstream::{Downstream, DownstreamDifficultyConfig},
                task_manager::TaskManager,
                DownstreamMessages,
            },
            upstream::diff_management::UpstreamDifficultyConfig,
        },
    };
    use pid::Pid;
    use roles_logic_sv2::utils::Mutex;
    use std::{collections::VecDeque, sync::Arc, time::Duration};
    use sv1_api::{
        server_to_client::Notify,
        utils::{HexU32Be, MerkleNode, PrevHash},
    };
    use tokio::sync::{broadcast, mpsc::channel};

    fn first_job(job_id: &str) -> Notify<'static> {
        Notify {
            job_id: job_id.to_string(),
            prev_hash: PrevHash::try_from("0".repeat(64).as_str()).unwrap(),
            coin_base1: "ffff".try_into().unwrap(),
            coin_base2: "ffff".try_into().unwrap(),
            merkle_branch: vec![MerkleNode::try_from(vec![1_u8; 32]).unwrap()],
            version: HexU32Be(5667),
            bits: HexU32Be(5678),
            time: HexU32Be(5609),
            clean_jobs: true,
        }
    }

    fn downstream_with_first_job(
        first_job: Notify<'static>,
        authorized_names: Vec<String>,
    ) -> (
        Downstream,
        tokio::sync::mpsc::Receiver<sv1_api::json_rpc::Message>,
    ) {
        let mut current_difficulties = VecDeque::new();
        current_difficulties.push_back(1.0);
        let difficulty_mgmt = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate: 1.0,
            submits: VecDeque::new(),
            pid_controller: Pid::new(*crate::SHARE_PER_MIN, 10.0),
            current_difficulties,
            initial_difficulty: 1.0,
        };
        let upstream_config = UpstreamDifficultyConfig {
            channel_diff_update_interval: crate::CHANNEL_DIFF_UPDTATE_INTERVAL,
            channel_nominal_hashrate: 0.0,
        };
        let (tx_sv1_submit, _rx_sv1_submit) = channel::<DownstreamMessages>(8);
        let (tx_outgoing, rx_outgoing) = channel(8);
        let (tx_update_token, _rx_update_token) = channel(8);

        (
            Downstream::new(
                1,
                authorized_names,
                vec![],
                None,
                None,
                tx_sv1_submit,
                tx_outgoing,
                0,
                difficulty_mgmt,
                Arc::new(Mutex::new(upstream_config)),
                StatsSender::new(),
                first_job,
                tx_update_token,
            ),
            rx_outgoing,
        )
    }

    #[tokio::test]
    async fn current_or_initial_job_seeds_recent_jobs_without_mutating_snapshot() {
        let first_job = first_job("42");
        let original_job_id = first_job.job_id.clone();
        let (mut downstream, _rx_outgoing) = downstream_with_first_job(first_job, vec![]);

        let seeded_job = current_or_initial_job(&mut downstream);

        assert_eq!(downstream.first_job.job_id, original_job_id);
        assert_eq!(downstream.recent_jobs.current_jobs().len(), 1);
        assert_ne!(seeded_job.job_id, downstream.first_job.job_id);
    }

    #[tokio::test]
    async fn start_notify_sends_initial_job_immediately_after_auth() {
        let first_job = first_job("77");
        let (downstream, mut rx_outgoing) =
            downstream_with_first_job(first_job, vec!["worker".to_string()]);
        let downstream = Arc::new(Mutex::new(downstream));
        let task_manager = TaskManager::initialize();
        let (_tx_notify, rx_notify) = broadcast::channel(8);

        start_notify(
            task_manager.clone(),
            downstream,
            rx_notify,
            "127.0.0.1".to_string(),
            1,
        )
        .await
        .unwrap();

        let first = tokio::time::timeout(Duration::from_secs(1), rx_outgoing.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(1), rx_outgoing.recv())
            .await
            .unwrap()
            .unwrap();

        let first = serde_json::to_string(&first).unwrap();
        let second = serde_json::to_string(&second).unwrap();
        assert!(first.contains("mining.set_difficulty"));
        assert!(second.contains("mining.notify"));

        if let Some(aborter) = task_manager.safe_lock(|t| t.get_aborter()).unwrap() {
            drop(aborter);
        }
    }
}
