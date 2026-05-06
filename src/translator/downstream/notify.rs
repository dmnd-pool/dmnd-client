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
    let difficulty = downstream.current_difficulty();
    if let Some(job) = downstream.recent_jobs.clone_last(difficulty) {
        return job;
    }

    let mut first_job = downstream.first_job.clone();
    downstream.recent_jobs.add_job(
        &mut first_job,
        downstream.version_rolling_mask.clone(),
        difficulty,
    );
    first_job
}

pub async fn start_notify(
    task_manager: Arc<Mutex<TaskManager>>,
    downstream: Arc<Mutex<Downstream>>,
    mut rx_sv1_notify: broadcast::Receiver<server_to_client::Notify<'static>>,
    host: String,
    connection_id: u32,
) -> Result<(), Error<'static>> {
    let is_closed = downstream.safe_lock(|d| d.is_closed())?;
    if is_closed {
        return Ok(());
    }

    let handle = {
        let task_manager = task_manager.clone();
        let (upstream_difficulty_config, stats_sender, latest_diff, registered_hashrate) =
            downstream.safe_lock(|d| {
                (
                    d.upstream_difficulty_config.clone(),
                    d.stats_sender.clone(),
                    d.difficulty_mgmt.current_difficulties.back().copied(),
                    d.difficulty_mgmt.estimated_downstream_hash_rate,
                )
            })?;
        upstream_difficulty_config.safe_lock(|c| {
            c.channel_nominal_hashrate += registered_hashrate;
            c.request_immediate_update();
        })?;
        downstream.safe_lock(|d| d.mark_channel_hashrate_registered())?;
        if let Err(e) = stats_sender.setup_stats_reliable(connection_id).await {
            error!("Failed to register downstream stats {connection_id}: {e}");
        }

        let is_closed = downstream.safe_lock(|d| d.is_closed())?;
        if is_closed {
            if let Err(e) = stats_sender.remove_stats_reliable(connection_id).await {
                error!("Failed to rollback downstream stats {connection_id}: {e}");
            }
            let should_subtract = downstream.safe_lock(|d| d.take_channel_hashrate_registered())?;
            if should_subtract {
                upstream_difficulty_config.safe_lock(|u| {
                    u.channel_nominal_hashrate -=
                        f32::min(registered_hashrate, u.channel_nominal_hashrate);
                    u.request_immediate_update();
                })?;
            }
            return Ok(());
        }

        task::spawn(async move {
            let timeout_timer = std::time::Instant::now();
            let mut authorized_in_time = true;
            let mut logged_waiting_for_auth = false;
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
                        if let Err(e) = downstream.safe_lock(|d| {
                            d.session_timing
                                .borrow_mut()
                                .record_first_notify_if_needed();
                        }) {
                            error!("Failed to record first notify timing: {e}");
                        }
                        let message: json_rpc::Message = job.into();
                        Downstream::send_message_downstream(downstream.clone(), message).await;
                    }
                    break;
                } else if !logged_waiting_for_auth {
                    debug!("Downstream {}: waiting for auth", connection_id);
                    logged_waiting_for_auth = true;
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
            if let Err(e) = start_update(task_manager, downstream.clone(), connection_id).await {
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
                            let difficulty = d.current_difficulty();
                            d.recent_jobs
                                .add_job(&mut sv1_mining_notify_msg, mask, difficulty);
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
                    if let Err(e) = downstream.safe_lock(|d| {
                        d.session_timing
                            .borrow_mut()
                            .record_first_notify_if_needed();
                    }) {
                        error!("Failed to record first notify timing: {e}");
                    }
                    let message: json_rpc::Message = sv1_mining_notify_msg.into();
                    Downstream::send_message_downstream(downstream.clone(), message).await;
                }
            }
            debug!(
                "Downstream: Shutting down sv1 downstream job notifier for {}",
                &host
            );
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
    let handle = task::spawn(async move {
        // Prevent difficulty adjustments until after delay elapses
        tokio::time::sleep(std::time::Duration::from_secs(crate::Configuration::delay())).await;
        loop {
            // Reconcile immediately after the delay instead of waiting a full interval first.
            if let Err(e) = Downstream::try_update_difficulty_settings(&downstream).await {
                error!("{e}");
                return;
            };

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
        }
    });
    TaskManager::add_update(task_manager, handle.into(), connection_id)
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)
}

#[cfg(test)]
mod tests {
    use super::{current_or_initial_job, start_notify, start_update};
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
    use std::{
        collections::VecDeque,
        sync::Arc,
        time::{Duration, Instant},
    };
    use sv1_api::{
        server_to_client::Notify,
        utils::{HexU32Be, MerkleNode, PrevHash},
    };
    use tokio::sync::{broadcast, mpsc::channel};

    const RAW_BOOTSTRAP_HASHRATE: f32 = 500_000_000_000_000.0;

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
            hard_minimum_difficulty: None,
        };
        let (upstream_config, _rx) =
            UpstreamDifficultyConfig::new(crate::CHANNEL_DIFF_UPDTATE_INTERVAL, 0.0);
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

    fn quantized_difficulty_for_hashrate(hashrate: f32) -> f32 {
        let share_per_second = *crate::SHARE_PER_MIN / 60.0;
        let raw_difficulty = hashrate / (share_per_second * 2f32.powi(32));
        crate::translator::downstream::diff_management::quantize_downstream_difficulty(
            raw_difficulty,
            None,
        )
    }

    fn hashrate_for_diff(diff: f32) -> f32 {
        let share_per_second = *crate::SHARE_PER_MIN / 60.0;
        share_per_second * diff * 2f32.powi(32)
    }

    fn assert_hashrate_close(actual: f32, expected: f32) {
        let tolerance = expected.abs().max(1.0) * 0.001;
        assert!(
            (actual - expected).abs() <= tolerance,
            "expected hashrate near {expected}, got {actual}",
        );
    }

    fn seeded_downstream_for_update_loop(
        estimated_downstream_hash_rate: f32,
        latest_difficulty: f32,
        pid_controller: Pid<f32>,
        submits: VecDeque<Instant>,
        channel_nominal_hashrate: f32,
    ) -> (Arc<Mutex<Downstream>>, Arc<Mutex<UpstreamDifficultyConfig>>) {
        let mut current_difficulties = VecDeque::new();
        current_difficulties.push_back(latest_difficulty);
        let difficulty_mgmt = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate,
            submits,
            pid_controller,
            current_difficulties,
            initial_difficulty: latest_difficulty,
            hard_minimum_difficulty: None,
        };
        let (upstream_config, _rx) = UpstreamDifficultyConfig::new(
            crate::CHANNEL_DIFF_UPDTATE_INTERVAL,
            channel_nominal_hashrate,
        );
        let upstream_config = Arc::new(Mutex::new(upstream_config));
        let (tx_sv1_submit, _rx_sv1_submit) = channel::<DownstreamMessages>(8);
        let (tx_outgoing, _rx_outgoing) = channel(8);
        let (tx_update_token, _rx_update_token) = channel(8);

        (
            Arc::new(Mutex::new(Downstream::new(
                1,
                vec!["worker".to_string()],
                vec![],
                None,
                None,
                tx_sv1_submit,
                tx_outgoing,
                0,
                difficulty_mgmt,
                upstream_config.clone(),
                StatsSender::new(),
                first_job("88"),
                tx_update_token,
            ))),
            upstream_config,
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

    #[tokio::test]
    async fn start_notify_requests_immediate_upstream_retarget_on_register() {
        let first_job = first_job("91");
        let (downstream, _rx_outgoing) =
            downstream_with_first_job(first_job, vec!["worker".to_string()]);
        let downstream = Arc::new(Mutex::new(downstream));
        let mut update_rx = downstream
            .safe_lock(|d| {
                d.upstream_difficulty_config
                    .safe_lock(|c| c.subscribe_updates())
                    .unwrap()
            })
            .unwrap();
        let task_manager = TaskManager::initialize();
        let (_tx_notify, rx_notify) = broadcast::channel(8);

        start_notify(
            task_manager.clone(),
            downstream.clone(),
            rx_notify,
            "127.0.0.1".to_string(),
            1,
        )
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(1), update_rx.changed())
            .await
            .unwrap()
            .unwrap();

        let channel_nominal_hashrate = downstream
            .safe_lock(|d| {
                d.upstream_difficulty_config
                    .safe_lock(|c| c.channel_nominal_hashrate)
                    .unwrap()
            })
            .unwrap();
        assert_eq!(channel_nominal_hashrate, 1.0);

        if let Some(aborter) = task_manager.safe_lock(|t| t.get_aborter()).unwrap() {
            drop(aborter);
        }
    }

    #[tokio::test]
    async fn first_retarget_waits_full_adjustment_interval_before_fixing_upstream_hashrate() {
        let quantized_difficulty = quantized_difficulty_for_hashrate(RAW_BOOTSTRAP_HASHRATE);
        let expected_first_retarget_hashrate = hashrate_for_diff(quantized_difficulty / 2.0);
        let mut pid = Pid::new(*crate::SHARE_PER_MIN, quantized_difficulty * 10.0);
        // If the first retarget runs immediately, this forces the bootstrap seed down one bucket.
        pid.p(-(quantized_difficulty * 0.05), f32::MAX)
            .i(0.0, f32::MAX)
            .d(0.0, f32::MAX);

        let (downstream, upstream_config) = seeded_downstream_for_update_loop(
            RAW_BOOTSTRAP_HASHRATE,
            quantized_difficulty,
            pid,
            VecDeque::new(),
            RAW_BOOTSTRAP_HASHRATE,
        );
        let task_manager = TaskManager::initialize();

        start_update(task_manager.clone(), downstream.clone(), 1)
            .await
            .unwrap();

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        let last_call_to_update_hr = downstream.safe_lock(|d| d.last_call_to_update_hr).unwrap();
        let channel_nominal_hashrate = upstream_config
            .safe_lock(|u| u.channel_nominal_hashrate)
            .unwrap();

        if let Some(aborter) = task_manager.safe_lock(|t| t.get_aborter()).unwrap() {
            drop(aborter);
        }

        assert_ne!(
            last_call_to_update_hr, 0,
            "the first retarget should reconcile the bootstrap seed immediately instead of waiting a full adjustment interval",
        );
        assert_hashrate_close(channel_nominal_hashrate, expected_first_retarget_hashrate);
    }
}
