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
        upstream_difficulty_config.safe_lock(|c| {
            c.channel_nominal_hashrate += *crate::EXPECTED_SV1_HASHPOWER;
        })?;
        stats_sender.setup_stats(connection_id);
        task::spawn(async move {
            let timeout_timer = std::time::Instant::now();
            let mut authorized_in_time = true;
            // Initilization loop
            loop {
                let is_a = downstream
                    .safe_lock(|d| !d.authorized_names.is_empty())
                    .unwrap();
                if !is_a {
                    warn!("Downstream {}: waiting for auth", connection_id);
                }
                if let Some(job) = downstream
                    .safe_lock(|d| d.recent_jobs.clone_last())
                    .unwrap()
                {
                    if is_a {
                        if let Err(e) = Downstream::init_difficulty_management(&downstream).await {
                            error!("Failed to initailize difficulty managemant {e}")
                        } else {
                            let message: json_rpc::Message = job.into();
                            Downstream::send_message_downstream(downstream.clone(), message).await;
                        }
                        break;
                    }
                } else {
                    warn!("Downstream {}: waiting for first job", connection_id);
                }
                // timeout connection if miner does not send the authorize message after sending a subscribe
                if timeout_timer.elapsed().as_secs() > SUBSCRIBE_TIMEOUT_SECS {
                    if is_a {
                        warn!("No configure received after timeout, use initial first job");
                        let job = downstream
                            .safe_lock(|d| {
                                let mut first_job = d.first_job.clone();
                                d.recent_jobs
                                    .add_job(&mut first_job, d.version_rolling_mask.clone());
                                d.first_job = first_job;
                                d.first_job.clone()
                            })
                            .unwrap();
                        if let Err(e) = Downstream::init_difficulty_management(&downstream).await {
                            error!("Failed to initailize difficulty managemant {e}")
                        } else {
                            let message: json_rpc::Message = job.into();
                            Downstream::send_message_downstream(downstream.clone(), message).await;
                        }
                    } else {
                        warn!(
                            "Downstream: miner.subscribe/miner.authorize TIMEOUT for {} {}",
                            &host, connection_id
                        );
                        authorized_in_time = false;
                    }
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            if let Err(e) = start_update(task_manager, downstream.clone(), connection_id).await {
                warn!("Translator impossible to start update task: {e}");
            } else if authorized_in_time {
                // Get the mask after initialization since is set by configure message
                let mask = downstream
                    .safe_lock(|d| d.version_rolling_mask.clone())
                    .unwrap();
                while let Ok(mut sv1_mining_notify_msg) = rx_sv1_notify.recv().await {
                    if downstream
                        .safe_lock(|d| {
                            d.recent_jobs.add_job(&mut sv1_mining_notify_msg,mask.clone());
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
                        ProxyState::update_downstream_state(
                            DownstreamType::TranslatorDownstream,
                        );
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
            // TODO here we want to be sure that on drop this is called
            let _ = Downstream::remove_downstream_hashrate_from_channel(&downstream);
            // TODO here we want to kill the tasks
            warn!(
                "Downstream: Shutting down sv1 downstream job notifier for {}",
                &host
            );
        })
    };
    TaskManager::add_notify(task_manager, handle.into())
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
            let share_count = crate::translator::utils::get_share_count(connection_id);
            let sleep_duration = if share_count >= crate::SHARE_PER_MIN * 3.0
                || share_count <= crate::SHARE_PER_MIN / 3.0
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
                return;
            };
        }
    });
    TaskManager::add_update(task_manager, handle.into())
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)
}
