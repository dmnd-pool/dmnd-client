use crate::proxy_state::{DownstreamType, ProxyState};
use crate::translator::downstream::SUBSCRIBE_TIMEOUT_SECS;
use crate::translator::error::Error;

use super::{downstream::Downstream, task_manager::TaskManager};
use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use sv1_api::json_rpc;
use sv1_api::server_to_client;
use sv1_api::utils::HexU32Be;
use tokio::sync::broadcast;
use tokio::task;
use tracing::{error, warn};

fn apply_mask(mask: Option<HexU32Be>, message: &mut server_to_client::Notify<'static>) {
    if let Some(mask) = mask {
        message.version = HexU32Be(message.version.0 & !mask.0);
    }
}

pub async fn start_notify(
    task_manager: Arc<Mutex<TaskManager>>,
    downstream: Arc<Mutex<Downstream>>,
    mut rx_sv1_notify: broadcast::Receiver<server_to_client::Notify<'static>>,
    last_notify: Option<server_to_client::Notify<'static>>,
    host: String,
    connection_id: u32,
) -> Result<(), Error<'static>> {
    let handle = {
        let task_manager = task_manager.clone();
        let (upstream_difficulty_config, stats_sender) = downstream
            .safe_lock(|d| (d.upstream_difficulty_config.clone(), d.stats_sender.clone()))?;
        upstream_difficulty_config.safe_lock(|c| {
            c.channel_nominal_hashrate += *crate::EXPECTED_SV1_HASHPOWER;
        })?;
        stats_sender.setup_stats(connection_id);
        task::spawn(async move {
            let timeout_timer = std::time::Instant::now();
            let mut first_sent = false;
            loop {
                let mask = downstream
                    .safe_lock(|d| d.version_rolling_mask.clone())
                    .unwrap();
                let is_a = match downstream.safe_lock(|d| !d.authorized_names.is_empty()) {
                    Ok(is_a) => is_a,
                    Err(e) => {
                        error!("{e}");
                        ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                        break;
                    }
                };
                if is_a && !first_sent && last_notify.is_some() {
                    if let Err(e) = Downstream::init_difficulty_management(&downstream).await {
                        error!("Failed to initailize difficulty managemant {e}")
                    };

                    let mut sv1_mining_notify_msg = match last_notify.clone() {
                        Some(sv1_mining_notify_msg) => sv1_mining_notify_msg,
                        None => {
                            error!("sv1_mining_notify_msg is None");
                            ProxyState::update_downstream_state(
                                DownstreamType::TranslatorDownstream,
                            );
                            break;
                        }
                    };
                    apply_mask(mask, &mut sv1_mining_notify_msg);
                    let message: json_rpc::Message = sv1_mining_notify_msg.into();
                    Downstream::send_message_downstream(downstream.clone(), message).await;
                    if downstream
                        .clone()
                        .safe_lock(|s| {
                            s.first_job_received = true;
                        })
                        .is_err()
                    {
                        error!("Translator Downstream Mutex Poisoned");
                        ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                        break;
                    }
                    first_sent = true;
                } else if is_a && last_notify.is_some() {
                    if let Err(e) =
                        start_update(task_manager, downstream.clone(), connection_id).await
                    {
                        warn!("Translator impossible to start update task: {e}");
                        break;
                    };

                    while let Ok(mut sv1_mining_notify_msg) = rx_sv1_notify.recv().await {
                        if downstream
                            .safe_lock(|d| d.last_notify = Some(sv1_mining_notify_msg.clone()))
                            .is_err()
                        {
                            error!("Translator Downstream Mutex Poisoned");
                            ProxyState::update_downstream_state(
                                DownstreamType::TranslatorDownstream,
                            );
                            break;
                        }

                        apply_mask(mask.clone(), &mut sv1_mining_notify_msg);
                        let message: json_rpc::Message = sv1_mining_notify_msg.into();
                        Downstream::send_message_downstream(downstream.clone(), message).await;
                    }
                    break;
                } else {
                    // timeout connection if miner does not send the authorize message after sending a subscribe
                    if timeout_timer.elapsed().as_secs() > SUBSCRIBE_TIMEOUT_SECS {
                        warn!(
                            "Downstream: miner.subscribe/miner.authorize TIMEOUT for {} {}",
                            &host, connection_id
                        );
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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
        // Prevent difficulty adjustments until after crate::ARGS.delay elapses
        tokio::time::sleep(std::time::Duration::from_secs(crate::ARGS.delay)).await;
        loop {
            let share_count = crate::translator::utils::get_share_count(connection_id);
            let sleep_duration = if share_count >= crate::SHARE_PER_MIN * 3.0
                || share_count <= crate::SHARE_PER_MIN / 3.0
            {
                std::time::Duration::from_millis(5000)
            } else {
                // TODO we really need to use differenet times seems to work well enaugh with 5 sec
                std::time::Duration::from_millis(5000)
            };

            tokio::time::sleep(sleep_duration).await;

            let ln = match downstream.safe_lock(|d| d.last_notify.clone()) {
                Ok(ln) => ln,
                Err(e) => {
                    error!("{e}");
                    return;
                }
            };
            assert!(ln.is_some());
            // if hashrate has changed, update difficulty management, and send new
            // mining.set_difficulty
            if let Err(e) = Downstream::try_update_difficulty_settings(&downstream, ln).await {
                error!("{e}");
                return;
            };
        }
    });
    TaskManager::add_update(task_manager, handle.into())
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)
}
