use crate::{
    config::Configuration,
    debug_timing::{record_stage, SessionTimingStage},
    proxy_state::{DownstreamType, ProxyState},
    translator::{
        error::Error, proxy::Bridge, upstream::diff_management::UpstreamDifficultyConfig,
    },
};

use super::{downstream::Downstream, task_manager::TaskManager, DownstreamMessages};
use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use sv1_api::server_to_client;
use tokio::sync::{
    broadcast,
    mpsc::{Receiver, Sender},
    Semaphore,
};
use tokio::task;
use tracing::{debug, error};

#[allow(clippy::too_many_arguments)]
pub async fn start_accept_connection(
    task_manager: Arc<Mutex<TaskManager>>,
    tx_sv1_submit: Sender<DownstreamMessages>,
    tx_mining_notify: broadcast::Sender<server_to_client::Notify<'static>>,
    bridge: Arc<Mutex<super::super::proxy::Bridge>>,
    upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
    mut downstreams: Receiver<crate::DownstreamConnection>,
    stats_sender: crate::api::stats::StatsSender,
    tx_update_token: Sender<String>,
) -> Result<(), Error<'static>> {
    let handle = {
        let task_manager = task_manager.clone();
        task::spawn(async move {
            // This is needed. When bridge want to send a notification if no downstream is
            // available at least one receiver must be around.
            let _s = tx_mining_notify.subscribe();
            let downstream_init_slots =
                Arc::new(Semaphore::new(crate::DOWNSTREAM_INIT_CONCURRENCY));
            // These values are global startup inputs, not per-miner properties.
            let initial_hash_rate = Configuration::downstream_hashrate();
            let share_per_second = *crate::SHARE_PER_MIN / 60.0;
            let hard_minimum_difficulty =
                crate::translator::downstream::diff_management::hard_minimum_difficulty_for_proxy_mode(
                    Configuration::local(),
                );
            let raw_initial_difficulty = initial_hash_rate / (share_per_second * 2f32.powf(32.0));
            let base_initial_difficulty =
                crate::translator::downstream::diff_management::quantize_downstream_difficulty(
                    raw_initial_difficulty,
                    hard_minimum_difficulty,
                );

            debug!(
                "Translator downstream startup params: hash_rate={} H/s, shares_per_second={}, raw_initial_difficulty={}, initial_difficulty={}",
                initial_hash_rate,
                share_per_second,
                raw_initial_difficulty,
                base_initial_difficulty,
            );

            match Bridge::ready(&bridge).await {
                Ok(_) => {
                    debug!("Bridge is ready, proceeding with downstream opens");
                }
                Err(_) => {
                    error!("Bridge not ready");
                    return;
                }
            };

            while let Some(connection) = downstreams.recv().await {
                let crate::DownstreamConnection {
                    send_to_downstream: send,
                    recv_from_downstream: recv,
                    address: addr,
                    accepted_at,
                } = connection;
                let initial_difficulty = base_initial_difficulty;
                let expected_hash_rate = share_per_second * initial_difficulty * 2f32.powf(32.0);
                debug!("Translator opening connection for ip {}", addr);
                record_stage(SessionTimingStage::AcceptQueueWait, accepted_at.elapsed());
                let bridge_open_started_at = std::time::Instant::now();
                let open_sv1_downstream =
                    match bridge.safe_lock(|s| s.on_new_sv1_connection(expected_hash_rate)) {
                        Ok(sv1_downstream) => sv1_downstream,
                        Err(e) => {
                            error!("{e}");
                            break;
                        }
                    };
                record_stage(
                    SessionTimingStage::BridgeOpen,
                    bridge_open_started_at.elapsed(),
                );

                match open_sv1_downstream {
                    Ok(opened) => {
                        debug!(
                            "Translator opening connection for ip {} with id {}",
                            addr, opened.channel_id
                        );
                        let tx_sv1_submit = tx_sv1_submit.clone();
                        let tx_mining_notify = tx_mining_notify.clone();
                        let downstream_init_slots = downstream_init_slots.clone();
                        let upstream_difficulty_config = upstream_difficulty_config.clone();
                        let task_manager = task_manager.clone();
                        let stats_sender = stats_sender.clone();
                        let tx_update_token = tx_update_token.clone();
                        let host = addr.to_string();
                        task::spawn(async move {
                            let permit = match downstream_init_slots.acquire_owned().await {
                                Ok(permit) => permit,
                                Err(_) => {
                                    error!("Downstream init limiter closed unexpectedly");
                                    return;
                                }
                            };
                            let _permit = permit;
                            let downstream_init_started_at = std::time::Instant::now();
                            Downstream::new_downstream(
                                opened.channel_id,
                                tx_sv1_submit,
                                tx_mining_notify.subscribe(),
                                opened.extranonce,
                                opened.last_notify,
                                opened.extranonce2_len as usize,
                                host,
                                upstream_difficulty_config,
                                send,
                                recv,
                                task_manager,
                                initial_difficulty,
                                hard_minimum_difficulty,
                                stats_sender,
                                tx_update_token,
                                accepted_at,
                            )
                            .await;
                            record_stage(
                                SessionTimingStage::DownstreamInit,
                                downstream_init_started_at.elapsed(),
                            );
                            record_stage(
                                SessionTimingStage::AcceptToDownstreamReady,
                                accepted_at.elapsed(),
                            );
                        });
                    }
                    Err(e) => {
                        error!("{e:?}");
                        ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                        break;
                    }
                }
            }
        })
    };
    TaskManager::add_accept_connection(task_manager, handle.into())
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)
}
