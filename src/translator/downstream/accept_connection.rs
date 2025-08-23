use crate::{
    proxy_state::{DownstreamType, ProxyState},
    translator::{
        error::Error, proxy::Bridge, upstream::diff_management::UpstreamDifficultyConfig,
    },
};

use super::{downstream::Downstream, task_manager::TaskManager, DownstreamMessages};
use roles_logic_sv2::utils::Mutex;
use std::{net::IpAddr, sync::Arc};
use sv1_api::server_to_client;
use tokio::sync::{
    broadcast,
    mpsc::{Receiver, Sender},
};
use tokio::task;
use tracing::{debug, error, info};

pub async fn start_accept_connection(
    task_manager: Arc<Mutex<TaskManager>>,
    tx_sv1_submit: Sender<DownstreamMessages>,
    tx_mining_notify: broadcast::Sender<server_to_client::Notify<'static>>,
    bridge: Arc<Mutex<super::super::proxy::Bridge>>,
    upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
    mut downstreams: Receiver<(Sender<String>, Receiver<String>, IpAddr)>,
    stats_sender: crate::api::stats::StatsSender,
    router: Arc<crate::router::Router>,
    //for logging purposes
    pool_address: std::net::SocketAddr,
) -> Result<(), Error<'static>> {
    let handle = {
        let task_manager = task_manager.clone();
        task::spawn(async move {
            // This is needed. When bridge want to send a notification if no downstream is
            // available at least one receiver must be around.
            let _s = tx_mining_notify.subscribe();
            while let Some((send, recv, addr)) = downstreams.recv().await {
                info!("Translator opening connection for ip {}", addr);
                // The initial difficulty is derived from the formula: difficulty = hash_rate / (shares_per_second * 2^32)
                let initial_hash_rate = *crate::EXPECTED_SV1_HASHPOWER;
                info!(
                    "Pool {}: Translator initial hash rate for ip {} is {} H/s",
                    pool_address, addr, initial_hash_rate
                );
                let share_per_second = crate::SHARE_PER_MIN / 60.0;
                info!(
                    "Pool {}: Translator share per second for ip {} is {} shares/s",
                    pool_address, addr, share_per_second
                );
                let initial_difficulty = initial_hash_rate / (share_per_second * 2f32.powf(32.0));
                let initial_difficulty =
                    crate::translator::downstream::diff_management::nearest_power_of_10(
                        initial_difficulty,
                    );
                info!(
                    "Pool {}: Translator initial difficulty for ip {} is {}",
                    pool_address, addr, initial_difficulty
                );
                // Formula: expected_hash_rate = (shares_per_second) * initial_difficulty * 2^32, where shares_per_second = SHARE_PER_MIN / 60
                let expected_hash_rate =
                    (crate::SHARE_PER_MIN / 60.0) * initial_difficulty * 2f32.powf(32.0);
                info!(
                    "Pool {}: Translator expected hash rate for ip {} is {} H/s",
                    pool_address, addr, expected_hash_rate
                );

                match Bridge::ready(&bridge).await {
                    Ok(_) => {
                        debug!("Bridge is ready, proceeding with connection");
                    }
                    Err(_) => {
                        error!("Bridge not ready");
                        break;
                    }
                };

                let open_sv1_downstream =
                    match bridge.safe_lock(|s| s.on_new_sv1_connection(expected_hash_rate)) {
                        Ok(sv1_downstream) => sv1_downstream,
                        Err(e) => {
                            error!("{e}");
                            break;
                        }
                    };

                match open_sv1_downstream {
                    Ok(opened) => {
                        info!(
                            "Pool {}: Translator opening connection for ip {} with id {}",
                            pool_address, addr, opened.channel_id
                        );
                        Downstream::new_downstream(
                            opened.channel_id,
                            tx_sv1_submit.clone(),
                            tx_mining_notify.subscribe(),
                            opened.extranonce,
                            opened.last_notify,
                            opened.extranonce2_len as usize,
                            addr.to_string(),
                            upstream_difficulty_config.clone(),
                            send,
                            recv,
                            task_manager.clone(),
                            initial_difficulty,
                            stats_sender.clone(),
                            router.clone(),
                            pool_address,
                        )
                        .await
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
