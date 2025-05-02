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
use tracing::{error, info};

pub async fn start_accept_connection(
    task_manager: Arc<Mutex<TaskManager>>,
    tx_sv1_submit: Sender<DownstreamMessages>,
    tx_mining_notify: broadcast::Sender<server_to_client::Notify<'static>>,
    bridge: Arc<Mutex<super::super::proxy::Bridge>>,
    upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
    mut downstreams: Receiver<(Sender<String>, Receiver<String>, IpAddr)>,
    stats_sender: crate::api::stats::StatsSender,
) -> Result<(), Error<'static>> {
    let handle = {
        let task_manager = task_manager.clone();
        task::spawn(async move {
            // This is needed. When bridge want to send a notification if no downstream is
            // available at least one receiver must be around.
            let _s = tx_mining_notify.subscribe();
            while let Some((send, recv, addr)) = downstreams.recv().await {
                info!("Translator opening connection for ip {}", addr);
                // TODO handle also cases where a cpuminer want to connect
                let expected_hash_rate = *crate::EXPECTED_SV1_HASHPOWER;
                if Bridge::ready(&bridge).await.is_err() {
                    error!("Bridge not ready");
                    break;
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
                            "Translator opening connection for ip {} with id {}",
                            addr, opened.channel_id
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
                            stats_sender.clone(),
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
