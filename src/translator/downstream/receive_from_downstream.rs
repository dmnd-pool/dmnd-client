use super::{downstream::Downstream, task_manager::TaskManager};
use crate::{monitor::MonitorAPI, proxy_state::ProxyState, translator::error::Error};
use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use sv1_api::json_rpc;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{debug, error, info, warn};

const DOWNSTREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(99999999);

pub(super) async fn process_incoming_message(
    downstream: Arc<Mutex<Downstream>>,
    incoming: json_rpc::Message,
) -> Result<(), Error<'static>> {
    let is_submit = matches!(
        &incoming,
        json_rpc::Message::StandardRequest(request) if request.method == "mining.submit"
    );
    if is_submit {
        downstream
            .safe_lock(|d| d.reset_submit_diff_count_flag())
            .map_err(|_| Error::TranslatorTaskManagerMutexPoisoned)?;
    }

    Downstream::handle_incoming_sv1(downstream.clone(), incoming).await?;

    if is_submit {
        let should_count_submit = downstream
            .safe_lock(|d| d.take_submit_diff_count_flag())
            .map_err(|_| Error::TranslatorTaskManagerMutexPoisoned)?;

        if should_count_submit {
            Downstream::save_share(downstream)?;
        }
    }

    Ok(())
}

pub async fn start_receive_downstream(
    task_manager: Arc<Mutex<TaskManager>>,
    downstream: Arc<Mutex<Downstream>>,
    mut recv_from_down: mpsc::Receiver<String>,
    connection_id: u32,
) -> Result<(), Error<'static>> {
    let handle = {
        let task_manager = task_manager.clone();
        task::spawn(async move {
            loop {
                let incoming = match tokio::time::timeout(
                    DOWNSTREAM_IDLE_TIMEOUT,
                    recv_from_down.recv(),
                )
                .await
                {
                    Ok(Some(incoming)) => incoming,
                    Ok(None) => break,
                    Err(_) => {
                        warn!(
                            "Downstream {connection_id} idle for more than {} seconds, disconnecting",
                            DOWNSTREAM_IDLE_TIMEOUT.as_secs(),
                        );
                        break;
                    }
                };
                let incoming_len = incoming.len();
                let incoming: Result<json_rpc::Message, _> = serde_json::from_str(&incoming);
                if let Ok(incoming) = incoming {
                    match &incoming {
                        json_rpc::Message::StandardRequest(request) => {
                            info!(
                                "dmnd-client-debug downstream_recv_message connection_id={} method={} id={:?} raw_len={}",
                                connection_id,
                                request.method,
                                request.id,
                                incoming_len
                            );
                        }
                        json_rpc::Message::OkResponse(response)
                        | json_rpc::Message::ErrorResponse(response) => {
                            info!(
                                "dmnd-client-debug downstream_recv_response connection_id={} id={:?} raw_len={}",
                                connection_id,
                                response.id,
                                incoming_len
                            );
                        }
                        _ => {
                            info!(
                                "dmnd-client-debug downstream_recv_other connection_id={} raw_len={}",
                                connection_id, incoming_len
                            );
                        }
                    }
                    if let Err(error) = process_incoming_message(downstream.clone(), incoming).await
                    {
                        error!("Failed to handle incoming sv1 msg: {:?}", error);
                        break;
                    }
                } else {
                    // Message received could not be converted to rpc message
                    error!(
                        "{}",
                        Error::V1Protocol(Box::new(
                            sv1_api::error::Error::InvalidJsonRpcMessageKind
                        ))
                    );
                    // Mark closed eagerly before breaking so deferred bootstrap tasks
                    // that check is_closed() see the disconnect immediately, even if
                    // the post-loop teardown hasn't run yet. (See #241)
                    if let Err(e) = downstream.safe_lock(|d| d.mark_closed()) {
                        error!("Failed to mark downstream {connection_id} closed: {e}");
                    }
                    break;
                }
            }
            if let Err(e) = downstream.safe_lock(|d| d.mark_closed()) {
                error!("Failed to mark downstream {connection_id} closed: {e}");
            }
            let stats_sender = downstream.safe_lock(|d| d.stats_sender.clone()).ok();
            if let Some(stats_sender) = stats_sender {
                if let Err(e) = stats_sender.remove_stats_reliable(connection_id).await {
                    error!("Failed to remove downstream stats {connection_id}: {e}");
                }
            }
            // No message to receive
            debug!(
                "Downstream: Shutting down sv1 downstream reader {}",
                connection_id
            );

            if let Err(e) = Downstream::remove_downstream_hashrate_from_channel(&downstream) {
                error!("Failed to remove downstream hashrate from channel: {}", e)
            };

            let worker_name = downstream
                .safe_lock(|d| d.authorized_names.first().cloned().unwrap_or_default())
                .unwrap_or_else(|e| {
                    error!("Failed to lock downstream: {:?}", e);
                    ProxyState::update_inconsistency(Some(1));
                    "unknown".to_string()
                });

            if !worker_name.is_empty() {
                MonitorAPI::worker_disconnected(connection_id);
            }

            // Apparently there is no way to make the compiler happy without unwrapping here. But
            // is not an issue since:
            // 1. the mutex should never get poisioned and if it does will be very very rare
            // 2. restarting the process after the unwrapping or restarting the all the tasks from
            //    inside the process (that is what we should do here) is almost the same thing
            let send_kill_signal = task_manager
                .safe_lock(|tm| tm.send_kill_signal.clone())
                .unwrap();
            if send_kill_signal.send(connection_id).await.is_err() {
                error!("Proxy can not abort downstreams tasks");
                ProxyState::update_inconsistency(Some(1));
            }
        })
    };
    TaskManager::add_receive_downstream(task_manager, handle.into(), connection_id)
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::stats::StatsSender,
        translator::{
            downstream::{
                downstream::{Downstream, DownstreamDifficultyConfig},
                notify::start_notify,
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
        json_rpc::Message,
        server_to_client::Notify,
        utils::{HexU32Be, MerkleNode, PrevHash},
    };
    use tokio::sync::{broadcast, mpsc::channel};

    const CONNECTION_ID: u32 = 1;
    const MALFORMED_JSON: &str = "{not valid json";

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

    async fn test_receive_fixture() -> (
        Arc<Mutex<Downstream>>,
        Arc<Mutex<TaskManager>>,
        tokio::sync::mpsc::Sender<String>,
        tokio::sync::mpsc::Receiver<String>,
        tokio::sync::mpsc::Receiver<Message>,
        StatsSender,
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
        let stats_sender = StatsSender::new();
        stats_sender
            .setup_stats_reliable(CONNECTION_ID)
            .await
            .unwrap();

        let downstream = Arc::new(Mutex::new(Downstream::new(
            CONNECTION_ID,
            vec![],
            vec![],
            None,
            None,
            tx_sv1_submit,
            tx_outgoing,
            4,
            difficulty_mgmt,
            Arc::new(Mutex::new(upstream_config)),
            stats_sender.clone(),
            first_job("42"),
            tx_update_token,
        )));
        let task_manager = TaskManager::initialize();
        let (tx_incoming, rx_incoming) = channel(8);

        (
            downstream,
            task_manager,
            tx_incoming,
            rx_incoming,
            rx_outgoing,
            stats_sender,
        )
    }

    async fn wait_until_closed(downstream: &Arc<Mutex<Downstream>>) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if downstream.safe_lock(|d| d.is_closed()).unwrap() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("downstream did not close in time");
    }

    #[tokio::test]
    async fn malformed_json_triggers_full_teardown() {
        let (
            downstream,
            task_manager,
            tx_incoming,
            rx_incoming,
            _rx_outgoing,
            stats_sender,
        ) = test_receive_fixture().await;

        let bootstrap_handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let bootstrap_abort = bootstrap_handle.abort_handle();
        TaskManager::add_bootstrap(
            task_manager.clone(),
            bootstrap_handle.into(),
            CONNECTION_ID,
        )
        .await
        .unwrap();

        start_receive_downstream(
            task_manager.clone(),
            downstream.clone(),
            rx_incoming,
            CONNECTION_ID,
        )
        .await
        .unwrap();

        tx_incoming
            .send(MALFORMED_JSON.to_string())
            .await
            .unwrap();
        drop(tx_incoming);

        wait_until_closed(&downstream).await;

        assert!(
            downstream.safe_lock(|d| d.is_closed()).unwrap(),
            "downstream must be marked closed after malformed JSON"
        );

        let stats = stats_sender.collect_stats().await.unwrap();
        assert!(
            !stats.contains_key(&CONNECTION_ID),
            "stats entry must be removed after malformed JSON teardown"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            bootstrap_abort.is_finished(),
            "kill signal must abort registered bootstrap tasks for the connection"
        );
    }

    #[tokio::test]
    async fn malformed_json_races_deferred_bootstrap() {
        let (
            downstream,
            task_manager,
            tx_incoming,
            rx_incoming,
            mut rx_outgoing,
            stats_sender,
        ) = test_receive_fixture().await;

        start_receive_downstream(
            task_manager.clone(),
            downstream.clone(),
            rx_incoming,
            CONNECTION_ID,
        )
        .await
        .unwrap();

        tx_incoming
            .send(MALFORMED_JSON.to_string())
            .await
            .unwrap();
        drop(tx_incoming);

        wait_until_closed(&downstream).await;

        let stats = stats_sender.collect_stats().await.unwrap();
        assert!(
            !stats.contains_key(&CONNECTION_ID),
            "teardown must remove stats before bootstrap can observe a live session"
        );

        let (_tx_notify, rx_notify) = broadcast::channel(8);
        let bootstrap = tokio::spawn({
            let downstream = downstream.clone();
            let task_manager = task_manager.clone();
            async move {
                let should_skip_bootstrap = downstream.safe_lock(|d| d.is_closed()).unwrap();
                if should_skip_bootstrap {
                    return;
                }

                start_notify(
                    task_manager,
                    downstream,
                    rx_notify,
                    "127.0.0.1".to_string(),
                    CONNECTION_ID,
                )
                .await
                .unwrap();
            }
        });
        bootstrap.await.unwrap();

        let stats = stats_sender.collect_stats().await.unwrap();
        assert!(
            !stats.contains_key(&CONNECTION_ID),
            "deferred bootstrap must not register stats for a closed downstream"
        );

        assert!(
            rx_outgoing.try_recv().is_err(),
            "deferred bootstrap must not enqueue mining.notify for a closed downstream"
        );
    }
}
