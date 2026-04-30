use super::{downstream::Downstream, task_manager::TaskManager};
use crate::{
    monitor::worker_activity::{WorkerActivity, WorkerActivityType},
    proxy_state::ProxyState,
    translator::error::Error,
};
use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use sv1_api::json_rpc;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{debug, error};

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
            while let Some(incoming) = recv_from_down.recv().await {
                let incoming: Result<json_rpc::Message, _> = serde_json::from_str(&incoming);
                if let Ok(incoming) = incoming {
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
                    return;
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

            let (worker_name, user_agent, token_handle) = downstream
                .safe_lock(|d| {
                    (
                        d.authorized_names.first().cloned().unwrap_or_default(),
                        d.user_agent.borrow().clone(),
                        d.token.clone(),
                    )
                })
                .unwrap_or_else(|e| {
                    error!("Failed to lock downstream: {:?}", e);
                    ProxyState::update_inconsistency(Some(1));
                    (
                        "unknown".to_string(),
                        "unknown".to_string(),
                        Arc::new(Mutex::new(String::new())),
                    )
                });

            let token = token_handle.safe_lock(|t| t.clone()).unwrap_or_else(|e| {
                error!("Failed to lock token: {:?}", e);
                ProxyState::update_inconsistency(Some(1));
                String::new()
            });

            if !worker_name.is_empty() {
                let worker_activity =
                    WorkerActivity::new(user_agent, worker_name, WorkerActivityType::Disconnected);

                worker_activity
                    .monitor_api()
                    .send_worker_activity(worker_activity, &token)
                    .await
                    .unwrap_or_else(|e| {
                        error!("Failed to send worker activity: {}", e);
                    });
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
