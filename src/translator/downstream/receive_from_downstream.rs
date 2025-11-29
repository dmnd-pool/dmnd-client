use super::{downstream::Downstream, task_manager::TaskManager};
use crate::{
    monitor::worker_activity::{WorkerActivity, WorkerActivityType},
    proxy_state::ProxyState,
    translator::error::Error,
};
use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use sv1_api::{client_to_server::Submit, json_rpc};
use tokio::sync::mpsc;
use tokio::task;
use tracing::{error, warn};

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
                    // if message is Submit Shares update difficulty management
                    if let sv1_api::Message::StandardRequest(standard_req) = incoming.clone() {
                        if let Ok(Submit { .. }) = standard_req.try_into() {
                            if let Err(e) = Downstream::save_share(downstream.clone()) {
                                error!("{}", e);
                                break;
                            }
                        }
                    }

                    if let Err(error) =
                        Downstream::handle_incoming_sv1(downstream.clone(), incoming).await
                    {
                        error!("Failed to handle incoming sv1 msg: {:?}", error);
                        break;
                    };
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
            if let Ok(stats_sender) = downstream.safe_lock(|d| d.stats_sender.clone()) {
                stats_sender.remove_stats(connection_id);
            }
            // No message to receive
            warn!(
                "Downstream: Shutting down sv1 downstream reader {}",
                connection_id
            );

            if let Err(e) = Downstream::remove_downstream_hashrate_from_channel(&downstream) {
                error!("Failed to remove downstream hashrate from channel: {}", e)
            };

            let (worker_name, user_agent) = downstream
                .safe_lock(|d| {
                    (
                        d.authorized_names.first().cloned().unwrap_or_default(),
                        d.user_agent.borrow().clone(),
                    )
                })
                .unwrap_or_else(|e| {
                    error!("Failed to lock downstream: {:?}", e);
                    ProxyState::update_inconsistency(Some(1));
                    ("unknown".to_string(), "unknown".to_string())
                });

            let worker_activity =
                WorkerActivity::new(user_agent, worker_name, WorkerActivityType::Disconnected);

            match worker_activity.monitor_api().await {
                Ok(api) => {
                    if let Err(e) = api.send_worker_activity(worker_activity).await {
                        warn!(
                            "Failed to send worker activity {:?} to monitor: {:?}",
                            WorkerActivityType::Disconnected,
                            e
                        );
                    }
                }
                Err(e) => {
                    warn!("Failed to get monitor API for worker activity logging: {:?}", e);
                }
            };
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
