use super::task_manager::TaskManager;
use crate::translator::error::Error;
use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use sv1_api::json_rpc;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{error, warn};

pub async fn start_send_to_downstream(
    task_manager: Arc<Mutex<TaskManager>>,
    mut receiver_outgoing: mpsc::Receiver<json_rpc::Message>,
    send_to_down: mpsc::Sender<String>,
    connection_id: u32,
    host: String,
) -> Result<(), Error<'static>> {
    let task_manager_clone = task_manager.clone();
    let handle = task::spawn(async move {
        while let Some(res) = receiver_outgoing.recv().await {
            let to_send = match serde_json::to_string(&res) {
                Ok(string) => format!("{string}\n"),
                Err(e) => {
                    error!("Failed to serialize msg {e:?}");
                    break;
                }
            };
            if send_to_down.send(to_send).await.is_err() {
                warn!("Downstream {} dropped", host);
                break;
            }
        }
            warn!(
                "Downstream: Shutting down sv1 downstream writer: {}",
                connection_id
            );
            if let Some(kill_signal) = task_manager_clone.safe_lock(|tm| tm.send_kill_signal.clone()).ok() {
                let _ = kill_signal.send(connection_id).await;
            }
        });
    TaskManager::add_send_downstream(task_manager, handle.into(), connection_id)
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)
}
