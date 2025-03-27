use super::{downstream::Downstream, task_manager::TaskManager};
use crate::{
    proxy_state::{DownstreamType, ProxyState},
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
    let handle = task::spawn(async move {
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
                    ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                };
            } else {
                // Message received could not be converted to rpc message
                error!(
                    "{}",
                    Error::V1Protocol(sv1_api::error::Error::InvalidJsonRpcMessageKind,)
                );
                return;
            }
        }
        crate::api::stats::DownstreamStatsRegistry.remove_downstream_stats(&connection_id);
        // No message to receive
        warn!(
            "Downstream: Shutting down sv1 downstream reader {}",
            connection_id
        );
    });
    TaskManager::add_receive_downstream(task_manager, handle.into())
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)
}
