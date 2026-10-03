use super::{downstream::Downstream, task_manager::TaskManager};
use crate::translator::error::Error;
use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use sv1_api::json_rpc;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{error, info, warn};

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
    let is_closed = downstream
        .safe_lock(|d| d.is_closed())
        .unwrap_or_else(|e| {
            error!("Failed to read downstream closed state before receive registration: {e}");
            true
        });
    info!(
        "Registering downstream receive task connection_id={} is_closed={}",
        connection_id, is_closed
    );
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
                    break;
                }
            }
            Downstream::teardown_session(downstream, task_manager, connection_id).await;
        })
    };
    TaskManager::add_receive_downstream(task_manager, handle.into(), connection_id)
        .await
        .map_err(|e| {
            error!(
                "Failed to register downstream receive task for connection_id={}: {e}",
                connection_id
            );
            e
        })
}
