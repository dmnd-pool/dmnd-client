use std::sync::Arc;

use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures::StreamExt;
use roles_logic_sv2::utils::Mutex;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, warn};

use super::AppState;

#[derive(Debug, Serialize, Clone)]
pub struct NewTemplateNotification {
    pub event: String,
    pub message: String,
    pub template_id: u64,
    pub timestamp: u64,
}
impl NewTemplateNotification {
    pub fn new(event: String, message: String, template_id: u64, timestamp: u64) -> Self {
        Self {
            event,
            message,
            template_id,
            timestamp,
        }
    }
}

// #[derive(Debug, Serialize, Clone)]
// pub struct JobDeclarationResponse {
//     pub success: bool,
//     pub message: String,
//     pub data: Option<JobDeclarationData>,
// }

// impl JobDeclarationResponse {
//     pub fn success(data: Option<JobDeclarationData>) -> Self {
//         Self {
//             success: true,
//             message: "Job declaration successful".to_string(),
//             data,
//         }
//     }
//     pub fn error(message: String) -> Self {
//         Self {
//             success: false,
//             message,
//             data: None,
//         }
//     }
// }

#[derive(Debug, Serialize, Clone)]
pub struct JobDeclarationData {
    pub template_id: Option<u64>,
    pub rejected_tx: Option<Vec<String>>,
    pub channel_id: Option<u32>,
    pub req_id: Option<u32>,
    pub job_id: Option<u32>,
    pub mining_job_token: Option<String>, // hex encoded mining job token
}

impl JobDeclarationData {
    pub fn new() -> Self {
        Self {
            template_id: None,
            rejected_tx: None,
            channel_id: None,
            req_id: None,
            job_id: None,
            mining_job_token: None,
        }
    }
}

// #[derive(Debug, Deserialize)]
// pub struct JobDeclarationRequest {
//     pub template_id: u32,
//     pub txids: Vec<String>,
// }

pub type TemplateNotificationBroadcaster = broadcast::Sender<NewTemplateNotification>;

async fn stream_event_notifications(
    mut socket: WebSocket,
    subscriber: broadcast::Receiver<NewTemplateNotification>,
) {
    let mut stream = BroadcastStream::new(subscriber);

    while let Some(event) = stream.next().await {
        match event {
            Ok(notification) => {
                if let Ok(json) = serde_json::to_string(&notification) {
                    if socket
                        .send(axum::extract::ws::Message::Text(json.into()))
                        .await
                        .is_err()
                    {
                        warn!("WebSocket closed while sending template notification");
                        break;
                    }
                }
            }
            Err(e) => {
                error!("Error receiving template notification: {}", e);
                break;
            }
        }
    }

    warn!("Template WebSocket connection closed");
}

pub async fn ws_event_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        let jd_event_subscriber = state.jd_event_broadcaster.subscribe();

        stream_event_notifications(socket, jd_event_subscriber)
    })
}
