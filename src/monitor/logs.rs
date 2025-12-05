#![allow(dead_code)]
use crate::monitor::{proxy_log_server_endpoint, MonitorAPI};
use serde::Serialize;
use std::sync::Arc;
use tracing::error;

/// A custom tracing Layer that sends error logs to the server.
///
/// This helps centralize error reporting by automatically forwarding error logs
#[derive(Clone)]
pub struct SendLogLayer {
    api: Arc<MonitorAPI>,
    content: String,
}

impl SendLogLayer {
    pub fn new() -> Self {
        let server_url = proxy_log_server_endpoint();
        let api = Arc::new(MonitorAPI::new(server_url));
        SendLogLayer {
            api,
            content: String::new(),
        }
    }
}

/// Implements the `Visit` trait to collect log fields into a string.
impl tracing::field::Visit for SendLogLayer {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.content
            .push_str(&format!("{}: {:?}, ", field.name(), value));
    }
}

impl<S> tracing_subscriber::Layer<S> for SendLogLayer
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    /// Called when an event is recorded.
    /// If the event is an error, we sends it to the server.
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() == tracing::Level::ERROR {
            let mut visitor = self.clone();
            event.record(&mut visitor);
            let content = format!(
                "{}: {}",
                event.metadata().target(),
                visitor.content.trim_end_matches(", ")
            );
            let payload = ProxyLog::new(Severity::Error, content);
            let api = Arc::clone(&self.api);
            let payload = Arc::new(payload);
            // Spawn a new task to send the log asynchronously
            tokio::spawn(async move {
                if let Err(e) = api.send_log(payload.as_ref().clone()).await {
                    error!("Failed to send log to API: {}", e);
                }
            });
        }
    }
}

/// Represent the log to be sent to the API server
#[derive(Serialize, Debug, Clone)]
pub struct ProxyLog {
    severity: Severity,
    content: String,
}
impl ProxyLog {
    fn new(severity: Severity, content: String) -> Self {
        ProxyLog { severity, content }
    }
}

#[derive(Debug, Clone, Serialize)]
enum Severity {
    _Info,
    _Warning,
    Error,
}
