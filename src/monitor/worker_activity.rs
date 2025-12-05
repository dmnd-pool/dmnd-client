use std::sync::Arc;

use crate::monitor::{worker_activity_server_endpoint, MonitorAPI};
use lazy_static::lazy_static;
use roles_logic_sv2::utils::Mutex;
use tracing::{debug, error};
lazy_static! {
    static ref ACTIVE_WORKERS: Arc<Mutex<Vec<WorkerActivity>>> = Arc::new(Mutex::new(Vec::new())); // To keep track of all active workers, so we can disconnect them on shutdown

}
#[derive(serde::Serialize, Debug, PartialEq, Eq, Clone)]
pub enum WorkerActivityType {
    Connected,
    Disconnected,
}

#[derive(serde::Serialize, Debug, PartialEq, Eq, Clone)]
pub struct WorkerActivity {
    user_agent: String,
    worker_name: String,
    activity: WorkerActivityType,
}

impl WorkerActivity {
    pub fn new(user_agent: String, worker_name: String, activity: WorkerActivityType) -> Self {
        if ACTIVE_WORKERS
            .safe_lock(|workers| match activity {
                WorkerActivityType::Connected => {
                    workers.push(WorkerActivity {
                        user_agent: user_agent.clone(),
                        worker_name: worker_name.clone(),
                        activity: activity.clone(),
                    });
                }
                WorkerActivityType::Disconnected => {
                    workers
                        .retain(|w| !(w.user_agent == user_agent && w.worker_name == worker_name));
                }
            })
            .is_err()
        {
            error!("Failed to acquire lock on ACTIVE_WORKERS");
            std::process::exit(1)
        }

        WorkerActivity {
            user_agent,
            worker_name,
            activity,
        }
    }

    pub fn monitor_api(&self) -> MonitorAPI {
        MonitorAPI::new(worker_activity_server_endpoint())
    }

    pub async fn disconnect_all_active_workers() -> Result<(), crate::shared::error::Error> {
        let active_workers = ACTIVE_WORKERS.safe_lock(|workers| workers.clone()).expect(
            "Failed to acquire lock on ACTIVE_WORKERS when disconnecting all active workers",
        );

        for worker in &active_workers {
            let worker_activity = WorkerActivity::new(
                worker.user_agent.clone(),
                worker.worker_name.clone(),
                WorkerActivityType::Disconnected,
            );

            worker_activity
                .monitor_api()
                .send_worker_activity(worker_activity)
                .await?;
            debug!(
                "Disconnected worker: {} ({})",
                worker.worker_name, worker.user_agent
            );
        }
        Ok(())
    }
}
