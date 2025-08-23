use std::{collections::HashMap, sync::Arc};

use crate::shared::utils::AbortOnDrop;
use roles_logic_sv2::utils::Mutex;
use tokio::sync::mpsc;
use tracing::warn;

#[allow(dead_code)]
enum Task {
    AcceptConnection(AbortOnDrop),
    ReceiveDownstream(AbortOnDrop),
    SendDownstream(AbortOnDrop),
    Notify(AbortOnDrop),
    Update(AbortOnDrop),
    SharesMonitor(AbortOnDrop),
}

pub struct TaskManager {
    send_task: mpsc::Sender<(Option<u32>, Task)>,
    abort: Option<AbortOnDrop>,
    tasks: Arc<Mutex<HashMap<Option<u32>, Vec<AbortOnDrop>>>>, // Track tasks by connection_id
}

impl TaskManager {
    pub fn initialize() -> Arc<Mutex<Self>> {
        type TaskMessage = (Option<u32>, Task);

        let (sender, mut receiver): (mpsc::Sender<TaskMessage>, mpsc::Receiver<TaskMessage>) =
            mpsc::channel(10);

        let tasks = Arc::new(Mutex::new(HashMap::new()));
        let task_clone = tasks.clone();
        let handle = tokio::task::spawn(async move {
            while let Some((connection_id, task)) = receiver.recv().await {
                if task_clone
                    .safe_lock(|tasks| {
                        let tasks_list: &mut Vec<AbortOnDrop> =
                            tasks.entry(connection_id).or_default();
                        tasks_list.push(task.into());
                    })
                    .is_err()
                {
                    tracing::error!("TasKManager Mutex Poisoned")
                };
            }
            warn!("Translator downstream task manager stopped, keep alive tasks");
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1000)).await;
            }
        });
        Arc::new(Mutex::new(Self {
            send_task: sender,
            abort: Some(handle.into()),
            tasks,
        }))
    }

    pub fn get_aborter(&mut self) -> Option<AbortOnDrop> {
        self.abort.take()
    }

    pub async fn add_receive_downstream(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
        connection_id: u32,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send((Some(connection_id), Task::ReceiveDownstream(abortable)))
            .await
            .map_err(|_| ())
    }
    pub async fn add_update(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
        connection_id: u32,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send((Some(connection_id), Task::Update(abortable)))
            .await
            .map_err(|_| ())
    }
    pub async fn add_notify(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
        connection_id: u32,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send((Some(connection_id), Task::Notify(abortable)))
            .await
            .map_err(|_| ())
    }
    pub async fn add_send_downstream(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
        connection_id: u32,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send((Some(connection_id), Task::SendDownstream(abortable)))
            .await
            .map_err(|_| ())
    }
    pub async fn add_accept_connection(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send((None, Task::AcceptConnection(abortable)))
            .await
            .map_err(|_| ())
    }

    pub async fn add_shares_monitor(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send((None, Task::SharesMonitor(abortable)))
            .await
            .map_err(|_| ())
    }

    /// Kills all tasks for a given `connection_id` and removes them from TaskManager.
    pub fn abort_tasks_for_connection_id(&mut self, connection_id: u32) {
        if self
            .tasks
            .safe_lock(|tasks| {
                if let Some(handles) = tasks.remove(&Some(connection_id)) {
                    for handle in handles {
                        drop(handle);
                    }
                }
            })
            .is_err()
        {
            tracing::error!("TasKManager Mutex Poisoned")
        };
        tracing::info!(
            "Aborted all tasks for downstream connection ID {}",
            connection_id
        );
    }
}

/// Converts a `Task` into its `AbortHandle` for task management.
impl From<Task> for AbortOnDrop {
    fn from(task: Task) -> Self {
        match task {
            Task::AcceptConnection(handle) => handle,
            Task::ReceiveDownstream(handle) => handle,
            Task::SendDownstream(handle) => handle,
            Task::Notify(handle) => handle,
            Task::Update(handle) => handle,
            Task::SharesMonitor(handle) => handle,
        }
    }
}
