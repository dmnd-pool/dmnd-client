use std::{collections::HashMap, sync::Arc};

use crate::{proxy_state::ProxyState, shared::utils::AbortOnDrop};
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

type TaskMessage = (Option<u32>, Task);

pub struct TaskManager {
    send_task: mpsc::Sender<TaskMessage>,
    abort: Option<AbortOnDrop>,
    pub send_kill_signal: mpsc::Sender<u32>,
}

impl TaskManager {
    pub fn initialize() -> Arc<Mutex<Self>> {
        let (sender, mut receiver): (mpsc::Sender<TaskMessage>, mpsc::Receiver<TaskMessage>) =
            mpsc::channel(10);
        let (send_kill_signal, mut receiver_kill_signal) = mpsc::channel(10);

        let tasks = Arc::new(Mutex::new(HashMap::new()));
        let task_clone = tasks.clone();
        let handle = tokio::task::spawn(async move {
            while let Some((connection_id, task)) = receiver.recv().await {
                // The tasks map is used to save task related to downstream managment, some of them
                // are "global" in the sense that live for all the life of the transalator (like
                // the task that create new downstreams when a downstream connect) others are
                // specific to a downstream like the one that receive messages from it. Specific
                // task have an id that is the connnection id, "global" ones do not have one; for
                // that TaskMessage is an (Option<u32>, Task) where u32 is the connection id.
                // "Global" tasks are saved in the map under the None key.
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
        let kill_tasks = tokio::task::spawn(async move {
            while let Some(connection_id) = receiver_kill_signal.recv().await {
                if tasks
                    .safe_lock(|tasks| {
                        if let Some(handles) = tasks.remove(&Some(connection_id)) {
                            for handle in handles {
                                drop(handle);
                            }
                        }
                    })
                    .is_err()
                {
                    tracing::error!("TasKManager Mutex Poisoned");
                    ProxyState::update_inconsistency(Some(1));
                };
                tracing::info!(
                    "Aborted all tasks for downstream connection ID {}",
                    connection_id
                );
            }
        });
        let mut aborter: AbortOnDrop = handle.into();
        aborter.add_task(kill_tasks);
        Arc::new(Mutex::new(Self {
            send_task: sender,
            abort: Some(aborter),
            send_kill_signal,
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
        connection_id: u32,
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send((Some(connection_id), Task::SharesMonitor(abortable)))
            .await
            .map_err(|_| ())
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
