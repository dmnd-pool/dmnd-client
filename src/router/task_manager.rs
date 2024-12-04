use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::error;

use crate::shared::utils::AbortOnDrop;

#[derive(Debug, Clone, Copy)]
pub enum ProxyState {
    Pool(PoolState),
}

#[derive(Debug, Clone, Copy)]
pub enum PoolState {
    Up,
    Down,
}

#[derive(Debug)]
pub enum Task {
    StateWatcherTask(AbortOnDrop),
}

pub struct TaskManager {
    pool_state_sender: watch::Sender<ProxyState>, // Sends pool state updates to listeners.
    pool_state_receiver: watch::Receiver<ProxyState>, // Receives pool state updates.
    send_task: mpsc::Sender<Task>,                // Sends tasks to task manager
    abort: Option<AbortOnDrop>,                   // Handle to manage task abortion.
}

impl TaskManager {
    /// Initailizes new Task Manger
    /// It manages tasks and keeps track of the pool's state.
    pub fn initialize() -> Arc<Mutex<Self>> {
        let (pool_state_sender, pool_state_receiver) =
            watch::channel(ProxyState::Pool(PoolState::Down));

        let (sender, mut receiver) = mpsc::channel(10);

        // Spawn a task to handle incoming tasks.
        let handle = tokio::spawn(async move {
            let mut tasks = vec![];
            while let Some(task) = receiver.recv().await {
                let Task::StateWatcherTask(abortable) = task;
                tasks.push(abortable);
            }
            tracing::warn!("Router task manager stopped, keeping tasks alive");
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1000)).await
            }
        });

        Arc::new(Mutex::new(Self {
            pool_state_sender,
            pool_state_receiver,
            send_task: sender,
            abort: Some(handle.into()),
        }))
    }

    /// Updates the pool state and notifies the listeners.
    pub fn update_pool_state(&self, new_state: PoolState) {
        if self
            .pool_state_sender
            .send(ProxyState::Pool(new_state))
            .is_err()
        {
            error!("Failed to send Pool state")
        }
    }

    /// Retrieves the current pool state.
    pub fn get_pool_state(&self) -> PoolState {
        match *self.pool_state_receiver.borrow() {
            ProxyState::Pool(state) => state,
        }
    }

    /// Adds a new task to the Task Manager.
    pub async fn add_task(self_: Arc<Mutex<Self>>, task: AbortOnDrop) -> Result<(), String> {
        let sender = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        sender
            .send(Task::StateWatcherTask(task))
            .await
            .map_err(|e| e.to_string())
    }

    /// Gets the abort handle for the Task Manager,
    pub fn get_aborter(&mut self) -> Option<AbortOnDrop> {
        self.abort.take()
    }

    // Watches for pool state changes and updates the global state
    pub async fn watch_pool_state(self_: Arc<Mutex<Self>>) {
        // Retrievs the pool_state_receiver
        let mut receiver = match self_.safe_lock(|s| s.pool_state_receiver.clone()) {
            Ok(receiver) => receiver,
            Err(e) => {
                error!("Failed to retrive receiver {:?}", e);
                return;
            }
        };

        // Spawn a task to monitor changes in the pool state.
        let task = tokio::spawn(async move {
            while receiver.changed().await.is_ok() {
                let state = *receiver.borrow();
                tracing::debug!("Current Proxy State: {:?}", state);

                // Updates Global Pool State
                match state {
                    ProxyState::Pool(PoolState::Down) => tracing::debug!("Pool is Down..."),
                    ProxyState::Pool(PoolState::Up) => tracing::debug!("Pool is Up!"),
                }
            }
        });

        if TaskManager::add_task(self_.clone(), task.into())
            .await
            .is_err()
        {
            error!("Error adding task");
        }
    }
}
