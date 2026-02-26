use std::sync::Arc;

use crate::shared::utils::AbortOnDrop;
use roles_logic_sv2::utils::Mutex;
use tokio::sync::mpsc;
use tracing::debug;

#[derive(Debug)]
#[allow(dead_code)]
enum Task {
    DiffManagment(AbortOnDrop),
    MainLoop(AbortOnDrop),
    HandleSubmit(AbortOnDrop),
    HandleTokenUpdate(AbortOnDrop),
}

pub struct TaskManager {
    send_task: mpsc::Sender<Task>,
    abort: Option<AbortOnDrop>,
}

impl TaskManager {
    pub fn initialize() -> Arc<Mutex<Self>> {
        let (sender, mut receiver) = mpsc::channel(10);
        let handle = tokio::task::spawn(async move {
            let mut tasks = vec![];
            while let Some(task) = receiver.recv().await {
                tasks.push(task);
            }
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1000)).await;
            }
        });
        debug!("Translator Task Manager initialized");
        Arc::new(Mutex::new(Self {
            send_task: sender,
            abort: Some(handle.into()),
        }))
    }

    pub fn get_aborter(&mut self) -> Option<AbortOnDrop> {
        self.abort.take()
    }
    pub async fn add_diff_managment(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send(Task::DiffManagment(abortable))
            .await
            .map_err(|_| ())
    }
    pub async fn add_main_loop(self_: Arc<Mutex<Self>>, abortable: AbortOnDrop) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send(Task::MainLoop(abortable))
            .await
            .map_err(|_| ())
    }
    pub async fn add_handle_submit(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send(Task::HandleSubmit(abortable))
            .await
            .map_err(|_| ())
    }

    pub async fn add_handle_token_update(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send(Task::HandleTokenUpdate(abortable))
            .await
            .map_err(|_| ())
    }
}
