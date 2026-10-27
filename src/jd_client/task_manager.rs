use std::sync::Arc;

use crate::shared::utils::AbortOnDrop;
use roles_logic_sv2::utils::Mutex;
use tokio::sync::mpsc;
use tracing::debug;

use super::mining_upstream::Upstream;

#[derive(Debug)]
struct MiningSession(Arc<Mutex<Upstream>>);

impl Drop for MiningSession {
    fn drop(&mut self) {
        // Break the links from upstream to downstream and back through JD. Other tasks have
        // already received their abort requests, and the returned node drops outside the lock.
        if let Ok(Some(downstream)) = self.0.safe_lock(|upstream| upstream.downstream.take()) {
            let _ = downstream.safe_lock(|downstream| downstream.abort_pending_tasks());
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
enum Task {
    MiningSession(MiningSession),
    JobDeclarator(AbortOnDrop),
    TemplateReceiver(AbortOnDrop),
    MiningUpstream(AbortOnDrop),
    MiningDowntream(AbortOnDrop),
}

#[derive(Debug)]
pub struct TaskManager {
    send_task: mpsc::Sender<Task>,
    abort: Option<AbortOnDrop>,
}

impl TaskManager {
    #[allow(unused_variables)]
    pub fn initialize() -> Arc<Mutex<Self>> {
        let (sender, mut receiver) = mpsc::channel(10);
        let handle = tokio::task::spawn(async move {
            let mut _session = None;
            let mut tasks = vec![];
            while let Some(task) = receiver.recv().await {
                match task {
                    Task::MiningSession(session) => _session = Some(session),
                    task => tasks.push(task),
                }
            }
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1000)).await;
            }
        });
        debug!("Share Accounter Task Manager initialized");
        Arc::new(Mutex::new(Self {
            send_task: sender,
            abort: Some(handle.into()),
        }))
    }

    pub fn get_aborter(&mut self) -> Option<AbortOnDrop> {
        self.abort.take()
    }

    pub async fn add_mining_session(
        self_: Arc<Mutex<Self>>,
        upstream: Arc<Mutex<Upstream>>,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send(Task::MiningSession(MiningSession(upstream)))
            .await
            .map_err(|_| ())
    }

    pub async fn add_job_declarator_task(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send(Task::JobDeclarator(abortable))
            .await
            .map_err(|_| ())
    }
    pub async fn add_template_receiver_task(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send(Task::TemplateReceiver(abortable))
            .await
            .map_err(|_| ())
    }
    pub async fn add_mining_upstream_task(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send(Task::MiningUpstream(abortable))
            .await
            .map_err(|_| ())
    }
    pub async fn add_mining_downtream_task(
        self_: Arc<Mutex<Self>>,
        abortable: AbortOnDrop,
    ) -> Result<(), ()> {
        let send_task = self_.safe_lock(|s| s.send_task.clone()).unwrap();
        send_task
            .send(Task::MiningDowntream(abortable))
            .await
            .map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jd_client::mining_downstream::DownstreamMiningNode;

    #[tokio::test]
    async fn aborting_jd_session_releases_linked_mining_state() {
        let manager = TaskManager::initialize();
        let abortable = manager.safe_lock(|manager| manager.get_aborter()).unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let upstream = Upstream::new(8, sender).await.unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let (solutions, _receiver) = mpsc::channel(1);
        let downstream = Arc::new(Mutex::new(DownstreamMiningNode::new(
            sender,
            Some(upstream.clone()),
            solutions,
            false,
            vec![],
            None,
        )));
        upstream
            .safe_lock(|state| state.downstream = Some(downstream.clone()))
            .unwrap();
        let old_upstream = Arc::downgrade(&upstream);
        let old_downstream = Arc::downgrade(&downstream);
        TaskManager::add_mining_session(manager.clone(), upstream.clone())
            .await
            .unwrap();
        drop(upstream);
        drop(downstream);
        drop(abortable);
        drop(manager);

        let released = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while old_upstream.strong_count() != 0 || old_downstream.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            released.is_ok(),
            "old mining state survives session shutdown"
        );
    }
}
