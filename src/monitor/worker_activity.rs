#[derive(serde::Serialize, Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkerActivityType {
    Connected,
    Disconnected,
}

#[derive(serde::Serialize, Debug, Clone)]
pub struct WorkerActivity {
    user_agent: String,
    worker_name: String,
    activity: WorkerActivityType,
}

impl WorkerActivity {
    pub fn new(user_agent: String, worker_name: String, activity: WorkerActivityType) -> Self {
        WorkerActivity {
            user_agent,
            worker_name,
            activity,
        }
    }

    pub(crate) fn worker_name(&self) -> &str {
        &self.worker_name
    }

    #[cfg(test)]
    pub(crate) fn activity(&self) -> WorkerActivityType {
        self.activity
    }
}
