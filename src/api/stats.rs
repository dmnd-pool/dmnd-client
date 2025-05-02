use serde::Serialize;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

#[derive(Debug)]
enum StatsCommand {
    SetupStats(u32),
    UpdateHashrate(u32, f32),
    UpdateDiff(u32, f32),
    UpdateAcceptedShares(u32),
    UpdateRejectedShares(u32),
    UpdateDeviceName(u32, String),
    RemoveStats(u32),
    GetStats(oneshot::Sender<HashMap<u32, DownstreamConnectionStats>>),
}

#[derive(Debug, Clone, Serialize)]
pub struct DownstreamConnectionStats {
    pub device_name: Option<String>,
    pub hashrate: f32,
    pub accepted_shares: u64,
    pub rejected_shares: u64,
    pub current_difficulty: f32,
}

impl DownstreamConnectionStats {
    fn new() -> Self {
        Self {
            device_name: None,
            hashrate: 0.0,
            accepted_shares: 0,
            rejected_shares: 0,
            current_difficulty: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StatsSender {
    sender: mpsc::Sender<StatsCommand>,
}

impl StatsSender {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(StatsManager::new(rx).run());
        Self { sender: tx }
    }

    fn send(&self, command: StatsCommand) {
        if let Err(e) = self.sender.try_send(command) {
            warn!("Failed to send command: {:?}", e);
        }
    }

    pub fn setup_stats(&self, connection_id: u32) {
        self.send(StatsCommand::SetupStats(connection_id));
    }

    pub fn update_hashrate(&self, connection_id: u32, hashrate: f32) {
        self.send(StatsCommand::UpdateHashrate(connection_id, hashrate));
    }

    pub fn update_diff(&self, connection_id: u32, diff: f32) {
        self.send(StatsCommand::UpdateDiff(connection_id, diff));
    }

    pub fn update_accepted_shares(&self, connection_id: u32) {
        self.send(StatsCommand::UpdateAcceptedShares(connection_id));
    }

    pub fn update_rejected_shares(&self, connection_id: u32) {
        self.send(StatsCommand::UpdateRejectedShares(connection_id));
    }

    pub fn update_device_name(&self, connection_id: u32, name: String) {
        self.send(StatsCommand::UpdateDeviceName(connection_id, name));
    }

    pub fn remove_stats(&self, connection_id: u32) {
        self.send(StatsCommand::RemoveStats(connection_id));
    }

    pub async fn collect_stats(&self) -> Result<HashMap<u32, DownstreamConnectionStats>, String> {
        let (tx, rx) = oneshot::channel();
        self.send(StatsCommand::GetStats(tx));
        match rx.await {
            Ok(stats) => Ok(stats),
            Err(e) => Err(e.to_string()),
        }
    }
}

struct StatsManager {
    stats: HashMap<u32, DownstreamConnectionStats>,
    receiver: mpsc::Receiver<StatsCommand>,
}

impl StatsManager {
    fn new(receiver: mpsc::Receiver<StatsCommand>) -> Self {
        Self {
            stats: HashMap::new(),
            receiver,
        }
    }

    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                StatsCommand::SetupStats(id) => {
                    self.stats.insert(id, DownstreamConnectionStats::new());
                }
                StatsCommand::UpdateHashrate(id, hashrate) => {
                    if let Some(stats) = self.stats.get_mut(&id) {
                        stats.hashrate = hashrate
                    }
                }
                StatsCommand::UpdateDiff(id, diff) => {
                    if let Some(stats) = self.stats.get_mut(&id) {
                        stats.current_difficulty = diff
                    }
                }
                StatsCommand::UpdateAcceptedShares(id) => {
                    if let Some(stats) = self.stats.get_mut(&id) {
                        stats.accepted_shares += 1
                    }
                }
                StatsCommand::UpdateRejectedShares(id) => {
                    if let Some(stats) = self.stats.get_mut(&id) {
                        stats.rejected_shares += 1
                    }
                }
                StatsCommand::UpdateDeviceName(id, name) => {
                    if let Some(stats) = self.stats.get_mut(&id) {
                        stats.device_name = Some(name)
                    }
                }
                StatsCommand::RemoveStats(id) => {
                    self.stats.remove(&id);
                }
                StatsCommand::GetStats(tx) => {
                    let _ = tx.send(self.stats.clone());
                }
            }
        }
    }
}
