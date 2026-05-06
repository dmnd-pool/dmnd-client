use crate::translator::error::Error;

use super::Upstream;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct UpstreamDifficultyConfig {
    pub channel_diff_update_interval: u32,
    pub channel_nominal_hashrate: f32,
    update_revision_tx: watch::Sender<u64>,
}

impl UpstreamDifficultyConfig {
    pub fn new(
        channel_diff_update_interval: u32,
        channel_nominal_hashrate: f32,
    ) -> (Self, watch::Receiver<u64>) {
        let (update_revision_tx, update_revision_rx) = watch::channel(0_u64);
        (
            Self {
                channel_diff_update_interval,
                channel_nominal_hashrate,
                update_revision_tx,
            },
            update_revision_rx,
        )
    }

    pub fn request_immediate_update(&self) {
        let next_revision = (*self.update_revision_tx.borrow()).wrapping_add(1);
        let _ = self.update_revision_tx.send(next_revision);
    }

    #[cfg(test)]
    pub fn subscribe_updates(&self) -> watch::Receiver<u64> {
        self.update_revision_tx.subscribe()
    }
}

use super::super::error::ProxyResult;
use binary_sv2::u256_from_int;
use roles_logic_sv2::{mining_sv2::UpdateChannel, parsers::Mining, utils::Mutex};
use std::{sync::Arc, time::Duration};
use tracing::error;

impl Upstream {
    /// Emit an `UpdateChannel` using the current aggregate nominal hashrate.
    pub(super) async fn try_update_hashrate(self_: Arc<Mutex<Self>>) -> ProxyResult<'static, ()> {
        let (channel_id_option, diff_mgmt, tx_message) = self_
            .safe_lock(|u| (u.channel_id, u.difficulty_config.clone(), u.sender.clone()))
            .map_err(|_e| Error::TranslatorDiffConfigMutexPoisoned)?;
        let Some(channel_id) = channel_id_option else {
            return Ok(());
        };
        let new_hashrate = diff_mgmt
            .safe_lock(|d| d.channel_nominal_hashrate)
            .map_err(|_| Error::TranslatorDiffConfigMutexPoisoned)?;
        let update_channel = UpdateChannel {
            channel_id,
            nominal_hash_rate: new_hashrate,
            maximum_target: u256_from_int(u64::MAX),
        };
        let message = Mining::UpdateChannel(update_channel);

        if tx_message.send(message).await.is_err() {
            error!("Failed to send message");
            return Err(Error::AsyncChannelError);
        }
        Ok(())
    }

    pub(super) async fn run_diff_management(
        self_: Arc<Mutex<Self>>,
        mut update_rx: watch::Receiver<u64>,
    ) {
        loop {
            let timeout = match self_.safe_lock(|u| {
                u.difficulty_config
                    .safe_lock(|d| d.channel_diff_update_interval)
                    .map_err(|_| Error::TranslatorDiffConfigMutexPoisoned)
            }) {
                Ok(Ok(timeout)) => timeout,
                Ok(Err(e)) => {
                    error!("Failed to read upstream diff interval: {e:?}");
                    return;
                }
                Err(e) => {
                    error!("Failed to read upstream diff interval: {e:?}");
                    return;
                }
            };

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(timeout as u64)) => {}
                changed = update_rx.changed() => {
                    if changed.is_err() {
                        error!("Upstream diff update channel closed");
                        return;
                    }
                }
            }

            if let Err(e) = Self::try_update_hashrate(self_.clone()).await {
                error!("Failed to update hashrate: {e:?}");
                return;
            }
        }
    }
}
