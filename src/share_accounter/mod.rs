mod errors;
mod task_manager;

use errors::Error;
use std::sync::Arc;
use tracing::error;

use dashmap::DashMap;
use demand_share_accounting_ext::*;
use parser::{PoolExtMessages, ShareAccountingMessages};
use roles_logic_sv2::{mining_sv2::SubmitSharesSuccess, parsers::Mining};
use task_manager::TaskManager;

use crate::{
    proxy_state::{ProxyState, ShareAccounterState},
    shared::utils::AbortOnDrop,
    PoolState,
};

pub async fn start(
    receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    sender: tokio::sync::mpsc::Sender<Mining<'static>>,
    up_receiver: tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>,
    up_sender: tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
) -> Result<AbortOnDrop, Error> {
    let task_manager = TaskManager::initialize();
    let shares_sent_up = Arc::new(DashMap::with_capacity(100));
    let abortable = task_manager
        .safe_lock(|t| t.get_aborter())
        .map_err(|_| Error::ShareAccounterTaskManagerMutexCorrupted)?
        .ok_or(Error::ShareAccounterTaskManagerError)?;

    let relay_up_task = relay_up(receiver, up_sender, shares_sent_up.clone());
    TaskManager::add_relay_up(task_manager.clone(), relay_up_task)
        .await
        .map_err(|_| Error::ShareAccounterTaskManagerError)?;

    let relay_down_task = relay_down(up_receiver, sender, shares_sent_up.clone());
    TaskManager::add_relay_down(task_manager.clone(), relay_down_task)
        .await
        .map_err(|_| Error::ShareAccounterTaskManagerError)?;
    Ok(abortable)
}

struct ShareSentUp {
    channel_id: u32,
    sequence_number: u32,
}

fn relay_up(
    mut receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    up_sender: tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
    shares_sent_up: Arc<DashMap<u32, ShareSentUp>>,
) -> AbortOnDrop {
    let task = tokio::spawn(async move {
        while let Some(msg) = receiver.recv().await {
            if let Mining::SubmitSharesExtended(m) = &msg {
                shares_sent_up.insert(
                    m.job_id,
                    ShareSentUp {
                        channel_id: m.channel_id,
                        sequence_number: m.sequence_number,
                    },
                );
            };
            let msg = PoolExtMessages::Mining(msg);
            if up_sender.send(msg).await.is_err() {
                break;
            }
        }
    });
    task.into()
}

fn msg_is_valid(msg: &PoolExtMessages) -> bool {
    match msg {
        PoolExtMessages::Mining(_) | PoolExtMessages::ShareAccountingMessages(_) => true,
        _ => false,
    }
}

fn relay_down(
    mut up_receiver: tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>,
    sender: tokio::sync::mpsc::Sender<Mining<'static>>,
    shares_sent_up: Arc<DashMap<u32, ShareSentUp>>,
) -> AbortOnDrop {
    let task = tokio::spawn(async move {
        while let Some(msg) = up_receiver.recv().await {
            if let Some(restart) =
                ProxyState::update_pool_state(PoolState::Down, !msg_is_valid(&msg))
            {
                error!("Pool send unexpected message on mining connection");
                restart();
                break;
            } else {
                match msg {
                    PoolExtMessages::ShareAccountingMessages(msg) => {
                        if let ShareAccountingMessages::ShareOk(msg) = msg {
                            let job_id_bytes = msg.ref_job_id.to_le_bytes();
                            let job_id = u32::from_le_bytes(job_id_bytes[4..8].try_into().expect("Internal error: job_id_bytes[4..8] can always be convertible into a u32"));
                            let shares = shares_sent_up.remove(&job_id);
                            if let Some(restart) =
                                ProxyState::update_pool_state(PoolState::Down, shares.is_none())
                            {
                                error!("Pool sent invalid share success");
                                restart();
                                return;
                            } else {
                                let share_sent_up = shares.unwrap().1;
                                let success = Mining::SubmitSharesSuccess(SubmitSharesSuccess {
                                    channel_id: share_sent_up.channel_id,
                                    last_sequence_number: share_sent_up.sequence_number,
                                    new_submits_accepted_count: 1,
                                    new_shares_sum: 1,
                                });
                                if let Err(e) = sender.send(success).await {
                                    error!("{e:?}");
                                    ProxyState::update_share_accounter_state(
                                        ShareAccounterState::Down,
                                    );
                                    break;
                                }
                            }
                        };
                    }
                    PoolExtMessages::Mining(msg) => {
                        if let Err(e) = sender.send(msg).await {
                            error!("{e}");
                            ProxyState::update_share_accounter_state(ShareAccounterState::Down);
                            break;
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    });
    task.into()
}
