mod downstream;

mod error;
mod proxy;
mod upstream;
mod utils;

use bitcoin::Address;
use error::Error;

use roles_logic_sv2::{parsers::Mining, utils::Mutex};
use tracing::error;

use std::{net::IpAddr, sync::Arc};
use tokio::sync::mpsc::channel;

use sv1_api::server_to_client;
use tokio::sync::broadcast;

use crate::{
    proxy_state::{ProxyState, TranslatorState},
    shared::utils::AbortOnDrop,
};
use tokio::sync::mpsc::{Receiver as TReceiver, Sender as TSender};

use self::upstream::diff_management::UpstreamDifficultyConfig;
mod task_manager;
use task_manager::TaskManager;

pub async fn start(
    downstreams: TReceiver<(TSender<String>, TReceiver<String>, IpAddr)>,
    pool_connection: TSender<(
        TSender<Mining<'static>>,
        TReceiver<Mining<'static>>,
        Option<Address>,
    )>,
    stats_sender: crate::api::stats::StatsSender,
) -> Result<AbortOnDrop, Error<'static>> {
    let task_manager = TaskManager::initialize(pool_connection.clone());
    let abortable = task_manager
        .safe_lock(|t| t.get_aborter())
        .map_err(|_| Error::TranslatorTaskManagerMutexPoisoned)?
        .ok_or(Error::TranslatorTaskManagerFailed)?;

    let (send_to_up, up_recv_from_here) = channel(crate::TRANSLATOR_BUFFER_SIZE);
    let (up_send_to_here, recv_from_up) = channel(crate::TRANSLATOR_BUFFER_SIZE);
    pool_connection
        .send((up_send_to_here, up_recv_from_here, None))
        .await
        .map_err(|_| {
            error!("Internal Error: Failed to send channels to the pool");
            Error::Unrecoverable // Propagate error to that caller. There, we will restart Proxy
        })?;

    // `tx_sv1_bridge` sender is used by `Downstream` to send a `DownstreamMessages` message to
    // `Bridge` via the `rx_sv1_downstream` receiver
    // (Sender<downstream_sv1::DownstreamMessages>, Receiver<downstream_sv1::DownstreamMessages>)
    let (tx_sv1_bridge, rx_sv1_bridge) = channel(crate::TRANSLATOR_BUFFER_SIZE);

    // Sender/Receiver to send a SV2 `SubmitSharesExtended` from the `Bridge` to the `Upstream`
    // (Sender<SubmitSharesExtended<'static>>, Receiver<SubmitSharesExtended<'static>>)
    let (tx_sv2_submit_shares_ext, rx_sv2_submit_shares_ext) =
        channel(crate::TRANSLATOR_BUFFER_SIZE);

    // Sender/Receiver to send a SV2 `SetNewPrevHash` message from the `Upstream` to the `Bridge`
    // (Sender<SetNewPrevHash<'static>>, Receiver<SetNewPrevHash<'static>>)
    let (tx_sv2_set_new_prev_hash, rx_sv2_set_new_prev_hash) =
        channel(crate::TRANSLATOR_BUFFER_SIZE);

    // Sender/Receiver to send a SV2 `NewExtendedMiningJob` message from the `Upstream` to the
    // `Bridge`
    // (Sender<NewExtendedMiningJob<'static>>, Receiver<NewExtendedMiningJob<'static>>)
    let (tx_sv2_new_ext_mining_job, rx_sv2_new_ext_mining_job) =
        channel(crate::TRANSLATOR_BUFFER_SIZE);

    // Sender/Receiver to send a new extranonce from the `Upstream` to this `main` function to be
    // passed to the `Downstream` upon a Downstream role connection
    // (Sender<ExtendedExtranonce>, Receiver<ExtendedExtranonce>)
    let (tx_sv2_extranonce, mut rx_sv2_extranonce) = channel(crate::TRANSLATOR_BUFFER_SIZE);
    let target = Arc::new(Mutex::new(vec![0; 32]));

    // Sender/Receiver to send SV1 `mining.notify` message from the `Bridge` to the `Downstream`
    let (tx_sv1_notify, _): (
        broadcast::Sender<server_to_client::Notify>,
        broadcast::Receiver<server_to_client::Notify>,
    ) = broadcast::channel(crate::TRANSLATOR_BUFFER_SIZE);

    let channel_nominal_hashrate = 0.0;

    let upstream_diff = UpstreamDifficultyConfig {
        channel_diff_update_interval: crate::CHANNEL_DIFF_UPDTATE_INTERVAL,
        channel_nominal_hashrate,
    };
    let diff_config = Arc::new(Mutex::new(upstream_diff));

    // Instantiate a new `Upstream` (SV2 Pool)
    let upstream = upstream::Upstream::new(
        tx_sv2_set_new_prev_hash,
        tx_sv2_new_ext_mining_job,
        crate::MIN_EXTRANONCE_SIZE - 1,
        tx_sv2_extranonce,
        target.clone(),
        diff_config.clone(),
        send_to_up,
    )
    .await?;

    let upstream_abortable =
        upstream::Upstream::start(upstream, recv_from_up, rx_sv2_submit_shares_ext).await?;
    TaskManager::add_upstream(task_manager.clone(), upstream_abortable)
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)?;

    let startup_task = {
        let target = target.clone();
        let task_manager = task_manager.clone();
        tokio::task::spawn(async move {
            let (extended_extranonce, up_id) = match rx_sv2_extranonce.recv().await {
                Some((extended_extranonce, up_id)) => (extended_extranonce, up_id),
                None => {
                    error!("Failed to receive from rx_sv2_extranonce");
                    ProxyState::update_translator_state(TranslatorState::Down);
                    return;
                }
            };

            loop {
                let target: [u8; 32] =  match target.safe_lock(|t| t.clone()) {
                    Ok(target) => target.try_into().expect("Internal error: this operation cannot fail because Vec<u8> can always be converted into [u8; 32]"),
                    Err(e) => {
                        error!("{}", Error::TargetError(roles_logic_sv2::Error::PoisonLock(e.to_string())));
                        break
                    }
                };

                if target != [0; 32] {
                    break;
                };
                tokio::task::yield_now().await;
            }

            // Instantiate a new `Bridge` and begins handling incoming messages
            let b = match proxy::Bridge::new(
                tx_sv2_submit_shares_ext,
                tx_sv1_notify.clone(),
                extended_extranonce,
                target,
                up_id,
            ) {
                Ok(b) => b,
                Err(e) => {
                    error!("Failed to instantiate new Bridge: {e}");
                    return;
                }
            };

            let bridge_aborter = match proxy::Bridge::start(
                b.clone(),
                rx_sv2_set_new_prev_hash,
                rx_sv2_new_ext_mining_job,
                rx_sv1_bridge,
            )
            .await
            {
                Ok(abortable) => abortable,
                Err(e) => {
                    error!("Failed to start bridge: {e}");
                    return;
                }
            };

            let downstream_aborter = match downstream::Downstream::accept_connections(
                tx_sv1_bridge,
                tx_sv1_notify,
                b,
                diff_config,
                downstreams,
                stats_sender,
            )
            .await
            {
                Ok(abortable) => abortable,
                Err(e) => {
                    error!("Downstream failed to accept connection: {e}");
                    return;
                }
            };

            if TaskManager::add_bridge(task_manager.clone(), bridge_aborter)
                .await
                .is_err()
            {
                error!("{}", Error::TranslatorTaskManagerFailed);
                return;
            };

            if TaskManager::add_downstream_listener(task_manager.clone(), downstream_aborter)
                .await
                .is_err()
            {
                error!("{}", Error::TranslatorTaskManagerFailed);
            }
        })
    };
    TaskManager::add_startup_task(task_manager.clone(), startup_task.into())
        .await
        .map_err(|_| Error::TranslatorTaskManagerFailed)?;

    Ok(abortable)
}
