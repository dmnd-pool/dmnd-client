#![allow(special_module_name)]

mod error;
pub mod job_declarator;
pub mod mining_downstream;
pub mod mining_upstream;
mod task_manager;
mod template_receiver;

use job_declarator::JobDeclarator;
use key_utils::Secp256k1PublicKey;
use mining_downstream::DownstreamMiningNode;
use std::sync::atomic::AtomicBool;
use task_manager::TaskManager;
use template_receiver::TemplateRx;
use tracing::{error, info};

/// Is used by the template receiver and the downstream. When a NewTemplate is received the context
/// that is running the template receiver set this value to false and then the message is sent to
/// the context that is running the Downstream that do something and then set it back to true.
///
/// In the meantime if the context that is running the template receiver receives a SetNewPrevHash
/// it wait until the value of this global is true before doing anything.
///
/// Acuire and Release memory ordering is used.
///
/// Memory Ordering Explanation:
/// We use Acquire-Release ordering instead of SeqCst or Relaxed for the following reasons:
/// 1. Acquire in template receiver context ensures we see all operations before the Release store
///    the downstream.
/// 2. Within the same execution context (template receiver), a Relaxed store followed by an Acquire
///    load is sufficient. This is because operations within the same context execute in the order
///    they appear in the code.
/// 3. The combination of Release in downstream and Acquire in template receiver contexts establishes
///    a happens-before relationship, guaranteeing that we handle the SetNewPrevHash message after
///    that downstream have finished handling the NewTemplate.
/// 3. SeqCst is overkill we only need to synchronize two contexts, a globally agreed-upon order
///    between all the contexts is not necessary.
pub static IS_NEW_TEMPLATE_HANDLED: AtomicBool = AtomicBool::new(true);

pub static IS_CUSTOM_JOB_SET: AtomicBool = AtomicBool::new(true);

use crate::proxy_state::{DownstreamType, ProxyState, TpState};
use roles_logic_sv2::{parsers::Mining, utils::Mutex};
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use crate::shared::utils::AbortOnDrop;

pub async fn start(
    receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    sender: tokio::sync::mpsc::Sender<Mining<'static>>,
    up_receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    up_sender: tokio::sync::mpsc::Sender<Mining<'static>>,
) -> Option<AbortOnDrop> {
    // This will not work when we implement support for multiple upstream
    IS_CUSTOM_JOB_SET.store(true, std::sync::atomic::Ordering::Release);
    IS_NEW_TEMPLATE_HANDLED.store(true, std::sync::atomic::Ordering::Release);
    initialize_jd(receiver, sender, up_receiver, up_sender).await
}

async fn initialize_jd(
    receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    sender: tokio::sync::mpsc::Sender<Mining<'static>>,
    up_receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    up_sender: tokio::sync::mpsc::Sender<Mining<'static>>,
) -> Option<AbortOnDrop> {
    let task_manager = TaskManager::initialize();
    let abortable = match task_manager.safe_lock(|t| t.get_aborter()) {
        Ok(abortable) => abortable?,
        Err(e) => {
            error!("Jdc task manager mutex corrupt: {e}");
            return None;
        }
    };
    let test_only_do_not_send_solution_to_tp = false;

    // When Downstream receive a share that meets bitcoin target it transformit in a
    // SubmitSolution and send it to the TemplateReceiver
    let (send_solution, recv_solution) = tokio::sync::mpsc::channel(10);

    // Instantiate a new `Upstream` (SV2 Pool)
    let upstream = match mining_upstream::Upstream::new(crate::MIN_EXTRANONCE_SIZE, up_sender).await
    {
        Ok(upstream) => upstream,
        Err(e) => {
            error!("Failed to instantiate new Upstream: {e}");
            drop(abortable);
            return None;
        }
    };

    // Initialize JD part
    let tp_address = match crate::TP_ADDRESS.safe_lock(|tp| tp.clone()) {
        Ok(tp_address) => tp_address
            .expect("Unreachable code, jdc is not instantiated when TP_ADDRESS not present"),
        Err(e) => {
            error!("TP_ADDRESS mutex corrupted: {e}");
            drop(abortable);
            return None;
        }
    };

    let mut parts = tp_address.split(':');
    let ip_tp = parts.next().expect("The passed value for TP address is not valid. Terminating.... TP_ADDRESS should be in this format `127.0.0.1:8442`").to_string();
    let port_tp = parts.next().expect("The passed value for TP address is not valid. Terminating.... TP_ADDRESS should be in this format `127.0.0.1:8442`").parse::<u16>().expect("This operation should not fail because a valid port_tp should always be converted to U16");

    let auth_pub_k: Secp256k1PublicKey = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let address = match crate::POOL_ADDRESS.safe_lock(|address| *address) {
        Ok(Some(address)) => address,
        Ok(None) => {
            error!("Pool address is missing");
            ProxyState::update_inconsistency(Some(1));
            return None;
        }
        Err(e) => {
            error!("Pool address mutex is poisoned: {e:?}");
            ProxyState::update_inconsistency(Some(1));
            return None;
        }
    };

    let (jd, jd_abortable) = match JobDeclarator::new(
        address,
        auth_pub_k.into_bytes(),
        upstream.clone(),
        true,
        false,
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to intialize Jd: {e}");
            drop(abortable);
            return None;
        }
    };

    if TaskManager::add_job_declarator_task(task_manager.clone(), jd_abortable)
        .await
        .is_err()
    {
        error!(
            "Task manager failed while trying to add job declarator task{}",
            error::Error::TaskManagerFailed
        );
        drop(abortable);
        return None;
    };

    let donwstream = Arc::new(Mutex::new(DownstreamMiningNode::new(
        sender,
        Some(upstream.clone()),
        send_solution,
        false,
        vec![],
        Some(jd.clone()),
    )));
    let downstream_abortable = match DownstreamMiningNode::start(donwstream.clone(), receiver).await
    {
        Ok(abortable) => abortable,
        Err(e) => {
            error!("Can not start downstream mining node: {e}");
            ProxyState::update_downstream_state(DownstreamType::JdClientMiningDownstream);
            return None;
        }
    };
    if TaskManager::add_mining_downtream_task(task_manager.clone(), downstream_abortable)
        .await
        .is_err()
    {
        error!(
            "Task manager failed while trying to add mining downstream task{}",
            error::Error::TaskManagerFailed
        );
        drop(abortable);
        return None;
    };
    if upstream
        .safe_lock(|u| u.downstream = Some(donwstream.clone()))
        .is_err()
    {
        error!("Upstream mutex failed");
        drop(abortable); // drop all tasks initailzed upto this point
        return None;
    };

    // Start receiving messages from the SV2 Upstream role
    let upstream_abortable =
        match mining_upstream::Upstream::parse_incoming(upstream.clone(), up_receiver).await {
            Ok(abortable) => abortable,
            Err(e) => {
                error!("Failed to get jdc upstream abortable: {e}");
                drop(abortable); // drop all tasks initailzed upto this point
                return None;
            }
        };
    if TaskManager::add_mining_upstream_task(task_manager.clone(), upstream_abortable)
        .await
        .is_err()
    {
        error!(
            "Task manager failed while trying to add mining upstream task{}",
            error::Error::TaskManagerFailed
        );
        drop(abortable); // drop all tasks initailzed upto this point
        return None;
    };
    let ip = IpAddr::from_str(ip_tp.as_str())
        .expect("Infallable Operation: Failed tp can always be converted into IpAddr");
    let tp_abortable = match TemplateRx::connect(
        SocketAddr::new(ip, port_tp),
        recv_solution,
        Some(jd.clone()),
        donwstream.clone(),
        vec![],
        None,
        test_only_do_not_send_solution_to_tp,
    )
    .await
    {
        Ok(abortable) => abortable,
        Err(_) => {
            info!("Dropping jd abortable");
            eprintln!("TP is unreachable, the proxy is in not in JD mode");
            drop(abortable);
            // Temporaily set TP_ADDRESS to None so that proxy can restart without it.
            // that means we will start mining without jd
            if crate::TP_ADDRESS.safe_lock(|tp| *tp = None).is_err() {
                error!("TP_ADDRESS mutex corrupt");
                return None;
            };
            tokio::spawn(retry_connection(tp_address));
            return None;
        }
    };

    if TaskManager::add_template_receiver_task(task_manager, tp_abortable)
        .await
        .is_err()
    {
        error!(
            "Task manager failed while trying to add template receiver task{}",
            error::Error::TaskManagerFailed
        );
        drop(abortable);
        return None;
    };
    Some(abortable)
}

// Used when tp is down or connection was unsuccessful to retry connection.
async fn retry_connection(address: String) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
    loop {
        info!("TP Retrying connection....");
        interval.tick().await;
        if tokio::net::TcpStream::connect(address.clone())
            .await
            .is_ok()
        {
            info!("Successfully reconnected to TP: Restarting Proxy...");
            if crate::TP_ADDRESS
                .safe_lock(|tp| *tp = Some(address))
                .is_err()
            {
                error!("TP_ADDRESS Mutex failed");
                std::process::exit(1);
            };
            // This force the proxy to restart. If we use Up the proxy just ignore it.
            // So updating it to Down and setting the TP_ADDRESS to Some(address) will make the
            // proxy restart with TP, the the TpState will be set to Up.
            ProxyState::update_tp_state(TpState::Down);
            break;
        }
    }
}
