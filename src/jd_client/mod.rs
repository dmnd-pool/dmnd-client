#![allow(special_module_name)]

mod error;
pub mod job_declarator;
pub mod mining_downstream;
pub mod mining_upstream;
mod task_manager;
mod template_receiver;
mod utils;

use bitcoin::TxOut;
use job_declarator::JobDeclarator;
use key_utils::Secp256k1PublicKey;
use mining_downstream::DownstreamMiningNode;
use roles_logic_sv2::template_distribution_sv2::SubmitSolution;
use std::{path::PathBuf, sync::atomic::AtomicBool};
use task_manager::TaskManager;
use template_receiver::TemplateRx;
use tracing::{error, info};
use utils::{get_coinbase_output, parse_tp_address, retry_connection, Config};

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

use crate::proxy_state::{DownstreamType, ProxyState};
use roles_logic_sv2::{parsers::Mining, utils::Mutex};
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use std::net::ToSocketAddrs;

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

/// Initializes JD in pool mining mode.
async fn initialize_jd(
    receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    sender: tokio::sync::mpsc::Sender<Mining<'static>>,
    up_receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    up_sender: tokio::sync::mpsc::Sender<Mining<'static>>,
) -> Option<AbortOnDrop> {
    let (jd, recv_solution) = JdSetUp::new()
        .await?
        .start_upstream_and_jd(up_sender, up_receiver)
        .await?
        .start_downstream(receiver, sender, None, vec![])
        .await?;
    jd.start_template_receiver(recv_solution, vec![], false)
        .await
}

/// Initializes JD in solo mining mode.
pub async fn initialize_jd_as_solo_miner(
    receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    sender: tokio::sync::mpsc::Sender<Mining<'static>>,
    config: PathBuf,
) -> Option<AbortOnDrop> {
    info!("Starting solo mining...");
    let config_str = std::fs::read_to_string(config).expect("Failed to read config file");
    let config: Config = toml::from_str(&config_str).expect("Failed to parse config file");
    let miner_coinbase_output = get_coinbase_output(&config).unwrap();

    let (jd_solo, recv_solution) = JdSetUp::new()
        .await?
        .start_downstream(
            receiver,
            sender,
            config.withhold(),
            miner_coinbase_output.clone(),
        )
        .await?;

    jd_solo
        .start_template_receiver(recv_solution, miner_coinbase_output, false)
        .await
}

struct JdSetUp {
    task_manager: Arc<Mutex<TaskManager>>,
    abortable: AbortOnDrop,
    downstream: Option<Arc<Mutex<DownstreamMiningNode>>>,
    upstream: Option<Arc<Mutex<mining_upstream::Upstream>>>,
    jd: Option<Arc<Mutex<JobDeclarator>>>,
}

impl JdSetUp {
    async fn new() -> Option<Self> {
        let task_manager = TaskManager::initialize();
        let abortable = match task_manager.safe_lock(|t| t.get_aborter()) {
            Ok(abortable) => abortable?,
            Err(e) => {
                error!("Jdc task manager mutex corrupt: {e}");
                return None;
            }
        };
        Some(Self {
            task_manager,
            abortable,
            downstream: None,
            upstream: None,
            jd: None,
        })
    }

    async fn start_upstream_and_jd(
        mut self,
        up_sender: tokio::sync::mpsc::Sender<Mining<'static>>,
        up_receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    ) -> Option<Self> {
        let upstream =
            match mining_upstream::Upstream::new(crate::MIN_EXTRANONCE_SIZE, up_sender).await {
                Ok(upstream) => upstream,
                Err(e) => {
                    error!("Failed to instantiate new Upstream: {e}");
                    drop(self.abortable);
                    return None;
                }
            };

        let upstream_abortable =
            match mining_upstream::Upstream::parse_incoming(upstream.clone(), up_receiver).await {
                Ok(abortable) => abortable,
                Err(e) => {
                    error!("Failed to get jdc upstream abortable: {e}");
                    drop(self.abortable);
                    return None;
                }
            };

        if TaskManager::add_mining_upstream_task(self.task_manager.clone(), upstream_abortable)
            .await
            .is_err()
        {
            error!("Failed to add mining upstream task");
            drop(self.abortable);
            return None;
        };
        let address = crate::POOL_ADDRESS
            .to_socket_addrs()
            .expect("The passed Pool Address is not valid")
            .next()?;
        let auth_pub_k: Secp256k1PublicKey =
            crate::AUTH_PUB_KEY.parse().expect("Invalid public key");

        let (jd, jd_abortable) = match JobDeclarator::new(
            address,
            auth_pub_k.into_bytes(),
            upstream.clone(),
            true,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to initialize Jd: {e}");
                drop(self.abortable);
                return None;
            }
        };

        if TaskManager::add_job_declarator_task(self.task_manager.clone(), jd_abortable)
            .await
            .is_err()
        {
            error!("Failed to add job declarator task");
            drop(self.abortable);
            return None;
        };
        self.upstream = Some(upstream);
        self.jd = Some(jd);
        Some(self)
    }

    async fn start_downstream(
        mut self,
        receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
        sender: tokio::sync::mpsc::Sender<Mining<'static>>,
        withhold: Option<bool>,
        miner_coinbase_output: Vec<TxOut>,
    ) -> Option<(Self, tokio::sync::mpsc::Receiver<SubmitSolution<'static>>)> {
        let (send_solution, recv_solution) = tokio::sync::mpsc::channel(10);

        let downstream = Arc::new(Mutex::new(DownstreamMiningNode::new(
            sender,
            self.upstream.clone(),
            send_solution,
            withhold.unwrap_or(false),
            miner_coinbase_output,
            self.jd.clone(),
        )));

        let downstream_abortable =
            match DownstreamMiningNode::start(downstream.clone(), receiver).await {
                Ok(abortable) => abortable,
                Err(e) => {
                    error!("Cannot start downstream mining node: {e}");
                    ProxyState::update_downstream_state(DownstreamType::JdClientMiningDownstream);
                    drop(self.abortable);
                    return None;
                }
            };

        if TaskManager::add_mining_downtream_task(self.task_manager.clone(), downstream_abortable)
            .await
            .is_err()
        {
            error!("Failed to add mining downstream task");
            drop(self.abortable);
            return None;
        };

        if let Some(upstream) = &self.upstream {
            if upstream
                .safe_lock(|u| u.downstream = Some(downstream.clone()))
                .is_err()
            {
                error!("Upstream mutex failed");
                drop(self.abortable);
                return None;
            };
        }

        self.downstream = Some(downstream);
        Some((self, recv_solution))
    }

    async fn start_template_receiver(
        self,
        recv_solution: tokio::sync::mpsc::Receiver<SubmitSolution<'static>>,
        miner_coinbase_tx: Vec<TxOut>,
        test_only_do_not_send_solution_to_tp: bool,
    ) -> Option<AbortOnDrop> {
        let (ip_tp, port_tp, tp_address) = parse_tp_address()?;

        let ip = IpAddr::from_str(ip_tp.as_str()).expect("Invalid IP address");

        match TemplateRx::connect(
            SocketAddr::new(ip, port_tp),
            recv_solution,
            self.jd,
            self.downstream.expect("Downstream must be initialized"),
            miner_coinbase_tx,
            None,
            test_only_do_not_send_solution_to_tp,
        )
        .await
        {
            Ok(tp_abortable) => {
                if TaskManager::add_template_receiver_task(self.task_manager, tp_abortable)
                    .await
                    .is_err()
                {
                    error!("Failed to add template receiver task");
                    drop(self.abortable);
                    return None;
                }
                Some(self.abortable)
            }
            Err(_) => {
                info!("Dropping jd abortable");
                eprintln!("TP is unreachable, the proxy is in not in JD mode");
                drop(self.abortable);
                if crate::TP_ADDRESS.safe_lock(|tp| *tp = None).is_err() {
                    error!("TP_ADDRESS mutex corrupt");
                    return None;
                };
                tokio::spawn(retry_connection(tp_address));
                None
            }
        }
    }
}
