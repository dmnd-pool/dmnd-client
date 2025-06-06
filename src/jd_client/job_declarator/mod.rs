pub mod message_handler;
mod task_manager;
use binary_sv2::{Seq0255, Seq064K, B016M, B064K, U256};
use bitcoin::{blockdata::transaction::Transaction, hashes::Hash};
use codec_sv2::{HandshakeRole, Initiator, StandardEitherFrame, StandardSv2Frame};
use demand_sv2_connection::noise_connection_tokio::Connection;
use roles_logic_sv2::{
    handlers::SendTo_,
    job_declaration_sv2::{AllocateMiningJobTokenSuccess, SubmitSolutionJd},
    mining_sv2::SubmitSharesExtended,
    parsers::{JobDeclaration, PoolMessages},
    template_distribution_sv2::SetNewPrevHash,
    utils::Mutex,
};
use std::{collections::HashMap, convert::TryInto};
use task_manager::TaskManager;
use tokio::sync::mpsc::{Receiver as TReceiver, Sender as TSender};
use tracing::{error, info};

use async_recursion::async_recursion;
use nohash_hasher::BuildNoHashHasher;
use roles_logic_sv2::{
    handlers::job_declaration::ParseServerJobDeclarationMessages,
    job_declaration_sv2::{AllocateMiningJobToken, DeclareMiningJob},
    template_distribution_sv2::NewTemplate,
    utils::Id,
};
use std::{net::SocketAddr, sync::Arc};

pub type Message = PoolMessages<'static>;
pub type SendTo = SendTo_<JobDeclaration<'static>, ()>;
pub type StdFrame = StandardSv2Frame<Message>;

pub mod setup_connection;
use setup_connection::SetupConnectionHandler;

use crate::{
    proxy_state::{JdState, PoolState, ProxyState},
    shared::utils::AbortOnDrop,
};

use super::{error::Error, mining_upstream::Upstream};

#[derive(Debug, Clone)]
pub struct LastDeclareJob {
    declare_job: DeclareMiningJob<'static>,
    template: NewTemplate<'static>,
    coinbase_pool_output: Vec<u8>,
    tx_list: Seq064K<'static, B016M<'static>>,
}

#[derive(Debug)]
pub struct JobDeclarator {
    sender: TSender<StandardEitherFrame<PoolMessages<'static>>>,
    allocated_tokens: Vec<AllocateMiningJobTokenSuccess<'static>>,
    req_ids: Id,
    min_extranonce_size: u16,
    // (Sent DeclareMiningJob, is future, template id, merkle path)
    last_declare_mining_jobs_sent: HashMap<u32, Option<LastDeclareJob>>,
    pub last_set_new_prev_hash: Option<SetNewPrevHash<'static>>,
    set_new_prev_hash_counter: u8,
    #[allow(clippy::type_complexity)]
    future_jobs: HashMap<
        u64,
        (
            DeclareMiningJob<'static>,
            Seq0255<'static, U256<'static>>,
            NewTemplate<'static>,
            // pool's outputs
            Vec<u8>,
        ),
        BuildNoHashHasher<u64>,
    >,
    up: Arc<Mutex<Upstream>>,
    pub coinbase_tx_prefix: B064K<'static>,
    pub coinbase_tx_suffix: B064K<'static>,
    pub task_manager: Arc<Mutex<TaskManager>>,
}

impl JobDeclarator {
    pub async fn new(
        address: SocketAddr,
        authority_public_key: [u8; 32],
        up: Arc<Mutex<Upstream>>,
        should_log_when_connected: bool,
        use_random_device_id: bool,
    ) -> Result<(Arc<Mutex<Self>>, AbortOnDrop), Error> {
        let stream = tokio::net::TcpStream::connect(address).await?;
        let initiator = Initiator::from_raw_k(authority_public_key)?;
        let (mut receiver, mut sender, _, _) =
            Connection::new(stream, HandshakeRole::Initiator(initiator))
                .await
                .map_err(|_| Error::Unrecoverable)?;

        SetupConnectionHandler::setup(&mut receiver, &mut sender, address, use_random_device_id)
            .await?; // don't use random device_id

        if should_log_when_connected {
            info!("JD CONNECTED");
        }

        let min_extranonce_size = crate::MIN_EXTRANONCE_SIZE;

        let task_manager = TaskManager::initialize();
        let abortable = task_manager
            .safe_lock(|t| t.get_aborter())
            .map_err(|_| Error::PoisonLock)?
            .ok_or(Error::Unrecoverable)?;
        let self_ = Arc::new(Mutex::new(JobDeclarator {
            sender,
            allocated_tokens: vec![],
            req_ids: Id::new(),
            min_extranonce_size,
            last_declare_mining_jobs_sent: HashMap::with_capacity(2),
            last_set_new_prev_hash: None,
            future_jobs: HashMap::with_hasher(BuildNoHashHasher::default()),
            up,
            coinbase_tx_prefix: vec![].try_into().expect("Internal error: this operation can not fail because Vec can always be converted into Inner"),
            coinbase_tx_suffix: vec![].try_into().expect("Internal error: this operation can not fail because Vec can always be converted into Inner"),
            set_new_prev_hash_counter: 0,
            task_manager,
        }));

        Self::allocate_tokens(&self_, 2).await;
        Self::on_upstream_message(self_.clone(), receiver).await?;
        Ok((self_, abortable))
    }

    fn get_last_declare_job_sent(
        self_mutex: &Arc<Mutex<Self>>,
        request_id: u32,
    ) -> Result<LastDeclareJob, Error> {
        let id = self_mutex
            .safe_lock(|s| s.last_declare_mining_jobs_sent.remove(&request_id).clone())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;
        Ok(id
            .expect("Impossible to get last declare job sent")
            .clone()
            .expect("This is ok"))
    }

    fn update_last_declare_job_sent(
        self_mutex: &Arc<Mutex<Self>>,
        request_id: u32,
        j: LastDeclareJob,
    ) -> Result<(), Error> {
        self_mutex
            .safe_lock(|s| {
                //check hashmap size in order to not let it grow indefinetely
                if s.last_declare_mining_jobs_sent.len() < 10 {
                    s.last_declare_mining_jobs_sent.insert(request_id, Some(j));
                } else if let Some(min_key) = s.last_declare_mining_jobs_sent.keys().min().cloned()
                {
                    s.last_declare_mining_jobs_sent.remove(&min_key);
                    s.last_declare_mining_jobs_sent.insert(request_id, Some(j));
                }
            })
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)
    }

    #[async_recursion]
    pub async fn get_last_token(
        self_mutex: &Arc<Mutex<Self>>,
    ) -> Result<AllocateMiningJobTokenSuccess<'static>, Error> {
        let mut token_len = self_mutex
            .safe_lock(|s| s.allocated_tokens.len())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;
        let task_manager = self_mutex
            .safe_lock(|s| s.task_manager.clone())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;
        match token_len {
            0 => {
                {
                    let task = {
                        let self_mutex = self_mutex.clone();
                        tokio::task::spawn(
                            async move { Self::allocate_tokens(&self_mutex, 2).await },
                        )
                    };
                    TaskManager::add_allocate_tokens(task_manager, task.into())
                        .await
                        .map_err(|_| Error::JobDeclaratorTaskManagerFailed)?;
                }

                // we wait for token allocation to avoid infinite recursion
                while token_len == 0 {
                    tokio::task::yield_now().await;
                    token_len = self_mutex
                        .safe_lock(|s| s.allocated_tokens.len())
                        .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;
                }

                Self::get_last_token(self_mutex).await
            }
            1 => {
                {
                    let task = {
                        let self_mutex = self_mutex.clone();
                        tokio::task::spawn(
                            async move { Self::allocate_tokens(&self_mutex, 1).await },
                        )
                    };
                    TaskManager::add_allocate_tokens(task_manager, task.into())
                        .await
                        .map_err(|_| Error::JobDeclaratorTaskManagerFailed)?;
                }
                // There is a token, unwrap is safe
                Ok(self_mutex
                    .safe_lock(|s| s.allocated_tokens.pop())
                    .map_err(|_| Error::JobDeclaratorMutexCorrupted)?
                    .expect("Last token not found"))
            }
            // There are tokens, unwrap is safe
            _ => Ok(self_mutex
                .safe_lock(|s| s.allocated_tokens.pop())
                .map_err(|_| Error::JobDeclaratorMutexCorrupted)?
                .expect("Last token not found")),
        }
    }

    pub async fn on_new_template(
        self_mutex: &Arc<Mutex<Self>>,
        template: NewTemplate<'static>,
        token: Vec<u8>,
        tx_list_: Seq064K<'static, B016M<'static>>,
        excess_data: B064K<'static>,
        coinbase_pool_output: Vec<u8>,
    ) -> Result<(), Error> {
        let now = std::time::Instant::now();
        while !super::IS_CUSTOM_JOB_SET.load(std::sync::atomic::Ordering::Acquire) {
            if now.elapsed().as_secs() > 30 {
                error!("Failed to set custom job");
                ProxyState::update_jd_state(JdState::Down);
                return Err(Error::Unrecoverable);
            }
            tokio::task::yield_now().await;
        }
        super::IS_CUSTOM_JOB_SET.store(false, std::sync::atomic::Ordering::Release);
        let (id, _, sender) = self_mutex
            .safe_lock(|s| (s.req_ids.next(), s.min_extranonce_size, s.sender.clone()))
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;

        let mut tx_list: Vec<Transaction> = Vec::new();
        let mut tx_ids = vec![];
        for tx in tx_list_.to_vec() {
            let transaction: Result<Transaction, bitcoin::consensus::encode::Error> =
                bitcoin::consensus::deserialize(&tx);
            match transaction {
                Ok(tx) => {
                    let id: U256 = tx.compute_txid().to_raw_hash().to_byte_array().into();
                    tx_list.push(tx);
                    tx_ids.push(id);
                }
                Err(_) => {
                    error!("Failed to deserailize transaction");
                    return Err(Error::Unrecoverable);
                }
            }
        }
        let tx_ids: Seq064K<'static, U256> = Seq064K::from(tx_ids);

        let coinbase_prefix = self_mutex
            .safe_lock(|s| s.coinbase_tx_prefix.clone())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;

        let coinbase_suffix = self_mutex
            .safe_lock(|s| s.coinbase_tx_suffix.clone())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;

        let declare_job = DeclareMiningJob {
            request_id: id,
            mining_job_token: token.try_into().expect("Internal error: this operation can not fail because Vec<U8> can always be converted into Inner"),
            version: template.version,
            coinbase_prefix,
            coinbase_suffix,
            tx_list: tx_ids,
            excess_data, // request transaction data
        };
        let last_declare = LastDeclareJob {
            declare_job: declare_job.clone(),
            template,
            coinbase_pool_output,
            tx_list: tx_list_.clone(),
        };
        Self::update_last_declare_job_sent(self_mutex, id, last_declare)?;
        let frame: StdFrame =
            PoolMessages::JobDeclaration(JobDeclaration::DeclareMiningJob(declare_job))
                .try_into()
                .expect("Infallable operation");
        sender
            .send(frame.into())
            .await
            .map_err(|_| Error::Unrecoverable)
    }

    pub async fn on_upstream_message(
        self_mutex: Arc<Mutex<Self>>,
        mut receiver: TReceiver<StandardEitherFrame<PoolMessages<'static>>>,
    ) -> Result<(), Error> {
        let up = self_mutex
            .safe_lock(|s| s.up.clone())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;
        let task_manager = self_mutex
            .safe_lock(|s| s.task_manager.clone())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;
        let main_task = tokio::task::spawn(async move {
            loop {
                let mut incoming: StdFrame = match receiver.recv().await {
                    Some(msg) => msg.try_into().unwrap_or_else(|_| {
                        error!("Invalid msg: Failed to convert msg to StdFrame");
                        std::process::exit(1)
                    }),
                    None => {
                        error!("Failed to receive msg from Pool");
                        ProxyState::update_pool_state(PoolState::Down);
                        break;
                    }
                };
                let message_type = match incoming.get_header() {
                    Some(header) => header.msg_type(),
                    None => {
                        error!("Invalid msg: Failed to get msg header");
                        std::process::exit(1)
                    }
                };
                let payload = incoming.payload();
                let next_message_to_send =
                    ParseServerJobDeclarationMessages::handle_message_job_declaration(
                        self_mutex.clone(),
                        message_type,
                        payload,
                    );
                match next_message_to_send {
                    Ok(SendTo::None(Some(JobDeclaration::DeclareMiningJobSuccess(m)))) => {
                        let new_token = m.new_mining_job_token;
                        let last_declare =
                            match Self::get_last_declare_job_sent(&self_mutex, m.request_id) {
                                Ok(last_declare) => last_declare,
                                Err(e) => {
                                    error!("{e}");
                                    ProxyState::update_jd_state(JdState::Down);
                                    break;
                                }
                            };
                        let mut last_declare_mining_job_sent = last_declare.declare_job;
                        let is_future = last_declare.template.future_template;
                        let id = last_declare.template.template_id;
                        let merkle_path = last_declare.template.merkle_path.clone();
                        let template = last_declare.template;

                        // TODO where we should have a sort of signaling that is green after
                        // that the token has been updated so that on_set_new_prev_hash know it
                        // and can decide if send the set_custom_job or not
                        if is_future {
                            last_declare_mining_job_sent.mining_job_token = new_token;
                            if let Err(e) = self_mutex.safe_lock(|s| {
                                s.future_jobs.insert(
                                    id,
                                    (
                                        last_declare_mining_job_sent,
                                        merkle_path,
                                        template,
                                        last_declare.coinbase_pool_output,
                                    ),
                                );
                            }) {
                                error!("{e}");
                                ProxyState::update_jd_state(JdState::Down);
                                break;
                            };
                        } else {
                            let set_new_prev_hash =
                                match self_mutex.safe_lock(|s| s.last_set_new_prev_hash.clone()) {
                                    Ok(set_new_prev_hash) => set_new_prev_hash,
                                    Err(e) => {
                                        error!("{e}");
                                        ProxyState::update_jd_state(JdState::Down);
                                        break;
                                    }
                                };
                            let mut template_outs = template.coinbase_tx_outputs.to_vec();
                            let mut pool_outs = last_declare.coinbase_pool_output;
                            pool_outs.append(&mut template_outs);
                            match set_new_prev_hash {
                                Some(p) => if let Err(e) =  Upstream::set_custom_jobs(
                                    &up,
                                    last_declare_mining_job_sent,
                                    p,
                                    merkle_path,
                                    new_token,
                                    template.coinbase_tx_version,
                                    template.coinbase_prefix,
                                    template.coinbase_tx_input_sequence,
                                    template.coinbase_tx_value_remaining,
                                    pool_outs,
                                    template.coinbase_tx_locktime,
                                    template.template_id
                                    ).await {error!("Failed to set custom jobd: {e}"); ProxyState::update_jd_state(JdState::Down);break;},
                                None => panic!("Invalid state we received a NewTemplate not future, without having received a set new prev hash")
                            }
                        }
                    }
                    Ok(SendTo::None(Some(JobDeclaration::DeclareMiningJobError(m)))) => {
                        error!("Job is not verified: {:?}", m);
                    }
                    Ok(SendTo::None(None)) => (),
                    Ok(SendTo::Respond(m)) => {
                        let sv2_frame: StdFrame = PoolMessages::JobDeclaration(m)
                            .try_into()
                            .expect("Infallable operatiion");
                        let sender = match self_mutex.safe_lock(|self_| self_.sender.clone()) {
                            Ok(sender) => sender,
                            Err(e) => {
                                error!("{e}");
                                ProxyState::update_jd_state(JdState::Down);
                                break;
                            }
                        };
                        if sender.send(sv2_frame.into()).await.is_err() {
                            error!("Job declarator failed to send message");
                            ProxyState::update_jd_state(JdState::Down);
                            break;
                        };
                    }
                    Ok(_) => unreachable!(),
                    Err(e) => {
                        error!("{e}");
                        ProxyState::update_jd_state(JdState::Down);
                        break;
                    }
                }
            }
        });
        TaskManager::add_allocate_tokens(task_manager, main_task.into())
            .await
            .map_err(|_| Error::JobDeclaratorTaskManagerFailed)
    }

    pub async fn on_set_new_prev_hash(
        self_mutex: Arc<Mutex<Self>>,
        set_new_prev_hash: SetNewPrevHash<'static>,
    ) -> Result<(), Error> {
        let task_manager = self_mutex
            .safe_lock(|s| s.task_manager.clone())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;
        let task = tokio::task::spawn(async move {
            let id = set_new_prev_hash.template_id;
            if self_mutex
                .safe_lock(|s| {
                    s.last_set_new_prev_hash = Some(set_new_prev_hash.clone());
                    s.set_new_prev_hash_counter += 1;
                })
                .is_err()
            {
                error!("{}", Error::JobDeclaratorMutexCorrupted);
                return;
            };
            let (job, up, merkle_path, template, mut pool_outs) = loop {
                match self_mutex.safe_lock(|s| {
                    if s.set_new_prev_hash_counter > 1
                        && s.last_set_new_prev_hash != Some(set_new_prev_hash.clone())
                    //it means that a new prev_hash is arrived while the previous hasn't exited the loop yet
                    {
                        s.set_new_prev_hash_counter -= 1;
                        Some(None)
                    } else {
                        s.future_jobs
                            .remove(&id)
                            .map(|(job, merkle_path, template, pool_outs)| {
                                s.future_jobs = HashMap::with_hasher(BuildNoHashHasher::default());
                                s.set_new_prev_hash_counter -= 1;
                                Some((job, s.up.clone(), merkle_path, template, pool_outs))
                            })
                    }
                }) {
                    Ok(Some(Some(future_job_tuple))) => break future_job_tuple,
                    Ok(Some(None)) => {
                        // No future jobs
                        error!(
                            "{}",
                            Error::RolesSv2Logic(roles_logic_sv2::errors::Error::NoFutureJobs,)
                        );
                        return;
                    }
                    Ok(None) => {}
                    Err(_) => {
                        error!("{}", Error::JobDeclaratorMutexCorrupted);
                        return;
                    }
                };
                tokio::task::yield_now().await;
            };
            let signed_token = job.mining_job_token.clone();
            let mut template_outs = template.coinbase_tx_outputs.to_vec();
            pool_outs.append(&mut template_outs);
            if let Err(e) = Upstream::set_custom_jobs(
                &up,
                job,
                set_new_prev_hash,
                merkle_path,
                signed_token,
                template.coinbase_tx_version,
                template.coinbase_prefix,
                template.coinbase_tx_input_sequence,
                template.coinbase_tx_value_remaining,
                pool_outs,
                template.coinbase_tx_locktime,
                template.template_id,
            )
            .await
            {
                error!("Failed to set custom jobs: {e}");
            }
        });
        TaskManager::add_allocate_tokens(task_manager, task.into())
            .await
            .map_err(|_| Error::JobDeclaratorTaskManagerFailed)
    }

    async fn allocate_tokens(self_mutex: &Arc<Mutex<Self>>, token_to_allocate: u32) {
        for i in 0..token_to_allocate {
            let device_id = crate::Configuration::device_id(false);
            let message = JobDeclaration::AllocateMiningJobToken(AllocateMiningJobToken {
                user_identifier: device_id.try_into().expect("Infallible operation"),
                request_id: i,
            });
            let sender = match self_mutex.safe_lock(|s| s.sender.clone()) {
                Ok(sender) => sender,
                Err(e) => {
                    error!("{e}");
                    //Poison lock
                    ProxyState::update_jd_state(JdState::Down);
                    return;
                }
            };

            // Safe unwrap message is build above and is valid, below can never panic
            let frame: StdFrame = PoolMessages::JobDeclaration(message)
                .try_into()
                .expect("Infallible operation");

            if sender.send(frame.into()).await.is_err() {
                error!("Job declarator failed to send message");
                ProxyState::update_jd_state(JdState::Down);
            }
        }
    }
    pub async fn on_solution(
        self_mutex: &Arc<Mutex<Self>>,
        solution: SubmitSharesExtended<'static>,
    ) -> Result<(), Error> {
        let prev_hash = match self_mutex
            .safe_lock(|s| s.last_set_new_prev_hash.clone())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?
        {
            Some(prev_hash) => prev_hash,
            None => {
                //? Internal Inconsistencies?
                Err(Error::Unrecoverable)?
            }
        };
        let solution = SubmitSolutionJd {
            extranonce: solution.extranonce,
            prev_hash: prev_hash.prev_hash,
            ntime: solution.ntime,
            nonce: solution.nonce,
            nbits: prev_hash.n_bits,
            version: solution.version,
        };
        let frame: StdFrame =
            PoolMessages::JobDeclaration(JobDeclaration::SubmitSolution(solution))
                .try_into()
                .expect("Infallible operation");
        let sender = self_mutex
            .safe_lock(|s| s.sender.clone())
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;
        sender.send(frame.into()).await.map_err(|_| {
            error!("JDC Sub solution receiver unavailable");
            Error::Unrecoverable
        })
    }
}
