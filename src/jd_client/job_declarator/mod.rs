pub mod message_handler;
mod task_manager;
use binary_sv2::{Seq0255, Seq064K, B016M, B064K, U256};
use bitcoin::{blockdata::transaction::Transaction, hashes::Hash, Txid};
use codec_sv2::{StandardEitherFrame, StandardSv2Frame};
use roles_logic_sv2::{
    handlers::SendTo_,
    job_declaration_sv2::{AllocateMiningJobTokenSuccess, SubmitSolutionJd},
    mining_sv2::SubmitSharesExtended,
    parsers::{JobDeclaration, PoolMessages},
    template_distribution_sv2::SetNewPrevHash,
    utils::Mutex,
};
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
};
use task_manager::TaskManager;
use tokio::sync::{
    mpsc::{Receiver as TReceiver, Sender as TSender},
    watch,
};
use tracing::{error, info, warn};

use async_recursion::async_recursion;
use nohash_hasher::BuildNoHashHasher;
use roles_logic_sv2::{
    handlers::job_declaration::ParseServerJobDeclarationMessages,
    job_declaration_sv2::{AllocateMiningJobToken, DeclareMiningJob},
    template_distribution_sv2::NewTemplate,
    utils::Id,
};
use std::sync::Arc;

pub type Message = PoolMessages<'static>;
pub type SendTo = SendTo_<JobDeclaration<'static>, ()>;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;

pub mod setup_connection;

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
    sender: TSender<EitherFrame>,
    allocated_tokens: Vec<AllocateMiningJobTokenSuccess<'static>>,
    last_allocated_token: Option<AllocateMiningJobTokenSuccess<'static>>,
    req_ids: Id,
    // (Sent DeclareMiningJob, is future, template id, merkle path)
    last_declare_mining_jobs_sent: HashMap<u32, Option<LastDeclareJob>>,
    jds_connection_state: watch::Receiver<bool>,
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
        sender: TSender<EitherFrame>,
        receiver: TReceiver<EitherFrame>,
        jds_connection_state: watch::Receiver<bool>,
        up: Arc<Mutex<Upstream>>,
        should_log_when_connected: bool,
    ) -> Result<(Arc<Mutex<Self>>, AbortOnDrop), Error> {
        if should_log_when_connected {
            info!("JD CONNECTED");
        }

        let task_manager = TaskManager::initialize();
        let abortable = task_manager
            .safe_lock(|t| t.get_aborter())
            .map_err(|_| Error::PoisonLock)?
            .ok_or(Error::Unrecoverable)?;
        let self_ = Arc::new(Mutex::new(JobDeclarator {
            sender,
            allocated_tokens: vec![],
            last_allocated_token: None,
            req_ids: Id::new(),
            last_declare_mining_jobs_sent: HashMap::with_capacity(2),
            last_set_new_prev_hash: None,
            future_jobs: HashMap::with_hasher(BuildNoHashHasher::default()),
            up,
            coinbase_tx_prefix: vec![].try_into().expect("Internal error: this operation can not fail because Vec can always be converted into Inner"),
            coinbase_tx_suffix: vec![].try_into().expect("Internal error: this operation can not fail because Vec can always be converted into Inner"),
            set_new_prev_hash_counter: 0,
            jds_connection_state: jds_connection_state.clone(),
            task_manager: task_manager.clone(),
        }));

        Self::allocate_tokens(&self_, 2).await;
        Self::on_upstream_message(self_.clone(), receiver).await?;
        Ok((self_, abortable))
    }

    fn refresh_jds_connection_state(self_mutex: &Arc<Mutex<Self>>) -> Result<(bool, bool), Error> {
        self_mutex
            .safe_lock(|s| {
                let state_changed = s.jds_connection_state.has_changed().unwrap_or(false);
                let connected = if state_changed {
                    let connected = *s.jds_connection_state.borrow_and_update();
                    super::IS_CUSTOM_JOB_SET.store(true, std::sync::atomic::Ordering::Release);
                    if connected {
                        s.reset_jds_session_state();
                        info!("JDS reconnected; cleared pending job declaration state");
                    } else {
                        info!("JDS disconnected; keeping last allocated mining job token");
                    }
                    connected
                } else {
                    *s.jds_connection_state.borrow()
                };
                (connected, state_changed)
            })
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)
    }

    fn reset_jds_session_state(&mut self) {
        self.allocated_tokens.clear();
        self.last_declare_mining_jobs_sent.clear();
        self.future_jobs.clear();
        self.set_new_prev_hash_counter = 0;
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
                if s.last_declare_mining_jobs_sent.len() < 1000 {
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
        // if JDS dropped the connection we simply return the last token seen
        if !Self::refresh_jds_connection_state(self_mutex)?.0 {
            return self_mutex
                .safe_lock(|s| s.last_allocated_token.clone())
                .map_err(|_| Error::JobDeclaratorMutexCorrupted)?
                .ok_or(Error::Unrecoverable);
        }

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
            if now.elapsed().as_secs() > 120 {
                error!(
                    "Failed to set custom job after 2 minutes for new template with id {}",
                    template.template_id
                );
                ProxyState::update_jd_state(JdState::Down);
                return Err(Error::Unrecoverable);
            }
            tokio::task::yield_now().await;
        }
        super::IS_CUSTOM_JOB_SET.store(false, std::sync::atomic::Ordering::Release);
        // now as u64 unix time
        let (id, sender) = self_mutex
            .safe_lock(|s| (s.req_ids.next(), s.sender.clone()))
            .map_err(|_| Error::JobDeclaratorMutexCorrupted)?;

        let template_transactions = tx_list_.to_vec();
        let prioritized_txids = crate::prioritized_transactions::snapshot_txids();
        let mut template_txids = HashSet::with_capacity(template_transactions.len());
        let mut tx_list: Vec<Transaction> = Vec::new();
        let mut tx_ids = vec![];
        for tx in template_transactions {
            let transaction: Result<Transaction, bitcoin::consensus::encode::Error> =
                bitcoin::consensus::deserialize(&tx);
            match transaction {
                Ok(tx) => {
                    template_txids.insert(tx.compute_txid());
                    let w_tx_id: U256 = tx.compute_wtxid().to_raw_hash().to_byte_array().into();
                    tx_list.push(tx);
                    tx_ids.push(w_tx_id);
                }
                Err(_) => {
                    error!("Failed to deserailize transaction");
                    return Err(Error::Unrecoverable);
                }
            }
        }
        let missing_txids = missing_prioritized_txids(&prioritized_txids, &template_txids);
        if !missing_txids.is_empty() {
            tokio::task::spawn(check_missing_prioritized_txids(
                missing_txids,
                template.template_id,
            ));
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
        let (jds_connected, jds_state_changed) = Self::refresh_jds_connection_state(self_mutex)?;
        if !jds_connected || jds_state_changed {
            super::IS_CUSTOM_JOB_SET.store(true, std::sync::atomic::Ordering::Release);
            warn!(
                "JDS unavailable or session changed; skipping DeclareMiningJob for template {}",
                last_declare.template.template_id
            );
            return Ok(());
        }
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
                            error!("Job declarator failed to send message AAAAA");
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
        TaskManager::add_main_task(task_manager, main_task.into())
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
                        s.set_new_prev_hash_counter = s.set_new_prev_hash_counter.saturating_sub(1);
                        Some(None)
                    } else {
                        s.future_jobs
                            .remove(&id)
                            .map(|(job, merkle_path, template, pool_outs)| {
                                s.future_jobs.clear();
                                s.set_new_prev_hash_counter =
                                    s.set_new_prev_hash_counter.saturating_sub(1);
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
            let message = JobDeclaration::AllocateMiningJobToken(AllocateMiningJobToken {
                user_identifier: "todo".to_string().try_into().expect("Infallible operation"),
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
                error!("Job declarator failed to send message BBBB");
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

fn missing_prioritized_txids(
    prioritized_txids: &[Txid],
    template_txids: &HashSet<Txid>,
) -> Vec<Txid> {
    prioritized_txids
        .iter()
        .filter(|txid| !template_txids.contains(*txid))
        .copied()
        .collect()
}

// Prioritized transactions are submitted to bitcoind with a large virtual fee delta so they
// should remain attractive for block templates while they are in the mempool. If such a
// transaction is missing from a template, check getmempoolentry before logging an error: when
// bitcoind no longer has it in the mempool, the most likely explanation is that it was mined.
async fn check_missing_prioritized_txids(missing_txids: Vec<Txid>, template_id: u64) {
    let Some(config) = crate::Configuration::bitcoind_rpc_config() else {
        return;
    };

    let rpc = crate::api::bitcoin_rpc::BitcoindRpc::new(
        config.url,
        config.user,
        config.pwd,
        config.fee_delta,
    );

    for txid in missing_txids {
        match rpc.transaction_in_mempool(&txid.to_string()).await {
            Ok(true) => {
                error!(
                    txid = %txid,
                    template_id,
                    "prioritized transaction is in mempool but missing from template transaction list"
                );
            }
            Ok(false) => {
                crate::prioritized_transactions::remove(&txid);
            }
            Err(e) => {
                warn!(
                    txid = %txid,
                    template_id,
                    error = %e,
                    "failed to check prioritized transaction mempool state"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Txid;
    use std::collections::HashSet;
    use tokio::sync::{mpsc, watch};

    fn txid(value: u8) -> Txid {
        format!("{value:064x}").parse().expect("valid txid")
    }

    fn make_allocate_token(request_id: u32) -> AllocateMiningJobTokenSuccess<'static> {
        AllocateMiningJobTokenSuccess {
            request_id,
            mining_job_token: vec![request_id as u8; 32]
                .try_into()
                .expect("mining job token should fit in B0255"),
            coinbase_output_max_additional_size: 100,
            coinbase_output: Vec::new()
                .try_into()
                .expect("coinbase output should fit in B064K"),
            async_mining_allowed: true,
        }
    }

    fn make_declare_job(request_id: u32) -> DeclareMiningJob<'static> {
        DeclareMiningJob {
            request_id,
            mining_job_token: vec![request_id as u8; 32]
                .try_into()
                .expect("mining job token should fit in B0255"),
            version: 0,
            coinbase_prefix: Vec::new()
                .try_into()
                .expect("coinbase prefix should fit in B064K"),
            coinbase_suffix: Vec::new()
                .try_into()
                .expect("coinbase suffix should fit in B064K"),
            tx_list: Seq064K::new(Vec::new()).expect("transaction list should be empty"),
            excess_data: Vec::new()
                .try_into()
                .expect("excess data should fit in B064K"),
        }
    }

    fn make_new_template(template_id: u64) -> NewTemplate<'static> {
        NewTemplate {
            template_id,
            future_template: true,
            version: 0,
            coinbase_tx_version: 1,
            coinbase_prefix: Vec::new()
                .try_into()
                .expect("coinbase prefix should fit in B0255"),
            coinbase_tx_input_sequence: 0,
            coinbase_tx_value_remaining: 0,
            coinbase_tx_outputs_count: 0,
            coinbase_tx_outputs: Vec::new()
                .try_into()
                .expect("coinbase outputs should fit in B064K"),
            coinbase_tx_locktime: 0,
            merkle_path: Vec::new().into(),
        }
    }

    fn make_last_declare_job(request_id: u32, template_id: u64) -> LastDeclareJob {
        LastDeclareJob {
            declare_job: make_declare_job(request_id),
            template: make_new_template(template_id),
            coinbase_pool_output: vec![1, 2, 3],
            tx_list: Seq064K::new(Vec::new()).expect("transaction list should be empty"),
        }
    }

    async fn make_job_declarator(
        jds_connection_state: watch::Receiver<bool>,
    ) -> Arc<Mutex<JobDeclarator>> {
        let (sender, _receiver) = mpsc::channel(1);
        let (up_sender, _up_receiver) = mpsc::channel(1);
        let up = Upstream::new(0, up_sender)
            .await
            .expect("upstream should initialize");
        Arc::new(Mutex::new(JobDeclarator {
            sender,
            allocated_tokens: vec![],
            last_allocated_token: None,
            req_ids: Id::new(),
            last_declare_mining_jobs_sent: HashMap::with_capacity(2),
            jds_connection_state,
            last_set_new_prev_hash: None,
            set_new_prev_hash_counter: 0,
            future_jobs: HashMap::with_hasher(BuildNoHashHasher::default()),
            up,
            coinbase_tx_prefix: vec![].try_into().expect(
                "Internal error: this operation can not fail because Vec can always be converted into Inner",
            ),
            coinbase_tx_suffix: vec![].try_into().expect(
                "Internal error: this operation can not fail because Vec can always be converted into Inner",
            ),
            task_manager: TaskManager::initialize(),
        }))
    }

    #[test]
    fn no_missing_prioritized_txids_when_all_are_in_template() {
        let a = txid(1);
        let b = txid(2);
        let c = txid(3);
        let prioritized = vec![a, b];
        let template = HashSet::from([a, b, c]);

        assert!(missing_prioritized_txids(&prioritized, &template).is_empty());
    }

    #[test]
    fn returns_only_prioritized_txids_missing_from_template() {
        let a = txid(1);
        let b = txid(2);
        let c = txid(3);
        let prioritized = vec![a, b, c];
        let template = HashSet::from([a, c]);

        assert_eq!(missing_prioritized_txids(&prioritized, &template), vec![b]);
    }

    #[tokio::test]
    async fn jds_reconnect_clears_future_jobs_and_prev_hash_counter() {
        let (state_sender, state_receiver) = watch::channel(true);
        let job_declarator = make_job_declarator(state_receiver).await;

        job_declarator
            .safe_lock(|s| {
                s.allocated_tokens.push(make_allocate_token(1));
                s.last_declare_mining_jobs_sent
                    .insert(2, Some(make_last_declare_job(2, 10)));
                s.future_jobs.insert(
                    10,
                    (
                        make_declare_job(2),
                        Vec::new().into(),
                        make_new_template(10),
                        vec![4, 5, 6],
                    ),
                );
                s.set_new_prev_hash_counter = 2;
            })
            .expect("job declarator lock should not be poisoned");

        state_sender.send_replace(false);
        assert_eq!(
            JobDeclarator::refresh_jds_connection_state(&job_declarator)
                .expect("disconnect state refresh should succeed"),
            (false, true)
        );

        state_sender.send_replace(true);
        assert_eq!(
            JobDeclarator::refresh_jds_connection_state(&job_declarator)
                .expect("reconnect state refresh should succeed"),
            (true, true)
        );

        job_declarator
            .safe_lock(|s| {
                assert!(s.allocated_tokens.is_empty());
                assert!(s.last_declare_mining_jobs_sent.is_empty());
                assert!(s.future_jobs.is_empty());
                assert_eq!(s.set_new_prev_hash_counter, 0);
            })
            .expect("job declarator lock should not be poisoned");
    }
}
