mod coinbase_output;
mod task_manager;
use crate::proxy_state::{DownstreamType, JdState, TpState};
use crate::shared::utils::AbortOnDrop;
use crate::{
    jd_client::mining_downstream::{DownstreamJob, DownstreamMiningNode as Downstream},
    proxy_state::ProxyState,
};

use super::{error::Error, job_declarator::JobDeclarator};
use bitcoin::{consensus::Encodable, TxOut};
#[cfg(test)]
use coinbase_output::coinbase_output_capacity;
use coinbase_output::{
    retained_capacity, CoinbaseOutputCapacity, CoinbaseOutputReservation, ReservationUpdate,
};
use demand_sv2_connection::noise_connection_tokio::Connection;
use demand_sv2_connection::{HandshakeRole, Initiator, StandardEitherFrame, StandardSv2Frame};
use key_utils::Secp256k1PublicKey;
use roles_logic_sv2::{
    handlers::{template_distribution::ParseServerTemplateDistributionMessages, SendTo_},
    job_declaration_sv2::AllocateMiningJobTokenSuccessOwned,
    parsers::{PoolMessagesOwned, TemplateDistributionOwned},
    template_distribution_sv2::{
        CoinbaseOutputConstraints, NewTemplateOwned, RequestTransactionDataOwned,
        SubmitSolutionOwned,
    },
    utils::Mutex,
};
use setup_connection::SetupConnectionHandler;
use std::{convert::TryInto, net::SocketAddr, sync::Arc};
use task_manager::TaskManager;
use tokio::sync::mpsc::{Receiver as TReceiver, Sender as TSender};
use tracing::{error, info, warn};

mod message_handler;
mod setup_connection;

pub type SendTo = SendTo_<roles_logic_sv2::parsers::TemplateDistributionOwned, ()>;
pub type Message = PoolMessagesOwned;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;

const DIFFICULTY_COMMITMENT_OUTPUT_SIZE: u32 = 21;

pub struct TemplateRx {
    sender: TSender<EitherFrame>,
    /// Allows the tp recv to communicate back to the main thread any status updates
    /// that would interest the main thread for error handling
    jd: Option<Arc<Mutex<super::job_declarator::JobDeclarator>>>,
    down: Arc<Mutex<Downstream>>,
    new_template_message: Option<NewTemplateOwned>,
    miner_coinbase_output: Vec<u8>,
    merge_mining_enabled: bool,
    coinbase_output_capacity: Arc<Mutex<CoinbaseOutputCapacity>>,
    test_only_do_not_send_solution_to_tp: bool,
}

fn coinbase_capacity_with_merge_mining(base: u32, enabled: bool) -> (u32, Option<usize>) {
    if !enabled {
        return (base, None);
    }
    match base.checked_add(crate::merge_mining::RESERVED_COINBASE_OUTPUT_BYTES) {
        Some(advertised) => (
            advertised,
            Some(crate::merge_mining::RESERVED_COINBASE_OUTPUT_BYTES as usize),
        ),
        None => (base, None),
    }
}

fn will_publish_template(
    wait_for_last_template_to_be_completed: bool,
    go_to_next_template: bool,
    discard_last_and_use_this: bool,
) -> bool {
    wait_for_last_template_to_be_completed || (!go_to_next_template && discard_last_and_use_this)
}

impl TemplateRx {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        address: SocketAddr,
        solution_receiver: TReceiver<SubmitSolutionOwned>,
        jd: Option<Arc<Mutex<super::job_declarator::JobDeclarator>>>,
        down: Arc<Mutex<Downstream>>,
        miner_coinbase_outputs: Vec<TxOut>,
        authority_public_key: Option<Secp256k1PublicKey>,
        test_only_do_not_send_solution_to_tp: bool,
    ) -> Result<AbortOnDrop, Error> {
        let mut encoded_outputs = vec![];
        miner_coinbase_outputs
            .consensus_encode(&mut encoded_outputs)
            .expect("Invalid coinbase output in config");
        let stream = tokio::net::TcpStream::connect(address)
            .await
            .map_err(Error::Io)?;

        let initiator = match authority_public_key {
            Some(pub_key) => Initiator::from_raw_k(pub_key.into_bytes()),
            None => Initiator::without_pk(),
        };

        let initiator = match initiator {
            Ok(init) => init,
            Err(_) => {
                error!("Impossible to connect to TP, wait a few seconds and retry");
                return Err(Error::Unrecoverable);
            }
        };

        let (mut receiver, mut sender, _, _) =
            match Connection::new(stream, HandshakeRole::Initiator(initiator)).await {
                Ok((receiver, sender, abortable, aborthandle)) => {
                    (receiver, sender, abortable, aborthandle)
                }
                Err(_) => {
                    error!("Impossible to connect to TP, wait a few seconds and retry");
                    return Err(Error::Unrecoverable);
                }
            };

        info!("Template Receiver try to set up connection");
        if let Err(e) = SetupConnectionHandler::setup(&mut receiver, &mut sender, address).await {
            error!("Impossible to connect to TP, wait a few seconds and retry");
            return Err(e);
        };

        info!("Template Receiver connection set up");

        let pool = crate::ACTIVE_POOL_ADDRESS
            .safe_lock(|address| *address)
            .map_err(|_| Error::TemplateRxMutexCorrupted)?;

        let self_mutex = Arc::new(Mutex::new(Self {
            sender: sender.clone(),
            jd,
            down,
            new_template_message: None,
            miner_coinbase_output: encoded_outputs,
            merge_mining_enabled: crate::merge_mining::enabled(),
            coinbase_output_capacity: retained_capacity(address, pool)?,
            test_only_do_not_send_solution_to_tp,
        }));

        let task_manager = TaskManager::initialize();
        let abortable = task_manager
            .safe_lock(|t| t.get_aborter())
            .map_err(|_| Error::TemplateRxMutexCorrupted)?
            .ok_or(Error::TemplateRxTaskManagerFailed)?;

        let on_new_solution_task =
            tokio::task::spawn(Self::on_new_solution(self_mutex.clone(), solution_receiver));
        TaskManager::add_on_new_solution(task_manager.clone(), on_new_solution_task.into())
            .await
            .map_err(|_| Error::TemplateRxTaskManagerFailed)?;
        let main_task = match Self::start_templates(self_mutex, receiver).await {
            Ok(main_task) => main_task,
            Err(e) => return Err(e),
        };
        TaskManager::add_main_task(task_manager, main_task)
            .await
            .map_err(|_| Error::TemplateRxTaskManagerFailed)?;

        Ok(abortable)
    }

    pub async fn send(self_: &Arc<Mutex<Self>>, sv2_frame: StdFrame) {
        let either_frame = sv2_frame.into();
        let sender_to_tp = match self_.safe_lock(|self_| self_.sender.clone()) {
            Ok(sender_to_tp) => sender_to_tp,
            Err(e) => {
                // Update global tp state to down
                error!("{e}");
                ProxyState::update_tp_state(TpState::Down);
                return;
            }
        };
        if sender_to_tp.send(either_frame).await.is_err() {
            error!("Failed to send msg to tp");
            // Update global tp state to down
            ProxyState::update_tp_state(TpState::Down);
        }
    }

    pub async fn send_max_coinbase_size(self_mutex: &Arc<Mutex<Self>>, size: u32, sigops: u16) {
        let coinbase_output_data_size = PoolMessagesOwned::TemplateDistribution(
            TemplateDistributionOwned::CoinbaseOutputConstraints(CoinbaseOutputConstraints {
                coinbase_output_max_additional_size: size
                    .saturating_add(DIFFICULTY_COMMITMENT_OUTPUT_SIZE),
                coinbase_output_max_additional_sigops: sigops,
            }),
        );
        let frame: StdFrame = coinbase_output_data_size.try_into().expect("Internal error: this operation can not fail because PoolMessages::TemplateDistribution can always be converted into StdFrame");

        Self::send(self_mutex, frame).await;
    }

    pub async fn send_tx_data_request(
        self_mutex: &Arc<Mutex<Self>>,
        new_template: NewTemplateOwned,
    ) {
        let tx_data_request = PoolMessagesOwned::TemplateDistribution(
            TemplateDistributionOwned::RequestTransactionData(RequestTransactionDataOwned {
                template_id: new_template.template_id,
            }),
        );
        let frame: StdFrame =  tx_data_request.try_into().expect("Internal error: this operation can not fail because PoolMessages::TemplateDistribution can always be converted into StdFrame");

        Self::send(self_mutex, frame).await
    }

    async fn get_last_token(
        jd: Option<Arc<Mutex<JobDeclarator>>>,
        miner_coinbase_output: &[u8],
    ) -> Option<AllocateMiningJobTokenSuccessOwned> {
        if let Some(jd) = jd {
            match super::job_declarator::JobDeclarator::get_last_token(&jd).await {
                Ok(last_token) => Some(last_token),
                Err(e) => {
                    error!("Failed to get last token: {e:?}");
                    None
                }
            }
        } else {
            Some(AllocateMiningJobTokenSuccessOwned {
                request_id: 0,
                mining_job_token: vec![0; 32].try_into().expect("Internal error: this operation can not fail because the vec![0; 32] can always be converted into Inner"),
                coinbase_outputs: miner_coinbase_output.to_vec().try_into().expect("Internal error: this operation can not fail because the Vec can always be converted into Inner"),
            })
        }
    }

    pub async fn start_templates(
        self_mutex: Arc<Mutex<Self>>,
        mut receiver: TReceiver<EitherFrame>,
    ) -> Result<AbortOnDrop, Error> {
        let jd = self_mutex
            .safe_lock(|s| s.jd.clone())
            .map_err(|_| Error::TemplateRxMutexCorrupted)?;

        let down = self_mutex
            .safe_lock(|s| s.down.clone())
            .map_err(|_| Error::JdClientDownstreamMutexCorrupted)?;
        let mut coinbase_reservation = CoinbaseOutputReservation::new(
            self_mutex
                .safe_lock(|s| s.coinbase_output_capacity.clone())
                .map_err(|_| Error::TemplateRxMutexCorrupted)?,
        );
        let mut last_token = None;
        let miner_coinbase_output = self_mutex
            .safe_lock(|s| s.miner_coinbase_output.clone())
            .map_err(|_| Error::TemplateRxMutexCorrupted)?;
        let miner_name = crate::config::Configuration::miner_name();
        let tag_prefix = crate::config::Configuration::tag_prefix();
        let merge_mining_enabled = self_mutex
            .safe_lock(|template_rx| template_rx.merge_mining_enabled)
            .map_err(|_| Error::TemplateRxMutexCorrupted)?;
        if merge_mining_enabled {
            crate::merge_mining::global().begin_template_session();
        }
        let main_task = {
            let self_mutex = self_mutex.clone();
            //? check
            tokio::task::spawn(async move {
                // Send CoinbaseOutputConstraints size to TP
                let mut pending_new_template: Option<NewTemplateOwned> = None;
                let mut pending_tx_data_template_id: Option<u64> = None;
                let mut pending_downstream_job: Option<DownstreamJob> = None;
                let mut pending_template_generation: Option<u64> = None;
                let mut merge_mining_reserved_bytes = None;
                let mut declaration_tasks = tokio::task::JoinSet::new();
                loop {
                    if last_token.is_none() {
                        let jd = match self_mutex.safe_lock(|s| s.jd.clone()) {
                            Ok(jd) => jd,
                            Err(_) => {
                                error!("Job declarator mutex poisoned!");
                                ProxyState::update_jd_state(JdState::Down);
                                break;
                            }
                        };
                        last_token =
                            Some(Self::get_last_token(jd, &miner_coinbase_output[..]).await);
                        let reservation = match last_token.as_ref() {
                            Some(Some(last_token)) => {
                                match coinbase_reservation
                                    .check_token(last_token.coinbase_outputs.as_bytes())
                                {
                                    Ok(reservation) => reservation,
                                    Err(error) => {
                                        error!(?error, "Invalid pool coinbase outputs");
                                        ProxyState::update_jd_state(JdState::Down);
                                        break;
                                    }
                                }
                            }
                            Some(None) => break,
                            None => break,
                        };
                        match reservation {
                            ReservationUpdate::Advertise(capacity) => {
                                let (advertised_size, reserved_bytes) =
                                    coinbase_capacity_with_merge_mining(
                                        capacity.size,
                                        merge_mining_enabled,
                                    );
                                merge_mining_reserved_bytes = reserved_bytes;
                                if merge_mining_enabled && reserved_bytes.is_none() {
                                    warn!(
                                        "Cannot reserve merge-mining coinbase output capacity; continuing with Bitcoin-only templates"
                                    );
                                }
                                Self::send_max_coinbase_size(
                                    &self_mutex,
                                    advertised_size,
                                    capacity.sigops,
                                )
                                .await;
                            }
                            ReservationUpdate::Reconnect => {
                                info!("Pool coinbase outputs need a larger template reservation; restarting the mining session");
                                super::request_coinbase_reservation_restart();
                                std::future::pending::<()>().await;
                            }
                            ReservationUpdate::Unchanged => {}
                        }
                    }

                    let ready_for_new_template = super::IS_NEW_TEMPLATE_HANDLED
                        .load(std::sync::atomic::Ordering::Acquire)
                        && pending_tx_data_template_id.is_none();
                    let use_pending_template =
                        ready_for_new_template && pending_new_template.is_some();
                    let (next_message_to_send, frame_for_log) = if use_pending_template {
                        let pending = pending_new_template
                            .take()
                            .expect("pending template missing");
                        (
                            Ok(SendTo::None(Some(TemplateDistributionOwned::NewTemplate(
                                pending,
                            )))),
                            None,
                        )
                    } else {
                        let received = match receiver.recv().await {
                            Some(received) => received,
                            None => {
                                error!("Failed to receive msg");
                                ProxyState::update_tp_state(TpState::Down);
                                break;
                            }
                        };
                        let frame: Result<StdFrame, _> = received.try_into();
                        let mut frame = match frame {
                            Ok(frame) => frame,
                            Err(_) => {
                                error!("Failed to covert TP message to StdFrame");
                                // Update global tp state to down
                                ProxyState::update_tp_state(TpState::Down);
                                continue;
                            }
                        };
                        let message_type = match crate::jd_client::protocol::message_type(
                            frame.get_header(),
                            |id| {
                                roles_logic_sv2::parsers::TemplateDistributionTypes::try_from(id)
                                .map(roles_logic_sv2::parsers::TemplateDistributionTypes::channel_bit)
                            },
                        ) {
                            Ok(message_type) => message_type,
                            Err(error) => {
                                warn!(?error, "Invalid Template Distribution frame header");
                                ProxyState::update_tp_state(TpState::Down);
                                break;
                            }
                        };
                        let payload = frame.payload();

                        let next_message_to_send =
                            ParseServerTemplateDistributionMessages::handle_message_template_distribution(
                                self_mutex.clone(),
                                message_type,
                                payload,
                            );
                        (next_message_to_send, Some(frame))
                    };

                    let mut frame_for_log = frame_for_log;
                    match next_message_to_send {
                        Ok(SendTo::None(m)) => {
                            match m {
                                // Send the new template along with the token to the JD so that JD can
                                // declare the mining job
                                Some(TemplateDistributionOwned::NewTemplate(mut m)) => {
                                    let can_process_template = super::IS_NEW_TEMPLATE_HANDLED
                                        .load(std::sync::atomic::Ordering::Acquire)
                                        && pending_tx_data_template_id.is_none();
                                    if !can_process_template {
                                        pending_new_template = Some(m);
                                        continue;
                                    }
                                    if let Err(e) = crate::shared::miner_tag::tag_new_template(
                                        &mut m,
                                        &tag_prefix,
                                        miner_name.as_deref(),
                                    ) {
                                        error!("Invalid miner tag coinbase script data: {e}");
                                        ProxyState::update_tp_state(TpState::Down);
                                        break;
                                    }
                                    let new_phash = super::IS_NEW_PHASH_ARRIVED
                                        .load(std::sync::atomic::Ordering::Acquire);
                                    let last_is_future = match self_mutex
                                        .safe_lock(|t| t.new_template_message.clone())
                                    {
                                        Ok(Some(new_template_message)) => {
                                            new_template_message.future_template
                                        }
                                        Ok(None) => false,
                                        Err(e) => {
                                            // Update global tp state to down
                                            error!("TemplateRx mutex poisoned: {e}");
                                            ProxyState::update_tp_state(TpState::Down);
                                            break;
                                        }
                                    };
                                    // THIS CODE ASSUME THAT TP SEND FUTURE ONLY BEFORE
                                    // SNPH
                                    // last_is_future	this_future	new_phash wait  next   discard
                                    // true	            true	    true	  true	false
                                    // true	            true	    false	  true	false
                                    // true	            false	    true	  true	true
                                    // true	            false	    false	  true	false
                                    // false	        true	    true	  false	false  true
                                    // false	        true	    false	  true	false
                                    // false	        false	    true	  false	true
                                    // false	        false	    false	  true	false
                                    //

                                    #[allow(clippy::nonminimal_bool)]
                                    let wait_for_last_template_to_be_completed =
                                        last_is_future && new_phash || !new_phash;
                                    let go_to_next_template = !m.future_template && new_phash;

                                    let discard_last_and_use_this =
                                        !last_is_future && !wait_for_last_template_to_be_completed;

                                    let will_publish_template = will_publish_template(
                                        wait_for_last_template_to_be_completed,
                                        go_to_next_template,
                                        discard_last_and_use_this,
                                    );

                                    if will_publish_template {
                                        pending_downstream_job = None;
                                        pending_template_generation = None;
                                        if let Err(error) =
                                            Downstream::wait_for_mining_channel(&down).await
                                        {
                                            error!(%error, "Failed to wait for mining channel setup");
                                            ProxyState::update_downstream_state(
                                                DownstreamType::JdClientMiningDownstream,
                                            );
                                            break;
                                        }
                                        if let Err(error) =
                                            Downstream::apply_difficulty_commitment(&down, &mut m)
                                        {
                                            error!(%error, "Failed to add difficulty commitment");
                                            ProxyState::update_downstream_state(
                                                DownstreamType::JdClientMiningDownstream,
                                            );
                                            break;
                                        }
                                        if let Some(reserved_bytes) = merge_mining_reserved_bytes {
                                            let pristine_template = m.clone();
                                            let pool_output_count = last_token
                                                .as_ref()
                                                .and_then(|token| token.as_ref())
                                                .and_then(|token| {
                                                    Downstream::decode_pool_coinbase_outputs(
                                                        token.coinbase_outputs.as_ref(),
                                                    )
                                                    .ok()
                                                })
                                                .map_or(usize::MAX, |outputs| outputs.len());
                                            match crate::merge_mining::global()
                                                .apply_to_template_with_pool_output_count(
                                                    &mut m,
                                                    reserved_bytes,
                                                    pool_output_count,
                                                ) {
                                                Ok(true) => {
                                                    pending_template_generation =
                                                        crate::merge_mining::global()
                                                            .template_generation(m.template_id);
                                                    if let Some(template_generation) =
                                                        pending_template_generation
                                                    {
                                                        let pool_output = last_token
                                                            .as_ref()
                                                            .and_then(|token| token.as_ref())
                                                            .map(|token| {
                                                                token
                                                                    .coinbase_outputs
                                                                    .as_bytes()
                                                                    .to_vec()
                                                            });
                                                        let preview = pool_output
                                                            .as_deref()
                                                            .ok_or(crate::jd_client::error::Error::Unrecoverable)
                                                            .and_then(|pool_output| {
                                                                Downstream::preview_template_job(
                                                                    &down,
                                                                    m.clone(),
                                                                    pool_output,
                                                                )
                                                                .map(|_| ())
                                                            });
                                                        if let Err(error) = preview {
                                                            crate::merge_mining::global()
                                                                .discard_template_generation(
                                                                    template_generation,
                                                                );
                                                            pending_template_generation = None;
                                                            m = pristine_template;
                                                            warn!(
                                                                template_id = m.template_id,
                                                                %error,
                                                                "RSK candidate job preflight failed; using the pristine Bitcoin template"
                                                            );
                                                        } else {
                                                            info!(
                                                                template_id = m.template_id,
                                                                "added RSK commitment to canonical Bitcoin mining job"
                                                            );
                                                        }
                                                    } else {
                                                        m = pristine_template;
                                                        warn!(
                                                            template_id = m.template_id,
                                                            "RSK context was unavailable; using the pristine Bitcoin template"
                                                        );
                                                    }
                                                }
                                                Ok(false) => {}
                                                Err(error) => {
                                                    m = pristine_template;
                                                    warn!(
                                                        template_id = m.template_id,
                                                        %error,
                                                        "RSK commitment was not applied; using the pristine Bitcoin template"
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    if wait_for_last_template_to_be_completed {
                                        info!("wait_for_last_template_to_be_completed");
                                        if new_phash {
                                            super::IS_NEW_PHASH_ARRIVED
                                                .store(false, std::sync::atomic::Ordering::Release);
                                        }
                                        // See coment on the definition of the global for memory
                                        // ordering
                                        super::IS_NEW_TEMPLATE_HANDLED
                                            .store(false, std::sync::atomic::Ordering::Release);
                                        pending_tx_data_template_id = Some(m.template_id);
                                        Self::send_tx_data_request(&self_mutex, m.clone()).await;
                                        if self_mutex
                                            .safe_lock(|t| t.new_template_message = Some(m.clone()))
                                            .is_err()
                                        {
                                            error!("TemplateRx Mutex is corrupt");
                                            // Update global tp state to down
                                            ProxyState::update_tp_state(TpState::Down);
                                            break;
                                        };

                                        let token = match last_token.clone() {
                                            Some(Some(token)) => token,
                                            Some(None) => break,
                                            None => break,
                                        };
                                        let pool_output =
                                            token.coinbase_outputs.as_bytes().to_vec();
                                        match Downstream::on_new_template(
                                            &down,
                                            m.clone(),
                                            &pool_output[..],
                                            pending_template_generation,
                                        )
                                        .await
                                        {
                                            Ok(job) => pending_downstream_job = job,
                                            Err(e) => {
                                                super::IS_NEW_TEMPLATE_HANDLED.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                error!("{e:?}");
                                                // Update global downstream state to down
                                                ProxyState::update_downstream_state(
                                                    DownstreamType::JdClientMiningDownstream,
                                                );
                                            }
                                        }
                                    } else if go_to_next_template {
                                        // last_is_future	this_future	new_phash wait  next   discard
                                        // 1)true           false	    true	  true	true
                                        // 2)false	        false	    true	  false	true
                                        // D)false	        true	    true	  false	false  true
                                        //
                                        // 1 ->  not possible cause we chak wait before
                                        //   so ok
                                        // 2 ->  we have to wait fot the next one that
                                        //   will be discard we don't need to change
                                        //   any global
                                        continue;
                                    } else if discard_last_and_use_this {
                                        if new_phash {
                                            super::IS_NEW_PHASH_ARRIVED
                                                .store(false, std::sync::atomic::Ordering::Release);
                                        }
                                        // Here we mark the IS_CUSTOM_JOB_SET as true since the last declared job
                                        // received is invalid if we are still in the process of declaring it we
                                        // want to free it since we are never going to do SetCustomJob for that
                                        // declared job. If we are not doing a declare + set job is already true so
                                        // nothing change.
                                        super::IS_CUSTOM_JOB_SET
                                            .store(true, std::sync::atomic::Ordering::Release);
                                        // See coment on the definition of the global for memory
                                        // ordering
                                        super::IS_NEW_TEMPLATE_HANDLED
                                            .store(false, std::sync::atomic::Ordering::Release);
                                        pending_tx_data_template_id = Some(m.template_id);
                                        Self::send_tx_data_request(&self_mutex, m.clone()).await;
                                        if self_mutex
                                            .safe_lock(|t| t.new_template_message = Some(m.clone()))
                                            .is_err()
                                        {
                                            error!("TemplateRx Mutex is corrupt");
                                            // Update global tp state to down
                                            ProxyState::update_tp_state(TpState::Down);
                                            break;
                                        };

                                        let token = match last_token.clone() {
                                            Some(Some(token)) => token,
                                            Some(None) => break,
                                            None => break,
                                        };
                                        let pool_output =
                                            token.coinbase_outputs.as_bytes().to_vec();
                                        match Downstream::on_new_template(
                                            &down,
                                            m.clone(),
                                            &pool_output[..],
                                            pending_template_generation,
                                        )
                                        .await
                                        {
                                            Ok(job) => pending_downstream_job = job,
                                            Err(e) => {
                                                super::IS_NEW_TEMPLATE_HANDLED.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                error!("{e:?}");
                                                // Update global downstream state to down
                                                ProxyState::update_downstream_state(
                                                    DownstreamType::JdClientMiningDownstream,
                                                );
                                            }
                                        }
                                    } else {
                                        unreachable!();
                                    }
                                }
                                Some(TemplateDistributionOwned::SetNewPrevHash(m)) => {
                                    super::IS_NEW_PHASH_ARRIVED
                                        .store(true, std::sync::atomic::Ordering::Release);
                                    info!("Received SetNewPrevHash, waiting for IS_NEW_TEMPLATE_HANDLED");
                                    // See coment on the definition of the global for memory
                                    // ordering
                                    //
                                    // This add ~2millis of latency, for now I leave it
                                    // here since it means 8*e^-7 % bigger rej rate it
                                    // looks like something acceptable
                                    //
                                    // 8*e^-7 is based on an old formula that model
                                    // rej rate could be a little higher but still
                                    // negligible
                                    while !super::IS_NEW_TEMPLATE_HANDLED
                                        .load(std::sync::atomic::Ordering::Acquire)
                                    {
                                        tokio::task::yield_now().await;
                                    }
                                    info!("IS_NEW_TEMPLATE_HANDLED ok");
                                    if crate::merge_mining::enabled() {
                                        if let Ok(prev_hash) =
                                            <[u8; 32]>::try_from(m.prev_hash.as_bytes().to_vec())
                                        {
                                            crate::merge_mining::global().record_chain_state(
                                                m.template_id,
                                                prev_hash,
                                                m.n_bits,
                                            );
                                        }
                                    }
                                    if let Some(jd) = jd.as_ref() {
                                        if let Err(e) = super::job_declarator::JobDeclarator::on_set_new_prev_hash(
                                            jd.clone(),
                                            m.clone(),
                                        ).await {
                                            error!("{e:?}");
                                            ProxyState::update_jd_state(JdState::Down); break;
                                        };
                                    }
                                    if let Err(e) = Downstream::on_set_new_prev_hash(&down, m).await
                                    {
                                        error!("SetNewPrevHash Error: {e:?}");
                                        // Update global tp state to down
                                        ProxyState::update_tp_state(TpState::Down);
                                        break;
                                    };
                                }

                                Some(TemplateDistributionOwned::RequestTransactionDataSuccess(
                                    m,
                                )) => {
                                    if pending_tx_data_template_id != Some(m.template_id) {
                                        warn!(
                                            "Ignoring RequestTransactionDataSuccess for stale template id {}",
                                            m.template_id
                                        );
                                        continue;
                                    }
                                    let new_template_message = match self_mutex
                                        .safe_lock(|t| t.new_template_message.clone())
                                    {
                                        Ok(new_template_message) => new_template_message,
                                        Err(e) => {
                                            error!("TemplateRx mutex poisoned: {e}");
                                            ProxyState::update_tp_state(TpState::Down);
                                            pending_tx_data_template_id = None;
                                            pending_downstream_job = None;
                                            pending_template_generation = None;
                                            last_token = None;
                                            continue;
                                        }
                                    };
                                    let new_template_message = match new_template_message {
                                        Some(new_template_message) => new_template_message,
                                        None => {
                                            ProxyState::update_tp_state(TpState::Down);
                                            pending_tx_data_template_id = None;
                                            pending_downstream_job = None;
                                            pending_template_generation = None;
                                            last_token = None;
                                            continue;
                                        }
                                    };
                                    if new_template_message.template_id != m.template_id {
                                        warn!(
                                            "Ignoring RequestTransactionDataSuccess for template id {} with mismatched active template",
                                            m.template_id
                                        );
                                        pending_tx_data_template_id = None;
                                        pending_downstream_job = None;
                                        pending_template_generation = None;
                                        last_token = None;
                                        continue;
                                    }
                                    let transactions = m.transaction_list.into_inner();
                                    let excess_data = m.excess_data;
                                    if merge_mining_enabled {
                                        if let Some(template_generation) =
                                            pending_template_generation
                                        {
                                            crate::merge_mining::global().record_transaction_count(
                                                template_generation,
                                                transactions.len(),
                                            );
                                        }
                                    }
                                    let token = match last_token.take() {
                                        Some(Some(token)) => token,
                                        Some(None) => break,
                                        None => break,
                                    };
                                    let downstream_job = pending_downstream_job.take();
                                    pending_template_generation = None;
                                    pending_tx_data_template_id = None;
                                    let jd = jd.clone();
                                    while declaration_tasks.try_join_next().is_some() {}
                                    declaration_tasks.spawn(async move {
                                        let transactions_data =
                                            match binary_sv2::Seq064KOwned::new(transactions) {
                                                Ok(transactions) => transactions,
                                                Err(error) => {
                                                    error!(
                                                        ?error,
                                                        "Too many template transactions"
                                                    );
                                                    ProxyState::update_jd_state(JdState::Down);
                                                    return;
                                                }
                                            };
                                        let mining_token =
                                            token.mining_job_token.as_bytes().to_vec();
                                        let pool_coinbase_out =
                                            token.coinbase_outputs.as_bytes().to_vec();
                                        if let (Some(jd), Some(downstream_job)) =
                                            (jd.as_ref(), downstream_job)
                                        {
                                            if let Err(e) = super::job_declarator::JobDeclarator::on_new_template(
                                                jd,
                                                new_template_message,
                                                mining_token,
                                                transactions_data,
                                                excess_data,
                                                pool_coinbase_out,
                                                downstream_job,
                                            )
                                            .await {
                                                error!("{e:?}");
                                                ProxyState::update_downstream_state(DownstreamType::JdClientMiningDownstream);
                                            };
                                        }
                                    });
                                }
                                Some(TemplateDistributionOwned::RequestTransactionDataError(m)) => {
                                    if pending_tx_data_template_id == Some(m.template_id) {
                                        pending_tx_data_template_id = None;
                                        pending_downstream_job = None;
                                        pending_template_generation = None;
                                        last_token = None;
                                    }
                                    warn!("The prev_hash of the template requested to Template Provider no longer points to the latest tip. Continuing work on the updated template.")
                                }
                                _ => {
                                    if let Some(mut frame) = frame_for_log.take() {
                                        error!("{:?}", frame);
                                        error!("{:?}", frame.payload());
                                        error!("{:?}", frame.get_header());
                                    }
                                    std::process::exit(1);
                                }
                            }
                        }
                        Ok(m) => {
                            error!("Unexpected next message {:?}", m);
                            if let Some(mut frame) = frame_for_log.take() {
                                error!("{:?}", frame);
                                error!("{:?}", frame.payload());
                                error!("{:?}", frame.get_header());
                            }
                            std::process::exit(1);
                        }
                        Err(roles_logic_sv2::Error::NoValidTemplate(_)) => {
                            // This can happen when we require data for a template, the TP
                            // already sent a new set prev hash, but the client did not saw it
                            // yet
                            error!("Required txs for a non valid template id, ignoring it");
                        }
                        Err(e) => {
                            error!("Impossible to get next message {:?}", e);
                            if let Some(mut frame) = frame_for_log.take() {
                                error!("{:?}", frame);
                                error!("{:?}", frame.payload());
                                error!("{:?}", frame.get_header());
                            }
                            std::process::exit(1);
                        }
                    }
                }
            })
        };
        Ok(main_task.into())
    }

    async fn on_new_solution(self_: Arc<Mutex<Self>>, mut rx: TReceiver<SubmitSolutionOwned>) {
        while let Some(solution) = rx.recv().await {
            let test_only = match self_.safe_lock(|s| s.test_only_do_not_send_solution_to_tp) {
                Ok(test_only) => test_only,
                Err(e) => {
                    error!("{e:?}");
                    // TemplateRx mutex poisoned
                    // Update global tp state to down
                    ProxyState::update_tp_state(TpState::Down);
                    return;
                }
            };

            if !test_only {
                let sv2_frame: StdFrame = PoolMessagesOwned::TemplateDistribution(
                    TemplateDistributionOwned::SubmitSolution(solution),
                )
                .try_into()
                .expect("Internal error: this operation can not fail because PoolMessages::TemplateDistribution can always be converted into StdFrame");
                Self::send(&self_, sv2_frame).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jd_client::{IS_CUSTOM_JOB_SET, IS_NEW_PHASH_ARRIVED, IS_NEW_TEMPLATE_HANDLED};
    use binary_sv2::{B016MOwned, B0255Owned, B064KOwned, Seq0255Owned, Seq064KOwned, U256Owned};
    use bitcoin::consensus::Encodable;
    use roles_logic_sv2::template_distribution_sv2::RequestTransactionDataSuccessOwned;
    use std::convert::TryInto;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::Duration;

    #[test]
    fn native_coinbase_capacity_counts_serialized_outputs_and_sigops() {
        let outputs = vec![
            TxOut {
                value: bitcoin::Amount::ZERO,
                script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0xac]),
            },
            TxOut {
                value: bitcoin::Amount::ZERO,
                script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0xae]),
            },
        ];
        assert_eq!(
            coinbase_output_capacity(&bitcoin::consensus::serialize(&outputs)).unwrap(),
            (20, 84)
        );
        assert!(coinbase_output_capacity(&bitcoin::consensus::serialize(&outputs[0])).is_err());
    }

    #[test]
    fn native_coinbase_capacity_reserves_p2pkh_sigop_cost() {
        let output = TxOut {
            value: bitcoin::Amount::ZERO,
            script_pubkey: bitcoin::ScriptBuf::new_p2pkh(&bitcoin::PubkeyHash::from_raw_hash(
                bitcoin::hashes::Hash::from_byte_array([1; 20]),
            )),
        };
        assert_eq!(
            coinbase_output_capacity(&bitcoin::consensus::serialize(&vec![output])).unwrap(),
            (34, 4)
        );
    }

    #[test]
    fn native_coinbase_capacity_rejects_sigop_cost_overflow() {
        let output = TxOut {
            value: bitcoin::Amount::ZERO,
            script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0xac; 16_384]),
        };
        assert!(coinbase_output_capacity(&bitcoin::consensus::serialize(&vec![output])).is_err());
    }

    #[test]
    fn native_coinbase_capacity_accepts_largest_representable_sigop_cost() {
        let output = TxOut {
            value: bitcoin::Amount::ZERO,
            script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0xac; 16_383]),
        };
        let (_, sigops) =
            coinbase_output_capacity(&bitcoin::consensus::serialize(&vec![output])).unwrap();
        assert_eq!(sigops, 65_532);
    }

    #[test]
    fn merge_mining_capacity_is_reserved_only_when_configured() {
        assert_eq!(coinbase_capacity_with_merge_mining(1, false), (1, None));
        assert_eq!(
            coinbase_capacity_with_merge_mining(1, true),
            (
                1 + crate::merge_mining::RESERVED_COINBASE_OUTPUT_BYTES,
                Some(crate::merge_mining::RESERVED_COINBASE_OUTPUT_BYTES as usize)
            )
        );
        assert_eq!(
            coinbase_capacity_with_merge_mining(u32::MAX, true),
            (u32::MAX, None)
        );
    }

    #[test]
    fn merge_mining_preparation_matches_template_publication_precedence() {
        let cases = [
            (true, true, true, true),
            (true, true, false, true),
            (true, false, true, true),
            (true, false, false, true),
            (false, true, true, true),
            (false, true, false, true),
            (false, false, true, false),
            (false, false, false, true),
        ];

        for (last_is_future, current_is_future, new_phash, expected) in cases {
            let wait_for_last_template_to_be_completed = last_is_future || !new_phash;
            let go_to_next_template = !current_is_future && new_phash;
            let discard_last_and_use_this =
                !last_is_future && !wait_for_last_template_to_be_completed;

            assert_eq!(
                will_publish_template(
                    wait_for_last_template_to_be_completed,
                    go_to_next_template,
                    discard_last_and_use_this,
                ),
                expected,
                "last_is_future={last_is_future}, current_is_future={current_is_future}, new_phash={new_phash}"
            );
        }
    }

    fn make_new_template(template_id: u64, future_template: bool) -> NewTemplateOwned {
        let coinbase_prefix: B0255Owned = vec![2, 33, 1, 0]
            .try_into()
            .expect("coinbase prefix should fit in B0255");
        let coinbase_tx_outputs: B064KOwned = Vec::new()
            .try_into()
            .expect("coinbase outputs should fit in B064K");
        let merkle_path: Seq0255Owned<U256Owned> =
            Vec::new().try_into().expect("empty merkle path");
        NewTemplateOwned {
            template_id,
            future_template,
            version: 0,
            coinbase_tx_version: 2,
            coinbase_prefix,
            coinbase_tx_input_sequence: 0,
            coinbase_tx_value_remaining: 0,
            coinbase_tx_outputs_count: 0,
            coinbase_tx_outputs,
            coinbase_tx_locktime: 0,
            merkle_path,
        }
    }

    fn make_tx_data_success(template_id: u64) -> RequestTransactionDataSuccessOwned {
        let excess_data: B064KOwned = Vec::new()
            .try_into()
            .expect("excess data should fit in B064K");
        let transaction_list: Seq064KOwned<B016MOwned> =
            Seq064KOwned::new(Vec::new()).expect("transaction list should be empty");
        RequestTransactionDataSuccessOwned {
            template_id,
            excess_data,
            transaction_list,
        }
    }

    async fn recv_std_frame(receiver: &mut mpsc::Receiver<EitherFrame>) -> StdFrame {
        let received = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("timed out waiting for frame")
            .expect("channel closed while waiting for frame");
        received.try_into().expect("failed to decode sv2 frame")
    }

    fn with_decoded_message<R>(frame: StdFrame, f: impl FnOnce(PoolMessagesOwned) -> R) -> R {
        let message_type = frame
            .get_header()
            .expect("sv2 frame missing header")
            .msg_type();
        let mut buf = vec![0_u8; frame.encoded_length()];
        frame
            .serialize(&mut buf)
            .expect("failed to serialize sv2 frame");
        let payload = &mut buf[framing_sv2::header::Header::SIZE..];
        let message = roles_logic_sv2::parsers::PoolMessages::try_from((message_type, payload))
            .expect("failed to parse sv2 payload")
            .into_owned();
        f(message)
    }

    async fn send_message(sender: &mpsc::Sender<EitherFrame>, message: PoolMessagesOwned) {
        let frame: StdFrame = message.try_into().expect("failed to encode sv2 frame");
        let len = frame.encoded_length();
        let mut buf = vec![0_u8; len];
        frame
            .serialize(&mut buf)
            .expect("failed to serialize sv2 frame");
        let frame = StdFrame::from_bytes(buf.into()).expect("failed to build serialized frame");
        sender
            .send(frame.into())
            .await
            .expect("failed to send frame");
    }

    async fn expect_tx_data_request(
        receiver: &mut mpsc::Receiver<EitherFrame>,
        expected_template_id: u64,
    ) {
        let frame = recv_std_frame(receiver).await;
        with_decoded_message(frame, |message| match message {
            PoolMessagesOwned::TemplateDistribution(
                TemplateDistributionOwned::RequestTransactionData(m),
            ) => {
                assert_eq!(m.template_id, expected_template_id);
            }
            other => panic!("unexpected message: {other:?}"),
        });
    }

    async fn run_fake_tp(
        to_client: mpsc::Sender<EitherFrame>,
        mut from_client: mpsc::Receiver<EitherFrame>,
        progress_tx: oneshot::Sender<()>,
    ) {
        let frame = recv_std_frame(&mut from_client).await;
        with_decoded_message(frame, |message| match message {
            PoolMessagesOwned::TemplateDistribution(
                TemplateDistributionOwned::CoinbaseOutputConstraints(m),
            ) => {
                assert_eq!(m.coinbase_output_max_additional_size, 43);
            }
            other => panic!("unexpected coinbase size message: {other:?}"),
        });

        send_message(
            &to_client,
            PoolMessagesOwned::TemplateDistribution(TemplateDistributionOwned::NewTemplate(
                make_new_template(1, true),
            )),
        )
        .await;
        expect_tx_data_request(&mut from_client, 1).await;

        send_message(
            &to_client,
            PoolMessagesOwned::TemplateDistribution(TemplateDistributionOwned::NewTemplate(
                make_new_template(2, true),
            )),
        )
        .await;

        send_message(
            &to_client,
            PoolMessagesOwned::TemplateDistribution(TemplateDistributionOwned::NewTemplate(
                make_new_template(3, true),
            )),
        )
        .await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        send_message(
            &to_client,
            PoolMessagesOwned::TemplateDistribution(
                TemplateDistributionOwned::RequestTransactionDataSuccess(make_tx_data_success(1)),
            ),
        )
        .await;
        expect_tx_data_request(&mut from_client, 3).await;

        send_message(
            &to_client,
            PoolMessagesOwned::TemplateDistribution(
                TemplateDistributionOwned::RequestTransactionDataSuccess(make_tx_data_success(2)),
            ),
        )
        .await;

        send_message(
            &to_client,
            PoolMessagesOwned::TemplateDistribution(
                TemplateDistributionOwned::RequestTransactionDataSuccess(make_tx_data_success(3)),
            ),
        )
        .await;

        let _ = progress_tx.send(());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn ignores_stale_tx_data_success_after_new_template() {
        IS_NEW_TEMPLATE_HANDLED.store(true, Ordering::Release);
        IS_CUSTOM_JOB_SET.store(true, Ordering::Release);
        IS_NEW_PHASH_ARRIVED.store(false, Ordering::Release);

        let prev_hook = std::panic::take_hook();
        let panic_seen = Arc::new(AtomicBool::new(false));
        let panic_seen_hook = panic_seen.clone();
        // Capture panics from background tasks.
        std::panic::set_hook(Box::new(move |info| {
            panic_seen_hook.store(true, Ordering::SeqCst);
            eprintln!("{info}");
        }));

        let (tp_to_client_tx, tp_to_client_rx) = mpsc::channel(10);
        let (client_to_tp_tx, client_to_tp_rx) = mpsc::channel(10);
        let (progress_tx, progress_rx) = oneshot::channel();
        let tp_task = tokio::spawn(run_fake_tp(tp_to_client_tx, client_to_tp_rx, progress_tx));

        let (solution_tx, _solution_rx) = mpsc::channel(1);
        let (down_tx, _down_rx) = mpsc::channel(16);
        let down = Arc::new(Mutex::new(Downstream::new(
            down_tx,
            None,
            solution_tx,
            false,
            Vec::new(),
            None,
        )));
        // Model a mining session: templates need a channel and a valid payout output.
        let output = TxOut {
            value: bitcoin::Amount::ZERO,
            script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0x51; 13]),
        };
        let mut factory = roles_logic_sv2::channel_logic::channel_factory::PoolChannelFactory::new(
            Arc::new(Mutex::new(roles_logic_sv2::utils::GroupId::new())),
            roles_logic_sv2::mining::ExtendedExtranonce::new(0..0, 0..4, 4..32),
            roles_logic_sv2::job_creator::JobsCreators::new(32),
            60.0,
            roles_logic_sv2::channel_logic::channel_factory::ExtendedChannelKind::Pool,
            vec![output.clone()],
            vec![],
        )
        .expect("test channel factory");
        factory
            .new_extended_channel(1, 100_000.0, 8)
            .expect("test channel");
        down.safe_lock(|state| state.status = crate::jd_client::mining_downstream::DownstreamMiningNodeStatus::SoloMinerChannelOpend(factory)).expect("test downstream lock");
        let mut encoded_outputs = vec![];
        vec![output]
            .consensus_encode(&mut encoded_outputs)
            .expect("encode payout output");
        let self_mutex = Arc::new(Mutex::new(TemplateRx {
            sender: client_to_tp_tx,
            jd: None,
            down,
            new_template_message: None,
            miner_coinbase_output: encoded_outputs,
            merge_mining_enabled: false,
            coinbase_output_capacity: Arc::new(Mutex::new(CoinbaseOutputCapacity::default())),
            test_only_do_not_send_solution_to_tp: true,
        }));
        let _abortable = TemplateRx::start_templates(self_mutex, tp_to_client_rx)
            .await
            .expect("template receiver start");

        tokio::time::timeout(Duration::from_secs(2), progress_rx)
            .await
            .expect("fake tp did not reach stale tx data send")
            .expect("fake tp progress channel closed");

        let wait_for_panic = async {
            while !panic_seen.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(2), wait_for_panic).await;
        std::panic::set_hook(prev_hook);
        tp_task.abort();
        assert!(
            !panic_seen.load(Ordering::SeqCst),
            "stale RequestTransactionDataSuccess triggers panic"
        );
    }
}
