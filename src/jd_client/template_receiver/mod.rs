mod task_manager;
use crate::proxy_state::{DownstreamType, JdState, TpState};
use crate::shared::utils::AbortOnDrop;
use crate::{
    jd_client::mining_downstream::DownstreamMiningNode as Downstream, proxy_state::ProxyState,
};

use super::{error::Error, job_declarator::JobDeclarator};
use bitcoin::{consensus::Encodable, TxOut};
use codec_sv2::{HandshakeRole, Initiator, StandardEitherFrame, StandardSv2Frame};
use demand_sv2_connection::noise_connection_tokio::Connection;
use key_utils::Secp256k1PublicKey;
use roles_logic_sv2::{
    handlers::{template_distribution::ParseServerTemplateDistributionMessages, SendTo_},
    job_declaration_sv2::AllocateMiningJobTokenSuccess,
    parsers::{PoolMessages, TemplateDistribution},
    template_distribution_sv2::{
        CoinbaseOutputDataSize, NewTemplate, RequestTransactionData, SubmitSolution,
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

pub type SendTo = SendTo_<roles_logic_sv2::parsers::TemplateDistribution<'static>, ()>;
pub type Message = PoolMessages<'static>;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;

pub struct TemplateRx {
    sender: TSender<EitherFrame>,
    /// Allows the tp recv to communicate back to the main thread any status updates
    /// that would interest the main thread for error handling
    jd: Option<Arc<Mutex<super::job_declarator::JobDeclarator>>>,
    down: Arc<Mutex<Downstream>>,
    new_template_message: Option<NewTemplate<'static>>,
    miner_coinbase_output: Vec<u8>,
    test_only_do_not_send_solution_to_tp: bool,
}

impl TemplateRx {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        address: SocketAddr,
        solution_receiver: TReceiver<SubmitSolution<'static>>,
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

        let self_mutex = Arc::new(Mutex::new(Self {
            sender: sender.clone(),
            jd,
            down,
            new_template_message: None,
            miner_coinbase_output: encoded_outputs,
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

    pub async fn send_max_coinbase_size(self_mutex: &Arc<Mutex<Self>>, size: u32) {
        let coinbase_output_data_size = PoolMessages::TemplateDistribution(
            TemplateDistribution::CoinbaseOutputDataSize(CoinbaseOutputDataSize {
                coinbase_output_max_additional_size: size,
            }),
        );
        let frame: StdFrame = coinbase_output_data_size.try_into().expect("Internal error: this operation can not fail because PoolMessages::TemplateDistribution can always be converted into StdFrame");

        Self::send(self_mutex, frame).await;
    }

    pub async fn send_tx_data_request(
        self_mutex: &Arc<Mutex<Self>>,
        new_template: NewTemplate<'static>,
    ) {
        let tx_data_request = PoolMessages::TemplateDistribution(
            TemplateDistribution::RequestTransactionData(RequestTransactionData {
                template_id: new_template.template_id,
            }),
        );
        let frame: StdFrame =  tx_data_request.try_into().expect("Internal error: this operation can not fail because PoolMessages::TemplateDistribution can always be converted into StdFrame");

        Self::send(self_mutex, frame).await
    }

    async fn get_last_token(
        jd: Option<Arc<Mutex<JobDeclarator>>>,
        miner_coinbase_output: &[u8],
    ) -> Option<AllocateMiningJobTokenSuccess<'static>> {
        if let Some(jd) = jd {
            match super::job_declarator::JobDeclarator::get_last_token(&jd).await {
                Ok(last_token) => Some(last_token),
                Err(e) => {
                    error!("Failed to get last token: {e:?}");
                    None
                }
            }
        } else {
            Some(AllocateMiningJobTokenSuccess {
                request_id: 0,
                mining_job_token: vec![0; 32].try_into().expect("Internal error: this operation can not fail because the vec![0; 32] can always be converted into Inner"),
                coinbase_output_max_additional_size: 100,
                coinbase_output: miner_coinbase_output.to_vec().try_into().expect("Internal error: this operation can not fail because the Vec can always be converted into Inner"),
                async_mining_allowed: true,
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
        let mut coinbase_output_max_additional_size_sent = false;
        let mut last_token = None;
        let miner_coinbase_output = self_mutex
            .safe_lock(|s| s.miner_coinbase_output.clone())
            .map_err(|_| Error::TemplateRxMutexCorrupted)?;
        let main_task = {
            let self_mutex = self_mutex.clone();
            //? check
            tokio::task::spawn(async move {
                // Send CoinbaseOutputDataSize size to TP
                let mut pending_new_template: Option<NewTemplate<'static>> = None;
                let mut pending_tx_data_template_id: Option<u64> = None;
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
                    }
                    let coinbase_output_max_additional_size = match last_token.clone() {
                        Some(Some(last_token)) => last_token.coinbase_output_max_additional_size,
                        Some(None) => break,
                        None => break,
                    };

                    if !coinbase_output_max_additional_size_sent {
                        coinbase_output_max_additional_size_sent = true;
                        Self::send_max_coinbase_size(
                            &self_mutex,
                            coinbase_output_max_additional_size,
                        )
                        .await;
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
                            Ok(SendTo::None(Some(TemplateDistribution::NewTemplate(
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
                        let message_type = match frame.get_header() {
                            Some(header) => header.msg_type(),
                            None => {
                                error!("Msg header not found");
                                // Update global tp state to down
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
                                Some(TemplateDistribution::NewTemplate(m)) => {
                                    let can_process_template = super::IS_NEW_TEMPLATE_HANDLED
                                        .load(std::sync::atomic::Ordering::Acquire)
                                        && pending_tx_data_template_id.is_none();
                                    if !can_process_template {
                                        pending_new_template = Some(m);
                                        continue;
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

                                    if wait_for_last_template_to_be_completed {
                                        println!("wait_for_last_template_to_be_completed");
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
                                        let pool_output = token.coinbase_output.to_vec();
                                        if let Err(e) = Downstream::on_new_template(
                                            &down,
                                            m.clone(),
                                            &pool_output[..],
                                        )
                                        .await
                                        {
                                            error!("{e:?}");
                                            // Update global downstream state to down
                                            ProxyState::update_downstream_state(
                                                DownstreamType::JdClientMiningDownstream,
                                            );
                                        };
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
                                        let pool_output = token.coinbase_output.to_vec();
                                        if let Err(e) = Downstream::on_new_template(
                                            &down,
                                            m.clone(),
                                            &pool_output[..],
                                        )
                                        .await
                                        {
                                            error!("{e:?}");
                                            // Update global downstream state to down
                                            ProxyState::update_downstream_state(
                                                DownstreamType::JdClientMiningDownstream,
                                            );
                                        };
                                    } else {
                                        unreachable!();
                                    }
                                }
                                Some(TemplateDistribution::SetNewPrevHash(m)) => {
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

                                Some(TemplateDistribution::RequestTransactionDataSuccess(m)) => {
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
                                            last_token = None;
                                            continue;
                                        }
                                    };
                                    let new_template_message = match new_template_message {
                                        Some(new_template_message) => new_template_message,
                                        None => {
                                            ProxyState::update_tp_state(TpState::Down);
                                            pending_tx_data_template_id = None;
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
                                        last_token = None;
                                        continue;
                                    }
                                    let token = match last_token.take() {
                                        Some(Some(token)) => token,
                                        Some(None) => break,
                                        None => break,
                                    };
                                    pending_tx_data_template_id = None;
                                    let jd = jd.clone();
                                    tokio::task::spawn(async move {
                                        let transactions_data = m.transaction_list;
                                        let excess_data = m.excess_data;
                                        let mining_token = token.mining_job_token.to_vec();
                                        let pool_coinbase_out = token.coinbase_output.to_vec();
                                        if let Some(jd) = jd.as_ref() {
                                            if let Err(e) = super::job_declarator::JobDeclarator::on_new_template(
                                                jd,
                                                new_template_message,
                                                mining_token,
                                                transactions_data,
                                                excess_data,
                                                pool_coinbase_out,
                                            )
                                            .await {
                                                error!("{e:?}");
                                                ProxyState::update_downstream_state(DownstreamType::JdClientMiningDownstream);
                                            };
                                        }
                                    });
                                }
                                Some(TemplateDistribution::RequestTransactionDataError(m)) => {
                                    if pending_tx_data_template_id == Some(m.template_id) {
                                        pending_tx_data_template_id = None;
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

    async fn on_new_solution(self_: Arc<Mutex<Self>>, mut rx: TReceiver<SubmitSolution<'static>>) {
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
                let sv2_frame: StdFrame = PoolMessages::TemplateDistribution(
                    TemplateDistribution::SubmitSolution(solution),
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
    use binary_sv2::{Seq0255, Seq064K, B016M, B0255, B064K, U256};
    use bitcoin::consensus::Encodable;
    use roles_logic_sv2::template_distribution_sv2::RequestTransactionDataSuccess;
    use std::convert::TryInto;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};

    fn make_new_template(template_id: u64, future_template: bool) -> NewTemplate<'static> {
        let coinbase_prefix: B0255<'static> = Vec::new()
            .try_into()
            .expect("coinbase prefix should fit in B0255");
        let coinbase_tx_outputs: B064K<'static> = Vec::new()
            .try_into()
            .expect("coinbase outputs should fit in B064K");
        let merkle_path: Seq0255<'static, U256<'static>> = Vec::new().into();
        NewTemplate {
            template_id,
            future_template,
            version: 0,
            coinbase_tx_version: 1,
            coinbase_prefix,
            coinbase_tx_input_sequence: 0,
            coinbase_tx_value_remaining: 0,
            coinbase_tx_outputs_count: 0,
            coinbase_tx_outputs,
            coinbase_tx_locktime: 0,
            merkle_path,
        }
    }

    fn make_tx_data_success(template_id: u64) -> RequestTransactionDataSuccess<'static> {
        let excess_data: B064K<'static> = Vec::new()
            .try_into()
            .expect("excess data should fit in B064K");
        let transaction_list: Seq064K<'static, B016M<'static>> =
            Seq064K::new(Vec::new()).expect("transaction list should be empty");
        RequestTransactionDataSuccess {
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

    fn with_decoded_message<R>(
        frame: StdFrame,
        f: impl for<'a> FnOnce(PoolMessages<'a>) -> R,
    ) -> R {
        let message_type = frame
            .get_header()
            .expect("sv2 frame missing header")
            .msg_type();
        let mut buf = vec![0_u8; frame.encoded_length()];
        frame
            .serialize(&mut buf)
            .expect("failed to serialize sv2 frame");
        let payload = &mut buf[framing_sv2::header::Header::SIZE..];
        let message =
            PoolMessages::try_from((message_type, payload)).expect("failed to parse sv2 payload");
        f(message)
    }

    async fn send_message(sender: &mpsc::Sender<EitherFrame>, message: PoolMessages<'static>) {
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
            PoolMessages::TemplateDistribution(TemplateDistribution::RequestTransactionData(m)) => {
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
            PoolMessages::TemplateDistribution(TemplateDistribution::CoinbaseOutputDataSize(_)) => {
            }
            other => panic!("unexpected coinbase size message: {other:?}"),
        });

        send_message(
            &to_client,
            PoolMessages::TemplateDistribution(TemplateDistribution::NewTemplate(
                make_new_template(1, true),
            )),
        )
        .await;
        expect_tx_data_request(&mut from_client, 1).await;

        send_message(
            &to_client,
            PoolMessages::TemplateDistribution(TemplateDistribution::NewTemplate(
                make_new_template(2, true),
            )),
        )
        .await;

        send_message(
            &to_client,
            PoolMessages::TemplateDistribution(TemplateDistribution::NewTemplate(
                make_new_template(3, true),
            )),
        )
        .await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        send_message(
            &to_client,
            PoolMessages::TemplateDistribution(
                TemplateDistribution::RequestTransactionDataSuccess(make_tx_data_success(1)),
            ),
        )
        .await;
        expect_tx_data_request(&mut from_client, 3).await;

        send_message(
            &to_client,
            PoolMessages::TemplateDistribution(
                TemplateDistribution::RequestTransactionDataSuccess(make_tx_data_success(2)),
            ),
        )
        .await;

        send_message(
            &to_client,
            PoolMessages::TemplateDistribution(
                TemplateDistribution::RequestTransactionDataSuccess(make_tx_data_success(3)),
            ),
        )
        .await;

        let _ = progress_tx.send(());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn panics_on_stale_tx_data_success_after_new_template() {
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
        let (down_tx, _down_rx) = mpsc::channel(1);
        let down = Arc::new(Mutex::new(Downstream::new(
            down_tx,
            None,
            solution_tx,
            false,
            Vec::new(),
            None,
        )));

        let mut encoded_outputs = vec![];
        Vec::<TxOut>::new()
            .consensus_encode(&mut encoded_outputs)
            .expect("encode coinbase outputs");
        let self_mutex = Arc::new(Mutex::new(TemplateRx {
            sender: client_to_tp_tx,
            jd: None,
            down,
            new_template_message: None,
            miner_coinbase_output: encoded_outputs,
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
