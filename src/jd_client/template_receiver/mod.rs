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
                request_id: crate::Configuration::get_id(),
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

                    match receiver.recv().await {
                        Some(received) => {
                            let frame: Result<StdFrame, _> = received.try_into();
                            if let Ok(mut frame) = frame {
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
                                match next_message_to_send {
                                    Ok(SendTo::None(m)) => {
                                        match m {
                                            // Send the new template along with the token to the JD so that JD can
                                            // declare the mining job
                                            Some(TemplateDistribution::NewTemplate(m)) => {
                                                // See coment on the definition of the global for memory
                                                // ordering
                                                super::IS_NEW_TEMPLATE_HANDLED.store(
                                                    false,
                                                    std::sync::atomic::Ordering::Release,
                                                );
                                                Self::send_tx_data_request(&self_mutex, m.clone())
                                                    .await;
                                                if self_mutex
                                                    .safe_lock(|t| {
                                                        t.new_template_message = Some(m.clone())
                                                    })
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
                                            }
                                            Some(TemplateDistribution::SetNewPrevHash(m)) => {
                                                info!("Received SetNewPrevHash, waiting for IS_NEW_TEMPLATE_HANDLED");
                                                // See coment on the definition of the global for memory
                                                // ordering
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
                                                if let Err(e) =
                                                    Downstream::on_set_new_prev_hash(&down, m).await
                                                {
                                                    error!("SetNewPrevHash Error: {e:?}");
                                                    // Update global tp state to down
                                                    ProxyState::update_tp_state(TpState::Down);
                                                    break;
                                                };
                                            }

                                            Some(
                                                TemplateDistribution::RequestTransactionDataSuccess(
                                                    m,
                                                ),
                                            ) => {
                                                // safe to unwrap because this message is received after the new
                                                // template message
                                                let transactions_data = m.transaction_list;
                                                let excess_data = m.excess_data;
                                                let expected_template_id = m.template_id;
                                                let m = match self_mutex
                                                    .safe_lock(|t| t.new_template_message.clone())
                                                {
                                                    Ok(new_template_message) => {
                                                        new_template_message.unwrap()
                                                    }
                                                    Err(e) => {
                                                        // Update global tp state to down
                                                        error!("TemplateRx mutex poisoned: {e}");
                                                        ProxyState::update_tp_state(TpState::Down);
                                                        break;
                                                    }
                                                };
                                                if m.template_id != expected_template_id {
                                                    continue;
                                                }
                                                let token = last_token.unwrap().unwrap();
                                                last_token = None;
                                                let mining_token = token.mining_job_token.to_vec();
                                                let pool_coinbase_out =
                                                    token.coinbase_output.to_vec();
                                                if let Some(jd) = jd.as_ref() {
                                                    if let Err(e) = super::job_declarator::JobDeclarator::on_new_template(
                                                jd,
                                                m.clone(),
                                                mining_token,
                                                transactions_data,
                                                excess_data,
                                                pool_coinbase_out,
                                            )
                                            .await {
                                                error!("{e:?}");
                                                break;

                                            };
                                                }
                                            }
                                            Some(
                                                TemplateDistribution::RequestTransactionDataError(
                                                    _,
                                                ),
                                            ) => {
                                                warn!("The prev_hash of the template requested to Template Provider no longer points to the latest tip. Continuing work on the updated template.")
                                            }
                                            _ => {
                                                error!("{:?}", frame);
                                                error!("{:?}", frame.payload());
                                                error!("{:?}", frame.get_header());
                                                std::process::exit(1);
                                            }
                                        }
                                    }
                                    Ok(m) => {
                                        error!("Unexpected next message {:?}", m);
                                        error!("{:?}", frame);
                                        error!("{:?}", frame.payload());
                                        error!("{:?}", frame.get_header());
                                        std::process::exit(1);
                                    }
                                    Err(roles_logic_sv2::Error::NoValidTemplate(_)) => {
                                        // This can happen when we require data for a template, the TP
                                        // already sent a new set prev hash, but the client did not saw it
                                        // yet
                                        error!(
                                            "Required txs for a non valid template id, ignoring it"
                                        );
                                    }
                                    Err(e) => {
                                        error!("Impossible to get next message {:?}", e);
                                        error!("{:?}", frame);
                                        error!("{:?}", frame.payload());
                                        error!("{:?}", frame.get_header());
                                        std::process::exit(1);
                                    }
                                }
                            } else {
                                error!("Failed to covert TP message to StdFrame");
                                // Update global tp state to down
                                ProxyState::update_tp_state(TpState::Down);
                            }
                        }

                        None => {
                            error!("Failed to receive msg");
                            ProxyState::update_tp_state(TpState::Down);
                            break;
                        }
                    };
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
