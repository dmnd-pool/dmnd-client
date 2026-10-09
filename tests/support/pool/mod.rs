pub mod control;
pub mod jd;
pub mod jobs;
pub mod mining;
pub mod shares;
pub mod templates;
pub mod transport;

use crate::support::{error, TestResult};
use control::{
    ChannelKey, ControlRequest, EventHistory, PoolCommand, PoolEvent, PoolHandle, PoolSnapshot,
};
use demand_share_accounting_ext::{parser::ShareAccountingMessages, ShareOk};
use jd::TokenRegistry;
use jobs::{JobEngine, JobKey};
use mining::{ChannelState, MiningSession};
#[cfg(not(legacy_sv2_transport))]
use roles_logic_sv2::binary_sv2;
use roles_logic_sv2::{
    common_messages_sv2::{Protocol, SetupConnectionError, SetupConnectionSuccess},
    job_declaration_sv2::{
        AllocateMiningJobTokenSuccess, DeclareMiningJobError, DeclareMiningJobSuccess,
        ProvideMissingTransactions,
    },
    mining_sv2::{
        SetCustomMiningJobError, SetNewPrevHash, SetTarget, SubmitSharesError, SubmitSharesSuccess,
        UpdateChannelError,
    },
    parsers::{CommonMessages, JobDeclaration, Mining, TemplateDistribution},
};
use shares::{ShareOutcome, ShareValidator};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
};
use templates::{TemplateClient, TemplateStore};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::{AbortHandle, JoinHandle, JoinSet},
};
use transport::{Message, PeerConnection, PeerId, PeerProtocol};

#[derive(Clone, Copy, Debug)]
pub enum AckMode {
    StandardSubmitSharesSuccess,
    DemandShareOk,
}

#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub listen: SocketAddr,
    pub tp_address: Option<SocketAddr>,
    pub token: String,
    pub target: [u8; 32],
    pub ack_mode: AckMode,
    pub request_missing_transactions: bool,
    pub event_journal: Option<std::path::PathBuf>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:0".parse().expect("loopback"),
            tp_address: None,
            token: "e2e-token".into(),
            target: bitcoin::Target::from_compact(bitcoin::CompactTarget::from_consensus(
                0x1e0f_ffff,
            ))
            .to_le_bytes(),
            ack_mode: AckMode::StandardSubmitSharesSuccess,
            request_missing_transactions: true,
            event_journal: None,
        }
    }
}

pub struct RunningPool {
    pub address: SocketAddr,
    handle: PoolHandle,
    task: Option<JoinHandle<TestResult>>,
}

impl RunningPool {
    pub async fn start(config: PoolConfig) -> TestResult<Self> {
        if !config.listen.ip().is_loopback() {
            return Err(error("test pool must listen on loopback"));
        }
        let listener = TcpListener::bind(config.listen).await?;
        let address = listener.local_addr()?;
        let (commands, rx) = mpsc::channel(128);
        let state = State::new(config)?;
        let task = tokio::spawn(state.run(listener, rx));
        Ok(Self {
            address,
            handle: PoolHandle { commands },
            task: Some(task),
        })
    }

    pub fn handle(&self) -> PoolHandle {
        self.handle.clone()
    }

    pub async fn shutdown(mut self) -> TestResult {
        let (reply, rx) = oneshot::channel();
        self.handle
            .commands
            .send(ControlRequest::Shutdown(reply))
            .await
            .map_err(|_| error("pool stopped"))?;
        tokio::time::timeout(std::time::Duration::from_secs(5), rx).await??;
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
}

impl Drop for RunningPool {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

enum Inbound {
    Ready(PeerId, mpsc::Sender<Message>),
    Message(PeerId, Message),
    Closed(PeerId),
    Template(TemplateDistribution<'static>),
    Error(String, String),
}

struct Peer {
    sender: mpsc::Sender<Message>,
    task: AbortHandle,
    protocol: Option<PeerProtocol>,
    identity: String,
}

struct State {
    config: PoolConfig,
    peers: HashMap<PeerId, Peer>,
    pending_tasks: HashMap<PeerId, AbortHandle>,
    history: EventHistory,
    jobs: JobEngine,
    mining: MiningSession,
    tokens: TokenRegistry,
    templates: TemplateStore,
    paused: bool,
    responses: VecDeque<(PeerId, Message)>,
    reject_submit: bool,
    reject_declaration: bool,
    validator: ShareValidator,
}

impl State {
    fn new(config: PoolConfig) -> TestResult<Self> {
        let history = EventHistory::new(config.event_journal.as_deref())?;
        Ok(Self {
            config,
            peers: HashMap::new(),
            pending_tasks: HashMap::new(),
            history,
            jobs: JobEngine::new()?,
            mining: MiningSession::default(),
            tokens: TokenRegistry::default(),
            templates: TemplateStore::default(),
            paused: false,
            responses: VecDeque::new(),
            reject_submit: false,
            reject_declaration: false,
            validator: ShareValidator::default(),
        })
    }

    async fn run(
        mut self,
        listener: TcpListener,
        mut commands: mpsc::Receiver<ControlRequest>,
    ) -> TestResult {
        let (tx, mut rx) = mpsc::channel(256);
        let mut tasks = JoinSet::new();
        if let Some(address) = self.config.tp_address {
            let tx = tx.clone();
            tasks.spawn(async move {
                if let Err(e) = TemplateClient::run(address, tx.clone()).await {
                    let _ = tx.send(Inbound::Error("TP".into(), e.to_string())).await;
                }
            });
        }
        let mut next_peer = 0;
        loop {
            let response_sender = self
                .responses
                .front()
                .filter(|_| !self.paused)
                .and_then(|(peer, _)| self.peers.get(peer))
                .map(|peer| peer.sender.clone());
            let flush_pending = response_sender.is_some();
            tokio::select! {
                permit = async { response_sender.expect("queued peer").reserve_owned().await }, if flush_pending => {
                    match permit {
                        Ok(permit) => {
                            let (_, message) = self.responses.pop_front().expect("queued response");
                            permit.send(message);
                        }
                        Err(_) => {
                            let peer = self.responses.front().expect("queued response").0;
                            self.disconnect(peer);
                        }
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    next_peer += 1;
                    let peer = PeerId(next_peer);
                    let tx = tx.clone();
                    let task = tasks.spawn(async move {
                        let result = async {
                            let mut connection = PeerConnection::accept(stream).await?;
                            let (sender, mut outgoing) = mpsc::channel::<Message>(256);
                            tx.send(Inbound::Ready(peer, sender)).await.map_err(|_| error("pool stopped"))?;
                            loop {
                                tokio::select! {
                                    message = connection.recv() => match message? {
                                        Some(message) => tx.send(Inbound::Message(peer, message)).await.map_err(|_| error("pool stopped"))?,
                                        None => break,
                                    },
                                    message = outgoing.recv() => match message {
                                        Some(message) => connection.send(message).await?,
                                        None => break,
                                    }
                                }
                            }
                            TestResult::Ok(())
                        }.await;
                        if let Err(e) = result { let _ = tx.send(Inbound::Error(format!("peer {}", peer.0), e.to_string())).await; }
                        let _ = tx.send(Inbound::Closed(peer)).await;
                    });
                    self.pending_tasks.insert(peer, task);
                }
                Some(inbound) = rx.recv() => {
                    let result = match inbound {
                        Inbound::Ready(peer, sender) => {
                            if let Some(task) = self.pending_tasks.remove(&peer) {
                                self.peers.insert(peer, Peer { sender, task, protocol: None, identity: String::new() });
                                self.history.push(PoolEvent::Connected { peer });
                            }
                            Ok(())
                        },
                        Inbound::Message(peer, message) => self.message(peer, message),
                        Inbound::Template(message) => self.template(message),
                        Inbound::Closed(peer) => { self.disconnect(peer); Ok(()) },
                        Inbound::Error(source, message) => { self.history.push(PoolEvent::Error { source, message }); Ok(()) },
                    };
                    if let Err(e) = result { self.history.push(PoolEvent::Error { source: "pool".into(), message: e.to_string() }); }
                }
                request = commands.recv() => match request {
                    Some(ControlRequest::Command(command, reply)) => {
                        let result = self.command(command).map(|_| self.snapshot());
                        let _ = reply.send(result);
                    },
                    Some(ControlRequest::Wait { after, matcher, reply }) => self.history.wait(after, matcher, reply),
                    Some(ControlRequest::Shutdown(reply)) => {
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                        let _ = reply.send(());
                        break;
                    },
                    None => break,
                },
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(e)) = result {
                        if !e.is_cancelled() { self.history.push(PoolEvent::Error { source: "task".into(), message: e.to_string() }); }
                    }
                }
            }
        }
        Ok(())
    }

    fn send(&mut self, peer: PeerId, message: Message) -> TestResult {
        if !self.peers.contains_key(&peer) {
            return Ok(());
        }
        if self.responses.len() >= 4096 {
            return Err(error("response queue full"));
        }
        // The main loop delivers queued responses with backpressure, without blocking controls.
        self.responses.push_back((peer, message));
        Ok(())
    }

    fn snapshot(&self) -> PoolSnapshot {
        PoolSnapshot {
            peers: self.peers.keys().copied().collect(),
            channels: self.mining.channels.keys().copied().collect(),
            accepted: self
                .history
                .records
                .iter()
                .filter(|r| matches!(r.event, PoolEvent::ShareAccepted(_)))
                .count(),
            rejected: self
                .history
                .records
                .iter()
                .filter(|r| matches!(r.event, PoolEvent::ShareRejected { .. }))
                .count(),
            events: self.history.records.clone(),
        }
    }

    fn disconnect(&mut self, peer: PeerId) {
        if let Some(connection) = self.peers.remove(&peer) {
            connection.task.abort();
            self.history.push(PoolEvent::Disconnected { peer });
        }
        if let Some(task) = self.pending_tasks.remove(&peer) {
            task.abort();
        }
        self.mining.channels.retain(|k, _| k.peer != peer);
        self.jobs.jobs.retain(|k, _| k.peer != peer);
        self.responses.retain(|(p, _)| *p != peer);
    }

    fn command(&mut self, command: PoolCommand) -> TestResult {
        match command {
            PoolCommand::SetTarget { channel, target } => {
                let state = self
                    .mining
                    .channels
                    .get_mut(&channel)
                    .ok_or_else(|| error("unknown channel"))?;
                self.jobs.target(channel.channel, target)?;
                state.target = target;
                self.send(
                    channel.peer,
                    Message::Mining(Mining::SetTarget(SetTarget {
                        channel_id: channel.channel,
                        maximum_target: target.into(),
                    })),
                )?;
                self.history
                    .push(PoolEvent::TargetChanged { channel, target });
            }
            PoolCommand::DisconnectPeer(peer) => self.disconnect(peer),
            PoolCommand::PauseResponses => self.paused = true,
            PoolCommand::ResumeResponses => self.paused = false,
            PoolCommand::RejectNextSubmit => self.reject_submit = true,
            PoolCommand::RejectNextDeclaration => self.reject_declaration = true,
            PoolCommand::Snapshot => {}
            PoolCommand::PublishTemplate(template) => {
                self.template(TemplateDistribution::NewTemplate(template))?
            }
            PoolCommand::PublishTip(tip) => {
                self.template(TemplateDistribution::SetNewPrevHash(tip))?
            }
        }
        Ok(())
    }

    fn publish(&mut self, channel: ChannelKey, mut message: Mining<'static>) -> TestResult {
        match &mut message {
            Mining::NewExtendedMiningJob(job) => {
                job.channel_id = channel.channel;
                self.jobs.record(channel, job.clone());
                self.history.push(PoolEvent::JobPublished {
                    channel,
                    job_id: job.job_id,
                });
            }
            Mining::SetNewPrevHash(tip) => {
                tip.channel_id = channel.channel;
                self.jobs.activate(
                    channel,
                    tip.job_id,
                    tip.prev_hash.to_vec().try_into().expect("U256"),
                    tip.min_ntime,
                    tip.nbits,
                );
            }
            _ => {}
        }
        self.send(channel.peer, Message::Mining(message))
    }

    fn template(&mut self, message: TemplateDistribution<'static>) -> TestResult {
        match message {
            TemplateDistribution::NewTemplate(template) => {
                let messages = self.jobs.template(template)?;
                for (channel_id, message) in messages {
                    if let Some(channel) = self
                        .mining
                        .channels
                        .keys()
                        .find(|c| c.channel == channel_id)
                        .copied()
                    {
                        self.publish(channel, message)?;
                    }
                }
            }
            TemplateDistribution::SetNewPrevHash(tip) => {
                let id = self.jobs.prev_hash(tip.clone())?;
                let hash = tip.prev_hash.to_vec().try_into().expect("U256");
                self.history.push(PoolEvent::TipChanged { prev_hash: hash });
                let channels = self.mining.channels.keys().copied().collect::<Vec<_>>();
                for channel in channels {
                    self.publish(
                        channel,
                        Mining::SetNewPrevHash(SetNewPrevHash {
                            channel_id: channel.channel,
                            job_id: id,
                            prev_hash: tip.prev_hash.clone(),
                            min_ntime: tip.header_timestamp,
                            nbits: tip.n_bits,
                        }),
                    )?;
                }
            }
            TemplateDistribution::RequestTransactionDataSuccess(data) => {
                self.templates.transaction_data(data)?
            }
            TemplateDistribution::RequestTransactionDataError(data) => {
                return Err(error(format!("TP transaction data: {data:?}")))
            }
            _ => return Err(error("unexpected template-distribution message")),
        }
        Ok(())
    }

    fn message(&mut self, peer: PeerId, message: Message) -> TestResult {
        let Some(connection) = self.peers.get(&peer) else {
            return Ok(());
        };
        let protocol = connection.protocol;
        match message {
            Message::Common(CommonMessages::SetupConnection(setup)) if protocol.is_none() => {
                let identity = String::from_utf8(setup.device_id.to_vec())?;
                let token = identity.rsplit("::POOLED::").next().unwrap_or("");
                let protocol = match setup.protocol {
                    Protocol::MiningProtocol => Some(PeerProtocol::Mining),
                    Protocol::JobDeclarationProtocol => Some(PeerProtocol::JobDeclaration),
                    _ => None,
                };
                let reason = if token != self.config.token {
                    Some("invalid-token")
                } else if setup.min_version > 2 || setup.max_version < 2 {
                    Some("unsupported-version")
                } else if protocol.is_none() {
                    Some("unsupported-protocol")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    self.history.push(PoolEvent::SetupRejected {
                        peer,
                        reason: reason.into(),
                    });
                    return self.send(
                        peer,
                        Message::Common(CommonMessages::SetupConnectionError(
                            SetupConnectionError {
                                flags: 0,
                                error_code: reason.to_string().try_into().expect("code"),
                            },
                        )),
                    );
                }
                let connection = self.peers.get_mut(&peer).expect("peer");
                connection.protocol = protocol;
                // A proxy generates separate device IDs for its JD and mining connections; the token links them.
                connection.identity = token.to_string();
                self.history.push(PoolEvent::Setup {
                    peer,
                    protocol: format!("{protocol:?}"),
                    identity,
                });
                self.send(
                    peer,
                    Message::Common(CommonMessages::SetupConnectionSuccess(
                        SetupConnectionSuccess {
                            used_version: 2,
                            flags: setup.flags,
                        },
                    )),
                )
            }
            Message::Mining(message) if protocol == Some(PeerProtocol::Mining) => {
                self.mining_message(peer, message)
            }
            Message::JobDeclaration(message) if protocol == Some(PeerProtocol::JobDeclaration) => {
                self.jd_message(peer, message)
            }
            other => Err(error(format!(
                "message before setup or wrong protocol: {other:?}"
            ))),
        }
    }

    fn mining_message(&mut self, peer: PeerId, message: Mining<'static>) -> TestResult {
        match message {
            Mining::OpenExtendedMiningChannel(request) => {
                let messages = self.jobs.open(
                    request.request_id,
                    request.nominal_hash_rate,
                    request.min_extranonce_size,
                )?;
                let mut opened = None;
                for mut message in messages {
                    if let Mining::OpenExtendedMiningChannelSuccess(success) = &mut message {
                        let max = bitcoin::Target::from_le_bytes(
                            request.max_target.to_vec().try_into().expect("U256"),
                        );
                        let target = bitcoin::Target::from_le_bytes(self.config.target)
                            .min(max)
                            .to_le_bytes();
                        success.target = target.into();
                        self.jobs.target(success.channel_id, target)?;
                        let channel = ChannelKey {
                            peer,
                            channel: success.channel_id,
                        };
                        self.mining
                            .channels
                            .insert(channel, ChannelState::opened(channel, success));
                        self.history.push(PoolEvent::ChannelOpened {
                            channel,
                            extranonce_prefix: success.extranonce_prefix.to_vec(),
                            extranonce_size: success.extranonce_size,
                        });
                        opened = Some(channel);
                    }
                    if let Some(channel) = opened {
                        self.publish(channel, message)?;
                    } else {
                        self.send(peer, Message::Mining(message))?;
                    }
                }
            }
            Mining::UpdateChannel(request) => {
                let channel = ChannelKey {
                    peer,
                    channel: request.channel_id,
                };
                if !self.mining.channels.contains_key(&channel) {
                    self.send(
                        peer,
                        Message::Mining(Mining::UpdateChannelError(UpdateChannelError {
                            channel_id: request.channel_id,
                            error_code: "invalid-channel-id".to_string().try_into().expect("code"),
                        })),
                    )?;
                }
            }
            Mining::SubmitSharesExtended(share) => {
                let channel = ChannelKey {
                    peer,
                    channel: share.channel_id,
                };
                let key = JobKey {
                    peer,
                    channel: share.channel_id,
                    job: share.job_id,
                };
                self.validator_submit(channel, key, share)?;
            }
            Mining::SetCustomMiningJob(custom) => {
                let channel = ChannelKey {
                    peer,
                    channel: custom.channel_id,
                };
                let identity = self.peers[&peer].identity.clone();
                let result = (|| {
                    if !self.mining.channels.contains_key(&channel) {
                        return Err(error("unknown-channel"));
                    }
                    let tip = self.jobs.tip.as_ref().ok_or_else(|| error("no-tip"))?;
                    if custom.prev_hash != tip.prev_hash
                        || custom.nbits != tip.n_bits
                        || custom.min_ntime < tip.header_timestamp
                    {
                        return Err(error("custom-job-stale-tip"));
                    }
                    self.tokens.custom(&identity, &custom, &self.templates)
                })();
                match result {
                    Ok(mut job) => {
                        let success = self.jobs.custom(custom.clone());
                        job.channel_id = channel.channel;
                        job.job_id = success.job_id;
                        self.jobs.record(channel, job);
                        self.jobs.activate(
                            channel,
                            success.job_id,
                            custom.prev_hash.to_vec().try_into().expect("U256"),
                            custom.min_ntime,
                            custom.nbits,
                        );
                        self.history.push(PoolEvent::CustomJobRegistered {
                            channel,
                            job_id: success.job_id,
                        });
                        self.send(
                            peer,
                            Message::Mining(Mining::SetCustomMiningJobSuccess(success)),
                        )?;
                    }
                    Err(e) => {
                        self.history.push(PoolEvent::CustomJobRejected {
                            channel,
                            reason: e.to_string(),
                        });
                        self.send(
                            peer,
                            Message::Mining(Mining::SetCustomMiningJobError(
                                SetCustomMiningJobError {
                                    channel_id: custom.channel_id,
                                    request_id: custom.request_id,
                                    error_code: "invalid-job-param-value"
                                        .to_string()
                                        .try_into()
                                        .expect("code"),
                                },
                            )),
                        )?;
                    }
                }
            }
            other => return Err(error(format!("unsupported mining message: {other:?}"))),
        }
        Ok(())
    }

    fn validator_submit(
        &mut self,
        channel: ChannelKey,
        key: JobKey,
        share: roles_logic_sv2::mining_sv2::SubmitSharesExtended<'static>,
    ) -> TestResult {
        self.validator.retain_jobs(&self.jobs.jobs);
        let outcome = self.validator.validate(
            channel.peer,
            self.mining.channels.get(&channel),
            self.jobs.jobs.get(&key),
            &share,
        );
        let reason = if std::mem::take(&mut self.reject_submit) {
            Some("forced-rejection")
        } else if let ShareOutcome::Rejected(reason) = &outcome {
            Some(reason.code())
        } else {
            None
        };
        if let Some(reason) = reason {
            self.history.push(PoolEvent::ShareRejected {
                channel,
                job_id: key.job,
                sequence_number: share.sequence_number,
                reason: reason.into(),
            });
            return self.send(
                channel.peer,
                Message::Mining(Mining::SubmitSharesError(SubmitSharesError {
                    channel_id: key.channel,
                    sequence_number: share.sequence_number,
                    error_code: reason.to_string().try_into().expect("code"),
                })),
            );
        }
        let validated = match outcome {
            ShareOutcome::Accepted(s) | ShareOutcome::BlockCandidate(s) => s,
            ShareOutcome::Rejected(_) => unreachable!(),
        };
        self.history.push(PoolEvent::ShareAccepted(validated));
        let response = match self.config.ack_mode {
            AckMode::StandardSubmitSharesSuccess => {
                Message::Mining(Mining::SubmitSharesSuccess(SubmitSharesSuccess {
                    channel_id: key.channel,
                    last_sequence_number: share.sequence_number,
                    new_submits_accepted_count: 1,
                    new_shares_sum: 1,
                }))
            }
            AckMode::DemandShareOk => {
                Message::ShareAccountingMessages(ShareAccountingMessages::ShareOk(ShareOk {
                    ref_job_id: ((key.job as u64) << 32) | key.channel as u64,
                    share_index: share.sequence_number,
                }))
            }
        };
        self.send(channel.peer, response)
    }

    fn declaration_error(&mut self, peer: PeerId, request_id: u32, reason: String) -> TestResult {
        self.history.push(PoolEvent::DeclarationRejected {
            peer,
            request_id,
            reason,
        });
        self.send(
            peer,
            Message::JobDeclaration(JobDeclaration::DeclareMiningJobError(
                DeclareMiningJobError {
                    request_id,
                    error_code: "invalid-mining-job-token"
                        .to_string()
                        .try_into()
                        .expect("code"),
                    error_details: Vec::<u8>::new().try_into().expect("empty"),
                },
            )),
        )
    }

    fn approve(&mut self, peer: PeerId, request_id: u32) -> TestResult {
        let token = self.tokens.approve(peer, request_id)?;
        self.history
            .push(PoolEvent::DeclarationRegistered { peer, request_id });
        self.send(
            peer,
            Message::JobDeclaration(JobDeclaration::DeclareMiningJobSuccess(
                DeclareMiningJobSuccess {
                    request_id,
                    new_mining_job_token: token.try_into().expect("token"),
                },
            )),
        )
    }

    fn jd_message(&mut self, peer: PeerId, message: JobDeclaration<'static>) -> TestResult {
        match message {
            JobDeclaration::AllocateMiningJobToken(request) => {
                let token = self.tokens.allocate(peer, &self.peers[&peer].identity);
                self.history.push(PoolEvent::TokenAllocated {
                    peer,
                    token: token.clone(),
                });
                self.send(
                    peer,
                    Message::JobDeclaration(JobDeclaration::AllocateMiningJobTokenSuccess(
                        AllocateMiningJobTokenSuccess {
                            request_id: request.request_id,
                            mining_job_token: token.try_into().expect("token"),
                            coinbase_output_max_additional_size: 10,
                            coinbase_output: TokenRegistry::coinbase_output()
                                .try_into()
                                .expect("output"),
                            async_mining_allowed: false,
                        },
                    )),
                )
            }
            JobDeclaration::DeclareMiningJob(message) => {
                let request_id = message.request_id;
                if std::mem::take(&mut self.reject_declaration) {
                    return self.declaration_error(peer, request_id, "forced-rejection".into());
                }
                match self.tokens.declare(
                    peer,
                    &self.peers[&peer].identity,
                    message,
                    &self.templates,
                    self.config.request_missing_transactions,
                ) {
                    Ok(missing) if missing.is_empty() => self.approve(peer, request_id),
                    Ok(missing) => {
                        self.history.push(PoolEvent::MissingTransactionsRequested {
                            peer,
                            request_id,
                            count: missing.len(),
                        });
                        self.send(
                            peer,
                            Message::JobDeclaration(JobDeclaration::ProvideMissingTransactions(
                                ProvideMissingTransactions {
                                    request_id,
                                    unknown_tx_position_list: binary_sv2::Seq064K::new(missing)
                                        .expect("positions"),
                                },
                            )),
                        )
                    }
                    Err(e) => self.declaration_error(peer, request_id, e.to_string()),
                }
            }
            JobDeclaration::ProvideMissingTransactionsSuccess(message) => {
                let request_id = message.request_id;
                match self.tokens.provide(peer, message, &mut self.templates) {
                    Ok(()) => self.approve(peer, request_id),
                    Err(e) => self.declaration_error(peer, request_id, e.to_string()),
                }
            }
            other => Err(error(format!("unsupported JD message: {other:?}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use control::EventMatcher;
    use roles_logic_sv2::common_messages_sv2::SetupConnection;
    use tokio::time::{timeout_at, Duration, Instant};

    #[tokio::test]
    async fn stalled_response_flush_keeps_messages_and_controls_responsive() -> TestResult {
        let mut state = State::new(PoolConfig::default())?;
        let stalled_peer = PeerId(0);
        // Holding a one-slot receiver without reading deterministically stalls outbound capacity.
        let (sender, stalled_receiver) = mpsc::channel(1);
        let stalled_task = tokio::spawn(std::future::pending::<()>());
        state.peers.insert(
            stalled_peer,
            Peer {
                sender,
                task: stalled_task.abort_handle(),
                protocol: None,
                identity: String::new(),
            },
        );
        state.paused = true;
        for _ in 0..2 {
            state.send(
                stalled_peer,
                Message::Common(CommonMessages::SetupConnectionSuccess(
                    SetupConnectionSuccess {
                        used_version: 2,
                        flags: 0,
                    },
                )),
            )?;
        }
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (commands, receiver) = mpsc::channel(128);
        let handle = PoolHandle { commands };
        let pool = RunningPool {
            address,
            handle: handle.clone(),
            task: Some(tokio::spawn(state.run(listener, receiver))),
        };
        let limit = Instant::now() + Duration::from_secs(3);
        timeout_at(limit, handle.command(PoolCommand::ResumeResponses)).await??;
        timeout_at(limit, async {
            while stalled_receiver.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let snapshot = timeout_at(limit, handle.command(PoolCommand::Snapshot)).await??;
        assert!(snapshot.peers.contains(&stalled_peer));
        timeout_at(limit, handle.command(PoolCommand::PauseResponses)).await??;
        timeout_at(limit, handle.command(PoolCommand::ResumeResponses)).await??;

        // A real connection's incoming setup is still processed while flushing is blocked.
        let mut peer = timeout_at(limit, PeerConnection::connect(address)).await??;
        peer.send(Message::Common(CommonMessages::SetupConnection(
            SetupConnection {
                protocol: Protocol::MiningProtocol,
                min_version: 2,
                max_version: 2,
                flags: 0,
                endpoint_host: "127.0.0.1".to_string().try_into().unwrap(),
                endpoint_port: address.port(),
                vendor: String::new().try_into().unwrap(),
                hardware_version: String::new().try_into().unwrap(),
                firmware: String::new().try_into().unwrap(),
                device_id: "invalid-token".to_string().try_into().unwrap(),
            },
        )))
        .await?;
        handle.wait_for(EventMatcher::SetupRejected, limit).await?;
        let snapshot = timeout_at(
            limit,
            handle.command(PoolCommand::DisconnectPeer(stalled_peer)),
        )
        .await??;
        assert!(!snapshot.peers.contains(&stalled_peer));
        assert!(stalled_task.await.unwrap_err().is_cancelled());
        // Removing the stalled peer releases the remaining response for the real connection.
        assert!(matches!(
            timeout_at(limit, peer.recv()).await??,
            Some(Message::Common(CommonMessages::SetupConnectionError(_)))
        ));
        drop(peer);
        timeout_at(limit, pool.shutdown()).await??;
        Ok(())
    }
}
