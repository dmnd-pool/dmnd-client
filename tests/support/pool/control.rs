use super::{shares::ValidatedShare, transport::PeerId};
use crate::support::{error, TestResult};
use serde::Serialize;
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize)]
pub struct ChannelKey {
    pub peer: PeerId,
    pub channel: u32,
}

#[derive(Debug)]
pub enum PoolCommand {
    SetTarget {
        channel: ChannelKey,
        target: [u8; 32],
    },
    DisconnectPeer(PeerId),
    PauseResponses,
    ResumeResponses,
    RejectNextSubmit,
    RejectNextDeclaration,
    Snapshot,
    PublishTemplate(super::protocol::NewTemplate),
    PublishTip(super::protocol::TemplatePrevHash),
}

#[derive(Clone, Debug, Serialize)]
pub enum PoolEvent {
    Connected {
        peer: PeerId,
    },
    Setup {
        peer: PeerId,
        protocol: String,
        identity: String,
    },
    SetupRejected {
        peer: PeerId,
        reason: String,
    },
    ChannelOpened {
        channel: ChannelKey,
        extranonce_prefix: Vec<u8>,
        extranonce_size: u16,
    },
    JobPublished {
        channel: ChannelKey,
        job_id: u32,
    },
    TipChanged {
        prev_hash: [u8; 32],
    },
    TargetChanged {
        channel: ChannelKey,
        target: [u8; 32],
    },
    TokenAllocated {
        peer: PeerId,
        token: Vec<u8>,
    },
    MissingTransactionsRequested {
        peer: PeerId,
        request_id: u32,
        count: usize,
    },
    DeclarationRegistered {
        peer: PeerId,
        request_id: u32,
    },
    DeclarationRejected {
        peer: PeerId,
        request_id: u32,
        reason: String,
    },
    CustomJobRegistered {
        channel: ChannelKey,
        job_id: u32,
    },
    CustomJobRejected {
        channel: ChannelKey,
        reason: String,
    },
    ShareAccepted(ValidatedShare),
    ShareRejected {
        channel: ChannelKey,
        job_id: u32,
        sequence_number: u32,
        reason: String,
    },
    Disconnected {
        peer: PeerId,
    },
    Error {
        source: String,
        message: String,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct EventRecord {
    pub sequence: u64,
    pub event: PoolEvent,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PoolSnapshot {
    pub peers: Vec<PeerId>,
    pub channels: Vec<ChannelKey>,
    pub accepted: usize,
    pub rejected: usize,
    pub events: Vec<EventRecord>,
}

#[derive(Clone, Copy, Debug)]
pub enum EventMatcher {
    ChannelOpened,
    ValidatedShare,
    RejectedShare,
    DeclarationRegistered,
    CustomJobRegistered,
    SetupRejected,
    TipChanged,
    Disconnected,
}

impl EventMatcher {
    fn matches(self, event: &PoolEvent) -> bool {
        matches!(
            (self, event),
            (Self::ChannelOpened, PoolEvent::ChannelOpened { .. })
                | (Self::ValidatedShare, PoolEvent::ShareAccepted(_))
                | (Self::RejectedShare, PoolEvent::ShareRejected { .. })
                | (
                    Self::DeclarationRegistered,
                    PoolEvent::DeclarationRegistered { .. }
                )
                | (
                    Self::CustomJobRegistered,
                    PoolEvent::CustomJobRegistered { .. }
                )
                | (Self::SetupRejected, PoolEvent::SetupRejected { .. })
                | (Self::TipChanged, PoolEvent::TipChanged { .. })
                | (Self::Disconnected, PoolEvent::Disconnected { .. })
        )
    }
}

pub(super) enum ControlRequest {
    Command(PoolCommand, oneshot::Sender<TestResult<PoolSnapshot>>),
    Wait {
        after: u64,
        matcher: EventMatcher,
        reply: oneshot::Sender<EventRecord>,
    },
    Shutdown(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct PoolHandle {
    pub(super) commands: mpsc::Sender<ControlRequest>,
}

impl PoolHandle {
    pub async fn command(&self, command: PoolCommand) -> TestResult<PoolSnapshot> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(ControlRequest::Command(command, tx))
            .await
            .map_err(|_| error("pool stopped"))?;
        tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .map_err(|_| error("pool command timed out"))?
            .map_err(|_| error("pool dropped command response"))?
    }

    pub async fn wait_for(
        &self,
        matcher: EventMatcher,
        deadline: Instant,
    ) -> TestResult<EventRecord> {
        self.wait_after(0, matcher, deadline).await
    }

    pub async fn wait_after(
        &self,
        after: u64,
        matcher: EventMatcher,
        deadline: Instant,
    ) -> TestResult<EventRecord> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(ControlRequest::Wait {
                after,
                matcher,
                reply,
            })
            .await
            .map_err(|_| error("pool stopped"))?;
        match tokio::time::timeout_at(deadline, rx).await {
            Ok(Ok(event)) => Ok(event),
            Ok(Err(_)) => Err(error("pool stopped while waiting for an event")),
            Err(_) => {
                let snapshot = self.command(PoolCommand::Snapshot).await?;
                let relevant = snapshot
                    .events
                    .iter()
                    .filter(|event| !matches!(event.event, PoolEvent::ShareAccepted(_)))
                    .rev()
                    .take(8)
                    .collect::<Vec<_>>();
                Err(error(format!(
                    "timed out waiting for {matcher:?} after {after}; peers={}, channels={}, accepted={}, rejected={}; recent events: {}",
                    snapshot.peers.len(),snapshot.channels.len(),snapshot.accepted,snapshot.rejected,serde_json::to_string(&relevant)?
                )))
            }
        }
    }

    pub async fn wait_for_validated_share(&self, deadline: Instant) -> TestResult<ValidatedShare> {
        match self
            .wait_for(EventMatcher::ValidatedShare, deadline)
            .await?
            .event
        {
            PoolEvent::ShareAccepted(share) => Ok(share),
            _ => unreachable!(),
        }
    }
}

pub(super) struct EventHistory {
    pub records: Vec<EventRecord>,
    waiters: Vec<(u64, EventMatcher, oneshot::Sender<EventRecord>)>,
    journal: Option<std::fs::File>,
}

impl EventHistory {
    pub fn new(path: Option<&std::path::Path>) -> TestResult<Self> {
        Ok(Self {
            records: vec![],
            waiters: vec![],
            journal: path.map(std::fs::File::create).transpose()?,
        })
    }

    pub fn push(&mut self, event: PoolEvent) {
        let record = EventRecord {
            sequence: self.records.len() as u64 + 1,
            event,
        };
        if let Some(journal) = &mut self.journal {
            use std::io::Write;
            serde_json::to_writer(&mut *journal, &record).expect("write pool event artifact");
            writeln!(journal).expect("write pool event artifact newline");
        }
        self.waiters.retain_mut(|(after, matcher, reply)| {
            if reply.is_closed() {
                return false;
            }
            if record.sequence > *after && matcher.matches(&record.event) {
                // Move the sender out without holding any locks or awaiting.
                let (placeholder, _) = oneshot::channel();
                let reply = std::mem::replace(reply, placeholder);
                let _ = reply.send(record.clone());
                false
            } else {
                true
            }
        });
        self.records.push(record);
    }

    pub fn wait(&mut self, after: u64, matcher: EventMatcher, reply: oneshot::Sender<EventRecord>) {
        if let Some(record) = self
            .records
            .iter()
            .find(|r| r.sequence > after && matcher.matches(&r.event))
        {
            let _ = reply.send(record.clone());
        } else {
            self.waiters.retain(|(_, _, reply)| !reply.is_closed());
            self.waiters.push((after, matcher, reply));
        }
    }
}
