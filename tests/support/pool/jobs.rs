use super::{control::ChannelKey, transport::PeerId};
use crate::support::{error, TestResult};
#[cfg(not(legacy_sv2_transport))]
use roles_logic_sv2::binary_sv2;
use roles_logic_sv2::{
    channel_logic::channel_factory::{ExtendedChannelKind, PoolChannelFactory},
    job_creator::JobsCreators,
    mining_sv2::{
        ExtendedExtranonce, NewExtendedMiningJob, SetCustomMiningJob, SetCustomMiningJobSuccess,
        Target,
    },
    parsers::Mining,
    template_distribution_sv2::{NewTemplate, SetNewPrevHash},
    utils::{GroupId, Mutex},
};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};

pub const EXTRANONCE_LEN: u8 = 32;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize)]
pub struct JobKey {
    pub peer: PeerId,
    pub channel: u32,
    pub job: u32,
}

#[derive(Clone, Debug)]
pub struct JobRecord {
    pub key: JobKey,
    pub message: NewExtendedMiningJob<'static>,
    pub prev_hash: Option<[u8; 32]>,
    pub min_ntime: u32,
    pub nbits: u32,
    pub valid: bool,
}

pub struct JobEngine {
    factory: PoolChannelFactory,
    pub jobs: HashMap<JobKey, JobRecord>,
    pub tip: Option<SetNewPrevHash<'static>>,
}

pub fn pool_output() -> bitcoin::TxOut {
    bitcoin::TxOut {
        value: bitcoin::Amount::ZERO,
        script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0x51]),
    }
}

impl JobEngine {
    pub fn new() -> TestResult<Self> {
        let factory = PoolChannelFactory::new(
            Arc::new(Mutex::new(GroupId::new())),
            ExtendedExtranonce::new(0..0, 0..4, 4..EXTRANONCE_LEN as usize),
            JobsCreators::new(EXTRANONCE_LEN),
            60.0,
            ExtendedChannelKind::Pool,
            vec![pool_output()],
            vec![],
        )
        .map_err(|e| error(format!("channel factory: {e:?}")))?;
        Ok(Self {
            factory,
            jobs: HashMap::new(),
            tip: None,
        })
    }

    pub fn open(
        &mut self,
        request_id: u32,
        hashrate: f32,
        min_size: u16,
    ) -> TestResult<Vec<Mining<'static>>> {
        self.factory
            .new_extended_channel(request_id, hashrate.max(1.0), min_size)
            .map_err(|e| error(format!("open channel: {e:?}")))
    }

    pub fn template(
        &mut self,
        mut template: NewTemplate<'static>,
    ) -> TestResult<Vec<(u32, Mining<'static>)>> {
        self.factory
            .on_new_template(&mut template)
            .map(|messages| messages.into_iter().collect())
            .map_err(|e| error(format!("new template: {e:?}")))
    }

    pub fn prev_hash(&mut self, tip: SetNewPrevHash<'static>) -> TestResult<u32> {
        let id = self
            .factory
            .on_new_prev_hash_from_tp(&tip)
            .map_err(|e| error(format!("prevhash: {e:?}")))?;
        self.tip = Some(tip);
        Ok(id)
    }

    pub fn target(&mut self, channel: u32, target: [u8; 32]) -> TestResult {
        let target: binary_sv2::U256 = target.into();
        self.factory
            .update_target_for_channel(channel, Target::from(target))
            .ok_or_else(|| error("unknown channel"))?;
        Ok(())
    }

    pub fn custom(&mut self, message: SetCustomMiningJob<'static>) -> SetCustomMiningJobSuccess {
        let mut success = self.factory.on_new_set_custom_mining_job(message);
        // The factory's template and custom-job counters are independent. Reserve a separate
        // namespace so a custom registration never overwrites an advertised pool job.
        success.job_id |= 0x8000_0000;
        success
    }

    pub fn record(&mut self, channel: ChannelKey, message: NewExtendedMiningJob<'static>) {
        let key = JobKey {
            peer: channel.peer,
            channel: channel.channel,
            job: message.job_id,
        };
        let tip = self.tip.as_ref().filter(|_| !message.is_future());
        let min_ntime = message.min_ntime.clone().into_inner().unwrap_or(0);
        self.jobs.insert(
            key,
            JobRecord {
                key,
                message,
                prev_hash: tip.map(|t| t.prev_hash.to_vec().try_into().expect("U256")),
                min_ntime,
                nbits: tip.map_or(0, |t| t.n_bits),
                valid: tip.is_some(),
            },
        );
        // Bound retained stale jobs while keeping a useful negative-test window.
        if self.jobs.len() > 4096 {
            let stale = self
                .jobs
                .iter()
                .filter(|(_, j)| !j.valid)
                .map(|(k, _)| *k)
                .take(2048)
                .collect::<Vec<_>>();
            for key in stale {
                self.jobs.remove(&key);
            }
        }
    }

    pub fn activate(
        &mut self,
        channel: ChannelKey,
        job: u32,
        prev_hash: [u8; 32],
        ntime: u32,
        nbits: u32,
    ) {
        for record in self
            .jobs
            .values_mut()
            .filter(|j| j.key.peer == channel.peer && j.key.channel == channel.channel)
        {
            if record.key.job == job {
                record.prev_hash = Some(prev_hash);
                record.min_ntime = ntime;
                record.nbits = nbits;
                record.valid = true;
            } else if record.prev_hash != Some(prev_hash) {
                record.valid = false;
            }
        }
    }
}
