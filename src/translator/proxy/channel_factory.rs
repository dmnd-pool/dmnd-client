use std::{
    collections::{HashMap, HashSet},
    hash::{Hash as StdHash, Hasher},
    ops::Deref,
    sync::Arc,
};

use bitcoin::{
    block::{Header, Version},
    hashes::Hash,
    CompactTarget, TxMerkleNode,
};
use nohash_hasher::BuildNoHashHasher;
use roles_logic_sv2::{
    channel_logic::channel_factory::{
        ExtendedChannelKind, OnNewShare, ProxyExtendedChannelFactory, Share,
    },
    mining::{ExtendedExtranonce, Target},
    mining_sv2::{
        NewExtendedMiningJobOwned, OpenExtendedMiningChannelSuccessOwned, SubmitSharesErrorOwned,
        SubmitSharesExtendedOwned, ERROR_CODE_SUBMIT_SHARES_DIFFICULTY_TOO_LOW,
    },
    parsers::MiningOwned,
    utils::{hash_rate_to_target, merkle_root_from_path, u256_to_block_hash, GroupId, Mutex},
    Error,
};

#[derive(Debug)]
pub(super) struct ChannelFactory {
    native: ProxyExtendedChannelFactory,
    ids: Arc<Mutex<GroupId>>,
    extranonces: ExtendedExtranonce,
    channels: HashMap<u32, OpenExtendedMiningChannelSuccessOwned, BuildNoHashHasher<u32>>,
    prefixes: HashSet<Prefix, BuildNoHashHasher<u64>>,
    share_per_min: f32,
    upstream_target: Target,
}

impl ChannelFactory {
    pub(super) fn new(
        extranonces: ExtendedExtranonce,
        share_per_min: f32,
        upstream_target: Target,
        upstream_channel: OpenExtendedMiningChannelSuccessOwned,
    ) -> Result<Self, Error> {
        if extranonces.get_len() > 32 {
            return Err(Error::InvalidExtranonceSize(
                32,
                extranonces.get_len().try_into().unwrap_or(u16::MAX),
            ));
        }
        let ids = Arc::new(Mutex::new(GroupId::new()));
        let native = ProxyExtendedChannelFactory::new(
            ids.clone(),
            extranonces.clone(),
            None,
            share_per_min,
            ExtendedChannelKind::Proxy {
                upstream_target: upstream_target.clone(),
            },
            None,
            upstream_channel,
        );
        Ok(Self {
            native,
            ids,
            extranonces,
            channels: HashMap::with_hasher(BuildNoHashHasher::default()),
            prefixes: HashSet::with_hasher(BuildNoHashHasher::default()),
            share_per_min,
            upstream_target,
        })
    }

    pub(super) fn open_channel(
        &mut self,
        request_id: u32,
        hash_rate: f32,
    ) -> Result<OpenExtendedMiningChannelSuccessOwned, Error> {
        let extranonce_size = self.extranonces.get_range2_len() as u16;
        let channel_id = self
            .ids
            .safe_lock(|ids| ids.new_channel_id(0))
            .map_err(|error| Error::PoisonLock(error.to_string()))?;
        let target = hash_rate_to_target(hash_rate.into(), self.share_per_min.into())?;
        let extranonce_prefix = loop {
            let prefix = self
                .extranonces
                .next_extended(usize::from(extranonce_size))
                .ok_or(Error::ExtranonceSpaceEnded)?
                .into_b032();
            if self.prefixes.insert(prefix_key(prefix.as_bytes())) {
                break prefix;
            }
        };
        let channel = OpenExtendedMiningChannelSuccessOwned {
            group_channel_id: 0,
            request_id,
            channel_id,
            target,
            extranonce_size,
            extranonce_prefix,
        };
        self.channels.insert(channel_id, channel.clone());
        Ok(channel)
    }

    pub(super) fn close_channel(&mut self, channel_id: u32) {
        if let Some(channel) = self.channels.remove(&channel_id) {
            self.prefixes
                .remove(&prefix_key(channel.extranonce_prefix.as_bytes()));
        }
    }

    pub(super) fn on_new_prev_hash(
        &mut self,
        message: roles_logic_sv2::mining_sv2::SetNewPrevHashOwned,
    ) -> Result<(), Error> {
        self.native.on_new_prev_hash(message)
    }

    pub(super) fn on_new_extended_mining_job(
        &mut self,
        message: NewExtendedMiningJobOwned,
    ) -> Result<HashMap<u32, MiningOwned, BuildNoHashHasher<u32>>, Error> {
        self.native.on_new_extended_mining_job(message)
    }

    pub(super) fn set_group_channel(
        &mut self,
        message: &roles_logic_sv2::mining_sv2::SetGroupChannel<'_>,
    ) {
        self.native.set_group_channel(message);
    }

    pub(super) fn update_target_for_channel(
        &mut self,
        channel_id: u32,
        target: Target,
    ) -> Option<bool> {
        self.channels.get_mut(&channel_id)?.target = target.into();
        Some(true)
    }

    pub(super) fn set_target(&mut self, target: &mut Target) {
        self.upstream_target = target.clone();
        self.native.set_target(target);
    }

    pub(super) fn on_submit_shares_extended(
        &mut self,
        mut share: SubmitSharesExtendedOwned,
    ) -> Result<OnNewShare, Error> {
        let job = &self
            .native
            .job(share.job_id)
            .ok_or(Error::ShareDoNotMatchAnyJob)?
            .0;
        let prev_hash = self
            .native
            .last_prev_hash()
            .ok_or(Error::ShareDoNotMatchAnyJob)?;
        let nbits = self
            .native
            .last_nbits()
            .ok_or(Error::ShareDoNotMatchAnyJob)?;
        if let Err(code) = roles_logic_sv2::version::validate_version(
            job.version,
            share.version,
            job.version_rolling_allowed,
        ) {
            return reject(&share, code);
        }
        let channel = self
            .channels
            .get(&share.channel_id)
            .ok_or(Error::ShareDoNotMatchAnyChannel)?;
        let mut buffer = [0; 32];
        let extranonce = full_extranonce(channel, &share, &mut buffer)?;
        let hash = share_hash(job, &share, extranonce, prev_hash, nbits)?;
        if hash == Target::new(0, 0) {
            let coinbase = [
                job.coinbase_tx_prefix.as_bytes(),
                extranonce,
                job.coinbase_tx_suffix.as_bytes(),
            ]
            .concat();
            share.extranonce = extranonce[self.extranonces.get_range0_len()..]
                .to_vec()
                .try_into()?;
            return Ok(OnNewShare::ShareMeetBitcoinTarget((
                Share::Extended(share),
                None,
                coinbase,
                extranonce.to_vec(),
            )));
        }
        if hash <= self.upstream_target {
            share.extranonce = extranonce[self.extranonces.get_range0_len()..]
                .to_vec()
                .try_into()?;
            Ok(OnNewShare::SendSubmitShareUpstream((
                Share::Extended(share),
                None,
            )))
        } else if hash <= Target::from(*channel.target.as_array()) {
            Ok(OnNewShare::ShareMeetDownstreamTarget)
        } else {
            reject(&share, ERROR_CODE_SUBMIT_SHARES_DIFFICULTY_TOO_LOW)
        }
    }
}

impl Deref for ChannelFactory {
    type Target = ProxyExtendedChannelFactory;

    fn deref(&self) -> &Self::Target {
        &self.native
    }
}

fn full_extranonce<'a>(
    channel: &OpenExtendedMiningChannelSuccessOwned,
    share: &SubmitSharesExtendedOwned,
    buffer: &'a mut [u8; 32],
) -> Result<&'a [u8], Error> {
    if share.extranonce.as_bytes().len() != usize::from(channel.extranonce_size) {
        return Err(Error::InvalidCoinbase);
    }
    let prefix = channel.extranonce_prefix.as_bytes();
    let full_len = prefix.len() + share.extranonce.as_bytes().len();
    let extranonce = buffer.get_mut(..full_len).ok_or(Error::InvalidCoinbase)?;
    extranonce[..prefix.len()].copy_from_slice(prefix);
    extranonce[prefix.len()..].copy_from_slice(share.extranonce.as_bytes());
    Ok(extranonce)
}

fn share_hash(
    job: &NewExtendedMiningJobOwned,
    share: &SubmitSharesExtendedOwned,
    extranonce: &[u8],
    prev_hash: binary_sv2::U256Owned,
    nbits: u32,
) -> Result<Target, Error> {
    let merkle_root = merkle_root_from_path(
        job.coinbase_tx_prefix.as_bytes(),
        job.coinbase_tx_suffix.as_bytes(),
        extranonce,
        job.merkle_path.as_slice(),
        &[],
    )
    .ok_or(Error::InvalidCoinbase)?;
    let header = Header {
        version: Version::from_consensus(share.version as i32),
        prev_blockhash: u256_to_block_hash(prev_hash),
        merkle_root: TxMerkleNode::from_slice(&merkle_root).map_err(|_| Error::InvalidCoinbase)?,
        time: share.ntime,
        bits: CompactTarget::from_consensus(nbits),
        nonce: share.nonce,
    };
    Ok(header.block_hash().to_byte_array().into())
}

#[derive(Debug, Eq, PartialEq)]
struct Prefix([u8; 32]);

impl StdHash for Prefix {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Native allocation increases the prefix as a big-endian counter. These bytes
        // are chosen locally, and the full prefix still determines equality.
        state.write_u64(u64::from_be_bytes(self.0[24..].try_into().unwrap()));
    }
}

fn prefix_key(prefix: &[u8]) -> Prefix {
    let mut key = [0; 32];
    key[32 - prefix.len()..].copy_from_slice(prefix);
    Prefix(key)
}

fn reject(share: &SubmitSharesExtendedOwned, code: &str) -> Result<OnNewShare, Error> {
    Ok(OnNewShare::SendErrorDownstream(SubmitSharesErrorOwned {
        channel_id: share.channel_id,
        sequence_number: share.sequence_number,
        error_code: code.to_string().try_into()?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use roles_logic_sv2::mining_sv2::SetNewPrevHashOwned;

    #[test]
    fn prefix_index_compares_all_bytes_when_hashes_match() {
        let mut prefixes = HashSet::with_hasher(BuildNoHashHasher::<u64>::default());
        let first = [1; 32];
        let mut second = first;
        second[0] = 2;
        assert!(prefixes.insert(prefix_key(&first)));
        assert!(prefixes.insert(prefix_key(&second)));
        assert!(!prefixes.insert(prefix_key(&first)));
        assert_eq!(prefixes.len(), 2);
        assert!(prefixes.remove(&prefix_key(&first)));
        assert!(prefixes.contains(&prefix_key(&second)));
    }

    fn factories() -> (ChannelFactory, ProxyExtendedChannelFactory) {
        let extranonces = ExtendedExtranonce::from_upstream_extranonce(
            vec![0x17, 0x33].try_into().unwrap(),
            0..2,
            2..3,
            3..16,
        )
        .unwrap();
        let upstream = OpenExtendedMiningChannelSuccessOwned {
            request_id: 1,
            channel_id: 42,
            group_channel_id: 43,
            target: [255; 32].into(),
            extranonce_size: 14,
            extranonce_prefix: vec![0x17, 0x33].try_into().unwrap(),
        };
        let native = ProxyExtendedChannelFactory::new(
            Arc::new(Mutex::new(GroupId::new())),
            extranonces.clone(),
            None,
            10.0,
            ExtendedChannelKind::Proxy {
                upstream_target: [255; 32].into(),
            },
            None,
            upstream.clone(),
        );
        let indexed = ChannelFactory::new(extranonces, 10.0, [255; 32].into(), upstream).unwrap();
        (indexed, native)
    }

    fn job(rolling: bool) -> NewExtendedMiningJobOwned {
        let transaction = bitcoin::Transaction {
            version: bitcoin::transaction::Version::ONE,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: vec![0; 16].into(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(50_000),
                script_pubkey: vec![0x51].into(),
            }],
        };
        let bytes = bitcoin::consensus::serialize(&transaction);
        NewExtendedMiningJobOwned {
            channel_id: 43,
            job_id: 11,
            min_ntime: binary_sv2::Sv2OptionOwned::new(Some(1_700_000_000)),
            version: 0x2000_0000,
            version_rolling_allowed: rolling,
            merkle_path: vec![[5; 32].into(), [9; 32].into()].try_into().unwrap(),
            coinbase_tx_prefix: bytes[..42].to_vec().try_into().unwrap(),
            coinbase_tx_suffix: bytes[58..].to_vec().try_into().unwrap(),
        }
    }

    fn seed(factory: &mut ProxyExtendedChannelFactory, rolling: bool) {
        factory
            .on_new_prev_hash(SetNewPrevHashOwned {
                channel_id: 42,
                job_id: 11,
                prev_hash: [3; 32].into(),
                min_ntime: 1_700_000_000,
                nbits: 0x1d00ffff,
            })
            .unwrap();
        factory.on_new_extended_mining_job(job(rolling)).unwrap();
    }

    fn share(channel_id: u32, nonce: u32) -> SubmitSharesExtendedOwned {
        SubmitSharesExtendedOwned {
            channel_id,
            sequence_number: 123,
            job_id: 11,
            nonce,
            ntime: 1_700_000_001,
            version: 0x2000_0000,
            extranonce: vec![4; 13].try_into().unwrap(),
        }
    }

    fn outcome(result: Result<OnNewShare, Error>) -> (String, Vec<u8>) {
        match result {
            Ok(OnNewShare::SendSubmitShareUpstream((Share::Extended(share), template))) => {
                assert!(template.is_none());
                ("upstream".into(), binary_sv2::to_bytes(share).unwrap())
            }
            Ok(OnNewShare::SendErrorDownstream(error)) => {
                ("rejected".into(), binary_sv2::to_bytes(error).unwrap())
            }
            Ok(OnNewShare::ShareMeetDownstreamTarget) => ("local".into(), vec![]),
            Err(error) => (format!("{error:?}"), vec![]),
            other => panic!("Unexpected share result: {other:?}"),
        }
    }

    #[test]
    fn indexed_channel_open_matches_native_bytes_and_exhaustion() {
        let (mut indexed, mut native) = factories();
        for id in 1..=255 {
            let indexed_message = indexed.open_channel(id, 1e12).unwrap();
            let native_message = native.new_extended_channel(id, 1e12, 13).unwrap().remove(0);
            let MiningOwned::OpenExtendedMiningChannelSuccess(native_message) = native_message
            else {
                panic!("Expected channel assignments");
            };
            assert_eq!(
                binary_sv2::to_bytes(indexed_message).unwrap(),
                binary_sv2::to_bytes(native_message).unwrap()
            );
        }
        assert_eq!(indexed.channels.len(), 255);
        assert_eq!(indexed.prefixes.len(), 255);
        assert!(matches!(
            indexed.open_channel(256, 1e12),
            Err(Error::ExtranonceSpaceEnded)
        ));
        assert!(matches!(
            native.new_extended_channel(256, 1e12, 0),
            Err(Error::ExtranonceSpaceEnded)
        ));
        indexed.close_channel(1);
        indexed.close_channel(1);
        assert_eq!(indexed.channels.len(), 254);
        assert_eq!(indexed.prefixes.len(), 254);
        assert!(matches!(
            indexed.open_channel(257, 1e12),
            Err(Error::ExtranonceSpaceEnded)
        ));
    }

    #[test]
    fn indexed_share_results_match_native_targets_versions_and_bytes() {
        for rolling in [false, true] {
            let (mut indexed, mut native) = factories();
            for _ in 0..3 {
                indexed.open_channel(0, 1e12).unwrap();
                native.new_extended_channel(0, 1e12, 0).unwrap();
            }
            seed(&mut indexed.native, rolling);
            seed(&mut native, rolling);
            for upstream in [0, 127, 255] {
                indexed.set_target(&mut [upstream; 32].into());
                native.set_target(&mut [upstream; 32].into());
                for (channel_id, downstream) in [(1, 0), (2, 127), (3, 255)] {
                    indexed.update_target_for_channel(channel_id, [downstream; 32].into());
                    native.update_target_for_channel(channel_id, [downstream; 32].into());
                    for version in [0x2000_0000, 0x2000_0020, 0x2000_0001] {
                        for nonce in 0..16 {
                            let mut share = share(channel_id, nonce);
                            share.version = version;
                            assert_eq!(
                                outcome(indexed.on_submit_shares_extended(share.clone())),
                                outcome(native.on_submit_shares_extended(share))
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn indexed_share_rejects_bad_channels_jobs_and_extranonce_lengths() {
        let (mut indexed, mut native) = factories();
        indexed.open_channel(0, 1e12).unwrap();
        native.new_extended_channel(0, 1e12, 0).unwrap();
        seed(&mut indexed.native, false);
        seed(&mut native, false);
        let mut cases = vec![share(999, 1)];
        let mut unknown_job = share(1, 1);
        unknown_job.job_id = 999;
        cases.push(unknown_job);
        for size in [0, 12, 14, 32] {
            let mut wrong_size = share(1, 1);
            wrong_size.extranonce = vec![0; size].try_into().unwrap();
            cases.push(wrong_size);
        }
        for share in cases {
            assert_eq!(
                outcome(indexed.on_submit_shares_extended(share.clone())),
                outcome(native.on_submit_shares_extended(share))
            );
        }
        indexed.close_channel(1);
        assert!(matches!(
            indexed.on_submit_shares_extended(share(1, 1)),
            Err(Error::ShareDoNotMatchAnyChannel)
        ));
        assert_eq!(indexed.update_target_for_channel(1, [255; 32].into()), None);
        assert!(indexed.channels.is_empty());
        assert!(indexed.prefixes.is_empty());
    }
}
