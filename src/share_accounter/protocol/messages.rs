use super::{Hash256, SequenceOwned};
use binary_sv2::{
    B016MOwned, B032Owned, B064KOwned, Deserialize, Seq064KOwned, Serialize, Str0255Owned,
    U256Owned,
};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareOk {
    pub ref_job_id: u64,
    pub share_index: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NewBlockFound {
    pub block_hash: U256Owned,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetWindow {
    pub block_hash: U256Owned,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetWindowSuccess {
    pub slices: SequenceOwned<Slice>,
    pub phashes: SequenceOwned<PHash>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetWindowBusy {
    pub retry_in_seconds: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetShares {
    pub request_id: u32,
    pub shares: Seq064KOwned<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetSharesSuccess {
    pub shares: SequenceOwned<Share>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NewTxs {
    pub transactions: Seq064KOwned<B016MOwned>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ErrorMessage {
    pub message: Str0255Owned,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Slice {
    pub number_of_shares: u32,
    pub difficulty: u64,
    pub fees: u64,
    pub root: Hash256,
    pub job_id: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PHash {
    pub phash: Hash256,
    pub index_start: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Share {
    pub nonce: u32,
    pub ntime: u32,
    pub version: u32,
    pub extranonce: B032Owned,
    pub job_id: u64,
    pub reference_job_id: u64,
    pub share_index: u32,
    pub merkle_path: B064KOwned,
}
