use binary_sv2::{B032Owned, B064KOwned};
use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
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
