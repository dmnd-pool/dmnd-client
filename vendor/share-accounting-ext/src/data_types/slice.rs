use crate::Hash256;
use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Slice {
    pub number_of_shares: u32,

    pub difficulty: u64,

    pub fees: u64,

    pub root: Hash256,

    pub job_id: u64,
}
