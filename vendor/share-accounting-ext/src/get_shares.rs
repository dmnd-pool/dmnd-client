use crate::{SequenceOwned, Share};
use binary_sv2::Seq064KOwned;
use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetShares {
    pub request_id: u32,

    pub shares: Seq064KOwned<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetSharesSuccess {
    pub shares: SequenceOwned<Share>,
}
