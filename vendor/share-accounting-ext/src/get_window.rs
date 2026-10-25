use crate::{PHash, SequenceOwned, Slice};
use binary_sv2::U256Owned;
use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetWindow {
    pub block_hash: U256Owned,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetWindowSuccess {
    pub slices: SequenceOwned<Slice>,

    pub phashes: SequenceOwned<PHash>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetWindowBusy {
    pub retry_in_seconds: u64,
}
