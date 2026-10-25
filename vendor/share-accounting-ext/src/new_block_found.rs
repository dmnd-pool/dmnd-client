use binary_sv2::U256Owned;
use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct NewBlockFound {
    pub block_hash: U256Owned,
}
