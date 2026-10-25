use crate::Hash256;
use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct PHash {
    pub phash: Hash256,
    pub index_start: u32,
}
