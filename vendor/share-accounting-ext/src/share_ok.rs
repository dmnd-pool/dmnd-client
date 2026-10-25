use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ShareOk {
    pub ref_job_id: u64,
    pub share_index: u32,
}
