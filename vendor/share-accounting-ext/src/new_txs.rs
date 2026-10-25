use binary_sv2::{B016MOwned, Seq064KOwned};
use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct NewTxs {
    pub transactions: Seq064KOwned<B016MOwned>,
}
