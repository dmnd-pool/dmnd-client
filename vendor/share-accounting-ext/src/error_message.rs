use binary_sv2::Str0255Owned;
use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ErrorMessage {
    pub message: Str0255Owned,
}
