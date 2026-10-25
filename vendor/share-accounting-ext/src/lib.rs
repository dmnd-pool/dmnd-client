#![cfg_attr(feature = "no_std", no_std)]

#[macro_use]
extern crate alloc;

mod activate_ext;
mod r#const;
mod data_types;
mod error_message;
mod get_shares;
mod get_window;
mod new_block_found;
mod new_txs;
pub mod parser;
mod share_ok;
mod verify_fees;

pub use crate::r#const::*;
pub use activate_ext::{Activate, ActivateSuccess};
pub use data_types::{Hash256, PHash, Share, Slice};
pub use error_message::ErrorMessage;
pub use get_shares::{GetShares, GetSharesSuccess};
pub use get_window::{GetWindow, GetWindowBusy, GetWindowSuccess};
pub use new_block_found::NewBlockFound;
pub use new_txs::NewTxs;
pub use share_ok::ShareOk;
pub use verify_fees::{
    GetTransationsInJob, GetTransationsInJobSuccess, IdentifyTransations,
    IdentifyTransationsSuccess, ProvideMissinTransactions, ProvideMissinTransactionsSuccess,
};
