#[cfg(not(feature = "with_serde"))]
use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2;
#[cfg(feature = "with_serde")]
use binary_sv2::GetSize;
use binary_sv2::{Deserialize, Seq064K, Serialize, B016M, U256};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetTransationsInJob {
    pub request_id: u32,
    pub job_id: u64,
}
#[cfg(feature = "with_serde")]
impl GetSize for GetTransationsInJob {
    fn get_size(&self) -> usize {
        self.request_id.get_size() + self.job_id.get_size()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetTransationsInJobSuccess<'decoder> {
    pub window_req_id: u32,
    pub request_id: u32,
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub coinbase_id: U256<'decoder>,
    // pub tx_short_hash_nonce: u64,
    tx_list: Seq064K<'decoder, B016M<'decoder>>,
    // #[cfg_attr(feature = "with_serde", serde(borrow))]
    // pub tx_short_hash_list: Seq064K<'decoder, ShortTxId<'decoder>>,
    // pub tx_hash_list_hash: U256<'decoder>,
}
#[cfg(feature = "with_serde")]
impl<'d> GetSize for GetTransationsInJobSuccess<'d> {
    fn get_size(&self) -> usize {
        self.window_req_id.get_size()
            + self.request_id.get_size()
            + self.coinbase.get_size()
            + self.tx_short_hash_nonce.get_size()
            + self.tx_short_hash_list.get_size()
            + self.tx_hash_list_hash.get_size()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct IdentifyTransations {
    pub request_id: u32,
}
#[cfg(feature = "with_serde")]
impl GetSize for IdentifyTransations {
    fn get_size(&self) -> usize {
        self.request_id.get_size()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct IdentifyTransationsSuccess<'decoder> {
    pub request_id: u32,
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub tx_data_hashes: Seq064K<'decoder, U256<'decoder>>,
}
#[cfg(feature = "with_serde")]
impl<'d> GetSize for IdentifyTransationsSuccess<'d> {
    fn get_size(&self) -> usize {
        self.request_id.get_size() + self.tx_data_hashet.get_size()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ProvideMissinTransactions<'decoder> {
    pub request_id: u32,
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub unknown_tx_position_list: Seq064K<'decoder, u16>,
}
#[cfg(feature = "with_serde")]
impl<'d> GetSize for ProvideMissinTransactions<'d> {
    fn get_size(&self) -> usize {
        self.request_id.get_size() + self.unknown_tx_position_list.get_size()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ProvideMissinTransactionsSuccess<'decoder> {
    pub request_id: u32,
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub transaction_list: Seq064K<'decoder, B016M<'decoder>>,
}
#[cfg(feature = "with_serde")]
impl<'d> GetSize for ProvideMissinTransactionsSuccess<'d> {
    fn get_size(&self) -> usize {
        self.request_id.get_size() + self.transaction_list.get_size()
    }
}
