use binary_sv2::{B016MOwned, Seq064KOwned, U256Owned};
use binary_sv2::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetTransationsInJob {
    pub request_id: u32,
    pub job_id: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetTransationsInJobSuccess {
    pub window_req_id: u32,
    pub request_id: u32,

    pub coinbase_id: U256Owned,

    tx_list: Seq064KOwned<B016MOwned>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct IdentifyTransations {
    pub request_id: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct IdentifyTransationsSuccess {
    pub request_id: u32,

    pub tx_data_hashes: Seq064KOwned<U256Owned>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ProvideMissinTransactions {
    pub request_id: u32,

    pub unknown_tx_position_list: Seq064KOwned<u16>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ProvideMissinTransactionsSuccess {
    pub request_id: u32,

    pub transaction_list: Seq064KOwned<B016MOwned>,
}
