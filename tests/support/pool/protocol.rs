pub use roles_logic_sv2::{
    common_messages_sv2::{Protocol, SetupConnectionSuccess},
    job_creator::extended_job_from_custom_job,
    mining_sv2::{SetCustomMiningJobSuccess, SubmitSharesSuccess},
};

pub use roles_logic_sv2::mining::{ExtendedExtranonce, Target};

mod messages {
    pub use binary_sv2::{B064KOwned as B064K, Seq064KOwned as Seq064K, U256Owned as U256};
    pub use roles_logic_sv2::{
        common_messages_sv2::{
            SetupConnectionErrorOwned as SetupConnectionError,
            SetupConnectionOwned as SetupConnection,
        },
        job_declaration_sv2::{
            AllocateMiningJobTokenSuccessOwned as AllocateMiningJobTokenSuccess,
            DeclareMiningJobErrorOwned as DeclareMiningJobError,
            DeclareMiningJobOwned as DeclareMiningJob,
            DeclareMiningJobSuccessOwned as DeclareMiningJobSuccess,
            ProvideMissingTransactionsOwned as ProvideMissingTransactions,
            ProvideMissingTransactionsSuccessOwned as ProvideMissingTransactionsSuccess,
        },
        mining_sv2::{
            NewExtendedMiningJobOwned as NewExtendedMiningJob,
            OpenExtendedMiningChannelSuccessOwned as OpenExtendedMiningChannelSuccess,
            SetCustomMiningJobErrorOwned as SetCustomMiningJobError,
            SetCustomMiningJobOwned as SetCustomMiningJob, SetNewPrevHashOwned as SetNewPrevHash,
            SetTargetOwned as SetTarget, SubmitSharesErrorOwned as SubmitSharesError,
            SubmitSharesExtendedOwned as SubmitSharesExtended,
            UpdateChannelErrorOwned as UpdateChannelError,
        },
        parsers::{
            CommonMessagesOwned as CommonMessages, JobDeclarationOwned as JobDeclaration,
            MiningOwned as Mining, TemplateDistributionOwned as TemplateDistribution,
        },
        template_distribution_sv2::{
            CoinbaseOutputConstraints, NewTemplateOwned as NewTemplate, RequestTransactionData,
            RequestTransactionDataSuccessOwned as RequestTransactionDataSuccess,
            SetNewPrevHashOwned as TemplatePrevHash,
        },
    };
}

pub use messages::*;

pub fn encode_coinbase_outputs(outputs: Vec<bitcoin::TxOut>) -> Vec<u8> {
    bitcoin::consensus::serialize(&outputs)
}

pub fn decode_coinbase_outputs(bytes: &[u8]) -> crate::support::TestResult<Vec<bitcoin::TxOut>> {
    Ok(bitcoin::consensus::deserialize(bytes)?)
}

pub fn declaration_coinbase(message: &DeclareMiningJob) -> (&[u8], &[u8]) {
    (
        message.coinbase_tx_prefix.as_ref(),
        message.coinbase_tx_suffix.as_ref(),
    )
}

pub fn declaration_wtxids(message: &DeclareMiningJob) -> Vec<U256> {
    message.wtxid_list.clone().into_inner()
}

pub fn set_declaration_coinbase(message: &mut DeclareMiningJob, prefix: B064K, suffix: B064K) {
    message.coinbase_tx_prefix = prefix;
    message.coinbase_tx_suffix = suffix;
}
