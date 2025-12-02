use super::JobDeclarator;
use roles_logic_sv2::{
    handlers::{job_declaration::ParseServerJobDeclarationMessages, SendTo_},
    job_declaration_sv2::{
        AllocateMiningJobTokenSuccess, DeclareMiningJobError, DeclareMiningJobSuccess,
        ProvideMissingTransactions, ProvideMissingTransactionsSuccess,
    },
    parsers::JobDeclaration,
};
pub type SendTo = SendTo_<JobDeclaration<'static>, ()>;
use roles_logic_sv2::errors::Error;
use tracing::debug;

impl ParseServerJobDeclarationMessages for JobDeclarator {
    fn handle_allocate_mining_job_token_success(
        &mut self,
        message: AllocateMiningJobTokenSuccess,
    ) -> Result<SendTo, Error> {
        self.allocated_tokens.push(message.into_static());

        Ok(SendTo::None(None))
    }

    fn handle_declare_mining_job_success(
        &mut self,
        message: DeclareMiningJobSuccess,
    ) -> Result<SendTo, Error> {
        let message = JobDeclaration::DeclareMiningJobSuccess(message.into_static());
        Ok(SendTo::None(Some(message)))
    }

    fn handle_declare_mining_job_error(
        &mut self,
        _message: DeclareMiningJobError,
    ) -> Result<SendTo, Error> {
        // TODO consider using declarative names instead of setting states
        super::super::IS_CUSTOM_JOB_SET.store(true, std::sync::atomic::Ordering::Release);
        Ok(SendTo::None(None))
    }

    fn handle_provide_missing_transactions(
        &mut self,
        message: ProvideMissingTransactions,
    ) -> Result<SendTo, Error> {
        let request_id = message.request_id;
        let tx_list = self
            .last_declare_mining_jobs_sent
            .get(&request_id)
            .ok_or(Error::UnknownRequestId(request_id))?
            .clone()
            .ok_or(Error::JDSMissingTransactions)?
            .tx_list
            .into_inner();

        let unknown_tx_position_list: Vec<u16> = message.unknown_tx_position_list.into_inner();
        let missing_transactions: Vec<binary_sv2::B016M> = unknown_tx_position_list
            .iter()
            .filter_map(|&pos| tx_list.get(pos as usize).cloned())
            .collect();

        debug!(
            request_id,
            requested_txs = unknown_tx_position_list.len(),
            "Sending ProvideMissingTransactionsSuccess"
        );

        let transaction_list = binary_sv2::Seq064K::new(missing_transactions)
            .map_err(|_| Error::JDSMissingTransactions)?;
        let message_provide_missing_transactions = ProvideMissingTransactionsSuccess {
            request_id,
            transaction_list,
        };
        let message_enum =
            JobDeclaration::ProvideMissingTransactionsSuccess(message_provide_missing_transactions);
        Ok(SendTo::Respond(message_enum))
    }
}
