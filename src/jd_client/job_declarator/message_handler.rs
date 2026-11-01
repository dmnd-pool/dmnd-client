use super::JobDeclarator;
use roles_logic_sv2::{
    handlers::{job_declaration::ParseServerJobDeclarationMessages, SendTo_},
    job_declaration_sv2::ProvideMissingTransactionsSuccessOwned,
    parsers::JobDeclarationOwned,
};
pub type SendTo = SendTo_<JobDeclarationOwned, ()>;
use roles_logic_sv2::errors::Error;
use tracing::warn;

impl ParseServerJobDeclarationMessages for JobDeclarator {
    fn handle_allocate_mining_job_token_success(
        &mut self,
        message: roles_logic_sv2::job_declaration_sv2::AllocateMiningJobTokenSuccess,
    ) -> Result<SendTo, Error> {
        self.allocated_tokens.push(message.into_owned());

        Ok(SendTo::None(None))
    }

    fn handle_declare_mining_job_success(
        &mut self,
        message: roles_logic_sv2::job_declaration_sv2::DeclareMiningJobSuccess,
    ) -> Result<SendTo, Error> {
        let message = JobDeclarationOwned::DeclareMiningJobSuccess(message.into_owned());
        Ok(SendTo::None(Some(message)))
    }

    fn handle_declare_mining_job_error(
        &mut self,
        message: roles_logic_sv2::job_declaration_sv2::DeclareMiningJobError,
    ) -> Result<SendTo, Error> {
        Ok(SendTo::None(Some(
            JobDeclarationOwned::DeclareMiningJobError(message.into_owned()),
        )))
    }

    fn handle_provide_missing_transactions(
        &mut self,
        message: roles_logic_sv2::job_declaration_sv2::ProvideMissingTransactions,
    ) -> Result<SendTo, Error> {
        let Some(last_declare) = self.last_declare_mining_jobs_sent.get(&message.request_id) else {
            warn!(
                request_id = message.request_id,
                "ignoring missing-transactions request for an unknown declaration"
            );
            return Ok(SendTo::None(None));
        };
        let tx_list = &last_declare
            .as_ref()
            .ok_or(Error::JDSMissingTransactions)?
            .tx_list;

        let request_id = message.request_id;
        let transaction_list =
            requested_transactions(tx_list, message.unknown_tx_position_list.as_slice())?;
        let message_provide_missing_transactions = ProvideMissingTransactionsSuccessOwned {
            request_id,
            transaction_list,
        };
        let message_enum = JobDeclarationOwned::ProvideMissingTransactionsSuccess(
            message_provide_missing_transactions,
        );
        Ok(SendTo::Respond(message_enum))
    }
}

fn requested_transactions(
    transactions: &binary_sv2::Seq064KOwned<binary_sv2::B016MOwned>,
    positions: &[u16],
) -> Result<binary_sv2::Seq064KOwned<binary_sv2::B016MOwned>, Error> {
    let selected = positions
        .iter()
        .map(|&position| {
            transactions
                .as_slice()
                .get(usize::from(position))
                .cloned()
                .ok_or(Error::JDSMissingTransactions)
        })
        .collect::<Result<Vec<_>, _>>()?;
    binary_sv2::Seq064KOwned::new(selected).map_err(|_| Error::JDSMissingTransactions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_transactions_reject_unknown_positions() {
        let transactions =
            binary_sv2::Seq064KOwned::new(vec![vec![1].try_into().unwrap()]).unwrap();
        assert!(requested_transactions(&transactions, &[0, 1]).is_err());
        assert_eq!(
            requested_transactions(&transactions, &[0, 0])
                .unwrap()
                .len(),
            2
        );
    }
}
