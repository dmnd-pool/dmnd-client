use super::TemplateRx;
use roles_logic_sv2::{
    errors::Error,
    handlers::template_distribution::{ParseServerTemplateDistributionMessages, SendTo},
    parsers::TemplateDistributionOwned,
    template_distribution_sv2::*,
};

impl ParseServerTemplateDistributionMessages for TemplateRx {
    fn handle_new_template(
        &mut self,
        m: roles_logic_sv2::template_distribution_sv2::NewTemplate,
    ) -> Result<SendTo, Error> {
        let new_template = m.into_owned();
        let new_template = TemplateDistributionOwned::NewTemplate(new_template);
        Ok(SendTo::None(Some(new_template)))
    }

    fn handle_set_new_prev_hash(
        &mut self,
        m: roles_logic_sv2::template_distribution_sv2::SetNewPrevHash,
    ) -> Result<SendTo, Error> {
        let new_prev_hash = SetNewPrevHashOwned {
            template_id: m.template_id,
            prev_hash: m.prev_hash.into_owned(),
            header_timestamp: m.header_timestamp,
            n_bits: m.n_bits,
            target: m.target.into_owned(),
        };
        let new_prev_hash = TemplateDistributionOwned::SetNewPrevHash(new_prev_hash);
        Ok(SendTo::None(Some(new_prev_hash)))
    }

    fn handle_request_tx_data_success(
        &mut self,
        m: roles_logic_sv2::template_distribution_sv2::RequestTransactionDataSuccess,
    ) -> Result<SendTo, Error> {
        let m = RequestTransactionDataSuccessOwned {
            transaction_list: m.transaction_list.into_owned(),
            excess_data: m.excess_data.into_owned(),
            template_id: m.template_id,
        };
        let tx_received = TemplateDistributionOwned::RequestTransactionDataSuccess(m);
        Ok(SendTo::None(Some(tx_received)))
    }

    fn handle_request_tx_data_error(
        &mut self,
        _m: roles_logic_sv2::template_distribution_sv2::RequestTransactionDataError,
    ) -> Result<SendTo, Error> {
        let m = RequestTransactionDataErrorOwned {
            template_id: _m.template_id,
            error_code: _m.error_code.into_owned(),
        };
        let error_code_string =
            std::str::from_utf8(m.error_code.as_ref()).unwrap_or("unknown error code");
        match error_code_string {
            "template-id-not-found" => Err(Error::NoValidTemplate(error_code_string.to_string())),
            "stale-template-id" => Ok(SendTo::None(Some(
                TemplateDistributionOwned::RequestTransactionDataError(m),
            ))),
            _ => Err(Error::NoValidTemplate(error_code_string.to_string())),
        }
    }
}
