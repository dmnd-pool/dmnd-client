use super::{
    protocol::{
        CoinbaseOutputConstraints, CommonMessages, Protocol, RequestTransactionData,
        RequestTransactionDataSuccess, SetupConnection, TemplateDistribution,
    },
    transport::{Message, PeerConnection},
    Inbound,
};
use crate::support::{error, TestResult};
use bitcoin::{consensus::deserialize, hashes::Hash, Transaction};
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::mpsc;

#[derive(Default)]
pub struct TemplateStore {
    pub transactions: HashMap<[u8; 32], Vec<u8>>,
}

impl TemplateStore {
    pub fn transaction_data(&mut self, data: RequestTransactionDataSuccess) -> TestResult {
        for bytes in data.transaction_list.into_inner() {
            let tx: Transaction = deserialize(bytes.as_ref())?;
            self.transactions
                .insert(tx.compute_wtxid().to_byte_array(), bytes.as_ref().to_vec());
        }
        // Only JD declarations need the cached mempool; request missing transactions when evicted.
        if self.transactions.len() > 32768 {
            self.transactions.clear();
        }
        Ok(())
    }
}

pub struct TemplateClient;

impl TemplateClient {
    pub(super) async fn run(address: SocketAddr, events: mpsc::Sender<Inbound>) -> TestResult {
        let mut connection = PeerConnection::connect(address).await?;
        connection
            .send(Message::Common(CommonMessages::SetupConnection(
                SetupConnection {
                    protocol: Protocol::TemplateDistributionProtocol,
                    min_version: 2,
                    max_version: 2,
                    flags: 0,
                    endpoint_host: "127.0.0.1".to_string().try_into().expect("host"),
                    endpoint_port: address.port(),
                    vendor: "mining-e2e".to_string().try_into().expect("vendor"),
                    hardware_version: String::new().try_into().expect("empty"),
                    firmware: String::new().try_into().expect("empty"),
                    device_id: String::new().try_into().expect("empty"),
                },
            )))
            .await?;
        match connection.recv().await? {
            Some(Message::Common(CommonMessages::SetupConnectionSuccess(_))) => {}
            other => return Err(error(format!("TP setup rejected: {other:?}"))),
        }
        connection
            .send(Message::TemplateDistribution(
                TemplateDistribution::CoinbaseOutputConstraints(CoinbaseOutputConstraints {
                    coinbase_output_max_additional_size: 10,
                    coinbase_output_max_additional_sigops: 0,
                }),
            ))
            .await?;
        while let Some(message) = connection.recv().await? {
            if let Message::TemplateDistribution(template_message) = message {
                if let TemplateDistribution::NewTemplate(template) = &template_message {
                    connection
                        .send(Message::TemplateDistribution(
                            TemplateDistribution::RequestTransactionData(RequestTransactionData {
                                template_id: template.template_id,
                            }),
                        ))
                        .await?;
                }
                events
                    .send(Inbound::Template(template_message))
                    .await
                    .map_err(|_| error("pool stopped"))?;
            } else {
                return Err(error("unexpected TP message"));
            }
        }
        Err(error("TP disconnected"))
    }
}
