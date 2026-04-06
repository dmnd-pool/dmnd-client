#![allow(unused_crate_dependencies)]
#[tokio::main]
async fn main() {
    let config = dmnd_client::Configuration::from_cli();
    dmnd_client::start(config).await;
}
