#![allow(unused_crate_dependencies)]
#[tokio::main]
async fn main() {
    let config = match dmnd_client::Configuration::from_cli() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    };
    dmnd_client::start(config).await;
}
