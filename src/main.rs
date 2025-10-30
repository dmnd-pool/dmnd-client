#![allow(unused_crate_dependencies)] // To avoid warnings about unused dependencies in this binary crate since the dependencies are used in the library crate.
#[tokio::main]
async fn main() {
    demand_cli::start().await;
}
