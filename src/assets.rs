use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "dashboard_build/"]
pub struct Asset;
