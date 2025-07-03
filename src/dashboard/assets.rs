use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "build/dashboard"]
pub struct Asset;
