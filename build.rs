fn main() {
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rustc-check-cfg=cfg(legacy_sv2_transport)");

    let source = std::fs::read_to_string("Cargo.toml").expect("cannot read Cargo.toml");
    let manifest: toml::Value = toml::from_str(&source).expect("cannot parse Cargo.toml");
    let has_codec_dependency = manifest
        .get("dependencies")
        .and_then(|dependencies| dependencies.get("codec_sv2"))
        .is_some();
    if has_codec_dependency {
        println!("cargo:rustc-cfg=legacy_sv2_transport");
    }
}
