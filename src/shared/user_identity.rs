use binary_sv2::Str0255;
use bitcoin::hex::DisplayHex;

pub fn new_user_identity() -> Str0255<'static> {
    let random_bytes = rand::random::<[u8; 16]>();
    format!("ABC{}", random_bytes.as_hex())
        .try_into()
        .expect("The 35-byte user identity must fit in Str0255")
}
