use roles_logic_sv2::{
    job_creator::extended_job_to_non_segwit,
    mining_sv2::{NewExtendedMiningJobOwned, SetNewPrevHashOwned},
};
use sv1_api::{
    server_to_client,
    utils::{HexU32Be, MerkleNode, PrevHash},
};
use tracing::info;

/// Creates a new SV1 `mining.notify` message if both SV2 `SetNewPrevHash` and
/// `NewExtendedMiningJob` messages have been received. If one of these messages is still being
/// waited on, the function returns `None`.
/// If clean_jobs = false, it means a new job is created, with the same PrevHash
pub fn create_notify(
    new_prev_hash: SetNewPrevHashOwned,
    new_job: NewExtendedMiningJobOwned,
    clean_jobs: bool,
    extranonce_len: usize,
) -> server_to_client::Notify {
    let new_job = extended_job_to_non_segwit(new_job, extranonce_len)
        .expect("failed to convert extended job to non segwit");
    let channel_id = new_job.channel_id;
    let job_id = new_job.job_id.to_string();
    let version = HexU32Be(new_job.version);
    let bits = HexU32Be(new_prev_hash.nbits);
    let time = HexU32Be(
        new_job
            .min_ntime
            .into_inner()
            .unwrap_or(new_prev_hash.min_ntime),
    );
    let prev_hash = PrevHash(new_prev_hash.prev_hash);
    let coin_base1 = new_job.coinbase_tx_prefix.into_bytes().into();
    let coin_base2 = new_job.coinbase_tx_suffix.into_bytes().into();
    let merkle_branch = new_job
        .merkle_path
        .into_inner()
        .into_iter()
        .map(MerkleNode)
        .collect();

    let notify_response = server_to_client::Notify {
        job_id,
        prev_hash,
        coin_base1,
        coin_base2,
        merkle_branch,
        version,
        bits,
        time,
        clean_jobs,
    };
    info!(
        "NextMiningNotify created for channel id: {:?}, Job id: {:?}, PrevHash: {:?} version: {:?}, bits: {:?}, time: {:?}, clean_jobs: {}",
        channel_id,
        notify_response.job_id, notify_response.prev_hash.0.reversed_hex_string(), notify_response.version, notify_response.bits, notify_response.time,
        notify_response.clean_jobs
    );
    notify_response
}
