use super::{Downstream, DownstreamMessages, SetDownstreamTarget};
use pid::Pid;
use roles_logic_sv2::{self, utils::from_u128_to_u256};
use sv1_api::{self};

use super::super::error::{Error, ProxyResult};
use primitive_types::U256;
use roles_logic_sv2::utils::Mutex;
use std::ops::{Div, Mul};
use std::sync::Arc;
use std::time::Duration;
use sv1_api::json_rpc;

use tracing::{error, info};

// Keep the non-local floor aligned with the power-of-two downstream difficulty policy.
pub const NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY: f32 = 131_072.0;

impl Downstream {
    /// Initializes difficult managment.
    /// Send downstream a first target.
    pub async fn init_difficulty_management(self_: &'_ Arc<Mutex<Self>>) -> ProxyResult<'_, ()> {
        let (diff, stats_sender, connection_id, estimated_hashrate) = self_.safe_lock(|d| {
            (
                d.difficulty_mgmt
                    .current_difficulties
                    .back()
                    .copied()
                    .unwrap_or(d.difficulty_mgmt.initial_difficulty),
                d.stats_sender.clone(),
                d.connection_id,
                d.difficulty_mgmt.estimated_downstream_hash_rate,
            )
        })?;

        let (message, _) = diff_to_sv1_message(diff as f64)?;
        Downstream::send_message_downstream(self_.clone(), message.clone()).await;
        stats_sender.update_diff(connection_id, diff);
        stats_sender.update_hashrate(connection_id, estimated_hashrate);

        // Even when dynamic difficulty updates are disabled, miners still need the initial
        // mining.set_difficulty bootstrap before they will submit shares.
        if crate::config::Configuration::difficulty_updates_disabled() {
            return Ok(());
        }

        let total_delay = Duration::from_secs(crate::Configuration::delay());
        let repeat_interval = Duration::from_secs(30);

        let self_clone = self_.clone();
        tokio::spawn(async move {
            let mut elapsed = Duration::from_secs(0);

            // Resend the same mining.set_difficulty message during delay to avoid miner connection timeout
            while elapsed < total_delay {
                let sleep_duration = repeat_interval.min(total_delay - elapsed);
                tokio::time::sleep(sleep_duration).await;
                elapsed += sleep_duration;

                Downstream::send_message_downstream(self_clone.clone(), message.clone()).await;
            }
        });
        let downstream = self_.clone();
        tokio::spawn(crate::translator::utils::check_share_rate_limit(downstream));

        Ok(())
    }

    /// Called before a miner disconnects so we can remove the miner's hashrate from the
    /// aggregated channel hashrate.
    pub fn remove_downstream_hashrate_from_channel(
        self_: &Arc<Mutex<Self>>,
    ) -> ProxyResult<'static, ()> {
        let (upstream_diff, estimated_downstream_hash_rate, should_subtract) =
            self_.safe_lock(|d| {
                (
                    d.upstream_difficulty_config.clone(),
                    d.difficulty_mgmt.estimated_downstream_hash_rate,
                    d.take_channel_hashrate_registered(),
                )
            })?;
        if !should_subtract {
            return Ok(());
        }
        info!(
            "Removing downstream hashrate from channel upstream_diff: {:?}, downstream_diff: {:?}",
            upstream_diff, estimated_downstream_hash_rate
        );
        upstream_diff.safe_lock(|u| {
            u.channel_nominal_hashrate -=
                // Make sure that upstream channel hasrate never goes below 0
                f32::min(estimated_downstream_hash_rate, u.channel_nominal_hashrate);
        })?;
        Ok(())
    }

    /// Checks the downstream's difficulty based on recent share submissions. And if is worth an update, update the
    /// downstream and the bridge.
    pub async fn try_update_difficulty_settings(
        self_: &Arc<Mutex<Self>>,
    ) -> ProxyResult<'static, ()> {
        if crate::config::Configuration::difficulty_updates_disabled() {
            return Ok(());
        }

        let channel_id = self_
            .clone()
            .safe_lock(|d| d.connection_id)
            .map_err(|_e| Error::TranslatorDiffConfigMutexPoisoned)?;

        if let Some(new_diff) = Self::update_difficulty_and_hashrate(self_)? {
            Self::update_diff_setting(self_, channel_id, new_diff.into()).await?;
        }
        Ok(())
    }

    /// This function:
    /// 1. Sends new difficulty as a SV1 message.
    /// 2. Resends the last `mining.notify` (if set).
    /// 3. Notifying the bridge of the updated target for channel.
    async fn update_diff_setting(
        self_: &Arc<Mutex<Self>>,
        channel_id: u32,
        new_diff: f64,
    ) -> ProxyResult<'static, ()> {
        if crate::config::Configuration::difficulty_updates_disabled() {
            return Ok(());
        }

        // Send messages downstream
        let (message, target) = diff_to_sv1_message(new_diff)?;
        Downstream::send_message_downstream(self_.clone(), message).await;

        // Get the last notify
        let recent_notify = self_
            .safe_lock(|d| d.recent_jobs.clone_last(new_diff as f32))
            .map_err(|_| Error::TranslatorDiffConfigMutexPoisoned)?;

        if let Some(notify) = recent_notify {
            Downstream::send_message_downstream(self_.clone(), notify.into()).await;
        }

        // Notify bridge of target update.
        let update_target_msg = SetDownstreamTarget {
            channel_id,
            new_target: target.into(),
        };
        Downstream::send_message_upstream(
            self_,
            DownstreamMessages::SetDownstreamTarget(update_target_msg),
        )
        .await;
        Ok(())
    }

    /// Increments the number of shares since the last difficulty update.
    pub(super) fn save_share(self_: Arc<Mutex<Self>>) -> ProxyResult<'static, ()> {
        if crate::config::Configuration::difficulty_updates_disabled() {
            return Ok(());
        }

        self_.safe_lock(|d| {
            d.difficulty_mgmt.on_new_valid_share();
        })?;
        Ok(())
    }

    /// Converts difficulty to a 256-bit target.
    /// The target T is calculated as T = pdiff / D, where pdiff is the maximum target
    pub fn difficulty_to_target(difficulty: f32) -> [u8; 32] {
        // Clamp difficulty to avoid division by zero or overflow
        let difficulty = f32::max(difficulty, 0.001);

        let pdiff: [u8; 32] = [
            0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let pdiff = U256::from_big_endian(&pdiff);
        let scale: u128 = 1_000_000;

        // To handle the floating-point diff and `pdiff`, we scale it by 10^6 (1_000_000) to convert it to an integer
        // For example, if difficulty is 0.001:
        //   diff_int = 0.001 * 1e6 = 1_000
        let scaled_difficulty = difficulty * (scale as f32);

        if scaled_difficulty > (u128::MAX as f32) {
            panic!("Difficulty too large: scaled value exceeds u128 maximum");
        }
        let diff: u128 = scaled_difficulty as u128;

        let diff = from_u128_to_u256(diff);
        let scale = from_u128_to_u256(scale);

        let target = pdiff.mul(scale).div(diff);
        let target: [u8; 32] = target.to_big_endian();

        target
    }

    /// 1. Calculates the realized share rate since the last update.
    /// 2. Adjusts difficulty using a PID controller, with aggressive tuning for zero-share cases over 5 secs.
    /// 3. Estimates a new hash rate and reconciles the miner state immediately, even when the
    ///    quantized difficulty bucket stays unchanged.
    ///
    /// Returns `Some(new_difficulty)` when a new downstream difficulty should be sent, or `None`
    /// when no `mining.set_difficulty` update is needed.
    pub fn update_difficulty_and_hashrate(
        self_: &Arc<Mutex<Self>>,
    ) -> ProxyResult<'static, Option<f32>> {
        let timestamp_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis();

        let realized_share_per_min = match self_.safe_lock(|d| {
            d.last_call_to_update_hr = timestamp_millis;
            d.difficulty_mgmt.share_count()
        })? {
            Some(value) => value,
            // we need at least 2 seconds of data
            None => return Ok(None),
        };

        if realized_share_per_min.is_sign_negative() {
            error!("realized_share_per_min should not be negative");
            return Err(Error::Unrecoverable);
        }
        if realized_share_per_min.is_nan() {
            error!("realized_share_per_min should not be nan");
            return Err(Error::Unrecoverable);
        }

        let (mut pid, latest_difficulty, initial_difficulty, hard_minimum_difficulty) = self_
            .safe_lock(|d| {
                (
                    d.difficulty_mgmt.pid_controller,
                    d.difficulty_mgmt
                        .current_difficulties
                        .back()
                        .copied()
                        .unwrap_or(d.difficulty_mgmt.initial_difficulty),
                    d.difficulty_mgmt.initial_difficulty,
                    d.difficulty_mgmt.hard_minimum_difficulty,
                )
            })?;

        let pid_output = pid.next_control_output(realized_share_per_min).output;
        let new_difficulty = quantize_downstream_difficulty(
            (latest_difficulty + pid_output).max(initial_difficulty * 0.1),
            hard_minimum_difficulty,
        );
        let new_estimation =
            Self::estimate_hash_rate_from_difficulty(new_difficulty, *crate::SHARE_PER_MIN);
        if new_difficulty == latest_difficulty {
            Self::reconcile_hash_rate_estimation(self_, new_estimation)?;
            return Ok(None);
        }

        if new_difficulty != initial_difficulty {
            let mut pid: Pid<f32> = Pid::new(*crate::SHARE_PER_MIN, new_difficulty * 10.0);
            let pk = -new_difficulty * 0.01;
            //let pi = initial_difficulty * 0.1;
            //let pd = initial_difficulty * 0.01;
            pid.p(pk, f32::MAX).i(0.0, f32::MAX).d(0.0, f32::MAX);
            self_.safe_lock(|d| {
                d.difficulty_mgmt.initial_difficulty = new_difficulty;
                d.difficulty_mgmt.pid_controller = pid;
            })?;
        }

        Self::update_self_with_new_hash_rate(self_, new_estimation, new_difficulty)?;
        Ok(Some(new_difficulty))
    }

    /// Estimates a miner's hash rate from its difficulty and share submission rate.
    /// Uses the formula: hash_rate = shares_per_second * difficulty * 2^32.
    fn estimate_hash_rate_from_difficulty(difficulty: f32, share_per_min: f32) -> f32 {
        let share_per_second = share_per_min / 60.0;
        share_per_second * difficulty * 2f32.powi(32)
    }

    fn hashrate_estimation_changed(old_estimation: f32, new_estimation: f32) -> bool {
        let tolerance =
            f32::EPSILON * old_estimation.abs().max(new_estimation.abs()).max(1.0) * 16.0;
        (old_estimation - new_estimation).abs() > tolerance
    }

    fn reconcile_hash_rate_estimation(
        self_: &Arc<Mutex<Self>>,
        new_estimation: f32,
    ) -> ProxyResult<'static, ()> {
        let (upstream_difficulty_config, old_estimation, connection_id, stats_sender, changed) =
            self_.safe_lock(|d| {
                let old_estimation = d.difficulty_mgmt.estimated_downstream_hash_rate;
                let changed = Self::hashrate_estimation_changed(old_estimation, new_estimation);
                if changed {
                    d.difficulty_mgmt.estimated_downstream_hash_rate = new_estimation;
                }

                (
                    d.upstream_difficulty_config.clone(),
                    old_estimation,
                    d.connection_id,
                    d.stats_sender.clone(),
                    changed,
                )
            })?;

        if !changed {
            return Ok(());
        }

        stats_sender.update_hashrate(connection_id, new_estimation);
        let hash_rate_delta = new_estimation - old_estimation;
        upstream_difficulty_config.safe_lock(|c| {
            if (c.channel_nominal_hashrate + hash_rate_delta) > 0.0 {
                c.channel_nominal_hashrate += hash_rate_delta;
            } else {
                c.channel_nominal_hashrate = 0.0;
            }
        })?;
        Ok(())
    }

    /// Updates the downstream miner's difficulty mgmt states and adjusts the upstream channel's nominal
    fn update_self_with_new_hash_rate(
        self_: &Arc<Mutex<Self>>,
        new_estimation: f32,
        current_diff: f32,
    ) -> ProxyResult<'static, ()> {
        let (connection_id, stats_sender) = self_.safe_lock(|d| {
            d.difficulty_mgmt.reset();
            d.difficulty_mgmt.add_difficulty(current_diff);

            (d.connection_id, d.stats_sender.clone())
        })?;
        stats_sender.update_diff(connection_id, current_diff);
        Self::reconcile_hash_rate_estimation(self_, new_estimation)?;
        Ok(())
    }
}

// Converts difficulty to SV1 `SetDifficulty` message and corresponding target.
/// Returns JSON-RPC message and the target.
fn diff_to_sv1_message(diff: f64) -> ProxyResult<'static, (json_rpc::Message, [u8; 32])> {
    let message: json_rpc::Message = json_rpc::Notification {
        method: "mining.set_difficulty".to_string(),
        params: serde_json::Value::Array(vec![downstream_difficulty_param(diff)]),
    }
    .into();
    let target = Downstream::difficulty_to_target(diff as f32);
    Ok((message, target))
}

fn downstream_difficulty_param(diff: f64) -> serde_json::Value {
    let rounded = diff.round();
    if diff.is_finite()
        && diff >= 0.0
        && rounded <= u64::MAX as f64
        && (diff - rounded).abs() <= f64::EPSILON * rounded.abs().max(1.0) * 8.0
    {
        serde_json::Value::from(rounded as u64)
    } else {
        serde_json::Value::from(diff)
    }
}

pub fn nearest_power_of_2(x: f32) -> f32 {
    if x <= 0.0 {
        return 0.000_976_562_5;
    }
    let exponent = x.log2().round() as i32;
    2f32.powi(exponent)
}

pub fn hard_minimum_difficulty_for_proxy_mode(local_mode: bool) -> Option<f32> {
    if local_mode {
        None
    } else {
        Some(NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY)
    }
}

pub fn clamp_downstream_difficulty_to_floor(
    difficulty: f32,
    hard_minimum_difficulty: Option<f32>,
) -> f32 {
    hard_minimum_difficulty.map_or(difficulty, |minimum| difficulty.max(minimum))
}

pub fn quantize_downstream_difficulty(
    difficulty: f32,
    hard_minimum_difficulty: Option<f32>,
) -> f32 {
    nearest_power_of_2(clamp_downstream_difficulty_to_floor(
        difficulty,
        hard_minimum_difficulty,
    ))
}

#[cfg(test)]
mod test {
    use super::super::super::upstream::diff_management::UpstreamDifficultyConfig;
    use super::{
        clamp_downstream_difficulty_to_floor, diff_to_sv1_message,
        hard_minimum_difficulty_for_proxy_mode, nearest_power_of_2, quantize_downstream_difficulty,
        NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY,
    };
    use crate::{
        api::stats::StatsSender,
        translator::downstream::{downstream::DownstreamDifficultyConfig, Downstream},
    };
    use binary_sv2::U256;
    use pid::Pid;
    use rand::{thread_rng, Rng};
    use roles_logic_sv2::{mining_sv2::Target, utils::Mutex};
    use sha2::{Digest, Sha256};
    use std::{
        collections::VecDeque,
        sync::Arc,
        time::{Duration, Instant},
    };
    use sv1_api::{
        server_to_client::Notify,
        utils::{HexU32Be, MerkleNode, PrevHash},
    };
    use tokio::sync::mpsc::channel;

    #[test]
    #[ignore] // TODO
    fn test_diff_management() {
        let expected_shares_per_minute = 1000.0;
        let total_run_time = std::time::Duration::from_secs(40);
        let initial_nominal_hashrate = dbg!(measure_hashrate(10));
        let target = match roles_logic_sv2::utils::hash_rate_to_target(
            initial_nominal_hashrate,
            expected_shares_per_minute,
        ) {
            Ok(target) => target,
            Err(_) => panic!(),
        };

        let mut share = generate_random_80_byte_array();
        let timer = std::time::Instant::now();
        let mut elapsed = std::time::Duration::from_secs(0);
        let mut count = 0;
        while elapsed <= total_run_time {
            // start hashing util a target is met and submit to
            mock_mine(target.clone().into(), &mut share);
            elapsed = timer.elapsed();
            count += 1;
        }

        let calculated_share_per_min = count as f32 / (elapsed.as_secs_f32() / 60.0);
        // This is the error margin for a confidence of 99% given the expect number of shares per
        // minute TODO the review the math under it
        let error_margin = get_error(expected_shares_per_minute);
        let error =
            (dbg!(calculated_share_per_min) - dbg!(expected_shares_per_minute as f32)).abs();
        assert!(
            dbg!(error) <= error_margin as f32,
            "Calculated shares per minute are outside the 99% confidence interval. Error: {error:?}, Error margin: {error_margin:?}, {calculated_share_per_min:?}"
        );
    }

    fn get_error(lambda: f64) -> f64 {
        let z_score_99 = 2.576;
        z_score_99 * lambda.sqrt()
    }

    fn mock_mine(target: Target, share: &mut [u8; 80]) {
        let mut hashed: Target = [255_u8; 32].into();
        while hashed > target {
            hashed = hash(share);
        }
    }

    // returns hashrate based on how fast the device hashes over the given duration
    fn measure_hashrate(duration_secs: u64) -> f64 {
        let mut share = generate_random_80_byte_array();
        let start_time = Instant::now();
        let mut hashes: u64 = 0;
        let duration = Duration::from_secs(duration_secs);

        while start_time.elapsed() < duration {
            for _ in 0..10000 {
                hash(&mut share);
                hashes += 1;
            }
        }

        let elapsed_secs = start_time.elapsed().as_secs_f64();
        hashes as f64 / elapsed_secs
    }

    fn hash(share: &mut [u8; 80]) -> Target {
        let nonce: [u8; 8] = share[0..8].try_into().unwrap();
        let mut nonce = u64::from_le_bytes(nonce);
        nonce += 1;
        share[0..8].copy_from_slice(&nonce.to_le_bytes());
        let hash = Sha256::digest(&share).to_vec();
        let hash: U256<'static> = hash.try_into().unwrap();
        hash.into()
    }

    fn generate_random_80_byte_array() -> [u8; 80] {
        let mut rng = thread_rng();
        let mut arr = [0u8; 80];
        rng.fill(&mut arr[..]);
        arr
    }

    fn get_diff(hashrate: f32) -> f32 {
        let share_per_second = *crate::SHARE_PER_MIN / 60.0;
        let initial_difficulty = hashrate / (share_per_second * 2f32.powf(32.0));
        crate::translator::downstream::diff_management::nearest_power_of_2(initial_difficulty)
    }

    const RAW_BOOTSTRAP_HASHRATE: f32 = 500_000_000_000_000.0;

    fn hashrate_for_diff(diff: f32) -> f32 {
        let share_per_second = *crate::SHARE_PER_MIN / 60.0;
        share_per_second * diff * 2f32.powi(32)
    }

    fn target_rate_submits() -> VecDeque<Instant> {
        let oldest = Instant::now() - Duration::from_secs(30);
        std::iter::repeat_n(oldest, 5).collect()
    }

    fn submits_with_stale_history(stale_count: usize) -> VecDeque<Instant> {
        let now = Instant::now();
        let mut submits = VecDeque::from(vec![now - Duration::from_secs(61); stale_count]);
        submits.push_back(now - Duration::from_secs(30));
        submits
    }

    fn assert_hashrate_close(actual: f32, expected: f32) {
        let tolerance = expected.abs().max(1.0) * 0.001;
        assert!(
            (actual - expected).abs() <= tolerance,
            "expected hashrate near {expected}, got {actual}",
        );
    }

    fn seeded_downstream(
        estimated_downstream_hash_rate: f32,
        latest_difficulty: f32,
        pid_controller: Pid<f32>,
        submits: VecDeque<Instant>,
        channel_nominal_hashrate: f32,
    ) -> (Arc<Mutex<Downstream>>, Arc<Mutex<UpstreamDifficultyConfig>>) {
        let mut current_difficulties = VecDeque::new();
        current_difficulties.push_back(latest_difficulty);
        let difficulty_mgmt = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate,
            submits,
            pid_controller,
            current_difficulties,
            initial_difficulty: latest_difficulty,
            hard_minimum_difficulty: None,
        };
        let upstream_config = Arc::new(Mutex::new(UpstreamDifficultyConfig {
            channel_diff_update_interval: 60,
            channel_nominal_hashrate,
        }));
        let (tx_sv1_submit, _rx_sv1_submit) = tokio::sync::mpsc::channel(10);
        let (tx_outgoing, _rx_outgoing) = channel(10);
        let (tx_update_token, _rx_update_token) = channel(10);
        let first_job = Notify {
            job_id: "seed".to_string(),
            prev_hash: PrevHash::try_from("0".repeat(64).as_str()).unwrap(),
            coin_base1: "ffff".try_into().unwrap(),
            coin_base2: "ffff".try_into().unwrap(),
            merkle_branch: vec![MerkleNode::try_from(vec![7_u8; 32]).unwrap()],
            version: HexU32Be(5667),
            bits: HexU32Be(5678),
            time: HexU32Be(5609),
            clean_jobs: true,
        };

        (
            Arc::new(Mutex::new(Downstream::new(
                1,
                vec![],
                vec![],
                None,
                None,
                tx_sv1_submit,
                tx_outgoing,
                0,
                difficulty_mgmt,
                upstream_config.clone(),
                StatsSender::new(),
                first_job,
                tx_update_token,
            ))),
            upstream_config,
        )
    }

    fn seeded_downstream_with_channels(
        estimated_downstream_hash_rate: f32,
        latest_difficulty: f32,
        pid_controller: Pid<f32>,
        submits: VecDeque<Instant>,
        channel_nominal_hashrate: f32,
    ) -> (
        Arc<Mutex<Downstream>>,
        Arc<Mutex<UpstreamDifficultyConfig>>,
        tokio::sync::mpsc::Receiver<sv1_api::json_rpc::Message>,
    ) {
        let mut current_difficulties = VecDeque::new();
        current_difficulties.push_back(latest_difficulty);
        let difficulty_mgmt = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate,
            submits,
            pid_controller,
            current_difficulties,
            initial_difficulty: latest_difficulty,
            hard_minimum_difficulty: None,
        };
        let upstream_config = Arc::new(Mutex::new(UpstreamDifficultyConfig {
            channel_diff_update_interval: 60,
            channel_nominal_hashrate,
        }));
        let (tx_sv1_submit, _rx_sv1_submit) = tokio::sync::mpsc::channel(10);
        let (tx_outgoing, rx_outgoing) = channel(10);
        let (tx_update_token, _rx_update_token) = channel(10);
        let first_job = Notify {
            job_id: "seed".to_string(),
            prev_hash: PrevHash::try_from("0".repeat(64).as_str()).unwrap(),
            coin_base1: "ffff".try_into().unwrap(),
            coin_base2: "ffff".try_into().unwrap(),
            merkle_branch: vec![MerkleNode::try_from(vec![7_u8; 32]).unwrap()],
            version: HexU32Be(5667),
            bits: HexU32Be(5678),
            time: HexU32Be(5609),
            clean_jobs: true,
        };

        (
            Arc::new(Mutex::new(Downstream::new(
                1,
                vec![],
                vec![],
                None,
                None,
                tx_sv1_submit,
                tx_outgoing,
                0,
                difficulty_mgmt,
                upstream_config.clone(),
                StatsSender::new(),
                first_job,
                tx_update_token,
            ))),
            upstream_config,
            rx_outgoing,
        )
    }

    fn extract_set_difficulty(message: &sv1_api::json_rpc::Message) -> Option<f64> {
        let json = serde_json::to_value(message).unwrap();
        if json["method"].as_str() != Some("mining.set_difficulty") {
            return None;
        }
        json["params"][0]
            .as_f64()
            .or_else(|| json["params"][0].as_u64().map(|value| value as f64))
    }

    #[tokio::test]
    async fn initial_seed_is_not_reconciled_when_quantized_difficulty_is_unchanged() {
        let quantized_difficulty = get_diff(RAW_BOOTSTRAP_HASHRATE);
        let quantized_hashrate = hashrate_for_diff(quantized_difficulty);
        let pid = Pid::new(*crate::SHARE_PER_MIN, quantized_difficulty * 10.0);

        let (downstream, upstream_config) = seeded_downstream(
            RAW_BOOTSTRAP_HASHRATE,
            quantized_difficulty,
            pid,
            target_rate_submits(),
            RAW_BOOTSTRAP_HASHRATE,
        );

        Downstream::update_difficulty_and_hashrate(&downstream).unwrap();

        let estimated_downstream_hash_rate = downstream
            .safe_lock(|d| d.difficulty_mgmt.estimated_downstream_hash_rate)
            .unwrap();
        let channel_nominal_hashrate = upstream_config
            .safe_lock(|u| u.channel_nominal_hashrate)
            .unwrap();

        assert_hashrate_close(estimated_downstream_hash_rate, quantized_hashrate);
        assert_hashrate_close(channel_nominal_hashrate, quantized_hashrate);
    }

    #[tokio::test]
    #[ignore = "non-zero delay is blocked until the stale bootstrap difficulty replay bug is fixed"]
    async fn positive_delay_does_not_replay_stale_bootstrap_difficulty_after_retarget() {
        let initial_difficulty = get_diff(RAW_BOOTSTRAP_HASHRATE);
        let expected_retarget_difficulty = initial_difficulty / 2.0;
        let mut pid = Pid::new(*crate::SHARE_PER_MIN, initial_difficulty * 10.0);
        pid.p(-(initial_difficulty * 0.05), f32::MAX)
            .i(0.0, f32::MAX)
            .d(0.0, f32::MAX);

        let (downstream, _upstream_config, mut rx_outgoing) = seeded_downstream_with_channels(
            RAW_BOOTSTRAP_HASHRATE,
            initial_difficulty,
            pid,
            VecDeque::new(),
            RAW_BOOTSTRAP_HASHRATE,
        );

        let (bootstrap_message, _) = diff_to_sv1_message(initial_difficulty as f64).unwrap();
        Downstream::send_message_downstream(downstream.clone(), bootstrap_message.clone()).await;

        let initial_bootstrap_message =
            tokio::time::timeout(Duration::from_secs(1), rx_outgoing.recv())
                .await
                .unwrap()
                .unwrap();
        let bootstrap_difficulty = extract_set_difficulty(&initial_bootstrap_message).unwrap();
        assert_eq!(bootstrap_difficulty as f32, initial_difficulty);

        let resend_downstream = downstream.clone();
        let resend_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Downstream::send_message_downstream(resend_downstream, bootstrap_message).await;
        });

        Downstream::try_update_difficulty_settings(&downstream)
            .await
            .unwrap();

        let retarget_message = tokio::time::timeout(Duration::from_secs(1), rx_outgoing.recv())
            .await
            .unwrap()
            .unwrap();
        let retarget_difficulty = extract_set_difficulty(&retarget_message).unwrap();
        assert_eq!(retarget_difficulty as f32, expected_retarget_difficulty);

        let maybe_stale_bootstrap =
            tokio::time::timeout(Duration::from_millis(100), rx_outgoing.recv()).await;
        resend_handle.await.unwrap();

        match maybe_stale_bootstrap {
            Err(_) => {}
            Ok(Some(message)) => {
                let replayed_difficulty = extract_set_difficulty(&message).unwrap();
                panic!(
                    "stale bootstrap mining.set_difficulty replayed after retarget: expected no further difficulty update, got {replayed_difficulty}"
                );
            }
            Ok(None) => panic!("downstream message channel closed unexpectedly"),
        }
    }

    #[tokio::test]
    async fn stale_submit_history_is_pruned_when_retarget_is_skipped() {
        let latest_difficulty = 1024.0;
        let mut pid = Pid::new(*crate::SHARE_PER_MIN, latest_difficulty * 10.0);
        pid.p(0.0, f32::MAX).i(0.0, f32::MAX).d(0.0, f32::MAX);

        let (downstream, _upstream_config) = seeded_downstream(
            hashrate_for_diff(latest_difficulty),
            latest_difficulty,
            pid,
            submits_with_stale_history(4_096),
            hashrate_for_diff(latest_difficulty),
        );

        let updated = Downstream::update_difficulty_and_hashrate(&downstream).unwrap();
        assert_eq!(updated, None);

        let retained_submits = downstream
            .safe_lock(|d| d.difficulty_mgmt.submits.len())
            .unwrap();
        assert_eq!(retained_submits, 1);
    }

    #[test]
    fn downstream_difficulties_are_quantized_to_powers_of_two() {
        assert_eq!(nearest_power_of_2(1.0), 1.0);
        assert_eq!(nearest_power_of_2(3.0), 4.0);
        assert_eq!(nearest_power_of_2(12.0), 16.0);
        assert_eq!(nearest_power_of_2(0.2), 0.25);

        assert_eq!(
            quantize_downstream_difficulty(10_000.0, Some(NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY)),
            NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY
        );
    }

    #[test]
    fn integer_difficulty_serializes_without_decimal_point() {
        let (message, _) = diff_to_sv1_message(NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY as f64).unwrap();
        let message = serde_json::to_value(message).unwrap();

        assert_eq!(
            message["params"][0],
            serde_json::json!(NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY as u64)
        );
    }

    #[test]
    fn non_local_proxy_mode_floor_is_power_of_two() {
        assert_eq!(
            hard_minimum_difficulty_for_proxy_mode(false),
            Some(NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY)
        );
        assert_eq!(
            clamp_downstream_difficulty_to_floor(
                10_000.0,
                Some(NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY)
            ),
            NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY
        );
        assert_eq!(
            clamp_downstream_difficulty_to_floor(
                1_000_000.0,
                Some(NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY)
            ),
            1_000_000.0
        );
    }

    #[tokio::test]
    async fn remote_downstream_retarget_respects_hard_floor() {
        let mut diff = VecDeque::new();
        diff.push_back(10_000.0);
        let downstream_conf = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate: 0.0,
            pid_controller: Pid::new(10.0, 100_000.0),
            current_difficulties: diff,
            submits: VecDeque::new(),
            initial_difficulty: 10_000.0,
            hard_minimum_difficulty: Some(NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY),
        };
        let upstream_config = UpstreamDifficultyConfig {
            channel_diff_update_interval: 60,
            channel_nominal_hashrate: 0.0,
        };
        let (tx_sv1_submit, _rx_sv1_submit) = tokio::sync::mpsc::channel(10);
        let (tx_outgoing, _rx_outgoing) = channel(10);
        let (tx_update_token, _rx_update_token) = channel(10);
        let random_str = rand::thread_rng().gen::<[u8; 32]>().to_vec();
        let first_job = Notify {
            job_id: "ciao".to_string(),
            prev_hash: PrevHash::try_from("0".repeat(64).as_str()).unwrap(),
            coin_base1: "ffff".try_into().unwrap(),
            coin_base2: "ffff".try_into().unwrap(),
            merkle_branch: vec![MerkleNode::try_from(random_str).unwrap()],
            version: HexU32Be(5667),
            bits: HexU32Be(5678),
            time: HexU32Be(5609),
            clean_jobs: true,
        };
        let downstream = Arc::new(Mutex::new(Downstream::new(
            1,
            vec![],
            vec![],
            None,
            None,
            tx_sv1_submit,
            tx_outgoing,
            0,
            downstream_conf,
            Arc::new(Mutex::new(upstream_config)),
            crate::api::stats::StatsSender::new(),
            first_job,
            tx_update_token,
        )));

        let updated = Downstream::update_difficulty_and_hashrate(&downstream).unwrap();

        assert_eq!(updated, Some(NON_LOCAL_DOWNSTREAM_MIN_DIFFICULTY));
    }

    #[tokio::test]
    async fn test_converge_to_spm_from_low() {
        test_converge_to_spm(1.0).await
    }

    #[tokio::test]
    async fn test_converge_to_spm_from_high() {
        // TODO make this converge in acceptable times also for bigger numbers
        test_converge_to_spm(500_000.0).await
    }

    async fn test_converge_to_spm(start_hashrate: f64) {
        let mut diff = VecDeque::new();
        diff.push_back(10_000_000_000.0);
        let downstream_conf = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate: 0.0, // updated below
            pid_controller: Pid::new(10.0, 100_000_000.0),
            current_difficulties: diff,
            submits: VecDeque::new(),
            initial_difficulty: 10_000_000_000.0,
            hard_minimum_difficulty: None,
        };
        let upstream_config = UpstreamDifficultyConfig {
            channel_diff_update_interval: 60,
            channel_nominal_hashrate: 0.0,
        };
        let (tx_sv1_submit, _rx_sv1_submit) = tokio::sync::mpsc::channel(10);
        let (tx_outgoing, _rx_outgoing) = channel(10);
        let (tx_update_token, _rx_update_token) = channel(10);
        let random_str = rand::thread_rng().gen::<[u8; 32]>().to_vec();
        let first_job = Notify {
            job_id: "ciao".to_string(),
            prev_hash: PrevHash::try_from("0".repeat(64).as_str()).unwrap(),
            coin_base1: "ffff".try_into().unwrap(),
            coin_base2: "ffff".try_into().unwrap(),
            merkle_branch: vec![MerkleNode::try_from(random_str).unwrap()],
            version: HexU32Be(5667),
            bits: HexU32Be(5678),
            time: HexU32Be(5609),
            clean_jobs: true,
        };
        let mut downstream = Downstream::new(
            1,
            vec![],
            vec![],
            None,
            None,
            tx_sv1_submit,
            tx_outgoing,
            0,
            downstream_conf.clone(),
            Arc::new(Mutex::new(upstream_config)),
            crate::api::stats::StatsSender::new(),
            first_job,
            tx_update_token,
        );
        downstream.difficulty_mgmt.estimated_downstream_hash_rate = start_hashrate as f32;

        let total_run_time = std::time::Duration::from_secs(10);
        let timer = std::time::Instant::now();
        let mut elapsed = std::time::Duration::from_secs(0);

        let expected_nominal_hashrate = measure_hashrate(5);
        let expected_diff = get_diff(expected_nominal_hashrate as f32);
        let expected_target: U256 = Downstream::difficulty_to_target(expected_diff).into();

        let initial_nominal_hashrate = start_hashrate;
        let initial_difficulty = get_diff(initial_nominal_hashrate as f32);
        let mut initial_target: U256 = Downstream::difficulty_to_target(initial_difficulty).into();
        let downstream = Arc::new(Mutex::new(downstream));
        Downstream::init_difficulty_management(&downstream)
            .await
            .unwrap();
        let mut share = generate_random_80_byte_array();
        while elapsed <= total_run_time {
            mock_mine(initial_target.clone().into(), &mut share);
            Downstream::save_share(downstream.clone()).unwrap();
            let _ = Downstream::try_update_difficulty_settings(&downstream).await;
            initial_target = Downstream::difficulty_to_target(
                *downstream_conf.current_difficulties.back().unwrap(),
            )
            .into();
            elapsed = timer.elapsed();
        }
        let expected_0s = trailing_0s(expected_target.inner_as_ref().to_vec());
        let actual_0s = trailing_0s(initial_target.inner_as_ref().to_vec());
        assert!(expected_0s.abs_diff(actual_0s) <= 1);
    }
    fn trailing_0s(mut v: Vec<u8>) -> usize {
        let mut ret = 0;
        while v.pop() == Some(0) {
            ret += 1;
        }
        ret
    }
    // TODO make a test where unknown donwstream is simulated and we do not wait for it to produce
    // a share but we try to updated the estimated hash power every 2 seconds and updated the
    // target consequentially this shuold start to provide shares within a normal amount of time
}
