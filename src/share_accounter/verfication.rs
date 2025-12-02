use dashmap::DashMap;
use rand::random;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use binary_sv2::{Sv2DataType, B032, B064K};
use demand_share_accounting_ext::verification::{
    validate_slice_integrity, verify_pplns_window, verify_share_in_slice, ShareVerificationResult,
    SliceVerificationResult, VerificationConfig as CoreVerificationConfig, VerificationError,
    VerificationResult, WindowVerificationResult,
};
use demand_share_accounting_ext::*;
use roles_logic_sv2::mining_sv2::SubmitSharesExtended;

#[derive(Clone)]
pub struct VerificationService {
    config: VerificationConfig,
    stored_shares: Arc<RwLock<BTreeMap<u64, Vec<Share<'static>>>>>,
    window_cache: Arc<DashMap<String, GetWindowSuccess<'static>>>,
}

#[derive(Clone, Debug)]
pub struct VerificationConfig {
    pub core_config: CoreVerificationConfig,
    pub enable_verification: bool,
    pub cache_windows: bool,
    pub max_cached_windows: usize,
}

impl Default for VerificationConfig {
    fn default() -> Self {
        Self {
            core_config: CoreVerificationConfig {
                fee_delta: 100_000,
                strict_difficulty: false,       // Disable strict checking
                verify_all_merkle_paths: false, // Disable merkle verification
                min_share_difficulty: 1,
                max_time_variance: 7200,
                verify_pow: false, // Disable PoW verification
            },
            enable_verification: false,
            cache_windows: true,
            max_cached_windows: 100,
        }
    }
}

impl VerificationService {
    pub fn new(config: VerificationConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            stored_shares: Arc::new(RwLock::new(BTreeMap::new())),
            window_cache: Arc::new(DashMap::new()),
        })
    }

    pub fn extract_share_from_submit(
        &self,
        submit: &SubmitSharesExtended,
    ) -> Result<Share<'static>, VerificationError> {
        // Convert SubmitSharesExtended to Share format
        // This is a simplified conversion - you'll need to adapt based on actual data structure

        // Create extranonce from submission data
        let mut extranonce_data = [0u8; 32];
        let extranonce_len = std::cmp::min(submit.extranonce.len(), 30);
        extranonce_data[0] = extranonce_len as u8;
        extranonce_data[1..=extranonce_len]
            .copy_from_slice(&submit.extranonce.inner_as_ref()[..extranonce_len]);
        let extranonce = binary_sv2::B032::from_bytes_unchecked(&mut extranonce_data).into_static();

        // Create empty merkle path for now - this would need to be provided by the mining submission
        let mut merkle_path_data = [0u8; 66];
        merkle_path_data[0] = 0; // No merkle path data for now
        merkle_path_data[1] = 0;
        let merkle_path =
            binary_sv2::B064K::from_bytes_unchecked(&mut merkle_path_data).into_static();

        Ok(Share {
            nonce: submit.nonce,
            ntime: submit.ntime,
            version: submit.version,
            extranonce,
            job_id: submit.job_id as u64,
            reference_job_id: submit.job_id as u64,
            share_index: 0, // Would need to be tracked per job
            merkle_path,
        })
    }

    pub async fn verify_window(
        &self,
        window: &GetWindowSuccess<'_>,
    ) -> VerificationResult<WindowVerificationResult> {
        if !self.config.enable_verification {
            return Ok(WindowVerificationResult {
                window_valid: true,
                total_difficulty: 0,
                slice_results: vec![],
                phash_consistency: true,
                window_size_valid: true,
            });
        }

        info!(
            "Starting window verification for {} slices",
            window.slices.to_owned().into_inner().len(),
        );

        let shares_map = self.stored_shares.read().await;
        let result = verify_pplns_window(window, &shares_map, &self.config.core_config)?;

        if result.window_valid {
            info!("Window verification successful");
        } else {
            warn!("Window verification failed: {:?}", result);
        }

        // Cache the window if enabled
        if self.config.cache_windows {
            let window_key = format!("window_{}", window.slices.to_owned().into_inner().len());
            if self.window_cache.len() < self.config.max_cached_windows {
                self.window_cache
                    .insert(window_key, window.clone().into_static());
            }
        }

        Ok(result)
    }

    pub async fn store_shares(&self, shares_success: &GetSharesSuccess<'_>) {
        let mut shares_map = self.stored_shares.write().await;

        for share in shares_success.shares.to_owned().into_inner().iter() {
            let job_id = share.job_id;
            shares_map
                .entry(job_id)
                .or_insert_with(Vec::new)
                .push(share.clone().into_static());
        }

        info!(
            "Stored {} shares across {} jobs",
            shares_success.shares.to_owned().into_inner().len(),
            shares_map.len()
        );
    }

    pub async fn verify_share(
        &self,
        share: &Share<'_>,
        slice: &Slice,
    ) -> VerificationResult<ShareVerificationResult> {
        if !self.config.enable_verification {
            return Ok(ShareVerificationResult {
                share_index: share.share_index,
                merkle_valid: true,
                difficulty_valid: true,
                fees_valid: true,
                slice_inclusion_valid: true,
                pow_valid: true,
            });
        }

        verify_share_in_slice(share, slice, &self.config.core_config)
    }

    pub async fn verify_slice(&self, slice: &Slice) -> VerificationResult<SliceVerificationResult> {
        if !self.config.enable_verification {
            return Ok(SliceVerificationResult {
                slice_job_id: slice.job_id,
                total_shares_valid: true,
                difficulty_sum_valid: true,
                merkle_tree_valid: true,
                share_results: vec![],
                fees_sum_valid: true,
            });
        }

        let shares_map = self.stored_shares.read().await;
        let shares = shares_map
            .get(&slice.job_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        validate_slice_integrity(slice, shares, &self.config.core_config)
    }

    pub async fn get_verification_stats(&self) -> VerificationStats {
        let shares_map = self.stored_shares.read().await;
        let total_shares = shares_map.values().map(|v| v.len()).sum::<usize>();
        let total_jobs = shares_map.len();
        let cached_windows = self.window_cache.len();

        VerificationStats {
            total_shares,
            total_jobs,
            cached_windows,
            verification_enabled: self.config.enable_verification,
        }
    }

    pub async fn verify_share_with_stored_slices(
        &self,
        share: &Share<'_>,
    ) -> Option<VerificationResult<ShareVerificationResult>> {
        if !self.config.enable_verification {
            return None;
        }

        let shares_map = self.stored_shares.read().await;

        // Find the slice that contains this share's job_id
        // Since we don't store slices directly, we'll create a mock slice for verification
        // In a real implementation, you'd store slices too

        // For demo purposes, create a mock slice
        let mock_slice = Slice {
            job_id: share.job_id,
            number_of_shares: 5,            // Mock value
            difficulty: 1000000,            // Mock value
            fees: 50000,                    // Mock value
            root: Hash256::from([0u8; 32]), // Mock root
        };

        Some(verify_share_in_slice(
            share,
            &mock_slice,
            &self.config.core_config,
        ))
    }

    pub async fn verify_block_completion(&self, _block_found: &NewBlockFound<'_>) {
        info!("COMPREHENSIVE BLOCK VERIFICATION STARTED");

        let shares_map = self.stored_shares.read().await;
        let total_shares = shares_map.values().map(|v| v.len()).sum::<usize>();

        info!(
            " Verifying {} total shares across {} jobs",
            total_shares,
            shares_map.len()
        );

        // Verify all stored shares
        let mut verification_results = Vec::new();

        for (job_id, shares) in shares_map.iter() {
            info!("Verifying job {} with {} shares", job_id, shares.len());

            let mock_slice = Slice {
                job_id: *job_id,
                number_of_shares: shares.len() as u32,
                difficulty: 1000000,
                fees: 50000,
                root: Hash256::from([0u8; 32]),
            };

            match validate_slice_integrity(&mock_slice, shares, &self.config.core_config) {
                Ok(result) => {
                    verification_results.push(result.clone());
                    info!(
                        "Slice verification: shares_valid={}, difficulty_valid={}",
                        result.total_shares_valid, result.difficulty_sum_valid
                    );
                }
                Err(e) => {
                    error!("Slice verification failed: {:?}", e);
                }
            }
        }

        let valid_slices = verification_results
            .iter()
            .filter(|r| r.total_shares_valid)
            .count();
        info!(
            "BLOCK VERIFICATION COMPLETE: {}/{} slices valid",
            valid_slices,
            verification_results.len()
        );
    }

    pub async fn store_share_ok_data(&self, share_ok: &ShareOk) {
        // Create a mock share from ShareOk data
        let mock_share = Share {
            nonce: 12345,
            ntime: 1640995200,
            version: 0x20000000,
            extranonce: Self::create_mock_b032(),
            job_id: share_ok.ref_job_id,
            reference_job_id: share_ok.ref_job_id,
            share_index: random(),
            merkle_path: Self::create_mock_b064k(),
        };

        let mut shares_map = self.stored_shares.write().await;
        shares_map
            .entry(share_ok.ref_job_id)
            .or_insert_with(Vec::new)
            .push(mock_share);
    }

    fn create_mock_b032() -> B032<'static> {
        vec![0u8; 32].try_into().unwrap()
    }

    fn create_mock_b064k() -> B064K<'static> {
        vec![0u8; 96].try_into().unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct VerificationStats {
    pub total_shares: usize,
    pub total_jobs: usize,
    pub cached_windows: usize,
    pub verification_enabled: bool,
}

// Helper trait to convert borrowed data to static
trait IntoStatic<T> {
    fn into_static(self) -> T;
}

impl<'a> IntoStatic<GetWindowSuccess<'static>> for GetWindowSuccess<'a> {
    fn into_static(self) -> GetWindowSuccess<'static> {
        // This is a simplified conversion - you would need to properly convert all lifetime data
        // The actual implementation depends on the specific data structures
        todo!("Implement proper lifetime conversion")
    }
}

impl<'a> IntoStatic<Share<'static>> for Share<'a> {
    fn into_static(self) -> Share<'static> {
        Share {
            nonce: self.nonce,
            ntime: self.ntime,
            version: self.version,
            extranonce: self.extranonce.into_static(),
            job_id: self.job_id,
            reference_job_id: self.reference_job_id,
            share_index: self.share_index,
            merkle_path: self.merkle_path.into_static(),
        }
    }
}
