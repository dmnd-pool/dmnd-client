use serde::Serialize;
use std::{
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum SessionTimingStage {
    AcceptQueueWait,
    BridgeOpen,
    DownstreamInit,
    AcceptToDownstreamReady,
    AcceptToSubscribe,
    AcceptToAuthorize,
    AcceptToFirstNotify,
}

#[derive(Debug)]
pub(crate) struct DownstreamSessionTiming {
    accepted_at: Instant,
    subscribe_recorded: bool,
    authorize_recorded: bool,
    first_notify_recorded: bool,
}

impl DownstreamSessionTiming {
    pub(crate) fn new(accepted_at: Instant) -> Self {
        Self {
            accepted_at,
            subscribe_recorded: false,
            authorize_recorded: false,
            first_notify_recorded: false,
        }
    }

    pub(crate) fn record_subscribe_if_needed(&mut self) {
        if !self.subscribe_recorded {
            record_stage(
                SessionTimingStage::AcceptToSubscribe,
                self.accepted_at.elapsed(),
            );
            self.subscribe_recorded = true;
        }
    }

    pub(crate) fn record_authorize_if_needed(&mut self) {
        if !self.authorize_recorded {
            record_stage(
                SessionTimingStage::AcceptToAuthorize,
                self.accepted_at.elapsed(),
            );
            self.authorize_recorded = true;
        }
    }

    pub(crate) fn record_first_notify_if_needed(&mut self) {
        if !self.first_notify_recorded {
            record_stage(
                SessionTimingStage::AcceptToFirstNotify,
                self.accepted_at.elapsed(),
            );
            self.first_notify_recorded = true;
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionStageStats {
    pub count: usize,
    pub min_ms: f64,
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub max_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionTimingSnapshot {
    pub enabled: bool,
    pub accept_queue_wait: Option<SessionStageStats>,
    pub bridge_open: Option<SessionStageStats>,
    pub downstream_init: Option<SessionStageStats>,
    pub accept_to_downstream_ready: Option<SessionStageStats>,
    pub accept_to_subscribe: Option<SessionStageStats>,
    pub accept_to_authorize: Option<SessionStageStats>,
    pub accept_to_first_notify: Option<SessionStageStats>,
}

#[derive(Debug, Default)]
struct SessionTimingCollector {
    accept_queue_wait_us: Vec<u64>,
    bridge_open_us: Vec<u64>,
    downstream_init_us: Vec<u64>,
    accept_to_downstream_ready_us: Vec<u64>,
    accept_to_subscribe_us: Vec<u64>,
    accept_to_authorize_us: Vec<u64>,
    accept_to_first_notify_us: Vec<u64>,
}

impl SessionTimingCollector {
    fn record(&mut self, stage: SessionTimingStage, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        match stage {
            SessionTimingStage::AcceptQueueWait => self.accept_queue_wait_us.push(micros),
            SessionTimingStage::BridgeOpen => self.bridge_open_us.push(micros),
            SessionTimingStage::DownstreamInit => self.downstream_init_us.push(micros),
            SessionTimingStage::AcceptToDownstreamReady => {
                self.accept_to_downstream_ready_us.push(micros)
            }
            SessionTimingStage::AcceptToSubscribe => self.accept_to_subscribe_us.push(micros),
            SessionTimingStage::AcceptToAuthorize => self.accept_to_authorize_us.push(micros),
            SessionTimingStage::AcceptToFirstNotify => self.accept_to_first_notify_us.push(micros),
        }
    }

    fn snapshot(&self) -> SessionTimingSnapshot {
        SessionTimingSnapshot {
            enabled: true,
            accept_queue_wait: stage_stats(&self.accept_queue_wait_us),
            bridge_open: stage_stats(&self.bridge_open_us),
            downstream_init: stage_stats(&self.downstream_init_us),
            accept_to_downstream_ready: stage_stats(&self.accept_to_downstream_ready_us),
            accept_to_subscribe: stage_stats(&self.accept_to_subscribe_us),
            accept_to_authorize: stage_stats(&self.accept_to_authorize_us),
            accept_to_first_notify: stage_stats(&self.accept_to_first_notify_us),
        }
    }
}

fn stage_stats(samples_us: &[u64]) -> Option<SessionStageStats> {
    if samples_us.is_empty() {
        return None;
    }

    let mut ordered = samples_us.to_vec();
    ordered.sort_unstable();
    let count = ordered.len();
    let sum_us: u128 = ordered.iter().map(|value| *value as u128).sum();

    Some(SessionStageStats {
        count,
        min_ms: ordered[0] as f64 / 1000.0,
        avg_ms: (sum_us as f64 / count as f64) / 1000.0,
        p50_ms: percentile_ms(&ordered, 0.50),
        p95_ms: percentile_ms(&ordered, 0.95),
        max_ms: ordered[count - 1] as f64 / 1000.0,
    })
}

fn percentile_ms(samples_us: &[u64], percentile: f64) -> f64 {
    let last_index = samples_us.len().saturating_sub(1);
    let index = ((last_index as f64) * percentile).round() as usize;
    samples_us[index.min(last_index)] as f64 / 1000.0
}

fn parse_enabled_flag(value: &str) -> bool {
    !matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "off" | "no"
    )
}

static SESSION_TIMING_ENABLED: OnceLock<bool> = OnceLock::new();
static SESSION_TIMING_COLLECTOR: OnceLock<Mutex<SessionTimingCollector>> = OnceLock::new();

pub fn session_timing_enabled() -> bool {
    *SESSION_TIMING_ENABLED.get_or_init(|| {
        std::env::var("SV1_SESSION_TIMING")
            .map(|value| parse_enabled_flag(&value))
            .unwrap_or(false)
    })
}

pub fn record_stage(stage: SessionTimingStage, duration: Duration) {
    if !session_timing_enabled() {
        return;
    }

    let collector =
        SESSION_TIMING_COLLECTOR.get_or_init(|| Mutex::new(SessionTimingCollector::default()));
    if let Ok(mut collector) = collector.lock() {
        collector.record(stage, duration);
    }
}

pub fn snapshot() -> SessionTimingSnapshot {
    if !session_timing_enabled() {
        return SessionTimingSnapshot {
            enabled: false,
            accept_queue_wait: None,
            bridge_open: None,
            downstream_init: None,
            accept_to_downstream_ready: None,
            accept_to_subscribe: None,
            accept_to_authorize: None,
            accept_to_first_notify: None,
        };
    }

    SESSION_TIMING_COLLECTOR
        .get_or_init(|| Mutex::new(SessionTimingCollector::default()))
        .lock()
        .map(|collector| collector.snapshot())
        .unwrap_or(SessionTimingSnapshot {
            enabled: true,
            accept_queue_wait: None,
            bridge_open: None,
            downstream_init: None,
            accept_to_downstream_ready: None,
            accept_to_subscribe: None,
            accept_to_authorize: None,
            accept_to_first_notify: None,
        })
}

#[cfg(test)]
mod tests {
    use super::{parse_enabled_flag, stage_stats};

    #[test]
    fn stage_stats_compute_basic_percentiles() {
        let stats = stage_stats(&[1_000, 2_000, 3_000, 4_000, 5_000]).expect("stats");
        assert_eq!(stats.count, 5);
        assert!((stats.min_ms - 1.0).abs() < f64::EPSILON);
        assert!((stats.avg_ms - 3.0).abs() < f64::EPSILON);
        assert!((stats.p50_ms - 3.0).abs() < f64::EPSILON);
        assert!((stats.p95_ms - 5.0).abs() < f64::EPSILON);
        assert!((stats.max_ms - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn enabled_flag_parser_handles_common_false_values() {
        for value in ["0", "false", "False", "off", "no"] {
            assert!(!parse_enabled_flag(value));
        }
        for value in ["1", "true", "yes", "debug"] {
            assert!(parse_enabled_flag(value));
        }
    }
}
