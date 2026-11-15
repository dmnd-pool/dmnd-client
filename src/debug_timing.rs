use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

/// Maximum number of timing samples retained per stage. Acts as a fixed-size
/// sliding window so the collector's memory (and snapshot cost) stay bounded
/// regardless of how long the proxy runs.
const MAX_TIMING_SAMPLES: usize = 10_000;

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
    accept_queue_wait_us: VecDeque<u64>,
    bridge_open_us: VecDeque<u64>,
    downstream_init_us: VecDeque<u64>,
    accept_to_downstream_ready_us: VecDeque<u64>,
    accept_to_subscribe_us: VecDeque<u64>,
    accept_to_authorize_us: VecDeque<u64>,
    accept_to_first_notify_us: VecDeque<u64>,
}

/// Push `value` into a bounded sliding window, evicting the oldest sample once
/// the window is full so the buffer never exceeds `MAX_TIMING_SAMPLES`.
fn push_bounded(buf: &mut VecDeque<u64>, value: u64) {
    if buf.len() >= MAX_TIMING_SAMPLES {
        buf.pop_front();
    }
    buf.push_back(value);
}

impl SessionTimingCollector {
    fn record(&mut self, stage: SessionTimingStage, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        match stage {
            SessionTimingStage::AcceptQueueWait => {
                push_bounded(&mut self.accept_queue_wait_us, micros)
            }
            SessionTimingStage::BridgeOpen => push_bounded(&mut self.bridge_open_us, micros),
            SessionTimingStage::DownstreamInit => {
                push_bounded(&mut self.downstream_init_us, micros)
            }
            SessionTimingStage::AcceptToDownstreamReady => {
                push_bounded(&mut self.accept_to_downstream_ready_us, micros)
            }
            SessionTimingStage::AcceptToSubscribe => {
                push_bounded(&mut self.accept_to_subscribe_us, micros)
            }
            SessionTimingStage::AcceptToAuthorize => {
                push_bounded(&mut self.accept_to_authorize_us, micros)
            }
            SessionTimingStage::AcceptToFirstNotify => {
                push_bounded(&mut self.accept_to_first_notify_us, micros)
            }
        }
    }

    fn snapshot(&self) -> SessionTimingSnapshot {
        SessionTimingSnapshot {
            enabled: true,
            accept_queue_wait: stage_stats_deque(&self.accept_queue_wait_us),
            bridge_open: stage_stats_deque(&self.bridge_open_us),
            downstream_init: stage_stats_deque(&self.downstream_init_us),
            accept_to_downstream_ready: stage_stats_deque(&self.accept_to_downstream_ready_us),
            accept_to_subscribe: stage_stats_deque(&self.accept_to_subscribe_us),
            accept_to_authorize: stage_stats_deque(&self.accept_to_authorize_us),
            accept_to_first_notify: stage_stats_deque(&self.accept_to_first_notify_us),
        }
    }
}

/// Compute stage stats from a bounded `VecDeque` window. The buffer is capped at
/// `MAX_TIMING_SAMPLES`, so collecting it into a contiguous slice for the
/// existing `stage_stats` is a constant-bounded cost regardless of uptime.
fn stage_stats_deque(samples: &VecDeque<u64>) -> Option<SessionStageStats> {
    if samples.is_empty() {
        return None;
    }
    let contiguous: Vec<u64> = samples.iter().copied().collect();
    stage_stats(&contiguous)
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
    use super::*;

    #[test]
    fn push_bounded_evicts_oldest_when_full() {
        let mut buf = VecDeque::new();
        for i in 0..MAX_TIMING_SAMPLES {
            push_bounded(&mut buf, i as u64);
        }
        assert_eq!(buf.len(), MAX_TIMING_SAMPLES);
        assert_eq!(buf[0], 0); // oldest sample

        // Pushing past the cap evicts the oldest, keeping the window bounded.
        push_bounded(&mut buf, 99_999);
        assert_eq!(buf.len(), MAX_TIMING_SAMPLES); // still capped, not +1
        assert_eq!(buf[0], 1); // 0 was evicted
        assert_eq!(buf[buf.len() - 1], 99_999); // newest at the back
    }

    #[test]
    fn collector_memory_stays_bounded() {
        let mut collector = SessionTimingCollector::default();
        let duration = Duration::from_micros(42);

        // Record well past the cap.
        for _ in 0..(MAX_TIMING_SAMPLES + 5_000) {
            collector.record(SessionTimingStage::AcceptQueueWait, duration);
        }

        assert_eq!(collector.accept_queue_wait_us.len(), MAX_TIMING_SAMPLES);

        let snap = collector.snapshot();
        let stats = snap.accept_queue_wait.expect("should have stats");
        assert_eq!(stats.count, MAX_TIMING_SAMPLES);
    }

    #[test]
    fn stage_stats_deque_matches_stage_stats_for_same_data() {
        let data = vec![1_000u64, 2_000, 3_000, 4_000, 5_000];
        let deque: VecDeque<u64> = data.iter().copied().collect();

        let from_slice = stage_stats(&data).expect("slice stats");
        let from_deque = stage_stats_deque(&deque).expect("deque stats");

        assert_eq!(from_slice.count, from_deque.count);
        assert!((from_slice.min_ms - from_deque.min_ms).abs() < f64::EPSILON);
        assert!((from_slice.avg_ms - from_deque.avg_ms).abs() < f64::EPSILON);
        assert!((from_slice.p50_ms - from_deque.p50_ms).abs() < f64::EPSILON);
        assert!((from_slice.p95_ms - from_deque.p95_ms).abs() < f64::EPSILON);
        assert!((from_slice.max_ms - from_deque.max_ms).abs() < f64::EPSILON);
    }

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
