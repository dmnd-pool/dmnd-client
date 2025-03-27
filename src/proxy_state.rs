use std::sync::Arc;

use lazy_static::lazy_static;
use roles_logic_sv2::utils::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

lazy_static! {
    static ref PROXY_STATE: Arc<Mutex<ProxyState>> = Arc::new(Mutex::new(ProxyState::new()));
}

/// Main enum representing the overall state of the proxy
#[derive(Debug, Clone, PartialEq)]
pub enum ProxyStates {
    Pool(PoolState),
    Tp(TpState),
    Jd(JdState),
    ShareAccounter(ShareAccounterState),
    InternalInconsistency(u32),
    Downstream(DownstreamState),
    Upstream(UpstreamState),
    Translator(TranslatorState),
}

/// Represents the state of the pool
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PoolState {
    Up,
    Down,
}

/// Represents the state of the Tp
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TpState {
    Up,
    Down,
}

/// Represents the state of the Translator
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TranslatorState {
    Up,
    Down,
}

/// Represents the state of the JD
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum JdState {
    Up,
    Down,
}

/// Represents the state of the Share Accounter
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ShareAccounterState {
    Up,
    Down,
}

/// Represents the state of the Downstream
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DownstreamState {
    Up,
    Down(Vec<DownstreamType>), // A specific downstream is down
}

/// Represents the state of the Upstream
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UpstreamState {
    Up,
    Down(Vec<UpstreamType>), // A specific upstream is down
}

/// Represents different downstreams
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DownstreamType {
    JdClientMiningDownstream,
    TranslatorDownstream,
}

/// Represents different upstreams
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UpstreamType {
    JDCMiningUpstream,
    TranslatorUpstream,
}

/// Represents global proxy state
#[derive(Debug, Serialize, Deserialize)]
pub struct ProxyState {
    pub pool: PoolState,
    pub tp: TpState,
    pub jd: JdState,
    pub share_accounter: ShareAccounterState,
    pub translator: TranslatorState,
    pub inconsistency: Option<u32>,
    pub downstream: DownstreamState,
    pub upstream: UpstreamState,
}

impl ProxyState {
    pub fn new() -> Self {
        Self {
            pool: PoolState::Up,
            tp: TpState::Up,
            jd: JdState::Up,
            share_accounter: ShareAccounterState::Up,
            translator: TranslatorState::Up,
            inconsistency: None,
            downstream: DownstreamState::Up,
            upstream: UpstreamState::Up,
        }
    }

    pub fn update_pool_state(pool_state: PoolState) {
        info!("Updating PoolState state to {:?}", pool_state);
        if PROXY_STATE
            .safe_lock(|state| {
                state.pool = pool_state;
                // // state.update_proxy_state();
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_tp_state(tp_state: TpState) {
        info!("Updating TpState state to {:?}", tp_state);
        if PROXY_STATE
            .safe_lock(|state| {
                state.tp = tp_state;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_jd_state(jd_state: JdState) {
        info!("Updating JdState state to {:?}", jd_state);
        if PROXY_STATE
            .safe_lock(|state| {
                state.jd = jd_state;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_translator_state(translator_state: TranslatorState) {
        info!("Updating Translator state to {:?}", translator_state);
        if PROXY_STATE
            .safe_lock(|state| {
                state.translator = translator_state;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_share_accounter_state(share_accounter_state: ShareAccounterState) {
        info!(
            "Updating ShareAccounterState state to {:?}",
            share_accounter_state
        );
        if PROXY_STATE
            .safe_lock(|state| {
                state.share_accounter = share_accounter_state;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_inconsistency(code: Option<u32>) {
        info!("Updating Internal Inconsistency state to {:?}", code);
        if PROXY_STATE
            .safe_lock(|state| {
                state.inconsistency = code;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_downstream_state(downstream_type: DownstreamType) {
        info!("Updating Downstream state to {:?}", downstream_type);
        if PROXY_STATE
            .safe_lock(|state| {
                state.downstream = DownstreamState::Down(vec![downstream_type]);
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_upstream_state(upstream_type: UpstreamType) {
        info!("Updating Upstream state to {:?}", upstream_type);
        if PROXY_STATE
            .safe_lock(|state| {
                state.upstream = UpstreamState::Down(vec![upstream_type]);
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_proxy_state_up() {
        if PROXY_STATE
            .safe_lock(|state| {
                state.pool = PoolState::Up;
                state.jd = JdState::Up;
                state.translator = TranslatorState::Up;
                state.tp = TpState::Up;
                state.share_accounter = ShareAccounterState::Up;
                state.upstream = UpstreamState::Up;
                state.downstream = DownstreamState::Up;
                state.inconsistency = None;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn is_proxy_down() -> (bool, Option<String>) {
        let errors = Self::get_errors();
        if errors.is_ok() && errors.as_ref().unwrap().is_empty() {
            (false, None)
        } else {
            let error_descriptions: Vec<String> =
                errors.iter().map(|e| format!("{:?}", e)).collect();
            (true, Some(error_descriptions.join(", ")))
        }
    }

    pub fn get_errors() -> Result<Vec<ProxyStates>, ()> {
        let mut errors = Vec::new();
        if PROXY_STATE
            .safe_lock(|state| {
                if state.pool == PoolState::Down {
                    errors.push(ProxyStates::Pool(state.pool));
                }
                if state.tp == TpState::Down {
                    errors.push(ProxyStates::Tp(state.tp));
                }
                if state.jd == JdState::Down {
                    errors.push(ProxyStates::Jd(state.jd));
                }
                if state.share_accounter == ShareAccounterState::Down {
                    errors.push(ProxyStates::ShareAccounter(state.share_accounter));
                }
                if state.translator == TranslatorState::Down {
                    errors.push(ProxyStates::Translator(state.translator));
                }
                if let Some(inconsistency) = state.inconsistency {
                    errors.push(ProxyStates::InternalInconsistency(inconsistency));
                }
                if matches!(state.downstream, DownstreamState::Down(_)) {
                    errors.push(ProxyStates::Downstream(state.downstream.clone()));
                }
                if matches!(state.upstream, UpstreamState::Down(_)) {
                    errors.push(ProxyStates::Upstream(state.upstream.clone()));
                }
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        } else {
            Ok(errors)
        }
    }
}
