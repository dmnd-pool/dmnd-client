use super::Error;
use crate::jd_client::mining_downstream::DownstreamMiningNode;
use bitcoin::TxOut;
use roles_logic_sv2::utils::Mutex;
use std::{net::SocketAddr, sync::Arc};

struct RetainedCapacity {
    template_provider: SocketAddr,
    pool: Option<SocketAddr>,
    capacity: Arc<Mutex<CoinbaseOutputCapacity>>,
}

lazy_static::lazy_static! {
    static ref RETAINED_CAPACITY: Mutex<Option<RetainedCapacity>> = Mutex::new(None);
}

pub(super) fn retained_capacity(
    template_provider: SocketAddr,
    pool: Option<SocketAddr>,
) -> Result<Arc<Mutex<CoinbaseOutputCapacity>>, Error> {
    RETAINED_CAPACITY
        .safe_lock(|retained| {
            if let Some(current) = retained {
                if current.template_provider == template_provider && current.pool == pool {
                    return current.capacity.clone();
                }
            }
            let capacity = Arc::new(Mutex::new(CoinbaseOutputCapacity::default()));
            *retained = Some(RetainedCapacity {
                template_provider,
                pool,
                capacity: capacity.clone(),
            });
            capacity
        })
        .map_err(|_| Error::TemplateRxMutexCorrupted)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct CoinbaseOutputCapacity {
    pub size: u32,
    pub sigops: u16,
}

pub(super) fn coinbase_output_capacity(outputs: &[u8]) -> Result<(u32, u16), Error> {
    let decoded = DownstreamMiningNode::decode_pool_coinbase_outputs(outputs)?;
    let sigops = decoded
        .iter()
        .try_fold(0usize, |total, output| {
            output
                .script_pubkey
                .count_sigops_legacy()
                .checked_mul(4)
                .and_then(|cost| total.checked_add(cost))
        })
        .ok_or(Error::Unrecoverable)?;
    Ok((
        decoded
            .iter()
            .map(TxOut::size)
            .sum::<usize>()
            .try_into()
            .map_err(|_| Error::Unrecoverable)?,
        sigops.try_into().map_err(|_| Error::Unrecoverable)?,
    ))
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ReservationUpdate {
    Advertise(CoinbaseOutputCapacity),
    Unchanged,
    Reconnect,
}

pub(super) struct CoinbaseOutputReservation {
    retained: Arc<Mutex<CoinbaseOutputCapacity>>,
    advertised: Option<CoinbaseOutputCapacity>,
    last_outputs: Vec<u8>,
}

impl CoinbaseOutputReservation {
    pub fn new(retained: Arc<Mutex<CoinbaseOutputCapacity>>) -> Self {
        Self {
            retained,
            advertised: None,
            last_outputs: Vec::new(),
        }
    }

    pub fn check_token(&mut self, outputs: &[u8]) -> Result<ReservationUpdate, Error> {
        if self.advertised.is_some() && outputs == self.last_outputs {
            return Ok(ReservationUpdate::Unchanged);
        }
        let (size, sigops) = coinbase_output_capacity(outputs)?;
        if let Some(advertised) = self.advertised {
            if size > advertised.size || sigops > advertised.sigops {
                self.retain(size, sigops)?;
                return Ok(ReservationUpdate::Reconnect);
            }
            self.last_outputs.clear();
            self.last_outputs.extend_from_slice(outputs);
            return Ok(ReservationUpdate::Unchanged);
        }
        let capacity = self.retain(size, sigops)?;
        self.advertised = Some(capacity);
        self.last_outputs.extend_from_slice(outputs);
        Ok(ReservationUpdate::Advertise(capacity))
    }

    fn retain(&self, size: u32, sigops: u16) -> Result<CoinbaseOutputCapacity, Error> {
        let capacity = self
            .retained
            .safe_lock(|capacity| {
                capacity.size = capacity.size.max(size);
                capacity.sigops = capacity.sigops.max(sigops);
                *capacity
            })
            .map_err(|_| Error::TemplateRxMutexCorrupted)?;
        Ok(capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outputs(script: &[u8]) -> Vec<u8> {
        bitcoin::consensus::serialize(&vec![TxOut {
            value: bitcoin::Amount::ZERO,
            script_pubkey: bitcoin::ScriptBuf::from_bytes(script.to_vec()),
        }])
    }

    #[test]
    fn coinbase_reservation_restarts_before_larger_token_outputs() {
        let retained = Arc::new(Mutex::new(CoinbaseOutputCapacity::default()));
        let mut reservation = CoinbaseOutputReservation::new(retained.clone());
        let small = outputs(&[0x51]);
        let large = outputs(&[0x51; 20]);
        assert_eq!(
            reservation.check_token(&small).unwrap(),
            ReservationUpdate::Advertise(CoinbaseOutputCapacity {
                size: 10,
                sigops: 0
            })
        );
        assert_eq!(
            reservation.check_token(&large).unwrap(),
            ReservationUpdate::Reconnect
        );
        let mut fresh_session = CoinbaseOutputReservation::new(retained);
        assert_eq!(
            fresh_session.check_token(&small).unwrap(),
            ReservationUpdate::Advertise(CoinbaseOutputCapacity {
                size: 29,
                sigops: 0
            })
        );
        assert_eq!(
            fresh_session.check_token(&large).unwrap(),
            ReservationUpdate::Unchanged
        );
    }

    #[test]
    fn coinbase_reservation_restarts_before_higher_token_sigop_cost() {
        let retained = Arc::new(Mutex::new(CoinbaseOutputCapacity::default()));
        let mut reservation = CoinbaseOutputReservation::new(retained.clone());
        assert_eq!(
            reservation.check_token(&outputs(&[0xac; 2])).unwrap(),
            ReservationUpdate::Advertise(CoinbaseOutputCapacity {
                size: 11,
                sigops: 8
            })
        );
        assert_eq!(
            reservation.check_token(&outputs(&[0xae])).unwrap(),
            ReservationUpdate::Reconnect
        );
        let mut fresh_session = CoinbaseOutputReservation::new(retained);
        assert_eq!(
            fresh_session.check_token(&outputs(&[0x51])).unwrap(),
            ReservationUpdate::Advertise(CoinbaseOutputCapacity {
                size: 11,
                sigops: 80,
            })
        );
    }

    #[test]
    fn coinbase_reservation_reuses_unchanged_outputs_without_allocating() {
        let retained = Arc::new(Mutex::new(CoinbaseOutputCapacity::default()));
        let mut reservation = CoinbaseOutputReservation::new(retained);
        let output = outputs(&[0xac]);
        reservation.check_token(&output).unwrap();
        assert_eq!(
            crate::test_alloc::allocations(|| {
                for _ in 0..100 {
                    assert_eq!(
                        reservation.check_token(&output).unwrap(),
                        ReservationUpdate::Unchanged
                    );
                }
            }),
            0
        );
    }

    #[test]
    fn coinbase_reservation_rejects_invalid_later_outputs() {
        let retained = Arc::new(Mutex::new(CoinbaseOutputCapacity::default()));
        let mut reservation = CoinbaseOutputReservation::new(retained);
        reservation.check_token(&outputs(&[0x51])).unwrap();
        assert!(reservation.check_token(&[0xff]).is_err());
    }

    #[test]
    fn coinbase_reservation_keeps_both_bounds_for_smaller_tokens() {
        let retained = Arc::new(Mutex::new(CoinbaseOutputCapacity::default()));
        let mut reservation = CoinbaseOutputReservation::new(retained.clone());
        let large = outputs(&[0xac; 20]);
        let small = outputs(&[0x51]);
        reservation.check_token(&large).unwrap();
        assert_eq!(
            reservation.check_token(&small).unwrap(),
            ReservationUpdate::Unchanged
        );
        assert_eq!(
            reservation.check_token(&large).unwrap(),
            ReservationUpdate::Unchanged
        );
        let mut fresh_session = CoinbaseOutputReservation::new(retained);
        assert_eq!(
            fresh_session.check_token(&small).unwrap(),
            ReservationUpdate::Advertise(CoinbaseOutputCapacity {
                size: 29,
                sigops: 80
            })
        );
    }
}
