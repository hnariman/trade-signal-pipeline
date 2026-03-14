use rust_decimal::Decimal;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq)]
pub enum CrossSignal {
    Above,
    Below,
}

pub struct SmaState {
    fast_period: usize,
    slow_period: usize,
    prices: VecDeque<Decimal>,
    prev_fast: Option<Decimal>,
    prev_slow: Option<Decimal>,
}

impl SmaState {
    pub fn new(fast_period: usize, slow_period: usize) -> Self {
        assert!(fast_period < slow_period);
        Self {
            fast_period,
            slow_period,
            prices: VecDeque::with_capacity(slow_period + 1),
            prev_fast: None,
            prev_slow: None,
        }
    }

    pub fn update(&mut self, price: Decimal) -> Option<CrossSignal> {
        self.prices.push_back(price);
        if self.prices.len() > self.slow_period {
            self.prices.pop_front();
        }
        if self.prices.len() < self.slow_period {
            return None;
        }

        let fast = self.sma(self.fast_period);
        let slow = self.sma(self.slow_period);

        let signal = match (self.prev_fast, self.prev_slow) {
            (Some(pf), Some(ps)) => {
                if pf <= ps && fast > slow {
                    Some(CrossSignal::Above)
                } else if pf >= ps && fast < slow {
                    Some(CrossSignal::Below)
                } else {
                    None
                }
            }
            _ => None,
        };

        self.prev_fast = Some(fast);
        self.prev_slow = Some(slow);
        signal
    }

    fn sma(&self, period: usize) -> Decimal {
        let sum: Decimal = self.prices.iter().rev().take(period).sum();
        sum / Decimal::from(period)
    }

    pub fn fast_sma(&self) -> Option<Decimal> {
        self.prev_fast
    }
    pub fn slow_sma(&self) -> Option<Decimal> {
        self.prev_slow
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn no_signal_until_slow_period_filled() {
        let mut s = SmaState::new(5, 20);
        for p in [dec!(100); 19] {
            assert!(s.update(p).is_none());
        }
    }

    #[test]
    fn detects_cross_above() {
        let mut s = SmaState::new(5, 20);
        for _ in 0..20 {
            s.update(dec!(100));
        }
        for _ in 0..5 {
            s.update(dec!(90));
        }
        s.update(dec!(100));
        s.update(dec!(110));
        let signal = s.update(dec!(120));
        assert_eq!(signal, Some(CrossSignal::Above));
    }
}
