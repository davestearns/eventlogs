use crate::Reduction;
use chrono::{TimeDelta, Utc};
use std::fmt::Debug;

/// Implemented by policies that control if a given [Reduction] gets cached.
pub trait CachingPolicy<A>: Debug + Send + Sync + 'static {
    /// Returns true if the [Reduction] should be cached, false if not.
    fn should_cache(&self, reduction: &Reduction<A>) -> bool;
}

/// An [CachingPolicy] that caches [Reduction]s
/// based on the number of events they have aggregated.
#[derive(Debug, Default)]
pub struct LogLengthPolicy {
    min_length: u32,
    max_length: u32,
}

impl LogLengthPolicy {
    /// Constructs an instance that caches [Reduction]s
    /// that have aggregated at least `min_length` events.
    pub fn at_least(min_length: u32) -> Self {
        Self {
            min_length,
            max_length: u32::MAX,
        }
    }

    /// Constructs an instance that caches [Reduction]s
    /// until they have aggregated `max_length` events.
    pub fn until(max_length: u32) -> Self {
        Self {
            min_length: 0,
            max_length,
        }
    }

    /// Constructs an instance that caches [Reduction]s
    /// that have aggregated at least `min_length` events,
    /// but not more than `max_length` events.
    pub fn between(min_length: u32, max_length: u32) -> Self {
        Self {
            min_length,
            max_length,
        }
    }
}

impl<A> CachingPolicy<A> for LogLengthPolicy {
    fn should_cache(&self, reduction: &Reduction<A>) -> bool {
        let length = reduction.through_index() + 1;
        length >= self.min_length && length <= self.max_length
    }
}

/// An [CachingPolicy] that caches [Reduction]s
/// based on how old the log is.
#[derive(Debug, Default)]
pub struct LogAgePolicy {
    min_age: TimeDelta,
    max_age: TimeDelta,
}

impl LogAgePolicy {
    /// Constructs an instance that caches [Reduction]s
    /// for logs that are at least `min_age` old.
    pub fn starting_at(min_age: TimeDelta) -> Self {
        Self {
            min_age,
            max_age: TimeDelta::max_value(),
        }
    }

    /// Constructs an instance that caches [Reduction]s
    /// for logs that are no more than `max_age` old.
    pub fn until(max_age: TimeDelta) -> Self {
        Self {
            min_age: TimeDelta::min_value(),
            max_age,
        }
    }

    /// Constructs an instance that caches [Reduction]s
    /// for logs created between `min_age` and `max_age`, inclusive.
    pub fn between(min_age: TimeDelta, max_age: TimeDelta) -> Self {
        Self { min_age, max_age }
    }
}

impl<A> CachingPolicy<A> for LogAgePolicy {
    fn should_cache(&self, reduction: &Reduction<A>) -> bool {
        let age = Utc::now() - reduction.log_id().created_at();
        age >= self.min_age && age <= self.max_age
    }
}

/// An [CachingPolicy] that always caches.
///
/// This is mostly used to make the type inference more clear.
#[derive(Debug, Default)]
pub struct NoPolicy;
impl<A> CachingPolicy<A> for NoPolicy {
    fn should_cache(&self, _reduction: &Reduction<A>) -> bool {
        true
    }
}
