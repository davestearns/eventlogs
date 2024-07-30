use chrono::{TimeDelta, Utc};

use crate::{Aggregation, AggregationCachingPolicy};

/// An [AggregationCachingPolicy] that caches [Aggregation]s
/// based on the number of events they have aggregated.
#[derive(Debug, Default)]
pub struct LogLengthPolicy {
    min_length: u32,
    max_length: u32,
}

impl LogLengthPolicy {
    /// Constructs an instance that caches [Aggregation]s
    /// that have aggregated at least `min_length` events.
    pub fn at_least(min_length: u32) -> Self {
        Self {
            min_length,
            max_length: u32::MAX,
        }
    }

    /// Constructs an instance that caches [Aggregation]s
    /// until they have aggregated `max_length` events.
    pub fn until(max_length: u32) -> Self {
        Self {
            min_length: 0,
            max_length,
        }
    }

    /// Constructs an instance that caches [Aggregation]s
    /// that have aggregated at least `min_length` events,
    /// but not more than `max_length` events.
    pub fn between(min_length: u32, max_length: u32) -> Self {
        Self {
            min_length,
            max_length,
        }
    }
}

impl<A> AggregationCachingPolicy<A> for LogLengthPolicy {
    fn should_cache(&self, aggregation: &Aggregation<A>) -> bool {
        let length = aggregation.through_index() + 1;
        length >= self.min_length && length <= self.max_length
    }
}

/// An [AggregationCachingPolicy] that caches [Aggregation]s
/// based on how old the log is.
#[derive(Debug, Default)]
pub struct LogAgePolicy {
    min_age: TimeDelta,
    max_age: TimeDelta,
}

impl LogAgePolicy {
    /// Constructs an instance that caches [Aggregation]s
    /// for logs that are at least `min_age` old.
    pub fn starting_at(min_age: TimeDelta) -> Self {
        Self {
            min_age,
            max_age: TimeDelta::max_value(),
        }
    }

    /// Constructs an instance that caches [Aggregation]s
    /// for logs that are no more than `max_age` old.
    pub fn until(max_age: TimeDelta) -> Self {
        Self {
            min_age: TimeDelta::min_value(),
            max_age,
        }
    }

    /// Constructs an instance that caches [Aggregation]s
    /// for logs created between `min_age` and `max_age`, inclusive.
    pub fn between(min_age: TimeDelta, max_age: TimeDelta) -> Self {
        Self { min_age, max_age }
    }
}

impl<A> AggregationCachingPolicy<A> for LogAgePolicy {
    fn should_cache(&self, aggregation: &Aggregation<A>) -> bool {
        let age = Utc::now() - aggregation.log_id().created_at();
        age >= self.min_age && age <= self.max_age
    }
}

/// An [AggregationCachingPolicy] that always caches.
///
/// This is mostly used to make the type inference more clear.
#[derive(Debug, Default)]
pub struct NoPolicy;
impl<A> AggregationCachingPolicy<A> for NoPolicy {
    fn should_cache(&self, _aggregation: &Aggregation<A>) -> bool {
        true
    }
}
