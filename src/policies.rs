use chrono::{TimeDelta, Utc};

use crate::{Aggregation, AggregationCachingPolicy};

#[derive(Debug, Default)]
pub struct LogLengthPolicy {
    min_length: u32,
    max_length: u32,
}

impl LogLengthPolicy {
    pub fn at_least(min_length: u32) -> Self {
        Self {
            min_length,
            max_length: u32::MAX,
        }
    }

    pub fn until(max_length: u32) -> Self {
        Self {
            min_length: 0,
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

#[derive(Debug, Default)]
pub struct LogAgePolicy {
    min_age: TimeDelta,
    max_age: TimeDelta,
}

impl LogAgePolicy {
    pub fn starting_at(min_age: TimeDelta) -> Self {
        Self {
            min_age,
            max_age: TimeDelta::max_value(),
        }
    }

    pub fn until(max_age: TimeDelta) -> Self {
        Self {
            min_age: TimeDelta::min_value(),
            max_age,
        }
    }
}

impl<A> AggregationCachingPolicy<A> for LogAgePolicy {
    fn should_cache(&self, aggregation: &Aggregation<A>) -> bool {
        let age = Utc::now() - aggregation.log_id().created_at();
        age >= self.min_age && age <= self.max_age
    }
}

#[derive(Debug, Default)]
pub struct NoPolicy;
impl<A> AggregationCachingPolicy<A> for NoPolicy {
    fn should_cache(&self, _aggregation: &Aggregation<A>) -> bool {
        true
    }
}
