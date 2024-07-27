use std::error::Error;

use crate::{ids::LogId, Aggregation};
use thiserror::Error;

pub mod redis;

#[cfg(test)]
pub mod fake;

#[derive(Debug, Error)]
pub enum AggregationCacheError {
    #[error("unexpected database error: {0}")]
    DatabaseError(Box<dyn std::error::Error>),
    #[error("problem encoding aggregation for caching: {0}")]
    EncodingFailure(Box<dyn std::error::Error>),
    #[error("problem decoding aggregation from cache: {0}")]
    DecodingFailure(Box<dyn std::error::Error>),
}

impl PartialEq for AggregationCacheError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DatabaseError(l0), Self::DatabaseError(r0)) => l0.to_string() == r0.to_string(),
            (Self::EncodingFailure(l0), Self::EncodingFailure(r0)) => {
                l0.to_string() == r0.to_string()
            }
            (Self::DecodingFailure(l0), Self::DecodingFailure(r0)) => {
                l0.to_string() == r0.to_string()
            }
            _ => false,
        }
    }
}

#[trait_variant::make(Send)]
pub trait AggregationCache<A> {
    async fn put(&self, aggregation: &Aggregation<A>) -> Result<(), AggregationCacheError>;
    async fn get(&self, log_id: &LogId) -> Result<Option<Aggregation<A>>, AggregationCacheError>;
}

pub trait AggregationCacheSerde<A> {
    fn serialize(&self, aggregation: &Aggregation<A>) -> Result<Vec<u8>, Box<dyn Error>>;
    fn deserialize(&self, buf: &[u8]) -> Result<Aggregation<A>, Box<dyn Error>>;
}
