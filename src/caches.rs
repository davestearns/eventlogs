use std::error::Error;

use crate::{ids::LogId, Aggregation};
use thiserror::Error;

/// Testing fakes.
pub mod fake;

/// An [AggregationCache] backed by a redis server/cluster.
#[cfg(feature = "redis-cache")]
pub mod redis;

/// The error type returned from [AggregationCache] methods.
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

/// The trait implemented by all aggregation caches.
#[trait_variant::make(Send)]
pub trait AggregationCache<A> {
    /// Puts an [Aggregation] into the cache.
    async fn put(&self, aggregation: &Aggregation<A>) -> Result<(), AggregationCacheError>;
    /// Gets an [Aggregation] from the cache.
    async fn get(&self, log_id: &LogId) -> Result<Option<Aggregation<A>>, AggregationCacheError>;
}

/// Some caches will use this to let you control how [Aggregation] instances
/// are serialized to bytes when written to the cache. For example, you can
/// implement these methods for any [serde] format you want, or you can use
/// something like protobuf, flatbuffers, or even rkyv.
pub trait AggregationCacheSerde<A> {
    /// Serializes an [Aggregation] into bytes.
    fn serialize(&self, aggregation: &Aggregation<A>) -> Result<Vec<u8>, impl Error + 'static>;
    /// Deserializes a slice of bytes into an [Aggregation].
    fn deserialize(&self, buf: &[u8]) -> Result<Aggregation<A>, impl Error + 'static>;
}
