use std::error::Error;

use crate::{ids::LogId, Reduction};
use thiserror::Error;

/// Testing fakes.
pub mod fake;

/// A [ReductionCache] backed by a redis server/cluster.
#[cfg(feature = "redis-cache")]
pub mod redis;

/// The error type returned from [ReductionCache] methods.
#[derive(Debug, Error)]
pub enum ReductionCacheError {
    #[error("unexpected database error: {0}")]
    DatabaseError(Box<dyn std::error::Error>),
    #[error("problem encoding reduction for caching: {0}")]
    EncodingFailure(Box<dyn std::error::Error>),
    #[error("problem decoding reduction from cache: {0}")]
    DecodingFailure(Box<dyn std::error::Error>),
}

impl PartialEq for ReductionCacheError {
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

/// The trait implemented by all reduction caches.
#[trait_variant::make(Send)]
pub trait ReductionCache<A> {
    /// Puts an [Reduction] into the cache.
    async fn put(&self, reduction: &Reduction<A>) -> Result<(), ReductionCacheError>;
    /// Gets an [Reduction] from the cache.
    async fn get(&self, log_id: &LogId) -> Result<Option<Reduction<A>>, ReductionCacheError>;
}

/// Some caches will use this to let you control how [Reduction] instances
/// are serialized to bytes when written to the cache. For example, you can
/// implement these methods for any [serde] format you want, or you can use
/// something like protobuf, flatbuffers, or even rkyv.
pub trait ReductionCacheSerde<A> {
    /// Serializes an [Reduction] into bytes.
    fn serialize(&self, reduction: &Reduction<A>) -> Result<Vec<u8>, impl Error + 'static>;
    /// Deserializes a slice of bytes into an [Reduction].
    fn deserialize(&self, buf: &[u8]) -> Result<Reduction<A>, impl Error + 'static>;
}
