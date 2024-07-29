use crate::{
    caches::{AggregationCache, AggregationCacheError},
    ids::LogId,
    Aggregation,
};
use chrono::TimeDelta;
use deadpool_redis::{
    redis::{self, AsyncCommands, RedisError, ToRedisArgs},
    Pool, PoolError,
};

use super::AggregationCacheSerde;

impl From<PoolError> for AggregationCacheError {
    fn from(value: PoolError) -> Self {
        Self::DatabaseError(Box::new(value))
    }
}

impl From<RedisError> for AggregationCacheError {
    fn from(value: RedisError) -> Self {
        Self::DatabaseError(Box::new(value))
    }
}

impl ToRedisArgs for LogId {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        out.write_arg(self.to_string().as_bytes())
    }
}

/// An [AggregationCache] backed by a redis server/cluster.
pub struct RedisAggregationCache<S> {
    pool: Pool,
    serde: S,
    ttl: Option<TimeDelta>,
}

impl<S> RedisAggregationCache<S> {
    /// Constructs a new instance given a pre-configured deadpool_redis Pool,
    /// an [AggregationCacheSerde], and an optional time-to-live for items
    /// written to the cache.
    pub fn new(pool: Pool, serde: S, ttl: Option<TimeDelta>) -> Self {
        Self {
            pool,
            serde,
            ttl: ttl.map(|td| td.abs()),
        }
    }
}

impl<A, S> AggregationCache<A> for RedisAggregationCache<S>
where
    A: Send + Sync,
    S: AggregationCacheSerde<A> + Send + Sync,
{
    async fn put(&self, aggregation: &Aggregation<A>) -> Result<(), AggregationCacheError> {
        let serialized = self
            .serde
            .serialize(aggregation)
            .map_err(|e| AggregationCacheError::EncodingFailure(Box::new(e)))?;

        let mut conn = self.pool.get().await?;
        if let Some(duration) = self.ttl {
            conn.set_ex(
                aggregation.log_id(),
                &serialized,
                duration.num_seconds() as u64,
            )
            .await?;
        } else {
            conn.set(aggregation.log_id(), &serialized).await?;
        }

        Ok(())
    }

    async fn get(&self, log_id: &LogId) -> Result<Option<Aggregation<A>>, AggregationCacheError> {
        let mut conn = self.pool.get().await?;

        let maybe_bytes: Option<Vec<u8>> = if let Some(duration) = self.ttl {
            conn.get_ex(log_id, redis::Expiry::EX(duration.num_seconds() as usize))
                .await?
        } else {
            conn.get(log_id).await?
        };

        if let Some(buf) = maybe_bytes {
            let aggregation: Aggregation<A> = self
                .serde
                .deserialize(&buf)
                .map_err(|e| AggregationCacheError::DecodingFailure(Box::new(e)))?;
            Ok(Some(aggregation))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use std::error::Error;

    use crate::tests::TestAggregate;
    use chrono::Utc;
    use deadpool_redis::{Config, Runtime};

    use super::*;

    struct JsonSerde;
    impl AggregationCacheSerde<TestAggregate> for JsonSerde {
        fn serialize(
            &self,
            aggregation: &Aggregation<TestAggregate>,
        ) -> Result<Vec<u8>, impl Error + 'static> {
            serde_json::to_vec(aggregation)
        }

        fn deserialize(
            &self,
            buf: &[u8],
        ) -> Result<Aggregation<TestAggregate>, impl Error + 'static> {
            serde_json::from_slice(buf)
        }
    }

    fn cache() -> RedisAggregationCache<JsonSerde> {
        let cgf = Config::from_url("redis://localhost");
        let pool = cgf.create_pool(Some(Runtime::Tokio1)).unwrap();
        RedisAggregationCache::new(pool, JsonSerde, Some(TimeDelta::seconds(60)))
    }

    #[tokio::test]
    async fn put_get() {
        let cache = cache();

        let log_id = LogId::new();
        let test_aggregate = TestAggregate { count: 5 };
        let aggregation = Aggregation {
            log_id: log_id.clone(),
            reduced_at: Utc::now(),
            through_index: 1,
            aggregate: test_aggregate,
        };

        cache.put(&aggregation).await.unwrap();
        let maybe_aggregation = cache.get(&log_id).await.unwrap();
        assert_eq!(Some(aggregation), maybe_aggregation);
    }

    #[tokio::test]
    async fn get_not_found() {
        let cache = cache();
        let maybe_aggregation: Option<Aggregation<TestAggregate>> =
            cache.get(&LogId::new()).await.unwrap();
        assert_eq!(maybe_aggregation, None);
    }
}
