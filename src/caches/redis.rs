use crate::{
    caches::{AggregationCache, AggregationCacheError},
    ids::LogId,
    Aggregate, Aggregation,
};
use deadpool_redis::{
    redis::{self, RedisError, ToRedisArgs},
    Pool, PoolError,
};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;

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

impl From<rmp_serde::encode::Error> for AggregationCacheError {
    fn from(value: rmp_serde::encode::Error) -> Self {
        Self::EncodingFailure(Box::new(value))
    }
}

impl From<rmp_serde::decode::Error> for AggregationCacheError {
    fn from(value: rmp_serde::decode::Error) -> Self {
        Self::DecodingFailure(Box::new(value))
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

pub struct RedisAggregationCache<E, A: Aggregate<E>> {
    pool: Pool,
    maybe_ttl_secs: Option<u32>,
    _phantom_e: PhantomData<E>,
    _phantom_a: PhantomData<A>,
}

impl<E, A> RedisAggregationCache<E, A>
where
    E: Send,
    A: Aggregate<E> + Send,
{
    pub fn new(pool: Pool, ttl_secs: Option<u32>) -> Self {
        Self {
            pool,
            maybe_ttl_secs: ttl_secs,
            _phantom_e: PhantomData,
            _phantom_a: PhantomData,
        }
    }
}

impl<E, A> AggregationCache<E, A> for RedisAggregationCache<E, A>
where
    E: Send + Sync,
    A: Aggregate<E> + Send + Sync + Serialize + DeserializeOwned,
{
    async fn put(&self, aggregation: &Aggregation<E, A>) -> Result<(), AggregationCacheError> {
        let mut conn = self.pool.get().await?;
        let serialized = rmp_serde::to_vec(aggregation)?;
        let mut cmd = redis::cmd("SET");
        cmd.arg(aggregation.log_id());
        cmd.arg(serialized);
        if let Some(ttl_secs) = self.maybe_ttl_secs {
            cmd.arg("EX");
            cmd.arg(ttl_secs);
        }
        cmd.query_async(&mut conn).await?;
        Ok(())
    }

    async fn get(
        &self,
        log_id: &LogId,
    ) -> Result<Option<Aggregation<E, A>>, AggregationCacheError> {
        let mut conn = self.pool.get().await?;
        let mut cmd = redis::cmd("GET");
        cmd.arg(log_id);
        if let Some(ttl_secs) = self.maybe_ttl_secs {
            cmd.arg("EX");
            cmd.arg(ttl_secs);
        }

        let maybe_bytes: Option<Vec<u8>> = cmd.query_async(&mut conn).await?;
        match maybe_bytes {
            None => Ok(None),
            Some(bytes) => Ok(rmp_serde::from_slice(&bytes)?),
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use crate::tests::{TestAggregate, TestEvent};
    use chrono::Utc;
    use deadpool_redis::{Config, Runtime};

    use super::*;

    fn cache() -> RedisAggregationCache<TestEvent, TestAggregate> {
        let cgf = Config::from_url("redis://localhost");
        let pool = cgf.create_pool(Some(Runtime::Tokio1)).unwrap();
        RedisAggregationCache::new(pool, None)
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
            _phantom_e: PhantomData,
        };

        cache.put(&aggregation).await.unwrap();
        let maybe_aggregation = cache.get(&log_id).await.unwrap();
        assert_eq!(Some(aggregation), maybe_aggregation);
    }

    #[tokio::test]
    async fn get_not_found() {
        let cache = cache();
        let maybe_aggregation = cache.get(&LogId::new()).await.unwrap();
        assert_eq!(maybe_aggregation, None);
    }
}
