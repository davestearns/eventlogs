use crate::stores::EventStore;
use caches::{AggregationCache, AggregationCacheError};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use ids::LogId;
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, sync::Arc};
use stores::EventStoreError;
use thiserror::Error;
use tokio::sync::mpsc::{self, Sender};

pub mod caches;
pub mod ids;
pub mod stores;

pub trait EventRecord<E> {
    fn index(&self) -> u32;
    fn recorded_at(&self) -> DateTime<Utc>;
    fn event(&self) -> E;
}

pub trait Aggregate<E>: Default {
    fn apply(&mut self, event_record: &impl EventRecord<E>);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Aggregation<E, A: Aggregate<E>> {
    log_id: LogId,
    reduced_at: DateTime<Utc>,
    through_index: u32,
    aggregate: A,
    _phantom_e: PhantomData<E>,
}

impl<E, A: Aggregate<E>> Aggregation<E, A> {
    pub fn log_id(&self) -> &LogId {
        &self.log_id
    }

    pub fn reduced_at(&self) -> DateTime<Utc> {
        self.reduced_at
    }

    pub fn through_index(&self) -> u32 {
        self.through_index
    }

    pub fn aggregate(&self) -> &A {
        &self.aggregate
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum LogManagerError {
    #[error("the event store produced an unexpected error: {0}")]
    EventStoreError(#[from] EventStoreError),
    #[error("the aggregation cache produced an unexpected error: {0}")]
    AggregationCacheError(#[from] AggregationCacheError),
    #[error(
        "another process already appended another event since your last aggregation; 
    reduce again to apply the new event, determine if you operation is still 
    relevant/necessary, and if so try again"
    )]
    ConcurrentAppend(LogId),
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct CreateOptions {
    idempotency_key: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct AppendOptions {
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IdempotentOutcome {
    New,
    Replay,
}

pub struct LogManager<E: Send, A: Aggregate<E>, ES: EventStore<E>, AC: AggregationCache<E, A>> {
    event_store: ES,
    aggregation_cache: Arc<AC>,
    aggregation_sender: Sender<Aggregation<E, A>>,
}

impl<E, A, ES, AC> LogManager<E, A, ES, AC>
where
    E: Send + Clone + 'static,
    A: Aggregate<E> + Send + Clone + 'static,
    ES: EventStore<E>,
    AC: AggregationCache<E, A> + Send + Sync + 'static,
{
    pub fn new(event_store: ES, aggregation_cache: AC) -> Self {
        let cache_arc = Arc::new(aggregation_cache);
        let cloned_cache_arc = cache_arc.clone();
        let (sender, mut receiver) = mpsc::channel::<Aggregation<E, A>>(1024);

        // Spawn a background task that reads from the aggregation receiver channel
        // and updates the aggregation cache. This is used to update the cache asynchronously
        // and optimistically, so the caller of reduce() doesn't have to wait at the end
        // of the reduce() method.
        tokio::spawn(async move {
            while let Some(aggregation) = receiver.recv().await {
                // ignore errors since this is an async best-effort operation
                let _ = cloned_cache_arc.put(&aggregation).await;
            }
        });

        Self {
            event_store,
            aggregation_cache: cache_arc,
            aggregation_sender: sender,
        }
    }

    pub async fn create(
        &self,
        first_event: &E,
        create_options: &CreateOptions,
    ) -> Result<(LogId, IdempotentOutcome), LogManagerError> {
        Ok(self.event_store.create(first_event, create_options).await?)
    }

    pub async fn reduce(&self, log_id: &LogId) -> Result<Aggregation<E, A>, LogManagerError> {
        let maybe_aggregation = self.aggregation_cache.get(log_id).await?;
        let (mut aggregate, starting_index) = maybe_aggregation
            .map(|re| (re.aggregate, re.through_index + 1))
            .unwrap_or((A::default(), 0));

        let row_stream = self.event_store.load(log_id, starting_index).await?;
        let mut through_index = 0;
        tokio::pin!(row_stream);
        while let Some(event_record) = row_stream.try_next().await? {
            aggregate.apply(&event_record);
            if event_record.index() > through_index {
                through_index = event_record.index();
            }
        }

        let aggregation = Aggregation {
            log_id: log_id.clone(),
            reduced_at: Utc::now(),
            through_index,
            aggregate,
            _phantom_e: PhantomData,
        };

        // try to send the aggregation, but ignore errors since this is really
        // just an optimization.
        // TODO: add tracing/metrics if this fails.
        let _ = self.aggregation_sender.try_send(aggregation.clone());
        Ok(aggregation)
    }

    pub async fn append(
        &self,
        aggregation: Aggregation<E, A>,
        next_event: &E,
        append_options: &AppendOptions,
    ) -> Result<IdempotentOutcome, LogManagerError> {
        let outcome = self
            .event_store
            .append(
                aggregation.log_id(),
                next_event,
                aggregation.through_index() + 1,
                append_options,
            )
            .await
            .map_err(|e| match e {
                EventStoreError::EventIndexAlreadyExists { log_id: lid, .. } => {
                    LogManagerError::ConcurrentAppend(lid)
                }
                _ => LogManagerError::EventStoreError(e),
            })?;
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use caches::fake::{FakeAggregationCache, FakeAggregationCacheOp};
    use stores::fake::FakeEventStore;
    use uuid::Uuid;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub enum TestEvent {
        Increment,
        Decrement,
    }

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    pub struct TestAggregate {
        pub count: i32,
    }

    impl Aggregate<TestEvent> for TestAggregate {
        fn apply(&mut self, event_record: &impl crate::EventRecord<TestEvent>) {
            match event_record.event() {
                TestEvent::Increment => self.count += 1,
                TestEvent::Decrement => self.count -= 1,
            }
        }
    }

    #[tokio::test]
    async fn create_reduce() {
        let mgr = LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeAggregationCache::<TestEvent, TestAggregate>::new(),
        );

        let (log_id, _) = mgr
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        let agg = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(agg.log_id(), &log_id);
        assert_eq!(agg.through_index(), 0);
        assert_eq!(agg.aggregate().count, 1);
    }

    #[tokio::test]
    async fn cached_reduction_gets_used() {
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<FakeAggregationCacheOp<TestEvent, TestAggregate>>(64);
        let mgr = LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeAggregationCache::<TestEvent, TestAggregate>::new_with_notifications(sender),
        );

        let (log_id, _) = mgr
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        let agg = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(agg.log_id(), &log_id);
        assert_eq!(agg.through_index(), 0);
        assert_eq!(agg.aggregate().count, 1);

        // first op should be a Get that found nothing
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeAggregationCacheOp::Get {
                log_id: log_id.clone(),
                response: None
            }
        );
        // next op should be the put of the reduction we just got back
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeAggregationCacheOp::Put {
                aggregation: agg.clone()
            }
        );

        mgr.append(
            agg.clone(),
            &TestEvent::Decrement,
            &AppendOptions::default(),
        )
        .await
        .unwrap();

        let re_agg = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(re_agg.log_id(), &log_id);
        assert_eq!(re_agg.through_index(), 1);
        assert_eq!(re_agg.aggregate().count, 0);

        // next op should be a Get that found the previous agg
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeAggregationCacheOp::Get {
                log_id: log_id.clone(),
                response: Some(agg.clone()),
            }
        );
        // next op should be the put of the reduction we just got back
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeAggregationCacheOp::Put {
                aggregation: re_agg.clone()
            }
        );
    }

    #[tokio::test]
    async fn idempotent_create() {
        let mgr = LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeAggregationCache::<TestEvent, TestAggregate>::new(),
        );

        let create_options = CreateOptions {
            idempotency_key: Some(Uuid::now_v7().to_string()),
            ..Default::default()
        };
        let (log_id, outcome) = mgr
            .create(&TestEvent::Increment, &create_options)
            .await
            .unwrap();
        assert_eq!(outcome, IdempotentOutcome::New);

        let (replay_log_id, replay_outcome) = mgr
            .create(&TestEvent::Increment, &create_options)
            .await
            .unwrap();

        assert_eq!(log_id, replay_log_id);
        assert_eq!(replay_outcome, IdempotentOutcome::Replay);
    }

    #[tokio::test]
    async fn idempotent_append() {
        let mgr = LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeAggregationCache::<TestEvent, TestAggregate>::new(),
        );

        let (log_id, _) = mgr
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        let append_options = AppendOptions {
            idempotency_key: Some(Uuid::now_v7().to_string()),
            ..Default::default()
        };

        let agg = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(agg.through_index(), 0);
        assert_eq!(agg.aggregate().count, 1);

        mgr.append(agg, &TestEvent::Decrement, &append_options)
            .await
            .unwrap();

        let agg = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(agg.through_index(), 1);
        assert_eq!(agg.aggregate().count, 0);

        // This should succeed, but be a no-op since an event with the same
        // idempotency key was already appended above.
        let outcome = mgr
            .append(agg, &TestEvent::Decrement, &append_options)
            .await
            .unwrap();
        assert_eq!(outcome, IdempotentOutcome::Replay);

        let agg = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(agg.through_index(), 1);
        assert_eq!(agg.aggregate().count, 0);
    }

    #[tokio::test]
    async fn concurrent_append() {
        let mgr = LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeAggregationCache::<TestEvent, TestAggregate>::new(),
        );

        let (log_id, _) = mgr
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        let agg = mgr.reduce(&log_id).await.unwrap();

        mgr.append(
            agg.clone(),
            &TestEvent::Decrement,
            &AppendOptions::default(),
        )
        .await
        .unwrap();

        let result = mgr
            .append(
                agg.clone(),
                &TestEvent::Decrement,
                &AppendOptions::default(),
            )
            .await;

        assert_eq!(result, Err(LogManagerError::ConcurrentAppend(log_id)));
    }
}
