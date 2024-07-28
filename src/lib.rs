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

pub trait Aggregate: Default {
    type Event;
    fn apply(&mut self, event_record: &impl EventRecord<Self::Event>);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Aggregation<A> {
    log_id: LogId,
    reduced_at: DateTime<Utc>,
    through_index: u32,
    aggregate: A,
}

impl<A> Aggregation<A> {
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
        "another process already appended another event to log_id={0} 
        since your last aggregation; reduce again to apply the new event, 
        determine if you operation is still relevant/necessary, and if so try again"
    )]
    ConcurrentAppend(LogId),
    #[error("an event with the provided idempotency key already exists")]
    IdempotentReplay {
        idempotency_key: String,
        log_id: LogId,
        event_index: u32,
    },
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct CreateOptions {
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct AppendOptions {
    pub idempotency_key: Option<String>,
}

#[derive(Debug)]
pub struct LogManager<E, A, ES, AC> {
    event_store: ES,
    aggregation_cache: Arc<AC>,
    aggregation_sender: Sender<Aggregation<A>>,
    _phantom_e: PhantomData<E>,
}

impl<E, A, ES, AC> LogManager<E, A, ES, AC>
where
    E: Send + 'static,
    A: Aggregate<Event = E> + Send + Clone + 'static,
    ES: EventStore<E>,
    AC: AggregationCache<A> + Send + Sync + 'static,
{
    pub fn new(event_store: ES, aggregation_cache: AC) -> Self {
        let cache_arc = Arc::new(aggregation_cache);
        let cloned_cache_arc = cache_arc.clone();
        let (sender, mut receiver) = mpsc::channel::<Aggregation<A>>(1024);

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
            _phantom_e: PhantomData,
        }
    }

    pub async fn create(
        &self,
        log_id: &LogId,
        first_event: &E,
        create_options: &CreateOptions,
    ) -> Result<(), LogManagerError> {
        self.event_store
            .create(log_id, first_event, create_options)
            .await
            .map_err(|e| match e {
                EventStoreError::IdempotentReplay {
                    idempotency_key,
                    log_id,
                    event_index,
                } => LogManagerError::IdempotentReplay {
                    idempotency_key,
                    log_id,
                    event_index,
                },
                _ => LogManagerError::EventStoreError(e),
            })?;

        Ok(())
    }

    pub async fn reduce(&self, log_id: &LogId) -> Result<Aggregation<A>, LogManagerError> {
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
        };

        // try to send the aggregation, but ignore errors since this is really
        // just an optimization.
        // TODO: add tracing/metrics if this fails.
        let _ = self.aggregation_sender.try_send(aggregation.clone());
        Ok(aggregation)
    }

    pub async fn append(
        &self,
        aggregation: Aggregation<A>,
        next_event: &E,
        append_options: &AppendOptions,
    ) -> Result<(), LogManagerError> {
        self.event_store
            .append(
                aggregation.log_id(),
                next_event,
                aggregation.through_index() + 1,
                append_options,
            )
            .await
            .map_err(|e| match e {
                // If the event index already exists, there was a concurrent append
                // and this process lost the race
                EventStoreError::EventIndexAlreadyExists { log_id: lid, .. } => {
                    LogManagerError::ConcurrentAppend(lid)
                }
                EventStoreError::IdempotentReplay {
                    idempotency_key,
                    log_id,
                    event_index,
                } => LogManagerError::IdempotentReplay {
                    idempotency_key,
                    log_id,
                    event_index,
                },
                _ => LogManagerError::EventStoreError(e),
            })?;
        Ok(())
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

    impl Aggregate for TestAggregate {
        type Event = TestEvent;

        fn apply(&mut self, event_record: &impl EventRecord<TestEvent>) {
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
            FakeAggregationCache::<TestAggregate>::new(),
        );

        let log_id = LogId::new();
        mgr.create(&log_id, &TestEvent::Increment, &CreateOptions::default())
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
            tokio::sync::mpsc::channel::<FakeAggregationCacheOp<TestAggregate>>(64);
        let mgr = LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeAggregationCache::<TestAggregate>::new_with_notifications(sender),
        );

        let log_id = LogId::new();
        mgr.create(&log_id, &TestEvent::Increment, &CreateOptions::default())
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
            FakeAggregationCache::<TestAggregate>::new(),
        );

        let log_id = LogId::new();
        let idempotency_key = Uuid::now_v7().to_string();
        let create_options = CreateOptions {
            idempotency_key: Some(idempotency_key.clone()),
            ..Default::default()
        };
        mgr.create(&log_id, &TestEvent::Increment, &create_options)
            .await
            .unwrap();

        let replay_log_id = LogId::new();
        let result = mgr
            .create(&replay_log_id, &TestEvent::Increment, &create_options)
            .await;

        assert_eq!(
            result,
            Err(LogManagerError::IdempotentReplay {
                idempotency_key: idempotency_key.clone(),
                log_id: log_id.clone(), // original log id, not replay
                event_index: 0
            })
        );
    }

    #[tokio::test]
    async fn idempotent_append() {
        let mgr = LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeAggregationCache::<TestAggregate>::new(),
        );

        let log_id = LogId::new();
        mgr.create(&log_id, &TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        let idempotency_key = Uuid::now_v7().to_string();
        let append_options = AppendOptions {
            idempotency_key: Some(idempotency_key.clone()),
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

        let result = mgr
            .append(agg, &TestEvent::Decrement, &append_options)
            .await;

        assert_eq!(
            result,
            Err(LogManagerError::IdempotentReplay {
                idempotency_key: idempotency_key.clone(),
                log_id: log_id.clone(),
                event_index: 1
            })
        );

        let agg = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(agg.through_index(), 1);
        assert_eq!(agg.aggregate().count, 0);
    }

    #[tokio::test]
    async fn concurrent_append() {
        let mgr = LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeAggregationCache::<TestAggregate>::new(),
        );

        let log_id = LogId::new();
        mgr.create(&log_id, &TestEvent::Increment, &CreateOptions::default())
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
