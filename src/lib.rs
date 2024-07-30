#![doc = include_str!("../README.md")]
use crate::stores::EventStore;
use caches::{ReductionCache, ReductionCacheError};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
pub use ids::LogId;
use policies::{CachingPolicy, NoPolicy};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, marker::PhantomData, sync::Arc};
use stores::EventStoreError;
use thiserror::Error;
use tokio::sync::mpsc::{self, Sender};

/// The [ReductionCache] trait and implementations.
pub mod caches;
/// Home of the [LogId] struct.
pub mod ids;
/// A few common implementations of the [CachingPolicy] trait.
pub mod policies;
/// The [EventStore] trait and implementations.
pub mod stores;

/// Represents an event read from an [EventStore].
///
/// Event stores implement this trait directly on their native row/record
/// objects so that they don't have to allocate and copy the data. An
/// [Aggregate] will get these passed to its [Aggregate::apply] method.
pub trait EventRecord<E> {
    /// Returns the zero-based sequential index of the event in its log.
    fn index(&self) -> u32;
    /// Returns the wall-clock time when this event was recorded
    /// in the [EventStore]. This is for informational purposes only
    /// since clocks on different service instances using this library
    /// could be skewed.
    fn recorded_at(&self) -> DateTime<Utc>;
    /// Returns the event.
    fn event(&self) -> E;
}

/// The trait that must be implemented on your aggregates.
///
/// This allows the framework to apply stored events to your aggregate
/// during a reduction. Your aggregate can `match` on the provided
/// `event_record.event()` and update its state accordingly. See the
/// crate overview documentation for a very simple example of this.
pub trait Aggregate: Default {
    /// The event type this aggregate aggregates.
    type Event;
    /// Applies a single event to the aggregate's state. Events will
    /// always be applied in index order.
    fn apply(&mut self, event_record: &impl EventRecord<Self::Event>);
}

/// Describes the result of a successful [LogManager::reduce] operation.
///
/// This is what is stored in the [ReductionCache]. During subsequent
/// reductions, the [LogManager] only selects the events with indexes
/// higher than the `through_index`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reduction<A> {
    log_id: LogId,
    reduced_at: DateTime<Utc>,
    through_index: u32,
    aggregate: A,
}

impl<A> Reduction<A> {
    /// Returns the ID of the log that was reduced.
    pub fn log_id(&self) -> &LogId {
        &self.log_id
    }

    /// Returns the wall-clock time at which the reduction occurred.
    /// This is for informational purposes only since the clocks
    /// on different service instances using this library might be
    /// skewed.
    pub fn reduced_at(&self) -> DateTime<Utc> {
        self.reduced_at
    }

    /// Returns the max event index included in the [Aggregate].
    pub fn through_index(&self) -> u32 {
        self.through_index
    }

    /// Returns a reference to the [Aggregate].
    pub fn aggregate(&self) -> &A {
        &self.aggregate
    }

    pub fn log_state(&self) -> LogState {
        LogState {
            next_index: self.through_index + 1,
        }
    }
}

/// The error type returned from all [LogManager] methods.
#[derive(Debug, Error, PartialEq)]
pub enum LogManagerError {
    /// Occurs when there is an unexpected error from the [EventStore].
    /// Typically this will be a networking or database server error.
    #[error("the event store produced an unexpected error: {0}")]
    EventStoreError(#[from] EventStoreError),
    /// Occurs when there is an unexpected error from the [ReductionCache].
    /// Typically this will be a networking or cache server error.
    #[error("the reduction cache produced an unexpected error: {0}")]
    ReductionCacheError(#[from] ReductionCacheError),
    /// Occurs when two different processes race to append an event
    /// to the same log, and this process lost the race. When this occurs,
    /// you should re-reduce the log to see the effect of the other
    /// process'es event, determine if your event is still relevant,
    /// and if so, try the append operation again.
    #[error(
        "another process already appended another event to log_id={0} 
        since your last operation; reduce again to apply the new event, 
        determine if you operation is still relevant/necessary, and if so try again"
    )]
    ConcurrentAppend(LogId),
    /// Occurs when a log is created or appended to using an idempotency key
    /// that already exists in the [EventStore]. The log_id and event_index
    /// fields in this error indicate the ID and index of the existing log event.
    #[error("an event with the provided idempotency key already exists")]
    IdempotentReplay {
        idempotency_key: String,
        log_id: LogId,
        event_index: u32,
    },
}

/// Options that can be specified when using [LogManager::append].
///
/// To protect yourself against more options being added in the future,
/// use `..Default::default()` at the end of an initialization, like so:
/// ```
/// # use eventlogs::AppendOptions;
/// # let caller_supplied_idempotency_key = "test".to_string();
/// let options = AppendOptions {
///     idempotency_key: Some(caller_supplied_idempotency_key),
///     ..Default::default()
/// };
/// ```
/// This will continue to compile even if more fields are added in the
/// future, as their types will always implement [Default].
#[derive(Debug, Default, Clone, PartialEq)]
pub struct AppendOptions {
    /// A universally-unique value that ensures the operation happens
    /// only once, regardless of retries after network timeouts or other
    /// communication failures.
    pub idempotency_key: Option<String>,
}

/// Options that can be used when constructing a [LogManager].
///
/// To protect yourself against more options being added in the future,
/// use `..Default::default()` at the end of an initialization, like so:
/// ```
/// use eventlogs::{LogManagerOptions, policies::LogLengthPolicy};
/// let options = LogManagerOptions {
///     caching_policy: Some(LogLengthPolicy::at_least(10)),
///     ..Default::default()
/// };
/// ```
/// This will continue to compile even if more fields are added in the
/// future, as their types will always implement [Default].
#[derive(Debug, Default)]
pub struct LogManagerOptions<ACP> {
    pub caching_policy: Option<ACP>,
}

/// Represents the known state of the log after an event has been
/// successfully appended by this process.
///
/// This (or a Reduction) must be passed back to the next append()
/// method call so that the state of the application can be
/// compared with the state of the database. This allows us to
/// detect and avoid concurrent appends to the same log by
/// different processes.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct LogState {
    next_index: u32,
}

impl LogState {
    /// Constructs a [LogState] for a new log. This is the same
    /// as calling [LogState::default].
    fn new() -> Self {
        Self { next_index: 0 }
    }
}

/// Converts a [Reduction] into a [LogState] by calling [Reduction::log_state].
impl<A> From<Reduction<A>> for LogState {
    fn from(reduction: Reduction<A>) -> Self {
        reduction.log_state()
    }
}

/// Manages logs with given Event and [Aggregate] types, stored in an [EventStore],
/// and cached in a [ReductionCache], optionally controlled by an
/// [CachingPolicy].
///
/// If you have multiple event/aggregate types in your
/// system, create a separate [LogManager] for each, though you can share the
/// same [EventStore] and [ReductionCache] since [LogId]s are universally unique.
#[derive(Debug)]
pub struct LogManager<E, A, ES, AC> {
    event_store: ES,
    reduction_cache: Arc<AC>,
    reduction_sender: Sender<Reduction<A>>,
    _phantom_e: PhantomData<E>,
}

impl<E, A, ES, AC> LogManager<E, A, ES, AC>
where
    E: Send + 'static,
    A: Aggregate<Event = E> + Send + Clone + 'static,
    ES: EventStore<E>,
    AC: ReductionCache<A> + Send + Sync + 'static,
{
    /// Constructs a new [LogManager] that uses the provided [EventStore] and [ReductionCache]
    /// with default [LogManagerOptions].
    pub fn new(event_store: ES, reduction_cache: AC) -> Self {
        Self::with_options::<NoPolicy>(event_store, reduction_cache, LogManagerOptions::default())
    }

    /// Constructs a new [LogManager] that uses the provided [EventStore], [ReductionCache],
    /// and [LogManagerOptions].
    ///
    /// To protect yourself against the addition of more fields to [LogManagerOptions]
    /// in the future, add `..Default::default()` to the end of the initialization:
    /// ```
    /// # use eventlogs::{LogManagerOptions, policies::LogLengthPolicy};
    /// let options = LogManagerOptions {
    ///     // only cache logs with 10 events or more
    ///     caching_policy: Some(LogLengthPolicy::at_least(10)),
    ///     ..Default::default()
    /// };
    /// ```
    pub fn with_options<ACP>(
        event_store: ES,
        reduction_cache: AC,
        options: LogManagerOptions<ACP>,
    ) -> Self
    where
        ACP: CachingPolicy<A> + Send + Sync + 'static,
    {
        let cache_arc = Arc::new(reduction_cache);
        let cloned_cache_arc = cache_arc.clone();
        let (sender, mut receiver) = mpsc::channel::<Reduction<A>>(1024);

        // Spawn a background task that reads from the reduction receiver channel
        // and updates the reduction cache. This is used to update the cache asynchronously
        // and optimistically, so the caller of reduce() doesn't have to wait at the end
        // of the reduce() method.
        tokio::spawn(async move {
            while let Some(reduction) = receiver.recv().await {
                if options
                    .caching_policy
                    .as_ref()
                    .map(|p| p.should_cache(&reduction))
                    .unwrap_or(true)
                {
                    // ignore errors since this is an async best-effort operation
                    let _ = cloned_cache_arc.put(&reduction).await;
                }
            }
        });

        Self {
            event_store,
            reduction_cache: cache_arc,
            reduction_sender: sender,
            _phantom_e: PhantomData,
        }
    }

    /// Creates a new log for the provided `log_id` and appends the `event`
    /// to that new log.
    ///
    /// Note that this is equivalent to calling `append()` passing
    /// `LogState::new()`, but is a bit more discoverable.
    ///
    /// If you specify an idempotency key in the [AppendOptions] and this is
    /// an idempotent replay, the returned [Result] will be a
    /// [LogManagerError::IdempotentReplay] error with the id of the
    /// log that was previously-created with the same idempotency key.
    pub async fn create(
        &self,
        log_id: &LogId,
        event: &E,
        append_options: &AppendOptions,
    ) -> Result<LogState, LogManagerError> {
        self.append(log_id, LogState::new(), event, append_options)
            .await
    }

    /// Appends an event to an existing log.
    ///
    /// The `log_state` argument can be either a [LogState] returned from the
    /// previous create/append call, or a [Reduction] returned from a previous
    /// reduce call. This allows us to compare the state of the application against
    /// the state of the database in order to detect concurrent appends to the
    /// same log by different processes.
    ///
    /// If multiple processes race to append an event to the same log, only one will
    /// win and the others will get a [LogManagerError::ConcurrentAppend] error.
    /// The losers should re-reduce the log to see the effect of the new event,
    /// decide if their event is still relevant, and if so, attempt to append again.
    ///
    /// If an idempotency key is provided in the [AppendOptions] and another event
    /// already exists with that same key, this will return a
    /// [LogManagerError::IdempotentReplay] error containing the log ID and index
    /// of the already-recorded event with that same idempotency key. If you want
    /// the idempotency key keys enforced per-log and not universally, you can set
    /// the [AppendOptions::idempotency_key] to the combination of the log ID and
    /// the idempotency key you received from your caller, like so:
    /// ```
    /// use eventlogs::{AppendOptions, LogId};
    /// # let log_id = LogId::new();
    /// # let idempotency_key_from_caller = "test".to_string();
    /// // scope the idempotency key provided by our caller to the log ID
    /// // so that it doesn't conflict with keys used in other logs.
    /// let append_options = AppendOptions {
    ///     idempotency_key: Some(format!("{log_id}_{idempotency_key_from_caller}")),
    ///     ..Default::default()
    /// };
    /// ```
    pub async fn append(
        &self,
        log_id: &LogId,
        log_state: impl Into<LogState>,
        event: &E,
        append_options: &AppendOptions,
    ) -> Result<LogState, LogManagerError> {
        let next_index = log_state.into().next_index;
        self.event_store
            .append(log_id, event, next_index, append_options)
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
        Ok(LogState {
            next_index: next_index + 1,
        })
    }

    /// Reduces the events in a given log to the configured [Aggregate].
    ///
    /// The [Reduction] will be cached asynchronously if the caching
    /// policy allows it. By default, all reductions will be cached.
    pub async fn reduce(&self, log_id: &LogId) -> Result<Reduction<A>, LogManagerError> {
        let maybe_reduction = self.reduction_cache.get(log_id).await?;
        let (aggregate, starting_index) = maybe_reduction
            .map(|re| (re.aggregate, re.through_index + 1))
            .unwrap_or((A::default(), 0));

        let event_stream = self
            .event_store
            .load(log_id, starting_index, u32::MAX)
            .await?;
        let reduction = Reduction {
            log_id: log_id.clone(),
            reduced_at: Utc::now(),
            through_index: 0,
            aggregate,
        };

        let reduction = event_stream
            .try_fold(reduction, |mut r, e| async move {
                r.aggregate.apply(&e);
                r.through_index = std::cmp::max(r.through_index, e.index());
                Ok(r)
            })
            .await?;

        // send the reduction to the async cache put task, but fail open.
        // TODO: emit a metric when this fails
        let _ = self.reduction_sender.send(reduction.clone()).await;

        Ok(reduction)
    }

    /// Returns an asynchronous stream of [EventRecord]s from the specified log,
    /// starting at the `starting_index`.
    pub async fn load<'a>(
        &'a self,
        log_id: &'a LogId,
        starting_index: u32,
        max_events: u32,
    ) -> Result<Vec<impl EventRecord<E> + 'a>, LogManagerError> {
        let row_stream = self
            .event_store
            .load(log_id, starting_index, max_events)
            .await?;

        let events = row_stream.try_collect().await?;
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use caches::fake::{FakeReductionCache, FakeReductionCacheOp};
    use policies::LogLengthPolicy;
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

    fn log_manager() -> LogManager<
        TestEvent,
        TestAggregate,
        FakeEventStore<TestEvent>,
        FakeReductionCache<TestAggregate>,
    > {
        LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeReductionCache::<TestAggregate>::new(),
        )
    }

    #[tokio::test]
    async fn create_reduce() {
        let mgr = log_manager();

        let log_id = LogId::new();
        mgr.create(&log_id, &TestEvent::Increment, &AppendOptions::default())
            .await
            .unwrap();

        let reduction = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(reduction.log_id(), &log_id);
        assert_eq!(reduction.through_index(), 0);
        assert_eq!(reduction.aggregate().count, 1);
    }

    #[tokio::test]
    async fn create_append_load() {
        let mgr = log_manager();

        let log_id = LogId::new();
        let log_state = mgr
            .create(&log_id, &TestEvent::Increment, &AppendOptions::default())
            .await
            .unwrap();

        mgr.append(
            &log_id,
            log_state,
            &TestEvent::Increment,
            &AppendOptions::default(),
        )
        .await
        .unwrap();

        let events = mgr.load(&log_id, 0, 100).await.unwrap();
        assert_eq!(events.len(), 2);
        for (idx, evt) in events.iter().enumerate() {
            assert_eq!(evt.index() as usize, idx);
            assert_eq!(evt.event(), TestEvent::Increment)
        }
    }

    #[tokio::test]
    async fn create_append_many() {
        let mgr = log_manager();

        let log_id = LogId::new();
        let mut log_state = mgr
            .create(&log_id, &TestEvent::Increment, &AppendOptions::default())
            .await
            .unwrap();

        for _i in 0..10 {
            log_state = mgr
                .append(
                    &log_id,
                    log_state,
                    &TestEvent::Increment,
                    &AppendOptions::default(),
                )
                .await
                .unwrap();
        }

        let reduction = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(reduction.aggregate().count, 11);
    }

    #[tokio::test]
    async fn cached_reduction_gets_used() {
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<FakeReductionCacheOp<TestAggregate>>(64);
        let mgr = LogManager::new(
            FakeEventStore::<TestEvent>::new(),
            FakeReductionCache::<TestAggregate>::with_notifications(sender),
        );

        let log_id = LogId::new();
        mgr.create(&log_id, &TestEvent::Increment, &AppendOptions::default())
            .await
            .unwrap();

        let first_reduction = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(first_reduction.log_id(), &log_id);
        assert_eq!(first_reduction.through_index(), 0);
        assert_eq!(first_reduction.aggregate().count, 1);

        // first op should be a Get that found nothing
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeReductionCacheOp::Get {
                log_id: log_id.clone(),
                response: None
            }
        );
        // next op should be the put of the reduction we just got back
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeReductionCacheOp::Put {
                reduction: first_reduction.clone()
            }
        );

        mgr.append(
            &log_id,
            first_reduction.clone(),
            &TestEvent::Decrement,
            &AppendOptions::default(),
        )
        .await
        .unwrap();

        let second_reduction = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(second_reduction.log_id(), &log_id);
        assert_eq!(second_reduction.through_index(), 1);
        assert_eq!(second_reduction.aggregate().count, 0);

        // next op should be a Get that found the previous agg
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeReductionCacheOp::Get {
                log_id: log_id.clone(),
                response: Some(first_reduction.clone()),
            }
        );
        // next op should be the put of the reduction we just got back
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeReductionCacheOp::Put {
                reduction: second_reduction.clone()
            }
        );
    }

    #[tokio::test]
    async fn idempotent_create() {
        let mgr = log_manager();

        let log_id = LogId::new();
        let idempotency_key = Uuid::now_v7().to_string();
        let create_options = AppendOptions {
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
        let mgr = log_manager();

        let log_id = LogId::new();
        let log_state = mgr
            .create(&log_id, &TestEvent::Increment, &AppendOptions::default())
            .await
            .unwrap();

        let idempotency_key = Uuid::now_v7().to_string();
        let append_options = AppendOptions {
            idempotency_key: Some(idempotency_key.clone()),
            ..Default::default()
        };

        let log_state = mgr
            .append(&log_id, log_state, &TestEvent::Decrement, &append_options)
            .await
            .unwrap();

        let reduction = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(reduction.through_index(), 1);
        assert_eq!(reduction.aggregate().count, 0);

        let result = mgr
            .append(&log_id, log_state, &TestEvent::Decrement, &append_options)
            .await;

        assert_eq!(
            result,
            Err(LogManagerError::IdempotentReplay {
                idempotency_key: idempotency_key.clone(),
                log_id: log_id.clone(),
                event_index: 1
            })
        );

        let reduction = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(reduction.through_index(), 1);
        assert_eq!(reduction.aggregate().count, 0);
    }

    #[tokio::test]
    async fn concurrent_append() {
        let mgr = log_manager();

        let log_id = LogId::new();
        let log_state = mgr
            .create(&log_id, &TestEvent::Increment, &AppendOptions::default())
            .await
            .unwrap();

        mgr.append(
            &log_id,
            log_state.clone(),
            &TestEvent::Decrement,
            &AppendOptions::default(),
        )
        .await
        .unwrap();

        let result = mgr
            .append(
                &log_id,
                log_state,
                &TestEvent::Decrement,
                &AppendOptions::default(),
            )
            .await;

        assert_eq!(result, Err(LogManagerError::ConcurrentAppend(log_id)));
    }

    #[tokio::test]
    async fn log_length_caching_policy() {
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<FakeReductionCacheOp<TestAggregate>>(64);
        let mgr = LogManager::with_options(
            FakeEventStore::<TestEvent>::new(),
            FakeReductionCache::<TestAggregate>::with_notifications(sender),
            LogManagerOptions {
                caching_policy: Some(LogLengthPolicy::at_least(2)),
            },
        );

        let log_id = LogId::new();
        mgr.create(&log_id, &TestEvent::Increment, &AppendOptions::default())
            .await
            .unwrap();

        let first_reduction = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(first_reduction.log_id(), &log_id);
        assert_eq!(first_reduction.through_index(), 0);
        assert_eq!(first_reduction.aggregate().count, 1);

        // first op should be a Get that found nothing
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeReductionCacheOp::Get {
                log_id: log_id.clone(),
                response: None
            }
        );
        // policy should have prohibited the aggregate from being cached
        // (will assert that below)

        mgr.append(
            &log_id,
            first_reduction,
            &TestEvent::Decrement,
            &AppendOptions::default(),
        )
        .await
        .unwrap();
        let second_reduction = mgr.reduce(&log_id).await.unwrap();
        assert_eq!(second_reduction.log_id(), &log_id);
        assert_eq!(second_reduction.through_index(), 1);
        assert_eq!(second_reduction.aggregate().count, 0);

        // first op should be a Get that found nothing because policy prohibited caching
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeReductionCacheOp::Get {
                log_id: log_id.clone(),
                response: None
            }
        );
        // but now that the log has the min length required, it should have cached the second_agg
        let op = receiver.recv().await.unwrap();
        assert_eq!(
            op,
            FakeReductionCacheOp::Put {
                reduction: second_reduction.clone(),
            }
        );
    }
}
