# High Performance, Batteries-Included, Event Sourcing in Rust

[![CI](https://github.com/davestearns/eventlogs/actions/workflows/ci.yml/badge.svg)](https://github.com/davestearns/eventlogs/actions/workflows/ci.yml)

This crate supports a style of transaction processing known as ["event sourcing."](https://martinfowler.com/eaaDev/EventSourcing.html) Instead of storing a single mutable record that is updated as the entity's state changes, event-sourcing systems record a series of immutable events about each entity, and reduce those events into a current state (known as an "aggregate") as needed. The event log for an entity provides a complete audit trail and makes it easier to record distinct properties about events that may occur multiple times (e.g., a payment that is partially captured or refunded several times).

## Built-In Features

- **Idempotency:** When creating a new log or appending an event to an existing one, the caller can include a unique `idempotency_key` that ensures the operation occurs only once, even if the request is retried. Idempotent replays will return a
[LogManagerError::IdempotentReplay] error with the previously-recorded [LogId] and event index, so that you can easily detect and react to them.
- **Concurrency:** If multiple service instances attempt to append a new event to the same log at the same time, only one will win the race, and the others will receive an error. The losers can then re-reduce the log to apply the new event to the aggregate, determine if their operation is still relevant, and try again.
- **Async Aggregate Caching:** When you reduce a log, the resulting aggregate is written asynchronously to a cache like Redis. Subsequent calls to `reduce()` will reuse that cached aggregate, and only fetch/apply events that were recorded _after_ the aggregate was last calculated. This makes subsequent reductions faster without slowing down your code.
- **Caching Policies:** Aggregates are always cached by default, but if you want to control when this occurs based on aggregate properties, you can provide an implementation of [AggregationCachingPolicy], which will be called by the asynchronous caching task to determine if the aggregate should be written to the cache.

## Example Usage
```rust
use std::error::Error;
use eventlogs::{ids::LogId, LogManager, LogManagerOptions, CreateOptions,
    AppendOptions, Aggregate, EventRecord};
use eventlogs::stores::fake::FakeEventStore;
use eventlogs::caches::fake::FakeAggregationCache;

/// Events are typically defined as members of an enum.
/// properties for events can be defined as fields on
/// the enum variant.
#[derive(Debug, Clone)]
pub enum TestEvent {
    Increment,
    Decrement,
}

/// An Aggregate is a simple struct with fields that get
/// calculated from the events recorded in the log. Note that
/// Aggregates must implement the Aggregate and Default traits.
#[derive(Debug, Default, Clone)]
pub struct TestAggregate {
    pub count: isize,
}

impl Aggregate for TestAggregate {
    type Event = TestEvent;
    fn apply(&mut self, event_record: &impl EventRecord<Self::Event>) {
        match event_record.event() {
            TestEvent::Increment => self.count += 1,
            TestEvent::Decrement => self.count -= 1,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // This uses testing fakes, but you would use PostgresEventStore
    // RedisAggregationCache configured to point to your servers.
    let log_manager = LogManager::new(
        FakeEventStore::<TestEvent>::new(),
        FakeAggregationCache::<TestAggregate>::new(),
        LogManagerOptions::default(),
    );
    
    // Create a new log with an Increment event as the first event.
    let log_id = LogId::new();
    log_manager.create(&log_id, &TestEvent::Increment, &CreateOptions::default()).await?;
    
    // Reduce the log to get the current state. The TestAggregate
    // will be cached automatically on a background task, so it won't
    // slow down your code.
    let aggregation = log_manager.reduce(&log_id).await?;
    assert_eq!(aggregation.aggregate().count, 1);

    // Append another event, this time a Decrement.
    log_manager.append(aggregation, &TestEvent::Decrement, &AppendOptions::default()).await?;

    // Re-reduce to apply the new event. The cached aggregate will
    // be used and only the new event will be fetched from the database.
    let aggregation = log_manager.reduce(&log_id).await?;
    assert_eq!(aggregation.aggregate().count, 0);

    Ok(())
}
```

## Idempotency
To ensure that log creation or an append happens only once, even if you
need to retry the operation due to a network timeout, supply a universally
unique idempotency key (the [uuid crate](https://docs.rs/uuid/latest/uuid/)
is handy for this).

```rust
let create_options = CreateOptions {
    idempotency_key: Some(uuid::Uuid::now_v7().to_string()),
    .. Default::default()
};

// pass create_options to log_manager.create() ...
// and test the `result` to determine if it's an idempotent replay error ...
# let result: Result<(), LogManagerError> = Ok(());
if let Err(LogManagerError::IdempotentReplay {log_id, ..}) = result {
    // a log was already created using that same idempotency key
    // and the log_id field of the error contains the log_id of
    // that previously-created log
}
```
