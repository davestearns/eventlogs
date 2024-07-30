# High-Performance, Batteries-Included, Event Sourcing for Rust

[![CI](https://github.com/davestearns/eventlogs/actions/workflows/ci.yml/badge.svg)](https://github.com/davestearns/eventlogs/actions/workflows/ci.yml)

This crate supports a style of transaction processing known as ["event sourcing."](https://martinfowler.com/eaaDev/EventSourcing.html) Instead of storing a single mutable record that is updated as the entity's state changes, event-sourcing systems record a series of immutable events about each entity, and reduce those events into a current state (known as an "aggregate") as needed. The event log for an entity provides a complete audit trail and makes it easier to record distinct properties about events that may occur multiple times (e.g., a task that is closed and re-opened multiple times).

**Caution:** The crate is functional and tested, but hasn't been used in production yet, so use at your own risk! If you'd like to do a pilot, create a [tracking issue](https://github.com/davestearns/eventlogs/issues) on GitHub and I'll gladly help you.

## Built-In Features

- **Idempotency:** When creating a new log or appending an event to an existing one, the caller can include a unique `idempotency_key` that ensures the operation occurs only once, even if the request is retried. Idempotent replays will return a
`IdempotentReplay` error with the previously-recorded `LogId` and event index, so that you can easily detect and react to them.
- **Concurrency:** If multiple service instances attempt to append a new event to the same log at the same time, only one will win the race, and the others will receive an error. The losers can then re-reduce the log to apply the new event to the aggregate, determine if their operation is still relevant, and try again.
- **Async Aggregate Caching:** When you reduce a log, the resulting aggregate is written asynchronously to a cache like Redis. Subsequent calls to `reduce()` will reuse that cached aggregate, and only fetch/apply events that were recorded _after_ the aggregate was last calculated. This makes subsequent reductions faster without slowing down your code.
- **Caching Policies:** Aggregates are always cached by default, but if you want to control when this occurs based on aggregate properties, you can provide an implementation of `CachingPolicy`, which will be called by the asynchronous caching task to determine if the aggregate should be written to the cache.

## Example Usage
```rust
use std::error::Error;
use eventlogs::{LogId, LogManager, LogManagerOptions, CreateOptions,
    AppendOptions, Aggregate, EventRecord};
use eventlogs::stores::fake::FakeEventStore;
use eventlogs::caches::fake::FakeReductionCache;

/// Events are typically defined as members of an enum.
/// Properties for events can be defined as fields on
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
    // To begin, create a LogManager, passing the event store and
    // reduction cache you want to use. This example uses testing fakes,
    // but you would use PostgresEventStore RedisReductionCache, configured
    // to point to your servers/clusters.
    let log_manager = LogManager::new(
        FakeEventStore::<TestEvent>::new(),
        FakeReductionCache::<TestAggregate>::new()
    );
    
    // Create a new log with an Increment event as the first event.
    // To ensure that the log is created only once, even if you or
    // your caller has to retry after a network timeout, use an
    // idempotency key in the CreateOptions. If a log was already
    // created with that same idempotency key, you will get an
    // IdempotencyReplay error with the previously-created log ID.
    let create_options = CreateOptions {
        idempotency_key: Some(uuid::Uuid::now_v7().to_string()),
        ..Default::default()
    };
    let log_id = LogId::new();
    log_manager.create(&log_id, &TestEvent::Increment, &create_options).await?;

    // Now let's say our service gets another request. In order to process
    // it, we need to know the current state of the transaction (the Aggregate).
    // Use the reduce() method to reduce the events into an Reduction, which
    // contains our TestAggregate with current state. The Reduction will be
    // cached automatically by a background task, so it won't slow down our
    // main code.
    let reduction = log_manager.reduce(&log_id).await?;
    assert_eq!(reduction.aggregate().count, 1);

    // Let's say that the Aggregate's current state allows the operation, and
    // this time we want to append a Decrement event.
    // If another process is racing with this one, only one will
    // successfully append, and the other will get a ConcurrentAppend
    // error. And idempotency keys may also be provided in the AppendOptions.
    log_manager.append(&log_id, reduction, &TestEvent::Decrement, &AppendOptions::default()).await?;

    // Re-reduce: the cached aggregate will automatically be used as
    // the starting point, so that we only have to select the events with
    // higher indexes than the `through_index()` on the cached reduction.
    let reduction = log_manager.reduce(&log_id).await?;
    assert_eq!(reduction.aggregate().count, 0);

    Ok(())
}
```
## Cargo Features

This crate defines the following Cargo/compiler features:

| Name | Description | Default? |
|------|-------------|----------|
| postgres-store | Enables the PostgresEventStore | Yes |
| redis-cache | Enables the RedisReductionCache | Yes |

Since Postgres and Redis are very common choices, these features
are on by default. As more `EventStore` and `ReductionCache`
implementations are added in the future, corresponding non-default
features will be defined.
