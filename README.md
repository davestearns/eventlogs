# High-Performance, Batteries-Included, Event Sourcing for Rust

[![CI](https://github.com/davestearns/eventlogs/actions/workflows/ci.yml/badge.svg)](https://github.com/davestearns/eventlogs/actions/workflows/ci.yml)

This crate supports a style of transaction processing known as ["event sourcing."](https://martinfowler.com/eaaDev/EventSourcing.html) That name is rather opaque, but the basic idea is quite simple: instead of storing mutable records that get updated as state changes, event-sourcing systems store a series of immutable events recording those state changes. When the system needs to know the state of any given entity, it selects the events related to that entity and [reduces (aka folds)](https://en.wikipedia.org/wiki/Fold_(higher-order_function)) them into an "aggregate," which is the current state of that entity. That is, the current state of the transaction is actually _calculated_ from the events, as they already capture everything that happened. 

The aggregate can then be cached aggressively because the events it was calculated from are immutable, and new events are always append to the end of the log. A cached aggregate can be quickly updated by selecting and applying only the events that were recorded _after_ the last reduction.

This approach provides not only a full audit trail for how a given entity's state ended up the way it did, but also an easy way to record events that can happen multiple times to the same entity. For example, a payment may be partially captured or refunded several times, but each of those events will have their own distinct properties for the monetary amounts and reference numbers.

The drawback of this approach is that is makes listing and querying of entities more complex: if you store individual events and calculate the overall state, how do you quickly find all payments that have been fully refunded? Most event-sourcing systems handle this by sending these sorts of queries to a separate, highly-indexed database containing the aggregates. These aggregates are typically recalculated and updated asynchronously in response to new events written to the transaction-processing database, so they are eventually-consistent, but listing/querying APIs are often that way in large distributed systems. This also keeps the writes to the transaction-processing database very fast since those are just inserts to a minimally-indexed table.

> ⚠️ **Caution:** The crate is functional and tested, but hasn't been used in production yet, so use at your own risk! If you'd like to do a pilot, create a [tracking issue](https://github.com/davestearns/eventlogs/issues) on GitHub and I'll gladly help you.

## Built-In Features

This is a batteries-included library that offers features one typically needs in a high-throughput distributed system:

- **Idempotency:** When creating a new log or appending an event to an existing one, the caller can include a unique `idempotency_key` that ensures the operation occurs only once, even if the request is retried. Idempotent replays will return a
`IdempotentReplay` error with the previously-recorded `LogId` and event index, so that you can easily detect and react to them appropriately.
- **Concurrency:** If multiple service instances attempt to append a new event to the same log at the same time, only one will win the race, and the others will receive an error. The losers can then re-reduce the log to see the effect of the new event on the aggregate, determine if their operation is still relevant, and try again.
- **Async Aggregate Caching:** When you reduce a log, the resulting aggregate is written asynchronously to a cache like Redis. Subsequent calls to `reduce()` will reuse that cached aggregate, and only fetch/apply events that were recorded _after_ the aggregate was last calculated. This makes subsequent reductions faster without slowing down your code.
- **Caching Policies:** Aggregates are always cached by default, but if you want to control when this occurs based on aggregate properties, you can provide an implementation of `CachingPolicy`. For example, if the state of the aggregates tells you that it will never be loaded again, you can skip caching it.
- **Event Streaming and Paging:** When reducing, events are asynchronously streamed from the database instead of buffered to limit the amount of memory consumed. But the library also offers a method you can use to get a page of events at a time as a `Vector`, which makes it easier to return them as a JSON array from your service's API.

## Example Usage
```rust
use std::error::Error;
use eventlogs::{LogId, LogManager, LogManagerOptions,
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
    let options = AppendOptions {
        idempotency_key: Some(uuid::Uuid::now_v7().to_string()),
        ..Default::default()
    };
    let log_id = LogId::new();
    log_manager.create(&log_id, &TestEvent::Increment, &options).await?;

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
