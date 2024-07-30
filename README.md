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
/// the enum variant. In this example, we will use a very
/// simple set of events that record credits and debits
/// to a single-currency account (amounts are in minor units).
#[derive(Debug, Clone)]
pub enum BalanceEvent {
    Credit { amount: u32 },
    Debit { amount: u32 },
}

/// An Aggregate is a simple struct with fields that get
/// calculated from the events recorded in the log. Note that
/// Aggregates must implement the Aggregate and Default traits,
/// but you can derive Default if your field types already
/// implement Default.
#[derive(Debug, Default, Clone)]
pub struct Account {
    pub balance: i64,
}

impl Aggregate for Account {
    type Event = BalanceEvent;
    fn apply(&mut self, event_record: &impl EventRecord<Self::Event>) {
        match event_record.event() {
            BalanceEvent::Credit { amount } => self.balance += amount as i64,
            BalanceEvent::Debit { amount } => self.balance -= amount as i64,
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
        FakeEventStore::<BalanceEvent>::new(),
        FakeReductionCache::<Account>::new()
    );
    
    // Say your service gets a request to create a new account with a starting
    // balance, but you want to ensure this happens only once, even if your 
    // caller gets a network timeout and retries their request. This is
    // easily supported by having your client provide a unique
    // idempotency key in the request, and using it in the AppendOptions.
    // If a log was already created with that same idempotency key, 
    // in a previous request, you will get an IdempotencyReplay error 
    // with the previously-created log ID.
    let caller_supplied_key = uuid::Uuid::now_v7().to_string();
    let options = AppendOptions {
        idempotency_key: Some(caller_supplied_key),
        ..Default::default()
    };
    let log_id = LogId::new();
    let starting_balance_event = BalanceEvent::Credit { amount: 100 };
    log_manager.create(&log_id, &starting_balance_event, &options).await?;

    // Now let's say our service gets another API request to debit the account,
    // but your rules say that accounts may not go negative.
    // So we need to reduce the log for this account to check the balance
    // before we add a the new debit event. The log_manager will also put
    // this reduction into the cache asynchronously, so it won't slow down
    // our code.
    let debit_amount: u32 = 10;
    let reduction = log_manager.reduce(&log_id).await?;
    assert!(reduction.aggregate().balance - (debit_amount as i64) >= 0);

    // We could use an idempotency key here as well, but we will
    // skip that to keep the code shorter.
    log_manager.append(
        &log_id,
        reduction,
        &BalanceEvent::Debit { amount: debit_amount }, 
        &AppendOptions::default())
        .await?;

    // If we reduce the log again, we will see that the balance has been
    // debited. But this time the log_manager will read the previous
    // reduction from the cache, and only apply the new events to it.
    let reduction = log_manager.reduce(&log_id).await?;
    assert_eq!(reduction.aggregate().balance, 90);

    // If you ever need to list all events in a given log, you can
    // fetch them in pages using the load() method:
    let starting_index = 0;
    let max_events = 100;
    let events = log_manager.load(&log_id, starting_index, max_events).await?;
    assert_eq!(events.len(), 2);

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
