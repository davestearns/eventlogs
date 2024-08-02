# High-Performance, Batteries-Included, Event Sourcing for Rust

[![CI](https://github.com/davestearns/eventlogs/actions/workflows/ci.yml/badge.svg)](https://github.com/davestearns/eventlogs/actions/workflows/ci.yml)
[![Crates.io Version](https://img.shields.io/crates/v/eventlogs)](https://crates.io/crates/eventlogs)
[![Documentation](https://docs.rs/eventlogs/badge.svg)](https://docs.rs/eventlogs)

This crate supports a style of transaction processing known as ["event sourcing."](https://martinfowler.com/eaaDev/EventSourcing.html) That name is rather opaque, but the basic idea is quite simple: instead of storing mutable records that get updated as state changes, event-sourcing systems store a series of immutable events describing those state changes. When the system needs to know the state of a given entity, it selects the events related to that entity and [reduces (aka folds)](https://en.wikipedia.org/wiki/Fold_(higher-order_function)) them into an "aggregate," which is the current state of that entity. In other words, the current state of a transaction is actually _calculated_ from the events recorded about that transaction.

Aggregates can be cached aggressively because the events they were calculated from are immutable, and new events are always append to the end of the log. A cached aggregate can also be quickly updated by selecting and applying only the events that were recorded _after_ it was created.

This approach provides not only a full audit trail for how a given entity's state ended up the way it did, but also an easy way to record events that can happen multiple times to the same entity. For example, a payment may be partially captured or refunded several times, but each of those events will have their own distinct properties for the monetary amounts and reference numbers.

The drawback of this approach is that is makes listing and querying of entities more complex: if you store individual events and calculate the overall state, how do you quickly find all payments that have been fully refunded? Most event-sourcing systems handle this by sending these sorts of queries to a separate, highly-indexed database containing aggregate snapshots. These aggregates are typically recalculated and updated asynchronously in response to new events written to the transaction-processing database. They are eventually-consistent, but listing/querying APIs are often that way in large distributed systems. This also keeps the writes to the transaction-processing database very fast since those are just inserts to a minimally-indexed table. This division of labor is typically called ["Command and Query Responsibility Separation"](https://learn.microsoft.com/en-us/azure/architecture/patterns/cqrs) or CQRS for short.

> ⚠️ **Caution:** The crate is functional and tested, but hasn't been used in production yet, so use at your own risk! If you'd like to do a pilot, create a [tracking issue](https://github.com/davestearns/eventlogs/issues) on GitHub and I'll gladly help you.

## Built-In Features

This is a batteries-included library that offers features one typically needs in a high-throughput distributed system:

- **Idempotency:** When creating a new log or appending an event to an existing one, the caller can include a unique `idempotency_key` that ensures the operation occurs only once, even if the request is retried. Idempotent replays will return an `IdempotentReplay` error with the previously-recorded `LogId` and event index, so that you can easily detect and react to them appropriately.
- **Optimistic Concurrency:** If multiple service instances attempt to append a new event to the same log at the same time, only one will win the race, and the others will receive an error. The losers can then re-reduce the log to see the effect of the new event on the aggregate, determine if their operation is still relevant, and try again.
- **Async Aggregate Caching:** When you reduce a log, the resulting aggregate is written asynchronously to a cache like Redis. Subsequent calls to `reduce()` will reuse that cached aggregate, and only fetch/apply events that were recorded _after_ the aggregate was last calculated. This makes subsequent reductions faster without slowing down your code.
- **Caching Policies:** Aggregates are always cached by default, but if you want to control when this occurs based on aggregate properties, you can provide an implementation of `CachingPolicy`. For example, if the state of the aggregates tells you that it will never be loaded again, you can skip caching it.
- **Event Streaming and Paging:** When reducing, events are asynchronously streamed from the database instead of buffered to limit the amount of memory consumed. But the library also offers a convenience method you can use to get a page of events at a time as a `Vector`, which makes it easier to return them as a JSON array from your service's API.

## Example Usage
```rust
use std::error::Error;
use eventlogs::{LogId, LogManager, LogManagerOptions,
    AppendOptions, Aggregate, EventRecord};
use eventlogs::stores::fake::FakeEventStore;
use eventlogs::caches::fake::FakeReductionCache;
use serde::{Serialize, Deserialize};

// A typical application of event-sourcing is the tracking of payments.
// A payment is really a series of events: authorization, increment,
// reversal, capture, clearing, refund, dispute, etc. Most events
// can occur several times, but each must capture distinct properties
// (e.g., the amount refunded). The overall state of the payment can 
// then be reduced from these events.

// Let's start by defining a struct to hold the initial payment request
// properties, which would include details about the card, cardholder,
// amount requested, etc. 
// To keep things simple, amounts will be tracked in minor units with an 
// assumed single currency.
#[derive(Debug, Default, PartialEq, Clone, Serialize)]
pub struct PaymentRequest {
    amount_requested: isize,
    // ... lots of other details ...
}

// Now let's define our events, which are typically variants in an enum.
// Since this is just an example, we'll define only a subset with only
// the most relevant properties. Timestamps are added automatically
// by this crate, so we don't need to define them in each event.
#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum PaymentEvent {
    Requested {
        request: PaymentRequest,
    },
    Authorized {
        amount: isize,
        approval_code: Option<String>,
    },
    Captured {
        amount: isize,
        statement_descriptor: String,
    },
    Refunded {
        amount: isize,
        reference_number: String,
    },
}

// Now let's define the "aggregate" for these events, which is the overall
// state of the payment. This is what we will reduce from the events,
// and use to decide if the current API request or operation is allowable.
// Aggregates must implement/derive Default, and implement Aggregate.
#[derive(Debug, Default, PartialEq, Clone, Serialize)]
pub struct Payment {
    request: PaymentRequest,
    amount_approved: isize,
    amount_captured: isize,
    amount_refunded: isize,
}

impl Payment {
    pub fn amount_outstanding(&self) -> isize {
        self.amount_approved - self.amount_captured - self.amount_refunded
    }
}

// To make Payment an aggregate, implement the Aggregate trait, which 
// adds a method for applying each event to the aggregate's current state.
impl Aggregate for Payment {
    type Event = PaymentEvent;

    fn apply(&mut self, event_record: &impl EventRecord<Self::Event>) {
        match event_record.event() {
            PaymentEvent::Requested { request } => { 
                // If you don't want to clone, you could use 
                // std::mem::take() with a mutable `request`
                // as the event isn't written back to the database.
                self.request = request.clone() 
            }
            PaymentEvent::Authorized { amount, .. } => self.amount_approved += amount,
            PaymentEvent::Captured { amount, .. } => self.amount_captured += amount,
            PaymentEvent::Refunded { amount, .. } => self.amount_refunded += amount,
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
        FakeEventStore::<PaymentEvent>::new(),
        FakeReductionCache::<Payment>::new(),
    );
    
    // Let's say we get an API request to create a new payment. We start
    // by creating a universally-unique log ID to track events related
    // to this payment. This will be serialized to a URL-safe string in
    // your API responses.
    let payment_id = LogId::new();

    // Then we create a new log, appending the Requested event.
    // To ensure this happens only once, even if your caller gets a network
    // timeout and retries their API request, have them pass a universally
    // unique idempotency key with their payment creation request. The
    // library will use that to ensure this log gets created only once.
    // See the LogManagerError::IdempotentReplay error for more details.
    let idempotency_key_from_caller = uuid::Uuid::now_v7().to_string();
    let options = AppendOptions {
        idempotency_key: Some(idempotency_key_from_caller),
        ..Default::default()
    };
    let req_event = PaymentEvent::Requested {
        request: PaymentRequest { amount_requested: 10000 },
    };
    let log_state = log_manager.create(&payment_id, &req_event, &options).await?;

    // We then talk to the payment gateway, and get an approved authorization...
    let auth_event = PaymentEvent::Authorized {
        amount: 10000,
        approval_code: Some("xyz123".to_string()),
    };
    
    // Append the authorized event to the log. You can use an idempotency key here
    // as well, but if you don't want to use them, just pass AppendOptions::default().
    // We pass the `log_state` that was returned from create() so that the library can
    // do optimistic concurrency and detect race conditions. If multiple processes try
    // to append to the same log at the same time, only one process will win, and the 
    // others will get a ConcurrentAppend error.
    log_manager.append(
        &payment_id, 
        log_state, 
        &auth_event, 
        &AppendOptions::default()).await?;

    // Now let's assume we shipped one of the items in the customer's order, so 
    // we get an API request to capture some of the payment. To know if this
    // is a valid request, we first reduce the log into a Payment to see if
    // the amount_outstanding is positive. The reduction will be automatically
    // cached asynchronously when we do this, so the next time we reduce, it
    // will only need to fetch new events and apply them.
    let reduction = log_manager.reduce(&payment_id).await?;
    assert!(reduction.aggregate().amount_outstanding() > 0);

    // Looks like we can do the capture, so let's record that event.
    let capture_event = PaymentEvent::Captured {
        amount: 4000,
        statement_descriptor: "Widgets, Inc".to_string(),
    };

    // You can pass a reduction instead of a log_state as the second argument
    // if you recently reduced the log. This again helps the library do optimistic
    // concurrency to detect race conditions. The reduction is consumed here since
    // it shouldn't be treated as a current reduction after this call, regardless 
    // of the Result returned.
    log_manager.append(
        &payment_id, 
        reduction, 
        &capture_event, 
        &AppendOptions::default()).await?;

    // Now if we reduce the log again, we should see the affect of the Capture.
    // This will use the cached reduction from before, and select/apply only 
    // the events that were appended after that reduction was created.
    let reduction = log_manager.reduce(&payment_id).await?;
    let payment = reduction.aggregate();
    assert_eq!(payment.amount_approved, 10000);
    assert_eq!(payment.amount_captured, 4000);
    assert_eq!(payment.amount_refunded, 0);
    assert_eq!(payment.amount_outstanding(), 6000);

    // Now let's assume the customer changed their mind and canceled the other
    // item in their order, so we need to refund that amount.
    let refund_event = PaymentEvent::Refunded {
        amount: 6000,
        reference_number: "abc789".to_string(),
    };
    log_manager.append(
        &payment_id, 
        reduction, 
        &refund_event, 
        &AppendOptions::default()).await?;

    // When we reduce, we should see that the amount outstanding is now zero.
    let reduction = log_manager.reduce(&payment_id).await?;
    let payment = reduction.aggregate();
    assert_eq!(payment.amount_approved, 10000);
    assert_eq!(payment.amount_captured, 4000);
    assert_eq!(payment.amount_refunded, 6000);
    assert_eq!(payment.amount_outstanding(), 0);

    // If you want to expose the raw events to your caller or
    // on show them on an admin page, you can get them a page 
    // at a time from the load() method. If you ask for one more 
    // than your page size, you'll know if there are more pages!
    let events = log_manager.load(&payment_id, 0, 101).await?;
    // In our case there should be only 4
    assert_eq!(events.len(), 4);
    let json = serde_json::to_string(&events)?;

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
