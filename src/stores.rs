use crate::ids::LogId;
use crate::{AppendOptions, EventRecord};
use futures_util::Stream;
use thiserror::Error;

/// Fake implementations for testing purposes.
pub mod fake;

/// A [EventStore] backed by a Postgres database.
#[cfg(feature = "postgres-store")]
pub mod postgres;

/// The error type returned from [EventStore] methods.
#[derive(Debug, Error)]
pub enum EventStoreError {
    /// An event with the specified index already exists in the specified log.
    #[error("log_id={log_id} already has an event_index={event_index}")]
    EventIndexAlreadyExists { log_id: LogId, event_index: u32 },
    /// An event with the specified idempotency key already exists.
    #[error("an event with the provided idempotency key already exists")]
    IdempotentReplay {
        idempotency_key: String,
        log_id: LogId,
        event_index: u32,
    },
    /// An unexpected database or connectivity error occurred.
    #[error("unexpected database error: {error}")]
    DatabaseError {
        error: Box<dyn std::error::Error + Sync + Send>,
    },
}

impl PartialEq for EventStoreError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::EventIndexAlreadyExists {
                    log_id: l_log_id,
                    event_index: l_event_index,
                },
                Self::EventIndexAlreadyExists {
                    log_id: r_log_id,
                    event_index: r_event_index,
                },
            ) => l_log_id == r_log_id && l_event_index == r_event_index,
            (
                Self::IdempotentReplay {
                    idempotency_key: l_idempotency_key,
                    log_id: l_log_id,
                    event_index: l_event_index,
                },
                Self::IdempotentReplay {
                    idempotency_key: r_idempotency_key,
                    log_id: r_log_id,
                    event_index: r_event_index,
                },
            ) => {
                l_idempotency_key == r_idempotency_key
                    && l_log_id == r_log_id
                    && l_event_index == r_event_index
            }
            (Self::DatabaseError { error: l_error }, Self::DatabaseError { error: r_error }) => {
                l_error.to_string() == r_error.to_string()
            }
            _ => false,
        }
    }
}

/// The trait implemented by all event stores.
#[trait_variant::make(Send)]
pub trait EventStore<E>
where
    E: Send,
{
    /// Appends an event to a log at the specified index.
    ///
    /// If an event already exists with that same index, this returns an
    /// [EventStoreError::EventIndexAlreadyExists] error.
    ///
    /// If an idempotency key is provided in the options, and an event already
    /// exists with that same key, this will return an
    /// [EventStoreError::IdempotentReplay] error.

    async fn append(
        &self,
        log_id: &LogId,
        event: &E,
        event_index: u32,
        append_options: &AppendOptions,
    ) -> Result<(), EventStoreError>;

    /// Returns an asynchronous stream of [EventRecord]s from the specified
    /// log ID, starting at the specified event index. Events must be ordered
    /// by index in ascending order.
    async fn load(
        &self,
        log_id: &LogId,
        starting_index: u32,
    ) -> Result<impl Stream<Item = Result<impl EventRecord<E>, EventStoreError>>, EventStoreError>;
}
