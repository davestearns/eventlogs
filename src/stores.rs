use crate::ids::LogId;
use crate::{AppendOptions, CreateOptions, EventRecord, IdempotentOutcome};
use futures_util::Stream;
use thiserror::Error;

pub mod postgres;

#[cfg(test)]
pub mod fake;

#[derive(Debug, Error)]
pub enum EventStoreError {
    #[error("log_id={log_id} already has an event_index={event_index}")]
    EventIndexAlreadyExists { log_id: LogId, event_index: u32 },
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
            (Self::DatabaseError { error: l_error }, Self::DatabaseError { error: r_error }) => {
                // strangely, std::error::Error doesn't implement PartialEq,
                // so the best we can do here is just compare the to_string()
                // representations for equality
                l_error.to_string() == r_error.to_string()
            }
            _ => false,
        }
    }
}

#[trait_variant::make(Send)]
pub trait EventStore<E>
where
    E: Send,
{
    async fn create(
        &self,
        first_event: &E,
        create_options: &CreateOptions,
    ) -> Result<LogId, EventStoreError>;

    async fn append(
        &self,
        log_id: &LogId,
        next_event: &E,
        event_index: u32,
        append_options: &AppendOptions,
    ) -> Result<IdempotentOutcome, EventStoreError>;

    async fn load(
        &self,
        log_id: &LogId,
        starting_index: u32,
    ) -> Result<impl Stream<Item = Result<impl EventRecord<E>, EventStoreError>>, EventStoreError>;
}
