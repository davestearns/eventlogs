use crate::ids::LogId;
use crate::stores::{EventStore, EventStoreError};
use crate::{AppendOptions, CreateOptions, EventRecord, IdempotentOutcome};
use chrono::{DateTime, Utc};
use futures_util::Stream;
use std::collections::HashMap;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct InternalEventRecord<E> {
    index: u32,
    recorded_at: DateTime<Utc>,
    event: E,
}

impl<E> EventRecord<E> for InternalEventRecord<E>
where
    E: Clone + Send + Sync,
{
    fn index(&self) -> u32 {
        self.index as u32
    }

    fn recorded_at(&self) -> DateTime<Utc> {
        self.recorded_at
    }

    fn event(&self) -> E {
        self.event.clone()
    }
}

#[derive(Debug)]
struct Database<E> {
    log_id_to_events: HashMap<LogId, Vec<InternalEventRecord<E>>>,
    idempotency_key_to_log_id: HashMap<String, LogId>,
}

impl<E> Database<E> {
    pub fn new() -> Self {
        Self {
            log_id_to_events: HashMap::new(),
            idempotency_key_to_log_id: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct FakeEventStore<E> {
    mx_db: Mutex<Database<E>>,
}

impl<E> FakeEventStore<E>
where
    E: Clone + Send + Sync,
{
    pub fn new() -> Self {
        Self {
            mx_db: Mutex::new(Database::new()),
        }
    }
}

impl<E> Default for FakeEventStore<E>
where
    E: Clone + Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> EventStore<E> for FakeEventStore<E>
where
    E: Clone + Send + Sync + 'static,
{
    async fn create(
        &self,
        first_event: &E,
        create_options: &CreateOptions,
    ) -> Result<(LogId, IdempotentOutcome), EventStoreError> {
        let log_id = LogId::new();
        let mut db = self.mx_db.lock().await;

        if let Some(ref key) = create_options.idempotency_key {
            if let Some(lid) = db.idempotency_key_to_log_id.get(key) {
                return Ok((lid.clone(), IdempotentOutcome::Replay));
            }
            db.idempotency_key_to_log_id
                .insert(key.clone(), log_id.clone());
        }

        let record = InternalEventRecord {
            index: 0,
            recorded_at: Utc::now(),
            event: first_event.clone(),
        };

        db.log_id_to_events.insert(log_id.clone(), vec![record]);

        Ok((log_id, IdempotentOutcome::New))
    }

    async fn append(
        &self,
        log_id: &LogId,
        next_event: &E,
        event_index: u32,
        append_options: &AppendOptions,
    ) -> Result<IdempotentOutcome, EventStoreError> {
        let mut db = self.mx_db.lock().await;

        if db
            .log_id_to_events
            .get(log_id)
            .expect("log id not found")
            .iter()
            .any(|e| e.index == event_index)
        {
            return Err(EventStoreError::EventIndexAlreadyExists {
                log_id: log_id.clone(),
                event_index,
            });
        }

        if let Some(ref key) = append_options.idempotency_key {
            if db.idempotency_key_to_log_id.contains_key(key) {
                return Ok(IdempotentOutcome::Replay);
            }
            db.idempotency_key_to_log_id
                .insert(key.clone(), log_id.clone());
        }

        let record = InternalEventRecord {
            index: event_index,
            recorded_at: Utc::now(),
            event: next_event.clone(),
        };

        db.log_id_to_events.get_mut(log_id).unwrap().push(record);
        Ok(IdempotentOutcome::New)
    }

    async fn load(
        &self,
        log_id: &LogId,
        starting_index: u32,
    ) -> Result<impl Stream<Item = Result<impl EventRecord<E>, EventStoreError>>, EventStoreError>
    {
        let db = self.mx_db.lock().await;
        let events: Vec<Result<InternalEventRecord<E>, EventStoreError>> =
            if let Some(v) = db.log_id_to_events.get(log_id) {
                v.iter()
                    .filter(|e| e.index >= starting_index)
                    .map(|e| Ok(e.clone()))
                    .collect()
            } else {
                vec![]
            };
        Ok(futures_util::stream::iter(events))
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use uuid::Uuid;

    use crate::tests::TestEvent;

    use super::*;

    #[tokio::test]
    async fn create_load() {
        let store = FakeEventStore::<TestEvent>::new();
        let (log_id, _) = store
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();
        let row_stream = store.load(&log_id, 0).await.unwrap();
        assert_eq!(row_stream.count().await, 1);
    }

    #[tokio::test]
    async fn create_append_load() {
        let store = FakeEventStore::<TestEvent>::new();
        let (log_id, _) = store
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        store
            .append(&log_id, &TestEvent::Decrement, 1, &AppendOptions::default())
            .await
            .unwrap();

        let row_stream = store.load(&log_id, 0).await.unwrap();
        assert_eq!(row_stream.count().await, 2);
    }

    #[tokio::test]
    async fn idempotent_create() {
        let store = FakeEventStore::<TestEvent>::new();
        let create_options = CreateOptions {
            idempotency_key: Some(Uuid::now_v7().to_string()),
        };

        let (log_id, outcome) = store
            .create(&TestEvent::Increment, &create_options)
            .await
            .unwrap();
        assert_eq!(outcome, IdempotentOutcome::New);

        let (replay_log_id, replay_outcome) = store
            .create(&TestEvent::Increment, &create_options)
            .await
            .unwrap();

        assert_eq!(replay_log_id, log_id);
        assert_eq!(replay_outcome, IdempotentOutcome::Replay);
    }

    #[tokio::test]
    async fn concurrent_append() {
        let store = FakeEventStore::<TestEvent>::new();
        let (log_id, _) = store
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        store
            .append(&log_id, &TestEvent::Decrement, 1, &AppendOptions::default())
            .await
            .unwrap();

        let result = store
            .append(&log_id, &TestEvent::Decrement, 1, &AppendOptions::default())
            .await;

        assert_eq!(
            result,
            Err(EventStoreError::EventIndexAlreadyExists {
                log_id: log_id,
                event_index: 1
            })
        );
    }

    #[tokio::test]
    async fn idempotent_append() {
        let store = FakeEventStore::<TestEvent>::new();
        let (log_id, _) = store
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        let append_options = AppendOptions {
            idempotency_key: Some(Uuid::now_v7().to_string()),
        };

        store
            .append(&log_id, &TestEvent::Decrement, 1, &append_options)
            .await
            .unwrap();
        // This should succeed, but be a no-op since a previous event was already appended
        // with the same idempotency key...
        let outcome = store
            .append(&log_id, &TestEvent::Decrement, 2, &append_options)
            .await
            .unwrap();
        assert_eq!(outcome, IdempotentOutcome::Replay);

        // ...and there should only be 2 events in the log
        let row_stream = store.load(&log_id, 0).await.unwrap();
        assert_eq!(row_stream.count().await, 2);
    }
}
