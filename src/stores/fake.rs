use crate::ids::LogId;
use crate::stores::{EventStore, EventStoreError};
use crate::{AppendOptions, EventRecord};
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
        self.index
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
    idempotency_key_to_log_id: HashMap<String, (LogId, u32)>,
}

impl<E> Database<E> {
    pub fn new() -> Self {
        Self {
            log_id_to_events: HashMap::new(),
            idempotency_key_to_log_id: HashMap::new(),
        }
    }
}

/// A fake implementation of [EventStore] that should only be used for testing.
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
    async fn append(
        &self,
        log_id: &LogId,
        event: &E,
        event_index: u32,
        append_options: &AppendOptions,
    ) -> Result<(), EventStoreError> {
        let mut db = self.mx_db.lock().await;

        if let Some(events) = db.log_id_to_events.get(log_id) {
            if events.iter().any(|e| e.index == event_index) {
                return Err(EventStoreError::EventIndexAlreadyExists {
                    log_id: log_id.clone(),
                    event_index,
                });
            }
        }

        if let Some(ref key) = append_options.idempotency_key {
            if let Some((lid, idx)) = db.idempotency_key_to_log_id.get(key) {
                return Err(EventStoreError::IdempotentReplay {
                    idempotency_key: key.clone(),
                    log_id: lid.clone(),
                    event_index: *idx,
                });
            }
            db.idempotency_key_to_log_id
                .insert(key.clone(), (log_id.clone(), event_index));
        }

        let record = InternalEventRecord {
            index: event_index,
            recorded_at: Utc::now(),
            event: event.clone(),
        };

        match db.log_id_to_events.get_mut(log_id) {
            Some(vec) => {
                vec.push(record);
            }
            None => {
                db.log_id_to_events.insert(log_id.clone(), vec![record]);
            }
        };
        Ok(())
    }

    async fn load(
        &self,
        log_id: &LogId,
        starting_index: u32,
        max_events: u32,
    ) -> Result<impl Stream<Item = Result<impl EventRecord<E>, EventStoreError>>, EventStoreError>
    {
        let db = self.mx_db.lock().await;
        let events: Vec<Result<InternalEventRecord<E>, EventStoreError>> =
            if let Some(v) = db.log_id_to_events.get(log_id) {
                v.iter()
                    .filter(|e| e.index >= starting_index)
                    .take(max_events as usize)
                    .cloned()
                    .map(Ok)
                    .collect()
            } else {
                vec![]
            };
        Ok(futures_util::stream::iter(events))
    }
}

#[cfg(test)]
mod tests {
    use std::u32;

    use futures_util::StreamExt;
    use uuid::Uuid;

    use crate::tests::TestEvent;

    use super::*;

    #[tokio::test]
    async fn append_load() {
        let log_id = LogId::new();
        let store = FakeEventStore::<TestEvent>::new();
        store
            .append(&log_id, &TestEvent::Increment, 0, &AppendOptions::default())
            .await
            .unwrap();
        let row_stream = store.load(&log_id, 0, u32::MAX).await.unwrap();
        assert_eq!(row_stream.count().await, 1);
    }

    #[tokio::test]
    async fn append_multiple_load() {
        let log_id = LogId::new();
        let store = FakeEventStore::<TestEvent>::new();
        store
            .append(&log_id, &TestEvent::Increment, 0, &AppendOptions::default())
            .await
            .unwrap();

        store
            .append(&log_id, &TestEvent::Decrement, 1, &AppendOptions::default())
            .await
            .unwrap();

        let row_stream = store.load(&log_id, 0, u32::MAX).await.unwrap();
        assert_eq!(row_stream.count().await, 2);
    }

    #[tokio::test]
    async fn idempotent_create() {
        let log_id = LogId::new();
        let idempotency_key = Uuid::now_v7().to_string();
        let store = FakeEventStore::<TestEvent>::new();
        let options = AppendOptions {
            idempotency_key: Some(idempotency_key.clone()),
        };

        store
            .append(&log_id, &TestEvent::Increment, 0, &options)
            .await
            .unwrap();

        let replay_log_id = LogId::new();
        let result = store
            .append(&replay_log_id, &TestEvent::Increment, 0, &options)
            .await;

        assert_eq!(
            result,
            Err(EventStoreError::IdempotentReplay {
                idempotency_key: idempotency_key.clone(),
                log_id: log_id.clone(), // original log id, not replay log id
                event_index: 0
            })
        );
    }

    #[tokio::test]
    async fn concurrent_append() {
        let log_id = LogId::new();
        let store = FakeEventStore::<TestEvent>::new();
        store
            .append(&log_id, &TestEvent::Increment, 0, &AppendOptions::default())
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
        let log_id = LogId::new();
        let store = FakeEventStore::<TestEvent>::new();
        store
            .append(&log_id, &TestEvent::Increment, 0, &AppendOptions::default())
            .await
            .unwrap();

        let idempotency_key = Uuid::now_v7().to_string();
        let options = AppendOptions {
            idempotency_key: Some(idempotency_key.clone()),
        };

        store
            .append(&log_id, &TestEvent::Decrement, 1, &options)
            .await
            .unwrap();

        let result = store
            .append(&log_id, &TestEvent::Decrement, 2, &options)
            .await;

        assert_eq!(
            result,
            Err(EventStoreError::IdempotentReplay {
                idempotency_key: idempotency_key.clone(),
                log_id: log_id.clone(), // original log Id
                event_index: 1,         // original event index
            })
        );

        // ...and there should only be 2 events in the log
        let row_stream = store.load(&log_id, 0, u32::MAX).await.unwrap();
        assert_eq!(row_stream.count().await, 2);
    }

    #[tokio::test]
    async fn load_limit() {
        let log_id = LogId::new();
        let store = FakeEventStore::<TestEvent>::new();
        for i in 0..15 {
            store
                .append(&log_id, &TestEvent::Increment, i, &AppendOptions::default())
                .await
                .unwrap();
        }
        
        let event_stream = store.load(&log_id, 0, 10).await.unwrap();
        assert_eq!(event_stream.count().await, 10);
    }
}
