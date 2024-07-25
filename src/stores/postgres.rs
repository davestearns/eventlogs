use crate::{
    ids::LogId,
    stores::{EventStore, EventStoreError},
    AppendOptions, CreateOptions, EventRecord,
};
use chrono::Utc;
use const_format::formatcp;
use deadpool_postgres::{GenericClient, Pool, PoolError};
use futures_util::TryStreamExt;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, marker::PhantomData};
use tokio_postgres::{
    error::SqlState,
    types::{FromSql, Json, ToSql, Type},
    Error, Row,
};

const SCHEMA_NAME: &str = "eventlogs";
const TABLE_NAME: &str = "events";
const QUALIFIED_TABLE_NAME: &str = formatcp!("{SCHEMA_NAME}.{TABLE_NAME}");
const PK_CONSTRAINT: &str = formatcp!("{TABLE_NAME}_pkey");
const IDEMPOTENCY_KEY_CONSTRAINT: &str = "idempotency_key_unique";
const COLUMN_LIST: &str = "log_id,event_index,recorded_at,idempotency_key,payload";
const SELECT_EVENTS: &str = formatcp!(
    "select {COLUMN_LIST} from {QUALIFIED_TABLE_NAME} 
    where log_id = $1 and event_index >= $2
    order by log_id, event_index"
);
const INSERT_EVENT: &str =
    formatcp!("insert into {QUALIFIED_TABLE_NAME} ({COLUMN_LIST}) values ($1,$2,$3,$4,$5)");
const SELECT_EVENT_FOR_IDEMPOTENCY_KEY: &str =
    formatcp!("select log_id, event_index from {QUALIFIED_TABLE_NAME} where idempotency_key=$1");

impl From<PoolError> for EventStoreError {
    fn from(value: PoolError) -> Self {
        EventStoreError::DatabaseError {
            error: Box::new(value),
        }
    }
}

impl From<Error> for EventStoreError {
    fn from(value: Error) -> Self {
        EventStoreError::DatabaseError {
            error: Box::new(value),
        }
    }
}

impl<'a> FromSql<'a> for LogId {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let s = String::from_utf8_lossy(raw);
        let log_id: LogId = s.parse()?;
        Ok(log_id)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty, &Type::VARCHAR | &Type::CHAR)
    }
}

pub trait PostgresEvent: Serialize + DeserializeOwned + Debug + Send + Sync {}
impl<T> PostgresEvent for T where T: Serialize + DeserializeOwned + Debug + Send + Sync {}

impl<E> EventRecord<E> for Row
where
    E: PostgresEvent,
{
    fn index(&self) -> u32 {
        self.get("event_index")
    }

    fn recorded_at(&self) -> chrono::DateTime<Utc> {
        self.get("recorded_at")
    }

    fn event(&self) -> E {
        self.get::<_, Json<E>>("payload").0
    }
}

pub struct PostgresEventStore<E> {
    pool: Pool,
    _phantom_e: PhantomData<E>,
}

impl<E> PostgresEventStore<E>
where
    E: PostgresEvent,
{
    pub fn new(pool: Pool) -> Self {
        PostgresEventStore {
            pool,
            _phantom_e: PhantomData,
        }
    }

    async fn insert(
        &self,
        log_id: &LogId,
        event: &E,
        event_index: u32,
        idempotency_key: &Option<String>,
    ) -> Result<(), EventStoreError> {
        let conn = self.pool.get().await?;
        let stmt = conn.prepare_cached(INSERT_EVENT).await?;
        let result = conn
            .execute(
                &stmt,
                &[
                    &log_id.to_string(),
                    &event_index,
                    &Utc::now(),
                    &idempotency_key,
                    &Json(event),
                ],
            )
            .await;

        // If there was a unique constraint violation,
        // return th appropriate EventStoreError
        if let Err(ref e) = result {
            if e.code() == Some(&SqlState::UNIQUE_VIOLATION) {
                if let Some(dbe) = e.as_db_error() {
                    if dbe.constraint() == Some(PK_CONSTRAINT) {
                        return Err(EventStoreError::EventIndexAlreadyExists {
                            log_id: log_id.clone(),
                            event_index,
                        });
                    }
                    if dbe.constraint() == Some(IDEMPOTENCY_KEY_CONSTRAINT) {
                        // If we got a duplicate idempotency key error, idempotency_key
                        // should have Some value, but just to be safe...
                        if let Some(key) = idempotency_key {
                            // Find the event with that idempotency key
                            let query = conn
                                .prepare_cached(SELECT_EVENT_FOR_IDEMPOTENCY_KEY)
                                .await?;
                            let row = conn.query_one(&query, &[&key]).await?;
                            return Err(EventStoreError::IdempotentReplay {
                                idempotency_key: key.clone(),
                                log_id: row.get("log_id"),
                                event_index: row.get("event_index"),
                            });
                        }
                    }
                }
            }
        }

        Ok(result.map(|_| ())?)
    }
}

impl<E> EventStore<E> for PostgresEventStore<E>
where
    E: PostgresEvent,
{
    async fn create(
        &self,
        log_id: &LogId,
        first_event: &E,
        create_options: &CreateOptions,
    ) -> Result<(), EventStoreError> {
        self.insert(log_id, first_event, 0, &create_options.idempotency_key)
            .await
    }

    async fn append(
        &self,
        log_id: &LogId,
        next_event: &E,
        event_index: u32,
        append_options: &AppendOptions,
    ) -> Result<(), EventStoreError> {
        self.insert(
            log_id,
            next_event,
            event_index,
            &append_options.idempotency_key,
        )
        .await
    }

    async fn load(
        &self,
        log_id: &LogId,
        starting_index: u32,
    ) -> Result<
        impl futures_util::Stream<Item = Result<impl EventRecord<E>, EventStoreError>>,
        EventStoreError,
    > {
        let conn = self.pool.get().await?;
        let stmt = conn.prepare_cached(SELECT_EVENTS).await?;

        let log_id_param = log_id.to_string();
        let params: Vec<&(dyn ToSql + Sync)> = vec![&log_id_param, &starting_index];
        let row_stream = conn.query_raw(&stmt, params).await?;

        Ok(row_stream.map_err(|e| EventStoreError::DatabaseError { error: Box::new(e) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::TestEvent;
    use deadpool_postgres::Config;
    use deadpool_redis::Runtime;
    use futures_util::StreamExt;
    use tokio_postgres::NoTls;
    use uuid::Uuid;

    fn store() -> PostgresEventStore<TestEvent> {
        let mut cfg = Config::new();
        cfg.host = Some("localhost".to_string());
        cfg.user = Some("postgres".to_string());
        cfg.password = Some("ci-postgres-password".to_string());
        cfg.dbname = Some("postgres".to_string());
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
        PostgresEventStore::new(pool)
    }

    #[tokio::test]
    async fn create_load() {
        let log_id = LogId::new();
        let store = store();
        store
            .create(&log_id, &TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        let row_stream = store.load(&log_id, 0).await.unwrap();
        assert_eq!(row_stream.count().await, 1);
    }

    #[tokio::test]
    async fn create_append_load() {
        let log_id = LogId::new();
        let store = store();
        store
            .create(&log_id, &TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        store
            .append(&log_id, &TestEvent::Decrement, 1, &AppendOptions::default())
            .await
            .unwrap();

        store
            .append(&log_id, &TestEvent::Increment, 2, &AppendOptions::default())
            .await
            .unwrap();

        let row_stream = store.load(&log_id, 0).await.unwrap();
        assert_eq!(row_stream.count().await, 3);
    }

    #[tokio::test]
    async fn idempotent_create() {
        let log_id = LogId::new();
        let store = store();
        let idempotency_key = Uuid::now_v7().to_string();
        let create_options = CreateOptions {
            idempotency_key: Some(idempotency_key.clone()),
            ..Default::default()
        };

        store
            .create(&log_id, &TestEvent::Increment, &create_options)
            .await
            .unwrap();

        let replay_log_id = LogId::new();
        let result = store
            .create(&replay_log_id, &TestEvent::Increment, &create_options)
            .await;

        assert_eq!(
            result,
            Err(EventStoreError::IdempotentReplay {
                idempotency_key: idempotency_key.clone(),
                log_id: log_id.clone(), // original log id, not replay log id
                event_index: 0
            })
        )
    }

    #[tokio::test]
    async fn concurrent_append() {
        let log_id = LogId::new();
        let store = store();
        store
            .create(&log_id, &TestEvent::Increment, &CreateOptions::default())
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
        )
    }

    #[tokio::test]
    async fn idempotent_append() {
        let log_id = LogId::new();
        let store = store();
        let idempotency_key = Uuid::now_v7().to_string();
        let append_options = AppendOptions {
            idempotency_key: Some(idempotency_key.clone()),
            ..Default::default()
        };

        store
            .create(&log_id, &TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        store
            .append(&log_id, &TestEvent::Decrement, 1, &append_options)
            .await
            .unwrap();

        let result = store
            .append(&log_id, &TestEvent::Decrement, 2, &append_options)
            .await;

        assert_eq!(
            result,
            Err(EventStoreError::IdempotentReplay {
                idempotency_key: idempotency_key.clone(),
                log_id: log_id.clone(), // original log id
                event_index: 1          // original event index
            })
        );

        //ensure this log only has 2 events
        let row_stream = store.load(&log_id, 0).await.unwrap();
        assert_eq!(row_stream.count().await, 2);
    }
}
