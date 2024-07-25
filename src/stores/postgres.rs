use crate::{
    ids::LogId,
    stores::{EventStore, EventStoreError},
    AppendOptions, CreateOptions, EventRecord, IdempotentOutcome,
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
    "select {COLUMN_LIST} from {QUALIFIED_TABLE_NAME} where log_id = $1 and event_index >= $2"
);
const INSERT_EVENT: &str =
    formatcp!("insert into {QUALIFIED_TABLE_NAME} ({COLUMN_LIST}) values ($1,$2,$3,$4,$5)");
const SELECT_LOG_ID_FOR_IDEMPOTENCY_KEY: &str =
    formatcp!("select log_id from {QUALIFIED_TABLE_NAME} where idempotency_key=$1");

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

impl<E> EventRecord<E> for Row
where
    E: Serialize + DeserializeOwned + Debug + Send,
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
    E: Serialize + DeserializeOwned + Debug + Send + Sync,
{
    pub fn new(pool: Pool) -> Self {
        PostgresEventStore {
            pool,
            _phantom_e: PhantomData,
        }
    }
}

impl<E> EventStore<E> for PostgresEventStore<E>
where
    E: Serialize + DeserializeOwned + Debug + Send + Sync,
{
    async fn create(
        &self,
        first_event: &E,
        create_options: &CreateOptions,
    ) -> Result<(LogId, IdempotentOutcome), EventStoreError> {
        let log_id = LogId::new();
        let conn = self.pool.get().await?;
        let stmt = conn.prepare_cached(INSERT_EVENT).await?;
        let event_index: u32 = 0;
        let result = conn
            .execute(
                &stmt,
                &[
                    &log_id.to_string(),
                    &event_index,
                    &Utc::now(),
                    &create_options.idempotency_key,
                    &Json(first_event),
                ],
            )
            .await;

        if let Err(ref e) = result {
            if is_duplicate_constraint_error(e, IDEMPOTENCY_KEY_CONSTRAINT) {
                if let Some(ref key) = create_options.idempotency_key {
                    let stmt = conn
                        .prepare_cached(SELECT_LOG_ID_FOR_IDEMPOTENCY_KEY)
                        .await?;
                    let row = conn.query_one(&stmt, &[key]).await?;
                    let prev_log_id: LogId = row.get("log_id");
                    return Ok((prev_log_id, IdempotentOutcome::Replay));
                }
            }
        }

        Ok(result.map(|_| (log_id, IdempotentOutcome::New))?)
    }

    async fn append(
        &self,
        log_id: &LogId,
        next_event: &E,
        event_index: u32,
        append_options: &AppendOptions,
    ) -> Result<IdempotentOutcome, EventStoreError> {
        let conn = self.pool.get().await?;
        let stmt = conn.prepare_cached(INSERT_EVENT).await?;
        let result = conn
            .execute(
                &stmt,
                &[
                    &log_id.to_string(),
                    &event_index,
                    &Utc::now(),
                    &append_options.idempotency_key,
                    &Json(next_event),
                ],
            )
            .await
            .map(|_| IdempotentOutcome::New);

        if let Err(ref e) = result {
            if is_duplicate_constraint_error(e, PK_CONSTRAINT) {
                return Err(EventStoreError::EventIndexAlreadyExists {
                    log_id: log_id.clone(),
                    event_index,
                });
            } else if is_duplicate_constraint_error(e, IDEMPOTENCY_KEY_CONSTRAINT) {
                // if the idempotency key already exists in the database, the
                // insert was already completed successfully, so just return Ok
                return Ok(IdempotentOutcome::Replay);
            }
        }

        Ok(result?)
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

fn is_duplicate_constraint_error(error: &tokio_postgres::Error, constraint_name: &str) -> bool {
    error
        .code()
        .map(|c| c == &SqlState::UNIQUE_VIOLATION)
        .unwrap_or(false)
        && error
            .as_db_error()
            .map(|dbe| dbe.constraint() == Some(constraint_name))
            .unwrap_or(false)
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
        let store = store();
        let (log_id, _) = store
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        let row_stream = store.load(&log_id, 0).await.unwrap();
        assert_eq!(row_stream.count().await, 1);
    }

    #[tokio::test]
    async fn create_append_load() {
        let store = store();
        let (log_id, _) = store
            .create(&TestEvent::Increment, &CreateOptions::default())
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
        let store = store();
        let create_options = CreateOptions {
            idempotency_key: Some(Uuid::now_v7().to_string()),
            ..Default::default()
        };

        let (first_log_id, outcome) = store
            .create(&TestEvent::Increment, &create_options)
            .await
            .unwrap();
        assert_eq!(outcome, IdempotentOutcome::New);

        let (replay_log_id, replay_outcome) = store
            .create(&TestEvent::Increment, &create_options)
            .await
            .unwrap();

        assert_eq!(first_log_id, replay_log_id);
        assert_eq!(replay_outcome, IdempotentOutcome::Replay);
    }

    #[tokio::test]
    async fn concurrent_append() {
        let store = store();
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
        )
    }

    #[tokio::test]
    async fn idempotent_append() {
        let store = store();
        let append_options = AppendOptions {
            idempotency_key: Some(Uuid::now_v7().to_string()),
            ..Default::default()
        };

        let (log_id, _) = store
            .create(&TestEvent::Increment, &CreateOptions::default())
            .await
            .unwrap();

        store
            .append(&log_id, &TestEvent::Decrement, 1, &append_options)
            .await
            .unwrap();

        // this should succeed, but be a no-op since an event with
        // the same idempotency key was already inserted above
        let outcome = store
            .append(&log_id, &TestEvent::Decrement, 2, &append_options)
            .await
            .unwrap();
        assert_eq!(outcome, IdempotentOutcome::Replay);

        //ensure this log only has 2 events
        let row_stream = store.load(&log_id, 0).await.unwrap();
        assert_eq!(row_stream.count().await, 2);
    }
}
