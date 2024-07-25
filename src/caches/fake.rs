use crate::{
    caches::{AggregationCache, AggregationCacheError},
    ids::LogId,
    Aggregate, Aggregation,
};
use std::collections::HashMap;
use tokio::sync::{mpsc::Sender, Mutex};

#[derive(Debug, PartialEq)]
pub enum FakeAggregationCacheOp<E, A: Aggregate<E> + Clone + Send + Sync> {
    Get {
        log_id: LogId,
        response: Option<Aggregation<E, A>>,
    },
    Put {
        aggregation: Aggregation<E, A>,
    },
}

#[derive(Debug)]
struct DB<E, A: Aggregate<E> + Clone + Sync + Send> {
    table: HashMap<LogId, Aggregation<E, A>>,
    op_sender: Option<Sender<FakeAggregationCacheOp<E, A>>>,
}

#[derive(Debug)]
pub struct FakeAggregationCache<E, A: Aggregate<E> + Clone + Send + Sync> {
    mx_db: Mutex<DB<E, A>>,
}

impl<E, A> FakeAggregationCache<E, A>
where
    A: Aggregate<E> + Clone + Sync + Send,
{
    pub fn new() -> Self {
        Self {
            mx_db: Mutex::new(DB {
                table: HashMap::new(),
                op_sender: None,
            }),
        }
    }

    pub fn new_with_notifications(op_sender: Sender<FakeAggregationCacheOp<E, A>>) -> Self {
        Self {
            mx_db: Mutex::new(DB {
                table: HashMap::new(),
                op_sender: Some(op_sender),
            }),
        }
    }

    pub async fn evict(&self, log_id: &LogId) {
        let mut db = self.mx_db.lock().await;
        db.table.remove(log_id);
    }
}

impl<E, A> Default for FakeAggregationCache<E, A>
where
    A: Aggregate<E> + Clone + Sync + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E, A> AggregationCache<E, A> for FakeAggregationCache<E, A>
where
    E: Sync + Send + Clone,
    A: Aggregate<E> + Clone + Sync + Send,
{
    async fn put(&self, aggregation: &Aggregation<E, A>) -> Result<(), AggregationCacheError> {
        let mut db = self.mx_db.lock().await;

        db.table
            .insert(aggregation.log_id().clone(), aggregation.clone());

        if let Some(sender) = &db.op_sender {
            sender
                .send(FakeAggregationCacheOp::Put {
                    aggregation: aggregation.clone(),
                })
                .await
                .unwrap();
        }

        Ok(())
    }

    async fn get(
        &self,
        log_id: &LogId,
    ) -> Result<Option<Aggregation<E, A>>, AggregationCacheError> {
        let db = self.mx_db.lock().await;

        let maybe_aggregation = db.table.get(log_id).cloned();

        if let Some(sender) = &db.op_sender {
            sender
                .send(FakeAggregationCacheOp::Get {
                    log_id: log_id.clone(),
                    response: maybe_aggregation.clone(),
                })
                .await
                .unwrap();
        }

        Ok(maybe_aggregation)
    }
}
