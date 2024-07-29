use crate::{
    caches::{AggregationCache, AggregationCacheError},
    ids::LogId,
    Aggregation,
};
use std::collections::HashMap;
use tokio::sync::{mpsc::Sender, Mutex};

#[derive(Debug, PartialEq)]
pub enum FakeAggregationCacheOp<A: Clone + Send + Sync> {
    Get {
        log_id: LogId,
        response: Option<Aggregation<A>>,
    },
    Put {
        aggregation: Aggregation<A>,
    },
}

#[derive(Debug)]
struct DB<A: Clone + Sync + Send> {
    table: HashMap<LogId, Aggregation<A>>,
    op_sender: Option<Sender<FakeAggregationCacheOp<A>>>,
}

/// A fake implementation of [AggregationCache] that should only be used for testing.
#[derive(Debug)]
pub struct FakeAggregationCache<A: Clone + Send + Sync> {
    mx_db: Mutex<DB<A>>,
}

impl<A> FakeAggregationCache<A>
where
    A: Clone + Sync + Send,
{
    pub fn new() -> Self {
        Self {
            mx_db: Mutex::new(DB {
                table: HashMap::new(),
                op_sender: None,
            }),
        }
    }

    pub fn with_notifications(op_sender: Sender<FakeAggregationCacheOp<A>>) -> Self {
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

impl<A> Default for FakeAggregationCache<A>
where
    A: Clone + Sync + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A> AggregationCache<A> for FakeAggregationCache<A>
where
    A: Clone + Sync + Send,
{
    async fn put(&self, aggregation: &Aggregation<A>) -> Result<(), AggregationCacheError> {
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

    async fn get(&self, log_id: &LogId) -> Result<Option<Aggregation<A>>, AggregationCacheError> {
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
