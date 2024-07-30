use crate::{
    caches::{ReductionCache, ReductionCacheError},
    ids::LogId,
    Reduction,
};
use std::collections::HashMap;
use tokio::sync::{mpsc::Sender, Mutex};

#[derive(Debug, PartialEq)]
pub enum FakeReductionCacheOp<A: Clone + Send + Sync> {
    Get {
        log_id: LogId,
        response: Option<Reduction<A>>,
    },
    Put {
        reduction: Reduction<A>,
    },
}

#[derive(Debug)]
struct DB<A: Clone + Sync + Send> {
    table: HashMap<LogId, Reduction<A>>,
    op_sender: Option<Sender<FakeReductionCacheOp<A>>>,
}

/// A fake implementation of [ReductionCache] that should only be used for testing.
#[derive(Debug)]
pub struct FakeReductionCache<A: Clone + Send + Sync> {
    mx_db: Mutex<DB<A>>,
}

impl<A> FakeReductionCache<A>
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

    pub fn with_notifications(op_sender: Sender<FakeReductionCacheOp<A>>) -> Self {
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

impl<A> Default for FakeReductionCache<A>
where
    A: Clone + Sync + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A> ReductionCache<A> for FakeReductionCache<A>
where
    A: Clone + Sync + Send,
{
    async fn put(&self, reduction: &Reduction<A>) -> Result<(), ReductionCacheError> {
        let mut db = self.mx_db.lock().await;

        db.table
            .insert(reduction.log_id().clone(), reduction.clone());

        if let Some(sender) = &db.op_sender {
            sender
                .send(FakeReductionCacheOp::Put {
                    reduction: reduction.clone(),
                })
                .await
                .unwrap();
        }

        Ok(())
    }

    async fn get(&self, log_id: &LogId) -> Result<Option<Reduction<A>>, ReductionCacheError> {
        let db = self.mx_db.lock().await;

        let maybe_reduction = db.table.get(log_id).cloned();

        if let Some(sender) = &db.op_sender {
            sender
                .send(FakeReductionCacheOp::Get {
                    log_id: log_id.clone(),
                    response: maybe_reduction.clone(),
                })
                .await
                .unwrap();
        }

        Ok(maybe_reduction)
    }
}
