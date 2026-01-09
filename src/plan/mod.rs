use crate::common::RecordBatch;

///contract for all executable ops.
pub trait ExecutionTask: Send + Sync {
    fn next_batch(&self) -> Result<Option<RecordBatch>, String>;
}