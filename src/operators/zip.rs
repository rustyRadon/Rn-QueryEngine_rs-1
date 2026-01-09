use crate::plan::ExecutionTask;
use crate::common::RecordBatch;
use std::sync::Arc;

pub struct ZipWorker {
    pub left: Arc<dyn ExecutionTask>,
    pub right: Arc<dyn ExecutionTask>,
}

impl ExecutionTask for ZipWorker {
    fn next_batch(&self) -> Result<Option<RecordBatch>, String> {
        let left_batch = self.left.next_batch()?;
        let right_batch = self.right.next_batch()?;

        match (left_batch, right_batch) {
            (Some(l), Some(r)) => {
                let mut combined_cols = l.columns;
                combined_cols.extend(r.columns);

                let mut combined_names = l.names;
                combined_names.extend(r.names);

                Ok(Some(RecordBatch::new(combined_names, combined_cols)))
            }
            _ => Ok(None),
        }
    }
}