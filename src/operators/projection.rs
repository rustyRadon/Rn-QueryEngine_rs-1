use std::sync::Arc;
use crate::common::RecordBatch;
use crate::plan::ExecutionTask;

pub struct ProjectionWorker {
    pub input: Arc<dyn ExecutionTask>,
    pub indices: Vec<usize>,
}

impl ExecutionTask for ProjectionWorker {
    fn next_batch(&self) -> Result<Option<RecordBatch>, String> {
        Ok(self.input.next_batch()?.map(|batch| {
            let (cols, names): (Vec<_>, Vec<_>) = self.indices.iter()
                .map(|&i| (batch.columns[i].clone(), batch.names[i].clone()))
                .unzip();

            RecordBatch::new(names, cols)
        }))
    }
}