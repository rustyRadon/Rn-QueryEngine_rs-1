use std::sync::Arc;
use crate::common::{Column, RecordBatch};
use crate::plan::ExecutionTask;

pub struct FilterWorker {
    pub input: Arc<dyn ExecutionTask>,
    pub predicate: fn(i32) -> bool,
}

impl ExecutionTask for FilterWorker {
    fn next_batch(&self) -> Result<Option<RecordBatch>, String> {
        Ok(self.input.next_batch()?.and_then(|batch| {
            match &batch.columns[0] {
                Column::Int32(data) => {
                    let filtered: Vec<i32> = data.iter()
                        .filter(|&&v| (self.predicate)(v))
                        .copied()
                        .collect();

                    (!filtered.is_empty()).then_some(RecordBatch::new(
                        batch.names,
                        vec![Column::Int32(Arc::new(filtered))],
                    ))
                },
            }
        }))
    }
}