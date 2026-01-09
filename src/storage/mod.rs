use std::fs::File;
use std::io::{BufReader, ErrorKind,};
use std::sync::{Arc, Mutex};
use byteorder::{LittleEndian, ReadBytesExt};
use crate::common::{Column, RecordBatch};
use crate::plan::ExecutionTask;

pub struct ScanWorker {
    pub file_handle: Mutex<BufReader<File>>,
    pub column_name: String,
}

impl ExecutionTask for ScanWorker {
    fn next_batch(&self) -> Result<Option<RecordBatch>, String> {
        let mut guard = self.file_handle.lock().map_err(|_| "Lock poisoned")?;
        let mut batch_data = Vec::with_capacity(1024);

        for _ in 0..1024 {
            match guard.read_i32::<LittleEndian>() {
                Ok(val) => batch_data.push(val),
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.to_string()),
            }
        }

        Ok((!batch_data.is_empty()).then_some(RecordBatch::new(
            vec![self.column_name.clone()],
            vec![Column::Int32(Arc::new(batch_data))],
        )))
    }
}