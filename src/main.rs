mod common;
mod storage;
mod plan;
mod operators;

use std::sync::{Arc, Mutex};
use std::fs::File;
use std::io::BufReader;
use crate::plan::ExecutionTask;
use crate::storage::ScanWorker;
use crate::operators::filter::FilterWorker;
use crate::operators::projection::ProjectionWorker;
use crate::common::Column;

fn main() -> Result<(), String> {
    let path = "age.bin";
    
    let scanner = Arc::new(ScanWorker {
        file_handle: Mutex::new(BufReader::new(File::open(path).map_err(|e| e.to_string())?)),
        column_name: "age".to_string(),
    });

    let filter = Arc::new(FilterWorker {
        input: scanner,
        predicate: |age| age > 21,
    });

    let pipeline = Arc::new(ProjectionWorker {
        input: filter,
        indices: vec![0],
    });

    while let Some(batch) = pipeline.next_batch()? {
        if let Column::Int32(data) = &batch.columns[0] {
            for val in data.iter() {
                println!("Result Row -> Age: {}", val);
            }
        }
    }

    Ok(())
}