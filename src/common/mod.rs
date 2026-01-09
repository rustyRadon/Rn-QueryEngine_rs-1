use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Column {
    Int32(Arc<Vec<i32>>),
}

#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub columns: Vec<Column>,
    pub names: Vec<String>,
}

impl RecordBatch {
    pub fn new(names: Vec<String>, columns: Vec<Column>) -> Self {
        Self { names, columns }
    }
}