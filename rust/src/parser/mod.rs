// Parser module: schema (PK + columns) and INSERT parsing for rows.

pub mod insert;
pub mod schema;

// InsertRow matches the JSON structure stored in per-table JSONL files.
// We keep columns list and a map of column->value (string with quotes preserved).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InsertRow {
    pub table: String,
    pub columns: Vec<String>,
    pub data: std::collections::HashMap<String, String>,
}

impl InsertRow {
    // JSON helpers removed; binary row format is used for performance.
}
