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
    // Convert to JSON string for JSONL storage.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    // Parse from JSON string back into InsertRow.
    pub fn from_json(line: &str) -> Result<InsertRow, serde_json::Error> {
        serde_json::from_str(line)
    }
}
