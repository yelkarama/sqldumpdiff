// Schema parser: extracts PRIMARY KEY columns and column lists from CREATE TABLE.
// The logic mirrors the Go implementation and favors speed over perfect SQL parsing.

use crate::logger;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub struct SchemaParser {
    create_table_re: Regex,
    primary_key_re: Regex,
}

impl SchemaParser {
    // Build regexes once for reuse.
    pub fn new() -> Self {
        let create_table_re = Regex::new(
            r"(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?[^`\s(]+`?\.)?`?([^`\s(]+)`?",
        )
        .expect("valid create table regex");
        let primary_key_re = Regex::new(r"(?i)PRIMARY\s+KEY\s*\(([^)]*)\)")
            .expect("valid primary key regex");
        Self {
            create_table_re,
            primary_key_re,
        }
    }

    // Parse PRIMARY KEY columns from CREATE TABLE blocks.
    pub fn parse_schemas(
        &self,
        filename: &str,
        bar: Option<&indicatif::ProgressBar>,
    ) -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
        logger::debug(&format!("ParseSchemas: Opening file {}", filename));
        let file = File::open(filename)?;
        let mut reader = BufReader::new(file);

        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut current_table = String::new();
        let mut in_create = false;
        let mut table_buf = String::new();
        let mut tables_processed = 0usize;

        let mut bytes_read: u64 = 0;
        let mut last_logged: u64 = 0;

        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            let line_len = line.len() as u64;
            bytes_read += line_len;
            if let Some(b) = bar {
                b.inc(line_len);
            } else if logger::is_debug() && bytes_read - last_logged > 100 * 1024 * 1024 {
                logger::debug(&format!(
                    "ParseSchemas: {} bytes read from {}",
                    bytes_read, filename
                ));
                last_logged = bytes_read;
            }

            if let Some(cap) = self.create_table_re.captures(&line) {
                if let Some(table) = cap.get(1) {
                    current_table = table.as_str().to_string();
                    in_create = true;
                    table_buf.clear();
                    table_buf.push_str(&line);
                    logger::debug(&format!(
                        "ParseSchemas: Found CREATE TABLE for {}",
                        current_table
                    ));
                }
            } else if in_create {
                table_buf.push_str(&line);
                let trimmed = line.trim();
                if trimmed.ends_with(';') {
                    // End of CREATE TABLE block; extract PK columns.
                    if let Some(pk_cap) = self.primary_key_re.captures(&table_buf) {
                        if let Some(pk_list) = pk_cap.get(1) {
                            let mut pk_cols = Vec::new();
                            for part in pk_list.as_str().split(',') {
                                let mut col = part.trim().trim_matches(['`', '"', '\''].as_ref());
                                col = col.trim();
                                if !col.is_empty() {
                                    // Store PK columns as lowercase for case-insensitive matching.
                                    pk_cols.push(col.to_lowercase());
                                }
                            }
                            if !pk_cols.is_empty() {
                                pk_map.insert(current_table.clone(), pk_cols);
                                logger::debug(&format!(
                                    "ParseSchemas: Table {} PK columns extracted",
                                    current_table
                                ));
                            }
                        }
                    }
                    tables_processed += 1;
                    in_create = false;
                    current_table.clear();
                }
            }

            line.clear();
        }

        if let Some(b) = bar {
            b.finish();
        }

        logger::debug(&format!(
            "ParseSchemas: Processed {} tables, found {} with PK",
            tables_processed,
            pk_map.len()
        ));

        Ok(pk_map)
    }

    // Parse column lists for each table, used when INSERT statements omit column names.
    pub fn parse_columns(
        &self,
        filename: &str,
        bar: Option<&indicatif::ProgressBar>,
    ) -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error + Send + Sync>> {
        logger::debug(&format!("ParseColumns: Opening file {}", filename));
        let file = File::open(filename)?;
        let mut reader = BufReader::new(file);

        let column_line_re = Regex::new(r"`(\w+)`\s+(\w+)").expect("valid column regex");

        let mut columns_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut current_table = String::new();
        let mut in_create = false;
        let mut table_buf = String::new();
        let mut tables_processed = 0usize;

        let mut bytes_read: u64 = 0;
        let mut last_logged: u64 = 0;

        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            let line_len = line.len() as u64;
            bytes_read += line_len;
            if let Some(b) = bar {
                b.inc(line_len);
            } else if logger::is_debug() && bytes_read - last_logged > 100 * 1024 * 1024 {
                logger::debug(&format!(
                    "ParseColumns: {} bytes read from {}",
                    bytes_read, filename
                ));
                last_logged = bytes_read;
            }

            if let Some(cap) = self.create_table_re.captures(&line) {
                if let Some(table) = cap.get(1) {
                    current_table = table.as_str().to_string();
                    in_create = true;
                    table_buf.clear();
                    table_buf.push_str(&line);
                    logger::debug(&format!(
                        "ParseColumns: Found CREATE TABLE for {}",
                        current_table
                    ));
                }
            } else if in_create {
                table_buf.push_str(&line);
                let trimmed = line.trim();
                if trimmed.ends_with(';') {
                    let mut columns = Vec::new();
                    for l in table_buf.lines() {
                        let trimmed_l = l.trim();
                        // Skip non-column lines.
                        if l.contains("CREATE TABLE")
                            || trimmed_l.starts_with("PRIMARY KEY")
                            || trimmed_l.starts_with("KEY ")
                            || trimmed_l.starts_with("UNIQUE KEY")
                            || trimmed_l.starts_with("CONSTRAINT")
                            || trimmed_l.starts_with("FULLTEXT")
                            || trimmed_l.starts_with(")")
                            || trimmed_l.is_empty()
                        {
                            continue;
                        }
                        if trimmed_l.starts_with('`') {
                            if let Some(m) = column_line_re.captures(l) {
                                if let Some(col) = m.get(1) {
                                    columns.push(col.as_str().to_string());
                                }
                            }
                        }
                    }
                    if !columns.is_empty() {
                        columns_map.insert(current_table.clone(), columns);
                        logger::debug(&format!(
                            "ParseColumns: Table {} columns extracted",
                            current_table
                        ));
                    }
                    tables_processed += 1;
                    in_create = false;
                    current_table.clear();
                }
            }

            line.clear();
        }

        if let Some(b) = bar {
            b.finish();
        }

        logger::debug(&format!(
            "ParseColumns: Processed {} tables, extracted columns for {}",
            tables_processed,
            columns_map.len()
        ));

        Ok(columns_map)
    }
}
