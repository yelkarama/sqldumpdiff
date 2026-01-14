// INSERT parser: streams INSERT statements and expands them into row objects.
// We intentionally keep parsing simple (no full SQL grammar) for speed.

use crate::logger;
use crate::parser::InsertRow;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub struct InsertParser;

impl InsertParser {
    pub fn new() -> Self {
        Self
    }

    // Parse INSERT statements from a file, streaming rows to the caller.
    // If a progress bar is provided, we increment it by bytes read.
    pub fn parse_inserts_stream<F>(
        &self,
        filename: &str,
        columns_map: &HashMap<String, Vec<String>>,
        bar: Option<&indicatif::ProgressBar>,
        label: &str,
        mut on_row: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut(InsertRow),
    {
        logger::debug(&format!("ParseInsertsStream: Opening file {}", filename));
        let file = File::open(filename)?;
        let mut reader = BufReader::new(file);

        let mut acc = InsertAccumulator::new();
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
                    "ParseInsertsStream: {} bytes read for {}",
                    bytes_read, label
                ));
                last_logged = bytes_read;
            }

            let statements = acc.process_line(&line);
            for stmt in statements {
                let rows = self.expand_insert(&stmt, columns_map);
                match rows {
                    Ok(rows) => {
                        for row in rows {
                            on_row(row);
                        }
                    }
                    Err(e) => {
                        logger::debug(&format!(
                            "ParseInsertsStream: failed to expand INSERT: {}",
                            e
                        ));
                    }
                }
            }

            line.clear();
        }

        // Flush any pending statement at EOF.
        for stmt in acc.finalize() {
            let rows = self.expand_insert(&stmt, columns_map);
            match rows {
                Ok(rows) => {
                    for row in rows {
                        on_row(row);
                    }
                }
                Err(e) => {
                    logger::debug(&format!(
                        "ParseInsertsStream: failed to expand INSERT: {}",
                        e
                    ));
                }
            }
        }

        if let Some(b) = bar {
            b.finish();
        }

        logger::debug(&format!(
            "ParseInsertsStream: processed {} statements",
            acc.statements_processed
        ));
        Ok(())
    }

    // Expand a single INSERT statement into one InsertRow per value group.
    pub fn expand_insert(
        &self,
        insert_stmt: &str,
        columns_map: &HashMap<String, Vec<String>>,
    ) -> Result<Vec<InsertRow>, String> {
        // Normalize whitespace for simpler parsing.
        let normalized = insert_stmt
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string();

        let upper = normalized.to_uppercase();
        let insert_idx = upper.find("INSERT INTO").ok_or("not INSERT")?;
        let mut rest = normalized[insert_idx + 11..].trim().to_string();

        // Parse table name.
        let table: String;
        if rest.starts_with('`') {
            let end = rest[1..]
                .find('`')
                .ok_or("invalid table name")?
                + 1;
            table = rest[1..end].to_string();
            rest = rest[end + 1..].to_string();
        } else {
            // Split by '(' or space.
            let mut parts = rest.split(|c: char| c == '(' || c.is_whitespace());
            table = parts.next().ok_or("invalid table name")?.to_string();
        }

        logger::debug(&format!("ExpandInsert: table {}", table));

        // Parse explicit columns if present.
        let mut columns: Vec<String> = Vec::new();
        let mut rest_trim = rest.trim().to_string();
        if rest_trim.starts_with('(') {
            let mut depth = 0i32;
            let mut end_idx = None;
            for (i, c) in rest_trim.chars().enumerate() {
                if c == '(' {
                    depth += 1;
                } else if c == ')' {
                    depth -= 1;
                    if depth == 0 {
                        end_idx = Some(i);
                        break;
                    }
                }
            }
            let end = end_idx.ok_or("invalid column list")?;
            let col_str = &rest_trim[1..end];
            for col in col_str.split(',') {
                let col = col.trim().trim_matches(['`', '\'', '"'].as_ref());
                if !col.is_empty() {
                    columns.push(col.to_string());
                }
            }
            rest_trim = rest_trim[end + 1..].to_string();
        }

        // Find VALUES keyword.
        let upper_rest = rest_trim.to_uppercase();
        let values_pos = upper_rest
            .find("VALUES")
            .ok_or("no VALUES clause")?;
        let mut values_part = rest_trim[values_pos + 6..].trim().to_string();
        if values_part.ends_with(';') {
            values_part.pop();
        }
        values_part = values_part.trim().to_string();

        // Use schema columns if INSERT has no explicit columns.
        if columns.is_empty() {
            if let Some(cols) = columns_map.get(&table) {
                columns = cols.clone();
            } else {
                // Case-insensitive fallback for table name mismatches.
                let table_lower = table.to_lowercase();
                for (key, cols) in columns_map.iter() {
                    if key.to_lowercase() == table_lower {
                        columns = cols.clone();
                        break;
                    }
                }
            }
        }
        if columns.is_empty() {
            return Err(format!("no columns defined for table {}", table));
        }

        let groups = split_value_groups_with_quotes(&values_part);
        logger::debug(&format!(
            "ExpandInsert: {} has {} value groups",
            table,
            groups.len()
        ));

        let mut rows = Vec::new();
        for mut group in groups {
            group = group.trim().to_string();
            if group.starts_with('(') && group.ends_with(')') {
                group = group[1..group.len() - 1].to_string();
            }
            let values = parse_values_with_quotes(&group);
            if values.len() != columns.len() {
                logger::debug(&format!(
                    "ExpandInsert: skip row (expected {}, got {})",
                    columns.len(),
                    values.len()
                ));
                continue;
            }
            let mut data: HashMap<String, String> = HashMap::new();
            for (i, col) in columns.iter().enumerate() {
                data.insert(col.clone(), values[i].clone());
            }
            rows.push(InsertRow {
                table: table.clone(),
                columns: columns.clone(),
                data,
            });
        }

        Ok(rows)
    }
}

// Tracks an INSERT statement across multiple lines.
struct InsertAccumulator {
    buffer: String,
    in_single_quote: bool,
    in_double_quote: bool,
    escape_next: bool,
    paren_depth: i32,
    in_insert: bool,
    statements_processed: usize,
}

impl InsertAccumulator {
    fn new() -> Self {
        Self {
            buffer: String::new(),
            in_single_quote: false,
            in_double_quote: false,
            escape_next: false,
            paren_depth: 0,
            in_insert: false,
            statements_processed: 0,
        }
    }

    // Process a single line and return any complete INSERT statements found.
    fn process_line(&mut self, line: &str) -> Vec<String> {
        let mut results = Vec::new();
        if !self.in_insert && line.to_uppercase().contains("INSERT INTO") {
            self.buffer.clear();
            self.buffer.push_str(line);
            self.in_insert = true;
            self.reset_state();
            self.process_line_content(line);
            if line.trim_end().ends_with(';') && self.paren_depth == 0 {
                results.push(self.buffer.clone());
                self.buffer.clear();
                self.in_insert = false;
                self.statements_processed += 1;
            }
            return results;
        }

        if self.in_insert {
            self.buffer.push_str(line);
            self.process_line_content(line);
            if line.trim_end().ends_with(';') && self.paren_depth == 0 {
                results.push(self.buffer.clone());
                self.buffer.clear();
                self.in_insert = false;
                self.statements_processed += 1;
            }
        }
        results
    }

    // Scan characters to keep track of quotes and parenthesis depth.
    fn process_line_content(&mut self, line: &str) {
        for c in line.chars() {
            if self.escape_next {
                self.escape_next = false;
                continue;
            }
            if c == '\\' {
                self.escape_next = true;
                continue;
            }
            if c == '\'' && !self.in_double_quote {
                self.in_single_quote = !self.in_single_quote;
            } else if c == '"' && !self.in_single_quote {
                self.in_double_quote = !self.in_double_quote;
            } else if !self.in_single_quote && !self.in_double_quote {
                if c == '(' {
                    self.paren_depth += 1;
                } else if c == ')' {
                    self.paren_depth -= 1;
                }
            }
        }
    }

    // Flush remaining buffer at EOF (best effort).
    fn finalize(&mut self) -> Vec<String> {
        let mut results = Vec::new();
        if self.in_insert && !self.buffer.is_empty() {
            results.push(self.buffer.clone());
            self.buffer.clear();
            self.statements_processed += 1;
        }
        results
    }

    fn reset_state(&mut self) {
        self.in_single_quote = false;
        self.in_double_quote = false;
        self.escape_next = false;
        self.paren_depth = 0;
    }
}

// Split value groups: (...),(...),... while respecting quotes.
fn split_value_groups_with_quotes(values_part: &str) -> Vec<String> {
    let mut groups = Vec::new();
    let mut buf = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escape = false;
    let mut paren_depth = 0i32;

    for c in values_part.chars() {
        if escape {
            buf.push(c);
            escape = false;
            continue;
        }
        if c == '\\' {
            buf.push(c);
            escape = true;
            continue;
        }
        if c == '\'' && !in_double {
            buf.push(c);
            in_single = !in_single;
            continue;
        }
        if c == '"' && !in_single {
            buf.push(c);
            in_double = !in_double;
            continue;
        }
        if !in_single && !in_double {
            if c == '(' {
                paren_depth += 1;
                buf.push(c);
                continue;
            }
            if c == ')' {
                paren_depth -= 1;
                buf.push(c);
                if paren_depth == 0 {
                    groups.push(buf.clone());
                    buf.clear();
                }
                continue;
            }
            if c == ',' && paren_depth == 0 {
                // Separator between groups; skip.
                continue;
            }
        }
        buf.push(c);
    }

    if !buf.trim().is_empty() {
        groups.push(buf.trim().to_string());
    }
    groups
}

// Parse comma-separated values within a single group, respecting quotes.
// We intentionally keep quotes in the value (matches Go/Java behavior).
fn parse_values_with_quotes(values_str: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut buf = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escape = false;
    let mut depth = 0i32;

    for c in values_str.chars() {
        if escape {
            buf.push(c);
            escape = false;
            continue;
        }
        if c == '\\' {
            buf.push(c);
            escape = true;
            continue;
        }
        if c == '\'' && !in_double && depth == 0 {
            buf.push(c);
            in_single = !in_single;
            continue;
        }
        if c == '"' && !in_single && depth == 0 {
            buf.push(c);
            in_double = !in_double;
            continue;
        }
        if !in_single && !in_double {
            if c == '(' {
                depth += 1;
            } else if c == ')' {
                depth -= 1;
            }
        }
        if !in_single && !in_double && depth == 0 && c == ',' {
            let val = buf.trim().to_string();
            values.push(if val == "NULL" { "NULL".to_string() } else { val });
            buf.clear();
        } else {
            buf.push(c);
        }
    }

    let val = buf.trim().to_string();
    values.push(if val == "NULL" { "NULL".to_string() } else { val });
    values
}

// Utility for testing: detect INSERT lines quickly.
#[allow(dead_code)]
fn is_insert_line(line: &str) -> bool {
    let re = Regex::new(r"(?i)INSERT\s+INTO").unwrap();
    re.is_match(line)
}
