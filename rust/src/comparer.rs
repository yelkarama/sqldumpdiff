// Comparison pipeline: split dumps into per-table JSONL, then compare via SQLite.
// This mirrors the Go implementation closely for consistent behavior.

use crate::logger;
use crate::parser::insert::InsertParser;
use crate::parser::InsertRow;
use rayon::prelude::*;
use rusqlite::{params, Connection};
use ahash::AHashSet;
use blake3;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, atomic::{AtomicU64, Ordering}};
use std::time::Instant;

pub struct ComparisonResult {
    pub changes: String,
    pub deletions: String,
    pub insert_count: usize,
    pub update_count: usize,
    pub delete_count: usize,
}

// Per-table timing data for profiling hot tables.
#[derive(Clone, Debug, serde::Serialize)]
pub struct TableTiming {
    table: String,
    load_ms: u128,
    compare_ms: u128,
    delete_ms: u128,
}

impl TableTiming {
    fn total_ms(&self) -> u128 {
        self.load_ms + self.compare_ms + self.delete_ms
    }
}

#[derive(Default, Clone)]
pub struct Summary {
    pub insert_count: usize,
    pub update_count: usize,
    pub delete_count: usize,
}

// SQLite tuning knobs (mirrors Go defaults).
#[derive(Clone, Copy)]
pub struct SqliteTunables {
    pub cache_kb: i64,
    pub mmap_mb: i64,
    pub batch_size: usize,
    pub workers: usize,
}

impl Default for SqliteTunables {
    fn default() -> Self {
        Self {
            cache_kb: 800_000,
            mmap_mb: 128,
            batch_size: 20_000,
            workers: 0,
        }
    }
}

pub struct DeltaGenerator {
    pk_map: HashMap<String, Vec<String>>,
    old_columns: HashMap<String, Vec<String>>,
    new_columns: HashMap<String, Vec<String>>,
    tunables: SqliteTunables,
    timing_enabled: bool,
}

#[derive(serde::Serialize)]
pub struct TimingReport {
    pub split_ms: u128,
    pub compare_ms: u128,
    pub delete_ms: u128,
    pub write_ms: u128,
    pub write_format_ms: u128,
    pub write_io_ms: u128,
    pub tables: Vec<TableTiming>,
}

impl DeltaGenerator {
    pub fn new(
        pk_map: HashMap<String, Vec<String>>,
        old_columns: HashMap<String, Vec<String>>,
        new_columns: HashMap<String, Vec<String>>,
        tunables: SqliteTunables,
        timing_enabled: bool,
    ) -> Self {
        Self {
            pk_map,
            old_columns,
            new_columns,
            tunables,
            timing_enabled,
        }
    }

    // Main pipeline: split dumps, compare tables, and write delta SQL.
    pub fn generate_delta(
        &self,
        old_file: &str,
        new_file: &str,
        progress: &crate::progress::ProgressManager,
        out: Arc<Mutex<Box<dyn Write + Send>>>,
    ) -> Result<(Summary, TimingReport), Box<dyn std::error::Error + Send + Sync>> {
        logger::debug("GenerateDelta: Starting delta generation");

        let tmp_dir = tempfile::Builder::new()
            .prefix("sqldumpdiff-rust-")
            .tempdir()?;
        let tmp_path = tmp_dir.path().to_path_buf();

        // Clone file paths so they can be moved into thread closures safely.
        let old_file = old_file.to_string();
        let new_file = new_file.to_string();

        let mut summary = Summary::default();
        let mut timing_report = TimingReport {
            split_ms: 0,
            compare_ms: 0,
            delete_ms: 0,
            write_ms: 0,
            write_format_ms: 0,
            write_io_ms: 0,
            tables: Vec::new(),
        };

        // Write header.
        {
            let mut guard = lock_or_err(&out, "write header")?;
            writeln!(guard, "-- Full Delta Update Script")?;
            writeln!(guard, "SET FOREIGN_KEY_CHECKS = 0;")?;
            writeln!(guard)?;
        }

        // Split old/new dumps into JSONL files per table.
        logger::debug("GenerateDelta: Splitting dumps by table");
        let split_start = Instant::now();
        let old_files = Arc::new(Mutex::new(HashMap::new()));
        let new_files = Arc::new(Mutex::new(HashMap::new()));
        let split_err = Arc::new(Mutex::new(None::<String>));

        let old_files_clone = Arc::clone(&old_files);
        let new_files_clone = Arc::clone(&new_files);
        let split_err_clone = Arc::clone(&split_err);
        let split_err_clone_new = Arc::clone(&split_err);

        // Run split in parallel for old/new.
        let old_handle = std::thread::spawn({
            let pk_map = self.pk_map.clone();
            let columns = self.old_columns.clone();
            let tmp = tmp_path.clone();
            let progress = progress.clone();
            move || {
                let res = split_dump_by_table(
                    &old_file,
                    &columns,
                    &pk_map,
                    &tmp,
                    "old",
                    &progress,
                    "Parsing old dump",
                );
                match res {
                    Ok(map) => {
                        match old_files_clone.lock() {
                            Ok(mut guard) => {
                                *guard = map;
                            }
                            Err(poisoned) => {
                                let mut guard = poisoned.into_inner();
                                *guard = map;
                            }
                        }
                    }
                    Err(e) => {
                        match split_err_clone.lock() {
                            Ok(mut guard) => {
                                *guard = Some(format!("split old: {}", e));
                            }
                            Err(poisoned) => {
                                let mut guard = poisoned.into_inner();
                                *guard = Some(format!("split old: {}", e));
                            }
                        }
                    }
                }
            }
        });

        let new_handle = std::thread::spawn({
            let pk_map = self.pk_map.clone();
            let columns = self.new_columns.clone();
            let tmp = tmp_path.clone();
            let progress = progress.clone();
            move || {
                let res = split_dump_by_table(
                    &new_file,
                    &columns,
                    &pk_map,
                    &tmp,
                    "new",
                    &progress,
                    "Parsing new dump",
                );
                match res {
                    Ok(map) => {
                        match new_files_clone.lock() {
                            Ok(mut guard) => {
                                *guard = map;
                            }
                            Err(poisoned) => {
                                let mut guard = poisoned.into_inner();
                                *guard = map;
                            }
                        }
                    }
                    Err(e) => {
                        match split_err_clone_new.lock() {
                            Ok(mut guard) => {
                                *guard = Some(format!("split new: {}", e));
                            }
                            Err(poisoned) => {
                                let mut guard = poisoned.into_inner();
                                *guard = Some(format!("split new: {}", e));
                            }
                        }
                    }
                }
            }
        });

        old_handle
            .join()
            .map_err(|_| "split old thread panicked")?;
        new_handle
            .join()
            .map_err(|_| "split new thread panicked")?;

        logger::debug(&format!(
            "Timing: split dumps took {:?}",
            split_start.elapsed()
        ));
        timing_report.split_ms = split_start.elapsed().as_millis();

        if let Some(err) = lock_or_err(&split_err, "read split error")?.take() {
            return Err(Box::<dyn std::error::Error + Send + Sync>::from(err));
        }

        let old_map = lock_or_err(&old_files, "read old table files")?.clone();
        let new_map = lock_or_err(&new_files, "read new table files")?.clone();

        let mut all_tables: AHashSet<String> = AHashSet::new();
        for t in old_map.keys() {
            all_tables.insert(t.clone());
        }
        for t in new_map.keys() {
            all_tables.insert(t.clone());
        }

        logger::debug(&format!(
            "GenerateDelta: old tables={} new tables={} all={} pkMap={}",
            old_map.len(),
            new_map.len(),
            all_tables.len(),
            self.pk_map.len()
        ));
        logger::debug(&format!("GenerateDelta: Comparing {} tables", all_tables.len()));
        let compare_start = Instant::now();

        // Configure worker count for table comparisons.
        let worker_count = if self.tunables.workers > 0 {
            self.tunables.workers
        } else {
            num_cpus::get()
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(worker_count)
            .build()
            .expect("build rayon pool");

        let compare_bar = progress.new_table_bar(all_tables.len() as u64);
        let out_mutex = Arc::clone(&out);
        let summary_mutex = Arc::new(Mutex::new(Summary::default()));
        let timings_mutex: Arc<Mutex<Vec<TableTiming>>> = Arc::new(Mutex::new(Vec::new()));
        let write_ms = Arc::new(AtomicU64::new(0));
        let write_format_ms = Arc::new(AtomicU64::new(0));
        let write_io_ms = Arc::new(AtomicU64::new(0));

        pool.install(|| {
            all_tables.par_iter().for_each(|table| {
                let pk_cols = match self.pk_map.get(table) {
                    Some(pk) => pk.clone(),
                    None => return,
                };
                let old_path = old_map.get(table).cloned().unwrap_or_default();
                let new_path = new_map.get(table).cloned().unwrap_or_default();

                match compare_table_sqlite(
                    table,
                    &pk_cols,
                    &old_path,
                    &new_path,
                    self.tunables,
                    self.timing_enabled,
                ) {
                    Ok((res, timing)) => {
                        if !res.changes.is_empty() || !res.deletions.is_empty() {
                            let format_start = Instant::now();
                            let mut buf = String::new();
                            if !res.changes.is_empty() {
                                buf.push_str(&res.changes);
                            }
                            if !res.deletions.is_empty() {
                                buf.push_str("-- DELETIONS\n");
                                buf.push_str(&res.deletions);
                            }
                            let format_dur = format_start.elapsed().as_millis() as u64;
                            write_format_ms.fetch_add(format_dur, Ordering::Relaxed);

                            match out_mutex.lock() {
                                Ok(mut guard) => {
                                    let write_start = Instant::now();
                                    let _ = guard.write_all(buf.as_bytes());
                                    let io_dur = write_start.elapsed().as_millis() as u64;
                                    write_io_ms.fetch_add(io_dur, Ordering::Relaxed);
                                    write_ms.fetch_add(format_dur + io_dur, Ordering::Relaxed);
                                }
                                Err(poisoned) => {
                                    let mut guard = poisoned.into_inner();
                                    let write_start = Instant::now();
                                    let _ = guard.write_all(buf.as_bytes());
                                    let io_dur = write_start.elapsed().as_millis() as u64;
                                    write_io_ms.fetch_add(io_dur, Ordering::Relaxed);
                                    write_ms.fetch_add(format_dur + io_dur, Ordering::Relaxed);
                                }
                            }
                        }
                        match summary_mutex.lock() {
                            Ok(mut guard) => {
                                guard.insert_count += res.insert_count;
                                guard.update_count += res.update_count;
                                guard.delete_count += res.delete_count;
                            }
                            Err(poisoned) => {
                                let mut guard = poisoned.into_inner();
                                guard.insert_count += res.insert_count;
                                guard.update_count += res.update_count;
                                guard.delete_count += res.delete_count;
                            }
                        }
                        match timings_mutex.lock() {
                            Ok(mut guard) => guard.push(timing),
                            Err(poisoned) => {
                                let mut guard = poisoned.into_inner();
                                guard.push(timing);
                            }
                        }
                        if let Some(bar) = &compare_bar {
                            bar.inc(1);
                        }
                    }
                    Err(e) => {
                        logger::error(&format!("Table {} compare error: {}", table, e));
                    }
                }
            });
        });

        if let Some(bar) = &compare_bar {
            bar.finish();
        }

        logger::debug(&format!(
            "Timing: compare tables took {:?}",
            compare_start.elapsed()
        ));
        timing_report.compare_ms = compare_start.elapsed().as_millis();

        if logger::is_debug() || self.timing_enabled {
            if let Ok(guard) = timings_mutex.lock() {
                let mut rows = guard.clone();
                rows.sort_by(|a, b| b.total_ms().cmp(&a.total_ms()));
                let top = rows.into_iter().take(10).collect::<Vec<_>>();
                if !top.is_empty() {
                    logger::debug("Timing: top 10 slowest tables (ms):");
                    for t in top {
                        logger::debug(&format!(
                            "  {} total={} load={} compare={} delete={}",
                            t.table,
                            t.total_ms(),
                            t.load_ms,
                            t.compare_ms,
                            t.delete_ms
                        ));
                    }
                }
            }
        }

        {
        let sum = lock_or_err(&summary_mutex, "read summary")?;
        summary.insert_count = sum.insert_count;
        summary.update_count = sum.update_count;
        summary.delete_count = sum.delete_count;
        }

        {
            let mut guard = lock_or_err(&out, "write footer")?;
            writeln!(guard, "SET FOREIGN_KEY_CHECKS = 1;")?;
        }
        logger::debug("GenerateDelta: Completed");
        if let Ok(guard) = timings_mutex.lock() {
            let mut rows = guard.clone();
            rows.sort_by(|a, b| b.total_ms().cmp(&a.total_ms()));
            timing_report.tables = rows;
        }
        timing_report.write_ms = write_ms.load(Ordering::Relaxed) as u128;
        timing_report.write_format_ms = write_format_ms.load(Ordering::Relaxed) as u128;
        timing_report.write_io_ms = write_io_ms.load(Ordering::Relaxed) as u128;
        timing_report.delete_ms = timing_report
            .tables
            .iter()
            .map(|t| t.delete_ms)
            .sum();

        Ok((summary, timing_report))
    }
}

// Split INSERT rows into per-table JSONL files.
fn split_dump_by_table(
    dump_file: &str,
    columns_map: &HashMap<String, Vec<String>>,
    pk_map: &HashMap<String, Vec<String>>,
    tmp_dir: &Path,
    label: &str,
    progress: &crate::progress::ProgressManager,
    progress_label: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
    let insert_parser = InsertParser::new();

    let mut writers: HashMap<String, BufWriter<File>> = HashMap::new();
    let mut table_paths: HashMap<String, String> = HashMap::new();
    let mut missing_tables: AHashSet<String> = AHashSet::new();
    let mut seen_tables: AHashSet<String> = AHashSet::new();

    let bar = progress.new_parse_bar(dump_file, progress_label);

    insert_parser.parse_inserts_stream(
        dump_file,
        columns_map,
        bar.as_ref(),
        progress_label,
        |row| {
            seen_tables.insert(row.table.clone());
            let resolved = resolve_table_name(pk_map, &row.table);
            let resolved = match resolved {
                Some(r) => r,
                None => {
                    missing_tables.insert(row.table.clone());
                    return;
                }
            };

            let writer = writers.entry(resolved.clone()).or_insert_with(|| {
                let name = sanitize_filename(&resolved);
                let path = tmp_dir.join(format!("{}_{}.jsonl", label, name));
                let file = File::create(&path).expect("create temp jsonl");
                table_paths.insert(resolved.clone(), path.to_string_lossy().to_string());
                BufWriter::new(file)
            });

            if let Ok(json) = row.to_json() {
                let _ = writeln!(writer, "{}", json);
            }
        },
    )?;

    for (_, mut w) in writers {
        let _ = w.flush();
    }

    if !missing_tables.is_empty() {
        let mut names: Vec<String> = missing_tables.into_iter().collect();
        names.sort();
        logger::debug(&format!(
            "splitDumpByTable: {} missing PK tables ({}): {}",
            label,
            names.len(),
            names.join(", ")
        ));
    }

    logger::debug(&format!(
        "splitDumpByTable: {} saw {} tables in INSERTs, wrote {} table files",
        label,
        seen_tables.len(),
        table_paths.len()
    ));

    Ok(table_paths)
}

// Normalize table name lookup to handle case differences.
fn resolve_table_name(pk_map: &HashMap<String, Vec<String>>, name: &str) -> Option<String> {
    if pk_map.contains_key(name) {
        return Some(name.to_string());
    }
    let lower = name.to_lowercase();
    for key in pk_map.keys() {
        if key.to_lowercase() == lower {
            return Some(key.clone());
        }
    }
    None
}

// Sanitize table name for filenames and append a short hash to avoid collisions.
fn sanitize_filename(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("table");
    }
    let sum = blake3::hash(name.as_bytes());
    format!("{}_{}", out, hex::encode(&sum.as_bytes()[..4]))
}

// Compare a single table using a per-table SQLite database.
fn compare_table_sqlite(
    table: &str,
    pk_cols: &[String],
    old_path: &str,
    new_path: &str,
    tunables: SqliteTunables,
    timing_enabled: bool,
) -> Result<(ComparisonResult, TableTiming), Box<dyn std::error::Error + Send + Sync>> {
    let mut result = ComparisonResult {
        changes: String::new(),
        deletions: String::new(),
        insert_count: 0,
        update_count: 0,
        delete_count: 0,
    };

    if old_path.is_empty() && new_path.is_empty() {
        return Ok((
            result,
            TableTiming {
                table: table.to_string(),
                load_ms: 0,
                compare_ms: 0,
                delete_ms: 0,
            },
        ));
    }

    let db_file = tempfile::Builder::new()
        .prefix("sqldumpdiff_table_")
        .suffix(".db")
        .tempfile()?;
    let db_path = db_file.path().to_path_buf();
    drop(db_file);

    let mut conn = Connection::open(&db_path)?;
    apply_sqlite_pragmas(&conn, tunables)?;
    setup_sqlite_schema(&conn)?;

    let mut timing = TableTiming {
        table: table.to_string(),
        load_ms: 0,
        compare_ms: 0,
        delete_ms: 0,
    };

    if !old_path.is_empty() {
        let load_start = Instant::now();
        load_table_jsonl(&mut conn, old_path, pk_cols, tunables.batch_size)?;
        timing.load_ms = load_start.elapsed().as_millis();
        if timing_enabled || logger::is_debug() {
            logger::debug(&format!(
                "Timing: {} load old rows took {:?}",
                table,
                load_start.elapsed()
            ));
        }
    }

    let mut seen: AHashSet<String> = AHashSet::new();

    if !new_path.is_empty() {
        let compare_start = Instant::now();
        compare_new_rows(&conn, new_path, pk_cols, &mut result, &mut seen)?;
        timing.compare_ms = compare_start.elapsed().as_millis();
        if timing_enabled || logger::is_debug() {
            logger::debug(&format!(
                "Timing: {} compare new rows took {:?}",
                table,
                compare_start.elapsed()
            ));
        }
    }

    let delete_start = Instant::now();
    emit_deletions(&conn, pk_cols, &mut result, &seen)?;
    timing.delete_ms = delete_start.elapsed().as_millis();
    if timing_enabled || logger::is_debug() {
        logger::debug(&format!(
            "Timing: {} deletions step done ({:?})",
            table,
            delete_start.elapsed()
        ));
    }

    // Clean up temp DB files (best effort).
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(db_path.with_extension("db-shm"));
    let _ = fs::remove_file(db_path.with_extension("db-wal"));

    Ok((result, timing))
}

// Load old rows into SQLite.
fn load_table_jsonl(
    conn: &mut Connection,
    path: &str,
    pk_cols: &[String],
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let mut tx = conn.transaction()?;
    let mut stmt = tx.prepare_cached(
        "INSERT OR REPLACE INTO rows(pk_hash, row_hash, row_json) VALUES(?1, ?2, ?3)",
    )?;

    let mut batch = 0usize;
    let mut line = String::new();

    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let row = InsertRow::from_json(&line)?;
        let pk = hash_pk(&row, pk_cols);
        let rh = row_hash(&row);
        stmt.execute(params![pk, rh, line])?;
        batch += 1;
        if batch >= batch_size {
            drop(stmt);
            tx.commit()?;
            tx = conn.transaction()?;
            stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO rows(pk_hash, row_hash, row_json) VALUES(?1, ?2, ?3)",
            )?;
            batch = 0;
        }
    }

    drop(stmt);
    tx.commit()?;
    Ok(())
}

// Compare new rows to old rows stored in SQLite.
fn compare_new_rows(
    conn: &Connection,
    new_path: &str,
    pk_cols: &[String],
    result: &mut ComparisonResult,
    seen: &mut AHashSet<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = File::open(new_path)?;
    let mut reader = BufReader::new(file);

    let mut stmt = conn.prepare("SELECT row_hash, row_json FROM rows WHERE pk_hash = ?1")?;

    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let new_row = InsertRow::from_json(&line)?;
        let pk = hash_pk(&new_row, pk_cols);
        let new_hash = row_hash(&new_row);

        let mut rows = stmt.query(params![pk.clone()])?;
        if let Some(row) = rows.next()? {
            let old_hash: Vec<u8> = row.get(0)?;
            let old_row_str: String = row.get(1)?;
            seen.insert(pk.clone());
            if old_hash == new_hash {
                continue;
            }
            let old_row = InsertRow::from_json(&old_row_str)?;
            let updates = find_updates(&old_row, &new_row);
            if !updates.is_empty() {
                result.changes.push_str(&format!("-- TABLE {}\n", new_row.table));
                for (col, old_val) in &updates {
                    result
                        .changes
                        .push_str(&format!("-- {} old value: {}\n", col, old_val));
                }
                result
                    .changes
                    .push_str(&build_update_statement(&new_row, pk_cols, &updates));
                result.changes.push_str("\n\n");
                result.update_count += 1;
            }
        } else {
            result
                .changes
                .push_str(&format!("-- NEW RECORD IN {}\n", new_row.table));
            result
                .changes
                .push_str(&build_insert_statement(&new_row));
            result.changes.push_str("\n\n");
            result.insert_count += 1;
        }
    }

    Ok(())
}

// Emit deletions for rows not seen in new file.
fn emit_deletions(
    conn: &Connection,
    pk_cols: &[String],
    result: &mut ComparisonResult,
    seen: &AHashSet<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut stmt = conn.prepare("SELECT pk_hash, row_json FROM rows")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let pk_hash: String = row.get(0)?;
        let row_str: String = row.get(1)?;
        if seen.contains(&pk_hash) {
            continue;
        }
        let old_row = InsertRow::from_json(&row_str)?;
        result
            .deletions
            .push_str(&format!("-- DELETED FROM {}: {}\n", old_row.table, pk_hash));
        result
            .deletions
            .push_str(&build_delete_statement(&old_row, pk_cols));
        result.deletions.push_str("\n\n");
        result.delete_count += 1;
    }
    Ok(())
}

// SQLite pragmas tuned for speed (not durability).
fn apply_sqlite_pragmas(
    conn: &Connection,
    tunables: SqliteTunables,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Use execute_batch to avoid errors for PRAGMAs that return rows.
    let cache_stmt = format!("PRAGMA cache_size={};", -tunables.cache_kb);
    let mmap_stmt = format!("PRAGMA mmap_size={};", tunables.mmap_mb << 20);
    let sql = format!(
        "PRAGMA journal_mode=OFF;\
         PRAGMA synchronous=OFF;\
         PRAGMA temp_store=MEMORY;\
         {}\
         PRAGMA page_size=32768;\
         {}\
         PRAGMA busy_timeout=5000;",
        cache_stmt, mmap_stmt
    );
    conn.execute_batch(&sql)?;
    Ok(())
}

// Create the schema for per-table DB.
fn setup_sqlite_schema(conn: &Connection) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // We don't need Send/Sync here, but keep error types consistent.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS rows (\
            pk_hash TEXT PRIMARY KEY,\
            row_hash BLOB NOT NULL,\
            row_json TEXT NOT NULL\
        );",
        [],
    )?;
    Ok(())
}

// Compute row hash using column order.
fn row_hash(row: &InsertRow) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    for col in &row.columns {
        // Keep case-insensitive behavior while avoiding extra allocations when possible.
        let lower = col.to_lowercase();
        hasher.update(lower.as_bytes());
        hasher.update(b"=");
        let val = get_column_value_ci(row, col);
        let norm = normalize_null(&val);
        hasher.update(norm.as_bytes());
        hasher.update(b";");
    }
    hasher.finalize().as_bytes().to_vec()
}

// Hash PK values into a deterministic key.
fn hash_pk(row: &InsertRow, pk_cols: &[String]) -> String {
    let mut hasher = blake3::Hasher::new();
    for (idx, col) in pk_cols.iter().enumerate() {
        if idx > 0 {
            hasher.update(b"|");
        }
        let val = get_column_value_ci(row, col);
        let norm = normalize_null(&val);
        hasher.update(norm.as_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

// Case-insensitive value lookup from row data.
fn get_column_value_ci(row: &InsertRow, col_name: &str) -> String {
    if let Some(val) = row.data.get(col_name) {
        return val.clone();
    }
    let target = col_name.to_lowercase();
    for (k, v) in &row.data {
        if k.to_lowercase() == target {
            return v.clone();
        }
    }
    String::new()
}

// Find changed columns and return old values for comments.
fn find_updates(old_row: &InsertRow, new_row: &InsertRow) -> HashMap<String, String> {
    let mut updates = HashMap::new();
    for col in &new_row.columns {
        let new_val = get_column_value_ci(new_row, col);
        let old_val = get_column_value_ci(old_row, col);
        let old_norm = normalize_null(&old_val);
        let new_norm = normalize_null(&new_val);
        if old_norm != new_norm {
            updates.insert(col.clone(), old_val);
        }
    }
    updates
}

// Normalize NULL: return "\0" for NULL, strip quotes for non-NULL.
fn normalize_null(val: &str) -> String {
    if val.eq_ignore_ascii_case("NULL") {
        return "\0".to_string();
    }
    if val.is_empty() {
        return String::new();
    }

    let mut v = val.trim().to_string();
    if v.len() >= 2 && v.starts_with('\'') && v.ends_with('\'') {
        v = v[1..v.len() - 1].to_string();
        v = v.replace("''", "'");
    }
    v
}

// Build INSERT SQL.
fn build_insert_statement(row: &InsertRow) -> String {
    let cols: Vec<String> = row
        .columns
        .iter()
        .map(|c| format!("`{}`", c))
        .collect();
    let vals: Vec<String> = row
        .columns
        .iter()
        .map(|c| format_sql_value(row.data.get(c).cloned().unwrap_or_default().as_str()))
        .collect();
    format!(
        "INSERT INTO `{}` ({}) VALUES ({});",
        row.table,
        cols.join(", "),
        vals.join(", ")
    )
}

// Build UPDATE SQL with changed columns.
fn build_update_statement(
    row: &InsertRow,
    pk_cols: &[String],
    changed: &HashMap<String, String>,
) -> String {
    let mut set_parts = Vec::new();
    for col in changed.keys() {
        let val = row.data.get(col).cloned().unwrap_or_default();
        set_parts.push(format!("`{}`={}", col, format_sql_value(&val)));
    }
    let where_clause = build_where_clause(row, pk_cols);
    format!(
        "UPDATE `{}` SET {} WHERE {};",
        row.table,
        set_parts.join(", "),
        where_clause
    )
}

// Build DELETE SQL.
fn build_delete_statement(row: &InsertRow, pk_cols: &[String]) -> String {
    let where_clause = build_where_clause(row, pk_cols);
    format!("DELETE FROM `{}` WHERE {};", row.table, where_clause)
}

// Build WHERE clause using PK columns.
fn build_where_clause(row: &InsertRow, pk_cols: &[String]) -> String {
    let mut parts = Vec::new();
    for col in pk_cols {
        let val = row.data.get(col).cloned().unwrap_or_default();
        let normalized = normalize_null(&val);
        if normalized == "\0" {
            parts.push(format!("`{}` IS NULL", col));
        } else {
            let escaped = normalized.replace('\'', "''");
            parts.push(format!("`{}`='{}'", col, escaped));
        }
    }
    parts.join(" AND ")
}

// Format SQL values for INSERT/UPDATE (handles NULL).
fn format_sql_value(val: &str) -> String {
    let normalized = normalize_null(val);
    if normalized == "\0" {
        return "NULL".to_string();
    }
    let escaped = normalized.replace('\'', "''");
    format!("'{}'", escaped)
}

// Lock helper that converts PoisonError into a readable error for `?` use.
fn lock_or_err<'a, T>(
    m: &'a Mutex<T>,
    ctx: &str,
) -> Result<MutexGuard<'a, T>, Box<dyn std::error::Error + Send + Sync>> {
    m.lock()
        .map_err(|_| format!("{}: mutex poisoned", ctx).into())
}
