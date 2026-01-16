// Comparison pipeline: split dumps into per-table binary files, then compare via store.
// This mirrors the Go implementation closely for consistent behavior.

use crate::logger;
use crate::parser::insert::InsertParser;
use crate::parser::InsertRow;
use rayon::prelude::*;
use ahash::AHashSet;
use blake3;
use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, atomic::{AtomicU64, Ordering}};
use std::time::Instant;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

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

// Store tuning knobs (mirrors Go defaults).
#[derive(Clone, Copy)]
pub struct StoreTunables {
    pub reader_kb: usize,
    pub writer_kb: usize,
    pub workers: usize,
    pub mmap: bool,
}

impl Default for StoreTunables {
    fn default() -> Self {
        Self {
            reader_kb: 1024,
            writer_kb: 1024,
            workers: 0,
            mmap: true,
        }
    }
}

pub struct DeltaGenerator {
    pk_map: HashMap<String, Vec<String>>,
    old_columns: HashMap<String, Vec<String>>,
    new_columns: HashMap<String, Vec<String>>,
    tunables: StoreTunables,
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
        tunables: StoreTunables,
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
        let writer_kb = self.tunables.writer_kb;

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
                    writer_kb,
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
                    writer_kb,
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

                match compare_table_store(
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
    writer_kb: usize,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
    let insert_parser = InsertParser::new();

    let mut writers: HashMap<String, TableBinWriter> = HashMap::new();
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
            let pk_cols = match pk_map.get(&resolved) {
                Some(cols) => cols,
                None => return,
            };
            if !row_has_pk(&row, pk_cols) {
                return;
            }

            let writer = writers.entry(resolved.clone()).or_insert_with(|| {
                let name = sanitize_filename(&resolved);
                let path = tmp_dir.join(format!("{}_{}.bin", label, name));
                let file = File::create(&path).expect("create temp table file");
                table_paths.insert(resolved.clone(), path.to_string_lossy().to_string());
                let cap = std::cmp::max(1024 * 1024, writer_kb * 1024);
                let mut w = BufWriter::with_capacity(cap, file);
                // Write header once per table (columns are stable for the table).
                if let Err(e) = write_table_header(&mut w, &row.columns) {
                    logger::debug(&format!("splitDumpByTable: failed to write header: {}", e));
                }
                TableBinWriter {
                    columns: row.columns.clone(),
                    writer: w,
                }
            });

            // Ensure columns are consistent; skip inconsistent rows to avoid corruption.
            if row.columns.len() != writer.columns.len() {
                logger::debug(&format!(
                    "splitDumpByTable: skip row for {} (column count mismatch {} != {})",
                    resolved,
                    row.columns.len(),
                    writer.columns.len()
                ));
                return;
            }
            let mut values = Vec::with_capacity(writer.columns.len());
            for col in &writer.columns {
                values.push(row.data.get(col).cloned().unwrap_or_default());
            }
            if let Err(e) = write_row_values(&mut writer.writer, &values) {
                logger::debug(&format!("splitDumpByTable: failed to write row: {}", e));
            }
        },
    )?;

    for (_, mut w) in writers {
        let _ = w.writer.flush();
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

fn row_has_pk(row: &InsertRow, pk_cols: &[String]) -> bool {
    for col in pk_cols {
        if row.data.contains_key(col) {
            continue;
        }
        let target = col.to_lowercase();
        let mut found = false;
        for key in row.data.keys() {
            if key.to_lowercase() == target {
                found = true;
                break;
            }
        }
        if !found {
            return false;
        }
    }
    true
}

struct TableStore {
    file: File,
    path: std::path::PathBuf,
    index: HashMap<[u8; 32], u64>,
    mmap: Option<Mmap>,
}

impl TableStore {
    fn lookup(&self, pk: [u8; 32]) -> Result<Option<([u8; 32], Vec<u8>)>, Box<dyn std::error::Error + Send + Sync>> {
        let offset = match self.index.get(&pk) {
            Some(off) => *off as usize,
            None => return Ok(None),
        };
        if let Some(mmap) = &self.mmap {
            if offset + 36 > mmap.len() {
                return Err("row header out of range".into());
            }
            let mut row_hash = [0u8; 32];
            row_hash.copy_from_slice(&mmap[offset..offset + 32]);
            let len = u32::from_le_bytes(mmap[offset + 32..offset + 36].try_into()?);
            let end = offset + 36 + len as usize;
            if end > mmap.len() {
                return Err("row blob out of range".into());
            }
            let blob = mmap[offset + 36..end].to_vec();
            return Ok(Some((row_hash, blob)));
        }
        let mut header = [0u8; 36];
        self.file.read_at(&mut header, offset as u64)?;
        let mut row_hash = [0u8; 32];
        row_hash.copy_from_slice(&header[..32]);
        let len = u32::from_le_bytes(header[32..36].try_into()?);
        let mut blob = vec![0u8; len as usize];
        if len > 0 {
            self.file.read_at(&mut blob, offset as u64 + 36)?;
        }
        Ok(Some((row_hash, blob)))
    }

    fn close(self) {
        if let Some(mmap) = self.mmap {
            drop(mmap);
        }
        let _ = fs::remove_file(&self.path);
    }
}

fn build_pk_index(columns: &[String], pk_cols: &[String]) -> Result<Vec<usize>, Box<dyn std::error::Error + Send + Sync>> {
    let mut idx = Vec::with_capacity(pk_cols.len());
    for pk in pk_cols {
        let mut found = None;
        for (i, col) in columns.iter().enumerate() {
            if col.eq_ignore_ascii_case(pk) {
                found = Some(i);
                break;
            }
        }
        match found {
            Some(i) => idx.push(i),
            None => return Err(format!("missing PK column '{}' in table columns", pk).into()),
        }
    }
    Ok(idx)
}

fn hash_pk_values_bytes(values: &[String], pk_index: &[usize]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for (i, idx) in pk_index.iter().enumerate() {
        if i > 0 {
            hasher.update(b"|");
        }
        let val = values.get(*idx).cloned().unwrap_or_default();
        let norm = normalize_null(&val);
        hasher.update(norm.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn row_hash_values_bytes(values: &[String], columns: &[String]) -> [u8; 32] {
    let mut map: HashMap<String, String> = HashMap::with_capacity(columns.len());
    for (i, col) in columns.iter().enumerate() {
        let key = col.to_lowercase();
        let val = values.get(i).cloned().unwrap_or_default();
        map.insert(key, normalize_null(&val));
    }
    let mut keys: Vec<String> = map.keys().cloned().collect();
    keys.sort();
    let mut hasher = blake3::Hasher::new();
    for key in keys {
        hasher.update(key.as_bytes());
        hasher.update(b"=");
        let val = map.get(&key).cloned().unwrap_or_default();
        hasher.update(val.as_bytes());
        hasher.update(b";");
    }
    *hasher.finalize().as_bytes()
}

fn build_table_store(
    reader: &mut BufReader<File>,
    columns: &[String],
    pk_cols: &[String],
    tunables: StoreTunables,
) -> Result<TableStore, Box<dyn std::error::Error + Send + Sync>> {
    let pk_index = build_pk_index(columns, pk_cols)?;
    let store_file = tempfile::Builder::new()
        .prefix("sqldumpdiff_store_")
        .suffix(".bin")
        .tempfile()?;
    let path = store_file.path().to_path_buf();
    let mut file = store_file.into_file();

    let cap = std::cmp::max(1024 * 1024, tunables.writer_kb * 1024);
    let mut index: HashMap<[u8; 32], u64> = HashMap::new();
    let mut offset: u64 = 0;
    {
        let mut writer = BufWriter::with_capacity(cap, &mut file);

    while let Some(values) = read_row_values(reader)? {
        let pk_hash = hash_pk_values_bytes(&values, &pk_index);
        let row_hash = row_hash_values_bytes(&values, columns);
        let blob = encode_row_values(&values);
        index.insert(pk_hash, offset);
        writer.write_all(&row_hash)?;
        writer.write_all(&(blob.len() as u32).to_le_bytes())?;
        writer.write_all(&blob)?;
        offset += 32 + 4 + blob.len() as u64;
    }
        writer.flush()?;
    }

    let mmap = if tunables.mmap {
        let len = file.metadata()?.len();
        if len > 0 {
            unsafe { Some(Mmap::map(&file)?) }
        } else {
            None
        }
    } else {
        None
    };

    Ok(TableStore { file, path, index, mmap })
}

// Compare a single table using a per-table on-disk store.
fn compare_table_store(
    table: &str,
    pk_cols: &[String],
    old_path: &str,
    new_path: &str,
    tunables: StoreTunables,
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

    let mut timing = TableTiming {
        table: table.to_string(),
        load_ms: 0,
        compare_ms: 0,
        delete_ms: 0,
    };

    let mut old_columns: Vec<String> = Vec::new();
    #[allow(unused_assignments)]
    let mut new_columns: Vec<String> = Vec::new();
    let mut store: Option<TableStore> = None;

    if !old_path.is_empty() {
        let load_start = Instant::now();
        let (cols, mut reader) = open_table_bin(old_path, tunables.reader_kb)?;
        old_columns = cols.clone();
        store = Some(build_table_store(&mut reader, &old_columns, pk_cols, tunables)?);
        timing.load_ms = load_start.elapsed().as_millis();
        if timing_enabled || logger::is_debug() {
            logger::debug(&format!(
                "Timing: {} load old rows took {:?}",
                table,
                load_start.elapsed()
            ));
        }
    }

    let mut seen: AHashSet<[u8; 32]> = AHashSet::new();

    if !new_path.is_empty() {
        let compare_start = Instant::now();
        let (cols, mut reader) = open_table_bin(new_path, tunables.reader_kb)?;
        new_columns = cols;
        if old_columns.is_empty() {
            old_columns = new_columns.clone();
        }
        compare_new_rows_store(
            table,
            &mut reader,
            &new_columns,
            &old_columns,
            pk_cols,
            store.as_ref(),
            &mut result,
            &mut seen,
        )?;
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
    emit_deletions_store(store.as_ref(), table, &old_columns, pk_cols, &mut result, &seen)?;
    timing.delete_ms = delete_start.elapsed().as_millis();
    if timing_enabled || logger::is_debug() {
        logger::debug(&format!(
            "Timing: {} deletions step done ({:?})",
            table,
            delete_start.elapsed()
        ));
    }

    if let Some(store) = store {
        store.close();
    }

    Ok((result, timing))
}
// Compare new rows to old rows stored in the on-disk store.
fn compare_new_rows_store(
    table: &str,
    reader: &mut BufReader<File>,
    new_columns: &[String],
    old_columns: &[String],
    pk_cols: &[String],
    store: Option<&TableStore>,
    result: &mut ComparisonResult,
    seen: &mut AHashSet<[u8; 32]>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pk_index = build_pk_index(new_columns, pk_cols)?;
    while let Some(values) = read_row_values(reader)? {
        let pk_hash = hash_pk_values_bytes(&values, &pk_index);
        if let Some(store) = store {
            if let Some((old_hash, old_blob)) = store.lookup(pk_hash)? {
                seen.insert(pk_hash);
                let new_hash = row_hash_values_bytes(&values, new_columns);
                if old_hash == new_hash {
                    continue;
                }
                let old_values = decode_row_values(&old_blob)?;
                let old_row = build_row_from_values(table, old_columns, &old_values);
                let new_row = build_row_from_values(table, new_columns, &values);
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
                continue;
            }
        }
        let new_row = build_row_from_values(table, new_columns, &values);
        result
            .changes
            .push_str(&format!("-- NEW RECORD IN {}\n", new_row.table));
        result
            .changes
            .push_str(&build_insert_statement(&new_row));
        result.changes.push_str("\n\n");
        result.insert_count += 1;
    }
    Ok(())
}

// Emit deletions for rows not seen in new file.
fn emit_deletions_store(
    store: Option<&TableStore>,
    table: &str,
    columns: &[String],
    pk_cols: &[String],
    result: &mut ComparisonResult,
    seen: &AHashSet<[u8; 32]>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let store = match store {
        Some(s) => s,
        None => return Ok(()),
    };
    for (pk, _) in &store.index {
        if seen.contains(pk) {
            continue;
        }
        if let Some((_hash, blob)) = store.lookup(*pk)? {
            let values = decode_row_values(&blob)?;
            let old_row = build_row_from_values(table, columns, &values);
            result
                .deletions
                .push_str(&format!("-- DELETED FROM {}: {}\n", old_row.table, hex::encode(pk)));
            result
                .deletions
                .push_str(&build_delete_statement(&old_row, pk_cols));
            result.deletions.push_str("\n\n");
            result.delete_count += 1;
        }
    }
    Ok(())
}

// Hash PK values into a deterministic key.

// Binary table file writer (header + rows).
struct TableBinWriter {
    columns: Vec<String>,
    writer: BufWriter<File>,
}

// Write a binary table header: magic, version, column list.
fn write_table_header(w: &mut BufWriter<File>, columns: &[String]) -> std::io::Result<()> {
    w.write_all(b"SQDR")?;
    write_u32(w, 1)?;
    write_u32(w, columns.len() as u32)?;
    for col in columns {
        write_bytes(w, col.as_bytes())?;
    }
    Ok(())
}

// Read header and return columns + reader.
fn open_table_bin(path: &str, reader_kb: usize) -> std::io::Result<(Vec<String>, BufReader<File>)> {
    let file = File::open(path)?;
    let cap = std::cmp::max(1024 * 1024, reader_kb * 1024);
    let mut reader = BufReader::with_capacity(cap, file);
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if &magic != b"SQDR" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid table file magic",
        ));
    }
    let version = read_u32_required(&mut reader)?;
    if version != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsupported table file version",
        ));
    }
    let col_count = read_u32_required(&mut reader)? as usize;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let bytes = read_bytes(&mut reader)?;
        columns.push(String::from_utf8(bytes).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid utf-8 column")
        })?);
    }
    Ok((columns, reader))
}

// Write a row as: num_vals + (len + bytes) per value.
fn write_row_values(w: &mut BufWriter<File>, values: &[String]) -> std::io::Result<()> {
    write_u32(w, values.len() as u32)?;
    for v in values {
        write_bytes(w, v.as_bytes())?;
    }
    Ok(())
}

// Encode values into a single blob for SQLite storage.
fn encode_row_values(values: &[String]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 8);
    let _ = write_u32(&mut buf, values.len() as u32);
    for v in values {
        let _ = write_bytes(&mut buf, v.as_bytes());
    }
    buf
}

// Decode values from a blob stored in SQLite.
fn decode_row_values(blob: &[u8]) -> std::io::Result<Vec<String>> {
    let mut reader = std::io::Cursor::new(blob);
    let count = read_u32_required(&mut reader)? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let bytes = read_bytes(&mut reader)?;
        values.push(String::from_utf8(bytes).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid utf-8 value")
        })?);
    }
    Ok(values)
}

// Read a row; returns None on EOF.
fn read_row_values(reader: &mut BufReader<File>) -> std::io::Result<Option<Vec<String>>> {
    let count = match read_u32(reader)? {
        Some(v) => v as usize,
        None => return Ok(None),
    };
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let bytes = read_bytes(reader)?;
        values.push(String::from_utf8(bytes).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid utf-8 value")
        })?);
    }
    Ok(Some(values))
}

fn write_u32<W: Write>(w: &mut W, v: u32) -> std::io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn read_u32<R: Read>(r: &mut R) -> std::io::Result<Option<u32>> {
    let mut buf = [0u8; 4];
    match r.read_exact(&mut buf) {
        Ok(()) => Ok(Some(u32::from_le_bytes(buf))),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

fn read_u32_required<R: Read>(r: &mut R) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn write_bytes<W: Write>(w: &mut W, bytes: &[u8]) -> std::io::Result<()> {
    write_u32(w, bytes.len() as u32)?;
    w.write_all(bytes)
}

fn read_bytes<R: Read>(r: &mut R) -> std::io::Result<Vec<u8>> {
    let len = read_u32_required(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

// Build InsertRow from values and columns without reparsing SQL.
fn build_row_from_values(table: &str, columns: &[String], values: &[String]) -> InsertRow {
    let mut data = HashMap::with_capacity(columns.len());
    for (i, col) in columns.iter().enumerate() {
        let val = values.get(i).cloned().unwrap_or_default();
        data.insert(col.clone(), val);
    }
    InsertRow {
        table: table.to_string(),
        columns: columns.to_vec(),
        data,
    }
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
