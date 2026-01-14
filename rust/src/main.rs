// sqldumpdiff Rust implementation.
// This program mirrors the Go version: parse schema and INSERTs, split by table,
// compare per-table with SQLite, and emit delta SQL.

mod comparer;
mod logger;
mod parser;
mod progress;

use clap::{CommandFactory, Parser};
use serde_json;
use comparer::{DeltaGenerator, SqliteTunables};
use parser::schema::SchemaParser;
use std::fs::File;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use std::thread;

// Command-line flags and positional arguments.
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Enable debug logging (disables progress bars).
    #[arg(long)]
    debug: bool,

    /// SQLite cache size in KB (negative = KB).
    #[arg(long, default_value_t = 800_000)]
    sqlite_cache_kb: i64,

    /// SQLite mmap size in MB.
    #[arg(long, default_value_t = 128)]
    sqlite_mmap_mb: i64,

    /// SQLite insert batch size.
    #[arg(long, default_value_t = 20_000)]
    sqlite_batch: usize,

    /// Max concurrent table compares (0 = num CPU).
    #[arg(long, default_value_t = 0)]
    sqlite_workers: usize,

    /// Emit timing diagnostics even without --debug.
    #[arg(long, default_value_t = false)]
    timing: bool,

    /// Old dump file path.
    old_dump: String,

    /// New dump file path.
    new_dump: String,

    /// Output delta SQL file (optional). If omitted, prints to stdout.
    output: Option<String>,

    /// Write timing report JSON to file.
    #[arg(long)]
    timing_json: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let wall_start = Instant::now();
    if std::env::args().len() == 1 {
        Args::command().print_help()?;
        eprintln!();
        std::process::exit(1);
    }
    let args = Args::parse();

    // Initialize logging based on --debug.
    logger::set_debug(args.debug);

    logger::debug("main: Starting SQL dump diff");
    logger::debug(&format!("main: Old file: {}", args.old_dump));
    logger::debug(&format!("main: New file: {}", args.new_dump));
    if let Some(out) = &args.output {
        logger::debug(&format!("main: Delta file: {}", out));
    }

    // Progress bars are disabled in debug mode to avoid mangled output.
    let progress = progress::ProgressManager::new(!args.debug);

    // We construct parser instances for each worker thread below, so this is unused.

    // Parse schemas + columns in parallel (old/new).
    let old_dump = args.old_dump.clone();
    let new_dump = args.new_dump.clone();

    let progress_schema_old = progress
        .new_file_bar(&old_dump, &format!("Schema {}", basename(&old_dump)));
    let progress_schema_new = progress
        .new_file_bar(&new_dump, &format!("Schema {}", basename(&new_dump)));
    let progress_cols_old = progress
        .new_file_bar(&old_dump, &format!("Columns {}", basename(&old_dump)));
    let progress_cols_new = progress
        .new_file_bar(&new_dump, &format!("Columns {}", basename(&new_dump)));

    let schema_parser_old = SchemaParser::new();
    let schema_parser_new = SchemaParser::new();
    let schema_parser_cols_old = SchemaParser::new();
    let schema_parser_cols_new = SchemaParser::new();

    let schema_start = Instant::now();
    let old_schema_handle = thread::spawn(move || {
        schema_parser_old.parse_schemas(&old_dump, progress_schema_old.as_ref())
    });
    let new_schema_handle = thread::spawn(move || {
        schema_parser_new.parse_schemas(&new_dump, progress_schema_new.as_ref())
    });

    let old_cols_dump = args.old_dump.clone();
    let new_cols_dump = args.new_dump.clone();
    let old_cols_handle = thread::spawn(move || {
        schema_parser_cols_old.parse_columns(&old_cols_dump, progress_cols_old.as_ref())
    });
    let new_cols_handle = thread::spawn(move || {
        schema_parser_cols_new.parse_columns(&new_cols_dump, progress_cols_new.as_ref())
    });

    let mut old_pk = old_schema_handle
        .join()
        .map_err(|_| "old schema thread panicked")??;
    let new_pk = new_schema_handle
        .join()
        .map_err(|_| "new schema thread panicked")??;
    let old_cols = old_cols_handle
        .join()
        .map_err(|_| "old columns thread panicked")??;
    let new_cols = new_cols_handle
        .join()
        .map_err(|_| "new columns thread panicked")??;
    logger::debug(&format!(
        "Timing: schema/columns parsing took {:?}",
        schema_start.elapsed()
    ));

    // Merge PK maps (prefer new file PKs if present).
    for (table, pk) in new_pk {
        old_pk.insert(table, pk);
    }

    let tunables = SqliteTunables {
        cache_kb: args.sqlite_cache_kb,
        mmap_mb: args.sqlite_mmap_mb,
        batch_size: args.sqlite_batch,
        workers: args.sqlite_workers,
    };

    let generator = DeltaGenerator::new(old_pk, old_cols, new_cols, tunables, args.timing);

    // Output target.
    let out: Box<dyn Write + Send> = if let Some(path) = args.output {
        Box::new(File::create(path)?)
    } else {
        Box::new(io::stdout())
    };
    let out = Arc::new(Mutex::new(out));

    let total_start = Instant::now();
    let (summary, timing_report) =
        generator.generate_delta(&args.old_dump, &args.new_dump, &progress, Arc::clone(&out))?;
    if args.timing || logger::is_debug() {
        logger::debug(&format!(
            "Timing: total wall time {:?}",
            wall_start.elapsed()
        ));
    }

    if let Some(path) = args.timing_json.as_ref() {
        let report = serde_json::json!({
            "schema_ms": schema_start.elapsed().as_millis(),
            "split_ms": timing_report.split_ms,
            "compare_ms": timing_report.compare_ms,
            "delete_ms": timing_report.delete_ms,
            "write_ms": timing_report.write_ms,
            "write_format_ms": timing_report.write_format_ms,
            "write_io_ms": timing_report.write_io_ms,
            "total_ms": schema_start.elapsed().as_millis()
                + timing_report.split_ms
                + timing_report.compare_ms
                + timing_report.delete_ms
                + timing_report.write_ms,
            "wall_ms": wall_start.elapsed().as_millis(),
            "tables": timing_report.tables,
        });
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(path, json)?;
    }
    logger::debug(&format!(
        "Timing: total delta generation took {:?}",
        total_start.elapsed()
    ));

    // Print summary in Java-style format.
    let sep = "=".repeat(60);
    {
        let mut stderr = io::stderr();
        writeln!(stderr, "\n{}\nSUMMARY\n{}", sep, sep)?;
        writeln!(stderr, "Inserts:  {}", summary.insert_count)?;
        writeln!(stderr, "Updates:  {}", summary.update_count)?;
        writeln!(stderr, "Deletes:  {}", summary.delete_count)?;
        writeln!(stderr, "Total:    {}", summary.insert_count + summary.update_count + summary.delete_count)?;
        writeln!(stderr, "{}", sep)?;
    }

    logger::debug("main: Delta generation complete");
    Ok(())
}

fn basename(path: &str) -> String {
    std::path::Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(path)
        .to_string()
}
