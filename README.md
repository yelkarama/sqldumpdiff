# SQL Dump Diff

A Python tool to compare two SQL dump files and generate a delta SQL script containing INSERT, UPDATE, and DELETE statements to transform the old dump into the new dump.

## Features

- 🔍 **Schema-aware comparison**: Automatically detects PRIMARY KEY constraints to identify records
- 📊 **Progress tracking**: Visual progress bars for large dump files using `tqdm`
- 📈 **Summary statistics**: Displays counts of inserts, updates, and deletes at the end
- 🔄 **Multi-line support**: Handles multi-line CREATE TABLE and INSERT statements
- 📦 **Multi-row INSERT support**: Handles default mysqldump multi-value INSERTs and INSERTs without explicit column lists
- 🧵 **Parallel per-table diffing**: Splits dumps by table and optionally processes tables in parallel
- 🎯 **Composite keys**: Supports tables with composite primary keys
- 📝 **Delta generation**: Produces a complete SQL script with:
  - INSERT statements for new records
  - UPDATE statements for modified records
  - DELETE statements for removed records
- ⚡ **Efficient processing**: Indexes records by primary key for fast lookups
- 🔌 **Flexible output**: Output to file or stdout for easy piping to other commands

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

### Prerequisites

- Python 3.14 or higher
- [uv](https://github.com/astral-sh/uv) installed

### Setup

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd sqldumpdiff
   ```

2. Install dependencies with uv:
   ```bash
   uv sync
   ```

## Usage

### Basic Usage

The script accepts two required arguments and an optional third argument:

```bash
uv run sqldumpdiff.py <old_dump.sql> <new_dump.sql> [output.sql]
```

- **If `output.sql` is provided**: The delta script is written to that file
- **If `output.sql` is omitted**: The delta script is printed to stdout (useful for piping)

**Note**: Progress messages and summary statistics are always printed to stderr, so they won't interfere with stdout output when piping.

### Examples

#### Write to a file

```bash
uv run sqldumpdiff.py database_old.sql database_new.sql delta.sql
```

This creates `delta.sql` with the delta script.

#### Print to stdout

```bash
uv run sqldumpdiff.py database_old.sql database_new.sql
```

The delta SQL is printed to stdout. Progress messages appear on stderr.

#### Pipe to MySQL

```bash
uv run sqldumpdiff.py database_old.sql database_new.sql | mysql mydatabase
```

#### Pipe to a file

```bash
uv run sqldumpdiff.py database_old.sql database_new.sql > delta.sql
```

### Output Example

The generated delta script will contain:

```sql
-- Full Delta Update Script
SET FOREIGN_KEY_CHECKS = 0;

-- NEW RECORD IN users
INSERT INTO `users` (`id`, `name`, `email`) VALUES ('1', 'John Doe', 'john@example.com');

-- email old value: old@example.com
UPDATE `users` SET `email`='new@example.com' WHERE `id`='1';

-- DELETIONS
-- DELETED FROM users: ('2',)
DELETE FROM `users` WHERE `id`='2';

SET FOREIGN_KEY_CHECKS = 1;
```

### Summary Output

After processing, the script displays a summary on stderr:

```
============================================================
SUMMARY
============================================================
Inserts:  1,234
Updates:  567
Deletes: 89
Total:    1,890
============================================================
```

## How It Works

The tool performs the following steps:

1. **Schema Parsing**: Extracts PRIMARY KEY definitions and column order from CREATE TABLE statements
2. **Per-table splitting**: Streams INSERT statements and writes per-table row files (handles multi-row VALUES)
3. **Comparison (parallel-capable)**: For each table, compares new rows vs old by primary key
   - Records not found in the old dump → INSERT statements
   - Records with changed values → UPDATE statements
   - Records matched are tracked to identify deletions
4. **Deletion Detection**: Identifies records present in the old dump but missing from the new dump → DELETE statements

## Requirements

- Python 3.14+
- `tqdm` for progress bars

## Limitations

- Tables without PRIMARY KEY constraints are skipped (records cannot be uniquely identified)
- Requires both dump files to be valid SQL with proper encoding (UTF-8)

## Performance

### Optimization Strategies

The tool uses multiple strategies to handle large SQL dumps efficiently:

1. **Streaming approach**: Processes data incrementally to handle files larger than available memory

2. **Parallel processing**:
   - INSERT statement parsing can be parallelized during the split phase using process-based parallelism to bypass Python's GIL
   - Table comparison runs in parallel with configurable workers
   - Both old and new files are split concurrently

### Environment Variables

Parallelism is enabled by default when more than one table exists. Fine-tune performance with these environment variables:

#### Table Comparison

- `SQLDUMPDIFF_PARALLEL=0` - Disable parallel processing entirely
- `SQLDUMPDIFF_WORKERS=<n>` - Set worker count for table comparison (default: CPU cores)
- `SQLDUMPDIFF_WORKERS=-1` - Use as many workers as there are tables (subject to hard cap)
- `SQLDUMPDIFF_MAX_WORKERS=<n>` - Hard cap on workers (default: 4x CPU cores)
- `SQLDUMPDIFF_EXECUTOR=thread` - Use threads instead of processes for table comparison (default: process)

#### INSERT Parsing Optimization

- `SQLDUMPDIFF_PARALLEL_INSERTS=1` - Enable parallel INSERT processing during splitting (recommended for large files)
- `SQLDUMPDIFF_INSERT_WORKERS=<n>` - Worker count for INSERT parsing (default: CPU cores)
- `SQLDUMPDIFF_INSERT_EXECUTOR=thread` - Use threads instead of processes (default: process)

### Performance Tips

For best performance with large dumps:

```bash
# Recommended settings for large files
SQLDUMPDIFF_PARALLEL_INSERTS=1 \
SQLDUMPDIFF_EXECUTOR=thread \
SQLDUMPDIFF_WORKERS=-1 \
uv run sqldumpdiff.py old.sql new.sql > delta.sql
```

**Note**: Multi-row INSERTs are expanded per row during splitting to avoid redundant parsing during comparison.

## Benchmarking

To find the optimal worker settings for your system, use the provided benchmark script:

```bash
./benchmark.sh 11.sql 22.sql
```

The script will test various configurations and save results with timing information. Common findings:

- **Process-based parallelism** (`INSERT_EXECUTOR=process`) performs better than threads for CPU-intensive INSERT parsing
- **Parallel INSERT processing** (`PARALLEL_INSERTS=1`) significantly speeds up large files (3-4x improvement)
- **Worker count**: Start with CPU cores, experiment with 2x CPU cores for I/O bound workloads
- **Table comparison**: Thread-based (`EXECUTOR=thread`) often performs better than processes when you have many small tables

Review `benchmark_results_*.txt` to identify the fastest configuration for your data.

## Output Format

The generated delta script:

- Disables foreign key checks at the start
- Includes comments indicating the type of change (NEW RECORD, UPDATE, DELETION)
- For UPDATEs, includes comments showing the old values
- Re-enables foreign key checks at the end

## Building a Native Executable

You can create a standalone executable that doesn't require Python to be installed.

### Prerequisites

Install build dependencies:

```bash
uv sync --extra dev
```

### Building

Use the provided build script:

```bash
./build.sh
```

Or build manually with PyInstaller:

```bash
uv run pyinstaller --onefile --name sqldumpdiff sqldumpdiff.py
```

The executable will be created in the `dist/` directory.

### Using the Executable

After building, you can use the executable directly:

```bash
# Write to file
./dist/sqldumpdiff <old_dump.sql> <new_dump.sql> output.sql

# Print to stdout
./dist/sqldumpdiff <old_dump.sql> <new_dump.sql>
```

The executable is platform-specific (macOS, Linux, or Windows) and includes all dependencies, so it can be distributed without requiring Python or any packages to be installed.

## Development

### Running the Script

```bash
# With output file
uv run python sqldumpdiff.py <old_dump.sql> <new_dump.sql> <output.sql>

# Print to stdout
uv run python sqldumpdiff.py <old_dump.sql> <new_dump.sql>
```

### Project Structure

```
sqldumpdiff/
├── sqldumpdiff.py    # Main script
├── pyproject.toml    # Project configuration
├── build.sh          # Build script for executable
├── sqldumpdiff.spec  # PyInstaller spec file
├── uv.lock           # Dependency lock file
└── README.md         # This file
```

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
