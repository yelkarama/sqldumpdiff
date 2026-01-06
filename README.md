# SQL Dump Diff

A Python tool to compare two SQL dump files and generate a delta SQL script containing INSERT, UPDATE, and DELETE statements to transform the old dump into the new dump.

## Features

- 🔍 **Schema-aware comparison**: Automatically detects PRIMARY KEY constraints to identify records
- 📊 **Progress tracking**: Visual progress bars for large dump files using `tqdm`
- 🔄 **Multi-line support**: Handles multi-line CREATE TABLE and INSERT statements
- 🎯 **Composite keys**: Supports tables with composite primary keys
- 📝 **Delta generation**: Produces a complete SQL script with:
  - INSERT statements for new records
  - UPDATE statements for modified records
  - DELETE statements for removed records
- ⚡ **Efficient processing**: Indexes records by primary key for fast lookups

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

```bash
python sqldumpdiff.py <old_dump.sql> <new_dump.sql>
```

This will generate a file named `full_delta_update.sql` containing the delta script.

### Example

```bash
python sqldumpdiff.py database_old.sql database_new.sql
```

The output file `full_delta_update.sql` will contain:

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

## How It Works

The tool performs the following steps:

1. **Schema Parsing**: Extracts PRIMARY KEY definitions from CREATE TABLE statements in the new dump file
2. **Indexing**: Builds an index of all records from the old dump file, keyed by table name and primary key values
3. **Comparison**: Compares records from the new dump against the indexed old records:
   - Records not found in the old dump → INSERT statements
   - Records with changed values → UPDATE statements
   - Records matched are tracked to identify deletions
4. **Deletion Detection**: Identifies records present in the old dump but missing from the new dump → DELETE statements

## Requirements

- Python 3.14+
- `tqdm` for progress bars

## Limitations

- Currently only supports single-row INSERT statements. Multi-row INSERTs (e.g., `INSERT INTO ... VALUES (...), (...), (...)`) are skipped
- Tables without PRIMARY KEY constraints are skipped (records cannot be uniquely identified)
- Requires both dump files to be valid SQL with proper encoding (UTF-8)

## Output Format

The generated delta script:
- Disables foreign key checks at the start
- Includes comments indicating the type of change (NEW RECORD, UPDATE, DELETION)
- For UPDATEs, includes comments showing the old values
- Re-enables foreign key checks at the end

## Development

### Running the Script

```bash
uv run python sqldumpdiff.py <old_dump.sql> <new_dump.sql>
```

### Project Structure

```
sqldumpdiff/
├── sqldumpdiff.py    # Main script
├── pyproject.toml     # Project configuration
├── uv.lock           # Dependency lock file
└── README.md         # This file
```

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]

