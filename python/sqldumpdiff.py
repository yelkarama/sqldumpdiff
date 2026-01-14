import re
import sys
import io
import csv
import os
import json
import tempfile
import subprocess
import shutil
import concurrent.futures
import argparse
import time
from tqdm import tqdm

# Wall-clock start time (module import time) for total runtime measurement.
_WALL_START = time.monotonic()

# Increase CSV field size limit to handle large TEXT/BLOB fields
# Default limit is 131072 (128KB), increase to handle large fields
try:
    # Try to set to 100MB, fall back to maxsize if that fails
    csv.field_size_limit(min(100 * 1024 * 1024, sys.maxsize))
except (OverflowError, ValueError):
    # If setting fails, try maxsize
    try:
        csv.field_size_limit(sys.maxsize)
    except (OverflowError, ValueError):
        pass  # Keep default limit if all else fails

# Regex Patterns
# Note: Both CREATE TABLE and INSERT statements now support multi-line parsing.
TABLE_NAME_RE = re.compile(r"CREATE TABLE `(.+?)`", re.IGNORECASE)
# Simplified INSERT patterns that don't try to capture huge VALUES parts (avoids backtracking)
INSERT_START_RE = re.compile(r"^\s*INSERT\s+INTO\s+`([^`]+)`", re.IGNORECASE)
COLUMNS_LIST_RE = re.compile(r"^\s*INSERT\s+INTO\s+`[^`]+`\s*\((.+?)\)\s*VALUES\s*", re.IGNORECASE | re.DOTALL)

# Legacy single-row matcher retained for compatibility with helpers
INSERT_RE = re.compile(r"INSERT INTO `(.+?)` \((.+?)\) VALUES \((.+?)\);", re.IGNORECASE)

def count_create_tables(filepath):
    """Counts CREATE TABLE statements in a SQL dump file."""
    count = 0
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if re.match(r'^CREATE TABLE', line, re.IGNORECASE):
                count += 1
    return count

def count_insert_statements(filepath):
    """Counts the total number of INSERT statements in a file for progress tracking."""
    count = 0
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for _ in parse_insert_statements(f):
                count += 1
    except Exception:
        # If counting fails, return None to indicate we can't show accurate progress
        return None
    return count

def count_file_lines(filepath):
    """Counts the total number of lines in a file for progress tracking."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)
    except Exception:
        return None

def get_table_schemas(filepath, show_progress=False):
    """Parses the SQL file to find Primary Keys (including composite).
    Handles multi-line CREATE TABLE statements.

    Returns a dict mapping table_name -> list of primary key columns.
    """
    schema_map = {}
    current_table = None
    table_content = []
    in_create_table = False
    
    # Count lines for progress if requested
    total_lines = count_file_lines(filepath) if show_progress else None
    
    with open(filepath, 'r', encoding='utf-8') as f:
        file_iter = tqdm(f, total=total_lines, desc="Parsing schemas", unit="lines", disable=not show_progress) if show_progress else f
        
        for line in file_iter:
            # Check if this line starts a CREATE TABLE
            table_match = TABLE_NAME_RE.search(line)
            if table_match:
                # If we were already in a table, process it first
                if in_create_table and current_table:
                    _process_table_definition(current_table, ''.join(table_content), schema_map)
                current_table = table_match.group(1)
                table_content = [line]
                in_create_table = True
                continue
            
            # If we're inside a CREATE TABLE, accumulate lines
            if in_create_table:
                table_content.append(line)
                # Check if this line ends the CREATE TABLE statement
                if line.strip().endswith(';'):
                    if current_table:
                        _process_table_definition(current_table, ''.join(table_content), schema_map)
                    current_table = None
                    table_content = []
                    in_create_table = False
        
        # Handle case where file ends without semicolon
        if in_create_table and current_table:
            _process_table_definition(current_table, ''.join(table_content), schema_map)
    
    return schema_map

def get_table_columns(filepath, show_progress=False):
    """Parses the SQL file to extract column order for each table from CREATE TABLE.
    Returns a dict mapping table_name -> list of columns in order.
    """
    columns_map = {}
    current_table = None
    table_content = []
    in_create_table = False

    total_lines = count_file_lines(filepath) if show_progress else None

    with open(filepath, 'r', encoding='utf-8') as f:
        file_iter = tqdm(f, total=total_lines, desc="Parsing columns", unit="lines", disable=not show_progress) if show_progress else f

        for line in file_iter:
            table_match = TABLE_NAME_RE.search(line)
            if table_match:
                if in_create_table and current_table:
                    columns_map[current_table] = _extract_columns_from_definition(''.join(table_content))
                current_table = table_match.group(1)
                table_content = [line]
                in_create_table = True
                continue

            if in_create_table:
                table_content.append(line)
                if line.strip().endswith(';'):
                    if current_table:
                        columns_map[current_table] = _extract_columns_from_definition(''.join(table_content))
                    current_table = None
                    table_content = []
                    in_create_table = False

        if in_create_table and current_table:
            columns_map[current_table] = _extract_columns_from_definition(''.join(table_content))

    return columns_map

def _process_table_definition(table_name, content, schema_map):
    """Extracts PRIMARY KEY from a CREATE TABLE statement (may be multi-line)."""
    # Use DOTALL flag to allow . to match newlines
    pk_match = re.search(r'PRIMARY\s+KEY\s*\((.+?)\)', content, re.IGNORECASE | re.DOTALL)
    if pk_match:
        # Extract columns, handling multi-line and whitespace
        pk_def = pk_match.group(1)
        # Remove newlines and normalize whitespace
        pk_def = ' '.join(pk_def.split())
        # Extract column names
        cols = [c.strip(' `') for c in pk_def.split(',')]
        schema_map[table_name] = cols

def _extract_columns_from_definition(content):
    """Extracts column names (in order) from a CREATE TABLE statement.
    Skips indexes and constraints.
    """
    # Isolate the portion inside the first matching parentheses after CREATE TABLE
    # This is a simple parser tracking parentheses and quotes
    in_single_quote = False
    in_double_quote = False
    escape_next = False
    paren_depth = 0
    capture = []
    started = False
    for ch in content:
        if escape_next:
            escape_next = False
            if started:
                capture.append(ch)
            continue
        if ch == '\\':
            escape_next = True
            if started:
                capture.append(ch)
            continue
        if ch == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            if started:
                capture.append(ch)
            continue
        if ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            if started:
                capture.append(ch)
            continue
        if not in_single_quote and not in_double_quote:
            if ch == '(':
                paren_depth += 1
                if not started:
                    started = True
                else:
                    capture.append(ch)
                continue
            if ch == ')':
                paren_depth -= 1
                if paren_depth == 0:
                    break
                else:
                    capture.append(ch)
                continue
        if started:
            capture.append(ch)

    inner = ''.join(capture)
    # Split into top-level items by commas
    items = []
    buf = []
    in_single_quote = False
    in_double_quote = False
    escape_next = False
    paren_depth = 0
    for ch in inner:
        if escape_next:
            buf.append(ch)
            escape_next = False
            continue
        if ch == '\\':
            buf.append(ch)
            escape_next = True
            continue
        if ch == "'" and not in_double_quote:
            buf.append(ch)
            in_single_quote = not in_single_quote
            continue
        if ch == '"' and not in_single_quote:
            buf.append(ch)
            in_double_quote = not in_double_quote
            continue
        if not in_single_quote and not in_double_quote:
            if ch == '(':
                paren_depth += 1
                buf.append(ch)
                continue
            if ch == ')':
                paren_depth -= 1
                buf.append(ch)
                continue
            if ch == ',' and paren_depth == 0:
                items.append(''.join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    if buf:
        items.append(''.join(buf).strip())

    columns = []
    for item in items:
        # Skip constraints
        if re.match(r'^(PRIMARY\s+KEY|UNIQUE\s+KEY|KEY|CONSTRAINT|FOREIGN\s+KEY)\b', item, re.IGNORECASE):
            continue
        # Extract the column name between backticks at the start
        m = re.match(r'^`([^`]+)`\s+', item)
        if m:
            columns.append(m.group(1))
    return columns

def parse_sql_values(values_str):
    """Handles CSV-style parsing of SQL values, respecting quotes and escapes."""
    f = io.StringIO(values_str)
    reader = csv.reader(f, quotechar="'", skipinitialspace=True, escapechar='\\')
    try:
        return next(reader)
    except StopIteration:
        return []

def parse_insert_statements(file_handle):
    """Generator that yields complete INSERT statements (handles multi-line).
    Yields the full INSERT statement line(s) as a string.
    Properly handles quoted strings and nested parentheses."""
    current_insert = None
    in_insert = False
    paren_depth = 0
    in_single_quote = False
    in_double_quote = False
    escape_next = False
    
    for line in file_handle:
        # Check if this line starts an INSERT statement
        if not in_insert and 'INSERT INTO' in line.upper():
            current_insert = line
            in_insert = True
            # Reset quote and escape state
            in_single_quote = False
            in_double_quote = False
            escape_next = False
            paren_depth = 0
            
            # Process the line to track state
            for char in line:
                if escape_next:
                    escape_next = False
                    continue
                if char == '\\':
                    escape_next = True
                    continue
                if char == "'" and not in_double_quote:
                    in_single_quote = not in_single_quote
                elif char == '"' and not in_single_quote:
                    in_double_quote = not in_double_quote
                elif not in_single_quote and not in_double_quote:
                    if char == '(':
                        paren_depth += 1
                    elif char == ')':
                        paren_depth -= 1
            
            # If statement is complete on one line
            if line.strip().endswith(';') and paren_depth == 0:
                yield current_insert
                current_insert = None
                in_insert = False
            continue
        
        # If we're accumulating an INSERT statement
        if in_insert:
            current_insert += line
            # Process the line to track quote state and parentheses
            for char in line:
                if escape_next:
                    escape_next = False
                    continue
                if char == '\\':
                    escape_next = True
                    continue
                if char == "'" and not in_double_quote:
                    in_single_quote = not in_single_quote
                elif char == '"' and not in_single_quote:
                    in_double_quote = not in_double_quote
                elif not in_single_quote and not in_double_quote:
                    if char == '(':
                        paren_depth += 1
                    elif char == ')':
                        paren_depth -= 1
            
            # Check if statement is complete (ends with ; and parentheses balanced)
            if line.strip().endswith(';') and paren_depth == 0:
                yield current_insert
                current_insert = None
                in_insert = False
                in_single_quote = False
                in_double_quote = False
                escape_next = False
    
    # Handle case where file ends without semicolon (shouldn't happen in dumps, but be safe)
    if in_insert and current_insert:
        yield current_insert

def _split_value_groups(values_part):
    """Splits the VALUES part "(row1),(row2),..." into a list of group strings including parentheses.
    Respects quotes, escapes, and inner parentheses (e.g., functions).
    """
    groups = []
    buf = []
    in_single_quote = False
    in_double_quote = False
    escape_next = False
    paren_depth = 0
    # Trim trailing semicolon if present
    values_part = values_part.strip()
    if values_part.endswith(';'):
        values_part = values_part[:-1]
    for ch in values_part:
        if escape_next:
            buf.append(ch)
            escape_next = False
            continue
        if ch == '\\':
            buf.append(ch)
            escape_next = True
            continue
        if ch == "'" and not in_double_quote:
            buf.append(ch)
            in_single_quote = not in_single_quote
            continue
        if ch == '"' and not in_single_quote:
            buf.append(ch)
            in_double_quote = not in_double_quote
            continue
        if not in_single_quote and not in_double_quote:
            if ch == '(':
                paren_depth += 1
                buf.append(ch)
                continue
            if ch == ')':
                paren_depth -= 1
                buf.append(ch)
                if paren_depth == 0:
                    # End of a group
                    groups.append(''.join(buf).strip())
                    buf = []
                continue
            if ch == ',' and paren_depth == 0:
                # Separator between groups; ignore
                continue
        buf.append(ch)
    # In case of trailing content (shouldn't for valid SQL)
    if buf:
        leftover = ''.join(buf).strip()
        if leftover:
            groups.append(leftover)
    return groups

def expand_insert_statement(insert_statement, columns_map_for_file):
    """Expands a potentially multi-row INSERT statement into per-row entries.
    Returns a list of tuples: (table, columns, data_dict, single_row_stmt)

    columns_map_for_file is used when the INSERT lacks an explicit column list (mysqldump default).
    """
    # Normalize whitespace for parsing (but keep original line breaks to avoid huge single lines)
    normalized = ' '.join(insert_statement.split())
    
    # Extract table name
    m_table = INSERT_START_RE.search(normalized)
    if not m_table:
        return []
    table = m_table.group(1)
    
    # Try to extract columns list
    columns = None
    values_start_pos = 0
    m_cols = COLUMNS_LIST_RE.search(normalized)
    if m_cols:
        columns = [c.strip(' `') for c in m_cols.group(1).split(',')]
        values_start_pos = m_cols.end()
    else:
        # No explicit columns, use schema
        columns = columns_map_for_file.get(table)
        # Find VALUES keyword
        values_idx = normalized.upper().find('VALUES')
        if values_idx == -1:
            return []
        values_start_pos = values_idx + 6  # len('VALUES')
    
    if not columns:
        return []
    
    # Extract values part (everything from VALUES to the semicolon)
    values_part = normalized[values_start_pos:].strip()
    if values_part.endswith(';'):
        values_part = values_part[:-1]
    
    if not values_part:
        return []

    groups = _split_value_groups(values_part)
    results = []
    # Determine if original had explicit columns by checking if COLUMNS_LIST_RE matched
    has_explicit_cols = m_cols is not None
    
    for grp in groups:
        inner = grp.strip()
        # Remove outer parentheses
        if inner.startswith('(') and inner.endswith(')'):
            inner = inner[1:-1]
        values = parse_sql_values(inner)
        if len(values) != len(columns):
            # Skip malformed row
            continue
        data = dict(zip(columns, values))
        # Build a single-row INSERT statement mirroring the original style
        if has_explicit_cols:
            # Preserve column list
            single_stmt = f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in columns)}) VALUES {grp};"
        else:
            single_stmt = f"INSERT INTO `{table}` VALUES {grp};"
        results.append((table, columns, data, single_stmt))
    return results


def _load_table_file(path):
    """Loads JSONL rows for a single table. Each line structure: {columns, data, stmt}."""
    entries = []
    if not path or not os.path.exists(path):
        return entries
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
    return entries


def _process_table(args):
    """Worker to compute delta for a single table. Designed to be picklable for multiprocessing."""
    table, pk_cols, old_path, new_path = args
    t_load_start = time.monotonic()

    old_records = {}
    # Load old rows
    for entry in _load_table_file(old_path):
        data = entry.get('data', {})
        pk_values = tuple(data.get(c) for c in pk_cols)
        old_records[pk_values] = entry
    load_ms = int((time.monotonic() - t_load_start) * 1000)

    changes_lines = []
    deletion_lines = []
    matched_old = set()
    insert_count = 0
    update_count = 0
    delete_count = 0

    # Process new rows
    t_compare_start = time.monotonic()
    for entry in _load_table_file(new_path):
        data = entry.get('data', {})
        cols = entry.get('columns', [])
        stmt = entry.get('stmt', '')
        pk_values = tuple(data.get(c) for c in pk_cols)
        old_entry = old_records.get(pk_values)

        if not old_entry:
            changes_lines.append(f"-- NEW RECORD IN {table}\n{stmt}\n")
            insert_count += 1
            continue

        matched_old.add(pk_values)
        updates = []
        comments = []
        old_data = old_entry.get('data', {})
        for col in cols:
            ov, nv = old_data.get(col), data.get(col)
            ov_norm, nv_norm = normalize_null(ov), normalize_null(nv)
            if ov_norm != nv_norm:
                comments.append(f"-- {col} old value: {ov}")
                if nv_norm is None:
                    updates.append(f"`{col}`=NULL")
                else:
                    safe_nv = str(nv).replace("'", "''")
                    updates.append(f"`{col}`='{safe_nv}'")

        if updates:
            where_str = build_where_clause(pk_cols, data)
            if comments:
                changes_lines.extend(c + "\n" for c in comments)
            changes_lines.append(f"UPDATE `{table}` SET {', '.join(updates)} WHERE {where_str};\n\n")
            update_count += 1

    compare_ms = int((time.monotonic() - t_compare_start) * 1000)

    # Handle deletions
    t_delete_start = time.monotonic()
    for pk_values, old_entry in old_records.items():
        if pk_values in matched_old:
            continue
        old_data = old_entry.get('data', {})
        where_str = build_where_clause(pk_cols, old_data)
        deletion_lines.append(f"-- DELETED FROM {table}: {pk_values}\n")
        deletion_lines.append(f"DELETE FROM `{table}` WHERE {where_str};\n\n")
        delete_count += 1
    delete_ms = int((time.monotonic() - t_delete_start) * 1000)

    return {
        'table': table,
        'changes': ''.join(changes_lines),
        'deletions': ''.join(deletion_lines),
        'insert_count': insert_count,
        'update_count': update_count,
        'delete_count': delete_count,
        'timing': {
            'load_ms': load_ms,
            'compare_ms': compare_ms,
            'delete_ms': delete_ms,
            'total_ms': load_ms + compare_ms + delete_ms,
        },
    }


def _sanitize_filename(name):
    """Sanitize table name for use in filename."""
    return re.sub(r'[^A-Za-z0-9_.-]+', '_', name)


def _process_insert_for_split(args):
    """Module-level function for parallel INSERT processing (picklable for multiprocessing)."""
    insert_stmt, cols_map = args
    return expand_insert_statement(insert_stmt, cols_map)


def split_dump_by_table(dump_file, columns_map, pk_map, tmpdir, label, show_progress=False):
    """Split a dump into per-table JSONL files for parallel processing.
    Returns a dict: table_name -> jsonl path.
    """
    table_files = {}
    file_handles = {}
    
    # Check if parallel INSERT processing is enabled
    parallel_inserts_env = os.getenv("SQLDUMPDIFF_PARALLEL_INSERTS", "0")
    parallel_inserts = parallel_inserts_env.lower() in ("1", "true", "yes")
    
    if parallel_inserts:
        # Parallel mode: collect all INSERT statements first, then process in parallel
        insert_statements = []
        with open(dump_file, 'r', encoding='utf-8') as f:
            for insert_stmt in parse_insert_statements(f):
                insert_statements.append(insert_stmt)
        
        if show_progress:
            print(f"Collected {len(insert_statements)} INSERT statements from {label}, processing in parallel...", file=sys.stderr)
        
        # Determine worker count for INSERT processing
        insert_workers_env = os.getenv("SQLDUMPDIFF_INSERT_WORKERS")
        if insert_workers_env:
            try:
                insert_workers = int(insert_workers_env)
                if insert_workers == -1:
                    insert_workers = os.cpu_count() or 4
            except ValueError:
                insert_workers = os.cpu_count() or 4
        else:
            insert_workers = os.cpu_count() or 4
        
        # Use processes for CPU-bound INSERT parsing (avoids GIL)
        executor_type = os.getenv("SQLDUMPDIFF_INSERT_EXECUTOR", "process").lower()
        if executor_type == "thread":
            executor_class = concurrent.futures.ThreadPoolExecutor
        else:
            executor_class = concurrent.futures.ProcessPoolExecutor
        
        with executor_class(max_workers=insert_workers) as executor:
            # Prepare args for process-based executor (needs to pass columns_map)
            args_list = [(stmt, columns_map) for stmt in insert_statements]
            results_iter = executor.map(_process_insert_for_split, args_list)
            if show_progress:
                results_iter = tqdm(results_iter, total=len(insert_statements), desc=f"Splitting {label}", unit="inserts", file=sys.stderr)
            
            for rows in results_iter:
                for table, cols, data, single_stmt in rows:
                    if table not in pk_map:
                        continue
                    pk_cols = pk_map[table]
                    if not all(c in data for c in pk_cols):
                        continue

                    if table not in table_files:
                        path = os.path.join(tmpdir, f"{label}_{_sanitize_filename(table)}.jsonl")
                        table_files[table] = path
                        file_handles[table] = open(path, 'w', encoding='utf-8')

                    line = json.dumps({
                        'columns': cols,
                        'data': data,
                        'stmt': single_stmt,
                    }, ensure_ascii=False)
                    file_handles[table].write(line + "\n")
    else:
        # Serial mode (default): stream and process one at a time
        with open(dump_file, 'r', encoding='utf-8') as f:
            insert_iter = parse_insert_statements(f)
            if show_progress:
                insert_iter = tqdm(insert_iter, desc=f"Splitting {label}", unit="inserts", file=sys.stderr)

            for insert_stmt in insert_iter:
                rows = expand_insert_statement(insert_stmt, columns_map)
                for table, cols, data, single_stmt in rows:
                    if table not in pk_map:
                        continue  # skip tables without PKs
                    pk_cols = pk_map[table]
                    if not all(c in data for c in pk_cols):
                        continue

                    if table not in table_files:
                        path = os.path.join(tmpdir, f"{label}_{_sanitize_filename(table)}.jsonl")
                        table_files[table] = path
                        file_handles[table] = open(path, 'w', encoding='utf-8')

                    line = json.dumps({
                        'columns': cols,
                        'data': data,
                        'stmt': single_stmt,
                    }, ensure_ascii=False)
                    file_handles[table].write(line + "\n")

    # Close handles
    for h in file_handles.values():
        h.close()

    return table_files

def normalize_null(val):
    """Normalizes None and 'NULL' string to None for consistent comparison."""
    if val is None:
        return None
    if isinstance(val, str) and val.upper() == 'NULL':
        return None
    return val

def build_where_clause(pk_cols, data):
    """Builds a SQL WHERE clause handling composite keys and NULL values."""
    where_parts = []
    for c in pk_cols:
        val = data.get(c)
        if val is None or (isinstance(val, str) and val.upper() == 'NULL'):
            where_parts.append(f"`{c}` IS NULL")
        else:
            safe_val = str(val).replace("'", "''")
            where_parts.append(f"`{c}`='{safe_val}'")
    return " AND ".join(where_parts)

def generate_delta(old_file, new_file, delta_out=None, debug=False, timing=False, timing_json=None):
    """Generates a delta SQL script comparing two SQL dump files.
    
    Args:
        old_file: Path to the old SQL dump file
        new_file: Path to the new SQL dump file
        delta_out: Path to output file (None means write to stdout)
    """
    # Normalize file paths (resolve relative paths to absolute)
    old_file = os.path.abspath(os.path.expanduser(old_file))
    new_file = os.path.abspath(os.path.expanduser(new_file))
    
    # Validate input files exist
    if not os.path.exists(old_file):
        raise FileNotFoundError(f"Old file not found: {old_file}")
    if not os.path.exists(new_file):
        raise FileNotFoundError(f"New file not found: {new_file}")
    
    # Determine output destination
    output_to_stdout = delta_out is None
    if delta_out:
        # Normalize output file path
        output_file = os.path.abspath(os.path.expanduser(delta_out))
    else:
        output_file = sys.stdout
    
    # Always send progress messages to stderr so they don't interfere with stdout
    progress_stream = sys.stderr
    
    wall_start = _WALL_START
    print("Step 1: Mapping Schemas from DDL...", file=sys.stderr)
    schema_ms = 0
    split_ms = 0
    compare_ms = 0
    delete_ms = 0
    write_ms = 0
    write_format_ms = 0
    write_io_ms = 0
    t_schema_start = time.monotonic()
    try:
        pk_map = get_table_schemas(new_file, show_progress=not debug)
        # Column maps for parsing INSERTs without column lists
        new_columns_map = get_table_columns(new_file, show_progress=not debug)
        old_columns_map = get_table_columns(old_file, show_progress=not debug)
    except Exception as e:
        raise RuntimeError(f"Error parsing schema: {e}") from e
    schema_ms = int((time.monotonic() - t_schema_start) * 1000)
    if debug or timing:
        print(f"[DEBUG] Timing: schema/columns parsing took {schema_ms}ms", file=sys.stderr)
    
    if not pk_map:
        print("Warning: No tables with PRIMARY KEY found in new file schema.", file=sys.stderr)

    # 2. Index Old Data
    old_records = {} 
    # 2. Split dumps by table and persist to temp files
    print("Step 2: Splitting dumps by table (streaming with optional parallel INSERT processing)...", file=sys.stderr)
    try:
        with tempfile.TemporaryDirectory(prefix="sqldumpdiff_") as tmpdir:
            # Split old and new files in parallel using threads (I/O bound)
            t_split_start = time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as split_executor:
                old_future = split_executor.submit(split_dump_by_table, old_file, old_columns_map, pk_map, tmpdir, "old", not debug)
                new_future = split_executor.submit(split_dump_by_table, new_file, new_columns_map, pk_map, tmpdir, "new", not debug)
                old_table_files = old_future.result()
                new_table_files = new_future.result()
            split_ms = int((time.monotonic() - t_split_start) * 1000)
                if debug or timing:
                    print(f"[DEBUG] Timing: split dumps took {split_ms}ms", file=sys.stderr)

            tables_to_process = sorted(set(old_table_files.keys()) | set(new_table_files.keys()))
            if not tables_to_process:
                print("No tables with primary keys found to process.", file=sys.stderr)
                return

            parallel_env = os.getenv("SQLDUMPDIFF_PARALLEL", "1")
            parallel = parallel_env.lower() not in ("0", "false", "no") and len(tables_to_process) > 1

            cpu_count = os.cpu_count() or 1
            hard_cap_env = os.getenv("SQLDUMPDIFF_MAX_WORKERS")
            # Default hard cap: 4x CPU cores
            hard_cap_default = max(cpu_count * 4, cpu_count)
            hard_cap = None
            try:
                hard_cap = int(hard_cap_env) if hard_cap_env else hard_cap_default
            except ValueError:
                hard_cap = hard_cap_default

            max_workers_env = os.getenv("SQLDUMPDIFF_WORKERS")
            max_workers = None
            if max_workers_env:
                try:
                    max_workers_val = int(max_workers_env)
                    if max_workers_val == -1:
                        max_workers = len(tables_to_process)
                    elif max_workers_val > 0:
                        max_workers = max_workers_val
                except ValueError:
                    max_workers = None

            # Defaults: if not specified, use CPU count
            if max_workers is None:
                max_workers = cpu_count

            # Oversubscription is allowed by default, but capped
            if parallel and hard_cap and max_workers > hard_cap:
                print(
                    f"SQLDUMPDIFF_WORKERS={max_workers} exceeds hard cap {hard_cap}; clamping to {hard_cap}. "
                    "Override cap with SQLDUMPDIFF_MAX_WORKERS.",
                    file=sys.stderr,
                )
                max_workers = hard_cap

            worker_args = [
                (table, pk_map[table], old_table_files.get(table), new_table_files.get(table))
                for table in tables_to_process
                if table in pk_map
            ]

            results = []
            mode_desc = f" using {max_workers} workers" if parallel else " serially"
            executor_type = os.getenv("SQLDUMPDIFF_EXECUTOR", "process").lower()
            if parallel:
                print(f"Step 3: Comparing {len(tables_to_process)} tables{mode_desc} ({executor_type})...", file=sys.stderr)
            else:
                print(f"Step 3: Comparing {len(tables_to_process)} tables{mode_desc}...", file=sys.stderr)

            if parallel:
                executor_type = os.getenv("SQLDUMPDIFF_EXECUTOR", "process").lower()
                if executor_type == "thread":
                    executor_class = concurrent.futures.ThreadPoolExecutor
                else:
                    executor_class = concurrent.futures.ProcessPoolExecutor
                
                with executor_class(max_workers=max_workers) as executor:
                    future_to_table = {executor.submit(_process_table, arg): arg[0] for arg in worker_args}
                    t_compare_start = time.monotonic()
                    for future in tqdm(concurrent.futures.as_completed(future_to_table), total=len(future_to_table), desc="Processing tables", unit="table", file=progress_stream, disable=debug):
                        try:
                            results.append(future.result())
                        except Exception as e:
                            print(f"Error processing table {future_to_table[future]}: {e}", file=sys.stderr)
                    compare_ms = int((time.monotonic() - t_compare_start) * 1000)
                    if debug or timing:
                        print(f"[DEBUG] Timing: compare tables took {compare_ms}ms", file=sys.stderr)
            else:
                t_compare_start = time.monotonic()
                for arg in tqdm(worker_args, desc="Processing tables", unit="table", file=progress_stream, disable=debug):
                    results.append(_process_table(arg))
                compare_ms = int((time.monotonic() - t_compare_start) * 1000)
                if debug or timing:
                    print(f"[DEBUG] Timing: compare tables took {compare_ms}ms", file=sys.stderr)

            # Open output file or use stdout
            if output_to_stdout:
                f_out = sys.stdout
            else:
                f_out = open(output_file, 'w', encoding='utf-8')

            try:
                t_write_start = time.monotonic()
                write_format_ms = 0
                write_io_ms = 0

                t_fmt = time.monotonic()
                header = "-- Full Delta Update Script\nSET FOREIGN_KEY_CHECKS = 0;\n\n"
                write_format_ms += int((time.monotonic() - t_fmt) * 1000)
                t_io = time.monotonic()
                f_out.write(header)
                write_io_ms += int((time.monotonic() - t_io) * 1000)

                insert_count = 0
                update_count = 0
                delete_count = 0

                # Write inserts/updates grouped by table (sorted for determinism)
                for res in sorted(results, key=lambda r: r['table']):
                    if res['changes']:
                        t_fmt = time.monotonic()
                        chunk = f"-- TABLE {res['table']}\n{res['changes']}"
                        write_format_ms += int((time.monotonic() - t_fmt) * 1000)
                        t_io = time.monotonic()
                        f_out.write(chunk)
                        write_io_ms += int((time.monotonic() - t_io) * 1000)
                    insert_count += res['insert_count']
                    update_count += res['update_count']

                # Deletions
                print("Step 4: Identifying Deletions...", file=sys.stderr)
                t_fmt = time.monotonic()
                del_header = "-- DELETIONS\n"
                write_format_ms += int((time.monotonic() - t_fmt) * 1000)
                t_io = time.monotonic()
                f_out.write(del_header)
                write_io_ms += int((time.monotonic() - t_io) * 1000)
                t_delete_start = time.monotonic()
                for res in sorted(results, key=lambda r: r['table']):
                    if res['deletions']:
                        t_fmt = time.monotonic()
                        dchunk = f"-- TABLE {res['table']}\n{res['deletions']}"
                        write_format_ms += int((time.monotonic() - t_fmt) * 1000)
                        t_io = time.monotonic()
                        f_out.write(dchunk)
                        write_io_ms += int((time.monotonic() - t_io) * 1000)
                    delete_count += res['delete_count']
                delete_ms = int((time.monotonic() - t_delete_start) * 1000)
                if debug or timing:
                    print(f"[DEBUG] Timing: deletion pass took {delete_ms}ms", file=sys.stderr)

                t_fmt = time.monotonic()
                tail = "SET FOREIGN_KEY_CHECKS = 1;\n"
                write_format_ms += int((time.monotonic() - t_fmt) * 1000)
                t_io = time.monotonic()
                f_out.write(tail)
                write_io_ms += int((time.monotonic() - t_io) * 1000)
                write_ms = int((time.monotonic() - t_write_start) * 1000)
            finally:
                if not output_to_stdout:
                    f_out.close()
    except Exception as e:
        raise RuntimeError(f"Error during processing: {e}") from e

    # Print summary to stderr (so it doesn't interfere with stdout output)
    total = insert_count + update_count + delete_count
    print("\n" + "="*60, file=sys.stderr)
    print("SUMMARY", file=sys.stderr)
    print("="*60, file=sys.stderr)

    if debug or timing:
        timings = []
        for r in results:
            t = r.get('timing', {})
            timings.append({
                'table': r.get('table'),
                'total_ms': t.get('total_ms', 0),
                'load_ms': t.get('load_ms', 0),
                'compare_ms': t.get('compare_ms', 0),
                'delete_ms': t.get('delete_ms', 0),
            })
        timings.sort(key=lambda t: t['total_ms'], reverse=True)
        top = timings[:10]
        if top:
            print("[DEBUG] Timing: top 10 slowest tables (ms):", file=sys.stderr)
            for t in top:
                print(
                    f"[DEBUG]   {t['table']} total={t['total_ms']} "
                    f"load={t['load_ms']} compare={t['compare_ms']} delete={t['delete_ms']}",
                    file=sys.stderr,
                )

    if timing_json:
        table_entries = [
            {
                'table': r.get('table'),
                'load_ms': r.get('timing', {}).get('load_ms', 0),
                'compare_ms': r.get('timing', {}).get('compare_ms', 0),
                'delete_ms': r.get('timing', {}).get('delete_ms', 0),
                'total_ms': r.get('timing', {}).get('total_ms', 0),
            }
            for r in results
        ]
        table_entries.sort(key=lambda t: t['total_ms'], reverse=True)
        report = {
            'schema_ms': schema_ms,
            'split_ms': split_ms,
            'compare_ms': compare_ms,
            'delete_ms': delete_ms,
            'write_ms': write_ms,
            'write_format_ms': write_format_ms,
            'write_io_ms': write_io_ms,
            'total_ms': schema_ms + split_ms + compare_ms + delete_ms + write_ms,
            'wall_ms': int((time.monotonic() - wall_start) * 1000),
            'tables': table_entries,
        }
        with open(timing_json, 'w', encoding='utf-8') as jf:
            json.dump(report, jf, indent=2)
    print(f"Inserts:  {insert_count:,}", file=sys.stderr)
    print(f"Updates:  {update_count:,}", file=sys.stderr)
    print(f"Deletes: {delete_count:,}", file=sys.stderr)
    print(f"Total:    {total:,}", file=sys.stderr)
    print("="*60, file=sys.stderr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare SQL dumps and generate delta SQL.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging and disable progress bars")
    parser.add_argument("--timing", action="store_true", help="Emit timing diagnostics even without --debug")
    parser.add_argument("old_dump", help="Old SQL dump file")
    parser.add_argument("new_dump", help="New SQL dump file")
    parser.add_argument("output", nargs="?", default=None, help="Output delta SQL (optional)")
    parser.add_argument("--timing-json", dest="timing_json", default=None, help="Write timing report JSON to file")
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()

    generate_delta(
        args.old_dump,
        args.new_dump,
        args.output,
        debug=args.debug,
        timing=args.timing,
        timing_json=args.timing_json,
    )
    if args.output:
        print(f"\nDelta script written to: {args.output}", file=sys.stderr)
